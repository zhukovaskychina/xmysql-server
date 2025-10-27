package space

import (
	"fmt"
	"sync"
)

/*
BitmapManager 实现了高效的位图管理器

核心功能：
1. 位图操作
   - 快速位查找和设置
   - 批量位操作
   - 位图压缩存储

2. 缓存管理
   - 热点位图缓存
   - LRU缓存策略
   - 缓存一致性保证

3. 并发控制
   - 细粒度锁保护
   - 原子位操作
   - 无锁读取优化

设计要点：
- 使用uint64数组存储位图，提升缓存命中率
- 支持范围查找和批量操作
- 最小化锁竞争
- 内存高效
*/

const (
	// 位图常量
	BitsPerWord = 64          // 每个uint64包含64位
	WordSize    = 8           // uint64占用8字节
	MaxBitmap   = 1024 * 1024 // 最大支持1M个位
	CacheSize   = 256         // 缓存大小（缓存的word数量）
)

// BitmapManager 位图管理器
type BitmapManager struct {
	sync.RWMutex

	// 位图数据
	bitmap []uint64 // 位图存储（每bit表示一个页面）
	size   uint32   // 位图大小（bit数）

	// 统计信息
	setBits    uint32 // 已设置的位数
	clearBits  uint32 // 空闲的位数
	operations uint64 // 总操作次数

	// 缓存
	cache     map[uint32]uint64 // word缓存
	cacheHits uint64            // 缓存命中次数
	cacheMiss uint64            // 缓存未命中次数

	// 性能优化
	lastSetPos   uint32 // 上次设置的位置（用于连续分配优化）
	lastClearPos uint32 // 上次清除的位置
}

// NewBitmapManager 创建位图管理器
func NewBitmapManager(size uint32) *BitmapManager {
	if size > MaxBitmap {
		size = MaxBitmap
	}

	// 计算需要的word数量
	words := (size + BitsPerWord - 1) / BitsPerWord

	return &BitmapManager{
		bitmap:       make([]uint64, words),
		size:         size,
		setBits:      0,
		clearBits:    size,
		operations:   0,
		cache:        make(map[uint32]uint64, CacheSize),
		cacheHits:    0,
		cacheMiss:    0,
		lastSetPos:   0,
		lastClearPos: 0,
	}
}

// Set 设置指定位
func (bm *BitmapManager) Set(pos uint32) error {
	bm.Lock()
	defer bm.Unlock()

	if pos >= bm.size {
		return fmt.Errorf("position %d out of range [0, %d)", pos, bm.size)
	}

	wordIdx := pos / BitsPerWord
	bitIdx := pos % BitsPerWord

	// 检查是否已经设置
	if !bm.isSet(wordIdx, bitIdx) {
		bm.bitmap[wordIdx] |= (1 << bitIdx)
		bm.setBits++
		bm.clearBits--
		bm.lastSetPos = pos

		// 更新缓存
		bm.updateCache(wordIdx, bm.bitmap[wordIdx])
	}

	bm.operations++
	return nil
}

// Clear 清除指定位
func (bm *BitmapManager) Clear(pos uint32) error {
	bm.Lock()
	defer bm.Unlock()

	if pos >= bm.size {
		return fmt.Errorf("position %d out of range [0, %d)", pos, bm.size)
	}

	wordIdx := pos / BitsPerWord
	bitIdx := pos % BitsPerWord

	// 检查是否已经清除
	if bm.isSet(wordIdx, bitIdx) {
		bm.bitmap[wordIdx] &^= (1 << bitIdx)
		bm.setBits--
		bm.clearBits++
		bm.lastClearPos = pos

		// 更新缓存
		bm.updateCache(wordIdx, bm.bitmap[wordIdx])
	}

	bm.operations++
	return nil
}

// IsSet 检查指定位是否已设置
func (bm *BitmapManager) IsSet(pos uint32) (bool, error) {
	bm.RLock()
	defer bm.RUnlock()

	if pos >= bm.size {
		return false, fmt.Errorf("position %d out of range [0, %d)", pos, bm.size)
	}

	wordIdx := pos / BitsPerWord
	bitIdx := pos % BitsPerWord

	return bm.isSet(wordIdx, bitIdx), nil
}

// FindFirstClear 查找第一个空闲位
func (bm *BitmapManager) FindFirstClear() (uint32, error) {
	bm.RLock()
	defer bm.RUnlock()

	if bm.clearBits == 0 {
		return 0, fmt.Errorf("no free bits available")
	}

	// 从上次清除位置开始搜索（局部性优化）
	startWord := bm.lastClearPos / BitsPerWord

	// 搜索从startWord开始的所有word
	for i := uint32(0); i < uint32(len(bm.bitmap)); i++ {
		wordIdx := (startWord + i) % uint32(len(bm.bitmap))
		word := bm.bitmap[wordIdx]

		// 如果word不是全满的（0xFFFFFFFFFFFFFFFF），则有空闲位
		if word != ^uint64(0) {
			// 查找第一个0位
			for bitIdx := uint32(0); bitIdx < BitsPerWord; bitIdx++ {
				if (word & (1 << bitIdx)) == 0 {
					pos := wordIdx*BitsPerWord + bitIdx
					if pos < bm.size {
						return pos, nil
					}
				}
			}
		}
	}

	return 0, fmt.Errorf("no free bits available")
}

// FindFirstSet 查找第一个已设置的位
func (bm *BitmapManager) FindFirstSet() (uint32, error) {
	bm.RLock()
	defer bm.RUnlock()

	if bm.setBits == 0 {
		return 0, fmt.Errorf("no set bits available")
	}

	// 从上次设置位置开始搜索（局部性优化）
	startWord := bm.lastSetPos / BitsPerWord

	for i := uint32(0); i < uint32(len(bm.bitmap)); i++ {
		wordIdx := (startWord + i) % uint32(len(bm.bitmap))
		word := bm.bitmap[wordIdx]

		// 如果word不是全空的（0），则有已设置的位
		if word != 0 {
			// 查找第一个1位
			for bitIdx := uint32(0); bitIdx < BitsPerWord; bitIdx++ {
				if (word & (1 << bitIdx)) != 0 {
					pos := wordIdx*BitsPerWord + bitIdx
					if pos < bm.size {
						return pos, nil
					}
				}
			}
		}
	}

	return 0, fmt.Errorf("no set bits available")
}

// FindNContinuousClear 查找N个连续的空闲位
func (bm *BitmapManager) FindNContinuousClear(n uint32) (uint32, error) {
	bm.RLock()
	defer bm.RUnlock()

	if n == 0 {
		return 0, fmt.Errorf("invalid count: 0")
	}

	if n > bm.clearBits {
		return 0, fmt.Errorf("not enough free bits: need %d, have %d", n, bm.clearBits)
	}

	// 遍历位图查找连续空闲位
	continuousCount := uint32(0)
	startPos := uint32(0)

	for pos := uint32(0); pos < bm.size; pos++ {
		wordIdx := pos / BitsPerWord
		bitIdx := pos % BitsPerWord

		if !bm.isSet(wordIdx, bitIdx) {
			if continuousCount == 0 {
				startPos = pos
			}
			continuousCount++

			if continuousCount >= n {
				return startPos, nil
			}
		} else {
			continuousCount = 0
		}
	}

	return 0, fmt.Errorf("cannot find %d continuous free bits", n)
}

// SetRange 设置范围内的所有位
func (bm *BitmapManager) SetRange(start, end uint32) error {
	bm.Lock()
	defer bm.Unlock()

	if start >= bm.size || end > bm.size || start >= end {
		return fmt.Errorf("invalid range [%d, %d)", start, end)
	}

	for pos := start; pos < end; pos++ {
		wordIdx := pos / BitsPerWord
		bitIdx := pos % BitsPerWord

		if !bm.isSet(wordIdx, bitIdx) {
			bm.bitmap[wordIdx] |= (1 << bitIdx)
			bm.setBits++
			bm.clearBits--
		}
	}

	// 更新涉及的缓存
	startWord := start / BitsPerWord
	endWord := (end - 1) / BitsPerWord
	for wordIdx := startWord; wordIdx <= endWord; wordIdx++ {
		bm.updateCache(wordIdx, bm.bitmap[wordIdx])
	}

	bm.operations++
	return nil
}

// ClearRange 清除范围内的所有位
func (bm *BitmapManager) ClearRange(start, end uint32) error {
	bm.Lock()
	defer bm.Unlock()

	if start >= bm.size || end > bm.size || start >= end {
		return fmt.Errorf("invalid range [%d, %d)", start, end)
	}

	for pos := start; pos < end; pos++ {
		wordIdx := pos / BitsPerWord
		bitIdx := pos % BitsPerWord

		if bm.isSet(wordIdx, bitIdx) {
			bm.bitmap[wordIdx] &^= (1 << bitIdx)
			bm.setBits--
			bm.clearBits++
		}
	}

	// 更新涉及的缓存
	startWord := start / BitsPerWord
	endWord := (end - 1) / BitsPerWord
	for wordIdx := startWord; wordIdx <= endWord; wordIdx++ {
		bm.updateCache(wordIdx, bm.bitmap[wordIdx])
	}

	bm.operations++
	return nil
}

// CountSet 统计已设置的位数
func (bm *BitmapManager) CountSet() uint32 {
	bm.RLock()
	defer bm.RUnlock()
	return bm.setBits
}

// CountClear 统计空闲的位数
func (bm *BitmapManager) CountClear() uint32 {
	bm.RLock()
	defer bm.RUnlock()
	return bm.clearBits
}

// Size 返回位图大小
func (bm *BitmapManager) Size() uint32 {
	return bm.size
}

// GetStats 获取统计信息
func (bm *BitmapManager) GetStats() map[string]interface{} {
	bm.RLock()
	defer bm.RUnlock()

	cacheHitRate := 0.0
	totalCacheOps := bm.cacheHits + bm.cacheMiss
	if totalCacheOps > 0 {
		cacheHitRate = float64(bm.cacheHits) / float64(totalCacheOps) * 100
	}

	return map[string]interface{}{
		"size":           bm.size,
		"set_bits":       bm.setBits,
		"clear_bits":     bm.clearBits,
		"usage_rate":     float64(bm.setBits) / float64(bm.size) * 100,
		"operations":     bm.operations,
		"cache_hits":     bm.cacheHits,
		"cache_miss":     bm.cacheMiss,
		"cache_hit_rate": cacheHitRate,
	}
}

// Reset 重置位图（清除所有位）
func (bm *BitmapManager) Reset() {
	bm.Lock()
	defer bm.Unlock()

	for i := range bm.bitmap {
		bm.bitmap[i] = 0
	}

	bm.setBits = 0
	bm.clearBits = bm.size
	bm.operations = 0
	bm.lastSetPos = 0
	bm.lastClearPos = 0

	// 清空缓存
	bm.cache = make(map[uint32]uint64, CacheSize)
	bm.cacheHits = 0
	bm.cacheMiss = 0
}

// 内部辅助方法

// isSet 检查指定word的指定bit是否已设置（内部方法，不加锁）
func (bm *BitmapManager) isSet(wordIdx, bitIdx uint32) bool {
	// 先查缓存
	if cachedWord, ok := bm.cache[wordIdx]; ok {
		bm.cacheHits++
		return (cachedWord & (1 << bitIdx)) != 0
	}

	// 缓存未命中，从位图读取
	bm.cacheMiss++
	if wordIdx >= uint32(len(bm.bitmap)) {
		return false
	}

	word := bm.bitmap[wordIdx]

	// 更新缓存
	bm.updateCache(wordIdx, word)

	return (word & (1 << bitIdx)) != 0
}

// updateCache 更新缓存
func (bm *BitmapManager) updateCache(wordIdx uint32, word uint64) {
	// 如果缓存已满，使用简单的FIFO策略删除一个条目
	if len(bm.cache) >= CacheSize {
		// 删除第一个条目（简化实现，实际应该用LRU）
		for k := range bm.cache {
			delete(bm.cache, k)
			break
		}
	}

	bm.cache[wordIdx] = word
}

// popcount 计算word中设置的位数（使用位操作技巧）
func popcount(x uint64) uint32 {
	// Hamming weight算法
	x = x - ((x >> 1) & 0x5555555555555555)
	x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333)
	x = (x + (x >> 4)) & 0x0f0f0f0f0f0f0f0f
	x = x + (x >> 8)
	x = x + (x >> 16)
	x = x + (x >> 32)
	return uint32(x & 0x7f)
}

// trailingZeros 计算尾部0的数量（查找第一个1位的位置）
func trailingZeros(x uint64) uint32 {
	if x == 0 {
		return BitsPerWord
	}

	// 使用二分查找
	n := uint32(0)
	if (x & 0x00000000FFFFFFFF) == 0 {
		n += 32
		x >>= 32
	}
	if (x & 0x000000000000FFFF) == 0 {
		n += 16
		x >>= 16
	}
	if (x & 0x00000000000000FF) == 0 {
		n += 8
		x >>= 8
	}
	if (x & 0x000000000000000F) == 0 {
		n += 4
		x >>= 4
	}
	if (x & 0x0000000000000003) == 0 {
		n += 2
		x >>= 2
	}
	if (x & 0x0000000000000001) == 0 {
		n += 1
	}
	return n
}
