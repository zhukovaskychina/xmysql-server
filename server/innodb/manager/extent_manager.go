package manager

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	extent2 "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/extent"
	"sync"
)

// ExtentManager 区管理器
type ExtentManager struct {
	sync.RWMutex

	// 底层存储
	bufferPool *buffer_pool.BufferPool

	// 区缓存
	extentCache map[uint32]*extent2.BaseExtent // key: extentID

	// 空闲区列表
	freeExtents []uint32

	// 统计信息
	stats *ExtentStats
}

// ExtentStats 区统计信息
type ExtentStats struct {
	TotalExtents   uint32  // 总区数
	FreeExtents    uint32  // 空闲区数
	FullExtents    uint32  // 已满区数
	FragmentRatio  float64 // 碎片率
	AvgUtilization float64 // 平均利用率
}

// NewExtentManager 创建区管理器
func NewExtentManager(bp *buffer_pool.BufferPool) *ExtentManager {
	return &ExtentManager{
		bufferPool:  bp,
		extentCache: make(map[uint32]*extent2.BaseExtent),
		freeExtents: make([]uint32, 0),
		stats:       &ExtentStats{},
	}
}

// AllocateExtent 分配新区
func (em *ExtentManager) AllocateExtent(spaceID uint32, extType basic.ExtentType) (*extent2.BaseExtent, error) {
	em.Lock()
	defer em.Unlock()

	// 优先从空闲列表分配
	var extentID uint32
	if len(em.freeExtents) > 0 {
		extentID = em.freeExtents[len(em.freeExtents)-1]
		em.freeExtents = em.freeExtents[:len(em.freeExtents)-1]
	} else {
		// 创建新区
		extentID = em.stats.TotalExtents
		em.stats.TotalExtents++
	}

	// 创建区对象
	ext := extent2.NewBaseExtent(spaceID, extentID, extType)

	// 加入缓存
	em.extentCache[extentID] = ext

	// 更新统计
	em.updateStats()

	return ext, nil
}

// GetExtent 获取区
func (em *ExtentManager) GetExtent(extentID uint32) (*extent2.BaseExtent, error) {
	em.RLock()
	defer em.RUnlock()

	// 先查缓存
	if ext, ok := em.extentCache[extentID]; ok {
		return ext, nil
	}

	// TODO: 从磁盘加载区信息

	return nil, extent2.ErrInvalidExtent
}

// FreeExtent 释放区
func (em *ExtentManager) FreeExtent(extentID uint32) error {
	em.Lock()
	defer em.Unlock()

	// 获取区对象
	ext, ok := em.extentCache[extentID]
	if !ok {
		return extent2.ErrInvalidExtent
	}

	// 重置区
	if err := ext.Reset(); err != nil {
		return err
	}

	// 加入空闲列表
	em.freeExtents = append(em.freeExtents, extentID)

	// 更新统计
	em.updateStats()

	return nil
}

// GetStats 获取统计信息
func (em *ExtentManager) GetStats() *ExtentStats {
	em.RLock()
	defer em.RUnlock()
	return em.stats
}

// updateStats 更新统计信息
func (em *ExtentManager) updateStats() {
	stats := &ExtentStats{
		TotalExtents: em.stats.TotalExtents,
		FreeExtents:  uint32(len(em.freeExtents)),
	}

	var fullCount uint32
	var totalSpace uint64
	var usedSpace uint64

	// 统计已用区
	for _, ext := range em.extentCache {
		if ext.IsFull() {
			fullCount++
		}
		totalSpace += 64 * 16 * 1024 // 64页 * 16KB
		usedSpace += 64*16*1024 - ext.GetFreeSpace()
	}

	stats.FullExtents = fullCount
	if totalSpace > 0 {
		stats.AvgUtilization = float64(usedSpace) / float64(totalSpace)
	}
	if em.stats.TotalExtents > 0 {
		stats.FragmentRatio = float64(em.stats.TotalExtents-fullCount) / float64(em.stats.TotalExtents)
	}

	em.stats = stats
}

// DefragmentExtent 整理区碎片
func (em *ExtentManager) DefragmentExtent(extentID uint32) error {
	em.Lock()
	defer em.Unlock()

	ext, ok := em.extentCache[extentID]
	if !ok {
		return extent2.ErrInvalidExtent
	}

	return ext.Defragment()
}

// GetFreeExtentCount 获取空闲区数量
func (em *ExtentManager) GetFreeExtentCount() int {
	em.RLock()
	defer em.RUnlock()
	return len(em.freeExtents)
}

// GetTotalExtentCount 获取总区数量
func (em *ExtentManager) GetTotalExtentCount() uint32 {
	em.RLock()
	defer em.RUnlock()
	return em.stats.TotalExtents
}
