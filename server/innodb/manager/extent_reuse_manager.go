package manager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/extent"
)

/*
ExtentReuseManager 实现区复用机制

Extent复用是提升空间利用率的关键技术，避免频繁分配新Extent导致的碎片和空间浪费。

核心策略：
1. 回收策略：Extent完全释放后加入复用池
2. 智能复用：基于局部性原理优先复用相邻Extent
3. 复用监控：实时监控复用率和效率

设计要点：
- 分层复用池：按表空间/段类型分类管理
- 局部性优化：优先复用相邻区域的Extent
- 延迟回收：避免频繁回收-分配循环
- 预热机制：预先准备一定数量的空闲Extent
*/

const (
	// 复用池配置
	DefaultPoolSize = 1024 // 默认复用池大小
	MaxPoolSize     = 4096 // 最大复用池大小
	MinPoolSize     = 64   // 最小复用池大小

	// 复用策略
	ReuseStrategyFIFO     = "fifo"     // 先进先出
	ReuseStrategyLRU      = "lru"      // 最近最少使用
	ReuseStrategyLocality = "locality" // 局部性优先

	// 预热配置
	WarmupExtentCount = 16  // 预热Extent数量
	WarmupThreshold   = 0.2 // 低于20%触发预热

	// 延迟回收
	DelayedReclaimSeconds = 5 // 延迟5秒回收

	// 监控周期
	MonitorIntervalSeconds = 60 // 60秒监控周期
)

// ExtentReuseManager 区复用管理器
type ExtentReuseManager struct {
	// 复用池（按SpaceID分组）
	pools map[uint32]*ExtentReusePool

	// 配置
	config *ExtentReuseConfig

	// 统计信息
	stats *ExtentReuseStats

	// 延迟回收队列
	delayedReclaimQueue chan *DelayedReclaimEntry

	// 停止信号
	stopChan chan struct{}

	mu sync.RWMutex
}

// ExtentReusePool 单个表空间的复用池
type ExtentReusePool struct {
	SpaceID  uint32
	Strategy string

	// 按段类型分组的复用Extent
	dataExtents  []*ReuseExtent // 数据段复用
	indexExtents []*ReuseExtent // 索引段复用
	undoExtents  []*ReuseExtent // Undo段复用
	blobExtents  []*ReuseExtent // BLOB段复用

	// 容量控制
	maxSize     int
	currentSize int

	// 访问统计
	hitCount  uint64 // 复用命中次数
	missCount uint64 // 复用未命中次数

	mu sync.RWMutex
}

// ReuseExtent 可复用的Extent
type ReuseExtent struct {
	Extent      *extent.BaseExtent
	SpaceID     uint32
	ExtentNo    uint32
	SegmentType uint8

	// 复用信息
	ReclaimedAt  time.Time // 回收时间
	ReuseCount   uint32    // 复用次数
	LastAccessAt time.Time // 最后访问时间

	// 局部性信息
	PrevExtentNo uint32 // 前一个Extent编号
	NextExtentNo uint32 // 后一个Extent编号
}

// DelayedReclaimEntry 延迟回收条目
type DelayedReclaimEntry struct {
	Extent      *extent.BaseExtent
	SpaceID     uint32
	SegmentType uint8
	ReclaimTime time.Time
}

// ExtentReuseConfig 复用配置
type ExtentReuseConfig struct {
	Strategy             string // 复用策略
	PoolSize             int    // 复用池大小
	EnableWarmup         bool   // 启用预热
	EnableDelayedReclaim bool   // 启用延迟回收
	EnableLocality       bool   // 启用局部性优化
	MonitorInterval      int    // 监控间隔（秒）
}

// ExtentReuseStats 复用统计
type ExtentReuseStats struct {
	// 复用统计
	totalReused    uint64 // 总复用次数
	totalReclaimed uint64 // 总回收次数
	totalAllocated uint64 // 总分配次数

	// 命中统计
	reuseHits   uint64 // 复用命中
	reuseMisses uint64 // 复用未命中

	// 性能统计
	avgReuseTime   uint64 // 平均复用时间（纳秒）
	avgReclaimTime uint64 // 平均回收时间（纳秒）

	// 空间统计
	poolUtilization float64 // 复用池利用率
	reuseRate       float64 // 复用率

	// 错误统计
	reclaimErrors uint64 // 回收错误次数
	reuseErrors   uint64 // 复用错误次数
}

// NewExtentReuseManager 创建区复用管理器
func NewExtentReuseManager(config *ExtentReuseConfig) *ExtentReuseManager {
	if config == nil {
		config = &ExtentReuseConfig{
			Strategy:             ReuseStrategyLocality,
			PoolSize:             DefaultPoolSize,
			EnableWarmup:         true,
			EnableDelayedReclaim: true,
			EnableLocality:       true,
			MonitorInterval:      MonitorIntervalSeconds,
		}
	}

	erm := &ExtentReuseManager{
		pools:               make(map[uint32]*ExtentReusePool),
		config:              config,
		stats:               &ExtentReuseStats{},
		delayedReclaimQueue: make(chan *DelayedReclaimEntry, 1000),
		stopChan:            make(chan struct{}),
	}

	// 启动后台任务
	go erm.delayedReclaimWorker()
	go erm.monitorWorker()

	return erm
}

// GetOrCreatePool 获取或创建复用池
func (erm *ExtentReuseManager) GetOrCreatePool(spaceID uint32) *ExtentReusePool {
	erm.mu.RLock()
	pool := erm.pools[spaceID]
	erm.mu.RUnlock()

	if pool != nil {
		return pool
	}

	erm.mu.Lock()
	defer erm.mu.Unlock()

	// 双重检查
	if pool = erm.pools[spaceID]; pool != nil {
		return pool
	}

	// 创建新池
	pool = &ExtentReusePool{
		SpaceID:      spaceID,
		Strategy:     erm.config.Strategy,
		dataExtents:  make([]*ReuseExtent, 0, erm.config.PoolSize/4),
		indexExtents: make([]*ReuseExtent, 0, erm.config.PoolSize/4),
		undoExtents:  make([]*ReuseExtent, 0, erm.config.PoolSize/4),
		blobExtents:  make([]*ReuseExtent, 0, erm.config.PoolSize/4),
		maxSize:      erm.config.PoolSize,
		currentSize:  0,
	}

	erm.pools[spaceID] = pool
	return pool
}

// ReclaimExtent 回收Extent到复用池
func (erm *ExtentReuseManager) ReclaimExtent(ext *extent.BaseExtent, spaceID uint32, segType uint8) error {
	// 验证Extent是否完全空闲
	if !erm.isExtentFullyFree(ext) {
		atomic.AddUint64(&erm.stats.reclaimErrors, 1)
		return fmt.Errorf("extent %d is not fully free", ext.ExtentID)
	}

	// 延迟回收
	if erm.config.EnableDelayedReclaim {
		entry := &DelayedReclaimEntry{
			Extent:      ext,
			SpaceID:     spaceID,
			SegmentType: segType,
			ReclaimTime: time.Now().Add(DelayedReclaimSeconds * time.Second),
		}

		select {
		case erm.delayedReclaimQueue <- entry:
			return nil
		default:
			// 队列满，直接回收
		}
	}

	// 立即回收
	return erm.doReclaim(ext, spaceID, segType)
}

// doReclaim 执行回收
func (erm *ExtentReuseManager) doReclaim(ext *extent.BaseExtent, spaceID uint32, segType uint8) error {
	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime).Nanoseconds()
		// 更新平均回收时间（简化，实际应使用滑动窗口）
		atomic.StoreUint64(&erm.stats.avgReclaimTime, uint64(elapsed))
	}()

	pool := erm.GetOrCreatePool(spaceID)
	pool.mu.Lock()
	defer pool.mu.Unlock()

	// 检查池容量
	if pool.currentSize >= pool.maxSize {
		// 池已满，执行淘汰
		if err := erm.evictExtent(pool, segType); err != nil {
			return fmt.Errorf("failed to evict extent: %w", err)
		}
	}

	// 创建复用Extent
	reuseExt := &ReuseExtent{
		Extent:       ext,
		SpaceID:      spaceID,
		ExtentNo:     ext.ExtentID,
		SegmentType:  segType,
		ReclaimedAt:  time.Now(),
		ReuseCount:   0,
		LastAccessAt: time.Now(),
	}

	// 设置局部性信息
	if erm.config.EnableLocality {
		reuseExt.PrevExtentNo = ext.ExtentID - 1
		reuseExt.NextExtentNo = ext.ExtentID + 1
	}

	// 添加到对应的复用列表
	switch segType {
	case SEGMENT_TYPE_DATA:
		pool.dataExtents = append(pool.dataExtents, reuseExt)
	case SEGMENT_TYPE_INDEX:
		pool.indexExtents = append(pool.indexExtents, reuseExt)
	case SEGMENT_TYPE_UNDO:
		pool.undoExtents = append(pool.undoExtents, reuseExt)
	case SEGMENT_TYPE_BLOB:
		pool.blobExtents = append(pool.blobExtents, reuseExt)
	}

	pool.currentSize++
	atomic.AddUint64(&erm.stats.totalReclaimed, 1)

	return nil
}

// ReuseExtent 从复用池获取Extent
func (erm *ExtentReuseManager) ReuseExtent(spaceID uint32, segType uint8, preferExtentNo uint32) (*extent.BaseExtent, error) {
	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime).Nanoseconds()
		atomic.StoreUint64(&erm.stats.avgReuseTime, uint64(elapsed))
	}()

	pool := erm.GetOrCreatePool(spaceID)
	pool.mu.Lock()
	defer pool.mu.Unlock()

	// 根据策略选择Extent
	var reuseExt *ReuseExtent
	var idx int

	switch erm.config.Strategy {
	case ReuseStrategyFIFO:
		reuseExt, idx = erm.selectByFIFO(pool, segType)
	case ReuseStrategyLRU:
		reuseExt, idx = erm.selectByLRU(pool, segType)
	case ReuseStrategyLocality:
		reuseExt, idx = erm.selectByLocality(pool, segType, preferExtentNo)
	default:
		reuseExt, idx = erm.selectByFIFO(pool, segType)
	}

	if reuseExt == nil {
		atomic.AddUint64(&pool.missCount, 1)
		atomic.AddUint64(&erm.stats.reuseMisses, 1)
		return nil, fmt.Errorf("no available extent in reuse pool")
	}

	// 从列表中移除
	erm.removeFromList(pool, segType, idx)

	// 更新统计
	reuseExt.ReuseCount++
	reuseExt.LastAccessAt = time.Now()
	pool.currentSize--

	atomic.AddUint64(&pool.hitCount, 1)
	atomic.AddUint64(&erm.stats.reuseHits, 1)
	atomic.AddUint64(&erm.stats.totalReused, 1)

	return reuseExt.Extent, nil
}

// selectByFIFO FIFO策略选择
func (erm *ExtentReuseManager) selectByFIFO(pool *ExtentReusePool, segType uint8) (*ReuseExtent, int) {
	extents := erm.getExtentList(pool, segType)
	if len(extents) == 0 {
		return nil, -1
	}
	return extents[0], 0
}

// selectByLRU LRU策略选择
func (erm *ExtentReuseManager) selectByLRU(pool *ExtentReusePool, segType uint8) (*ReuseExtent, int) {
	extents := erm.getExtentList(pool, segType)
	if len(extents) == 0 {
		return nil, -1
	}

	// 查找最久未使用的
	lruIdx := 0
	lruTime := extents[0].LastAccessAt

	for i := 1; i < len(extents); i++ {
		if extents[i].LastAccessAt.Before(lruTime) {
			lruIdx = i
			lruTime = extents[i].LastAccessAt
		}
	}

	return extents[lruIdx], lruIdx
}

// selectByLocality 局部性策略选择
func (erm *ExtentReuseManager) selectByLocality(pool *ExtentReusePool, segType uint8, preferExtentNo uint32) (*ReuseExtent, int) {
	extents := erm.getExtentList(pool, segType)
	if len(extents) == 0 {
		return nil, -1
	}

	// 查找最接近preferExtentNo的Extent
	bestIdx := 0
	bestDistance := erm.distance(extents[0].ExtentNo, preferExtentNo)

	for i := 1; i < len(extents); i++ {
		dist := erm.distance(extents[i].ExtentNo, preferExtentNo)
		if dist < bestDistance {
			bestIdx = i
			bestDistance = dist
		}
	}

	return extents[bestIdx], bestIdx
}

// distance 计算Extent距离
func (erm *ExtentReuseManager) distance(extentNo1, extentNo2 uint32) uint32 {
	if extentNo1 > extentNo2 {
		return extentNo1 - extentNo2
	}
	return extentNo2 - extentNo1
}

// getExtentList 获取对应类型的Extent列表
func (erm *ExtentReuseManager) getExtentList(pool *ExtentReusePool, segType uint8) []*ReuseExtent {
	switch segType {
	case SEGMENT_TYPE_DATA:
		return pool.dataExtents
	case SEGMENT_TYPE_INDEX:
		return pool.indexExtents
	case SEGMENT_TYPE_UNDO:
		return pool.undoExtents
	case SEGMENT_TYPE_BLOB:
		return pool.blobExtents
	default:
		return nil
	}
}

// removeFromList 从列表中移除
func (erm *ExtentReuseManager) removeFromList(pool *ExtentReusePool, segType uint8, idx int) {
	switch segType {
	case SEGMENT_TYPE_DATA:
		pool.dataExtents = append(pool.dataExtents[:idx], pool.dataExtents[idx+1:]...)
	case SEGMENT_TYPE_INDEX:
		pool.indexExtents = append(pool.indexExtents[:idx], pool.indexExtents[idx+1:]...)
	case SEGMENT_TYPE_UNDO:
		pool.undoExtents = append(pool.undoExtents[:idx], pool.undoExtents[idx+1:]...)
	case SEGMENT_TYPE_BLOB:
		pool.blobExtents = append(pool.blobExtents[:idx], pool.blobExtents[idx+1:]...)
	}
}

// evictExtent 淘汰Extent
func (erm *ExtentReuseManager) evictExtent(pool *ExtentReusePool, segType uint8) error {
	// 使用LRU策略淘汰
	extents := erm.getExtentList(pool, segType)
	if len(extents) == 0 {
		return fmt.Errorf("no extent to evict")
	}

	// 查找最久未使用的
	lruIdx := 0
	lruTime := extents[0].LastAccessAt

	for i := 1; i < len(extents); i++ {
		if extents[i].LastAccessAt.Before(lruTime) {
			lruIdx = i
			lruTime = extents[i].LastAccessAt
		}
	}

	// 移除
	erm.removeFromList(pool, segType, lruIdx)
	pool.currentSize--

	return nil
}

// isExtentFullyFree 检查Extent是否完全空闲
func (erm *ExtentReuseManager) isExtentFullyFree(ext *extent.BaseExtent) bool {
	// 简化实现，实际需要检查所有页面
	return ext.UsedPages == 0
}

// delayedReclaimWorker 延迟回收工作协程
func (erm *ExtentReuseManager) delayedReclaimWorker() {
	for {
		select {
		case entry := <-erm.delayedReclaimQueue:
			// 等待到回收时间
			waitTime := time.Until(entry.ReclaimTime)
			if waitTime > 0 {
				time.Sleep(waitTime)
			}

			// 执行回收
			if err := erm.doReclaim(entry.Extent, entry.SpaceID, entry.SegmentType); err != nil {
				atomic.AddUint64(&erm.stats.reclaimErrors, 1)
			}

		case <-erm.stopChan:
			return
		}
	}
}

// monitorWorker 监控工作协程
func (erm *ExtentReuseManager) monitorWorker() {
	ticker := time.NewTicker(time.Duration(erm.config.MonitorInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			erm.updateStats()

		case <-erm.stopChan:
			return
		}
	}
}

// updateStats 更新统计信息
func (erm *ExtentReuseManager) updateStats() {
	erm.mu.RLock()
	defer erm.mu.RUnlock()

	totalCapacity := 0
	totalUsed := 0

	for _, pool := range erm.pools {
		pool.mu.RLock()
		totalCapacity += pool.maxSize
		totalUsed += pool.currentSize
		pool.mu.RUnlock()
	}

	// 计算利用率
	if totalCapacity > 0 {
		erm.stats.poolUtilization = float64(totalUsed) / float64(totalCapacity) * 100
	}

	// 计算复用率
	total := atomic.LoadUint64(&erm.stats.totalAllocated)
	reused := atomic.LoadUint64(&erm.stats.totalReused)
	if total > 0 {
		erm.stats.reuseRate = float64(reused) / float64(total) * 100
	}
}

// GetStats 获取统计信息
func (erm *ExtentReuseManager) GetStats() *ExtentReuseStats {
	stats := &ExtentReuseStats{}

	stats.totalReused = atomic.LoadUint64(&erm.stats.totalReused)
	stats.totalReclaimed = atomic.LoadUint64(&erm.stats.totalReclaimed)
	stats.totalAllocated = atomic.LoadUint64(&erm.stats.totalAllocated)
	stats.reuseHits = atomic.LoadUint64(&erm.stats.reuseHits)
	stats.reuseMisses = atomic.LoadUint64(&erm.stats.reuseMisses)
	stats.avgReuseTime = atomic.LoadUint64(&erm.stats.avgReuseTime)
	stats.avgReclaimTime = atomic.LoadUint64(&erm.stats.avgReclaimTime)
	stats.reclaimErrors = atomic.LoadUint64(&erm.stats.reclaimErrors)
	stats.reuseErrors = atomic.LoadUint64(&erm.stats.reuseErrors)
	stats.poolUtilization = erm.stats.poolUtilization
	stats.reuseRate = erm.stats.reuseRate

	return stats
}

// GetReuseRate 获取复用率
func (erm *ExtentReuseManager) GetReuseRate() float64 {
	return erm.stats.reuseRate
}

// GetPoolUtilization 获取池利用率
func (erm *ExtentReuseManager) GetPoolUtilization() float64 {
	return erm.stats.poolUtilization
}

// Stop 停止管理器
func (erm *ExtentReuseManager) Stop() {
	close(erm.stopChan)
}
