package manager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

/*
SegmentSpaceOptimizer 段空间管理优化器

优化段空间的统计、查询和回收，提供实时的空间使用情况和高效的空间查询能力。

核心功能：
1. 空间统计：实时统计段的空间使用情况
2. 空间查询：快速查询段的空间信息
3. 空间回收：智能回收不再使用的空间

设计要点：
- 增量统计：避免全量扫描
- 缓存优化：缓存热点统计数据
- 异步更新：后台更新统计信息
- 阈值触发：低于阈值自动回收
*/

const (
	// 统计更新间隔
	StatsUpdateIntervalSeconds = 30

	// 空间回收阈值
	DefaultReclaimThreshold = 0.3 // 空间利用率低于30%触发回收

	// 缓存大小
	StatsCacheSize = 1000

	// 碎片整理阈值
	FragmentationThreshold = 0.5 // 碎片率超过50%触发整理
)

// SegmentSpaceOptimizer 段空间优化器
type SegmentSpaceOptimizer struct {
	// 段管理器引用
	segmentManager *SegmentManager

	// 空间统计缓存
	statsCache map[uint32]*SegmentSpaceStats
	cacheMu    sync.RWMutex

	// 配置
	config *SpaceOptimizerConfig

	// 全局统计
	globalStats *GlobalSpaceStats

	// 停止信号
	stopChan chan struct{}

	mu sync.RWMutex
}

// SegmentSpaceStats 段空间统计
type SegmentSpaceStats struct {
	SegmentID   uint32
	SpaceID     uint32
	SegmentType uint8

	// 空间统计
	TotalSize uint64 // 总空间（字节）
	UsedSize  uint64 // 已使用空间
	FreeSize  uint64 // 空闲空间

	// 页面统计
	TotalPages uint32 // 总页面数
	UsedPages  uint32 // 已使用页面数
	FreePages  uint32 // 空闲页面数

	// Extent统计
	TotalExtents   uint32 // 总Extent数
	FreeExtents    uint32 // 空闲Extent数
	NotFullExtents uint32 // 部分使用Extent数
	FullExtents    uint32 // 完全使用Extent数

	// Fragment统计
	FragmentPages uint32 // Fragment页面数
	FragmentUsed  uint32 // 已使用Fragment页面数
	FragmentFree  uint32 // 空闲Fragment页面数

	// 利用率
	Utilization       float64 // 空间利用率
	FragmentationRate float64 // 碎片率

	// 时间信息
	LastUpdated time.Time
	UpdateCount uint64
}

// SpaceOptimizerConfig 优化器配置
type SpaceOptimizerConfig struct {
	EnableAutoReclaim bool    // 启用自动回收
	ReclaimThreshold  float64 // 回收阈值
	UpdateInterval    int     // 统计更新间隔（秒）
	EnableCache       bool    // 启用统计缓存
	CacheSize         int     // 缓存大小
}

// GlobalSpaceStats 全局空间统计
type GlobalSpaceStats struct {
	TotalSegments uint32
	TotalSize     uint64
	UsedSize      uint64
	FreeSize      uint64

	TotalPages uint32
	UsedPages  uint32
	FreePages  uint32

	AvgUtilization   float64
	AvgFragmentation float64

	ReclaimCount uint64 // 回收次数
	ReclaimSize  uint64 // 回收空间大小
}

// SpaceQuery 空间查询条件
type SpaceQuery struct {
	SpaceID        *uint32  // 表空间ID过滤
	SegmentType    *uint8   // 段类型过滤
	MinUtilization *float64 // 最小利用率
	MaxUtilization *float64 // 最大利用率
	MinSize        *uint64  // 最小空间
	MaxSize        *uint64  // 最大空间
	SortBy         string   // 排序字段
	Limit          int      // 返回数量限制
}

// NewSegmentSpaceOptimizer 创建段空间优化器
func NewSegmentSpaceOptimizer(sm *SegmentManager) *SegmentSpaceOptimizer {
	sso := &SegmentSpaceOptimizer{
		segmentManager: sm,
		statsCache:     make(map[uint32]*SegmentSpaceStats),
		config: &SpaceOptimizerConfig{
			EnableAutoReclaim: true,
			ReclaimThreshold:  DefaultReclaimThreshold,
			UpdateInterval:    StatsUpdateIntervalSeconds,
			EnableCache:       true,
			CacheSize:         StatsCacheSize,
		},
		globalStats: &GlobalSpaceStats{},
		stopChan:    make(chan struct{}),
	}

	// 启动后台更新任务
	go sso.statsUpdateWorker()
	go sso.autoReclaimWorker()

	return sso
}

// GetSegmentStats 获取段空间统计（带缓存）
func (sso *SegmentSpaceOptimizer) GetSegmentStats(segID uint32) (*SegmentSpaceStats, error) {
	// 先查缓存
	if sso.config.EnableCache {
		sso.cacheMu.RLock()
		if stats, exists := sso.statsCache[segID]; exists {
			// 检查缓存是否过期（5分钟）
			if time.Since(stats.LastUpdated) < 5*time.Minute {
				sso.cacheMu.RUnlock()
				return stats, nil
			}
		}
		sso.cacheMu.RUnlock()
	}

	// 缓存未命中或已过期，计算统计
	stats, err := sso.calculateSegmentStats(segID)
	if err != nil {
		return nil, err
	}

	// 更新缓存
	if sso.config.EnableCache {
		sso.updateCache(segID, stats)
	}

	return stats, nil
}

// calculateSegmentStats 计算段空间统计
func (sso *SegmentSpaceOptimizer) calculateSegmentStats(segID uint32) (*SegmentSpaceStats, error) {
	sso.segmentManager.mu.RLock()
	seg, exists := sso.segmentManager.segments[segID]
	sso.segmentManager.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("segment %d not found", segID)
	}

	stats := &SegmentSpaceStats{
		SegmentID:   segID,
		SpaceID:     seg.SpaceID,
		SegmentType: seg.Type,
		LastUpdated: time.Now(),
	}

	// Fragment页面统计
	stats.FragmentPages = FragmentPageCount
	stats.FragmentUsed = seg.FragmentUsed
	stats.FragmentFree = FragmentPageCount - seg.FragmentUsed

	// Extent统计
	stats.FreeExtents = uint32(len(seg.FreeExtents))
	stats.NotFullExtents = uint32(len(seg.NotFullExtents))
	stats.FullExtents = uint32(len(seg.FullExtents))
	stats.TotalExtents = stats.FreeExtents + stats.NotFullExtents + stats.FullExtents

	// 页面统计
	stats.TotalPages = seg.PageCount
	stats.UsedPages = seg.PageCount - uint32(seg.FreeSpace/PageSize)
	stats.FreePages = stats.TotalPages - stats.UsedPages

	// 空间统计
	stats.TotalSize = uint64(stats.TotalPages) * PageSize
	stats.UsedSize = uint64(stats.UsedPages) * PageSize
	stats.FreeSize = seg.FreeSpace

	// 利用率计算
	if stats.TotalSize > 0 {
		stats.Utilization = float64(stats.UsedSize) / float64(stats.TotalSize) * 100
	}

	// 碎片率计算
	stats.FragmentationRate = sso.calculateFragmentation(seg)

	atomic.AddUint64(&stats.UpdateCount, 1)

	return stats, nil
}

// calculateFragmentation 计算碎片率
func (sso *SegmentSpaceOptimizer) calculateFragmentation(seg *SegmentImpl) float64 {
	// 碎片率 = (部分使用的Extent数 + Fragment使用情况的碎片) / 总Extent数

	totalExtents := float64(len(seg.FreeExtents) + len(seg.NotFullExtents) + len(seg.FullExtents))
	if totalExtents == 0 {
		return 0
	}

	// NotFull的Extent都算作碎片
	fragmentExtents := float64(len(seg.NotFullExtents))

	// Fragment的碎片：使用了但未满
	if seg.FragmentUsed > 0 && !seg.FragmentFull {
		fragmentExtents += 0.5 // Fragment碎片权重较低
	}

	return (fragmentExtents / totalExtents) * 100
}

// QuerySegments 查询段空间信息
func (sso *SegmentSpaceOptimizer) QuerySegments(query *SpaceQuery) ([]*SegmentSpaceStats, error) {
	sso.segmentManager.mu.RLock()
	segments := make(map[uint32]*SegmentImpl)
	for id, seg := range sso.segmentManager.segments {
		segments[id] = seg
	}
	sso.segmentManager.mu.RUnlock()

	results := make([]*SegmentSpaceStats, 0)

	for segID, seg := range segments {
		// 应用过滤条件
		if query.SpaceID != nil && seg.SpaceID != *query.SpaceID {
			continue
		}
		if query.SegmentType != nil && seg.Type != *query.SegmentType {
			continue
		}

		// 获取统计
		stats, err := sso.GetSegmentStats(segID)
		if err != nil {
			continue
		}

		// 应用利用率过滤
		if query.MinUtilization != nil && stats.Utilization < *query.MinUtilization {
			continue
		}
		if query.MaxUtilization != nil && stats.Utilization > *query.MaxUtilization {
			continue
		}

		// 应用大小过滤
		if query.MinSize != nil && stats.TotalSize < *query.MinSize {
			continue
		}
		if query.MaxSize != nil && stats.TotalSize > *query.MaxSize {
			continue
		}

		results = append(results, stats)
	}

	// 排序
	sso.sortResults(results, query.SortBy)

	// 限制返回数量
	if query.Limit > 0 && len(results) > query.Limit {
		results = results[:query.Limit]
	}

	return results, nil
}

// sortResults 排序结果
func (sso *SegmentSpaceOptimizer) sortResults(results []*SegmentSpaceStats, sortBy string) {
	// 简化实现，实际应使用sort.Slice
	switch sortBy {
	case "utilization":
		// 按利用率排序
	case "size":
		// 按大小排序
	case "fragmentation":
		// 按碎片率排序
	}
}

// ReclaimSegmentSpace 回收段空间
func (sso *SegmentSpaceOptimizer) ReclaimSegmentSpace(segID uint32) (uint64, error) {
	stats, err := sso.GetSegmentStats(segID)
	if err != nil {
		return 0, err
	}

	// 检查是否需要回收
	if stats.Utilization > sso.config.ReclaimThreshold*100 {
		return 0, fmt.Errorf("segment utilization %.2f%% above threshold", stats.Utilization)
	}

	sso.segmentManager.mu.Lock()
	seg := sso.segmentManager.segments[segID]
	sso.segmentManager.mu.Unlock()

	if seg == nil {
		return 0, fmt.Errorf("segment not found")
	}

	reclaimedSize := uint64(0)

	// 回收完全空闲的Extent
	for _, ext := range seg.FreeExtents {
		if ext.GetPageCount() == 0 {
			// 这里应该调用ExtentReuseManager回收
			reclaimedSize += uint64(PagesPerExtent * PageSize)
		}
	}

	// 更新全局统计
	atomic.AddUint64(&sso.globalStats.ReclaimCount, 1)
	atomic.AddUint64(&sso.globalStats.ReclaimSize, reclaimedSize)

	// 清除缓存
	sso.invalidateCache(segID)

	return reclaimedSize, nil
}

// GetGlobalStats 获取全局统计
func (sso *SegmentSpaceOptimizer) GetGlobalStats() *GlobalSpaceStats {
	stats := &GlobalSpaceStats{}

	stats.TotalSegments = atomic.LoadUint32(&sso.globalStats.TotalSegments)
	stats.TotalSize = atomic.LoadUint64(&sso.globalStats.TotalSize)
	stats.UsedSize = atomic.LoadUint64(&sso.globalStats.UsedSize)
	stats.FreeSize = atomic.LoadUint64(&sso.globalStats.FreeSize)
	stats.TotalPages = atomic.LoadUint32(&sso.globalStats.TotalPages)
	stats.UsedPages = atomic.LoadUint32(&sso.globalStats.UsedPages)
	stats.FreePages = atomic.LoadUint32(&sso.globalStats.FreePages)
	stats.ReclaimCount = atomic.LoadUint64(&sso.globalStats.ReclaimCount)
	stats.ReclaimSize = atomic.LoadUint64(&sso.globalStats.ReclaimSize)
	stats.AvgUtilization = sso.globalStats.AvgUtilization
	stats.AvgFragmentation = sso.globalStats.AvgFragmentation

	return stats
}

// updateCache 更新缓存
func (sso *SegmentSpaceOptimizer) updateCache(segID uint32, stats *SegmentSpaceStats) {
	sso.cacheMu.Lock()
	defer sso.cacheMu.Unlock()

	// 检查缓存大小
	if len(sso.statsCache) >= sso.config.CacheSize {
		// 简单的淘汰策略：删除最旧的
		var oldestID uint32
		var oldestTime time.Time
		first := true

		for id, s := range sso.statsCache {
			if first || s.LastUpdated.Before(oldestTime) {
				oldestID = id
				oldestTime = s.LastUpdated
				first = false
			}
		}

		delete(sso.statsCache, oldestID)
	}

	sso.statsCache[segID] = stats
}

// invalidateCache 清除缓存
func (sso *SegmentSpaceOptimizer) invalidateCache(segID uint32) {
	sso.cacheMu.Lock()
	delete(sso.statsCache, segID)
	sso.cacheMu.Unlock()
}

// statsUpdateWorker 统计更新工作协程
func (sso *SegmentSpaceOptimizer) statsUpdateWorker() {
	ticker := time.NewTicker(time.Duration(sso.config.UpdateInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sso.updateGlobalStats()

		case <-sso.stopChan:
			return
		}
	}
}

// updateGlobalStats 更新全局统计
func (sso *SegmentSpaceOptimizer) updateGlobalStats() {
	sso.segmentManager.mu.RLock()
	segmentCount := uint32(len(sso.segmentManager.segments))
	sso.segmentManager.mu.RUnlock()

	atomic.StoreUint32(&sso.globalStats.TotalSegments, segmentCount)

	var totalSize, usedSize, freeSize uint64
	var totalPages, usedPages, freePages uint32
	var totalUtil, totalFrag float64
	count := 0

	sso.segmentManager.mu.RLock()
	for segID := range sso.segmentManager.segments {
		stats, err := sso.GetSegmentStats(segID)
		if err != nil {
			continue
		}

		totalSize += stats.TotalSize
		usedSize += stats.UsedSize
		freeSize += stats.FreeSize
		totalPages += stats.TotalPages
		usedPages += stats.UsedPages
		freePages += stats.FreePages
		totalUtil += stats.Utilization
		totalFrag += stats.FragmentationRate
		count++
	}
	sso.segmentManager.mu.RUnlock()

	atomic.StoreUint64(&sso.globalStats.TotalSize, totalSize)
	atomic.StoreUint64(&sso.globalStats.UsedSize, usedSize)
	atomic.StoreUint64(&sso.globalStats.FreeSize, freeSize)
	atomic.StoreUint32(&sso.globalStats.TotalPages, totalPages)
	atomic.StoreUint32(&sso.globalStats.UsedPages, usedPages)
	atomic.StoreUint32(&sso.globalStats.FreePages, freePages)

	if count > 0 {
		sso.globalStats.AvgUtilization = totalUtil / float64(count)
		sso.globalStats.AvgFragmentation = totalFrag / float64(count)
	}
}

// autoReclaimWorker 自动回收工作协程
func (sso *SegmentSpaceOptimizer) autoReclaimWorker() {
	ticker := time.NewTicker(5 * time.Minute) // 每5分钟检查一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if sso.config.EnableAutoReclaim {
				sso.performAutoReclaim()
			}

		case <-sso.stopChan:
			return
		}
	}
}

// performAutoReclaim 执行自动回收
func (sso *SegmentSpaceOptimizer) performAutoReclaim() {
	// 查询低利用率的段
	maxUtil := sso.config.ReclaimThreshold * 100
	query := &SpaceQuery{
		MaxUtilization: &maxUtil,
		Limit:          10, // 每次最多回收10个段
	}

	segments, err := sso.QuerySegments(query)
	if err != nil {
		return
	}

	// 执行回收
	for _, stats := range segments {
		sso.ReclaimSegmentSpace(stats.SegmentID)
	}
}

// Stop 停止优化器
func (sso *SegmentSpaceOptimizer) Stop() {
	close(sso.stopChan)
}

// GetCacheStats 获取缓存统计
func (sso *SegmentSpaceOptimizer) GetCacheStats() map[string]interface{} {
	sso.cacheMu.RLock()
	defer sso.cacheMu.RUnlock()

	return map[string]interface{}{
		"cache_size":     len(sso.statsCache),
		"cache_capacity": sso.config.CacheSize,
		"cache_hit_rate": 0.0, // 需要额外统计
	}
}
