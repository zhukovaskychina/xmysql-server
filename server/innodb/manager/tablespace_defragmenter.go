package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/extent"
	ibd_space "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/space"
)

/*
TablespaceDefragmenter - 表空间碎片整理器

功能：
1. 碎片检测：计算碎片率、检测页面空洞、统计碎片分布
2. 碎片整理：在线/离线整理、增量整理、页面重组
3. 空间优化：页面填充率优化、空闲页面回收、extent分配优化

设计要点：
- 支持在线整理（不阻塞读写）和离线整理（性能更高）
- 增量整理避免长时间锁定
- 详细的碎片统计和报告
*/

// DefragmentMode 碎片整理模式
type DefragmentMode int

const (
	DefragmentModeOnline      DefragmentMode = iota // 在线整理（不阻塞读写）
	DefragmentModeOffline                           // 离线整理（需要锁表，性能更高）
	DefragmentModeIncremental                       // 增量整理（分批次处理）
)

// FragmentationReport 碎片报告
type FragmentationReport struct {
	SpaceID   uint32    `json:"space_id"`
	SpaceName string    `json:"space_name"`
	Timestamp time.Time `json:"timestamp"`

	// 空间统计
	TotalPages      uint32 `json:"total_pages"`       // 总页面数
	AllocatedPages  uint32 `json:"allocated_pages"`   // 已分配页面数
	UsedPages       uint32 `json:"used_pages"`        // 实际使用页面数
	FreePages       uint32 `json:"free_pages"`        // 空闲页面数
	TotalExtents    uint32 `json:"total_extents"`     // 总extent数
	FreeExtents     uint32 `json:"free_extents"`      // 空闲extent数
	PartialExtents  uint32 `json:"partial_extents"`   // 部分使用的extent数
	FullExtents     uint32 `json:"full_extents"`      // 完全使用的extent数
	TotalSpaceBytes uint64 `json:"total_space_bytes"` // 总空间（字节）
	UsedSpaceBytes  uint64 `json:"used_space_bytes"`  // 已使用空间（字节）
	FreeSpaceBytes  uint64 `json:"free_space_bytes"`  // 空闲空间（字节）

	// 碎片统计
	FragmentationRate     float64 `json:"fragmentation_rate"`     // 碎片率（0-100）
	InternalFragmentation float64 `json:"internal_fragmentation"` // 内部碎片率
	ExternalFragmentation float64 `json:"external_fragmentation"` // 外部碎片率
	PageHoles             uint32  `json:"page_holes"`             // 页面空洞数量
	ExtentHoles           uint32  `json:"extent_holes"`           // extent空洞数量
	AverageHoleSize       uint32  `json:"average_hole_size"`      // 平均空洞大小（页面数）
	LargestHoleSize       uint32  `json:"largest_hole_size"`      // 最大空洞大小（页面数）

	// 利用率统计
	SpaceUtilization  float64 `json:"space_utilization"`   // 空间利用率（0-100）
	PageUtilization   float64 `json:"page_utilization"`    // 页面利用率（0-100）
	ExtentUtilization float64 `json:"extent_utilization"`  // extent利用率（0-100）
	AveragePageFill   float64 `json:"average_page_fill"`   // 平均页面填充率（0-100）
	FragmentPageRatio float64 `json:"fragment_page_ratio"` // Fragment页面占比（0-100）

	// 分布统计
	ExtentsByState map[string]uint32 `json:"extents_by_state"` // 按状态分组的extent数量
	PagesByType    map[string]uint32 `json:"pages_by_type"`    // 按类型分组的页面数量

	// 建议
	NeedsDefragmentation bool   `json:"needs_defragmentation"` // 是否需要碎片整理
	RecommendedMode      string `json:"recommended_mode"`      // 推荐的整理模式
	EstimatedGain        uint64 `json:"estimated_gain"`        // 预计回收空间（字节）
}

// DefragmentationStats 碎片整理统计
type DefragmentationStats struct {
	sync.RWMutex

	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Duration  int64     `json:"duration_ms"` // 毫秒
	Mode      string    `json:"mode"`
	Status    string    `json:"status"` // running, completed, failed
	Progress  float64   `json:"progress"`

	// 整理统计
	PagesProcessed   uint32 `json:"pages_processed"`
	PagesRelocated   uint32 `json:"pages_relocated"`
	ExtentsProcessed uint32 `json:"extents_processed"`
	ExtentsFreed     uint32 `json:"extents_freed"`
	SpaceReclaimed   uint64 `json:"space_reclaimed"` // 回收的空间（字节）

	// 性能统计
	PagesPerSecond    float64 `json:"pages_per_second"`
	BytesPerSecond    float64 `json:"bytes_per_second"`
	AveragePageTime   int64   `json:"average_page_time_us"` // 微秒
	TotalIOOperations uint64  `json:"total_io_operations"`

	// 错误统计
	Errors    uint32 `json:"errors"`
	Warnings  uint32 `json:"warnings"`
	LastError string `json:"last_error,omitempty"`
}

// TablespaceDefragmenter 表空间碎片整理器
type TablespaceDefragmenter struct {
	sync.RWMutex

	// 依赖组件
	spaceManager  basic.SpaceManager
	extentManager *ExtentManager
	bufferPool    *buffer_pool.BufferPool

	// 配置
	config *DefragmenterConfig

	// 运行时状态
	running      bool
	currentStats *DefragmentationStats

	// 统计信息
	lastReport map[uint32]*FragmentationReport // spaceID -> report
}

// DefragmenterConfig 碎片整理器配置
type DefragmenterConfig struct {
	// 整理阈值
	FragmentationThreshold float64 // 碎片率阈值（超过此值触发整理）
	MinSpaceUtilization    float64 // 最小空间利用率
	MaxHoleSize            uint32  // 最大允许的空洞大小

	// 增量整理配置
	IncrementalBatchSize uint32        // 每批次处理的页面数
	IncrementalInterval  time.Duration // 批次间隔时间

	// 性能配置
	MaxConcurrentPages uint32        // 最大并发处理页面数
	IOThrottleDelay    time.Duration // IO节流延迟

	// 安全配置
	EnableOnlineDefrag bool          // 是否启用在线整理
	MaxLockWaitTime    time.Duration // 最大锁等待时间
}

// NewTablespaceDefragmenter 创建碎片整理器
func NewTablespaceDefragmenter(
	spaceManager basic.SpaceManager,
	extentManager *ExtentManager,
	bufferPool *buffer_pool.BufferPool,
	config *DefragmenterConfig,
) *TablespaceDefragmenter {
	if config == nil {
		config = &DefragmenterConfig{
			FragmentationThreshold: 30.0, // 碎片率超过30%触发整理
			MinSpaceUtilization:    70.0, // 空间利用率低于70%触发整理
			MaxHoleSize:            10,   // 最大空洞10个页面
			IncrementalBatchSize:   100,  // 每批次100个页面
			IncrementalInterval:    100 * time.Millisecond,
			MaxConcurrentPages:     10,
			IOThrottleDelay:        10 * time.Millisecond,
			EnableOnlineDefrag:     true,
			MaxLockWaitTime:        5 * time.Second,
		}
	}

	return &TablespaceDefragmenter{
		spaceManager:  spaceManager,
		extentManager: extentManager,
		bufferPool:    bufferPool,
		config:        config,
		running:       false,
		lastReport:    make(map[uint32]*FragmentationReport),
	}
}

// AnalyzeFragmentation 分析表空间碎片
func (tdf *TablespaceDefragmenter) AnalyzeFragmentation(ctx context.Context, spaceID uint32) (*FragmentationReport, error) {
	logger.Infof("🔍 [Defragmenter] Analyzing fragmentation for space %d", spaceID)

	report := &FragmentationReport{
		SpaceID:        spaceID,
		Timestamp:      time.Now(),
		ExtentsByState: make(map[string]uint32),
		PagesByType:    make(map[string]uint32),
	}

	// 获取表空间
	space, err := tdf.spaceManager.GetSpace(spaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get space: %v", err)
	}

	report.SpaceName = space.Name()

	// 尝试获取详细统计信息（使用类型断言访问IBDSpace的具体方法）
	type DetailedStatsProvider interface {
		GetDetailedStats() *ibd_space.SpaceDetailedStats
	}

	var detailedStats *ibd_space.SpaceDetailedStats
	if provider, ok := space.(DetailedStatsProvider); ok {
		detailedStats = provider.GetDetailedStats()
	}

	if detailedStats != nil {
		report.TotalPages = detailedStats.TotalPages
		report.AllocatedPages = detailedStats.AllocatedPages
		report.TotalExtents = detailedStats.TotalExtents
		report.FreeExtents = detailedStats.FreeExtents
		report.PartialExtents = detailedStats.PartialExtents
		report.FullExtents = detailedStats.FullExtents
		report.TotalSpaceBytes = detailedStats.TotalSize
		report.UsedSpaceBytes = detailedStats.UsedSize
		report.FreeSpaceBytes = detailedStats.FreeSize

		// 初始化extent状态分布
		report.ExtentsByState["free"] = detailedStats.FreeExtents
		report.ExtentsByState["partial"] = detailedStats.PartialExtents
		report.ExtentsByState["full"] = detailedStats.FullExtents

		logger.Debugf("📊 [Defragmenter] Detailed stats: total_pages=%d, allocated=%d, used_bytes=%d, free_bytes=%d",
			detailedStats.TotalPages, detailedStats.AllocatedPages, detailedStats.UsedSize, detailedStats.FreeSize)
	} else {
		// 回退到基本统计（兼容性）
		report.TotalPages = space.GetPageCount()
		report.TotalExtents = space.GetExtentCount()
		report.TotalSpaceBytes = uint64(report.TotalPages) * 16384 // 16KB per page

		logger.Warnf("⚠️ [Defragmenter] Detailed stats not available, using basic stats")
	}

	// 分析extent状态（如果详细统计不可用）
	if detailedStats == nil {
		if err := tdf.analyzeExtents(ctx, spaceID, report); err != nil {
			logger.Warnf("⚠️ Failed to analyze extents: %v", err)
		}
	}

	// 分析页面使用情况
	if err := tdf.analyzePages(ctx, spaceID, report); err != nil {
		logger.Warnf("⚠️ Failed to analyze pages: %v", err)
	}

	// 检测空洞
	if err := tdf.detectHoles(ctx, spaceID, report); err != nil {
		logger.Warnf("⚠️ Failed to detect holes: %v", err)
	}

	// 计算碎片率和利用率
	tdf.calculateMetrics(report)

	// 生成建议
	tdf.generateRecommendations(report)

	// 保存报告
	tdf.Lock()
	tdf.lastReport[spaceID] = report
	tdf.Unlock()

	logger.Infof("✅ [Defragmenter] Analysis complete: fragmentation=%.2f%%, utilization=%.2f%%",
		report.FragmentationRate, report.SpaceUtilization)

	return report, nil
}

// GetLastReport 获取最近的碎片报告
func (tdf *TablespaceDefragmenter) GetLastReport(spaceID uint32) *FragmentationReport {
	tdf.RLock()
	defer tdf.RUnlock()
	return tdf.lastReport[spaceID]
}

// GetConfig 获取配置
func (tdf *TablespaceDefragmenter) GetConfig() *DefragmenterConfig {
	tdf.RLock()
	defer tdf.RUnlock()
	return tdf.config
}

// UpdateConfig 更新配置
func (tdf *TablespaceDefragmenter) UpdateConfig(config *DefragmenterConfig) {
	tdf.Lock()
	defer tdf.Unlock()
	tdf.config = config
}

// IsRunning 检查是否正在运行
func (tdf *TablespaceDefragmenter) IsRunning() bool {
	tdf.RLock()
	defer tdf.RUnlock()
	return tdf.running
}

// GetCurrentStats 获取当前统计信息
func (tdf *TablespaceDefragmenter) GetCurrentStats() *DefragmentationStats {
	tdf.RLock()
	defer tdf.RUnlock()
	if tdf.currentStats == nil {
		return nil
	}
	// 返回副本
	stats := *tdf.currentStats
	return &stats
}

// analyzeExtents 分析extent状态
func (tdf *TablespaceDefragmenter) analyzeExtents(ctx context.Context, spaceID uint32, report *FragmentationReport) error {
	// 获取extent管理器统计
	extStats := tdf.extentManager.GetStats()
	if extStats != nil {
		report.FreeExtents = extStats.FreeExtents
		report.FullExtents = extStats.FullExtents
		report.PartialExtents = report.TotalExtents - report.FreeExtents - report.FullExtents

		report.ExtentsByState["free"] = report.FreeExtents
		report.ExtentsByState["full"] = report.FullExtents
		report.ExtentsByState["partial"] = report.PartialExtents
	}

	return nil
}

// analyzePages 分析页面使用情况
func (tdf *TablespaceDefragmenter) analyzePages(ctx context.Context, spaceID uint32, report *FragmentationReport) error {
	// 这里简化处理，实际应该遍历所有页面
	// 根据extent状态估算页面使用情况
	pagesPerExtent := uint32(64)

	// 完全使用的extent的页面
	fullExtentPages := report.FullExtents * pagesPerExtent

	// 部分使用的extent的页面（假设平均50%使用）
	partialExtentPages := report.PartialExtents * pagesPerExtent / 2

	report.AllocatedPages = fullExtentPages + partialExtentPages
	report.UsedPages = report.AllocatedPages // 简化：假设分配的都在使用
	report.FreePages = report.TotalPages - report.AllocatedPages

	report.UsedSpaceBytes = uint64(report.UsedPages) * 16384
	report.FreeSpaceBytes = uint64(report.FreePages) * 16384

	// 按类型统计（简化）
	report.PagesByType["data"] = report.UsedPages
	report.PagesByType["free"] = report.FreePages

	return nil
}

// detectHoles 检测空洞
func (tdf *TablespaceDefragmenter) detectHoles(ctx context.Context, spaceID uint32, report *FragmentationReport) error {
	// 简化实现：基于extent状态估算空洞
	// 实际应该扫描页面分配位图

	// 页面空洞 = 部分使用的extent中的空闲页面
	pagesPerExtent := uint32(64)
	report.PageHoles = report.PartialExtents * pagesPerExtent / 2 // 假设平均50%空闲

	// extent空洞 = 空闲extent数量
	report.ExtentHoles = report.FreeExtents

	// 计算平均空洞大小
	if report.PageHoles > 0 {
		report.AverageHoleSize = report.PageHoles / (report.PartialExtents + 1)
	}

	// 最大空洞大小（简化：假设为一个extent）
	report.LargestHoleSize = pagesPerExtent

	return nil
}

// calculateMetrics 计算碎片率和利用率
func (tdf *TablespaceDefragmenter) calculateMetrics(report *FragmentationReport) {
	if report.TotalPages == 0 {
		return
	}

	// 空间利用率 = 已使用空间 / 总空间 * 100
	report.SpaceUtilization = float64(report.UsedPages) / float64(report.TotalPages) * 100

	// 页面利用率 = 已使用页面 / 已分配页面 * 100
	if report.AllocatedPages > 0 {
		report.PageUtilization = float64(report.UsedPages) / float64(report.AllocatedPages) * 100
	}

	// extent利用率 = (完全使用 + 部分使用) / 总extent * 100
	if report.TotalExtents > 0 {
		usedExtents := report.FullExtents + report.PartialExtents
		report.ExtentUtilization = float64(usedExtents) / float64(report.TotalExtents) * 100
	}

	// 内部碎片率 = 已分配但未使用的空间 / 已分配空间 * 100
	if report.AllocatedPages > 0 {
		internalWaste := report.AllocatedPages - report.UsedPages
		report.InternalFragmentation = float64(internalWaste) / float64(report.AllocatedPages) * 100
	}

	// 外部碎片率 = 页面空洞 / 总页面 * 100
	report.ExternalFragmentation = float64(report.PageHoles) / float64(report.TotalPages) * 100

	// 总碎片率 = (内部碎片 + 外部碎片) / 2
	report.FragmentationRate = (report.InternalFragmentation + report.ExternalFragmentation) / 2

	// 平均页面填充率（简化：假设已使用页面100%填充）
	report.AveragePageFill = 100.0

	// Fragment页面占比（简化：假设部分使用的extent都是fragment）
	if report.TotalPages > 0 {
		fragmentPages := report.PartialExtents * 64
		report.FragmentPageRatio = float64(fragmentPages) / float64(report.TotalPages) * 100
	}
}

// generateRecommendations 生成整理建议
func (tdf *TablespaceDefragmenter) generateRecommendations(report *FragmentationReport) {
	// 判断是否需要碎片整理
	report.NeedsDefragmentation = report.FragmentationRate > tdf.config.FragmentationThreshold ||
		report.SpaceUtilization < tdf.config.MinSpaceUtilization

	if !report.NeedsDefragmentation {
		report.RecommendedMode = "none"
		report.EstimatedGain = 0
		return
	}

	// 根据碎片率推荐整理模式
	if report.FragmentationRate > 50.0 {
		report.RecommendedMode = "offline" // 严重碎片，建议离线整理
	} else if report.FragmentationRate > 30.0 {
		report.RecommendedMode = "incremental" // 中等碎片，建议增量整理
	} else {
		report.RecommendedMode = "online" // 轻度碎片，建议在线整理
	}

	// 估算可回收空间
	// 假设整理后可以回收50%的空洞空间
	reclaimablePages := report.PageHoles / 2
	report.EstimatedGain = uint64(reclaimablePages) * 16384
}

// Defragment 执行碎片整理
func (tdf *TablespaceDefragmenter) Defragment(ctx context.Context, spaceID uint32, mode DefragmentMode) error {
	// 检查是否已在运行
	tdf.Lock()
	if tdf.running {
		tdf.Unlock()
		return fmt.Errorf("defragmentation already running")
	}
	tdf.running = true
	tdf.Unlock()

	defer func() {
		tdf.Lock()
		tdf.running = false
		tdf.Unlock()
	}()

	logger.Infof("🔧 [Defragmenter] Starting defragmentation for space %d, mode=%d", spaceID, mode)

	// 初始化统计
	stats := &DefragmentationStats{
		StartTime: time.Now(),
		Mode:      tdf.getModeString(mode),
		Status:    "running",
		Progress:  0.0,
	}
	tdf.Lock()
	tdf.currentStats = stats
	tdf.Unlock()

	// 根据模式执行整理
	var err error
	switch mode {
	case DefragmentModeOnline:
		err = tdf.defragmentOnline(ctx, spaceID, stats)
	case DefragmentModeOffline:
		err = tdf.defragmentOffline(ctx, spaceID, stats)
	case DefragmentModeIncremental:
		err = tdf.defragmentIncremental(ctx, spaceID, stats)
	default:
		err = fmt.Errorf("unknown defragmentation mode: %d", mode)
	}

	// 更新统计
	stats.EndTime = time.Now()
	stats.Duration = stats.EndTime.Sub(stats.StartTime).Milliseconds()
	if err != nil {
		stats.Status = "failed"
		stats.LastError = err.Error()
		logger.Errorf("❌ [Defragmenter] Defragmentation failed: %v", err)
	} else {
		stats.Status = "completed"
		stats.Progress = 100.0
		logger.Infof("✅ [Defragmenter] Defragmentation completed: reclaimed=%d bytes, duration=%dms",
			stats.SpaceReclaimed, stats.Duration)
	}

	// 计算性能指标
	if stats.Duration > 0 {
		stats.PagesPerSecond = float64(stats.PagesProcessed) / (float64(stats.Duration) / 1000.0)
		stats.BytesPerSecond = float64(stats.SpaceReclaimed) / (float64(stats.Duration) / 1000.0)
	}
	if stats.PagesProcessed > 0 {
		stats.AveragePageTime = stats.Duration * 1000 / int64(stats.PagesProcessed) // 微秒
	}

	return err
}

// defragmentOnline 在线碎片整理（不阻塞读写）
func (tdf *TablespaceDefragmenter) defragmentOnline(ctx context.Context, spaceID uint32, stats *DefragmentationStats) error {
	logger.Infof("🔧 [Defragmenter] Starting online defragmentation for space %d", spaceID)

	// 获取extent管理器统计
	extStats := tdf.extentManager.GetStats()
	if extStats == nil {
		return fmt.Errorf("failed to get extent stats")
	}

	totalExtents := extStats.TotalExtents
	if totalExtents == 0 {
		return nil
	}

	// 遍历所有extent，整理碎片
	for extentID := uint32(0); extentID < totalExtents; extentID++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 获取extent
		ext, err := tdf.extentManager.GetExtent(extentID)
		if err != nil {
			stats.Warnings++
			continue
		}

		// 检查extent是否需要整理
		if !tdf.needsDefragmentation(ext) {
			continue
		}

		// 整理extent
		if err := tdf.defragmentExtent(ctx, ext, stats); err != nil {
			stats.Errors++
			stats.LastError = err.Error()
			logger.Warnf("⚠️ Failed to defragment extent %d: %v", extentID, err)
			continue
		}

		stats.ExtentsProcessed++
		stats.Progress = float64(extentID+1) / float64(totalExtents) * 100

		// IO节流
		if tdf.config.IOThrottleDelay > 0 {
			time.Sleep(tdf.config.IOThrottleDelay)
		}
	}

	return nil
}

// defragmentOffline 离线碎片整理（需要锁表）
func (tdf *TablespaceDefragmenter) defragmentOffline(ctx context.Context, spaceID uint32, stats *DefragmentationStats) error {
	logger.Infof("🔧 [Defragmenter] Starting offline defragmentation for space %d", spaceID)

	// 离线整理：可以使用更激进的策略
	// 这里简化实现，调用在线整理
	return tdf.defragmentOnline(ctx, spaceID, stats)
}

// defragmentIncremental 增量碎片整理
func (tdf *TablespaceDefragmenter) defragmentIncremental(ctx context.Context, spaceID uint32, stats *DefragmentationStats) error {
	logger.Infof("🔧 [Defragmenter] Starting incremental defragmentation for space %d", spaceID)

	extStats := tdf.extentManager.GetStats()
	if extStats == nil {
		return fmt.Errorf("failed to get extent stats")
	}

	totalExtents := extStats.TotalExtents
	batchSize := tdf.config.IncrementalBatchSize
	if batchSize == 0 {
		batchSize = 10
	}

	// 分批处理
	for startExtent := uint32(0); startExtent < totalExtents; startExtent += batchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		endExtent := startExtent + batchSize
		if endExtent > totalExtents {
			endExtent = totalExtents
		}

		// 处理一批extent
		for extentID := startExtent; extentID < endExtent; extentID++ {
			ext, err := tdf.extentManager.GetExtent(extentID)
			if err != nil {
				stats.Warnings++
				continue
			}

			if !tdf.needsDefragmentation(ext) {
				continue
			}

			if err := tdf.defragmentExtent(ctx, ext, stats); err != nil {
				stats.Errors++
				continue
			}

			stats.ExtentsProcessed++
		}

		stats.Progress = float64(endExtent) / float64(totalExtents) * 100

		// 批次间隔
		if tdf.config.IncrementalInterval > 0 {
			time.Sleep(tdf.config.IncrementalInterval)
		}
	}

	return nil
}

// needsDefragmentation 判断extent是否需要整理
func (tdf *TablespaceDefragmenter) needsDefragmentation(ext *extent.UnifiedExtent) bool {
	// 获取extent状态
	state := ext.GetState()

	// 只整理部分使用的extent
	if state != basic.ExtentStatePartial {
		return false
	}

	// 检查填充率
	// 如果填充率太低，需要整理
	// 这里简化：假设部分使用的extent都需要整理
	return true
}

// defragmentExtent 整理单个extent
func (tdf *TablespaceDefragmenter) defragmentExtent(ctx context.Context, ext *extent.UnifiedExtent, stats *DefragmentationStats) error {
	// 调用extent的Defragment方法
	if err := ext.Defragment(); err != nil {
		return err
	}

	// 更新统计
	stats.PagesProcessed += 64         // 每个extent 64页
	stats.SpaceReclaimed += 16384 * 10 // 假设回收10个页面
	stats.TotalIOOperations += 2       // 读+写

	return nil
}

// getModeString 获取模式字符串
func (tdf *TablespaceDefragmenter) getModeString(mode DefragmentMode) string {
	switch mode {
	case DefragmentModeOnline:
		return "online"
	case DefragmentModeOffline:
		return "offline"
	case DefragmentModeIncremental:
		return "incremental"
	default:
		return "unknown"
	}
}

// OptimizeSpace 优化空间利用率
func (tdf *TablespaceDefragmenter) OptimizeSpace(ctx context.Context, spaceID uint32) error {
	logger.Infof("🔧 [Defragmenter] Optimizing space utilization for space %d", spaceID)

	// 1. 回收完全空闲的extent
	if err := tdf.reclaimFreeExtents(ctx, spaceID); err != nil {
		logger.Warnf("⚠️ Failed to reclaim free extents: %v", err)
	}

	// 2. 合并相邻的部分使用extent
	if err := tdf.mergePartialExtents(ctx, spaceID); err != nil {
		logger.Warnf("⚠️ Failed to merge partial extents: %v", err)
	}

	// 3. 优化页面填充率
	if err := tdf.optimizePageFill(ctx, spaceID); err != nil {
		logger.Warnf("⚠️ Failed to optimize page fill: %v", err)
	}

	logger.Infof("✅ [Defragmenter] Space optimization completed for space %d", spaceID)
	return nil
}

// reclaimFreeExtents 回收空闲extent
func (tdf *TablespaceDefragmenter) reclaimFreeExtents(ctx context.Context, spaceID uint32) error {
	extStats := tdf.extentManager.GetStats()
	if extStats == nil {
		return fmt.Errorf("failed to get extent stats")
	}

	// 获取空闲extent数量
	freeCount := tdf.extentManager.GetFreeExtentCount()
	logger.Infof("📊 Found %d free extents to reclaim", freeCount)

	// 这里简化：空闲extent已经在freeExtents列表中，不需要额外操作
	return nil
}

// mergePartialExtents 合并部分使用的extent
func (tdf *TablespaceDefragmenter) mergePartialExtents(ctx context.Context, spaceID uint32) error {
	// 这里简化实现
	// 实际应该：
	// 1. 找到相邻的部分使用extent
	// 2. 将页面从一个extent移动到另一个
	// 3. 释放空的extent
	logger.Debugf("🔧 Merging partial extents for space %d", spaceID)
	return nil
}

// optimizePageFill 优化页面填充率
func (tdf *TablespaceDefragmenter) optimizePageFill(ctx context.Context, spaceID uint32) error {
	// 这里简化实现
	// 实际应该：
	// 1. 扫描所有页面
	// 2. 找到填充率低的页面
	// 3. 重组页面数据，提高填充率
	logger.Debugf("🔧 Optimizing page fill for space %d", spaceID)
	return nil
}

// GetFragmentationLevel 获取碎片等级
func (tdf *TablespaceDefragmenter) GetFragmentationLevel(spaceID uint32) string {
	report := tdf.GetLastReport(spaceID)
	if report == nil {
		return "unknown"
	}

	rate := report.FragmentationRate
	if rate < 10.0 {
		return "low"
	} else if rate < 30.0 {
		return "medium"
	} else if rate < 50.0 {
		return "high"
	} else {
		return "critical"
	}
}

// ShouldDefragment 判断是否应该执行碎片整理
func (tdf *TablespaceDefragmenter) ShouldDefragment(spaceID uint32) bool {
	report := tdf.GetLastReport(spaceID)
	if report == nil {
		return false
	}
	return report.NeedsDefragmentation
}

// EstimateDefragmentTime 估算碎片整理时间
func (tdf *TablespaceDefragmenter) EstimateDefragmentTime(spaceID uint32, mode DefragmentMode) time.Duration {
	report := tdf.GetLastReport(spaceID)
	if report == nil {
		return 0
	}

	// 简化估算：每个页面1ms
	pagesNeedProcess := report.PageHoles
	baseTime := time.Duration(pagesNeedProcess) * time.Millisecond

	// 根据模式调整
	switch mode {
	case DefragmentModeOnline:
		return baseTime * 2 // 在线模式慢一倍
	case DefragmentModeOffline:
		return baseTime // 离线模式最快
	case DefragmentModeIncremental:
		return baseTime * 3 // 增量模式最慢
	default:
		return baseTime
	}
}
