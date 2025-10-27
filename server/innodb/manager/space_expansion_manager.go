package manager

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"math"
	"sync"
	"time"
)

/*
SpaceExpansionManager 表空间扩展管理器

核心功能：
1. 预测性扩展
   - 基于历史增长率预测空间需求
   - 提前扩展避免阻塞
   - 自适应扩展大小

2. 性能优化
   - 批量Extent分配
   - 异步扩展
   - 扩展操作并行化

3. 空间限制
   - 最大表空间大小限制
   - 磁盘配额检查
   - 扩展失败处理

设计要点：
- 支持固定、比例、自适应三种扩展策略
- 监控空间使用趋势
- 最小化扩展次数
- 提供扩展历史和统计
*/

const (
	// 扩展策略
	ExpansionStrategyFixed    = "fixed"    // 固定扩展
	ExpansionStrategyPercent  = "percent"  // 比例扩展
	ExpansionStrategyAdaptive = "adaptive" // 自适应扩展

	// 扩展阈值
	DefaultLowWaterMark  = 10.0 // 默认低水位（剩余空间百分比）
	DefaultHighWaterMark = 30.0 // 默认高水位（剩余空间百分比）

	// 扩展大小
	DefaultFixedExtents  = 16   // 默认固定扩展Extent数
	DefaultPercentExpand = 25.0 // 默认比例扩展（当前大小的25%）
	MinExtentsPerExpand  = 4    // 最小扩展Extent数
	MaxExtentsPerExpand  = 1024 // 最大扩展Extent数

	// 预测参数
	HistoryWindowSize = 10  // 历史窗口大小
	PredictionHorizon = 300 // 预测时间窗口（秒）

	// 限制
	DefaultMaxSpaceSize = 64 * 1024 * 1024 * 1024 // 默认最大64GB
)

// SpaceExpansionManager 表空间扩展管理器
type SpaceExpansionManager struct {
	sync.RWMutex

	// 空间管理器引用
	spaceManager basic.SpaceManager

	// 配置
	config *ExpansionConfig

	// 扩展历史
	history []*ExpansionRecord

	// 使用率历史（用于预测）
	usageHistory []UsageSnapshot

	// 统计信息
	stats *ExpansionStats

	// 后台扩展任务
	expandChan chan *ExpansionRequest
	stopChan   chan struct{}
	wg         sync.WaitGroup
}

// ExpansionConfig 扩展配置
type ExpansionConfig struct {
	// 策略配置
	Strategy    string // 扩展策略
	AutoExpand  bool   // 是否自动扩展
	AsyncExpand bool   // 是否异步扩展

	// 阈值配置
	LowWaterMark  float64 // 触发扩展的低水位
	HighWaterMark float64 // 目标高水位

	// 扩展大小配置
	FixedExtents  uint32  // 固定扩展Extent数
	PercentExpand float64 // 比例扩展百分比
	MinExtents    uint32  // 最小扩展Extent数
	MaxExtents    uint32  // 最大扩展Extent数

	// 限制配置
	MaxSpaceSize uint64 // 最大表空间大小（字节）
	DiskQuota    uint64 // 磁盘配额（字节）

	// 预测配置
	EnablePrediction bool // 是否启用预测性扩展
	PredictionWindow int  // 预测时间窗口（秒）
}

// ExpansionRecord 扩展记录
type ExpansionRecord struct {
	Timestamp    time.Time     // 扩展时间
	SpaceID      uint32        // 表空间ID
	BeforeSize   uint64        // 扩展前大小（字节）
	AfterSize    uint64        // 扩展后大小（字节）
	ExtentsAdded uint32        // 添加的Extent数
	Strategy     string        // 使用的策略
	Duration     time.Duration // 扩展耗时
	Triggered    string        // 触发方式（auto/manual/predicted）
}

// UsageSnapshot 使用率快照
type UsageSnapshot struct {
	Timestamp time.Time // 时间戳
	SpaceID   uint32    // 表空间ID
	TotalSize uint64    // 总大小
	UsedSize  uint64    // 已使用大小
	UsageRate float64   // 使用率（百分比）
}

// ExpansionStats 扩展统计
type ExpansionStats struct {
	sync.RWMutex

	TotalExpansions     uint64 // 总扩展次数
	AutoExpansions      uint64 // 自动扩展次数
	ManualExpansions    uint64 // 手动扩展次数
	PredictedExpansions uint64 // 预测性扩展次数
	FailedExpansions    uint64 // 失败的扩展次数

	TotalExtentsAdded uint64        // 总添加的Extent数
	TotalBytesAdded   uint64        // 总添加的字节数
	AverageExpandTime time.Duration // 平均扩展时间
	LastExpansion     time.Time     // 最后扩展时间

	CurrentGrowthRate float64   // 当前增长率（MB/小时）
	PredictedFullTime time.Time // 预测满时间
}

// ExpansionRequest 扩展请求
type ExpansionRequest struct {
	SpaceID    uint32 // 表空间ID
	MinExtents uint32 // 最小Extent数
	Triggered  string // 触发方式
	ResultChan chan *ExpansionResult
}

// ExpansionResult 扩展结果
type ExpansionResult struct {
	Success      bool
	ExtentsAdded uint32
	NewSize      uint64
	Error        error
}

// NewSpaceExpansionManager 创建表空间扩展管理器
func NewSpaceExpansionManager(spaceManager basic.SpaceManager, config *ExpansionConfig) *SpaceExpansionManager {
	if config == nil {
		config = &ExpansionConfig{
			Strategy:         ExpansionStrategyAdaptive,
			AutoExpand:       true,
			AsyncExpand:      true,
			LowWaterMark:     DefaultLowWaterMark,
			HighWaterMark:    DefaultHighWaterMark,
			FixedExtents:     DefaultFixedExtents,
			PercentExpand:    DefaultPercentExpand,
			MinExtents:       MinExtentsPerExpand,
			MaxExtents:       MaxExtentsPerExpand,
			MaxSpaceSize:     DefaultMaxSpaceSize,
			DiskQuota:        0, // 0表示无限制
			EnablePrediction: true,
			PredictionWindow: PredictionHorizon,
		}
	}

	sem := &SpaceExpansionManager{
		spaceManager: spaceManager,
		config:       config,
		history:      make([]*ExpansionRecord, 0, 100),
		usageHistory: make([]UsageSnapshot, 0, HistoryWindowSize),
		stats:        &ExpansionStats{},
		expandChan:   make(chan *ExpansionRequest, 10),
		stopChan:     make(chan struct{}),
	}

	// 启动后台扩展worker
	if config.AsyncExpand {
		sem.wg.Add(1)
		go sem.expansionWorker()
	}

	return sem
}

// CheckAndExpand 检查并扩展表空间
func (sem *SpaceExpansionManager) CheckAndExpand(spaceID uint32) error {
	sem.Lock()
	defer sem.Unlock()

	// 获取表空间信息
	space, err := sem.spaceManager.GetSpace(spaceID)
	if err != nil {
		return fmt.Errorf("failed to get space: %v", err)
	}

	// 计算使用率
	usageRate := sem.calculateUsageRate(space)

	// 记录使用率快照
	sem.recordUsageSnapshot(spaceID, space, usageRate)

	// 检查是否需要扩展
	if usageRate >= (100.0 - sem.config.LowWaterMark) {
		// 计算需要扩展的Extent数
		extents := sem.calculateExpansionSize(space, usageRate)

		// 执行扩展
		if sem.config.AsyncExpand {
			return sem.expandAsync(spaceID, extents, "auto")
		} else {
			return sem.expandSync(spaceID, extents, "auto")
		}
	}

	return nil
}

// ExpandSpace 手动扩展表空间
func (sem *SpaceExpansionManager) ExpandSpace(spaceID uint32, extents uint32) error {
	if sem.config.AsyncExpand {
		return sem.expandAsync(spaceID, extents, "manual")
	} else {
		return sem.expandSync(spaceID, extents, "manual")
	}
}

// PredictiveExpand 预测性扩展
func (sem *SpaceExpansionManager) PredictiveExpand(spaceID uint32) error {
	if !sem.config.EnablePrediction {
		return nil
	}

	sem.RLock()
	defer sem.RUnlock()

	// 基于历史数据预测增长
	growthRate := sem.calculateGrowthRate()
	if growthRate <= 0 {
		return nil // 没有增长，不需要扩展
	}

	// 预测未来时间窗口内的空间需求
	space, err := sem.spaceManager.GetSpace(spaceID)
	if err != nil {
		return err
	}

	currentSize := sem.getSpaceSize(space)
	predictedGrowth := growthRate * float64(sem.config.PredictionWindow)

	if currentSize > 0 {
		predictedUsage := (float64(currentSize) + predictedGrowth) / float64(currentSize) * 100

		// 如果预测使用率超过阈值，提前扩展
		if predictedUsage >= (100.0 - sem.config.LowWaterMark) {
			extents := sem.calculateExtentsForGrowth(predictedGrowth)
			return sem.expandAsync(spaceID, extents, "predicted")
		}
	}

	return nil
}

// expandSync 同步扩展
func (sem *SpaceExpansionManager) expandSync(spaceID uint32, extents uint32, triggered string) error {
	startTime := time.Now()

	// 获取表空间
	space, err := sem.spaceManager.GetSpace(spaceID)
	if err != nil {
		sem.stats.FailedExpansions++
		return fmt.Errorf("failed to get space: %v", err)
	}

	beforeSize := sem.getSpaceSize(space)

	// 检查限制
	if err := sem.checkLimits(beforeSize, extents); err != nil {
		sem.stats.FailedExpansions++
		return err
	}

	// 分配Extent
	addedExtents := uint32(0)
	for i := uint32(0); i < extents; i++ {
		_, err := sem.spaceManager.AllocateExtent(spaceID, basic.ExtentPurposeData)
		if err != nil {
			// 部分成功
			break
		}
		addedExtents++
	}

	if addedExtents == 0 {
		sem.stats.FailedExpansions++
		return fmt.Errorf("failed to allocate any extents")
	}

	afterSize := beforeSize + uint64(addedExtents)*64*16384 // 64页/extent * 16KB/页

	// 记录扩展历史
	record := &ExpansionRecord{
		Timestamp:    time.Now(),
		SpaceID:      spaceID,
		BeforeSize:   beforeSize,
		AfterSize:    afterSize,
		ExtentsAdded: addedExtents,
		Strategy:     sem.config.Strategy,
		Duration:     time.Since(startTime),
		Triggered:    triggered,
	}
	sem.recordExpansion(record)

	// 更新统计
	sem.updateStats(record, triggered)

	return nil
}

// expandAsync 异步扩展
func (sem *SpaceExpansionManager) expandAsync(spaceID uint32, extents uint32, triggered string) error {
	req := &ExpansionRequest{
		SpaceID:    spaceID,
		MinExtents: extents,
		Triggered:  triggered,
		ResultChan: make(chan *ExpansionResult, 1),
	}

	select {
	case sem.expandChan <- req:
		// 请求已发送，不等待结果（异步）
		return nil
	default:
		return fmt.Errorf("expansion queue is full")
	}
}

// expansionWorker 后台扩展worker
func (sem *SpaceExpansionManager) expansionWorker() {
	defer sem.wg.Done()

	for {
		select {
		case req := <-sem.expandChan:
			err := sem.expandSync(req.SpaceID, req.MinExtents, req.Triggered)
			result := &ExpansionResult{
				Success: err == nil,
				Error:   err,
			}

			select {
			case req.ResultChan <- result:
			default:
			}

		case <-sem.stopChan:
			return
		}
	}
}

// calculateUsageRate 计算使用率
func (sem *SpaceExpansionManager) calculateUsageRate(space basic.Space) float64 {
	// TODO: 实现实际的使用率计算
	// 这里需要根据space接口获取总大小和已用大小
	return 0.0
}

// calculateExpansionSize 计算扩展大小
func (sem *SpaceExpansionManager) calculateExpansionSize(space basic.Space, usageRate float64) uint32 {
	switch sem.config.Strategy {
	case ExpansionStrategyFixed:
		return sem.config.FixedExtents

	case ExpansionStrategyPercent:
		currentSize := sem.getSpaceSize(space)
		expandBytes := uint64(float64(currentSize) * sem.config.PercentExpand / 100.0)
		extents := uint32(expandBytes / (64 * 16384))
		if extents < sem.config.MinExtents {
			extents = sem.config.MinExtents
		}
		if extents > sem.config.MaxExtents {
			extents = sem.config.MaxExtents
		}
		return extents

	case ExpansionStrategyAdaptive:
		// 自适应：基于使用率和增长率
		growthRate := sem.calculateGrowthRate()
		if growthRate > 0 {
			// 预留足够空间容纳未来增长
			futureGrowth := growthRate * float64(sem.config.PredictionWindow)
			extents := sem.calculateExtentsForGrowth(futureGrowth)
			if extents < sem.config.MinExtents {
				extents = sem.config.MinExtents
			}
			if extents > sem.config.MaxExtents {
				extents = sem.config.MaxExtents
			}
			return extents
		}
		// 回退到固定策略
		return sem.config.FixedExtents

	default:
		return sem.config.FixedExtents
	}
}

// calculateGrowthRate 计算增长率（字节/秒）
func (sem *SpaceExpansionManager) calculateGrowthRate() float64 {
	if len(sem.usageHistory) < 2 {
		return 0.0
	}

	// 使用最近的历史数据计算平均增长率
	var totalGrowth uint64
	var totalTime float64

	for i := 1; i < len(sem.usageHistory); i++ {
		prev := sem.usageHistory[i-1]
		curr := sem.usageHistory[i]

		if curr.UsedSize > prev.UsedSize {
			growth := curr.UsedSize - prev.UsedSize
			duration := curr.Timestamp.Sub(prev.Timestamp).Seconds()

			totalGrowth += growth
			totalTime += duration
		}
	}

	if totalTime > 0 {
		return float64(totalGrowth) / totalTime
	}

	return 0.0
}

// calculateExtentsForGrowth 根据增长量计算需要的Extent数
func (sem *SpaceExpansionManager) calculateExtentsForGrowth(growthBytes float64) uint32 {
	extentSize := 64 * 16384 // 64页 * 16KB
	extents := uint32(math.Ceil(growthBytes / float64(extentSize)))
	return extents
}

// checkLimits 检查扩展限制
func (sem *SpaceExpansionManager) checkLimits(currentSize uint64, extents uint32) error {
	expandSize := uint64(extents) * 64 * 16384
	newSize := currentSize + expandSize

	// 检查最大表空间大小
	if sem.config.MaxSpaceSize > 0 && newSize > sem.config.MaxSpaceSize {
		return fmt.Errorf("exceeds max space size: %d > %d", newSize, sem.config.MaxSpaceSize)
	}

	// 检查磁盘配额
	if sem.config.DiskQuota > 0 && newSize > sem.config.DiskQuota {
		return fmt.Errorf("exceeds disk quota: %d > %d", newSize, sem.config.DiskQuota)
	}

	return nil
}

// recordUsageSnapshot 记录使用率快照
func (sem *SpaceExpansionManager) recordUsageSnapshot(spaceID uint32, space basic.Space, usageRate float64) {
	snapshot := UsageSnapshot{
		Timestamp: time.Now(),
		SpaceID:   spaceID,
		TotalSize: sem.getSpaceSize(space),
		UsedSize:  uint64(float64(sem.getSpaceSize(space)) * usageRate / 100.0),
		UsageRate: usageRate,
	}

	sem.usageHistory = append(sem.usageHistory, snapshot)

	// 保持历史窗口大小
	if len(sem.usageHistory) > HistoryWindowSize {
		sem.usageHistory = sem.usageHistory[1:]
	}
}

// recordExpansion 记录扩展历史
func (sem *SpaceExpansionManager) recordExpansion(record *ExpansionRecord) {
	sem.history = append(sem.history, record)

	// 保持历史记录数量
	if len(sem.history) > 100 {
		sem.history = sem.history[1:]
	}
}

// updateStats 更新统计信息
func (sem *SpaceExpansionManager) updateStats(record *ExpansionRecord, triggered string) {
	sem.stats.Lock()
	defer sem.stats.Unlock()

	sem.stats.TotalExpansions++

	switch triggered {
	case "auto":
		sem.stats.AutoExpansions++
	case "manual":
		sem.stats.ManualExpansions++
	case "predicted":
		sem.stats.PredictedExpansions++
	}

	sem.stats.TotalExtentsAdded += uint64(record.ExtentsAdded)
	sem.stats.TotalBytesAdded += (record.AfterSize - record.BeforeSize)
	sem.stats.LastExpansion = record.Timestamp

	// 更新平均扩展时间
	if sem.stats.TotalExpansions > 0 {
		totalTime := sem.stats.AverageExpandTime * time.Duration(sem.stats.TotalExpansions-1)
		sem.stats.AverageExpandTime = (totalTime + record.Duration) / time.Duration(sem.stats.TotalExpansions)
	}

	// 更新增长率
	sem.stats.CurrentGrowthRate = sem.calculateGrowthRate() * 3600 / (1024 * 1024) // 转换为MB/小时
}

// getSpaceSize 获取表空间大小
func (sem *SpaceExpansionManager) getSpaceSize(space basic.Space) uint64 {
	// TODO: 从space接口获取实际大小
	// 简化实现
	return 0
}

// GetStats 获取统计信息
func (sem *SpaceExpansionManager) GetStats() *ExpansionStats {
	sem.stats.RLock()
	defer sem.stats.RUnlock()

	statsCopy := *sem.stats
	return &statsCopy
}

// GetHistory 获取扩展历史
func (sem *SpaceExpansionManager) GetHistory() []*ExpansionRecord {
	sem.RLock()
	defer sem.RUnlock()

	historyCopy := make([]*ExpansionRecord, len(sem.history))
	copy(historyCopy, sem.history)
	return historyCopy
}

// Stop 停止扩展管理器
func (sem *SpaceExpansionManager) Stop() {
	close(sem.stopChan)
	sem.wg.Wait()
}
