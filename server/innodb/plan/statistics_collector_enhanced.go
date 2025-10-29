package plan

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// EnhancedStatisticsCollector 增强版统计信息收集器
// 实现OPT-016：基于真实存储引擎的统计信息收集
type EnhancedStatisticsCollector struct {
	mu sync.RWMutex

	// 统计信息存储
	tableStats  map[string]*TableStats
	columnStats map[string]*ColumnStats
	indexStats  map[string]*IndexStats

	// 配置参数
	config *StatisticsConfig

	// 存储引擎接口
	spaceManager      basic.SpaceManager
	btreeManager      basic.BPlusTreeManager
	bufferPoolManager interface{} // 预留接口

	// 后台任务控制
	stopCh   chan struct{}
	updateCh chan *StatisticsUpdateRequest

	// 采样策略
	sampler *AdaptiveSampler
}

// StorageEngineAccessor 存储引擎访问器接口
type StorageEngineAccessor interface {
	// GetTableRowCount 获取表行数
	GetTableRowCount(spaceID uint32) (int64, error)
	// SampleTableRecords 采样表记录
	SampleTableRecords(spaceID uint32, sampleRate float64) ([][]interface{}, error)
	// GetIndexCardinality 获取索引基数
	GetIndexCardinality(indexID uint32) (int64, error)
	// GetTableSpaceSize 获取表空间大小
	GetTableSpaceSize(spaceID uint32) (dataSize int64, indexSize int64, err error)
	// GetBTreeStatistics 获取B+树统计
	GetBTreeStatistics(indexID uint32) (treeDepth int, leafPages int64, nonLeafPages int64, err error)
}

// AdaptiveSampler 自适应采样器
type AdaptiveSampler struct {
	mu sync.RWMutex
	// 采样率配置表
	sampleRates map[int64]float64
}

// NewAdaptiveSampler 创建自适应采样器
func NewAdaptiveSampler() *AdaptiveSampler {
	return &AdaptiveSampler{
		sampleRates: map[int64]float64{
			10000:    1.0,  // < 1万行: 100%
			100000:   0.5,  // 1万-10万行: 50%
			1000000:  0.1,  // 10万-100万行: 10%
			10000000: 0.05, // > 100万行: 5%
		},
	}
}

// GetSampleRate 获取采样率
func (s *AdaptiveSampler) GetSampleRate(rowCount int64) float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if rowCount < 10000 {
		return 1.0
	} else if rowCount < 100000 {
		return 0.5
	} else if rowCount < 1000000 {
		return 0.1
	}
	return 0.05
}

// GetSampleSize 获取采样行数
func (s *AdaptiveSampler) GetSampleSize(rowCount int64) int64 {
	rate := s.GetSampleRate(rowCount)
	sampleSize := int64(float64(rowCount) * rate)

	// 最少1000行，最多50000行
	if sampleSize < 1000 {
		sampleSize = int64(math.Min(float64(rowCount), 1000))
	}
	if sampleSize > 50000 {
		sampleSize = 50000
	}

	return sampleSize
}

// NewEnhancedStatisticsCollector 创建增强版统计信息收集器
func NewEnhancedStatisticsCollector(
	config *StatisticsConfig,
	spaceManager basic.SpaceManager,
	btreeManager basic.BPlusTreeManager,
) *EnhancedStatisticsCollector {
	if config == nil {
		config = &StatisticsConfig{
			AutoUpdateInterval: 1 * time.Hour,
			SampleRate:         0.1,
			HistogramBuckets:   64,
			ExpirationTime:     24 * time.Hour,
			EnableAutoUpdate:   true,
		}
	}

	esc := &EnhancedStatisticsCollector{
		tableStats:   make(map[string]*TableStats),
		columnStats:  make(map[string]*ColumnStats),
		indexStats:   make(map[string]*IndexStats),
		config:       config,
		spaceManager: spaceManager,
		btreeManager: btreeManager,
		stopCh:       make(chan struct{}),
		updateCh:     make(chan *StatisticsUpdateRequest, 100),
		sampler:      NewAdaptiveSampler(),
	}

	// 启动后台更新任务
	if config.EnableAutoUpdate {
		go esc.backgroundUpdateWorker()
	}

	return esc
}

// CollectTableStatistics 收集表统计信息 (OPT-016.1)
func (esc *EnhancedStatisticsCollector) CollectTableStatistics(
	ctx context.Context,
	table *metadata.Table,
) (*TableStats, error) {
	esc.mu.Lock()
	defer esc.mu.Unlock()

	// 检查缓存
	cacheKey := table.Name
	if stats, exists := esc.tableStats[cacheKey]; exists {
		if time.Since(time.Unix(stats.LastAnalyzeTime, 0)) < esc.config.ExpirationTime {
			return stats, nil
		}
	}

	// 获取表空间
	spaceID := esc.getTableSpaceID(table)
	space, err := esc.spaceManager.GetSpace(spaceID)
	if err != nil {
		// 降级为估算模式
		return esc.estimateTableStats(table), nil
	}

	// 1. 获取真实行数
	rowCount := esc.getRealRowCount(space)

	// 2. 获取空间大小
	dataSize, indexSize := esc.getSpaceSize(space)

	// 3. 计算平均行长度
	avgRowLength := int64(0)
	if rowCount > 0 {
		avgRowLength = dataSize / rowCount
	}

	// 4. 构建表统计信息
	stats := &TableStats{
		TableName:       table.Name,
		RowCount:        rowCount,
		TotalSize:       dataSize + indexSize,
		ModifyCount:     0, // TODO: 从undo日志获取
		LastAnalyzeTime: time.Now().Unix(),

		// 扩展字段
		AvgRowLength:  avgRowLength,
		DataLength:    dataSize,
		IndexLength:   indexSize,
		DataFree:      esc.getFreeSpace(space),
		AutoIncrement: esc.getAutoIncrementValue(table),
		SampleSize:    esc.sampler.GetSampleSize(rowCount),
	}

	// 更新缓存
	esc.tableStats[cacheKey] = stats

	return stats, nil
}

// CollectColumnStatistics 收集列统计信息 (OPT-016.2 + OPT-016.3)
func (esc *EnhancedStatisticsCollector) CollectColumnStatistics(
	ctx context.Context,
	table *metadata.Table,
	column *metadata.Column,
) (*ColumnStats, error) {
	esc.mu.Lock()
	defer esc.mu.Unlock()

	// 检查缓存
	cacheKey := fmt.Sprintf("%s.%s", table.Name, column.Name)
	if stats, exists := esc.columnStats[cacheKey]; exists {
		if time.Since(time.Unix(stats.LastUpdated, 0)) < esc.config.ExpirationTime {
			return stats, nil
		}
	}

	// 获取表统计信息
	var tableRowCount int64
	if tableStats, exists := esc.tableStats[table.Name]; exists {
		tableRowCount = tableStats.RowCount
	} else {
		tableStats, err := esc.CollectTableStatistics(ctx, table)
		if err != nil {
			return nil, err
		}
		tableRowCount = tableStats.RowCount
		esc.mu.Unlock()
		esc.mu.Lock()
	}

	// 获取表空间进行采样
	spaceID := esc.getTableSpaceID(table)
	space, err := esc.spaceManager.GetSpace(spaceID)
	if err != nil {
		// 降级为估算模式
		return esc.estimateColumnStats(table, column, tableRowCount), nil
	}

	// 采样数据
	sampleData := esc.sampleColumnData(space, column, tableRowCount)

	// 构建列统计信息
	stats := &ColumnStats{
		ColumnName:    column.Name,
		NotNullCount:  0,
		NullCount:     0,
		DistinctCount: 0,
		LastUpdated:   time.Now().Unix(),
	}

	// 使用HyperLogLog估算NDV
	hll := NewHyperLogLog(14) // 14位精度，误差约0.81%

	for _, value := range sampleData {
		if value == nil {
			stats.NullCount++
		} else {
			stats.NotNullCount++
			hll.Add(value)
		}
	}

	// 估算总体NDV
	sampleNDV := hll.Count()
	sampleSize := int64(len(sampleData))
	if sampleSize > 0 && tableRowCount > sampleSize {
		// 根据采样比例推算总体NDV
		samplingRatio := float64(sampleSize) / float64(tableRowCount)
		stats.DistinctCount = int64(float64(sampleNDV) / samplingRatio)

		// 限制NDV不超过总行数
		if stats.DistinctCount > tableRowCount {
			stats.DistinctCount = tableRowCount
		}
	} else {
		stats.DistinctCount = sampleNDV
	}

	// 推算总体NULL计数
	if sampleSize > 0 {
		nullRatio := float64(stats.NullCount) / float64(sampleSize)
		stats.NullCount = int64(float64(tableRowCount) * nullRatio)
		stats.NotNullCount = tableRowCount - stats.NullCount
	}

	// 获取最大最小值
	stats.MaxValue, stats.MinValue = esc.findMinMax(sampleData)

	// 构建直方图
	histogramType := esc.determineHistogramType(column)
	stats.HistogramType = esc.histogramTypeToString(histogramType)
	stats.Histogram = esc.buildEnhancedHistogram(sampleData, column, histogramType, tableRowCount)
	stats.BucketCount = len(stats.Histogram.Buckets)
	stats.SamplingPercent = float64(sampleSize) / float64(tableRowCount) * 100

	// 更新缓存
	esc.columnStats[cacheKey] = stats

	return stats, nil
}

// CollectIndexStatistics 收集索引统计信息 (扩展版)
func (esc *EnhancedStatisticsCollector) CollectIndexStatistics(
	ctx context.Context,
	table *metadata.Table,
	index *metadata.Index,
) (*IndexStats, error) {
	esc.mu.Lock()
	defer esc.mu.Unlock()

	// 检查缓存
	cacheKey := fmt.Sprintf("%s.%s", table.Name, index.Name)
	if stats, exists := esc.indexStats[cacheKey]; exists {
		if time.Since(time.Unix(stats.LastUpdated, 0)) < esc.config.ExpirationTime {
			return stats, nil
		}
	}

	// 获取表行数
	var tableRowCount int64
	if tableStats, exists := esc.tableStats[table.Name]; exists {
		tableRowCount = tableStats.RowCount
	} else {
		tableStats, err := esc.CollectTableStatistics(ctx, table)
		if err != nil {
			return nil, err
		}
		tableRowCount = tableStats.RowCount
		esc.mu.Unlock()
		esc.mu.Lock()
	}

	// 构建索引统计信息
	stats := &IndexStats{
		IndexName:   index.Name,
		LastUpdated: time.Now().Unix(),
	}

	// 估算基数
	if index.IsUnique {
		stats.Cardinality = tableRowCount
	} else {
		// 根据索引列数估算基数
		cardinality := tableRowCount
		for i := 0; i < len(index.Columns); i++ {
			// 每增加一列，基数减少20%
			cardinality = int64(float64(cardinality) * 0.8)
		}
		stats.Cardinality = int64(math.Max(1, float64(cardinality)))
	}

	// 计算选择性
	if tableRowCount > 0 {
		stats.Selectivity = float64(stats.Cardinality) / float64(tableRowCount)
	} else {
		stats.Selectivity = 1.0
	}

	// 估算聚簇因子
	if index.IsPrimary {
		stats.ClusterFactor = 1.0
	} else {
		stats.ClusterFactor = 1.5 // 非聚簇索引
	}

	// 估算B+树深度和页面数
	stats.TreeDepth = esc.estimateTreeDepth(tableRowCount)
	stats.LeafPages = esc.estimateLeafPages(tableRowCount)
	stats.NonLeafPages = esc.estimateNonLeafPages(stats.TreeDepth, stats.LeafPages)
	stats.KeysPerPage = esc.estimateKeysPerPage(tableRowCount, stats.LeafPages)

	// 更新缓存
	esc.indexStats[cacheKey] = stats

	return stats, nil
}
