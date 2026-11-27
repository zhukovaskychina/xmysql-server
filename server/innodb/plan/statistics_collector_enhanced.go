package plan

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
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
	tableStats   map[string]*TableStats
	columnStats  map[string]*ColumnStats
	indexStats   map[string]*IndexStats
	modifyCounts map[string]int64

	// 配置参数
	config *StatisticsConfig

	// 存储引擎接口
	spaceManager      basic.SpaceManager
	btreeManager      basic.BPlusTreeManager
	storageAccessor   StorageEngineAccessor
	bufferPoolManager interface{} // 预留接口

	// 后台任务控制
	stopCh   chan struct{}
	updateCh chan *StatisticsUpdateRequest
	stopOnce sync.Once

	// 采样策略
	sampler *AdaptiveSampler
}

// SetStorageEngineAccessor connects ANALYZE column sampling to the engine's
// decoded row source. Call it during optimizer setup, before statistics are
// collected; page-level estimation remains the compatibility fallback when no
// accessor is configured.
func (esc *EnhancedStatisticsCollector) SetStorageEngineAccessor(accessor StorageEngineAccessor) {
	if esc != nil {
		esc.storageAccessor = accessor
	}
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
	if s == nil {
		return 0.05
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.sampleRates) == 0 {
		return 0.05
	}
	thresholds := make([]int64, 0, len(s.sampleRates))
	for threshold := range s.sampleRates {
		if threshold > 0 {
			thresholds = append(thresholds, threshold)
		}
	}
	if len(thresholds) == 0 {
		return 0.05
	}
	sort.Slice(thresholds, func(i, j int) bool { return thresholds[i] < thresholds[j] })
	for _, threshold := range thresholds {
		if rowCount < threshold {
			return clampSampleRate(s.sampleRates[threshold])
		}
	}
	return clampSampleRate(s.sampleRates[thresholds[len(thresholds)-1]])
}

func clampSampleRate(rate float64) float64 {
	if rate < 0 {
		return 0
	}
	if rate > 1 {
		return 1
	}
	return rate
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
		modifyCounts: make(map[string]int64),
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

// GetTableStatistics returns the cached table statistics, if present.
func (esc *EnhancedStatisticsCollector) GetTableStatistics(tableName string) (*TableStats, bool) {
	esc.mu.RLock()
	defer esc.mu.RUnlock()
	if stats, ok := esc.tableStats[tableName]; ok {
		return stats, true
	}
	for key, stats := range esc.tableStats {
		if strings.EqualFold(key, tableName) {
			return stats, true
		}
	}
	return nil, false
}

// GetColumnStatistics returns cached column statistics, if present.
func (esc *EnhancedStatisticsCollector) GetColumnStatistics(tableName, columnName string) (*ColumnStats, bool) {
	esc.mu.RLock()
	defer esc.mu.RUnlock()
	key := fmt.Sprintf("%s.%s", tableName, columnName)
	if stats, ok := esc.columnStats[key]; ok {
		return stats, true
	}
	for candidate, stats := range esc.columnStats {
		if strings.EqualFold(candidate, key) {
			return stats, true
		}
	}
	return nil, false
}

// GetIndexStatistics returns cached index statistics, if present.
func (esc *EnhancedStatisticsCollector) GetIndexStatistics(tableName, indexName string) (*IndexStats, bool) {
	esc.mu.RLock()
	defer esc.mu.RUnlock()
	key := fmt.Sprintf("%s.%s", tableName, indexName)
	if stats, ok := esc.indexStats[key]; ok {
		return stats, true
	}
	for candidate, stats := range esc.indexStats {
		if strings.EqualFold(candidate, key) {
			return stats, true
		}
	}
	return nil, false
}

// Invalidate drops table, column and index statistics so the next request
// refreshes all related optimizer inputs.
func (esc *EnhancedStatisticsCollector) Invalidate(tableName string) {
	esc.mu.Lock()
	defer esc.mu.Unlock()
	modifyKey := tableName
	for key := range esc.tableStats {
		if strings.EqualFold(key, tableName) {
			modifyKey = key
			break
		}
	}
	esc.modifyCounts[modifyKey]++
	for key := range esc.tableStats {
		if strings.EqualFold(key, tableName) {
			delete(esc.tableStats, key)
		}
	}
	for key := range esc.columnStats {
		if statisticsTableKeyMatches(key, tableName) {
			delete(esc.columnStats, key)
		}
	}
	for key := range esc.indexStats {
		if statisticsTableKeyMatches(key, tableName) {
			delete(esc.indexStats, key)
		}
	}
}

func statisticsTableKeyMatches(key, tableName string) bool {
	dot := strings.IndexByte(key, '.')
	if dot < 0 {
		return false
	}
	return strings.EqualFold(key[:dot], tableName)
}

// CollectTableStatistics 收集表统计信息 (OPT-016.1)
func (esc *EnhancedStatisticsCollector) CollectTableStatistics(
	ctx context.Context,
	table *metadata.Table,
) (*TableStats, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	esc.mu.Lock()
	defer esc.mu.Unlock()

	// 检查缓存
	cacheKey := table.Name
	if stats, exists := lookupCaseInsensitiveStats(esc.tableStats, cacheKey); exists {
		if time.Since(time.Unix(stats.LastAnalyzeTime, 0)) < esc.config.ExpirationTime {
			return stats, nil
		}
	}

	// A collector can be used by the planner before a storage accessor is
	// wired. Keep that path safe and cache the deterministic fallback.
	if esc.spaceManager == nil {
		stats := esc.estimateTableStats(table)
		esc.applyStorageAccessorTableStats(table, stats)
		stats.ModifyCount = esc.consumeModifyCount(cacheKey)
		esc.tableStats[cacheKey] = stats
		return stats, nil
	}

	// 获取表空间
	spaceID := esc.getTableSpaceID(table)
	space, err := esc.spaceManager.GetSpace(spaceID)
	if err != nil {
		stats := esc.estimateTableStats(table)
		// A temporary space-manager miss must not discard real statistics from
		// an accessor that can still resolve the logical table.
		esc.applyStorageAccessorTableStats(table, stats)
		stats.ModifyCount = esc.consumeModifyCount(cacheKey)
		esc.tableStats[cacheKey] = stats
		return stats, nil
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
		ModifyCount:     esc.consumeModifyCount(cacheKey),
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

// consumeModifyCount returns the number of invalidations since the previous
// refresh and resets the counter, matching ANALYZE's refresh boundary.
func (esc *EnhancedStatisticsCollector) consumeModifyCount(tableName string) int64 {
	count := esc.modifyCounts[tableName]
	delete(esc.modifyCounts, tableName)
	return count
}

func (esc *EnhancedStatisticsCollector) applyStorageAccessorTableStats(table *metadata.Table, stats *TableStats) {
	if esc == nil || esc.storageAccessor == nil || table == nil || stats == nil {
		return
	}
	spaceID := esc.getTableSpaceID(table)
	if rowCount, err := esc.storageAccessor.GetTableRowCount(spaceID); err == nil && rowCount >= 0 {
		stats.RowCount = rowCount
	}
	if dataSize, indexSize, err := esc.storageAccessor.GetTableSpaceSize(spaceID); err == nil && dataSize >= 0 && indexSize >= 0 {
		stats.DataLength = dataSize
		stats.IndexLength = indexSize
		stats.TotalSize = dataSize + indexSize
	}
	if stats.RowCount > 0 {
		stats.AvgRowLength = stats.DataLength / stats.RowCount
	}
	stats.SampleSize = esc.sampler.GetSampleSize(stats.RowCount)
}

// CollectColumnStatistics 收集列统计信息 (OPT-016.2 + OPT-016.3)
func (esc *EnhancedStatisticsCollector) CollectColumnStatistics(
	ctx context.Context,
	table *metadata.Table,
	column *metadata.Column,
) (*ColumnStats, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cacheKey := fmt.Sprintf("%s.%s", table.Name, column.Name)
	esc.mu.RLock()
	if stats, exists := lookupCaseInsensitiveStats(esc.columnStats, cacheKey); exists && time.Since(time.Unix(stats.LastUpdated, 0)) < esc.config.ExpirationTime {
		esc.mu.RUnlock()
		return stats, nil
	}
	tableStats, hasTableStats := lookupCaseInsensitiveStats(esc.tableStats, table.Name)
	esc.mu.RUnlock()

	// Do not call CollectTableStatistics while holding esc.mu: the collector
	// uses the same lock and the first column request would deadlock.
	if !hasTableStats {
		var err error
		tableStats, err = esc.CollectTableStatistics(ctx, table)
		if err != nil {
			return nil, err
		}
	}
	if tableStats == nil {
		return nil, fmt.Errorf("table statistics unavailable for %s", table.Name)
	}
	tableRowCount := tableStats.RowCount

	esc.mu.Lock()
	defer esc.mu.Unlock()

	// Recheck after acquiring the write lock in case another collector filled
	// the cache while this request was obtaining table statistics.
	if stats, exists := lookupCaseInsensitiveStats(esc.columnStats, cacheKey); exists {
		if time.Since(time.Unix(stats.LastUpdated, 0)) < esc.config.ExpirationTime {
			return stats, nil
		}
	}

	// 获取表空间进行采样
	spaceID := esc.getTableSpaceID(table)
	var space basic.Space
	var err error
	if esc.spaceManager != nil {
		space, err = esc.spaceManager.GetSpace(spaceID)
	} else {
		err = fmt.Errorf("storage accessor is unavailable")
	}
	if err != nil {
		if sampleData, ok := esc.sampleColumnDataFromAccessor(spaceID, column, tableRowCount); ok {
			return esc.buildColumnStatisticsFromSample(table, column, tableRowCount, sampleData), nil
		}
		// 降级为估算模式
		return esc.estimateColumnStats(table, column, tableRowCount), nil
	}

	// 采样数据
	sampleData := esc.sampleColumnData(space, column, tableRowCount)

	return esc.buildColumnStatisticsFromSample(table, column, tableRowCount, sampleData), nil
}

func (esc *EnhancedStatisticsCollector) buildColumnStatisticsFromSample(
	table *metadata.Table,
	column *metadata.Column,
	tableRowCount int64,
	sampleData []interface{},
) *ColumnStats {
	cacheKey := fmt.Sprintf("%s.%s", table.Name, column.Name)
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
	if tableRowCount > 0 {
		stats.SamplingPercent = float64(sampleSize) / float64(tableRowCount) * 100
	}

	// 更新缓存
	esc.columnStats[cacheKey] = stats

	return stats
}

// CollectIndexStatistics 收集索引统计信息 (扩展版)
func (esc *EnhancedStatisticsCollector) CollectIndexStatistics(
	ctx context.Context,
	table *metadata.Table,
	index *metadata.Index,
) (*IndexStats, error) {
	cacheKey := fmt.Sprintf("%s.%s", table.Name, index.Name)
	esc.mu.RLock()
	if stats, exists := lookupCaseInsensitiveStats(esc.indexStats, cacheKey); exists {
		if time.Since(time.Unix(stats.LastUpdated, 0)) < esc.config.ExpirationTime {
			esc.mu.RUnlock()
			return stats, nil
		}
	}
	tableStats, hasTableStats := lookupCaseInsensitiveStats(esc.tableStats, table.Name)
	esc.mu.RUnlock()

	if !hasTableStats {
		var err error
		tableStats, err = esc.CollectTableStatistics(ctx, table)
		if err != nil {
			return nil, err
		}
	}
	if tableStats == nil {
		return nil, fmt.Errorf("table statistics unavailable for %s", table.Name)
	}
	tableRowCount := tableStats.RowCount

	esc.mu.Lock()
	defer esc.mu.Unlock()
	if stats, exists := lookupCaseInsensitiveStats(esc.indexStats, cacheKey); exists {
		if time.Since(time.Unix(stats.LastUpdated, 0)) < esc.config.ExpirationTime {
			return stats, nil
		}
	}

	// 构建索引统计信息
	stats := &IndexStats{
		IndexName:   index.Name,
		LastUpdated: time.Now().Unix(),
	}

	// 估算基数
	if index.IsUnique && !uniqueIndexHasNullableColumn(table, index) {
		stats.Cardinality = tableRowCount
	} else {
		if cardinality, ok := esc.sampleIndexCardinality(ctx, table, index, tableRowCount); ok {
			stats.Cardinality = cardinality
		} else {
			// 根据索引列数估算基数
			cardinality := tableRowCount
			for i := 0; i < len(index.Columns); i++ {
				// 每增加一列，基数减少20%
				cardinality = int64(float64(cardinality) * 0.8)
			}
			stats.Cardinality = int64(math.Max(1, float64(cardinality)))
		}
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
	// Prefer live index-manager measurements when the storage accessor can
	// resolve the logical table/index name to its physical index ID.
	if live, ok := esc.readLiveIndexStatistics(table, index, tableRowCount); ok {
		if live.Cardinality > 0 || tableRowCount == 0 {
			stats.Cardinality = live.Cardinality
		}
		if live.TreeDepth > 0 {
			stats.TreeDepth = live.TreeDepth
		}
		if live.LeafPages > 0 {
			stats.LeafPages = live.LeafPages
		}
		if live.NonLeafPages > 0 {
			stats.NonLeafPages = live.NonLeafPages
		}
		if tableRowCount > 0 {
			stats.Selectivity = float64(stats.Cardinality) / float64(tableRowCount)
		}
		if stats.LeafPages > 0 && stats.Cardinality > 0 {
			stats.KeysPerPage = float64(stats.Cardinality) / float64(stats.LeafPages)
		}
	}
	// ANALYZE and index rebuilds can persist page/cardinality metadata on the
	// dictionary index. Prefer those durable measurements over the fallback
	// shape estimate while retaining estimates for fields not persisted by the
	// current metadata format.
	if persisted := index.Stats; persisted != nil {
		if persisted.Cardinality > 0 {
			stats.Cardinality = persisted.Cardinality
		}
		if persisted.LeafPages > 0 {
			stats.LeafPages = persisted.LeafPages
		}
		if persisted.NonLeafPages > 0 {
			stats.NonLeafPages = persisted.NonLeafPages
		}
		if tableRowCount > 0 {
			stats.Selectivity = float64(stats.Cardinality) / float64(tableRowCount)
		}
		if stats.LeafPages > 0 && stats.Cardinality > 0 {
			stats.KeysPerPage = float64(stats.Cardinality) / float64(stats.LeafPages)
		}
	}

	// 更新缓存
	esc.indexStats[cacheKey] = stats

	return stats, nil
}

func uniqueIndexHasNullableColumn(table *metadata.Table, index *metadata.Index) bool {
	if table == nil || index == nil {
		return false
	}
	for _, columnName := range index.Columns {
		column, ok := table.GetColumn(columnName)
		if ok && column != nil && column.IsNullable {
			return true
		}
	}
	return false
}

type storageIndexIDResolver interface {
	GetIndexID(schemaName, tableName, indexName string) (uint32, error)
}

type liveIndexStatistics struct {
	Cardinality  int64
	TreeDepth    int
	LeafPages    int64
	NonLeafPages int64
}

func (esc *EnhancedStatisticsCollector) readLiveIndexStatistics(
	table *metadata.Table,
	index *metadata.Index,
	tableRowCount int64,
) (liveIndexStatistics, bool) {
	if esc == nil || esc.storageAccessor == nil || table == nil || index == nil {
		return liveIndexStatistics{}, false
	}
	resolver, ok := esc.storageAccessor.(storageIndexIDResolver)
	if !ok {
		return liveIndexStatistics{}, false
	}
	schemaName := ""
	if table.Schema != nil {
		schemaName = table.Schema.Name
	}
	indexID, err := resolver.GetIndexID(schemaName, table.Name, index.Name)
	if err != nil || indexID == 0 {
		return liveIndexStatistics{}, false
	}
	result := liveIndexStatistics{}
	if cardinality, cardinalityErr := esc.storageAccessor.GetIndexCardinality(indexID); cardinalityErr == nil && cardinality >= 0 {
		result.Cardinality = cardinality
	}
	if treeDepth, leafPages, nonLeafPages, statsErr := esc.storageAccessor.GetBTreeStatistics(indexID); statsErr == nil {
		result.TreeDepth = treeDepth
		result.LeafPages = leafPages
		result.NonLeafPages = nonLeafPages
	}
	if result.Cardinality == 0 && tableRowCount > 0 && result.LeafPages == 0 {
		return liveIndexStatistics{}, false
	}
	return result, true
}

// sampleIndexCardinality derives distinct index keys from decoded clustered
// records when the storage accessor can provide them. It keeps the planner's
// fallback estimate for accessors that cannot sample, and scales sampled
// distinct keys conservatively when the sample covers only part of the table.
func (esc *EnhancedStatisticsCollector) sampleIndexCardinality(
	ctx context.Context,
	table *metadata.Table,
	index *metadata.Index,
	tableRowCount int64,
) (int64, bool) {
	if esc == nil || esc.storageAccessor == nil || table == nil || index == nil || len(index.Columns) == 0 {
		return 0, false
	}
	if err := ctx.Err(); err != nil {
		return 0, false
	}
	rate := esc.config.SampleRate
	if rate <= 0 {
		rate = 1
	}
	records, err := esc.storageAccessor.SampleTableRecords(esc.getTableSpaceID(table), rate)
	if err != nil {
		return 0, false
	}
	ordinals := make([]int, 0, len(index.Columns))
	for _, name := range index.Columns {
		column, ok := table.GetColumn(name)
		if !ok || column == nil || column.OrdinalPosition <= 0 {
			return 0, false
		}
		ordinals = append(ordinals, column.OrdinalPosition-1)
	}
	distinct := make(map[string]struct{}, len(records))
	for _, record := range records {
		key := strings.Builder{}
		valid := true
		for _, ordinal := range ordinals {
			if ordinal >= len(record) {
				valid = false
				break
			}
			value := record[ordinal]
			key.WriteString(fmt.Sprintf("%T:%#v|", value, value))
		}
		if valid {
			distinct[key.String()] = struct{}{}
		}
	}
	if len(records) == 0 {
		return 0, true
	}
	cardinality := int64(len(distinct))
	if tableRowCount > int64(len(records)) && rate < 1 {
		ratio := float64(len(records)) / float64(tableRowCount)
		if ratio > 0 {
			cardinality = int64(math.Ceil(float64(cardinality) / ratio))
		}
		if cardinality > tableRowCount {
			cardinality = tableRowCount
		}
	}
	return cardinality, true
}
