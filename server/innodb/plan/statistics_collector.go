package plan

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// StatisticsCollector 统计信息收集器
type StatisticsCollector struct {
	mu sync.RWMutex

	// 统计信息存储
	tableStats  map[string]*TableStats
	columnStats map[string]*ColumnStats
	indexStats  map[string]*IndexStats

	// 配置参数
	config *StatisticsConfig

	// 后台任务控制
	stopCh   chan struct{}
	updateCh chan *StatisticsUpdateRequest
}

// StatisticsConfig 统计信息配置
type StatisticsConfig struct {
	// 自动更新间隔
	AutoUpdateInterval time.Duration
	// 采样率 (0.0-1.0)
	SampleRate float64
	// 直方图桶数量
	HistogramBuckets int
	// 统计信息过期时间
	ExpirationTime time.Duration
	// 是否启用自动更新
	EnableAutoUpdate bool
}

// StatisticsUpdateRequest 统计信息更新请求
type StatisticsUpdateRequest struct {
	TableName  string
	ColumnName string
	IndexName  string
	UpdateType StatisticsUpdateType
}

// StatisticsUpdateType 统计信息更新类型
type StatisticsUpdateType int

const (
	UpdateTypeTable StatisticsUpdateType = iota
	UpdateTypeColumn
	UpdateTypeIndex
	UpdateTypeAll
)

// NewStatisticsCollector 创建统计信息收集器
func NewStatisticsCollector(config *StatisticsConfig) *StatisticsCollector {
	if config == nil {
		config = &StatisticsConfig{
			AutoUpdateInterval: 1 * time.Hour,
			SampleRate:         0.1,
			HistogramBuckets:   64,
			ExpirationTime:     24 * time.Hour,
			EnableAutoUpdate:   true,
		}
	}

	sc := &StatisticsCollector{
		tableStats:  make(map[string]*TableStats),
		columnStats: make(map[string]*ColumnStats),
		indexStats:  make(map[string]*IndexStats),
		config:      config,
		stopCh:      make(chan struct{}),
		updateCh:    make(chan *StatisticsUpdateRequest, 100),
	}

	// 启动后台更新任务
	if config.EnableAutoUpdate {
		go sc.backgroundUpdateWorker()
	}

	return sc
}

// CollectTableStatistics 收集表统计信息
func (sc *StatisticsCollector) CollectTableStatistics(
	ctx context.Context,
	table *metadata.Table,
) (*TableStats, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 检查缓存
	cacheKey := table.Name
	if stats, exists := sc.tableStats[cacheKey]; exists {
		if time.Since(time.Unix(stats.LastAnalyzeTime, 0)) < sc.config.ExpirationTime {
			return stats, nil
		}
	}

	// 收集新的统计信息
	stats := &TableStats{
		TableName:       table.Name,
		RowCount:        0,
		TotalSize:       0,
		ModifyCount:     0,
		LastAnalyzeTime: time.Now().Unix(),
	}

	// 模拟数据收集（实际实现需要访问存储引擎）
	stats.RowCount = sc.estimateRowCount(table)
	stats.TotalSize = sc.estimateDataSize(table, stats.RowCount) + sc.estimateIndexSize(table)

	// 更新缓存
	sc.tableStats[cacheKey] = stats

	return stats, nil
}

// CollectColumnStatistics 收集列统计信息
func (sc *StatisticsCollector) CollectColumnStatistics(
	ctx context.Context,
	table *metadata.Table,
	column *metadata.Column,
) (*ColumnStats, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 检查缓存
	cacheKey := fmt.Sprintf("%s.%s", table.Name, column.Name)
	if stats, exists := sc.columnStats[cacheKey]; exists {
		// 由于ColumnStats没有LastUpdated字段，我们使用表的LastAnalyzeTime
		if tableStats, tableExists := sc.tableStats[table.Name]; tableExists {
			if time.Since(time.Unix(tableStats.LastAnalyzeTime, 0)) < sc.config.ExpirationTime {
				return stats, nil
			}
		}
	}

	// 收集新的统计信息
	stats := &ColumnStats{
		ColumnName: column.Name,
	}

	// 收集基本统计信息
	if err := sc.collectBasicColumnStats(ctx, table, column, stats); err != nil {
		return nil, fmt.Errorf("收集基本列统计信息失败: %v", err)
	}

	// 构建直方图
	if err := sc.buildHistogram(ctx, table, column, stats); err != nil {
		return nil, fmt.Errorf("构建直方图失败: %v", err)
	}

	// 更新缓存
	sc.columnStats[cacheKey] = stats

	return stats, nil
}

// CollectIndexStatistics 收集索引统计信息
func (sc *StatisticsCollector) CollectIndexStatistics(
	ctx context.Context,
	table *metadata.Table,
	index *metadata.Index,
) (*IndexStats, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 检查缓存
	cacheKey := fmt.Sprintf("%s.%s", table.Name, index.Name)
	if stats, exists := sc.indexStats[cacheKey]; exists {
		// 由于IndexStats没有LastUpdated字段，我们使用表的LastAnalyzeTime
		if tableStats, tableExists := sc.tableStats[table.Name]; tableExists {
			if time.Since(time.Unix(tableStats.LastAnalyzeTime, 0)) < sc.config.ExpirationTime {
				return stats, nil
			}
		}
	}

	// 收集新的统计信息
	stats := &IndexStats{
		IndexName: index.Name,
	}

	// 收集索引统计信息
	if err := sc.collectBasicIndexStats(ctx, table, index, stats); err != nil {
		return nil, fmt.Errorf("收集索引统计信息失败: %v", err)
	}

	// 更新缓存
	sc.indexStats[cacheKey] = stats

	return stats, nil
}

// collectBasicColumnStats 收集基本列统计信息
func (sc *StatisticsCollector) collectBasicColumnStats(
	ctx context.Context,
	table *metadata.Table,
	column *metadata.Column,
	stats *ColumnStats,
) error {
	// 模拟数据收集（实际实现需要执行SQL查询）

	// 获取表行数 - 直接从缓存获取，避免死锁
	var totalCount int64
	if tableStats, exists := sc.tableStats[table.Name]; exists {
		totalCount = tableStats.RowCount
	} else {
		// 如果没有表统计信息，使用估算值
		totalCount = sc.estimateRowCount(table)
	}

	// 根据数据类型估算统计信息
	switch column.DataType {
	case metadata.TypeInt, metadata.TypeBigInt:
		stats.DistinctCount = int64(math.Min(float64(totalCount), 1000000))
		stats.NullCount = int64(float64(totalCount) * 0.05) // 假设5%为NULL
		stats.NotNullCount = totalCount - stats.NullCount
		stats.MinValue = int64(1)
		stats.MaxValue = int64(totalCount)

	case metadata.TypeVarchar, metadata.TypeText:
		stats.DistinctCount = int64(math.Min(float64(totalCount)*0.8, 100000))
		stats.NullCount = int64(float64(totalCount) * 0.02) // 假设2%为NULL
		stats.NotNullCount = totalCount - stats.NullCount
		stats.MinValue = "a"
		stats.MaxValue = "zzz"

	case metadata.TypeDateTime, metadata.TypeTimestamp:
		stats.DistinctCount = int64(math.Min(float64(totalCount)*0.9, 1000000))
		stats.NullCount = int64(float64(totalCount) * 0.01) // 假设1%为NULL
		stats.NotNullCount = totalCount - stats.NullCount
		stats.MinValue = time.Now().AddDate(-1, 0, 0)
		stats.MaxValue = time.Now()

	default:
		stats.DistinctCount = int64(math.Min(float64(totalCount)*0.5, 10000))
		stats.NullCount = int64(float64(totalCount) * 0.1) // 假设10%为NULL
		stats.NotNullCount = totalCount - stats.NullCount
	}

	return nil
}

// buildHistogram 构建直方图
func (sc *StatisticsCollector) buildHistogram(
	ctx context.Context,
	table *metadata.Table,
	column *metadata.Column,
	stats *ColumnStats,
) error {
	// 获取表行数 - 直接从缓存获取，避免死锁
	var totalCount int64
	if tableStats, exists := sc.tableStats[table.Name]; exists {
		totalCount = tableStats.RowCount
	} else {
		// 如果没有表统计信息，使用估算值
		totalCount = sc.estimateRowCount(table)
	}

	// 创建直方图
	histogram := &Histogram{
		NumBuckets: sc.config.HistogramBuckets,
		TotalCount: totalCount,
		Buckets:    make([]Bucket, 0, sc.config.HistogramBuckets),
		NDV:        stats.DistinctCount,
	}

	// 根据数据类型构建不同的直方图
	switch column.DataType {
	case metadata.TypeInt, metadata.TypeBigInt:
		sc.buildNumericHistogram(histogram, stats)
	case metadata.TypeVarchar, metadata.TypeText:
		sc.buildStringHistogram(histogram, stats)
	case metadata.TypeDateTime, metadata.TypeTimestamp:
		sc.buildDateTimeHistogram(histogram, stats)
	default:
		sc.buildGenericHistogram(histogram, stats)
	}

	stats.Histogram = histogram
	return nil
}

// buildNumericHistogram 构建数值直方图
func (sc *StatisticsCollector) buildNumericHistogram(histogram *Histogram, stats *ColumnStats) {
	minVal := stats.MinValue.(int64)
	maxVal := stats.MaxValue.(int64)
	bucketSize := (maxVal - minVal) / int64(histogram.NumBuckets)
	if bucketSize == 0 {
		bucketSize = 1
	}

	for i := 0; i < histogram.NumBuckets; i++ {
		lowerBound := minVal + int64(i)*bucketSize
		upperBound := minVal + int64(i+1)*bucketSize
		if i == histogram.NumBuckets-1 {
			upperBound = maxVal
		}

		// 估算桶中的记录数
		bucketCount := histogram.TotalCount / int64(histogram.NumBuckets)
		if i == 0 || i == histogram.NumBuckets-1 {
			bucketCount = int64(float64(bucketCount) * 1.2) // 边界桶可能有更多数据
		}

		bucket := Bucket{
			LowerBound: lowerBound,
			UpperBound: upperBound,
			Count:      bucketCount,
			Distinct:   bucketCount / 10, // 假设每10个值有1个不同值
		}
		histogram.Buckets = append(histogram.Buckets, bucket)
	}
}

// buildStringHistogram 构建字符串直方图
func (sc *StatisticsCollector) buildStringHistogram(histogram *Histogram, stats *ColumnStats) {
	// 简化实现：按字母顺序分桶
	bucketCount := histogram.NumBuckets
	totalCount := histogram.TotalCount

	for i := 0; i < bucketCount; i++ {
		// 计算字母范围
		startChar := 'a' + rune(i*26/bucketCount)
		endChar := 'a' + rune((i+1)*26/bucketCount-1)
		if i == bucketCount-1 {
			endChar = 'z'
		}

		bucket := Bucket{
			LowerBound: string(startChar),
			UpperBound: string(endChar),
			Count:      totalCount / int64(bucketCount),
			Distinct:   (totalCount / int64(bucketCount)) / 5,
		}
		histogram.Buckets = append(histogram.Buckets, bucket)
	}
}

// buildDateTimeHistogram 构建日期时间直方图
func (sc *StatisticsCollector) buildDateTimeHistogram(histogram *Histogram, stats *ColumnStats) {
	minTime := stats.MinValue.(time.Time)
	maxTime := stats.MaxValue.(time.Time)
	duration := maxTime.Sub(minTime)
	bucketDuration := duration / time.Duration(histogram.NumBuckets)

	for i := 0; i < histogram.NumBuckets; i++ {
		lowerBound := minTime.Add(time.Duration(i) * bucketDuration)
		upperBound := minTime.Add(time.Duration(i+1) * bucketDuration)
		if i == histogram.NumBuckets-1 {
			upperBound = maxTime
		}

		bucket := Bucket{
			LowerBound: lowerBound,
			UpperBound: upperBound,
			Count:      histogram.TotalCount / int64(histogram.NumBuckets),
			Distinct:   (histogram.TotalCount / int64(histogram.NumBuckets)) / 100,
		}
		histogram.Buckets = append(histogram.Buckets, bucket)
	}
}

// buildGenericHistogram 构建通用直方图
func (sc *StatisticsCollector) buildGenericHistogram(histogram *Histogram, stats *ColumnStats) {
	bucketCount := histogram.NumBuckets
	totalCount := histogram.TotalCount

	for i := 0; i < bucketCount; i++ {
		bucket := Bucket{
			LowerBound: fmt.Sprintf("bucket_%d_start", i),
			UpperBound: fmt.Sprintf("bucket_%d_end", i),
			Count:      totalCount / int64(bucketCount),
			Distinct:   (totalCount / int64(bucketCount)) / 10,
		}
		histogram.Buckets = append(histogram.Buckets, bucket)
	}
}

// collectBasicIndexStats 收集基本索引统计信息
func (sc *StatisticsCollector) collectBasicIndexStats(
	ctx context.Context,
	table *metadata.Table,
	index *metadata.Index,
	stats *IndexStats,
) error {
	// 获取表统计信息 - 直接从缓存获取，避免死锁
	var tableRowCount int64
	if tableStats, exists := sc.tableStats[table.Name]; exists {
		tableRowCount = tableStats.RowCount
	} else {
		// 如果没有表统计信息，使用估算值
		tableRowCount = sc.estimateRowCount(table)
	}

	// 基本索引统计信息
	keyCount := tableRowCount

	// 计算基数
	if index.IsUnique {
		stats.Cardinality = tableRowCount
	} else {
		// 根据索引列数估算基数
		cardinality := tableRowCount
		for range index.Columns {
			cardinality = int64(float64(cardinality) * 0.8) // 每增加一列，基数减少20%
		}
		stats.Cardinality = int64(math.Max(1, float64(cardinality)))
	}

	// 计算选择性
	if tableRowCount > 0 {
		stats.Selectivity = float64(stats.Cardinality) / float64(tableRowCount)
	} else {
		stats.Selectivity = 1.0
	}

	// 计算聚簇因子（简化实现）
	stats.ClusterFactor = float64(keyCount) / float64(stats.Cardinality)

	return nil
}

// 估算方法

// estimateRowCount 估算表行数
func (sc *StatisticsCollector) estimateRowCount(table *metadata.Table) int64 {
	// 简化实现：根据表名生成模拟数据
	hash := 0
	for _, c := range table.Name {
		hash = hash*31 + int(c)
	}
	if hash < 0 {
		hash = -hash
	}
	return int64(hash%1000000 + 1000) // 1000-1000000行
}

// estimateDataSize 估算数据大小
func (sc *StatisticsCollector) estimateDataSize(table *metadata.Table, rowCount int64) int64 {
	avgRowSize := int64(0)
	for _, col := range table.Columns {
		switch col.DataType {
		case metadata.TypeInt:
			avgRowSize += 4
		case metadata.TypeBigInt:
			avgRowSize += 8
		case metadata.TypeVarchar:
			avgRowSize += int64(col.CharMaxLength / 2) // 假设平均使用一半长度
		case metadata.TypeText:
			avgRowSize += 1000 // 假设平均1KB
		case metadata.TypeDateTime:
			avgRowSize += 8
		default:
			avgRowSize += 10
		}
	}
	return rowCount * avgRowSize
}

// estimateIndexSize 估算索引大小
func (sc *StatisticsCollector) estimateIndexSize(table *metadata.Table) int64 {
	totalIndexSize := int64(0)
	for _, index := range table.Indices {
		indexSize := int64(0)
		for _, colName := range index.Columns {
			if col, exists := table.GetColumn(colName); exists {
				switch col.DataType {
				case metadata.TypeInt:
					indexSize += 4
				case metadata.TypeBigInt:
					indexSize += 8
				case metadata.TypeVarchar:
					indexSize += int64(col.CharMaxLength / 3) // 索引通常更紧凑
				default:
					indexSize += 8
				}
			}
		}
		// 估算索引记录数和页开销
		rowCount := sc.estimateRowCount(table)
		totalIndexSize += indexSize * rowCount * 12 / 10 // 增加20%的页开销
	}
	return totalIndexSize
}

// 后台更新任务

// backgroundUpdateWorker 后台更新工作器
func (sc *StatisticsCollector) backgroundUpdateWorker() {
	ticker := time.NewTicker(sc.config.AutoUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sc.stopCh:
			return
		case <-ticker.C:
			sc.performPeriodicUpdate()
		case req := <-sc.updateCh:
			sc.handleUpdateRequest(req)
		}
	}
}

// performPeriodicUpdate 执行周期性更新
func (sc *StatisticsCollector) performPeriodicUpdate() {
	sc.mu.RLock()
	expiredTables := make([]string, 0)
	expiredColumns := make([]string, 0)
	expiredIndexes := make([]string, 0)

	now := time.Now()

	// 检查过期的表统计信息
	for key, stats := range sc.tableStats {
		if now.Sub(time.Unix(stats.LastAnalyzeTime, 0)) > sc.config.ExpirationTime {
			expiredTables = append(expiredTables, key)
		}
	}

	// 检查过期的列统计信息（基于对应表的LastAnalyzeTime）
	for key := range sc.columnStats {
		// 从key中提取表名（格式为 "tableName.columnName"）
		parts := strings.Split(key, ".")
		if len(parts) >= 2 {
			tableName := parts[0]
			if tableStats, exists := sc.tableStats[tableName]; exists {
				if now.Sub(time.Unix(tableStats.LastAnalyzeTime, 0)) > sc.config.ExpirationTime {
					expiredColumns = append(expiredColumns, key)
				}
			}
		}
	}

	// 检查过期的索引统计信息（基于对应表的LastAnalyzeTime）
	for key := range sc.indexStats {
		// 从key中提取表名（格式为 "tableName.indexName"）
		parts := strings.Split(key, ".")
		if len(parts) >= 2 {
			tableName := parts[0]
			if tableStats, exists := sc.tableStats[tableName]; exists {
				if now.Sub(time.Unix(tableStats.LastAnalyzeTime, 0)) > sc.config.ExpirationTime {
					expiredIndexes = append(expiredIndexes, key)
				}
			}
		}
	}
	sc.mu.RUnlock()

	// 清理过期统计信息
	if len(expiredTables) > 0 || len(expiredColumns) > 0 || len(expiredIndexes) > 0 {
		sc.mu.Lock()
		for _, key := range expiredTables {
			delete(sc.tableStats, key)
		}
		for _, key := range expiredColumns {
			delete(sc.columnStats, key)
		}
		for _, key := range expiredIndexes {
			delete(sc.indexStats, key)
		}
		sc.mu.Unlock()
	}
}

// handleUpdateRequest 处理更新请求
func (sc *StatisticsCollector) handleUpdateRequest(req *StatisticsUpdateRequest) {
	// 实际实现中，这里会触发相应的统计信息更新
	// 现在只是简单地从缓存中删除，强制下次重新收集
	sc.mu.Lock()
	defer sc.mu.Unlock()

	switch req.UpdateType {
	case UpdateTypeTable:
		delete(sc.tableStats, req.TableName)
	case UpdateTypeColumn:
		key := fmt.Sprintf("%s.%s", req.TableName, req.ColumnName)
		delete(sc.columnStats, key)
	case UpdateTypeIndex:
		key := fmt.Sprintf("%s.%s", req.TableName, req.IndexName)
		delete(sc.indexStats, key)
	case UpdateTypeAll:
		// 清空所有相关统计信息
		for key := range sc.tableStats {
			if key == req.TableName {
				delete(sc.tableStats, key)
			}
		}
		for key := range sc.columnStats {
			if len(key) > len(req.TableName) && key[:len(req.TableName)] == req.TableName {
				delete(sc.columnStats, key)
			}
		}
		for key := range sc.indexStats {
			if len(key) > len(req.TableName) && key[:len(req.TableName)] == req.TableName {
				delete(sc.indexStats, key)
			}
		}
	}
}

// 公共接口方法

// GetTableStatistics 获取表统计信息
func (sc *StatisticsCollector) GetTableStatistics(tableName string) (*TableStats, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	stats, exists := sc.tableStats[tableName]
	return stats, exists
}

// GetColumnStatistics 获取列统计信息
func (sc *StatisticsCollector) GetColumnStatistics(tableName, columnName string) (*ColumnStats, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	key := fmt.Sprintf("%s.%s", tableName, columnName)
	stats, exists := sc.columnStats[key]
	return stats, exists
}

// GetIndexStatistics 获取索引统计信息
func (sc *StatisticsCollector) GetIndexStatistics(tableName, indexName string) (*IndexStats, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	key := fmt.Sprintf("%s.%s", tableName, indexName)
	stats, exists := sc.indexStats[key]
	return stats, exists
}

// RequestUpdate 请求更新统计信息
func (sc *StatisticsCollector) RequestUpdate(req *StatisticsUpdateRequest) {
	select {
	case sc.updateCh <- req:
	default:
		// 如果通道满了，忽略请求
	}
}

// Stop 停止统计信息收集器
func (sc *StatisticsCollector) Stop() {
	close(sc.stopCh)
}

// GetAllStatistics 获取所有统计信息
func (sc *StatisticsCollector) GetAllStatistics() (
	map[string]*TableStats,
	map[string]*ColumnStats,
	map[string]*IndexStats,
) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	// 复制映射以避免并发访问问题
	tableStats := make(map[string]*TableStats)
	columnStats := make(map[string]*ColumnStats)
	indexStats := make(map[string]*IndexStats)

	for k, v := range sc.tableStats {
		tableStats[k] = v
	}
	for k, v := range sc.columnStats {
		columnStats[k] = v
	}
	for k, v := range sc.indexStats {
		indexStats[k] = v
	}

	return tableStats, columnStats, indexStats
}
