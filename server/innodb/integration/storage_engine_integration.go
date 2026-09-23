package integration

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

// StorageEngineIntegrator 存储引擎集成器
// 负责查询优化器与InnoDB存储引擎的深度集成
type StorageEngineIntegrator struct {
	sync.RWMutex

	// 存储引擎组件
	storageManager     *manager.StorageManager
	spaceManager       basic.SpaceManager
	systemSpaceManager *manager.SystemSpaceManager
	bufferPoolManager  *manager.OptimizedBufferPoolManager
	btreeManager       basic.BPlusTreeManager

	// 查询优化器组件
	optimizerManager    *manager.OptimizerManager
	statisticsCollector *plan.StatisticsCollector
	costEstimator       *plan.CostEstimator
	indexOptimizer      *plan.IndexPushdownOptimizer
	enhancedStatistics  *plan.EnhancedStatisticsCollector
	storageAccessor     plan.StorageEngineAccessor

	// 集成状态
	isInitialized    bool
	integrationStats *IntegrationStats
}

// IntegrationStats 集成统计信息
type IntegrationStats struct {
	OptimizedQueries      uint64
	IndexPushdownCount    uint64
	StorageAccessCount    uint64
	CacheHitRate          float64
	AvgOptimizationTime   time.Duration
	TotalOptimizationTime time.Duration
}

// NewStorageEngineIntegrator 创建存储引擎集成器
func NewStorageEngineIntegrator(
	storageManager *manager.StorageManager,
	optimizerManager *manager.OptimizerManager,
) *StorageEngineIntegrator {
	integrator := &StorageEngineIntegrator{
		storageManager:   storageManager,
		optimizerManager: optimizerManager,
		isInitialized:    false,
		integrationStats: &IntegrationStats{},
	}

	// 初始化集成组件
	integrator.initializeIntegration()

	return integrator
}

// initializeIntegration 初始化集成组件
func (sei *StorageEngineIntegrator) initializeIntegration() {
	// 获取存储引擎组件
	sei.spaceManager = sei.storageManager.GetSpaceManager()
	sei.systemSpaceManager = sei.storageManager.GetSystemSpaceManager()
	sei.bufferPoolManager = sei.storageManager.GetBufferPoolManager()
	sei.btreeManager = sei.storageManager.GetBTreeManager()

	// 初始化查询优化器组件
	sei.initializeOptimizerComponents()

	// 建立集成连接
	sei.establishIntegrationConnections()

	sei.isInitialized = true
	logger.Info("存储引擎集成器初始化完成")
}

// initializeOptimizerComponents 初始化优化器组件
func (sei *StorageEngineIntegrator) initializeOptimizerComponents() {
	// 创建统计信息收集器
	statsConfig := &plan.StatisticsConfig{
		AutoUpdateInterval: 1 * time.Hour,
		SampleRate:         0.1,
		HistogramBuckets:   32,
		ExpirationTime:     24 * time.Hour,
		EnableAutoUpdate:   true,
	}
	sei.statisticsCollector = plan.NewStatisticsCollector(statsConfig)

	// 创建代价估算器
	sei.costEstimator = plan.NewCostEstimator(sei.statisticsCollector, plan.NewDefaultCostModel())

	// 创建索引下推优化器
	sei.indexOptimizer = plan.NewIndexPushdownOptimizer()
	sei.enhancedStatistics = plan.NewEnhancedStatisticsCollector(
		statsConfig,
		sei.spaceManager,
		sei.btreeManager,
	)
}

// establishIntegrationConnections 建立集成连接
func (sei *StorageEngineIntegrator) establishIntegrationConnections() {
	// 将存储引擎统计信息注入到优化器
	sei.injectStorageStatistics()

	// 配置优化器使用存储引擎接口
	sei.configureOptimizerStorageAccess()

	// 启动后台统计信息收集
	sei.startBackgroundStatisticsCollection()
}

// injectStorageStatistics 注入存储引擎统计信息
func (sei *StorageEngineIntegrator) injectStorageStatistics() {
	// 当前版本的 StatisticsCollector 不维护表空间统计信息，
	// 因此这里仅遍历现有表空间以确保接口调用正常。
	if spaces, err := sei.storageManager.ListSpaces(); err == nil {
		_ = spaces
	}
}

// configureOptimizerStorageAccess 配置优化器存储访问
func (sei *StorageEngineIntegrator) configureOptimizerStorageAccess() {
	// 设置统计收集器和代价估算器共用的存储访问接口。
	sei.storageAccessor = &StorageAccessor{
		storageManager:    sei.storageManager,
		spaceManager:      sei.spaceManager,
		bufferPoolManager: sei.bufferPoolManager,
		btreeManager:      sei.btreeManager,
	}
	sei.enhancedStatistics.SetStorageEngineAccessor(sei.storageAccessor)
	sei.costEstimator.SetStatisticsProvider(sei.enhancedStatistics)
}

// startBackgroundStatisticsCollection 启动后台统计信息收集
func (sei *StorageEngineIntegrator) startBackgroundStatisticsCollection() {
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			if err := sei.collectRuntimeStatistics(); err != nil {
				logger.Errorf("收集运行时统计信息失败: %v", err)
			}
		}
	}()
}

// collectRuntimeStatistics 收集运行时统计信息
func (sei *StorageEngineIntegrator) collectRuntimeStatistics() error {
	sei.Lock()
	defer sei.Unlock()

	// 收集缓冲池统计信息
	bufferStats := sei.bufferPoolManager.GetStatistics()
	sei.integrationStats.CacheHitRate = bufferStats.HitRate

	// 当前 BPlusTreeManager 接口未提供统计信息，
	// 因此这里只更新缓冲池命中率。

	return nil
}

// OptimizeQuery 优化查询
func (sei *StorageEngineIntegrator) OptimizeQuery(
	ctx context.Context,
	table *metadata.Table,
	whereConditions []plan.Expression,
	selectColumns []string,
) (*OptimizedQueryPlan, error) {
	startTime := time.Now()
	defer func() {
		sei.updateOptimizationStats(time.Since(startTime))
	}()

	if !sei.isInitialized {
		return nil, fmt.Errorf("存储引擎集成器未初始化")
	}

	// 1. 收集表统计信息
	tableStats, err := sei.collectTableStatistics(ctx, table)
	if err != nil {
		return nil, fmt.Errorf("收集表统计信息失败: %v", err)
	}
	if err := sei.refreshEnhancedStatistics(ctx, table); err != nil {
		return nil, fmt.Errorf("刷新增强统计信息失败: %v", err)
	}

	// 2. 索引下推优化
	indexCandidate, err := sei.optimizeIndexAccess(table, whereConditions, selectColumns)
	if err != nil {
		return nil, fmt.Errorf("索引优化失败: %v", err)
	}

	// 3. 代价估算
	costEstimate, err := sei.estimateQueryCost(table, indexCandidate, whereConditions)
	if err != nil {
		return nil, fmt.Errorf("代价估算失败: %v", err)
	}

	// 4. 生成优化后的查询计划
	optimizedPlan := &OptimizedQueryPlan{
		Table:            table,
		IndexCandidate:   indexCandidate,
		CostEstimate:     costEstimate,
		TableStats:       tableStats,
		AccessMethod:     sei.determineAccessMethod(indexCandidate),
		StorageHints:     sei.generateStorageHints(indexCandidate),
		OptimizationTime: time.Since(startTime),
	}

	return optimizedPlan, nil
}

// refreshEnhancedStatistics populates the same authoritative collector used
// by CostEstimator. Without this bridge the integration layer could collect
// real statistics for display while the cost model continued reading an empty
// legacy collector.
func (sei *StorageEngineIntegrator) refreshEnhancedStatistics(ctx context.Context, table *metadata.Table) error {
	if sei.enhancedStatistics == nil || table == nil {
		return nil
	}
	if _, err := sei.enhancedStatistics.CollectTableStatistics(ctx, table); err != nil {
		return err
	}
	for _, index := range table.Indices {
		if index == nil {
			continue
		}
		if _, err := sei.enhancedStatistics.CollectIndexStatistics(ctx, table, index); err != nil {
			return err
		}
	}
	for _, column := range table.Columns {
		if column == nil {
			continue
		}
		if _, err := sei.enhancedStatistics.CollectColumnStatistics(ctx, table, column); err != nil {
			return err
		}
	}
	return nil
}

// collectTableStatistics 收集表统计信息
func (sei *StorageEngineIntegrator) collectTableStatistics(
	ctx context.Context,
	table *metadata.Table,
) (*plan.TableStats, error) {
	// 从存储引擎获取实际的表统计信息
	spaceID := sei.getTableSpaceID(table)
	space, err := sei.spaceManager.GetSpace(spaceID)
	if err != nil {
		return nil, fmt.Errorf("获取表空间失败: %v", err)
	}

	rowCount, dataSize, indexSize, modifyCount := sei.resolveTableStatistics(table, spaceID, space)

	// 构建表统计信息
	tableStats := &plan.TableStats{
		TableName:       table.Name,
		RowCount:        rowCount,
		TotalSize:       dataSize + indexSize,
		ModifyCount:     modifyCount,
		LastAnalyzeTime: time.Now().Unix(),
	}

	return tableStats, nil
}

// optimizeIndexAccess 优化索引访问
func (sei *StorageEngineIntegrator) optimizeIndexAccess(
	table *metadata.Table,
	whereConditions []plan.Expression,
	selectColumns []string,
) (*plan.IndexCandidate, error) {
	// 设置统计信息到索引优化器
	sei.updateOptimizerStatistics(table)

	// 执行索引优化
	candidate, err := sei.indexOptimizer.OptimizeIndexAccess(table, whereConditions, selectColumns)
	if err != nil {
		return nil, err
	}

	if candidate != nil {
		sei.integrationStats.IndexPushdownCount++
	}

	return candidate, nil
}

// estimateQueryCost 估算查询代价
func (sei *StorageEngineIntegrator) estimateQueryCost(
	table *metadata.Table,
	indexCandidate *plan.IndexCandidate,
	whereConditions []plan.Expression,
) (*plan.CostEstimate, error) {
	if indexCandidate != nil {
		// 索引扫描代价
		return sei.costEstimator.EstimateIndexScanCost(
			table, indexCandidate.Index, indexCandidate.Selectivity, indexCandidate.Conditions)
	} else {
		// 全表扫描代价
		selectivity := sei.estimateSelectivity(table, whereConditions)
		return sei.costEstimator.EstimateTableScanCost(table, selectivity)
	}
}

// updateOptimizerStatistics 更新优化器统计信息
func (sei *StorageEngineIntegrator) updateOptimizerStatistics(table *metadata.Table) {
	if table == nil || sei.indexOptimizer == nil {
		return
	}
	// 收集表统计信息
	tableStats := make(map[string]*plan.TableStats)
	indexStats := make(map[string]*plan.IndexStats)
	columnStats := make(map[string]*plan.ColumnStats)

	// 从存储引擎获取实际统计信息
	spaceID := sei.getTableSpaceID(table)
	space, _ := sei.spaceManager.GetSpace(spaceID)

	if space != nil {
		rowCount, dataSize, indexSize, modifyCount := sei.resolveTableStatistics(table, spaceID, space)
		tableStats[table.Name] = &plan.TableStats{
			TableName:       table.Name,
			RowCount:        rowCount,
			TotalSize:       dataSize + indexSize,
			ModifyCount:     modifyCount,
			LastAnalyzeTime: time.Now().Unix(),
		}

		// 收集索引统计信息
		for _, index := range table.Indices {
			key := fmt.Sprintf("%s.%s", table.Name, index.Name)
			indexStats[key] = &plan.IndexStats{
				IndexName:     index.Name,
				Cardinality:   int64(sei.estimateIndexCardinality(space, index)),
				ClusterFactor: sei.estimateClusterFactor(space, index),
				PrefixLength:  sei.calculatePrefixLength(index),
				Selectivity:   sei.estimateIndexSelectivity(space, index),
			}
		}

		// 收集列统计信息
		for _, column := range table.Columns {
			key := fmt.Sprintf("%s.%s", table.Name, column.Name)
			columnStats[key] = &plan.ColumnStats{
				ColumnName:    column.Name,
				NotNullCount:  int64(sei.estimateNotNullCount(space, column)),
				DistinctCount: int64(sei.estimateDistinctCount(space, column)),
				NullCount:     int64(sei.estimateNullCount(space, column)),
				MinValue:      sei.getColumnMinValue(space, column),
				MaxValue:      sei.getColumnMaxValue(space, column),
			}
		}
	}

	// The enhanced collector is the authoritative source after ANALYZE and
	// after a persisted statistics reload. Overlay it on the compatibility
	// estimates above so index pushdown and CostEstimator make the same choice.
	if sei.enhancedStatistics != nil {
		if stats, err := sei.enhancedStatistics.CollectTableStatistics(context.Background(), table); err == nil && stats != nil {
			tableStats[table.Name] = stats
		} else if stats, ok := sei.enhancedStatistics.GetTableStatistics(table.Name); ok && stats != nil {
			tableStats[table.Name] = stats
		}
		for _, index := range table.Indices {
			if index == nil {
				continue
			}
			if stats, err := sei.enhancedStatistics.CollectIndexStatistics(context.Background(), table, index); err == nil && stats != nil {
				indexStats[fmt.Sprintf("%s.%s", table.Name, index.Name)] = stats
			} else if stats, ok := sei.enhancedStatistics.GetIndexStatistics(table.Name, index.Name); ok && stats != nil {
				indexStats[fmt.Sprintf("%s.%s", table.Name, index.Name)] = stats
			}
		}
		for _, column := range table.Columns {
			if column == nil {
				continue
			}
			if stats, err := sei.enhancedStatistics.CollectColumnStatistics(context.Background(), table, column); err == nil && stats != nil {
				columnStats[fmt.Sprintf("%s.%s", table.Name, column.Name)] = stats
			} else if stats, ok := sei.enhancedStatistics.GetColumnStatistics(table.Name, column.Name); ok && stats != nil {
				columnStats[fmt.Sprintf("%s.%s", table.Name, column.Name)] = stats
			}
		}
	}

	// 设置统计信息到优化器
	sei.indexOptimizer.SetStatistics(tableStats, indexStats, columnStats)
}

// 辅助方法
func (sei *StorageEngineIntegrator) getTableSpaceID(table *metadata.Table) uint32 {
	if table == nil || sei.spaceManager == nil {
		if table == nil || sei.storageAccessor == nil {
			return 0
		}
	}
	if resolver, ok := sei.storageAccessor.(interface {
		GetTableSpaceID(schemaName, tableName string) (uint32, error)
	}); ok {
		schemaName := ""
		if table.Schema != nil {
			schemaName = table.Schema.Name
		}
		if spaceID, err := resolver.GetTableSpaceID(schemaName, table.Name); err == nil && spaceID > 0 {
			return spaceID
		}
	}
	if sei.spaceManager == nil {
		return 0
	}

	ts, err := sei.spaceManager.GetTableSpaceByName(table.Name)
	if err != nil {
		return 0
	}

	return ts.GetSpaceId()
}

func (sei *StorageEngineIntegrator) estimateRowCount(space basic.Space) uint64 {
	// 基于页面数量估算行数
	pageCount := space.GetPageCount()
	avgRowsPerPage := uint64(100) // 假设每页平均100行
	return uint64(pageCount) * avgRowsPerPage
}

// resolveTableStatistics prefers the decoded storage-engine counters and
// retains page metadata only as an explicit compatibility fallback. Keeping
// this in one place prevents ANALYZE and index optimization from observing
// different row/size values for the same table.
func (sei *StorageEngineIntegrator) resolveTableStatistics(
	table *metadata.Table,
	spaceID uint32,
	space basic.Space,
) (rowCount, dataSize, indexSize, modifyCount int64) {
	if space != nil {
		rowCount = int64(sei.estimateRowCount(space))
		dataSize = int64(space.GetUsedSpace())
	}
	if sei.storageAccessor == nil {
		return rowCount, dataSize, indexSize, modifyCount
	}
	if exactRows, err := sei.storageAccessor.GetTableRowCount(spaceID); err == nil && exactRows >= 0 {
		rowCount = exactRows
	}
	if exactData, exactIndex, err := sei.storageAccessor.GetTableSpaceSize(spaceID); err == nil && exactData >= 0 && exactIndex >= 0 {
		dataSize, indexSize = exactData, exactIndex
	}
	if provider, ok := sei.storageAccessor.(interface {
		GetTableModifyCount(schemaName, tableName string) (int64, error)
	}); ok {
		schemaName := ""
		if table != nil && table.Schema != nil {
			schemaName = table.Schema.Name
		}
		if count, err := provider.GetTableModifyCount(schemaName, table.Name); err == nil && count >= 0 {
			modifyCount = count
		}
	}
	return rowCount, dataSize, indexSize, modifyCount
}

func (sei *StorageEngineIntegrator) estimateIndexCardinality(space basic.Space, index *metadata.Index) uint64 {
	if index != nil && index.Stats != nil && index.Stats.Cardinality > 0 {
		return uint64(index.Stats.Cardinality)
	}
	// 简化实现，基于空间大小估算
	return sei.estimateRowCount(space) / 2
}

func (sei *StorageEngineIntegrator) estimateClusterFactor(space basic.Space, index *metadata.Index) float64 {
	// 聚簇因子估算
	if index.IsPrimary {
		return 1.0
	}
	return 1.5
}

func (sei *StorageEngineIntegrator) calculatePrefixLength(index *metadata.Index) int {
	// 计算索引前缀长度：基于列的最大字符长度估算
	if index == nil || index.Table == nil {
		return 0
	}

	totalLength := 0
	for _, colName := range index.Columns {
		if col, ok := index.Table.GetColumn(colName); ok {
			totalLength += col.CharMaxLength
		}
	}
	return totalLength
}

func (sei *StorageEngineIntegrator) estimateIndexSelectivity(space basic.Space, index *metadata.Index) float64 {
	cardinality := sei.estimateIndexCardinality(space, index)
	rowCount := sei.estimateRowCount(space)
	if rowCount == 0 {
		return 1.0
	}
	return float64(cardinality) / float64(rowCount)
}

func (sei *StorageEngineIntegrator) estimateNotNullCount(space basic.Space, column *metadata.Column) uint64 {
	rowCount := sei.estimateRowCount(space)
	if column != nil && !column.IsNullable {
		return rowCount
	}
	return rowCount * 95 / 100 // 假设95%非空
}

func (sei *StorageEngineIntegrator) estimateDistinctCount(space basic.Space, column *metadata.Column) uint64 {
	rowCount := sei.estimateRowCount(space)
	return rowCount * 80 / 100 // 假设80%唯一值
}

func (sei *StorageEngineIntegrator) estimateNullCount(space basic.Space, column *metadata.Column) uint64 {
	if column != nil && !column.IsNullable {
		return 0
	}
	rowCount := sei.estimateRowCount(space)
	return rowCount * 5 / 100 // 假设5%为空
}

func (sei *StorageEngineIntegrator) getColumnMinValue(space basic.Space, column *metadata.Column) interface{} {
	// 简化实现，返回类型默认最小值
	if column == nil {
		return nil
	}

	switch column.DataType {
	case metadata.TypeInt, metadata.TypeBigInt:
		return int64(1)
	case metadata.TypeVarchar, metadata.TypeText:
		return "A"
	default:
		return nil
	}
}

func (sei *StorageEngineIntegrator) getColumnMaxValue(space basic.Space, column *metadata.Column) interface{} {
	// 简化实现，返回类型默认最大值
	if column == nil {
		return nil
	}

	switch column.DataType {
	case metadata.TypeInt, metadata.TypeBigInt:
		return int64(1000000)
	case metadata.TypeVarchar, metadata.TypeText:
		return "ZZZZ"
	default:
		return nil
	}
}

func (sei *StorageEngineIntegrator) estimateSelectivity(table *metadata.Table, whereConditions []plan.Expression) float64 {
	if len(whereConditions) == 0 {
		return 1.0
	}
	selectivity := 1.0
	for _, condition := range whereConditions {
		selectivity *= sei.estimatePredicateSelectivity(table, condition)
	}
	return math.Max(0, math.Min(1, selectivity))
}

func (sei *StorageEngineIntegrator) estimatePredicateSelectivity(table *metadata.Table, expression plan.Expression) float64 {
	binaryExpression, ok := expression.(*plan.BinaryOperation)
	if !ok || binaryExpression == nil {
		return 0.1
	}
	if binaryExpression.Op == plan.OpAnd {
		return sei.estimatePredicateSelectivity(table, binaryExpression.Left) *
			sei.estimatePredicateSelectivity(table, binaryExpression.Right)
	}
	if binaryExpression.Op == plan.OpOr {
		left := sei.estimatePredicateSelectivity(table, binaryExpression.Left)
		right := sei.estimatePredicateSelectivity(table, binaryExpression.Right)
		return left + right - left*right
	}
	column, ok := binaryExpression.Left.(*plan.Column)
	if !ok || column == nil || table == nil || sei.enhancedStatistics == nil {
		return 0.1
	}
	columnName := column.Name
	if dot := strings.LastIndex(columnName, "."); dot >= 0 {
		columnName = columnName[dot+1:]
	}
	stats, exists := sei.enhancedStatistics.GetColumnStatistics(table.Name, columnName)
	if !exists || stats == nil {
		return 0.1
	}
	rowCount := int64(0)
	if tableStats, ok := sei.enhancedStatistics.GetTableStatistics(table.Name); ok && tableStats != nil {
		rowCount = tableStats.RowCount
	}
	notNullRatio := 1.0
	if rowCount > 0 {
		notNullRatio = math.Min(1, float64(stats.NotNullCount)/float64(rowCount))
	}
	ndv := math.Max(1, float64(stats.DistinctCount))
	switch binaryExpression.Op {
	case plan.OpEQ:
		return notNullRatio / ndv
	case plan.OpNE:
		return notNullRatio * math.Max(0, 1-1/ndv)
	case plan.OpLT, plan.OpLE, plan.OpGT, plan.OpGE:
		return notNullRatio / 3
	case plan.OpLike, plan.OpIn:
		return math.Min(notNullRatio, 0.1)
	default:
		return 0.1
	}
}

func (sei *StorageEngineIntegrator) determineAccessMethod(candidate *plan.IndexCandidate) AccessMethod {
	if candidate != nil {
		if candidate.CoverIndex {
			return AccessMethodCoveringIndex
		}
		return AccessMethodIndexScan
	}
	return AccessMethodTableScan
}

func (sei *StorageEngineIntegrator) generateStorageHints(candidate *plan.IndexCandidate) *StorageHints {
	hints := &StorageHints{
		UseIndex:       candidate != nil,
		PrefetchPages:  true,
		BufferPoolHint: "NORMAL",
		ReadAheadPages: 4,
	}

	if candidate != nil {
		hints.IndexName = candidate.Index.Name
		hints.UseIndexOnly = candidate.CoverIndex
	}

	return hints
}

func (sei *StorageEngineIntegrator) updateOptimizationStats(duration time.Duration) {
	sei.Lock()
	defer sei.Unlock()

	sei.integrationStats.OptimizedQueries++
	sei.integrationStats.TotalOptimizationTime += duration
	sei.integrationStats.AvgOptimizationTime =
		sei.integrationStats.TotalOptimizationTime / time.Duration(sei.integrationStats.OptimizedQueries)
}

// GetIntegrationStats 获取集成统计信息
func (sei *StorageEngineIntegrator) GetIntegrationStats() *IntegrationStats {
	sei.RLock()
	defer sei.RUnlock()

	// 返回统计信息副本
	stats := *sei.integrationStats
	return &stats
}

// Close 关闭集成器
func (sei *StorageEngineIntegrator) Close() error {
	sei.Lock()
	defer sei.Unlock()

	if sei.statisticsCollector != nil {
		sei.statisticsCollector.Stop()
	}

	sei.isInitialized = false
	logger.Info("存储引擎集成器已关闭")
	return nil
}

// OptimizedQueryPlan 优化后的查询计划
type OptimizedQueryPlan struct {
	Table            *metadata.Table
	IndexCandidate   *plan.IndexCandidate
	CostEstimate     *plan.CostEstimate
	TableStats       *plan.TableStats
	AccessMethod     AccessMethod
	StorageHints     *StorageHints
	OptimizationTime time.Duration
}

// AccessMethod 访问方法
type AccessMethod int

const (
	AccessMethodTableScan AccessMethod = iota
	AccessMethodIndexScan
	AccessMethodCoveringIndex
)

func (am AccessMethod) String() string {
	switch am {
	case AccessMethodTableScan:
		return "TABLE_SCAN"
	case AccessMethodIndexScan:
		return "INDEX_SCAN"
	case AccessMethodCoveringIndex:
		return "COVERING_INDEX"
	default:
		return "UNKNOWN"
	}
}

// StorageHints 存储提示
type StorageHints struct {
	UseIndex       bool
	IndexName      string
	UseIndexOnly   bool
	PrefetchPages  bool
	BufferPoolHint string
	ReadAheadPages int
}

// StorageAccessor 存储访问器
type StorageAccessor struct {
	storageManager    *manager.StorageManager
	spaceManager      basic.SpaceManager
	bufferPoolManager *manager.OptimizedBufferPoolManager
	btreeManager      basic.BPlusTreeManager
}

// GetTableRowCount returns the exact number of decodable clustered records
// when the table-to-storage mapping and table metadata are available.
func (sa *StorageAccessor) GetTableRowCount(spaceID uint32) (int64, error) {
	records, err := sa.SampleTableRecords(spaceID, 1)
	if err != nil {
		return 0, err
	}
	return int64(len(records)), nil
}

// GetTableSpaceID resolves the canonical storage mapping used by ANALYZE and
// the optimizer. The name-based hash fallback in the planner is retained only
// for accessors that do not implement this optional resolver.
func (sa *StorageAccessor) GetTableSpaceID(schemaName, tableName string) (uint32, error) {
	if sa == nil || sa.storageManager == nil {
		return 0, fmt.Errorf("storage manager is unavailable")
	}
	tableStorage := sa.storageManager.GetTableStorageManager()
	if tableStorage == nil {
		return 0, fmt.Errorf("table storage mapping is unavailable")
	}
	info, err := tableStorage.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return 0, err
	}
	if info == nil || info.SpaceID == 0 {
		return 0, fmt.Errorf("table storage mapping has no space id for %s.%s", schemaName, tableName)
	}
	return info.SpaceID, nil
}

// GetIndexID resolves a logical index name to the physical IndexManager ID.
// It is intentionally an optional planner-side capability: older accessors
// can continue to provide table/row statistics without implementing it.
func (sa *StorageAccessor) GetIndexID(schemaName, tableName, indexName string) (uint32, error) {
	if sa == nil || sa.storageManager == nil {
		return 0, fmt.Errorf("storage manager is unavailable")
	}
	indexManager := sa.storageManager.GetIndexManager()
	if indexManager == nil {
		return 0, fmt.Errorf("index manager is unavailable")
	}
	tableID := manager.SecondaryIndexTableID(schemaName, tableName)
	index := indexManager.GetIndexByName(tableID, indexName)
	if index == nil {
		for _, candidate := range indexManager.ListIndexes(tableID) {
			if candidate != nil && strings.EqualFold(candidate.Name, indexName) {
				index = candidate
				break
			}
		}
	}
	if index == nil || index.IndexID == 0 || index.IndexID > uint64(^uint32(0)) {
		return 0, fmt.Errorf("index %s.%s.%s is unavailable", schemaName, tableName, indexName)
	}
	return uint32(index.IndexID), nil
}

// SampleTableRecords scans clustered records through the same decoder used by
// the execution engine, then applies a deterministic rate-based sample. This
// keeps ANALYZE column values tied to persisted rows rather than generated
// placeholders.
func (sa *StorageAccessor) SampleTableRecords(spaceID uint32, sampleRate float64) ([][]interface{}, error) {
	if sa == nil || sa.storageManager == nil {
		return nil, fmt.Errorf("storage manager is unavailable")
	}
	if sampleRate <= 0 {
		sampleRate = 1
	}
	tableStorage := sa.storageManager.GetTableStorageManager()
	tableManager := sa.storageManager.GetTableManager()
	if tableStorage == nil || tableManager == nil {
		return nil, fmt.Errorf("table storage mapping or table manager is unavailable")
	}
	info, err := tableStorage.GetTableBySpaceID(spaceID)
	if err != nil {
		return nil, err
	}
	tableMeta, err := tableManager.GetTableMetadata(context.Background(), info.SchemaName, info.TableName)
	if err != nil {
		return nil, err
	}
	btree, err := tableStorage.CreateBTreeManagerForTable(context.Background(), info.SchemaName, info.TableName)
	if err != nil {
		return nil, err
	}
	decoded, err := engine.NewClusteredIndexScanner(btree, tableMeta).Scan(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	if len(decoded) == 0 {
		return [][]interface{}{}, nil
	}
	sampleCount := int(math.Ceil(float64(len(decoded)) * sampleRate))
	if sampleCount < 1 {
		sampleCount = 1
	}
	if sampleCount > len(decoded) {
		sampleCount = len(decoded)
	}
	result := make([][]interface{}, 0, sampleCount)
	for i := 0; i < sampleCount; i++ {
		rowIndex := i * len(decoded) / sampleCount
		values := make([]interface{}, 0, len(tableMeta.Columns))
		for _, column := range tableMeta.Columns {
			if column == nil {
				values = append(values, nil)
				continue
			}
			values = append(values, decoded[rowIndex].ColumnValues[column.Name])
		}
		result = append(result, values)
	}
	return result, nil
}

func (sa *StorageAccessor) GetIndexCardinality(indexID uint32) (int64, error) {
	if sa == nil || sa.storageManager == nil || sa.storageManager.GetIndexManager() == nil {
		return 0, fmt.Errorf("index manager is unavailable")
	}
	stats, err := sa.storageManager.GetIndexManager().GetIndexStats(uint64(indexID))
	if err != nil {
		return 0, err
	}
	return int64(stats.KeyCount), nil
}

func (sa *StorageAccessor) GetTableSpaceSize(spaceID uint32) (dataSize int64, indexSize int64, err error) {
	if sa == nil || sa.spaceManager == nil {
		return 0, 0, fmt.Errorf("space manager is unavailable")
	}
	space, err := sa.spaceManager.GetSpace(spaceID)
	if err != nil {
		return 0, 0, err
	}
	return int64(space.GetUsedSpace()), 0, nil
}

func (sa *StorageAccessor) GetBTreeStatistics(indexID uint32) (treeDepth int, leafPages int64, nonLeafPages int64, err error) {
	if sa == nil || sa.storageManager == nil || sa.storageManager.GetIndexManager() == nil {
		return 0, 0, 0, fmt.Errorf("index manager is unavailable")
	}
	stats, err := sa.storageManager.GetIndexManager().GetIndexStats(uint64(indexID))
	if err != nil {
		return 0, 0, 0, err
	}
	return int(stats.Height), int64(stats.LeafPages), int64(stats.NonLeafPages), nil
}

// GetSpaceStatistics 获取空间统计信息
func (sa *StorageAccessor) GetSpaceStatistics(spaceID uint32) (*plan.SpaceStatistics, error) {
	space, err := sa.spaceManager.GetSpace(spaceID)
	if err != nil {
		return nil, err
	}

	return &plan.SpaceStatistics{
		SpaceID:     space.ID(),
		PageCount:   space.GetPageCount(),
		ExtentCount: space.GetExtentCount(),
		UsedSpace:   space.GetUsedSpace(),
	}, nil
}

// GetBufferPoolStatistics 获取缓冲池统计信息
func (sa *StorageAccessor) GetBufferPoolStatistics() *manager.BufferPoolStatistics {
	return sa.bufferPoolManager.GetStatistics()
}
