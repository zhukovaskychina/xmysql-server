package plan

import (
	"fmt"
	"math"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// CostEstimator 代价估算器
type CostEstimator struct {
	// 统计信息收集器
	statsCollector *StatisticsCollector
	statsProvider  StatisticsProvider

	// 代价模型参数
	costModel *CostModel
}

// StatisticsProvider is the common read-only contract implemented by both
// the legacy and enhanced statistics collectors.
type StatisticsProvider interface {
	GetTableStatistics(tableName string) (*TableStats, bool)
	GetColumnStatistics(tableName, columnName string) (*ColumnStats, bool)
	GetIndexStatistics(tableName, indexName string) (*IndexStats, bool)
}

// NewCostEstimator 创建代价估算器
func NewCostEstimator(statsCollector *StatisticsCollector, costModel *CostModel) *CostEstimator {
	if costModel == nil {
		costModel = NewDefaultCostModel()
	}

	return &CostEstimator{
		statsCollector: statsCollector,
		statsProvider:  statsCollector,
		costModel:      costModel,
	}
}

// SetStatisticsProvider switches cost estimation to an authoritative
// statistics source. Passing nil restores the collector supplied at creation.
func (ce *CostEstimator) SetStatisticsProvider(provider StatisticsProvider) {
	if provider == nil {
		ce.statsProvider = ce.statsCollector
		return
	}
	ce.statsProvider = provider
}

func (ce *CostEstimator) statisticsProvider() StatisticsProvider {
	if ce.statsProvider != nil {
		return ce.statsProvider
	}
	return ce.statsCollector
}

func (ce *CostEstimator) requireStatisticsProvider() (StatisticsProvider, error) {
	provider := ce.statisticsProvider()
	if provider == nil {
		return nil, fmt.Errorf("统计信息提供者未配置")
	}
	return provider, nil
}

// EstimateTableScanCost 估算表扫描代价
func (ce *CostEstimator) EstimateTableScanCost(
	table *metadata.Table,
	selectivity float64,
) (*CostEstimate, error) {
	// 获取表统计信息
	provider, err := ce.requireStatisticsProvider()
	if err != nil {
		return nil, err
	}
	tableStats, exists := provider.GetTableStatistics(table.Name)
	if !exists {
		return nil, fmt.Errorf("表 %s 的统计信息不存在", table.Name)
	}

	// 计算需要读取的页数
	avgRowSize := ce.estimateAvgRowSize(table, tableStats)
	rowsPerPage := float64(16384) / avgRowSize // 假设页大小为16KB
	totalPages := math.Ceil(float64(tableStats.RowCount) / rowsPerPage)
	if tableStats.DataLength > 0 {
		// Prefer measured data size when ANALYZE or the storage accessor has
		// populated it. This accounts for row format, variable-length columns,
		// NULL bitmap and page overhead more accurately than metadata guesses.
		totalPages = math.Ceil(float64(tableStats.DataLength) / 16384.0)
	}
	if totalPages < 1 && tableStats.RowCount > 0 {
		totalPages = 1
	}

	// 计算I/O代价
	ioCost := totalPages * ce.costModel.DiskReadCost

	// 计算CPU代价
	cpuCost := float64(tableStats.RowCount) * ce.costModel.CPUTupleCost

	// 计算选择性影响
	outputRows := float64(tableStats.RowCount) * selectivity

	return &CostEstimate{
		IOCost:      ioCost,
		CPUCost:     cpuCost,
		TotalCost:   ioCost + cpuCost,
		OutputRows:  int64(outputRows),
		Selectivity: selectivity,
	}, nil
}

// EstimateIndexScanCost 估算索引扫描代价
func (ce *CostEstimator) EstimateIndexScanCost(
	table *metadata.Table,
	index *metadata.Index,
	selectivity float64,
	conditions []*IndexCondition,
) (*CostEstimate, error) {
	// 获取表和索引统计信息
	provider, err := ce.requireStatisticsProvider()
	if err != nil {
		return nil, err
	}
	tableStats, exists := provider.GetTableStatistics(table.Name)
	if !exists {
		return nil, fmt.Errorf("表 %s 的统计信息不存在", table.Name)
	}

	indexStats, exists := provider.GetIndexStatistics(table.Name, index.Name)
	if !exists {
		return nil, fmt.Errorf("索引 %s.%s 的统计信息不存在", table.Name, index.Name)
	}

	// 计算索引扫描代价
	indexScanCost := ce.calculateIndexScanCost(indexStats, selectivity, conditions)

	// 计算回表代价（如果需要）
	lookupCost := 0.0
	if !ce.isCoveringIndex(index, table) {
		lookupCost = ce.calculateLookupCost(tableStats, selectivity)
	}

	// 计算CPU代价
	cpuCost := float64(tableStats.RowCount) * selectivity * ce.costModel.CPUIndexCost

	// 计算输出行数
	outputRows := float64(tableStats.RowCount) * selectivity

	return &CostEstimate{
		IOCost:      indexScanCost + lookupCost,
		CPUCost:     cpuCost,
		TotalCost:   indexScanCost + lookupCost + cpuCost,
		OutputRows:  int64(outputRows),
		Selectivity: selectivity,
	}, nil
}

// EstimateJoinCost 估算连接代价
func (ce *CostEstimator) EstimateJoinCost(
	leftTable *metadata.Table,
	rightTable *metadata.Table,
	joinType JoinType,
	joinConditions []Expression,
) (*CostEstimate, error) {
	// 获取左右表统计信息
	provider, err := ce.requireStatisticsProvider()
	if err != nil {
		return nil, err
	}
	leftStats, exists := provider.GetTableStatistics(leftTable.Name)
	if !exists {
		return nil, fmt.Errorf("左表 %s 的统计信息不存在", leftTable.Name)
	}

	rightStats, exists := provider.GetTableStatistics(rightTable.Name)
	if !exists {
		return nil, fmt.Errorf("右表 %s 的统计信息不存在", rightTable.Name)
	}

	// 根据连接类型选择算法
	switch joinType {
	case JoinTypeInner:
		return ce.estimateNestedLoopJoinCost(leftStats, rightStats, joinConditions)
	case JoinTypeLeft:
		return ce.estimateHashJoinCost(leftStats, rightStats, joinConditions)
	case JoinTypeRight:
		return ce.estimateSortMergeJoinCost(leftStats, rightStats, joinConditions)
	default:
		return ce.estimateNestedLoopJoinCost(leftStats, rightStats, joinConditions)
	}
}

// EstimateAggregationCost 估算聚合代价
func (ce *CostEstimator) EstimateAggregationCost(
	inputRows int64,
	groupByColumns []string,
	aggregateFunctions []string,
) (*CostEstimate, error) {
	// 估算分组数量
	groupCount := ce.estimateGroupCount(inputRows, len(groupByColumns))

	// 计算排序代价（如果需要）
	sortCost := 0.0
	if len(groupByColumns) > 0 {
		sortCost = ce.estimateSortCost(inputRows)
	}

	// 计算聚合计算代价
	aggCost := float64(inputRows) * float64(len(aggregateFunctions)) * ce.costModel.CPUOperatorCost

	// 计算哈希表代价
	hashCost := float64(groupCount) * ce.costModel.MemoryTupleCost

	totalCost := sortCost + aggCost + hashCost

	return &CostEstimate{
		IOCost:      0,
		CPUCost:     totalCost,
		TotalCost:   totalCost,
		OutputRows:  groupCount,
		Selectivity: float64(groupCount) / float64(inputRows),
	}, nil
}

// EstimateSortCost 估算排序代价
func (ce *CostEstimator) EstimateSortCost(
	inputRows int64,
	sortColumns []string,
) (*CostEstimate, error) {
	sortCost := ce.estimateSortCost(inputRows)

	return &CostEstimate{
		IOCost:      0,
		CPUCost:     sortCost,
		TotalCost:   sortCost,
		OutputRows:  inputRows,
		Selectivity: 1.0,
	}, nil
}

// 辅助方法

// calculateIndexScanCost 计算索引扫描代价
func (ce *CostEstimator) calculateIndexScanCost(
	indexStats *IndexStats,
	selectivity float64,
	conditions []*IndexCondition,
) float64 {
	// 基础索引扫描代价
	baseCost := ce.costModel.DiskSeekCost
	if indexStats.LeafPages > 0 {
		// A measured B+Tree size is more useful than a fixed seek-only cost.
		// Even a highly selective lookup must touch at least one leaf page.
		pagesToRead := math.Ceil(float64(indexStats.LeafPages) * math.Max(0.01, selectivity))
		pagesToRead = math.Max(1, pagesToRead)
		baseCost += pagesToRead * ce.costModel.DiskReadCost
	}

	// 根据选择性调整代价
	selectivityFactor := math.Max(0.01, selectivity)

	// 根据索引类型调整
	indexTypeFactor := 1.0
	if indexStats.Selectivity > 0.9 { // 高选择性索引
		indexTypeFactor = 0.5
	} else if indexStats.Selectivity < 0.1 { // 低选择性索引
		indexTypeFactor = 2.0
	}
	if indexStats.ClusterFactor > 1 {
		indexTypeFactor *= math.Min(4, indexStats.ClusterFactor)
	}

	// 根据条件数量调整
	conditionFactor := 1.0 + float64(len(conditions))*0.1

	return baseCost * selectivityFactor * indexTypeFactor * conditionFactor
}

// calculateLookupCost 计算回表代价
func (ce *CostEstimator) calculateLookupCost(
	tableStats *TableStats,
	selectivity float64,
) float64 {
	// 需要回表的行数
	lookupRows := float64(tableStats.RowCount) * selectivity

	// 假设回表的随机I/O代价
	return lookupRows * ce.costModel.DiskSeekCost * 0.1
}

// isCoveringIndex 检查是否为覆盖索引
func (ce *CostEstimator) isCoveringIndex(index *metadata.Index, table *metadata.Table) bool {
	// 简化实现：假设主键索引总是覆盖索引
	return index.IsPrimary
}

// estimateNestedLoopJoinCost 估算嵌套循环连接代价
func (ce *CostEstimator) estimateNestedLoopJoinCost(
	leftStats *TableStats,
	rightStats *TableStats,
	joinConditions []Expression,
) (*CostEstimate, error) {
	// 外表扫描代价
	outerCost := float64(leftStats.RowCount) * ce.costModel.CPUTupleCost

	// 内表扫描代价（对每个外表行都要扫描内表）
	innerCost := float64(leftStats.RowCount) * float64(rightStats.RowCount) * ce.costModel.CPUTupleCost

	// 连接条件计算代价
	joinCost := float64(leftStats.RowCount) * float64(rightStats.RowCount) *
		float64(len(joinConditions)) * ce.costModel.CPUOperatorCost

	selectivity := ce.estimateJoinSelectivity(leftStats, rightStats, joinConditions)
	outputRows := int64(float64(leftStats.RowCount) * float64(rightStats.RowCount) * selectivity)

	totalCost := outerCost + innerCost + joinCost

	return &CostEstimate{
		IOCost:      0,
		CPUCost:     totalCost,
		TotalCost:   totalCost,
		OutputRows:  outputRows,
		Selectivity: selectivity,
	}, nil
}

// estimateHashJoinCost 估算哈希连接代价
func (ce *CostEstimator) estimateHashJoinCost(
	leftStats *TableStats,
	rightStats *TableStats,
	joinConditions []Expression,
) (*CostEstimate, error) {
	// 选择较小的表作为构建端（build side）
	var buildRows, probeRows float64
	var buildStats, probeStats *TableStats

	if leftStats.RowCount <= rightStats.RowCount {
		buildRows = float64(leftStats.RowCount)
		probeRows = float64(rightStats.RowCount)
		buildStats = leftStats
		probeStats = rightStats
	} else {
		buildRows = float64(rightStats.RowCount)
		probeRows = float64(leftStats.RowCount)
		buildStats = rightStats
		probeStats = leftStats
	}

	// 1. 构建哈希表代价
	// 1.1 读取构建端数据
	buildReadCost := buildRows * ce.costModel.CPUTupleCost

	// 1.2 计算哈希值
	buildHashCost := buildRows * ce.costModel.CPUOperatorCost

	// 1.3 插入哈希表
	buildInsertCost := buildRows * ce.costModel.CPUOperatorCost

	buildCost := buildReadCost + buildHashCost + buildInsertCost

	// 2. 探测代价
	// 2.1 读取探测端数据
	probeReadCost := probeRows * ce.costModel.CPUTupleCost

	// 2.2 计算哈希值并查找
	probeLookupCost := probeRows * ce.costModel.CPUOperatorCost * 2.0

	probeCost := probeReadCost + probeLookupCost

	// 3. 内存代价（哈希表）
	// 估算每行占用内存（包括键、值和指针）
	avgRowSize := float64(buildStats.AvgRowLength)
	if avgRowSize == 0 {
		avgRowSize = 100.0 // 默认100字节
	}
	hashTableMemory := buildRows * avgRowSize
	memoryCost := hashTableMemory * ce.costModel.MemoryTupleCost

	// 4. 连接条件评估代价
	joinCondCost := 0.0
	if len(joinConditions) > 0 {
		// 估算匹配的行数（使用选择率）
		selectivity := ce.estimateJoinSelectivity(buildStats, probeStats, joinConditions)
		matchedPairs := buildRows * probeRows * selectivity
		joinCondCost = matchedPairs * float64(len(joinConditions)) * ce.costModel.CPUOperatorCost
	}

	// 5. 估算输出行数
	selectivity := ce.estimateJoinSelectivity(buildStats, probeStats, joinConditions)
	outputRows := int64(buildRows * probeRows * selectivity)

	totalCost := buildCost + probeCost + memoryCost + joinCondCost

	return &CostEstimate{
		IOCost:      0,
		CPUCost:     totalCost,
		TotalCost:   totalCost,
		OutputRows:  outputRows,
		Selectivity: selectivity,
	}, nil
}

// estimateSortMergeJoinCost 估算排序合并连接代价
func (ce *CostEstimator) estimateSortMergeJoinCost(
	leftStats *TableStats,
	rightStats *TableStats,
	joinConditions []Expression,
) (*CostEstimate, error) {
	// 左表排序代价
	leftSortCost := ce.estimateSortCost(leftStats.RowCount)

	// 右表排序代价
	rightSortCost := ce.estimateSortCost(rightStats.RowCount)

	// 合并代价
	mergeCost := float64(leftStats.RowCount+rightStats.RowCount) * ce.costModel.CPUTupleCost

	selectivity := ce.estimateJoinSelectivity(leftStats, rightStats, joinConditions)
	outputRows := int64(float64(leftStats.RowCount) * float64(rightStats.RowCount) * selectivity)

	totalCost := leftSortCost + rightSortCost + mergeCost

	return &CostEstimate{
		IOCost:      0,
		CPUCost:     totalCost,
		TotalCost:   totalCost,
		OutputRows:  outputRows,
		Selectivity: selectivity,
	}, nil
}

// estimateSortCost 估算排序代价
func (ce *CostEstimator) estimateSortCost(rows int64) float64 {
	if rows <= 1 {
		return 0
	}

	// 使用 O(n log n) 复杂度
	return float64(rows) * math.Log2(float64(rows)) * ce.costModel.CPUTupleCost
}

// estimateGroupCount 估算分组数量
func (ce *CostEstimator) estimateGroupCount(inputRows int64, groupByColumns int) int64 {
	if groupByColumns == 0 {
		return 1 // 没有GROUP BY，只有一个组
	}

	// 简化估算：假设每个分组列减少50%的重复
	factor := math.Pow(0.5, float64(groupByColumns))
	groupCount := float64(inputRows) * factor

	// 至少有1个组，最多不超过输入行数
	return int64(math.Max(1, math.Min(float64(inputRows), groupCount)))
}

// estimateAvgRowSize 估算平均行大小
func (ce *CostEstimator) estimateAvgRowSize(table *metadata.Table, tableStats *TableStats) float64 {
	if tableStats != nil {
		if tableStats.AvgRowLength > 0 {
			return float64(tableStats.AvgRowLength)
		}
		if tableStats.DataLength > 0 && tableStats.RowCount > 0 {
			return float64(tableStats.DataLength) / float64(tableStats.RowCount)
		}
		if tableStats.TotalSize > 0 && tableStats.RowCount > 0 {
			return float64(tableStats.TotalSize) / float64(tableStats.RowCount)
		}
	}

	totalSize := 0.0

	for _, col := range table.Columns {
		switch col.DataType {
		case metadata.TypeInt:
			totalSize += 4
		case metadata.TypeBigInt:
			totalSize += 8
		case metadata.TypeVarchar:
			totalSize += float64(col.CharMaxLength) * 0.5 // 假设平均使用一半长度
		case metadata.TypeText:
			totalSize += 1000 // 假设平均1KB
		case metadata.TypeDateTime:
			totalSize += 8
		default:
			totalSize += 10
		}
	}

	// 加上行头开销
	return math.Max(1, totalSize+20)
}

// CostEstimate 代价估算结果
type CostEstimate struct {
	IOCost      float64 // I/O代价
	CPUCost     float64 // CPU代价
	TotalCost   float64 // 总代价
	OutputRows  int64   // 输出行数
	Selectivity float64 // 选择性
}

// JoinType 连接类型
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
)

// String 返回连接类型的字符串表示
func (jt JoinType) String() string {
	switch jt {
	case JoinTypeInner:
		return "INNER"
	case JoinTypeLeft:
		return "LEFT"
	case JoinTypeRight:
		return "RIGHT"
	case JoinTypeFull:
		return "FULL"
	default:
		return "UNKNOWN"
	}
}

// CompareCosts 比较两个代价估算结果
func CompareCosts(cost1, cost2 *CostEstimate) int {
	if cost1.TotalCost < cost2.TotalCost {
		return -1
	} else if cost1.TotalCost > cost2.TotalCost {
		return 1
	}
	return 0
}

// GetCostRatio 获取代价比率
func GetCostRatio(cost1, cost2 *CostEstimate) float64 {
	if cost2.TotalCost == 0 {
		return math.Inf(1)
	}
	return cost1.TotalCost / cost2.TotalCost
}

// estimateJoinSelectivity 估算连接选择率
func (ce *CostEstimator) estimateJoinSelectivity(
	leftStats *TableStats,
	rightStats *TableStats,
	joinConditions []Expression,
) float64 {
	if len(joinConditions) == 0 {
		// 笛卡尔积
		return 1.0
	}

	selectivity := 1.0

	for _, cond := range joinConditions {
		condSelectivity := ce.estimateConditionSelectivity(cond, leftStats, rightStats)
		selectivity *= condSelectivity
	}

	// 确保选择率在合理范围内
	selectivity = math.Max(0.0, math.Min(1.0, selectivity))

	return selectivity
}

// estimateConditionSelectivity 估算单个条件的选择率
func (ce *CostEstimator) estimateConditionSelectivity(
	cond Expression,
	leftStats *TableStats,
	rightStats *TableStats,
) float64 {
	operation, left, right, ok := binaryJoinCondition(cond)
	if !ok {
		return 0.1
	}
	if operation == OpAnd {
		return ce.estimateJoinSelectivity(leftStats, rightStats, []Expression{left}) *
			ce.estimateJoinSelectivity(leftStats, rightStats, []Expression{right})
	}
	if operation == OpOr {
		leftSelectivity := ce.estimateConditionSelectivity(left, leftStats, rightStats)
		rightSelectivity := ce.estimateConditionSelectivity(right, leftStats, rightStats)
		return leftSelectivity + rightSelectivity - leftSelectivity*rightSelectivity
	}

	leftColumn, leftOK := left.(*Column)
	rightColumn, rightOK := right.(*Column)
	if leftOK && rightOK {
		leftNDV := ce.joinColumnNDV(leftStats.TableName, leftColumn.Name, leftStats.RowCount)
		rightNDV := ce.joinColumnNDV(rightStats.TableName, rightColumn.Name, rightStats.RowCount)
		if operation == OpEQ {
			ndv := math.Max(leftNDV, rightNDV)
			if ndv <= 0 {
				return 0.1
			}
			selectivity := 1.0 / ndv
			selectivity *= ce.joinColumnNotNullRatio(leftStats.TableName, leftColumn.Name, leftStats.RowCount)
			selectivity *= ce.joinColumnNotNullRatio(rightStats.TableName, rightColumn.Name, rightStats.RowCount)
			return selectivity
		}
		if operation == OpLT || operation == OpLE || operation == OpGT || operation == OpGE {
			return 1.0 / 3.0
		}
	}

	return 0.1
}

func binaryJoinCondition(expr Expression) (BinaryOp, Expression, Expression, bool) {
	binary, ok := expr.(*BinaryOperation)
	if !ok || binary == nil {
		return 0, nil, nil, false
	}
	operation := binary.Op
	if strings.TrimSpace(binary.Operator) != "" {
		switch strings.ToUpper(strings.TrimSpace(binary.Operator)) {
		case "=":
			operation = OpEQ
		case "!=", "<>":
			operation = OpNE
		case "<":
			operation = OpLT
		case "<=":
			operation = OpLE
		case ">":
			operation = OpGT
		case ">=":
			operation = OpGE
		case "AND":
			operation = OpAnd
		case "OR":
			operation = OpOr
		}
	}
	return operation, binary.Left, binary.Right, true
}

func (ce *CostEstimator) joinColumnNDV(tableName, columnName string, rowCount int64) float64 {
	if provider := ce.statisticsProvider(); provider != nil {
		if stats, ok := provider.GetColumnStatistics(tableName, strings.Trim(strings.TrimSpace(columnName), "`")); ok && stats != nil && stats.DistinctCount > 0 {
			return float64(stats.DistinctCount)
		}
	}
	if rowCount <= 0 {
		return 1
	}
	return math.Sqrt(float64(rowCount))
}

func (ce *CostEstimator) joinColumnNotNullRatio(tableName, columnName string, rowCount int64) float64 {
	provider := ce.statisticsProvider()
	if rowCount <= 0 || provider == nil {
		return 1
	}
	stats, ok := provider.GetColumnStatistics(tableName, strings.Trim(strings.TrimSpace(columnName), "`"))
	if !ok || stats == nil || stats.NotNullCount <= 0 {
		return 1
	}
	return math.Max(0, math.Min(1, float64(stats.NotNullCount)/float64(rowCount)))
}

// ChooseBestJoinAlgorithm 选择最佳连接算法
func (ce *CostEstimator) ChooseBestJoinAlgorithm(
	leftStats *TableStats,
	rightStats *TableStats,
	joinConditions []Expression,
) (string, *CostEstimate, error) {
	// 估算三种连接算法的代价

	// 1. 嵌套循环连接
	nestedLoopCost, err := ce.estimateNestedLoopJoinCost(leftStats, rightStats, joinConditions)
	if err != nil {
		return "", nil, err
	}

	// 2. 哈希连接
	hashJoinCost, err := ce.estimateHashJoinCost(leftStats, rightStats, joinConditions)
	if err != nil {
		return "", nil, err
	}

	// 3. 排序合并连接
	sortMergeJoinCost, err := ce.estimateSortMergeJoinCost(leftStats, rightStats, joinConditions)
	if err != nil {
		return "", nil, err
	}

	// 选择代价最小的算法
	bestAlgorithm := "NESTED_LOOP"
	bestCost := nestedLoopCost

	if hashJoinCost.TotalCost < bestCost.TotalCost {
		bestAlgorithm = "HASH_JOIN"
		bestCost = hashJoinCost
	}

	if sortMergeJoinCost.TotalCost < bestCost.TotalCost {
		bestAlgorithm = "SORT_MERGE_JOIN"
		bestCost = sortMergeJoinCost
	}

	return bestAlgorithm, bestCost, nil
}

// ChooseAggregateAlgorithm 选择最佳聚合算法
func (ce *CostEstimator) ChooseAggregateAlgorithm(
	inputRows int64,
	groupByColumns []string,
	aggregateFunctions []string,
) (string, *CostEstimate, error) {
	// 估算分组数（简化：假设为输入行数的平方根）
	groupCount := int64(math.Sqrt(float64(inputRows)))
	if groupCount == 0 {
		groupCount = 1
	}

	// 1. 哈希聚合代价
	hashAggCost := ce.estimateHashAggregateCost(inputRows, groupCount, aggregateFunctions)

	// 2. 排序聚合代价
	sortAggCost := ce.estimateSortAggregateCost(inputRows, groupCount, aggregateFunctions)

	// 选择代价最小的算法
	if hashAggCost.TotalCost < sortAggCost.TotalCost {
		return "HASH_AGGREGATE", hashAggCost, nil
	}
	return "SORT_AGGREGATE", sortAggCost, nil
}

// estimateHashAggregateCost 估算哈希聚合代价
func (ce *CostEstimator) estimateHashAggregateCost(
	inputRows int64,
	groupCount int64,
	aggregateFunctions []string,
) *CostEstimate {
	// 1. 读取输入数据
	readCost := float64(inputRows) * ce.costModel.CPUTupleCost

	// 2. 计算哈希值并分组
	hashCost := float64(inputRows) * ce.costModel.CPUOperatorCost

	// 3. 聚合计算
	aggCost := float64(inputRows) * float64(len(aggregateFunctions)) * ce.costModel.CPUOperatorCost

	// 4. 哈希表内存代价
	memoryCost := float64(groupCount) * ce.costModel.MemoryTupleCost

	totalCost := readCost + hashCost + aggCost + memoryCost

	return &CostEstimate{
		IOCost:      0,
		CPUCost:     totalCost,
		TotalCost:   totalCost,
		OutputRows:  groupCount,
		Selectivity: float64(groupCount) / float64(inputRows),
	}
}

// estimateSortAggregateCost 估算排序聚合代价
func (ce *CostEstimator) estimateSortAggregateCost(
	inputRows int64,
	groupCount int64,
	aggregateFunctions []string,
) *CostEstimate {
	// 1. 排序代价
	sortCost := ce.estimateSortCost(inputRows)

	// 2. 读取排序后的数据
	readCost := float64(inputRows) * ce.costModel.CPUTupleCost

	// 3. 聚合计算（顺序扫描，每组只计算一次）
	aggCost := float64(groupCount) * float64(len(aggregateFunctions)) * ce.costModel.CPUOperatorCost

	totalCost := sortCost + readCost + aggCost

	return &CostEstimate{
		IOCost:      0,
		CPUCost:     totalCost,
		TotalCost:   totalCost,
		OutputRows:  groupCount,
		Selectivity: float64(groupCount) / float64(inputRows),
	}
}
