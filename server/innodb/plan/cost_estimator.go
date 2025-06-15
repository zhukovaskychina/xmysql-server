package plan

import (
	"fmt"
	"math"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// CostEstimator 代价估算器
type CostEstimator struct {
	// 统计信息收集器
	statsCollector *StatisticsCollector

	// 代价模型参数
	costModel *CostModel
}

// NewCostEstimator 创建代价估算器
func NewCostEstimator(statsCollector *StatisticsCollector, costModel *CostModel) *CostEstimator {
	if costModel == nil {
		costModel = NewDefaultCostModel()
	}

	return &CostEstimator{
		statsCollector: statsCollector,
		costModel:      costModel,
	}
}

// EstimateTableScanCost 估算表扫描代价
func (ce *CostEstimator) EstimateTableScanCost(
	table *metadata.Table,
	selectivity float64,
) (*CostEstimate, error) {
	// 获取表统计信息
	tableStats, exists := ce.statsCollector.GetTableStatistics(table.Name)
	if !exists {
		return nil, fmt.Errorf("表 %s 的统计信息不存在", table.Name)
	}

	// 计算需要读取的页数
	avgRowSize := ce.estimateAvgRowSize(table)
	rowsPerPage := float64(16384) / avgRowSize // 假设页大小为16KB
	totalPages := math.Ceil(float64(tableStats.RowCount) / rowsPerPage)

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
	tableStats, exists := ce.statsCollector.GetTableStatistics(table.Name)
	if !exists {
		return nil, fmt.Errorf("表 %s 的统计信息不存在", table.Name)
	}

	indexStats, exists := ce.statsCollector.GetIndexStatistics(table.Name, index.Name)
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
	leftStats, exists := ce.statsCollector.GetTableStatistics(leftTable.Name)
	if !exists {
		return nil, fmt.Errorf("左表 %s 的统计信息不存在", leftTable.Name)
	}

	rightStats, exists := ce.statsCollector.GetTableStatistics(rightTable.Name)
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

	// 根据选择性调整代价
	selectivityFactor := math.Max(0.01, selectivity)

	// 根据索引类型调整
	indexTypeFactor := 1.0
	if indexStats.Selectivity > 0.9 { // 高选择性索引
		indexTypeFactor = 0.5
	} else if indexStats.Selectivity < 0.1 { // 低选择性索引
		indexTypeFactor = 2.0
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

	// 估算输出行数（简化：假设10%的连接率）
	outputRows := int64(float64(leftStats.RowCount) * float64(rightStats.RowCount) * 0.1)

	totalCost := outerCost + innerCost + joinCost

	return &CostEstimate{
		IOCost:      0,
		CPUCost:     totalCost,
		TotalCost:   totalCost,
		OutputRows:  outputRows,
		Selectivity: 0.1,
	}, nil
}

// estimateHashJoinCost 估算哈希连接代价
func (ce *CostEstimator) estimateHashJoinCost(
	leftStats *TableStats,
	rightStats *TableStats,
	joinConditions []Expression,
) (*CostEstimate, error) {
	// 构建哈希表代价（较小的表）
	buildRows := math.Min(float64(leftStats.RowCount), float64(rightStats.RowCount))
	buildCost := buildRows * ce.costModel.CPUTupleCost

	// 探测代价（较大的表）
	probeRows := math.Max(float64(leftStats.RowCount), float64(rightStats.RowCount))
	probeCost := probeRows * ce.costModel.CPUTupleCost

	// 哈希计算代价
	hashCost := (buildRows + probeRows) * ce.costModel.CPUOperatorCost

	// 估算输出行数
	outputRows := int64(float64(leftStats.RowCount) * float64(rightStats.RowCount) * 0.1)

	totalCost := buildCost + probeCost + hashCost

	return &CostEstimate{
		IOCost:      0,
		CPUCost:     totalCost,
		TotalCost:   totalCost,
		OutputRows:  outputRows,
		Selectivity: 0.1,
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

	// 估算输出行数
	outputRows := int64(float64(leftStats.RowCount) * float64(rightStats.RowCount) * 0.1)

	totalCost := leftSortCost + rightSortCost + mergeCost

	return &CostEstimate{
		IOCost:      0,
		CPUCost:     totalCost,
		TotalCost:   totalCost,
		OutputRows:  outputRows,
		Selectivity: 0.1,
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
func (ce *CostEstimator) estimateAvgRowSize(table *metadata.Table) float64 {
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
	return totalSize + 20
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
