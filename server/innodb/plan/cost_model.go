package plan

import (
	"math"
)

// CostModel 成本模型
// 用于估算查询执行的成本，包括IO成本和CPU成本
type CostModel struct {
	// IO成本参数
	SeqPageReadCost    float64 // 顺序读取页面成本
	RandomPageReadCost float64 // 随机读取页面成本
	PageWriteCost      float64 // 页面写入成本

	// CPU成本参数
	RowEvalCost    float64 // 行评估成本
	ComparisonCost float64 // 比较操作成本
	TupleCost      float64 // 元组处理成本
	OperatorCost   float64 // 操作符执行成本

	// 内存成本参数
	MemoryAllocCost float64 // 内存分配成本
	HashTableCost   float64 // 哈希表构建成本
	SortCost        float64 // 排序成本因子

	// 网络成本参数
	NetworkTupleCost float64 // 网络传输元组成本

	// 兼容旧字段
	DiskReadCost    float64 // 磁盘读成本（兼容）
	DiskSeekCost    float64 // 磁盘寻道成本（兼容）
	CPUTupleCost    float64 // CPU元组成本（兼容）
	CPUIndexCost    float64 // CPU索引成本（兼容）
	CPUOperatorCost float64 // CPU操作符成本（兼容）
	MemoryTupleCost float64 // 内存元组成本（兼容）
}

// DefaultCostModel 默认成本模型
// 基于MySQL和PostgreSQL的经验值
var DefaultCostModel = &CostModel{
	// IO成本（单位：毫秒）
	SeqPageReadCost:    1.0,  // 顺序读一个页面约1ms
	RandomPageReadCost: 4.0,  // 随机读一个页面约4ms
	PageWriteCost:      10.0, // 写一个页面约10ms

	// CPU成本（单位：微秒）
	RowEvalCost:    0.01,  // 评估一行约0.01ms
	ComparisonCost: 0.002, // 一次比较约0.002ms
	TupleCost:      0.01,  // 处理一个元组约0.01ms
	OperatorCost:   0.005, // 执行一个操作符约0.005ms

	// 内存成本
	MemoryAllocCost: 0.001, // 内存分配约0.001ms
	HashTableCost:   0.02,  // 哈希表构建约0.02ms per entry
	SortCost:        0.05,  // 排序因子

	// 网络成本
	NetworkTupleCost: 0.1, // 网络传输一个元组约0.1ms

	// 兼容旧字段
	DiskReadCost:    1.0,
	DiskSeekCost:    4.0,
	CPUTupleCost:    0.01,
	CPUIndexCost:    0.005,
	CPUOperatorCost: 0.005,
	MemoryTupleCost: 0.001,
}

// NewDefaultCostModel 创建默认成本模型
func NewDefaultCostModel() *CostModel {
	return DefaultCostModel
}

// QueryCost 查询成本
type QueryCost struct {
	IOCost      float64 // IO成本
	CPUCost     float64 // CPU成本
	MemoryCost  float64 // 内存成本
	NetworkCost float64 // 网络成本
	TotalCost   float64 // 总成本
	Cardinality int64   // 结果集基数（行数）
}

// Add 累加成本
func (c *QueryCost) Add(other *QueryCost) {
	c.IOCost += other.IOCost
	c.CPUCost += other.CPUCost
	c.MemoryCost += other.MemoryCost
	c.NetworkCost += other.NetworkCost
	c.TotalCost = c.IOCost + c.CPUCost + c.MemoryCost + c.NetworkCost
}

// Multiply 成本乘以倍数
func (c *QueryCost) Multiply(factor float64) {
	c.IOCost *= factor
	c.CPUCost *= factor
	c.MemoryCost *= factor
	c.NetworkCost *= factor
	c.TotalCost *= factor
}

// CompareTo 比较成本
// 返回值：-1表示c < other，0表示相等，1表示c > other
func (c *QueryCost) CompareTo(other *QueryCost) int {
	diff := c.TotalCost - other.TotalCost
	if math.Abs(diff) < 0.001 { // 误差范围内认为相等
		return 0
	}
	if diff < 0 {
		return -1
	}
	return 1
}

// EstimateSeqScanCost 估算全表顺序扫描成本
func (cm *CostModel) EstimateSeqScanCost(numPages int64, numRows int64) *QueryCost {
	cost := &QueryCost{
		Cardinality: numRows,
	}

	// IO成本：顺序读取所有页面
	cost.IOCost = float64(numPages) * cm.SeqPageReadCost

	// CPU成本：处理所有行
	cost.CPUCost = float64(numRows) * cm.RowEvalCost

	cost.TotalCost = cost.IOCost + cost.CPUCost
	return cost
}

// EstimateIndexScanCost 估算索引扫描成本
func (cm *CostModel) EstimateIndexScanCost(
	indexPages int64, // 索引页面数
	dataPages int64, // 数据页面数
	numRows int64, // 扫描行数
	selectivity float64, // 选择率
) *QueryCost {
	cost := &QueryCost{
		Cardinality: int64(float64(numRows) * selectivity),
	}

	// 索引扫描的IO成本
	// 1. 索引页面读取（通常是顺序的）
	indexReadCost := float64(indexPages) * cm.SeqPageReadCost * selectivity

	// 2. 回表读取数据页（通常是随机的）
	// 假设不同行在不同页面上
	dataReadPages := math.Min(float64(dataPages), float64(cost.Cardinality))
	dataReadCost := dataReadPages * cm.RandomPageReadCost

	cost.IOCost = indexReadCost + dataReadCost

	// CPU成本：处理索引和数据行
	cost.CPUCost = float64(cost.Cardinality) * cm.RowEvalCost

	cost.TotalCost = cost.IOCost + cost.CPUCost
	return cost
}

// EstimateIndexOnlyScanCost 估算覆盖索引扫描成本（不需要回表）
func (cm *CostModel) EstimateIndexOnlyScanCost(
	indexPages int64,
	numRows int64,
	selectivity float64,
) *QueryCost {
	cost := &QueryCost{
		Cardinality: int64(float64(numRows) * selectivity),
	}

	// 只需要读取索引页面
	cost.IOCost = float64(indexPages) * cm.SeqPageReadCost * selectivity

	// CPU成本
	cost.CPUCost = float64(cost.Cardinality) * cm.RowEvalCost

	cost.TotalCost = cost.IOCost + cost.CPUCost
	return cost
}

// EstimateJoinCost 估算连接操作成本
func (cm *CostModel) EstimateJoinCost(
	leftCost *QueryCost,
	rightCost *QueryCost,
	joinType string,
	joinSelectivity float64,
) *QueryCost {
	cost := &QueryCost{}

	// 累加输入成本
	cost.Add(leftCost)
	cost.Add(rightCost)

	// 连接本身的成本
	switch joinType {
	case "NESTED_LOOP":
		// 嵌套循环：外表每行都要扫描内表
		cost.CPUCost += float64(leftCost.Cardinality*rightCost.Cardinality) * cm.ComparisonCost
		cost.IOCost += float64(leftCost.Cardinality) * rightCost.IOCost

	case "HASH_JOIN":
		// 哈希连接：构建哈希表 + 探测
		cost.MemoryCost += float64(rightCost.Cardinality) * cm.HashTableCost
		cost.CPUCost += float64(leftCost.Cardinality+rightCost.Cardinality) * cm.RowEvalCost

	case "MERGE_JOIN":
		// 归并连接：需要排序
		sortCost := (float64(leftCost.Cardinality) + float64(rightCost.Cardinality)) *
			math.Log2(float64(leftCost.Cardinality+rightCost.Cardinality)) * cm.SortCost
		cost.CPUCost += sortCost
	}

	// 结果集基数
	cost.Cardinality = int64(float64(leftCost.Cardinality*rightCost.Cardinality) * joinSelectivity)

	cost.TotalCost = cost.IOCost + cost.CPUCost + cost.MemoryCost
	return cost
}

// EstimateSortCost 估算排序成本
func (cm *CostModel) EstimateSortCost(inputCost *QueryCost) *QueryCost {
	cost := &QueryCost{
		Cardinality: inputCost.Cardinality,
	}

	// 累加输入成本
	cost.Add(inputCost)

	// 排序成本：O(n log n)
	if inputCost.Cardinality > 0 {
		sortCPU := float64(inputCost.Cardinality) *
			math.Log2(float64(inputCost.Cardinality)) * cm.SortCost
		cost.CPUCost += sortCPU
	}

	// 如果数据量大，可能需要外部排序（IO成本）
	const memoryThreshold = 10000 // 假设内存可容纳1万行
	if inputCost.Cardinality > memoryThreshold {
		// 外部排序需要多次读写
		passes := math.Ceil(math.Log2(float64(inputCost.Cardinality) / memoryThreshold))
		cost.IOCost += passes * float64(inputCost.Cardinality) * cm.PageWriteCost / 100
	}

	cost.TotalCost = cost.IOCost + cost.CPUCost + cost.MemoryCost
	return cost
}

// EstimateGroupByCost 估算分组聚合成本
func (cm *CostModel) EstimateGroupByCost(
	inputCost *QueryCost,
	numGroups int64,
	useHashAgg bool,
) *QueryCost {
	cost := &QueryCost{
		Cardinality: numGroups,
	}

	// 累加输入成本
	cost.Add(inputCost)

	if useHashAgg {
		// 哈希聚合
		cost.MemoryCost += float64(numGroups) * cm.HashTableCost
		cost.CPUCost += float64(inputCost.Cardinality) * cm.RowEvalCost
	} else {
		// 排序聚合：先排序再分组
		sortCost := cm.EstimateSortCost(inputCost)
		cost.CPUCost += sortCost.CPUCost
		cost.IOCost += sortCost.IOCost
	}

	cost.TotalCost = cost.IOCost + cost.CPUCost + cost.MemoryCost
	return cost
}

// EstimateFilterCost 估算过滤操作成本
func (cm *CostModel) EstimateFilterCost(
	inputCost *QueryCost,
	numPredicates int,
	selectivity float64,
) *QueryCost {
	cost := &QueryCost{
		Cardinality: int64(float64(inputCost.Cardinality) * selectivity),
	}

	// 累加输入成本
	cost.Add(inputCost)

	// 过滤CPU成本：每行需要评估所有谓词
	cost.CPUCost += float64(inputCost.Cardinality*int64(numPredicates)) * cm.ComparisonCost

	cost.TotalCost = cost.IOCost + cost.CPUCost + cost.MemoryCost
	return cost
}

// CostComparator 成本比较器
type CostComparator struct {
	model *CostModel
}

// NewCostComparator 创建成本比较器
func NewCostComparator(model *CostModel) *CostComparator {
	return &CostComparator{model: model}
}

// SelectBest 选择成本最低的计划
func (cc *CostComparator) SelectBest(costs ...*QueryCost) *QueryCost {
	if len(costs) == 0 {
		return nil
	}

	best := costs[0]
	for i := 1; i < len(costs); i++ {
		if costs[i].CompareTo(best) < 0 {
			best = costs[i]
		}
	}

	return best
}

// CBOCostEstimator 成本估算器接口（CBO专用）
type CBOCostEstimator interface {
	EstimateCost(plan LogicalPlan) *QueryCost
}
