package plan

import "math"

// CostModel 代价模型
type CostModel struct {
	// IO相关参数
	DiskSeekCost  float64 // 磁盘寻道代价
	DiskReadCost  float64 // 磁盘读取代价(每页)
	DiskWriteCost float64 // 磁盘写入代价(每页)
	NetworkCost   float64 // 网络传输代价(每字节)

	// CPU相关参数
	CPUOperatorCost float64 // CPU运算代价(每次)
	CPUTupleCost    float64 // CPU处理元组代价(每个)
	CPUIndexCost    float64 // CPU索引扫描代价(每次)
	CPUEvalCost     float64 // CPU表达式求值代价(每次)

	// 内存相关参数
	MemoryTupleCost float64 // 内存中处理元组代价(每个)
	MemoryHashCost  float64 // 内存哈希表代价(每个)
	MemorySortCost  float64 // 内存排序代价(每个)

	// 缓存相关参数
	BufferHitRatio float64 // 缓存命中率
	BufferSize     int64   // 缓存大小(页)
}

// NewDefaultCostModel 创建默认代价模型
func NewDefaultCostModel() *CostModel {
	return &CostModel{
		DiskSeekCost:  10.0,
		DiskReadCost:  1.0,
		DiskWriteCost: 2.0,
		NetworkCost:   0.01,

		CPUOperatorCost: 0.1,
		CPUTupleCost:    0.01,
		CPUIndexCost:    0.05,
		CPUEvalCost:     0.1,

		MemoryTupleCost: 0.001,
		MemoryHashCost:  0.1,
		MemorySortCost:  0.1,

		BufferHitRatio: 0.8,
		BufferSize:     1000,
	}
}

// Cost 计算物理计划的总代价
func (c *CostModel) Cost(plan PhysicalPlan) float64 {
	switch p := plan.(type) {
	case *PhysicalTableScan:
		return c.tableScanCost(p)
	case *PhysicalIndexScan:
		return c.indexScanCost(p)
	case *PhysicalHashJoin:
		return c.hashJoinCost(p)
	case *PhysicalMergeJoin:
		return c.mergeJoinCost(p)
	case *PhysicalHashAgg:
		return c.hashAggCost(p)
	case *PhysicalStreamAgg:
		return c.streamAggCost(p)
	case *PhysicalSort:
		return c.sortCost(p)
	case *PhysicalProjection:
		return c.projectionCost(p)
	case *PhysicalSelection:
		return c.selectionCost(p)
	default:
		return math.MaxFloat64
	}
}

// 各算子代价计算

func (c *CostModel) tableScanCost(p *PhysicalTableScan) float64 {
	// 1. IO代价
	pageCount := float64(p.Table.Stats.TotalSize) / float64(16*1024) // 假设页大小16KB
	ioCost := c.DiskSeekCost + pageCount*c.DiskReadCost*(1-c.BufferHitRatio)

	// 2. CPU代价
	rowCount := float64(p.Table.Stats.RowCount)
	cpuCost := rowCount * c.CPUTupleCost

	return ioCost + cpuCost
}

func (c *CostModel) indexScanCost(p *PhysicalIndexScan) float64 {
	// 1. 索引扫描IO代价
	//indexPages := float64(p.Index.Stats.Cardinality) / float64(100) // 假设每页100个索引项
	//indexIOCost := c.DiskSeekCost + indexPages*c.DiskReadCost*(1-c.BufferHitRatio)

	//// 2. 回表IO代价
	//selectivity := p.Index.Stats.Selectivity
	//tablePages := float64(p.Table.Stats.RowCount) * selectivity / float64(100) // 假设每页100行
	//tableIOCost := tablePages * c.DiskReadCost * (1 - c.BufferHitRatio)
	//
	//// 3. CPU代价
	//indexCPUCost := float64(p.Index.Stats.Cardinality) * c.CPUIndexCost
	//tableCPUCost := float64(p.Table.Stats.RowCount) * selectivity * c.CPUTupleCost
	//
	//return indexIOCost + tableIOCost + indexCPUCost + tableCPUCost
	return 0
}

func (c *CostModel) hashJoinCost(p *PhysicalHashJoin) float64 {
	// 1. 构建哈希表代价
	buildRows := float64(p.Children()[0].(*PhysicalTableScan).Table.Stats.RowCount)
	buildCost := buildRows * (c.MemoryHashCost + c.CPUTupleCost)

	// 2. 探测代价
	probeRows := float64(p.Children()[1].(*PhysicalTableScan).Table.Stats.RowCount)
	probeCost := probeRows * (c.CPUTupleCost + c.MemoryTupleCost)

	// 3. 输出代价
	outputRows := buildRows * probeRows * 0.1 // 假设选择率0.1
	outputCost := outputRows * c.CPUTupleCost

	return buildCost + probeCost + outputCost
}

func (c *CostModel) mergeJoinCost(p *PhysicalMergeJoin) float64 {
	// 1. 排序代价
	leftRows := float64(p.Children()[0].(*PhysicalTableScan).Table.Stats.RowCount)
	rightRows := float64(p.Children()[1].(*PhysicalTableScan).Table.Stats.RowCount)
	sortCost := (leftRows + rightRows) * c.MemorySortCost

	// 2. 合并代价
	mergeCost := (leftRows + rightRows) * c.CPUTupleCost

	// 3. 输出代价
	outputRows := leftRows * rightRows * 0.1 // 假设选择率0.1
	outputCost := outputRows * c.CPUTupleCost

	return sortCost + mergeCost + outputCost
}

func (c *CostModel) hashAggCost(p *PhysicalHashAgg) float64 {
	// 1. 构建哈希表代价
	inputRows := float64(p.Children()[0].(*PhysicalTableScan).Table.Stats.RowCount)
	buildCost := inputRows * (c.MemoryHashCost + c.CPUTupleCost)

	// 2. 聚合计算代价
	groupCount := float64(len(p.GroupByItems))
	aggCount := float64(len(p.AggFuncs))
	aggCost := inputRows * (groupCount + aggCount) * c.CPUEvalCost

	return buildCost + aggCost
}

func (c *CostModel) streamAggCost(p *PhysicalStreamAgg) float64 {
	// 1. 排序代价(如果需要)
	inputRows := float64(p.Children()[0].(*PhysicalTableScan).Table.Stats.RowCount)
	sortCost := inputRows * c.MemorySortCost

	// 2. 聚合计算代价
	groupCount := float64(len(p.GroupByItems))
	aggCount := float64(len(p.AggFuncs))
	aggCost := inputRows * (groupCount + aggCount) * c.CPUEvalCost

	return sortCost + aggCost
}

func (c *CostModel) sortCost(p *PhysicalSort) float64 {
	inputRows := float64(p.Children()[0].(*PhysicalTableScan).Table.Stats.RowCount)
	return inputRows * c.MemorySortCost
}

func (c *CostModel) projectionCost(p *PhysicalProjection) float64 {
	inputRows := float64(p.Children()[0].(*PhysicalTableScan).Table.Stats.RowCount)
	exprCount := float64(len(p.Exprs))
	return inputRows * exprCount * c.CPUEvalCost
}

func (c *CostModel) selectionCost(p *PhysicalSelection) float64 {
	inputRows := float64(p.Children()[0].(*PhysicalTableScan).Table.Stats.RowCount)
	condCount := float64(len(p.Conditions))
	return inputRows * condCount * c.CPUEvalCost
}
