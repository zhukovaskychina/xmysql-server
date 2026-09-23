package plan

import (
	"context"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"math"
)

// PhysicalPlan 物理计划接口
type PhysicalPlan interface {
	Plan
	// Schema 返回计划的输出模式
	Schema() *metadata.DatabaseSchema
	// Children 返回子计划
	Children() []PhysicalPlan
	// SetChildren 设置子计划
	SetChildren(children []PhysicalPlan)
	// Cost 返回计划的代价估算
	Cost() float64
}

// PlanRowReader lets an engine adapter execute a physical child plan and
// return its rows to parallel operators without coupling the plan package to
// a particular storage executor.
type PlanRowReader func(context.Context, PhysicalPlan) ([][]interface{}, error)

// BasePhysicalPlan 基础物理计划实现
type BasePhysicalPlan struct {
	schema   *metadata.DatabaseSchema
	children []PhysicalPlan
	cost     float64
}

func (p *BasePhysicalPlan) Schema() *metadata.DatabaseSchema {
	return p.schema
}

// SetSchema lets the engine adapter preserve a logically pruned output
// schema when it materializes a physical scan in another package.
func (p *BasePhysicalPlan) SetSchema(schema *metadata.DatabaseSchema) {
	if p != nil {
		p.schema = schema
	}
}

func (p *BasePhysicalPlan) Children() []PhysicalPlan {
	return p.children
}

func (p *BasePhysicalPlan) SetChildren(children []PhysicalPlan) {
	p.children = children
}

func (p *BasePhysicalPlan) Cost() float64 {
	return p.cost
}

func (p *BasePhysicalPlan) GetEstimateBlocks() int64 {
	return 1 // 默认实现
}

func (p *BasePhysicalPlan) GetEstimateRows() int64 {
	return 100 // 默认实现
}

func (p *BasePhysicalPlan) GetPlanId() int {
	return 1 // 默认实现
}

func (p *BasePhysicalPlan) Scan(session server.MySQLServerSession) basic.Cursor {
	return nil // 默认实现，子类应该重写
}

func (p *BasePhysicalPlan) ToString() string {
	return "BasePhysicalPlan"
}

func (p *BasePhysicalPlan) GetExtraInfo() string {
	return ""
}

func (p *BasePhysicalPlan) GetPlanAccessType() string {
	return "ALL"
}

// PhysicalTableScan 表扫描物理计划
type PhysicalTableScan struct {
	BasePhysicalPlan
	Table           *metadata.Table
	RequiredColumns []string
	ChunkReader     TableChunkReader
}

// TableChunkReader reads one logical row range for a parallel table scan.
// The planner does not know the storage implementation, so the engine can
// provide a cancellation-aware reader while tests and lightweight callers can
// continue using the deterministic row-id fallback.
type TableChunkReader func(context.Context, DataChunk) ([][]interface{}, error)

func (p *PhysicalTableScan) GetEstimateBlocks() int64 {
	if p.Table.Stats != nil {
		return p.Table.Stats.RowCount / 100 // 假设每个块100行
	}
	return 1
}

func (p *PhysicalTableScan) GetEstimateRows() int64 {
	if p.Table.Stats != nil {
		return p.Table.Stats.RowCount
	}
	return 100
}

func (p *PhysicalTableScan) ToString() string {
	return "TableScan(" + p.Table.Name + ")"
}

func (p *PhysicalTableScan) GetPlanAccessType() string {
	return "ALL"
}

// PhysicalIndexScan 索引扫描物理计划
type PhysicalIndexScan struct {
	BasePhysicalPlan
	Table           *metadata.Table
	Index           *metadata.Index
	RequiredColumns []string
	ChunkReader     IndexChunkReader
}

// IndexChunkReader reads one logical row range for a parallel index scan.
// The storage adapter owns the index/range semantics; DataChunk only bounds
// the returned logical rows so the plan package stays storage-independent.
type IndexChunkReader func(context.Context, DataChunk) ([][]interface{}, error)

// RequiredColumnNames converts a pruned logical schema into the physical
// scan's source-column contract. A nil result means the source should return
// the full table row, preserving the existing adapter behavior.
func RequiredColumnNames(table *metadata.Table, schema *metadata.DatabaseSchema) []string {
	if table == nil || schema == nil {
		return nil
	}
	pruned, ok := schema.GetTable(table.Name)
	if !ok || pruned == nil || len(pruned.Columns) == 0 {
		return nil
	}
	columns := make([]string, 0, len(pruned.Columns))
	for _, column := range pruned.Columns {
		if column != nil {
			columns = append(columns, column.Name)
		}
	}
	return columns
}

func (p *PhysicalIndexScan) GetEstimateRows() int64 {
	if p != nil && p.Table != nil && p.Table.Stats != nil {
		return p.Table.Stats.RowCount
	}
	return 100
}

func (p *PhysicalIndexScan) GetEstimateBlocks() int64 {
	if p.Table.Stats != nil {
		return p.Table.Stats.RowCount / 1000 // 索引扫描更高效
	}
	return 1
}

// PhysicalHashJoin 哈希连接物理计划
type PhysicalHashJoin struct {
	BasePhysicalPlan
	JoinType     string
	Conditions   []Expression
	LeftSchema   *metadata.DatabaseSchema
	RightSchema  *metadata.DatabaseSchema
	HashJoinKeys []HashJoinKey
}

// HashJoinKey identifies the equi-join columns used by the parallel hash
// implementation. More complex predicates remain on the normal executor
// path.
type HashJoinKey struct {
	LeftIndex  int
	RightIndex int
}

func (p *PhysicalHashJoin) GetEstimateBlocks() int64 {
	return 10 // 连接操作的估计块数
}

// PhysicalMergeJoin 归并连接物理计划
type PhysicalMergeJoin struct {
	BasePhysicalPlan
	JoinType    string
	Conditions  []Expression
	LeftSchema  *metadata.DatabaseSchema
	RightSchema *metadata.DatabaseSchema
}

func (p *PhysicalMergeJoin) GetEstimateBlocks() int64 {
	return 5 // 归并连接通常更高效
}

// PhysicalHashAgg 哈希聚合物理计划
type PhysicalHashAgg struct {
	BasePhysicalPlan
	GroupByItems  []Expression
	AggFuncs      []AggregateFunc
	AggregateSpec *HashAggregateSpec
}

// HashAggregateSpec describes the row-oriented aggregate subset that can be
// executed by the parallel physical adapter without binding it to a storage
// record implementation.
type HashAggregateSpec struct {
	GroupByIndexes []int
	Functions      []HashAggregateFunction
}

type HashAggregateFunction struct {
	Name         string
	InputIndex   int // -1 means COUNT(*) when InputIndexes is empty
	InputIndexes []int
	Distinct     bool
	Separator    string
}

func (p *PhysicalHashAgg) GetEstimateBlocks() int64 {
	return 3 // 聚合操作的估计块数
}

// PhysicalStreamAgg 流式聚合物理计划
type PhysicalStreamAgg struct {
	BasePhysicalPlan
	GroupByItems []Expression
	AggFuncs     []AggregateFunc
}

func (p *PhysicalStreamAgg) GetEstimateBlocks() int64 {
	return 2 // 流式聚合更高效
}

// PhysicalSort 排序物理计划
type PhysicalSort struct {
	BasePhysicalPlan
	ByItems []ByItem
}

func (p *PhysicalSort) GetEstimateBlocks() int64 {
	return 5 // 排序操作的估计块数
}

// PhysicalProjection 投影物理计划
type PhysicalProjection struct {
	BasePhysicalPlan
	Exprs       []Expression
	OutputNames []string
	Distinct    bool
	OrderBy     []ByItem
	Offset      int64
	Limit       int64
	HasLimit    bool
}

func (p *PhysicalProjection) GetEstimateBlocks() int64 {
	return 1 // 投影操作开销很小
}

// PhysicalSelection 选择物理计划
type PhysicalSelection struct {
	BasePhysicalPlan
	Conditions []Expression
}

func (p *PhysicalSelection) GetEstimateBlocks() int64 {
	return 1 // 选择操作开销很小
}

// PhysicalValues emits one in-memory row for constant/no-FROM SELECTs.
// It is also the anchor source used by recursive CTE plans such as SELECT 1.
type PhysicalValues struct {
	BasePhysicalPlan
	Exprs []Expression
}

func (p *PhysicalValues) ToString() string { return "Values" }

type PhysicalUnion struct {
	BasePhysicalPlan
	UnionType string
	OrderBy   []ByItem
	Offset    int64
	Limit     int64
	HasLimit  bool
}

func (p *PhysicalUnion) ToString() string { return "Union(" + p.UnionType + ")" }

type PhysicalCTE struct {
	BasePhysicalPlan
	Name      string
	Columns   []string
	Recursive bool
}

func (p *PhysicalCTE) ToString() string { return "CTE(" + p.Name + ")" }

type PhysicalRecursiveCTE struct {
	BasePhysicalPlan
	Name    string
	Columns []string
}

func (p *PhysicalRecursiveCTE) ToString() string { return "RecursiveCTE(" + p.Name + ")" }

type PhysicalCTEScan struct {
	BasePhysicalPlan
	Name    string
	Columns []string
}

func (p *PhysicalCTEScan) ToString() string { return "CTEScan(" + p.Name + ")" }

type PhysicalCTEStatement struct {
	BasePhysicalPlan
	DefinitionCount int
}

func (p *PhysicalCTEStatement) ToString() string { return "CTEStatement" }

// ConvertToPhysicalPlan 将逻辑计划转换为物理计划
func ConvertToPhysicalPlan(logicalPlan LogicalPlan) PhysicalPlan {
	switch v := logicalPlan.(type) {
	case *LogicalTableScan:
		return &PhysicalTableScan{
			BasePhysicalPlan: BasePhysicalPlan{
				schema: v.Schema(),
				cost:   float64(v.Table.Stats.RowCount),
			},
			Table: v.Table,
		}
	case *LogicalIndexScan:
		// 将plan.Index转换为metadata.Index
		metadataIndex := &metadata.Index{
			Name:     v.Index.Name,
			Columns:  v.Index.Columns,
			IsUnique: v.Index.Unique,
		}
		return &PhysicalIndexScan{
			BasePhysicalPlan: BasePhysicalPlan{
				schema: v.Schema(),
				cost:   float64(v.Table.Stats.RowCount) * 0.3, // 索引扫描代价估算
			},
			Table: v.Table,
			Index: metadataIndex,
		}
	case *LogicalJoin:
		// 选择连接算法
		if shouldUseHashJoin(v) {
			physical := &PhysicalHashJoin{
				BasePhysicalPlan: BasePhysicalPlan{
					schema: v.Schema(),
					cost:   estimateHashJoinCost(v),
				},
				JoinType:    v.JoinType,
				Conditions:  v.Conditions,
				LeftSchema:  v.LeftSchema,
				RightSchema: v.RightSchema,
			}
			physical.SetChildren(convertPhysicalChildren(v.Children()))
			return physical
		}
		physical := &PhysicalMergeJoin{
			BasePhysicalPlan: BasePhysicalPlan{
				schema: v.Schema(),
				cost:   estimateMergeJoinCost(v),
			},
			JoinType:    v.JoinType,
			Conditions:  v.Conditions,
			LeftSchema:  v.LeftSchema,
			RightSchema: v.RightSchema,
		}
		physical.SetChildren(convertPhysicalChildren(v.Children()))
		return physical
	case *LogicalAggregation:
		// 选择聚合算法
		if shouldUseHashAgg(v) {
			physical := &PhysicalHashAgg{
				BasePhysicalPlan: BasePhysicalPlan{
					schema: v.Schema(),
					cost:   estimateHashAggCost(v),
				},
				GroupByItems: v.GroupByItems,
				AggFuncs:     v.AggFuncs,
			}
			physical.SetChildren(convertPhysicalChildren(v.Children()))
			return physical
		}
		physical := &PhysicalStreamAgg{
			BasePhysicalPlan: BasePhysicalPlan{
				schema: v.Schema(),
				cost:   estimateStreamAggCost(v),
			},
			GroupByItems: v.GroupByItems,
			AggFuncs:     v.AggFuncs,
		}
		physical.SetChildren(convertPhysicalChildren(v.Children()))
		return physical
	case *LogicalProjection:
		physical := &PhysicalProjection{
			BasePhysicalPlan: BasePhysicalPlan{
				schema: v.Schema(),
				cost:   0, // 投影代价很小，忽略不计
			},
			Exprs:       v.Exprs,
			OutputNames: v.OutputNames,
			Distinct:    v.Distinct,
			OrderBy:     v.OrderBy,
			Offset:      v.Offset,
			Limit:       v.Limit,
			HasLimit:    v.HasLimit,
		}
		physical.SetChildren(convertPhysicalChildren(v.Children()))
		return physical
	case *LogicalSelection:
		physical := &PhysicalSelection{
			BasePhysicalPlan: BasePhysicalPlan{
				schema: v.Schema(),
				cost:   0, // 选择代价很小，忽略不计
			},
			Conditions: v.Conditions,
		}
		physical.SetChildren(convertPhysicalChildren(v.Children()))
		return physical
	case *LogicalValues:
		return &PhysicalValues{
			BasePhysicalPlan: BasePhysicalPlan{schema: v.Schema(), cost: 1},
			Exprs:            v.Exprs,
		}
	case *LogicalUnion:
		children := make([]PhysicalPlan, 0, len(v.Children()))
		for _, child := range v.Children() {
			children = append(children, ConvertToPhysicalPlan(child))
		}
		return &PhysicalUnion{
			BasePhysicalPlan: BasePhysicalPlan{schema: v.Schema(), children: children, cost: float64(len(children))},
			UnionType:        v.UnionType,
			OrderBy:          v.OrderBy,
			Offset:           v.Offset,
			Limit:            v.Limit,
			HasLimit:         v.HasLimit,
		}
	case *LogicalCTE:
		var child PhysicalPlan
		if v.Query != nil {
			child = ConvertToPhysicalPlan(v.Query)
		}
		children := []PhysicalPlan{}
		if child != nil {
			children = append(children, child)
		}
		return &PhysicalCTE{
			BasePhysicalPlan: BasePhysicalPlan{schema: v.Schema(), children: children, cost: 1},
			Name:             v.Name,
			Columns:          v.Columns,
			Recursive:        v.Recursive,
		}
	case *LogicalRecursiveCTE:
		children := make([]PhysicalPlan, 0, 2)
		if v.Anchor != nil {
			children = append(children, ConvertToPhysicalPlan(v.Anchor))
		}
		if v.Recursive != nil {
			children = append(children, ConvertToPhysicalPlan(v.Recursive))
		}
		return &PhysicalRecursiveCTE{
			BasePhysicalPlan: BasePhysicalPlan{schema: v.Schema(), children: children, cost: 2},
			Name:             v.Name,
			Columns:          v.Columns,
		}
	case *LogicalCTEScan:
		return &PhysicalCTEScan{
			BasePhysicalPlan: BasePhysicalPlan{schema: v.Schema(), cost: 1},
			Name:             v.Name,
		}
	case *LogicalCTEStatement:
		children := make([]PhysicalPlan, 0, len(v.Children()))
		for _, child := range v.Children() {
			children = append(children, ConvertToPhysicalPlan(child))
		}
		return &PhysicalCTEStatement{
			BasePhysicalPlan: BasePhysicalPlan{schema: v.Schema(), children: children, cost: float64(len(children))},
			DefinitionCount:  len(v.Definitions),
		}
	case *LogicalSubquery:
		// 转换子查询的子计划
		var subplan PhysicalPlan
		if v.Subplan != nil {
			subplan = ConvertToPhysicalPlan(v.Subplan)
		}
		return &PhysicalSubquery{
			BasePhysicalPlan: BasePhysicalPlan{
				schema: v.Schema(),
				cost:   estimateSubqueryCost(v),
			},
			SubqueryType: v.SubqueryType,
			Correlated:   v.Correlated,
			OuterRefs:    v.OuterRefs,
			Subplan:      subplan,
		}
	case *LogicalApply:
		physical := &PhysicalApply{
			BasePhysicalPlan: BasePhysicalPlan{
				schema: v.Schema(),
				cost:   estimateApplyCost(v),
			},
			ApplyType:  v.ApplyType,
			Correlated: v.Correlated,
			JoinConds:  v.JoinConds,
		}
		physical.SetChildren(convertPhysicalChildren(v.Children()))
		return physical
	}
	return nil
}

func convertPhysicalChildren(children []LogicalPlan) []PhysicalPlan {
	if len(children) == 0 {
		return nil
	}
	converted := make([]PhysicalPlan, 0, len(children))
	for _, child := range children {
		if physicalChild := ConvertToPhysicalPlan(child); physicalChild != nil {
			converted = append(converted, physicalChild)
		}
	}
	return converted
}

// 代价估算辅助函数
func shouldUseHashJoin(join *LogicalJoin) bool {
	if join == nil {
		return false
	}
	if len(join.Children()) != 2 {
		return true
	}
	leftRows := estimateLogicalRowsForNode(join.Children()[0])
	rightRows := estimateLogicalRowsForNode(join.Children()[1])
	if leftRows <= 0 || rightRows <= 0 {
		return false
	}

	// 无连接条件时先不走哈希，避免全表笛卡尔放大
	if len(join.Conditions) == 0 {
		return false
	}

	// 小结果集且哈希代价优先才走哈希连接
	buildSideRows := float64(leftRows)
	if buildSideRows > float64(rightRows) {
		buildSideRows = float64(rightRows)
	}
	memoryPressure := buildSideRows * 0.2
	if memoryPressure > 10000 {
		return false
	}

	hashCost := estimateHashJoinCost(join)
	mergeCost := estimateMergeJoinCost(join)
	if mergeCost == 0 {
		return true
	}
	return hashCost <= mergeCost
}

func shouldUseHashAgg(agg *LogicalAggregation) bool {
	if agg == nil {
		return false
	}
	children := agg.Children()
	if len(children) == 0 {
		// 关键：空树场景下兜底返回 HashAgg，保持既有兼容预期
		return true
	}
	inputRows := estimateLogicalRowsForNode(children[0])
	if inputRows <= 0 {
		return true
	}

	// 小规模数据优先流式聚合，减少内存压力
	if inputRows <= 100 {
		return true
	}

	hashCost := estimateHashAggCost(agg)
	streamCost := estimateStreamAggCost(agg)
	return hashCost <= streamCost
}

func estimateHashJoinCost(join *LogicalJoin) float64 {
	if join == nil || len(join.Children()) != 2 {
		return 0
	}
	leftRows := float64(estimateLogicalRowsForNode(join.Children()[0]))
	rightRows := float64(estimateLogicalRowsForNode(join.Children()[1]))
	if leftRows <= 0 || rightRows <= 0 {
		return 0
	}

	buildRows := leftRows
	probeRows := rightRows
	if buildRows > probeRows {
		buildRows, probeRows = probeRows, buildRows
	}

	joinCondCost := 1.0 + float64(len(join.Conditions))

	// 建表成本：读取+哈希+插槽
	buildCost := buildRows * 0.6
	// 探测成本：读取+匹配
	probeCost := probeRows * 0.4 * joinCondCost
	// 内存/溢出惩罚
	memoryPenalty := buildRows * 0.0005

	return buildCost + probeCost + memoryPenalty
}

func estimateMergeJoinCost(join *LogicalJoin) float64 {
	if join == nil || len(join.Children()) != 2 {
		return 0
	}
	leftRows := float64(estimateLogicalRowsForNode(join.Children()[0]))
	rightRows := float64(estimateLogicalRowsForNode(join.Children()[1]))
	if leftRows <= 0 || rightRows <= 0 {
		return 0
	}

	leftSortCost := estimateSortCost(leftRows)
	rightSortCost := estimateSortCost(rightRows)
	mergeCost := (leftRows + rightRows) * 0.5
	conditionFactor := 1.0 + float64(len(join.Conditions))*0.1

	return (leftSortCost + rightSortCost + mergeCost) * conditionFactor
}

func estimateHashAggCost(agg *LogicalAggregation) float64 {
	if agg == nil || len(agg.Children()) == 0 {
		return 0
	}
	inputRows := estimateLogicalRowsForNode(agg.Children()[0])
	if inputRows <= 0 {
		return 0
	}

	groupCols := len(agg.GroupByItems)
	groupCount := estimateHashGroupCount(inputRows, groupCols)

	// 输入扫描 + 分组键计算 + 聚合状态维护
	readCost := float64(inputRows) * 0.6
	aggCost := float64(inputRows) * math.Max(1, float64(len(agg.AggFuncs))) * 0.08
	// 哈希表按组数量预估内存
	memoryPenalty := float64(groupCount) * 0.12

	return readCost + aggCost + memoryPenalty
}

func estimateStreamAggCost(agg *LogicalAggregation) float64 {
	if agg == nil || len(agg.Children()) == 0 {
		return 0
	}
	inputRows := estimateLogicalRowsForNode(agg.Children()[0])
	if inputRows <= 0 {
		return 0
	}

	groupCols := len(agg.GroupByItems)
	groupCount := estimateHashGroupCount(inputRows, groupCols)
	sortCost := estimateSortCost(float64(inputRows))
	aggCost := float64(groupCount) * math.Max(1, float64(len(agg.AggFuncs))) * 0.12

	return sortCost + aggCost + float64(inputRows)*0.1
}

func estimateLogicalRowsForNode(plan LogicalPlan) int64 {
	if plan == nil {
		return 0
	}

	switch v := plan.(type) {
	case *LogicalTableScan:
		if v.Table != nil && v.Table.Stats != nil {
			return v.Table.Stats.RowCount
		}
		return 100
	case *LogicalIndexScan:
		if v.Table != nil && v.Table.Stats != nil {
			return v.Table.Stats.RowCount
		}
		return 100
	case *LogicalSelection:
		children := v.Children()
		if len(children) == 0 {
			return 100
		}
		return estimateLogicalRowsForNode(children[0])
	case *LogicalProjection:
		children := v.Children()
		if len(children) == 0 {
			return 100
		}
		return estimateLogicalRowsForNode(children[0])
	case *LogicalJoin:
		children := v.Children()
		if len(children) != 2 {
			return 100
		}
		leftRows := estimateLogicalRowsForNode(children[0])
		rightRows := estimateLogicalRowsForNode(children[1])
		selectivity := 0.2
		if len(v.Conditions) == 0 {
			selectivity = 1.0
		}
		estimated := int64(float64(leftRows) * float64(rightRows) * selectivity)
		if estimated < 1 {
			return 1
		}
		if estimated > 1_000_000 {
			// 防止后续公式溢出，做上限保护
			estimated = 1_000_000
		}
		return estimated
	case *LogicalAggregation:
		children := v.Children()
		if len(children) == 0 {
			return 1
		}
		baseRows := estimateLogicalRowsForNode(children[0])
		groupCols := len(v.GroupByItems)
		return estimateHashGroupCount(baseRows, groupCols)
	case *LogicalSubquery:
		children := v.Children()
		if len(children) == 0 {
			return 1
		}
		return estimateLogicalRowsForNode(children[0])
	case *LogicalApply:
		children := v.Children()
		if len(children) == 0 {
			return 100
		}
		return estimateLogicalRowsForNode(children[0])
	default:
		children := plan.Children()
		if len(children) > 0 {
			return estimateLogicalRowsForNode(children[0])
		}
	}

	return 100
}

func estimateHashGroupCount(inputRows int64, groupCols int) int64 {
	if inputRows <= 1 {
		return 1
	}
	if groupCols == 0 {
		return 1
	}

	// 简化估算：每个分组列大约折半重复性
	factor := math.Pow(0.5, float64(groupCols))
	estimated := int64(float64(inputRows) * factor)
	if estimated < 1 {
		estimated = 1
	}
	if estimated > inputRows {
		return inputRows
	}
	return estimated
}

func estimateSortCost(rows float64) float64 {
	if rows <= 1 {
		return 0
	}
	return rows * math.Log2(rows+1.0) * 0.05
}

func estimateSubqueryCost(subquery *LogicalSubquery) float64 {
	// 子查询代价 = 子计划代价 * (关联子查询需要多次执行)
	baseCost := 100.0
	if subquery.Subplan != nil {
		// 递归估算子计划代价
		baseCost = 1000.0 // 简化估算
	}

	// 关联子查询代价更高（需要为外层每行执行一次）
	if subquery.Correlated {
		baseCost *= 100.0
	}

	return baseCost
}

func estimateApplyCost(apply *LogicalApply) float64 {
	// Apply算子代价 = 左表行数 * 右表代价
	baseCost := 1000.0

	// 关联Apply代价更高
	if apply.Correlated {
		baseCost *= 100.0
	}

	// SEMI/ANTI JOIN可以提前终止，代价较低
	if apply.ApplyType == "SEMI" || apply.ApplyType == "ANTI" {
		baseCost *= 0.5
	}

	return baseCost
}

// ByItem 排序项
type ByItem struct {
	Expr           Expression
	Desc           bool
	NullOrder      string
	ColumnIndex    int // output ordinal when ColumnIndexSet is true
	ColumnIndexSet bool
}

// PhysicalSubquery 子查询物理计划
type PhysicalSubquery struct {
	BasePhysicalPlan
	SubqueryType string       // "SCALAR", "IN", "EXISTS", "ANY", "ALL"
	Correlated   bool         // 是否为关联子查询
	OuterRefs    []string     // 外部引用的列
	Subplan      PhysicalPlan // 子查询的物理计划
}

func (p *PhysicalSubquery) ToString() string {
	return "Subquery(" + p.SubqueryType + ")"
}

func (p *PhysicalSubquery) GetPlanAccessType() string {
	return "SUBQUERY"
}

// PhysicalApply Apply算子物理计划（用于关联子查询）
type PhysicalApply struct {
	BasePhysicalPlan
	ApplyType  string       // "INNER", "LEFT", "SEMI", "ANTI"
	Correlated bool         // 是否为关联
	JoinConds  []Expression // 关联条件
}

func (p *PhysicalApply) ToString() string {
	return "Apply(" + p.ApplyType + ")"
}

func (p *PhysicalApply) GetPlanAccessType() string {
	return "APPLY"
}
