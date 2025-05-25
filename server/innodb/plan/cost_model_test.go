package plan

import (
	"testing"
)

func TestCostModel(t *testing.T) {
	model := NewDefaultCostModel()

	// 测试默认参数
	if model.DiskSeekCost != 10.0 {
		t.Errorf("DiskSeekCost = %v, want %v", model.DiskSeekCost, 10.0)
	}
	if model.BufferHitRatio != 0.8 {
		t.Errorf("BufferHitRatio = %v, want %v", model.BufferHitRatio, 0.8)
	}
}

func TestTableScanCost(t *testing.T) {
	model := NewDefaultCostModel()

	// 创建测试表扫描计划
	table := &TableStats{
		RowCount:  1000,
		TotalSize: 16 * 1024 * 10, // 10页，每页16KB
	}

	plan := &PhysicalTableScan{
		Table: table,
	}

	cost := model.tableScanCost(plan)

	// 验证代价计算
	// IO代价：10 (seek) + 10 * 1 * 0.2 (read pages * cost per page * miss ratio)
	// CPU代价：1000 * 0.01 (rows * cost per tuple)
	expectedCost := 10.0 + 2.0 + 10.0
	if cost != expectedCost {
		t.Errorf("tableScanCost = %v, want %v", cost, expectedCost)
	}
}

func TestIndexScanCost(t *testing.T) {
	model := NewDefaultCostModel()

	// 创建测试索引扫描计划
	table := &TableStats{
		RowCount:  1000,
		TotalSize: 16 * 1024 * 10,
	}
	index := &IndexStats{
		Cardinality: 100,
		Selectivity: 0.1,
	}

	plan := &PhysicalIndexScan{
		Table: table,
		Index: index,
	}

	cost := model.indexScanCost(plan)

	// 验证代价是否合理
	if cost <= model.tableScanCost(&PhysicalTableScan{Table: table}) {
		t.Error("Index scan cost should be less than table scan cost for selective index")
	}
}

func TestHashJoinCost(t *testing.T) {
	model := NewDefaultCostModel()

	// 创建测试哈希连接计划
	leftTable := &TableStats{RowCount: 1000}
	rightTable := &TableStats{RowCount: 2000}

	plan := &PhysicalHashJoin{
		BasePhysicalPlan: BasePhysicalPlan{
			children: []PhysicalPlan{
				&PhysicalTableScan{Table: leftTable},
				&PhysicalTableScan{Table: rightTable},
			},
		},
	}

	cost := model.hashJoinCost(plan)

	// 验证代价计算
	// 构建代价：1000 * (0.1 + 0.01)
	// 探测代价：2000 * (0.01 + 0.001)
	// 输出代价：(1000 * 2000 * 0.1) * 0.01
	expectedBuildCost := 1000 * (0.1 + 0.01)
	expectedProbeCost := 2000 * (0.01 + 0.001)
	expectedOutputCost := (1000 * 2000 * 0.1) * 0.01
	expectedTotalCost := expectedBuildCost + expectedProbeCost + expectedOutputCost

	if cost != expectedTotalCost {
		t.Errorf("hashJoinCost = %v, want %v", cost, expectedTotalCost)
	}
}

func TestHashAggCost(t *testing.T) {
	model := NewDefaultCostModel()

	// 创建测试哈希聚合计划
	table := &TableStats{RowCount: 1000}

	plan := &PhysicalHashAgg{
		BasePhysicalPlan: BasePhysicalPlan{
			children: []PhysicalPlan{
				&PhysicalTableScan{Table: table},
			},
		},
		GroupByItems: []Expression{&Column{Name: "id"}},
		AggFuncs:     []AggregateFunc{&Function{Name: "COUNT"}},
	}

	cost := model.hashAggCost(plan)

	// 验证代价计算
	// 构建代价：1000 * (0.1 + 0.01)
	// 聚合计算代价：1000 * 2 * 0.1 (rows * (group + agg) * eval cost)
	expectedBuildCost := 1000 * (0.1 + 0.01)
	expectedAggCost := 1000 * 2 * 0.1
	expectedTotalCost := expectedBuildCost + expectedAggCost

	if cost != expectedTotalCost {
		t.Errorf("hashAggCost = %v, want %v", cost, expectedTotalCost)
	}
}

func TestSortCost(t *testing.T) {
	model := NewDefaultCostModel()

	// 创建测试排序计划
	table := &TableStats{RowCount: 1000}

	plan := &PhysicalSort{
		BasePhysicalPlan: BasePhysicalPlan{
			children: []PhysicalPlan{
				&PhysicalTableScan{Table: table},
			},
		},
		ByItems: []ByItem{{Expr: &Column{Name: "id"}}},
	}

	cost := model.sortCost(plan)

	// 验证代价计算
	// 排序代价：1000 * 0.1
	expectedCost := 1000 * 0.1
	if cost != expectedCost {
		t.Errorf("sortCost = %v, want %v", cost, expectedCost)
	}
}

func TestProjectionCost(t *testing.T) {
	model := NewDefaultCostModel()

	// 创建测试投影计划
	table := &TableStats{RowCount: 1000}

	plan := &PhysicalProjection{
		BasePhysicalPlan: BasePhysicalPlan{
			children: []PhysicalPlan{
				&PhysicalTableScan{Table: table},
			},
		},
		Exprs: []Expression{
			&Column{Name: "id"},
			&Column{Name: "name"},
		},
	}

	cost := model.projectionCost(plan)

	// 验证代价计算
	// 投影代价：1000 * 2 * 0.1 (rows * exprs * eval cost)
	expectedCost := 1000 * 2 * 0.1
	if cost != expectedCost {
		t.Errorf("projectionCost = %v, want %v", cost, expectedCost)
	}
}

func TestSelectionCost(t *testing.T) {
	model := NewDefaultCostModel()

	// 创建测试选择计划
	table := &TableStats{RowCount: 1000}

	plan := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{
			children: []PhysicalPlan{
				&PhysicalTableScan{Table: table},
			},
		},
		Conditions: []Expression{
			&BinaryOperation{
				Op:    OpEQ,
				Left:  &Column{Name: "id"},
				Right: &Constant{Value: 1},
			},
		},
	}

	cost := model.selectionCost(plan)

	// 验证代价计算
	// 选择代价：1000 * 1 * 0.1 (rows * conditions * eval cost)
	expectedCost := 1000 * 1 * 0.1
	if cost != expectedCost {
		t.Errorf("selectionCost = %v, want %v", cost, expectedCost)
	}
}

func TestCompositePlanCost(t *testing.T) {
	model := NewDefaultCostModel()

	// 创建一个复合查询计划：
	// Sort
	//   HashAgg
	//     HashJoin
	//       TableScan(left)
	//       IndexScan(right)
	leftTable := &TableStats{RowCount: 1000}
	rightTable := &TableStats{RowCount: 2000}
	rightIndex := &IndexStats{
		Cardinality: 200,
		Selectivity: 0.1,
	}

	plan := &PhysicalSort{
		BasePhysicalPlan: BasePhysicalPlan{
			children: []PhysicalPlan{
				&PhysicalHashAgg{
					BasePhysicalPlan: BasePhysicalPlan{
						children: []PhysicalPlan{
							&PhysicalHashJoin{
								BasePhysicalPlan: BasePhysicalPlan{
									children: []PhysicalPlan{
										&PhysicalTableScan{Table: leftTable},
										&PhysicalIndexScan{
											Table: rightTable,
											Index: rightIndex,
										},
									},
								},
							},
						},
					},
					GroupByItems: []Expression{&Column{Name: "id"}},
					AggFuncs:     []AggregateFunc{&Function{Name: "COUNT"}},
				},
			},
		},
		ByItems: []ByItem{{Expr: &Column{Name: "id"}}},
	}

	cost := model.Cost(plan)

	// 验证总代价大于各个子计划的代价
	if cost <= 0 {
		t.Error("Composite plan cost should be greater than 0")
	}
}
