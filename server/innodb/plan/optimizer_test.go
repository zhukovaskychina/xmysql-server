package plan

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestEliminateAggregationSimpleMax(t *testing.T) {
	table := createTestTable()
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}

	agg := &LogicalAggregation{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		GroupByItems:    nil,
		AggFuncs: []AggregateFunc{&Function{
			FuncName: "MAX",
			FuncArgs: []Expression{&Column{Name: "id"}},
		}},
	}

	proj := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{agg}},
		Exprs:           []Expression{&Function{FuncName: "MAX", FuncArgs: []Expression{&Column{Name: "id"}}}},
	}

	optimized := OptimizeLogicalPlan(proj)

	p, ok := optimized.(*LogicalProjection)
	if !ok {
		t.Fatalf("expected projection")
	}

	if _, ok := p.Children()[0].(*LogicalAggregation); ok {
		t.Fatalf("aggregation not eliminated")
	}

	if len(p.Exprs) != 1 {
		t.Fatalf("unexpected projection expr count")
	}

	col, ok := p.Exprs[0].(*Column)
	if !ok || col.Name != "id" {
		t.Fatalf("expected projection on id")
	}
}

func TestEliminateAggregationSimpleMin(t *testing.T) {
	table := createTestTable()
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}

	agg := &LogicalAggregation{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		GroupByItems:    nil,
		AggFuncs: []AggregateFunc{&Function{
			FuncName: "MIN",
			FuncArgs: []Expression{&Column{Name: "id"}},
		}},
	}

	proj := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{agg}},
		Exprs:           []Expression{&Function{FuncName: "MIN", FuncArgs: []Expression{&Column{Name: "id"}}}},
	}

	optimized := OptimizeLogicalPlan(proj)

	p, ok := optimized.(*LogicalProjection)
	if !ok {
		t.Fatalf("expected projection")
	}

	if _, ok := p.Children()[0].(*LogicalAggregation); ok {
		t.Fatalf("aggregation not eliminated")
	}

	if len(p.Exprs) != 1 {
		t.Fatalf("unexpected projection expr count")
	}

	col, ok := p.Exprs[0].(*Column)
	if !ok || col.Name != "id" {
		t.Fatalf("expected projection on id")
	}
}

// TestPredicatePushdown_SelectionOverTableScan OPT-011：Selection(谓词) over TableScan 优化后谓词在 TableScan 之上的 Selection 中
func TestPredicatePushdown_SelectionOverTableScan(t *testing.T) {
	table := createTestTable()
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	sel := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}},
		},
	}
	optimized := OptimizeLogicalPlan(sel)
	// 谓词下推后应为 Selection(TableScan) 或 Selection(IndexScan)，且条件被保留
	outSel, ok := optimized.(*LogicalSelection)
	if !ok {
		// 可能被优化成 IndexScan 等，则其父节点或自身应带条件
		if _, isIdx := optimized.(*LogicalIndexScan); isIdx {
			return // 索引扫描也视为下推成功
		}
		t.Fatalf("expected LogicalSelection or LogicalIndexScan at top, got %T", optimized)
	}
	if len(outSel.Conditions) == 0 {
		t.Errorf("expected at least one condition after pushdown, got 0")
	}
	child := outSel.Children()[0]
	if _, ok := child.(*LogicalTableScan); !ok {
		if _, ok := child.(*LogicalIndexScan); !ok {
			t.Errorf("expected TableScan or IndexScan under Selection, got %T", child)
		}
	}
}

func TestIndexAccessOptimizationPreservesSelectionPredicate(t *testing.T) {
	table := createTestTable()
	sel := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{
				BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
				Table:           table,
			},
		}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}},
		},
	}

	optimized := OptimizeLogicalPlan(sel)
	outSel, ok := optimized.(*LogicalSelection)
	if !ok {
		t.Fatalf("optimized plan = %T, want LogicalSelection so residual predicates are retained", optimized)
	}
	if len(outSel.Conditions) != 1 {
		t.Fatalf("optimized selection conditions = %d, want 1", len(outSel.Conditions))
	}
	if _, ok := outSel.Children()[0].(*LogicalIndexScan); !ok {
		t.Fatalf("optimized selection child = %T, want LogicalIndexScan", outSel.Children()[0])
	}
}

func TestPredicatePushdownThroughDirectProjectionAlias(t *testing.T) {
	table := createTestTable()
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	projection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		Exprs:           []Expression{&Column{Name: "id"}},
		OutputNames:     []string{"user_id"},
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{projection}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpGT, Left: &Column{Name: "user_id"}, Right: &Constant{Value: int64(1)}},
		},
	}

	optimized := pushDownPredicates(selection)
	optimizedProjection, ok := optimized.(*LogicalProjection)
	if !ok {
		t.Fatalf("expected projection after pushing alias predicate, got %T", optimized)
	}
	if _, ok := optimizedProjection.Children()[0].(*LogicalSelection); !ok {
		t.Fatalf("expected selection below direct projection alias, got %T", optimizedProjection.Children()[0])
	}
	pushed := optimizedProjection.Children()[0].(*LogicalSelection)
	if len(pushed.Conditions) != 1 {
		t.Fatalf("pushed conditions = %d, want 1", len(pushed.Conditions))
	}
	predicate, ok := pushed.Conditions[0].(*BinaryOperation)
	if !ok {
		t.Fatalf("pushed predicate = %T, want BinaryOperation", pushed.Conditions[0])
	}
	column, ok := predicate.Left.(*Column)
	if !ok || !strings.EqualFold(column.Name, "id") {
		t.Fatalf("pushed predicate column = %T %v, want id", predicate.Left, predicate.Left)
	}
}

// TestPredicatePushdown_Join 谓词下推 OPT-001：仅涉及左表/右表的条件应下推到对应子计划
func TestPredicatePushdown_Join(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableA.AddColumn(&metadata.Column{Name: "a_name", DataType: metadata.TypeVarchar})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)

	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableB.AddColumn(&metadata.Column{Name: "b_score", DataType: metadata.TypeInt})
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)

	scanA := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: schemaA},
		Table:           tableA,
	}
	scanB := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: schemaB},
		Table:           tableB,
	}
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scanA, scanB}},
		JoinType:        "INNER",
		Conditions:      []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Column{Name: "id"}}},
		LeftSchema:      schemaA,
		RightSchema:     schemaB,
	}
	sel := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a_name"}, Right: &Constant{Value: "x"}},
			&BinaryOperation{Op: OpGT, Left: &Column{Name: "b_score"}, Right: &Constant{Value: int64(0)}},
		},
	}

	optimized := OptimizeLogicalPlan(sel)

	// 全部条件可下推时，顶层返回 Join
	outJoin, ok := optimized.(*LogicalJoin)
	if !ok {
		t.Fatalf("expected Join at top after pushdown, got %T", optimized)
	}
	left := outJoin.Children()[0]
	right := outJoin.Children()[1]
	leftSel, leftIsSel := left.(*LogicalSelection)
	rightSel, rightIsSel := right.(*LogicalSelection)
	if !leftIsSel || !rightIsSel {
		t.Fatalf("expected Selection under Join on both sides, got left=%T right=%T", left, right)
	}
	if len(leftSel.Conditions) != 1 {
		t.Errorf("expected 1 condition pushed to left, got %d", len(leftSel.Conditions))
	}
	if len(rightSel.Conditions) != 1 {
		t.Errorf("expected 1 condition pushed to right, got %d", len(rightSel.Conditions))
	}
}

func TestPredicatePushdownInfersJoinSideConstantFromEquality(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType:    "INNER",
		Conditions:  []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Column{Name: "b.id"}}},
		LeftSchema:  schemaA,
		RightSchema: schemaB,
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions:      []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Constant{Value: int64(7)}}},
	}

	optimized, ok := pushDownPredicates(selection).(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized plan = %T, want LogicalJoin", OptimizeLogicalPlan(selection))
	}
	if _, ok := optimized.Children()[0].(*LogicalSelection); !ok {
		t.Fatalf("left child = %T, want inferred/pushed selection", optimized.Children()[0])
	}
	rightSelection, ok := optimized.Children()[1].(*LogicalSelection)
	if !ok || len(rightSelection.Conditions) != 1 {
		t.Fatalf("right child = %T with %d conditions, want one inferred selection", optimized.Children()[1], func() int {
			if ok {
				return len(rightSelection.Conditions)
			}
			return 0
		}())
	}
	inferred, ok := rightSelection.Conditions[0].(*BinaryOperation)
	if !ok || inferred.Op != OpEQ {
		t.Fatalf("inferred condition = %T %v, want equality", rightSelection.Conditions[0], rightSelection.Conditions[0])
	}
	rightColumn, ok := inferred.Left.(*Column)
	if !ok || rightColumn.Name != "b.id" {
		t.Fatalf("inferred left column = %T %v, want b.id", inferred.Left, inferred.Left)
	}
}

func TestPredicatePushdownInfersJoinSideRangeFromEquality(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType:   "INNER",
		Conditions: []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Column{Name: "b.id"}}},
		LeftSchema: schemaA, RightSchema: schemaB,
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions:      []Expression{&BinaryOperation{Op: OpGT, Left: &Column{Name: "a.id"}, Right: &Constant{Value: int64(7)}}},
	}

	optimized, ok := OptimizeLogicalPlan(selection).(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized plan = %T, want LogicalJoin", OptimizeLogicalPlan(selection))
	}
	rightSelection, ok := optimized.Children()[1].(*LogicalSelection)
	if !ok || len(rightSelection.Conditions) != 1 {
		t.Fatalf("right child = %T with %d conditions, want one inferred range", optimized.Children()[1], func() int {
			if ok {
				return len(rightSelection.Conditions)
			}
			return 0
		}())
	}
	inferred, ok := rightSelection.Conditions[0].(*BinaryOperation)
	if !ok || inferred.Op != OpGT {
		t.Fatalf("inferred condition = %T %v, want greater-than", rightSelection.Conditions[0], rightSelection.Conditions[0])
	}
	column, ok := inferred.Left.(*Column)
	if !ok || column.Name != "b.id" {
		t.Fatalf("inferred left column = %T %v, want b.id", inferred.Left, inferred.Left)
	}
}

func TestPredicatePushdownInfersNullabilityFromInnerJoinEquality(t *testing.T) {
	build := func(name string) (*metadata.Table, *metadata.DatabaseSchema) {
		table := metadata.NewTable(name)
		table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
		schema := metadata.NewSchema("test")
		_ = schema.AddTable(table)
		return table, schema
	}
	leftTable, leftSchema := build("a")
	rightTable, rightSchema := build("b")
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftSchema}, Table: leftTable},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightSchema}, Table: rightTable},
		}},
		JoinType:    "INNER",
		Conditions:  []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Column{Name: "b.id"}}},
		LeftSchema:  leftSchema,
		RightSchema: rightSchema,
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions: []Expression{
			&IsNullExpression{Column: &Column{Name: "a.id"}, IsNull: true},
		},
	}

	optimized, ok := OptimizeLogicalPlan(selection).(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized plan = %T, want LogicalJoin", OptimizeLogicalPlan(selection))
	}
	rightSelection, ok := optimized.Children()[1].(*LogicalSelection)
	if !ok || len(rightSelection.Conditions) != 1 {
		t.Fatalf("right child = %T with %d conditions, want one inferred IS NULL", optimized.Children()[1], func() int {
			if ok {
				return len(rightSelection.Conditions)
			}
			return 0
		}())
	}
	inferred, ok := rightSelection.Conditions[0].(*IsNullExpression)
	if !ok || !inferred.IsNull {
		t.Fatalf("inferred condition = %T %v, want IS NULL", rightSelection.Conditions[0], rightSelection.Conditions[0])
	}
	column, ok := inferred.Column.(*Column)
	if !ok || column.Name != "b.id" {
		t.Fatalf("inferred column = %T %v, want b.id", inferred.Column, inferred.Column)
	}
}

func TestPredicatePushdownMovesInnerJoinEqualityIntoJoinCondition(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType:    "INNER",
		LeftSchema:  schemaA,
		RightSchema: schemaB,
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions:      []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Column{Name: "b.id"}}},
	}

	optimized, ok := OptimizeLogicalPlan(selection).(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized plan = %T, want LogicalJoin", OptimizeLogicalPlan(selection))
	}
	if len(optimized.Conditions) != 1 {
		t.Fatalf("join conditions = %d, want one moved equality", len(optimized.Conditions))
	}
}

func TestPredicatePushdownMovesNestedInnerJoinEqualitiesIntoJoinCondition(t *testing.T) {
	tableA := metadata.NewTable("nested_a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableA.AddColumn(&metadata.Column{Name: "tenant_id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	tableB := metadata.NewTable("nested_b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableB.AddColumn(&metadata.Column{Name: "tenant_id", DataType: metadata.TypeInt})
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType:    "INNER",
		LeftSchema:  schemaA,
		RightSchema: schemaB,
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpAnd,
			Left:  &BinaryOperation{Op: OpEQ, Left: &Column{Name: "nested_a.id"}, Right: &Column{Name: "nested_b.id"}},
			Right: &BinaryOperation{Op: OpEQ, Left: &Column{Name: "nested_a.tenant_id"}, Right: &Column{Name: "nested_b.tenant_id"}},
		}},
	}

	joinConditions, remaining := moveInnerJoinEqualities(join, selection.Conditions)
	if len(joinConditions) != 2 {
		t.Fatalf("join conditions = %d, want two moved equalities", len(joinConditions))
	}
	if len(remaining) != 0 {
		t.Fatalf("remaining conditions = %d, want none", len(remaining))
	}
}

func TestPredicatePushdown_OuterJoinSingleSidePredicates(t *testing.T) {
	build := func(joinType string) LogicalPlan {
		tableA := metadata.NewTable("a")
		tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
		tableA.AddColumn(&metadata.Column{Name: "a_name", DataType: metadata.TypeVarchar})
		schemaA := metadata.NewSchema("test")
		_ = schemaA.AddTable(tableA)
		tableB := metadata.NewTable("b")
		tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
		tableB.AddColumn(&metadata.Column{Name: "b_score", DataType: metadata.TypeInt})
		schemaB := metadata.NewSchema("test")
		_ = schemaB.AddTable(tableB)
		join := &LogicalJoin{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
				&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
				&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
			}},
			JoinType: joinType, LeftSchema: schemaA, RightSchema: schemaB,
		}
		return &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
			Conditions: []Expression{
				&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a_name"}, Right: &Constant{Value: "x"}},
				&BinaryOperation{Op: OpGT, Left: &Column{Name: "b_score"}, Right: &Constant{Value: int64(0)}},
			},
		}
	}

	for _, joinType := range []string{"LEFT", "RIGHT"} {
		t.Run(joinType, func(t *testing.T) {
			optimized := OptimizeLogicalPlan(build(joinType))
			topSelection, ok := optimized.(*LogicalSelection)
			if !ok {
				t.Fatalf("optimized %s plan = %T, want top LogicalSelection", joinType, optimized)
			}
			join, ok := topSelection.Children()[0].(*LogicalJoin)
			if !ok {
				t.Fatalf("top selection child of %s = %T, want LogicalJoin", joinType, topSelection.Children()[0])
			}
			if joinType == "LEFT" {
				if _, ok := join.Children()[0].(*LogicalSelection); !ok {
					t.Fatalf("left child of LEFT join = %T, want pushed selection", join.Children()[0])
				}
				if _, ok := join.Children()[1].(*LogicalTableScan); !ok {
					t.Fatalf("right child of LEFT join = %T, want scan without null-side pushdown", join.Children()[1])
				}
			} else {
				if _, ok := join.Children()[0].(*LogicalTableScan); !ok {
					t.Fatalf("left child of RIGHT join = %T, want scan without null-side pushdown", join.Children()[0])
				}
				if _, ok := join.Children()[1].(*LogicalSelection); !ok {
					t.Fatalf("right child of RIGHT join = %T, want pushed selection", join.Children()[1])
				}
			}
		})
	}
}

func TestPredicatePushdownOuterJoinRetainsNullExtendedSideFilter(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableB.AddColumn(&metadata.Column{Name: "score", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType: "LEFT", LeftSchema: schemaA, RightSchema: schemaB,
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpGT, Left: &Column{Name: "score"}, Right: &Constant{Value: int64(0)}},
		},
	}

	optimized := OptimizeLogicalPlan(selection)
	topSelection, ok := optimized.(*LogicalSelection)
	if !ok {
		t.Fatalf("LEFT JOIN null-extended-side filter was removed from top of plan: %T", optimized)
	}
	if len(topSelection.Conditions) != 1 {
		t.Fatalf("top selection conditions = %d, want 1", len(topSelection.Conditions))
	}
	if _, ok := topSelection.Children()[0].(*LogicalJoin); !ok {
		t.Fatalf("top selection child = %T, want LogicalJoin", topSelection.Children()[0])
	}
}

func TestPredicatePushdownOuterJoinRetainsDynamicBetweenCrossChildFilter(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "value", DataType: metadata.TypeInt})
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "lower_bound", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType: "LEFT", LeftSchema: schemaA, RightSchema: schemaB,
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions: []Expression{&BetweenExpression{
			Column:    &Column{Name: "a.value"},
			LowerExpr: &Column{Name: "b.lower_bound"},
			Upper:     int64(100),
		}},
	}

	optimized := OptimizeLogicalPlan(selection)
	topSelection, ok := optimized.(*LogicalSelection)
	if !ok {
		t.Fatalf("cross-child dynamic BETWEEN optimized plan = %T, want top LogicalSelection", optimized)
	}
	if len(topSelection.Conditions) != 1 {
		t.Fatalf("top selection conditions = %d, want 1", len(topSelection.Conditions))
	}
	if _, ok := topSelection.Children()[0].(*LogicalJoin); !ok {
		t.Fatalf("top selection child = %T, want LogicalJoin", topSelection.Children()[0])
	}
}

func TestPredicatePushdownInfersOuterJoinPreservedSidePredicateAcrossEquality(t *testing.T) {
	build := func(joinType string) LogicalPlan {
		tableA := metadata.NewTable("a")
		tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
		schemaA := metadata.NewSchema("test")
		_ = schemaA.AddTable(tableA)
		tableB := metadata.NewTable("b")
		tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
		schemaB := metadata.NewSchema("test")
		_ = schemaB.AddTable(tableB)
		join := &LogicalJoin{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
				&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
				&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
			}},
			JoinType:    joinType,
			Conditions:  []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Column{Name: "b.id"}}},
			LeftSchema:  schemaA,
			RightSchema: schemaB,
		}
		column := "a.id"
		if joinType == "RIGHT" {
			column = "b.id"
		}
		return &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
			Conditions:      []Expression{&BinaryOperation{Op: OpGT, Left: &Column{Name: column}, Right: &Constant{Value: int64(7)}}},
		}
	}

	for _, joinType := range []string{"LEFT", "RIGHT"} {
		t.Run(joinType, func(t *testing.T) {
			optimized, ok := OptimizeLogicalPlan(build(joinType)).(*LogicalJoin)
			if !ok {
				t.Fatalf("optimized %s plan = %T, want LogicalJoin", joinType, OptimizeLogicalPlan(build(joinType)))
			}
			left, right := optimized.Children()[0], optimized.Children()[1]
			leftSelection, leftIsSelection := left.(*LogicalSelection)
			rightSelection, rightIsSelection := right.(*LogicalSelection)
			if joinType == "LEFT" {
				if !leftIsSelection || !rightIsSelection {
					t.Fatalf("LEFT JOIN children = %T, %T, want selections on both sides", left, right)
				}
				if len(leftSelection.Conditions) != 1 || len(rightSelection.Conditions) != 1 {
					t.Fatalf("LEFT JOIN selection sizes = %d, %d, want one each", len(leftSelection.Conditions), len(rightSelection.Conditions))
				}
				inferred, ok := rightSelection.Conditions[0].(*BinaryOperation)
				if !ok || inferred.Op != OpGT {
					t.Fatalf("LEFT JOIN inferred predicate = %T %v, want >", rightSelection.Conditions[0], rightSelection.Conditions[0])
				}
				column, ok := inferred.Left.(*Column)
				if !ok || column.Name != "b.id" {
					t.Fatalf("LEFT JOIN inferred column = %T %v, want b.id", inferred.Left, inferred.Left)
				}
			} else {
				if !leftIsSelection || !rightIsSelection {
					t.Fatalf("RIGHT JOIN children = %T, %T, want selections on both sides", left, right)
				}
				if len(leftSelection.Conditions) != 1 || len(rightSelection.Conditions) != 1 {
					t.Fatalf("RIGHT JOIN selection sizes = %d, %d, want one each", len(leftSelection.Conditions), len(rightSelection.Conditions))
				}
				inferred, ok := leftSelection.Conditions[0].(*BinaryOperation)
				if !ok || inferred.Op != OpGT {
					t.Fatalf("RIGHT JOIN inferred predicate = %T %v, want >", leftSelection.Conditions[0], leftSelection.Conditions[0])
				}
				column, ok := inferred.Left.(*Column)
				if !ok || column.Name != "a.id" {
					t.Fatalf("RIGHT JOIN inferred column = %T %v, want a.id", inferred.Left, inferred.Left)
				}
			}
		})
	}
}

func TestPredicatePushdownInfersOuterJoinStructuredPredicatesAcrossEquality(t *testing.T) {
	build := func(joinType string, condition Expression) LogicalPlan {
		tableA := metadata.NewTable("a")
		tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
		schemaA := metadata.NewSchema("test")
		_ = schemaA.AddTable(tableA)
		tableB := metadata.NewTable("b")
		tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
		schemaB := metadata.NewSchema("test")
		_ = schemaB.AddTable(tableB)
		join := &LogicalJoin{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
				&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
				&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
			}},
			JoinType:    joinType,
			Conditions:  []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Column{Name: "b.id"}}},
			LeftSchema:  schemaA,
			RightSchema: schemaB,
		}
		return &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
			Conditions:      []Expression{condition},
		}
	}

	leftPlan := OptimizeLogicalPlan(build("LEFT", &BinaryOperation{Op: OpNotLike, Left: &Column{Name: "a.id"}, Right: &Constant{Value: "7%"}}))
	leftJoin, ok := leftPlan.(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized LEFT structured plan = %T, want LogicalJoin", leftPlan)
	}
	if _, ok := leftJoin.Children()[1].(*LogicalSelection); !ok {
		t.Fatalf("LEFT JOIN right child = %T, want inferred selection", leftJoin.Children()[1])
	}

	leftLikePlan := OptimizeLogicalPlan(build("LEFT", &LikeExpression{Column: &Column{Name: "a.id"}, Pattern: "7%"}))
	leftLikeJoin, ok := leftLikePlan.(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized LEFT LIKE plan = %T, want LogicalJoin", leftLikePlan)
	}
	if _, ok := leftLikeJoin.Children()[1].(*LogicalSelection); !ok {
		t.Fatalf("LEFT JOIN LIKE right child = %T, want inferred selection", leftLikeJoin.Children()[1])
	}

	rightPlan := OptimizeLogicalPlan(build("RIGHT", &BetweenExpression{
		Column: &Column{Name: "b.id"},
		Lower:  int64(1),
		Upper:  int64(10),
	}))
	rightJoin, ok := rightPlan.(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized RIGHT structured plan = %T, want LogicalJoin", rightPlan)
	}
	if _, ok := rightJoin.Children()[0].(*LogicalSelection); !ok {
		t.Fatalf("RIGHT JOIN left child = %T, want inferred selection", rightJoin.Children()[0])
	}
}

func TestPredicatePushdownInfersAllOuterJoinPredicatesAcrossEquality(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType: "LEFT",
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Column{Name: "b.id"}},
		},
		LeftSchema: schemaA, RightSchema: schemaB,
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpGT, Left: &Column{Name: "a.id"}, Right: &Constant{Value: int64(7)}},
			&BinaryOperation{Op: OpLT, Left: &Column{Name: "a.id"}, Right: &Constant{Value: int64(10)}},
		},
	}

	optimized, ok := OptimizeLogicalPlan(selection).(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized LEFT JOIN plan = %T, want LogicalJoin", OptimizeLogicalPlan(selection))
	}
	rightSelection, ok := optimized.Children()[1].(*LogicalSelection)
	if !ok {
		t.Fatalf("right child = %T, want LogicalSelection", optimized.Children()[1])
	}
	if len(rightSelection.Conditions) != 2 {
		t.Fatalf("inferred right predicates = %d, want 2", len(rightSelection.Conditions))
	}
}

func TestPredicatePushdownInfersOuterJoinPredicateAcrossTupleEquality(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "k1", DataType: metadata.TypeInt})
	tableA.AddColumn(&metadata.Column{Name: "k2", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "k1", DataType: metadata.TypeInt})
	tableB.AddColumn(&metadata.Column{Name: "k2", DataType: metadata.TypeInt})
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType: "LEFT",
		Conditions: []Expression{&BinaryOperation{
			Op: OpEQ,
			Left: &TupleExpression{Exprs: []Expression{
				&Column{Name: "a.k1"},
				&Column{Name: "a.k2"},
			}},
			Right: &TupleExpression{Exprs: []Expression{
				&Column{Name: "b.k1"},
				&Column{Name: "b.k2"},
			}},
		}},
		LeftSchema: schemaA, RightSchema: schemaB,
	}
	optimized, ok := OptimizeLogicalPlan(&LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions: []Expression{&BinaryOperation{
			Op: OpGT, Left: &Column{Name: "a.k1"}, Right: &Constant{Value: int64(7)},
		}},
	}).(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized tuple-equality LEFT JOIN plan = %T, want LogicalJoin", OptimizeLogicalPlan(&LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
			Conditions: []Expression{&BinaryOperation{
				Op: OpGT, Left: &Column{Name: "a.k1"}, Right: &Constant{Value: int64(7)},
			}},
		}))
	}
	rightSelection, ok := optimized.Children()[1].(*LogicalSelection)
	if !ok || len(rightSelection.Conditions) != 1 {
		t.Fatalf("tuple-equality right child = %T with %d conditions, want inferred selection", optimized.Children()[1], func() int {
			if !ok {
				return 0
			}
			return len(rightSelection.Conditions)
		}())
	}
	inferred, ok := rightSelection.Conditions[0].(*BinaryOperation)
	if !ok || inferred.Op != OpGT {
		t.Fatalf("tuple-equality inferred predicate = %T %v, want >", rightSelection.Conditions[0], rightSelection.Conditions[0])
	}
	inferredColumn, ok := inferred.Left.(*Column)
	if !ok || inferredColumn.Name != "b.k1" {
		t.Fatalf("tuple-equality inferred column = %T %v, want b.k1", inferred.Left, inferred.Left)
	}
}

func TestPredicatePushdownMovesTupleEqualityIntoInnerJoin(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "k1", DataType: metadata.TypeInt})
	tableA.AddColumn(&metadata.Column{Name: "k2", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "k1", DataType: metadata.TypeInt})
	tableB.AddColumn(&metadata.Column{Name: "k2", DataType: metadata.TypeInt})
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType:    "INNER",
		LeftSchema:  schemaA,
		RightSchema: schemaB,
	}
	condition := &BinaryOperation{
		Op: OpEQ,
		Left: &TupleExpression{Exprs: []Expression{
			&Column{Name: "a.k1"},
			&Column{Name: "a.k2"},
		}},
		Right: &TupleExpression{Exprs: []Expression{
			&Column{Name: "b.k1"},
			&Column{Name: "b.k2"},
		}},
	}
	optimized := OptimizeLogicalPlan(&LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions:      []Expression{condition},
	})
	optimizedJoin, ok := optimized.(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized tuple-equality INNER JOIN plan = %T, want LogicalJoin", optimized)
	}
	if len(optimizedJoin.Conditions) != 1 {
		t.Fatalf("inner join conditions = %d, want one tuple equality", len(optimizedJoin.Conditions))
	}
	if _, ok := optimizedJoin.Conditions[0].(*BinaryOperation); !ok {
		t.Fatalf("inner join condition = %T, want BinaryOperation", optimizedJoin.Conditions[0])
	}
}

func TestAggregatePredicateInsideCaseIsNotPushedBelowAggregation(t *testing.T) {
	table := metadata.NewTable("metrics")
	table.AddColumn(&metadata.Column{Name: "tenant_id", DataType: metadata.TypeInt})
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	agg := &LogicalAggregation{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schema}, Table: table},
		}},
		GroupByItems: []Expression{&Column{Name: "tenant_id"}},
	}
	condition := &CaseExpression{
		Whens: []CaseWhen{{
			Condition: &Constant{Value: true},
			Value: &Function{FuncName: "COUNT", FuncArgs: []Expression{
				&Column{Name: "tenant_id"},
			}},
		}},
		Else: &Constant{Value: int64(0)},
	}

	pushable, nonPushable := splitAggregatePredicate([]Expression{condition}, agg)
	if len(pushable) != 0 || len(nonPushable) != 1 {
		t.Fatalf("CASE aggregate predicate split = pushable %d, non-pushable %d; want 0, 1", len(pushable), len(nonPushable))
	}
}

func TestPredicatePushdownOuterJoinRecognizesQualifiedColumnExpression(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableA.AddColumn(&metadata.Column{Name: "a_name", DataType: metadata.TypeVarchar})
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType: "LEFT", LeftSchema: schemaA, RightSchema: schemaB,
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.a_name"}, Right: &Constant{Value: "x"}},
		},
	}

	optimized := OptimizeLogicalPlan(selection)
	optimizedJoin, ok := optimized.(*LogicalJoin)
	if !ok {
		t.Fatalf("qualified preserved-side predicate remained above LEFT JOIN: %T", optimized)
	}
	if _, ok := optimizedJoin.Children()[0].(*LogicalSelection); !ok {
		t.Fatalf("qualified preserved-side predicate was not pushed to left child: %T", optimizedJoin.Children()[0])
	}

	joinWithoutSchemas := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{Table: tableA},
			&LogicalTableScan{Table: tableB},
		}},
		JoinType: "LEFT",
	}
	withoutSchemas := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{joinWithoutSchemas}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.a_name"}, Right: &Constant{Value: "x"}},
		},
	}
	optimizedWithoutSchemas, ok := OptimizeLogicalPlan(withoutSchemas).(*LogicalJoin)
	if !ok {
		t.Fatalf("qualified predicate without explicit schemas remained above LEFT JOIN: %T", withoutSchemas)
	}
	if _, ok := optimizedWithoutSchemas.Children()[0].(*LogicalSelection); !ok {
		t.Fatalf("qualified predicate without explicit schemas was not pushed: %T", optimizedWithoutSchemas.Children()[0])
	}
}

func TestPredicatePushdownInfersOuterJoinWrappedColumnExpression(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType:   "LEFT",
		Conditions: []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Column{Name: "b.id"}}},
		LeftSchema: schemaA, RightSchema: schemaB,
	}
	wrapped := &BinaryOperation{
		Op:    OpGT,
		Left:  &BinaryOperation{Op: OpAdd, Left: &Column{Name: "a.id"}, Right: &Constant{Value: int64(1)}},
		Right: &Constant{Value: int64(7)},
	}
	optimized, ok := OptimizeLogicalPlan(&LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions:      []Expression{wrapped},
	}).(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized plan = %T, want LogicalJoin", OptimizeLogicalPlan(&LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
			Conditions:      []Expression{wrapped},
		}))
	}
	rightSelection, ok := optimized.Children()[1].(*LogicalSelection)
	if !ok || len(rightSelection.Conditions) != 1 {
		t.Fatalf("right child = %T with %d conditions, want inferred wrapped predicate", optimized.Children()[1], func() int {
			if !ok {
				return 0
			}
			return len(rightSelection.Conditions)
		}())
	}
	inferred, ok := rightSelection.Conditions[0].(*BinaryOperation)
	if !ok || inferred.Op != OpGT {
		t.Fatalf("inferred predicate = %T %v, want >", rightSelection.Conditions[0], rightSelection.Conditions[0])
	}
	inferredLeft, ok := inferred.Left.(*BinaryOperation)
	if !ok || inferredLeft.Op != OpAdd {
		t.Fatalf("inferred left expression = %T %v, want addition", inferred.Left, inferred.Left)
	}
	inferredColumn, ok := inferredLeft.Left.(*Column)
	if !ok || inferredColumn.Name != "b.id" {
		t.Fatalf("inferred wrapped column = %T %v, want b.id", inferredLeft.Left, inferredLeft.Left)
	}
}

func TestPredicatePushdownInfersOuterJoinWrappedJoinKeyExpression(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)

	joinKey := func(table string) Expression {
		return &BinaryOperation{
			Op:    OpAdd,
			Left:  &Column{Name: table + ".id"},
			Right: &Constant{Value: int64(1)},
		}
	}
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType:   "LEFT",
		Conditions: []Expression{&BinaryOperation{Op: OpEQ, Left: joinKey("a"), Right: joinKey("b")}},
		LeftSchema: schemaA, RightSchema: schemaB,
	}
	leftCondition := &BinaryOperation{Op: OpGT, Left: joinKey("a"), Right: &Constant{Value: int64(7)}}
	_, rightConds := inferOuterJoinPreservedPredicates(join, []Expression{leftCondition}, nil)
	if len(rightConds) != 1 {
		t.Fatalf("inferred right conditions = %d, want 1", len(rightConds))
	}
	inferred, ok := rightConds[0].(*BinaryOperation)
	if !ok || inferred.Op != OpGT {
		t.Fatalf("inferred condition = %T %v, want >", rightConds[0], rightConds[0])
	}
	inferredKey, ok := inferred.Left.(*BinaryOperation)
	if !ok || inferredKey.Op != OpAdd {
		t.Fatalf("inferred join key = %T %v, want addition", inferred.Left, inferred.Left)
	}
	inferredColumn, ok := inferredKey.Left.(*Column)
	if !ok || inferredColumn.Name != "b.id" {
		t.Fatalf("inferred column = %T %v, want b.id", inferredKey.Left, inferredKey.Left)
	}
}

func TestPredicatePushdownInfersOuterJoinFunctionWrappedJoinKeyExpression(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)

	joinKey := func(table string) Expression {
		return &Function{FuncName: "ABS", FuncArgs: []Expression{&Column{Name: table + ".id"}}}
	}
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType:   "LEFT",
		Conditions: []Expression{&BinaryOperation{Op: OpEQ, Left: joinKey("a"), Right: joinKey("b")}},
		LeftSchema: schemaA, RightSchema: schemaB,
	}
	leftCondition := &BinaryOperation{Op: OpGT, Left: joinKey("a"), Right: &Constant{Value: int64(7)}}
	_, rightConds := inferOuterJoinPreservedPredicates(join, []Expression{leftCondition}, nil)
	if len(rightConds) != 1 {
		t.Fatalf("inferred right conditions = %d, want 1", len(rightConds))
	}
	inferred, ok := rightConds[0].(*BinaryOperation)
	if !ok || inferred.Op != OpGT {
		t.Fatalf("inferred condition = %T %v, want >", rightConds[0], rightConds[0])
	}
	inferredFunction, ok := inferred.Left.(*Function)
	if !ok || !strings.EqualFold(inferredFunction.FuncName, "ABS") {
		t.Fatalf("inferred join key = %T %v, want ABS", inferred.Left, inferred.Left)
	}
	inferredColumn, ok := inferredFunction.FuncArgs[0].(*Column)
	if !ok || inferredColumn.Name != "b.id" {
		t.Fatalf("inferred function column = %T %v, want b.id", inferredFunction.FuncArgs[0], inferredFunction.FuncArgs[0])
	}
}

func TestPredicatePushdownRejectsNondeterministicOuterJoinFunctionKey(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	joinKey := func(table string) Expression {
		return &Function{FuncName: "RAND", FuncArgs: []Expression{&Column{Name: table + ".id"}}}
	}
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType:   "LEFT",
		Conditions: []Expression{&BinaryOperation{Op: OpEQ, Left: joinKey("a"), Right: joinKey("b")}},
		LeftSchema: schemaA, RightSchema: schemaB,
	}
	_, rightConds := inferOuterJoinPreservedPredicates(join, []Expression{
		&BinaryOperation{Op: OpGT, Left: joinKey("a"), Right: &Constant{Value: int64(7)}},
	}, nil)
	if len(rightConds) != 0 {
		t.Fatalf("inferred right conditions = %d, want 0 for nondeterministic function", len(rightConds))
	}
}

func TestPredicatePushdownInfersOuterJoinBooleanWrappedPredicate(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType:   "LEFT",
		Conditions: []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Column{Name: "b.id"}}},
		LeftSchema: schemaA, RightSchema: schemaB,
	}
	condition := &IsTruthExpression{Expr: &Column{Name: "a.id"}, Operator: "is true"}
	optimized, ok := OptimizeLogicalPlan(&LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions:      []Expression{condition},
	}).(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized plan = %T, want LogicalJoin", OptimizeLogicalPlan(&LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
			Conditions:      []Expression{condition},
		}))
	}
	rightSelection, ok := optimized.Children()[1].(*LogicalSelection)
	if !ok || len(rightSelection.Conditions) != 1 {
		t.Fatalf("right child = %T with %d conditions, want inferred NOT predicate", optimized.Children()[1], func() int {
			if !ok {
				return 0
			}
			return len(rightSelection.Conditions)
		}())
	}
	inferred, ok := rightSelection.Conditions[0].(*IsTruthExpression)
	if !ok {
		t.Fatalf("inferred predicate = %T %v, want IS TRUE expression", rightSelection.Conditions[0], rightSelection.Conditions[0])
	}
	inferredColumn, ok := inferred.Expr.(*Column)
	if !ok || inferredColumn.Name != "b.id" {
		t.Fatalf("inferred boolean-wrapped column = %T %v, want b.id", inferred.Expr, inferred.Expr)
	}
}

func TestPredicatePushdownInfersSingleColumnOrPredicateAcrossOuterJoin(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType: "LEFT",
		Conditions: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "a.id"},
			Right: &Column{Name: "b.id"},
		}},
		LeftSchema: schemaA, RightSchema: schemaB,
	}
	condition := &BinaryOperation{
		Op:    OpOr,
		Left:  &BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Constant{Value: int64(1)}},
		Right: &BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Constant{Value: int64(2)}},
	}
	optimized, ok := OptimizeLogicalPlan(&LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions:      []Expression{condition},
	}).(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized plan = %T, want LogicalJoin", OptimizeLogicalPlan(&LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
			Conditions:      []Expression{condition},
		}))
	}
	rightSelection, ok := optimized.Children()[1].(*LogicalSelection)
	if !ok || len(rightSelection.Conditions) != 1 {
		t.Fatalf("right child = %T with %d conditions, want one inferred OR predicate", optimized.Children()[1], func() int {
			if !ok {
				return 0
			}
			return len(rightSelection.Conditions)
		}())
	}
	inferred, ok := rightSelection.Conditions[0].(*BinaryOperation)
	if !ok || inferred.Op != OpOr {
		t.Fatalf("inferred predicate = %T %v, want OR", rightSelection.Conditions[0], rightSelection.Conditions[0])
	}
	for _, branch := range []Expression{inferred.Left, inferred.Right} {
		comparison, ok := branch.(*BinaryOperation)
		if !ok || comparison.Op != OpEQ {
			t.Fatalf("inferred branch = %T %v, want equality", branch, branch)
		}
		column, ok := comparison.Left.(*Column)
		if !ok || column.Name != "b.id" {
			t.Fatalf("inferred branch column = %T %v, want b.id", comparison.Left, comparison.Left)
		}
	}
}

func TestPredicatePushdownInfersWrappedStructuredPredicatesAcrossOuterJoin(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaA}, Table: tableA},
			&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schemaB}, Table: tableB},
		}},
		JoinType:    "LEFT",
		Conditions:  []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a.id"}, Right: &Column{Name: "b.id"}}},
		LeftSchema:  schemaA,
		RightSchema: schemaB,
	}
	leftConds := []Expression{&InExpression{
		Column: &BinaryOperation{Op: OpAdd, Left: &Column{Name: "a.id"}, Right: &Constant{Value: int64(1)}},
		Values: []interface{}{int64(8), int64(9)},
	}}
	_, rightConds := inferOuterJoinPreservedPredicates(join, leftConds, nil)
	if len(rightConds) != 1 {
		t.Fatalf("inferred right conditions = %d, want 1", len(rightConds))
	}
	inferred, ok := rightConds[0].(*InExpression)
	if !ok {
		t.Fatalf("inferred condition = %T, want *InExpression", rightConds[0])
	}
	inferredColumn, ok := inferred.Column.(*BinaryOperation)
	if !ok || inferredColumn.Op != OpAdd {
		t.Fatalf("inferred IN column = %T %v, want wrapped addition", inferred.Column, inferred.Column)
	}
	column, ok := inferredColumn.Left.(*Column)
	if !ok || column.Name != "b.id" {
		t.Fatalf("inferred wrapped column = %T %v, want b.id", inferredColumn.Left, inferredColumn.Left)
	}
}

// TestPredicatePushdown_SelectionOverAggregation OPT-011：仅含 GROUP BY 列的条件应下推到聚合之下
func TestPredicatePushdown_SelectionOverAggregation(t *testing.T) {
	table := createTestTable()
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	agg := &LogicalAggregation{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		GroupByItems:    []Expression{&Column{Name: "id"}},
		AggFuncs:        []AggregateFunc{&Function{FuncName: "COUNT", FuncArgs: []Expression{&Column{Name: "col1"}}}},
	}
	sel := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{agg}},
		Conditions:      []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}}},
	}
	proj := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{sel}},
		Exprs:           []Expression{&Column{Name: "id"}, &Function{FuncName: "COUNT", FuncArgs: []Expression{&Column{Name: "col1"}}}},
	}
	optimized := OptimizeLogicalPlan(proj)
	// 应存在一层 Selection(id=1) 在 Agg 的子节点上（谓词已下推）
	var foundSelWithIDCond bool
	var visit func(LogicalPlan)
	visit = func(p LogicalPlan) {
		if p == nil {
			return
		}
		if sel, ok := p.(*LogicalSelection); ok && len(sel.Conditions) > 0 {
			cols := collectUsedColumns(sel.Conditions)
			for _, col := range cols {
				if col == "id" {
					foundSelWithIDCond = true
					return
				}
			}
		}
		if _, ok := p.(*LogicalIndexScan); ok {
			foundSelWithIDCond = true
			return
		}
		for _, c := range p.Children() {
			visit(c)
			if foundSelWithIDCond {
				return
			}
		}
	}
	visit(optimized)
	if !foundSelWithIDCond {
		t.Error("expected predicate on id pushed below Aggregation or used as IndexScan (OPT-011)")
	}
}

// TestColumnPruning_ProjectionOverTableScan OPT-012：Proj(仅列 id) over TableScan 后，子计划输出列被裁剪为仅 id
func TestColumnPruning_ProjectionOverTableScan(t *testing.T) {
	table := createTestTable()
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	proj := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		Exprs:           []Expression{&Column{Name: "id"}},
	}
	optimized := OptimizeLogicalPlan(proj)
	// 列裁剪后，找到最底层 TableScan，其 schema 应只含 id
	var findTableScan func(LogicalPlan) *LogicalTableScan
	findTableScan = func(p LogicalPlan) *LogicalTableScan {
		if ts, ok := p.(*LogicalTableScan); ok {
			return ts
		}
		for _, c := range p.Children() {
			if ts := findTableScan(c); ts != nil {
				return ts
			}
		}
		return nil
	}
	ts := findTableScan(optimized)
	if ts == nil {
		t.Fatalf("no TableScan in optimized plan")
	}
	sch := ts.Schema()
	if sch == nil {
		t.Fatalf("TableScan has nil schema")
	}
	tbl, ok := sch.GetTable("test_table")
	if !ok {
		// 可能表名在 schema 里是别的 key
		for _, tbl = range sch.Tables {
			break
		}
	}
	if tbl == nil {
		t.Fatalf("no table in schema")
	}
	if len(tbl.Columns) != 1 {
		t.Errorf("column pruning: expected 1 column in pruned schema, got %d: %v", len(tbl.Columns), tbl.Columns)
	}
	if len(tbl.Columns) >= 1 && tbl.Columns[0].Name != "id" {
		t.Errorf("column pruning: expected column id, got %s", tbl.Columns[0].Name)
	}
}

func TestColumnPruningTranslatesDirectProjectionAlias(t *testing.T) {
	table := createTestTable()
	table.AddColumn(&metadata.Column{Name: "user_id", DataType: metadata.TypeInt})
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	projection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		Exprs:           []Expression{&Column{Name: "id"}},
		OutputNames:     []string{"user_id"},
	}

	optimized := columnPruning(projection, []string{"user_id"})
	optimizedProjection, ok := optimized.(*LogicalProjection)
	if !ok {
		t.Fatalf("optimized plan = %T, want LogicalProjection", optimized)
	}
	optimizedScan, ok := optimizedProjection.Children()[0].(*LogicalTableScan)
	if !ok {
		t.Fatalf("optimized child = %T, want LogicalTableScan", optimizedProjection.Children()[0])
	}
	if got := tableColumnNames(optimizedScan.Schema()); !reflect.DeepEqual(got, []string{"id"}) {
		t.Fatalf("pruned columns = %v, want [id]", got)
	}
}

func TestColumnPruningDropsUnusedComputedProjectionOutputs(t *testing.T) {
	table := createTestTable()
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	projection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		Exprs: []Expression{
			&BinaryOperation{Op: OpAdd, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}},
			&Column{Name: "name"},
		},
		OutputNames: []string{"next_id", "name"},
	}

	optimized := columnPruning(projection, []string{"next_id"})
	optimizedProjection, ok := optimized.(*LogicalProjection)
	if !ok {
		t.Fatalf("optimized plan = %T, want LogicalProjection", optimized)
	}
	if len(optimizedProjection.Exprs) != 1 || len(optimizedProjection.OutputNames) != 1 || optimizedProjection.OutputNames[0] != "next_id" {
		t.Fatalf("projection outputs = %v/%v, want only next_id", optimizedProjection.Exprs, optimizedProjection.OutputNames)
	}
	optimizedScan, ok := optimizedProjection.Children()[0].(*LogicalTableScan)
	if !ok {
		t.Fatalf("optimized child = %T, want LogicalTableScan", optimizedProjection.Children()[0])
	}
	if got := tableColumnNames(optimizedScan.Schema()); !reflect.DeepEqual(got, []string{"id"}) {
		t.Fatalf("pruned columns = %v, want [id]", got)
	}
}

func TestColumnPruningRetainsDistinctProjectionOutputs(t *testing.T) {
	table := createTestTable()
	projection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{&LogicalTableScan{Table: table}}},
		Exprs:           []Expression{&Column{Name: "id"}, &Column{Name: "name"}},
		OutputNames:     []string{"id", "name"},
		Distinct:        true,
	}

	columnPruning(projection, []string{"id"})
	if len(projection.Exprs) != 2 || len(projection.OutputNames) != 2 {
		t.Fatalf("DISTINCT projection outputs = %v/%v, want both outputs retained", projection.Exprs, projection.OutputNames)
	}
}

// TestColumnPruning_ProjectionOverSelectionOverTableScan OPT-012：SELECT name WHERE id=1 需同时保留 id 与 name，不能只留谓词列或只留投影列。
func TestColumnPruning_ProjectionOverSelectionOverTableScan(t *testing.T) {
	table := createTestTable()
	dbSchema := metadata.NewSchema("test")
	if err := dbSchema.AddTable(table); err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	sel := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}},
		},
	}
	proj := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{sel}},
		Exprs:           []Expression{&Column{Name: "name"}},
	}
	optimized := OptimizeLogicalPlan(proj)

	var findLeafScan func(LogicalPlan) (LogicalPlan, bool)
	findLeafScan = func(p LogicalPlan) (LogicalPlan, bool) {
		switch n := p.(type) {
		case *LogicalTableScan, *LogicalIndexScan:
			return n, true
		default:
			for _, c := range p.Children() {
				if leaf, ok := findLeafScan(c); ok {
					return leaf, true
				}
			}
		}
		return nil, false
	}
	leaf, ok := findLeafScan(optimized)
	if !ok {
		t.Fatalf("no TableScan/IndexScan in optimized plan")
	}
	var sch *metadata.DatabaseSchema
	switch s := leaf.(type) {
	case *LogicalTableScan:
		sch = s.Schema()
	case *LogicalIndexScan:
		sch = s.Schema()
	default:
		t.Fatalf("unexpected leaf %T", leaf)
	}
	if sch == nil {
		t.Fatalf("leaf scan has nil schema")
	}
	var tbl *metadata.Table
	for _, tdef := range sch.Tables {
		tbl = tdef
		break
	}
	if tbl == nil {
		t.Fatalf("no table in schema")
	}
	if len(tbl.Columns) != 2 {
		t.Fatalf("expected 2 columns (id,name), got %d", len(tbl.Columns))
	}
	names := []string{tbl.Columns[0].Name, tbl.Columns[1].Name}
	if names[0] > names[1] {
		names[0], names[1] = names[1], names[0]
	}
	if names[0] != "id" || names[1] != "name" {
		t.Errorf("expected columns id+name, got %v %v", tbl.Columns[0].Name, tbl.Columns[1].Name)
	}
}

// TestColumnPruning_TraversesSubquerySubplan P1-OPT-002：子查询的 Subplan
// 不在 LogicalPlan.Children() 中，列裁剪仍必须进入子查询内部。
func TestColumnPruning_TraversesSubquerySubplan(t *testing.T) {
	table := createTestTable()
	dbSchema := metadata.NewSchema("test")
	if err := dbSchema.AddTable(table); err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	subplan := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalSelection{
				BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
				Conditions: []Expression{
					&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}},
				},
			},
		}},
		Exprs: []Expression{&Column{Name: "name"}},
	}
	subquery := &LogicalSubquery{
		SubqueryType: "SCALAR",
		Subplan:      subplan,
	}

	optimized := OptimizeLogicalPlan(subquery)
	optimizedSubquery, ok := optimized.(*LogicalSubquery)
	if !ok {
		t.Fatalf("expected LogicalSubquery, got %T", optimized)
	}
	optimizedScan := findLeafScanInPlan(optimizedSubquery.Subplan)
	if optimizedScan == nil {
		t.Fatal("expected table/index scan inside subquery subplan")
	}
	if got := tableColumnNames(optimizedScan.Schema()); !reflect.DeepEqual(got, []string{"id", "name"}) {
		t.Fatalf("expected subquery scan columns id+name, got %v", got)
	}
}

// TestColumnPruning_ApplyKeepsJoinConditionColumns P1-OPT-002：Apply 的关联
// 条件列必须同时保留在左右子计划，即使它们不在上层投影中。
func TestColumnPruning_ApplyKeepsJoinConditionColumns(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "left_table"
	rightTable := createTestTable()
	rightTable.Name = "right_table"
	leftSchema := metadata.NewSchema("left")
	if err := leftSchema.AddTable(leftTable); err != nil {
		t.Fatalf("AddTable left: %v", err)
	}
	rightSchema := metadata.NewSchema("right")
	if err := rightSchema.AddTable(rightTable); err != nil {
		t.Fatalf("AddTable right: %v", err)
	}
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	right := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
		ApplyType:       "INNER",
		Correlated:      true,
		JoinConds: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Column{Name: "id"}},
		},
	}

	optimized := OptimizeLogicalPlan(apply)
	optimizedApply, ok := optimized.(*LogicalApply)
	if !ok {
		t.Fatalf("expected LogicalApply, got %T", optimized)
	}
	if got := tableColumnNames(optimizedApply.Children()[0].Schema()); !reflect.DeepEqual(got, []string{"id"}) {
		t.Fatalf("expected left apply child to retain join column id, got %v", got)
	}
	if got := tableColumnNames(optimizedApply.Children()[1].Schema()); !reflect.DeepEqual(got, []string{"id"}) {
		t.Fatalf("expected right apply child to retain join column id, got %v", got)
	}
}

func TestPredicatePushdownTraversesSubquerySubplan(t *testing.T) {
	table := createTestTable()
	dbSchema := metadata.NewSchema("test")
	if err := dbSchema.AddTable(table); err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	projection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		Exprs:           []Expression{&Column{Name: "id"}},
	}
	subquery := &LogicalSubquery{
		SubqueryType: "SCALAR",
		Subplan: &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{projection}},
			Conditions: []Expression{
				&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}},
			},
		},
	}

	optimized := pushDownPredicates(subquery)
	optimizedSubquery, ok := optimized.(*LogicalSubquery)
	if !ok {
		t.Fatalf("expected LogicalSubquery, got %T", optimized)
	}
	optimizedProjection, ok := optimizedSubquery.Subplan.(*LogicalProjection)
	if !ok {
		t.Fatalf("expected predicate to move below subquery selection, got %T", optimizedSubquery.Subplan)
	}
	if _, ok := optimizedProjection.Children()[0].(*LogicalSelection); !ok {
		t.Fatalf("expected pushed selection below subquery projection, got %T", optimizedProjection.Children()[0])
	}
}

func TestColumnPruningSubqueryKeepsRequestedOutputColumns(t *testing.T) {
	table := createTestTable()
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	subquery := &LogicalSubquery{
		SubqueryType: "IN",
		Subplan:      scan,
	}

	columnPruning(subquery, []string{"name"})

	got := tableColumnNames(scan.Schema())
	if !reflect.DeepEqual(got, []string{"name"}) {
		t.Fatalf("subquery output pruning retained columns %v, want [name]", got)
	}
}

func TestJoinChildColumnSetsUseUnionVisibleOutputNames(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "union_visible_left"
	rightTable := createTestTable()
	rightTable.Name = "union_visible_right"
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	rightScan := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	rightProjection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{rightScan}},
		Exprs:           []Expression{&Column{Name: "union_visible_right.id"}},
		OutputNames:     []string{"union_visible_key"},
	}
	union := &LogicalUnion{BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{rightProjection}}}
	join := &LogicalJoin{BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, union}}}

	_, rightSet := getJoinChildColumnSets(join)
	if !columnInJoinSet(rightSet, "union_visible_key") {
		t.Fatalf("UNION right ownership set = %#v, want visible alias union_visible_key", rightSet)
	}
	if columnInJoinSet(rightSet, "union_visible_right.id") {
		t.Fatalf("UNION right ownership set exposed hidden physical column: %#v", rightSet)
	}
}

func TestJoinChildColumnSetsUseSubqueryVisibleOutputNames(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "subquery_visible_left"
	rightTable := createTestTable()
	rightTable.Name = "subquery_visible_right"
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	rightScan := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	rightProjection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{rightScan}},
		Exprs:           []Expression{&Column{Name: "subquery_visible_right.id"}},
		OutputNames:     []string{"subquery_visible_key"},
	}
	rightSubquery := &LogicalSubquery{SubqueryType: "IN", Subplan: rightProjection}
	join := &LogicalJoin{BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, rightSubquery}}}

	_, rightSet := getJoinChildColumnSets(join)
	if !columnInJoinSet(rightSet, "subquery_visible_key") {
		t.Fatalf("subquery right ownership set = %#v, want visible alias subquery_visible_key", rightSet)
	}
	if columnInJoinSet(rightSet, "subquery_visible_right.id") {
		t.Fatalf("subquery right ownership set exposed hidden physical column: %#v", rightSet)
	}
}

func TestJoinChildColumnSetsUseCTEVisibleOutputNames(t *testing.T) {
	physicalTable := createTestTable()
	physicalTable.Name = "cte_physical_source"
	scan := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: physicalTable.Schema}, Table: physicalTable}

	tests := []struct {
		name string
		plan LogicalPlan
	}{
		{
			name: "cte",
			plan: &LogicalCTE{
				Name:    "recent_rows",
				Columns: []string{"cte_visible_id"},
				Query:   scan,
			},
		},
		{
			name: "recursive_cte",
			plan: &LogicalRecursiveCTE{
				Name:      "recent_rows",
				Columns:   []string{"cte_visible_id"},
				Anchor:    scan,
				Recursive: scan,
			},
		},
		{
			name: "cte_statement",
			plan: &LogicalCTEStatement{
				Definitions: []LogicalPlan{&LogicalCTE{
					Name:    "recent_rows",
					Columns: []string{"cte_visible_id"},
					Query:   scan,
				}},
				Body: &LogicalCTEScan{
					Name:    "recent_rows",
					Columns: []string{"cte_visible_id"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leftTable := createTestTable()
			leftTable.Name = "cte_visible_left"
			left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
			join := &LogicalJoin{BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, tt.plan}}}

			_, rightSet := getJoinChildColumnSets(join)
			if !columnInJoinSet(rightSet, "cte_visible_id") {
				t.Fatalf("%s right ownership set = %#v, want visible alias cte_visible_id", tt.name, rightSet)
			}
			if columnInJoinSet(rightSet, "cte_physical_source.id") {
				t.Fatalf("%s right ownership set exposed hidden physical column: %#v", tt.name, rightSet)
			}
		})
	}
}

func TestPredicatePushdownInfersOuterApplyPredicateFromInnerSelection(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "apply_inner_inference_left"
	rightTable := createTestTable()
	rightTable.Name = "apply_inner_inference_right"
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	rightScan := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	right := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{rightScan}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "apply_inner_inference_right.id"},
			Right: &Constant{Value: int64(7)},
		}},
	}
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
		ApplyType:       "INNER",
		Correlated:      true,
		JoinConds: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "apply_inner_inference_left.id"},
			Right: &Column{Name: "apply_inner_inference_right.id"},
		}},
	}

	optimized, ok := pushDownPredicates(apply).(*LogicalApply)
	if !ok {
		t.Fatalf("optimized Apply = %T, want *LogicalApply", optimized)
	}
	leftSelection, ok := optimized.Children()[0].(*LogicalSelection)
	if !ok || len(leftSelection.Conditions) != 1 {
		t.Fatalf("left Apply child = %T with %d conditions, want one inferred condition", optimized.Children()[0], func() int {
			if ok {
				return len(leftSelection.Conditions)
			}
			return 0
		}())
	}
	inferred, ok := leftSelection.Conditions[0].(*BinaryOperation)
	if !ok || inferred.Op != OpEQ {
		t.Fatalf("inferred left Apply predicate = %T %v, want id = 7", leftSelection.Conditions[0], leftSelection.Conditions[0])
	}
	column, ok := inferred.Left.(*Column)
	if !ok || !strings.EqualFold(column.Name, "apply_inner_inference_left.id") {
		t.Fatalf("inferred left Apply column = %T %v, want apply_inner_inference_left.id", inferred.Left, inferred.Left)
	}
}

func TestPredicatePushdownInfersOuterApplyPredicateThroughProjection(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "apply_projection_inference_left"
	rightTable := createTestTable()
	rightTable.Name = "apply_projection_inference_right"
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	rightScan := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	rightSelection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{rightScan}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "apply_projection_inference_right.id"},
			Right: &Constant{Value: int64(9)},
		}},
	}
	right := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{rightSelection}},
		Exprs:           []Expression{&Column{Name: "apply_projection_inference_right.id"}},
		OutputNames:     []string{"apply_projection_inference_right_key"},
	}
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
		ApplyType:       "INNER",
		Correlated:      true,
		JoinConds: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "apply_projection_inference_left.id"},
			Right: &Column{Name: "apply_projection_inference_right_key"},
		}},
	}

	optimized, ok := pushDownPredicates(apply).(*LogicalApply)
	if !ok {
		t.Fatalf("optimized Apply = %T, want *LogicalApply", optimized)
	}
	leftSelection, ok := optimized.Children()[0].(*LogicalSelection)
	if !ok || len(leftSelection.Conditions) != 1 {
		t.Fatalf("left Apply child = %T with %d conditions, want one inferred condition", optimized.Children()[0], func() int {
			if ok {
				return len(leftSelection.Conditions)
			}
			return 0
		}())
	}
	inferred, ok := leftSelection.Conditions[0].(*BinaryOperation)
	if !ok || inferred.Op != OpEQ {
		t.Fatalf("inferred left Apply predicate = %T %v, want id = 9", leftSelection.Conditions[0], leftSelection.Conditions[0])
	}
	column, ok := inferred.Left.(*Column)
	if !ok || !strings.EqualFold(column.Name, "apply_projection_inference_left.id") {
		t.Fatalf("inferred left Apply column = %T %v, want apply_projection_inference_left.id", inferred.Left, inferred.Left)
	}
}

func TestPredicatePushdownInfersApplyPredicateThroughSemiSubquery(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "apply_subquery_inference_left"
	rightTable := createTestTable()
	rightTable.Name = "apply_subquery_inference_right"
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	rightScan := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	rightSelection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{rightScan}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "apply_subquery_inference_right.id"},
			Right: &Constant{Value: int64(11)},
		}},
	}
	rightSubquery := &LogicalSubquery{
		SubqueryType: "IN",
		Subplan:      rightSelection,
	}
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, rightSubquery}},
		ApplyType:       "SEMI",
		Correlated:      true,
		JoinConds: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "apply_subquery_inference_left.id"},
			Right: &Column{Name: "apply_subquery_inference_right.id"},
		}},
	}

	optimized, ok := pushDownPredicates(apply).(*LogicalApply)
	if !ok {
		t.Fatalf("optimized Apply = %T, want *LogicalApply", optimized)
	}
	leftSelection, ok := optimized.Children()[0].(*LogicalSelection)
	if !ok || len(leftSelection.Conditions) != 1 {
		t.Fatalf("left Apply child = %T with %d conditions, want one inferred condition", optimized.Children()[0], func() int {
			if ok {
				return len(leftSelection.Conditions)
			}
			return 0
		}())
	}
	inferred, ok := leftSelection.Conditions[0].(*BinaryOperation)
	if !ok || inferred.Op != OpEQ {
		t.Fatalf("inferred left Apply predicate = %T %v, want id = 11", leftSelection.Conditions[0], leftSelection.Conditions[0])
	}
	column, ok := inferred.Left.(*Column)
	if !ok || !strings.EqualFold(column.Name, "apply_subquery_inference_left.id") {
		t.Fatalf("inferred left Apply column = %T %v, want apply_subquery_inference_left.id", inferred.Left, inferred.Left)
	}
}

func TestPredicatePushdownInfersAllNestedApplySelections(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "apply_nested_selection_left"
	rightTable := createTestTable()
	rightTable.Name = "apply_nested_selection_right"
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	rightScan := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	inner := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{rightScan}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpGT,
			Left:  &Column{Name: "apply_nested_selection_right.id"},
			Right: &Constant{Value: int64(3)},
		}},
	}
	right := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{inner}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpLT,
			Left:  &Column{Name: "apply_nested_selection_right.id"},
			Right: &Constant{Value: int64(10)},
		}},
	}
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
		ApplyType:       "INNER",
		Correlated:      true,
		JoinConds: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "apply_nested_selection_left.id"},
			Right: &Column{Name: "apply_nested_selection_right.id"},
		}},
	}

	optimized, ok := pushDownPredicates(apply).(*LogicalApply)
	if !ok {
		t.Fatalf("optimized Apply = %T, want *LogicalApply", optimized)
	}
	leftSelection, ok := optimized.Children()[0].(*LogicalSelection)
	if !ok || len(leftSelection.Conditions) != 2 {
		t.Fatalf("left Apply child = %T with %d conditions, want both nested predicates", optimized.Children()[0], func() int {
			if ok {
				return len(leftSelection.Conditions)
			}
			return 0
		}())
	}
	ops := make(map[BinaryOp]bool)
	for _, expression := range leftSelection.Conditions {
		condition, ok := expression.(*BinaryOperation)
		if !ok {
			t.Fatalf("inferred condition = %T %v, want binary predicate", expression, expression)
		}
		ops[condition.Op] = true
	}
	if !ops[OpGT] || !ops[OpLT] {
		t.Fatalf("inferred operators = %#v, want both > and <", ops)
	}
}

func TestPredicatePushdownRewritesNestedSelectionProjectionAliases(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "apply_nested_alias_left"
	rightTable := createTestTable()
	rightTable.Name = "apply_nested_alias_right"
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	rightScan := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	inner := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{rightScan}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpGT,
			Left:  &Column{Name: "apply_nested_alias_right.id"},
			Right: &Constant{Value: int64(3)},
		}},
	}
	projection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{inner}},
		Exprs:           []Expression{&Column{Name: "apply_nested_alias_right.id"}},
		OutputNames:     []string{"apply_nested_alias_right_key"},
	}
	right := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{projection}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpLT,
			Left:  &Column{Name: "apply_nested_alias_right_key"},
			Right: &Constant{Value: int64(10)},
		}},
	}
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
		ApplyType:       "INNER",
		Correlated:      true,
		JoinConds: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "apply_nested_alias_left.id"},
			Right: &Column{Name: "apply_nested_alias_right_key"},
		}},
	}

	optimized := pushDownPredicates(apply).(*LogicalApply)
	leftSelection, ok := optimized.Children()[0].(*LogicalSelection)
	if !ok || len(leftSelection.Conditions) != 2 {
		t.Fatalf("left Apply child = %T with %d conditions, want two rewritten predicates", optimized.Children()[0], func() int {
			if ok {
				return len(leftSelection.Conditions)
			}
			return 0
		}())
	}
	for _, expression := range leftSelection.Conditions {
		condition, ok := expression.(*BinaryOperation)
		if !ok {
			t.Fatalf("inferred condition = %T %v, want binary predicate", expression, expression)
		}
		column, ok := condition.Left.(*Column)
		if !ok || !strings.EqualFold(column.Name, "apply_nested_alias_left.id") {
			t.Fatalf("inferred column = %T %v, want apply_nested_alias_left.id", condition.Left, condition.Left)
		}
	}
}

func TestPredicatePushdownSplitsApplyChildPredicates(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "apply_left"
	rightTable := createTestTable()
	rightTable.Name = "apply_right"
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	right := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
		ApplyType:       "INNER",
		JoinConds: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "apply_left.id"}, Right: &Column{Name: "apply_right.id"}},
		},
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{apply}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "apply_left.col1"}, Right: &Constant{Value: int64(1)}},
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "apply_right.col2"}, Right: &Constant{Value: int64(2)}},
		},
	}

	optimized, ok := pushDownPredicates(selection).(*LogicalApply)
	if !ok {
		t.Fatalf("expected all INNER Apply predicates to be pushed, got %T", optimized)
	}
	if _, ok := optimized.Children()[0].(*LogicalSelection); !ok {
		t.Fatalf("expected left Apply child selection, got %T", optimized.Children()[0])
	}
	if _, ok := optimized.Children()[1].(*LogicalSelection); !ok {
		t.Fatalf("expected right Apply child selection, got %T", optimized.Children()[1])
	}

	leftJoin := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
		ApplyType:       "LEFT",
	}
	leftSelection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{leftJoin}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "apply_left.col1"}, Right: &Constant{Value: int64(1)}},
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "apply_right.col2"}, Right: &Constant{Value: int64(2)}},
		},
	}
	optimizedLeft, ok := pushDownPredicates(leftSelection).(*LogicalSelection)
	if !ok {
		t.Fatalf("expected LEFT Apply right predicate to remain above Apply, got %T", optimizedLeft)
	}
	if _, ok := optimizedLeft.Children()[0].(*LogicalApply); !ok {
		t.Fatalf("expected selection to retain LEFT Apply boundary, got %T", optimizedLeft.Children()[0])
	}
}

func TestPredicatePushdownInfersInnerApplyCorrelationPredicate(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "apply_infer_left"
	rightTable := createTestTable()
	rightTable.Name = "apply_infer_right"
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	right := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
		ApplyType:       "INNER",
		Correlated:      true,
		JoinConds: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "apply_infer_left.id"}, Right: &Column{Name: "apply_infer_right.id"}},
		},
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{apply}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpGT, Left: &Column{Name: "apply_infer_left.id"}, Right: &Constant{Value: int64(10)}},
		},
	}

	optimized, ok := pushDownPredicates(selection).(*LogicalApply)
	if !ok {
		t.Fatalf("expected INNER Apply selection to be removed, got %T", selection)
	}
	rightSelection, ok := optimized.Children()[1].(*LogicalSelection)
	if !ok {
		t.Fatalf("expected inferred predicate on right Apply child, got %T", optimized.Children()[1])
	}
	if len(rightSelection.Conditions) != 1 {
		t.Fatalf("right Apply child conditions = %d, want 1", len(rightSelection.Conditions))
	}
	inferred, ok := rightSelection.Conditions[0].(*BinaryOperation)
	if !ok || inferred.Op != OpGT {
		t.Fatalf("inferred right Apply predicate = %T %v, want id > 10", rightSelection.Conditions[0], rightSelection.Conditions[0])
	}
	column, ok := inferred.Left.(*Column)
	if !ok || !strings.EqualFold(column.Name, "apply_infer_right.id") {
		t.Fatalf("inferred right Apply column = %T %v, want apply_infer_right.id", inferred.Left, inferred.Left)
	}

	leftApply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
		ApplyType:       "LEFT",
		Correlated:      true,
		JoinConds: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "apply_infer_left.id"}, Right: &Column{Name: "apply_infer_right.id"}},
		},
	}
	leftSelection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{leftApply}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpGT, Left: &Column{Name: "apply_infer_left.id"}, Right: &Constant{Value: int64(10)}},
		},
	}
	optimizedLeft, ok := pushDownPredicates(leftSelection).(*LogicalApply)
	if !ok {
		t.Fatalf("expected LEFT Apply to retain Apply boundary, got %T", leftSelection)
	}
	if _, ok := optimizedLeft.Children()[1].(*LogicalTableScan); !ok {
		t.Fatalf("LEFT Apply nullable child was inferred unexpectedly: %T", optimizedLeft.Children()[1])
	}
}

func TestPredicatePushdownInfersInnerApplyWithConjunctiveCorrelation(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "apply_conjunctive_left"
	rightTable := createTestTable()
	rightTable.Name = "apply_conjunctive_right"
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	right := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
		ApplyType:       "INNER",
		Correlated:      true,
		JoinConds: []Expression{&BinaryOperation{
			Op:    OpAnd,
			Left:  &BinaryOperation{Op: OpEQ, Left: &Column{Name: "apply_conjunctive_left.id"}, Right: &Column{Name: "apply_conjunctive_right.id"}},
			Right: &BinaryOperation{Op: OpEQ, Left: &Column{Name: "apply_conjunctive_left.col1"}, Right: &Column{Name: "apply_conjunctive_right.col1"}},
		}},
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{apply}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpGT, Left: &Column{Name: "apply_conjunctive_left.id"}, Right: &Constant{Value: int64(10)}},
		},
	}

	optimized, ok := pushDownPredicates(selection).(*LogicalApply)
	if !ok {
		t.Fatalf("expected INNER Apply selection to be removed, got %T", selection)
	}
	rightSelection, ok := optimized.Children()[1].(*LogicalSelection)
	if !ok || len(rightSelection.Conditions) != 1 {
		t.Fatalf("right Apply child = %T with %d conditions, want one inferred condition", optimized.Children()[1], func() int {
			if ok {
				return len(rightSelection.Conditions)
			}
			return 0
		}())
	}
	inferred, ok := rightSelection.Conditions[0].(*BinaryOperation)
	if !ok || inferred.Op != OpGT {
		t.Fatalf("inferred right Apply predicate = %T %v, want id > 10", rightSelection.Conditions[0], rightSelection.Conditions[0])
	}
	column, ok := inferred.Left.(*Column)
	if !ok || !strings.EqualFold(column.Name, "apply_conjunctive_right.id") {
		t.Fatalf("inferred right Apply column = %T %v, want apply_conjunctive_right.id", inferred.Left, inferred.Left)
	}
}

func TestPredicatePushdownInfersNullSafeApplyCorrelation(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "apply_nullsafe_left"
	rightTable := createTestTable()
	rightTable.Name = "apply_nullsafe_right"
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	right := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
		ApplyType:       "INNER",
		Correlated:      true,
		JoinConds: []Expression{&BinaryOperation{
			Op:    OpNullSafeEQ,
			Left:  &Column{Name: "apply_nullsafe_left.id"},
			Right: &Column{Name: "apply_nullsafe_right.id"},
		}},
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{apply}},
		Conditions:      []Expression{&IsNullExpression{Column: &Column{Name: "apply_nullsafe_left.id"}}},
	}

	optimized, ok := pushDownPredicates(selection).(*LogicalApply)
	if !ok {
		t.Fatalf("expected INNER Apply selection to be removed, got %T", selection)
	}
	rightSelection, ok := optimized.Children()[1].(*LogicalSelection)
	if !ok || len(rightSelection.Conditions) != 1 {
		t.Fatalf("right Apply child = %T with %d conditions, want one inferred condition", optimized.Children()[1], func() int {
			if ok {
				return len(rightSelection.Conditions)
			}
			return 0
		}())
	}
	inferred, ok := rightSelection.Conditions[0].(*IsNullExpression)
	if !ok {
		t.Fatalf("inferred right Apply predicate = %T %v, want IS NULL", rightSelection.Conditions[0], rightSelection.Conditions[0])
	}
	column, ok := inferred.Column.(*Column)
	if !ok || !strings.EqualFold(column.Name, "apply_nullsafe_right.id") {
		t.Fatalf("inferred right Apply column = %T %v, want apply_nullsafe_right.id", inferred.Column, inferred.Column)
	}
}

func TestPredicatePushdownInfersApplyFunctionWrappedCorrelation(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "apply_function_left"
	rightTable := createTestTable()
	rightTable.Name = "apply_function_right"
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftTable.Schema}, Table: leftTable}
	right := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightTable.Schema}, Table: rightTable}
	joinKey := func(table string) Expression {
		return &Function{FuncName: "ABS", FuncArgs: []Expression{&Column{Name: table + ".id"}}}
	}
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
		ApplyType:       "INNER",
		Correlated:      true,
		JoinConds: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  joinKey("apply_function_left"),
			Right: joinKey("apply_function_right"),
		}},
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{apply}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpGT,
			Left:  joinKey("apply_function_left"),
			Right: &Constant{Value: int64(10)},
		}},
	}

	optimized, ok := pushDownPredicates(selection).(*LogicalApply)
	if !ok {
		t.Fatalf("expected INNER Apply selection to be removed, got %T", selection)
	}
	rightSelection, ok := optimized.Children()[1].(*LogicalSelection)
	if !ok || len(rightSelection.Conditions) != 1 {
		t.Fatalf("right Apply child = %T with %d conditions, want one inferred condition", optimized.Children()[1], func() int {
			if ok {
				return len(rightSelection.Conditions)
			}
			return 0
		}())
	}
	inferred, ok := rightSelection.Conditions[0].(*BinaryOperation)
	if !ok || inferred.Op != OpGT {
		t.Fatalf("inferred right Apply predicate = %T %v, want ABS(id) > 10", rightSelection.Conditions[0], rightSelection.Conditions[0])
	}
	inferredFunction, ok := inferred.Left.(*Function)
	if !ok || !strings.EqualFold(inferredFunction.FuncName, "ABS") {
		t.Fatalf("inferred right Apply expression = %T %v, want ABS", inferred.Left, inferred.Left)
	}
	inferredColumn, ok := inferredFunction.FuncArgs[0].(*Column)
	if !ok || !strings.EqualFold(inferredColumn.Name, "apply_function_right.id") {
		t.Fatalf("inferred right Apply column = %T %v, want apply_function_right.id", inferredFunction.FuncArgs[0], inferredFunction.FuncArgs[0])
	}
}

func TestPredicatePushdownThroughUnionBranches(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "union_filter_left"
	rightTable := createTestTable()
	rightTable.Name = "union_filter_right"
	leftSchema := metadata.NewSchema("left")
	rightSchema := metadata.NewSchema("right")
	if err := leftSchema.AddTable(leftTable); err != nil {
		t.Fatalf("add left table: %v", err)
	}
	if err := rightSchema.AddTable(rightTable); err != nil {
		t.Fatalf("add right table: %v", err)
	}
	union := &LogicalUnion{BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
		&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftSchema}, Table: leftTable},
		&LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightSchema}, Table: rightTable},
	}}}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{union}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "col1"},
			Right: &Constant{Value: int64(1)},
		}},
	}

	optimized, ok := pushDownPredicates(selection).(*LogicalUnion)
	if !ok {
		t.Fatalf("expected selection over UNION to be removed, got %T", optimized)
	}
	for index, child := range optimized.Children() {
		if _, ok := child.(*LogicalSelection); !ok {
			t.Fatalf("UNION branch %d = %T, want pushed selection", index, child)
		}
	}
}

func TestIndexAccessOptimizationTraversesSubquerySubplan(t *testing.T) {
	table := createTestTable()
	dbSchema := metadata.NewSchema("test")
	if err := dbSchema.AddTable(table); err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	subquery := &LogicalSubquery{
		SubqueryType: "SCALAR",
		Subplan: &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
			Conditions: []Expression{
				&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}},
			},
		},
	}

	optimized := OptimizeLogicalPlan(subquery)
	optimizedSubquery, ok := optimized.(*LogicalSubquery)
	if !ok {
		t.Fatalf("expected LogicalSubquery, got %T", optimized)
	}
	optimizedSelection, ok := optimizedSubquery.Subplan.(*LogicalSelection)
	if !ok {
		t.Fatalf("expected subquery predicate to remain above index access, got %T", optimizedSubquery.Subplan)
	}
	if _, ok := optimizedSelection.Children()[0].(*LogicalIndexScan); !ok {
		t.Fatalf("expected subquery predicate to use index access, got %T", optimizedSelection.Children()[0])
	}
}

func TestExpressionNormalizationTraversesSubquerySubplan(t *testing.T) {
	subquery := &LogicalSubquery{
		SubqueryType: "SCALAR",
		Subplan: &LogicalSelection{
			Conditions: []Expression{&NotExpression{Operand: &NotExpression{Operand: &BinaryOperation{
				Op:    OpGT,
				Left:  &Column{Name: "id"},
				Right: &Constant{Value: int64(0)},
			}}}},
		},
	}

	optimized := normalizeExpressions(subquery)
	optimizedSubquery, ok := optimized.(*LogicalSubquery)
	if !ok {
		t.Fatalf("expected LogicalSubquery, got %T", optimized)
	}
	selection, ok := optimizedSubquery.Subplan.(*LogicalSelection)
	if !ok || len(selection.Conditions) != 1 {
		t.Fatalf("expected normalized selection in subquery, got %T", optimizedSubquery.Subplan)
	}
	if _, ok := selection.Conditions[0].(*BinaryOperation); !ok {
		t.Fatalf("expected double negation removed in subquery, got %T", selection.Conditions[0])
	}
}

func TestAggregationEliminationTraversesSubquerySubplan(t *testing.T) {
	table := createTestTable()
	dbSchema := metadata.NewSchema("test")
	if err := dbSchema.AddTable(table); err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	subquery := &LogicalSubquery{
		SubqueryType: "SCALAR",
		Subplan: &LogicalAggregation{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
			AggFuncs: []AggregateFunc{&Function{
				FuncName: "MAX",
				FuncArgs: []Expression{&Column{Name: "id"}},
			}},
		},
	}

	optimized := OptimizeLogicalPlan(subquery)
	optimizedSubquery, ok := optimized.(*LogicalSubquery)
	if !ok {
		t.Fatalf("expected LogicalSubquery, got %T", optimized)
	}
	if _, ok := optimizedSubquery.Subplan.(*LogicalProjection); !ok {
		t.Fatalf("expected MAX aggregation to be eliminated inside subquery, got %T", optimizedSubquery.Subplan)
	}
}

func TestMinMaxProjectionSimplificationTraversesSubquerySubplan(t *testing.T) {
	column := &Column{Name: "id"}
	innerProjection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{&LogicalTableScan{Table: createTestTable()}}},
		Exprs:           []Expression{column},
	}
	outerProjection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{innerProjection}},
		Exprs: []Expression{&Function{
			FuncName: "MAX",
			FuncArgs: []Expression{&Column{Name: "id"}},
		}},
	}
	subquery := &LogicalSubquery{SubqueryType: "SCALAR", Subplan: outerProjection}

	optimized := simplifyProjMinMaxRoot(subquery)
	optimizedSubquery, ok := optimized.(*LogicalSubquery)
	if !ok {
		t.Fatalf("expected LogicalSubquery, got %T", optimized)
	}
	if optimizedSubquery.Subplan != innerProjection {
		t.Fatalf("expected MIN/MAX wrapper to be removed inside subquery, got %T", optimizedSubquery.Subplan)
	}
}

func TestExpressionNormalizationCoversApplyJoinConditions(t *testing.T) {
	apply := &LogicalApply{
		JoinConds: []Expression{&NotExpression{Operand: &NotExpression{Operand: &BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "left_id"},
			Right: &Column{Name: "right_id"},
		}}}},
	}

	normalized := normalizeExpressions(apply)
	normalizedApply, ok := normalized.(*LogicalApply)
	if !ok || len(normalizedApply.JoinConds) != 1 {
		t.Fatalf("expected normalized LogicalApply, got %T", normalized)
	}
	if _, ok := normalizedApply.JoinConds[0].(*BinaryOperation); !ok {
		t.Fatalf("expected double negation removed from Apply join condition, got %T", normalizedApply.JoinConds[0])
	}
}

func findLeafScanInPlan(plan LogicalPlan) LogicalPlan {
	switch plan.(type) {
	case *LogicalTableScan, *LogicalIndexScan:
		return plan
	}
	if plan == nil {
		return nil
	}
	for _, child := range plan.Children() {
		if scan := findLeafScanInPlan(child); scan != nil {
			return scan
		}
	}
	if subquery, ok := plan.(*LogicalSubquery); ok {
		return findLeafScanInPlan(subquery.Subplan)
	}
	return nil
}

func tableColumnNames(schema *metadata.DatabaseSchema) []string {
	if schema == nil || len(schema.Tables) == 0 {
		return nil
	}
	var table *metadata.Table
	for _, candidate := range schema.Tables {
		table = candidate
		break
	}
	if table == nil {
		return nil
	}
	names := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		names = append(names, column.Name)
	}
	sort.Strings(names)
	return names
}
