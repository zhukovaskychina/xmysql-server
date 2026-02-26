package plan

import (
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
