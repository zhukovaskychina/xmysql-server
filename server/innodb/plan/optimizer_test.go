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
			Name: "MAX",
			Args: []Expression{&Column{Name: "id"}},
		}},
	}

	proj := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{agg}},
		Exprs:           []Expression{&Function{Name: "MAX", Args: []Expression{&Column{Name: "id"}}}},
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
			Name: "MIN",
			Args: []Expression{&Column{Name: "id"}},
		}},
	}

	proj := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{agg}},
		Exprs:           []Expression{&Function{Name: "MIN", Args: []Expression{&Column{Name: "id"}}}},
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
