package plan

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestPredicatePushdownThroughIdentityProjection(t *testing.T) {
	table := metadata.NewTable("orders")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "status", DataType: metadata.TypeVarchar})
	schema := metadata.NewSchema("app")
	if err := schema.AddTable(table); err != nil {
		t.Fatalf("add table: %v", err)
	}
	scan := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schema}, Table: table}
	projection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		Exprs:           []Expression{&Column{Name: "id"}, &Column{Name: "status"}},
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{projection}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(7)}},
		},
	}

	optimized, ok := OptimizeLogicalPlan(selection).(*LogicalProjection)
	if !ok {
		t.Fatalf("optimized plan = %T, want LogicalProjection", OptimizeLogicalPlan(selection))
	}
	if len(optimized.Children()) != 1 {
		t.Fatalf("projection children = %d, want 1", len(optimized.Children()))
	}
	pushed, ok := optimized.Children()[0].(*LogicalSelection)
	if !ok {
		t.Fatalf("projection child = %T, want LogicalSelection", optimized.Children()[0])
	}
	if len(pushed.Conditions) != 1 {
		t.Fatalf("pushed conditions = %d, want 1", len(pushed.Conditions))
	}
	if _, ok := pushed.Children()[0].(*LogicalTableScan); !ok {
		t.Fatalf("pushed selection child = %T, want LogicalTableScan", pushed.Children()[0])
	}
}

func TestPredicatePushdownDoesNotCrossComputedProjection(t *testing.T) {
	table := metadata.NewTable("orders")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	schema := metadata.NewSchema("app")
	if err := schema.AddTable(table); err != nil {
		t.Fatalf("add table: %v", err)
	}
	scan := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schema}, Table: table}
	projection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		Exprs:           []Expression{&BinaryOperation{Op: OpAdd, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}}},
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{projection}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpGT, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(7)}},
		},
	}

	optimized := OptimizeLogicalPlan(selection)
	if _, ok := optimized.(*LogicalSelection); !ok {
		t.Fatalf("optimized computed projection plan = %T, want top LogicalSelection", optimized)
	}
}
