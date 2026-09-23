package engine

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

func TestGeneratePhysicalPlanPreservesUnaryChild(t *testing.T) {
	child := &plan.LogicalValues{Exprs: []plan.Expression{&plan.Constant{Value: int64(7)}}}
	projection := &plan.LogicalProjection{Exprs: []plan.Expression{&plan.Constant{Value: int64(8)}}}
	projection.SetChildren([]plan.LogicalPlan{child})

	physical, err := (&XMySQLExecutor{}).generatePhysicalPlan(projection, nil)
	if err != nil {
		t.Fatalf("generatePhysicalPlan() error = %v", err)
	}
	if len(physical.Children()) != 1 {
		t.Fatalf("generated projection children = %d, want 1", len(physical.Children()))
	}
	if _, ok := physical.Children()[0].(*plan.PhysicalValues); !ok {
		t.Fatalf("generated projection child = %T, want *plan.PhysicalValues", physical.Children()[0])
	}
}

func TestGeneratePhysicalProjectionPreservesExecutionProperties(t *testing.T) {
	child := &plan.LogicalValues{Exprs: []plan.Expression{&plan.Constant{Value: int64(7)}}}
	projection := &plan.LogicalProjection{
		Exprs:    []plan.Expression{&plan.Constant{Value: int64(8)}},
		Distinct: true,
		OrderBy:  []plan.ByItem{{Expr: &plan.Constant{Value: int64(8)}, Desc: true}},
		Offset:   2,
		Limit:    3,
		HasLimit: true,
	}
	projection.SetChildren([]plan.LogicalPlan{child})

	physical, err := (&XMySQLExecutor{}).generatePhysicalPlan(projection, nil)
	if err != nil {
		t.Fatalf("generatePhysicalPlan() error = %v", err)
	}
	physicalProjection, ok := physical.(*plan.PhysicalProjection)
	if !ok {
		t.Fatalf("generated projection = %T, want *plan.PhysicalProjection", physical)
	}
	if !physicalProjection.Distinct || len(physicalProjection.OrderBy) != 1 ||
		physicalProjection.Offset != 2 || physicalProjection.Limit != 3 || !physicalProjection.HasLimit {
		t.Fatalf("generated projection properties = distinct %v order %d offset %d limit %d hasLimit %v; want preserved logical properties",
			physicalProjection.Distinct, len(physicalProjection.OrderBy), physicalProjection.Offset,
			physicalProjection.Limit, physicalProjection.HasLimit)
	}
}

func TestGeneratePhysicalSortPreservesChildPlan(t *testing.T) {
	child := &plan.LogicalValues{Exprs: []plan.Expression{&plan.Constant{Value: int64(7)}}}
	logicalSort := &plan.BaseLogicalPlan{}
	logicalSort.SetChildren([]plan.LogicalPlan{child})

	physical, err := (&XMySQLExecutor{}).generatePhysicalSort(logicalSort, nil)
	if err != nil {
		t.Fatalf("generatePhysicalSort() error = %v", err)
	}
	if _, ok := physical.(*plan.PhysicalSort); !ok {
		t.Fatalf("generated sort = %T, want *plan.PhysicalSort", physical)
	}
	if len(physical.Children()) != 1 {
		t.Fatalf("generated sort children = %d, want 1", len(physical.Children()))
	}
	if _, ok := physical.Children()[0].(*plan.PhysicalValues); !ok {
		t.Fatalf("generated sort child = %T, want *plan.PhysicalValues", physical.Children()[0])
	}
}

func TestGeneratePhysicalPlanRejectsUnknownLogicalPlan(t *testing.T) {
	_, err := (&XMySQLExecutor{}).generatePhysicalPlan(&unsupportedLogicalPlan{}, nil)
	if err == nil {
		t.Fatal("generatePhysicalPlan() should reject an unknown logical plan instead of returning a table scan")
	}
}

func TestGeneratePhysicalPlanPreservesJoinAndAggregateChildren(t *testing.T) {
	left := &plan.LogicalValues{Exprs: []plan.Expression{&plan.Constant{Value: int64(1)}}}
	right := &plan.LogicalValues{Exprs: []plan.Expression{&plan.Constant{Value: int64(2)}}}
	join := &plan.LogicalJoin{JoinType: "INNER"}
	join.SetChildren([]plan.LogicalPlan{left, right})

	physicalJoin, err := (&XMySQLExecutor{}).generatePhysicalPlan(join, nil)
	if err != nil {
		t.Fatalf("generate join physical plan error = %v", err)
	}
	if len(physicalJoin.Children()) != 2 {
		t.Fatalf("generated join children = %d, want 2", len(physicalJoin.Children()))
	}

	aggregation := &plan.LogicalAggregation{
		AggFuncs: []plan.AggregateFunc{&plan.Function{FuncName: "COUNT"}},
	}
	aggregation.SetChildren([]plan.LogicalPlan{left})
	physicalAgg, err := (&XMySQLExecutor{}).generatePhysicalPlan(aggregation, nil)
	if err != nil {
		t.Fatalf("generate aggregate physical plan error = %v", err)
	}
	if len(physicalAgg.Children()) != 1 {
		t.Fatalf("generated aggregate children = %d, want 1", len(physicalAgg.Children()))
	}
}

type unsupportedLogicalPlan struct{ plan.BaseLogicalPlan }

func (p *unsupportedLogicalPlan) String() string { return "unsupported" }

var _ plan.LogicalPlan = (*unsupportedLogicalPlan)(nil)
