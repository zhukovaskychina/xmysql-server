package engine

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

func TestVolcanoExecutorExecutesPhysicalCTEPlanThroughCTEOperator(t *testing.T) {
	constant := &plan.Constant{Value: int64(7)}
	definitionQuery := &plan.PhysicalValues{Exprs: []plan.Expression{constant}}
	definition := &plan.PhysicalCTE{
		Name: "recent",
	}
	definition.SetChildren([]plan.PhysicalPlan{definitionQuery})
	body := &plan.PhysicalCTEScan{Name: "recent"}
	statement := &plan.PhysicalCTEStatement{DefinitionCount: 1}
	statement.SetChildren([]plan.PhysicalPlan{definition, body})

	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	if err := executor.BuildFromPhysicalPlan(context.Background(), statement); err != nil {
		t.Fatalf("build physical CTE plan failed: %v", err)
	}
	rows, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("execute physical CTE plan failed: %v", err)
	}
	if len(rows) != 1 || rows[0].GetValueByIndex(0).Int() != 7 {
		t.Fatalf("expected one materialized CTE row with value 7, got %v", rows)
	}
}

func TestVolcanoExecutorExecutesRecursivePhysicalCTEPlan(t *testing.T) {
	one := &plan.Constant{Value: int64(1)}
	anchor := &plan.PhysicalValues{Exprs: []plan.Expression{one}}

	cteScan := &plan.PhysicalCTEScan{Name: "nums"}
	lessThanThree := &plan.BinaryOperation{
		Op:    plan.OpLT,
		Left:  &plan.Column{Name: "n"},
		Right: &plan.Constant{Value: int64(3)},
	}
	selection := &plan.PhysicalSelection{Conditions: []plan.Expression{lessThanThree}}
	selection.SetChildren([]plan.PhysicalPlan{cteScan})
	increment := &plan.BinaryOperation{
		Op:    plan.OpAdd,
		Left:  &plan.Column{Name: "n"},
		Right: &plan.Constant{Value: int64(1)},
	}
	recursive := &plan.PhysicalProjection{Exprs: []plan.Expression{increment}}
	recursive.SetChildren([]plan.PhysicalPlan{selection})

	definition := &plan.PhysicalRecursiveCTE{Name: "nums", Columns: []string{"n"}}
	definition.SetChildren([]plan.PhysicalPlan{anchor, recursive})
	body := &plan.PhysicalCTEScan{Name: "nums"}
	statement := &plan.PhysicalCTEStatement{DefinitionCount: 1}
	statement.SetChildren([]plan.PhysicalPlan{definition, body})

	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	if err := executor.BuildFromPhysicalPlan(context.Background(), statement); err != nil {
		t.Fatalf("build recursive physical CTE plan failed: %v", err)
	}
	rows, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("execute recursive physical CTE plan failed: %v", err)
	}
	values := make([]int64, 0, len(rows))
	for _, row := range rows {
		values = append(values, row.GetValueByIndex(0).Int())
	}
	if len(rows) != 3 || values[0] != 1 || values[1] != 2 || values[2] != 3 {
		t.Fatalf("expected recursive rows [1 2 3], got %v", values)
	}
}
