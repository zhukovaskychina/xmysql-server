package engine

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type bridgeInfoSchema struct{ table *metadata.Table }

func (b bridgeInfoSchema) TableByName(string) (*metadata.Table, error) { return b.table, nil }

func TestEngineOptimizeLogicalPlanUsesPlanOptimizationPipeline(t *testing.T) {
	table := metadata.NewTable("users")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	statement, err := sqlparser.Parse("select id from users where 1 = 1")
	if err != nil {
		t.Fatal(err)
	}
	selectStatement, ok := statement.(*sqlparser.Select)
	if !ok {
		t.Fatalf("expected SELECT, got %T", statement)
	}
	logical, err := plan.BuildLogicalPlan(selectStatement, bridgeInfoSchema{table: table})
	if err != nil {
		t.Fatal(err)
	}

	optimized := OptimizeLogicalPlan(logical)
	var optimizedSelection *plan.LogicalSelection
	var findSelection func(plan.LogicalPlan)
	findSelection = func(current plan.LogicalPlan) {
		if current == nil || optimizedSelection != nil {
			return
		}
		if selection, ok := current.(*plan.LogicalSelection); ok {
			optimizedSelection = selection
			return
		}
		for _, child := range current.Children() {
			findSelection(child)
		}
	}
	findSelection(optimized)
	if optimizedSelection == nil || len(optimizedSelection.Conditions) != 1 {
		t.Fatalf("expected optimized selection with one condition, got %T", optimized)
	}
	constant, ok := optimizedSelection.Conditions[0].(*plan.Constant)
	if !ok || constant.Value != true {
		t.Fatalf("expected constant TRUE after normalization, got %#v", optimizedSelection.Conditions[0])
	}
}
