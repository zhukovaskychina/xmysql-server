package plan

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"testing"
)

type mockInfoSchema struct{ tables map[string]*metadata.Table }

func (m *mockInfoSchema) TableByName(name string) (*metadata.Table, error) {
	if t, ok := m.tables[name]; ok {
		return t, nil
	}
	return nil, fmt.Errorf("table %s not found", name)
}

func createTestTable2() *metadata.Table {
	table := &metadata.Table{
		Name: "users",
		Columns: []*metadata.Column{
			{Name: "id", DataType: metadata.TypeBigInt},
			{Name: "name", DataType: metadata.TypeVarchar},
		},
	}
	schema := metadata.NewSchema("test")
	schema.AddTable(table)
	return table
}

func TestBuildLogicalPlan_GroupBy(t *testing.T) {
	table := createTestTable2()
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	_ = schema // avoid unused
	info := &mockInfoSchema{tables: map[string]*metadata.Table{"users": table}}

	stmt, err := sqlparser.Parse("SELECT name, COUNT(id) FROM users GROUP BY name")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	sel := stmt.(*sqlparser.Select)

	planNode, err := BuildLogicalPlan(sel, info)
	if err != nil {
		t.Fatalf("build plan error: %v", err)
	}

	proj, ok := planNode.(*LogicalProjection)
	if !ok {
		t.Fatalf("expected projection")
	}
	agg, ok := proj.Children()[0].(*LogicalAggregation)
	if !ok {
		t.Fatalf("expected aggregation node")
	}
	if len(agg.GroupByItems) != 1 || len(agg.AggFuncs) != 1 {
		t.Fatalf("unexpected agg content")
	}
}
