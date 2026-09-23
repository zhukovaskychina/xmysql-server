package plan

import (
	"fmt"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type ctePlanInfoSchema struct {
	table *metadata.Table
}

func (s ctePlanInfoSchema) TableByName(name string) (*metadata.Table, error) {
	if name == s.table.Name {
		return s.table, nil
	}
	return nil, &missingPlanTableError{name: name}
}

type missingPlanTableError struct{ name string }

func (e *missingPlanTableError) Error() string { return "missing table: " + e.name }

func newCTEPlanInfoSchema() ctePlanInfoSchema {
	schema := metadata.NewSchema("app")
	table := metadata.NewTable("users")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar})
	_ = schema.AddTable(table)
	return ctePlanInfoSchema{table: table}
}

func TestBuildLogicalPlanStatementBindsNonRecursiveCTEToScopeScan(t *testing.T) {
	stmt, err := sqlparser.Parse("WITH recent AS (SELECT id FROM users) SELECT id FROM recent")
	if err != nil {
		t.Fatalf("parse CTE failed: %v", err)
	}
	logical, err := BuildLogicalPlanStatement(stmt, newCTEPlanInfoSchema())
	if err != nil {
		t.Fatalf("build CTE plan failed: %v", err)
	}
	cteStatement, ok := logical.(*LogicalCTEStatement)
	if !ok || len(cteStatement.Definitions) != 1 {
		t.Fatalf("expected one LogicalCTE definition, got %T with %d definitions", logical, len(cteStatement.Definitions))
	}
	if _, ok := cteStatement.Definitions[0].(*LogicalCTE); !ok {
		t.Fatalf("expected LogicalCTE definition, got %T", cteStatement.Definitions[0])
	}
	projection, ok := cteStatement.Body.(*LogicalProjection)
	if !ok || len(projection.Children()) != 1 {
		t.Fatalf("expected projected CTE body, got %T", cteStatement.Body)
	}
	if _, ok := projection.Children()[0].(*LogicalCTEScan); !ok {
		t.Fatalf("expected CTE scan in body, got %T", projection.Children()[0])
	}
	physical := ConvertToPhysicalPlan(logical)
	if _, ok := physical.(*PhysicalCTEStatement); !ok {
		t.Fatalf("expected PhysicalCTEStatement, got %T", physical)
	}
}

func TestCTEScanOutputUsesExplicitColumnAliases(t *testing.T) {
	stmt, err := sqlparser.Parse("WITH recent (recent_id) AS (SELECT id FROM users) SELECT recent_id FROM recent")
	if err != nil {
		t.Fatalf("parse aliased CTE failed: %v", err)
	}
	logical, err := BuildLogicalPlanStatement(stmt, newCTEPlanInfoSchema())
	if err != nil {
		t.Fatalf("build aliased CTE plan failed: %v", err)
	}
	statement := logical.(*LogicalCTEStatement)
	body := statement.Body.(*LogicalProjection)
	scan, ok := body.Children()[0].(*LogicalCTEScan)
	if !ok {
		t.Fatalf("body child = %T, want *LogicalCTEScan", body.Children()[0])
	}
	if got, want := getPlanOutputColumnNames(scan), []string{"recent_id"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("CTE scan output columns = %v, want explicit aliases %v", got, want)
	}
	columnPruning(scan, []string{"recent_id"})
	prunedTable, ok := scan.Schema().GetTable("users")
	if !ok || len(prunedTable.Columns) != 1 || prunedTable.Columns[0].Name != "id" {
		t.Fatalf("pruned aliased CTE scan schema = %#v, want underlying id column", prunedTable)
	}
}

func TestBuildLogicalPlanStatementBindsRecursiveCTEAnchorAndMember(t *testing.T) {
	stmt, err := sqlparser.Parse("WITH RECURSIVE nums (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 3) SELECT n FROM nums")
	if err != nil {
		t.Fatalf("parse recursive CTE failed: %v", err)
	}
	logical, err := BuildLogicalPlanStatement(stmt, newCTEPlanInfoSchema())
	if err != nil {
		t.Fatalf("build recursive CTE plan failed: %v", err)
	}
	cteStatement := logical.(*LogicalCTEStatement)
	if _, ok := cteStatement.Definitions[0].(*LogicalRecursiveCTE); !ok {
		t.Fatalf("expected LogicalRecursiveCTE, got %T", cteStatement.Definitions[0])
	}
	physical := ConvertToPhysicalPlan(logical)
	if _, ok := physical.(*PhysicalCTEStatement); !ok {
		t.Fatalf("expected physical recursive CTE statement, got %T", physical)
	}
}
