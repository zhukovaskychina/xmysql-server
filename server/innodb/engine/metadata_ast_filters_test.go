package engine

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestMetadataASTBooleanPredicates(t *testing.T) {
	stmt, err := sqlparser.ParseForExecution("SELECT * FROM information_schema.tables WHERE (table_schema = 'app' AND table_name IN ('a','b')) OR table_name IS NULL")
	if err != nil {
		t.Fatal(err)
	}
	selectStmt := stmt.AST.(*sqlparser.Select)
	keep, err := evaluateMetadataPredicate(selectStmt.Where.Expr, map[string]interface{}{"table_schema": "app", "table_name": "b"})
	if err != nil || !keep {
		t.Fatalf("expected row to pass, keep=%v err=%v", keep, err)
	}
	keep, err = evaluateMetadataPredicate(selectStmt.Where.Expr, map[string]interface{}{"table_schema": "other", "table_name": "x"})
	if err != nil || keep {
		t.Fatalf("expected row to be filtered, keep=%v err=%v", keep, err)
	}
}

func TestMetadataASTProjectionAndWriteTargets(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{"INSERT INTO information_schema.tables (table_name) VALUES ('x')", "write"},
		{"UPDATE information_schema.tables SET table_name = 'x' WHERE table_schema = 'app'", "write"},
		{"DELETE FROM information_schema.tables WHERE table_name = 'x'", "write"},
	} {
		parsed, err := sqlparser.ParseForExecution(tc.query)
		if err != nil {
			t.Fatal(err)
		}
		refs, err := sqlparser.CollectObjectReferences(parsed.AST, "information_schema")
		if err != nil || len(refs) != 1 || string(refs[0].Access) != tc.want {
			t.Fatalf("%s refs=%v err=%v", tc.query, refs, err)
		}
	}
}

func TestPerformanceSchemaTableReferencesTrustSuccessfulASTParse(t *testing.T) {
	if refs := performanceSchemaTableReferences("SELECT 'from app.orders' AS text_value", "app"); len(refs) != 0 {
		t.Fatalf("literal produced table references: %#v", refs)
	}
	refs := performanceSchemaTableReferences("SELECT * FROM app.orders", "app")
	if len(refs) != 1 || refs[0][0] != "app" || refs[0][1] != "orders" {
		t.Fatalf("refs=%#v", refs)
	}
}

func TestMySQLSystemTablePredicateASTScopeAndNulls(t *testing.T) {
	row := map[string]interface{}{"COST_NAME": "row_evaluate_cost", "COST_VALUE": nil, "COMMENT": "none"}
	for _, tc := range []struct {
		query string
		want  bool
	}{
		{"select cost_name from mysql.server_cost where cost_name = 'missing' OR comment = 'none'", true},
		{"select cost_name from mysql.server_cost where cost_name = 'row_evaluate_cost' and cost_value not in (1, NULL)", false},
		{"select cost_name from mysql.server_cost where cost_value is null", true},
		{"select 'cost_name = ''missing''' from mysql.server_cost where cost_name = 'row_evaluate_cost'", true},
		{"select cost_name from mysql.server_cost where not (cost_name = 'missing')", true},
		{"select cost_name from mysql.server_cost where cost_name = 'ROW_EVALUATE_COST'", true},
	} {
		if got := mysqlSystemTableValuesMatch(tc.query, row); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.query, got, tc.want)
		}
	}
}

func TestProcesslistASTPredicatesPreserveQualifiedColumns(t *testing.T) {
	query := "SELECT * FROM information_schema.processlist WHERE ID = 62 AND INFO LIKE '%orders%'"
	expr, err := metadataPredicateFromQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]interface{}{"id": int64(62), "info": "SELECT orders FROM app"}
	keep, err := evaluateMetadataPredicateCaseInsensitive(expr, values)
	if err != nil || !keep {
		t.Fatalf("expr=%s keep=%v err=%v", sqlparser.String(expr), keep, err)
	}
	row := []interface{}{int64(62), "bob", "localhost", "app", "Query", int64(0), "", "SELECT orders FROM app", int64(0), int64(0)}
	if !processlistRowMatchesQuery(row, query, 61) {
		t.Fatal("complete processlist query rejected")
	}
}
