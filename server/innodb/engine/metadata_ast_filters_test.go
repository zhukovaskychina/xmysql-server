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
