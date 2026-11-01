package engine

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestQueryASTRewriteScopeAndLiterals(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("WITH q AS (SELECT 'from t' AS text FROM app.t) SELECT text FROM q WHERE text = 'select'")
	if err != nil {
		t.Fatal(err)
	}
	rewritten, err := rewriteStatementAST(parsed.AST)
	if err != nil {
		t.Fatal(err)
	}
	if sqlparser.String(rewritten) != sqlparser.String(parsed.AST) {
		t.Fatalf("rewrite changed SQL: %s != %s", sqlparser.String(rewritten), sqlparser.String(parsed.AST))
	}
}

func TestQueryASTRewriteNullAndMultiplicity(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("SELECT id FROM app.t WHERE id IN (1, 2) UNION ALL SELECT id FROM app.t WHERE id IS NULL")
	if err != nil {
		t.Fatal(err)
	}
	rewritten, err := rewriteStatementAST(parsed.AST)
	if err != nil {
		t.Fatal(err)
	}
	if sqlparser.String(rewritten) == "" {
		t.Fatal("empty rewritten UNION")
	}
}
