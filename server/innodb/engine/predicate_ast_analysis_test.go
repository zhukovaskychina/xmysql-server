package engine

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestPredicateASTDependencyScope(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("SELECT * FROM t WHERE amount > 1 AND note = 'amount'")
	if err != nil {
		t.Fatal(err)
	}
	selectStmt := parsed.AST.(*sqlparser.Select)
	columns := predicateASTColumns(selectStmt.Where.Expr)
	if len(columns) != 2 || columns[0] != "amount" || columns[1] != "note" {
		t.Fatalf("columns=%v", columns)
	}
}

func TestPredicateASTAnalysisConservativePruning(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("SELECT * FROM t WHERE amount > 1")
	if err != nil {
		t.Fatal(err)
	}
	if !predicateASTAllowsPruning(parsed.AST.(*sqlparser.Select).Where.Expr) {
		t.Fatal("simple predicate should be eligible")
	}
	parsed, err = sqlparser.ParseForExecution("SELECT * FROM t WHERE unknown_fn(amount) = 1")
	if err != nil {
		t.Fatal(err)
	}
	if predicateASTAllowsPruning(parsed.AST.(*sqlparser.Select).Where.Expr) {
		t.Fatal("unknown function must keep full scan")
	}
}
