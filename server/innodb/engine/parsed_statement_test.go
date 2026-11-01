package engine

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestEnsureParsedStatementContextIsolation(t *testing.T) {
	executor := &XMySQLExecutor{}
	parent := &ExecutionContext{Context: context.Background()}
	child := &ExecutionContext{Context: context.Background()}

	parentParsed, err := executor.ensureParsedStatement(parent, "SELECT 1")
	if err != nil {
		t.Fatalf("parent parse error = %v", err)
	}
	parentAgain, err := executor.ensureParsedStatement(parent, "SELECT 1")
	if err != nil {
		t.Fatalf("parent reuse error = %v", err)
	}
	if parentAgain != parentParsed {
		t.Fatal("same context did not reuse its immutable parse result")
	}

	childParsed, err := executor.ensureParsedStatement(child, "SELECT 1")
	if err != nil {
		t.Fatalf("child parse error = %v", err)
	}
	if childParsed == parentParsed {
		t.Fatal("child context reused the parent parse result")
	}
	if parent.RawQuery != "SELECT 1" || child.RawQuery != "SELECT 1" {
		t.Fatalf("raw queries = %q and %q", parent.RawQuery, child.RawQuery)
	}
}

func TestEnsureParsedStatementRejectsUnsupportedWithoutCaching(t *testing.T) {
	executor := &XMySQLExecutor{}
	ctx := &ExecutionContext{Context: context.Background()}
	if _, err := executor.ensureParsedStatement(ctx, "CALL app.p(1)"); err == nil {
		t.Fatal("CALL unexpectedly entered the staged parser")
	}
	if ctx.Parsed != nil || ctx.RawQuery != "" {
		t.Fatalf("failed parse changed context: parsed=%#v raw=%q", ctx.Parsed, ctx.RawQuery)
	}
}

func TestParsedRoutingAuthorizationLockConsistency(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("WITH q AS (SELECT * FROM app.source) UPDATE app.target SET id = id + 1 WHERE id IN (SELECT id FROM q)")
	if err != nil {
		t.Fatal(err)
	}
	tables, writes, ok := statementTableAccessesFromParsed(parsed, "app")
	if !ok || !writes || len(tables) != 2 {
		t.Fatalf("tables=%v writes=%v ok=%v", tables, writes, ok)
	}
	refs, err := sqlparser.CollectObjectReferences(parsed.AST, "app")
	if err != nil || len(refs) != 2 {
		t.Fatalf("refs=%v err=%v", refs, err)
	}
}
