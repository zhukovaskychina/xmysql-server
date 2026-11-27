package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestViewASTExpansionUsesExecutorContext(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("CREATE VIEW app.v AS SELECT * FROM app.t")
	if err != nil {
		t.Fatal(err)
	}
	executor := &XMySQLExecutor{}
	definition, err := executor.expandViewStatement(nil, parsed.AST, "app")
	if err != nil {
		t.Fatal(err)
	}
	if sqlparser.String(definition) == "" {
		t.Fatal("empty definition")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = executor.expandViewStatement(&ExecutionContext{Context: canceled}, parsed.AST, "app")
	if err == nil {
		t.Fatal("canceled view expansion returned success")
	}
}

func TestViewQueryRewriteUsesASTForTableReference(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, "app"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]string{"definition": "SELECT id FROM app.base"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "app", "v.view.json"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	executor := &XMySQLExecutor{conf: &conf.Cfg{DataDir: dataDir}}
	rewritten, err := executor.rewriteViewQuery("SELECT v.id FROM app.v AS v WHERE v.id > 1", "app")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(rewritten), "app.base") || !strings.Contains(strings.ToLower(rewritten), "where v.id > 1") {
		t.Fatalf("AST view rewrite lost definition or predicate: %s", rewritten)
	}
	if strings.Contains(strings.ToLower(rewritten), "from app.v") {
		t.Fatalf("view source table was not replaced: %s", rewritten)
	}
	unchanged, err := executor.rewriteViewQuery("SELECT 'from app.v' AS literal FROM app.base", "app")
	if err != nil {
		t.Fatal(err)
	}
	if unchanged != "" {
		t.Fatalf("literal text was treated as a view reference: %s", unchanged)
	}
}

func TestViewQueryRewriteEmbedsCTEWithAST(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, "app"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]string{"definition": "WITH q AS (SELECT id FROM app.base) SELECT id FROM q"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "app", "v.view.json"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	executor := &XMySQLExecutor{conf: &conf.Cfg{DataDir: dataDir}}
	rewritten, err := executor.rewriteViewQuery("SELECT id FROM app.v", "app")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(rewritten), "with q") || !strings.Contains(strings.ToLower(rewritten), "app.base") {
		t.Fatalf("CTE view rewrite=%s", rewritten)
	}
}
