package engine

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestStatementTableAccessesFromParsedUsesAST(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("INSERT INTO app.target SELECT * FROM source")
	if err != nil {
		t.Fatalf("ParseForExecution() error = %v", err)
	}
	tables, writes, ok := statementTableAccessesFromParsed(parsed, "app")
	if !ok {
		t.Fatal("statementTableAccessesFromParsed() did not accept parsed DML")
	}
	want := []string{"app.source", "app.target"}
	if len(tables) != len(want) || tables[0] != want[0] || tables[1] != want[1] {
		t.Fatalf("tables = %#v, want %#v", tables, want)
	}
	if !writes {
		t.Fatal("writes = false, want true")
	}
}

func TestStatementTableAccessesFromParsedRejectsNil(t *testing.T) {
	if tables, writes, ok := statementTableAccessesFromParsed(nil, "app"); ok || writes || tables != nil {
		t.Fatalf("nil result = %#v, %v, %v", tables, writes, ok)
	}
}
