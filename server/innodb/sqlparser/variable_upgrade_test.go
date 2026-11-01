package sqlparser

import "testing"

func TestVariableUpgradeScopeAndCollation(t *testing.T) {
	parsed, err := ParseForExecution("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*Set)
	if stmt.NamesCharset != "utf8mb4" || stmt.NamesCollation != "utf8mb4_bin" {
		t.Fatalf("set options=%#v", stmt)
	}
	parsed, err = ParseForExecution("SET PERSIST_ONLY max_connections = 100")
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.AST.(*Set).Persist {
		t.Fatalf("persist flag not retained: %#v", parsed.AST)
	}
}
