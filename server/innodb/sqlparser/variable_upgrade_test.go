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

func TestPersistVariableASTModeAndLiteralBoundaries(t *testing.T) {
	for _, query := range []string{
		"SET\nPERSIST_ONLY max_connections=101",
		"SET /* persist 'fake' */ PERSIST_ONLY\tmax_connections=101",
	} {
		parsed, err := ParseForExecution(query)
		if err != nil {
			t.Fatal(err)
		}
		stmt := parsed.AST.(*Set)
		if !stmt.Persist || !stmt.PersistOnly || len(stmt.Exprs) != 1 {
			t.Fatalf("set=%#v", stmt)
		}
		if _, err := ParseForExecution(String(stmt)); err != nil {
			t.Fatalf("formatted SET does not parse: %v", err)
		}
	}
	parsed, err := ParseForExecution("SET @message='persist', @other='reset'")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*Set)
	if stmt.Persist || stmt.PersistOnly || stmt.Reset {
		t.Fatalf("literal text changed SET mode: %#v", stmt)
	}
}
