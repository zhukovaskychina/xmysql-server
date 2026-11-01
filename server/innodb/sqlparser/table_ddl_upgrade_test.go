package sqlparser

import (
	"errors"
	"testing"
)

func TestTableDDLUpgradePreservesDefinitions(t *testing.T) {
	parsed, err := ParseForExecution("CREATE TABLE app.dst LIKE app.src")
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := parsed.AST.(*TableDDLStatement)
	if !ok || stmt.Like.Name.String() != "src" || stmt.Like.Qualifier.String() != "app" {
		t.Fatalf("unexpected LIKE AST: %#v", stmt)
	}
	if len(stmt.Targets) != 1 || stmt.Targets[0].Name.String() != "dst" {
		t.Fatalf("unexpected target: %#v", stmt.Targets)
	}

	parsed, err = ParseForExecution("DROP TABLE app.a, app.b")
	if err != nil {
		t.Fatal(err)
	}
	stmt = parsed.AST.(*TableDDLStatement)
	if len(stmt.Targets) != 2 || stmt.Targets[1].Name.String() != "b" {
		t.Fatalf("unexpected drop targets: %#v", stmt.Targets)
	}

	parsed, err = ParseForExecution("RENAME TABLE app.a TO app.b, app.c TO app.d")
	if err != nil {
		t.Fatal(err)
	}
	stmt = parsed.AST.(*TableDDLStatement)
	if len(stmt.Targets) != 2 || len(stmt.RenameTo) != 2 || stmt.RenameTo[1].Name.String() != "d" {
		t.Fatalf("unexpected rename targets: %#v", stmt)
	}

	parsed, err = ParseForExecution("CREATE TABLE app.generated (id INT, total INT GENERATED ALWAYS AS (id + 1) STORED) PARTITION BY RANGE (id)")
	if err != nil {
		t.Fatal(err)
	}
	stmt = parsed.AST.(*TableDDLStatement)
	if stmt.Definition == "" || stmt.PartitionSQL == "" {
		t.Fatalf("definition fragments were lost: %#v", stmt)
	}
}

func TestTableDDLRejectsTrailingTokens(t *testing.T) {
	_, err := ParseForExecution("DROP TABLE app.t totally_invalid_tail")
	if !errors.Is(err, ErrInvalidStatement) {
		t.Fatalf("expected invalid statement, got %v", err)
	}
}
