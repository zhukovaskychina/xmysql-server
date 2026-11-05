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
	parsed, err = ParseForExecution("RENAME TABLE app.a TO app.b")
	if err != nil {
		t.Fatal(err)
	}
	stmt = parsed.AST.(*TableDDLStatement)
	if len(stmt.Targets) != 1 || len(stmt.RenameTo) != 1 || stmt.Targets[0].Name.String() != "a" || stmt.RenameTo[0].Name.String() != "b" {
		t.Fatalf("unexpected single rename targets: %#v", stmt)
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

func TestCreateSelectASTUsesTokenBoundaries(t *testing.T) {
	for _, query := range []string{
		"CREATE TABLE app.dst\nAS\nSELECT concat('as select', ')') AS label",
		"CREATE TABLE app.dst (label varchar(40) COMMENT 'as select )') IGNORE SELECT concat('as', 'select') AS label",
		"CREATE TABLE app.dst COMMENT 'not as select' AS WITH s AS (SELECT 1 AS n) SELECT n FROM s",
	} {
		parsed, err := ParseForExecution(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		ddl, ok := parsed.AST.(*TableDDLStatement)
		if !ok || ddl.AsSelect == nil {
			t.Fatalf("missing SELECT AST for %s: %#v", query, parsed.AST)
		}
		if _, err := ParseForExecution(String(ddl)); err != nil {
			t.Fatalf("formatted CTAS does not parse: %v", err)
		}
	}
	_, matched, err := ParseCreateTableAsSelect("CREATE TABLE app.dst AS SELECT n FROM")
	if !matched || err == nil {
		t.Fatalf("invalid CTAS source: matched=%v, err=%v", matched, err)
	}
}

func TestCreateIndexVisibilityNativeAST(t *testing.T) {
	stmt, err := ParseStrictDDL("CREATE TABLE t (n int, KEY `i,n` (n) COMMENT 'invisible' VISIBLE, KEY hidden (n) INVISIBLE)")
	if err != nil {
		t.Fatal(err)
	}
	ddl := stmt.(*DDL)
	if ddl.TableSpec == nil || len(ddl.TableSpec.Indexes) != 2 {
		t.Fatalf("definition=%#v", ddl)
	}
	first, second := ddl.TableSpec.Indexes[0], ddl.TableSpec.Indexes[1]
	if first.Info.Name.String() != "i,n" || len(first.Options) != 2 || first.Options[1].Name != "visible" || len(second.Options) != 1 || second.Options[0].Name != "invisible" {
		t.Fatalf("visibility: %#v, %#v", first, second)
	}
	if _, err := ParseStrictDDL(String(ddl)); err != nil {
		t.Fatalf("formatted index options do not parse: %v", err)
	}
}
