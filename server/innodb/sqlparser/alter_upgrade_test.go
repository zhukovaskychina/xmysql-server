package sqlparser

import (
	"errors"
	"strings"
	"testing"
)

func TestAlterUpgradePositionAndModifyFields(t *testing.T) {
	parsed, err := ParseForExecution("ALTER TABLE app.t ADD COLUMN c INT, ADD INDEX idx_c(c)")
	if err != nil {
		t.Fatalf("ParseForExecution() error = %v", err)
	}
	alter, ok := parsed.AST.(*AlterTable)
	if !ok {
		t.Fatalf("AST = %T, want *AlterTable", parsed.AST)
	}
	if alter.Table.Qualifier.String() != "app" || alter.Table.Name.String() != "t" {
		t.Fatalf("table = %#v", alter.Table)
	}
	if len(alter.Operations) != 2 || alter.Operations[0].Kind != AlterOperationAddColumn || alter.Operations[1].Kind != AlterOperationAddIndex {
		t.Fatalf("operations = %#v", alter.Operations)
	}
	if alter.Operations[0].Name != "c" || alter.Operations[1].Name != "idx_c" || len(alter.Operations[1].Columns) != 1 || alter.Operations[1].Columns[0] != "c" {
		t.Fatalf("operation fields = %#v", alter.Operations)
	}
}

func TestAlterUpgradeRejectsTrailingTokens(t *testing.T) {
	_, err := ParseForExecution("ALTER TABLE t MODIFY COLUMN c INT totally_invalid_tail")
	if !errors.Is(err, ErrInvalidStatement) {
		t.Fatalf("error = %v, want ErrInvalidStatement", err)
	}
}

func TestAlterUpgradeConstraintOperations(t *testing.T) {
	parsed, err := ParseForExecution("ALTER TABLE app.t ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES app.parent(id) ON DELETE CASCADE, DROP FOREIGN KEY fk")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*AlterTable)
	if len(stmt.Operations) != 2 || stmt.Operations[0].Kind != AlterOperationAddConstraint || stmt.Operations[1].Kind != AlterOperationDropConstraint {
		t.Fatalf("operations=%#v", stmt.Operations)
	}
	if stmt.Operations[0].ConstraintType != "foreign" || stmt.Operations[0].ReferencedSchema != "app" || stmt.Operations[0].ReferencedTable != "parent" || stmt.Operations[0].OnDelete != "CASCADE" || stmt.Operations[1].ConstraintType != "foreign" {
		t.Fatalf("foreign fields=%#v", stmt.Operations)
	}
}

func TestAlterUpgradeUniqueConstraintFields(t *testing.T) {
	parsed, err := ParseForExecution("ALTER TABLE app.t ADD CONSTRAINT uq UNIQUE (code)")
	if err != nil {
		t.Fatal(err)
	}
	op := parsed.AST.(*AlterTable).Operations[0]
	if op.Kind != AlterOperationAddConstraint || op.ConstraintType != "unique" || op.Name != "uq" || len(op.Columns) != 1 {
		t.Fatalf("op=%#v", op)
	}
}

func TestAlterUpgradePrimaryAndCheckConstraintFields(t *testing.T) {
	for _, query := range []string{
		"ALTER TABLE app.t ADD PRIMARY KEY (id)",
		"ALTER TABLE app.t ADD CONSTRAINT ck CHECK (id > 0) NOT ENFORCED",
		"ALTER TABLE app.t DROP CHECK ck",
		"ALTER TABLE app.t DROP PRIMARY KEY",
	} {
		parsed, err := ParseForExecution(query)
		if err != nil {
			t.Fatal(err)
		}
		op := parsed.AST.(*AlterTable).Operations[0]
		if strings.Contains(strings.ToLower(query), "check") && op.ConstraintType != "check" {
			t.Fatalf("check operation=%#v", op)
		}
		if strings.Contains(strings.ToLower(query), "primary") && op.ConstraintType != "primary" {
			t.Fatalf("primary operation=%#v", op)
		}
	}
}

func TestAlterUpgradePartitionAndTablespaceOptions(t *testing.T) {
	for _, query := range []string{
		"ALTER TABLE app.t TABLESPACE archive DISCARD TABLESPACE",
		"ALTER TABLE app.t PARTITION p0",
		"ALTER TABLE app.t DROP PARTITION p0",
		"ALTER TABLE app.t TRUNCATE PARTITION p0",
		"ALTER TABLE app.t REMOVE PARTITIONING",
		"ALTER TABLE app.t EXCHANGE PARTITION p0 WITH TABLE app.exchange",
		"ALTER TABLE app.t REORGANIZE PARTITION p0 INTO (PARTITION p1 VALUES LESS THAN (10))",
	} {
		parsed, err := ParseForExecution(query)
		if err != nil {
			t.Fatal(err)
		}
		if len(parsed.AST.(*AlterTable).Operations) != 1 {
			t.Fatalf("%s operations=%#v", query, parsed.AST)
		}
		if strings.HasPrefix(strings.ToLower(query), "alter table app.t add partition") && parsed.AST.(*AlterTable).Operations[0].PartitionDefinition == "" {
			t.Fatalf("%s partition definition missing", query)
		}
		if strings.Contains(strings.ToLower(query), "exchange partition") && parsed.AST.(*AlterTable).Operations[0].PartitionExchangeTable == "" {
			t.Fatalf("%s exchange target missing", query)
		}
		if parsed.AST.(*AlterTable).Operations[0].Kind != AlterOperationPartition && !strings.Contains(strings.ToLower(query), "tablespace") {
			t.Fatalf("%s operation=%#v", query, parsed.AST.(*AlterTable).Operations[0])
		}
	}
}

func TestAlterUpgradeIndexVisibility(t *testing.T) {
	for _, query := range []string{"ALTER TABLE app.t ALTER INDEX idx INVISIBLE", "ALTER TABLE app.t ADD INDEX idx (code) INVISIBLE"} {
		parsed, err := ParseForExecution(query)
		if err != nil {
			t.Fatal(err)
		}
		if parsed.AST.(*AlterTable).Operations[0].Visible != "invisible" {
			t.Fatalf("%s op=%#v", query, parsed.AST.(*AlterTable).Operations[0])
		}
	}
}

func TestAlterUpgradeOrderedOperations(t *testing.T) {
	parsed, err := ParseForExecution("ALTER TABLE app.t ADD COLUMN c INT FIRST, ADD INDEX idx_c(c), MODIFY COLUMN c BIGINT AFTER id")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*AlterTable)
	if len(stmt.Operations) != 3 || !stmt.Operations[0].First || stmt.Operations[1].Name != "idx_c" || stmt.Operations[2].After != "id" {
		t.Fatalf("operations=%#v", stmt.Operations)
	}
}
