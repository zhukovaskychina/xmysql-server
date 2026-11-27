package engine

import (
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestStructuredAlterOperationsMapToMetadataOperations(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("ALTER TABLE app.t ADD COLUMN c INT, ADD INDEX idx_c(c)")
	if err != nil {
		t.Fatalf("ParseForExecution() error = %v", err)
	}
	stmt := parsed.AST.(*sqlparser.AlterTable)
	operations, err := structuredAlterOperations(stmt)
	if err != nil {
		t.Fatalf("structuredAlterOperations() error = %v", err)
	}
	if len(operations) != 2 {
		t.Fatalf("operations = %#v", operations)
	}
	if operations[0].Kind != AlterAddColumn || operations[0].NewName != "c" || operations[0].Definition != "INT" {
		t.Fatalf("column operation = %#v", operations[0])
	}
	if operations[1].Kind != AlterAddIndex || operations[1].IndexName != "idx_c" || len(operations[1].Columns) != 1 || operations[1].Columns[0] != "c" {
		t.Fatalf("index operation = %#v", operations[1])
	}
}

func TestStructuredAlterOperationsDoNotFlattenConstraintsIntoIndexes(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("ALTER TABLE app.child ADD FOREIGN KEY fk_parent(parent_id) REFERENCES app.parent(id)")
	if err != nil {
		t.Fatalf("ParseForExecution() error = %v", err)
	}
	_, err = structuredAlterOperations(parsed.AST.(*sqlparser.AlterTable))
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "unsupported") {
		t.Fatalf("foreign key unexpectedly entered compact index adapter: %v", err)
	}
}

func TestStructuredAlterConstraintKindsRemainTyped(t *testing.T) {
	for _, query := range []string{
		"ALTER TABLE app.t ADD PRIMARY KEY (id)",
		"ALTER TABLE app.t ADD CONSTRAINT ck CHECK (id > 0) NOT ENFORCED",
		"ALTER TABLE app.t DROP CHECK ck",
		"ALTER TABLE app.t DROP PRIMARY KEY",
	} {
		parsed, err := sqlparser.ParseForExecution(query)
		if err != nil {
			t.Fatal(err)
		}
		op := parsed.AST.(*sqlparser.AlterTable).Operations[0]
		if strings.EqualFold(op.ConstraintType, "primary") && op.Kind == sqlparser.AlterOperationAddConstraint {
			t.Fatalf("primary key unexpectedly treated as generic constraint: %#v", op)
		}
		if strings.Contains(strings.ToLower(query), "check") && op.ConstraintType != "check" {
			t.Fatalf("check operation=%#v", op)
		}
	}
}

func TestAlterASTFailureIsAtomic(t *testing.T) {
	metadata := map[string]interface{}{
		"columns": []interface{}{map[string]interface{}{"name": "id", "type": "INT"}},
		"indexes": []interface{}{},
	}
	before := cloneAlterMetadataMap(metadata)
	failed := []AlterOperation{{Kind: AlterAddColumn, NewName: "c", Definition: "INT"}, {Kind: AlterDropColumn, OldName: "missing"}}
	if err := applyAlterOperationsWithForeignKeyChecks(metadata, failed, true); err == nil {
		t.Fatal("expected second operation to fail")
	}
	if len(metadata["columns"].([]interface{})) != len(before["columns"].([]interface{})) {
		t.Fatalf("metadata partially mutated: %#v", metadata)
	}
}
