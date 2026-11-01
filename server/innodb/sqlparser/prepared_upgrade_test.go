package sqlparser

import (
	"errors"
	"testing"
)

func TestPreparedUpgradeStructuredStatement(t *testing.T) {
	parsed, err := ParseForExecution("PREPARE s FROM 'SELECT 1'")
	if err != nil {
		t.Fatalf("ParseForExecution() error = %v", err)
	}
	statement, ok := parsed.AST.(*PreparedStatement)
	if !ok || statement.Kind != "prepare" || statement.Name != "s" || statement.Source != "SELECT 1" {
		t.Fatalf("prepared = %#v", parsed.AST)
	}
	if statement.SourceExpression != "'SELECT 1'" {
		t.Fatalf("source expression = %q", statement.SourceExpression)
	}
}

func TestPreparedUpgradeRejectsTrailingTokens(t *testing.T) {
	_, err := ParseForExecution("PREPARE s FROM 'SELECT 1' trailing")
	if !errors.Is(err, ErrInvalidStatement) {
		t.Fatalf("error = %v, want ErrInvalidStatement", err)
	}
}
