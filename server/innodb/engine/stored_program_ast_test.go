package engine

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestStoredProgramASTScopeAndHandlers(t *testing.T) {
	block, err := sqlparser.ParseStoredProgramBlock("DECLARE x INT; SET x = 1; BEGIN SET x = 2; END")
	if err != nil {
		t.Fatal(err)
	}
	if len(block.Statements) != 3 || block.Statements[1].AST == nil || block.Statements[2].Children == nil {
		t.Fatalf("unexpected block: %#v", block)
	}
	state := &storedRoutineState{vars: map[string]string{}}
	executor := &XMySQLExecutor{}
	ctx := &ExecutionContext{Context: context.Background()}
	if err := executor.executeStoredProgram(ctx, "app", block, state); err != nil {
		t.Fatal(err)
	}
}

func TestStoredRoutineValueSubstitutionUsesASTScopes(t *testing.T) {
	got := substituteStoredRoutineValues("x + 'x' + t.x", map[string]string{"x": "7"})
	if got != "7 + 'x' + t.x" {
		t.Fatalf("substitution=%q", got)
	}
}

func TestStoredRoutineParameterSubstitutionPreservesSQLLiteralsAndQualifiedColumns(t *testing.T) {
	input := "SET output_value = input_value + 1; SELECT 'input_value', t.input_value, input_value; -- input_value\n"
	got := substituteStoredRoutineParameters(input, map[string]string{"input_value": "7"})
	want := "SET output_value = 7 + 1; SELECT 'input_value', t.input_value, 7; -- input_value\n"
	if got != want {
		t.Fatalf("parameter substitution=%q, want %q", got, want)
	}
}
