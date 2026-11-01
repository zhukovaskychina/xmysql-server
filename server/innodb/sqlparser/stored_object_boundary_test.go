package sqlparser

import (
	"strings"
	"testing"
)

func TestStoredObjectDefinerAndLiteralBoundaries(t *testing.T) {
	queries := []string{
		"CREATE DEFINER='job''owner'@'localhost' EVENT app.ev ON SCHEDULE EVERY 2 HOUR COMMENT 'begin do at ends preserve' DO SELECT 'do begin'",
		"CREATE\nDEFINER = 'job''owner'@'localhost'\nEVENT app.ev ON SCHEDULE EVERY 2 HOUR COMMENT 'begin do at ends preserve'\nDO SELECT 'do begin'",
	}
	for _, query := range queries {
		parsed, err := ParseForExecution(query)
		if err != nil {
			t.Fatal(err)
		}
		stmt, ok := parsed.AST.(*StoredObjectStatement)
		if !ok {
			t.Fatalf("AST=%T", parsed.AST)
		}
		if stmt.Definer.User != "job'owner" || stmt.Definer.Host != "localhost" || stmt.Name.Qualifier.String() != "app" || stmt.Name.Name.String() != "ev" {
			t.Fatalf("header=%#v", stmt)
		}
		if stmt.ScheduleKind != "EVERY" || stmt.IntervalValue != 2 || stmt.IntervalUnit != "HOUR" || stmt.Starts != "" || stmt.Ends != "" || stmt.OnCompletionPreserve {
			t.Fatalf("schedule=%#v", stmt)
		}
		if stmt.Body != "DO SELECT 'do begin'" {
			t.Fatalf("body=%q", stmt.Body)
		}
	}
}

func TestStoredRoutineAttributesIgnoreBodyAndComments(t *testing.T) {
	parsed, err := ParseForExecution("CREATE DEFINER='owner'@'localhost' FUNCTION app.f(x DECIMAL(10,2)) RETURNS DECIMAL(12,3) NOT DETERMINISTIC READS SQL DATA COMMENT 'begin return modifies sql data' RETURN x")
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := parsed.AST.(*StoredObjectStatement)
	if !ok {
		t.Fatalf("AST=%T", parsed.AST)
	}
	if !strings.EqualFold(stmt.ReturnType, "DECIMAL(12,3)") || stmt.Deterministic || stmt.DataAccess != "READS SQL DATA" || stmt.Body != "RETURN x" || stmt.Comment != "begin return modifies sql data" {
		t.Fatalf("routine=%#v", stmt)
	}
}

func TestStoredCallArgumentsPreserveNestedExpressionsAndLiterals(t *testing.T) {
	parsed, err := ParseForExecution("CALL app.p(concat('a)', ',b'), (1 + 2), @output)")
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := parsed.AST.(*StoredObjectStatement)
	if !ok || len(stmt.Arguments) != 3 {
		t.Fatalf("call=%#v", parsed.AST)
	}
	function, ok := stmt.Arguments[0].(*FuncExpr)
	if !ok || function.Name.String() != "concat" {
		t.Fatalf("argument=%#v", stmt.Arguments[0])
	}
	first := function.Exprs[0].(*AliasedExpr).Expr.(*SQLVal)
	if string(first.Val) != "a)" {
		t.Fatalf("literal=%q", first.Val)
	}
}

func TestStoredCallAndDropRejectTrailingStatements(t *testing.T) {
	for _, query := range []string{"CALL p(); SELECT 2", "CALL p(1 FROM other_table)", "DROP PROCEDURE p extra"} {
		if _, err := ParseForExecution(query); err == nil {
			t.Fatalf("accepted %s", query)
		}
	}
}
