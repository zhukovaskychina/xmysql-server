package sqlparser

import (
	"errors"
	"testing"
)

func TestParseForExecutionRejectsUnsupportedFamilies(t *testing.T) {
	cases := []string{
		"HANDLER app.t OPEN",
	}
	for _, query := range cases {
		parsed, err := ParseForExecution(query)
		if parsed != nil {
			t.Fatalf("ParseForExecution(%q) returned AST for unsupported statement: %#v", query, parsed)
		}
		if !errors.Is(err, ErrUnsupportedStatement) {
			t.Fatalf("ParseForExecution(%q) error = %v, want ErrUnsupportedStatement", query, err)
		}
	}
}

func TestParseForExecutionRejectsTrailingTokens(t *testing.T) {
	_, err := ParseForExecution("SELECT 1 FROM")
	if !errors.Is(err, ErrInvalidStatement) {
		t.Fatalf("error = %v, want ErrInvalidStatement", err)
	}

	_, err = ParseForExecution("SELECT 1; SELECT 2")
	if !errors.Is(err, ErrInvalidStatement) {
		t.Fatalf("multi-statement error = %v, want ErrInvalidStatement", err)
	}
}

func TestParseForExecutionPreservesSupportedSelectFeatures(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  StatementFamily
		check func(t *testing.T, stmt Statement)
	}{
		{
			name:  "select",
			query: "SELECT 1",
			want:  StatementFamilySelect,
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*Select); !ok {
					t.Fatalf("AST type = %T, want *Select", stmt)
				}
			},
		},
		{
			name:  "cte",
			query: "WITH q AS (SELECT 1 AS id) SELECT * FROM q",
			want:  StatementFamilySelect,
			check: func(t *testing.T, stmt Statement) {
				with, ok := stmt.(*With)
				if !ok || len(with.CTEs) != 1 || with.Body == nil {
					t.Fatalf("AST = %#v, want one CTE with a body", stmt)
				}
			},
		},
		{
			name:  "window",
			query: "SELECT row_number() OVER (ORDER BY id) AS rn FROM app.t",
			want:  StatementFamilySelect,
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*Select); !ok {
					t.Fatalf("AST type = %T, want *Select", stmt)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseForExecution(tc.query)
			if err != nil {
				t.Fatalf("ParseForExecution() error = %v", err)
			}
			if parsed == nil || !parsed.Complete || parsed.Family != tc.want {
				t.Fatalf("parsed = %#v, want complete family %q", parsed, tc.want)
			}
			tc.check(t, parsed.AST)
		})
	}
}

func TestParseForExecutionRetainsRawSQL(t *testing.T) {
	const query = "  /* source */ SELECT 1;  "
	parsed, err := ParseForExecution(query)
	if err != nil {
		t.Fatalf("ParseForExecution() error = %v", err)
	}
	if parsed.RawSQL != query {
		t.Fatalf("RawSQL = %q, want original input %q", parsed.RawSQL, query)
	}
}

func TestParseForExecutionPreservesSetNamesCollation(t *testing.T) {
	parsed, err := ParseForExecution("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
	if err != nil {
		t.Fatalf("ParseForExecution() error = %v", err)
	}
	setStmt, ok := parsed.AST.(*Set)
	if !ok || setStmt.NamesCharset != "utf8mb4" || setStmt.NamesCollation != "utf8mb4_bin" {
		t.Fatalf("SET AST = %#v", parsed.AST)
	}
}

func TestParseForExecutionPreservesExtractSyntax(t *testing.T) {
	parsed, err := ParseForExecution("SELECT EXTRACT(YEAR FROM created_at) FROM app.t")
	if err != nil {
		t.Fatal(err)
	}
	selectStmt, ok := parsed.AST.(*Select)
	if !ok || len(selectStmt.SelectExprs) != 1 {
		t.Fatalf("unexpected AST: %#v", parsed.AST)
	}
	aliased, ok := selectStmt.SelectExprs[0].(*AliasedExpr)
	if !ok {
		t.Fatalf("unexpected select expression: %#v", selectStmt.SelectExprs[0])
	}
	extract, ok := aliased.Expr.(*ExtractExpr)
	if !ok || !extract.Unit.Equal(NewColIdent("year")) {
		t.Fatalf("unexpected EXTRACT AST: %#v", aliased.Expr)
	}
}

func TestClassifyStatementSkipsComments(t *testing.T) {
	family, err := ClassifyStatement("/* leading */ -- comment\n SELECT 1")
	if err != nil {
		t.Fatalf("ClassifyStatement() error = %v", err)
	}
	if family != StatementFamilySelect {
		t.Fatalf("family = %q, want %q", family, StatementFamilySelect)
	}
}
