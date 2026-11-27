package sqlparser

import (
	"errors"
	"strings"
	"testing"
)

func TestShowUpgradeStructuredOptions(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		wantType   string
		wantSchema string
		wantName   string
		wantDB     string
		wantLike   string
		wantLimit  bool
	}{
		{"create table", "SHOW CREATE TABLE app.t", "create table", "app", "t", "", "", false},
		{"index", "SHOW INDEX FROM app.t", "index", "app", "t", "", "", false},
		{"columns like", "SHOW COLUMNS FROM app.t LIKE 'id'", "columns", "app", "t", "", "id", false},
		{"full tables", "SHOW FULL TABLES FROM app LIKE 'u%'", "tables", "", "", "app", "u%", false},
		{"warnings limit", "SHOW WARNINGS LIMIT 1, 2", "warnings", "", "", "", "", true},
		{"table status database", "SHOW TABLE STATUS FROM app", "table status", "", "", "app", "", false},
		{"triggers database", "SHOW TRIGGERS FROM app", "triggers", "", "", "app", "", false},
		{"procedure status database", "SHOW PROCEDURE STATUS FROM app", "procedure", "", "", "app", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseForExecution(tc.query)
			if err != nil {
				t.Fatalf("ParseForExecution() error = %v", err)
			}
			show, ok := parsed.AST.(*Show)
			if !ok {
				t.Fatalf("AST = %T, want *Show", parsed.AST)
			}
			if show.Type != tc.wantType || show.OnTable.Qualifier.String() != tc.wantSchema || show.OnTable.Name.String() != tc.wantName {
				t.Fatalf("show target = %#v, want type=%q schema=%q name=%q", show, tc.wantType, tc.wantSchema, tc.wantName)
			}
			gotDB := ""
			if show.ShowTablesOpt != nil {
				gotDB = show.ShowTablesOpt.DbName
			}
			if gotDB != tc.wantDB {
				t.Fatalf("database target = %q, want %q", gotDB, tc.wantDB)
			}
			like := ""
			if show.Filter != nil {
				like = show.Filter.Like
			}
			if like != tc.wantLike {
				t.Fatalf("LIKE = %q, want %q", like, tc.wantLike)
			}
			if (show.Limit != nil) != tc.wantLimit {
				t.Fatalf("Limit present = %v, want %v", show.Limit != nil, tc.wantLimit)
			}
		})
	}
}

func TestShowUpgradeRejectsTrailingTokens(t *testing.T) {
	_, err := ParseForExecution("SHOW CREATE TABLE app.t totally_invalid_tail")
	if !errors.Is(err, ErrInvalidStatement) {
		t.Fatalf("error = %v, want ErrInvalidStatement", err)
	}
}

func TestShowUpgradeFormatAndWalk(t *testing.T) {
	parsed, err := ParseForExecution("SHOW WARNINGS LIMIT 1, 2")
	if err != nil {
		t.Fatalf("ParseForExecution() error = %v", err)
	}
	show := parsed.AST.(*Show)
	formatted := String(show)
	if formatted == "" || !strings.Contains(strings.ToLower(formatted), "show warnings") {
		t.Fatalf("formatted SHOW = %q", formatted)
	}
	visited := 0
	if err := Walk(func(node SQLNode) (bool, error) {
		visited++
		return true, nil
	}, show); err != nil {
		t.Fatalf("Walk() error = %v", err)
	}
	if visited < 2 {
		t.Fatalf("visited = %d, want SHOW and LIMIT nodes", visited)
	}
}

func TestExplainUpgradePreservesTargetAndInnerStatement(t *testing.T) {
	cases := []struct {
		query     string
		wantKind  string
		wantTable string
		wantInner bool
	}{
		{"DESCRIBE app.t", "describe", "t", false},
		{"EXPLAIN SELECT 1", "explain", "", true},
	}
	for _, tc := range cases {
		parsed, err := ParseForExecution(tc.query)
		if err != nil {
			t.Fatalf("ParseForExecution(%q) error = %v", tc.query, err)
		}
		explain, ok := parsed.AST.(*ExplainStatement)
		if !ok {
			t.Fatalf("AST = %T, want *ExplainStatement", parsed.AST)
		}
		if explain.Kind != tc.wantKind || explain.Table.Name.String() != tc.wantTable || (explain.Statement != nil) != tc.wantInner {
			t.Fatalf("ExplainStatement = %#v", explain)
		}
	}
}

func TestExplainUpgradeOptions(t *testing.T) {
	parsed, err := ParseForExecution("EXPLAIN ANALYZE FORMAT=JSON SELECT * FROM app.t")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*ExplainStatement)
	if !stmt.Analyze || stmt.ExplainFormat != "json" || stmt.Statement == nil {
		t.Fatalf("explain=%#v", stmt)
	}
}

func TestShowUpgradeReplicationTypeNames(t *testing.T) {
	for _, query := range []string{"SHOW MASTER STATUS", "SHOW SOURCE STATUS", "SHOW SLAVE HOSTS", "SHOW REPLICAS", "SHOW RELAYLOG EVENTS"} {
		parsed, err := ParseForExecution(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if parsed.AST.(*Show).Type == "" {
			t.Fatalf("%s has empty type", query)
		}
	}
}
