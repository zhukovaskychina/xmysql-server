package engine

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestSQLLikePatternMatchHonorsMySQLEscapeAndCaseRules(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		pattern string
		want    bool
	}{
		{name: "escaped percent is literal", value: "a%b", pattern: `a\%b`, want: true},
		{name: "escaped underscore is literal", value: "a_b", pattern: `a\_b`, want: true},
		{name: "escaped wildcard does not match arbitrary text", value: "axxb", pattern: `a\%b`, want: false},
		{name: "default text matching is case insensitive", value: "Alice", pattern: "a%", want: true},
		{name: "trailing escape matches a literal backslash", value: `a\`, pattern: `a\`, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sqlLikePatternMatch(tt.value, tt.pattern); got != tt.want {
				t.Fatalf("sqlLikePatternMatch(%q, %q) = %v, want %v", tt.value, tt.pattern, got, tt.want)
			}
		})
	}
}

func TestSQLLikePredicateHonorsExplicitEscapeCharacter(t *testing.T) {
	truth, err := evalPredicateTruth(&sqlparser.ComparisonExpr{
		Operator: sqlparser.LikeStr,
		Left:     &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte("a%b")},
		Right:    &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte("a#%b")},
		Escape:   &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte("#")},
	}, nil)
	if err != nil {
		t.Fatalf("LIKE ESCAPE evaluation failed: %v", err)
	}
	if truth != sqlTruthTrue {
		t.Fatalf("LIKE ESCAPE truth = %d, want %d", truth, sqlTruthTrue)
	}
}
