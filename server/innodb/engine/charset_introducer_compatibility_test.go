package engine

import "testing"

func TestRewriteCharsetIntroducers(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "utf8mb4 literal", query: "SELECT _utf8mb4'兼容' AS value", want: "SELECT '兼容' AS value"},
		{name: "spaced introducer", query: "SELECT _latin1 'legacy'", want: "SELECT 'legacy'"},
		{name: "identifier remains", query: "SELECT _tenant_name FROM users", want: "SELECT _tenant_name FROM users"},
		{name: "quoted introducer remains", query: "SELECT '_utf8mb4x'", want: "SELECT '_utf8mb4x'"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := rewriteCharsetIntroducers(test.query); got != test.want {
				t.Fatalf("rewriteCharsetIntroducers(%q) = %q, want %q", test.query, got, test.want)
			}
		})
	}
}
