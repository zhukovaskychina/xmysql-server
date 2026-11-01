package sqlparser

import (
	"strings"
	"testing"
)

func TestExtendedAdminUpgrade(t *testing.T) {
	cases := []struct{ query, family string }{
		{"PREPARE p FROM 'select 1'", string(StatementFamilyPrepared)},
		{"EXECUTE p USING @a, @b", string(StatementFamilyPrepared)},
		{"HANDLER OPEN app.t", string(StatementFamilyHandler)},
		{"HANDLER OPEN app.t AS h", string(StatementFamilyHandler)},
		{"HANDLER READ h FIRST LIMIT 2", string(StatementFamilyHandler)},
		{"INSTALL COMPONENT 'file://component/a'", string(StatementFamilyComponent)},
		{"INSTALL COMPONENT 'file://component/a', 'file://component/b'", string(StatementFamilyComponent)},
	}
	for _, tc := range cases {
		parsed, err := ParseForExecution(tc.query)
		if err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		if string(parsed.Family) != tc.family {
			t.Fatalf("%s family=%s", tc.query, parsed.Family)
		}
		if handler, ok := parsed.AST.(*HandlerStatement); ok && strings.Contains(tc.query, " AS ") && handler.Alias != "h" {
			t.Fatalf("handler alias=%q", handler.Alias)
		}
		if component, ok := parsed.AST.(*ComponentStatement); ok && strings.Contains(tc.query, ",") && len(component.URNs) != 2 {
			t.Fatalf("component URNs=%v", component.URNs)
		}
	}
}
