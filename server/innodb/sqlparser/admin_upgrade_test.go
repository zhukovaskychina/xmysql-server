package sqlparser

import (
	"errors"
	"testing"
)

func TestAdminUpgradeStructuredOptions(t *testing.T) {
	cases := []struct {
		query string
		kind  string
		id    string
		count int
	}{
		{"REPAIR TABLE app.t QUICK", "repair", "", 1},
		{"KILL QUERY 123", "kill", "123", 0},
		{"LOCK TABLES app.t READ", "lock", "", 1},
	}
	for _, tc := range cases {
		parsed, err := ParseForExecution(tc.query)
		if err != nil {
			t.Fatalf("ParseForExecution(%q) error = %v", tc.query, err)
		}
		admin, ok := parsed.AST.(*AdminStatement)
		if !ok || admin.Kind != tc.kind || admin.ID != tc.id || len(admin.Targets) != tc.count {
			t.Fatalf("admin = %#v", parsed.AST)
		}
	}
}

func TestAdminUpgradeRejectsTrailingTokens(t *testing.T) {
	_, err := ParseForExecution("REPAIR TABLE app.t totally_invalid_tail")
	if !errors.Is(err, ErrInvalidStatement) {
		t.Fatalf("error = %v, want ErrInvalidStatement", err)
	}
}

func TestAdminUpgradeRetainsCommandAndOptions(t *testing.T) {
	cases := []struct {
		query, kind, scope, channel, option string
	}{
		{"ANALYZE TABLE app.t", "analyze", "", "", ""},
		{"REPAIR TABLE app.t QUICK", "repair", "", "", "quick"},
		{"KILL QUERY 123", "kill", "query", "", ""},
		{"START REPLICA FOR CHANNEL 'analytics'", "start", "", "analytics", "replica"},
		{"CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1' FOR CHANNEL 'analytics'", "change", "", "analytics", "source_host"},
		{"PURGE BINARY LOGS TO 'binlog.000003'", "purge", "binary", "", "to"},
	}
	for _, tc := range cases {
		parsed, err := ParseForExecution(tc.query)
		if err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		stmt := parsed.AST.(*AdminStatement)
		if stmt.Kind != tc.kind || stmt.Scope != tc.scope || stmt.Channel != tc.channel {
			t.Fatalf("%s: %#v", tc.query, stmt)
		}
		if tc.option != "" {
			if tc.kind == "start" {
				if stmt.Options[tc.option] == "" {
					t.Fatalf("%s options=%v", tc.query, stmt.Options)
				}
			} else if stmt.Options[tc.option] == "" {
				t.Fatalf("%s options=%v", tc.query, stmt.Options)
			}
		}
	}
}
