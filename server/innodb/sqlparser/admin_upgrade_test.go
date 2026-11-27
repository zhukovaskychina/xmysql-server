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

func TestAdminTableLockModesAndEmptyChannel(t *testing.T) {
	parsed, err := ParseForExecution("LOCK TABLES app.t READ LOCAL, app.other WRITE")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*AdminStatement)
	if len(stmt.TargetModes) != 2 || stmt.TargetModes[0] != "read" || stmt.TargetModes[1] != "write" {
		t.Fatalf("modes=%v", stmt.TargetModes)
	}
	for _, query := range []string{"LOCK TABLES t", "LOCK TABLES t READ,", "UNLOCK TABLES t"} {
		if _, err := ParseForExecution(query); err == nil {
			t.Fatalf("accepted %s", query)
		}
	}
	parsed, err = ParseForExecution("START REPLICA FOR CHANNEL ''")
	if err != nil {
		t.Fatal(err)
	}
	stmt = parsed.AST.(*AdminStatement)
	if !stmt.ChannelSpecified || stmt.Channel != "" {
		t.Fatalf("channel=%#v", stmt)
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
		{"CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1' FOR CHANNEL 'analytics'", "change", "source", "analytics", "source_host"},
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

func TestAdminFlushAndBinlogBoundaries(t *testing.T) {
	parsed, err := ParseForExecution("FLUSH /* tables, fake */ LOCAL TABLES `app`.`a,b`, app.other WITH READ LOCK")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*AdminStatement)
	if len(stmt.Targets) != 2 || stmt.Targets[0].Name.String() != "a,b" || stmt.Options["with_read_lock"] != "true" {
		t.Fatalf("flush=%#v", stmt)
	}
	parsed, err = ParseForExecution("PURGE BINARY LOGS TO 'binlog.''003'")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.AST.(*AdminStatement).ID != "binlog.'003" {
		t.Fatal("quoted target was not decoded")
	}
	for _, query := range []string{
		"PURGE TABLE LOGS TO 'x'", "PURGE BINARY LOGS 'x'", "PURGE BINARY LOGS TO x",
		"RESET BINARY LOGS GTIDS", "RESET BINARY LOGS AND GTIDS TO", "RESET REPLICA ALL ALL",
		"FLUSH TABLES t, WITH READ LOCK", "FLUSH TABLES t READ LOCK", "FLUSH TABLES t,",
	} {
		if _, err := ParseForExecution(query); err == nil {
			t.Fatalf("accepted %q", query)
		}
	}
}

func TestAdminMaintenanceASTPreservesIdentifiers(t *testing.T) {
	parsed, err := ParseForExecution("ANALYZE LOCAL TABLE `db`.`a,b` UPDATE HISTOGRAM ON `c)d`, comment WITH 12 BUCKETS")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*AdminStatement)
	if len(stmt.Targets) != 1 || stmt.Targets[0].Name.String() != "a,b" || stmt.Histogram == nil || stmt.Histogram.Buckets != "12" || len(stmt.Histogram.Columns) != 2 || stmt.Histogram.Columns[0].String() != "c)d" || stmt.Histogram.Columns[1].String() != "comment" {
		t.Fatalf("maintenance=%#v", stmt)
	}
	for _, query := range []string{
		"ANALYZE TABLE t UPDATE HISTOGRAM ON", "ANALYZE TABLE t DROP HISTOGRAM ON c WITH 3 BUCKETS",
		"CHECK TABLE t,", "CHECK TABLE 't'", "ANALYZE TABLE t UPDATE HISTOGRAM ON c,",
	} {
		if _, err := ParseForExecution(query); err == nil {
			t.Fatalf("accepted %q", query)
		}
	}
}
