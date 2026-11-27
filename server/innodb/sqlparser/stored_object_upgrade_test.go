package sqlparser

import "testing"

func TestStoredObjectUpgradeStructuredDefinitions(t *testing.T) {
	cases := []string{
		"CREATE PROCEDURE app.p(IN x INT) BEGIN SELECT x; END",
		"CREATE FUNCTION app.f(x INT) RETURNS INT RETURN x + 1",
		"CREATE TRIGGER app.tr BEFORE INSERT ON app.t FOR EACH ROW SET NEW.c = 1",
		"CREATE EVENT app.ev ON SCHEDULE EVERY 1 DAY DO DELETE FROM app.t",
		"CALL app.p(1)",
		"DROP PROCEDURE app.p",
	}
	for _, query := range cases {
		parsed, err := ParseForExecution(query)
		if err != nil {
			t.Fatalf("ParseForExecution(%q) error = %v", query, err)
		}
		statement, ok := parsed.AST.(*StoredObjectStatement)
		if !ok || statement.Name.Name.String() == "" {
			t.Fatalf("stored object AST for %q = %#v", query, parsed.AST)
		}
	}
	parsed, err := ParseForExecution("CREATE TRIGGER app.tr BEFORE INSERT ON app.t FOR EACH ROW SET NEW.c = 1")
	if err != nil {
		t.Fatalf("trigger parse error = %v", err)
	}
	trigger := parsed.AST.(*StoredObjectStatement)
	if trigger.TriggerTiming != "BEFORE" || trigger.TriggerEvent != "INSERT" || trigger.TargetTable.Name.String() != "t" {
		t.Fatalf("trigger metadata = %#v", trigger)
	}
	parsed, err = ParseForExecution("CREATE EVENT app.ev ON SCHEDULE EVERY 2 HOUR STARTS '2026-01-01 00:00:00' DO SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	event := parsed.AST.(*StoredObjectStatement)
	if event.ScheduleKind != "EVERY" || event.IntervalValue != 2 || event.IntervalUnit != "HOUR" || event.Starts == "" {
		t.Fatalf("event schedule=%#v", event)
	}
}

func TestStoredObjectUpgradeRoutineAttributes(t *testing.T) {
	parsed, err := ParseForExecution("CREATE FUNCTION app.f(x INT) RETURNS INT DETERMINISTIC READS SQL DATA SQL SECURITY INVOKER RETURN x + 1")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*StoredObjectStatement)
	if stmt.ReturnType == "" || !stmt.Deterministic || stmt.Security != "INVOKER" || stmt.Body == "" {
		t.Fatalf("routine=%#v", stmt)
	}
}

func TestStoredObjectUpgradeDefinitions(t *testing.T) {
	queries := []string{
		"CREATE PROCEDURE app.p(IN x INT, OUT y INT) BEGIN SET y = x; END",
		"CREATE FUNCTION app.f(x INT) RETURNS INT DETERMINISTIC RETURN x + 1",
		"CREATE TRIGGER app.tr BEFORE INSERT ON app.t FOR EACH ROW SET NEW.c = 1",
		"CREATE EVENT app.ev ON SCHEDULE EVERY 1 DAY DO SELECT 1",
		"CALL app.p(1)",
		"DROP PROCEDURE app.p",
	}
	for _, query := range queries {
		parsed, err := ParseForExecution(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if _, ok := parsed.AST.(*StoredObjectStatement); !ok {
			t.Fatalf("%s AST=%T", query, parsed.AST)
		}
	}
}
