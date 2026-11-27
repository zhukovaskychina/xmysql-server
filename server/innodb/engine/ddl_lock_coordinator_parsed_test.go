package engine

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestStatementTableAccessesFromParsedUsesAST(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("INSERT INTO app.target SELECT * FROM source")
	if err != nil {
		t.Fatalf("ParseForExecution() error = %v", err)
	}
	tables, writes, ok := statementTableAccessesFromParsed(parsed, "app")
	if !ok {
		t.Fatal("statementTableAccessesFromParsed() did not accept parsed DML")
	}
	want := []string{"app.source", "app.target"}
	if len(tables) != len(want) || tables[0] != want[0] || tables[1] != want[1] {
		t.Fatalf("tables = %#v, want %#v", tables, want)
	}
	if !writes {
		t.Fatal("writes = false, want true")
	}
}

func TestStatementTableAccessesFromParsedRejectsNil(t *testing.T) {
	if tables, writes, ok := statementTableAccessesFromParsed(nil, "app"); ok || writes || tables != nil {
		t.Fatalf("nil result = %#v, %v, %v", tables, writes, ok)
	}
}

func TestStatementTableLocksLeaveExplicitLockingToCommandHandlers(t *testing.T) {
	for _, query := range []string{
		"LOCK TABLES app.t WRITE", "ALTER TABLE app.t ADD COLUMN n INT",
		"CREATE TRIGGER app.tr BEFORE INSERT ON app.t FOR EACH ROW SET @n=1",
		"CREATE PROCEDURE app.p() SELECT * FROM app.t",
	} {
		parsed, err := sqlparser.ParseForExecution(query)
		if err != nil {
			t.Fatal(err)
		}
		tables, writes, ok := statementTableAccessesFromParsed(parsed, "app")
		if !ok || len(tables) != 0 || writes {
			t.Fatalf("generic statement locks for %s: %v, writes=%v, accepted=%v", query, tables, writes, ok)
		}
	}
}
