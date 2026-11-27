package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestTriggerEventASTRowAndScheduleSemantics(t *testing.T) {
	trigger, err := sqlparser.ParseForExecution("CREATE TRIGGER app.tr BEFORE UPDATE ON app.t FOR EACH ROW SET NEW.value = OLD.value + 1")
	require.NoError(t, err)
	triggerStmt := trigger.AST.(*sqlparser.StoredObjectStatement)
	require.Equal(t, "BEFORE", triggerStmt.TriggerTiming)
	require.Equal(t, "UPDATE", triggerStmt.TriggerEvent)
	require.Equal(t, "t", triggerStmt.TargetTable.Name.String())
	event, err := sqlparser.ParseForExecution("CREATE EVENT app.ev ON SCHEDULE EVERY 5 MINUTE STARTS '2026-01-01 00:00:00' ENDS '2026-01-02 00:00:00' DO SELECT 1")
	require.NoError(t, err)
	eventStmt := event.AST.(*sqlparser.StoredObjectStatement)
	require.Equal(t, "EVERY", eventStmt.ScheduleKind)
	require.Equal(t, int64(5), eventStmt.IntervalValue)
	require.Equal(t, "MINUTE", eventStmt.IntervalUnit)
	require.NotEmpty(t, eventStmt.Starts)
	require.NotEmpty(t, eventStmt.Ends)
}

func TestStoredDefinitionReloadCompatibility(t *testing.T) {
	definition := "CREATE FUNCTION app.f(x INT) RETURNS INT DETERMINISTIC RETURN x + 1"
	first, err := sqlparser.ParseForExecution(definition)
	require.NoError(t, err)
	second, err := sqlparser.ParseForExecution(first.RawSQL)
	require.NoError(t, err)
	require.Equal(t, sqlparser.String(first.AST), sqlparser.String(second.AST))
}
