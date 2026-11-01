package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventScheduleASTPreservesHeaderAndBodyBoundaries(t *testing.T) {
	definition := "CREATE DEFINER='owner'@'localhost' EVENT app.ev ON SCHEDULE EVERY 2 HOUR COMMENT 'at ends do preserve' DO INSERT INTO log VALUES ('on completion preserve')"
	schedule, err := parseSQLEventSchedule(definition)
	require.NoError(t, err)
	require.True(t, schedule.Repeat)
	require.Equal(t, 2*time.Hour, schedule.Interval)
	require.False(t, schedule.OnCompletionPreserve)
	require.Equal(t, "INSERT INTO log VALUES ('on completion preserve')", schedule.Statement)
	for _, query := range []string{
		"CREATE EVENT ev ON SCHEDULE AT CURRENT_TIMESTAMP DO SELECT 1",
		"CREATE EVENT ev ON SCHEDULE AT CURRENT_TIMESTAMP() DO SELECT 1",
	} {
		before := time.Now().UTC()
		schedule, err := parseSQLEventSchedule(query)
		require.NoError(t, err)
		require.False(t, schedule.Repeat)
		require.WithinDuration(t, before, schedule.ExecuteAt, time.Second)
		require.Equal(t, "SELECT 1", schedule.Statement)
	}
}

func TestEventScheduleASTExecutesDefinerEventThroughScheduler(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "", "create user 'owner'@'localhost' identified by 'secret'")
	mustExecSQL(t, engine, "app", "create table event_log (id int primary key)")
	scheduler := NewEventScheduler(false)
	engine.SetEventScheduler(scheduler)
	mustExecSQL(t, engine, "app", "create definer='owner'@'localhost' event ast_event on schedule at current_timestamp on completion preserve comment 'do at begin' do insert into event_log values (7)")
	scheduler.runDue(context.Background(), time.Now().UTC().Add(time.Second))
	require.Empty(t, scheduler.LastError("app.ast_event"))
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, engine, "app", "select id from event_log"))
}

func TestEventScheduleASTExecutesDuringConcurrentReads(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table event_log (id int primary key, note varchar(20))")
	scheduler := NewEventScheduler(true)
	engine.SetEventScheduler(scheduler)
	require.NoError(t, engine.StartEventScheduler(context.Background()))
	defer engine.StopEventScheduler()
	mustExecSQL(t, engine, "app", "create event concurrent_event on schedule at current_timestamp on completion preserve do insert into event_log values (1, 'fired')")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rows := mustQuerySQL(t, engine, "app", "select note from event_log")
		if len(rows) == 1 && rows[0][0] == "fired" {
			return
		}
		if last := scheduler.LastError("app.concurrent_event"); last != "" {
			t.Fatalf("scheduler: %s", last)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("event did not execute; scheduler error=%q", scheduler.LastError("app.concurrent_event"))
}
