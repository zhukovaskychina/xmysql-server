package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStoredObjectDefinitionsPersistAndAppearInMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure refresh_users() begin select 1; end")
	mustExecSQL(t, executor, "app", "create trigger users_before_insert before insert on users for each row set @seen = 1")
	require.FileExists(t, filepath.Join(executor.GetDataDir(), "app", "refresh_users.routine.json"))
	require.FileExists(t, filepath.Join(executor.GetDataDir(), "app", "users_before_insert.trigger.json"))

	rows := mustQuerySQL(t, executor, "app", "select routine_name from information_schema.routines")
	require.Len(t, rows, 1)
	require.Equal(t, "refresh_users", rows[0][2])

	mustExecSQL(t, executor, "app", "drop procedure refresh_users")
	_, err := os.Stat(filepath.Join(executor.GetDataDir(), "app", "refresh_users.routine.json"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestViewDDLWaitsForExplicitViewMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	dropper := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table view_lock_source (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "create view view_lock_target as select id from view_lock_source")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables view_lock_target read")

	dropDone := make(chan *Result, 1)
	go func() {
		dropDone <- <-executor.ExecuteQuery(dropper, "drop view view_lock_target", "app")
	}()

	select {
	case result := <-dropDone:
		t.Fatalf("DROP VIEW completed while the view metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-dropDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("DROP VIEW did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestCreateViewWaitsForExplicitSourceMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	creator := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table view_source_lock (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables view_source_lock write")

	createDone := make(chan *Result, 1)
	go func() {
		createDone <- <-executor.ExecuteQuery(creator, "create view view_source_lock_target as select id from view_source_lock", "app")
	}()

	select {
	case result := <-createDone:
		t.Fatalf("CREATE VIEW completed while the source metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-createDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("CREATE VIEW did not complete after UNLOCK TABLES released the source metadata lock")
	}
}

func TestCreateViewWaitsForNestedViewSourceMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	creator := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table nested_view_source (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "create view nested_view_inner as select id from nested_view_source")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables nested_view_source write")

	createDone := make(chan *Result, 1)
	go func() {
		createDone <- <-executor.ExecuteQuery(creator, "create view nested_view_outer as select id from nested_view_inner", "app")
	}()

	select {
	case result := <-createDone:
		t.Fatalf("CREATE VIEW completed while the nested source metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-createDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("CREATE VIEW did not complete after UNLOCK TABLES released the nested source metadata lock")
	}
}

func TestCreateViewWaitsForCrossDatabaseSourceMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	creator := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database source_db")
	mustExecSessionSQL(t, executor, owner, "", "create database target_db")
	mustExecSessionSQL(t, executor, owner, "source_db", "create table cross_database_source (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "source_db", "lock tables source_db.cross_database_source write")

	createDone := make(chan *Result, 1)
	go func() {
		createDone <- <-executor.ExecuteQuery(creator, "create view target_db.cross_database_view as select id from source_db.cross_database_source", "target_db")
	}()

	select {
	case result := <-createDone:
		t.Fatalf("cross-database CREATE VIEW completed while the source metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "source_db", "unlock tables")
	select {
	case result := <-createDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("cross-database CREATE VIEW did not complete after UNLOCK TABLES released the source metadata lock")
	}
}

func TestCreateOrReplaceViewRejectsDependencyCycle(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table view_cycle_source (id int primary key)")
	mustExecSQL(t, executor, "app", "create view view_cycle_target as select id from view_cycle_source")

	result := <-executor.ExecuteQuery(nil, "create or replace view view_cycle_target as select id from view_cycle_target", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "view dependency cycle")
}

func TestShowCreateViewWaitsForExplicitViewMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table show_view_source (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "create view show_view_target as select id from show_view_source")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables show_view_target write")

	showDone := make(chan *Result, 1)
	go func() {
		showDone <- <-executor.ExecuteQuery(reader, "show create view show_view_target", "app")
	}()

	select {
	case result := <-showDone:
		t.Fatalf("SHOW CREATE VIEW completed while the view metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-showDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("SHOW CREATE VIEW did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestShowCreateStoredObjectWaitsForExplicitMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create procedure locked_metadata_proc() begin select 1; end")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables app.locked_metadata_proc write")

	showDone := make(chan *Result, 1)
	go func() {
		showDone <- <-executor.ExecuteQuery(reader, "show create procedure app.locked_metadata_proc", "app")
	}()

	select {
	case result := <-showDone:
		t.Fatalf("SHOW CREATE PROCEDURE completed while the object metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-showDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("SHOW CREATE PROCEDURE did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestStoredObjectDDLWaitsForExplicitMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	dropper := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create procedure ddl_locked_proc() begin select 1; end")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables app.ddl_locked_proc write")

	dropDone := make(chan *Result, 1)
	go func() {
		dropDone <- <-executor.ExecuteQuery(dropper, "drop procedure app.ddl_locked_proc", "app")
	}()

	select {
	case result := <-dropDone:
		t.Fatalf("DROP PROCEDURE completed while the object metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-dropDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("DROP PROCEDURE did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestShowRoutineStatusWaitsForExplicitMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create procedure status_locked_proc() begin select 1; end")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables app.status_locked_proc write")

	statusDone := make(chan *Result, 1)
	go func() {
		statusDone <- <-executor.ExecuteQuery(reader, "show procedure status from app", "app")
	}()

	select {
	case result := <-statusDone:
		t.Fatalf("SHOW PROCEDURE STATUS completed while the object metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-statusDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("SHOW PROCEDURE STATUS did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestCallStoredObjectWaitsForExplicitMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	caller := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create procedure call_locked_proc() begin select 1; end")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables app.call_locked_proc write")

	callDone := make(chan *Result, 1)
	go func() {
		callDone <- <-executor.ExecuteQuery(caller, "call app.call_locked_proc()", "app")
	}()

	select {
	case result := <-callDone:
		t.Fatalf("CALL completed while the object metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-callDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("CALL did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestNestedCallStoredObjectWaitsForExplicitMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	caller := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create procedure nested_locked_proc() begin select 1; end")
	mustExecSessionSQL(t, executor, owner, "app", "create procedure outer_call_proc() begin call nested_locked_proc(); end")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables app.nested_locked_proc write")

	callDone := make(chan *Result, 1)
	go func() {
		callDone <- <-executor.ExecuteQuery(caller, "call app.outer_call_proc()", "app")
	}()

	select {
	case result := <-callDone:
		t.Fatalf("nested CALL completed while the inner object metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-callDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("nested CALL did not complete after UNLOCK TABLES released the inner object metadata lock")
	}
}

func TestStoredFunctionCallWaitsForExplicitMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	caller := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create function function_lock_target() returns int deterministic begin return 1; end")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables app.function_lock_target write")

	callDone := make(chan *Result, 1)
	go func() {
		callDone <- <-executor.ExecuteQuery(caller, "select function_lock_target()", "app")
	}()

	select {
	case result := <-callDone:
		t.Fatalf("stored function call completed while the object metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-callDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("stored function call did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestInformationSchemaRoutinesWaitsForExplicitMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create procedure routines_metadata_lock() begin select 1; end")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables app.routines_metadata_lock write")

	queryDone := make(chan *Result, 1)
	go func() {
		queryDone <- <-executor.ExecuteQuery(reader, "select routine_name from information_schema.routines where routine_schema = 'app'", "")
	}()

	select {
	case result := <-queryDone:
		t.Fatalf("INFORMATION_SCHEMA.ROUTINES completed while the object metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-queryDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("INFORMATION_SCHEMA.ROUTINES did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestInformationSchemaParametersWaitsForExplicitMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create procedure parameters_metadata_lock(in value int) begin select value; end")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables app.parameters_metadata_lock write")

	queryDone := make(chan *Result, 1)
	go func() {
		queryDone <- <-executor.ExecuteQuery(reader, "select specific_name from information_schema.parameters where specific_schema = 'app'", "")
	}()

	select {
	case result := <-queryDone:
		t.Fatalf("INFORMATION_SCHEMA.PARAMETERS completed while the object metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-queryDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("INFORMATION_SCHEMA.PARAMETERS did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestCreateTriggerRejectsDuplicateNameWithoutReplace(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table duplicate_trigger_target (id int primary key, flag varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger duplicate_trigger before insert on duplicate_trigger_target for each row set new.flag = 'first'")
	duplicate := <-executor.ExecuteQuery(nil, "create trigger duplicate_trigger before insert on duplicate_trigger_target for each row set new.flag = 'second'", "app")
	require.Error(t, duplicate.Err)
	require.Contains(t, strings.ToLower(duplicate.Err.Error()), "already exists")
	mustExecSQL(t, executor, "app", "insert into duplicate_trigger_target values (1, 'caller')")
	require.Equal(t, [][]interface{}{{"first"}}, mustQuerySQL(t, executor, "app", "select flag from duplicate_trigger_target"))
}

func TestCreateOrReplaceTriggerUpdatesExistingDefinition(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table replace_trigger_target (id int primary key, flag varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger replace_trigger before insert on replace_trigger_target for each row set new.flag = 'first'")
	mustExecSQL(t, executor, "app", "create or replace trigger replace_trigger before insert on replace_trigger_target for each row set new.flag = 'second'")
	mustExecSQL(t, executor, "app", "insert into replace_trigger_target values (1, 'caller')")
	require.Equal(t, [][]interface{}{{"second"}}, mustQuerySQL(t, executor, "app", "select flag from replace_trigger_target"))
}

func TestCreateTriggerRejectsMissingOrderingReference(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table missing_order_target (id int primary key)")
	result := <-executor.ExecuteQuery(nil, "create trigger ordered_trigger before insert on missing_order_target for each row follows missing_trigger set @seen = 1", "app")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "does not exist")
	require.NoFileExists(t, filepath.Join(executor.GetDataDir(), "app", "ordered_trigger.trigger.json"))
}

func TestInformationSchemaTriggersReturnsTriggerShapeMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit_rows (id int primary key)")
	mustExecSQL(t, executor, "app", "create trigger audit_before before insert on audit_rows for each row set @seen = 1")
	rows := mustQuerySQL(t, executor, "app", "select event_manipulation, event_object_table, action_timing from information_schema.triggers where trigger_name = 'audit_before'")
	require.Equal(t, [][]interface{}{{"INSERT", "audit_rows", "BEFORE"}}, rows)
}

func TestShowTriggersReturnsTriggerMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit_rows (id int primary key)")
	mustExecSQL(t, executor, "app", "create trigger audit_before before insert on audit_rows for each row set @seen = 1")

	result := <-executor.ExecuteQuery(nil, "show triggers from app like 'audit%'", "app")
	require.NoError(t, result.Err)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok, "expected SHOW result map, got %T", result.Data)
	require.Equal(t, []string{"Trigger", "Event", "Table", "Statement", "Timing", "Created", "sql_mode", "Definer", "character_set_client", "collation_connection", "Database Collation"}, data["columns"])
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok, "expected SHOW rows, got %T", data["rows"])
	require.Len(t, rows, 1)
	require.Equal(t, "audit_before", fmt.Sprint(rows[0][0]))
	require.Equal(t, "INSERT", fmt.Sprint(rows[0][1]))
	require.Equal(t, "audit_rows", fmt.Sprint(rows[0][2]))
	require.Equal(t, "BEFORE", fmt.Sprint(rows[0][4]))
}

func TestInformationSchemaEventsReturnsScheduleMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create event refresh_event on schedule every 2 hour starts '2030-01-02 03:04:05' do select 1")
	rows := mustQuerySQL(t, executor, "app", "select event_type, interval_value, interval_field, status from information_schema.events where event_name = 'refresh_event'")
	require.Equal(t, [][]interface{}{{"RECURRING", "2", "HOUR", "ENABLED"}}, rows)
}

func TestShowEventsReturnsScheduleMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create event refresh_event on schedule every 2 hour starts '2030-01-02 03:04:05' do select 1")

	result := <-executor.ExecuteQuery(nil, "show events from app like 'refresh%'", "app")
	require.NoError(t, result.Err)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok, "expected SHOW result map, got %T", result.Data)
	require.Equal(t, []string{"Db", "Name", "Definer", "Time zone", "Event type", "Execute at", "Interval value", "Interval field", "Starts", "Ends", "Status", "Originator", "character_set_client", "collation_connection", "Database Collation"}, data["columns"])
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok, "expected SHOW rows, got %T", data["rows"])
	require.Len(t, rows, 1)
	require.Equal(t, "app", fmt.Sprint(rows[0][0]))
	require.Equal(t, "refresh_event", fmt.Sprint(rows[0][1]))
	require.Equal(t, "RECURRING", fmt.Sprint(rows[0][4]))
	require.Equal(t, "2", fmt.Sprint(rows[0][6]))
	require.Equal(t, "HOUR", fmt.Sprint(rows[0][7]))
	require.Equal(t, "ENABLED", fmt.Sprint(rows[0][10]))
}

func TestEventOperationsRequireEventPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create event existing_event on schedule every 1 hour do select 1")
	mustExecSQL(t, executor, "", "create user 'event_viewer'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "event_viewer")
	session.SetParamByName("host", "localhost")

	for _, query := range []string{
		"show events from app",
		"show create event existing_event",
		"create event denied_event on schedule every 1 hour do select 1",
		"alter event existing_event disable",
		"drop event existing_event",
	} {
		denied := <-executor.ExecuteQuery(session, query, "app")
		require.Error(t, denied.Err, query)
		require.Contains(t, strings.ToLower(denied.Err.Error()), "event", query)
	}
	metadataDenied := mustQuerySessionSQL(t, executor, session, "", "select event_name from information_schema.events where event_schema = 'app'")
	require.Empty(t, metadataDenied)

	mustExecSQL(t, executor, "", "grant event on app.* to 'event_viewer'@'localhost'")
	for _, query := range []string{
		"show events from app",
		"show create event existing_event",
		"create event allowed_event on schedule every 1 hour do select 1",
		"alter event existing_event disable",
		"drop event existing_event",
	} {
		allowed := <-executor.ExecuteQuery(session, query, "app")
		require.NoError(t, allowed.Err, query)
	}
	metadataAllowed := mustQuerySessionSQL(t, executor, session, "", "select event_name from information_schema.events where event_schema = 'app'")
	require.Equal(t, [][]interface{}{{"allowed_event"}}, metadataAllowed)
}

func TestTriggerOperationsRequireTriggerPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table trigger_target (id int primary key, flag int)")
	mustExecSQL(t, executor, "app", "create trigger existing_trigger before insert on trigger_target for each row set new.flag = 1")
	mustExecSQL(t, executor, "", "create user 'trigger_viewer'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "trigger_viewer")
	session.SetParamByName("host", "localhost")

	for _, query := range []string{
		"show triggers from app",
		"show create trigger existing_trigger",
		"create trigger denied_trigger before insert on trigger_target for each row set new.flag = 2",
		"drop trigger existing_trigger",
	} {
		denied := <-executor.ExecuteQuery(session, query, "app")
		require.Error(t, denied.Err, query)
		require.Contains(t, strings.ToLower(denied.Err.Error()), "trigger", query)
	}
	metadataDenied := mustQuerySessionSQL(t, executor, session, "", "select trigger_name from information_schema.triggers where trigger_schema = 'app'")
	require.Empty(t, metadataDenied)

	mustExecSQL(t, executor, "", "grant trigger on app.* to 'trigger_viewer'@'localhost'")
	for _, query := range []string{
		"show triggers from app",
		"show create trigger existing_trigger",
		"create trigger allowed_trigger before insert on trigger_target for each row set new.flag = 3",
		"drop trigger existing_trigger",
	} {
		allowed := <-executor.ExecuteQuery(session, query, "app")
		require.NoError(t, allowed.Err, query)
	}
	metadataAllowed := mustQuerySessionSQL(t, executor, session, "", "select trigger_name from information_schema.triggers where trigger_schema = 'app'")
	require.Equal(t, [][]interface{}{{"allowed_trigger"}}, metadataAllowed)
}

func TestTriggerOperationsAcceptTableScopedTriggerPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table table_trigger_target (id int primary key, flag int)")
	mustExecSQL(t, executor, "", "create user 'table_trigger_user'@'localhost' identified by 'secret'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "table_trigger_user")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "create trigger table_scoped_trigger before insert on table_trigger_target for each row set new.flag = 1", "app")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "trigger")

	mustExecSQL(t, executor, "", "grant trigger on app.table_trigger_target to 'table_trigger_user'@'localhost'")
	mustExecSessionSQL(t, executor, session, "app", "create trigger table_scoped_trigger before insert on table_trigger_target for each row set new.flag = 1")
	mustExecSessionSQL(t, executor, session, "app", "show triggers from app")
	mustExecSessionSQL(t, executor, session, "app", "show create trigger table_scoped_trigger")
	require.Len(t, mustQuerySessionSQL(t, executor, session, "", "select trigger_name from information_schema.triggers where trigger_schema = 'app'"), 1)
	mustExecSessionSQL(t, executor, session, "app", "drop trigger table_scoped_trigger")
}

func TestRoutineDDLRequiresRoutinePrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "", "create user 'routine_ddl_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "routine_ddl_user")
	session.SetParamByName("host", "localhost")

	deniedCreate := <-executor.ExecuteQuery(session, "create procedure denied_proc() begin select 1; end", "app")
	require.Error(t, deniedCreate.Err)
	require.Contains(t, strings.ToLower(deniedCreate.Err.Error()), "create routine")

	mustExecSQL(t, executor, "", "grant create routine on app.* to 'routine_ddl_user'@'localhost'")
	allowedCreate := <-executor.ExecuteQuery(session, "create procedure allowed_proc() begin select 1; end", "app")
	require.NoError(t, allowedCreate.Err)

	deniedAlter := <-executor.ExecuteQuery(session, "alter procedure allowed_proc comment 'changed'", "app")
	require.Error(t, deniedAlter.Err)
	require.Contains(t, strings.ToLower(deniedAlter.Err.Error()), "alter routine")

	mustExecSQL(t, executor, "", "grant alter routine on app.* to 'routine_ddl_user'@'localhost'")
	allowedAlter := <-executor.ExecuteQuery(session, "alter procedure allowed_proc comment 'changed'", "app")
	require.NoError(t, allowedAlter.Err)
	allowedDrop := <-executor.ExecuteQuery(session, "drop procedure allowed_proc", "app")
	require.NoError(t, allowedDrop.Err)
}

func TestDatabaseDDLRequiresDatabasePrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'database_ddl_user'@'localhost' identified by 'secret'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "database_ddl_user")
	session.SetParamByName("host", "localhost")

	deniedCreate := <-executor.ExecuteQuery(session, "create database created_db", "")
	require.Error(t, deniedCreate.Err)
	require.Contains(t, strings.ToLower(deniedCreate.Err.Error()), "create")

	mustExecSQL(t, executor, "", "grant create on created_db.* to 'database_ddl_user'@'localhost'")
	allowedCreate := <-executor.ExecuteQuery(session, "create database created_db", "")
	require.NoError(t, allowedCreate.Err)

	deniedDrop := <-executor.ExecuteQuery(session, "drop database created_db", "")
	require.Error(t, deniedDrop.Err)
	require.Contains(t, strings.ToLower(deniedDrop.Err.Error()), "drop")

	mustExecSQL(t, executor, "", "grant drop on created_db.* to 'database_ddl_user'@'localhost'")
	allowedDrop := <-executor.ExecuteQuery(session, "drop database created_db", "")
	require.NoError(t, allowedDrop.Err)
}

func TestTruncateRequiresDropPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table truncate_target (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into truncate_target values (1)")
	mustExecSQL(t, executor, "", "create user 'truncate_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "truncate_user")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "truncate table truncate_target", "app")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "drop")

	mustExecSQL(t, executor, "", "grant drop on app.truncate_target to 'truncate_user'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "truncate table truncate_target", "app")
	require.NoError(t, allowed.Err)
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from truncate_target"))
}

func TestTableMaintenanceRequiresMySQLPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table maintenance_target (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'maintenance_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "maintenance_user")
	session.SetParamByName("host", "localhost")

	for _, query := range []string{
		"check table maintenance_target",
		"analyze table maintenance_target",
		"optimize table maintenance_target",
	} {
		denied := <-executor.ExecuteQuery(session, query, "app")
		require.Error(t, denied.Err, query)
		require.Contains(t, strings.ToLower(denied.Err.Error()), "privilege", query)
	}

	mustExecSQL(t, executor, "", "grant select on app.maintenance_target to 'maintenance_user'@'localhost'")
	checkAllowed := <-executor.ExecuteQuery(session, "check table maintenance_target", "app")
	require.NoError(t, checkAllowed.Err)
	analyzeDeniedWithoutInsert := <-executor.ExecuteQuery(session, "analyze table maintenance_target", "app")
	require.Error(t, analyzeDeniedWithoutInsert.Err)
	optimizeDeniedWithoutInsert := <-executor.ExecuteQuery(session, "optimize table maintenance_target", "app")
	require.Error(t, optimizeDeniedWithoutInsert.Err)

	mustExecSQL(t, executor, "", "grant insert on app.maintenance_target to 'maintenance_user'@'localhost'")
	analyzeAllowed := <-executor.ExecuteQuery(session, "analyze table maintenance_target", "app")
	require.NoError(t, analyzeAllowed.Err)
	optimizeAllowed := <-executor.ExecuteQuery(session, "optimize table maintenance_target", "app")
	require.NoError(t, optimizeAllowed.Err)
}

func TestLockTablesRequiresLockAndSelectPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table lock_target (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'lock_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "lock_user")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "lock tables lock_target read", "app")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "privilege")

	mustExecSQL(t, executor, "", "grant select on app.lock_target to 'lock_user'@'localhost'")
	deniedWithoutLock := <-executor.ExecuteQuery(session, "lock tables lock_target read", "app")
	require.Error(t, deniedWithoutLock.Err)
	require.Contains(t, strings.ToLower(deniedWithoutLock.Err.Error()), "lock tables")

	mustExecSQL(t, executor, "", "grant lock tables on app.* to 'lock_user'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "lock tables lock_target read", "app")
	require.NoError(t, allowed.Err)
	unlocked := <-executor.ExecuteQuery(session, "unlock tables", "app")
	require.NoError(t, unlocked.Err)
}

func TestTableDDLRequiresTablePrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table alter_target (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'table_ddl_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "table_ddl_user")
	session.SetParamByName("host", "localhost")

	deniedCreate := <-executor.ExecuteQuery(session, "create table create_target (id int primary key)", "app")
	require.Error(t, deniedCreate.Err)
	require.Contains(t, strings.ToLower(deniedCreate.Err.Error()), "create")

	mustExecSQL(t, executor, "", "grant create on app.* to 'table_ddl_user'@'localhost'")
	allowedCreate := <-executor.ExecuteQuery(session, "create table create_target (id int primary key)", "app")
	require.NoError(t, allowedCreate.Err)

	deniedAlter := <-executor.ExecuteQuery(session, "alter table alter_target add column label varchar(20)", "app")
	require.Error(t, deniedAlter.Err)
	require.Contains(t, strings.ToLower(deniedAlter.Err.Error()), "alter")

	mustExecSQL(t, executor, "", "grant alter on app.alter_target to 'table_ddl_user'@'localhost'")
	deniedAlterWithoutInsert := <-executor.ExecuteQuery(session, "alter table alter_target add column blocked_label varchar(20)", "app")
	require.Error(t, deniedAlterWithoutInsert.Err)
	require.Contains(t, strings.ToLower(deniedAlterWithoutInsert.Err.Error()), "insert")

	mustExecSQL(t, executor, "", "grant insert on app.alter_target to 'table_ddl_user'@'localhost'")
	allowedAlter := <-executor.ExecuteQuery(session, "alter table alter_target add column label varchar(20)", "app")
	require.NoError(t, allowedAlter.Err)

	deniedDrop := <-executor.ExecuteQuery(session, "drop table create_target", "app")
	require.Error(t, deniedDrop.Err)
	require.Contains(t, strings.ToLower(deniedDrop.Err.Error()), "drop")

	mustExecSQL(t, executor, "", "grant drop on app.create_target to 'table_ddl_user'@'localhost'")
	allowedDrop := <-executor.ExecuteQuery(session, "drop table create_target", "app")
	require.NoError(t, allowedDrop.Err)
}

func TestDMLRequiresTablePrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table dml_target (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "", "create user 'dml_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "dml_user")
	session.SetParamByName("host", "localhost")

	deniedInsert := <-executor.ExecuteQuery(session, "insert into dml_target values (1, 'one')", "app")
	require.Error(t, deniedInsert.Err)
	require.Contains(t, strings.ToLower(deniedInsert.Err.Error()), "insert")

	mustExecSQL(t, executor, "", "grant insert on app.dml_target to 'dml_user'@'localhost'")
	allowedInsert := <-executor.ExecuteQuery(session, "insert into dml_target values (1, 'one')", "app")
	require.NoError(t, allowedInsert.Err)

	deniedUpdate := <-executor.ExecuteQuery(session, "update dml_target set label = 'updated'", "app")
	require.Error(t, deniedUpdate.Err)
	require.Contains(t, strings.ToLower(deniedUpdate.Err.Error()), "update")

	mustExecSQL(t, executor, "", "grant update on app.dml_target to 'dml_user'@'localhost'")
	allowedUpdate := <-executor.ExecuteQuery(session, "update dml_target set label = 'updated'", "app")
	require.NoError(t, allowedUpdate.Err)

	deniedDelete := <-executor.ExecuteQuery(session, "delete from dml_target", "app")
	require.Error(t, deniedDelete.Err)
	require.Contains(t, strings.ToLower(deniedDelete.Err.Error()), "delete")

	mustExecSQL(t, executor, "", "grant delete on app.dml_target to 'dml_user'@'localhost'")
	allowedDelete := <-executor.ExecuteQuery(session, "delete from dml_target", "app")
	require.NoError(t, allowedDelete.Err)
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from dml_target"))
}

func TestConflictDMLRequiresAdditionalPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table conflict_target (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "", "create user 'conflict_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "conflict_user")
	session.SetParamByName("host", "localhost")
	mustExecSQL(t, executor, "", "grant insert on app.conflict_target to 'conflict_user'@'localhost'")

	inserted := <-executor.ExecuteQuery(session, "insert into conflict_target values (1, 'one')", "app")
	require.NoError(t, inserted.Err)
	deniedUpdate := <-executor.ExecuteQuery(session, "insert into conflict_target values (1, 'duplicate') on duplicate key update label = 'updated'", "app")
	require.Error(t, deniedUpdate.Err)
	require.Contains(t, strings.ToLower(deniedUpdate.Err.Error()), "update")

	mustExecSQL(t, executor, "", "grant update on app.conflict_target to 'conflict_user'@'localhost'")
	allowedUpdate := <-executor.ExecuteQuery(session, "insert into conflict_target values (1, 'duplicate') on duplicate key update label = 'updated'", "app")
	require.NoError(t, allowedUpdate.Err)

	deniedReplace := <-executor.ExecuteQuery(session, "replace into conflict_target values (1, 'replaced')", "app")
	require.Error(t, deniedReplace.Err)
	require.Contains(t, strings.ToLower(deniedReplace.Err.Error()), "delete")

	mustExecSQL(t, executor, "", "grant delete on app.conflict_target to 'conflict_user'@'localhost'")
	allowedReplace := <-executor.ExecuteQuery(session, "replace into conflict_target values (1, 'replaced')", "app")
	require.NoError(t, allowedReplace.Err)
}

func TestRawAlterRequiresAlterPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table raw_alter_target (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'raw_alter_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "raw_alter_user")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "alter table raw_alter_target comment = 'hidden'", "app")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "alter")

	mustExecSQL(t, executor, "", "grant alter on app.raw_alter_target to 'raw_alter_user'@'localhost'")
	deniedWithoutCreate := <-executor.ExecuteQuery(session, "alter table raw_alter_target comment = 'blocked'", "app")
	require.Error(t, deniedWithoutCreate.Err)
	require.Contains(t, strings.ToLower(deniedWithoutCreate.Err.Error()), "create")

	mustExecSQL(t, executor, "", "grant create on app.raw_alter_target to 'raw_alter_user'@'localhost'")
	deniedWithoutInsert := <-executor.ExecuteQuery(session, "alter table raw_alter_target comment = 'blocked'", "app")
	require.Error(t, deniedWithoutInsert.Err)
	require.Contains(t, strings.ToLower(deniedWithoutInsert.Err.Error()), "insert")

	mustExecSQL(t, executor, "", "grant insert on app.raw_alter_target to 'raw_alter_user'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "alter table raw_alter_target comment = 'visible'", "app")
	require.NoError(t, allowed.Err)
}

func TestMultiTableDropRequiresDropPrivilegeForEveryTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table drop_one (id int primary key)")
	mustExecSQL(t, executor, "app", "create table drop_two (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'multi_drop_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "multi_drop_user")
	session.SetParamByName("host", "localhost")
	mustExecSQL(t, executor, "", "grant drop on app.drop_one to 'multi_drop_user'@'localhost'")

	denied := <-executor.ExecuteQuery(session, "drop table drop_one, drop_two", "app")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "drop")
	exists, err := executor.QueryExecutor.checkTableExists("app", "drop_one")
	require.NoError(t, err)
	require.True(t, exists)

	mustExecSQL(t, executor, "", "grant drop on app.drop_two to 'multi_drop_user'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "drop table drop_one, drop_two", "app")
	require.NoError(t, allowed.Err)
}

func TestRenameTableRequiresSourceAndDestinationPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table rename_source (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'rename_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "rename_user")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "rename table rename_source to rename_target", "app")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "alter")

	mustExecSQL(t, executor, "", "grant alter, drop on app.rename_source to 'rename_user'@'localhost'")
	deniedDestination := <-executor.ExecuteQuery(session, "rename table rename_source to rename_target", "app")
	require.Error(t, deniedDestination.Err)
	require.Contains(t, strings.ToLower(deniedDestination.Err.Error()), "create")

	mustExecSQL(t, executor, "", "grant create, insert on app.* to 'rename_user'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "rename table rename_source to rename_target", "app")
	require.NoError(t, allowed.Err)
}

func TestTemporaryTableRequiresCreateTemporaryTablesPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "", "create user 'temporary_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "temporary_user")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("database", "app")

	denied := <-executor.ExecuteQuery(session, "create temporary table tmp_priv (id int)", "app")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "temporary")

	mustExecSQL(t, executor, "", "grant create temporary tables on app.* to 'temporary_user'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "create temporary table tmp_priv (id int)", "app")
	require.NoError(t, allowed.Err)
	inserted := <-executor.ExecuteQuery(session, "insert into tmp_priv values (1)", "app")
	require.NoError(t, inserted.Err)
}

func TestColumnSelectPrivilegeRestrictsProjectionAndPredicateColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table column_secret (id int primary key, public_value varchar(20), secret_value varchar(20))")
	mustExecSQL(t, executor, "app", "insert into column_secret values (1, 'visible', 'hidden')")
	mustExecSQL(t, executor, "", "create user 'column_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select (secret_value) on app.column_secret to 'column_reader'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "column_reader")
	session.SetParamByName("host", "localhost")

	allowed := mustQuerySessionSQL(t, executor, session, "app", "select secret_value from column_secret")
	require.Equal(t, [][]interface{}{{"hidden"}}, allowed)

	deniedProjection := <-executor.ExecuteQuery(session, "select public_value from column_secret", "app")
	require.Error(t, deniedProjection.Err)
	require.Contains(t, strings.ToLower(deniedProjection.Err.Error()), "column")

	deniedPredicate := <-executor.ExecuteQuery(session, "select secret_value from column_secret where public_value = 'visible'", "app")
	require.Error(t, deniedPredicate.Err)
	require.Contains(t, strings.ToLower(deniedPredicate.Err.Error()), "column")

	mustExecSQL(t, executor, "", "grant select (public_value) on app.column_secret to 'column_reader'@'localhost'")
	require.Equal(t, [][]interface{}{{"hidden"}}, mustQuerySessionSQL(t, executor, session, "app", "select secret_value from column_secret where public_value = 'visible'"))
}

func TestColumnDMLPrivilegesRestrictInsertAndUpdateColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table column_write (id int primary key auto_increment, public_value varchar(20), secret_value varchar(20))")
	mustExecSQL(t, executor, "", "create user 'column_writer'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant insert (secret_value), update (secret_value) on app.column_write to 'column_writer'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "column_writer")
	session.SetParamByName("host", "localhost")

	allowedInsert := <-executor.ExecuteQuery(session, "insert into column_write (secret_value) values ('hidden')", "app")
	require.NoError(t, allowedInsert.Err)
	deniedInsert := <-executor.ExecuteQuery(session, "insert into column_write (public_value) values ('visible')", "app")
	require.Error(t, deniedInsert.Err)
	require.Contains(t, strings.ToLower(deniedInsert.Err.Error()), "column")

	allowedUpdate := <-executor.ExecuteQuery(session, "update column_write set secret_value = 'updated'", "app")
	require.NoError(t, allowedUpdate.Err)
	deniedRead := <-executor.ExecuteQuery(session, "update column_write set secret_value = 'read-blocked' where public_value = 'visible'", "app")
	require.Error(t, deniedRead.Err)
	require.Contains(t, strings.ToLower(deniedRead.Err.Error()), "column")

	mustExecSQL(t, executor, "", "grant select (public_value) on app.column_write to 'column_writer'@'localhost'")
	allowedRead := <-executor.ExecuteQuery(session, "update column_write set secret_value = 'read-allowed' where public_value = 'visible'", "app")
	require.NoError(t, allowedRead.Err)

	deniedUpdate := <-executor.ExecuteQuery(session, "update column_write set public_value = 'blocked'", "app")
	require.Error(t, deniedUpdate.Err)
	require.Contains(t, strings.ToLower(deniedUpdate.Err.Error()), "column")
}

func TestColumnUpdatePrivilegeAppliesToUpdateJoin(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table column_join_target (id int primary key, secret_value varchar(20))")
	mustExecSQL(t, executor, "app", "create table column_join_source (id int primary key, source_value varchar(20))")
	mustExecSQL(t, executor, "app", "insert into column_join_target values (1, 'old')")
	mustExecSQL(t, executor, "app", "insert into column_join_source values (1, 'new')")
	mustExecSQL(t, executor, "", "create user 'column_join_writer'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant update (secret_value) on app.column_join_target to 'column_join_writer'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on app.column_join_source to 'column_join_writer'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "column_join_writer")
	session.SetParamByName("host", "localhost")

	allowed := <-executor.ExecuteQuery(session, "update column_join_target t join column_join_source s on t.id = s.id set t.secret_value = s.source_value", "app")
	require.NoError(t, allowed.Err)
	require.Equal(t, [][]interface{}{{"new"}}, mustQuerySQL(t, executor, "app", "select secret_value from column_join_target"))
}

func TestStandaloneIndexDDLRequiresIndexPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table index_target (id int primary key, value varchar(20))")
	mustExecSQL(t, executor, "", "create user 'index_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "index_user")
	session.SetParamByName("host", "localhost")

	deniedCreate := <-executor.ExecuteQuery(session, "create index idx_value on index_target (value)", "app")
	require.Error(t, deniedCreate.Err)
	require.Contains(t, strings.ToLower(deniedCreate.Err.Error()), "index")

	mustExecSQL(t, executor, "", "grant index on app.index_target to 'index_user'@'localhost'")
	allowedCreate := <-executor.ExecuteQuery(session, "create index idx_value on index_target (value)", "app")
	require.NoError(t, allowedCreate.Err)

	mustExecSQL(t, executor, "", "revoke index on app.index_target from 'index_user'@'localhost'")
	deniedDrop := <-executor.ExecuteQuery(session, "drop index idx_value on index_target", "app")
	require.Error(t, deniedDrop.Err)
	require.Contains(t, strings.ToLower(deniedDrop.Err.Error()), "index")

	mustExecSQL(t, executor, "", "grant index on app.index_target to 'index_user'@'localhost'")
	allowedDrop := <-executor.ExecuteQuery(session, "drop index idx_value on index_target", "app")
	require.NoError(t, allowedDrop.Err)
}

func TestStandaloneDropIndexIfExistsIsIdempotent(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table index_if_exists (id int primary key, value varchar(20))")

	missing := <-executor.ExecuteQuery(newTestMySQLSession(), "drop index if exists idx_missing on index_if_exists", "app")
	require.NoError(t, missing.Err)

	mustExecSQL(t, executor, "app", "create index idx_value on index_if_exists (value)")
	removed := <-executor.ExecuteQuery(newTestMySQLSession(), "drop index if exists idx_value on index_if_exists", "app")
	require.NoError(t, removed.Err)

	repeated := <-executor.ExecuteQuery(newTestMySQLSession(), "drop index if exists idx_value on index_if_exists", "app")
	require.NoError(t, repeated.Err)
}

func TestStandaloneCreateIndexIfNotExistsIsIdempotent(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table index_if_not_exists (id int primary key, value varchar(20))")

	created := <-executor.ExecuteQuery(newTestMySQLSession(), "create index if not exists idx_value on index_if_not_exists (value)", "app")
	require.NoError(t, created.Err)

	repeated := <-executor.ExecuteQuery(newTestMySQLSession(), "create index if not exists idx_value on index_if_not_exists (value)", "app")
	require.NoError(t, repeated.Err)

	show := <-executor.ExecuteQuery(newTestMySQLSession(), "show index from index_if_not_exists", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	indexes, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	indexNames := make([]interface{}, 0, len(indexes))
	for _, row := range indexes {
		if len(row) > 2 {
			indexNames = append(indexNames, row[2])
		}
	}
	count := 0
	for _, name := range indexNames {
		if fmt.Sprint(name) == "idx_value" {
			count++
		}
	}
	require.Equal(t, 1, count)
}

func TestStandaloneCreateIndexVisibilityIsPersisted(t *testing.T) {
	match := standaloneCreateIndexPattern.FindStringSubmatch("create index idx_value on index_visibility (value) invisible")
	require.Len(t, match, 8)
	require.Equal(t, "invisible", strings.ToLower(match[7]))
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table index_visibility (id int primary key, value varchar(20))")

	mustExecSQL(t, executor, "app", "create index idx_value on index_visibility (value) invisible")
	info, err := readTableMetadataMap(filepath.Join(executor.GetDataDir(), "app", "index_visibility.frm"))
	require.NoError(t, err)
	require.Equal(t, false, info["indexes"].([]interface{})[1].(map[string]interface{})["visible"])
	show := <-executor.ExecuteQuery(newTestMySQLSession(), "show index from index_visibility", "app")
	require.NoError(t, show.Err)
	showRows := show.Data.(map[string]interface{})["rows"].([][]interface{})
	require.Equal(t, "NO", showRows[1][13])
}

func TestViewMetadataRequiresShowViewPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table view_source (id int primary key)")
	mustExecSQL(t, executor, "app", "create view visible_view as select id from view_source")
	mustExecSQL(t, executor, "", "create user 'view_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "view_reader")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "show create view visible_view", "app")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "show view")
	metadataDenied := mustQuerySessionSQL(t, executor, session, "", "select table_name from information_schema.views where table_schema = 'app'")
	require.Empty(t, metadataDenied)

	mustExecSQL(t, executor, "", "grant show view on app.* to 'view_reader'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "show create view visible_view", "app")
	require.NoError(t, allowed.Err)
	metadataAllowed := mustQuerySessionSQL(t, executor, session, "", "select table_name from information_schema.views where table_schema = 'app'")
	require.Equal(t, [][]interface{}{{"visible_view"}}, metadataAllowed)
}

func TestViewDDLRequiresObjectPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table view_priv_source (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'view_ddl_user'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "view_ddl_user")
	session.SetParamByName("host", "localhost")

	deniedCreate := <-executor.ExecuteQuery(session, "create view protected_view as select id from view_priv_source", "app")
	require.Error(t, deniedCreate.Err)
	require.Contains(t, strings.ToLower(deniedCreate.Err.Error()), "create view")

	mustExecSQL(t, executor, "", "grant create view on app.* to 'view_ddl_user'@'localhost'")
	deniedSource := <-executor.ExecuteQuery(session, "create view protected_view as select id from view_priv_source", "app")
	require.Error(t, deniedSource.Err)
	require.Contains(t, strings.ToLower(deniedSource.Err.Error()), "select")

	mustExecSQL(t, executor, "", "grant select (id) on app.view_priv_source to 'view_ddl_user'@'localhost'")
	allowedCreate := <-executor.ExecuteQuery(session, "create view protected_view as select id from view_priv_source", "app")
	require.NoError(t, allowedCreate.Err)

	deniedAlter := <-executor.ExecuteQuery(session, "alter view protected_view as select id from view_priv_source", "app")
	require.Error(t, deniedAlter.Err)
	require.Contains(t, strings.ToLower(deniedAlter.Err.Error()), "drop")

	mustExecSQL(t, executor, "", "grant drop on app.* to 'view_ddl_user'@'localhost'")
	deniedDefiner := <-executor.ExecuteQuery(session, "alter view protected_view as select id from view_priv_source", "app")
	require.Error(t, deniedDefiner.Err)
	require.Contains(t, strings.ToLower(deniedDefiner.Err.Error()), "definer")

	mustExecSQL(t, executor, "", "grant set_any_definer on *.* to 'view_ddl_user'@'localhost'")
	allowedAlter := <-executor.ExecuteQuery(session, "alter view protected_view as select id from view_priv_source", "app")
	require.NoError(t, allowedAlter.Err)

	mustExecSQL(t, executor, "", "revoke drop on app.* from 'view_ddl_user'@'localhost'")
	deniedDrop := <-executor.ExecuteQuery(session, "drop view protected_view", "app")
	require.Error(t, deniedDrop.Err)
	require.Contains(t, strings.ToLower(deniedDrop.Err.Error()), "drop")

	mustExecSQL(t, executor, "", "grant drop on app.* to 'view_ddl_user'@'localhost'")
	allowedDrop := <-executor.ExecuteQuery(session, "drop view protected_view", "app")
	require.NoError(t, allowedDrop.Err)
}

func TestCreateOrReplaceViewRequiresDropPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table replace_source (id int primary key)")
	mustExecSQL(t, executor, "app", "create view replace_view as select id from replace_source")
	mustExecSQL(t, executor, "", "create user 'replace_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant create view on app.* to 'replace_user'@'localhost'")
	mustExecSQL(t, executor, "", "grant select (id) on app.replace_source to 'replace_user'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "replace_user")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "create or replace view replace_view as select id from replace_source", "app")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "drop")

	mustExecSQL(t, executor, "", "grant drop on app.* to 'replace_user'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "create or replace view replace_view as select id from replace_source", "app")
	require.NoError(t, allowed.Err)
}

func TestCreateViewDefinerRequiresDefinerPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table definer_source (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'definer_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant create view on app.* to 'definer_user'@'localhost'")
	mustExecSQL(t, executor, "", "grant select (id) on app.definer_source to 'definer_user'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "definer_user")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "create definer = 'root'@'localhost' view explicit_definer as select id from definer_source", "app")
	require.Error(t, denied.Err)
	require.Contains(t, strings.ToLower(denied.Err.Error()), "definer")

	mustExecSQL(t, executor, "", "grant set_any_definer on *.* to 'definer_user'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "create definer = 'root'@'localhost' view explicit_definer as select id from definer_source", "app")
	require.NoError(t, allowed.Err)
}

func TestViewExecutionHonorsDefinerAndInvokerSecurity(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table security_source (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into security_source values (7)")
	mustExecSQL(t, executor, "app", "create view definer_view as select id from security_source")
	mustExecSQL(t, executor, "app", "create sql security invoker view invoker_view as select id from security_source")
	mustExecSQL(t, executor, "", "create user 'view_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "view_reader")
	session.SetParamByName("host", "localhost")
	definerResult := <-executor.ExecuteQuery(session, "select id from definer_view", "app")
	require.NoError(t, definerResult.Err)
	definerData, ok := definerResult.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, definerData.Records, 1)
	require.Equal(t, int64(7), definerData.Records[0].GetValueByIndex(0).Int())

	invokerResult := <-executor.ExecuteQuery(session, "select id from invoker_view", "app")
	require.Error(t, invokerResult.Err)
	require.Contains(t, strings.ToLower(invokerResult.Err.Error()), "select")
}

func TestShowTablesHidesObjectsWithoutTablePrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table visible_table (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'table_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "table_reader")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("database", "app")

	denied := <-executor.ExecuteQuery(session, "show tables from app", "app")
	require.NoError(t, denied.Err)
	deniedData, ok := denied.Data.(map[string]interface{})
	require.True(t, ok)
	require.Empty(t, deniedData["rows"])
	deniedFull := <-executor.ExecuteQuery(session, "show full tables from app", "app")
	require.NoError(t, deniedFull.Err)
	deniedFullData, ok := deniedFull.Data.(map[string]interface{})
	require.True(t, ok)
	require.Empty(t, deniedFullData["rows"])
	deniedStatus := <-executor.ExecuteQuery(session, "show table status from app like 'visible_table'", "app")
	require.NoError(t, deniedStatus.Err)
	deniedStatusData, ok := deniedStatus.Data.(map[string]interface{})
	require.True(t, ok)
	require.Empty(t, deniedStatusData["rows"])
	deniedCreate := <-executor.ExecuteQuery(session, "show create table visible_table", "app")
	require.Error(t, deniedCreate.Err)
	require.Contains(t, strings.ToLower(deniedCreate.Err.Error()), "privilege")
	deniedColumns := <-executor.ExecuteQuery(session, "show columns from visible_table", "app")
	require.NoError(t, deniedColumns.Err)
	deniedColumnsData, ok := deniedColumns.Data.(map[string]interface{})
	require.True(t, ok)
	require.Empty(t, deniedColumnsData["rows"])
	deniedIndex := <-executor.ExecuteQuery(session, "show index from visible_table", "app")
	require.NoError(t, deniedIndex.Err)
	deniedIndexData, ok := deniedIndex.Data.(map[string]interface{})
	require.True(t, ok)
	require.Empty(t, deniedIndexData["rows"])

	mustExecSQL(t, executor, "", "grant select on app.visible_table to 'table_reader'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "show tables from app", "app")
	require.NoError(t, allowed.Err)
	allowedData, ok := allowed.Data.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"visible_table"}}, allowedData["rows"])
	allowedFull := <-executor.ExecuteQuery(session, "show full tables from app", "app")
	require.NoError(t, allowedFull.Err)
	allowedFullData, ok := allowedFull.Data.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"visible_table", "BASE TABLE"}}, allowedFullData["rows"])
	allowedStatus := <-executor.ExecuteQuery(session, "show table status from app like 'visible_table'", "app")
	require.NoError(t, allowedStatus.Err)
	allowedStatusData, ok := allowedStatus.Data.(map[string]interface{})
	require.True(t, ok)
	require.Len(t, allowedStatusData["rows"], 1)
	allowedCreate := <-executor.ExecuteQuery(session, "show create table visible_table", "app")
	require.NoError(t, allowedCreate.Err)
}

func TestInformationSchemaTableMetadataRequiresTablePrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table secret_table (id int primary key, label varchar(20))")
	mustExecSQL(t, executor, "", "create user 'metadata_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "metadata_reader")
	session.SetParamByName("host", "localhost")

	showDatabasesDenied := <-executor.ExecuteQuery(session, "show databases like 'app'", "")
	require.NoError(t, showDatabasesDenied.Err)
	showDatabasesDeniedData, ok := showDatabasesDenied.Data.(map[string]interface{})
	require.True(t, ok)
	require.Empty(t, showDatabasesDeniedData["rows"])
	showCreateDatabaseDenied := <-executor.ExecuteQuery(session, "show create database app", "")
	require.Error(t, showCreateDatabaseDenied.Err)
	require.Contains(t, strings.ToLower(showCreateDatabaseDenied.Err.Error()), "privilege")

	for _, query := range []string{
		"select schema_name from information_schema.schemata where schema_name = 'app'",
		"select table_name from information_schema.tables where table_schema = 'app' and table_name = 'secret_table'",
		"select column_name from information_schema.columns where table_schema = 'app' and table_name = 'secret_table'",
		"select index_name from information_schema.statistics where table_schema = 'app' and table_name = 'secret_table'",
	} {
		require.Empty(t, mustQuerySessionSQL(t, executor, session, "", query), query)
	}

	mustExecSQL(t, executor, "", "grant select on app.secret_table to 'metadata_reader'@'localhost'")
	showDatabasesAllowed := <-executor.ExecuteQuery(session, "show databases like 'app'", "")
	require.NoError(t, showDatabasesAllowed.Err)
	showDatabasesAllowedData, ok := showDatabasesAllowed.Data.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"app"}}, showDatabasesAllowedData["rows"])
	showCreateDatabaseAllowed := <-executor.ExecuteQuery(session, "show create database app", "")
	require.NoError(t, showCreateDatabaseAllowed.Err)
	require.Equal(t, [][]interface{}{{"app"}}, mustQuerySessionSQL(t, executor, session, "", "select schema_name from information_schema.schemata where schema_name = 'app'"))
	require.Equal(t, [][]interface{}{{"secret_table"}}, mustQuerySessionSQL(t, executor, session, "", "select table_name from information_schema.tables where table_schema = 'app' and table_name = 'secret_table'"))
	require.Equal(t, [][]interface{}{{"id"}, {"label"}}, mustQuerySessionSQL(t, executor, session, "", "select column_name from information_schema.columns where table_schema = 'app' and table_name = 'secret_table' order by ordinal_position"))
	require.Equal(t, [][]interface{}{{"PRIMARY"}}, mustQuerySessionSQL(t, executor, session, "", "select index_name from information_schema.statistics where table_schema = 'app' and table_name = 'secret_table' and index_name = 'PRIMARY'"))
}

func TestShowRoutineStatusReturnsProcedureAndFunctionMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure refresh_proc() begin select 1; end")
	mustExecSQL(t, executor, "app", "create function refresh_fn(value int) returns int return value + 1")

	result := <-executor.ExecuteQuery(nil, "show procedure status from app like 'refresh%'", "app")
	require.NoError(t, result.Err)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok, "expected SHOW result map, got %T", result.Data)
	require.Equal(t, []string{"Db", "Name", "Type", "Definer", "Modified", "Created", "Security_type", "Comment", "character_set_client", "collation_connection", "Database Collation"}, data["columns"])
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok, "expected SHOW rows, got %T", data["rows"])
	require.Len(t, rows, 1)
	require.Equal(t, "app", fmt.Sprint(rows[0][0]))
	require.Equal(t, "refresh_proc", fmt.Sprint(rows[0][1]))
	require.Equal(t, "PROCEDURE", fmt.Sprint(rows[0][2]))

	functionResult := <-executor.ExecuteQuery(nil, "show function status from app like 'refresh%'", "app")
	require.NoError(t, functionResult.Err)
	functionData, ok := functionResult.Data.(map[string]interface{})
	require.True(t, ok, "expected function SHOW result map, got %T", functionResult.Data)
	functionRows, ok := functionData["rows"].([][]interface{})
	require.True(t, ok, "expected function SHOW rows, got %T", functionData["rows"])
	require.Len(t, functionRows, 1)
	require.Equal(t, "refresh_fn", fmt.Sprint(functionRows[0][1]))
	require.Equal(t, "FUNCTION", fmt.Sprint(functionRows[0][2]))
}

func TestShowRoutineStatusRequiresShowRoutinePrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure report() begin select 1; end")
	mustExecSQL(t, executor, "app", "create function score() returns int return 1")
	mustExecSQL(t, executor, "", "create user 'routine_status_viewer'@'localhost' identified by 'secret'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "routine_status_viewer")
	session.SetParamByName("host", "localhost")

	for _, query := range []string{
		"show procedure status from app",
		"show function status from app",
		"select routine_name from information_schema.routines where routine_schema = 'app'",
		"select specific_name from information_schema.parameters where specific_schema = 'app'",
	} {
		denied := <-executor.ExecuteQuery(session, query, "")
		require.Error(t, denied.Err, query)
		require.Contains(t, strings.ToLower(denied.Err.Error()), "show_routine", query)
	}

	mustExecSQL(t, executor, "", "grant show_routine on *.* to 'routine_status_viewer'@'localhost'")
	for _, query := range []string{
		"show procedure status from app",
		"show function status from app",
		"select routine_name from information_schema.routines where routine_schema = 'app'",
		"select specific_name from information_schema.parameters where specific_schema = 'app'",
	} {
		allowed := <-executor.ExecuteQuery(session, query, "")
		require.NoError(t, allowed.Err, query)
	}
}

func TestShowCreateStoredObjectsReturnsMySQLMetadataShape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit_rows (id int primary key, flag varchar(20))")
	mustExecSQL(t, executor, "app", "create procedure refresh_proc() begin select 1; end")
	mustExecSQL(t, executor, "app", "create function refresh_fn(value int) returns int return value + 1")
	mustExecSQL(t, executor, "app", "create trigger audit_before before insert on audit_rows for each row set new.flag = 'seen'")
	mustExecSQL(t, executor, "app", "create event refresh_event on schedule every 2 hour do select 1")

	tests := []struct {
		query   string
		columns []string
		name    string
	}{
		{"show create procedure app.refresh_proc", []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}, "refresh_proc"},
		{"show create function app.refresh_fn", []string{"Function", "sql_mode", "Create Function", "character_set_client", "collation_connection", "Database Collation"}, "refresh_fn"},
		{"show create trigger app.audit_before", []string{"Trigger", "sql_mode", "SQL Original Statement", "character_set_client", "collation_connection", "Database Collation"}, "audit_before"},
		{"show create event app.refresh_event", []string{"Event", "sql_mode", "time_zone", "Create Event", "character_set_client", "collation_connection", "Database Collation"}, "refresh_event"},
	}
	for _, test := range tests {
		result := <-executor.ExecuteQuery(nil, test.query, "")
		require.NoError(t, result.Err, test.query)
		selectResult, ok := result.Data.(*SelectResult)
		require.True(t, ok, "%s result = %T", test.query, result.Data)
		require.Equal(t, test.columns, selectResult.Columns, test.query)
		require.Len(t, selectResult.Records, 1, test.query)
		values := selectResult.Records[0].GetValues()
		require.Equal(t, test.name, values[0].String(), test.query)
		createIndex := len(test.columns) - len([]string{"character_set_client", "collation_connection", "Database Collation"}) - 1
		require.Contains(t, strings.ToUpper(values[createIndex].String()), "CREATE", test.query)
	}
}

func TestPersistedProcedureCallExecutesSelectBody(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure report() begin select 1; end")
	result := <-executor.ExecuteQuery(nil, "call report()", "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok, "expected procedure result set, got %T", result.Data)
	require.NotEmpty(t, selectResult.Records)
}

func TestPersistedProcedureCallSupportsQualifiedRoutineName(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure report() begin select 7; end")

	result := <-executor.ExecuteQuery(nil, "call app.report()", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok, "expected qualified procedure result set, got %T", result.Data)
	require.NotEmpty(t, selectResult.Records)
	require.Equal(t, int64(7), selectResult.Records[0].GetValues()[0].Int())
}

func TestAlterRoutinePersistsSecurityAndCommentMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure report() begin select 1; end")
	mustExecSQL(t, executor, "", "alter procedure app.report sql security invoker comment 'reporting routine'")

	rows := mustQuerySQL(t, executor, "", "select security_type, routine_comment from information_schema.routines where routine_schema = 'app' and routine_name = 'report'")
	require.Equal(t, [][]interface{}{{"INVOKER", "reporting routine"}}, rows)
}

func TestRoutineMetadataReflectsDeterminismDataAccessAndCreateComment(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create function lookup() returns int not deterministic reads sql data comment 'lookup routine' begin return 1; end")

	rows := mustQuerySQL(t, executor, "", "select is_deterministic, sql_data_access, routine_comment from information_schema.routines where routine_schema = 'app' and routine_name = 'lookup'")
	require.Equal(t, [][]interface{}{{"NO", "READS SQL DATA", "lookup routine"}}, rows)
}

func TestShowCreateRoutineRequiresShowRoutinePrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure report() begin select 1; end")
	mustExecSQL(t, executor, "", "create user 'routine_viewer'@'localhost' identified by 'secret'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "routine_viewer")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "show create procedure app.report", "")
	require.Error(t, denied.Err)
	require.Contains(t, denied.Err.Error(), "SHOW_ROUTINE")

	mustExecSQL(t, executor, "", "grant show_routine on *.* to 'routine_viewer'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "show create procedure app.report", "")
	require.NoError(t, allowed.Err)
}

func TestAlterRoutinePersistsDeterminismAndDataAccess(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create function lookup() returns int deterministic no sql begin return 1; end")
	mustExecSQL(t, executor, "", "alter function app.lookup not deterministic reads sql data")

	rows := mustQuerySQL(t, executor, "", "select is_deterministic, sql_data_access from information_schema.routines where routine_schema = 'app' and routine_name = 'lookup'")
	require.Equal(t, [][]interface{}{{"NO", "READS SQL DATA"}}, rows)
}

func TestPersistedProcedureCallExecutesDMLBody(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table counters (id int primary key, value int)")
	mustExecSQL(t, executor, "app", "create procedure add_counter() begin insert into counters values (1, 7); end")
	result := <-executor.ExecuteQuery(nil, "call add_counter()", "app")
	require.NoError(t, result.Err)
	rows := mustQuerySQL(t, executor, "app", "select value from counters where id = 1")
	require.Equal(t, [][]interface{}{{"7"}}, rows)
}

func TestProcedureParametersArePersistedAndSubstitutedOnCall(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table values_table (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into values_table values (7)")
	mustExecSQL(t, executor, "app", "create procedure echo_value(IN input_value INT) begin select id from values_table where id = input_value; end")
	result := <-executor.ExecuteQuery(nil, "call echo_value(7)", "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.NotEmpty(t, selectResult.Records)
	require.Equal(t, int64(7), selectResult.Records[0].GetValues()[0].Int())
}

func TestProcedureSelectIntoPublishesQueryValueToOutParameter(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table readings (id int primary key, reading int)")
	mustExecSQL(t, executor, "app", "insert into readings values (7, 42)")
	mustExecSQL(t, executor, "app", "create procedure lookup_reading(IN reading_id INT, OUT reading_value INT) begin select reading into reading_value from readings where id = reading_id; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call lookup_reading(7, @answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"42"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureSelectIntoNoRowsInvokesNotFoundHandler(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table readings (id int primary key, reading int)")
	mustExecSQL(t, executor, "app", "create procedure lookup_missing(OUT reading_value INT) begin declare continue handler for not found set reading_value = 99; select reading into reading_value from readings where id = 7; set reading_value = reading_value + 1; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call lookup_missing(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"100"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureSelectIntoMultipleRowsInvokesSQLExceptionHandler(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table readings (id int primary key, reading int)")
	mustExecSQL(t, executor, "app", "insert into readings values (1, 41), (2, 42)")
	mustExecSQL(t, executor, "app", "create procedure lookup_many(OUT reading_value INT) begin declare continue handler for sqlexception set reading_value = 77; select reading into reading_value from readings; set reading_value = reading_value + 1; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call lookup_many(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"78"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureOutParameterPublishesSessionUserVariable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure bump(IN input_value INT, OUT output_value INT) begin set output_value = input_value + 1; end")
	session := newTestMySQLSession()
	result := <-executor.ExecuteQuery(session, "call bump(7, @answer)", "app")
	require.NoError(t, result.Err)
	rows := mustQuerySessionSQL(t, executor, session, "app", "select @answer")
	require.Equal(t, [][]interface{}{{"8"}}, rows)
}

func TestProcedureIfControlFlowChoosesBranch(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure choose(IN input_value INT, OUT output_value INT) begin if input_value > 0 then set output_value = 1; else set output_value = 0; end if; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call choose(-1, @answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"0"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureWhileControlFlowRepeatsUntilConditionFails(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure count_up(OUT output_value INT) begin set @counter = 0; while @counter < 3 do set @counter = @counter + 1; end while; set output_value = @counter; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call count_up(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"3"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureLocalVariablesSupportDeclareSetAndWhile(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure count_local(OUT output_value INT) begin declare counter INT default 0; while counter < 3 do set counter = counter + 1; end while; set output_value = counter; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call count_local(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"3"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureRepeatControlFlowRunsBodyBeforeCondition(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure count_repeat(OUT output_value INT) begin declare counter INT default 0; repeat set counter = counter + 1; until counter >= 3 end repeat; set output_value = counter; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call count_repeat(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"3"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureNestedBeginEndBlockExecutesWithLocalStatements(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure nested_block(OUT output_value INT) begin declare total INT default 1; begin declare increment INT default 2; set total = total + increment; end; set output_value = total; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call nested_block(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"3"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureNestedBeginEndBlockDoesNotLeakLocalVariables(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure scoped_block(OUT output_value INT) begin begin declare inner_value INT default 2; end; set output_value = inner_value; end")
	session := newTestMySQLSession()
	result := <-executor.ExecuteQuery(session, "call scoped_block(@answer)", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "inner_value")
}

func TestProcedureIfSupportsElseIfBranches(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure choose_branch(IN input_value INT, OUT output_value INT) begin if input_value > 0 then set output_value = 1; elseif input_value = 0 then set output_value = 2; else set output_value = 3; end if; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call choose_branch(0, @answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureSupportsRecursiveCallWithChangingInput(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table recursion_trace (n int)")
	mustExecSQL(t, executor, "app", "create procedure descend(IN input_value INT) begin if input_value > 0 then insert into recursion_trace values (input_value); call descend(input_value - 1); end if; end")
	result := <-executor.ExecuteQuery(nil, "call descend(3)", "app")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"1"}, {"2"}, {"3"}}, mustQuerySQL(t, executor, "app", "select n from recursion_trace order by n"))
}

func TestProcedureSupportsRecursiveInOutUserVariable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure accumulate(IN input_value INT, INOUT total_value INT) begin if input_value > 0 then set total_value = total_value + input_value; call accumulate(input_value - 1, total_value); end if; end")
	mustExecSessionSQL(t, executor, session, "app", "set @total = 0")
	require.NoError(t, (<-executor.ExecuteQuery(session, "call accumulate(3, @total)", "app")).Err)
	require.Equal(t, [][]interface{}{{"6"}}, mustQuerySessionSQL(t, executor, session, "app", "select @total"))
}

func TestProcedurePropagatesNestedLocalInOutParameter(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure increment(INOUT value INT) begin set value = value + 1; end")
	mustExecSessionSQL(t, executor, session, "app", "create procedure wrapper(OUT output_value INT) begin declare local_value INT default 1; call increment(local_value); set output_value = local_value; end")
	require.NoError(t, (<-executor.ExecuteQuery(session, "call wrapper(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureCursorHandlerAndLoopCanConsumeRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table values_table (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into values_table (id) values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "create procedure sum_cursor(OUT output_value INT) begin declare total INT default 0; declare done INT default 0; declare current_value INT default 0; declare cur cursor for select id from values_table order by id; declare continue handler for not found set done = 1; open cur; read_loop: loop fetch cur into current_value; if done = 1 then leave read_loop; end if; set total = total + current_value; end loop; close cur; set output_value = total; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call sum_cursor(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"6"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureExitHandlerLeavesRoutineAfterNotFound(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table empty_values (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into empty_values values (1)")
	mustExecSQL(t, executor, "app", "create procedure exit_on_empty(OUT output_value INT) begin declare exit handler for not found set output_value = 99; declare current_value INT default 0; declare cur cursor for select id from empty_values where id > 99; open cur; fetch cur into current_value; set output_value = current_value; close cur; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call exit_on_empty(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"99"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureCursorRequiresOpenBeforeFetch(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table cursor_values (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into cursor_values values (1)")
	mustExecSQL(t, executor, "app", "create procedure fetch_closed(OUT output_value INT) begin declare current_value INT default 0; declare cur cursor for select id from cursor_values; fetch cur into current_value; set output_value = current_value; end")
	session := newTestMySQLSession()
	result := <-executor.ExecuteQuery(session, "call fetch_closed(@answer)", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "not open")
}

func TestProcedureCursorCanReopenAfterClose(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table reopen_values (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into reopen_values values (7)")
	mustExecSQL(t, executor, "app", "create procedure reopen_cursor(OUT output_value INT) begin declare current_value INT default 0; declare cur cursor for select id from reopen_values; open cur; fetch cur into current_value; close cur; open cur; fetch cur into current_value; close cur; set output_value = current_value; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call reopen_cursor(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureCursorSupportsFetchNextFromSyntax(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table fetch_next_values (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "insert into fetch_next_values values (7)")
	mustExecSessionSQL(t, executor, session, "app", "create procedure fetch_next(OUT output_value INT) begin declare current_value INT default 0; declare cur cursor for select id from fetch_next_values; open cur; fetch next from cur into current_value; close cur; set output_value = current_value; end")
	mustExecSessionSQL(t, executor, session, "app", "call fetch_next(@answer)")
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureCursorEvaluatesQueryAtOpen(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table open_cursor_values (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "insert into open_cursor_values values (1), (2)")
	mustExecSessionSQL(t, executor, session, "app", "create procedure cursor_open_time(OUT output_value INT) begin declare wanted INT default 1; declare current_value INT default 0; declare cur cursor for select id from open_cursor_values where id = wanted; set wanted = 2; open cur; fetch cur into current_value; close cur; set output_value = current_value; end")
	mustExecSessionSQL(t, executor, session, "app", "call cursor_open_time(@answer)")
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureCursorRejectsOpeningAnAlreadyOpenCursor(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table open_twice_values (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into open_twice_values values (1)")
	mustExecSQL(t, executor, "app", "create procedure open_twice() begin declare cur cursor for select id from open_twice_values; open cur; open cur; end")

	result := <-executor.ExecuteQuery(newTestMySQLSession(), "call open_twice()", "app")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "already open")
}

func TestProcedureFetchRejectsMismatchedOrUndeclaredTargets(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table fetch_targets (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into fetch_targets values (1)")
	mustExecSQL(t, executor, "app", "create procedure fetch_mismatch() begin declare first_value INT; declare second_value INT; declare cur cursor for select id from fetch_targets; open cur; fetch cur into first_value, second_value; end")
	mustExecSQL(t, executor, "app", "create procedure fetch_undeclared() begin declare cur cursor for select id from fetch_targets; open cur; fetch cur into missing_value; end")

	result := <-executor.ExecuteQuery(newTestMySQLSession(), "call fetch_mismatch()", "app")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "target")

	result = <-executor.ExecuteQuery(newTestMySQLSession(), "call fetch_undeclared()", "app")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "declared")
}

func TestProcedureNestedBlockPreservesVariableScope(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure nested_scope(OUT first_value INT, OUT second_value INT) begin declare value INT default 1; begin declare value INT default 2; set value = 3; end; set first_value = value; begin set value = 4; end; set second_value = value; end")
	mustExecSessionSQL(t, executor, session, "app", "call nested_scope(@first_answer, @second_answer)")
	require.Equal(t, [][]interface{}{{"1", "4"}}, mustQuerySessionSQL(t, executor, session, "app", "select @first_answer, @second_answer"))
}

func TestProcedureRejectsDuplicateLocalVariableInSameBlock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure duplicate_local() begin declare value int; declare value int; end")
	result := <-executor.ExecuteQuery(session, "call duplicate_local()", "app")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "duplicate")
}

func TestProcedureContinueHandlerCatchesSQLExceptions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table unique_values (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into unique_values values (1)")
	mustExecSQL(t, executor, "app", "create procedure catch_duplicate(OUT output_value INT) begin declare continue handler for sqlexception set output_value = 42; insert into unique_values values (1); set output_value = output_value + 1; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call catch_duplicate(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"43"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureSignalSQLStateUsesSpecificHandler(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure signal_handler(OUT output_value INT) begin declare continue handler for SQLSTATE '45000' set output_value = 7; signal SQLSTATE '45000' set message_text = 'boom'; set output_value = output_value + 1; end")
	result := <-executor.ExecuteQuery(session, "call signal_handler(@answer)", "app")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"8"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureHandlerSupportsMultipleConditions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure multi_condition_handler(OUT output_value INT) begin declare continue handler for SQLWARNING, SQLSTATE '45000' set output_value = output_value + 1; set output_value = 0; signal SQLSTATE '45000' set message_text = 'boom'; set output_value = output_value + 1; end")
	mustExecSessionSQL(t, executor, session, "app", "call multi_condition_handler(@answer)")
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureHandlerConditionAliasSupportsConditionLists(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table handler_alias_values (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "insert into handler_alias_values values (1)")
	mustExecSessionSQL(t, executor, session, "app", "create procedure handler_alias(OUT output_value INT) begin declare duplicate_row condition for 1062; declare continue handler for duplicate_row, SQLWARNING set output_value = output_value + 1; set output_value = 0; insert into handler_alias_values values (1); set output_value = output_value + 1; end")
	mustExecSessionSQL(t, executor, session, "app", "call handler_alias(@answer)")
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureHandlerPrefersSpecificConditionOverGenericHandler(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure handler_precedence(OUT output_value INT) begin declare continue handler for SQLSTATE '45000' set output_value = 1; declare continue handler for SQLEXCEPTION set output_value = 2; set output_value = 0; signal SQLSTATE '45000' set message_text = 'boom'; end")
	mustExecSessionSQL(t, executor, session, "app", "call handler_precedence(@answer)")
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureSupportsSimpleAndSearchedCaseStatements(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure choose_case(IN input_value INT, OUT output_value INT) begin case input_value when 1 then set output_value = 10; when 2 then set output_value = 20; else set output_value = 0; end case; end")
	mustExecSessionSQL(t, executor, session, "app", "call choose_case(2, @answer)")
	require.Equal(t, [][]interface{}{{"20"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
	mustExecSessionSQL(t, executor, session, "app", "create procedure choose_searched_case(IN input_value INT, OUT output_value INT) begin case when input_value < 0 then set output_value = -1; when input_value = 0 then set output_value = 0; else set output_value = 1; end case; end")
	mustExecSessionSQL(t, executor, session, "app", "call choose_searched_case(3, @answer)")
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureConditionAliasSelectsSQLStateHandler(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure condition_alias(OUT output_value INT) begin declare duplicate CONDITION FOR SQLSTATE '45000'; declare continue handler for duplicate set output_value = 7; signal SQLSTATE '45000'; set output_value = output_value + 1; end")
	require.NoError(t, (<-executor.ExecuteQuery(session, "call condition_alias(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"8"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureSignalSQLStateWarningUsesWarningHandler(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure warning_signal(OUT output_value INT) begin declare continue handler for SQLSTATE '01000' set output_value = 3; signal SQLSTATE '01000' set message_text = 'warning'; set output_value = output_value + 1; end")
	result := <-executor.ExecuteQuery(session, "call warning_signal(@answer)", "app")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"4"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureResignalRewritesActiveConditionMessage(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	result := <-executor.ExecuteQuery(session, "create procedure resignal_message() begin declare continue handler for sqlstate '45000' resignal set message_text = 'rewritten'; signal sqlstate '45000' set message_text = 'original'; end", "app")
	require.NoError(t, result.Err)
	result = <-executor.ExecuteQuery(session, "call resignal_message()", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "rewritten")
}

func TestProcedureResignalCanRewriteMySQLErrorNumber(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure resignal_errno() begin declare continue handler for sqlstate '45000' resignal set mysql_errno = 30002, message_text = 'rewritten'; signal sqlstate '45000' set mysql_errno = 30001, message_text = 'original'; end")
	result := <-executor.ExecuteQuery(session, "call resignal_errno()", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "ERROR 30002 (45000)")
	require.Contains(t, result.Err.Error(), "rewritten")
}

func TestProcedureGetDiagnosticsReadsSignaledMessage(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure read_diagnostics(OUT output_value VARCHAR(40)) begin declare continue handler for SQLSTATE '45000' get diagnostics condition 1 output_value = MESSAGE_TEXT; signal SQLSTATE '45000' set message_text = 'boom'; end")
	mustExecSessionSQL(t, executor, session, "app", "call read_diagnostics(@answer)")
	require.Equal(t, [][]interface{}{{"boom"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureSignalCanSetMySQLErrorNumber(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure signal_errno(OUT output_value INT) begin declare continue handler for SQLSTATE '45000' get diagnostics condition 1 output_value = MYSQL_ERRNO; signal SQLSTATE '45000' SET MYSQL_ERRNO = 30001, MESSAGE_TEXT = 'boom'; end")
	mustExecSessionSQL(t, executor, session, "app", "call signal_errno(@answer)")
	require.Equal(t, [][]interface{}{{"30001"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureSignalReturnsCustomErrorContractWithoutHandler(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create procedure fail_with_errno() begin signal sqlstate '45000' set mysql_errno = 30003, message_text = 'failed'; end")
	result := <-executor.ExecuteQuery(session, "call fail_with_errno()", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "ERROR 30003 (45000)")
	require.Contains(t, result.Err.Error(), "failed")
}

func TestProcedureGetDiagnosticsReadsStatementErrorMessage(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table diagnostics_unique (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "insert into diagnostics_unique values (1)")
	mustExecSessionSQL(t, executor, session, "app", "create procedure read_statement_error(OUT output_value VARCHAR(200)) begin declare continue handler for SQLEXCEPTION get diagnostics condition 1 output_value = MESSAGE_TEXT; insert into diagnostics_unique values (1); end")
	mustExecSessionSQL(t, executor, session, "app", "call read_statement_error(@answer)")
	rows := mustQuerySessionSQL(t, executor, session, "app", "select @answer")
	require.Len(t, rows, 1)
	require.Contains(t, strings.ToLower(fmt.Sprint(rows[0][0])), "duplicate")
}

func TestProcedureGetDiagnosticsReadsStatementErrorNumber(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table diagnostics_errno (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "insert into diagnostics_errno values (1)")
	mustExecSessionSQL(t, executor, session, "app", "create procedure read_statement_errno(OUT output_value INT) begin declare continue handler for SQLEXCEPTION get diagnostics condition 1 output_value = MYSQL_ERRNO; insert into diagnostics_errno values (1); end")
	mustExecSessionSQL(t, executor, session, "app", "call read_statement_errno(@answer)")
	require.Equal(t, [][]interface{}{{"1062"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureGetDiagnosticsReadsStatementSQLState(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table diagnostics_state (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "insert into diagnostics_state values (1)")
	mustExecSessionSQL(t, executor, session, "app", "create procedure read_statement_state(OUT output_value VARCHAR(10)) begin declare continue handler for SQLEXCEPTION get diagnostics condition 1 output_value = RETURNED_SQLSTATE; insert into diagnostics_state values (1); end")
	mustExecSessionSQL(t, executor, session, "app", "call read_statement_state(@answer)")
	require.Equal(t, [][]interface{}{{"23000"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureGetDiagnosticsReadsSelectIntoCardinalityError(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table diagnostics_rows (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "insert into diagnostics_rows values (1), (2)")
	mustExecSessionSQL(t, executor, session, "app", "create procedure read_cardinality_error(OUT output_value VARCHAR(200)) begin declare continue handler for SQLEXCEPTION get diagnostics condition 1 output_value = MESSAGE_TEXT; select id into output_value from diagnostics_rows; end")
	mustExecSessionSQL(t, executor, session, "app", "call read_cardinality_error(@answer)")
	rows := mustQuerySessionSQL(t, executor, session, "app", "select @answer")
	require.Len(t, rows, 1)
	require.Contains(t, strings.ToLower(fmt.Sprint(rows[0][0])), "more than one row")
}

func TestProcedureDynamicSQLPrepareExecuteAndDeallocate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure run_dynamic(OUT output_value INT) begin set @dynamic_sql = 'set @dynamic_value = 41'; prepare dynamic_stmt from @dynamic_sql; execute dynamic_stmt; deallocate prepare dynamic_stmt; set output_value = @dynamic_value + 1; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call run_dynamic(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"42"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureContinueHandlerCatchesSQLWarnings(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table warning_values (id int primary key, name varchar(3))")
	mustExecSQL(t, executor, "app", "create procedure catch_warning(OUT output_value INT) begin declare continue handler for sqlwarning set output_value = 1; insert ignore into warning_values values (1, 'long-name'); set output_value = output_value + 1; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call catch_warning(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureExitHandlerExitsOnlyDeclaringNestedBlock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table nested_unique (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into nested_unique values (1)")
	mustExecSQL(t, executor, "app", "create procedure nested_exit(OUT output_value INT) begin begin declare exit handler for sqlexception set output_value = 7; insert into nested_unique values (1); set output_value = 99; end; set output_value = output_value + 1; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call nested_exit(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"8"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureDynamicSQLExecuteUsingBindsUserVariables(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure run_dynamic_using(OUT output_value INT) begin set @dynamic_sql = 'set @dynamic_value = ?'; set @dynamic_arg = 41; prepare dynamic_stmt from @dynamic_sql; execute dynamic_stmt using @dynamic_arg; deallocate prepare dynamic_stmt; set output_value = @dynamic_value + 1; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call run_dynamic_using(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"42"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureDynamicSQLBindsMultipleMarkersWithoutReplacingQuotedQuestionMarks(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure run_dynamic_multiple(OUT output_value VARCHAR(40)) begin set @dynamic_sql = 'set @dynamic_text = concat(?, ''?'', ?)'; set @dynamic_first = 'left'; set @dynamic_second = 'right'; prepare dynamic_stmt from @dynamic_sql; execute dynamic_stmt using @dynamic_first, @dynamic_second; deallocate prepare dynamic_stmt; set output_value = @dynamic_text; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call run_dynamic_multiple(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"left?right"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestProcedureDynamicSQLExecuteUsingBindsLocalVariable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure run_dynamic_local(OUT output_value INT) begin declare dynamic_arg INT default 0; set dynamic_arg = 41; set @dynamic_sql = 'set @dynamic_value = ?'; prepare dynamic_stmt from @dynamic_sql; execute dynamic_stmt using dynamic_arg; deallocate prepare dynamic_stmt; set output_value = @dynamic_value + 1; end")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "call run_dynamic_local(@answer)", "app")).Err)
	require.Equal(t, [][]interface{}{{"42"}}, mustQuerySessionSQL(t, executor, session, "app", "select @answer"))
}

func TestStoredFunctionCanBeUsedInsideRowProjection(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table values_table (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into values_table values (1), (4)")
	mustExecSQL(t, executor, "app", "create function plus_one(input_value INT) returns INT deterministic begin return input_value + 1; end")
	rows := mustQuerySQL(t, executor, "app", "select id, plus_one(id) as next_value from values_table order by id")
	require.Equal(t, [][]interface{}{{"1", "2"}, {"4", "5"}}, rows)
}

func TestBeforeInsertTriggerMutatesRowBeforeValidation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit (id int primary key, flag varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger audit_before before insert on audit for each row set new.flag = 'triggered'")
	mustExecSQL(t, executor, "app", "insert into audit values (1, 'caller')")
	rows := mustQuerySQL(t, executor, "app", "select flag from audit")
	if len(rows) != 1 || rows[0][0] != "triggered" {
		t.Fatalf("trigger mutation = %#v, want triggered", rows)
	}
}

func TestBeforeInsertTriggerProgramSummaryTracksExecution(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit_program (id int primary key, flag varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger audit_program_before before insert on audit_program for each row set new.flag = 'triggered'")
	mustExecSQL(t, executor, "app", "insert into audit_program values (1, 'caller')")

	program := mustQuerySQL(t, executor, "", "select object_type, object_schema, object_name, count_statements from performance_schema.events_statements_summary_by_program where object_type='TRIGGER' and object_schema='app' and object_name='audit_program_before'")
	require.Len(t, program, 1)
	require.NotEqual(t, "0", program[0][3])
}

func TestDMLWaitsForExplicitTriggerMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	writer := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table trigger_lock_target (id int primary key, flag varchar(20))")
	mustExecSessionSQL(t, executor, owner, "app", "create trigger trigger_lock_before before insert on trigger_lock_target for each row set new.flag = 'triggered'")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables app.trigger_lock_before write")

	insertDone := make(chan *Result, 1)
	go func() {
		insertDone <- <-executor.ExecuteQuery(writer, "insert into trigger_lock_target values (1, 'caller')", "app")
	}()

	select {
	case result := <-insertDone:
		t.Fatalf("INSERT completed while the trigger metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-insertDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("INSERT did not complete after UNLOCK TABLES released the trigger metadata lock")
	}
	require.Equal(t, [][]interface{}{{"triggered"}}, mustQuerySQL(t, executor, "app", "select flag from trigger_lock_target"))
}

func TestBeforeInsertTriggerExecutesMultipleNewAssignments(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit_multi (id int primary key, flag varchar(20), note varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger audit_multi_before before insert on audit_multi for each row begin set new.flag = 'triggered'; set new.note = 'audited'; end")
	mustExecSQL(t, executor, "app", "insert into audit_multi values (1, 'caller', 'caller-note')")
	require.Equal(t, [][]interface{}{{"triggered", "audited"}}, mustQuerySQL(t, executor, "app", "select flag, note from audit_multi"))
}

func TestBeforeInsertTriggerEvaluatesNewColumnExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit_expression (id int primary key, price int, quantity int, total int)")
	mustExecSQL(t, executor, "app", "create trigger audit_expression_before before insert on audit_expression for each row set new.total = new.price * new.quantity")
	mustExecSQL(t, executor, "app", "insert into audit_expression values (1, 6, 7, 0)")
	require.Equal(t, [][]interface{}{{"42"}}, mustQuerySQL(t, executor, "app", "select total from audit_expression"))
}

func TestBeforeTriggersRespectFollowsOrdering(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit_order (id int primary key, flag varchar(20), note varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger z_first before insert on audit_order for each row set new.flag = 'first'")
	mustExecSQL(t, executor, "app", "create trigger a_second before insert on audit_order for each row follows z_first set new.note = new.flag")
	mustExecSQL(t, executor, "app", "insert into audit_order values (1, 'caller', 'caller-note')")
	require.Equal(t, [][]interface{}{{"first", "first"}}, mustQuerySQL(t, executor, "app", "select flag, note from audit_order"))
}

func TestBeforeTriggersRespectPrecedesOrdering(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit_precedes (id int primary key, flag varchar(20), note varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger a_second before insert on audit_precedes for each row set new.note = new.flag")
	mustExecSQL(t, executor, "app", "create trigger z_first before insert on audit_precedes for each row precedes a_second set new.flag = 'first'")
	mustExecSQL(t, executor, "app", "insert into audit_precedes values (1, 'caller', 'caller-note')")
	require.Equal(t, [][]interface{}{{"first", "first"}}, mustQuerySQL(t, executor, "app", "select flag, note from audit_precedes"))
}

func TestBeforeUpdateTriggerMutatesStoredRow(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit (id int primary key, flag varchar(20))")
	mustExecSQL(t, executor, "app", "insert into audit values (1, 'caller')")
	mustExecSQL(t, executor, "app", "create trigger audit_before_update before update on audit for each row set new.flag = 'triggered'")
	mustExecSQL(t, executor, "app", "update audit set flag = 'caller2' where id = 1")
	require.Equal(t, [][]interface{}{{"triggered"}}, mustQuerySQL(t, executor, "app", "select flag from audit"))
}

func TestAfterTriggerRunsAtPostWriteBoundaryAndRejectsNewMutation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit (id int primary key, flag varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger audit_after after insert on audit for each row set new.flag = 'invalid'")
	result := <-executor.ExecuteQuery(nil, "insert into audit values (1, 'caller')", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "AFTER trigger")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id, flag from audit"))
}

func TestAfterUpdateAndDeleteTriggerFailuresRestoreBaseRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table trigger_failure_rows (id int primary key, flag varchar(20))")
	mustExecSQL(t, executor, "app", "insert into trigger_failure_rows values (1, 'before')")
	mustExecSQL(t, executor, "app", "create trigger trigger_failure_after_update after update on trigger_failure_rows for each row set new.flag = 'invalid'")
	updateResult := <-executor.ExecuteQuery(nil, "update trigger_failure_rows set flag = 'after' where id = 1", "app")
	require.Error(t, updateResult.Err)
	require.Equal(t, [][]interface{}{{"1", "before"}}, mustQuerySQL(t, executor, "app", "select id, flag from trigger_failure_rows"))

	mustExecSQL(t, executor, "app", "drop trigger trigger_failure_after_update")
	mustExecSQL(t, executor, "app", "create trigger trigger_failure_after_delete after delete on trigger_failure_rows for each row set new.flag = 'invalid'")
	deleteResult := <-executor.ExecuteQuery(nil, "delete from trigger_failure_rows where id = 1", "app")
	require.Error(t, deleteResult.Err)
	require.Equal(t, [][]interface{}{{"1", "before"}}, mustQuerySQL(t, executor, "app", "select id, flag from trigger_failure_rows"))
}

func TestAfterInsertTriggerExecutesAuditSideEffect(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit (id int primary key, flag varchar(20))")
	mustExecSQL(t, executor, "app", "create table audit_log (id int primary key, note varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger audit_after after insert on audit for each row insert into audit_log values (new.id, 'inserted')")
	mustExecSQL(t, executor, "app", "insert into audit values (1, 'caller')")
	require.Equal(t, [][]interface{}{{"inserted"}}, mustQuerySQL(t, executor, "app", "select note from audit_log"))
	program := mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_statements_summary_by_program where object_type='TRIGGER' and object_schema='app' and object_name='audit_after'")
	require.Len(t, program.Records, 1)
	require.NotEqual(t, "0", selectResultRows(program)[0][16])
}

func TestAfterTriggerRejectsRecursiveTriggerCycle(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table trigger_cycle_a (id int)")
	mustExecSessionSQL(t, executor, session, "app", "create table trigger_cycle_b (id int)")
	mustExecSessionSQL(t, executor, session, "app", "create trigger cycle_a_after after insert on trigger_cycle_a for each row insert into trigger_cycle_b values (new.id)")
	mustExecSessionSQL(t, executor, session, "app", "create trigger cycle_b_after after insert on trigger_cycle_b for each row insert into trigger_cycle_a values (new.id)")

	result := <-executor.ExecuteQuery(session, "insert into trigger_cycle_a values (1)", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "trigger recursion detected")
	require.Empty(t, mustQuerySessionSQL(t, executor, session, "app", "select id from trigger_cycle_a"))
	require.Empty(t, mustQuerySessionSQL(t, executor, session, "app", "select id from trigger_cycle_b"))
}

func TestAfterTriggersWithOrderingExecuteSideEffects(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table audit_order_after (id int primary key)")
	mustExecSQL(t, executor, "app", "create table audit_order_log (id int primary key, note varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger z_first after insert on audit_order_after for each row insert into audit_order_log values (1, 'first')")
	mustExecSQL(t, executor, "app", "create trigger a_second after insert on audit_order_after for each row follows z_first insert into audit_order_log values (2, 'second')")
	mustExecSQL(t, executor, "app", "insert into audit_order_after values (1)")
	require.Equal(t, [][]interface{}{{"first"}, {"second"}}, mustQuerySQL(t, executor, "app", "select note from audit_order_log order by id"))
}

func TestAfterUpdateAndDeleteTriggersExecuteAuditSideEffects(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_rows (id int primary key, flag varchar(20))")
	mustExecSQL(t, executor, "app", "create table update_log (id int primary key, note varchar(20))")
	mustExecSQL(t, executor, "app", "create table delete_log (id int primary key, note varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger source_after_update after update on source_rows for each row insert into update_log values (new.id, 'updated')")
	mustExecSQL(t, executor, "app", "create trigger source_after_delete after delete on source_rows for each row insert into delete_log values (old.id, 'deleted')")
	mustExecSQL(t, executor, "app", "insert into source_rows values (1, 'before')")
	mustExecSQL(t, executor, "app", "update source_rows set flag = 'after' where id = 1")
	mustExecSQL(t, executor, "app", "delete from source_rows where id = 1")
	require.Equal(t, [][]interface{}{{"updated"}}, mustQuerySQL(t, executor, "app", "select note from update_log"))
	require.Equal(t, [][]interface{}{{"deleted"}}, mustQuerySQL(t, executor, "app", "select note from delete_log"))
}

func TestAfterUpdateTriggerReceivesDistinctOldAndNewRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_old_new (id int primary key, flag varchar(20))")
	mustExecSQL(t, executor, "app", "create table old_new_log (id int primary key, old_flag varchar(20), new_flag varchar(20))")
	mustExecSQL(t, executor, "app", "create trigger source_old_new_after after update on source_old_new for each row insert into old_new_log values (new.id, old.flag, new.flag)")
	mustExecSQL(t, executor, "app", "insert into source_old_new values (1, 'before')")
	mustExecSQL(t, executor, "app", "update source_old_new set flag = 'after' where id = 1")
	require.Equal(t, [][]interface{}{{"1", "before", "after"}}, mustQuerySQL(t, executor, "app", "select id, old_flag, new_flag from old_new_log"))
}

func TestStoredObjectDefinerMustExistAndInvokerNeedsExecute(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	missing := <-executor.ExecuteQuery(nil, "create definer = 'missing'@'localhost' procedure report() begin select 1; end", "app")
	require.Error(t, missing.Err)
	mustExecSQL(t, executor, "", "create user 'owner'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "app", "create definer = 'owner'@'localhost' procedure report() begin select 1; end")
	session := newTestMySQLSession()
	session.SetParamByName("user", "owner")
	session.SetParamByName("host", "localhost")
	denied := <-executor.ExecuteQuery(session, "call report()", "app")
	require.Error(t, denied.Err)
	mustExecSQL(t, executor, "", "grant execute on app.report to 'owner'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "call report()", "app")
	require.NoError(t, allowed.Err)
}

func TestMySQLProcsPrivReflectsRoutineExecuteGrant(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure report() begin select 1; end")
	mustExecSQL(t, executor, "", "create user 'reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant execute on app.report to 'reader'@'localhost'")
	rows := mustQuerySQL(t, executor, "", "select Host, User, Db, Routine_name, Proc_priv, Routine_type = 'PROCEDURE' as is_proc from mysql.procs_priv where Db = 'app' and Routine_name = 'report'")
	require.Equal(t, [][]interface{}{{"localhost", "reader", "app", "report", "EXECUTE", "1"}}, rows)
}

func TestDropTableProtectsStoredObjectDependencies(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create view active_users as select id from users")
	result := <-executor.ExecuteQuery(nil, "drop table users", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "active_users")
}

func TestRoutineMetadataPersistsReturnAndLocalVariableDefinitions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create function add_one(input_value int) returns int begin declare local_value int; set local_value = input_value + 1; return local_value; end")
	raw, err := os.ReadFile(filepath.Join(executor.GetDataDir(), "app", "add_one.routine.json"))
	require.NoError(t, err)
	var object persistedStoredObject
	require.NoError(t, json.Unmarshal(raw, &object))
	require.Equal(t, "INT", object.ReturnType)
	require.Contains(t, object.LocalVariables, "local_value int")
	require.Equal(t, "local_value", object.ReturnExpression)
}

func TestInformationSchemaParametersReturnsStoredRoutineMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure update_value(IN input_value INT, OUT output_value VARCHAR(20)) begin set output_value = input_value; end")
	mustExecSQL(t, executor, "app", "create function add_one(input_value INT) returns INT begin return input_value + 1; end")
	rows := mustQuerySQL(t, executor, "", "select specific_name, column_name, column_type, data_type, type_name from information_schema.parameters where specific_schema = 'app' and specific_name = 'add_one'")
	require.Equal(t, [][]interface{}{{"add_one", "input_value", "1", "4", "INT"}, {"add_one", "RETURN_VALUE", "5", "4", "INT"}}, rows)
}

func TestInformationSchemaRoutinesReturnsNativeRoutineColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create function add_one(input_value INT) returns INT deterministic begin return input_value + 1; end")
	result := <-executor.ExecuteQuery(nil, "select routine_schema, routine_name, routine_type, data_type, is_deterministic from information_schema.routines where routine_schema = 'app'", "")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "DATA_TYPE", "IS_DETERMINISTIC"}, selectResult.Columns)
	require.Len(t, selectResult.Records, 1)
	require.Equal(t, "app", selectResult.Records[0].GetValues()[0].String())
	require.Equal(t, "add_one", selectResult.Records[0].GetValues()[1].String())
	require.Equal(t, "FUNCTION", selectResult.Records[0].GetValues()[2].String())
	require.Equal(t, "INT", selectResult.Records[0].GetValues()[3].String())
	require.Equal(t, "YES", selectResult.Records[0].GetValues()[4].String())
}

func TestInformationSchemaStoredObjectsApplyNativeFieldFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create function add_one(input_value INT) returns INT deterministic begin return input_value + 1; end")
	mustExecSQL(t, executor, "app", "create table audit_rows (id int primary key)")
	mustExecSQL(t, executor, "app", "create trigger audit_before before insert on audit_rows for each row set @seen = 1")
	mustExecSQL(t, executor, "app", "create event refresh_event on schedule every 2 hour do select 1")

	require.Empty(t, mustQuerySQL(t, executor, "", "select routine_schema, routine_name, routine_type from information_schema.routines where routine_schema='app' and routine_name='add_one' and routine_type='PROCEDURE'"))
	require.Equal(t, [][]interface{}{{"app", "add_one", "FUNCTION"}}, mustQuerySQL(t, executor, "", "select routine_schema, routine_name, routine_type from information_schema.routines where routine_schema='app' and routine_name='add_one' and routine_type='FUNCTION'"))
	require.Empty(t, mustQuerySQL(t, executor, "", "select trigger_name from information_schema.triggers where trigger_schema='app' and trigger_name='audit_before' and event_manipulation='UPDATE'"))
	require.Equal(t, [][]interface{}{{"audit_before"}}, mustQuerySQL(t, executor, "", "select trigger_name from information_schema.triggers where trigger_schema='app' and trigger_name='audit_before' and action_timing='BEFORE'"))
	require.Empty(t, mustQuerySQL(t, executor, "", "select event_name from information_schema.events where event_schema='app' and event_name='refresh_event' and event_type='ONE TIME'"))
	require.Equal(t, [][]interface{}{{"refresh_event"}}, mustQuerySQL(t, executor, "", "select event_name from information_schema.events where event_schema='app' and event_name='refresh_event' and interval_field='HOUR'"))
}

func TestStoredFunctionCanBeInvokedFromSelect(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create function add_one(input_value int) returns int begin declare local_value int; set local_value = input_value + 1; return local_value; end")
	result := <-executor.ExecuteQuery(nil, "select add_one(7)", "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, selectResult.Records, 1)
	require.Equal(t, int64(8), selectResult.Records[0].GetValues()[0].Int())
	mustExecSQL(t, executor, "app", "create function greet(name varchar) returns varchar begin return concat('hello ', name); end")
	result = <-executor.ExecuteQuery(nil, "select greet('world')", "app")
	require.NoError(t, result.Err)
	selectResult, ok = result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, "hello world", selectResult.Records[0].GetValues()[0].String())
}

func TestStoredFunctionCanBeUsedInWherePredicate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table numbers (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into numbers values (1), (2), (3)")
	mustExecSQL(t, executor, "app", "create function plus_one(input_value int) returns int begin return input_value + 1; end")
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySQL(t, executor, "app", "select id from numbers where plus_one(id) = 3"))
	require.Equal(t, [][]interface{}{{"2", "4"}}, mustQuerySQL(t, executor, "app", "select id, plus_one(id) + 1 as total from numbers where plus_one(id) = 3"))
}

func TestStoredFunctionRowExpressionsEnforceExecutePrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table numbers (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into numbers values (1)")
	mustExecSQL(t, executor, "app", "create function plus_one(input_value int) returns int begin return input_value + 1; end")
	mustExecSQL(t, executor, "", "create user 'reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "reader")
	session.SetParamByName("host", "localhost")
	result := <-executor.ExecuteQuery(session, "select plus_one(id) from numbers", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "lacks EXECUTE privilege")

	mustExecSQL(t, executor, "", "grant execute on app.plus_one to 'reader'@'localhost'")
	result = <-executor.ExecuteQuery(session, "select plus_one(id) + 1 from numbers where plus_one(id) = 2", "app")
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, selectResult.Records, 1)
	require.Equal(t, int64(3), selectResult.Records[0].GetValues()[0].Int())
}

func TestStoredFunctionExecutePrivilegeInheritsActiveRole(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table numbers (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into numbers values (1)")
	mustExecSQL(t, executor, "app", "create function plus_one(input_value int) returns int begin return input_value + 1; end")
	mustExecSQL(t, executor, "", "create role 'routine_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant execute on app.plus_one to 'routine_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'routine_reader'@'localhost' to 'reader'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "reader")
	session.SetParamByName("host", "localhost")
	require.NoError(t, (<-executor.ExecuteQuery(session, "set role none", "app")).Err)
	denied := <-executor.ExecuteQuery(session, "select plus_one(id) from numbers", "app")
	require.Error(t, denied.Err)

	require.NoError(t, (<-executor.ExecuteQuery(session, "set role 'routine_reader'@'localhost'", "app")).Err)
	allowed := <-executor.ExecuteQuery(session, "select plus_one(id) from numbers", "app")
	require.NoError(t, allowed.Err)
	selectResult, ok := allowed.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, int64(2), selectResult.Records[0].GetValues()[0].Int())

	require.NoError(t, (<-executor.ExecuteQuery(session, "set default role 'routine_reader'@'localhost' to 'reader'@'localhost'", "app")).Err)
	require.NoError(t, (<-executor.ExecuteQuery(session, "set role default", "app")).Err)
	defaultAllowed := <-executor.ExecuteQuery(session, "select plus_one(id) from numbers", "app")
	require.NoError(t, defaultAllowed.Err)
}

func TestStoredFunctionDerivedTableExpressionsEnforceExecutePrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table numbers (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into numbers values (1)")
	mustExecSQL(t, executor, "app", "create function plus_one(input_value int) returns int begin return input_value + 1; end")
	mustExecSQL(t, executor, "", "create user 'reader'@'localhost' identified by 'secret'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "reader")
	session.SetParamByName("host", "localhost")
	denied := <-executor.ExecuteQuery(session, "select plus_one(d.id) from (select id from numbers) d", "app")
	require.Error(t, denied.Err)
	mustExecSQL(t, executor, "", "grant execute on app.plus_one to 'reader'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "select plus_one(d.id) from (select id from numbers) d", "app")
	require.NoError(t, allowed.Err)
	selectResult, ok := allowed.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, int64(2), selectResult.Records[0].GetValues()[0].Int())
}

func TestSQLEventExecutesDoStatementWhenSchedulerIsEnabled(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table event_log (id int primary key, note varchar(20))")
	scheduler := NewEventScheduler(true)
	executor.SetEventScheduler(scheduler)
	require.NoError(t, executor.StartEventScheduler(context.Background()))
	defer executor.StopEventScheduler()

	result := <-executor.ExecuteQuery(nil, "create event write_once on schedule at current_timestamp do insert into event_log values (1, 'fired')", "app")
	require.NoError(t, result.Err)
	require.Eventually(t, func() bool {
		rows := mustQuerySQL(t, executor, "app", "select note from event_log")
		return len(rows) == 1 && rows[0][0] == "fired"
	}, 2*time.Second, 20*time.Millisecond)

	result = <-executor.ExecuteQuery(nil, "drop event write_once", "app")
	require.NoError(t, result.Err)
}

func TestSQLEventProgramSummaryTracksExecutionResults(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table event_log (id int primary key, note varchar(20))")
	scheduler := NewEventScheduler(true)
	executor.SetEventScheduler(scheduler)
	require.NoError(t, executor.StartEventScheduler(context.Background()))
	defer executor.StopEventScheduler()

	result := <-executor.ExecuteQuery(nil, "create event accounting_event on schedule at current_timestamp on completion preserve do insert into event_log values (1, 'fired')", "app")
	require.NoError(t, result.Err)
	require.Eventually(t, func() bool {
		rows := mustQuerySQL(t, executor, "app", "select note from event_log")
		return len(rows) == 1 && rows[0][0] == "fired"
	}, 2*time.Second, 20*time.Millisecond)

	program := mustQuerySQL(t, executor, "", "select object_type, object_schema, object_name, sum_rows_affected from performance_schema.events_statements_summary_by_program where object_type='EVENT' and object_schema='app' and object_name='accounting_event'")
	require.Len(t, program, 1)
	require.NotEqual(t, "0", program[0][3])
}

func TestSQLEventAlterDisableAndEnableUpdatesSchedulerLifecycle(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table event_log (id int primary key, note varchar(20))")
	mustExecSQL(t, executor, "app", "insert into event_log values (99, 'initial')")
	scheduler := NewEventScheduler(true)
	executor.SetEventScheduler(scheduler)
	require.NoError(t, executor.StartEventScheduler(context.Background()))
	defer executor.StopEventScheduler()

	result := <-executor.ExecuteQuery(nil, "create event lifecycle_event on schedule at '2099-01-01 00:00:00' do insert into event_log values (1, 'fired')", "app")
	require.NoError(t, result.Err)
	result = <-executor.ExecuteQuery(nil, "alter event lifecycle_event disable", "app")
	require.NoError(t, result.Err)
	time.Sleep(100 * time.Millisecond)
	require.Len(t, mustQuerySQL(t, executor, "app", "select id from event_log"), 1)

	result = <-executor.ExecuteQuery(nil, "alter event lifecycle_event on schedule at current_timestamp enable do insert into event_log values (1, 'fired')", "app")
	require.NoError(t, result.Err)
	require.Eventually(t, func() bool {
		rows := mustQuerySQL(t, executor, "app", "select note from event_log")
		return len(rows) == 2 && rows[1][0] == "fired"
	}, 2*time.Second, 20*time.Millisecond)
}

func TestStoredObjectCreateIfNotExistsIsIdempotentAndEventRenameMovesRegistration(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create procedure stable() begin select 1; end")
	mustExecSQL(t, executor, "app", "create procedure if not exists stable() begin select 2; end")
	raw, err := os.ReadFile(filepath.Join(executor.GetDataDir(), "app", "stable.routine.json"))
	require.NoError(t, err)
	var routine persistedStoredObject
	require.NoError(t, json.Unmarshal(raw, &routine))
	require.Contains(t, strings.ToLower(routine.Definition), "select 1")

	mustExecSQL(t, executor, "app", "create table event_log (id int primary key, note varchar(20))")
	scheduler := NewEventScheduler(true)
	executor.SetEventScheduler(scheduler)
	require.NoError(t, executor.StartEventScheduler(context.Background()))
	defer executor.StopEventScheduler()
	mustExecSQL(t, executor, "app", "create event old_name on schedule at '2099-01-01 00:00:00' do insert into event_log values (1, 'renamed')")
	mustExecSQL(t, executor, "app", "alter event old_name rename to new_name")
	_, err = os.Stat(filepath.Join(executor.GetDataDir(), "app", "old_name.event.json"))
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(executor.GetDataDir(), "app", "new_name.event.json"))
	require.NoError(t, err)
	mustExecSQL(t, executor, "app", "alter event new_name on schedule at current_timestamp enable do insert into event_log values (1, 'renamed')")
	require.Eventually(t, func() bool {
		rows := mustQuerySQL(t, executor, "app", "select note from event_log")
		return len(rows) == 1 && rows[0][0] == "renamed"
	}, 2*time.Second, 20*time.Millisecond)
}

func TestSQLEventOnCompletionControlsOneShotMetadataRetention(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table event_log (id int primary key, note varchar(20))")
	scheduler := NewEventScheduler(true)
	executor.SetEventScheduler(scheduler)
	require.NoError(t, executor.StartEventScheduler(context.Background()))
	defer executor.StopEventScheduler()

	mustExecSQL(t, executor, "app", "create event transient_event on schedule at current_timestamp do insert into event_log values (1, 'transient')")
	require.Eventually(t, func() bool {
		rows := mustQuerySQL(t, executor, "app", "select note from event_log")
		return len(rows) == 1 && rows[0][0] == "transient"
	}, 2*time.Second, 20*time.Millisecond)
	_, err := os.Stat(filepath.Join(executor.GetDataDir(), "app", "transient_event.event.json"))
	require.Error(t, err)

	mustExecSQL(t, executor, "app", "create event retained_event on schedule at current_timestamp on completion preserve do insert into event_log values (2, 'retained')")
	require.Eventually(t, func() bool {
		rows := mustQuerySQL(t, executor, "app", "select note from event_log order by id")
		return len(rows) == 2 && rows[1][0] == "retained"
	}, 2*time.Second, 20*time.Millisecond)
	_, err = os.Stat(filepath.Join(executor.GetDataDir(), "app", "retained_event.event.json"))
	require.NoError(t, err)
}
