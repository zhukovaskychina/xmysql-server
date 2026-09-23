package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestReplicationAdminCommandsDelegateToRuntime(t *testing.T) {
	executor := &XMySQLExecutor{}
	var started, stopped int
	executor.SetReplicationControl(func() error { started++; return nil }, func() error { stopped++; return nil })
	ctx := &ExecutionContext{Context: context.Background()}

	handled, err := executor.executeAdminCompatibility(ctx, nil, "start replica")
	require.True(t, handled)
	require.NoError(t, err)
	handled, err = executor.executeAdminCompatibility(ctx, nil, "START SLAVE")
	require.True(t, handled)
	require.NoError(t, err)
	handled, err = executor.executeAdminCompatibility(ctx, nil, "stop replica")
	require.True(t, handled)
	require.NoError(t, err)
	handled, err = executor.executeAdminCompatibility(ctx, nil, "STOP SLAVE")
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, 2, started)
	require.Equal(t, 2, stopped)
}

func TestKillConnectionDelegatesToSessionControl(t *testing.T) {
	executor := &XMySQLExecutor{}
	var targetID uint32
	var targetSession server.MySQLServerSession
	executor.SetSessionKillControl(func(id uint32, session server.MySQLServerSession) error {
		targetID = id
		targetSession = session
		return nil
	})
	ctx := &ExecutionContext{Context: context.Background()}

	handled, err := executor.executeAdminCompatibility(ctx, nil, "KILL CONNECTION 42")
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, uint32(42), targetID)
	require.Nil(t, targetSession)

	var queryTargetID uint32
	executor.SetSessionQueryKillControl(func(id uint32, session server.MySQLServerSession) error {
		queryTargetID = id
		return nil
	})
	handled, err = executor.executeAdminCompatibility(ctx, nil, "KILL QUERY 42")
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, uint32(42), queryTargetID)
}

func TestCancelActiveQueryCancelsRegisteredContext(t *testing.T) {
	executor := &XMySQLExecutor{}
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(77)
	queryContext, cleanup := executor.beginActiveQuery(session)
	defer cleanup()

	if err := executor.CancelActiveQuery(77); err != nil {
		t.Fatalf("CancelActiveQuery failed: %v", err)
	}
	select {
	case <-queryContext.Done():
		require.ErrorIs(t, queryContext.Err(), context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("registered query context was not canceled")
	}
}

func TestReplicationAdminCommandsRejectUnsupportedThreadOptions(t *testing.T) {
	executor := &XMySQLExecutor{}
	executor.SetReplicationControl(func() error { return nil }, func() error { return nil })
	ctx := &ExecutionContext{Context: context.Background()}
	handled, err := executor.executeAdminCompatibility(ctx, nil, "start replica sql_thread")
	require.True(t, handled)
	require.Error(t, err)
	require.Contains(t, err.Error(), "thread options")
}

func TestChangeReplicationSourceParsesLegacyAndCurrentOptions(t *testing.T) {
	executor := &XMySQLExecutor{}
	var sourceURL string
	executor.SetReplicationSourceControl(func(value string) error {
		sourceURL = value
		return nil
	})
	ctx := &ExecutionContext{Context: context.Background()}

	handled, err := executor.executeAdminCompatibility(ctx, nil, "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=3307")
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:3307", sourceURL)

	handled, err = executor.executeAdminCompatibility(ctx, nil, "CHANGE MASTER TO MASTER_HOST='https://source.example', MASTER_PORT=443")
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, "https://source.example:443", sourceURL)
}

func TestChangeReplicationSourceRejectsCredentialsAndMissingHost(t *testing.T) {
	executor := &XMySQLExecutor{}
	executor.SetReplicationSourceControl(func(value string) error { return nil })
	ctx := &ExecutionContext{Context: context.Background()}
	for _, query := range []string{
		"change replication source to source_password='secret'",
		"change replication source to source_port=3306",
	} {
		handled, err := executor.executeAdminCompatibility(ctx, nil, query)
		require.True(t, handled)
		require.Error(t, err, query)
	}
}

func TestResetReplicationAdminCommandsDelegateToRuntime(t *testing.T) {
	executor := &XMySQLExecutor{}
	resetCount := 0
	executor.SetReplicationResetControl(func() error {
		resetCount++
		return nil
	})
	ctx := &ExecutionContext{Context: context.Background()}
	for _, query := range []string{"reset replica", "RESET SLAVE"} {
		handled, err := executor.executeAdminCompatibility(ctx, nil, query)
		require.True(t, handled)
		require.NoError(t, err)
	}
	require.Equal(t, 2, resetCount)
}

func TestResetReplicationAdminAllDelegatesToRuntime(t *testing.T) {
	executor := &XMySQLExecutor{}
	resetAllCount := 0
	executor.SetReplicationResetAllControl(func() error {
		resetAllCount++
		return nil
	})
	handled, err := executor.executeAdminCompatibility(&ExecutionContext{}, nil, "reset replica all")
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, 1, resetAllCount)
}

func TestSourceBinlogAdminCommandsDelegateToRuntime(t *testing.T) {
	executor := &XMySQLExecutor{}
	flushCount, resetCount := 0, 0
	executor.SetReplicationSourceAdminControl(func() error {
		flushCount++
		return nil
	}, func() error {
		resetCount++
		return nil
	})
	ctx := &ExecutionContext{Context: context.Background()}
	for _, query := range []string{"flush binary logs", "FLUSH BINARY LOGS;"} {
		handled, err := executor.executeAdminCompatibility(ctx, nil, query)
		require.True(t, handled)
		require.NoError(t, err)
	}
	handled, err := executor.executeAdminCompatibility(ctx, nil, "reset master")
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, 2, flushCount)
	require.Equal(t, 1, resetCount)
}

func TestLockTablesEnforcesSessionWriteModeAndUnlock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "insert into users (id) values (0)")
	mustExecSessionSQL(t, executor, session, "app", "lock tables users read")

	result := <-executor.ExecuteQuery(session, "insert into users (id) values (1)", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "READ lock")
	require.NoError(t, (<-executor.ExecuteQuery(session, "select * from users", "app")).Err)

	mustExecSessionSQL(t, executor, session, "app", "unlock tables")
	mustExecSessionSQL(t, executor, session, "app", "insert into users (id) values (1)")
}

func TestLockTablesSupportsMultipleTablesAndResetReleasesCoordinatorLocks(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	other := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "create table orders (id int primary key)")

	mustExecSessionSQL(t, executor, owner, "app", "lock tables users write, orders read local")
	locks, ok := owner.GetParamByName("locked_tables").(map[string]string)
	require.True(t, ok)
	require.Equal(t, map[string]string{"users": "write", "orders": "read"}, locks)
	lease := owner.GetParamByName("__table_lock_lease").(*sessionTableLockLease)
	directLock := lease.entries[1].lock
	acquiredDirect := make(chan struct{})
	go func() {
		directLock.RLock()
		close(acquiredDirect)
		directLock.RUnlock()
	}()
	select {
	case <-acquiredDirect:
		t.Fatal("direct reader acquired a table WRITE lock")
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, (<-executor.ExecuteQuery(owner, "select * from users", "app")).Err)
	result := <-executor.ExecuteQuery(owner, "insert into orders (id) values (1)", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "READ lock")

	blocked := executor.ExecuteQuery(other, "select * from users", "app")
	select {
	case result := <-blocked:
		t.Fatalf("reader acquired a table WRITE lock: %#v", result)
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, executor.ResetSession(owner))
	select {
	case result := <-blocked:
		require.NoError(t, result.Err)
	case <-time.After(time.Second):
		t.Fatal("reader did not acquire the table after session reset released LOCK TABLES")
	}
}

func TestLockWaitRespondsToKillQueryCancellation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	waiter := newTestMySQLSession()
	waiter.SessionContext().SetConnectionID(9042)
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables users write")

	blocked := executor.ExecuteQuery(waiter, "select * from users", "app")
	select {
	case result := <-blocked:
		t.Fatalf("reader was not waiting on the table lock: %#v", result)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, executor.QueryExecutor.CancelActiveQuery(9042))
	select {
	case result := <-blocked:
		require.ErrorIs(t, result.Err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("lock wait did not stop after KILL QUERY cancellation")
	}
	require.NoError(t, executor.ResetSession(owner))
}

func TestLockWaitReturnsMySQLTimeoutError(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	waiter := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables users write")
	mustExecSessionSQL(t, executor, waiter, "app", "set session innodb_lock_wait_timeout = 1")

	started := time.Now()
	result := <-executor.ExecuteQuery(waiter, "select * from users", "app")
	require.Error(t, result.Err)
	require.Contains(t, strings.ToLower(result.Err.Error()), "lock wait timeout")
	require.GreaterOrEqual(t, time.Since(started), 900*time.Millisecond)
	require.NoError(t, executor.ResetSession(owner))
}

func TestJoinWaitsForEveryReferencedTableLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	waiter := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "create table orders (id int primary key, user_id int)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into users values (1)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into orders values (1, 1)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables users write")

	blocked := executor.ExecuteQuery(waiter, "select users.id from orders join users on users.id = orders.user_id", "app")
	select {
	case result := <-blocked:
		t.Fatalf("join acquired an uncoordinated table lock: %#v", result)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, executor.ResetSession(owner))
	select {
	case result := <-blocked:
		require.NoError(t, result.Err)
	case <-time.After(time.Second):
		t.Fatal("join did not continue after the referenced table lock was released")
	}
}

func TestUnsupportedFileCommandsReturnExplicitError(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	for _, query := range []string{
		"load data infile '/tmp/users.csv' into table users",
		"select * from users into outfile '/tmp/users.csv'",
	} {
		result := <-executor.ExecuteQuery(nil, query, "app")
		require.Error(t, result.Err, query)
		require.Contains(t, result.Err.Error(), "file import/export is not supported", query)
	}
}

func TestAdminCompatibilityTableMaintenanceUsesRealTableMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1)")

	for _, statement := range []string{"check table users", "analyze table users", "optimize table users"} {
		result := <-executor.ExecuteQuery(nil, statement, "app")
		require.NoError(t, result.Err, statement)
		data, ok := result.Data.(map[string]interface{})
		require.True(t, ok, "%s returned %T", statement, result.Data)
		rows, ok := data["rows"].([][]interface{})
		require.True(t, ok)
		require.Len(t, rows, 1)
		require.Equal(t, "status", rows[0][2])
	}
}

func TestAdminCompatibilityTableMaintenanceSupportsMultipleTables(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "create table orders (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1)")
	mustExecSQL(t, executor, "app", "insert into orders (id) values (2)")

	for _, statement := range []string{"check table users, orders", "analyze table users, orders", "optimize table users, orders"} {
		result := <-executor.ExecuteQuery(nil, statement, "app")
		require.NoError(t, result.Err, statement)
		data, ok := result.Data.(map[string]interface{})
		require.True(t, ok, "%s returned %T", statement, result.Data)
		rows, ok := data["rows"].([][]interface{})
		require.True(t, ok)
		require.Len(t, rows, 2, statement)
		require.Equal(t, "users", rows[0][0])
		require.Equal(t, "orders", rows[1][0])
	}
}

func TestAdminCompatibilityTableMaintenanceAcceptsCommonModifiers(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1)")

	for _, statement := range []string{
		"check table users for upgrade",
		"analyze local table users",
		"optimize no_write_to_binlog table users",
	} {
		result := <-executor.ExecuteQuery(nil, statement, "app")
		require.NoError(t, result.Err, statement)
		data, ok := result.Data.(map[string]interface{})
		require.True(t, ok, "%s returned %T", statement, result.Data)
		rows, ok := data["rows"].([][]interface{})
		require.True(t, ok)
		require.Len(t, rows, 1, statement)
		require.Equal(t, "users", rows[0][0])
	}
}

func TestLockTablesImplicitlyCommitsActiveTransaction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	otherSession := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table lock_commit (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "start transaction")
	mustExecSessionSQL(t, executor, session, "app", "insert into lock_commit values (1)")
	require.True(t, sessionBoolParam(session, "in_transaction"))

	mustExecSessionSQL(t, executor, session, "app", "lock tables lock_commit read")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.Len(t, mustQuerySessionSQL(t, executor, otherSession, "app", "select id from lock_commit"), 1)
	mustExecSessionSQL(t, executor, session, "app", "unlock tables")
}

func TestFlushAndTableMaintenanceImplicitlyCommitActiveTransaction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	otherSession := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table admin_commit (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "start transaction")
	mustExecSessionSQL(t, executor, session, "app", "insert into admin_commit values (1)")
	mustExecSessionSQL(t, executor, session, "app", "flush tables admin_commit")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.Len(t, mustQuerySessionSQL(t, executor, otherSession, "app", "select id from admin_commit"), 1)

	mustExecSessionSQL(t, executor, session, "app", "start transaction")
	mustExecSessionSQL(t, executor, session, "app", "insert into admin_commit values (2)")
	mustExecSessionSQL(t, executor, session, "app", "analyze table admin_commit")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.Len(t, mustQuerySessionSQL(t, executor, otherSession, "app", "select id from admin_commit"), 2)
}

func TestAnalyzeTableWaitsForExplicitTableMetadataLock(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	maintainer := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table analyze_lock (id int primary key, value int)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into analyze_lock values (1, 10)")
	mustExecSessionSQL(t, executor, owner, "app", "lock tables analyze_lock write")

	analyzeDone := make(chan *Result, 1)
	go func() {
		analyzeDone <- <-executor.ExecuteQuery(maintainer, "analyze table analyze_lock", "app")
	}()

	select {
	case result := <-analyzeDone:
		t.Fatalf("ANALYZE TABLE completed while the table metadata lock was held: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "unlock tables")
	select {
	case result := <-analyzeDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("ANALYZE TABLE did not complete after UNLOCK TABLES released the metadata lock")
	}
}

func TestAdminCompatibilityFlushTablesFlushesStorage(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1)")

	for _, statement := range []string{"flush tables", "flush tables users"} {
		result := <-executor.ExecuteQuery(nil, statement, "app")
		require.NoError(t, result.Err, statement)
	}
}

func TestAdminCompatibilityFlushTablesAcceptsCommonModifiers(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")

	for _, statement := range []string{
		"flush no_write_to_binlog tables users",
		"flush local tables users",
	} {
		result := <-executor.ExecuteQuery(nil, statement, "app")
		require.NoError(t, result.Err, statement)
	}
}

func TestFlushTablesWithReadLockBlocksWritesButAllowsReads(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	writer := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into users (id) values (1)")

	locked := <-executor.ExecuteQuery(owner, "flush tables users with read lock", "app")
	require.NoError(t, locked.Err)
	require.True(t, executor.QueryExecutor.globalReadLockHeld)
	require.True(t, globalReadLockWriteStatement("insert into users (id) values (2)"))

	read := <-executor.ExecuteQuery(writer, "select * from users", "app")
	require.NoError(t, read.Err)

	blocked := executor.ExecuteQuery(writer, "insert into users (id) values (2)", "app")
	select {
	case result := <-blocked:
		t.Fatalf("write completed while FLUSH TABLES WITH READ LOCK was held: %#v", result)
	case <-time.After(100 * time.Millisecond):
	}

	unlocked := <-executor.ExecuteQuery(owner, "unlock tables", "app")
	require.NoError(t, unlocked.Err)
	select {
	case result := <-blocked:
		require.NoError(t, result.Err)
	case <-time.After(time.Second):
		t.Fatal("blocked write did not continue after UNLOCK TABLES")
	}
}

func TestFlushTablesWithReadLockReleasesOnSessionReset(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	writer := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into users (id) values (1)")

	locked := <-executor.ExecuteQuery(owner, "flush tables with read lock", "app")
	require.NoError(t, locked.Err)

	blocked := executor.ExecuteQuery(writer, "insert into users (id) values (2)", "app")
	select {
	case result := <-blocked:
		t.Fatalf("write was not blocked by the global read lock: %#v", result)
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, executor.ResetSession(owner))
	select {
	case result := <-blocked:
		require.NoError(t, result.Err)
	case <-time.After(time.Second):
		t.Fatal("blocked write did not continue after the lock owner session was reset")
	}
	require.False(t, executor.QueryExecutor.globalReadLockHeld)
}

func TestFlushTablesWithReadLockWaitRespondsToKillQueryCancellation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	writer := newTestMySQLSession()
	writer.SessionContext().SetConnectionID(9043)
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into users (id) values (1)")

	locked := <-executor.ExecuteQuery(owner, "flush tables with read lock", "app")
	require.NoError(t, locked.Err)

	blocked := executor.ExecuteQuery(writer, "insert into users (id) values (2)", "app")
	select {
	case result := <-blocked:
		t.Fatalf("write was not blocked by the global read lock: %#v", result)
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, executor.QueryExecutor.CancelActiveQuery(9043))
	select {
	case result := <-blocked:
		require.ErrorIs(t, result.Err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("global read-lock wait did not stop after KILL QUERY cancellation")
	}
	require.NoError(t, executor.ResetSession(owner))
}

func TestAnalyzeTablePersistsOptimizerStatistics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice'), (2, 'bob'), (3, 'alice')")

	result := <-executor.ExecuteQuery(nil, "analyze table users", "app")
	require.NoError(t, result.Err)

	queryExecutor := executor.QueryExecutor
	stats, err := queryExecutor.infosSchemaManager.GetTableStats(context.Background(), "app", "users")
	require.NoError(t, err)
	require.Equal(t, uint64(3), stats.RowCount)
	require.Greater(t, stats.DataSize, uint64(0))
	require.Greater(t, stats.IndexSize, uint64(0))
	require.Greater(t, stats.AvgRowSize, uint32(0))
	require.Equal(t, uint64(2), stats.ColumnStats["name"].DistinctCount)
	require.Equal(t, uint64(0), stats.ColumnStats["name"].NullCount)
	require.Equal(t, uint64(3), stats.IndexStats["PRIMARY"].DistinctCount)
	require.Equal(t, int64(1), stats.ColumnStats["id"].MinValue)
	require.Equal(t, int64(3), stats.ColumnStats["id"].MaxValue)

	explain := <-executor.ExecuteQuery(nil, "explain select * from users", "app")
	require.NoError(t, explain.Err)
	explainData, ok := explain.Data.(map[string]interface{})
	require.True(t, ok)
	explainRows, ok := explainData["rows"].([][]interface{})
	require.True(t, ok)
	require.Equal(t, int64(3), explainRows[0][9])

	statistics := <-executor.ExecuteQuery(nil, "select index_name, cardinality from information_schema.statistics where table_schema = 'app' and table_name = 'users' and index_name = 'PRIMARY'", "app")
	require.NoError(t, statistics.Err)
	statisticsResult, ok := statistics.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, statisticsResult.Records, 1)
	require.Equal(t, int64(3), statisticsResult.Records[0].GetValues()[1].Int())
}

func TestInformationSchemaTablesExposesAnalyzedLogicalSizes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(20), index idx_name (name))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice'), (2, 'bob'), (3, 'carol')")
	mustExecSQL(t, executor, "app", "analyze table users")

	result := <-executor.ExecuteQuery(nil, "select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_schema = 'app' and table_name = 'users'", "app")
	require.NoError(t, result.Err)
	tables, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, tables.Records, 1)
	values := tables.Records[0].GetValues()
	require.Len(t, values, 4)
	require.Greater(t, values[0].Int(), int64(0))
	require.Greater(t, values[1].Int(), int64(0))
	require.Greater(t, values[2].Int(), int64(0))
	require.Greater(t, values[3].Int(), int64(0))
}

func TestAnalyzeTableUsesTypedNumericMinMax(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table numeric_stats (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into numeric_stats (id) values (2), (10)")
	mustExecSQL(t, executor, "app", "analyze table numeric_stats")

	stats, err := executor.QueryExecutor.infosSchemaManager.GetTableStats(context.Background(), "app", "numeric_stats")
	require.NoError(t, err)
	require.Equal(t, int64(2), stats.ColumnStats["id"].MinValue)
	require.Equal(t, int64(10), stats.ColumnStats["id"].MaxValue)
}

func TestOptimizeTableRefreshesOptimizerStatistics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice')")
	mustExecSQL(t, executor, "app", "analyze table users")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (2, 'bob')")

	mustExecSQL(t, executor, "app", "optimize table users")
	stats, err := executor.QueryExecutor.infosSchemaManager.GetTableStats(context.Background(), "app", "users")
	require.NoError(t, err)
	require.Equal(t, uint64(2), stats.RowCount)
	require.Equal(t, uint64(2), stats.ColumnStats["name"].DistinctCount)
}

func TestLegacyMaintenancePathRefreshesOptimizeStatistics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(20))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice')")
	mustExecSQL(t, executor, "app", "analyze table users")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (2, 'bob')")

	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), DatabaseName: "app", Results: results}
	require.True(t, executor.QueryExecutor.executeAdminReadQuery(ctx, "optimize table users", "app"))
	result := <-results
	require.NoError(t, result.Err)
	stats, err := executor.QueryExecutor.infosSchemaManager.GetTableStats(context.Background(), "app", "users")
	require.NoError(t, err)
	require.Equal(t, uint64(2), stats.RowCount)
}

func TestAnalyzeStatisticsReloadIntoOptimizerAfterEngineRestart(t *testing.T) {
	dataDir := t.TempDir()
	first := NewXMySQLEngine(&conf.Cfg{DataDir: dataDir, InnodbDataDir: dataDir, InnodbBufferPoolSize: 16 * 1024 * 1024, InnodbPageSize: 16384})
	mustExecSQL(t, first, "", "create database app")
	mustExecSQL(t, first, "app", "create table users (id int primary key, name varchar(20))")
	mustExecSQL(t, first, "app", "insert into users (id, name) values (1, 'alice'), (2, 'bob'), (3, 'alice')")
	mustExecSQL(t, first, "app", "analyze table users")
	require.NoError(t, first.Close())

	second := NewXMySQLEngine(&conf.Cfg{DataDir: dataDir, InnodbDataDir: dataDir, InnodbBufferPoolSize: 16 * 1024 * 1024, InnodbPageSize: 16384})
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	stats, err := second.QueryExecutor.infosSchemaManager.GetTableStats(context.Background(), "app", "users")
	require.NoError(t, err)
	require.Equal(t, uint64(3), stats.RowCount)
	require.Equal(t, uint64(2), stats.ColumnStats["name"].DistinctCount)
	require.Equal(t, uint64(3), stats.IndexStats["PRIMARY"].DistinctCount)
}

func TestAnalyzeTableWritesVersionedStatisticsSidecar(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users (id) values (1), (2)")
	mustExecSQL(t, executor, "app", "analyze table users")

	raw, err := os.ReadFile(filepath.Join(dataDir, "app", "users.stats.json"))
	require.NoError(t, err)
	var sidecar map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &sidecar))
	require.Equal(t, float64(1), sidecar["format_version"])
	require.Equal(t, "app", sidecar["schema_name"])
	require.Equal(t, "users", sidecar["table_name"])
	require.NotEmpty(t, sidecar["table_fingerprint"])
	require.NotEmpty(t, sidecar["checksum"])
	stats, ok := sidecar["stats"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, float64(2), stats["RowCount"])
}

func TestCorruptStatisticsSidecarFallsBackToEmbeddedMetadata(t *testing.T) {
	dataDir := t.TempDir()
	first := NewXMySQLEngine(&conf.Cfg{DataDir: dataDir, InnodbDataDir: dataDir, InnodbBufferPoolSize: 16 * 1024 * 1024, InnodbPageSize: 16384})
	mustExecSQL(t, first, "", "create database app")
	mustExecSQL(t, first, "app", "create table users (id int primary key)")
	mustExecSQL(t, first, "app", "insert into users (id) values (1), (2)")
	mustExecSQL(t, first, "app", "analyze table users")
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "app", "users.stats.json"), []byte("broken"), 0644))
	require.NoError(t, first.Close())

	second := NewXMySQLEngine(&conf.Cfg{DataDir: dataDir, InnodbDataDir: dataDir, InnodbBufferPoolSize: 16 * 1024 * 1024, InnodbPageSize: 16384})
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	stats, err := second.QueryExecutor.infosSchemaManager.GetTableStats(context.Background(), "app", "users")
	require.NoError(t, err)
	require.Equal(t, uint64(2), stats.RowCount)
}

func TestDMLInvalidatesPersistedIndexCardinality(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(20), index idx_name (name))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice'), (2, 'bob')")
	mustExecSQL(t, executor, "app", "analyze table users")
	require.FileExists(t, filepath.Join(executor.GetDataDir(), "app", "users.stats.json"))
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (3, 'carol')")
	require.NoFileExists(t, filepath.Join(executor.GetDataDir(), "app", "users.stats.json"))

	show := <-executor.ExecuteQuery(nil, "show index from users", "app")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	found := false
	for _, row := range rows {
		if len(row) > 6 && row[2] == "idx_name" {
			require.Equal(t, int64(3), row[6])
			found = true
		}
	}
	require.True(t, found, "SHOW INDEX did not return idx_name: %#v", rows)
	statisticsResult := <-executor.ExecuteQuery(nil, "select cardinality from information_schema.statistics where table_schema = 'app' and table_name = 'users' and index_name = 'idx_name'", "app")
	require.NoError(t, statisticsResult.Err)
	statistics, ok := statisticsResult.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{string(metadata.TypeBigInt)}, statistics.ColumnTypes)
	require.Len(t, statistics.Records, 1)
	require.Equal(t, int64(3), statistics.Records[0].GetValues()[0].Int())
}

func TestInformationSchemaStatisticsExposesIndexPages(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(20), index idx_name (name))")
	mustExecSQL(t, executor, "app", "insert into users (id, name) values (1, 'alice'), (2, 'bob')")

	result := <-executor.ExecuteQuery(nil, "select index_name, pages from information_schema.statistics where table_schema = 'app' and table_name = 'users' order by index_name", "app")
	require.NoError(t, result.Err)
	statistics, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, statistics.Records, 2)
	for _, record := range statistics.Records {
		values := record.GetValues()
		require.Len(t, values, 2)
		require.Greater(t, values[1].Int(), int64(0), "index %s should expose a non-zero page estimate", values[0].ToString())
	}
}
