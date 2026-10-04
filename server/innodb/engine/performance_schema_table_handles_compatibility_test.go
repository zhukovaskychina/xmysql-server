package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestPerformanceSchemaTableHandlesExposeImplicitStatementLease(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database table_handle_runtime")
	mustExecSQL(t, executor, "table_handle_runtime", "create table orders (id int primary key)")

	owner := newTestMySQLSession()
	owner.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	owner.SetParamByName("user", "owner")
	owner.SetParamByName("host", "localhost")
	owner.SetParamByName("connection_id", int64(601))
	owner.SessionContext().SetConnectionID(601)
	observer := newTestMySQLSession()
	observer.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	observer.SetParamByName("user", "observer")
	observer.SetParamByName("host", "localhost")
	observer.SetParamByName("connection_id", int64(602))
	observer.SessionContext().SetConnectionID(602)
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{owner, observer}
	})

	unlock, err := executor.QueryExecutor.acquireStatementTableLocks(
		&ExecutionContext{Session: owner}, owner, "select * from orders", "table_handle_runtime",
	)
	require.NoError(t, err)

	visible := executor.QueryExecutor.executePerformanceSchemaTableHandlesSelect(
		"select object_schema, object_name, owner_thread_id, internal_lock, external_lock from performance_schema.table_handles",
		observer,
	)
	require.Equal(t, [][]interface{}{{"table_handle_runtime", "orders", "601", "READ", "READ"}}, selectResultRows(visible))

	unlock()
	after := executor.QueryExecutor.executePerformanceSchemaTableHandlesSelect(
		"select object_schema, object_name from performance_schema.table_handles",
		observer,
	)
	require.Empty(t, after.Records)
}

func TestPerformanceSchemaMetadataLocksExposeLockDurationLifecycle(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	owner.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	owner.SetParamByName("user", "owner")
	owner.SetParamByName("host", "localhost")
	owner.SetParamByName("connection_id", int64(603))
	owner.SessionContext().SetConnectionID(603)

	statementUnlock, err := executor.QueryExecutor.acquireStatementTableLocks(
		&ExecutionContext{Session: owner}, owner, "select * from statement_orders", "app",
	)
	require.NoError(t, err)
	statement := executor.QueryExecutor.executePerformanceSchemaMetadataLocksSelect(
		"select object_name, lock_duration, owner_thread_id from performance_schema.metadata_locks where object_name='statement_orders'",
	)
	require.Equal(t, [][]interface{}{{"statement_orders", "STATEMENT", "603"}}, selectResultRows(statement))
	statementUnlock()

	owner.SetParamByName("in_transaction", true)
	transactionUnlock, err := executor.QueryExecutor.acquireStatementTableLocks(
		&ExecutionContext{Session: owner}, owner, "select * from transaction_orders", "app",
	)
	require.NoError(t, err)
	transaction := executor.QueryExecutor.executePerformanceSchemaMetadataLocksSelect(
		"select object_name, lock_duration, owner_thread_id from performance_schema.metadata_locks where object_name='transaction_orders'",
	)
	require.Equal(t, [][]interface{}{{"transaction_orders", "TRANSACTION", "603"}}, selectResultRows(transaction))
	transactionUnlock()
	executor.QueryExecutor.releaseSessionTransactionTableLocks(owner)
	owner.SetParamByName("in_transaction", false)

	require.NoError(t, executor.QueryExecutor.acquireSessionTableLocks(context.Background(), owner, []sessionTableLockRequest{{table: "app.explicit_orders", mode: "read"}}))
	explicit := executor.QueryExecutor.executePerformanceSchemaMetadataLocksSelect(
		"select object_name, lock_duration, owner_thread_id from performance_schema.metadata_locks where object_name='explicit_orders'",
	)
	require.Equal(t, [][]interface{}{{"explicit_orders", "EXPLICIT", "603"}}, selectResultRows(explicit))
	executor.QueryExecutor.releaseSessionTableLocks(owner)
}

func TestPerformanceSchemaMetadataLockLostCountsUnfilteredCapacity(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_metadata_locks", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	owner := newTestMySQLSession()
	owner.SetParamByName("connection_id", int64(604))
	owner.SessionContext().SetConnectionID(604)
	require.NoError(t, executor.QueryExecutor.acquireSessionTableLocks(context.Background(), owner, []sessionTableLockRequest{
		{table: "app.first_metadata", mode: "read"},
		{table: "app.second_metadata", mode: "read"},
	}))
	t.Cleanup(func() { executor.QueryExecutor.releaseSessionTableLocks(owner) })

	filtered := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.metadata_locks where object_name='first_metadata'")
	require.Equal(t, [][]interface{}{{"first_metadata"}}, selectResultRows(filtered))
	lost := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_metadata_lock_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(lost))
}

func TestPerformanceSchemaTableHandleCapacityDoesNotResurrectEvictedRows(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_table_handles", "1")
	require.NoError(t, err)

	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	owner := newTestMySQLSession()
	owner.SetParamByName("connection_id", int64(606))
	owner.SessionContext().SetConnectionID(606)
	require.NoError(t, executor.QueryExecutor.acquireSessionTableLocks(context.Background(), owner, []sessionTableLockRequest{
		{table: "app.first_handle", mode: "read"},
		{table: "app.second_handle", mode: "read"},
	}))
	t.Cleanup(func() { executor.QueryExecutor.releaseSessionTableLocks(owner) })
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{owner}
	})

	retained := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_handles where object_name='first_handle'")
	require.Equal(t, [][]interface{}{{"first_handle"}}, selectResultRows(retained))
	evicted := mustSelectResultSQL(t, executor, "", "select object_name from performance_schema.table_handles where object_name='second_handle'")
	require.Empty(t, selectResultRows(evicted))
	lost := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_table_handles_lost'")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(lost))
}
