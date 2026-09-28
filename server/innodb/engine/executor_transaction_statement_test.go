package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func TestTransactionStatementsReturnOK(t *testing.T) {
	executor := NewXMySQLExecutor(nil, &conf.Cfg{InnodbDataDir: t.TempDir()})
	session := newTestMySQLSession()

	for _, query := range []string{
		"begin",
		"start transaction",
		"commit; ",
		"rollback",
		"savepoint sp1",
		"rollback to savepoint sp1",
		"release savepoint sp1",
	} {
		t.Run(query, func(t *testing.T) {
			results := executor.ExecuteWithQuery(session, query, "")
			got := <-results
			require.NoError(t, got.Err)
			require.Equal(t, common.RESULT_TYPE_QUERY, got.ResultType)
		})
	}
}

func TestUserVariableAssignmentSupportsReplicationHandshakeList(t *testing.T) {
	executor := NewXMySQLExecutor(nil, &conf.Cfg{InnodbDataDir: t.TempDir()})
	session := newTestMySQLSession()
	session.SetParamByName("binlog_checksum", "CRC32")

	result := <-executor.ExecuteWithQuery(session, "SET @master_heartbeat_period = 3000000000, @master_binlog_checksum = @@global.binlog_checksum, @source_binlog_checksum = @@global.binlog_checksum", "")
	require.NoError(t, result.Err)
	require.Equal(t, int64(3000000000), session.GetParamByName("master_heartbeat_period"))
	require.Equal(t, "CRC32", session.GetParamByName("master_binlog_checksum"))
	require.Equal(t, "CRC32", session.GetParamByName("source_binlog_checksum"))
}

func TestExplicitTransactionHoldsMetadataLockUntilCommit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	ddl := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "begin")
	mustExecSessionSQL(t, executor, owner, "app", "select * from users")

	ddlDone := make(chan *Result, 1)
	go func() {
		ddlDone <- <-executor.ExecuteQuery(ddl, "alter table users add column note int", "app")
	}()

	select {
	case result := <-ddlDone:
		t.Fatalf("DDL completed before the transaction boundary: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "commit")
	select {
	case result := <-ddlDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("DDL did not complete after COMMIT released the metadata lock")
	}
}

func TestExplicitTransactionBlocksDropUntilCommit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	dropper := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "begin")
	mustExecSessionSQL(t, executor, owner, "app", "select * from users")
	lease, ok := owner.GetParamByName("__transaction_table_lock_lease").(*sessionTableLockLease)
	require.True(t, ok)
	require.Len(t, lease.entries, 1)

	dropDone := make(chan *Result, 1)
	go func() {
		dropDone <- <-executor.ExecuteQuery(dropper, "drop table users", "app")
	}()

	select {
	case result := <-dropDone:
		t.Fatalf("DROP TABLE completed before the transaction boundary: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "rollback")
	select {
	case result := <-dropDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("DROP TABLE did not complete after ROLLBACK released the metadata lock")
	}
}

func TestAutocommitOnReleasesReadMetadataLeaseWithoutMaterializedTransaction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "set autocommit=0")
	mustExecSessionSQL(t, executor, session, "app", "select * from users")
	require.True(t, sessionHasTransactionTableLocks(session))
	require.False(t, sessionBoolParam(session, "in_transaction"))
	mustExecSessionSQL(t, executor, session, "app", "set autocommit=1")
	require.False(t, sessionHasTransactionTableLocks(session))
	mustExecSessionSQL(t, executor, session, "app", "truncate table users")
}

func TestDDLImplicitlyCommitsTransactionMetadataLease(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "select * from users")
	mustExecSessionSQL(t, executor, session, "app", "alter table users add column note int")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.False(t, sessionHasTransactionTableLocks(session))
}

func TestDatabaseDDLImplicitlyCommitsTransactionMetadataLease(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "select * from users")
	mustExecSessionSQL(t, executor, session, "app", "create database ddl_boundary")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.False(t, sessionHasTransactionTableLocks(session))
	mustExecSessionSQL(t, executor, session, "", "drop database ddl_boundary")
}

func TestExplicitTransactionBlocksDropDatabaseUntilCommit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	dropper := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "begin")
	mustExecSessionSQL(t, executor, owner, "app", "select * from users")

	dropDone := make(chan *Result, 1)
	go func() {
		dropDone <- <-executor.ExecuteQuery(dropper, "drop database app", "")
	}()

	select {
	case result := <-dropDone:
		t.Fatalf("DROP DATABASE completed before the transaction boundary: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "commit")
	select {
	case result := <-dropDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("DROP DATABASE did not complete after COMMIT released the metadata lock")
	}
}

func TestStoredObjectAndViewDDLImplicitlyCommitTransaction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table ddl_objects (id int primary key)")

	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "select * from ddl_objects")
	mustExecSessionSQL(t, executor, session, "app", "create procedure p_implicit_commit() select 1")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.False(t, sessionHasTransactionTableLocks(session))

	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "select * from ddl_objects")
	mustExecSessionSQL(t, executor, session, "app", "create view v_implicit_commit as select id from ddl_objects")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.False(t, sessionHasTransactionTableLocks(session))
}

func TestExplicitTransactionBlocksRenameUntilCommit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	renamer := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table users (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "begin")
	mustExecSessionSQL(t, executor, owner, "app", "select * from users")

	renameDone := make(chan *Result, 1)
	go func() {
		renameDone <- <-executor.ExecuteQuery(renamer, "rename table users to customers", "app")
	}()

	select {
	case result := <-renameDone:
		t.Fatalf("RENAME TABLE completed before the transaction boundary: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "commit")
	select {
	case result := <-renameDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("RENAME TABLE did not complete after COMMIT released the metadata lock")
	}
}

func TestInvalidTransactionStatementsAreNotAccepted(t *testing.T) {
	for _, query := range []string{
		"savepoint ",
		"rollback to savepoint ",
		"rollback to ",
		"release savepoint ",
	} {
		t.Run(query, func(t *testing.T) {
			_, _, ok := normalizedTransactionCommand(query)
			require.False(t, ok)
		})
	}
}

func TestNormalizedTransactionCommandSupportsReadOnlyStart(t *testing.T) {
	cmd, name, ok := normalizedTransactionCommand("begin work")
	require.True(t, ok)
	require.Equal(t, "begin", cmd)
	require.Empty(t, name)

	cmd, name, ok = normalizedTransactionCommand("start transaction read only")
	require.True(t, ok)
	require.Equal(t, "begin_read_only", cmd)
	require.Empty(t, name)

	cmd, name, ok = normalizedTransactionCommand("start transaction read write")
	require.True(t, ok)
	require.Equal(t, "begin_read_write", cmd)
	require.Empty(t, name)

	cmd, name, ok = normalizedTransactionCommand("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
	require.True(t, ok)
	require.Equal(t, "begin_read_only", cmd)
	require.Empty(t, name)

	cmd, name, ok = normalizedTransactionCommand("START TRANSACTION WITH CONSISTENT SNAPSHOT READ WRITE")
	require.True(t, ok)
	require.Equal(t, "begin_read_write", cmd)
	require.Empty(t, name)

	cmd, name, ok = normalizedTransactionCommand("START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT")
	require.True(t, ok)
	require.Equal(t, "begin_read_only", cmd)
	require.Empty(t, name)

	cmd, name, ok = normalizedTransactionCommand("START TRANSACTION READ WRITE, WITH CONSISTENT SNAPSHOT")
	require.True(t, ok)
	require.Equal(t, "begin_read_write", cmd)
	require.Empty(t, name)
}

func TestNormalizedTransactionCommandSupportsCommitAndRollbackChain(t *testing.T) {
	for _, test := range []struct {
		query string
		cmd   string
		name  string
	}{
		{query: "commit and chain", cmd: "commit"},
		{query: "commit and no chain", cmd: "commit"},
		{query: "commit work", cmd: "commit"},
		{query: "commit work and chain", cmd: "commit"},
		{query: "commit work and no chain", cmd: "commit"},
		{query: "commit release", cmd: "commit"},
		{query: "commit work release", cmd: "commit"},
		{query: "commit and chain release", cmd: "commit"},
		{query: "rollback and chain", cmd: "rollback"},
		{query: "rollback and no chain", cmd: "rollback"},
		{query: "rollback work", cmd: "rollback"},
		{query: "rollback work and chain", cmd: "rollback"},
		{query: "rollback work and no chain", cmd: "rollback"},
		{query: "rollback release", cmd: "rollback"},
		{query: "rollback work release", cmd: "rollback"},
		{query: "rollback and no chain release", cmd: "rollback"},
	} {
		cmd, name, ok := normalizedTransactionCommand(test.query)
		require.True(t, ok, test.query)
		require.Equal(t, test.cmd, cmd, test.query)
		if strings.Contains(test.query, " and ") || strings.Contains(test.query, "release") {
			require.NotEmpty(t, name, test.query)
		} else {
			require.Empty(t, name, test.query)
		}
	}

	for _, query := range []string{
		"start transaction read only, read write",
		"start transaction read only, read only",
		"start transaction with consistent snapshot, with consistent snapshot",
		"start transaction read only, unsupported",
		"commit and release and chain",
		"rollback and release and chain",
		"commit and chain and release and no chain",
	} {
		_, _, ok := normalizedTransactionCommand(query)
		require.False(t, ok, query)
	}
}

func TestCommitReleaseMarksSessionForClose(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "", "begin")

	result := <-engine.ExecuteQuery(session, "commit release", "")
	require.NoError(t, result.Err)
	require.True(t, sessionBoolParam(session, "should_close"))

	result = <-engine.ExecuteQuery(session, "rollback work", "")
	require.NoError(t, result.Err)
}

func TestCommitAndRollbackChainStartTheNextTransaction(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "", "create database app")

	mustExecSessionSQL(t, engine, session, "app", "set session transaction isolation level read committed")
	mustExecSessionSQL(t, engine, session, "app", "start transaction read only")
	mustExecSessionSQL(t, engine, session, "app", "commit and chain")

	state, ok := session.GetParamByName("transaction_dml_state").(*sessionTransactionState)
	require.True(t, ok)
	require.True(t, sessionBoolParam(session, "in_transaction"))
	require.Equal(t, "READ COMMITTED", state.IsolationLevel)
	require.Equal(t, "READ ONLY", state.AccessMode)

	// ROLLBACK AND CHAIN keeps the transaction active and carries the
	// isolation/access mode of the transaction it just ended.
	mustExecSessionSQL(t, engine, session, "app", "rollback and chain")
	state, ok = session.GetParamByName("transaction_dml_state").(*sessionTransactionState)
	require.True(t, ok)
	require.True(t, sessionBoolParam(session, "in_transaction"))
	require.Equal(t, "READ COMMITTED", state.IsolationLevel)
	require.Equal(t, "READ ONLY", state.AccessMode)

	mustExecSessionSQL(t, engine, session, "app", "rollback")
	require.False(t, sessionBoolParam(session, "in_transaction"))
}

func TestStartTransactionCommitsPreviousActiveTransaction(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	otherSession := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "", "create database app")
	mustExecSessionSQL(t, engine, session, "app", "create table start_transaction_boundary (id int primary key)")

	mustExecSessionSQL(t, engine, session, "app", "start transaction")
	mustExecSessionSQL(t, engine, session, "app", "insert into start_transaction_boundary values (1)")
	require.True(t, sessionBoolParam(session, "in_transaction"))

	mustExecSessionSQL(t, engine, session, "app", "start transaction")
	require.True(t, sessionBoolParam(session, "in_transaction"))
	require.Len(t, mustQuerySessionSQL(t, engine, otherSession, "app", "select id from start_transaction_boundary"), 1)

	mustExecSessionSQL(t, engine, session, "app", "rollback")
	require.Len(t, mustQuerySessionSQL(t, engine, otherSession, "app", "select id from start_transaction_boundary"), 1)
}

func TestSetAutocommitOnCommitsActiveTransaction(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	otherSession := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "", "create database app")
	mustExecSessionSQL(t, engine, session, "app", "create table autocommit_boundary (id int primary key)")

	mustExecSessionSQL(t, engine, session, "app", "set autocommit = 0")
	mustExecSessionSQL(t, engine, session, "app", "insert into autocommit_boundary values (1)")
	require.True(t, sessionBoolParam(session, "in_transaction"))

	mustExecSessionSQL(t, engine, session, "app", "set autocommit = 1")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.Equal(t, "1", session.GetParamByName("autocommit"))
	require.Len(t, mustQuerySessionSQL(t, engine, otherSession, "app", "select id from autocommit_boundary"), 1)
}

func TestAutocommitTransitionPreservesCurrentReadsAndAggregates(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("transaction_isolation", "REPEATABLE-READ")
	mustExecSessionSQLFully(t, engine, session, "", "create database app")
	mustExecSessionSQLFully(t, engine, session, "app", "create table jdbc_visibility (id int primary key, bucket varchar(20), amount int)")
	mustExecSessionSQLFully(t, engine, session, "app", "insert into jdbc_visibility values (1, 'a', 10)")

	mustExecSessionSQLFully(t, engine, session, "app", "set autocommit = 0")
	mustExecSessionSQLFully(t, engine, session, "app", "update jdbc_visibility set amount = 20 where id = 1")
	require.Equal(t, [][]interface{}{{"1", "20"}}, mustQuerySessionSQL(t, engine, session, "app", "select id, amount from jdbc_visibility where id = 1"))
	mustExecSessionSQLFully(t, engine, session, "app", "commit")
	mustExecSessionSQLFully(t, engine, session, "app", "set autocommit = 1")
	mustExecSessionSQLFully(t, engine, session, "app", "insert into jdbc_visibility values (2, 'b', 30)")

	aggregate := mustSelectResultSessionSQL(t, engine, session, "app", "select count(*), sum(amount) from jdbc_visibility")
	require.Len(t, aggregate.Records, 1)
	aggregateValues := aggregate.Records[0].GetValues()
	require.Len(t, aggregateValues, 2)
	require.Equal(t, int64(2), aggregateValues[0].Int())
	require.InDelta(t, 50.0, aggregateValues[1].Float64(), 0.0001)

	grouped := mustSelectResultSessionSQL(t, engine, session, "app", "select bucket, count(*) from jdbc_visibility group by bucket order by bucket")
	require.Len(t, grouped.Records, 2)
	require.Equal(t, "a", grouped.Records[0].GetValues()[0].String())
	require.Equal(t, int64(1), grouped.Records[0].GetValues()[1].Int())
	require.Equal(t, "b", grouped.Records[1].GetValues()[0].String())
	require.Equal(t, int64(1), grouped.Records[1].GetValues()[1].Int())
}

func TestRepeatedJdbcTransactionSequenceKeepsDecimalRowsVisible(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("transaction_isolation", "REPEATABLE-READ")
	mustExecSessionSQLFully(t, engine, session, "", "create database app")
	mustExecSessionSQLFully(t, engine, session, "app", "create table accounts (id int primary key auto_increment, account_name varchar(100) not null, balance decimal(10,2) not null default 0.00)")

	// TransactionTest.testTransactionCommit
	mustExecSessionSQLFully(t, engine, session, "app", "set autocommit = 0")
	mustExecSessionSQLFully(t, engine, session, "app", "insert into accounts (account_name, balance) values ('Alice', 1000.00)")
	mustExecSessionSQLFully(t, engine, session, "app", "insert into accounts (account_name, balance) values ('Bob', 2000.00)")
	mustExecSessionSQLFully(t, engine, session, "app", "commit")
	require.Len(t, mustQuerySessionSQL(t, engine, session, "app", "select count(*) from accounts"), 1)
	require.True(t, transactionSnapshotCaptured(session))
	mustExecSessionSQLFully(t, engine, session, "app", "set autocommit = 1")
	require.False(t, transactionSnapshotCaptured(session))

	mustExecSessionSQLFully(t, engine, session, "app", "truncate table accounts")
	// TransactionTest.testTransactionRollback
	mustExecSessionSQLFully(t, engine, session, "app", "set autocommit = 0")
	mustExecSessionSQLFully(t, engine, session, "app", "insert into accounts (account_name, balance) values ('Charlie', 3000.00)")
	mustExecSessionSQLFully(t, engine, session, "app", "rollback")
	mustExecSessionSQLFully(t, engine, session, "app", "set autocommit = 1")

	mustExecSessionSQLFully(t, engine, session, "app", "truncate table accounts")
	// TransactionTest.testTransferMoneySuccess
	mustExecSessionSQLFully(t, engine, session, "app", "insert into accounts (account_name, balance) values ('Alice', 1000.00)")
	mustExecSessionSQLFully(t, engine, session, "app", "insert into accounts (account_name, balance) values ('Bob', 500.00)")
	mustExecSessionSQLFully(t, engine, session, "app", "set autocommit = 0")
	mustExecSessionSQLFully(t, engine, session, "app", "update accounts set balance = balance - 200 where account_name = 'Alice'")
	mustExecSessionSQLFully(t, engine, session, "app", "update accounts set balance = balance + 200 where account_name = 'Bob'")
	mustExecSessionSQLFully(t, engine, session, "app", "commit")
	mustExecSessionSQLFully(t, engine, session, "app", "set autocommit = 1")

	mustExecSessionSQLFully(t, engine, session, "app", "truncate table accounts")
	// TransactionTest.testTransferMoneyFailureRollback: this is the first
	// failing JDBC read in the full suite.
	mustExecSessionSQLFully(t, engine, session, "app", "insert into accounts (account_name, balance) values ('Alice', 100.00)")
	mustExecSessionSQLFully(t, engine, session, "app", "insert into accounts (account_name, balance) values ('Bob', 500.00)")
	mustExecSessionSQLFully(t, engine, session, "app", "set autocommit = 0")
	mustExecSessionSQLFully(t, engine, session, "app", "update accounts set balance = balance - 200 where account_name = 'Alice'")
	result := mustSelectResultSessionSQL(t, engine, session, "app", "select balance from accounts where account_name = 'Alice'")
	require.Len(t, result.Records, 1)
}

func TestSetTransactionCharacteristicsRejectsActiveTransaction(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "", "create database app")
	mustExecSessionSQL(t, engine, session, "app", "start transaction")

	for _, query := range []string{
		"set transaction isolation level serializable",
		"set session transaction read only",
	} {
		t.Run(query, func(t *testing.T) {
			result := <-engine.ExecuteQuery(session, query, "app")
			require.Error(t, result.Err)
			require.Contains(t, strings.ToLower(result.Err.Error()), "transaction")
			require.Contains(t, strings.ToLower(result.Err.Error()), "progress")
		})
	}

	mustExecSessionSQL(t, engine, session, "app", "rollback")
}

func TestSetGlobalTransactionCharacteristicsAllowedDuringActiveTransaction(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.SuperPriv})
	mustExecSessionSQL(t, engine, session, "", "create database app")
	mustExecSessionSQL(t, engine, session, "app", "start transaction")

	result := <-engine.ExecuteQuery(session, "set global transaction isolation level serializable", "app")
	require.NoError(t, result.Err)
	mustExecSessionSQL(t, engine, session, "app", "rollback")
}

func TestReadOnlyTransactionRejectsWritesButAllowsReplicationReplay(t *testing.T) {
	session := newTestMySQLSession()
	session.SetParamByName("tx_read_only", int64(1))
	session.SetParamByName("in_transaction", true)

	if err := rejectReadOnlyDML(session, "INSERT"); err == nil {
		t.Fatal("read-only transaction should reject INSERT")
	}

	session.SetParamByName("replication_replay", true)
	if err := rejectReadOnlyDML(session, "INSERT"); err != nil {
		t.Fatalf("replication replay should bypass client read-only guard: %v", err)
	}
}

func TestReplicationStorageTransactionContextIsReusedAcrossStatements(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	dml, err := engine.QueryExecutor.newStorageIntegratedDMLExecutor()
	require.NoError(t, err)
	shared := &StorageTransactionContext{}
	ctx := context.WithValue(context.Background(), "replication_storage_transaction", shared)

	first, err := dml.beginStorageTransaction(ctx)
	require.NoError(t, err)
	second, err := dml.beginStorageTransaction(ctx)
	require.NoError(t, err)
	require.Same(t, shared, first)
	require.Same(t, shared, second)
}

func TestClientTransactionUsesSharedStorageTransactionBoundary(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	setup := newTestMySQLSession()
	mustExecSessionSQLFully(t, engine, setup, "", "create database app")
	mustExecSessionSQLFully(t, engine, setup, "app", "create table client_transaction_boundary (id int primary key, note varchar(20))")

	writer := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQLFully(t, engine, writer, "app", "set autocommit = 0")
	mustExecSessionSQLFully(t, engine, writer, "app", "insert into client_transaction_boundary values (1, 'one')")
	require.Equal(t, [][]interface{}{{"1", "one"}}, mustQuerySessionSQL(t, engine, writer, "app", "select id, note from client_transaction_boundary"))
	require.Empty(t, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from client_transaction_boundary"))

	mustExecSessionSQLFully(t, engine, writer, "app", "insert into client_transaction_boundary values (2, 'two')")
	require.Empty(t, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from client_transaction_boundary"))

	mustExecSessionSQLFully(t, engine, writer, "app", "commit")
	require.Equal(t, [][]interface{}{{"1", "one"}, {"2", "two"}}, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from client_transaction_boundary order by id"))

	mustExecSessionSQLFully(t, engine, writer, "app", "insert into client_transaction_boundary values (3, 'three')")
	require.Equal(t, [][]interface{}{{"1", "one"}, {"2", "two"}, {"3", "three"}}, mustQuerySessionSQL(t, engine, writer, "app", "select id, note from client_transaction_boundary order by id"))
	mustExecSessionSQLFully(t, engine, writer, "app", "rollback")
	require.Equal(t, [][]interface{}{{"1", "one"}, {"2", "two"}}, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from client_transaction_boundary order by id"))
}

func TestClientTransactionHidesPendingUpdatesAndDeletes(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	setup := newTestMySQLSession()
	mustExecSessionSQLFully(t, engine, setup, "", "create database app")
	mustExecSessionSQLFully(t, engine, setup, "app", "create table client_transaction_update_delete (id int primary key, note varchar(20), index idx_note (note))")
	mustExecSessionSQLFully(t, engine, setup, "app", "insert into client_transaction_update_delete values (1, 'old')")

	writer := newTestMySQLSession()
	reader := newTestMySQLSession()
	mustExecSessionSQLFully(t, engine, writer, "app", "set autocommit = 0")
	mustExecSessionSQLFully(t, engine, writer, "app", "update client_transaction_update_delete set note = 'new' where id = 1")
	require.Equal(t, [][]interface{}{{"1", "new"}}, mustQuerySessionSQL(t, engine, writer, "app", "select id, note from client_transaction_update_delete where note = 'new'"))
	require.Equal(t, [][]interface{}{{"1", "old"}}, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from client_transaction_update_delete where note = 'old'"))
	require.Empty(t, mustQuerySessionSQL(t, engine, reader, "app", "select id from client_transaction_update_delete where note = 'new'"))

	mustExecSessionSQLFully(t, engine, writer, "app", "delete from client_transaction_update_delete where id = 1")
	require.Empty(t, mustQuerySessionSQL(t, engine, writer, "app", "select id from client_transaction_update_delete"))
	require.Equal(t, [][]interface{}{{"1", "old"}}, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from client_transaction_update_delete where note = 'old'"))

	mustExecSessionSQLFully(t, engine, writer, "app", "rollback")
	require.Equal(t, [][]interface{}{{"1", "old"}}, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from client_transaction_update_delete where note = 'old'"))

	mustExecSessionSQLFully(t, engine, writer, "app", "update client_transaction_update_delete set note = 'committed' where id = 1")
	mustExecSessionSQLFully(t, engine, writer, "app", "commit")
	require.Equal(t, [][]interface{}{{"1", "committed"}}, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from client_transaction_update_delete where note = 'committed'"))
}

func TestRepeatableReadSnapshotHidesLaterCommittedDML(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	setup := newTestMySQLSession()
	mustExecSessionSQLFully(t, engine, setup, "", "create database app")
	mustExecSessionSQLFully(t, engine, setup, "app", "create table repeatable_read_snapshot (id int primary key, note varchar(20))")
	mustExecSessionSQLFully(t, engine, setup, "app", "insert into repeatable_read_snapshot values (1, 'old')")

	reader := newTestMySQLSession()
	writer := newTestMySQLSession()
	mustExecSessionSQLFully(t, engine, reader, "app", "set autocommit = 0")
	require.Equal(t, [][]interface{}{{"1", "old"}}, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from repeatable_read_snapshot order by id"))

	mustExecSessionSQLFully(t, engine, writer, "app", "insert into repeatable_read_snapshot values (2, 'later')")
	mustExecSessionSQLFully(t, engine, writer, "app", "update repeatable_read_snapshot set note = 'new' where id = 1")
	require.Equal(t, [][]interface{}{{"1", "old"}}, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from repeatable_read_snapshot order by id"))

	mustExecSessionSQLFully(t, engine, reader, "app", "commit")
	require.Equal(t, [][]interface{}{{"1", "new"}, {"2", "later"}}, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from repeatable_read_snapshot order by id"))
}

func TestRepeatableReadSnapshotCapturesEmptyEpoch(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	setup := newTestMySQLSession()
	mustExecSessionSQLFully(t, engine, setup, "", "create database app")
	mustExecSessionSQLFully(t, engine, setup, "app", "create table repeatable_read_empty_epoch (id int primary key, note varchar(20))")

	reader := newTestMySQLSession()
	writer := newTestMySQLSession()
	mustExecSessionSQLFully(t, engine, reader, "app", "set autocommit = 0")
	require.Empty(t, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from repeatable_read_empty_epoch"))

	mustExecSessionSQLFully(t, engine, writer, "app", "insert into repeatable_read_empty_epoch values (1, 'later')")
	require.Empty(t, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from repeatable_read_empty_epoch"))

	mustExecSessionSQLFully(t, engine, reader, "app", "commit")
	require.Equal(t, [][]interface{}{{"1", "later"}}, mustQuerySessionSQL(t, engine, reader, "app", "select id, note from repeatable_read_empty_epoch"))
}

func TestCommittedHistoryRetainsRecordsNeededByLongSnapshot(t *testing.T) {
	committedClientTransactionHistory.Lock()
	previousItems := append([]committedClientTransactionRecord(nil), committedClientTransactionHistory.items...)
	previousEpoch := committedClientTransactionHistory.nextEpoch
	previousSnapshots := make(map[string]uint64, len(committedClientTransactionHistory.activeSnapshot))
	for key, epoch := range committedClientTransactionHistory.activeSnapshot {
		previousSnapshots[key] = epoch
	}
	committedClientTransactionHistory.items = nil
	committedClientTransactionHistory.nextEpoch = 0
	committedClientTransactionHistory.activeSnapshot = make(map[string]uint64)
	committedClientTransactionHistory.Unlock()
	t.Cleanup(func() {
		committedClientTransactionHistory.Lock()
		committedClientTransactionHistory.items = previousItems
		committedClientTransactionHistory.nextEpoch = previousEpoch
		committedClientTransactionHistory.activeSnapshot = previousSnapshots
		committedClientTransactionHistory.Unlock()
	})

	reader := newTestMySQLSession()
	reader.SetParamByName("autocommit", "0")
	reader.SetParamByName("transaction_isolation", "REPEATABLE-READ")
	ensureTransactionSnapshot(reader)
	for index := 0; index < maxCommittedClientTransactionHistory+1; index++ {
		recordAutocommitClientTransactionChanges("history-test", []transactionDMLChange{{
			tableName: "app.history", before: map[string]interface{}{}, after: map[string]interface{}{"id": int64(index)}, kind: "insert",
		}})
	}
	committedClientTransactionHistory.RLock()
	require.Greater(t, len(committedClientTransactionHistory.items), maxCommittedClientTransactionHistory)
	committedClientTransactionHistory.RUnlock()

	clearTransactionSnapshot(reader)
	committedClientTransactionHistory.RLock()
	require.LessOrEqual(t, len(committedClientTransactionHistory.items), maxCommittedClientTransactionHistory)
	committedClientTransactionHistory.RUnlock()
}

func TestRepeatableReadSnapshotCoversJoinAndAggregateSources(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	setup := newTestMySQLSession()
	mustExecSessionSQLFully(t, engine, setup, "", "create database app")
	mustExecSessionSQLFully(t, engine, setup, "app", "create table rr_join_parent (id int primary key)")
	mustExecSessionSQLFully(t, engine, setup, "app", "create table rr_join_child (id int primary key, parent_id int, index idx_parent_id (parent_id))")
	mustExecSessionSQLFully(t, engine, setup, "app", "insert into rr_join_parent values (1)")
	mustExecSessionSQLFully(t, engine, setup, "app", "insert into rr_join_child values (1, 1)")

	reader := newTestMySQLSession()
	writer := newTestMySQLSession()
	mustExecSessionSQLFully(t, engine, reader, "app", "set autocommit = 0")
	joinQuery := "select count(*) from rr_join_parent p join rr_join_child c on c.parent_id = p.id"
	derivedJoinQuery := "select count(*) from (select p.id from rr_join_parent p join rr_join_child c on c.parent_id = p.id) joined_rows"
	require.Equal(t, [][]interface{}{{"\x00\x00\x00\x00\x00\x00\x00\x01"}}, mustQuerySessionSQL(t, engine, reader, "app", joinQuery))
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySessionSQL(t, engine, reader, "app", derivedJoinQuery))

	mustExecSessionSQLFully(t, engine, writer, "app", "insert into rr_join_parent values (2)")
	mustExecSessionSQLFully(t, engine, writer, "app", "insert into rr_join_child values (2, 2)")
	require.Equal(t, [][]interface{}{{"\x00\x00\x00\x00\x00\x00\x00\x01"}}, mustQuerySessionSQL(t, engine, reader, "app", joinQuery))
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySessionSQL(t, engine, reader, "app", derivedJoinQuery))

	mustExecSessionSQLFully(t, engine, reader, "app", "commit")
	require.Equal(t, [][]interface{}{{"\x00\x00\x00\x00\x00\x00\x00\x02"}}, mustQuerySessionSQL(t, engine, reader, "app", joinQuery))
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySessionSQL(t, engine, reader, "app", derivedJoinQuery))
}

func TestReadOnlyTransactionRejectsSQLDML(t *testing.T) {
	cfg := &conf.Cfg{DataDir: t.TempDir(), InnodbDataDir: t.TempDir(), InnodbBufferPoolSize: 16 * 1024 * 1024}
	cfg.InnodbDataDir = cfg.DataDir
	engine := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	session := newTestMySQLSession()

	require.NoError(t, (<-engine.ExecuteQuery(session, "create database app", "")).Err)
	require.NoError(t, (<-engine.ExecuteQuery(session, "create table readonly_dml (id int primary key)", "app")).Err)
	require.NoError(t, (<-engine.ExecuteQuery(session, "insert into readonly_dml values (1)", "app")).Err)

	readOnly := <-engine.ExecuteQuery(session, "start transaction read only", "app")
	require.NoError(t, readOnly.Err)
	mustExecSessionSQL(t, engine, session, "app", "create temporary table readonly_tmp (id int primary key)")
	temporaryWrite := <-engine.ExecuteQuery(session, "insert into readonly_tmp values (1)", "app")
	require.NoError(t, temporaryWrite.Err)

	for _, query := range []string{
		"insert into readonly_dml values (2)",
		"update readonly_dml set id = 2 where id = 1",
		"delete from readonly_dml where id = 1",
	} {
		t.Run(query, func(t *testing.T) {
			result := <-engine.ExecuteQuery(session, query, "app")
			require.Error(t, result.Err)
			require.Contains(t, result.Err.Error(), "READ ONLY")
		})
	}

	session.SetParamByName("transaction_read_only", int64(1))
	readWrite := <-engine.ExecuteQuery(session, "start transaction read write", "app")
	require.NoError(t, readWrite.Err)
	require.NoError(t, (<-engine.ExecuteQuery(session, "insert into readonly_dml values (2)", "app")).Err)
	require.NoError(t, (<-engine.ExecuteQuery(session, "commit", "app")).Err)

	selectResult := <-engine.ExecuteQuery(session, "select id from readonly_dml", "app")
	require.NoError(t, selectResult.Err)
}

func TestSetTransactionReadOnlySQLSynchronizesAliases(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()

	mustExecSessionSQL(t, engine, session, "", "create database app")
	mustExecSessionSQL(t, engine, session, "app", "create table set_transaction_readonly (id int primary key)")

	setReadOnly := <-engine.ExecuteQuery(session, "set transaction read only", "app")
	require.NoError(t, setReadOnly.Err)
	require.True(t, sessionBoolValue(session.GetParamByName("next_transaction_read_only")))
	require.False(t, sessionBoolParam(session, "tx_read_only"))
	require.False(t, sessionBoolParam(session, "transaction_read_only"))

	blocked := <-engine.ExecuteQuery(session, "insert into set_transaction_readonly values (1)", "app")
	require.Error(t, blocked.Err)
	require.Contains(t, blocked.Err.Error(), "READ ONLY")

	setReadWrite := <-engine.ExecuteQuery(session, "set transaction read write", "app")
	require.NoError(t, setReadWrite.Err)
	require.False(t, sessionBoolValue(session.GetParamByName("next_transaction_read_only")))
	require.False(t, sessionBoolParam(session, "tx_read_only"))
	require.False(t, sessionBoolParam(session, "transaction_read_only"))
	require.NoError(t, (<-engine.ExecuteQuery(session, "insert into set_transaction_readonly values (1)", "app")).Err)
}

func TestTransactionContextFromSessionUsesIsolationAndReadOnly(t *testing.T) {
	session := newTestMySQLSession()
	session.SetParamByName("transaction_isolation", "READ COMMITTED")
	session.SetParamByName("transaction_read_only", int64(1))
	session.SetParamByName("in_transaction", true)

	ctx := transactionContextForSession(context.Background(), session)
	require.Equal(t, uint8(1), ctx.Value("isolation_level"))
	require.True(t, ctx.Value("read_only").(bool))
}

func TestSetTransactionIsolationSynchronizesCompatibilityAliases(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "", "create database app")

	result := <-engine.ExecuteQuery(session, "set session transaction isolation level read committed", "app")
	require.NoError(t, result.Err)
	require.Equal(t, "READ COMMITTED", session.GetParamByName("transaction_isolation"))
	require.Equal(t, "READ COMMITTED", session.GetParamByName("tx_isolation"))

	mustExecSessionSQL(t, engine, session, "app", "start transaction")
	state, ok := session.GetParamByName("transaction_dml_state").(*sessionTransactionState)
	require.True(t, ok)
	require.Equal(t, "READ COMMITTED", state.IsolationLevel)
	require.Equal(t, uint8(1), transactionContextForSession(context.Background(), session).Value("isolation_level"))
	mustExecSessionSQL(t, engine, session, "app", "commit")

	invalid := <-engine.ExecuteQuery(session, "set session transaction isolation level snapshot", "app")
	require.Error(t, invalid.Err)
}

func TestSetTransactionCharacteristicsCanBeCombined(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "", "create database app")

	result := <-engine.ExecuteQuery(session, "set transaction isolation level read committed, read only", "app")
	require.NoError(t, result.Err)
	require.Equal(t, "READ COMMITTED", session.GetParamByName("next_transaction_isolation"))
	require.True(t, sessionBoolValue(session.GetParamByName("next_transaction_read_only")))

	mustExecSessionSQL(t, engine, session, "app", "start transaction")
	state, ok := session.GetParamByName("transaction_dml_state").(*sessionTransactionState)
	require.True(t, ok)
	require.Equal(t, "READ COMMITTED", state.IsolationLevel)
	require.Equal(t, "READ ONLY", state.AccessMode)
	mustExecSessionSQL(t, engine, session, "app", "rollback")
}

func TestSetTransactionRejectsConflictingAccessModes(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "", "create database app")

	for _, query := range []string{
		"set transaction read only, read write",
		"set transaction isolation level read committed, isolation level serializable",
	} {
		t.Run(query, func(t *testing.T) {
			result := <-engine.ExecuteQuery(session, query, "app")
			require.Error(t, result.Err)
			require.Contains(t, strings.ToLower(result.Err.Error()), "transaction")
		})
	}
}

func TestSetSessionTransactionReadOnlyAppliesToNextTransaction(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "", "create database app")
	mustExecSessionSQL(t, engine, session, "app", "create table session_tx_default (id int primary key)")

	mustExecSessionSQL(t, engine, session, "app", "set session transaction read only")
	// SET SESSION TRANSACTION changes the default for the next transaction;
	// it must not reject an implicit/autocommit write before that transaction.
	mustExecSessionSQL(t, engine, session, "app", "insert into session_tx_default values (1)")

	mustExecSessionSQL(t, engine, session, "app", "start transaction")
	blocked := <-engine.ExecuteQuery(session, "insert into session_tx_default values (2)", "app")
	require.Error(t, blocked.Err)
	require.Contains(t, blocked.Err.Error(), "READ ONLY")
	mustExecSessionSQL(t, engine, session, "app", "rollback")
}

func TestUnscopedSetTransactionAppliesOnlyToNextTransaction(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "", "create database app")

	result := <-engine.ExecuteQuery(session, "set transaction isolation level read uncommitted", "app")
	require.NoError(t, result.Err)
	require.Nil(t, session.GetParamByName("transaction_isolation"))
	require.Equal(t, "READ UNCOMMITTED", session.GetParamByName("next_transaction_isolation"))

	mustExecSessionSQL(t, engine, session, "app", "start transaction")
	state, ok := session.GetParamByName("transaction_dml_state").(*sessionTransactionState)
	require.True(t, ok)
	require.Equal(t, "READ UNCOMMITTED", state.IsolationLevel)
	require.Nil(t, session.GetParamByName("next_transaction_isolation"))
	mustExecSessionSQL(t, engine, session, "app", "commit")
}

func TestSetGlobalTransactionIsolationSynchronizesAliases(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	admin := newTestMySQLSession()
	admin.SetParamByName("global_privileges", []common.PrivilegeType{common.SuperPriv})

	result := <-engine.ExecuteQuery(admin, "set global transaction isolation level serializable", "")
	require.NoError(t, result.Err)
	sysVars := engine.storageMgr.GetSystemVariablesManager()
	globalIsolation, err := sysVars.GetVariable("", "transaction_isolation", manager.GlobalScope)
	require.NoError(t, err)
	legacyIsolation, err := sysVars.GetVariable("", "tx_isolation", manager.GlobalScope)
	require.NoError(t, err)
	require.Equal(t, "SERIALIZABLE", fmt.Sprint(globalIsolation))
	require.Equal(t, "SERIALIZABLE", fmt.Sprint(legacyIsolation))
}

func TestGlobalTransactionDefaultsApplyToNewSession(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	admin := newTestMySQLSession()
	admin.SetParamByName("global_privileges", []common.PrivilegeType{common.SuperPriv})
	mustExecSessionSQL(t, engine, admin, "", "create database app")

	require.NoError(t, (<-engine.ExecuteQuery(admin, "set global transaction isolation level read committed", "")).Err)
	require.NoError(t, (<-engine.ExecuteQuery(admin, "set global transaction read only", "")).Err)

	client := newTestMySQLSession()
	mustExecSessionSQL(t, engine, client, "app", "start transaction")
	state, ok := client.GetParamByName("transaction_dml_state").(*sessionTransactionState)
	require.True(t, ok)
	require.Equal(t, "READ COMMITTED", state.IsolationLevel)
	require.Equal(t, "READ ONLY", state.AccessMode)
	require.NoError(t, (<-engine.ExecuteQuery(client, "rollback", "app")).Err)
}

func TestGlobalReadOnlyRejectsClientWritesButAllowsAdminAndReplay(t *testing.T) {
	dataDir := t.TempDir()
	engine := newTestStorageIntegratedExecutor(t, dataDir)
	session := newTestMySQLSession()

	mustExecSessionSQL(t, engine, session, "", "create database app")
	mustExecSessionSQL(t, engine, session, "app", "create table global_readonly (id int primary key)")
	mustExecSessionSQL(t, engine, session, "app", "insert into global_readonly values (1)")

	admin := newTestMySQLSession()
	admin.SetParamByName("global_privileges", []common.PrivilegeType{common.SuperPriv})
	setReadOnly := <-engine.ExecuteQuery(admin, "set global read_only = ON", "")
	require.NoError(t, setReadOnly.Err)
	showReadOnly := <-engine.ExecuteQuery(session, "show global variables like 'read_only'", "")
	require.NoError(t, showReadOnly.Err)
	showData, ok := showReadOnly.Data.(map[string]interface{})
	require.True(t, ok)
	showRows, ok := showData["rows"].([][]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"read_only", "ON"}}, showRows)

	clientWrite := <-engine.ExecuteQuery(session, "insert into global_readonly values (2)", "app")
	require.Error(t, clientWrite.Err)
	require.Contains(t, clientWrite.Err.Error(), "read-only")

	selectResult := <-engine.ExecuteQuery(session, "select id from global_readonly", "app")
	require.NoError(t, selectResult.Err)

	mustExecSessionSQL(t, engine, session, "app", "create temporary table global_readonly_tmp (id int primary key)")
	temporaryWrite := <-engine.ExecuteQuery(session, "insert into global_readonly_tmp values (1)", "app")
	require.NoError(t, temporaryWrite.Err)
	temporaryRead := <-engine.ExecuteQuery(session, "select id from global_readonly_tmp", "app")
	require.NoError(t, temporaryRead.Err)

	session.SetParamByName("global_privileges", []common.PrivilegeType{common.SuperPriv})
	adminWrite := <-engine.ExecuteQuery(session, "insert into global_readonly values (2)", "app")
	require.NoError(t, adminWrite.Err)

	session.SetParamByName("global_privileges", nil)
	session.SetParamByName("replication_replay", true)
	replayWrite := <-engine.ExecuteQuery(session, "insert into global_readonly values (3)", "app")
	require.NoError(t, replayWrite.Err)

	setReadWrite := <-engine.ExecuteQuery(admin, "set global read_only = OFF", "")
	require.NoError(t, setReadWrite.Err)
	normalWrite := <-engine.ExecuteQuery(session, "insert into global_readonly values (4)", "app")
	require.NoError(t, normalWrite.Err)
}

func TestSuperReadOnlyBlocksPrivilegedClientsAndSynchronizesReadOnly(t *testing.T) {
	dataDir := t.TempDir()
	engine := newTestStorageIntegratedExecutor(t, dataDir)
	client := newTestMySQLSession()
	admin := newTestMySQLSession()
	admin.SetParamByName("global_privileges", []common.PrivilegeType{common.SuperPriv})

	mustExecSessionSQL(t, engine, client, "", "create database app")
	mustExecSessionSQL(t, engine, client, "app", "create table super_readonly (id int primary key)")
	mustExecSessionSQL(t, engine, client, "app", "insert into super_readonly values (1)")

	mustExecSessionSQL(t, engine, admin, "", "set global super_read_only = on")
	show := <-engine.ExecuteQuery(client, "show global variables like 'super_read_only'", "")
	require.NoError(t, show.Err)
	showData, ok := show.Data.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"super_read_only", "ON"}}, showData["rows"])

	readOnly := <-engine.ExecuteQuery(client, "show global variables like 'read_only'", "")
	require.NoError(t, readOnly.Err)
	readOnlyData, ok := readOnly.Data.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"read_only", "ON"}}, readOnlyData["rows"])

	adminWrite := <-engine.ExecuteQuery(admin, "insert into super_readonly values (2)", "app")
	require.Error(t, adminWrite.Err)
	require.Contains(t, strings.ToLower(adminWrite.Err.Error()), "super-read-only")

	mustExecSessionSQL(t, engine, client, "app", "create temporary table super_readonly_tmp (id int primary key)")
	temporaryWrite := <-engine.ExecuteQuery(client, "insert into super_readonly_tmp values (1)", "app")
	require.NoError(t, temporaryWrite.Err)

	client.SetParamByName("replication_replay", true)
	replayWrite := <-engine.ExecuteQuery(client, "insert into super_readonly values (3)", "app")
	require.NoError(t, replayWrite.Err)
	client.SetParamByName("replication_replay", false)

	// MySQL turns super_read_only off implicitly when read_only is set to OFF.
	// This is the escape hatch used by administrators to restore writes.
	mustExecSessionSQL(t, engine, admin, "", "set global read_only = off")
	showSuperReadOnly := <-engine.ExecuteQuery(client, "show global variables like 'super_read_only'", "")
	require.NoError(t, showSuperReadOnly.Err)
	showSuperData, ok := showSuperReadOnly.Data.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, [][]interface{}{{"super_read_only", "OFF"}}, showSuperData["rows"])
	adminCanWrite := <-engine.ExecuteQuery(admin, "insert into super_readonly values (4)", "app")
	require.NoError(t, adminCanWrite.Err)

	mustExecSessionSQL(t, engine, admin, "", "set global read_only = off")
	normalWrite := <-engine.ExecuteQuery(client, "insert into super_readonly values (5)", "app")
	require.NoError(t, normalWrite.Err)
}

func TestGlobalReadOnlyBlocksPersistentDDLButAllowsTemporaryAndReplicationDDL(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	client := newTestMySQLSession()
	admin := newTestMySQLSession()
	admin.SetParamByName("global_privileges", []common.PrivilegeType{common.SuperPriv})

	mustExecSessionSQL(t, engine, client, "", "create database app")
	mustExecSessionSQL(t, engine, client, "app", "create table read_only_existing (id int primary key)")
	mustExecSessionSQL(t, engine, admin, "", "set global read_only = on")

	blockedCreate := <-engine.ExecuteQuery(client, "create table read_only_blocked (id int primary key)", "app")
	require.Error(t, blockedCreate.Err)
	require.Contains(t, strings.ToLower(blockedCreate.Err.Error()), "read-only")
	blockedDrop := <-engine.ExecuteQuery(client, "drop table read_only_existing", "app")
	require.Error(t, blockedDrop.Err)
	require.Contains(t, strings.ToLower(blockedDrop.Err.Error()), "read-only")

	mustExecSessionSQL(t, engine, client, "app", "create temporary table read_only_tmp (id int primary key)")
	mustExecSessionSQL(t, engine, client, "app", "alter table read_only_tmp add column note varchar(20)")
	mustExecSessionSQL(t, engine, client, "app", "drop table read_only_tmp")
	mustExecSessionSQL(t, engine, client, "app", "create temporary table read_only_tmp_mixed (id int primary key)")
	mixedDrop := <-engine.ExecuteQuery(client, "drop table read_only_tmp_mixed, read_only_existing", "app")
	require.Error(t, mixedDrop.Err)
	require.Contains(t, strings.ToLower(mixedDrop.Err.Error()), "read-only")
	mustExecSessionSQL(t, engine, client, "app", "create temporary table read_only_tmp_mixed_2 (id int primary key)")
	mustExecSessionSQL(t, engine, client, "app", "drop table read_only_tmp_mixed, read_only_tmp_mixed_2")
	analyze := <-engine.ExecuteQuery(client, "analyze table read_only_existing", "app")
	require.NoError(t, analyze.Err)

	mustExecSessionSQL(t, engine, admin, "app", "create table read_only_admin (id int primary key)")
	mustExecSessionSQL(t, engine, admin, "", "set global read_only = off")
	mustExecSessionSQL(t, engine, admin, "", "set global super_read_only = on")

	blockedAdminDDL := <-engine.ExecuteQuery(admin, "create table super_read_only_blocked (id int primary key)", "app")
	require.Error(t, blockedAdminDDL.Err)
	require.Contains(t, strings.ToLower(blockedAdminDDL.Err.Error()), "super-read-only")
	mustExecSessionSQL(t, engine, admin, "app", "create temporary table super_read_only_tmp (id int primary key)")

	client.SetParamByName("replication_replay", true)
	mustExecSessionSQL(t, engine, client, "app", "create table replication_ddl_allowed (id int primary key)")
}

func TestSetGlobalReadOnlyRejectsPendingTransaction(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	admin := newTestMySQLSession()
	admin.SetParamByName("global_privileges", []common.PrivilegeType{common.SuperPriv})
	mustExecSessionSQL(t, engine, admin, "", "create database app")
	mustExecSessionSQL(t, engine, admin, "app", "start transaction")

	blocked := <-engine.ExecuteQuery(admin, "set global read_only = on", "app")
	require.Error(t, blocked.Err)
	require.Contains(t, strings.ToLower(blocked.Err.Error()), "transaction")

	mustExecSessionSQL(t, engine, admin, "app", "rollback")
	mustExecSessionSQL(t, engine, admin, "app", "set global read_only = on")
	mustExecSessionSQL(t, engine, admin, "app", "set global read_only = off")
}

func TestSetGlobalReadOnlyRejectsExplicitTableLock(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	admin := newTestMySQLSession()
	admin.SetParamByName("global_privileges", []common.PrivilegeType{common.SuperPriv})
	mustExecSessionSQL(t, engine, admin, "", "create database app")
	mustExecSessionSQL(t, engine, admin, "app", "create table locked_target (id int primary key)")
	mustExecSessionSQL(t, engine, admin, "app", "lock tables locked_target read")

	blocked := <-engine.ExecuteQuery(admin, "set global read_only = on", "app")
	require.Error(t, blocked.Err)
	require.Contains(t, strings.ToLower(blocked.Err.Error()), "lock")

	mustExecSessionSQL(t, engine, admin, "app", "unlock tables")
	mustExecSessionSQL(t, engine, admin, "app", "set global read_only = on")
	mustExecSessionSQL(t, engine, admin, "app", "set global read_only = off")
}

func TestEngineTransactionStatementsReturnOKBeforeParse(t *testing.T) {
	cfg := &conf.Cfg{InnodbDataDir: t.TempDir()}
	executor := NewXMySQLExecutor(nil, cfg)
	xengine := &XMySQLEngine{conf: cfg, QueryExecutor: executor}
	session := newTestMySQLSession()

	for _, query := range []string{
		"begin",
		"savepoint sp1",
		"rollback to savepoint sp1",
		"release savepoint sp1",
		"commit",
	} {
		t.Run(query, func(t *testing.T) {
			got := <-xengine.ExecuteQuery(session, query, "")
			require.NoError(t, got.Err)
			require.Equal(t, common.RESULT_TYPE_QUERY, got.ResultType)
		})
	}

	require.Equal(t, false, session.GetParamByName("in_transaction"))
	require.False(t, session.SessionContext().GetInTransaction())
	require.Empty(t, session.GetParamByName("savepoints"))
}

func TestResetSessionRollsBackRuntimeTransactionState(t *testing.T) {
	executor := NewXMySQLExecutor(nil, &conf.Cfg{InnodbDataDir: t.TempDir()})
	session := newTestMySQLSession()
	session.SetParamByName("autocommit", "0")
	session.SetParamByName("in_transaction", true)
	session.SetParamByName("transaction_journal_active", true)
	session.SetParamByName("database", "app")
	session.SetParamByName("last_insert_id", uint64(42))
	session.SetParamByName("user_variables", map[string]interface{}{"answer": int64(42)})
	session.SetParamByName("transaction_dml_state", &sessionTransactionState{})
	session.SessionContext().SetInTransaction(true)

	require.NoError(t, executor.ResetSession(session))
	require.Equal(t, "1", session.GetParamByName("autocommit"))
	require.Equal(t, false, session.GetParamByName("in_transaction"))
	require.Equal(t, false, session.GetParamByName("transaction_journal_active"))
	require.Equal(t, "", session.GetParamByName("database"))
	require.Equal(t, uint64(0), session.GetParamByName("last_insert_id"))
	require.False(t, session.SessionContext().GetInTransaction())
	require.Empty(t, session.GetParamByName("user_variables"))
}
