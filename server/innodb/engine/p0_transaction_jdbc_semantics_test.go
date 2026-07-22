package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
)

func TestP0RollbackRestoresInsertedRows(t *testing.T) {
	executor := newP0TransactionTestExecutor(t)
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key, name varchar(50))")

	mustExecSessionSQL(t, executor, session, "app", "set autocommit=0")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (id, name) values (1, 'alice')")
	require.Equal(t, 1, mustCountRows(t, executor, nil, "app", "accounts"))

	mustExecSessionSQL(t, executor, session, "app", "rollback")
	require.Equal(t, 0, mustCountRows(t, executor, nil, "app", "accounts"))
}

func TestP0RollbackToSavepointKeepsEarlierRows(t *testing.T) {
	executor := newP0TransactionTestExecutor(t)
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key, name varchar(50))")

	mustExecSessionSQL(t, executor, session, "app", "set autocommit=0")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (id, name) values (1, 'alice')")
	mustExecSessionSQL(t, executor, session, "app", "savepoint sp1")
	mustExecSessionSQL(t, executor, session, "app", "insert into accounts (id, name) values (2, 'bob')")

	mustExecSessionSQL(t, executor, session, "app", "rollback to savepoint sp1")
	mustExecSessionSQL(t, executor, session, "app", "commit")
	require.Equal(t, 1, mustCountRows(t, executor, nil, "app", "accounts"))
}

func TestP0RollbackPreservesOtherSessionCommittedRows(t *testing.T) {
	executor := newP0TransactionTestExecutor(t)
	transactionSession := newTestMySQLSession()
	committedSession := newTestMySQLSession()
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key, name varchar(50))")

	mustExecSessionSQL(t, executor, transactionSession, "app", "set autocommit=0")
	mustExecSessionSQL(t, executor, transactionSession, "app", "insert into accounts (id, name) values (1, 'alice')")
	mustExecSessionSQL(t, executor, committedSession, "app", "insert into accounts (id, name) values (2, 'bob')")
	mustExecSessionSQL(t, executor, transactionSession, "app", "rollback")

	require.Equal(t, 1, mustCountRows(t, executor, nil, "app", "accounts"))
}

func newP0TransactionTestExecutor(t *testing.T) *XMySQLEngine {
	t.Helper()
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	return executor
}

func mustCountRows(t *testing.T, executor *XMySQLEngine, session server.MySQLServerSession, databaseName, tableName string) int {
	t.Helper()
	return len(mustQuerySessionSQL(t, executor, session, databaseName, "select * from "+tableName))
}
