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

func TestP0RollbackRestoresUpdatedRowAndSecondaryIndex(t *testing.T) {
	executor := newP0TransactionTestExecutor(t)
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key, email varchar(50), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into accounts (id, email) values (1, 'old@example.com')")

	mustExecSessionSQL(t, executor, session, "app", "set autocommit=0")
	mustExecSessionSQL(t, executor, session, "app", "update accounts set email = 'new@example.com' where id = 1")
	require.Equal(t, [][]interface{}{{"1", "new@example.com"}}, mustQuerySQL(t, executor, "app", "select id, email from accounts where email = 'new@example.com'"))

	mustExecSessionSQL(t, executor, session, "app", "rollback")
	require.Equal(t, [][]interface{}{{"1", "old@example.com"}}, mustQuerySQL(t, executor, "app", "select id, email from accounts where email = 'old@example.com'"))
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from accounts where email = 'new@example.com'"))
}

func TestP0RollbackRestoresDeletedRowAndSecondaryIndex(t *testing.T) {
	executor := newP0TransactionTestExecutor(t)
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key, email varchar(50), index idx_email (email))")
	mustExecSQL(t, executor, "app", "insert into accounts (id, email) values (1, 'alice@example.com')")

	mustExecSessionSQL(t, executor, session, "app", "set autocommit=0")
	mustExecSessionSQL(t, executor, session, "app", "delete from accounts where id = 1")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from accounts where email = 'alice@example.com'"))

	mustExecSessionSQL(t, executor, session, "app", "rollback")
	require.Equal(t, [][]interface{}{{"1", "alice@example.com"}}, mustQuerySQL(t, executor, "app", "select id, email from accounts where email = 'alice@example.com'"))
}

func TestP0RollbackRestoresOnDeleteCascadeRows(t *testing.T) {
	executor := newP0TransactionTestExecutor(t)
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, label varchar(50), index idx_label (label), foreign key (parent_id) references parents(id) on delete cascade)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id, label) values (1, 1, 'child')")

	mustExecSessionSQL(t, executor, session, "app", "set autocommit=0")
	mustExecSessionSQL(t, executor, session, "app", "delete from parents where id = 1")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from children where label = 'child'"))

	mustExecSessionSQL(t, executor, session, "app", "rollback")
	require.Len(t, mustQuerySQL(t, executor, "app", "select id, parent_id, label from children where label = 'child'"), 1)
}

func TestP0RollbackRestoresOnUpdateCascadeRows(t *testing.T) {
	executor := newP0TransactionTestExecutor(t)
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "app", "create table parents (id int primary key)")
	mustExecSQL(t, executor, "app", "create table children (id int primary key, parent_id int, index idx_parent_id (parent_id), foreign key (parent_id) references parents(id) on update cascade)")
	mustExecSQL(t, executor, "app", "insert into parents (id) values (1)")
	mustExecSQL(t, executor, "app", "insert into children (id, parent_id) values (1, 1)")

	mustExecSessionSQL(t, executor, session, "app", "set autocommit=0")
	mustExecSessionSQL(t, executor, session, "app", "update parents set id = 2 where id = 1")
	require.Len(t, mustQuerySQL(t, executor, "app", "select id, parent_id from children where parent_id = 2"), 1)

	mustExecSessionSQL(t, executor, session, "app", "rollback")
	require.Len(t, mustQuerySQL(t, executor, "app", "select id, parent_id from children where parent_id = 1"), 1)
	require.Empty(t, mustQuerySQL(t, executor, "app", "select id from children where parent_id = 2"))
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
