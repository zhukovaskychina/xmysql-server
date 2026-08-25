package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestP0InsertOnDuplicateKeyUpdate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, username varchar(50) unique, age int)")
	mustExecSQL(t, executor, "app", "insert into users (id, username, age) values (1, 'alice', 20)")

	mustExecSQL(t, executor, "app", "insert into users (id, username, age) values (1, 'alice', 21) on duplicate key update age = 22")

	require.Equal(t, [][]interface{}{{"alice", "22"}}, mustQuerySQL(t, executor, "app", "select username, age from users where id = 1"))
	require.Equal(t, [][]interface{}{{"\x00\x00\x00\x00\x00\x00\x00\x01"}}, mustQuerySQL(t, executor, "app", "select count(*) from users"))
}

func TestP0InsertOnDuplicateMixedBatchUpdatesAndInserts(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, username varchar(50) unique, age int)")
	mustExecSQL(t, executor, "app", "insert into users (id, username, age) values (1, 'alice', 20)")

	mustExecSQL(t, executor, "app", "insert into users (id, username, age) values (1, 'alice', 21), (2, 'bob', 30) on duplicate key update age = 22")

	require.Equal(t, [][]interface{}{{"1", "alice", "22"}, {"2", "bob", "30"}}, mustQuerySQL(t, executor, "app", "select id, username, age from users order by id"))
}

func TestP0ReplaceIntoReplacesExistingPrimaryKey(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, username varchar(50), age int)")
	mustExecSQL(t, executor, "app", "insert into users (id, username, age) values (1, 'alice', 20)")

	mustExecSQL(t, executor, "app", "replace into users (id, username, age) values (1, 'alice2', 30)")

	require.Equal(t, [][]interface{}{{"alice2", "30"}}, mustQuerySQL(t, executor, "app", "select username, age from users where id = 1"))
	require.Equal(t, [][]interface{}{{"\x00\x00\x00\x00\x00\x00\x00\x01"}}, mustQuerySQL(t, executor, "app", "select count(*) from users"))
}

func TestP0InsertSelectCopiesRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table source_users (id int primary key, username varchar(50), age int)")
	mustExecSQL(t, executor, "app", "create table copied_users (id int primary key, username varchar(50), age int)")
	mustExecSQL(t, executor, "app", "insert into source_users (id, username, age) values (1, 'alice', 20), (2, 'bob', 30)")

	mustExecSQL(t, executor, "app", "insert into copied_users (id, username, age) select id, username, age from source_users where age >= 20")

	require.Equal(t, [][]interface{}{{"1", "alice", "20"}, {"2", "bob", "30"}}, mustQuerySQL(t, executor, "app", "select id, username, age from copied_users order by id"))
}
