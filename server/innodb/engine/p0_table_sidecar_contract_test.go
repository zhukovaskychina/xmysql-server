package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestP0StorageIntegratedCRUDDoesNotCreateTableRowsSidecar(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)

	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(50), age int)")

	sidecarPath := filepath.Join(dataDir, "app", "users.xrows.json")

	mustExecSQL(t, executor, "app", "insert into users (id, name, age) values (1, 'alice', 20), (2, 'bob', 30)")
	require.NoFileExists(t, sidecarPath)
	require.Equal(t, [][]interface{}{{"1", "alice"}, {"2", "bob"}}, mustQuerySQL(t, executor, "app", "select id, name from users order by id"))

	mustExecSQL(t, executor, "app", "update users set age = 31 where id = 2")
	require.NoFileExists(t, sidecarPath)
	require.Equal(t, [][]interface{}{{"bob", "31"}}, mustQuerySQL(t, executor, "app", "select name, age from users where id = 2"))

	mustExecSQL(t, executor, "app", "delete from users where id = 1")
	require.NoFileExists(t, sidecarPath)
	require.Equal(t, [][]interface{}{{"2", "bob"}}, mustQuerySQL(t, executor, "app", "select id, name from users order by id"))

	_, err := os.Stat(sidecarPath)
	require.True(t, os.IsNotExist(err), "table row sidecar should not exist: %v", err)
}
