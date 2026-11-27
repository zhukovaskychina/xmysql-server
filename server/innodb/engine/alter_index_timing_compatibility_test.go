package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAlterAddIndexRebuildsBeforeFollowingDML(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(50))")
	mustExecSQL(t, executor, "app", "insert into users values (1, 'old@example.com')")
	mustExecSQL(t, executor, "app", "alter table users add index idx_email (email)")
	mustExecSQL(t, executor, "app", "insert into users values (2, 'new@example.com')")

	result := <-executor.ExecuteQuery(nil, "select id from users where email = 'old@example.com'", "app")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	require.Len(t, rows.Records, 1)
	require.Equal(t, "1", rows.Records[0].GetValueByIndex(0).ToString())
}
