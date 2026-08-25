package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestP0CompositePrimaryKeyUpdateAndDelete(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table memberships (user_id int, group_id int, score int, primary key (user_id, group_id))")
	mustExecSQL(t, executor, "app", "insert into memberships values (1, 100, 5)")
	mustExecSQL(t, executor, "app", "update memberships set score = 9 where user_id = 1 and group_id = 100")
	require.Equal(t, [][]interface{}{{"9"}}, mustQuerySQL(t, executor, "app", "select score from memberships where user_id = 1 and group_id = 100"))

	mustExecSQL(t, executor, "app", "delete from memberships where user_id = 1 and group_id = 100")
	require.Equal(t, 0, mustCountRows(t, executor, nil, "app", "memberships"))
}

func TestP0IndexedTableWithoutPrimaryKeySupportsDML(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (tenant_id int, event_name varchar(50), index idx_tenant (tenant_id))")
	mustExecSQL(t, executor, "app", "insert into events values (10, 'created')")
	mustExecSQL(t, executor, "app", "update events set event_name = 'updated' where tenant_id = 10")
	require.Equal(t, [][]interface{}{{"updated"}}, mustQuerySQL(t, executor, "app", "select event_name from events where tenant_id = 10"))
	mustExecSQL(t, executor, "app", "delete from events where tenant_id = 10")
	require.Equal(t, 0, mustCountRows(t, executor, nil, "app", "events"))
}
