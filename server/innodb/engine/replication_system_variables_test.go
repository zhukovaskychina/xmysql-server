package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestQualifiedGlobalReplicationVariablesAreVisibleToClients(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("server_id", int64(37))
	session.SetParamByName("gtid_mode", "ON")
	session.SetParamByName("log_bin", "ON")
	session.SetParamByName("binlog_format", "ROW")
	require.Len(t, directSystemVariableSelectPattern.FindStringSubmatch("select @@global.version, @@global.server_id"), 2)
	require.Len(t, directSystemVariableTokenPattern.FindStringSubmatch("@@global.version"), 3)
	direct, handled, directErr := executor.QueryExecutor.executeDirectSystemVariableSelect("select @@global.version, @@global.server_id, @@global.gtid_mode, @@global.log_bin, @@global.binlog_format", session)
	require.True(t, handled)
	require.NoError(t, directErr)
	require.NotNil(t, direct)

	result := mustSelectResultSessionSQL(t, executor, session, "", "select @@global.version, @@global.server_id, @@global.gtid_mode, @@global.log_bin, @@global.binlog_format")
	values := result.Records[0].GetValues()
	require.Equal(t, "8.0.32", values[0].String())
	require.Equal(t, int64(37), values[1].Int())
	require.Equal(t, "ON", values[2].String())
	require.Equal(t, "ON", values[3].String())
	require.Equal(t, "ROW", values[4].String())
}

func TestQualifiedGlobalGtidExecutedUsesReplicationRuntimeState(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	defer executor.Close()
	executor.QueryExecutor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{ExecutedGTIDs: "source-uuid:6-12"}
	})

	result, handled, err := executor.QueryExecutor.executeDirectSystemVariableSelect(
		"select @@global.gtid_executed", newTestMySQLSession())
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, "source-uuid:6-12", result.Records[0].GetValues()[0].String())
}
