package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplicationStatusAndBinlogShowsEnforceDistinctPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'replication_observer'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "replication_observer")
	session.SetParamByName("host", "localhost")

	clientQueries := []string{"show master status", "show binary logs"}
	slaveQueries := []string{"show binlog events", "show relaylog events"}
	for _, query := range append(append([]string{}, clientQueries...), slaveQueries...) {
		result := <-executor.ExecuteQuery(session, query, "")
		require.Error(t, result.Err, query)
	}

	mustExecSQL(t, executor, "", "grant replication client on *.* to 'replication_observer'@'localhost'")
	for _, query := range clientQueries {
		require.NoError(t, (<-executor.ExecuteQuery(session, query, "")).Err, query)
	}
	for _, query := range slaveQueries {
		result := <-executor.ExecuteQuery(session, query, "")
		require.Error(t, result.Err, query)
		require.Contains(t, result.Err.Error(), "REPLICATION SLAVE", query)
	}

	mustExecSQL(t, executor, "", "grant replication slave on *.* to 'replication_observer'@'localhost'")
	for _, query := range slaveQueries {
		require.NoError(t, (<-executor.ExecuteQuery(session, query, "")).Err, query)
	}
}
