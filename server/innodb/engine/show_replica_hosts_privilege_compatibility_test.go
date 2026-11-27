package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShowReplicaHostsRequiresReplicationSlave(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'replica_hosts_observer'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "replica_hosts_observer")
	session.SetParamByName("host", "localhost")

	queries := []string{"show replicas", "show slave hosts", "show replica hosts"}
	for _, query := range queries {
		result := <-executor.ExecuteQuery(session, query, "")
		require.Error(t, result.Err, query)
		require.Contains(t, result.Err.Error(), "REPLICATION SLAVE", query)
	}

	mustExecSQL(t, executor, "", "grant replication slave on *.* to 'replica_hosts_observer'@'localhost'")
	for _, query := range queries {
		result := <-executor.ExecuteQuery(session, query, "")
		require.NoError(t, result.Err, query)
	}
}
