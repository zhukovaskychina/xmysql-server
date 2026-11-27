package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplicationControlRequiresReplicationSlaveAdminOrSuper(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	started := 0
	executor.QueryExecutor.SetReplicationControl(func() error {
		started++
		return nil
	}, func() error { return nil })
	reset := 0
	executor.QueryExecutor.SetReplicationResetControl(func() error {
		reset++
		return nil
	})
	mustExecSQL(t, executor, "", "create user 'replication_operator'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "replication_operator")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "start replica", "")
	require.Error(t, denied.Err)
	require.Contains(t, denied.Err.Error(), "REPLICATION_SLAVE_ADMIN")

	mustExecSQL(t, executor, "", "grant replication_slave_admin on *.* to 'replication_operator'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "start replica", "")
	require.NoError(t, allowed.Err)
	require.Equal(t, 1, started)

	mustExecSQL(t, executor, "", "revoke replication_slave_admin on *.* from 'replication_operator'@'localhost'")
	mustExecSQL(t, executor, "", "grant super on *.* to 'replication_operator'@'localhost'")
	legacyAllowed := <-executor.ExecuteQuery(session, "start slave", "")
	require.NoError(t, legacyAllowed.Err)
	require.Equal(t, 2, started)

	deniedReset := <-executor.ExecuteQuery(session, "reset replica", "")
	require.Error(t, deniedReset.Err)
	require.Contains(t, deniedReset.Err.Error(), "RELOAD")
	mustExecSQL(t, executor, "", "grant reload on *.* to 'replication_operator'@'localhost'")
	allowedReset := <-executor.ExecuteQuery(session, "reset slave", "")
	require.NoError(t, allowedReset.Err)
	require.Equal(t, 1, reset)
}
