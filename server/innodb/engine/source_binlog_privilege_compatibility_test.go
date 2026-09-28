package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceBinlogAdministrationRequiresReloadOrBinlogAdmin(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	flushes, resets, resetBinaryLogsAndGTIDs, purges := 0, 0, 0, 0
	executor.QueryExecutor.SetReplicationSourceAdminControl(func() error {
		flushes++
		return nil
	}, func() error {
		resets++
		return nil
	})
	executor.QueryExecutor.SetReplicationSourcePurgeControl(func(string) error {
		purges++
		return nil
	})
	executor.QueryExecutor.SetReplicationResetBinaryLogsAndGTIDsControl(func(uint32) error {
		resetBinaryLogsAndGTIDs++
		return nil
	})
	mustExecSQL(t, executor, "", "create user 'binlog_operator'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "binlog_operator")
	session.SetParamByName("host", "localhost")

	for _, query := range []string{"flush binary logs", "reset master", "reset binary logs and gtids"} {
		result := <-executor.ExecuteQuery(session, query, "")
		require.Error(t, result.Err, query)
		require.Contains(t, result.Err.Error(), "RELOAD", query)
	}
	purgeDenied := <-executor.ExecuteQuery(session, "purge binary logs to 'binlog.000003'", "")
	require.Error(t, purgeDenied.Err)
	require.Contains(t, purgeDenied.Err.Error(), "BINLOG_ADMIN")

	mustExecSQL(t, executor, "", "grant reload on *.* to 'binlog_operator'@'localhost'")
	for _, query := range []string{"flush binary logs", "reset master", "reset binary logs and gtids to 1234"} {
		require.NoError(t, (<-executor.ExecuteQuery(session, query, "")).Err, query)
	}
	require.Equal(t, 1, flushes)
	require.Equal(t, 1, resets)
	require.Equal(t, 1, resetBinaryLogsAndGTIDs)

	mustExecSQL(t, executor, "", "grant binlog_admin on *.* to 'binlog_operator'@'localhost'")
	purgeAllowed := <-executor.ExecuteQuery(session, "purge binary logs to 'binlog.000003'", "")
	require.NoError(t, purgeAllowed.Err)
	require.Equal(t, 1, purges)
}
