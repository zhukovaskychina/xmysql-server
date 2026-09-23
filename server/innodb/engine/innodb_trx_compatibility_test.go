package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func TestInformationSchemaInnoDBTrxProjectsActiveTransactionSnapshot(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	txManager, err := executor.QueryExecutor.getTransactionManager()
	require.NoError(t, err)
	trx, err := txManager.Begin(false, 2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, txManager.Rollback(trx)) })

	result := <-executor.ExecuteQuery(nil, "select trx_id, trx_state, trx_started, trx_is_read_only, trx_isolation_level from information_schema.innodb_trx", "")
	require.NoError(t, result.Err)
	rows, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Contains(t, rows.Columns, "TRX_ID")
	require.Contains(t, rows.Columns, "TRX_STATE")

	found := false
	for _, record := range rows.Records {
		values := record.GetValues()
		if len(values) >= 5 && values[0].Int() == trx.ID {
			found = true
			require.Equal(t, "RUNNING", values[1].String())
			require.Equal(t, int64(0), values[3].Int())
			require.Equal(t, "REPEATABLE READ", values[4].String())
		}
	}
	require.True(t, found, "active transaction was not projected: %#v", rows.Records)
	wrongState := <-executor.ExecuteQuery(nil, "select trx_id from information_schema.innodb_trx where trx_state = 'PREPARED'", "")
	require.NoError(t, wrongState.Err)
	require.Empty(t, wrongState.Data.(*SelectResult).Records)
	matchingIsolation := <-executor.ExecuteQuery(nil, "select trx_id from information_schema.innodb_trx where trx_isolation_level = 'REPEATABLE READ'", "")
	require.NoError(t, matchingIsolation.Err)
	require.NotEmpty(t, matchingIsolation.Data.(*SelectResult).Records)
	wrongReadOnly := <-executor.ExecuteQuery(nil, "select trx_id from information_schema.innodb_trx where trx_is_read_only = 1", "")
	require.NoError(t, wrongReadOnly.Err)
	require.Empty(t, wrongReadOnly.Data.(*SelectResult).Records)
	wrongStarted := <-executor.ExecuteQuery(nil, "select trx_id from information_schema.innodb_trx where trx_started like '1900%'", "")
	require.NoError(t, wrongStarted.Err)
	require.Empty(t, wrongStarted.Data.(*SelectResult).Records)
	wrongLockStructs := <-executor.ExecuteQuery(nil, "select trx_id from information_schema.innodb_trx where trx_lock_structs = 999", "")
	require.NoError(t, wrongLockStructs.Err)
	require.Empty(t, wrongLockStructs.Data.(*SelectResult).Records)
}

func TestInformationSchemaInnoDBLockViewsProjectWaitGraph(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	lockManager := manager.NewLockManager()
	defer lockManager.Close()
	executor.QueryExecutor.SetLockManager(lockManager)
	require.NoError(t, lockManager.AcquireLock(11, 1, 2, 3, manager.LOCK_X))
	require.Error(t, lockManager.AcquireLock(22, 1, 2, 3, manager.LOCK_X))

	waits := <-executor.ExecuteQuery(nil, "select requesting_trx_id, requested_lock_id, blocking_trx_id, blocking_lock_id from information_schema.innodb_lock_waits", "")
	require.NoError(t, waits.Err)
	rows, ok := waits.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, rows.Records, 1)
	require.Equal(t, int64(22), rows.Records[0].GetValues()[0].Int())
	require.Equal(t, int64(11), rows.Records[0].GetValues()[2].Int())

	locks := <-executor.ExecuteQuery(nil, "select lock_trx_id, lock_mode, lock_type from information_schema.innodb_locks", "")
	require.NoError(t, locks.Err)
	lockRows, ok := locks.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, lockRows.Records, 2)

	wrongRequestedLock := <-executor.ExecuteQuery(nil, "select requested_lock_id from information_schema.innodb_lock_waits where requested_lock_id = 'wrong-lock'", "")
	require.NoError(t, wrongRequestedLock.Err)
	require.Empty(t, wrongRequestedLock.Data.(*SelectResult).Records)
	wrongBlockingLock := <-executor.ExecuteQuery(nil, "select blocking_lock_id from information_schema.innodb_lock_waits where blocking_lock_id = 'wrong-lock'", "")
	require.NoError(t, wrongBlockingLock.Err)
	require.Empty(t, wrongBlockingLock.Data.(*SelectResult).Records)
	wrongLockID := <-executor.ExecuteQuery(nil, "select lock_id from information_schema.innodb_locks where lock_id = 'wrong-lock'", "")
	require.NoError(t, wrongLockID.Err)
	require.Empty(t, wrongLockID.Data.(*SelectResult).Records)
	wrongLockMode := <-executor.ExecuteQuery(nil, "select lock_mode from information_schema.innodb_locks where lock_mode = 'S'", "")
	require.NoError(t, wrongLockMode.Err)
	require.Empty(t, wrongLockMode.Data.(*SelectResult).Records)
	wrongLockType := <-executor.ExecuteQuery(nil, "select lock_type from information_schema.innodb_locks where lock_type = 'TABLE'", "")
	require.NoError(t, wrongLockType.Err)
	require.Empty(t, wrongLockType.Data.(*SelectResult).Records)
	wrongLockTable := <-executor.ExecuteQuery(nil, "select lock_table from information_schema.innodb_locks where lock_table = 'other'", "")
	require.NoError(t, wrongLockTable.Err)
	require.Empty(t, wrongLockTable.Data.(*SelectResult).Records)
}

func TestInformationSchemaInnoDBMetricsReportsLiveStateAndFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	txManager, err := executor.QueryExecutor.getTransactionManager()
	require.NoError(t, err)
	trx, err := txManager.Begin(false, 2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, txManager.Rollback(trx)) })

	result := <-executor.ExecuteQuery(nil, "select name, subsystem, count, status, type from information_schema.innodb_metrics where name = 'trx_active_transactions'", "")
	require.NoError(t, result.Err)
	rows, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, rows.Records, 1)
	values := rows.Records[0].GetValues()
	require.Equal(t, "trx_active_transactions", values[0].String())
	require.Equal(t, int64(1), values[2].Int())
	require.Equal(t, "enabled", values[3].String())
	require.Equal(t, "gauge", values[4].String())

	filtered := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_metrics where name like 'lock_wait%'", "")
	require.NoError(t, filtered.Err)
	filteredRows, ok := filtered.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, filteredRows.Records, 2)
	wrongSubsystem := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_metrics where subsystem = 'replication'", "")
	require.NoError(t, wrongSubsystem.Err)
	require.Empty(t, wrongSubsystem.Data.(*SelectResult).Records)
	wrongType := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_metrics where type = 'counter'", "")
	require.NoError(t, wrongType.Err)
	require.Empty(t, wrongType.Data.(*SelectResult).Records)
	wrongCount := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_metrics where name = 'trx_active_transactions' and count = 999", "")
	require.NoError(t, wrongCount.Err)
	require.Empty(t, wrongCount.Data.(*SelectResult).Records)
}
