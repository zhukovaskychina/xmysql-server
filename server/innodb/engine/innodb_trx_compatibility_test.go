package engine

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	observabilitymetrics "github.com/zhukovaskychina/xmysql-server/server/observability/metrics"
)

func TestInformationSchemaInnoDBProcessTablesRequireProcessPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	session.SetParamByName("user", "reporter")
	session.SetParamByName("global_privileges", []common.PrivilegeType{})

	for _, table := range []string{"innodb_trx", "innodb_metrics", "innodb_buffer_pool_stats", "innodb_buffer_page", "innodb_buffer_page_lru", "innodb_cached_indexes", "innodb_cmp", "innodb_cmp_per_index", "innodb_cmpmem", "innodb_tablespaces", "innodb_tablespaces_brief", "innodb_datafiles", "innodb_tables", "innodb_session_temp_tablespaces", "innodb_fields", "innodb_columns", "innodb_indexes", "innodb_foreign", "innodb_foreign_cols", "innodb_tablestats", "innodb_temp_table_info", "innodb_virtual", "files"} {
		result := <-executor.ExecuteQuery(session, "select * from information_schema."+table, "")
		require.Error(t, result.Err, table)
		require.Contains(t, result.Err.Error(), "PROCESS", table)
	}
}

func TestInformationSchemaInnoDBTrxProjectsActiveTransactionSnapshot(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	txManager, err := executor.QueryExecutor.getTransactionManager()
	require.NoError(t, err)
	trx, err := txManager.Begin(false, 2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, txManager.Rollback(trx)) })

	result := <-executor.ExecuteQuery(nil, "select trx_id, trx_state, trx_started, trx_is_read_only, trx_isolation_level, trx_adaptive_hash_latched, trx_adaptive_hash_timeout, trx_autocommit_non_locking, trx_schedule_weight from information_schema.innodb_trx", "")
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
			require.Equal(t, int64(0), values[5].Int())
			require.Equal(t, int64(0), values[6].Int())
			require.Equal(t, int64(0), values[7].Int())
			require.True(t, values[8].IsNull())
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

func TestInformationSchemaInnoDBTrxProjectsHeldLockCounts(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	txManager, err := executor.QueryExecutor.getTransactionManager()
	require.NoError(t, err)
	trx, err := txManager.Begin(false, manager.TRX_ISO_REPEATABLE_READ)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, txManager.Rollback(trx)) })

	lockManager := manager.NewLockManager()
	defer lockManager.Close()
	executor.QueryExecutor.SetLockManager(lockManager)
	require.NoError(t, lockManager.AcquireLock(uint64(trx.ID), 7, 1, 11, manager.LOCK_X))
	require.NoError(t, lockManager.AcquireLock(uint64(trx.ID), 7, 1, 12, manager.LOCK_X))
	require.NoError(t, lockManager.AcquireLock(uint64(trx.ID), 8, 1, 13, manager.LOCK_X))

	result := <-executor.ExecuteQuery(nil, "select trx_id, trx_tables_locked, trx_rows_locked from information_schema.innodb_trx", "")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	var found []interface{}
	for _, record := range rows.Records {
		values := record.GetValues()
		if values[0].Int() == trx.ID {
			found = []interface{}{values[1].Int(), values[2].Int()}
			break
		}
	}
	require.Equal(t, []interface{}{int64(2), int64(3)}, found)
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

func TestInformationSchemaInnoDBLocksIncludesGrantedLocksWithoutWaitEdge(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	lockManager := manager.NewLockManager()
	defer lockManager.Close()
	executor.QueryExecutor.SetLockManager(lockManager)
	require.NoError(t, lockManager.AcquireLock(31, 7, 1, 11, manager.LOCK_X))

	result := <-executor.ExecuteQuery(nil, "select lock_id, lock_trx_id, lock_mode, lock_type, lock_table, lock_page, lock_rec from information_schema.innodb_locks", "")
	require.NoError(t, result.Err)
	rows, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, rows.Records, 1)
	values := rows.Records[0].GetValues()
	require.Equal(t, "7_1_11:31", values[0].String())
	require.Equal(t, int64(31), values[1].Int())
	require.Equal(t, "X", values[2].String())
	require.Equal(t, "RECORD", values[3].String())
	require.Equal(t, "7_1_11", values[4].String())
	require.Equal(t, int64(1), values[5].Int())
	require.Equal(t, int64(11), values[6].Int())
}

func TestInformationSchemaInnoDBMetricsReportsLiveStateAndFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	txManager, err := executor.QueryExecutor.getTransactionManager()
	require.NoError(t, err)
	lockManager := manager.NewLockManager()
	defer lockManager.Close()
	executor.QueryExecutor.SetLockManager(lockManager)
	require.NoError(t, lockManager.AcquireLock(11, 1, 2, 3, manager.LOCK_X))
	require.Error(t, lockManager.AcquireLock(22, 1, 2, 3, manager.LOCK_X))
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
	require.Equal(t, int64(0), values[2].Int())
	require.Equal(t, "disabled", values[3].String())
	require.Equal(t, "counter", values[4].String())

	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_enable", "module_trx"))
	enabled := <-executor.ExecuteQuery(nil, "select name, count, status, type from information_schema.innodb_metrics where name = 'trx_active_transactions'", "")
	require.NoError(t, enabled.Err)
	enabledValues := enabled.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, int64(1), enabledValues[1].Int())
	require.Equal(t, "enabled", enabledValues[2].String())
	require.Equal(t, "counter", enabledValues[3].String())

	filtered := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_metrics where name like 'lock_wait%'", "")
	require.NoError(t, filtered.Err)
	filteredRows, ok := filtered.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, filteredRows.Records, 2)
	wrongSubsystem := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_metrics where subsystem = 'replication'", "")
	require.NoError(t, wrongSubsystem.Err)
	require.Empty(t, wrongSubsystem.Data.(*SelectResult).Records)
	wrongType := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_metrics where name = 'trx_active_transactions' and type = 'gauge'", "")
	require.NoError(t, wrongType.Err)
	require.Empty(t, wrongType.Data.(*SelectResult).Records)
	wrongCount := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_metrics where name = 'trx_active_transactions' and count = 999", "")
	require.NoError(t, wrongCount.Err)
	require.Empty(t, wrongCount.Data.(*SelectResult).Records)

	officialWaits := <-executor.ExecuteQuery(nil, "select name, count, subsystem, type from information_schema.innodb_metrics where name = 'lock_row_lock_current_waits'", "")
	require.NoError(t, officialWaits.Err)
	require.Len(t, officialWaits.Data.(*SelectResult).Records, 1)
	officialValues := officialWaits.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, int64(1), officialValues[1].Int())
	require.Equal(t, "lock", officialValues[2].String())
	require.Equal(t, "status_counter", officialValues[3].String())

	officialThreads := <-executor.ExecuteQuery(nil, "select name, count from information_schema.innodb_metrics where name = 'lock_threads_waiting'", "")
	require.NoError(t, officialThreads.Err)
	require.Len(t, officialThreads.Data.(*SelectResult).Records, 1)
	require.Equal(t, int64(1), officialThreads.Data.(*SelectResult).Records[0].GetValues()[1].Int())
}

func TestInformationSchemaInnoDBMetricsExposeStaticMySQLCatalogRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	require.GreaterOrEqual(t, len(mysql84InnoDBMetricCatalog), 300)
	result := <-executor.ExecuteQuery(nil, "select name, subsystem, count, status, type from information_schema.innodb_metrics where name in ('metadata_table_handles_opened', 'buffer_flush_batch_scanned', 'os_data_fsyncs', 'trx_rw_commits', 'purge_invoked', 'log_writes', 'index_page_splits', 'dml_system_reads', 'cpu_utime_abs')", "")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	byName := make(map[string][]basic.Value, len(rows.Records))
	for _, record := range rows.Records {
		values := record.GetValues()
		byName[values[0].String()] = values
	}
	for _, name := range []string{
		"buffer_flush_batch_scanned", "os_data_fsyncs",
		"trx_rw_commits", "purge_invoked", "log_writes", "index_page_splits",
		"dml_system_reads", "cpu_utime_abs",
	} {
		values, ok := byName[name]
		require.True(t, ok, "missing static MySQL INNODB_METRICS catalog row %s", name)
		require.Equal(t, int64(0), values[2].Int(), name)
		require.Equal(t, "disabled", values[3].String(), name)
	}
	metadataValues := byName["metadata_table_handles_opened"]
	require.NotEmpty(t, metadataValues)
	require.GreaterOrEqual(t, metadataValues[2].Int(), int64(0))
	require.Equal(t, "enabled", metadataValues[3].String())
}

func TestInformationSchemaInnoDBMetricsExposeOpenFileCount(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	queryOpenCount := func() int64 {
		result := <-executor.ExecuteQuery(nil, "select count from information_schema.innodb_metrics where name = 'file_num_open_files'", "")
		require.NoError(t, result.Err)
		require.Len(t, result.Data.(*SelectResult).Records, 1)
		return result.Data.(*SelectResult).Records[0].GetValues()[0].Int()
	}
	baseline := queryOpenCount()
	executor.QueryExecutor.metricsRecorder.RecordFileOpen("innodb_metrics_catalog.ibd")
	result := <-executor.ExecuteQuery(nil, "select name, subsystem, count, status, type from information_schema.innodb_metrics where name = 'file_num_open_files'", "")
	require.NoError(t, result.Err)
	require.Len(t, result.Data.(*SelectResult).Records, 1)
	values := result.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, "file_num_open_files", values[0].String())
	require.Equal(t, "file_system", values[1].String())
	require.Equal(t, baseline+1, values[2].Int())
	require.Equal(t, "enabled", values[3].String())
	require.Equal(t, "status_counter", values[4].String())

	executor.QueryExecutor.metricsRecorder.RecordFileClose("innodb_metrics_catalog.ibd")
	require.Equal(t, baseline, queryOpenCount())
}

func TestInformationSchemaInnoDBMetricsExposeTransactionRollbackCount(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	queryRollbackCount := func() int64 {
		result := <-executor.ExecuteQuery(nil, "select count from information_schema.innodb_metrics where name = 'trx_rollbacks'", "")
		require.NoError(t, result.Err)
		require.Len(t, result.Data.(*SelectResult).Records, 1)
		return result.Data.(*SelectResult).Records[0].GetValues()[0].Int()
	}
	baseline := queryRollbackCount()
	executor.QueryExecutor.metricsRecorder.RecordTransactionRollback("session", "compatibility-test")

	result := <-executor.ExecuteQuery(nil, "select name, subsystem, count, status, type from information_schema.innodb_metrics where name = 'trx_rollbacks'", "")
	require.NoError(t, result.Err)
	require.Len(t, result.Data.(*SelectResult).Records, 1)
	values := result.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, "trx_rollbacks", values[0].String())
	require.Equal(t, "transaction", values[1].String())
	require.Equal(t, baseline+1, values[2].Int())
	require.Equal(t, "enabled", values[3].String())
	require.Equal(t, "counter", values[4].String())
}

func TestInformationSchemaInnoDBMetricsExposeRedoLogFsyncCount(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	queryFsyncCount := func() int64 {
		result := <-executor.ExecuteQuery(nil, "select count from information_schema.innodb_metrics where name = 'os_log_fsyncs'", "")
		require.NoError(t, result.Err)
		require.Len(t, result.Data.(*SelectResult).Records, 1)
		return result.Data.(*SelectResult).Records[0].GetValues()[0].Int()
	}
	redo := executor.QueryExecutor.txManager.GetRedoLogManager()
	require.NotNil(t, redo)
	before := queryFsyncCount()
	completed := make(chan error, 1)
	redo.FlushAsync(0, func(err error) { completed <- err })
	require.NoError(t, <-completed)

	stats := redo.GetGroupCommitStats()
	require.NotNil(t, stats)
	require.Greater(t, stats.TotalFsyncs, uint64(before))
	result := <-executor.ExecuteQuery(nil, "select name, subsystem, count, status, type from information_schema.innodb_metrics where name = 'os_log_fsyncs'", "")
	require.NoError(t, result.Err)
	require.Len(t, result.Data.(*SelectResult).Records, 1)
	values := result.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, "os_log_fsyncs", values[0].String())
	require.Equal(t, "os", values[1].String())
	require.Equal(t, int64(stats.TotalFsyncs), values[2].Int())
	require.Equal(t, "enabled", values[3].String())
	require.Equal(t, "status_counter", values[4].String())
}

func TestInformationSchemaInnoDBMetricsExposeMetadataTableHandleLifecycle(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	session.SetParamByName("connection_id", int64(501))
	session.SessionContext().SetConnectionID(501)
	observer := newTestMySQLSession()
	observer.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	observer.SetParamByName("connection_id", int64(502))
	observer.SessionContext().SetConnectionID(502)
	mustExecSessionSQL(t, executor, session, "", "create database metadata_metrics")
	mustExecSessionSQL(t, executor, session, "metadata_metrics", "create table handles (id int primary key)")
	mustExecSessionSQL(t, executor, session, "metadata_metrics", "commit")
	queryMetrics := func() (int64, int64, int64, int64, int64, int64) {
		result := <-executor.ExecuteQuery(observer, "select name, count from information_schema.innodb_metrics", "metadata_metrics")
		require.NoError(t, result.Err)
		values := map[string]int64{}
		for _, record := range result.Data.(*SelectResult).Records {
			row := record.GetValues()
			values[row[0].String()] = row[1].Int()
		}
		require.Contains(t, values, "metadata_table_handles_opened")
		require.Contains(t, values, "metadata_table_handles_closed")
		require.Contains(t, values, "metadata_table_reference_count")
		require.Contains(t, values, "lock_table_lock_created")
		require.Contains(t, values, "lock_table_lock_removed")
		require.Contains(t, values, "lock_table_locks")
		return values["metadata_table_handles_opened"], values["metadata_table_handles_closed"], values["metadata_table_reference_count"], values["lock_table_lock_created"], values["lock_table_lock_removed"], values["lock_table_locks"]
	}
	openedBaseline, closedBaseline, referencesBaseline, createdBaseline, removedBaseline, locksBaseline := queryMetrics()

	mustExecSessionSQL(t, executor, session, "metadata_metrics", "lock tables handles read")
	openedAfterLock, closedAfterLock, referencesAfterLock, createdAfterLock, removedAfterLock, locksAfterLock := queryMetrics()
	require.Equal(t, openedBaseline+2, openedAfterLock)
	require.Equal(t, closedBaseline+1, closedAfterLock)
	require.Equal(t, referencesBaseline+1, referencesAfterLock)
	require.Equal(t, createdBaseline+2, createdAfterLock)
	require.Equal(t, removedBaseline+1, removedAfterLock)
	require.Equal(t, locksBaseline+1, locksAfterLock)

	mustExecSessionSQL(t, executor, session, "metadata_metrics", "unlock tables")
	openedAfterUnlock, closedAfterUnlock, referencesAfterUnlock, createdAfterUnlock, removedAfterUnlock, locksAfterUnlock := queryMetrics()
	require.Equal(t, openedAfterLock+1, openedAfterUnlock)
	require.Equal(t, closedAfterLock+2, closedAfterUnlock)
	require.Equal(t, referencesBaseline, referencesAfterUnlock)
	require.Equal(t, createdAfterLock+1, createdAfterUnlock)
	require.Equal(t, removedAfterLock+2, removedAfterUnlock)
	require.Equal(t, locksBaseline, locksAfterUnlock)
}

func TestInformationSchemaInnoDBMetricsExposeRedoLSNState(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	disabled := <-executor.ExecuteQuery(nil, "select name, count, status, type from information_schema.innodb_metrics where name = 'log_lsn_current'", "")
	require.NoError(t, disabled.Err)
	disabledRows, ok := disabled.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, disabledRows.Records, 1)
	require.Equal(t, int64(0), disabledRows.Records[0].GetValues()[1].Int())
	require.Equal(t, "disabled", disabledRows.Records[0].GetValues()[2].String())
	require.Equal(t, "value", disabledRows.Records[0].GetValues()[3].String())

	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_enable", "module_log"))

	current := <-executor.ExecuteQuery(nil, "select name, count, status, type from information_schema.innodb_metrics where name = 'log_lsn_current'", "")
	require.NoError(t, current.Err)
	currentValues := current.Data.(*SelectResult).Records[0].GetValues()
	require.GreaterOrEqual(t, currentValues[1].Int(), int64(1))
	require.Equal(t, "enabled", currentValues[2].String())

	checkpoint := <-executor.ExecuteQuery(nil, "select name, count, status, type from information_schema.innodb_metrics where name = 'log_lsn_last_checkpoint'", "")
	require.NoError(t, checkpoint.Err)
	checkpointValues := checkpoint.Data.(*SelectResult).Records[0].GetValues()
	require.GreaterOrEqual(t, checkpointValues[1].Int(), int64(0))
	require.Equal(t, "enabled", checkpointValues[2].String())
	require.Equal(t, "value", checkpointValues[3].String())

	age := <-executor.ExecuteQuery(nil, "select name, count, status, type from information_schema.innodb_metrics where name = 'log_lsn_checkpoint_age'", "")
	require.NoError(t, age.Err)
	ageValues := age.Data.(*SelectResult).Records[0].GetValues()
	require.GreaterOrEqual(t, ageValues[1].Int(), int64(0))
	require.Equal(t, "enabled", ageValues[2].String())
	require.Equal(t, "value", ageValues[3].String())
}

func TestInformationSchemaInnoDBMetricsExposeCompletedLockWaitCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	lockManager := manager.NewLockManager()
	defer lockManager.Close()
	executor.QueryExecutor.SetLockManager(lockManager)
	require.NoError(t, lockManager.AcquireLock(11, 1, 2, 3, manager.LOCK_X))
	require.Error(t, lockManager.AcquireLock(22, 1, 2, 3, manager.LOCK_X))
	lockManager.ReleaseLocks(11)

	result := <-executor.ExecuteQuery(nil, "select name, subsystem, count, type from information_schema.innodb_metrics where name like 'lock_row_lock_%'", "")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	found := make(map[string][]basic.Value)
	for _, record := range rows.Records {
		values := record.GetValues()
		found[values[0].String()] = values
	}
	for _, name := range []string{"lock_row_lock_waits", "lock_row_lock_time", "lock_row_lock_time_avg", "lock_row_lock_time_max"} {
		require.Contains(t, found, name)
		require.Equal(t, "lock", found[name][1].String())
		require.Equal(t, "status_counter", found[name][3].String())
	}
	require.Equal(t, int64(1), found["lock_row_lock_waits"][2].Int())

	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_disable", "module_lock"))
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_reset_all", "module_lock"))
	reset := <-executor.ExecuteQuery(nil, "select name, count, status from information_schema.innodb_metrics where name = 'lock_row_lock_waits'", "")
	require.NoError(t, reset.Err)
	resetValues := reset.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, int64(0), resetValues[1].Int())
	require.Equal(t, "disabled", resetValues[2].String())
}

func TestInformationSchemaInnoDBMetricsLockSummaryResetProjectsResetColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	lockManager := manager.NewLockManager()
	defer lockManager.Close()
	executor.QueryExecutor.SetLockManager(lockManager)

	recordWait := func(owner, waiter uint64, resource uint32) {
		require.NoError(t, lockManager.AcquireLock(owner, 1, resource, 1, manager.LOCK_X))
		require.Error(t, lockManager.AcquireLock(waiter, 1, resource, 1, manager.LOCK_X))
		lockManager.ReleaseLocks(owner)
	}
	recordWait(101, 102, 11)

	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_reset", "module_lock"))

	reset := <-executor.ExecuteQuery(nil, "select count, count_reset, max_count_reset, min_count_reset, avg_count_reset, time_reset from information_schema.innodb_metrics where name = 'lock_row_lock_waits'", "")
	require.NoError(t, reset.Err)
	resetValues := reset.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, int64(1), resetValues[0].Int(), "COUNT remains cumulative after reset")
	require.Equal(t, int64(0), resetValues[1].Int(), "COUNT_RESET starts at zero after reset")
	require.True(t, resetValues[2].IsNull())
	require.True(t, resetValues[3].IsNull())
	require.True(t, resetValues[4].IsNull())
	require.False(t, resetValues[5].IsNull())

	recordWait(103, 104, 12)
	updated := <-executor.ExecuteQuery(nil, "select count, count_reset, max_count_reset, min_count_reset, avg_count_reset from information_schema.innodb_metrics where name = 'lock_row_lock_waits'", "")
	require.NoError(t, updated.Err)
	updatedValues := updated.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, int64(2), updatedValues[0].Int())
	require.Equal(t, int64(1), updatedValues[1].Int())
	require.Equal(t, int64(1), updatedValues[2].Int())
	require.Equal(t, int64(1), updatedValues[3].Int())
	require.Equal(t, float64(1), updatedValues[4].Float64())
}

func TestInformationSchemaInnoDBMetricsExposeDMLRowCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	createDatabase := <-executor.ExecuteQuery(nil, "create database metricsdb", "")
	require.NoError(t, createDatabase.Err)
	for _, query := range []string{
		"create table metrics_dml (id int primary key, value int)",
		"insert into metrics_dml values (1, 10), (2, 20), (3, 30)",
		"update metrics_dml set value = value + 1 where id <= 2",
		"delete from metrics_dml where id = 3",
	} {
		result := <-executor.ExecuteQuery(nil, query, "metricsdb")
		require.NoError(t, result.Err, query)
	}

	result := <-executor.ExecuteQuery(nil, "select name, subsystem, count, status, type from information_schema.innodb_metrics where name like 'dml_%'", "metricsdb")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	byName := make(map[string][]basic.Value)
	for _, record := range rows.Records {
		values := record.GetValues()
		byName[values[0].String()] = values
	}

	for _, name := range []string{"dml_inserts", "dml_updates", "dml_deletes"} {
		values, ok := byName[name]
		require.True(t, ok, "missing source-backed DML metric %s", name)
		require.Equal(t, "dml", values[1].String())
		require.Equal(t, "enabled", values[3].String())
		require.Equal(t, "status_counter", values[4].String())
	}
	require.Equal(t, int64(3), byName["dml_inserts"][2].Int())
	require.Equal(t, int64(2), byName["dml_updates"][2].Int())
	require.Equal(t, int64(1), byName["dml_deletes"][2].Int())
}

func TestInformationSchemaInnoDBMetricsExposeDMLReadCounter(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	createDatabase := <-executor.ExecuteQuery(nil, "create database metrics_reads", "")
	require.NoError(t, createDatabase.Err)
	for _, query := range []string{
		"create table metrics_read (id int primary key)",
		"insert into metrics_read values (1), (2), (3)",
	} {
		result := <-executor.ExecuteQuery(nil, query, "metrics_reads")
		require.NoError(t, result.Err, query)
	}

	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_enable", "module_dml"))

	result := <-executor.ExecuteQuery(nil, "select id from metrics_read", "metrics_reads")
	require.NoError(t, result.Err)
	require.Len(t, result.Data.(*SelectResult).Records, 3)

	metric := <-executor.ExecuteQuery(nil, "select name, subsystem, count, status, type from information_schema.innodb_metrics where name = 'dml_reads'", "metrics_reads")
	require.NoError(t, metric.Err)
	values := metric.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, "dml_reads", values[0].String())
	require.Equal(t, "dml", values[1].String())
	require.Equal(t, int64(3), values[2].Int())
	require.Equal(t, "enabled", values[3].String())
	require.Equal(t, "status_counter", values[4].String())
}

func TestInformationSchemaInnoDBMetricsDMLLifecycleAndReset(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	createDatabase := <-executor.ExecuteQuery(nil, "create database metrics_lifecycle", "")
	require.NoError(t, createDatabase.Err)
	for _, query := range []string{
		"create table metrics_dml (id int primary key)",
		"insert into metrics_dml values (1), (2)",
	} {
		result := <-executor.ExecuteQuery(nil, query, "metrics_lifecycle")
		require.NoError(t, result.Err, query)
	}

	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	queryMetric := func(query string) []basic.Value {
		result := <-executor.ExecuteQuery(nil, query, "metrics_lifecycle")
		require.NoError(t, result.Err)
		rows := result.Data.(*SelectResult).Records
		require.Len(t, rows, 1)
		return rows[0].GetValues()
	}

	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_reset", "dml_inserts"))
	reset := queryMetric("select count, max_count, count_reset, max_count_reset from information_schema.innodb_metrics where name = 'dml_inserts'")
	require.Equal(t, int64(2), reset[0].Int())
	require.Equal(t, int64(2), reset[1].Int())
	require.Equal(t, int64(0), reset[2].Int())
	require.True(t, reset[3].IsNull())

	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_disable", "dml_inserts"))
	insertAfterDisable := <-executor.ExecuteQuery(nil, "insert into metrics_dml values (3)", "metrics_lifecycle")
	require.NoError(t, insertAfterDisable.Err)
	disabled := queryMetric("select count, status from information_schema.innodb_metrics where name = 'dml_inserts'")
	require.Equal(t, int64(2), disabled[0].Int())
	require.Equal(t, "disabled", disabled[1].String())

	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_reset_all", "dml_inserts"))
	cleared := queryMetric("select count, max_count, min_count, avg_count, count_reset, status from information_schema.innodb_metrics where name = 'dml_inserts'")
	require.Equal(t, int64(0), cleared[0].Int())
	require.True(t, cleared[1].IsNull())
	require.True(t, cleared[2].IsNull())
	require.True(t, cleared[3].IsNull())
	require.Equal(t, int64(0), cleared[4].Int())
	require.Equal(t, "disabled", cleared[5].String())
}

func TestInformationSchemaInnoDBMetricsExposeBufferPoolRuntimeCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "select name, count, type from information_schema.innodb_metrics where name like 'buffer_pool_pages_%'", "")
	require.NoError(t, result.Err)
	rows, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.NotEmpty(t, rows.Records)

	found := make(map[string]bool)
	for _, record := range rows.Records {
		values := record.GetValues()
		require.Len(t, values, 3)
		found[values[0].String()] = true
		require.Equal(t, "value", values[2].String())
	}
	for _, name := range []string{"buffer_pool_pages_total", "buffer_pool_pages_data", "buffer_pool_pages_dirty", "buffer_pool_pages_misc"} {
		require.True(t, found[name], "missing live buffer-pool metric %s", name)
	}
	result = <-executor.ExecuteQuery(nil, "select name, count from information_schema.innodb_metrics where name in ('buffer_pool_pages_total', 'buffer_pool_pages_free', 'buffer_pool_pages_data', 'buffer_pool_pages_misc')", "")
	require.NoError(t, result.Err)
	byName := make(map[string]int64)
	for _, record := range result.Data.(*SelectResult).Records {
		values := record.GetValues()
		byName[values[0].String()] = values[1].Int()
	}
	require.Equal(t, byName["buffer_pool_pages_total"]-byName["buffer_pool_pages_free"]-byName["buffer_pool_pages_data"], byName["buffer_pool_pages_misc"])
}

func TestInformationSchemaInnoDBMetricsExposeBufferPoolByteAndSizeValues(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "select name, subsystem, count, type from information_schema.innodb_metrics where name like 'buffer_pool_%'", "")
	require.NoError(t, result.Err)
	rows, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	found := make(map[string][]basic.Value)
	for _, record := range rows.Records {
		values := record.GetValues()
		found[values[0].String()] = values
	}
	require.Contains(t, found, "buffer_pool_bytes_data")
	require.Contains(t, found, "buffer_pool_bytes_dirty")
	require.Contains(t, found, "buffer_pool_size")
	require.Equal(t, "buffer", found["buffer_pool_bytes_data"][1].String())
	require.Equal(t, "buffer", found["buffer_pool_bytes_dirty"][1].String())
	require.Equal(t, "server", found["buffer_pool_size"][1].String())
	require.Equal(t, "value", found["buffer_pool_bytes_data"][3].String())
	require.Equal(t, "value", found["buffer_pool_bytes_dirty"][3].String())
	require.Equal(t, "value", found["buffer_pool_size"][3].String())
	require.Equal(t, found["buffer_pool_pages_data"][2].Int()*16384, found["buffer_pool_bytes_data"][2].Int())
	require.Equal(t, found["buffer_pool_pages_dirty"][2].Int()*16384, found["buffer_pool_bytes_dirty"][2].Int())
	require.Equal(t, found["buffer_pool_pages_total"][2].Int()*16384, found["buffer_pool_size"][2].Int())
	require.Greater(t, found["buffer_pool_size"][2].Int(), int64(0))
}

func TestInformationSchemaInnoDBMetricsExposeBufferPageCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	prefetcher, ok := executor.QueryExecutor.bufferPoolManager.(interface {
		PrefetchPage(uint32, uint32)
	})
	require.True(t, ok, "the live buffer-pool manager must expose read-ahead scheduling")
	prefetcher.PrefetchPage(0, 0)
	require.Eventually(t, func() bool {
		result := <-executor.ExecuteQuery(nil, "select count from information_schema.innodb_metrics where name = 'buffer_pages_read'", "")
		if result.Err != nil || len(result.Data.(*SelectResult).Records) != 1 {
			return false
		}
		return result.Data.(*SelectResult).Records[0].GetValues()[0].Int() > 0
	}, 2*time.Second, 20*time.Millisecond)

	result := <-executor.ExecuteQuery(nil, "select name, subsystem, count, type from information_schema.innodb_metrics where name in ('buffer_data_reads', 'buffer_data_written', 'buffer_pages_read', 'buffer_pages_written', 'os_data_reads', 'os_data_writes', 'innodb_page_size')", "")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	found := make(map[string][]basic.Value)
	for _, record := range rows.Records {
		values := record.GetValues()
		found[values[0].String()] = values
	}

	require.Contains(t, found, "buffer_pages_read")
	require.Contains(t, found, "buffer_pages_written")
	require.Contains(t, found, "buffer_data_reads")
	require.Contains(t, found, "buffer_data_written")
	require.Contains(t, found, "os_data_reads")
	require.Contains(t, found, "os_data_writes")
	require.Contains(t, found, "innodb_page_size")
	require.Equal(t, "buffer", found["buffer_data_reads"][1].String())
	require.Equal(t, "buffer", found["buffer_data_written"][1].String())
	require.Equal(t, "status_counter", found["buffer_data_reads"][3].String())
	require.Equal(t, "status_counter", found["buffer_data_written"][3].String())
	require.Equal(t, "os", found["os_data_reads"][1].String())
	require.Equal(t, "os", found["os_data_writes"][1].String())
	require.Equal(t, "status_counter", found["os_data_reads"][3].String())
	require.Equal(t, "status_counter", found["os_data_writes"][3].String())
	require.Equal(t, "buffer", found["buffer_pages_read"][1].String())
	require.Equal(t, "buffer", found["buffer_pages_written"][1].String())
	require.Equal(t, "status_counter", found["buffer_pages_read"][3].String())
	require.Equal(t, "status_counter", found["buffer_pages_written"][3].String())
	require.Equal(t, "server", found["innodb_page_size"][1].String())
	require.Equal(t, "value", found["innodb_page_size"][3].String())
	require.Greater(t, found["innodb_page_size"][2].Int(), int64(0))
	require.Equal(t, found["buffer_pages_read"][2].Int()*found["innodb_page_size"][2].Int(), found["buffer_data_reads"][2].Int())
	require.Equal(t, found["buffer_pages_written"][2].Int()*found["innodb_page_size"][2].Int(), found["buffer_data_written"][2].Int())
	require.Equal(t, found["buffer_pages_read"][2].Int(), found["os_data_reads"][2].Int())
	require.Equal(t, found["buffer_pages_written"][2].Int(), found["os_data_writes"][2].Int())
}

func TestInformationSchemaInnoDBMetricsExposeCompletedReadAheadPages(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	prefetcher, ok := executor.QueryExecutor.bufferPoolManager.(interface {
		PrefetchPage(uint32, uint32)
	})
	require.True(t, ok, "the live buffer-pool manager must expose read-ahead scheduling")
	prefetcher.PrefetchPage(0, 0)

	require.Eventually(t, func() bool {
		result := <-executor.ExecuteQuery(nil, "select count from information_schema.innodb_metrics where name = 'buffer_pool_read_ahead'", "")
		if result.Err != nil || len(result.Data.(*SelectResult).Records) != 1 {
			return false
		}
		return result.Data.(*SelectResult).Records[0].GetValues()[0].Int() > 0
	}, 2*time.Second, 20*time.Millisecond)
}

func TestInformationSchemaInnoDBBufferPoolStatsExposeReadAheadCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	prefetcher, ok := executor.QueryExecutor.bufferPoolManager.(interface {
		PrefetchPage(uint32, uint32)
	})
	require.True(t, ok, "the live buffer-pool manager must expose read-ahead scheduling")
	prefetcher.PrefetchPage(0, 0)

	require.Eventually(t, func() bool {
		result := <-executor.ExecuteQuery(nil, "select number_pages_read_ahead, number_read_ahead_evicted from information_schema.innodb_buffer_pool_stats", "")
		if result.Err != nil || len(result.Data.(*SelectResult).Records) != 1 {
			return false
		}
		values := result.Data.(*SelectResult).Records[0].GetValues()
		return values[0].Int() > 0 && values[1].Int() >= 0
	}, 2*time.Second, 20*time.Millisecond)
}

func TestInformationSchemaInnoDBBufferPoolStatsExposeCreatedPageRate(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	allocator, ok := executor.QueryExecutor.bufferPoolManager.(interface {
		AllocatePage(uint32) (*buffer_pool.BufferPage, error)
	})
	require.True(t, ok, "the live buffer-pool manager must expose page allocation")
	_, err := allocator.AllocatePage(0)
	require.NoError(t, err)

	result := <-executor.ExecuteQuery(nil, "select number_pages_created, pages_create_rate from information_schema.innodb_buffer_pool_stats", "")
	require.NoError(t, result.Err)
	values := result.Data.(*SelectResult).Records[0].GetValues()
	require.Greater(t, values[0].Int(), int64(0))
	require.GreaterOrEqual(t, values[1].Float64(), float64(0))
}

func TestInformationSchemaInnoDBBufferPoolStatsExposeYoungPromotionCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	bufferPool, ok := executor.QueryExecutor.bufferPoolManager.(interface {
		GetPage(uint32, uint32) (*buffer_pool.BufferPage, error)
		AllocatePage(uint32) (*buffer_pool.BufferPage, error)
		SnapshotPages() []buffer_pool.PageSnapshot
	})
	require.True(t, ok, "the live buffer-pool manager must expose page snapshots")
	for pageNo := 0; pageNo < 520; pageNo++ {
		_, err := bufferPool.AllocatePage(0)
		require.NoError(t, err)
	}
	var oldPage buffer_pool.PageSnapshot
	for _, snapshot := range bufferPool.SnapshotPages() {
		if snapshot.IsOld {
			oldPage = snapshot
			break
		}
	}
	require.NotZero(t, oldPage.PageNo, "expected an old-list page after cache reorganization")
	for i := 0; i < 4; i++ {
		_, err := bufferPool.GetPage(oldPage.SpaceID, oldPage.PageNo)
		require.NoError(t, err)
	}

	result := <-executor.ExecuteQuery(nil, "select pages_made_young, pages_not_made_young, pages_made_young_rate, pages_not_made_young_rate, young_make_per_thousand_gets, not_young_make_per_thousand_gets from information_schema.innodb_buffer_pool_stats", "")
	require.NoError(t, result.Err)
	values := result.Data.(*SelectResult).Records[0].GetValues()
	require.Greater(t, values[0].Int(), int64(0))
	require.GreaterOrEqual(t, values[1].Int(), int64(0))
	require.GreaterOrEqual(t, values[2].Float64(), float64(0))
	require.GreaterOrEqual(t, values[3].Float64(), float64(0))
	require.GreaterOrEqual(t, values[4].Int(), int64(0))
	require.GreaterOrEqual(t, values[5].Int(), int64(0))
}

func TestInformationSchemaInnoDBBufferPoolStatsExposePendingFlushList(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	bufferPool, ok := executor.QueryExecutor.bufferPoolManager.(interface {
		GetDirtyPage(uint32, uint32) (*buffer_pool.BufferPage, error)
	})
	require.True(t, ok, "the live buffer-pool manager must expose dirty-page registration")
	_, err := bufferPool.GetDirtyPage(0, 1)
	require.NoError(t, err)

	result := <-executor.ExecuteQuery(nil, "select pending_reads, pending_flush_lru, pending_flush_list from information_schema.innodb_buffer_pool_stats", "")
	require.NoError(t, result.Err)
	values := result.Data.(*SelectResult).Records[0].GetValues()
	require.GreaterOrEqual(t, values[0].Int(), int64(0))
	require.GreaterOrEqual(t, values[1].Int(), int64(0))
	require.Equal(t, int64(1), values[2].Int())
}

func TestInformationSchemaInnoDBBufferPoolStatsExposePendingDecompress(t *testing.T) {
	executor := NewXMySQLEngine(&conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbCompression:    conf.InnodbCompressionConfig{Enabled: true, Method: "zlib", AllSpaces: true},
	})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	compression := executor.GetStorageManager().GetCompressionManager()
	require.NotNil(t, compression)
	compression.SetCompressionSettings(7, &manager.CompressionSettings{
		SpaceID: 7, Method: manager.COMPRESSION_ZLIB, Level: manager.COMPRESSION_LEVEL_DEFAULT, MinSavings: 0,
	})
	compressed, err := compression.CompressPage(7, 1, bytes.Repeat([]byte("pending-decompress"), 1024))
	require.NoError(t, err)
	started := make(chan struct{})
	release := make(chan struct{})
	compressionHook := func() {
		close(started)
		<-release
	}
	compression.SetDecompressStartedHookForTest(compressionHook)
	decompressDone := make(chan error, 1)
	go func() {
		_, decompressErr := compression.DecompressPage(7, 1, compressed)
		decompressDone <- decompressErr
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("decompression did not start")
	}
	result := mustSelectResultSQL(t, executor, "", "select pending_decompress from information_schema.innodb_buffer_pool_stats")
	require.Equal(t, [][]interface{}{{"1"}}, selectResultRows(result))
	close(release)
	require.NoError(t, <-decompressDone)
	compression.SetDecompressStartedHookForTest(nil)
	result = mustSelectResultSQL(t, executor, "", "select pending_decompress from information_schema.innodb_buffer_pool_stats")
	require.Equal(t, [][]interface{}{{"0"}}, selectResultRows(result))
}

func TestInformationSchemaInnoDBBufferPoolStatsExposeUncompressTotal(t *testing.T) {
	executor := NewXMySQLEngine(&conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbCompression:    conf.InnodbCompressionConfig{Enabled: true, Method: "zlib", AllSpaces: true},
	})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table compressed_buffer_stats (id int primary key, payload varchar(64))")

	storage, err := executor.GetStorageManager().GetTableStorageManager().GetTableStorageInfo("app", "compressed_buffer_stats")
	require.NoError(t, err)
	compression := executor.GetStorageManager().GetCompressionManager()
	require.NotNil(t, compression)
	compression.SetCompressionSettings(storage.SpaceID, &manager.CompressionSettings{
		SpaceID: storage.SpaceID, Method: manager.COMPRESSION_ZLIB, Level: manager.COMPRESSION_LEVEL_DEFAULT, MinSavings: 0,
	})
	compression.GetStatsAndReset()
	compressed, err := compression.CompressPage(storage.SpaceID, 1, bytes.Repeat([]byte("b"), 4096))
	require.NoError(t, err)
	_, err = compression.DecompressPage(storage.SpaceID, 1, compressed)
	require.NoError(t, err)

	result := mustSelectResultSQL(t, executor, "", "select uncompress_total, uncompress_current from information_schema.innodb_buffer_pool_stats")
	require.Equal(t, [][]interface{}{{"1", "1"}}, selectResultRows(result))
	result = mustSelectResultSQL(t, executor, "", "select uncompress_total, uncompress_current from information_schema.innodb_buffer_pool_stats")
	require.Equal(t, [][]interface{}{{"1", "0"}}, selectResultRows(result))
}

func TestInformationSchemaInnoDBBufferPoolStatsExposeLRUIOWindow(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table lru_io_stats (id int primary key, payload varchar(64))")
	storage, err := executor.GetStorageManager().GetTableStorageManager().GetTableStorageInfo("app", "lru_io_stats")
	require.NoError(t, err)
	bpm, ok := executor.QueryExecutor.bufferPoolManager.(interface {
		GetPage(uint32, uint32) (*buffer_pool.BufferPage, error)
		GetLRUIOStatsAndResetCurrent() (uint64, uint64)
		ClearCache()
	})
	require.True(t, ok, "the live buffer-pool manager must expose dedicated LRU I/O counters")
	bpm.ClearCache()
	_, _ = bpm.GetLRUIOStatsAndResetCurrent()
	_, err = bpm.GetPage(storage.SpaceID, 0)
	require.NoError(t, err)

	result := mustSelectResultSQL(t, executor, "", "select lru_io_total, lru_io_current from information_schema.innodb_buffer_pool_stats")
	values := result.Records[0].GetValues()
	require.GreaterOrEqual(t, values[0].Int(), int64(1))
	require.GreaterOrEqual(t, values[1].Int(), int64(1))

	result = mustSelectResultSQL(t, executor, "", "select lru_io_total, lru_io_current from information_schema.innodb_buffer_pool_stats")
	values = result.Records[0].GetValues()
	require.GreaterOrEqual(t, values[0].Int(), int64(1))
	require.Equal(t, int64(0), values[1].Int())
}

func TestInformationSchemaInnoDBMetricsExposeLockLifecycleCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	lockManager := manager.NewLockManager()
	executor.QueryExecutor.SetLockManager(lockManager)
	t.Cleanup(lockManager.Close)
	require.NoError(t, lockManager.AcquireLock(9001, 7, 8, 9, manager.LOCK_X))

	query := "select name, count from information_schema.innodb_metrics where name in ('lock_rec_lock_requests', 'lock_rec_lock_created', 'lock_rec_lock_removed', 'lock_rec_lock_waits', 'lock_rec_locks', 'lock_rec_release_attempts', 'lock_rec_grant_attempts', 'lock_deadlocks')"
	result := <-executor.ExecuteQuery(nil, query, "")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult).Records
	counts := make(map[string]int64, len(rows))
	for _, row := range rows {
		values := row.GetValues()
		counts[values[0].String()] = values[1].Int()
	}
	require.Equal(t, int64(1), counts["lock_rec_lock_requests"])
	require.Equal(t, int64(1), counts["lock_rec_lock_created"])
	require.Equal(t, int64(0), counts["lock_rec_lock_removed"])
	require.Equal(t, int64(1), counts["lock_rec_locks"])
	require.Equal(t, int64(0), counts["lock_rec_lock_waits"])
	require.Equal(t, int64(0), counts["lock_rec_release_attempts"])
	require.Equal(t, int64(1), counts["lock_rec_grant_attempts"])
	require.Equal(t, int64(0), counts["lock_deadlocks"])

	require.Error(t, lockManager.AcquireLock(9002, 7, 8, 9, manager.LOCK_S))
	lockManager.ReleaseLocks(9001)
	result = <-executor.ExecuteQuery(nil, query, "")
	require.NoError(t, result.Err)
	counts = make(map[string]int64, len(result.Data.(*SelectResult).Records))
	for _, row := range result.Data.(*SelectResult).Records {
		values := row.GetValues()
		counts[values[0].String()] = values[1].Int()
	}
	require.Equal(t, int64(1), counts["lock_rec_lock_removed"])
	require.Equal(t, int64(1), counts["lock_rec_locks"])
	require.Equal(t, int64(1), counts["lock_rec_lock_waits"])
	require.Equal(t, int64(1), counts["lock_rec_release_attempts"])
	require.Equal(t, int64(3), counts["lock_rec_grant_attempts"])
}

func TestInformationSchemaInnoDBMetricsExposeCompletedTableLockWaits(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	coordinator := executor.QueryExecutor.getDDLCoordinator()
	lock := coordinator.lockFor("app.users")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "metric-owner"))
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- lock.lockWithContextOwned(context.Background(), tableLockWrite, "metric-waiter")
	}()
	require.Eventually(t, func() bool {
		lock.stateMu.Lock()
		defer lock.stateMu.Unlock()
		_, waiting := lock.waiters["metric-waiter"]
		return waiting
	}, time.Second, time.Millisecond)
	lock.unlockOwned(tableLockWrite, "metric-owner")
	require.NoError(t, <-waitDone)
	lock.unlockOwned(tableLockWrite, "metric-waiter")

	result := mustSelectResultSQL(t, executor, "", "select name, count from information_schema.innodb_metrics where name = 'lock_table_lock_waits'")
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, "lock_table_lock_waits", values[0].String())
	require.Equal(t, int64(1), values[1].Int())
}

func TestInformationSchemaInnoDBMetricsExposeTableLockTimeouts(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	coordinator := executor.QueryExecutor.getDDLCoordinator()
	lock := coordinator.lockFor("app.users")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "timeout-owner"))
	before := observabilitymetrics.DefaultRuntimeRecorder().LockTimeouts()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := lock.lockWithContextOwned(ctx, tableLockWrite, "timeout-waiter")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	after := observabilitymetrics.DefaultRuntimeRecorder().LockTimeouts()
	require.Equal(t, before+1, after)
	lock.unlockOwned(tableLockWrite, "timeout-owner")

	result := mustSelectResultSQL(t, executor, "", "select name, count from information_schema.innodb_metrics where name = 'lock_timeouts'")
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, "lock_timeouts", values[0].String())
	require.GreaterOrEqual(t, values[1].Int(), after)
}

func TestInformationSchemaInnoDBMetricsExposeReadWriteCommitCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_enable", "module_trx"))
	session := newTestMySQLSession()
	readCounts := func() map[string]int64 {
		result := mustSelectResultSQL(t, executor, "", "select name, count from information_schema.innodb_metrics where name in ('trx_rw_commits', 'trx_ro_commits')")
		counts := make(map[string]int64, len(result.Records))
		for _, row := range result.Records {
			values := row.GetValues()
			counts[values[0].String()] = values[1].Int()
		}
		return counts
	}
	before := readCounts()
	mustExecSessionSQL(t, executor, session, "", "start transaction read only")
	mustExecSessionSQL(t, executor, session, "", "commit")
	afterReadOnly := readCounts()
	require.Equal(t, before["trx_rw_commits"], afterReadOnly["trx_rw_commits"])
	require.Equal(t, before["trx_ro_commits"]+1, afterReadOnly["trx_ro_commits"])
	mustExecSessionSQL(t, executor, session, "", "start transaction read write")
	mustExecSessionSQL(t, executor, session, "", "commit")
	afterReadWrite := readCounts()
	require.Equal(t, afterReadOnly["trx_rw_commits"]+1, afterReadWrite["trx_rw_commits"])
	require.Equal(t, afterReadOnly["trx_ro_commits"], afterReadWrite["trx_ro_commits"])
}

func TestInformationSchemaInnoDBMetricsExposeInsertUpdateCommitCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_enable", "module_trx"))
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table commit_metric_rows (id int primary key, value int)")
	mustExecSessionSQL(t, executor, session, "app", "insert into commit_metric_rows values (1, 10)")
	readCount := func() int64 {
		result := mustSelectResultSQL(t, executor, "", "select count from information_schema.innodb_metrics where name = 'trx_commits_insert_update'")
		require.Len(t, result.Records, 1)
		return result.Records[0].GetValues()[0].Int()
	}
	before := readCount()
	mustExecSessionSQL(t, executor, session, "app", "start transaction read write")
	mustExecSessionSQL(t, executor, session, "app", "update commit_metric_rows set value = 11 where id = 1")
	mustExecSessionSQL(t, executor, session, "app", "commit")
	require.Equal(t, before+1, readCount())
	beforeDelete := readCount()
	mustExecSessionSQL(t, executor, session, "app", "start transaction read write")
	mustExecSessionSQL(t, executor, session, "app", "delete from commit_metric_rows where id = 1")
	mustExecSessionSQL(t, executor, session, "app", "commit")
	require.Equal(t, beforeDelete, readCount())
	beforeReadOnly := readCount()
	mustExecSessionSQL(t, executor, session, "app", "start transaction read only")
	mustExecSessionSQL(t, executor, session, "app", "select * from commit_metric_rows")
	mustExecSessionSQL(t, executor, session, "app", "commit")
	require.Equal(t, beforeReadOnly, readCount())
}

func TestInformationSchemaInnoDBMetricsExposeNonLockingAutocommitReadOnlyCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_enable", "module_trx"))
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table nonlocking_read_metric_rows (id int primary key, value int)")
	mustExecSessionSQL(t, executor, session, "app", "insert into nonlocking_read_metric_rows values (1, 10)")
	readCount := func() int64 {
		result := mustSelectResultSQL(t, executor, "", "select count from information_schema.innodb_metrics where name = 'trx_nl_ro_commits'")
		require.Len(t, result.Records, 1)
		return result.Records[0].GetValues()[0].Int()
	}
	before := readCount()
	require.Len(t, mustQuerySessionSQL(t, executor, session, "app", "select value from nonlocking_read_metric_rows where id = 1"), 1)
	require.Equal(t, before+1, readCount())
	// A constant expression does not open an InnoDB table and must not be
	// reported as an InnoDB non-locking read-only transaction.
	mustQuerySessionSQL(t, executor, session, "app", "select 1")
	require.Equal(t, before+1, readCount())
	// An explicit READ ONLY transaction is a regular read-only commit, not an
	// auto-commit non-locking transaction.
	mustExecSessionSQL(t, executor, session, "app", "start transaction read only")
	mustQuerySessionSQL(t, executor, session, "app", "select value from nonlocking_read_metric_rows where id = 1")
	mustExecSessionSQL(t, executor, session, "app", "commit")
	require.Equal(t, before+1, readCount())
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_reset", "trx_nl_ro_commits"))
	reset := mustSelectResultSQL(t, executor, "", "select count, count_reset, status from information_schema.innodb_metrics where name = 'trx_nl_ro_commits'")
	require.Len(t, reset.Records, 1)
	resetValues := reset.Records[0].GetValues()
	require.Equal(t, before+1, resetValues[0].Int())
	require.Equal(t, int64(0), resetValues[1].Int())
	require.Equal(t, "enabled", resetValues[2].String())
	mustQuerySessionSQL(t, executor, session, "app", "select value from nonlocking_read_metric_rows where id = 1")
	updated := mustSelectResultSQL(t, executor, "", "select count, count_reset from information_schema.innodb_metrics where name = 'trx_nl_ro_commits'")
	require.Equal(t, before+2, updated.Records[0].GetValues()[0].Int())
	require.Equal(t, int64(1), updated.Records[0].GetValues()[1].Int())
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_disable", "trx_nl_ro_commits"))
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_reset_all", "trx_nl_ro_commits"))
	cleared := mustSelectResultSQL(t, executor, "", "select count, status from information_schema.innodb_metrics where name = 'trx_nl_ro_commits'")
	require.Equal(t, int64(0), cleared.Records[0].GetValues()[0].Int())
	require.Equal(t, "disabled", cleared.Records[0].GetValues()[1].String())
}

func TestInformationSchemaInnoDBMetricsExposeTransactionAllocationCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_enable", "module_trx"))
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table allocation_metric_rows (id int primary key, value int)")
	readCount := func() int64 {
		result := mustSelectResultSQL(t, executor, "", "select count from information_schema.innodb_metrics where name = 'trx_allocations'")
		require.Len(t, result.Records, 1)
		return result.Records[0].GetValues()[0].Int()
	}
	before := readCount()
	mustExecSessionSQL(t, executor, session, "app", "start transaction read write")
	mustExecSessionSQL(t, executor, session, "app", "insert into allocation_metric_rows values (1, 10)")
	mustExecSessionSQL(t, executor, session, "app", "commit")
	require.Equal(t, before+1, readCount())
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_reset", "trx_allocations"))
	reset := mustSelectResultSQL(t, executor, "", "select count, count_reset, status from information_schema.innodb_metrics where name = 'trx_allocations'")
	resetValues := reset.Records[0].GetValues()
	require.Equal(t, before+1, resetValues[0].Int())
	require.Equal(t, int64(0), resetValues[1].Int())
	require.Equal(t, "enabled", resetValues[2].String())
	mustExecSessionSQL(t, executor, session, "app", "start transaction read write")
	mustExecSessionSQL(t, executor, session, "app", "insert into allocation_metric_rows values (2, 20)")
	mustExecSessionSQL(t, executor, session, "app", "commit")
	updated := mustSelectResultSQL(t, executor, "", "select count, count_reset from information_schema.innodb_metrics where name = 'trx_allocations'")
	require.Equal(t, before+2, updated.Records[0].GetValues()[0].Int())
	require.Equal(t, int64(1), updated.Records[0].GetValues()[1].Int())
}

func TestInformationSchemaInnoDBMetricsExposeRedoLogBytesWritten(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	readCount := func() int64 {
		result := mustSelectResultSQL(t, executor, "", "select count from information_schema.innodb_metrics where name = 'os_log_bytes_written'")
		require.Len(t, result.Records, 1)
		return result.Records[0].GetValues()[0].Int()
	}
	before := readCount()
	redo := executor.QueryExecutor.txManager.GetRedoLogManager()
	beforeFile, err := redo.GetFileSnapshot()
	require.NoError(t, err)
	_, err = redo.Append(&manager.RedoLogEntry{TrxID: 9101, Type: manager.LOG_TYPE_INSERT, PageID: 77, Data: bytes.Repeat([]byte("redo"), 8)})
	require.NoError(t, err)
	require.NoError(t, redo.Flush(0))
	afterFile, err := redo.GetFileSnapshot()
	require.NoError(t, err)
	require.Greater(t, afterFile.SizeInBytes, beforeFile.SizeInBytes)
	require.Greater(t, readCount(), before)
	require.Equal(t, afterFile.SizeInBytes, readCount())
}

func TestInformationSchemaInnoDBMetricsExposeUndoSlotsUsed(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_enable", "module_trx"))
	readMetric := func() []basic.Value {
		result := mustSelectResultSQL(t, executor, "", "select count, status from information_schema.innodb_metrics where name = 'trx_undo_slots_used'")
		require.Len(t, result.Records, 1)
		return result.Records[0].GetValues()
	}
	before := readMetric()
	require.Equal(t, int64(0), before[0].Int())
	err := executor.QueryExecutor.txManager.GetUndoLogManager().Append(&manager.UndoLogEntry{
		LSN: 1, TrxID: 9201, Type: manager.LOG_TYPE_INSERT, Data: []byte("undo-slot"),
	})
	require.NoError(t, err)
	after := readMetric()
	require.Greater(t, after[0].Int(), before[0].Int())
	require.Equal(t, "enabled", after[1].String())
}

func TestInformationSchemaInnoDBMetricsExposePendingRedoWrites(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	readMetric := func() []basic.Value {
		result := mustSelectResultSQL(t, executor, "", "select count, status from information_schema.innodb_metrics where name = 'os_log_pending_writes'")
		require.Len(t, result.Records, 1)
		return result.Records[0].GetValues()
	}
	before := readMetric()
	require.Equal(t, "enabled", before[1].String())
	_, err := executor.QueryExecutor.txManager.GetRedoLogManager().Append(&manager.RedoLogEntry{
		TrxID: 9301, Type: manager.LOG_TYPE_INSERT, PageID: 88, Data: bytes.Repeat([]byte("pending"), 8),
	})
	require.NoError(t, err)
	after := readMetric()
	require.Greater(t, after[0].Int(), before[0].Int())
}

func TestInformationSchemaInnoDBMetricsExposePendingRedoFsyncs(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	redo := executor.QueryExecutor.txManager.GetRedoLogManager()
	require.NoError(t, redo.ConfigureGroupCommit(500*time.Millisecond, 100))
	readMetric := func() []basic.Value {
		result := mustSelectResultSQL(t, executor, "", "select count, status from information_schema.innodb_metrics where name = 'os_log_pending_fsyncs'")
		require.Len(t, result.Records, 1)
		return result.Records[0].GetValues()
	}
	before := readMetric()
	require.Equal(t, "enabled", before[1].String())
	lsn, err := redo.Append(&manager.RedoLogEntry{
		TrxID: 9401, Type: manager.LOG_TYPE_INSERT, PageID: 99, Data: bytes.Repeat([]byte("fsync"), 8),
	})
	require.NoError(t, err)
	done := make(chan error, 1)
	redo.FlushAsync(lsn, func(err error) { done <- err })
	after := readMetric()
	require.Greater(t, after[0].Int(), before[0].Int())
	require.NoError(t, <-done)
}

type blockingMetricRollbackExecutor struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *blockingMetricRollbackExecutor) InsertRecord(tableID, recordID uint64, data []byte) error {
	return nil
}

func (e *blockingMetricRollbackExecutor) UpdateRecord(tableID, recordID uint64, data, columnBitmap []byte) error {
	return nil
}

func (e *blockingMetricRollbackExecutor) DeleteRecord(tableID, recordID uint64, primaryKeyData []byte) error {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	return nil
}

func TestInformationSchemaInnoDBMetricsExposeActiveRollbacks(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_enable", "trx_rollback_active"))

	readMetric := func() []basic.Value {
		result := mustSelectResultSQL(t, executor, "", "select count, status from information_schema.innodb_metrics where name = 'trx_rollback_active'")
		require.Len(t, result.Records, 1)
		return result.Records[0].GetValues()
	}
	require.Equal(t, int64(0), readMetric()[0].Int())
	require.Equal(t, "enabled", readMetric()[1].String())

	txManager := executor.QueryExecutor.txManager
	rollbackExecutor := &blockingMetricRollbackExecutor{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	txManager.GetUndoLogManager().SetRollbackExecutor(rollbackExecutor)
	trx, err := txManager.Begin(false, manager.TRX_ISO_REPEATABLE_READ)
	require.NoError(t, err)
	require.NoError(t, txManager.GetUndoLogManager().Append(&manager.UndoLogEntry{
		LSN:      1,
		TrxID:    trx.ID,
		TableID:  1,
		RecordID: 1,
		Type:     manager.LOG_TYPE_INSERT,
		Data:     []byte("rollback-metric"),
	}))

	rollbackDone := make(chan error, 1)
	go func() { rollbackDone <- txManager.Rollback(trx) }()
	select {
	case <-rollbackExecutor.entered:
	case <-time.After(time.Second):
		t.Fatal("rollback executor was not entered")
	}
	active := readMetric()
	require.Equal(t, int64(1), active[0].Int())
	require.Equal(t, "enabled", active[1].String())

	close(rollbackExecutor.release)
	require.NoError(t, <-rollbackDone)
	require.Equal(t, int64(0), readMetric()[0].Int())
}

func TestInformationSchemaInnoDBMetricsExposeSavepointRollbackCounters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	monitorSession := newTestMySQLSession()
	monitorSession.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(monitorSession, "innodb_monitor_enable", "module_trx"))
	session := newTestMySQLSession()
	mustExecSQL(t, executor, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table savepoint_metric_rows (id int primary key, value int)")
	mustExecSessionSQL(t, executor, session, "app", "insert into savepoint_metric_rows values (1, 10)")
	readCount := func() int64 {
		result := mustSelectResultSQL(t, executor, "", "select count from information_schema.innodb_metrics where name = 'trx_rollbacks_savepoint'")
		require.Len(t, result.Records, 1)
		return result.Records[0].GetValues()[0].Int()
	}
	before := readCount()
	mustExecSessionSQL(t, executor, session, "app", "start transaction read write")
	mustExecSessionSQL(t, executor, session, "app", "update savepoint_metric_rows set value = 11 where id = 1")
	mustExecSessionSQL(t, executor, session, "app", "savepoint before_second_update")
	mustExecSessionSQL(t, executor, session, "app", "update savepoint_metric_rows set value = 12 where id = 1")
	mustExecSessionSQL(t, executor, session, "app", "rollback to savepoint before_second_update")
	require.Equal(t, before+1, readCount())
	mustExecSessionSQL(t, executor, session, "app", "commit")
}

func TestInformationSchemaInnoDBMetricsExposeCompressionRuntimeCounters(t *testing.T) {
	executor := NewXMySQLEngine(&conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbCompression:    conf.InnodbCompressionConfig{Enabled: true, Method: "zlib", AllSpaces: true},
	})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	compression := executor.GetStorageManager().GetCompressionManager()
	compression.SetCompressionSettings(0, &manager.CompressionSettings{
		Method:    manager.COMPRESSION_ZLIB,
		Level:     manager.COMPRESSION_LEVEL_FASTEST,
		BlockSize: 16 * 1024,
	})
	disabled := <-executor.ExecuteQuery(nil, "select name, status, count from information_schema.innodb_metrics where name = 'compress_pages_compressed'", "")
	require.NoError(t, disabled.Err)
	disabledRows, ok := disabled.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, disabledRows.Records, 1)
	require.Equal(t, "disabled", disabledRows.Records[0].GetValues()[1].String())
	require.Equal(t, int64(0), disabledRows.Records[0].GetValues()[2].Int())

	session := newTestMySQLSession()
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_monitor_enable", "module_compress"))

	page := bytes.Repeat([]byte("compression metrics should be backed by real page operations"), 64)
	compressed, err := compression.CompressPage(0, 1, page)
	require.NoError(t, err)
	_, err = compression.DecompressPage(0, 1, compressed)
	require.NoError(t, err)

	result := <-executor.ExecuteQuery(nil, "select name, subsystem, count, status, type from information_schema.innodb_metrics where name like 'compress_pages_%'", "")
	require.NoError(t, result.Err)
	rows, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, rows.Records, 2)

	counts := make(map[string]int64)
	for _, record := range rows.Records {
		values := record.GetValues()
		require.Equal(t, "compression", values[1].String())
		require.Equal(t, "enabled", values[3].String())
		require.Equal(t, "status_counter", values[4].String())
		counts[values[0].String()] = values[2].Int()
	}
	require.Equal(t, int64(1), counts["compress_pages_compressed"])
	require.Equal(t, int64(1), counts["compress_pages_decompressed"])
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_monitor_reset", "module_compress"))
	reset := <-executor.ExecuteQuery(nil, "select name, count from information_schema.innodb_metrics where name = 'compress_pages_compressed'", "")
	require.NoError(t, reset.Err)
	require.Equal(t, int64(1), reset.Data.(*SelectResult).Records[0].GetValues()[1].Int())
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_monitor_disable", "module_compress"))
	disabledAgain := <-executor.ExecuteQuery(nil, "select name, status from information_schema.innodb_metrics where name = 'compress_pages_compressed'", "")
	require.NoError(t, disabledAgain.Err)
	require.Equal(t, "disabled", disabledAgain.Data.(*SelectResult).Records[0].GetValues()[1].String())
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_monitor_enable", "compress_pages_%"))
	compressed, err = compression.CompressPage(0, 2, page)
	require.NoError(t, err)
	_, err = compression.DecompressPage(0, 2, compressed)
	require.NoError(t, err)
	wildcard := <-executor.ExecuteQuery(nil, "select name, count from information_schema.innodb_metrics where name like 'compress_pages_%'", "")
	require.NoError(t, wildcard.Err)
	for _, record := range wildcard.Data.(*SelectResult).Records {
		require.Equal(t, int64(2), record.GetValues()[1].Int())
	}
}

func TestInformationSchemaInnoDBMetricsExposeCompressionLifecycleState(t *testing.T) {
	executor := NewXMySQLEngine(&conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbCompression:    conf.InnodbCompressionConfig{Enabled: true, Method: "zlib", AllSpaces: true},
	})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	compression := executor.GetStorageManager().GetCompressionManager()
	compression.SetCompressionSettings(0, &manager.CompressionSettings{
		Method:    manager.COMPRESSION_ZLIB,
		Level:     manager.COMPRESSION_LEVEL_FASTEST,
		BlockSize: 16 * 1024,
	})
	session := newTestMySQLSession()
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_monitor_enable", "module_compress"))

	page := bytes.Repeat([]byte("compression lifecycle metrics should expose timestamps"), 64)
	compressed, err := compression.CompressPage(0, 1, page)
	require.NoError(t, err)
	_, err = compression.DecompressPage(0, 1, compressed)
	require.NoError(t, err)

	active := <-executor.ExecuteQuery(nil, "select count, count_reset, time_enabled, time_elapsed, time_reset from information_schema.innodb_metrics where name = 'compress_pages_compressed'", "")
	require.NoError(t, active.Err)
	activeValues := active.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, int64(1), activeValues[0].Int())
	require.Equal(t, int64(1), activeValues[1].Int())
	require.False(t, activeValues[2].IsNull())
	require.False(t, activeValues[3].IsNull())
	require.True(t, activeValues[4].IsNull())

	require.NoError(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_monitor_reset", "module_compress"))
	reset := <-executor.ExecuteQuery(nil, "select count, count_reset, time_reset from information_schema.innodb_metrics where name = 'compress_pages_compressed'", "")
	require.NoError(t, reset.Err)
	resetValues := reset.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, int64(1), resetValues[0].Int(), "COUNT is cumulative since enable and must survive reset")
	require.Equal(t, int64(0), resetValues[1].Int())
	require.False(t, resetValues[2].IsNull())

	require.NoError(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_monitor_disable", "module_compress"))
	disabled := <-executor.ExecuteQuery(nil, "select time_enabled, time_disabled, time_elapsed from information_schema.innodb_metrics where name = 'compress_pages_compressed'", "")
	require.NoError(t, disabled.Err)
	disabledValues := disabled.Data.(*SelectResult).Records[0].GetValues()
	require.False(t, disabledValues[0].IsNull())
	require.False(t, disabledValues[1].IsNull())
	require.False(t, disabledValues[2].IsNull())
}

func TestInformationSchemaInnoDBMetricsResetAllRequiresDisabledCompressionCounters(t *testing.T) {
	executor := NewXMySQLEngine(&conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbCompression:    conf.InnodbCompressionConfig{Enabled: true, Method: "zlib", AllSpaces: true},
	})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	session := newTestMySQLSession()
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_monitor_enable", "module_compress"))
	require.Error(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_monitor_reset_all", "module_compress"))
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_monitor_disable", "module_compress"))
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_monitor_reset_all", "module_compress"))
}
