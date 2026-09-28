package engine

import (
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

const (
	compressionMetricPagesCompressed   = "compress_pages_compressed"
	compressionMetricPagesDecompressed = "compress_pages_decompressed"
)

func (e *XMySQLExecutor) recordTableHandleOpen(count int) {
	if e == nil || e.metricsRecorder == nil {
		return
	}
	e.metricsRecorder.RecordTableHandleOpen(count)
}

func (e *XMySQLExecutor) recordTableHandleClose(count int) {
	if e == nil || e.metricsRecorder == nil {
		return
	}
	e.metricsRecorder.RecordTableHandleClose(count)
}

func (e *XMySQLExecutor) innoDBRedoLogBytesWritten() int64 {
	if e == nil || e.txManager == nil {
		return 0
	}
	redo := e.txManager.GetRedoLogManager()
	if redo == nil {
		return 0
	}
	snapshot, err := redo.GetFileSnapshot()
	if err != nil || snapshot == nil || snapshot.SizeInBytes < 0 {
		return 0
	}
	return snapshot.SizeInBytes
}

func (e *XMySQLExecutor) innoDBUndoSlotsUsed() int64 {
	if e == nil || e.txManager == nil || e.txManager.GetUndoLogManager() == nil {
		return 0
	}
	stats := e.txManager.GetUndoLogManager().GetStats()
	if stats == nil || stats.ActiveTxns < 0 {
		return 0
	}
	return int64(stats.ActiveTxns)
}

// executeInformationSchemaInnoDBMetricsSelect exposes only metrics backed by
// live engine state. MySQL has a much larger metric registry; returning rows
// with invented zero counters would be less compatible than omitting metrics
// for which this engine has no authoritative source.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBMetricsSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{
		"NAME", "SUBSYSTEM", "COUNT", "MAX_COUNT", "MIN_COUNT", "AVG_COUNT",
		"COUNT_RESET", "MAX_COUNT_RESET", "MIN_COUNT_RESET", "AVG_COUNT_RESET",
		"TIME_ENABLED", "TIME_DISABLED", "TIME_ELAPSED", "TIME_RESET", "STATUS", "TYPE", "COMMENT",
	})
	rows := make([][]interface{}, 0, 3)
	activeTransactions := 0
	if e != nil && e.txManager != nil {
		activeTransactions = len(e.txManager.GetActiveTransactionSnapshots())
	}
	waitEdges := make([]managerWaitEdgeSnapshot, 0)
	if e != nil {
		for _, edge := range e.performanceSchemaWaitEdges() {
			waitEdges = append(waitEdges, managerWaitEdgeSnapshot{durationMilliseconds: edge.WaitDuration.Milliseconds()})
		}
	}
	waitDurationMs := int64(0)
	for _, edge := range waitEdges {
		if edge.durationMilliseconds > 0 {
			waitDurationMs += edge.durationMilliseconds
		}
	}
	allCompletedWaits := []manager.WaitEdge(nil)
	if e != nil && e.lockManager != nil {
		allCompletedWaits = e.lockManager.WaitSummarySnapshot()
	}
	lockMetricBaseline := 0
	lockMetricResetBaseline := 0
	tableLockWaitBaseline := 0
	tableLockWaitResetBaseline := 0
	lockTimeoutBaseline := int64(0)
	lockTimeoutResetBaseline := int64(0)
	rwCommitBaseline := int64(0)
	rwCommitResetBaseline := int64(0)
	roCommitBaseline := int64(0)
	roCommitResetBaseline := int64(0)
	nlRoCommitBaseline := int64(0)
	nlRoCommitResetBaseline := int64(0)
	transactionAllocationBaseline := int64(0)
	transactionAllocationResetBaseline := int64(0)
	dmlCommitBaseline := int64(0)
	dmlCommitResetBaseline := int64(0)
	rollbackSavepointBaseline := int64(0)
	rollbackSavepointResetBaseline := int64(0)
	undoSlotsUsedBaseline := int64(0)
	undoSlotsUsedResetBaseline := int64(0)
	osLogBytesWrittenBaseline := int64(0)
	osLogBytesWrittenResetBaseline := int64(0)
	if e != nil {
		e.innodbMetricMu.RLock()
		lockMetricBaseline = e.innodbLockMetricBaseline
		lockMetricResetBaseline = e.innodbLockMetricResetBaseline
		tableLockWaitBaseline = e.innodbTableLockWaitBaseline
		tableLockWaitResetBaseline = e.innodbTableLockWaitResetBaseline
		lockTimeoutBaseline = e.innodbLockTimeoutBaseline
		lockTimeoutResetBaseline = e.innodbLockTimeoutResetBaseline
		rwCommitBaseline = e.innodbRWCommitBaseline
		rwCommitResetBaseline = e.innodbRWCommitResetBaseline
		roCommitBaseline = e.innodbROCommitBaseline
		roCommitResetBaseline = e.innodbROCommitResetBaseline
		nlRoCommitBaseline = e.innodbNLROCommitBaseline
		nlRoCommitResetBaseline = e.innodbNLROCommitResetBaseline
		transactionAllocationBaseline = e.innodbTransactionAllocationBaseline
		transactionAllocationResetBaseline = e.innodbTransactionAllocationResetBaseline
		dmlCommitBaseline = e.innodbDMLCommitBaseline
		dmlCommitResetBaseline = e.innodbDMLCommitResetBaseline
		rollbackSavepointBaseline = e.innodbRollbackSavepointBaseline
		rollbackSavepointResetBaseline = e.innodbRollbackSavepointResetBaseline
		undoSlotsUsedBaseline = e.innodbUndoSlotsUsedBaseline
		undoSlotsUsedResetBaseline = e.innodbUndoSlotsUsedResetBaseline
		osLogBytesWrittenBaseline = e.innodbOSLogBytesWrittenBaseline
		osLogBytesWrittenResetBaseline = e.innodbOSLogBytesWrittenResetBaseline
		e.innodbMetricMu.RUnlock()
	}
	completedWaits := waitEdgesAfterBaseline(allCompletedWaits, lockMetricBaseline)
	resetWaits := waitEdgesAfterBaseline(allCompletedWaits, lockMetricResetBaseline)
	completedLockWaits := int64(len(completedWaits))
	completedLockWaitTimeMs := int64(0)
	completedLockWaitMaxMs := int64(0)
	for _, edge := range completedWaits {
		waitMs := edge.WaitDuration.Milliseconds()
		completedLockWaitTimeMs += waitMs
		if waitMs > completedLockWaitMaxMs {
			completedLockWaitMaxMs = waitMs
		}
	}
	completedLockWaitAvgMs := int64(0)
	if completedLockWaits > 0 {
		completedLockWaitAvgMs = completedLockWaitTimeMs / completedLockWaits
	}
	tableLockWaits := 0
	if e != nil {
		tableLockWaits = len(e.getDDLCoordinator().MetadataLockWaitSummary())
	}
	lockTimeouts := int64(0)
	if e != nil && e.metricsRecorder != nil {
		lockTimeouts = e.metricsRecorder.LockTimeouts()
	}
	rwCommits := int64(0)
	roCommits := int64(0)
	nlRoCommits := int64(0)
	transactionAllocations := int64(0)
	dmlCommits := int64(0)
	rollbackSavepointCommits := int64(0)
	undoSlotsUsed := e.innoDBUndoSlotsUsed()
	if e != nil && e.metricsRecorder != nil {
		rwCommits, roCommits = e.metricsRecorder.TransactionCommitModeTotals()
		nlRoCommits = e.metricsRecorder.TransactionNonLockingReadOnlyCommitTotal()
		dmlCommits = e.metricsRecorder.TransactionDMLCommitTotal()
		rollbackSavepointCommits = e.metricsRecorder.TransactionRollbackSavepointTotal()
	}
	if e != nil && e.txManager != nil {
		transactionAllocations = e.txManager.GetTransactionAllocations()
	}
	activeRollbackMetricEnabled := e.isInnoDBMetricEnabled("trx_rollback_active")
	activeRollbackMetricCount := int64(0)
	activeRollbackMetricStatus := "disabled"
	if activeRollbackMetricEnabled && e.txManager != nil {
		activeRollbackMetricCount = e.txManager.GetActiveRollbacks()
		activeRollbackMetricStatus = "enabled"
	}
	transactionMetricEnabled := e.isInnoDBMetricEnabled("trx_active_transactions")
	transactionMetricCount := int64(0)
	transactionMetricStatus := "disabled"
	if transactionMetricEnabled {
		transactionMetricCount = int64(activeTransactions)
		transactionMetricStatus = "enabled"
	}
	lockWaitMetricEnabled := e.isInnoDBMetricEnabled("lock_row_lock_current_waits")
	lockWaitMetricCount := int64(0)
	lockWaitMetricStatus := "disabled"
	if lockWaitMetricEnabled {
		lockWaitMetricCount = int64(len(waitEdges))
		lockWaitMetricStatus = "enabled"
	}
	lockThreadsMetricEnabled := e.isInnoDBMetricEnabled("lock_threads_waiting")
	lockThreadsMetricCount := int64(0)
	lockThreadsMetricStatus := "disabled"
	if lockThreadsMetricEnabled {
		lockThreadsMetricCount = int64(len(waitEdges))
		lockThreadsMetricStatus = "enabled"
	}
	metrics := []innodbMetricRow{
		{name: "trx_active_transactions", subsystem: "transaction", count: transactionMetricCount, metricType: "counter", status: transactionMetricStatus, comment: "Number of active transactions"},
		{name: "trx_rollback_active", subsystem: "transaction", count: activeRollbackMetricCount, metricType: "counter", status: activeRollbackMetricStatus, comment: "Number of active rollback operations"},
		{name: "lock_waits_current", subsystem: "lock", count: int64(len(waitEdges)), metricType: "gauge", comment: "Current row-lock wait edges maintained by LockManager"},
		{name: "lock_wait_time_ms_current", subsystem: "lock", count: waitDurationMs, metricType: "gauge", comment: "Sum of current row-lock wait durations in milliseconds"},
		{name: "lock_row_lock_current_waits", subsystem: "lock", count: lockWaitMetricCount, metricType: "status_counter", status: lockWaitMetricStatus, comment: "Number of row locks currently being waited for"},
		{name: "lock_threads_waiting", subsystem: "lock", count: lockThreadsMetricCount, metricType: "status_counter", status: lockThreadsMetricStatus, comment: "Number of threads currently waiting for row locks"},
	}
	if e != nil && e.metricsRecorder != nil {
		_, rolledBack := e.metricsRecorder.TransactionTotals()
		metrics = append(metrics, innodbMetricRow{
			name: "trx_rollbacks", subsystem: "transaction", count: rolledBack,
			metricType: "counter", status: "enabled",
			comment: "Number of transactions rolled back at the transaction completion boundary",
		})
		for _, definition := range []struct {
			name          string
			count         int64
			baseline      int64
			resetBaseline int64
			comment       string
		}{
			{name: "trx_rw_commits", count: rwCommits, baseline: rwCommitBaseline, resetBaseline: rwCommitResetBaseline, comment: "Number of read-write transaction commits"},
			{name: "trx_ro_commits", count: roCommits, baseline: roCommitBaseline, resetBaseline: roCommitResetBaseline, comment: "Number of read-only transaction commits"},
			{name: "trx_nl_ro_commits", count: nlRoCommits, baseline: nlRoCommitBaseline, resetBaseline: nlRoCommitResetBaseline, comment: "Number of non-locking auto-commit read-only transactions committed"},
			{name: "trx_allocations", count: transactionAllocations, baseline: transactionAllocationBaseline, resetBaseline: transactionAllocationResetBaseline, comment: "Number of transaction objects allocated"},
			{name: "trx_commits_insert_update", count: dmlCommits, baseline: dmlCommitBaseline, resetBaseline: dmlCommitResetBaseline, comment: "Number of transactions committed with inserts and updates"},
			{name: "trx_rollbacks_savepoint", count: rollbackSavepointCommits, baseline: rollbackSavepointBaseline, resetBaseline: rollbackSavepointResetBaseline, comment: "Number of transactions rolled back to a savepoint"},
			{name: "trx_undo_slots_used", count: undoSlotsUsed, baseline: undoSlotsUsedBaseline, resetBaseline: undoSlotsUsedResetBaseline, comment: "Number of undo slots currently used"},
		} {
			runtime := e.innoDBTransactionCommitMetricRuntimeSnapshot(definition.name, definition.count, definition.baseline, definition.resetBaseline)
			status := "disabled"
			if runtime != nil && runtime.Enabled {
				status = "enabled"
			}
			metrics = append(metrics, innodbMetricRow{
				name: definition.name, subsystem: "transaction", count: definition.count,
				metricType: "status_counter", status: status, comment: definition.comment, runtime: runtime,
			})
		}
	}
	for _, definition := range []struct {
		name    string
		count   int64
		comment string
	}{
		{name: "lock_rec_lock_waits", count: completedLockWaits, comment: "Number of completed record lock waits"},
		{name: "lock_row_lock_waits", count: completedLockWaits, comment: "Number of row lock waits"},
		{name: "lock_row_lock_time", count: completedLockWaitTimeMs, comment: "Total time to acquire row locks, in milliseconds"},
		{name: "lock_row_lock_time_avg", count: completedLockWaitAvgMs, comment: "Average time to acquire a row lock, in milliseconds"},
		{name: "lock_row_lock_time_max", count: completedLockWaitMaxMs, comment: "Maximum time to acquire a row lock, in milliseconds"},
	} {
		runtime := e.innoDBLockMetricRuntimeSnapshot(definition.name, completedWaits, resetWaits)
		status := "disabled"
		if runtime != nil && runtime.Enabled {
			status = "enabled"
		}
		metrics = append(metrics, innodbMetricRow{name: definition.name, subsystem: "lock", count: definition.count, metricType: "status_counter", status: status, comment: definition.comment, runtime: runtime})
	}
	tableLockWaitRuntime := e.innoDBTableLockWaitMetricRuntimeSnapshot(tableLockWaits, tableLockWaitBaseline, tableLockWaitResetBaseline)
	tableLockWaitStatus := "disabled"
	if tableLockWaitRuntime != nil && tableLockWaitRuntime.Enabled {
		tableLockWaitStatus = "enabled"
	}
	metrics = append(metrics, innodbMetricRow{
		name: "lock_table_lock_waits", subsystem: "lock", count: int64(tableLockWaits),
		metricType: "status_counter", status: tableLockWaitStatus,
		comment: "Number of completed table-lock waits", runtime: tableLockWaitRuntime,
	})
	lockTimeoutRuntime := e.innoDBLockTimeoutMetricRuntimeSnapshot(lockTimeouts, lockTimeoutBaseline, lockTimeoutResetBaseline)
	lockTimeoutStatus := "disabled"
	if lockTimeoutRuntime != nil && lockTimeoutRuntime.Enabled {
		lockTimeoutStatus = "enabled"
	}
	metrics = append(metrics, innodbMetricRow{
		name: "lock_timeouts", subsystem: "lock", count: lockTimeouts,
		metricType: "status_counter", status: lockTimeoutStatus,
		comment: "Number of lock waits that reached a deadline", runtime: lockTimeoutRuntime,
	})
	if e != nil && e.lockManager != nil {
		lockRuntimeStats := e.lockManager.LockRuntimeStatsSnapshot()
		for _, definition := range []struct {
			name    string
			count   uint64
			comment string
		}{
			{name: "lock_rec_lock_requests", count: lockRuntimeStats.RecordLockRequests, comment: "Number of record lock requests"},
			{name: "lock_rec_grant_attempts", count: lockRuntimeStats.RecordLockGrantAttempts, comment: "Number of record lock grant attempts"},
			{name: "lock_rec_release_attempts", count: lockRuntimeStats.RecordLockReleaseAttempts, comment: "Number of record lock release attempts"},
			{name: "lock_rec_lock_created", count: lockRuntimeStats.RecordLockCreated, comment: "Number of record locks created"},
			{name: "lock_rec_lock_removed", count: lockRuntimeStats.RecordLockRemoved, comment: "Number of record locks removed"},
			{name: "lock_rec_locks", count: lockRuntimeStats.RecordLocks, comment: "Current granted record locks"},
			{name: "lock_deadlocks", count: lockRuntimeStats.Deadlocks, comment: "Number of deadlocks detected"},
		} {
			runtime := e.innoDBLockLifecycleMetricRuntimeSnapshot(definition.name, lockRuntimeStats)
			status := "disabled"
			if runtime != nil && runtime.Enabled {
				status = "enabled"
			}
			metrics = append(metrics, innodbMetricRow{
				name: definition.name, subsystem: "lock", count: int64(definition.count),
				metricType: "status_counter", status: status, comment: definition.comment, runtime: runtime,
			})
		}
	}
	for _, dmlMetric := range []struct {
		name    string
		comment string
	}{
		{name: "dml_inserts", comment: "Number of rows inserted"},
		{name: "dml_updates", comment: "Number of rows updated"},
		{name: "dml_deletes", comment: "Number of rows deleted"},
		{name: "dml_reads", comment: "Number of rows read by scans"},
	} {
		runtime := e.innoDBMetricRuntimeSnapshot(dmlMetric.name)
		if runtime == nil {
			continue
		}
		status := "disabled"
		if runtime.Enabled {
			status = "enabled"
		}
		metrics = append(metrics, innodbMetricRow{
			name: dmlMetric.name, subsystem: "dml", count: int64(runtime.Count),
			metricType: "status_counter", status: status, comment: dmlMetric.comment,
			runtime: runtime,
		})
	}
	if e != nil && e.txManager != nil && e.txManager.GetRedoLogManager() != nil {
		redoManager := e.txManager.GetRedoLogManager()
		redoStats := redoManager.GetStats()
		osLogBytesWritten := e.innoDBRedoLogBytesWritten()
		for _, definition := range []struct {
			name    string
			count   uint64
			comment string
		}{
			{name: "log_lsn_current", count: redoStats.CurrentLSN, comment: "Current redo log sequence number"},
			{name: "log_lsn_last_checkpoint", count: redoStats.LastCheckpoint, comment: "Redo log sequence number of the last checkpoint"},
			{name: "log_lsn_checkpoint_age", count: redoLSNCheckpointAge(redoStats.CurrentLSN, redoStats.LastCheckpoint), comment: "Distance between the current LSN and the last checkpoint LSN"},
		} {
			enabled := e.isInnoDBMetricEnabled(definition.name)
			count := int64(0)
			status := "disabled"
			if enabled {
				count = int64(definition.count)
				status = "enabled"
			}
			metrics = append(metrics, innodbMetricRow{
				name: definition.name, subsystem: "log", count: count,
				metricType: "value", status: status, comment: definition.comment,
			})
		}
		if groupCommitStats := redoManager.GetGroupCommitStats(); groupCommitStats != nil {
			metrics = append(metrics, innodbMetricRow{
				name: "os_log_fsyncs", subsystem: "os", count: int64(groupCommitStats.TotalFsyncs),
				metricType: "status_counter", status: "enabled",
				comment: "Number of redo-log group-commit fsync operations",
			})
		}
		pendingWritesEnabled := e.isInnoDBMetricEnabled("os_log_pending_writes")
		pendingWritesCount := int64(0)
		pendingWritesStatus := "disabled"
		if pendingWritesEnabled {
			pendingWritesCount = int64(redoStats.BufferedLogs)
			pendingWritesStatus = "enabled"
		}
		metrics = append(metrics, innodbMetricRow{
			name: "os_log_pending_writes", subsystem: "os", count: pendingWritesCount,
			metricType: "status_counter", status: pendingWritesStatus,
			comment: "Number of redo-log records waiting to be written",
		})
		pendingFsyncsEnabled := e.isInnoDBMetricEnabled("os_log_pending_fsyncs")
		pendingFsyncsCount := int64(0)
		pendingFsyncsStatus := "disabled"
		if pendingFsyncsEnabled {
			pendingFsyncsCount = int64(redoStats.PendingFsyncs)
			pendingFsyncsStatus = "enabled"
		}
		metrics = append(metrics, innodbMetricRow{
			name: "os_log_pending_fsyncs", subsystem: "os", count: pendingFsyncsCount,
			metricType: "status_counter", status: pendingFsyncsStatus,
			comment: "Number of redo-log fsync requests currently pending",
		})
		osLogBytesWrittenRuntime := e.innoDBCounterMetricRuntimeSnapshot(
			"os_log_bytes_written", osLogBytesWritten,
			osLogBytesWrittenBaseline, osLogBytesWrittenResetBaseline)
		osLogBytesWrittenStatus := "disabled"
		if osLogBytesWrittenRuntime != nil && osLogBytesWrittenRuntime.Enabled {
			osLogBytesWrittenStatus = "enabled"
		}
		metrics = append(metrics, innodbMetricRow{
			name: "os_log_bytes_written", subsystem: "os", count: osLogBytesWritten,
			metricType: "status_counter", status: osLogBytesWrittenStatus,
			comment: "Number of bytes written to the redo log", runtime: osLogBytesWrittenRuntime,
		})
	}
	if e != nil && e.bufferPoolManager != nil {
		if provider, ok := e.bufferPoolManager.(interface{ GetStats() map[string]interface{} }); ok && provider != nil {
			stats := provider.GetStats()
			totalPages := informationSchemaBufferPoolStatInt64(stats, "total_pages")
			dataPages := informationSchemaBufferPoolStatInt64(stats, "cache_size")
			dirtyPages := informationSchemaBufferPoolStatInt64(stats, "dirty_pages")
			freePages := totalPages - dataPages
			if freePages < 0 {
				freePages = 0
			}
			miscPages := totalPages - freePages - dataPages
			if miscPages < 0 {
				miscPages = 0
			}
			hits := informationSchemaBufferPoolStatInt64(stats, "hits")
			misses := informationSchemaBufferPoolStatInt64(stats, "misses")
			pageReads := informationSchemaBufferPoolStatInt64(stats, "page_reads")
			pageWrites := informationSchemaBufferPoolStatInt64(stats, "page_writes")
			pageCreates := informationSchemaBufferPoolStatInt64(stats, "page_creates")
			readAhead := informationSchemaBufferPoolStatInt64(stats, "read_ahead")
			readAheadEvicted := informationSchemaBufferPoolStatInt64(stats, "read_ahead_evicted")
			pageSize := informationSchemaBufferPageSize(e, 0)
			metrics = append(metrics,
				innodbMetricRow{name: "buffer_pool_pages_total", subsystem: "buffer", count: totalPages, metricType: "value", comment: "Total buffer pool size in pages"},
				innodbMetricRow{name: "buffer_pool_pages_data", subsystem: "buffer", count: dataPages, metricType: "value", comment: "Buffer pages containing data"},
				innodbMetricRow{name: "buffer_pool_pages_free", subsystem: "buffer", count: freePages, metricType: "value", comment: "Buffer pages currently free"},
				innodbMetricRow{name: "buffer_pool_pages_misc", subsystem: "buffer", count: miscPages, metricType: "value", comment: "Buffer pages used for administrative overhead"},
				innodbMetricRow{name: "buffer_pool_pages_dirty", subsystem: "buffer", count: dirtyPages, metricType: "value", comment: "Buffer pages currently dirty"},
				innodbMetricRow{name: "buffer_pool_bytes_data", subsystem: "buffer", count: dataPages * pageSize, metricType: "value", comment: "Buffer bytes containing data"},
				innodbMetricRow{name: "buffer_pool_bytes_dirty", subsystem: "buffer", count: dirtyPages * pageSize, metricType: "value", comment: "Buffer bytes currently dirty"},
				innodbMetricRow{name: "buffer_pool_size", subsystem: "server", count: totalPages * pageSize, metricType: "value", comment: "Server buffer pool size in bytes"},
				innodbMetricRow{name: "buffer_pool_read_requests", subsystem: "buffer", count: hits + misses, metricType: "status_counter", comment: "Number of logical read requests"},
				innodbMetricRow{name: "buffer_pool_reads", subsystem: "buffer", count: misses, metricType: "status_counter", comment: "Number of reads directly from disk"},
				innodbMetricRow{name: "buffer_pool_write_requests", subsystem: "buffer", count: pageWrites, metricType: "status_counter", comment: "Number of write requests"},
				// MySQL exposes buffer_data_reads and buffer_data_written as byte
				// counters (innodb_data_read/innodb_data_written), while the local
				// manager records the authoritative I/O event count in pages.
				innodbMetricRow{name: "buffer_data_reads", subsystem: "buffer", count: pageReads * pageSize, metricType: "status_counter", comment: "Amount of data read in bytes"},
				innodbMetricRow{name: "buffer_data_written", subsystem: "buffer", count: pageWrites * pageSize, metricType: "status_counter", comment: "Amount of data written in bytes"},
				innodbMetricRow{name: "buffer_pages_read", subsystem: "buffer", count: pageReads, metricType: "status_counter", comment: "Number of buffer pages read"},
				innodbMetricRow{name: "buffer_pages_created", subsystem: "buffer", count: pageCreates, metricType: "status_counter", comment: "Number of buffer pages created"},
				innodbMetricRow{name: "buffer_pages_written", subsystem: "buffer", count: pageWrites, metricType: "status_counter", comment: "Number of buffer pages written"},
				// The storage manager's page I/O counters are the authoritative
				// completed physical read/write operations available to xmysql.
				innodbMetricRow{name: "os_data_reads", subsystem: "os", count: pageReads, metricType: "status_counter", comment: "Number of reads initiated"},
				innodbMetricRow{name: "os_data_writes", subsystem: "os", count: pageWrites, metricType: "status_counter", comment: "Number of writes initiated"},
				innodbMetricRow{name: "buffer_pool_read_ahead", subsystem: "buffer", count: readAhead, metricType: "status_counter", comment: "Number of pages read into the buffer pool by read-ahead"},
				innodbMetricRow{name: "buffer_pool_read_ahead_evicted", subsystem: "buffer", count: readAheadEvicted, metricType: "status_counter", comment: "Number of read-ahead pages evicted without being accessed"},
				innodbMetricRow{name: "innodb_page_size", subsystem: "server", count: pageSize, metricType: "value", comment: "InnoDB page size in bytes"},
			)
		}
	}
	if e != nil && e.storageManager != nil {
		if compression := e.storageManager.GetCompressionManager(); compression != nil {
			stats := compression.GetCompressionMetricStats()
			compressionMetrics := []struct {
				name    string
				count   uint64
				enabled bool
				comment string
			}{
				{name: compressionMetricPagesCompressed, count: stats.CompressedPages, enabled: compression.IsCompressionMetricEnabled(compressionMetricPagesCompressed), comment: "Number of pages compressed"},
				{name: compressionMetricPagesDecompressed, count: stats.UncompressedPages, enabled: compression.IsCompressionMetricEnabled(compressionMetricPagesDecompressed), comment: "Number of pages decompressed"},
			}
			for _, compressionMetric := range compressionMetrics {
				compressionRuntime := compression.GetCompressionMetricRuntime(compressionMetric.name)
				status := "disabled"
				count := int64(0)
				if compressionMetric.enabled {
					status = "enabled"
					count = int64(compressionMetric.count)
				}
				metrics = append(metrics, innodbMetricRow{
					name: compressionMetric.name, subsystem: "compression", count: count,
					metricType: "status_counter", status: status, comment: compressionMetric.comment,
					runtime: innoDBMetricRuntimeSnapshotFromCompression(compressionRuntime),
				})
			}
		}
	}
	if e != nil && e.metricsRecorder != nil {
		openFiles := int64(0)
		for _, file := range e.metricsRecorder.FileSummary() {
			if file.OpenCount > 0 {
				openFiles += file.OpenCount
			}
		}
		metrics = append(metrics, innodbMetricRow{
			name: "file_num_open_files", subsystem: "file_system", count: openFiles,
			metricType: "status_counter", comment: "Number of open files",
		})
		opened, closed, references := e.metricsRecorder.TableHandleTotals()
		for _, definition := range []struct {
			name    string
			count   int64
			comment string
		}{
			{name: "metadata_table_handles_opened", count: opened, comment: "Number of table-handle lease acquisitions"},
			{name: "metadata_table_handles_closed", count: closed, comment: "Number of table-handle lease releases"},
			{name: "metadata_table_reference_count", count: references, comment: "Current table-handle lease references"},
			{name: "lock_table_lock_created", count: opened, comment: "Number of table-lock lease acquisitions"},
			{name: "lock_table_lock_removed", count: closed, comment: "Number of table-lock lease releases"},
			{name: "lock_table_locks", count: references, comment: "Current table-lock lease references"},
		} {
			subsystem := "metadata"
			if strings.HasPrefix(definition.name, "lock_") {
				subsystem = "lock"
			}
			metrics = append(metrics, innodbMetricRow{
				name: definition.name, subsystem: subsystem, count: definition.count,
				metricType: "status_counter", status: "enabled", comment: definition.comment,
			})
		}
	}
	// Keep the complete static MySQL monitor catalog visible. Dynamic rows
	// above always win; catalog-only rows deliberately remain disabled because
	// xmysql has no authoritative source for their internal InnoDB counters yet.
	knownMetrics := make(map[string]struct{}, len(metrics))
	for _, metric := range metrics {
		knownMetrics[metric.name] = struct{}{}
	}
	for _, definition := range mysql84InnoDBMetricCatalog {
		if _, exists := knownMetrics[definition.name]; exists {
			continue
		}
		metrics = append(metrics, innodbMetricRow{
			name: definition.name, subsystem: definition.subsystem, count: 0,
			metricType: definition.metricType, status: "disabled",
			comment: "MySQL 8.4 InnoDB monitor catalog entry; runtime source unavailable",
		})
	}
	nameFilter, filterOperator, hasNameFilter := informationSchemaMetricNameFilter(query)
	if hasNameFilter {
		filtered := metrics[:0]
		for _, metric := range metrics {
			if metricNameMatches(metric.name, filterOperator, nameFilter) {
				filtered = append(filtered, metric)
			}
		}
		metrics = filtered
	}
	filtered := metrics[:0]
	for _, metric := range metrics {
		status := metric.status
		if status == "" {
			status = "enabled"
		}
		if !performanceSchemaSummaryFilterMatches(query, "subsystem", metric.subsystem) ||
			!performanceSchemaSummaryFilterMatches(query, "status", status) ||
			!performanceSchemaSummaryFilterMatches(query, "type", metric.metricType) {
			continue
		}
		filtered = append(filtered, metric)
	}
	metrics = filtered
	sort.Slice(metrics, func(i, j int) bool { return metrics[i].name < metrics[j].name })
	for _, metric := range metrics {
		status := metric.status
		if status == "" {
			status = "enabled"
		}
		values := map[string]interface{}{
			"NAME": metric.name, "SUBSYSTEM": metric.subsystem, "COUNT": metric.count,
			"MAX_COUNT": metric.count, "MIN_COUNT": metric.count, "AVG_COUNT": metric.count,
			"COUNT_RESET": nil, "MAX_COUNT_RESET": nil, "MIN_COUNT_RESET": nil, "AVG_COUNT_RESET": nil,
			"TIME_ENABLED": nil, "TIME_DISABLED": nil, "TIME_ELAPSED": nil, "TIME_RESET": nil,
			"STATUS": status, "TYPE": metric.metricType, "COMMENT": metric.comment,
		}
		if metric.runtime != nil {
			runtime := metric.runtime
			values["COUNT"] = int64(runtime.Count)
			if runtime.HasCount {
				values["MAX_COUNT"] = int64(runtime.MaxCount)
				values["MIN_COUNT"] = int64(runtime.MinCount)
				values["AVG_COUNT"] = runtime.AvgCount
			} else {
				values["MAX_COUNT"] = nil
				values["MIN_COUNT"] = nil
				values["AVG_COUNT"] = nil
			}
			values["COUNT_RESET"] = int64(runtime.CountReset)
			if runtime.HasCountReset {
				values["MAX_COUNT_RESET"] = int64(runtime.MaxCountReset)
				values["MIN_COUNT_RESET"] = int64(runtime.MinCountReset)
				values["AVG_COUNT_RESET"] = runtime.AvgCountReset
			} else {
				values["MAX_COUNT_RESET"] = nil
				values["MIN_COUNT_RESET"] = nil
				values["AVG_COUNT_RESET"] = nil
			}
			if !runtime.TimeEnabled.IsZero() {
				values["TIME_ENABLED"] = runtime.TimeEnabled
			}
			if !runtime.TimeDisabled.IsZero() {
				values["TIME_DISABLED"] = runtime.TimeDisabled
			}
			values["TIME_ELAPSED"] = runtime.TimeElapsed
			if !runtime.TimeReset.IsZero() {
				values["TIME_RESET"] = runtime.TimeReset
			}
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema.innodb_metrics", columns, rows)
}

type innodbMetricRow struct {
	name       string
	subsystem  string
	count      int64
	metricType string
	status     string
	comment    string
	runtime    *innodbMetricRuntimeSnapshot
}

func redoLSNCheckpointAge(current, checkpoint uint64) uint64 {
	if current < checkpoint {
		return 0
	}
	return current - checkpoint
}

type innodbMetricRuntimeSnapshot struct {
	Count         uint64
	MaxCount      uint64
	MinCount      uint64
	AvgCount      float64
	CountReset    uint64
	MaxCountReset uint64
	MinCountReset uint64
	AvgCountReset float64
	HasCount      bool
	HasCountReset bool
	Enabled       bool
	TimeEnabled   time.Time
	TimeDisabled  time.Time
	TimeElapsed   int64
	TimeReset     time.Time
}

func innoDBMetricRuntimeSnapshotFromCompression(runtime manager.CompressionMetricRuntime) *innodbMetricRuntimeSnapshot {
	return &innodbMetricRuntimeSnapshot{
		Count: runtime.Count, MaxCount: runtime.MaxCount, MinCount: runtime.MinCount, AvgCount: runtime.AvgCount,
		CountReset: runtime.CountReset, MaxCountReset: runtime.MaxCountReset, MinCountReset: runtime.MinCountReset, AvgCountReset: runtime.AvgCountReset,
		HasCount: runtime.HasCount, HasCountReset: runtime.HasCountReset, Enabled: runtime.Enabled,
		TimeEnabled: runtime.TimeEnabled, TimeDisabled: runtime.TimeDisabled, TimeElapsed: runtime.TimeElapsed, TimeReset: runtime.TimeReset,
	}
}

type managerWaitEdgeSnapshot struct {
	durationMilliseconds int64
}

func waitEdgesAfterBaseline(edges []manager.WaitEdge, baseline int) []manager.WaitEdge {
	if baseline < 0 {
		baseline = 0
	}
	if baseline > len(edges) {
		baseline = len(edges)
	}
	return edges[baseline:]
}

func (e *XMySQLExecutor) innoDBLockMetricRuntimeSnapshot(name string, cumulative, reset []manager.WaitEdge) *innodbMetricRuntimeSnapshot {
	runtime := e.innoDBMetricRuntimeSnapshot(name)
	if runtime == nil {
		return nil
	}
	if !runtime.Enabled {
		runtime.Count = 0
		runtime.CountReset = 0
		runtime.HasCount = false
		runtime.HasCountReset = false
		return runtime
	}
	cumulativeCount, cumulativeSum, cumulativeMax := lockWaitMetricStats(cumulative)
	resetCount, resetSum, resetMax := lockWaitMetricStats(reset)
	value, resetValue := int64(cumulativeCount), int64(resetCount)
	switch name {
	case "lock_row_lock_time":
		value, resetValue = cumulativeSum, resetSum
	case "lock_row_lock_time_avg":
		value, resetValue = averageLockWaitMetric(cumulativeCount, cumulativeSum), averageLockWaitMetric(resetCount, resetSum)
	case "lock_row_lock_time_max":
		value, resetValue = cumulativeMax, resetMax
	}
	runtime.Count = uint64(nonNegativeMetricValue(value))
	runtime.CountReset = uint64(nonNegativeMetricValue(resetValue))
	runtime.HasCount = cumulativeCount > 0
	runtime.HasCountReset = resetCount > 0
	runtime.MaxCount = runtime.Count
	runtime.MinCount = runtime.Count
	runtime.AvgCount = float64(runtime.Count)
	runtime.MaxCountReset = runtime.CountReset
	runtime.MinCountReset = runtime.CountReset
	runtime.AvgCountReset = float64(runtime.CountReset)
	return runtime
}

func (e *XMySQLExecutor) innoDBTableLockWaitMetricRuntimeSnapshot(current, baseline, resetBaseline int) *innodbMetricRuntimeSnapshot {
	runtime := e.innoDBMetricRuntimeSnapshot("lock_table_lock_waits")
	if runtime == nil {
		return nil
	}
	if !runtime.Enabled {
		runtime.Count = 0
		runtime.CountReset = 0
		runtime.HasCount = false
		runtime.HasCountReset = false
		return runtime
	}
	value := current - baseline
	resetValue := current - resetBaseline
	if value < 0 {
		value = 0
	}
	if resetValue < 0 {
		resetValue = 0
	}
	runtime.Count = uint64(value)
	runtime.CountReset = uint64(resetValue)
	runtime.HasCount = value > 0
	runtime.HasCountReset = resetValue > 0
	runtime.MaxCount = runtime.Count
	runtime.MinCount = runtime.Count
	runtime.AvgCount = float64(runtime.Count)
	runtime.MaxCountReset = runtime.CountReset
	runtime.MinCountReset = runtime.CountReset
	runtime.AvgCountReset = float64(runtime.CountReset)
	return runtime
}

func (e *XMySQLExecutor) innoDBLockTimeoutMetricRuntimeSnapshot(current, baseline, resetBaseline int64) *innodbMetricRuntimeSnapshot {
	runtime := e.innoDBMetricRuntimeSnapshot("lock_timeouts")
	if runtime == nil {
		return nil
	}
	if !runtime.Enabled {
		runtime.Count = 0
		runtime.CountReset = 0
		runtime.HasCount = false
		runtime.HasCountReset = false
		return runtime
	}
	value := current - baseline
	resetValue := current - resetBaseline
	if value < 0 {
		value = 0
	}
	if resetValue < 0 {
		resetValue = 0
	}
	runtime.Count = uint64(value)
	runtime.CountReset = uint64(resetValue)
	runtime.HasCount = value > 0
	runtime.HasCountReset = resetValue > 0
	runtime.MaxCount = runtime.Count
	runtime.MinCount = runtime.Count
	runtime.AvgCount = float64(runtime.Count)
	runtime.MaxCountReset = runtime.CountReset
	runtime.MinCountReset = runtime.CountReset
	runtime.AvgCountReset = float64(runtime.CountReset)
	return runtime
}

func (e *XMySQLExecutor) innoDBCounterMetricRuntimeSnapshot(name string, current, baseline, resetBaseline int64) *innodbMetricRuntimeSnapshot {
	runtime := e.innoDBMetricRuntimeSnapshot(name)
	if runtime == nil {
		return nil
	}
	if !runtime.Enabled {
		runtime.Count = 0
		runtime.CountReset = 0
		runtime.HasCount = false
		runtime.HasCountReset = false
		return runtime
	}
	value := current - baseline
	resetValue := current - resetBaseline
	if value < 0 {
		value = 0
	}
	if resetValue < 0 {
		resetValue = 0
	}
	runtime.Count = uint64(value)
	runtime.CountReset = uint64(resetValue)
	runtime.HasCount = value > 0
	runtime.HasCountReset = resetValue > 0
	runtime.MaxCount = runtime.Count
	runtime.MinCount = runtime.Count
	runtime.AvgCount = float64(runtime.Count)
	runtime.MaxCountReset = runtime.CountReset
	runtime.MinCountReset = runtime.CountReset
	runtime.AvgCountReset = float64(runtime.CountReset)
	return runtime
}

func (e *XMySQLExecutor) innoDBTransactionCommitMetricRuntimeSnapshot(name string, current, baseline, resetBaseline int64) *innodbMetricRuntimeSnapshot {
	return e.innoDBCounterMetricRuntimeSnapshot(name, current, baseline, resetBaseline)
}

func (e *XMySQLExecutor) innoDBLockLifecycleMetricRuntimeSnapshot(name string, current manager.LockRuntimeStats) *innodbMetricRuntimeSnapshot {
	runtime := e.innoDBMetricRuntimeSnapshot(name)
	if runtime == nil {
		return nil
	}
	if !runtime.Enabled {
		runtime.Count = 0
		runtime.CountReset = 0
		runtime.HasCount = false
		runtime.HasCountReset = false
		return runtime
	}
	e.innodbMetricMu.RLock()
	baseline := e.innodbLockLifecycleBaseline
	resetBaseline := e.innodbLockLifecycleResetBaseline
	e.innodbMetricMu.RUnlock()
	value := lockLifecycleMetricValue(name, current) - lockLifecycleMetricValue(name, baseline)
	resetValue := lockLifecycleMetricValue(name, current) - lockLifecycleMetricValue(name, resetBaseline)
	runtime.Count = value
	runtime.CountReset = resetValue
	runtime.HasCount = value > 0
	runtime.HasCountReset = resetValue > 0
	runtime.MaxCount = runtime.Count
	runtime.MinCount = runtime.Count
	runtime.AvgCount = float64(runtime.Count)
	runtime.MaxCountReset = runtime.CountReset
	runtime.MinCountReset = runtime.CountReset
	runtime.AvgCountReset = float64(runtime.CountReset)
	return runtime
}

func lockLifecycleMetricValue(name string, stats manager.LockRuntimeStats) uint64 {
	switch name {
	case "lock_rec_lock_requests":
		return stats.RecordLockRequests
	case "lock_rec_grant_attempts":
		return stats.RecordLockGrantAttempts
	case "lock_rec_release_attempts":
		return stats.RecordLockReleaseAttempts
	case "lock_rec_lock_created":
		return stats.RecordLockCreated
	case "lock_rec_lock_removed":
		return stats.RecordLockRemoved
	case "lock_deadlocks":
		return stats.Deadlocks
	default:
		return 0
	}
}

func lockWaitMetricStats(edges []manager.WaitEdge) (count int, totalMs int64, maxMs int64) {
	for _, edge := range edges {
		count++
		waitMs := edge.WaitDuration.Milliseconds()
		totalMs += waitMs
		if waitMs > maxMs {
			maxMs = waitMs
		}
	}
	return count, totalMs, maxMs
}

func averageLockWaitMetric(count int, totalMs int64) int64 {
	if count == 0 {
		return 0
	}
	return totalMs / int64(count)
}

func nonNegativeMetricValue(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func informationSchemaMetricNameFilter(query string) (string, string, bool) {
	match := regexp.MustCompile(`(?is)\bname\b\s*(=|like)\s*('([^']*)'|"([^"]*)")`).FindStringSubmatch(query)
	if len(match) != 5 {
		return "", "", false
	}
	value := match[3]
	if value == "" {
		value = match[4]
	}
	return value, strings.ToLower(match[1]), true
}

func metricNameMatches(name, operator, pattern string) bool {
	if operator == "=" {
		return strings.EqualFold(name, pattern)
	}
	regex := "^" + regexp.QuoteMeta(strings.ToLower(pattern)) + "$"
	regex = strings.ReplaceAll(regex, "%", ".*")
	regex = strings.ReplaceAll(regex, "_", ".")
	matched, _ := regexp.MatchString(regex, strings.ToLower(name))
	return matched
}
