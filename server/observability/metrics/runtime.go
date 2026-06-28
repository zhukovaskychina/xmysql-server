package metrics

import "time"

// RuntimeRecorder converts runtime events into P0 metric updates.
//
// It is intentionally small and dependency-free so execution, transaction,
// lock, recovery, and checkpoint paths can adopt it incrementally.
type RuntimeRecorder struct {
	registry *Registry
}

// NewRuntimeRecorder creates a recorder backed by registry.
func NewRuntimeRecorder(registry *Registry) *RuntimeRecorder {
	return &RuntimeRecorder{registry: registry}
}

// RecordQuery records one completed query.
func (r *RuntimeRecorder) RecordQuery(database, statementType, status string, latency time.Duration) {
	labels := Labels{
		"database": database,
		"status":   status,
	}
	_ = r.registry.IncCounter("xmysql_queries_total", 1, labels)
	_ = r.registry.ObserveHistogram("xmysql_query_latency_ms", float64(latency.Milliseconds()), Labels{
		"database":       database,
		"statement_type": statementType,
		"status":         status,
	})
}

// RecordQueryError records a query error.
func (r *RuntimeRecorder) RecordQueryError(database, errorClass, errorCode string) {
	_ = r.registry.IncCounter("xmysql_query_errors_total", 1, Labels{
		"database":    database,
		"error_class": errorClass,
		"error_code":  errorCode,
	})
}

// SetActiveConnections records the current active connection count.
func (r *RuntimeRecorder) SetActiveConnections(listener string, count int) {
	_ = r.registry.SetGauge("xmysql_connections_active", float64(count), Labels{
		"listener": listener,
	})
}

// SetActiveTransactions records the current active transaction count.
func (r *RuntimeRecorder) SetActiveTransactions(isolationLevel string, count int) {
	_ = r.registry.SetGauge("xmysql_transactions_active", float64(count), Labels{
		"isolation_level": isolationLevel,
	})
}

// RecordTransactionCommit records one committed transaction.
func (r *RuntimeRecorder) RecordTransactionCommit(isolationLevel string) {
	_ = r.registry.IncCounter("xmysql_transactions_committed_total", 1, Labels{
		"isolation_level": isolationLevel,
	})
}

// RecordTransactionRollback records one rolled-back transaction.
func (r *RuntimeRecorder) RecordTransactionRollback(isolationLevel, reason string) {
	_ = r.registry.IncCounter("xmysql_transactions_rolled_back_total", 1, Labels{
		"isolation_level": isolationLevel,
		"reason":          reason,
	})
}

// RecordLockWait records one lock wait and its duration.
func (r *RuntimeRecorder) RecordLockWait(lockType, resourceType string, duration time.Duration) {
	labels := Labels{
		"lock_type":     lockType,
		"resource_type": resourceType,
	}
	_ = r.registry.IncCounter("xmysql_lock_waits_total", 1, labels)
	_ = r.registry.ObserveHistogram("xmysql_lock_wait_duration_ms", float64(duration.Milliseconds()), labels)
}

// SetLongTransactions records active long transaction count.
func (r *RuntimeRecorder) SetLongTransactions(level string, count int) {
	_ = r.registry.SetGauge("xmysql_long_transactions_active", float64(count), Labels{
		"level": level,
	})
}

// RecordRecoveryRun records one recovery attempt.
func (r *RuntimeRecorder) RecordRecoveryRun(result string) {
	_ = r.registry.IncCounter("xmysql_recovery_runs_total", 1, Labels{
		"result": result,
	})
}

// RecordRecoveryFailure records one recovery failure at stage.
func (r *RuntimeRecorder) RecordRecoveryFailure(stage string) {
	_ = r.registry.IncCounter("xmysql_recovery_failures_total", 1, Labels{
		"stage": stage,
	})
}

// SetCheckpointDirtyPages records current checkpoint dirty page count.
func (r *RuntimeRecorder) SetCheckpointDirtyPages(space string, count int) {
	_ = r.registry.SetGauge("xmysql_checkpoint_dirty_pages", float64(count), Labels{
		"space": space,
	})
}

// RecordCheckpointRun records one checkpoint run.
func (r *RuntimeRecorder) RecordCheckpointRun(result string) {
	_ = r.registry.IncCounter("xmysql_checkpoint_runs_total", 1, Labels{
		"result": result,
	})
}
