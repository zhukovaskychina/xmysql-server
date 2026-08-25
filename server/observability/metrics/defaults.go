package metrics

// RegisterP0Metrics registers the minimum P0-D metric catalog.
func RegisterP0Metrics(registry *Registry) {
	registry.RegisterCounter("xmysql_queries_total", "Total accepted SQL or query requests.", "database", "status")
	registry.RegisterCounter("xmysql_query_errors_total", "Total query execution errors.", "database", "error_class", "error_code")
	registry.RegisterHistogram("xmysql_query_latency_ms", "Query execution latency in milliseconds.", nil, "database", "statement_type", "status")

	registry.RegisterGauge("xmysql_connections_active", "Currently active client connections.", "listener")
	registry.RegisterGauge("xmysql_transactions_active", "Currently active transactions.", "isolation_level")
	registry.RegisterCounter("xmysql_transactions_committed_total", "Committed transaction count.", "isolation_level")
	registry.RegisterCounter("xmysql_transactions_rolled_back_total", "Rolled-back transaction count.", "isolation_level", "reason")

	registry.RegisterCounter("xmysql_lock_waits_total", "Lock wait events.", "lock_type", "resource_type")
	registry.RegisterHistogram("xmysql_lock_wait_duration_ms", "Lock wait duration in milliseconds.", nil, "lock_type", "resource_type")
	registry.RegisterGauge("xmysql_long_transactions_active", "Current long transaction count.", "level")

	registry.RegisterCounter("xmysql_recovery_runs_total", "Crash recovery run count.", "result")
	registry.RegisterCounter("xmysql_recovery_failures_total", "Crash recovery failure count.", "stage")
	registry.RegisterGauge("xmysql_checkpoint_dirty_pages", "Dirty pages tracked by checkpoint subsystem.", "space")
	registry.RegisterCounter("xmysql_checkpoint_runs_total", "Checkpoint execution count.", "result")
}
