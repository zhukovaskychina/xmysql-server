package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/zhukovaskychina/xmysql-server/server/observability/metrics"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "HTTP address for the focused P0 metrics endpoint")
	flag.Parse()

	registry := metrics.NewRegistry()
	metrics.RegisterP0Metrics(registry)
	recordSampleMetrics(registry)

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(registry))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})

	fmt.Fprintf(os.Stderr, "focused P0 metrics endpoint listening on http://%s/metrics\n", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		fmt.Fprintf(os.Stderr, "metrics endpoint failed: %v\n", err)
		os.Exit(1)
	}
}

func recordSampleMetrics(registry *metrics.Registry) {
	_ = registry.IncCounter("xmysql_queries_total", 1200, metrics.Labels{
		"database": "test",
		"status":   "ok",
	})
	_ = registry.IncCounter("xmysql_query_errors_total", 3, metrics.Labels{
		"database":    "test",
		"error_class": "duplicate_key",
		"error_code":  "ER_DUP_ENTRY",
	})
	_ = registry.ObserveHistogram("xmysql_query_latency_ms", 12, metrics.Labels{
		"database":       "test",
		"statement_type": "select",
		"status":         "ok",
	})
	_ = registry.ObserveHistogram("xmysql_query_latency_ms", 245, metrics.Labels{
		"database":       "test",
		"statement_type": "insert",
		"status":         "ok",
	})
	_ = registry.SetGauge("xmysql_connections_active", 7, metrics.Labels{
		"listener": "focused",
	})
	_ = registry.SetGauge("xmysql_transactions_active", 2, metrics.Labels{
		"isolation_level": "repeatable_read",
	})
	_ = registry.IncCounter("xmysql_transactions_committed_total", 340, metrics.Labels{
		"isolation_level": "repeatable_read",
	})
	_ = registry.IncCounter("xmysql_transactions_rolled_back_total", 4, metrics.Labels{
		"isolation_level": "repeatable_read",
		"reason":          "client_abort",
	})
	_ = registry.IncCounter("xmysql_lock_waits_total", 9, metrics.Labels{
		"lock_type":     "next_key",
		"resource_type": "index",
	})
	_ = registry.ObserveHistogram("xmysql_lock_wait_duration_ms", 18, metrics.Labels{
		"lock_type":     "next_key",
		"resource_type": "index",
	})
	_ = registry.SetGauge("xmysql_long_transactions_active", 1, metrics.Labels{
		"level": "warning",
	})
	_ = registry.IncCounter("xmysql_recovery_runs_total", 1, metrics.Labels{
		"result": "pass",
	})
	_ = registry.IncCounter("xmysql_recovery_failures_total", 0, metrics.Labels{
		"stage": "redo",
	})
	_ = registry.SetGauge("xmysql_checkpoint_dirty_pages", 42, metrics.Labels{
		"space": "system",
	})
	_ = registry.IncCounter("xmysql_checkpoint_runs_total", 5, metrics.Labels{
		"result": "pass",
	})
}
