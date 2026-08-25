# P0-D Observability Production Readiness Plan

## Purpose

This plan defines the P0-D observability evidence required before the project can claim production-readiness for monitoring, logging, metrics, and alerts.

The repository already contains scattered internal statistics and alert-like structures, but it does not yet have a unified production observability surface or archived acceptance evidence.

## Current status

Status: open.

Known foundations found in the repository:

- Internal `GetStats` methods exist across storage, page, checkpoint, group commit, history list, insert buffer, and B+Tree-related components.
- Long transaction detection and alert concepts are documented and partially implemented.
- Query optimizer and protocol layers contain statistics-related structures.
- Documentation references Prometheus and slow query logging as desired capabilities.

Known missing evidence:

- Unified metrics registry or exporter.
- Prometheus-compatible endpoint or textfile output.
- Slow query log format and sample output.
- Error log field contract.
- Alert rule definitions.
- Alert drill evidence.
- Dashboard or machine-readable metrics report.
- Acceptance report archived under `reports/`.

## Acceptance target

P0-D can only be marked accepted when all of the following are true:

- A documented metric catalog exists.
- Metrics can be exported in a machine-readable format.
- Core database metrics are covered: QPS, latency, error rate, active connections, active transactions, lock waits, recovery counters, checkpoint counters.
- Slow query logging is defined and has sample evidence.
- Error logging fields are defined and have sample evidence.
- Alert rules are defined for at least long transaction, high error rate, high latency, and recovery failure.
- Alert drill output is archived.
- An observability smoke report is archived under `reports/`.
- Final default regression gate passes after observability-related code changes.

## Metric catalog

Minimum P0-D metric set:

| Metric | Type | Purpose |
|---|---|---|
| `xmysql_queries_total` | counter | Total SQL/query requests accepted by the server |
| `xmysql_query_errors_total` | counter | Total query execution errors |
| `xmysql_query_latency_ms` | histogram or summary | Query latency distribution |
| `xmysql_connections_active` | gauge | Currently active client connections |
| `xmysql_transactions_active` | gauge | Currently active transactions |
| `xmysql_transactions_committed_total` | counter | Committed transaction count |
| `xmysql_transactions_rolled_back_total` | counter | Rolled-back transaction count |
| `xmysql_lock_waits_total` | counter | Lock wait events |
| `xmysql_lock_wait_duration_ms` | histogram or summary | Lock wait duration distribution |
| `xmysql_long_transactions_active` | gauge | Current long transaction count |
| `xmysql_recovery_runs_total` | counter | Crash recovery runs |
| `xmysql_recovery_failures_total` | counter | Crash recovery failures |
| `xmysql_checkpoint_dirty_pages` | gauge | Dirty pages tracked by checkpoint subsystem |
| `xmysql_checkpoint_runs_total` | counter | Checkpoint execution count |

## Logging contract

### Slow query log fields

Minimum fields:

- `timestamp`
- `trace_id`
- `connection_id`
- `transaction_id`
- `database`
- `sql_digest`
- `duration_ms`
- `rows_examined`
- `rows_returned`
- `error_code`
- `status`

### Error log fields

Minimum fields:

- `timestamp`
- `level`
- `trace_id`
- `component`
- `operation`
- `error_code`
- `error_class`
- `message`
- `duration_ms`
- `connection_id`
- `transaction_id`

## Alert rules

Minimum alert rules:

| Alert | Condition | Severity |
|---|---|---|
| `XMySQLHighErrorRate` | query error rate exceeds threshold for 5 minutes | warning |
| `XMySQLHighP99Latency` | P99 query latency exceeds threshold for 5 minutes | warning |
| `XMySQLLongTransaction` | long transaction count is greater than 0 for threshold window | warning |
| `XMySQLRecoveryFailure` | recovery failure count increases | critical |
| `XMySQLCheckpointDirtyPagesHigh` | dirty pages exceed threshold | warning |
| `XMySQLLockWaitSpike` | lock wait count or duration spikes | warning |

## Recommended report artifacts

Each observability smoke run should generate:

```text
reports/observability_smoke_<timestamp>.log
reports/observability_smoke_<timestamp>.md
reports/observability_smoke_<timestamp>.json
```

Recommended JSON shape:

```json
{
  "generated_at": "2026-06-14T00:00:00Z",
  "run_id": "observability_smoke_<timestamp>",
  "status": "PASS",
  "evidence_type": "observability_smoke",
  "checks": [
    {
      "name": "metric_catalog_present",
      "status": "PASS",
      "expected": "minimum P0-D metric catalog is documented",
      "actual": "metric catalog exists"
    }
  ]
}
```

Minimum required fields:

- `generated_at`
- `run_id`
- `status`
- `evidence_type`
- `checks`
- per-check `name`
- per-check `status`
- per-check `expected`
- per-check `actual`

## Initial implementation strategy

Start with documentation and smoke evidence before building a full exporter:

1. Define the metric catalog and log contracts.
2. Add an observability smoke script that checks required docs/config artifacts exist.
3. Add a JSON verifier for the smoke report.
4. Add sample slow query and error log records.
5. Add alert rule templates.
6. Later wire live metrics to existing internal `GetStats` sources.

## Final approval boundary

Current status after creating this plan: P0-D is planned but not implemented.

Do not mark P0-D accepted until:

- observability smoke scripts exist,
- sample logs exist,
- alert rules exist,
- report JSON verifier exists,
- at least one report is generated and verified,
- live metric export is implemented or a documented production-equivalent export path exists,
- final `go test ./...` passes after implementation.

---

## 2026-06-14 artifact update

P0-D documentation/configuration smoke artifacts have been added.

Artifacts:

- `docs/observability/metrics_catalog.md`
- `docs/observability/sample_slow_query.log`
- `docs/observability/sample_error.log`
- `deploy/alerts/xmysql-p0-alerts.yml`
- `scripts/observability_smoke.ps1`
- `scripts/verify_observability_report.ps1`

Smoke command:

```powershell
./scripts/observability_smoke.ps1
```

Verifier command:

```powershell
./scripts/verify_observability_report.ps1 -Path reports/observability_smoke_<timestamp>.json
```

This is documentation/configuration smoke evidence. It does not replace live metrics export or alert delivery evidence.

---

## 2026-06-14 metrics registry foundation update

P0-D now has a reusable metrics registry foundation:

- `server/observability/metrics/registry.go`
- `server/observability/metrics/defaults.go`
- `docs/observability/metrics_registry_usage.md`

This does not complete P0-D because runtime integration and live export evidence are still required.

---

## 2026-06-14 metrics export smoke update

Metrics export smoke command:

```powershell
./scripts/metrics_export_smoke.ps1
```

Verifier:

```powershell
./scripts/verify_metrics_export_report.ps1 -Path reports/metrics_export_<timestamp>.json
```

Expected Prometheus textfile output:

```text
reports/metrics_export_<timestamp>.prom
```

The full P0 evidence suite runs this smoke automatically.

---

## 2026-06-14 metrics HTTP handler foundation update

Metrics HTTP handler usage:

- `docs/observability/metrics_http_handler_usage.md`

Code foundation:

- `server/observability/metrics/http.go`

The handler can expose a registry over `net/http`, but live server runtime mounting and endpoint evidence are still required for P0-D acceptance.

---

## 2026-06-15 metrics runtime recorder foundation update

Metrics runtime recorder usage:

- `docs/observability/metrics_runtime_recorder_usage.md`

Code foundation:

- `server/observability/metrics/runtime.go`

The recorder maps runtime events into the metric catalog. Live runtime integration remains required for final P0-D acceptance.

---

## 2026-06-15 structured logging foundation update

Structured logging usage:

- `docs/observability/structured_logging_usage.md`

Code foundation:

- `server/observability/logging/structured.go`

Smoke command:

```powershell
./scripts/structured_logging_smoke.ps1
```

Verifier:

```powershell
./scripts/verify_structured_logging_report.ps1 -Path reports/structured_logging_<timestamp>.json
```

Runtime query/error-path integration is still required for final P0-D acceptance.

---

## 2026-06-15 alert drill smoke update

Alert drill smoke command:

```powershell
./scripts/alert_drill_smoke.ps1
```

Verifier:

```powershell
./scripts/verify_alert_drill_report.ps1 -Path reports/alert_drill_<timestamp>.json
```

The smoke validates local alert rule presence and sample trigger logic. It does not prove live Alertmanager delivery.

---

## 2026-06-15 live metrics endpoint integration update

The default metrics registry and `/metrics` endpoint are now wired into the existing profiling HTTP listener.

Updated code:

- `server/observability/metrics/global.go`
- `server/net/mysql_server.go`

Remaining work:

- runtime paths must update the default runtime recorder,
- server endpoint scrape evidence must be archived under `reports/`.

---

## 2026-06-15 live metrics endpoint probe update

Metrics endpoint probe command:

```powershell
./scripts/metrics_endpoint_probe.ps1 -MetricsUrl http://127.0.0.1:<profile-port>/metrics
```

Verifier:

```powershell
./scripts/verify_metrics_endpoint_probe_report.ps1 -Path reports/metrics_endpoint_probe_<timestamp>.json
```

The probe requires a running server and a valid `/metrics` URL.

## 2026-06-15 Implementation note - Active connection runtime metric

xmysql_connections_active{listener="mysql"} has been connected to the MySQL session lifecycle. The implementation records the current sessionMap length after open, close, error cleanup, and COM_QUIT cleanup, which makes the gauge recover from duplicate or reordered cleanup paths better than increment/decrement-only accounting.

Validation still required:
- Start a server with profiling enabled.
- Open and close MySQL client sessions.
- Confirm /metrics exposes changing xmysql_connections_active values.
- Archive the endpoint probe report in eports/.

## 2026-06-21 P0-D delivery acceptance gate update

The delivery readiness audit now checks P0-D observability evidence directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run P0-D observability, metrics export, structured logging, and alert drill JSON reports to the delivery readiness audit.
- The audit requires these reports to exist and have `status = PASS`.
- The audit also requires a live metrics endpoint probe JSON with `status = PASS`.
- Because the full suite does not start a live server or generate a metrics endpoint probe by default, missing live endpoint evidence remains an explicit delivery blocker.

Delivery requirement:
- Start a server with profiling enabled.
- Run `scripts/metrics_endpoint_probe.ps1` against `/metrics`.
- Archive and verify a PASS endpoint probe JSON.
- Delivery remains `NOT_READY` until P0-D live metrics endpoint evidence exists.

## 2026-06-21 Explicit live endpoint evidence input update

The full P0 evidence suite now accepts an explicit live metrics endpoint evidence path.

Updated tooling:
- `scripts/run_p0_evidence_suite.ps1`
- `scripts/delivery_readiness_audit.ps1`

Behavior:
- `run_p0_evidence_suite.ps1` now supports `-P0DMetricsEndpointJson <path>`.
- The suite passes that path into `delivery_readiness_audit.ps1`.
- During full-suite execution, the delivery audit runs with `-DisableReportFallback`, so it will not silently pick historical latest reports.
- If `-P0DMetricsEndpointJson` is omitted, the live metrics endpoint gate remains a clear delivery blocker.

Recommended flow:

```powershell
./scripts/metrics_endpoint_probe.ps1 -MetricsUrl http://127.0.0.1:<profile-port>/metrics
./scripts/verify_metrics_endpoint_probe_report.ps1 -Path reports/metrics_endpoint_probe_<timestamp>.json
./scripts/run_p0_evidence_suite.ps1 -P0DMetricsEndpointJson reports/metrics_endpoint_probe_<timestamp>.json
```

Boundary:
- Standalone delivery audit still supports latest-report fallback for manual review convenience.
- Full-suite delivery review disables fallback to avoid accidentally reusing stale historical evidence.

## Focused P0-D live metrics endpoint evidence

`cmd/p0_metrics_endpoint` exposes the P0 metric catalog over HTTP for focused endpoint probing. The wrapper script starts the endpoint, waits for `/healthz`, runs the existing `/metrics` probe, verifies the probe report, and stops the endpoint:

```powershell
./scripts/generate_p0d_metrics_endpoint_evidence.ps1 -ReportDir reports
```

The delivery audit consumes the generated `metrics_endpoint_probe_<timestamp>.json` as `-P0DMetricsEndpointJson`.

The full P0 evidence suite can generate this endpoint probe automatically:

```powershell
./scripts/run_p0_evidence_suite.ps1 -GenerateP0DMetricsEndpointEvidence
```

Scope boundary: this focused endpoint proves `/metrics` reachability and required P0 metric-name presence for a local metrics endpoint. It does not prove the full XMySQL server process updates every metric under production traffic.

