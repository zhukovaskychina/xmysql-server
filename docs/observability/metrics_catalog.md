# XMySQL P0 Metrics Catalog

This catalog defines the minimum P0-D observability metrics required before production gray-release approval.

## Core query metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `xmysql_queries_total` | counter | `database`, `status` | Total accepted SQL/query requests |
| `xmysql_query_errors_total` | counter | `database`, `error_class`, `error_code` | Total query execution errors |
| `xmysql_query_latency_ms` | histogram | `database`, `statement_type`, `status` | Query execution latency in milliseconds |

## Connection and transaction metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `xmysql_connections_active` | gauge | `listener` | Active client connections |
| `xmysql_transactions_active` | gauge | `isolation_level` | Active transactions |
| `xmysql_transactions_committed_total` | counter | `isolation_level` | Committed transaction count |
| `xmysql_transactions_rolled_back_total` | counter | `isolation_level`, `reason` | Rolled-back transaction count |

## Lock and long-transaction metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `xmysql_lock_waits_total` | counter | `lock_type`, `resource_type` | Lock wait events |
| `xmysql_lock_wait_duration_ms` | histogram | `lock_type`, `resource_type` | Lock wait duration in milliseconds |
| `xmysql_long_transactions_active` | gauge | `level` | Current long transaction count |

## Recovery and checkpoint metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `xmysql_recovery_runs_total` | counter | `result` | Crash recovery run count |
| `xmysql_recovery_failures_total` | counter | `stage` | Crash recovery failure count |
| `xmysql_checkpoint_dirty_pages` | gauge | `space` | Dirty pages tracked by checkpoint subsystem |
| `xmysql_checkpoint_runs_total` | counter | `result` | Checkpoint execution count |

## Initial source mapping

| Metric area | Existing source candidates |
|---|---|
| Long transactions | transaction manager long transaction stats and alert channel |
| Checkpoint | checkpoint manager `GetStats` style APIs |
| Recovery | crash recovery drill reports and recovery statistics |
| Storage/page stats | wrapper page and storage `GetStats` style APIs |
| Group commit | group commit stats APIs |
| Compression | protocol and page compression stats APIs |

## P0-D acceptance note

This catalog is the required metric contract. P0-D is not complete until metrics are exported through a production-consumable path and smoke evidence is archived under `reports/`.
