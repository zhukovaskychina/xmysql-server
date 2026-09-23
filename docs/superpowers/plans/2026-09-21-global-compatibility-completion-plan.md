# XMySQL Global Compatibility Completion Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在保持当前 InnoDB、集群和 Connector/J P0 基线稳定的前提下，补齐完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA、非 Connector/J 客户端矩阵以及 XA 与原生 binlog/复制/崩溃恢复的互操作能力。

**Architecture:** 先建立 MySQL 兼容性矩阵和统一结果形状，再分别扩展元数据/运行时观测、客户端协议验证和 XA/binlog 事务边界。所有新增能力都必须通过 engine 定向测试、全仓 Go、集群 smoke、客户端矩阵和 release-candidate gate；不把当前的兼容性测试通过误判为完整 MySQL 8.4 实现。

**Tech Stack:** Go、PowerShell、MySQL wire protocol、Connector/J、MySQL CLI、Go MySQL client、PyMySQL、Node.js mysql2、现有 `server/innodb/engine`、`server/protocol`、`server/net`、`server/replication` 和 `scripts/compatibility` 测试基础设施。

**Spec:** `docs/planning/P1_CAPABILITY_BACKLOG_20260715.md` 及本计划中的 Global Scope Decision。

## Global Scope Decision

- 纳入全局任务：完整 INFORMATION_SCHEMA、完整 PERFORMANCE_SCHEMA、非 Connector/J 客户端兼容矩阵、XA 与原生 binlog/复制/崩溃恢复互操作。
- 保持 P0 基线：MySQL 可启动、单机核心 CRUD、集群复制/故障切换、Connector/J 当前 139 项门禁。
- 暂不纳入本轮：FULLTEXT 及全文检索生态。
- 明确不处理：MyISAM、ARCHIVE、CSV 等非 InnoDB 引擎、非 InnoDB `REPAIR TABLE`、非 InnoDB 引擎转换。
- 所有测试和报告必须区分“已实现并验证”“部分实现”“未覆盖”；不得使用旧报告证明新代码已通过。
- 保留现有脏工作区，不执行 reset、clean、覆盖用户已有修改或提交未授权的 commit。

## Current global-task audit boundary (2026-09-22)

- `INFORMATION_SCHEMA` privilege/role work remains in scope. The existing role views
  (`ENABLED_ROLES`, `APPLICABLE_ROLES`, `ADMINISTRABLE_ROLE_AUTHORIZATIONS`,
  `ROLE_*_GRANTS`) are session-aware for the implemented role graph; the four
  account-grant views (`COLUMN_PRIVILEGES`, `TABLE_PRIVILEGES`,
  `SCHEMA_PRIVILEGES`, `USER_PRIVILEGES`) still need a dedicated session-visibility
  matrix covering current account, object visibility, grant option, role-derived
  visibility, and administrator behavior.
- The MySQL 8.4 P_S registry is intentionally not treated as complete merely
  because a table has a name and column list. The remaining component/runtime
  families without an authoritative xmysql source are tracked as partial:
  clone, firewall/keyring, component scheduler, NDB sync, table handles,
  thread-pool, UDF, mutex/cond/rwlock instrument instances, and group-replication
  members/actions/stats/configuration/communication. Empty rows are a truthful
  runtime result only when the corresponding component is absent; they are not a
  substitute for component interoperability.
- Non-Connector/J client coverage remains globally required. Go, PyMySQL and
  Node.js/mysql2 have local evidence; the MySQL CLI is still environment-unverified
  when `mysql.exe` is absent, and the full cross-client functional matrix remains
  open until that environment evidence exists.
- XA state-machine and xmysql-native binlog/recovery paths remain in scope, while
  official-MySQL bidirectional binlog/GTID/XA/crash-recovery interoperability is
  an external fixture gate, not something to infer from local tests.

## Priority and Exit Criteria

| Priority | Scope | Exit criteria |
|---|---|---|
| P0 baseline | Core server, cluster, Connector/J | Existing release gate remains `GO`; 139 Connector/J tests pass with 0 failures/errors/skips |
| P1-A | INFORMATION_SCHEMA/PERFORMANCE_SCHEMA | Inventory has no unclassified table; supported tables have MySQL-compatible columns, NULL/type/projection/filter semantics and runtime freshness tests |
| P1-B | Non-Connector/J clients | MySQL CLI, Go, Python and Node.js matrix passes connection/auth/prepared statement/transaction/metadata/error/charset cases |
| P1-C | XA/native replication | XA prepare/recover/commit/rollback, binlog position/GTID dump, replica apply, crash recovery and promotion preserve exactly-once transaction visibility |
| P2 | FULLTEXT | Explicitly deferred until the user reopens scope |
| Out of scope | Non-InnoDB engines and repair/conversion | Must not appear as a release blocker or remaining implementation task |

## Review Focus

- Metadata projection asks for an unsupported or newly added column: return the correct column shape and NULL/default behavior instead of silently falling through to ordinary SELECT.
- PERFORMANCE_SCHEMA reads concurrently with DML, metadata locks, waits and disconnects: counters must not expose torn rows or stale session identities.
- A non-JDBC client uses prepared statements, multi-result responses, EOF/OK variants, UTF-8/UTF-8MB4 and MySQL error codes: protocol behavior must remain client-compatible.
- XA crashes after prepare or after binlog append: restart/recovery must not expose a transaction twice or lose a committed transaction.
- Replica reconnects at a file/position or GTID boundary: partial events must be rejected or resumed at a transaction-safe boundary.

### Task 1: Freeze the compatibility inventory and scope registry — complete

**Files:**
- Modify: `docs/planning/P1_CAPABILITY_BACKLOG_20260715.md`
- Modify: `docs/superpowers/plans/2026-08-22-mysql-compatibility-development-plan.md`
- Create: `scripts/compatibility/compatibility_scope_matrix.ps1`
- Create: `reports/compatibility/README.md` section for matrix status

**Interfaces:**
- Produces a machine-readable matrix with `area`, `feature`, `status`, `priority`, `test_command`, `evidence_path`, and `out_of_scope`.

- [ ] **Step 1: Write the failing inventory check**

  Add cases for the required INFORMATION_SCHEMA/PERFORMANCE_SCHEMA table registry, client families, XA/binlog paths, and the explicit non-InnoDB exclusions.

- [ ] **Step 2: Run the inventory check**

  Run `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/compatibility/compatibility_scope_matrix.ps1`.

  Expected: FAIL for unclassified or duplicate feature entries.

- [ ] **Step 3: Implement the scope matrix**

  Read the handler dispatch in `server/innodb/engine/executor.go`, protocol entry points in `server/protocol`, network/binlog paths in `server/net`, and replication paths in `server/replication`; emit explicit statuses instead of inferring support from file existence.

- [ ] **Step 4: Verify the matrix**

  Run the same PowerShell command and require zero unclassified in-scope features and explicit `out_of_scope=true` for non-InnoDB work.

### Task 2: Complete INFORMATION_SCHEMA result shapes and metadata semantics — partial, native core implemented

**Files:**
- Modify: `server/innodb/engine/executor.go:6239-6400, 9000-10780`
- Modify: `server/innodb/engine/account_metadata_compatibility.go`
- Modify: `server/innodb/engine/executor_ddl_test.go`
- Modify: `server/innodb/engine/admin_compatibility_test.go`
- Create: `server/innodb/engine/information_schema_compatibility_test.go`

**Interfaces:**
- Consumes the existing `executeInformationSchemaMetadataSelect`, `informationSchemaMetadataFilters`, `projectInformationSchemaRow`, session visibility checks, durable `.frm` metadata, and persisted privilege/role metadata.
- Produces one canonical handler contract per supported I_S table: ordered columns, MySQL-compatible NULL/type conversion, projection, `WHERE`, `LIKE`, ordering, session visibility, and lock-safe reads.

- [ ] **Step 1: Write the table inventory tests**

  Cover `SCHEMATA`, `TABLES`, `COLUMNS`, `STATISTICS`, `KEY_COLUMN_USAGE`, `TABLE_CONSTRAINTS`, `CHECK_CONSTRAINTS`, `REFERENTIAL_CONSTRAINTS`, `PARTITIONS`, `VIEWS`, `ROUTINES`, `PARAMETERS`, `TRIGGERS`, `EVENTS`, `PLUGINS`, `ENGINES`, `PROCESSLIST`, privilege/role tables, and InnoDB tables currently dispatched by the executor.

- [ ] **Step 2: Run the tests to expose gaps**

  Run `go test ./server/innodb/engine -run 'TestInformationSchema' -count=1 -timeout 15m`.

  Expected: failures identify missing table dispatch, wrong columns, wrong NULL values, incomplete filters, or stale session visibility.

- [ ] **Step 3: Implement canonical metadata projections**

  Extend the existing dispatch instead of routing metadata tables through ordinary SELECT; define each table's ordered column list and row projector; preserve `NULL` for fields MySQL exposes as nullable; apply schema/table/column/constraint filters before projection; keep session-owned temporary tables isolated.

- [ ] **Step 4: Add lock and restart coverage**

  Verify reads during DDL/MDL waits, after table rename/drop, after restart, and across sessions with different privileges.

- [ ] **Step 5: Run the task gate**

  Run `go test ./server/innodb/engine -count=1 -timeout 30m` and record the report path in the scope matrix.

### Task 3: Complete PERFORMANCE_SCHEMA runtime coverage — partial, scoped runtime families implemented

**Files:**
- Modify: `server/innodb/engine/executor.go:6286-6400, 6481-9000`
- Modify: `server/session`
- Modify: `server/innodb/manager`
- Modify: `server/observability/metrics`
- Modify: `server/innodb/engine/performance_schema_compatibility_test.go` or create it if absent

**Interfaces:**
- Consumes real session IDs, connection attributes, statement history, transaction history, MDL/lock diagnostics, replication state, metrics and server lifecycle events.
- Produces consistent rows for statement/stage/transaction/wait/lock/thread/socket/file/memory/setup/status/variable tables with bounded history and deterministic filtering.

- [ ] **Step 1: Write runtime freshness tests**

  Execute a statement, acquire/release a metadata lock, start/commit/rollback a transaction, open/close a client session, and verify corresponding P_S rows and counters.

- [ ] **Step 2: Run the tests before implementation**

  Run `go test ./server/innodb/engine -run 'TestPerformanceSchema|TestInformationSchemaPerformance' -count=1 -timeout 15m`.

- [ ] **Step 3: Implement event lifecycle hooks**

  Register and retire session/thread identities on connect/reset/disconnect; record statement and transaction boundaries exactly once; expose lock waits from the existing diagnostics; cap history without invalidating current-event tables.

- [ ] **Step 4: Verify concurrency and restart behavior**

  Run parallel readers and writers, then restart the engine and confirm durable metadata remains valid while runtime-only counters reset according to MySQL semantics.

- [ ] **Step 5: Run full engine and release checks**

  Run `go test ./server/innodb/engine -count=1 -timeout 30m`, `go test ./... -count=1 -timeout 45m`, cluster smoke, and the release candidate gate.

### Task 4: Add the non-Connector/J client compatibility matrix — partial, CLI environment unverified

**Files:**
- Create: `scripts/compatibility/client_matrix.ps1`
- Create: `scripts/compatibility/client_matrix_cases.json`
- Create: `client_compatibility/mysql_cli/`
- Create: `client_compatibility/go/`
- Create: `client_compatibility/python/`
- Create: `client_compatibility/node/`
- Modify: `scripts/compatibility/release_candidate_gate.ps1`
- Modify: `docs/README.md` or the existing compatibility test documentation

**Interfaces:**
- Uses one isolated server/config/data directory per client case and emits one JSON report with client name, version, DSN, case, stdout/stderr, exit code, and normalized result.
- Covers MySQL CLI, Go MySQL driver, PyMySQL, and Node.js `mysql2`; missing optional runtimes produce an explicit `SKIPPED_ENVIRONMENT` result and cannot be mistaken for a pass.

- [ ] **Step 1: Define portable cases**

  Add connection/auth, database/table DDL, prepared statements, transactions, NULL/type conversion, UTF-8/UTF-8MB4, metadata queries, multi-result/error handling, reconnect, and cluster endpoint cases.

- [ ] **Step 2: Run the matrix in environment-diagnostic mode**

  Run `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/compatibility/client_matrix.ps1 -DiagnosticOnly` and record missing runtimes without changing server behavior.

- [ ] **Step 3: Implement isolated client runners**

  Reuse the existing isolated-server pattern from `scripts/compatibility/release_candidate_gate.ps1`; never reuse the live `data` directory and never embed credentials in files or logs.

- [ ] **Step 4: Verify protocol edge cases**

  Compare result rows, column metadata, SQLSTATE/error number, affected rows, generated keys, transaction visibility, character encoding, and prepared-statement behavior against the expected MySQL contract.

- [ ] **Step 5: Make the matrix a release check**

  Add a mandatory `client-matrix` check after JDBC. A missing runtime is `NO-GO` for the full matrix release but remains separately classified from a functional failure.

### Task 5: Close XA and native binlog interoperability — partial, official-MySQL fixture unavailable

**Files:**
- Modify: `server/replication/source.go`
- Modify: `server/replication/binlog_writer.go`
- Modify: `server/replication/binlog_events.go`
- Modify: `server/replication/native_decoder.go`
- Modify: `server/replication/runtime.go`
- Modify: `server/net/binlog_compatibility.go`
- Modify: `server/innodb/engine` XA/transaction handlers
- Create or modify: `server/replication/xa_native_interoperability_test.go`
- Create or modify: `server/net/binlog_compatibility_test.go`

**Interfaces:**
- Consumes XA transaction state, durable prepared-XA records, native binlog files/positions, GTID state, replica registry and crash-recovery hooks.
- Produces exactly-once mapping between XA commit state, binlog transaction boundaries, `COM_BINLOG_DUMP`/GTID consumption, replica apply and restart recovery.

- [ ] **Step 1: Write failure-injection tests**

  Cover crash after XA PREPARE, after binlog append, before commit marker, after commit marker, replica reconnect at transaction middle, duplicate event delivery, rotate, GTID resume and `XA RECOVER` after restart.

- [ ] **Step 2: Run the tests to establish current gaps**

  Run `go test ./server/replication ./server/net ./server/innodb/engine -run 'XA|Binlog|Replication|Recovery' -count=1 -timeout 30m`.

- [ ] **Step 3: Implement durable transaction mapping**

  Persist the XA/XID and binlog transaction relationship before exposing commit; reconcile prepared/in-doubt records during startup; make replay idempotent by GTID/XID and reject partial transactions at unsafe boundaries.

- [ ] **Step 4: Verify native protocol interoperability**

  Exercise file/position and GTID dump consumers, rotate files, reconnect replicas, promote a replica, and compare committed row visibility and duplicate suppression.

- [ ] **Step 5: Run crash and cluster gates**

  Run the replication package tests, crash-recovery matrix, cluster smoke, full Go suite, and release candidate gate.

### Task 6: Integrate, document and close the scoped global task — pending final external evidence

**Files:**
- Modify: `scripts/compatibility/release_candidate_gate.ps1`
- Modify: `docs/planning/P1_CAPABILITY_BACKLOG_20260715.md`
- Modify: `docs/superpowers/plans/2026-08-22-mysql-compatibility-development-plan.md`
- Modify: `reports/compatibility/README.md`

- [ ] **Step 1: Add required checks**

  Require I_S/P_S coverage, client matrix and XA/native replication evidence in the release report; keep Connector/J and cluster checks mandatory.

- [ ] **Step 2: Run the complete scoped gate**

  Run `go test ./... -count=1 -timeout 45m`, cluster smoke, client matrix, crash-recovery matrix, and `scripts/compatibility/release_candidate_gate.ps1` with all checks enabled.

- [ ] **Step 3: Reconcile documentation**

  Mark each scoped capability as `implemented`, `partial`, or `deferred`; mark non-InnoDB engine/repair/conversion as `out_of_scope`; keep FULLTEXT as deferred.

- [ ] **Step 4: Perform final safety review**

  Run `git diff --check`, verify no credentials or generated live-data artifacts were added, verify the current report revision, and preserve the dirty worktree without reset/clean/commit.
