# XMySQL Global Compatibility Completion Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在保持当前 InnoDB、集群和 Connector/J P0 基线稳定的前提下，补齐完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA、非 Connector/J 客户端矩阵以及 XA 与原生 binlog/复制/崩溃恢复的互操作能力。

**Architecture:** 先建立 MySQL 兼容性矩阵和统一结果形状，再分别扩展元数据/运行时观测、客户端协议验证和 XA/binlog 事务边界。所有新增能力都必须通过 engine 定向测试、全仓 Go、集群 smoke、客户端矩阵和 release-candidate gate；不把当前的兼容性测试通过误判为完整 MySQL 8.4 实现。

## Continuation 1342

- [x] P2 XA 一阶段 native binlog：`XA COMMIT ... ONE PHASE` 写入
  `XA_PREPARE_EVENT(one_phase=1)`，decoder/replica/promotion 保留该标记并立即提交。
- [x] 通过 replication 全包、XA/replication/recovery/promotion 定向回归。
- [ ] 保持 P2/P4 的统一物理提交协议和官方 MySQL fixture 互操作为后续全局任务。

**Tech Stack:** Go、PowerShell、MySQL wire protocol、Connector/J、MySQL CLI、Go MySQL client、PyMySQL、Node.js mysql2、现有 `server/innodb/engine`、`server/protocol`、`server/net`、`server/replication` 和 `scripts/compatibility` 测试基础设施。

**Spec:** `docs/planning/P1_CAPABILITY_BACKLOG_20260715.md` 及本计划中的 Global Scope Decision。

## Global Scope Decision

- 纳入全局任务：完整 INFORMATION_SCHEMA、完整 PERFORMANCE_SCHEMA、非 Connector/J 客户端兼容矩阵、XA 与原生 binlog/复制/崩溃恢复互操作。
- 保持 P0 基线：MySQL 可启动、单机核心 CRUD、集群复制/故障切换、Connector/J 当前 139 项门禁。
- 优先级重排：完整 I_S/P_S 语义为 P1；xmysql-native XA/binlog/recovery 收口为 P2；非 Connector/J 全量客户端矩阵及集群 endpoint 验证为 P3；官方 MySQL 双向 XA/binlog/GTID/复制/崩溃恢复互操作为 P4。它们都保留在全局任务中，但不阻塞 P0 基线交付。
- 暂不纳入本轮：FULLTEXT 及全文检索生态。
- 明确不处理：MyISAM、ARCHIVE、CSV 等非 InnoDB 引擎、非 InnoDB `REPAIR TABLE`、非 InnoDB 引擎转换。
- 所有测试和报告必须区分“已实现并验证”“部分实现”“未覆盖”；不得使用旧报告证明新代码已通过。
- 保留现有脏工作区，不执行 reset、clean、覆盖用户已有修改或提交未授权的 commit。

## Scope reconciliation (2026-09-25)

The following items are global requirements and must remain in the completion plan:

1. Full `INFORMATION_SCHEMA` and `PERFORMANCE_SCHEMA` table/column coverage, exact metadata,
   visibility rules, and authoritative runtime values.
2. The complete non-Connector/J client matrix, including MySQL CLI, Go, PyMySQL, Node.js/mysql2,
   and cluster endpoint reconnect/failover behavior.
3. XA interoperability with native binlog, GTID/applied state, replication, crash recovery,
   and promotion; the local xmysql-to-xmysql protocol and the official-MySQL fixture are separate
   acceptance gates.

The following remain explicitly excluded from this global task: `FULLTEXT` for the current round,
and non-InnoDB engines such as MyISAM, ARCHIVE, and CSV, including their engine-specific
`REPAIR TABLE` and conversion behavior. Do not downgrade the three global requirements merely
because the current local matrix lacks `mysql.exe`, protected credentials, or an official MySQL
fixture; record those as unverified or pending-external evidence instead.

## Scope reconciliation (2026-09-28)

- Full INFORMATION_SCHEMA/PERFORMANCE_SCHEMA remains a global P1 deliverable. Existing table
  shapes, selected runtime paths, and permission slices are evidence-backed partial progress,
  not completion of all MySQL 8.4 runtime/component semantics.
- The complete non-Connector-J client matrix remains a global P3 deliverable and is now marked
  implemented by the continuation1493 evidence: MySQL CLI through the Docker MySQL 8.4.11
  client image, Go mysql driver, PyMySQL, and Node mysql2 passed against both source and
  promoted endpoints.
- XA/native binlog/GTID/replication/crash/promotion interoperability remains global P2/P4 work;
  local state-machine and official-source read evidence do not close reverse, bidirectional,
  duplicate-delivery, crash-window, or promotion acceptance gates.
- FULLTEXT remains deferred. MyISAM, ARCHIVE, CSV, non-InnoDB REPAIR TABLE, and engine
  conversion remain explicitly out of scope.

### Continuation 1539

- [x] Close the local Performance Schema `setup_actors` multi-row lifecycle: most-specific
  matching for new foreground sessions, INSERT/DELETE with privilege checks, and TRUNCATE.
- [x] Verify the focused actor tests, the complete Performance Schema test family, and the
  full serial Go repository regression.
- [ ] Keep the aggregate P1 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA requirement partial until
  all remaining table, component, runtime, and permission semantics are independently closed.

## Continuation 1370 verification

- [x] Re-run the focused P1/P2 INFORMATION_SCHEMA, PERFORMANCE_SCHEMA, replication barrier,
  XA/native-binlog retry, promotion, and auto-failover gates.
- [x] Preserve `mysql` CLI as `unverified` and the official MySQL interoperability fixture as
  `pending_external` after confirming the required executables, protected credential, and Docker
  daemon are unavailable in the current environment.
- [ ] Do not mark the global objective complete until those external gates and the remaining
  full P1 semantics are independently verified.

## Continuation 1371

- [x] P1 INFORMATION_SCHEMA NDB registry: add the official
  `ndb_transid_mysql_connection_map` table shape, lowercase column names, and exact
  `BIGINT UNSIGNED`/`INT UNSIGNED`/`BIGINT UNSIGNED` non-null metadata.
- [x] Add a red/green regression proving the table is discoverable with an empty result
  when the NDB runtime is absent; the full INFORMATION_SCHEMA test family passes.
- [ ] Keep the aggregate P1 schema row partial because NDB runtime rows, complete component
  lifecycle semantics, permissions, and remaining P1/P2/P3/P4 gates are not closed.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1406

- [x] P1 `ROLE_ADMIN` grantability projection: `APPLICABLE_ROLES` and
  `ADMINISTRABLE_ROLE_AUTHORIZATIONS` now report applicable roles as grantable
  for a session holding dynamic `ROLE_ADMIN`, even without a persisted admin
  option on the role edge.
- [x] Red/green role regression, related role/privilege tests, and full engine
  regression passed. Evidence:
  `reports/compatibility/p1-information-schema-role-admin-grantability-current-continuation1406.txt`.
- [ ] Complete I_S/P_S table/field/runtime/component/permission semantics,
  P2 native binlog/GTID/XA/crash/promotion interoperability, the P3
  non-Connector-J client matrix, and the P4 official MySQL fixture remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1410

- [x] P2 tagged GTID request and basic event projection: preserve Binary v0
  and add MySQL 8.4 Binary v1/v2 `COM_BINLOG_DUMP_GTID` parsing for tags,
  repeated UUIDs, serialization varints, and delta-encoded intervals; emit
  native event type 42 for canonical `uuid:tag` identities.
- [x] Red/green tagged v1/v2 protocol regressions and the full `server/net`
  package regression passed. Evidence:
  `reports/compatibility/p2-native-gtid-tagged-dump-set-current-continuation1410.txt`.
- [ ] Full official tagged-GTID source/replica event emission, cross-server XA
  and crash/promotion interoperability, and the external MySQL fixture remain
  open. Complete I_S/P_S semantics and the P3 non-Connector-J client matrix
  remain global tasks; FULLTEXT remains deferred and non-InnoDB remains out of
  scope.

### Continuation 1411

- [x] Tagged `PREVIOUS_GTIDS_EVENT`: retain Binary v0 for untagged sets and
  emit/decode Binary v1 for tagged and mixed TSID sets, preserving `uuid:tag`
  identity and validating tag length/syntax.
- [x] Focused tagged previous-set regression plus full replication/net package
  regression passed. Evidence:
  `reports/compatibility/p2-native-previous-gtid-tagged-set-current-continuation1411.txt`.
- [ ] Binary v2 previous-set decoding, official mysqld source/replica fixture
  verification, and the wider native XA/crash/promotion interoperability
  remain open.

### Continuation 1412

- [x] Tagged GTID SQL functions: `GTID_SUBSET()` and `GTID_SUBTRACT()` now
  parse and preserve `uuid:tag:interval` groups, multiple intervals, and
  multiple tags per UUID.
- [x] Plan-level red/green tests and the engine SQL projection regression
  passed. Evidence:
  `reports/compatibility/p2-tagged-gtid-sql-functions-current-continuation1412.txt`.
- [ ] Full official source/replica, XA, crash/promotion, and external fixture
  interoperability remain open.

### Continuation 1413

- [x] Replication GTID text parser now accepts mixed untagged and multiple
  tagged TSID interval groups while preserving `uuid:tag` identity.
- [x] Tagged, multi-tag, and legacy merge regressions passed. Evidence:
  `reports/compatibility/p2-tagged-gtid-text-parser-current-continuation1413.txt`.

### Continuation 1414

- [x] Native `PREVIOUS_GTIDS_EVENT` now decodes MySQL tagged Binary v2:
  header/tag table, compressed TSID UUIDs, serialization varlen values, and
  delta-encoded interval boundaries including the first-boundary optimization.
- [x] Red/green focused regression and `server/replication` plus `server/net`
  package regressions passed. Evidence:
  `reports/compatibility/p2-native-previous-gtid-binary-v2-current-continuation1414.txt`.
- [ ] Live official mysqld source/replica, XA/binlog, crash-recovery/promotion
  interoperability and the full client/I_S/P_S matrices remain open; FULLTEXT
  remains deferred and non-InnoDB remains out of scope.

### Continuation 1415

- [x] Replication GTID text parsing now rejects sequence zero and reversed
  inclusive ranges instead of silently dropping them.
- [x] Red/green invalid-boundary regression and existing tagged/multi-tag range
  regressions passed. Evidence:
  `reports/compatibility/p2-gtid-text-invalid-boundary-current-continuation1415.txt`.
- [ ] Official source/replica, XA/binlog, crash/promotion, full I_S/P_S and
  client-matrix interoperability remain open; FULLTEXT remains deferred and
  non-InnoDB remains out of scope.

### Continuation 1416

- [x] Run the same client fixture against official MySQL 8.4 and an isolated
  xmysql server. Go, PyMySQL, and Node mysql2 passed on both sides.
- [x] Correct the shared multi-result aliases for MySQL 8.4 and make the
  Python/Node runners select the `mysql` database before the missing-table case.
  Evidence:
  `reports/compatibility/p3-client-baseline-official-vs-xmysql-current-continuation1416.txt`.
- [ ] The host still lacks `mysql.exe`, so the CLI remains unverified; cluster
  endpoint failover, broader client versions/boundaries, complete I_S/P_S,
  official XA/binlog interoperability, and crash/promotion interoperability
  remain open. FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1417

- [x] Compare official MySQL 8.4 table names with the local I_S/P_S registries.
  The six missing I_S names are all deferred InnoDB FULLTEXT tables; no official
  P_S table name is missing from the local registry. Evidence:
  `reports/compatibility/p1-official-table-registry-diff-current-continuation1417.txt`.
- [ ] Exact I_S/P_S column contracts, authoritative runtime values, component
  lifecycle, and privilege semantics remain open; this table-name audit does not
  promote P1 to complete. FULLTEXT remains deferred and non-InnoDB remains out
  of scope.

### Continuation 1407

- [x] Extend ROLE_ADMIN grantability to persisted `mysql.global_grants` and
  effective active/inherited role privilege sources when a local session does
  not carry an explicit dynamic-privilege parameter.
- [x] Focused red/green, related role/privilege tests, and full engine regression
  passed. Evidence:
  `reports/compatibility/p1-information-schema-role-admin-persisted-grant-current-continuation1407.txt`.
- [x] Full serial repository regression after this extension passed with
  `FINAL_EXIT_CODE=0`. Continue the remaining I_S/P_S, P2, P3, and P4 backlog;
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1408

- [x] Add a negative `WITH ADMIN OPTION` boundary regression: a directly granted
  parent role is grantable, while a child role visible only through inheritance
  remains non-grantable unless the child itself has ADMIN OPTION or the session
  has `ROLE_ADMIN`.
- [x] Evidence:
  `reports/compatibility/p1-information-schema-nested-role-admin-boundary-current-continuation1408.txt`.
- [ ] Complete I_S/P_S table/field/runtime/component/permission semantics,
  P2 native binlog/GTID/XA/crash/promotion interoperability, the P3
  non-Connector-J client matrix, and the P4 official MySQL fixture remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1409

- [x] Refresh P3 environment evidence: mysql-cli is unavailable; Go, PyMySQL,
  and Node/mysql2 are available; authenticated client and cluster endpoint
  scenarios remain skipped because `XMYSQL_CLIENT_PASSWORD` is unset.
- [x] Evidence:
  `reports/compatibility/p3-client-environment-current-continuation1409.txt`.
- [ ] Keep P3 as `partial`/`unverified`; continue local P2 work without treating
  environment availability as functional compatibility proof.

### Continuation 1376

- [x] P1 Performance Schema wait-summary lifecycle: thread, account, host,
  and user dimensions now use isolated completed-wait reset fingerprints;
  live waits and global summaries remain independent.
- [x] Focused isolation, wait-summary, and full engine regressions passed.
  Evidence:
  `reports/compatibility/p1-performance-schema-wait-summary-dimension-truncate-current-continuation1376.txt`.
- [ ] Complete I_S/P_S coverage, permissions, component lifecycle, P2 native
  binlog/GTID/XA/crash/promotion interoperability, and the P3 non-Connector-J
  client matrix remain open. FULLTEXT remains deferred and non-InnoDB remains
  out of scope.

### Continuation 1377

- [x] Refresh the P3 diagnostic boundary for Go, PyMySQL, Node.js/mysql2,
  MySQL CLI, and the cluster endpoint runner.
- [x] Go/Python/Node runtimes are available; MySQL CLI is unavailable and the
  protected-auth source/replica/promotion scenario is skipped because
  `XMYSQL_CLIENT_PASSWORD` is unset. Evidence:
  `reports/compatibility/p3-client-boundary-current-continuation1377.txt`.
- [ ] Keep the aggregate P3 matrix partial, MySQL CLI unverified, and cluster
  endpoint behavior partial until a real authenticated functional run exists.

### Continuation 1378

- [x] Attempt the functional non-Connector-J client matrix in the isolated
  runner.
- [x] The runner stopped before server startup because
  `XMYSQL_CLIENT_PASSWORD` was unavailable; no credentials were written and no
  client case is claimed as PASS. Evidence:
  `reports/compatibility/p3-client-functional-boundary-current-continuation1378.txt`.
- [ ] Keep P3 partial until a protected authenticated invocation is available.

## Continuation 1372

- [x] P1 thread-pool registry shape: replace the old placeholder columns for
  `tp_thread_group_state`, `tp_thread_group_stats`, and `tp_thread_state` in both
  INFORMATION_SCHEMA and PERFORMANCE_SCHEMA with the MySQL 8.4 native shapes.
- [x] Add a regression proving both schemas expose the same ordered shapes and empty
  rows when the Enterprise Thread Pool runtime is absent; the I_S/P_S family and full
  serial engine regression pass.
- [ ] Keep exact component runtime/type semantics and the aggregate P1 schema row open;
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

## Continuation 1299 update

- 对齐 `performance_schema.events_transactions_current`、`events_transactions_history`
  和 `events_transactions_history_long` 的 MySQL 8.4 官方 24 列形状，补齐
  `XA_STATE`、`SOURCE`，将 `XID_FORMAT` 正名为 `XID_FORMAT_ID`，并补齐事务事件
  字段的精确类型、长度、unsigned 与 NULLability 元数据。
- `XA_STATE` 由真实 session XA 状态机投影；旧 `XID_FORMAT` 作为显式查询别名保留，
  不进入 `SELECT *` 或 `INFORMATION_SCHEMA.COLUMNS`。
- 红测、事务/Performance Schema 专项、engine 包、受影响包和全仓串行回归均通过。
  证据：`reports/compatibility/p1-performance-schema-transaction-events-metadata-current-continuation1299.txt`。
- 完整 I_S/P_S 运行时/组件/权限语义、P2 native XA/binlog/replication/crash、P3
  全客户端矩阵和 P4 官方 fixture 仍未完成；FULLTEXT 继续 deferred，非 InnoDB 继续
  out of scope。

## Continuation 1300 update

- 对齐 `performance_schema.events_statements_current`、`events_statements_history`
  和 `events_statements_history_long` 的 MySQL 8.4 语句事件元数据，覆盖标识、计时、
  SQL/digest 文本、错误字段、语句计数、嵌套事件、CPU/内存和执行引擎字段。
- 修正官方字段名 `NESTING_EVENT_LEVEL`，不再通过 `SELECT *` 暴露仓库自定义的
  `NESTING_LEVEL`。
- 红测、I_S/P_S 专项、engine 包、受影响包和全仓串行回归均通过。证据：
  `reports/compatibility/p1-performance-schema-statement-events-metadata-current-continuation1300.txt`。
- 完整 I_S/P_S 运行时/组件/权限语义、P2 native XA/binlog/replication/crash、P3
  全客户端矩阵和 P4 官方 fixture 仍未完成；FULLTEXT 继续 deferred，非 InnoDB 继续
  out of scope。

## Continuation 1301 update

- 对齐 `performance_schema.events_statements_summary_by_digest` 的采样字段元数据：
  `QUERY_SAMPLE_SEEN` 现在投影为可空 `TIMESTAMP(6)`，
  `QUERY_SAMPLE_TIMER_WAIT` 投影为可空 `BIGINT UNSIGNED`，不再落入通用
  `VARCHAR(255)` 或有符号 `BIGINT` 兜底。
- 红测、P1 I_S/P_S 专项、engine/manager/net/replication 受影响包和全仓串行回归均通过。
  证据：`reports/compatibility/p1-performance-schema-digest-sampling-metadata-current-continuation1301.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；digest 容量/淘汰、完整
  采样生命周期、组件运行时、P2 native XA/binlog/replication/crash、P3 全客户端矩阵和
  P4 官方 fixture 仍未完成；FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

## Continuation 1302 update

- 对齐 `performance_schema.events_statements_histogram_by_digest` 和
  `events_statements_histogram_global` 的 `BUCKET_QUANTILE` 元数据：现在投影为
  MySQL 8.4 契约要求的非空 `DOUBLE`，不再落入 `VARCHAR(255)` 可空通用兜底。
- 红测、P1 I_S/P_S 专项、engine/manager/net/replication 受影响包和全仓串行回归均通过。
  证据：`reports/compatibility/p1-performance-schema-histogram-quantile-metadata-current-continuation1302.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；直方图运行时/容量/生命周期、
  组件来源、权限语义、P2 native XA/binlog/replication/crash、P3 全客户端矩阵和 P4
  官方 fixture 仍未完成；FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

## Continuation 1303 update

- 对齐两张 statement histogram 表的默认 `SELECT *` 形状：digest 表收敛为官方 9 列，
  global 表收敛为官方 6 列，移除仓库自定义的 `COUNT_BUCKET_AND_UPPER` 和 `COUNT_STAR`
  默认暴露；内部显式列投影仍保留这些值以兼容已有内部查询。
- 红测、P1 I_S/P_S 专项、engine/manager/net/replication 受影响包和全仓串行回归均通过。
  证据：`reports/compatibility/p1-performance-schema-histogram-column-shape-current-continuation1303.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；直方图运行时/容量/生命周期、
  组件来源、权限语义、P2 native XA/binlog/replication/crash、P3 全客户端矩阵和 P4
  官方 fixture 仍未完成；FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

## Continuation 1304 update

- 对齐两张 statement histogram 表的官方字段元数据：`DIGEST` 为可空
  `VARCHAR(64)`，`BUCKET_NUMBER` 为非空 `INT UNSIGNED`，计时/计数列为非空
  `BIGINT UNSIGNED`，`BUCKET_QUANTILE` 为非空 `DOUBLE(7,6)`。
- 红测、P1 I_S/P_S 专项、engine/manager/net/replication 受影响包和全仓串行回归均通过。
  证据：`reports/compatibility/p1-performance-schema-histogram-column-metadata-current-continuation1304.txt`。
- 该切片关闭该直方图家族的官方元数据契约，但不改变 P1 complete-table-registry 的
  partial 状态；完整 P_S 运行时/组件/权限、P2 native XA/binlog/replication/crash、
  P3 全客户端矩阵和 P4 官方 fixture 仍未完成；FULLTEXT 继续 deferred，非 InnoDB
  继续 out of scope。

## Continuation 1305 update

- 支持 `TRUNCATE TABLE performance_schema.events_statements_histogram_global` 和
  `events_statements_histogram_by_digest`，并为 global 与 by-digest 维护独立的运行时样本，
  保证两张表可以独立清零。
- 红测、P1 I_S/P_S 专项、metrics/engine/manager/net/replication 受影响包和全仓串行回归均通过。
  证据：`reports/compatibility/p1-performance-schema-histogram-truncate-current-continuation1305.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；摘要表隐式 reset、完整 P_S
  生命周期/组件/权限语义、P2 native XA/binlog/replication/crash、P3 全客户端矩阵和 P4
  官方 fixture 仍未完成；FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

## Continuation 1306 update

- 对齐 `performance_schema.events_statements_summary_by_digest.DIGEST` 的官方元数据：
  现在为可空 `VARCHAR(64)`；此前通用事件表分支错误地投影为 `CHAR(64)`。
- 红测、P1 I_S/P_S 专项、metrics/engine/manager/net/replication 受影响包和全仓串行回归均通过。
  证据：`reports/compatibility/p1-performance-schema-digest-summary-metadata-current-continuation1306.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；完整 P_S 字段/运行时/组件/权限、
  P2 native XA/binlog/replication/crash、P3 全客户端矩阵和 P4 官方 fixture 仍未完成；
  FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

## Continuation 1309 update

- 实现 `performance_schema.events_statements_summary_by_program` 的 `TRUNCATE TABLE`：
  保留 program 行身份，清零计数、计时、statement 子统计、错误、告警和行计数，并清理
  program history fallback。
- 红测、P1 I_S/P_S 专项、metrics/engine/manager/net/replication 受影响包和全仓串行回归均通过。
  证据：`reports/compatibility/p1-performance-schema-program-summary-truncate-current-continuation1309.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；完整 P_S 字段/运行时/组件/权限、
  P2 native XA/binlog/replication/crash、P3 全客户端矩阵和 P4 官方 fixture 仍未完成；
  FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

## Continuation 1308 update

- 将 `events_statements_summary_by_digest` 从普通 statement summary 的临时聚合中拆出，
  建立独立的 schema/digest 聚合状态；global statement summary 保持独立。
- 实现摘要表截断联动：digest 摘要截断会删除 digest 行并清空 digest histogram，但保留
  global 摘要；global 摘要截断会清零普通摘要并清空 global histogram，但保留 digest 摘要
  和 digest histogram。红测、P1 I_S/P_S 专项、metrics/engine/manager/net/replication
  受影响包和全仓串行回归均通过。证据：
  `reports/compatibility/p1-performance-schema-summary-truncate-isolation-current-continuation1308.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；完整 P_S 字段/运行时/组件/权限、
  P2 native XA/binlog/replication/crash、P3 全客户端矩阵和 P4 官方 fixture 仍未完成；
  FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

## Continuation 1307 update

- 对齐 `performance_schema.events_statements_summary_by_digest` 的时间与采样字段元数据：
  `FIRST_SEEN`、`LAST_SEEN`、`QUERY_SAMPLE_SEEN` 为 `TIMESTAMP(6) NOT NULL`，
  `QUERY_SAMPLE_TIMER_WAIT` 为 `BIGINT UNSIGNED NOT NULL`；此前摘要时间字段还会落入
  通用 `VARCHAR(255)` fallback。
- 红测、P1 I_S/P_S 专项、metrics/engine/manager/net/replication 受影响包和全仓串行回归均通过。
  证据：`reports/compatibility/p1-performance-schema-digest-summary-time-metadata-current-continuation1307.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；完整 P_S 字段/运行时/组件/权限、
  P2 native XA/binlog/replication/crash、P3 全客户端矩阵和 P4 官方 fixture 仍未完成；
  FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

## Current global-task audit boundary (2026-09-22)

- `INFORMATION_SCHEMA` privilege/role work remains in scope. The existing role views
  (`ENABLED_ROLES`, `APPLICABLE_ROLES`, `ADMINISTRABLE_ROLE_AUTHORIZATIONS`,
  `ROLE_*_GRANTS`) are session-aware for the implemented role graph; the four
  account-grant views (`COLUMN_PRIVILEGES`, `TABLE_PRIVILEGES`,
  `SCHEMA_PRIVILEGES`, `USER_PRIVILEGES`) now cover ordinary schema-scoped
  visibility and global `mysql.user` visibility, while the complete matrix still
  needs dedicated coverage for grant option, role-derived visibility, partial
  revokes, and administrator behavior.
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

## Current implementation map (2026-09-23, native XA continuation)

| Priority | Capability | Current assessment | Remaining work / exit condition |
|---|---|---|---|
| P0 | MySQL startup and InnoDB core CRUD | Implemented and release-gated | Keep the existing startup/core CRUD regression and release-candidate gate green |
| P0 | xmysql cluster replication and failover | Implemented for the current xmysql topology | Retain cluster smoke coverage; official MySQL Group Replication is not implied by this item |
| P0 | Connector/J | Protocol implementation and the isolated full gate are green: 139 tests, 0 failures, 0 errors, 0 skipped; `PerformanceTest` is isolated and completed all 8 cases in 614.011 seconds | Keep the isolated full gate repeatable and ensure later protocol/engine changes do not regress it |
| P0 | Query dispatch path | `SELECT 1` now goes through the configured SQL dispatcher in both enhanced network paths; the old hardcoded response shortcut was removed | Preserve the no-table privilege-probe exception while verifying the real executor result through dispatcher tests |
| P1-A | INFORMATION_SCHEMA | Core metadata, InnoDB dictionary/tablespace views, privilege/role views, filters and common extensions are implemented; persisted creation time is projected for `TABLES`, `PARTITIONS` and logical `FILES` rows; `COLUMNS` now derives integer/DECIMAL precision including the default `DECIMAL(10,0)`, temporal fractional precision, UNSIGNED type text, table-level character-set inheritance and character-set-specific octet length from persisted definitions, including source metadata propagated into ordinary view columns; `PARAMETERS` and function rows in `ROUTINES` now project type precision; `INNODB_BUFFER_POOL_STATS` now receives hit rate, young/old hit counts and resident-page count from both optimized and legacy buffer-pool managers; `INNODB_METRICS` now exposes live buffer-pool page/request/write counters when a manager provides them; `INNODB_TRX` now projects authoritative record-lock table/row counts when the lock manager is attached; the registry covers the supported MySQL 8.4 surface | Complete remaining field precision and authoritative runtime values across the other views. Keep `NULL` where xmysql has no source; do not fabricate InnoDB internal encodings, full statistics or FULLTEXT-only tables |
| P1-A | PERFORMANCE_SCHEMA | Core statement/stage/transaction/wait/lock/thread/socket/file/memory/status/variable/setup and replication views have runtime producers; transaction views now project available `TRX_ID` and XA XID components and record XA transaction timer starts; `table_handles` exposes explicit `LOCK TABLES` and transaction-scoped implicit leases, `threads` now projects recorder-backed current/high-water memory bytes, accepts per-thread `INSTRUMENTED/HISTORY` updates, and receives an OS-thread ID captured at the protocol execution boundary, socket summaries now consume transport-boundary read/write counts, timers and byte totals, global/session status and `status_by_account/status_by_host/status_by_user` expose runtime-backed common query counters, internal auth SQL is excluded from client counters, real protocol execution no longer double-records those counters, statement/program/transaction/record-lock/metadata-lock/table-I/O summary views retain instance-lifetime totals beyond bounded history windows, statement-derived stage/status summaries use the same lifetime source, `prepared_statements_instances` now records COM_STMT_EXECUTE duration/error/warning/row accounting from the live prepared-statement manager, stored-program summaries now retain authoritative rows-examined deltas from child statement summaries, actual clustered/partitioned table scans now project `NO_INDEX_USED` and predicate-bearing table scans now project `NO_GOOD_INDEX_USED` into statement history and lifetime summaries, completed statement events now project monotonic `TIMER_START/TIMER_END/TIMER_WAIT` windows while current events retain open-event NULL completion timers, real `ORDER BY` execution now projects `SORT_ROWS` and table-scan `SORT_SCAN`, real secondary-index range access now projects `SELECT_RANGE` and `SORT_RANGE`, the materialized full-scan JOIN path now projects `ROWS_EXAMINED`, `SELECT_SCAN` and `SELECT_FULL_JOIN`, and `data_locks` now exposes all manager-backed granted/waiting record-lock requests with transaction-qualified IDs | Add remaining runtime counters and exact event/lock/thread semantics, plus non-statement component sources. Reprepare/lock/tmp-table/sort counters other than `SORT_ROWS`/`SORT_SCAN`/`SORT_RANGE` and `NO_GOOD_INDEX_USED` remain zero or unavailable where no authoritative xmysql source exists; `SELECT_FULL_RANGE_JOIN` and `SELECT_RANGE_CHECK` remain zero until real indexed range-join planning exists; clone, firewall/keyring, component scheduler, NDB sync, thread-pool, UDF and group-replication component tables remain shape/empty-result or component-dependent until a real source exists |
| P2 | XA and xmysql-native replication | XA state transitions, durable recovery, native XA PREPARE/COMMIT/ROLLBACK emission and decode, native binlog dump, GTID filtering, logical/physical replica apply and local crash recovery are test-covered; prepared XA GTIDs remain unexecuted until commit; prepared transaction keys are durable and PREPARE retries are idempotent; keyed transaction identity and executed GTIDs share an atomic source state, keyed identities recover from the logical binlog, promotion persists replica GTIDs and imports unresolved prepared XA branches, native prepared-XA promotion is covered, replicas resume both logical and physical native transactions from durable relay events, including resume after XA PREPARE; independent replicas suppress duplicate XA delivery; identity-aware apply callbacks now receive the committed GTID; row-image replay converges by checking the durable post-image before reapplying; statement-only replay now appends and syncs a durable `replication_commit` record in the active journal before the final committed marker, and recovery preserves storage after marker publication fails | Unify storage/WAL, native binlog and GTID/applied-state publication into one physical commit protocol for multi-row and statement-only apply, then run the external fixture gate |
| P2 | XA and xmysql-native replication | XA state transitions, durable recovery, native XA PREPARE/COMMIT/ROLLBACK emission and decode, native binlog dump, GTID filtering, logical/physical replica apply and local crash recovery are test-covered; prepared XA GTIDs remain unexecuted until commit; prepared transaction keys are durable and PREPARE retries are idempotent; keyed transaction identity and executed GTIDs share an atomic source state, keyed identities recover from the logical binlog, promotion persists replica GTIDs and imports unresolved prepared XA branches, native prepared-XA promotion is covered, replicas resume both logical and physical native transactions from durable relay events, including resume after XA PREPARE; independent replicas suppress duplicate XA delivery; identity-aware apply callbacks now receive the committed GTID; row-image replay converges by checking the durable post-image before reapplying; replication statement/row replay now reuses one physical storage transaction across all source statements before the durable `replication_commit` record and final committed marker | Unify storage/WAL, native binlog and GTID/applied-state publication into one physical commit protocol for multi-row and statement-only apply, including the remaining ordinary-client transaction path, then run the external fixture gate |
| P3 | Non-Connector/J clients | Go MySQL driver, PyMySQL and Node.js/mysql2 have local runner coverage for the 9-case matrix, including a dedicated UTF-8/UTF-8MB4 case, multi-result/error and reconnect; a real source-stop/replica-promote/client-reconnect harness is now available, but the full matrix remains partial | Add MySQL CLI evidence when `mysql.exe` is available, run the protected-auth source/promote scenario, and extend the same cases to the remaining supported clients |
| P4 | Official MySQL interoperability | Not verified in the current environment; no official MySQL fixture is available | Verify bidirectional interoperability with an official MySQL fixture, including XA/binlog boundaries, GTID resume, duplicate delivery, crash-after-prepare/append/commit and promotion |
| Deferred | FULLTEXT | Deferred by scope decision | Do not block the current release; reopen only when explicitly requested |
| Out of scope | MyISAM/ARCHIVE/CSV, non-InnoDB `REPAIR TABLE`, engine conversion | Explicitly excluded | Do not put these back into the global remaining-task list |

The distinction above is intentional: an official table name and column list proves
catalog discovery only; a runtime producer and behavior test are required before a
table is considered semantically implemented. MySQL's official
[Performance Schema table reference](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-table-reference.html)
and [Information Schema table reference](https://dev.mysql.com/doc/refman/8.4/en/information-schema-general-table-reference.html)
remain the inventory baseline.

## Priority and Exit Criteria

| Priority | Scope | Exit criteria |
|---|---|---|
| P0 baseline | Core server, cluster, Connector/J | Existing release gate remains `GO`; 139 Connector/J tests pass with 0 failures/errors/skips |
| P1-A | INFORMATION_SCHEMA/PERFORMANCE_SCHEMA | Inventory has no unclassified table; supported tables have MySQL-compatible columns, NULL/type/projection/filter semantics and runtime freshness tests |
| P2 | xmysql-native XA/replication | XA prepare/recover/commit/rollback, binlog position/GTID dump, replica apply, crash recovery and promotion preserve exactly-once transaction visibility in xmysql-to-xmysql topology |
| P3 | Non-Connector/J clients | MySQL CLI, Go, Python and Node.js plus the supported client set pass connection/auth/prepared statement/transaction/metadata/error/charset cases |
| P4 | Official MySQL interoperability | An official MySQL fixture proves bidirectional XA/binlog/GTID/replication/crash-recovery behavior; absence of the fixture remains an explicit external gate, not a local pass |
| Deferred | FULLTEXT | Explicitly deferred until the user reopens scope |
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
- Consumes real session IDs, connection attributes, bounded statement history plus instance-lifetime statement summaries, transaction history, MDL/lock diagnostics, replication state, metrics and server lifecycle events.
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
- Create: `scripts/compatibility/client_cluster_endpoint.ps1`
- Modify: `scripts/compatibility/release_candidate_gate.ps1`
- Modify: `docs/README.md` or the existing compatibility test documentation

**Interfaces:**
- Uses one isolated server/config/data directory per client case and emits one JSON report with client name, version, DSN, case, stdout/stderr, exit code, and normalized result.
- Covers MySQL CLI, Go MySQL driver, PyMySQL, and Node.js `mysql2`; missing optional runtimes produce an explicit `SKIPPED_ENVIRONMENT` result and cannot be mistaken for a pass.

- [ ] **Step 1: Define portable cases**

  Add connection/auth, database/table DDL, prepared statements, transactions, NULL/type conversion, UTF-8/UTF-8MB4, metadata queries, multi-result/error handling, reconnect, and cluster endpoint cases. The cluster endpoint case must exercise source, replica catch-up, source stop, HTTP promotion, and a second client connection to the promoted SQL port.

- [ ] **Step 2: Run the matrix in environment-diagnostic mode**

  Run `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/compatibility/client_matrix.ps1 -DiagnosticOnly` and record missing runtimes without changing server behavior.

- [ ] **Step 3: Implement isolated client runners**

  Reuse the existing isolated-server pattern from `scripts/compatibility/release_candidate_gate.ps1`; never reuse the live `data` directory and never embed credentials in files or logs.

- [ ] **Step 4: Verify protocol edge cases**

  Compare result rows, column metadata, SQLSTATE/error number, affected rows, generated keys, transaction visibility, character encoding, and prepared-statement behavior against the expected MySQL contract.

- [ ] **Step 5: Make the matrix a release check**

  Add mandatory `client-matrix` and `client-cluster-endpoint` checks after JDBC. A missing runtime or protected authentication environment is `NO-GO` for the full matrix release but remains separately classified from a functional failure.

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

- [x] **Step 1: Write failure-injection tests**

  Cover crash after XA PREPARE, after binlog append, before commit marker, after commit marker, replica reconnect at transaction middle, duplicate event delivery, rotate, GTID resume and `XA RECOVER` after restart.

- [x] **Step 2: Run the tests to establish current gaps**

  Run `go test ./server/replication ./server/net ./server/innodb/engine -run 'XA|Binlog|Replication|Recovery' -count=1 -timeout 30m`.

- [x] **Step 3: Implement durable transaction mapping**

  Persist the XA/XID and binlog transaction relationship before exposing commit; reconcile prepared/in-doubt records during startup; make replay idempotent by GTID/XID and reject partial transactions at unsafe boundaries.

- [x] **Step 4: Verify native protocol interoperability — scoped xmysql-to-xmysql coverage complete**

  Exercise file/position and GTID dump consumers, rotate files, reconnect replicas, promote a replica, and compare committed row visibility and duplicate suppression. Current tests cover native prepared-XA promotion and two independent replicas consuming the same XA stream exactly once; official MySQL interoperability remains the separate P4 gate.

- [x] **Step 5: Run crash and cluster gates after the latest promotion changes — local code gates complete**

  Replication/engine/net focused tests, the full `go test -p 1 ./... -count=1 -timeout 45m` suite, and the current cluster smoke passed. The remaining replica storage-engine crash window and the environment-blocked client matrix prevent the overall release candidate from being GO.

### Task 6: Integrate, document and close the scoped global task — pending final external evidence

**Files:**
- Modify: `scripts/compatibility/release_candidate_gate.ps1`
- Modify: `docs/planning/P1_CAPABILITY_BACKLOG_20260715.md`
- Modify: `docs/superpowers/plans/2026-08-22-mysql-compatibility-development-plan.md`
- Modify: `reports/compatibility/README.md`

- [x] **Step 1: Add required checks**

  Require P1 I_S/P_S coverage and P2 native XA/replication evidence in the release report; keep P0 Connector/J and cluster checks mandatory. Keep both P3 `client-matrix` and `client-cluster-endpoint` mandatory. Record P3 client and P4 official-MySQL fixture checks as `pending_external` when their environment/fixture is unavailable; never convert them to a local pass. The gate now runs the scope-matrix validator, the expanded P1 metadata/privilege/role regression, and the P2 stable client-commit retry regression.

- [ ] **Step 2: Run the complete scoped gate**

  Run `go test ./... -count=1 -timeout 45m`, cluster smoke, client matrix, crash-recovery matrix, and `scripts/compatibility/release_candidate_gate.ps1` with all checks enabled.

- [ ] **Step 3: Reconcile documentation**

  Mark each scoped capability as `implemented`, `partial`, or `deferred`; mark non-InnoDB engine/repair/conversion as `out_of_scope`; keep FULLTEXT as deferred.

- [ ] **Step 4: Perform final safety review**

  Run `git diff --check`, verify no credentials or generated live-data artifacts were added, verify the current report revision, and preserve the dirty worktree without reset/clean/commit.

## Current audit boundary — Continuation 1166

The global task includes the complete INFORMATION_SCHEMA/PERFORMANCE_SCHEMA
catalog and runtime surface, the non-Connector/J client matrix, and XA/native
binlog/GTID/replication/crash-recovery interoperability. FULLTEXT remains
deferred. Non-InnoDB engines and their repair/conversion paths are explicitly
out of scope.

The remaining P2 storage gap is architectural, not a missing JSON field. The
replica currently persists relay state, applies storage, persists an
`appliedTransactions` marker, and then replaces the executed GTID state. The
engine's identity-aware replay path executes a session transaction, but each
ordinary DML still opens and commits its own storage transaction; the outer
transaction journal provides compensating rollback rather than one storage
commit spanning the complete source transaction. Closing the storage-page to
replica-state crash window therefore requires a unified storage transaction /
replication commit protocol and a failure-injection gate. Until that protocol
exists, P2 remains `partial` even though xmysql-to-xmysql replay, promotion,
native binlog consumption, XA recovery, and marker-recovery tests pass.

The current P3 diagnostic run found Go, PyMySQL, and Node.js available, but no
`mysql.exe` and no protected `XMYSQL_CLIENT_PASSWORD`; the client and cluster
endpoint gates therefore remain environment-blocked rather than passed.

## Current audit boundary — Continuation 1167

The role-visibility audit found that `mandatory_roles` was discoverable through
`APPLICABLE_ROLES` but could be removed from a session by `SET ROLE NONE`.
The executor now reapplies configured mandatory roles when loading the account
context and when executing `SET ROLE`, so the existing privilege and
`ENABLED_ROLES` paths see the same always-enabled role set. The regression is
covered by `TestMandatoryRolesRemainActiveAfterSetRoleNone`.

This closes one P1 role semantic, not the whole P1 surface. The remaining P1
work is still complete I_S/P_S table/runtime coverage and the rest of the
privilege/role visibility matrix; P2/P3/P4 remain as described above.

## Current audit boundary — Continuation 1168

The xmysql-to-xmysql replication path now has a durable recovery protocol for
identity-aware apply. A replay session uses a stable journal ID derived from
the source transaction identity; the journal is synced before the commit
marker, the marker is written after storage commit, and the journal is cleared
only afterward. A later apply with the same identity skips once the marker is
present. If marker persistence is fault-injected before completion, startup
recovery compensates the durable journal and the source transaction can be
applied again.

This closes the statement-only duplicate-apply window covered by local
failure-injection tests. It does not prove a single physical commit across
storage pages, replica state, native MySQL binlog, and GTID state; official
MySQL interoperability remains P4 and the deeper storage/WAL coordination
remains P2 partial.

## Current audit boundary — Continuation 1169

The official MySQL 8.4 table inventories were rechecked after the P2 change.
Within the selected scope, the I_S registry covers the general, privilege,
role, InnoDB non-FULLTEXT, thread-pool, connection-control, and firewall
directories. NDB-only directories remain outside the non-InnoDB scope and the
`INNODB_FT_*` directories remain deferred with FULLTEXT.

The P_S registry plus dedicated handlers cover the official 8.4 table names.
Tables whose rows require an absent component or protocol—such as Clone,
keyring, firewall, Group Replication, NDB, and component scheduler—keep
shape-correct empty results instead of fabricated runtime data. The remaining
P1 work is therefore semantic precision, visibility/filter behavior, and
authoritative runtime sources rather than blindly adding table names.

## Current audit boundary — Continuation 1170

The P1 metadata pass now projects `INFORMATION_SCHEMA.KEYWORDS.RESERVED` as
the MySQL 8.4 integer 1/0 contract and recognizes both numeric and boolean
predicates. `KEYWORDS`, `ST_UNITS_OF_MEASURE`, and
`ST_SPATIAL_REFERENCE_SYSTEMS` no longer fall back to generic metadata for
their verified column types, lengths, unsigned flags, and NULLability. The
new regressions were written before the implementation and the complete
`TestInformationSchema` suite passes. Remaining P1 gaps are still the other
unverified field contracts, visibility/filter edges, and runtime sources.

The same pass also corrected `ST_GEOMETRY_COLUMNS` to the MySQL 8.4 column
shape: `TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_NAME`, `COLUMN_NAME`,
`SRS_NAME`, `SRS_ID`, and `GEOMETRY_TYPE_NAME`. The previous bounding-box
columns were not part of the official 8.4 view and are no longer projected;
the exact `SELECT *` order is covered by regression tests.

The same metadata pass also gives `RESOURCE_GROUPS` its verified MySQL 8.4
field definitions (`VARCHAR(64)`, `ENUM('SYSTEM','USER')`, `TINYINT(1)`,
`VARCHAR(1024)`, and `INT`). Virtual Information Schema metadata now reports
the base `DATA_TYPE` separately from parameterized `COLUMN_TYPE`, as required
for the enum and boolean-backed fields.

`COLUMN_STATISTICS` now also projects its verified dictionary contract:
`SCHEMA_NAME`, `TABLE_NAME`, and `COLUMN_NAME` are `VARCHAR(64) NOT NULL`,
while `HISTOGRAM` is `JSON NOT NULL`. This only tightens metadata; the
existing `ANALYZE TABLE` histogram producer and durable sidecar remain the
runtime source.

Continuation 1173 verification is green: the combined Information Schema /
Performance Schema suite passed in 41.761s, and the serial full-repository
suite passed with engine 181.356s, manager 7.338s, net 7.885s, and
replication 2.657s. The refreshed scope matrix remains 24 in-scope items and
2 explicitly out-of-scope items; this does not close the partial external
client, native MySQL interop, or component-runtime gates.

The deprecated `PROFILING` view now also uses the verified MySQL 8.4 field
contract: integer query/sequence and counter fields, `VARCHAR(30/20)` state
and source fields, and `DECIMAL(9,6)` duration fields with the documented
NULLability. Its runtime rows still come from xmysql statement history.

The same P1 metadata pass now gives `USER_ATTRIBUTES` its native MySQL 8.4
shape: `USER CHAR(32) NOT NULL`, `HOST CHAR(255) NOT NULL`, and
`ATTRIBUTE JSON NULL`. The persisted account attribute rows, visibility rules,
and attribute filters remain backed by the existing account compatibility
handler. A red/green regression first captured the prior generic
`VARCHAR(255) NULL` metadata and then verified the corrected contract; the
combined I_S/P_S suite passed in 42.655s.

The next P1 pass corrected the Performance Schema setup and connection-summary
metadata against the MySQL 8.4 definitions. `accounts`, `hosts`, and `users`
now expose the native CHAR/ASCII and BIGINT/unsigned contracts; the setup
consumer, instrument, actor, object, meter, and metric tables expose their
native enum/set/VARCHAR shapes and NULLability. `setup_instruments` now uses
`FLAGS` and `DOCUMENTATION` instead of the stale `DOCUMENT` column. The
previous xmysql-specific `setup_threads` session listing was replaced with
the native thread-instrumentation configuration row and its ENABLED/HISTORY
updates. Focused metadata and runtime regressions pass.

The replication Performance Schema catalog was also corrected to the MySQL
8.4 definitions. The applier status view now uses its four native columns;
connection configuration/status and coordinator/worker status now expose the
native channel, error, timestamp, transaction, and retry columns. The live
xmysql snapshot fills only authoritative fields and leaves unavailable
MySQL-specific timestamps or group fields NULL. Focused metadata and
replication runtime tests pass; this improves the diagnostic contract without
claiming official MySQL replication interoperability.

The direct `ExecuteWithQuery` path also received a lifecycle ordering fix. Its
result-forwarding goroutine now completes `rows_sent` accounting before the
worker publishes statement history, eliminating the order-dependent zero-row
history entry seen by the direct executor regression. The lifecycle test passed
ten consecutive times, and the combined INFORMATION_SCHEMA/PERFORMANCE_SCHEMA
suite passed in 42.078s. The subsequent serial full-repository suite also
passed: engine 180.065s, manager 7.151s, net 7.696s, and replication 2.644s.
The refreshed matrix is
`reports/compatibility/scope-matrix-current-continuation1176.json`; remaining
partial, external, deferred, and out-of-scope entries are intentionally still
tracked rather than being promoted to implemented.

## Current audit boundary — Continuation 1178

The P1 role/account visibility pass found a real Host-pattern edge: an account
whose only matching definition used Host `%` was rejected because the session
account selector initialized its specificity score above the score assigned to
that valid pattern. The selector now keeps exact Host precedence while allowing
the wildcard account to be selected when it is the only match. All role
metadata projections (`APPLICABLE_ROLES`, `ADMINISTRABLE_ROLE_AUTHORIZATIONS`,
and `ROLE_*_GRANTS`) now reuse that same host-aware account selection instead
of requiring an exact Host string.

The red/green regression is
`TestRoleMetadataResolvesWildcardHostAccountLikePrivilegeChecks`; it failed
with an empty result before the change and passes after the fix. The focused
role/account/privilege and I_S/P_S regression command passed. This closes one
P1 visibility edge only; complete privilege semantics, component runtime
sources, P2 unified storage/replication commit, P3 external client evidence,
and the P4 official-MySQL fixture remain open.

## Current audit boundary — Continuation 1179

The Information Schema metadata pass now gives four existing InnoDB dictionary
views their native MySQL 8.4 field contracts instead of generic fallback shapes.
`INNODB_FIELDS` uses `BIGINT UNSIGNED`, `VARCHAR(64)`, and `INT UNSIGNED`;
`INNODB_VIRTUAL` uses an unsigned table identifier and unsigned integer position
fields; `INNODB_FOREIGN` uses `VARCHAR(193)` identifiers plus unsigned integer
counts/flags; and `INNODB_FOREIGN_COLS` uses `VARCHAR(193)` for the foreign-key
identifier, `VARCHAR(64)` for column names, and an unsigned integer position.

The red/green regression is
`TestInformationSchemaInnoDBDictionaryColumnsUseNativeMetadata`. The complete
Information Schema suite passed in 23.574s. This is a metadata precision slice,
not closure of the full InnoDB catalog/runtime matrix: complete table sources,
privilege visibility, P2 native XA/binlog/GTID interoperability, P3 external
client evidence, and the P4 official MySQL fixture remain open.

## Current audit boundary — Continuation 1180

The full-repository regression exposed a real concurrency defect in the
dispatcher-side mock session used by asynchronous message handling. Its
`params` map was accessed directly by `SetParamByName` and `GetParamByName`,
so the message-bus integration path could crash with a concurrent map read/write
failure depending on package/test ordering. The session now protects parameter
initialization, reads, and writes with an `RWMutex`.

The red/green regression is
`TestEnhancedMockMySQLServerSessionConcurrentParamAccess`; it reproduced
`concurrent map writes` before the lock and passed twenty consecutive runs after
the fix. The message-bus integration test passed ten consecutive runs, and the
serial repository suite `go test -p 1 ./... -count=1 -timeout 45m` completed with
all packages passing. The race-enabled variant could not start because the
current environment has `CGO_ENABLED=0`; this is recorded as an environment
limitation, not as positive race-detector evidence.

## Current audit boundary — Continuation 1181

The InnoDB dictionary metadata pass now also gives
`INFORMATION_SCHEMA.INNODB_INDEXES` its native field contract: unsigned BIGINT
identifiers, `VARCHAR(193)` index names, and non-null INT fields for index type,
field count, root page, space, and merge threshold. The existing index rows and
filters remain backed by the durable table/index catalog.

The `innodb_indexes` child case of
`TestInformationSchemaInnoDBDictionaryColumnsUseNativeMetadata` first failed
against the generic BIGINT fallback and then passed after the registry update.
The affected Information Schema/InnoDB metadata regression passed in 10.175s.
`INNODB_COLUMNS` default-value/version precision remains a separate slice because
its binary/default representations need stronger type evidence before being
promoted from the current conservative contract.

## Current audit boundary — Continuation 1182

The InnoDB column dictionary projection now follows the official MySQL 8.4
`INNODB_COLUMNS` dynamic-table shape: `TABLE_ID`, `NAME`, `POS`, `MTYPE`,
`PRTYPE`, `LEN`, `HAS_DEFAULT`, and nullable `DEFAULT_VALUE`. Project-only
`DEFAULT_VALUE_UTF8`, `VERSION`, and `HAS_NO_DEFAULT` fields were removed from
the projection because they are not part of the official 8.4 dynamic table.

The metadata regression first observed the old generic eleven-column shape and
then passed after the registry and row projection were corrected. The focused
`innodb_columns` case passed, and the combined Information Schema/Performance
Schema regression passed in 42.690s. `PRTYPE` and the binary representation of
instant-add `DEFAULT_VALUE` remain value-semantics work; the current NULL values
are intentionally not counted as complete InnoDB dictionary compatibility.

## Current audit boundary — Continuation 1183

`INNODB_COLUMNS.PRTYPE` now projects the portable InnoDB encoding for common
persisted SQL definitions: the MySQL field-type byte, NOT NULL/UNSIGNED/binary
flags, and known collation IDs. A red/green regression covers signed NOT NULL
integers, unsigned BIGINT, VARCHAR, and VARBINARY values. The combined
Information Schema/Performance Schema suite passed in 44.969s.

Unknown types and collations intentionally remain NULL. MySQL's instant-add
`DEFAULT_VALUE` is an internal binary dictionary value, so the project does not
claim completion by serializing the SQL default literal; that remains an open
P1 value-semantics slice.

## Current audit boundary — Continuation 1184

The xmysql-native replication marker window is now guarded by the active
transaction journal. After storage commit, a replication transaction appends and
syncs a `replication_commit` record before publishing the separate committed
marker. Recovery recognizes that record and preserves the DML when marker
publication fails; duplicate apply after recovery or restart is suppressed by
the same transaction identity. The focused engine and replication suites pass.

This is an internal recovery protocol improvement, not proof of one physical
commit point across storage pages, native binlog, GTID/applied state, and an
official MySQL XA participant. The P2 native/external interoperability work
therefore remains partial until that protocol and the official fixture gate are
closed.

## Current audit boundary — Continuation 1185

The P0 Connector/J gate was rerun with a fresh isolated server and data
directory. The ten non-performance test classes passed 131/131, and the
separately isolated `PerformanceTest` passed 8/8 in 614.011 seconds. The
combined evidence is 139/139 tests with zero failures, errors, or skips:
`reports/compatibility/connectorj-full-current-continuation1185/connectorj-report.json`.

The compatibility scope matrix now records `connector-j-139-gate` as
implemented at P0. This closes the P0 client gate, but does not promote the
complete INFORMATION_SCHEMA/PERFORMANCE_SCHEMA registries, native
XA/binlog/GTID/crash-recovery interoperability, non-Connector/J external
client matrix, or the official MySQL fixture to complete. FULLTEXT remains
deferred and non-InnoDB engines remain explicitly out of scope.

## Current audit boundary — Continuation 1186-1187

The available non-Connector-J clients now have fresh authenticated evidence.
Go MySQL Driver, PyMySQL, and Node mysql2 each passed the required connection,
DDL/DML, prepared-statement, transaction, type, metadata, multi-result/error,
and reconnect cases. The local MySQL CLI executable is absent, so its row
remains unverified rather than being treated as a server failure.

The cluster endpoint harness also had a false-negative bug: the replication
status JSON omits an empty `last_error`, while strict PowerShell property access
treated that normal response as an exception. The harness now treats an absent
empty error as clean. Fresh Go, PyMySQL, and Node runs all passed source client
traffic, GTID catch-up, source stop, replica promotion, and promoted-endpoint
client traffic. Reports are under
`reports/compatibility/client-cluster-endpoint-current-continuation1187/`.

The matrix therefore promotes the three available client implementations to
implemented, while the aggregate P3 client rows remain partial until mysql CLI
is available and tested. P1 complete I_S/P_S runtime coverage, P2 native
MySQL/XA/binlog/GTID interoperability, and P4 official MySQL fixtures remain
open.

### Continuation 1208

- P1 错误汇总表管理操作补齐本地运行时闭环：`TRUNCATE TABLE performance_schema.events_errors_summary_*` 现在清空进程内错误聚合状态，但不删除有界的 `error_log` 记录；新增 `TestPerformanceSchemaErrorSummaryTruncateResetsRuntimeAggregates` 回归。
- 该实现覆盖当前引擎的运行时聚合生命周期，但尚未证明 MySQL 对 account/host/user/thread/global 五类汇总表在连接断开、身份保留和逐维度截断时的全部细节，因此 P1 完整 Performance Schema 语义仍保持 partial。官方规则参见 [MySQL 8.4 Error Summary Tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html)。
- P2 本地 XA/native binlog/复制状态机继续通过既有回归；由于当前环境仍没有 `mysql/mysqld/mysqladmin` 且 Docker daemon 不可用，原生 MySQL XA/binlog/复制/崩溃恢复互操作仍保持 pending_external，不把本地状态机通过写成官方兼容完成。

### Continuation 1209

- P1 对齐 MySQL 8.4 错误汇总表契约：五张 `events_errors_summary_*_by_error` 表由项目自定义的 `ERROR_COUNT/WARNING_COUNT` 改为官方 `SUM_ERROR_RAISED/SUM_ERROR_HANDLED`，并同步修正 SELECT 投影与 `INFORMATION_SCHEMA.COLUMNS` 元数据。
- `ERROR_NUMBER` 现在按官方 `INTEGER` 暴露；`ERROR_NAME`/`SQL_STATE` 的长度、`SUM_ERROR_*` 的 `BIGINT UNSIGNED NOT NULL`、`FIRST_SEEN/LAST_SEEN TIMESTAMP(0) NULL` 均有回归覆盖。官方表清单和汇总列定义见 [MySQL 8.4 Performance Schema Table Reference](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-table-reference.html) 与 [Error Summary Tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html)。
- 当前运行时仍只采集查询错误，`SUM_ERROR_HANDLED` 没有 SQL 异常处理器来源；未知错误的 `ERROR_NUMBER=0` NULL 聚合行、五类表逐维度截断保留规则仍属于 P1 partial，不能因为列形状已对齐而宣称完整语义。

### Continuation 1210

- P1 收口 `performance_schema.data_locks` 与 `data_lock_waits` 的 I_S 元数据：按 MySQL 8.4 官方定义补齐锁 ID `VARCHAR(128)`、引擎/锁状态 `VARCHAR(32)`、对象标识 `VARCHAR(64)`、可空 unsigned 事务/线程/事件 ID，以及非空 `OBJECT_INSTANCE_BEGIN` 字段。
- 新增 `TestInformationSchemaPerformanceSchemaDataLockMetadataUsesMySQL84Shapes`，与错误汇总表元数据测试共同纳入 P1 合并回归。运行时锁来源和完整 InnoDB 并发场景仍需继续验证，不因元数据对齐而宣称完整锁诊断语义。

### Continuation 1211

- P1 修正错误汇总表 `TRUNCATE` 运行时语义：现有 global/account/host/user/thread 聚合行不再被直接删除，而是保留行并将计数归零、时间清空；`error_log` ring buffer 仍保持独立不受影响。新增回归同时验证 global 行及 identity 行。
- 已知内部错误码现在投影为官方错误名（例如 `E_SCHEMA_TABLE_NOT_FOUND` 映射为 `ER_NO_SUCH_TABLE`），旧内部名称仍可用于过滤兼容；未知错误仍未完整实现 MySQL 的 `ERROR_NUMBER=0` NULL 聚合行，`SUM_ERROR_HANDLED` 也没有 SQL handler 运行时来源，因此错误采集仍为 P1 partial。
- 官方依据：[MySQL 8.4 Error Summary Tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html)。

## Current audit boundary — Continuation 1188

The InnoDB metadata precision slice now covers `INNODB_CACHED_INDEXES` with
the MySQL-compatible unsigned numeric and `VARCHAR(193)` contracts. The change
was driven by a red/green `INFORMATION_SCHEMA.COLUMNS` regression and followed
the same conservative rule as the earlier dictionary-table slices: only stable
field metadata was promoted; no missing runtime rows were fabricated.

The focused Information Schema and Performance Schema suites passed in 44.994s.
The global state is unchanged at the capability level: P1 remains partial for
the remaining auxiliary-table type/value semantics, complete runtime sources,
and privilege visibility; P2 remains partial/pending for native XA/binlog/GTID
and crash-recovery interoperability; P3 remains partial until the mysql CLI
and the full external-client matrix are available; P4 remains pending on the
official MySQL fixture. FULLTEXT and non-InnoDB engines remain explicitly
deferred/out of scope.

The refreshed matrix is
`reports/compatibility/scope-matrix-current-continuation1188.json`: 26 total
rows, 24 in scope, with 13 implemented, 8 partial, 1 unverified, 1
pending-external, 1 deferred, and 2 explicitly out of scope.

## Current audit boundary — Continuation 1189

The P1 InnoDB metadata precision pass now covers the stable `INNODB_TABLES`
and `INNODB_DATAFILES` contracts in addition to the earlier dictionary slices.
The implementation follows the persisted catalog sources already used by the
row producers; it does not synthesize missing runtime statistics. The combined
Information Schema/Performance Schema suite passed in 45.090s.

Remaining P1 work is concentrated in the auxiliary runtime tablespaces,
temporary-table, compression, buffer-page, metrics, transaction/lock, and
Performance Schema field/value semantics. The global matrix remains partial
until those authoritative sources and permission edges are closed.

## Current audit boundary — Continuation 1190

The temporary-table slice now follows the MySQL 8.4 dynamic-table contracts:
`INNODB_SESSION_TEMP_TABLESPACES` exposes `ID/SPACE/PATH/SIZE/STATE/PURPOSE`,
and `INNODB_TEMP_TABLE_INFO` exposes `TABLE_ID/NAME/N_COLS/SPACE`. Runtime
rows use the session-local temporary file mapping, actual file size, and `.frm`
column definitions rather than row-ordinal or NULL placeholders. The combined
P1 regression passed in 44.352s.

The temporary `TABLE_ID` is explicitly documented as an xmysql session-lifetime
identifier because the current ordinary-table path does not populate an
official InnoDB dictionary table ID. Remaining P1 work is still the other
auxiliary runtime tables and permission edges; P2 native XA/binlog/GTID/crash
recovery interoperability, P3 full non-Connector/J coverage, and P4 official
MySQL fixtures remain open. FULLTEXT stays deferred and non-InnoDB engines
stay out of scope.

## Current audit boundary — Continuation 1191

The `INNODB_TABLESPACES_BRIEF.FLAG` value now comes from the persisted
tablespace FSP flags instead of a constant zero. A regression cross-checks it
against `INNODB_TABLESPACES.FLAG` and verifies the single-tablespace type; the
focused BRIEF test and the combined Information Schema/Performance Schema
suite passed, with the latter completing in 44.476 seconds.

This closes one runtime-value gap only. Exact INFORMATION_SCHEMA metadata for
all BRIEF expressions, the remaining auxiliary runtime statistics, and
privilege/role visibility are still P1 work. The refreshed matrix is
`reports/compatibility/scope-matrix-current-continuation1191.json`: 26 total
rows, 24 in scope, with 13 implemented, 8 partial, 1 unverified, 1
pending-external, 1 deferred, and 2 explicitly out of scope. P2 native
XA/binlog/GTID/crash-recovery interoperability, P3 complete non-Connector/J
coverage, and P4 official MySQL fixtures remain open.

## Current audit boundary — Continuation 1194

The real-session permission boundary now rejects `INFORMATION_SCHEMA.INNODB_TRX`
and `INNODB_METRICS` queries without `PROCESS`, `SUPER`, or `ALL`, matching the
documented MySQL requirement while preserving the internal nil-session test
path. The focused permission regression and the combined P1 suite passed, with
the latter completing in 44.562 seconds.

This is a two-table permission slice, not a complete privilege audit. Other
InnoDB dynamic tables, Performance Schema visibility, and role-derived global
privileges still require table-by-table verification. P2 native
XA/binlog/GTID/crash-recovery interoperability, P3 complete non-Connector/J
coverage, and P4 official MySQL fixtures remain open.

## Current audit boundary — Continuation 1193

The `INNODB_TRX` projection now includes the three MySQL 8.4 tail columns
missing from the previous 22-column shape: `TRX_ADAPTIVE_HASH_LATCHED`,
`TRX_ADAPTIVE_HASH_TIMEOUT`, and nullable `TRX_SCHEDULE_WEIGHT`. Their
metadata, row projection, and active-transaction regression are aligned; the
combined P1 suite passed in 43.670 seconds.

The implementation deliberately reports zero for the deprecated adaptive-hash
timeout and NULL for schedule weight when the transaction is not in a lock
wait, matching the documented observable contract without inventing internal
CATS state. Full transaction/lock semantics and PROCESS privilege behavior
remain P1 work. P2 native XA/binlog/GTID/crash-recovery interoperability, P3
complete non-Connector/J coverage, and P4 official MySQL fixtures remain open.

## Current audit boundary — Continuation 1192

The `INNODB_METRICS` metadata slice now follows the MySQL 8.4 dynamic-table
contract: string fields use the 64-character non-null shape, counter fields
use signed `BIGINT` with the documented nullability, average fields use
nullable `FLOAT`, and metric timestamps use nullable `DATETIME`. The red/green
metadata regression caught and removed the previous generic `VARCHAR(255)`
fallback; the combined Information Schema/Performance Schema and metrics
regression completed in 43.720 seconds.

This is metadata precision only. It does not claim that xmysql exposes the
complete MySQL monitor registry or the full enable/reset/process semantics.
P1 complete table sources and runtime semantics remain partial; P2 native
XA/binlog/GTID/crash-recovery interoperability, P3 complete non-Connector/J
coverage, and P4 official MySQL fixtures remain open.

## Current audit boundary — Continuation 1195

The buffer-pool dynamic views now expose the MySQL 8.4 column contracts for
`INNODB_BUFFER_POOL_STATS`, `INNODB_BUFFER_PAGE`, and
`INNODB_BUFFER_PAGE_LRU`. The page view includes `IS_STALE`; the LRU view uses
`COMPRESSED` instead of the previous non-native `PAGE_STATE`; and the pool
statistics names use the official `*_PER_THOUSAND_GETS` spellings. Numeric,
character, unsigned, and nullable metadata are covered by dedicated regressions.

Runtime projection now uses `YES/NO` for `IS_OLD`, NULL for states that the
current buffer-pool snapshot cannot prove, and safe non-null defaults for
native numeric fields. The three views reject real sessions without PROCESS;
the focused tests and the combined P1 suite completed successfully in 46.115
seconds. The refreshed matrix is
`reports/compatibility/scope-matrix-current-continuation1195.json`: 26 total
rows, 24 in scope, with 13 implemented, 8 partial, 1 unverified, 1
pending-external, 1 deferred, and 2 explicitly out of scope.

This closes a buffer-pool metadata/value/permission slice only. Complete
Information Schema and Performance Schema registration/runtime coverage, P2
native XA/binlog/GTID/crash-recovery interoperability, P3 full non-Connector-J
client coverage, and the P4 official MySQL fixture remain open. FULLTEXT stays
deferred and non-InnoDB engines plus repair/conversion stay out of scope.

## Current audit boundary — Continuation 1196

The InnoDB compression dynamic views now use the MySQL 8.4 shapes instead of
generic fallback metadata. `INNODB_CMP` and its reset view expose the six
`INT` columns; the per-index views expose `VARCHAR(192)` names plus the five
`INT` counters; and `INNODB_CMPMEM`/`_RESET` expose the six official columns,
including `PAGES_USED`, `PAGES_FREE`, `RELOCATION_OPS`, and
`RELOCATION_TIME`. Runtime projection and PROCESS checks were updated, and the
focused plus combined P1 regressions passed; the combined suite completed in
46.855 seconds.

The compression slice still does not claim official multi-page-size or
per-index counter fidelity, nor complete reset-state behavior. The refreshed
matrix is `reports/compatibility/scope-matrix-current-continuation1196.json`:
26 total rows, 24 in scope, with 13 implemented, 8 partial, 1 unverified, 1
pending-external, 1 deferred, and 2 explicitly out of scope. Complete I_S/P_S
coverage, P2 native XA/binlog/GTID/crash recovery, P3 full non-Connector-J
coverage, and P4 official MySQL fixtures remain open.

## Current audit boundary — Continuation 1197

`INNODB_TABLESPACES_BRIEF` now has an explicit MySQL-compatible metadata
contract: `SPACE BIGINT UNSIGNED`, `NAME VARCHAR(655)`, `PATH VARCHAR(512)`,
`FLAG INT`, and `SPACE_TYPE VARCHAR(10)`. Its runtime flag continues to come
from the persisted FSP flags used by the full tablespaces view, and real
sessions without PROCESS are rejected. The focused metadata and privilege
regressions passed.

This is one tablespace-view slice only; the global Information Schema and
Performance Schema registry/value/privilege audit remains partial. The matrix
is `reports/compatibility/scope-matrix-current-continuation1197.json`: 26 total
rows, 24 in scope, with 13 implemented, 8 partial, 1 unverified, 1
pending-external, 1 deferred, and 2 explicitly out of scope. P2 native
XA/binlog/GTID/crash recovery, P3 complete non-Connector-J coverage, and P4
official MySQL fixtures remain open.

## Current audit boundary — Continuation 1198

The Performance Schema thread/process-list slice now follows the MySQL 8.4
plugin-table definitions. `performance_schema.threads` has precise metadata for
all 24 columns, including the signedness and nullability of thread IDs and
memory counters, the `VARCHAR` lengths for process attributes, and the enum
contracts for instrumentation and execution state. The
`performance_schema.processlist` registry now includes the official
`EXECUTION_ENGINE` column with a source-backed `PRIMARY` value; the existing
xmysql diagnostic extensions remain explicit after the native columns.

The focused regression passed, and the combined Information Schema/Performance
Schema/runtime suite passed in 46.942 seconds. The refreshed matrix is
`reports/compatibility/scope-matrix-current-continuation1198.json` with 26
total rows and 24 in scope. This closes one precise P1 schema/runtime slice;
complete I_S/P_S table coverage and privilege semantics remain partial. P2
native XA/binlog/GTID/crash-recovery interoperability, P3 complete
non-Connector-J coverage, and P4 official MySQL fixtures remain open.

The full serial repository regression also passed with exit code 0:
`go test -p 1 ./... -count=1 -timeout 45m` (engine 187.861 seconds,
integration 1.006 seconds, manager 7.040 seconds, net 7.781 seconds,
replication 4.637 seconds). This is regression evidence for the current
change, not proof that the remaining compatibility areas are complete.

## Current audit boundary — Continuation 1199

The thread/process-list visibility regression now uses real session objects.
Without `PROCESS`, `performance_schema.processlist` exposes the current user
and same-user sessions but hides another user's session; with `PROCESS`, the
other user's row becomes visible. `performance_schema.threads` continues to
expose rows across users without a `PROCESS` gate, and its
`EXECUTION_ENGINE=PRIMARY` value remains source-backed. The focused regression
passed and the matrix was refreshed to
`reports/compatibility/scope-matrix-current-continuation1199.json`.

This is a permission/visibility slice only. Component-specific authoritative
runtime sources, complete I_S/P_S field/filter coverage, P2 native
XA/binlog/GTID/crash-recovery interoperability, P3 full non-Connector-J
coverage, and P4 official MySQL fixtures remain open.

## Current audit boundary — Continuation 1200

The Performance Schema registry and INFORMATION_SCHEMA metadata now expose the
official MySQL 8.4 shape for
`binary_log_transaction_compression_stats`: fourteen columns, including the
transaction counters, compression percentage, first/last transaction fields,
and microsecond timestamps. `SELECT *` column order is covered by regression.

The runtime rows are intentionally still empty until xmysql has a real
binlog/relay-log transaction-compression monitoring source. This slice does not
claim the compression counters, `TRUNCATE TABLE` reset behavior, or native
binlog interoperability. The focused tests passed and the refreshed matrix is
`reports/compatibility/scope-matrix-current-continuation1200.json`.

The full serial repository regression also passed with exit code 0:
`go test -p 1 ./... -count=1 -timeout 45m`; the engine package completed in
184.159 seconds and the remaining packages passed afterward. This is regression
evidence for the current changes, not evidence that the external MySQL fixture
or the remaining runtime compatibility scopes are complete.

Complete I_S/P_S field/filter/runtime coverage, P2 native
XA/binlog/GTID/crash-recovery interoperability, P3 full non-Connector-J
coverage, and P4 official MySQL fixtures remain open. FULLTEXT remains deferred
by scope, while non-InnoDB engines, repair, and engine conversion remain out of
scope.

## Current audit boundary — Continuation 1201

`performance_schema.log_status` now uses the MySQL 8.4 four-column contract:
`SERVER_UUID`, `LOCAL`, `REPLICATION`, and `STORAGE_ENGINES`. Its metadata and
runtime JSON keys are aligned, and real sessions require `BACKUP_ADMIN` before
the status row is exposed. The regression covers the shape, runtime projection,
and denied/allowed privilege paths.

The xmysql runtime does not currently expose a source-backed local binlog file
position, relay-log file/resource collector, or InnoDB log-resource snapshot.
The implementation therefore returns only known GTID state and empty JSON
structures for those unsupported sub-resources. This is a precise partial
runtime implementation, not a claim of full online-backup log-status
compatibility. The refreshed matrix is
`reports/compatibility/scope-matrix-current-continuation1201.json`.

The focused P1 suite passed, and the full serial repository regression also
passed with exit code 0: `go test -p 1 ./... -count=1 -timeout 45m`; the engine
package completed in 187.063 seconds and all remaining packages passed.

This slice also closes the statement-routing edge of the two related tables:
`TRUNCATE performance_schema.binary_log_transaction_compression_stats` returns
success, while `TRUNCATE performance_schema.log_status` returns an explicit
not-permitted error. The metadata compatibility path is now limited to read
statements, so DDL reaches its dedicated semantics. The compression table still
has no real transaction-compression collector to reset.

Complete I_S/P_S field/filter/runtime coverage, P2 native
XA/binlog/GTID/crash-recovery interoperability, P3 full non-Connector-J
coverage, and P4 official MySQL fixtures remain open.

## Current audit boundary — Continuation 1202

The metadata compatibility dispatcher now only claims read statements. This
prevents DDL such as `TRUNCATE performance_schema.log_status` from being
returned as a virtual SELECT result; the dedicated truncate implementation now
returns MySQL-compatible success/denial behavior for the two audited tables.

The focused I_S/P_S regression passed in 16.423 seconds. Package regressions
also passed for `server/replication` (23.039 seconds), `server/net` (17.546
seconds), and `server/dispatcher` (4.203 seconds). A first full serial run
failed because the C: drive exhausted its space during engine integration
tests. A second run with TEMP/TMP/GOCACHE/GOTMPDIR on D: remained in
`engine.test` for more than 29 minutes and was stopped while still responsive;
it is not counted as a full-repository pass.

The refreshed matrix is
`reports/compatibility/scope-matrix-current-continuation1202.json`.
Complete I_S/P_S field/filter/runtime coverage, P2 native
XA/binlog/GTID/crash-recovery interoperability, P3 full non-Connector-J
coverage, and P4 official MySQL fixtures remain open. FULLTEXT remains
deferred by scope, while non-InnoDB engines, repair, and engine conversion
remain out of scope.

## Current audit boundary — Continuation 1203

The `performance_schema.innodb_redo_log_files` metadata contract now matches
the MySQL 8.4 native table definition: signed non-null `BIGINT` LSN/size
fields, `VARCHAR(2000)` file name, `TINYINT` full flag, and `INT` consumer
level. Its existing source-backed redo snapshot executor remains unchanged.

The new metadata regression and the combined P1 suite passed; the latter
completed in 45.774 seconds. The official references are the MySQL 8.4
miscellaneous-table documentation and the 8.4 `storage/innobase/log/log0pfs.cc`
table definition. This is one P1 schema-precision slice, not completion of
all InnoDB/Performance Schema runtime sources.

## Current audit boundary — Continuation 1204

The `performance_schema.error_log` contract now uses the native MySQL 8.4
columns: `LOGGED`, `THREAD_ID`, `PRIO`, `ERROR_CODE`, `SUBSYSTEM`, and `DATA`.
The old project-only `TIMESTAMP` column is removed. Error events retain the
available query-boundary thread ID, and numeric internal priorities are
projected as the native `System/Error/Warning/Note` enum labels.

The focused error-log regression passed in 2.460 seconds. The combined P1
Information Schema/Performance Schema/runtime regression passed in 45.569
seconds. This closes a schema and projection mismatch; it does not claim a
full MySQL error-log ring buffer or complete P_S runtime implementation.

## Current audit boundary — Continuation 1205

The `performance_schema.component_scheduler_tasks` registry now uses the
MySQL 8.4 six-column contract: `NAME`, `STATUS`, `COMMENT`,
`INTERVAL_SECONDS`, `TIMES_RUN`, and `TIMES_FAILED`. The previous project-only
seven-column shape is removed. Information Schema metadata covers the native
lengths, enum values, signed interval, and unsigned counters.

The focused scheduler metadata and registry regression passed:
`go test -p 1 ./server/innodb/engine -run
'^(TestInformationSchemaVirtualComponentSchedulerTasksUsesMySQL84Shape|TestPerformanceSchemaRegistryCoversMySQL84VirtualTables)$'
-count=1 -timeout 20m`.

The Enterprise `scheduler` component is not part of this repository, so the
table intentionally returns an empty runtime task set. This is discovery and
schema compatibility only; no synthetic audit-log task is claimed. The
official references are the [MySQL 8.4 component_scheduler_tasks
documentation](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-component-scheduler-tasks-table.html)
and the [Scheduler Component
documentation](https://dev.mysql.com/doc/refman/8.0/en/scheduler-component.html).

Complete I_S/P_S field/filter/runtime coverage, P2 native
XA/binlog/GTID/crash-recovery interoperability, P3 full non-Connector-J
coverage, and P4 official MySQL fixtures remain open. FULLTEXT remains
deferred, while non-InnoDB engines, repair, and engine conversion remain out
of scope.

## Current audit boundary — Continuation 1213

P1 error-summary runtime accounting now distinguishes MySQL's
`SUM_ERROR_HANDLED` from warning counts. A caught nested SQL exception is
recorded once as raised and once as handled, and a direct `SIGNAL SQLSTATE`
exception handled by a stored-routine handler follows the same path. Direct
`SQLWARNING` signals remain outside the error-summary counters.

The initial implementation exposed a regression because suppressing child
error metrics also suppressed the child statement rows used by
`events_statements_summary_by_program`. The execution context now defers only
error aggregation for routine children and continues publishing statement
summary rows. The focused error-summary/program-summary regression passed, and
the full P1 Information Schema/Performance Schema/runtime gate passed in
46.755 seconds. Evidence is recorded in
`reports/compatibility/p1-error-summary-current-continuation1213.txt`.

This closes the repository-owned SQL-handler counter slice, not all P1
observability semantics. Component-dependent tables, complete I_S/P_S runtime
sources and every privilege/filter/lifecycle edge remain partial; P2 official
XA/binlog/GTID/crash-recovery interoperability remains pending external
fixture validation; P3 non-Connector-J coverage and P4 official fixtures
remain open. FULLTEXT remains deferred, while non-InnoDB engines, repair, and
engine conversion remain out of scope.

## Current audit boundary — Continuation 1206

`performance_schema.log_status` now consumes the existing local replication
source when one is configured. Its `LOCAL.binary_log_file` and
`LOCAL.binary_log_position` values come from the native binlog's durable file
and physical end position. The `REPLICATION` value keeps the MySQL 8.4 JSON
object shape with its `channels` array; because the runtime does not yet retain
local relay-log file/position metadata, an unknown channel set is represented
as `{"channels":[]}` rather than a project-specific shape or invented values.

The focused P_S regression passed, the combined P1 suite passed, and the local
P2 XA/native-binlog/replication gate passed. The crash-recovery manager matrix
also passed three consecutive repetitions. These results cover local behavior
only. No `mysql`, `mysqld`, or `mysqladmin` binary is available in the current
environment, and Docker cannot connect to its daemon, so official MySQL
XA/binlog/replication interoperability remains pending external fixture
validation.

Complete I_S/P_S field/filter/runtime coverage, P2 official
XA/binlog/GTID/crash-recovery interoperability, P3 full non-Connector-J
coverage, and P4 official MySQL fixtures remain open. FULLTEXT remains
deferred, while non-InnoDB engines, repair, and engine conversion remain out
of scope.

## Current audit boundary — Continuation 1207

Review of the MySQL 8.4 `storage/perfschema/table_log_status.cc` source
corrected two tentative assumptions from the previous slice. The native
`log_status` read gate checks `BACKUP_ADMIN`; this repository therefore does
not add a separate `SELECT` gate. The native `REPLICATION` value is also a
JSON object containing a `channels` array, so the empty runtime shape remains
`{\"channels\":[]}`.

The retained implementation is limited to behavior supported by both the
official source and local regressions: source-backed native binlog file/
position projection, the official JSON shape, and the `BACKUP_ADMIN` denial
path. It does not claim that every Performance Schema table has a complete
MySQL privilege and filter matrix. Complete I_S/P_S semantics, official
replication interoperability, and the non-Connector/J client matrix remain
open.
## Current audit boundary — Continuation 1212

P1 error-summary aggregation now follows the MySQL 8.4 unknown-error rule:
errors without a local MySQL error mapping are merged into one row with
`ERROR_NUMBER=0`, `ERROR_NAME=NULL`, and `SQL_STATE=NULL`, while their raised
and handled counters and first/last timestamps are aggregated per summary
dimension. Known internal errors continue to project official MySQL names and
numbers, and the existing compatibility filters remain accepted.

The new unknown-error regression and the focused error-summary regression pass.
The complete P1 Information Schema/Performance Schema/runtime gate passed in
47.535 seconds. The local P2 XA/native-binlog/replication gate also passed:
engine 14.675 seconds, net 2.395 seconds, and replication 2.602 seconds.
Connector/J remains backed by the existing 139-test PASS report at
`reports/compatibility/connectorj-full-current-continuation1185/connectorj-report.json`.

This closes the unknown-error-row slice only. `SUM_ERROR_HANDLED` still has no
SQL handler runtime source, exact per-dimension truncation/identity lifecycle
is not fully proven, complete I_S/P_S runtime sources remain partial, official
MySQL XA/binlog/replication interoperability remains pending external fixture
validation, P3 non-Connector-J coverage remains partial, and P4 remains
external. FULLTEXT stays deferred; non-InnoDB engines, repair, and engine
conversion remain out of scope. The official rule is documented in the
[MySQL 8.4 Error Summary Tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html).

## Current audit boundary — Continuation 1214

The P0 Connector/J row is corrected to match the latest isolated full-gate
evidence: all 139 tests passed with zero failures, errors, or skips; the eight
`PerformanceTest` cases completed in the isolated server run. The previous
NO-GO wording was stale and has been removed from the implementation map.

P1 `performance_schema.data_locks` now consumes detached snapshots from the
real InnoDB `LockManager`. It exposes granted record locks even when no wait
edge exists, preserves waiting requests, projects shared/exclusive record-lock
modes, and makes `data_lock_waits` IDs point to the corresponding
transaction-qualified lock rows. The manager and P_S lock regressions passed;
the evidence is in
`reports/compatibility/p1-data-locks-current-continuation1214.txt`.

The remaining limitation is explicit: the current lock manager exposes a
numeric resource key rather than authoritative schema/table/index/record
metadata, so those fields cannot yet be populated with native InnoDB values.
Complete P1 component/runtime sources, P2 unified storage/WAL/binlog commit
publication, P3 complete client evidence, and the external P4 MySQL fixture
remain open. FULLTEXT stays deferred and non-InnoDB engines/repair/conversion
remain out of scope.

## Current audit boundary — Continuation 1215

The local P2 regression boundary was re-run after the data-lock change:
replication XA/native/recovery/GTID/replica/source tests passed in 4.754
seconds, and the engine replication tests passed in 2.432 seconds. This is
evidence for the xmysql-to-xmysql state machine and recovery paths only; it is
not evidence of native MySQL bidirectional interoperability.

The P3 environment diagnostic was refreshed. Go, PyMySQL, and Node.js/mysql2
are available, while `mysql.exe` is absent. No password was supplied and no
client server run was started in this diagnostic, so the aggregate non-
Connector/J matrix remains partial and the MySQL CLI row remains unverified.
Evidence: `reports/compatibility/client-matrix-diagnostic-current-continuation1215/client-matrix-20260923-233348.json`.

The architectural P2 limitation remains unchanged: ordinary storage DML still
commits its storage transaction per statement and relies on the outer journal
and inverse changes for session rollback. A single unified physical commit
protocol covering storage/WAL, native binlog, GTID publication, and crash
recovery is therefore still required before P2 can be marked complete.

## Current audit boundary — Continuation 1216

P1 Performance Schema statement and stage history now use the current session
thread as their source scope. `events_statements_history` and
`events_stages_history` no longer expose another connection's rows; the
`*_history_long` variants remain instance-wide. The runtime recorder keeps a
defensive, bounded per-thread history and the executor selects it using the
current MySQL session connection id. Evidence:
`reports/compatibility/p1-thread-history-current-continuation1216.txt`.

The focused red/green tests, full P1 engine gate, and full metrics package gate
passed. This closes the per-thread source/filtering gap only. Exact MySQL
history capacity/autosizing, system-variable behavior, thread-end discard
lifecycle, waits-history lifecycle, and complete Performance Schema
table/runtime semantics remain partial. P2 unified physical storage/WAL/binlog
commit, P3 full non-Connector/J client evidence, and P4 official-MySQL
interoperability remain open. FULLTEXT stays deferred; non-InnoDB engines,
repair, and engine conversion remain out of scope. The official per-thread
history contract is documented in the
[MySQL 8.4 statement history table](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-events-statements-history-table.html).

## Current audit boundary — Continuation 1217

P1 `performance_schema.events_waits_history` now filters completed lock and
metadata wait rows by the current session/thread id. The long history view
remains instance-wide, and current-view, consumer, instrument, timer, lock,
metadata-lock, and summary regressions remain green. Evidence:
`reports/compatibility/p1-wait-history-current-continuation1217.txt`.

This closes only the wait-history session-filter gap. Exact MySQL wait-history
capacity/lifecycle and complete ownership mapping for every wait instrument
class remain partial; P2 unified physical storage/WAL/binlog commit, P3 full
non-Connector/J client evidence, and P4 official-MySQL interoperability remain
open. FULLTEXT stays deferred; non-InnoDB engines, repair, and engine
conversion remain out of scope.

## Current audit boundary — Continuation 1218

P2 local replication replay now creates one shared physical storage transaction
for a source transaction containing multiple SQL statements or row-image
changes. Replayed DML reuses that transaction without per-statement commit or
flush; the outer COMMIT/ROLLBACK owns the physical boundary. The durable
`replication_commit` record and final committed marker remain ordered after
storage commit. Evidence:
`reports/compatibility/p2-replication-storage-transaction-current-continuation1218.txt`.

The replay-context red test, engine replication tests, replication/net XA and
binlog tests, and the complete P1 engine gate passed. This is still not the
complete P2 unified protocol: ordinary client transactions, storage/WAL,
native binlog append, GTID/applied-state publication, and crash recovery are
not yet one atomic physical commit point. Official MySQL interoperability
remains the separate external P4 gate.

## Current audit boundary — Continuation 1220

The ordinary client path now keeps one shared storage transaction for
`autocommit=0` and explicit transactions. A focused regression verifies that
uncommitted INSERT rows are visible to the writer, hidden from another client
session, become visible after COMMIT, and disappear after ROLLBACK. UPDATE and
DELETE changes, including secondary-index predicates, now use the same
committed-view reconstruction. The pending registry is scoped to the owning
data directory. The existing inverse-DML rollback path remains in place
because the B+Tree writer still applies pages eagerly. Evidence:
`reports/compatibility/p2-client-transaction-boundary-current-continuation1220.txt`.

The XA finish path also closes the session-owned shared storage transaction on
XA COMMIT/ROLLBACK, including prepared-transaction retry paths.

This is a scoped P2 improvement, not completion of the unified commit
protocol. Repeatable-read statement snapshots and the single physical
storage/WAL/native-binlog/GTID/crash-recovery commit point remain partial. The
full engine gate was also limited by a Windows Go runtime
`VirtualAlloc` out-of-memory termination; the bounded P1/transaction gates
passed. FULLTEXT remains deferred and non-InnoDB engines/repair/conversion
remain out of scope.

## Current audit boundary — Continuation 1221

The ordinary client visibility fence now records a bounded commit epoch for
committed autocommit DML and shared client transactions. A REPEATABLE READ or
SERIALIZABLE client transaction captures the current epoch at its first
consistent SELECT; later committed INSERT/UPDATE/DELETE changes are
reconstructed back to the captured view, while COMMIT clears the snapshot.
The focused repeatable-read test and the combined P1/transaction/XA/replication
gate passed. Evidence:
`reports/compatibility/p2-client-transaction-boundary-current-continuation1220.txt`.

This is logical client-view compatibility over the current eager B+Tree page
writer. Physical page-level MVCC, durable version history across restart, and
the single atomic storage/WAL/native-binlog/GTID/crash-recovery commit point
remain P2 partial. Complete I_S/P_S registry/runtime semantics, P3 full
non-Connector/J evidence, and the P4 official-MySQL fixture remain open.
FULLTEXT remains deferred; non-InnoDB engines, repair, and engine conversion
remain out of scope.

The retention policy is safe for long-running snapshots: active snapshots keep
all older committed records they may need, and normal bounded pruning resumes
after those snapshots end. Empty-epoch and long-snapshot regressions are part
of the transaction gate.

## Current audit boundary — Continuation 1222

The bounded repository-side native binlog, GTID, replica apply, XA/binlog,
recovery, and promotion suites passed. Evidence:
`reports/compatibility/p2-replication-binlog-current-continuation1222.txt`.

This is fresh local evidence only. A real upstream/downstream MySQL 8.4
interoperability run and one physical commit point spanning storage pages,
WAL, native binlog, GTID state, and crash recovery are still not proven.
They remain P2/P4 work.

## Current audit boundary — Continuation 1223

The client environment diagnostic was refreshed. Go, PyMySQL, and Node.js/mysql2
are available for the non-Connector/J matrix; the current host still has no
`mysql.exe`, so the MySQL CLI row remains `unverified` rather than being
reported as a compatibility failure. No protected client password was supplied,
so this run intentionally performed environment discovery only. Evidence:
`reports/compatibility/client-matrix-diagnostic-current-continuation1223/client-matrix-20260924-004657.json`.

## Current audit boundary — Continuation 1224

The implemented INFORMATION_SCHEMA privilege/role projections passed their
focused repository-side regression, including account, active-role,
applicable-role, administrable-role, and role-grant filters. Evidence:
`reports/compatibility/p1-privilege-role-current-continuation1224.txt`.

This is not complete MySQL authorization lifecycle compatibility. Dynamic
grant/revoke visibility, all role-inheritance edges, PROCESS-scoped metadata,
and every account/plugin privilege source remain in the broader P1 partial
scope.

## Current audit boundary — Continuation 1225

The JOIN materialization path now applies the same session-aware transaction
visibility fence as single-table SELECTs. A REPEATABLE READ regression covers
aggregates over both a direct parent/child JOIN and a derived JOIN: rows
committed after the first read are hidden until the reader commits, then become
visible. The same regression also fixed JOIN `COUNT(*)` aggregation by marking
the accumulator as count-star.
Evidence:
`reports/compatibility/p2-client-transaction-boundary-current-continuation1220.txt`.

This closes the JOIN execution-path gap only. The visibility layer remains a
logical fence over eager B+Tree writes; physical MVCC, durable history across
restart, and a single storage/WAL/native-binlog/GTID/crash-recovery commit
point remain P2 partial.

## Current audit boundary — Continuation 1226

The repository-side P1 metadata and privilege regression passed:
`go test -p 1 ./server/innodb/engine -run
'^(TestInformationSchema|TestPerformanceSchema|TestRole|TestApplicableRoles|TestMandatoryRoles|TestInformationSchemaPrivilege)'
-count=1 -timeout 60m` (59.778s). Evidence:
`reports/compatibility/p1-metadata-privilege-regression-current-continuation1226.txt`.

This confirms the currently implemented INFORMATION_SCHEMA and Performance Schema
shapes, filters, runtime slices, account visibility, roles, and privilege views are
stable in repository tests. It does not close the full MySQL 8.4 table/column/runtime
contract, exact component-dependent lifecycle semantics, or all authorization and
role-inheritance edges; P1 remains partial. P2 physical commit publication and
official XA/binlog/GTID/crash-recovery interoperability, P3 non-Connector/J client
evidence, and the P4 official fixture remain open. FULLTEXT remains deferred, and
non-InnoDB engines/repair/conversion remain out of scope.

## Current audit boundary — Continuation 1219

P1 `INFORMATION_SCHEMA.INNODB_COLUMNS.DEFAULT_VALUE` now preserves explicit
empty string defaults and quoted defaults containing spaces during the ALTER
TABLE fallback metadata path. The projection returns the supported string
payload bytes, keeps the existing InnoDB integer encoding, and continues to
return NULL for DEFAULT NULL, expressions, and unsupported internal encodings.
The regression and complete P1 engine gate passed; evidence:
`reports/compatibility/p1-innodb-default-value-current-continuation1219.txt`.

This closes a concrete persisted-default edge, not the complete MySQL Field
conversion matrix. Temporal, DECIMAL, expression, collation-specific, and
FULLTEXT-dependent default encodings still require authoritative source data
before they can be marked complete. P1 component/runtime sources, P2 unified
storage/WAL/binlog commit publication, P3 full client evidence, and the P4
official-MySQL fixture remain open.

## Current audit boundary — Continuation 1227

The local client commit publication boundary is now storage-first and retryable:
the shared storage transaction commits before replication/binlog publication,
and a stable per-transaction commit key prevents a post-append publication
retry from producing a second GTID/native transaction. Autocommit DML/DDL also
clear the transaction journal and key after successful publication. If redo has
committed but a modified-page flush fails, the storage context remains
`COMMITTED` and returns a durability error; the client path promotes committed
changes and retries only publication. Evidence:
`reports/compatibility/p2-commit-order-and-retry-current-continuation1227.txt`.

This closes a concrete local commit-order/idempotency defect. It does not close
one physical atomic commit point across storage pages, WAL, native binlog, GTID,
and crash recovery, and it does not prove official MySQL 8.4 XA/binlog/GTID
interoperability. P2 remains partial, P4 remains pending_external; full P1
INFORMATION_SCHEMA/Performance Schema coverage and P3 non-Connector/J client
coverage remain global backlog items. FULLTEXT remains deferred and non-InnoDB
engines/repair/conversion remain out of scope.

## Current audit boundary — Continuation 1228

The release-candidate gate now explicitly runs the compatibility scope-matrix
validator, the expanded P1 metadata/privilege/role regression, and the P2
stable client commit/retry regression. Fresh evidence:
`reports/compatibility/gate-wiring-current-continuation1228.txt`.

The matrix validator passed with 26 entries, 24 in scope, and 2 explicit
out-of-scope entries. The P1 gate passed in 51.403s; the P2 gate passed across
engine, net, and replication. The gate still requires P3 client matrix and
cluster-endpoint checks and the P4 official MySQL fixture. Missing `mysql.exe`,
protected credentials, or the official fixture remains an explicit environment
blocker/`pending_external`, not a local compatibility pass.

## Current audit boundary — Continuation 1229

The P3 environment diagnostic was refreshed. Go, PyMySQL, and Node.js/mysql2
are available; `mysql.exe` is absent and the protected
`XMYSQL_CLIENT_PASSWORD` variable is not present, so no authenticated client
matrix was started. Evidence:
`reports/compatibility/client-matrix-diagnostic-current-continuation1229.txt`.

The MySQL CLI row remains `unverified`, and the aggregate non-Connector-J
matrix remains partial. This is the current environment boundary, not a
functional client failure.

## Current audit boundary — Continuation 1232/1233

The full local Go integration gate was rerun with `GOMAXPROCS=1` and `GOGC=10`
after reproducing Windows commit-limit failures. Engine, net, protocol, and
replication all passed; evidence:
`reports/compatibility/release-candidate-current-continuation1232-boundary.txt`.

The release gate reached Connector/J. Six result classes completed with zero
failures and zero errors, but the run did not produce a final PerformanceTest
result: the JVM remained in the 10,000-row batch load for
`PerformanceTest.testAggregateQuery`. The complete Connector/J gate therefore
remains unverified for this run. The gate also exposed and now fixes a harness
path bug: JDBC changes the working directory, so gate log paths are resolved to
absolute paths before child actions start. A new full run is required for the
authoritative `release-candidate.json`.

The next gate attempt exposed a second harness-only path issue: the scope-matrix
writer joined an already absolute OutputPath to the current directory. It now
uses absolute paths directly; the standalone matrix diagnostic passed again
with 26 entries, 24 in scope, and 2 explicit out-of-scope entries. Evidence:
`reports/compatibility/release-candidate-current-continuation1234-boundary.txt`.
A fresh full gate is still required.

## Current audit boundary — Continuation 1238

The remaining Connector/J transaction visibility defect was fixed at the
autocommit boundary: when a connection had captured a read-only Repeatable Read
snapshot, `SET autocommit=1` now clears that snapshot and the session transaction
state. The regression `TestRepeatedJdbcTransactionSequenceKeepsDecimalRowsVisible`
is green, as are the current-read/aggregate transaction tests.

The fresh authoritative Connector/J gate is green: `139/139` tests, zero
failures, zero errors, and zero skipped. `TransactionTest` passed 8/8 and
`PerformanceTest` passed all 8 cases. Evidence:
`reports/compatibility/release-candidate-current-continuation1238/jdbc.log`.

The complete release-candidate report also records PASS for build, unit and
integration, the scope matrix, P1 metadata/observability, P2 native
XA/binlog/replication/recovery regression, Go core, cluster smoke, three crash
recovery repetitions, concurrency, and observability. The authoritative report
is:
`reports/compatibility/release-candidate-current-continuation1238/release-candidate.json`.

The overall result remains `NO-GO` only because the mandatory P3 checks could
not perform a protected authenticated run: `XMYSQL_CLIENT_PASSWORD` was absent
and `mysql.exe` was not installed. Go, PyMySQL, and Node.js/mysql2 are
available, but that environment discovery does not prove the full
non-Connector/J matrix. The full client matrix remains a global P3 task.

The global backlog boundary is explicit: full INFORMATION_SCHEMA and
PERFORMANCE_SCHEMA table/column/runtime/privilege semantics remain P1 work;
xmysql-native XA/binlog/replication/crash-recovery paths are in the local P2
gate, while official MySQL cross-server interoperability remains the P4
fixture gate. FULLTEXT remains deferred. Non-InnoDB engines, their dedicated
`REPAIR TABLE` behavior, and engine conversion remain out of scope.

## Current audit boundary — Continuation 1239

The InnoDB compression manager now retains both aggregate and per-tablespace
compression counters, including synchronized failure counts. The
`INFORMATION_SCHEMA.INNODB_CMP_PER_INDEX` and reset views use the persisted
`.frm` index metadata and expose a source-backed PRIMARY clustered-index row
when its tablespace has observed compression activity. Counters are not copied
onto secondary indexes because the current page compressor cannot prove that
per-index attribution.

The red/green regressions are
`TestCompressionManagerKeepsPerSpaceStatistics` and
`TestInformationSchemaInnoDBCompressionPerIndexProjectsPrimarySpaceStats`;
the related compression, buffer-pool, and cached-index slices also pass.
This closes one P1 runtime-source gap, but complete I_S/P_S semantics remain
partial until the remaining table-specific sources, exact counters, filters,
permissions, and lifecycle behavior are covered. P2 official MySQL
interoperability, P3 full client evidence, and P4 fixture validation remain
separate gates.

## Current audit boundary — Continuation 1240

Compression reset lifecycle is now source-backed. The aggregate and
per-tablespace compression managers expose atomic snapshot-and-clear methods;
the `INNODB_CMP_RESET`, `INNODB_CMPMEM_RESET`, and
`INNODB_CMP_PER_INDEX_RESET` views consume those methods. The regressions prove
that a reset read returns the counters and that a subsequent ordinary view is
empty, while resetting one tablespace leaves another untouched.

The evidence is
`reports/compatibility/p1-compression-reset-current-continuation1240.txt`.
This closes the reset lifecycle for the implemented compression sources only;
complete I_S/P_S coverage, compression timing/decompression counters, and
secondary-index attribution remain partial.

## Current audit boundary — Continuation 1241

Successful compressed-page decompression is now counted at the compression
manager source and projected as `UNCOMPRESS_OPS` by the aggregate and
per-index InnoDB compression views. Uncompressed input and failed decompression
do not increment the counter, and reset snapshots clear it with the other
compression counters.

The evidence is
`reports/compatibility/p1-compression-uncompress-current-continuation1241.txt`.
Compression timing units, exact secondary-index attribution, and complete
I_S/P_S semantics remain partial because the current engine does not yet expose
authoritative sources for them.

## Current audit boundary — Continuation 1242

Compression timing is now source-backed. The compression manager accumulates
actual durations for compression attempts and successful decompression, and
the InnoDB compression views project complete seconds. A short operation may
therefore legitimately appear as zero seconds, while the manager-level source
retains the sub-second duration.

The evidence is
`reports/compatibility/p1-compression-timing-current-continuation1242.txt`.
The per-index enable switch and authoritative secondary-index attribution are
still missing, so the broader I_S/P_S task remains partial.

## Current audit boundary — Continuation 1243

The global innodb_cmp_per_index_enabled switch is now implemented with the
MySQL default of OFF. Aggregate compression counters continue regardless of
the switch, while per-index collection begins a fresh window only after the
switch is enabled. SET GLOBAL and persisted startup values synchronize the
runtime manager, and the per-index views are empty while collection is off.

The evidence is
reports/compatibility/p1-compression-per-index-switch-current-continuation1243.txt.
Only the source-backed PRIMARY projection is available; secondary-index
attribution and complete I_S/P_S semantics remain partial.

## Current audit boundary — Continuation 1244

`INFORMATION_SCHEMA.INNODB_METRICS` now exposes the official MySQL 8.4
`compress_pages_compressed` and `compress_pages_decompressed` counters from
the real compression manager. They use the `compression` subsystem and
`status_counter` type, are disabled by default, and support the implemented
`module_compress` module/metric enable, disable, and reset monitor variables. Persisted monitor
values are synchronized during startup application. Evidence:
`reports/compatibility/p1-innodb-metrics-compression-current-continuation1244.txt`.

This is one source-backed P1 slice; complete INFORMATION_SCHEMA/
PERFORMANCE_SCHEMA coverage remains open.

## Current audit boundary — Continuation 1245

The live Buffer Pool rows in `INFORMATION_SCHEMA.INNODB_METRICS` now align
with MySQL 8.4 metadata where current sources are authoritative:
`buffer_pool_bytes_data`, `buffer_pool_bytes_dirty`, and `buffer_pool_size` are
derived from real page counts and page size; page rows use `value`, request
rows use `status_counter`, and subsystem labels use `buffer` or `server`.
`buffer_pool_pages_misc`, read-ahead, and wait-free remain omitted because
their authoritative sources are not exposed. Evidence:
`reports/compatibility/p1-innodb-metrics-buffer-pool-current-continuation1245.txt`.

Complete INFORMATION_SCHEMA/PERFORMANCE_SCHEMA coverage remains open.

## Current audit boundary — Continuation 1246

The implemented compression monitor selectors now support the official
`compress_pages_%` wildcard as well as exact names, `module_compress`, and
`all`. The red/green regression confirms that the wildcard enables both
source-backed compression counters. Evidence:
`reports/compatibility/p1-innodb-metrics-monitor-pattern-current-continuation1246.txt`.

The full `INNODB_METRICS` registry and lifecycle timestamp/reset-column
semantics remain open.

## Current audit boundary — Continuation 1247

The two source-backed compression metrics now expose lifecycle state through
`COUNT_RESET`, `TIME_ENABLED`, `TIME_DISABLED`, `TIME_ELAPSED`, and
`TIME_RESET`. `innodb_monitor_reset_all` rejects an enabled selected counter
and succeeds after the counter is disabled, matching the MySQL 8.4 contract.
Evidence:
`reports/compatibility/p1-innodb-metrics-lifecycle-current-continuation1247.txt`.

This closes lifecycle/reset behavior for the implemented compression metrics
only. The full `INNODB_METRICS` registry, all official metric sources, and
complete INFORMATION_SCHEMA/PERFORMANCE_SCHEMA semantics remain open.

## Current audit boundary — Continuation 1248

The live LockManager wait graph now backs the official
`lock_row_lock_current_waits` and `lock_threads_waiting` rows. The official
`trx_active_transactions` row is disabled by default, uses type `counter`, and
returns the active transaction count after `module_trx` is enabled. Exact
names, subsystem selectors, implemented modules, and wildcard selection are
covered by the regression. Evidence:
`reports/compatibility/p1-innodb-metrics-transaction-lock-current-continuation1248.txt`.

The full MySQL metrics registry and all monitor modules remain open.

## Current audit boundary — Continuation 1249

The official `dml_inserts`, `dml_updates`, and `dml_deletes` metrics now use
successful DML affected-row results as a source-backed runtime counter. They
default to enabled, support `module_dml`, exact/wildcard selection, lifecycle
timestamps, ordinary reset, and disabled-counter `reset_all`. The regression
coverage is:

`TestInformationSchemaInnoDBMetricsExposeDMLRowCounters`
`TestInformationSchemaInnoDBMetricsDMLLifecycleAndReset`

Evidence:
`reports/compatibility/p1-innodb-metrics-dml-current-continuation1249.txt`.

`dml_reads`, system-DML metrics, and the remaining MySQL 8.4 INNODB_METRICS
modules remain open because their authoritative runtime sources are not yet
exposed by the current engine. Complete INFORMATION_SCHEMA/PERFORMANCE_SCHEMA
coverage remains P1 partial.

## Current audit boundary — Continuation 1250

The official `buffer_pages_read`, `buffer_pages_written`, and
`innodb_page_size` rows now use source-backed Buffer Pool/configuration values.
The focused Buffer Pool metric regression passes. Evidence:
`reports/compatibility/p1-innodb-metrics-buffer-page-current-continuation1250.txt`.

`buffer_pool_pages_misc`, read-ahead, read-ahead-evicted, wait-free, and the
remaining INNODB_METRICS modules remain open because their authoritative
runtime sources are not exposed by the current engine.

## Current audit boundary — Continuation 1251

LockManager's completed wait summary now provides the official lock summary
rows `lock_row_lock_waits`, `lock_row_lock_time`,
`lock_row_lock_time_avg`, and `lock_row_lock_time_max`, including a
disabled-counter reset baseline. Evidence:
`reports/compatibility/p1-innodb-metrics-lock-summary-current-continuation1251.txt`.

Deadlock/timeout counters, record-lock lifecycle counters, and the remaining
INNODB_METRICS modules remain open because authoritative cumulative sources are
not exposed by the current engine.

## Current audit boundary — Continuation 1252

The four source-backed lock-summary metrics now implement ordinary reset
windows as well as cumulative values. `COUNT` survives
`innodb_monitor_reset`; reset-window count/min/max/average and `TIME_RESET` use
the completed-wait entries after the reset baseline. Evidence:
`reports/compatibility/p1-innodb-metrics-lock-reset-current-continuation1252.txt`.

The full INNODB_METRICS registry and lock metrics without authoritative
runtime sources remain open.

## Continuation 1253 — Buffer Pool runtime metrics

Completed the next source-backed P1 slice. `buffer_pool_pages_misc` now uses
the live page-count relationship, while `buffer_pool_read_ahead` and
`buffer_pool_read_ahead_evicted` use actual prefetch completion and LRU
eviction events. The manager eviction regression and the complete focused
INFORMATION_SCHEMA metrics regression pass. Evidence:
`reports/compatibility/p1-innodb-metrics-buffer-runtime-current-continuation1253.txt`.

This does not close the full registry. `buffer_pool_wait_free`, remaining
INNODB_METRICS modules, full I_S/P_S table coverage, non-Connector/J client
matrix coverage, and official MySQL XA/binlog interoperability remain tracked
in their existing priority lanes.

## Continuation 1254 — Buffer Pool auxiliary view

Projected the same live read-ahead and prefetch-eviction counters into
`INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS`. The new regression was red
against the previous fixed-zero projection and passes after the source wiring.
Evidence:
`reports/compatibility/p1-innodb-buffer-pool-stats-current-continuation1254.txt`.

Rate columns remain open until the manager exposes a time-windowed source.

## Continuation 1255 — Buffer Pool rate fields

Added snapshot-delta rates for page reads, page writes, read-ahead, and
read-ahead evictions, and projected them into
`INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS`. The red/green manager test and
engine regression pass. Evidence:
`reports/compatibility/p1-innodb-buffer-pool-rate-current-continuation1255.txt`.

Page-create and LRU I/O rates remain open because their authoritative event
sources are not exposed.

## Continuation 1256 — InnoDB lock lifecycle metrics

Added source-backed cumulative LockManager counters for record-lock requests,
record-lock creation, record-lock removal, and deadlock detection. Exposed the
four official `INNODB_METRICS` rows and wired lock-module monitor lifecycle and
reset baselines. Evidence:
`reports/compatibility/p1-innodb-metrics-lock-lifecycle-current-continuation1256.txt`.

The global plan remains open for complete INFORMATION_SCHEMA/PERFORMANCE_SCHEMA
registry and field coverage, the complete non-Connector/J client matrix, and
native MySQL XA/binlog/replication/crash-recovery interoperability. Non-InnoDB
engines and non-InnoDB repair/engine conversion remain out of scope.

## Continuation 1257 — Buffer Pool page creation

Projected successful live page allocations into
`NUMBER_PAGES_CREATED` and `PAGES_CREATE_RATE`, and added the corresponding
source-backed `buffer_pages_created` metric. Evidence:
`reports/compatibility/p1-innodb-buffer-pool-create-rate-current-continuation1257.txt`.

The global completion plan remains open for full I_S/P_S registry coverage,
the complete non-Connector/J client matrix, and official MySQL
XA/binlog/replication/crash-recovery interoperability.

## Continuation 1258 — client environment boundary

The client-matrix diagnostic found Go, PyMySQL, and Node/mysql2 available but
no `mysql` CLI executable. The diagnostic is recorded as environment evidence;
it does not close the non-Connector/J compatibility matrix or the mysql-cli
gate. Evidence:
`reports/compatibility/client-matrix-diagnostic-continuation1257/client-matrix-20260924-073703.json`.

The cluster endpoint diagnostic also requires the protected
`XMYSQL_CLIENT_PASSWORD` environment value before it can start the
authenticated source/replica scenario. The client cluster gate therefore
remains partial. Evidence:
`reports/compatibility/client-cluster-endpoint-diagnostic-continuation1258/client-cluster-endpoint-20260924-074328.json`.

## Continuation 1260 — Buffer Pool young-page counters

Connected real old-list access and successful promotion events to the
`PAGES_MADE_YOUNG`/`PAGES_NOT_MADE_YOUNG` fields, snapshot rates, and
per-thousand-get ratios. Evidence:
`reports/compatibility/p1-innodb-buffer-pool-young-pages-current-continuation1260.txt`.

LRU I/O and the remaining full I_S/P_S observability fields remain open.

## Continuation 1261 — Buffer Pool pending work

Projected the live prefetch queue and dirty-page registry into
`PENDING_READS` and `PENDING_FLUSH_LIST`. Evidence:
`reports/compatibility/p1-innodb-buffer-pool-pending-work-current-continuation1261.txt`.

`PENDING_FLUSH_LRU`, LRU I/O, and remaining Buffer Pool sampling fields remain
open.

## Continuation 1262 — Buffer Pool decompression counters

`INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS.UNCOMPRESS_TOTAL` now projects
the aggregate successful page-decompression counter from CompressionManager.
`UNCOMPRESS_CURRENT` is backed by a separate current-window counter and is
consumed/reset by a statistics snapshot without erasing the cumulative total.
The regression observes `1/1` and then `1/0` for total/current. Evidence:
`reports/compatibility/p1-innodb-buffer-pool-uncompress-current-continuation1262.txt`.

`PENDING_FLUSH_LRU`, `LRU_IO_TOTAL`, and `LRU_IO_CURRENT` remain open because
the current xmysql buffer-pool implementation has no source equivalent to
MySQL's dedicated LRU-policy I/O event counters; ordinary page read/write
totals are intentionally not substituted.
## Continuation 1263 update

- Closed one source-backed P1 slice: dedicated LRU I/O counters now project
  `INNODB_BUFFER_POOL_STATS.LRU_IO_TOTAL` and `LRU_IO_CURRENT` with cumulative
  versus consumed-current-window semantics.
- Verified with manager and engine tests against a real created tablespace.
- Kept `PENDING_FLUSH_LRU`, full I_S/P_S runtime semantics, native MySQL
  XA/binlog/GTID/replication/crash-recovery interoperability, and the full
  non-Connector/J client matrix open; non-InnoDB engines remain out of scope.
## Continuation 1264 update

- Closed the local `PENDING_FLUSH_LRU` source gap: dirty pages evicted from the
  optimized LRU now enter a dedicated durable flush queue, with live pending
  count, background persistence, and close-time draining.
- Verified the blocked-storage lifecycle and the full InnoDB manager/engine
  regression.
- Full I_S/P_S semantics beyond this slice, native MySQL interoperability,
  external client matrix evidence, and official fixture work remain open.

## Continuation 1265 update

- Closed the local P1 `PENDING_DECOMPRESS` source gap: CompressionManager now
  exposes an atomic in-flight decompression count and the I_S projection uses
  it for the live `INNODB_BUFFER_POOL_STATS` value.
- Verified the blocked decompression lifecycle (`1 -> 0`) through manager and
  engine tests, then passed full manager and engine regressions.
- Complete I_S/P_S semantics, native MySQL XA/binlog/GTID/replication/
  crash-recovery interoperability, the full non-Connector/J client matrix,
  and official fixture work remain open; non-InnoDB engines remain out of
  scope.

## Continuation 1266 update

- Closed the local P1 `HIT_RATE` scale defect: the manager continues to expose
  a 0..1 ratio, while `INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS` now
  converts it to InnoDB's per-thousand value (`0.75 -> 750`).
- Verified the red/green field test and the related Information Schema and
  Performance Schema engine regressions.
- Complete I_S/P_S semantics, native MySQL XA/binlog/GTID/replication/
  crash-recovery interoperability, the full non-Connector-J client matrix,
  and official fixture work remain open; non-InnoDB remains out of scope.

## Continuation 1267 update

- Closed the local P1 `INFORMATION_SCHEMA.COLUMNS.PRIVILEGES` defect: the
  projection now evaluates effective table/global and column grants for the
  current session, including active roles and wildcard column scopes.
- Verified the column-only SELECT lifecycle and the permission/role/I_S
  regression selection; the full engine regression passed in 186.939s.
- The broader complete I_S/P_S permission/runtime matrix and P2/P3/P4 gates
  remain open; non-InnoDB remains out of scope.

## Continuation 1288 update

- Added a source-backed `INFORMATION_SCHEMA.INNODB_METRICS.trx_allocations`
  counter from `TransactionManager.Begin`, including `module_trx`
  enable/disable/reset-baseline behavior.
- The focused transaction-metric selection, full InnoDB engine regression, and
  full serial repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-transaction-allocations-current-continuation1288.txt`.
- The complete I_S/P_S registry/runtime/permission task, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and
  P4 official fixture remain open; FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

## Continuation 1293 update

- Added source-backed `INFORMATION_SCHEMA.INNODB_METRICS.trx_rollback_active`
  from `TransactionManager`'s real active rollback counter, covering full
  rollback and rollback-to-savepoint operations with `module_trx` routing.
- Full rollback now releases the transaction-manager write lock while Undo work
  executes and uses an in-progress guard to reject concurrent COMMIT or duplicate
  rollback of the same transaction, allowing INFORMATION_SCHEMA to observe the
  live metric without waiting for Undo completion.
- The manager metric test, SQL metric red/green test, full InnoDB engine
  regression, and full serial repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-rollback-active-current-continuation1293.txt`.
- The complete I_S/P_S registry/runtime/permission task, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and
  P4 official fixture remain open; FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

## Continuation 1294 update

- Hardened the shared replication state-file replacement path: it now writes and
  `File.Sync`s the temporary file before close and atomic rename, with a focused
  fault-injection test proving sync failures do not install a target or leave a
  temporary file.
- The replication-focused tests, P2 selection, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p2-replication-state-fsync-current-continuation1294.txt`.
- This closes only state-file replacement durability. A single physical commit
  point across storage pages/WAL, native binlog, GTID state, replica-applied
  state, and crash recovery remains partial; official MySQL interoperability is
  still the P4 external-fixture task.

## Continuation 1295 update

- Closed the in-process retry window where the logical JSONL stream was already
  durable but native binlog append failed. A retry with the same coordinator key
  now rebuilds native files from the logical recovery source and acknowledges
  the existing transaction without appending a duplicate.
- Added a deterministic native-append fault-injection regression. The focused
  test, replication package, affected P2 selection, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p2-native-binlog-retry-recovery-current-continuation1295.txt`.
- This is still a scoped P2 recovery slice: XA terminal retries, one physical
  commit point across storage/WAL/binlog/GTID/replica-applied state/crash
  recovery, and official MySQL interoperability remain open.

## Continuation 1296 update

- Extended the logical/native recovery boundary to XA terminal events. After a
  synced logical `XA COMMIT` or `XA ROLLBACK` encounters a native append error,
  retrying the same terminal rebuilds native files and avoids duplicate XA
  terminal events.
- The red regression reproduced two XA terminal events before the fix; the
  focused ordinary/XA retry tests, replication package, affected P2 selection,
  and full serial repository regression now pass. Evidence:
  `reports/compatibility/p2-xa-terminal-retry-recovery-current-continuation1296.txt`.
- This remains a scoped P2 retry-recovery improvement. The unified physical
  storage/WAL/binlog/GTID/replica-applied/crash commit point and official MySQL
  interoperability remain open.

## Continuation 1292 update

## Continuation 1297 update

- P1 为 `events_waits_current`、`events_waits_history` 和
  `events_waits_history_long` 增加精确的 MySQL 8.4 元数据契约：事件/计时字段的
  unsigned BIGINT 与 NULLability、事件源/对象/操作字段长度均不再落入通用
  `VARCHAR(255)` 兜底。
- 红测先暴露 `END_EVENT_ID`、计时字段、`SPINS`、`OBJECT_TYPE`、`OPERATION` 和
  `FLAGS` 的形状/NULLability 缺口；修复后单测、完整 I_S/P_S、engine、受影响包和
  全仓串行回归均通过。证据：
  `reports/compatibility/p1-performance-schema-wait-events-metadata-current-continuation1297.txt`。
- 该切片只关闭一个 P1 元数据家族；完整 I_S/P_S 运行时/组件/权限语义、P2 统一
  物理提交、P3 全量客户端和 P4 官方 fixture 仍未关闭。FULLTEXT 继续 deferred，
  非 InnoDB 引擎/修复/转换继续 out_of_scope。

## Continuation 1298 update

- P1 为 `events_stages_current`、`events_stages_history` 和
  `events_stages_history_long` 增加精确的 MySQL 8.4 元数据契约：事件、计时、
  工作量和嵌套事件字段的 unsigned BIGINT 与 NULLability、stage source 和嵌套
  event type 的字段长度均不再落入通用 `VARCHAR(255)` 兜底。
- 红测先暴露 `END_EVENT_ID`、计时字段、`WORK_COMPLETED`、`WORK_ESTIMATED`、
  `NESTING_EVENT_ID` 和 `NESTING_EVENT_TYPE` 的形状/NULLability 缺口；修复后
  单测、完整 I_S/P_S、engine、受影响包和全仓串行回归均通过。证据：
  `reports/compatibility/p1-performance-schema-stage-events-metadata-current-continuation1298.txt`。
- 该切片只关闭一个 P1 元数据家族；完整 I_S/P_S 运行时/组件/权限语义、P2 统一
  物理提交、P3 全量客户端和 P4 官方 fixture 仍未关闭。FULLTEXT 继续 deferred，
  非 InnoDB 引擎/修复/转换继续 out_of_scope。

## Continuation 1292 update

- Added source-backed `INFORMATION_SCHEMA.INNODB_METRICS.os_log_pending_fsyncs`
  from an atomic `RedoLogManager` async-fsync pending counter, including
  `module_os` target routing.
- Manager tests, focused metric regression, the full InnoDB engine regression,
  and the full serial repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-pending-redo-fsyncs-current-continuation1292.txt`.
- The complete I_S/P_S registry/runtime/permission task, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and
  P4 official fixture remain open; FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

## Continuation 1291 update

- Added source-backed `INFORMATION_SCHEMA.INNODB_METRICS.os_log_pending_writes`
  from `RedoLogStats.BufferedLogs`, including `module_os` monitor target
  routing.
- The focused metric regression, full InnoDB engine regression, and full serial
  repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-pending-redo-writes-current-continuation1291.txt`.
- `os_log_pending_fsyncs` remains open because the current manager has no direct
  fsync-request source. The complete I_S/P_S registry/runtime/permission task,
  P2 native XA/binlog/replication/crash interoperability, P3 full client
  matrix, and P4 official fixture remain open; FULLTEXT remains deferred and
  non-InnoDB remains out of scope.

## Continuation 1290 update

- Added source-backed `INFORMATION_SCHEMA.INNODB_METRICS.trx_undo_slots_used`
  from `UndoLogManager.GetStats().ActiveTxns`, including `module_trx`
  enable/disable/reset-baseline behavior.
- The focused transaction-metric regression, full InnoDB engine regression, and
  full serial repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-undo-slots-current-continuation1290.txt`.
- `trx_rollback_active` and `trx_rseg_history_len` remain open pending an
  authoritative local source. The complete I_S/P_S registry/runtime/permission
  task, P2 native XA/binlog/replication/crash interoperability, P3 full client
  matrix, and P4 official fixture remain open; FULLTEXT remains deferred and
  non-InnoDB remains out of scope.

## Continuation 1268 update

- Closed a local P2 crash-recovery window for ordinary client transactions:
  after storage commit, the executor now writes and syncs a durable
  `replication_commit` record in the DML journal before invoking native
  binlog/GTID publication.
- Verified restart after a post-append publisher error preserves the committed
  row, then passed the P2 XA/binlog/GTID/recovery selection and full engine
  regression. Evidence:
  `reports/compatibility/p2-client-commit-recovery-current-continuation1268.txt`.
- This is not the final unified physical commit protocol or official MySQL
  interoperability proof; those remain partial/external respectively.

## Continuation 1269 update

- Closed a source-backed P1 `INFORMATION_SCHEMA.INNODB_METRICS` slice:
  `log_lsn_current`, `log_lsn_last_checkpoint`, and
  `log_lsn_checkpoint_age` now read from `RedoLogManager` and are controlled
  together by `innodb_monitor_enable=module_log`.
- Verified the disabled/enabled projection and the complete InnoDB engine
  regression. Evidence:
  `reports/compatibility/p1-innodb-metrics-redo-lsn-current-continuation1269.txt`.
- Complete I_S/P_S semantics, native MySQL XA/binlog/GTID/replication/
  crash-recovery interoperability, and the full non-Connector-J client matrix
  remain open; non-InnoDB remains out of scope.

## Continuation 1319 update

- Implemented `TRUNCATE TABLE` for Performance Schema statement, stage,
  transaction, and wait history/history_long tables with explicit runtime reset
  hooks and independent statement/stage buffers.
- Verified the focused history lifecycle red/green test, all Performance Schema
  tests, affected packages, and the full serial repository regression. Evidence:
  `reports/compatibility/p1-performance-schema-history-truncate-current-continuation1319.txt`.
- The global task remains open for complete I_S/P_S table/column/runtime/
  permission semantics, native XA/binlog/replication/crash interoperability,
  full non-Connector-J client coverage, and official MySQL fixture validation.
  FULLTEXT is deferred and non-InnoDB is out of scope.

## Continuation 1317 update

- Implemented the socket summary truncate lifecycle for
  `performance_schema.socket_summary_by_event_name` and
  `performance_schema.socket_summary_by_instance`: preserve the socket/thread
  row identity while zeroing read/write counts, timers, and byte totals, and
  suppress the active-session count fallback after reset.
- Official reference: [MySQL socket summary tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-socket-summary-tables.html).
  Focused socket coverage, all Performance Schema tests, affected packages,
  and `go test -p 1 ./... -count=1 -timeout 35m` passed. Evidence:
  `reports/compatibility/p1-performance-schema-socket-summary-truncate-current-continuation1317.txt`.
- The remaining plan boundaries are unchanged: complete I_S/P_S coverage,
  native XA/binlog/replication/crash interoperability, the full
  non-Connector-J client matrix, and official MySQL fixture validation remain
  open; FULLTEXT is deferred and non-InnoDB remains out of scope.

## Continuation 1310 update

- Implemented the MySQL 8.4 transaction-summary truncate lifecycle for
  `events_transactions_summary_global_by_event_name`. Reset identities are kept
  separately from live transaction events so global and dependent account,
  host, user, and thread projections return zero-valued rows without counting
  reset placeholders as transactions.
- Added a regression test and passed the focused Performance Schema test, the
  affected package set, and the full serial repository regression. Evidence:
  `reports/compatibility/p1-performance-schema-transaction-summary-truncate-current-continuation1310.txt`.
- Complete I_S/P_S registry/runtime/permission semantics, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and P4
  official fixture remain open; FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

## Continuation 1313 update

- Implemented independent table I/O and index-usage summary state for the
  recorder-backed Performance Schema projections. Table-level truncation now
  preserves table/index identities, zeroes table counters, and clears the
  dependent index-usage summary; index-level truncation is independent, and
  statement summaries remain unchanged.
- Added the isolation regression and passed the focused table-I/O tests, the
  affected package set, and the full serial repository regression. Evidence:
  `reports/compatibility/p1-performance-schema-table-io-summary-truncate-current-continuation1313.txt`.
- Complete I_S/P_S registry/runtime/permission semantics, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and P4
  official fixture remain open; FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

## Continuation 1314 update

- Implemented independent table-lock summary state for record-lock and
  metadata-lock waits. Truncating `table_lock_waits_summary_by_table` now keeps
  grouped table identities and zeroes its counters without changing
  `events_waits` summaries, bounded history, or live wait projections.
- Added the isolation regression and passed the focused Performance Schema
  test, affected package set, and full serial repository regression. Evidence:
  `reports/compatibility/p1-performance-schema-table-lock-summary-truncate-current-continuation1314.txt`.
- Complete I_S/P_S registry/runtime/permission semantics, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and P4
  official fixture remain open; FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

## Continuation 1315 update

- Implemented global memory-summary baseline reset. Truncation preserves memory
  identities and current ownership, reduces allocation/free counters and byte
  totals to a new baseline, and resets low/high watermarks to current usage.
- Added the regression and passed the focused Performance Schema test, affected
  package set, and full serial repository regression. Evidence:
  `reports/compatibility/p1-performance-schema-memory-summary-truncate-current-continuation1315.txt`.
- Complete I_S/P_S registry/runtime/permission semantics, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and P4
  official fixture remain open; FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

## Continuation 1316 update

- Implemented identity-preserving reset for the file I/O summary tables. Both
  event-name and instance projections now clear read/write/misc counters and
  timers without discarding file identity, object-instance IDs, or open-handle
  state.
- Added the regression and passed the focused Performance Schema test and full
  serial repository regression. Evidence:
  `reports/compatibility/p1-performance-schema-file-summary-truncate-current-continuation1316.txt`.
- Complete I_S/P_S registry/runtime/permission semantics, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and P4
  official fixture remain open; FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

## Continuation 1312 update

- Implemented global wait-summary truncation for the current record-lock and
  metadata-lock observability sources. Reset identities are retained for global,
  thread, and instance projections, while bounded history and live wait state
  remain independent.
- Added the regression and passed the focused test, affected package set, and
  full serial repository regression. Evidence:
  `reports/compatibility/p1-performance-schema-wait-summary-truncate-current-continuation1312.txt`.
- Complete I_S/P_S registry/runtime/permission semantics, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and P4
  official fixture remain open; FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

## Continuation 1311 update

- Added an independent stage/sql/execute aggregate to the metrics recorder and
  implemented global stage-summary truncation with identity-preserving zero rows.
  Statement and digest summaries remain unchanged when the stage summary is
  truncated.
- Added the isolation regression and passed the focused test, the affected
  package set, and the full serial repository regression. Evidence:
  `reports/compatibility/p1-performance-schema-stage-summary-truncate-current-continuation1311.txt`.
- Complete I_S/P_S registry/runtime/permission semantics, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and P4
  official fixture remain open; FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

## Continuation 1289 update

- Added source-backed `INFORMATION_SCHEMA.INNODB_METRICS.os_log_bytes_written`
  from `RedoLogManager.GetFileSnapshot().SizeInBytes`, including `module_os`
  enable/disable/reset-baseline behavior.
- The red/green metric test, full InnoDB engine regression, and full serial
  repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-redo-log-bytes-current-continuation1289.txt`.
- The complete I_S/P_S registry/runtime/permission task, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and
  P4 official fixture remain open; FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

## Continuation 1274 update

- Corrected the P1 `INFORMATION_SCHEMA.INNODB_METRICS` byte semantics:
  `buffer_data_reads` and `buffer_data_written` now use live page I/O counts
  multiplied by the live InnoDB page size; `os_data_reads` and
  `os_data_writes` expose the corresponding storage I/O counts.
- Added a 331-name static MySQL 8.4 InnoDB monitor catalog. Catalog-only rows
  remain explicitly `disabled/0` until a real xmysql runtime source exists;
  source-backed rows override the catalog entries.
- Focused metrics, full InnoDB, net/replication, and full serial repository
  regressions passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-catalog-current-continuation1274.txt`.
- The registry is now broadly discoverable, but complete runtime values,
  exact monitor types, lifecycle/reset timestamps, component sources, P2/P3/P4
  interoperability and client evidence remain open.

## Continuation 1275 update

- Added a source-backed `file_num_open_files` projection using the existing
  physical file lifecycle recorder's live open-handle count.
- The focused metric regression uses an open/close delta against the process
  baseline; full InnoDB and full serial repository regressions passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-open-files-current-continuation1275.txt`.
- Complete I_S/P_S runtime/component/permission semantics and P2/P3/P4 remain
  open; the static catalog does not by itself make unbacked rows complete.

## Continuation 1276 update

- Added a source-backed `trx_rollbacks` projection from the transaction
  completion boundary and a synchronized `RuntimeRecorder.TransactionTotals()`
  accessor.
- The focused rollback metric and RuntimeRecorder regressions passed. The
  implementation deliberately leaves `trx_rw_commits` and `trx_ro_commits`
  catalog-only because read/write classification is not authoritative at the
  current completion boundary.
- Complete I_S/P_S runtime/component/permission semantics and P2/P3/P4 remain
  open.

## Continuation 1277 update

- Added a source-backed `os_log_fsyncs` projection from the RedoLogManager
  group-commit `TotalFsyncs` counter.
- The red/green regression drives a real asynchronous redo flush and verifies
  the exposed metric against the manager's authoritative counter.
- Complete I_S/P_S runtime/component/permission semantics and P2/P3/P4 remain
  open.

## Continuation 1272 update

- The full release-candidate gate reached a terminal `NO-GO` with all local
  code gates and Connector/J passing: build, unit, integration, scope matrix,
  P1/P2, go-core, cluster smoke, three crash-recovery repeats, concurrency,
  observability, and 139 JDBC tests all passed.
- The only failed checks were the environment-blocked non-Connector/J client
  matrix and cluster endpoint authentication (`XMYSQL_CLIENT_PASSWORD` absent;
  `mysql.exe` absent). Evidence:
  `reports/compatibility/global-audit-current-continuation1273.txt` and
  `reports/compatibility/release-candidate-current-continuation1272/release-candidate.json`.
- This is a release-gate boundary, not completion: P1 complete registry/
  component sources, P2 unified physical commit, and P4 official MySQL
  interoperability remain open.

## Continuation 1273 update

- Closed two source-backed P1 `INFORMATION_SCHEMA.INNODB_METRICS` rows:
  `buffer_data_reads` and `buffer_data_written` now use the live Buffer Pool
  page-read/page-write counters and expose `buffer/status_counter` metadata.
- The red test, focused metrics selection, and Buffer Pool-related selection
  passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-buffer-data-io-current-continuation1273.txt`.
- The complete MySQL 314-row metric registry and missing component/runtime
  sources remain P1 work; P2/P3/P4 remain open and non-InnoDB remains out of
  scope.

## Continuation 1278 update

- Added real executor-lease sources for six P1 `INFORMATION_SCHEMA.INNODB_METRICS`
  rows: metadata table handles opened, closed, and current references plus the
  corresponding table-lock created, removed, and current-lock projections.
- The focused lifecycle/static-catalog tests, full InnoDB engine regression, and
  full serial repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-metadata-handles-current-continuation1278.txt`.
- This does not change the global boundary: complete I_S/P_S runtime semantics,
  unified P2 physical commit/interoperability, the full P3 client matrix, and P4
  official MySQL comparison remain open; FULLTEXT is deferred and non-InnoDB is
  out of scope.

### Continuation 1355

- [x] P1 Performance Schema `host_cache` metadata: MySQL 8.4 IP/host
  character shapes, validation enum, signed error counters, and timestamp
  nullability are now explicit rather than generic fallback metadata.
- [x] Focused metadata test, I_S/P_S regression, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-host-cache-metadata-current-continuation1355.txt`.
- [ ] Complete P_S table/column coverage, component runtime semantics,
  permissions, P2/P3/P4 interoperability and client matrix work remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1356

- [x] P1 Performance Schema `table_handles` and
  `prepared_statements_instances` metadata: object-field nullability/lengths,
  prepared-statement execution-engine enum, MySQL 8.4 added CPU/memory/secondary
  columns, and the official column set are now explicit and ordered correctly.
- [x] Focused metadata/runtime tests, I_S/P_S regression, and full serial
  repository regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-table-handle-prepared-metadata-current-continuation1356.txt`.
- [ ] Full P_S table/column coverage, component runtime semantics, permissions,
  P2/P3/P4 interoperability and client matrix work remain open. FULLTEXT
  remains deferred and non-InnoDB remains out of scope.

## Continuation 1279 update

- Added a LockManager-backed source for `INFORMATION_SCHEMA.INNODB_METRICS.lock_rec_locks`,
  covering the current number of granted record-lock requests.
- The focused manager/engine tests, full InnoDB engine regression, and full serial
  repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-record-lock-current-continuation1279.txt`.
- Remaining lock wait/timeout/grant/release metrics, complete I_S/P_S runtime
  semantics, P2 unified physical commit, P3 full client matrix, and P4 official
  interoperability remain open.

## Continuation 1280 update

- Added the LockManager completed-wait source for
  `INFORMATION_SCHEMA.INNODB_METRICS.lock_rec_lock_waits`, including the
  existing `module_lock` enable/disable/reset-baseline behavior.
- A real conflict/release scenario first reproduced the stale zero metric; the
  focused manager/engine tests, full InnoDB engine regression, and full serial
  repository regression then passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-record-lock-waits-current-continuation1280.txt`.
- Lock timeout and grant/release-attempt metrics, complete I_S/P_S runtime
  semantics, P2 unified physical commit, P3 full client matrix, and P4 official
  interoperability remain open.

## Continuation 1281 update

- Added LockManager-backed sources for
  `INFORMATION_SCHEMA.INNODB_METRICS.lock_rec_grant_attempts` and
  `lock_rec_release_attempts`, including a second grant attempt when a waiting
  request is actually granted after release.
- Focused manager/engine tests and the full InnoDB engine regression passed; the
  full serial repository run completed with no failed package output. Evidence:
  `reports/compatibility/p1-innodb-metrics-record-lock-attempts-current-continuation1281.txt`.
- Lock timeouts, table-lock waits, complete I_S/P_S runtime semantics, P2
  unified physical commit/native interoperability, P3 full client matrix, and
  P4 official interoperability remain open.

## Continuation 1282 update

- Added the tableDDLCoordinator source for
  `INFORMATION_SCHEMA.INNODB_METRICS.lock_table_lock_waits`, with independent
  runtime and reset baselines.
- The focused real owner/waiter/release test, full InnoDB engine regression, and
  full serial repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-table-lock-waits-current-continuation1282.txt`.
- Lock timeouts, complete I_S/P_S runtime semantics, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and P4
  official interoperability remain open.

## Continuation 1283 update

- Added a real table-lock deadline source for
  `INFORMATION_SCHEMA.INNODB_METRICS.lock_timeouts`; immediate lock conflicts
  are not counted as timeouts, and reset baselines are independent.
- Focused tests and the full serial repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-lock-timeouts-current-continuation1283.txt`.
- Native record-lock wait/timeout behavior, complete I_S/P_S runtime semantics,
  P2 native XA/binlog/replication/crash interoperability, P3 full client
  matrix, and P4 official interoperability remain open.

## Continuation 1284 update

- Added source-backed `INFORMATION_SCHEMA.INNODB_METRICS.trx_rw_commits` and
  `trx_ro_commits` counters from the real transaction completion boundary,
  including `module_trx` enable/disable/reset-baseline behavior.
- Focused tests, the full InnoDB engine regression, and the full serial
  repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-transaction-commit-modes-current-continuation1284.txt`.
- `trx_nl_ro_commits` remains open because the current transaction path has no
  authoritative MySQL-compatible non-locking read-only classification. The
  complete I_S/P_S registry/runtime/permission task, P2 native XA/binlog/
  replication/crash interoperability, P3 full client matrix, and P4 official
  fixture remain open; FULLTEXT remains deferred and non-InnoDB remains out of scope.

## Continuation 1285 update

- Added source-backed `INFORMATION_SCHEMA.INNODB_METRICS.trx_commits_insert_update`
  from the existing transaction DML change journal. INSERT/UPDATE-containing
  commits increment it; DELETE-only and READ ONLY commits do not.
- Focused tests, the full InnoDB engine regression, and the full serial
  repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-transaction-insert-update-commits-current-continuation1285.txt`.
- `trx_nl_ro_commits`, the complete I_S/P_S registry/runtime/permission task,
  P2 native XA/binlog/replication/crash interoperability, P3 full client
  matrix, and P4 official fixture remain open; FULLTEXT remains deferred and
  non-InnoDB remains out of scope.

## Continuation 1286 update

- Added source-backed `INFORMATION_SCHEMA.INNODB_METRICS.trx_rollbacks_savepoint`
  from successful `ROLLBACK TO SAVEPOINT` execution, including `module_trx`
  enable/disable/reset-baseline behavior.
- Focused tests, the full InnoDB engine regression, and the full serial
  repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-savepoint-rollbacks-current-continuation1286.txt`.
- `trx_nl_ro_commits`, the complete I_S/P_S registry/runtime/permission task,
  P2 native XA/binlog/replication/crash interoperability, P3 full client
  matrix, and P4 official fixture remain open; FULLTEXT remains deferred and
  non-InnoDB remains out of scope.

## Continuation 1287 update

- Added source-backed `INFORMATION_SCHEMA.INNODB_METRICS.trx_nl_ro_commits`
  from successful table-backed non-locking autocommit SELECT completion;
  constant and metadata SELECTs are excluded, and explicit READ ONLY
  transactions remain under `trx_ro_commits`.
- Added `module_trx` enable/disable/reset-baseline handling and regression
  coverage. Focused tests, the full InnoDB engine regression, and the full
  serial repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-metrics-nonlocking-autocommit-readonly-current-continuation1287.txt`.
- The complete I_S/P_S registry/runtime/permission task, P2 native
  XA/binlog/replication/crash interoperability, P3 full client matrix, and
  P4 official fixture remain open; FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

## Continuation 1271 update

- The fresh serial repository regression passed:
  `go test -p 1 ./... -count=1 -timeout 45m` (exit 0). The current local
  cluster smoke also passed. Evidence:
  `reports/compatibility/global-audit-current-continuation1271.txt`.
- The refreshed client diagnostics confirm Go, PyMySQL, and Node.js/mysql2
  are available, while `mysql.exe` is absent and the protected client
  password is not provided; no authenticated full-matrix claim is made.
- The current matrix remains 13 implemented, 8 partial, 1 unverified,
  1 pending_external, 1 deferred, and 2 out_of_scope. Complete I_S/P_S
  semantics, the unified storage/WAL/binlog/GTID/replica/crash commit point,
  the full non-Connector/J matrix, and official MySQL interoperability remain
  open. FULLTEXT is deferred and non-InnoDB remains out of scope.

## Continuation 1270 update

- Closed a source-backed P1 `INFORMATION_SCHEMA.INNODB_METRICS` slice:
  `dml_reads` now records `SelectExecutor.rowsExamined` after successful table
  reads and participates in the existing `module_dml` enable/disable/reset
  lifecycle.
- Verified the three-row scan red/green test, the metrics regression selection,
  and the complete InnoDB engine regression. Evidence:
  `reports/compatibility/p1-innodb-metrics-dml-reads-current-continuation1270.txt`.

## Continuation 1318 update

- Implemented an independent completed object-wait summary for
  `performance_schema.objects_summary_global_by_type`, aggregation of repeated
  waits by object, and MySQL-compatible truncate behavior that retains object
  rows while zeroing summary counters.
- Official reference: [MySQL object summary global-by-type table](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-objects-summary-global-by-type-table.html).
  The red/green aggregation and truncate test, all Performance Schema tests,
  affected packages, and `go test -p 1 ./... -count=1 -timeout 35m` passed.
  Evidence:
  `reports/compatibility/p1-performance-schema-object-summary-current-continuation1318.txt`.
- Complete I_S/P_S coverage, native XA/binlog/replication/crash
  interoperability, the full non-Connector-J client matrix, and official
  MySQL fixture validation remain open; FULLTEXT is deferred and non-InnoDB
  remains out of scope.
- Complete I_S/P_S semantics, native MySQL XA/binlog/GTID/replication/
  crash-recovery interoperability, and the full non-Connector-J client matrix
  remain open; non-InnoDB remains out of scope.
## Continuation 1320 update

- Added independent metadata-lock wait history and history-long buffers for
  `performance_schema.events_waits_history` and
  `events_waits_history_long`.
- Added independent query projection and `TRUNCATE TABLE` reset behavior, with
  a red/green regression proving that long-history truncation preserves the
  short metadata-lock history.
- Focused Performance Schema tests, affected packages, and the full serial
  repository regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-mdl-history-truncate-current-continuation1320.txt`.
- Complete I_S/P_S semantics, native XA/binlog/replication/crash
  interoperability, the full non-Connector-J client matrix, and official
  MySQL fixture validation remain open; FULLTEXT remains deferred and
  non-InnoDB remains out of scope.

## Continuation 1343 update

- Implemented separate phase GTIDs for two-phase XA. XA PREPARE retains its
  prepare GTID; XA COMMIT/ROLLBACK receives an independent terminal GTID, which
  is emitted as a native `GTID_EVENT` before the terminal XA query.
- Preserved both identities through logical events, native decoding, replica
  apply, relay import, restart recovery, promotion history, and idempotent
  retries. Evidence:
  `reports/compatibility/p2-xa-separate-phase-gtids-current-continuation1343.txt`.
- Fresh verification passed: replication package, engine XA/recovery slice, and
  `go test -p 1 ./... -count=1 -timeout 45m`. Official MySQL fixture
  interoperability remains P4 pending_external; broader P2 atomic commit
  semantics remain partial. Complete I_S/P_S semantics and the full
  non-Connector-J client matrix remain global tasks; FULLTEXT stays deferred and
  non-InnoDB stays out of scope.

## Continuation 1341 update

- Routed SQL updates to Performance Schema `setup_*` tables through the current
  authenticated session and enforced table-level `UPDATE` privilege checks.
  Unprivileged updates now fail and an account granted `UPDATE` on the matching
  setup table succeeds. The focused red/green test and full Performance Schema
  test set passed. Evidence:
  `reports/compatibility/p1-performance-schema-setup-update-privilege-current-continuation1341.txt`.
- Complete I_S/P_S table, component, lifecycle, and runtime semantics remain
  P1 partial; native XA/binlog/replication/crash interoperability, the full
  non-Connector-J client matrix, and official MySQL fixture validation remain
  open. FULLTEXT remains deferred and non-InnoDB remains out of scope.

## Continuation 1334 update

- Added timestamp-based `PURGE BINARY LOGS BEFORE` and the legacy
  `PURGE MASTER LOGS BEFORE` alias. The writer chooses the file containing the
  first event at or after the cutoff, persists the retention boundary, prunes
  physical GTID positions, and protects the boundary during native rebuild.
- Focused red/green tests and the affected replication, engine, and net packages
  passed. Evidence:
  `reports/compatibility/p2-purge-binary-logs-before-current-continuation1334.txt`.
- P2 remains partial pending the unified storage/WAL/native-binlog/GTID/relay/
  applied-state commit protocol and official MySQL fixture interoperability.
  P1/P3, FULLTEXT, and non-InnoDB boundaries are unchanged.

## Continuation 1335 update

- Added restart-time recovery for a storage-committed client transaction whose
  source publisher failed. Durable row images and the stable transaction key are
  republished, then the committed marker is written and the active journal is
  cleared. Replica replay journals are explicitly excluded from upstream
  republishing.
- Test-first focused coverage and the full serial repository regression passed.
  Evidence:
  `reports/compatibility/p2-replication-commit-recovery-current-continuation1335.txt`.
- P2 remains partial pending one physical storage/WAL/native-binlog/GTID/relay/
  applied-state commit protocol, complete promotion semantics, and official
  MySQL fixture interoperability. P1/P3 remain open; FULLTEXT is deferred and
  non-InnoDB remains out of scope.

## Continuation 1336 update

- Audited INFORMATION_SCHEMA/PERFORMANCE_SCHEMA registry coverage against the
  MySQL 8.4 official table references. Core registrations, dedicated runtime
  handlers, and SELECT/COLUMNS shape checks are present; absent component
  runtimes do not emit fabricated rows.
- P1 remains partial because exact column metadata, privilege visibility, and
  complete lifecycle/runtime semantics still require independent closure.
  Evidence:
  `reports/compatibility/p1-official-schema-registry-audit-current-continuation1336.txt`.

## Continuation 1337 update

- Extended durable commit recovery to preserve original SQL statements alongside
  row images and the stable transaction key. A restart retry can now reproduce
  both row-based and statement-based source publication payloads.
- Red/green focused coverage, affected packages, and the full serial repository
  regression passed. Evidence:
  `reports/compatibility/p2-replication-commit-statements-recovery-current-continuation1337.txt`.
- P2 remains partial pending the unified physical storage/WAL/native-binlog/GTID/
  relay/applied-state commit protocol and official MySQL fixture interoperability.

## Continuation 1338 update

- `Source.AppendCommittedTransactionWithKey` now returns the physical events from
  the first successful keyed commit instead of discarding them. A repeated key
  remains idempotent and returns no duplicate events.
- The red/green regression and the full `server/replication` package regression
  pass. Evidence: `reports/compatibility/p2-keyed-commit-event-return-current-continuation1338.txt`.
- This closes only keyed commit result observability; the unified physical commit
  protocol, complete I_S/P_S semantics, full non-Connector-J matrix, and official
  MySQL fixture remain open. FULLTEXT is deferred and non-InnoDB is out of scope.

## Continuation 1340 update

- Added a promotion regression proving that a promoted replica exposes its
  committed relay history through the new source's logical and native binlogs.
- Imported events retain upstream GTID/key identity while native frames use the
  promoted server-id; subsequent promoted writes remain ordered after history.
- Focused promotion/XA/native tests and the full replication package passed.
- The unified storage/WAL/native-binlog/GTID/relay/applied-state commit protocol
  and official MySQL interoperability fixture remain open.

## Continuation 1339 update

- Statement-only autocommit publication now writes and syncs the stable
  `replication_commit` journal record before invoking the source publisher.
  Publisher failure therefore leaves the exact SQL payload recoverable after
  restart, including DDL with no row image.
- The red/green recovery test and affected engine/replication/net regressions
  pass. Evidence: `reports/compatibility/p2-autocommit-statement-recovery-current-continuation1339.txt`.
- P2 remains partial pending the unified physical commit protocol and official
  MySQL fixture; complete I_S/P_S, the full non-Connector-J matrix, and those
  external interoperability gates remain open. FULLTEXT is deferred and
  non-InnoDB is out of scope.

## Continuation 1331 update

The HTTP logical replication response now carries `log_name`. `Runtime.pullOnce`
uses that source filename when persisting and publishing the final replica
source-coordinate state, so restart status retains both the binlog file and
position. Focused and full replication-package tests pass. Evidence:
`reports/compatibility/p2-runtime-source-file-persistence-current-continuation1331.txt`.
The P2 unified physical commit protocol and official MySQL fixture remain open.

## Continuation 1332 update

The fresh client diagnostic finds Go, PyMySQL, and Node/mysql2 available, while
mysql CLI remains unavailable because `mysql.exe` is missing. Protected live
client execution was not attempted without the configured password; the P3
matrix therefore remains partial. Evidence:
`reports/compatibility/p3-client-matrix-diagnostic-current-continuation1332.txt`.

## Continuation 1333 update

- Added durable `PURGE BINARY LOGS TO` support across `BinlogWriter`, `Source`,
  `Runtime`, and the SQL admin boundary. Purged native files and GTID physical
  positions are removed, while the first retained file number is persisted so
  restart/rebuild does not resurrect purged files.
- Red/green evidence and focused package verification are recorded in
  `reports/compatibility/p2-purge-binary-logs-current-continuation1333.txt`.
- The unified storage/WAL/native-binlog/GTID/relay/applied-state commit
  protocol, `PURGE ... BEFORE` time semantics, P4 official MySQL fixture,
  complete I_S/P_S semantics, and full non-Connector-J client matrix remain
  open. FULLTEXT is deferred and non-InnoDB remains out of scope.

## Continuation 1327 update

- Added dedicated source-backed projections for `mysql.role_edges` and
  `mysql.default_roles`, using the durable role and default-role state already
  maintained by the account compatibility layer.
- Added column projection and grant-table filters, plus a regression that
  verifies role edges, `WITH_ADMIN_OPTION`, and default-role rows after GRANT
  and SET DEFAULT ROLE.
- Focused role-table and account/role/privilege/metadata regressions passed;
  the full serial repository regression also passed. Evidence:
  `reports/compatibility/p1-role-grant-tables-current-continuation1327.txt`.
- Grantor provenance in INFORMATION_SCHEMA role grant views, complete I_S/P_S
  runtime/component/permission semantics, P2 native interoperability, P3
  client matrix, and P4 official MySQL fixture validation remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1354

- [x] P1 INFORMATION_SCHEMA privilege visibility: persisted account and
  enabled-role `GlobalGrants` now participate in effective `*.*` checks, so
  the MySQL `CREATE USER + SYSTEM_USER` administrator exception is honored.
- [x] Focused privilege test, account/role regression, and full serial
  repository regression passed. Evidence:
  `reports/compatibility/p1-information-schema-dynamic-system-user-visibility-current-continuation1354.txt`.
- [ ] Complete role inheritance, partial-revoke and object-level visibility
  matrices, complete I_S/P_S semantics, P2/P3/P4 interoperability and client
  matrix work remain open. FULLTEXT remains deferred and non-InnoDB remains
  out of scope.

## Continuation 1328 update

- Persisted grantor provenance for newly written table, column, routine, and
  role grants in backward-compatible account metadata.
- Updated ROLE_TABLE_GRANTS, ROLE_COLUMN_GRANTS, and ROLE_ROUTINE_GRANTS to
  project the recorded grantor, with root@localhost fallback for legacy rows.
- The non-root grantor red test, role/account/I_S regressions, and the full
  serial repository regression passed. Evidence:
  `reports/compatibility/p1-role-grantor-provenance-current-continuation1328.txt`.
- Complete I_S/P_S component/runtime/permission parity, P2/P3/P4, FULLTEXT,
  and non-InnoDB boundaries remain as previously defined.

## Continuation 1326 update

- Corrected replication replay transaction-context ownership so an `autocommit=0`
  replay transaction is not registered as a client storage transaction as well.
- Added a durable recovery boundary for the case where storage reaches COMMITTED
  but a later dirty-page flush fails: persist and sync `replication_commit` before
  returning the flush error. Evidence:
  `reports/compatibility/p2-replication-storage-commit-record-current-continuation1326.txt`.
- The complete storage/WAL/native-binlog/GTID/relay/applied-state atomic protocol
  and official MySQL fixture interoperability remain open; P3 client coverage,
  FULLTEXT, and non-InnoDB scope boundaries are unchanged.

## Continuation 1325 update

- Fixed the native pull path so `Replica.ReplicateNativeFrom` persists physical
  `native_relay_events` together with prepared-XA state, using the same durable
  boundary as `ApplyNative`.
- The regression first failed with an empty native relay after a pulled XA
  PREPARE, then passed after the fix. Replication, net, and engine native/binlog/
  GTID/XA/recovery/promotion tests passed. Evidence:
  `reports/compatibility/p2-native-replica-relay-persistence-current-continuation1325.txt`.
- The unified storage/WAL/native-binlog/GTID/applied-state physical commit protocol
  and official MySQL fixture remain open; P3 client coverage, FULLTEXT, and
  non-InnoDB scope boundaries are unchanged.

## Continuation 1324 update

- Added source-backed file and socket rows to `performance_schema.setup_instruments`:
  `wait/io/file/innodb/innodb_data_file`, `wait/io/file/innodb/innodb_log_file`,
  `wait/io/file/sql/FRM`, `wait/io/file/sql/file`, and
  `wait/io/socket/sql/client_connection`.
- Synchronized the SQL setup state with low-level file/socket recording. `ENABLED=NO`
  suppresses new events; `TIMED=NO` preserves counts/bytes while suppressing wait-time
  accumulation. Focused, affected-package, and full serial regression evidence is in
  `reports/compatibility/p1-performance-schema-file-socket-instrument-control-current-continuation1324.txt`.
- Complete I_S/P_S semantics, native XA/binlog/replication/crash interoperability, the
  full non-Connector-J client matrix, and official MySQL fixture validation remain open;
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

## Continuation 1329 update

- Added durable source file and source position fields to the replica state file.
  `NewReplica` now restores them, and RESET REPLICA clears them together with the
  applied-transaction markers.
- Native and logical pull replication carry a source-position snapshot only into the
  final durable state replacement. Intermediate relay and applied-marker writes retain
  the previous position so a crash before storage apply cannot skip a source transaction.
- `TestReplicaPersistsNativeSourcePositionAcrossRestart` verifies restart/resume and
  GTID idempotence. The focused replication package and full serial repository regression
  passed. Evidence:
  `reports/compatibility/p2-native-source-position-persistence-current-continuation1329.txt`.
- Added final-state-persist failure injection to verify that source position remains at
  the old durable boundary and the restart path uses the applied marker without a second
  storage apply.
- P2 remains partial pending a single physical storage/WAL/native-binlog/GTID/relay/
  applied-state commit protocol and official MySQL fixture interoperability. P3 client
  coverage, P4, FULLTEXT and non-InnoDB scope boundaries are unchanged.

## Continuation 1330 update

- Fixed the background HTTP replica pull path: `Runtime.pullOnce` now persists the
  source position as part of the final replica state replacement instead of updating
  it only in memory after `Replica.Apply`. Empty GTID-filtered responses that advance
  `next_position` are persisted too.
- `TestRuntimePullPersistsSourcePositionAcrossRestart` first failed with a live position
  of 8 reopening as 0, then passed after the fix. Replication and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p2-runtime-pull-source-position-current-continuation1330.txt`.
- The unified physical storage/WAL/native-binlog/GTID/relay/applied-state commit protocol
  and official MySQL fixture remain open; P3/P4, FULLTEXT and non-InnoDB boundaries are
  unchanged.

## Global scope alignment 2026-09-25

The confirmed global boundary is consolidated in
`docs/planning/GLOBAL_COMPATIBILITY_SCOPE_20260925.md`. Complete
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA coverage, XA/native binlog/GTID/relay/applied-state/
crash-recovery interoperability, the full non-Connector-J client matrix, and the official
MySQL fixture remain in scope. Connector/J remains P0 alongside startup and cluster
compatibility. FULLTEXT stays deferred, while non-InnoDB engines and non-InnoDB
repair/conversion remain explicitly out of scope.

## Continuation 1326 update

- Corrected replication replay transaction-context ownership so an `autocommit=0`
  replay transaction is not registered as a client storage transaction as well.
- Added a durable recovery boundary for the case where storage reaches COMMITTED
  but a later dirty-page flush fails: persist and sync `replication_commit` before
  returning the flush error. Evidence:
  `reports/compatibility/p2-replication-storage-commit-record-current-continuation1326.txt`.
- The complete storage/WAL/native-binlog/GTID/relay/applied-state atomic protocol
  and official MySQL fixture interoperability remain open; P3 client coverage,
  FULLTEXT, and non-InnoDB scope boundaries are unchanged.

## Continuation 1325 update

- Fixed the native pull path so `Replica.ReplicateNativeFrom` persists physical
  `native_relay_events` together with prepared-XA state, using the same durable
  boundary as `ApplyNative`.
- The regression first failed with an empty native relay after a pulled XA
  PREPARE, then passed after the fix. Replication, net, and engine native/binlog/
  GTID/XA/recovery/promotion tests passed. Evidence:
  `reports/compatibility/p2-native-replica-relay-persistence-current-continuation1325.txt`.
- The unified storage/WAL/native-binlog/GTID/applied-state physical commit protocol
  and official MySQL fixture remain open; P3 client coverage, FULLTEXT, and
  non-InnoDB scope boundaries are unchanged.

## Continuation 1323 update

- Added the source-backed `memory/sql/THD::main_mem_root` instrument to
  `performance_schema.setup_instruments`, with `ENABLED='YES'` and
  `TIMED=NULL` as required for non-timed memory instrumentation.
- Gated both executor and engine memory allocation/free recording by the
  instrument and global/thread consumers. The focused control test, all
  Performance Schema tests, affected packages, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-memory-instrument-control-current-continuation1323.txt`.
- Other memory instruments, complete I_S/P_S semantics, native
  XA/binlog/replication/crash interoperability, the full non-Connector-J
  client matrix, and official MySQL fixture validation remain open; FULLTEXT
  remains deferred and non-InnoDB remains out of scope.

## Continuation 1324 update

- Added source-backed file and socket rows to `performance_schema.setup_instruments`:
  `wait/io/file/innodb/innodb_data_file`, `wait/io/file/innodb/innodb_log_file`,
  `wait/io/file/sql/FRM`, `wait/io/file/sql/file`, and
  `wait/io/socket/sql/client_connection`.
- Synchronized the SQL setup state with low-level file/socket recording. `ENABLED=NO`
  suppresses new events; `TIMED=NO` preserves counts/bytes while suppressing wait-time
  accumulation. Focused and affected-package regression evidence is in
  `reports/compatibility/p1-performance-schema-file-socket-instrument-control-current-continuation1324.txt`.
- Complete I_S/P_S semantics, native XA/binlog/replication/crash interoperability, the
  full non-Connector-J client matrix, and official MySQL fixture validation remain open;
  FULLTEXT remains deferred and non-InnoDB remains out of scope.
## Continuation 1321 update

- Added independent global/account/host/user/thread error-summary aggregates
  for the Performance Schema error summary tables.
- Added dimension-aware projection and truncate behavior: derived-dimension
  truncation is isolated, while global truncation resets the derived views as
  required by MySQL.
- Focused error-summary tests, all Performance Schema tests, affected
  packages, and the full serial repository regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-error-summary-dimension-truncate-current-continuation1321.txt`.
- Complete I_S/P_S runtime, component, permission, and connection-lifecycle
  semantics; native XA/binlog/replication/crash interoperability; the full
  non-Connector-J client matrix; and official MySQL fixture validation remain
  open. FULLTEXT remains deferred and non-InnoDB remains out of scope.

## Continuation 1322 update

- Added the `error` instrument to `performance_schema.setup_instruments` with
  MySQL-compatible default enable/timing values.
- Routed executor-side error-summary recording through the instrument setting
  and global/thread instrumentation gates. The focused control test, all
  Performance Schema tests, affected packages, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-error-instrument-control-current-continuation1322.txt`.
- Complete I_S/P_S semantics, native XA/binlog/replication/crash
  interoperability, the full non-Connector-J client matrix, and official
  MySQL fixture validation remain open; FULLTEXT remains deferred and
  non-InnoDB remains out of scope.
### Continuation 1344

- P2 closed the local XA rollback terminal retry gap. A retry after durable
  rollback completion now succeeds even when prepared-XA memory state was lost
  or the source was reloaded, and terminal GTIDs are deduplicated for both
  commit and rollback.
- Focused red/green, replication, engine XA/recovery/promotion, and full serial
  repository regression all passed. Evidence:
  `reports/compatibility/p2-xa-rollback-retry-idempotency-current-continuation1344.txt`.
- The unified storage/WAL/native-binlog/GTID/relay/applied-state protocol and
  official MySQL fixture interoperability remain open. P1 full I_S/P_S semantics
  and P3 full non-Connector-J client coverage remain global tasks; FULLTEXT is
  deferred and non-InnoDB remains out of scope.
### Continuation 1345

- [x] P1 Performance Schema synchronization instance metadata: precise MySQL
  8.4 contracts for `cond_instances`, `mutex_instances`, and `rwlock_instances`
  are now used instead of the generic VARCHAR/nullable fallback.
- [x] Red/green focused test, I_S/P_S regression, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-sync-instance-metadata-current-continuation1345.txt`.
- [ ] Keep full P1 runtime rows, complete per-table metadata, privilege/lifecycle
  semantics, P2/P3/P4 interoperability, and client matrix work open. FULLTEXT
  remains deferred and non-InnoDB remains out of scope.
### Continuation 1346

- [x] P1 Performance Schema file/socket instance metadata: exact MySQL 8.4
  definitions for `file_instances` and `socket_instances` are now used instead
  of generic fallback metadata.
- [x] Red/green focused test, I_S/P_S regression, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-file-socket-instance-metadata-current-continuation1346.txt`.
- [ ] Full instance lifecycle/runtime rows, all P1 metadata and permissions,
  P2/P3/P4 interoperability and client matrix work remain open. FULLTEXT is
  deferred and non-InnoDB remains out of scope.
### Continuation 1347

- [x] P1 Performance Schema `performance_timers` metadata: exact MySQL 8.4
  `TIMER_NAME` enum and nullable BIGINT timer characteristic columns now replace
  the generic virtual-column fallback.
- [x] Red/green focused test, I_S/P_S regression, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-performance-timers-metadata-current-continuation1347.txt`.
- [ ] Complete P1 table/field coverage, runtime timer and event lifecycle,
  permissions, P2/P3/P4 interoperability and client matrix work remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.
### Continuation 1348

- [x] P1 Performance Schema connection-attribute metadata: exact MySQL 8.4
  definitions for `session_connect_attrs` and `session_account_connect_attrs`
  now replace generic fallback metadata.
- [x] Red/green focused test, I_S/P_S regression, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-connection-attributes-metadata-current-continuation1348.txt`.
- [ ] Real connection-attribute lifecycle ingestion, all P1 metadata and
  permissions, P2/P3/P4 interoperability and client matrix work remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1349

- [x] P1 Performance Schema thread-variable/status metadata: exact MySQL 8.4
  definitions for `user_variables_by_thread`, `variables_by_thread`, and
  `status_by_thread` now replace the generic fallback metadata.
- [x] Focused metadata test, I_S/P_S regression, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-thread-variable-metadata-current-continuation1349.txt`.
- [ ] Runtime row lifecycle, complete P1 table/field coverage, permissions,
  P2/P3/P4 interoperability and client matrix work remain open. FULLTEXT
  remains deferred and non-InnoDB remains out of scope.

### Continuation 1350

- [x] P1 Performance Schema memory-summary metadata: exact MySQL 8.4
  definitions for the global, thread, account, host, and user memory summary
  tables now replace generic fallback nullability and numeric shapes.
- [x] Focused metadata test, I_S/P_S regression, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-memory-summary-metadata-current-continuation1350.txt`.
- [ ] Runtime aggregation, complete P1 table/field coverage, permissions,
  P2/P3/P4 interoperability and client matrix work remain open. FULLTEXT
  remains deferred and non-InnoDB remains out of scope.

### Continuation 1351

- [x] P1 Performance Schema status-dimension metadata: exact MySQL 8.4
  definitions for `status_by_account`, `status_by_host`, and `status_by_user`
  now replace the generic fallback metadata.
- [x] Focused metadata test, I_S/P_S regression, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-status-dimension-metadata-current-continuation1351.txt`.
- [ ] Runtime status aggregation, complete P1 table/field coverage,
  permissions, P2/P3/P4 interoperability and client matrix work remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1352

- [x] P1 Performance Schema variable metadata and registry: exact MySQL 8.4
  definitions for session/global status and variable views, persisted variables,
  and `variables_info`, including `MIN_VALUE` and `MAX_VALUE`, are now exposed.
- [x] Focused metadata test, I_S/P_S regression, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-variable-metadata-current-continuation1352.txt`.
- [ ] Runtime variable persistence, complete P1 table/field coverage,
  permissions, P2/P3/P4 interoperability and client matrix work remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1353

- [x] P1 Performance Schema I/O summary metadata/runtime projection: exact
  MySQL 8.4 definitions for table, file, and socket I/O summary views now
  include official column order, signed/unsigned byte totals, and file byte
  accumulation from IBD reads/writes.
- [x] Focused metrics/engine tests, I_S/P_S regression, and full serial
  repository regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-io-summary-metadata-current-continuation1353.txt`.
- [ ] Complete P1 table/field coverage, all runtime lifecycle semantics,
  permissions, P2/P3/P4 interoperability and client matrix work remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1357

- [x] P1 SQL text-protocol prepared statements: implement session-local named
  `PREPARE ... FROM`, `EXECUTE ... USING`, and `DEALLOCATE PREPARE`; merge their
  inventory and execution accounting into
  `performance_schema.prepared_statements_instances`; clear them on reset and
  change-user lifecycle boundaries.
- [x] Focused SQL prepared-statement test, I_S/P_S regression, and network
  prepared/reset/change-user regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-sql-prepared-statements-current-continuation1357.txt`.
- [ ] Complete I_S/P_S table/field/runtime/component/permission semantics,
  unified P2 storage/WAL/native-binlog/GTID commit, P3 full client matrix, and
  P4 official MySQL fixture remain open. FULLTEXT is deferred and non-InnoDB
  remains out of scope.

### Continuation 1358

- [x] P2 durable commit barrier: ordinary client and replication storage
  transactions now publish the local recovery marker immediately after the
  real storage commit and before dirty-page flushing; callback errors preserve
  flushing and retry/recovery behavior.
- [x] Focused ordering test, full engine regression, network lifecycle
  regression, and full serial repository regression passed. Evidence:
  `reports/compatibility/p2-storage-commit-barrier-current-continuation1358.txt`.
- [x] Full regression caught and fixed XA double publication: XA sessions skip
  the ordinary client publisher and retain the XA-specific terminal publisher.
- [ ] External MySQL native binlog/GTID/XA fixture and promotion topology,
  complete P1 I_S/P_S runtime/component/permission semantics, and the full P3
  client matrix remain open. FULLTEXT remains deferred and non-InnoDB remains
  out of scope.

### Continuation 1360

- [x] P2 applied-GTID barrier: replication replay now writes the durable
  committed/applied marker alongside the replication journal marker immediately
  after real storage commit and before dirty-page flushing; outer replay COMMIT
  remains idempotent.
- [x] Focused marker-order test, full engine regression, and full serial
  repository regression passed. Evidence:
  `reports/compatibility/p2-applied-gtid-commit-barrier-current-continuation1360.txt`.
- [ ] Official MySQL fixture, external promotion topology, complete P1
  component/permission semantics, and full P3 client matrix remain open.

### Continuation 1373

- [x] P1 Performance Schema statement-summary lifecycle: global, thread,
  account, host, and user dimensions now have independent aggregates, and
  dimension-specific `TRUNCATE` resets only its own rows.
- [x] Focused isolation, Performance Schema, metrics, and full engine
  regressions passed. Evidence:
  `reports/compatibility/p1-performance-schema-summary-dimension-truncate-current-continuation1373.txt`.
- [ ] Other summary families still need equivalent dimension-specific runtime
  and reset semantics; complete I_S/P_S coverage, permissions, component
  lifecycle, P2 native binlog/GTID/XA/crash/promotion interoperability, and
  the P3 non-Connector/J client matrix remain open. FULLTEXT remains deferred
and non-InnoDB remains out of scope.

### Continuation 1375

- [x] P1 Performance Schema transaction-summary lifecycle: thread, account,
  host, and user dimensions now use independent reset cutoffs, so a
  dimension-specific `TRUNCATE` preserves global and other dimensions.
- [x] Focused isolation, transaction/summary, and full engine regressions
  passed. Evidence:
  `reports/compatibility/p1-performance-schema-transaction-summary-dimension-truncate-current-continuation1375.txt`.
- [ ] Wait-summary dimension isolation, complete I_S/P_S coverage, permissions,
  component lifecycle, P2 native binlog/GTID/XA/crash/promotion
  interoperability, and the P3 non-Connector-J client matrix remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1374

- [x] P1 Performance Schema stage-summary lifecycle: thread, account, host,
  and user dimensions now have independent aggregates, and dimension-specific
  `TRUNCATE` resets only its own rows.
- [x] Focused isolation, stage/summary, metrics, full engine, and full serial
  repository regressions passed. Evidence:
  `reports/compatibility/p1-performance-schema-stage-summary-dimension-truncate-current-continuation1374.txt`.
- [ ] Other summary families still need equivalent dimension-specific runtime
  and reset semantics; complete I_S/P_S coverage, permissions, component
  lifecycle, P2 native binlog/GTID/XA/crash/promotion interoperability, and
  the P3 non-Connector/J client matrix remain open. FULLTEXT remains deferred
  and non-InnoDB remains out of scope.

### Continuation 1379

- [x] P1 INFORMATION_SCHEMA InnoDB tablespace metadata: align
  `INNODB_TABLESPACES` native column order and explicit INT/BIGINT,
  unsignedness, lengths, numeric precision, and nullability.
- [x] Focused red/green metadata regression, I_S privilege/role regression,
  full engine regression, and full serial repository regression passed.
  Evidence:
  `reports/compatibility/p1-innodb-tablespaces-metadata-current-continuation1379.txt`.
- [ ] Complete I_S/P_S table/field/runtime/component/permission semantics,
  P2 native binlog/GTID/XA/crash/promotion interoperability, the P3
  non-Connector-J client matrix, and the P4 official MySQL fixture remain
  open. FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1385

- [x] P1 Performance Schema replication-filter metadata: register the complete
  MySQL 8.4 column sets for `replication_applier_filters` and
  `replication_applier_global_filters`, with exact CHAR/LONGTEXT/ENUM/
  TIMESTAMP(6)/BIGINT UNSIGNED contracts and nullability.
- [x] Red/green metadata regression, focused Performance Schema tests, full
  engine regression, and full serial repository regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-replication-filters-metadata-current-continuation1385.txt`.
- [ ] Runtime filter lifecycle is still not implemented because xmysql has no
  native replication-filter source; complete I_S/P_S coverage, P2 native
  binlog/GTID/XA/crash/promotion interoperability, P3 non-Connector-J client
  matrix, and P4 official MySQL fixture remain open. FULLTEXT remains deferred
  and non-InnoDB remains out of scope.

### Continuation 1386

- [x] P1 Performance Schema asynchronous failover metadata: correct the
  `replication_asynchronous_connection_failover` column set to use
  `MANAGED_NAME`, correct the managed table column set, and lock all ten
  MySQL 8.4 CHAR/JSON/INT/UNSIGNED/nullability contracts.
- [x] Red/green metadata regression, focused Performance Schema tests, full
  engine regression, and full serial repository regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-async-failover-metadata-current-continuation1386.txt`.
- [ ] Native asynchronous failover source-list persistence, managed
  Group Replication integration, and promotion switching are still not
  implemented; complete I_S/P_S coverage, P2 native binlog/GTID/XA/crash/
  promotion interoperability, P3 non-Connector-J client matrix, and P4
  official MySQL fixture remain open. FULLTEXT remains deferred and
  non-InnoDB remains out of scope.

### Continuation 1387

- [x] P1 Performance Schema Group Replication metadata: replace the stale
  `replication_group_member_stats` subset with the complete MySQL 8.4 13-column
  shape and exact CHAR/BIGINT UNSIGNED/LONGTEXT/TEXT metadata.
- [x] Red/green metadata and focused runtime/registry regression, full engine
  regression, and full serial repository regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-group-member-stats-metadata-current-continuation1387.txt`.
- [ ] Group Replication member statistics/communication/membership/actions and
  promotion runtime remain unimplemented; complete I_S/P_S coverage, P2 native
  binlog/GTID/XA/crash/promotion interoperability, P3 non-Connector-J client
  matrix, and P4 official MySQL fixture remain open. FULLTEXT remains deferred
  and non-InnoDB remains out of scope.

### Continuation 1388

- [x] P1 Group Replication Performance Schema shape: correct
  `replication_group_communication_information`,
  `replication_group_configuration_version`, and
  `replication_group_member_actions`; lock exact metadata for these tables and
  `replication_group_members`.
- [x] Add a red/green metadata regression and verify all five Group Replication
  views expose native ordered columns with empty rows when the runtime is
  absent. Focused, full engine, and full serial repository regressions passed.
  Evidence:
  `reports/compatibility/p1-performance-schema-group-replication-metadata-current-continuation1388.txt`.
- [ ] Group Replication membership, consensus communication, member actions,
  statistics, and promotion runtime remain unimplemented; complete I_S/P_S
  coverage, P2 native binlog/GTID/XA/crash/promotion interoperability, P3
  non-Connector-J client matrix, and P4 official MySQL fixture remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1389

- [x] P1 Performance Schema exact metadata: align `setup_loggers` with the
  MySQL 8.4 `VARCHAR(128)`/lowercase five-value ENUM/`VARCHAR(1023)` contract;
  align `tls_channel_status` with `VARCHAR(128)`, `VARCHAR(128)`, and
  `VARCHAR(2048)` non-null columns.
- [x] Correct `COLUMN_TYPE` formatting to preserve ENUM/SET member spelling
  while canonicalizing only the type keyword; add red/green metadata and TLS
  runtime regressions. Focused, full engine, and full serial repository
  regressions passed. Evidence:
  `reports/compatibility/p1-performance-schema-setup-loggers-tls-metadata-current-continuation1389.txt`.
- [ ] Complete I_S/P_S table/field/runtime/component/privilege semantics, P2
  native binlog/GTID/XA/crash/promotion interoperability, the P3 non-
  Connector-J client matrix, and the P4 official MySQL fixture remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1390

- [x] P1 Performance Schema Clone shape: expand `clone_status` to the native
  MySQL 8.4 12-column contract and add exact nullable/type metadata for its
  status, error, binlog, and GTID columns.
- [x] Add red/green SELECT-shape and I_S metadata coverage; when the Clone
  plugin is absent, preserve the native columns and return no fabricated rows.
  Focused and full serial repository regressions passed. Evidence:
  `reports/compatibility/p1-performance-schema-clone-status-shape-current-continuation1390.txt`.
- [ ] Clone execution/persisted lifecycle/binlog-GTID coordinate publication and
  restart recovery remain unimplemented. Complete I_S/P_S semantics, P2 native
  binlog/GTID/XA/crash/promotion interoperability, P3 non-Connector-J clients,
  and P4 official MySQL fixture validation remain open. FULLTEXT remains deferred
  and non-InnoDB remains out of scope.

### Continuation 1391

- [x] P1 Performance Schema Clone shape: correct `clone_progress` from the
  non-native `THREAD_ID` to MySQL 8.4 `THREADS`, and add exact metadata for all
  eleven columns including `TIMESTAMP(6)` and byte counters.
- [x] Add red/green SELECT-shape and I_S metadata coverage; absent Clone plugin
  keeps the official columns but returns no fabricated progress rows. Focused
  and full serial repository regressions passed. Evidence:
  `reports/compatibility/p1-performance-schema-clone-progress-shape-current-continuation1391.txt`.
- [ ] Clone execution/progress persistence/binlog-GTID publication/restart
  recovery remain unimplemented. Complete I_S/P_S semantics, P2 native
  binlog/GTID/XA/crash/promotion interoperability, P3 non-Connector-J clients,
  and P4 official MySQL fixture validation remain open. FULLTEXT remains deferred
  and non-InnoDB remains out of scope.

### Continuation 1392

- [x] P1 Performance Schema UDF/keyring metadata: align
  `user_defined_functions`, `keyring_component_status`, and `keyring_keys` with
  the MySQL 8.4.11 native source definitions, including lengths, nullability,
  character metadata, and signed BIGINT usage count.
- [x] Add red/green I_S metadata coverage and verify the complete serial Go
  repository. Evidence:
  `reports/compatibility/p1-performance-schema-udf-keyring-metadata-current-continuation1392.txt`.
- [ ] UDF registration/lifecycle and keyring/component service integration remain
  unimplemented. Complete I_S/P_S semantics, P2 native binlog/GTID/XA/crash/
  promotion interoperability, P3 non-Connector-J clients, and P4 official
  MySQL fixture validation remain open. FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

### Continuation 1393

- [x] P1 Performance Schema lock shape: add the missing native
  `metadata_locks.COLUMN_NAME`, align all 11 columns with the MySQL 8.4.11
  source definition, and expose the column in the DDL-lock runtime projection.
- [x] Add red/green I_S metadata and runtime SELECT coverage. Table-level
  coordinator locks keep `COLUMN_NAME` empty instead of inventing column-level
  MDL. Evidence: `reports/compatibility/p1-performance-schema-metadata-locks-shape-current-continuation1393.txt`.
- [ ] Column-level MDL, full native state transitions/source-owner semantics,
  complete I_S/P_S coverage, P2 native binlog/GTID/XA/crash/promotion
  interoperability, P3 non-Connector-J clients, and P4 official MySQL
  fixture validation remain open. FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

### Continuation 1394

- [x] P1 Performance Schema standard metadata audit: close explicit contracts
  for `events_statements_summary_by_program`, digest quantiles,
  `objects_summary_global_by_type`, `table_lock_waits_summary_by_table`, and
  the existing processlist `TIME_MS` extension.
- [x] Add a registry-wide audit test. Firewall/NDB/Thread Pool component
  surfaces and legacy `setup_timers` remain explicit boundaries rather than
  being claimed as native MySQL 8.4 tables. Evidence:
  `reports/compatibility/p1-performance-schema-standard-metadata-audit-current-continuation1394.txt`.
- [ ] Complete P_S runtime statistics/component lifecycle/permissions,
  complete I_S/P_S coverage, P2 native binlog/GTID/XA/crash/promotion
  interoperability, P3 non-Connector-J clients, and P4 official MySQL
  fixture validation remain open. FULLTEXT remains deferred and non-InnoDB
  remains out of scope.

### Continuation 1395

- [x] P2 ordinary DML native transaction capture: record REPLACE delete/insert
  row images and INSERT ... ON DUPLICATE KEY UPDATE insert/update images.
- [x] Put single-statement autocommit INSERT/REPLACE/UPDATE/DELETE behind the
  same journal-before-storage-commit boundary used by explicit client
  transactions; keep native publication after storage commit and retryable on
  publisher failure. Evidence:
  `reports/compatibility/p2-autocommit-dml-row-images-current-continuation1395.txt`.
- [ ] Complete the unified physical storage/WAL/native-binlog/GTID/relay/
  applied-state protocol, full crash/promotion interoperability, P1 I_S/P_S,
  P3 non-Connector-J client matrix, and P4 official MySQL fixture. FULLTEXT
  remains deferred and non-InnoDB remains out of scope.

### Continuation 1396

- [x] Add a durable `LOG_TYPE_TXN_COMMIT` marker to the physical redo stream and
  make orphan journal recovery preserve storage transactions whose physical
  commit is already durable; retain transaction identity and replay statements
  for a retryable native publication.
- [x] Isolate embedded engine redo/undo streams by honoring typed Cfg paths and
  using DataDir-local defaults when no parsed Raw configuration exists. Add
  crash-window and publisher-retry red/green coverage; focused, full engine,
  manager/replication, and full serial repository verification passed.
  Evidence: `reports/compatibility/p2-storage-wal-commit-marker-and-redo-isolation-current-continuation1396.txt`.
- [ ] Full MySQL-native binlog/replication/crash/promotion interoperability,
  complete I_S/P_S runtime/permission coverage, P3 non-Connector-J client
  matrix, and P4 official MySQL fixture remain open. FULLTEXT remains deferred
  and non-InnoDB remains out of scope.

### Continuation 1397

- [x] Restore redo LSN continuity after process restart by initializing the
  allocator from the highest complete persisted redo record. Add a red/green
  restart test; manager and full engine verification passed.
  Evidence: `reports/compatibility/p2-redo-lsn-restart-continuity-current-continuation1397.txt`.
- [ ] Full MySQL-native binlog/replication/crash/promotion interoperability,
  complete I_S/P_S runtime/permission coverage, P3 non-Connector-J client
  matrix, and P4 official MySQL fixture remain open. FULLTEXT remains deferred
  and non-InnoDB remains out of scope.

### Continuation 1398

- [x] Add explicit MySQL 8.4 INFORMATION_SCHEMA metadata contracts for the
  high-value transaction/lock, routine/parameter, partition, storage-space,
  variable, and connection-control tables; add a critical-table audit.
- [x] Update routine metadata regression to the MySQL 8.4 LONGTEXT/TIMESTAMP
  contract; focused, full engine, and full serial repository verification passed.
  Evidence: `reports/compatibility/p1-information-schema-metadata-contracts-current-continuation1398.txt`.
- [ ] Complete the remaining Enterprise Firewall component table contracts and
  runtime semantics, then continue complete I_S/P_S runtime and permission
  coverage, P2 native binlog/GTID/XA/crash/promotion interoperability, the P3
  non-Connector-J client matrix, and the P4 official MySQL fixture. FULLTEXT
  remains deferred and non-InnoDB remains out of scope.

### Continuation 1399

- [x] Add explicit MySQL 8.4 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA metadata
  contracts for `tp_thread_group_state`, `tp_thread_group_stats`, and
  `tp_thread_state`, including exact column order, types, lengths, precision,
  nullability, and `COLUMN_TYPE` in both schemas.
- [x] Add a red/green Thread Pool contract test and include the tables in the
  registry-wide P_S explicit metadata audit. Focused, full engine, and full
  serial repository verification passed. Evidence:
  `reports/compatibility/p1-thread-pool-metadata-contracts-current-continuation1399.txt`.
- [ ] Enterprise Firewall exact 8.4 plugin metadata/runtime semantics remain
  open because the public Community source/docs do not prove the internal
  plugin column definitions. Complete I_S/P_S runtime/permission coverage,
  P2 native binlog/GTID/XA/crash/promotion interoperability, the P3
  non-Connector-J client matrix, and the P4 official MySQL fixture remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1400

- [x] Align the `PERFORMANCE_SCHEMA.tp_connections` registry with the public
  MySQL 8.4 contract: replace the legacy 12-column shape with the documented
  19 columns in the documented order, and add a red/green shape regression.
- [x] Run the focused test, the registry metadata audit, the full engine suite,
  and the full serial repository suite. Evidence:
  `reports/compatibility/p1-thread-pool-connections-shape-current-continuation1400.txt`.
- [x] Refresh the machine-readable scope matrix with the 1398-1400 metadata
  evidence while preserving the aggregate partial/pending/deferred boundaries:
  `reports/compatibility/scope-matrix-current-continuation1400.json`.
- [ ] Keep exact Enterprise plugin metadata types, runtime lifecycle/statistics,
  and permissions open until an Enterprise fixture or privileged build proves
  them. Complete the remaining I_S/P_S runtime/permission surface, P2 native
  binlog/GTID/XA/crash/promotion interoperability, P3 non-Connector-J client
  matrix, and P4 official MySQL fixture. FULLTEXT remains deferred and
  non-InnoDB remains out of scope.

### Continuation 1401

- [x] Implement MySQL 8.4 `TRUNCATE TABLE` semantics for
  `performance_schema.accounts`, `hosts`, and `users`: retain current
  authenticated sessions, reset their total counts to current counts, and
  remove historical rows with no current session.
- [x] Add a red/green regression and run the connection-summary group, metrics
  package, and full engine suite. Evidence:
  `reports/compatibility/p1-performance-schema-connection-truncate-current-continuation1401.txt`.
- [x] Refresh the machine-readable scope matrix with the connection-table reset
  evidence: `reports/compatibility/scope-matrix-current-continuation1401.json`.
- [ ] Complete the dependent account/host/user summary reset families and the
  remaining I_S/P_S runtime/component/permission semantics. P2 native
  binlog/GTID/XA/crash/promotion interoperability, P3 non-Connector-J client
  matrix, and P4 official MySQL fixture remain open. FULLTEXT remains deferred
  and non-InnoDB remains out of scope.

### Continuation 1384

- [x] P1 InnoDB privilege boundary: enforce PROCESS for the implemented MySQL
  8.4 InnoDB dictionary/monitoring views, including `INNODB_TABLESPACES`,
  `INNODB_DATAFILES`, `INNODB_TABLES`, `INNODB_SESSION_TEMP_TABLESPACES`,
  `INNODB_CACHED_INDEXES`, `INNODB_FIELDS`, `INNODB_COLUMNS`,
  `INNODB_INDEXES`, `INNODB_FOREIGN(_COLS)`, `INNODB_TABLESTATS`,
  `INNODB_TEMP_TABLE_INFO`, and `INNODB_VIRTUAL`; lock auxiliary metadata.
- [x] Red/green privilege and metadata regression, I_S regression, full engine
  regression, and full serial repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-process-privilege-current-continuation1384.txt`.
- [ ] Complete I_S/P_S table/field/runtime/component/permission semantics,
  P2 native binlog/GTID/XA/crash/promotion interoperability, the P3
  non-Connector-J client matrix, and the P4 official MySQL fixture remain
  open. FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1383

- [x] P1 InnoDB dictionary shape: align `INFORMATION_SCHEMA.INNODB_TABLESPACES`
  with MySQL 8.4 by removing project-only `FLAGS`, `SDI_*`, and
  `SPACE_FLAGS*` columns from the registry, runtime projection, and metadata.
- [x] Red/green `SELECT *` shape regression, I_S metadata regression, full
  engine regression, and full serial repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-tablespaces-shape-current-continuation1383.txt`.
- [ ] Complete I_S/P_S table/field/runtime/component/permission semantics,
  P2 native binlog/GTID/XA/crash/promotion interoperability, the P3
  non-Connector-J client matrix, and the P4 official MySQL fixture remain
  open. FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1382

- [x] P1 InnoDB dictionary shape: align `INFORMATION_SCHEMA.INNODB_VIRTUAL`
  with MySQL 8.4 by removing the extra non-native `M_COLS` column from the
  registry, runtime projection, and metadata mapping.
- [x] Red/green shape regression, I_S regression, full engine regression, and
  full serial repository regression passed. Evidence:
  `reports/compatibility/p1-innodb-virtual-shape-current-continuation1382.txt`.
- [ ] Complete I_S/P_S table/field/runtime/component/permission semantics,
  P2 native binlog/GTID/XA/crash/promotion interoperability, the P3
  non-Connector-J client matrix, and the P4 official MySQL fixture remain
  open. FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1381

- [x] P1 InnoDB dictionary metadata regression: lock the MySQL 8.4 column
  contracts for `INFORMATION_SCHEMA.INNODB_INDEXES` and
  `INFORMATION_SCHEMA.INNODB_COLUMNS`, including native order, numeric
  signedness, `VARCHAR(193)` index names, `BIGINT UNSIGNED` column positions,
  and nullable BLOB default values.
- [x] Focused metadata, full engine, and full serial repository regressions
  passed. Evidence:
  `reports/compatibility/p1-innodb-dictionary-metadata-current-continuation1381.txt`.
- [ ] Complete I_S/P_S table/field/runtime/component/permission semantics,
  P2 native binlog/GTID/XA/crash/promotion interoperability, the P3
  non-Connector-J client matrix, and the P4 official MySQL fixture remain
  open. FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1402

- [x] P1 Performance Schema connection-table dependent reset semantics:
  `accounts` resets account/thread summaries; `hosts` additionally resets host
  summaries; `users` additionally resets user summaries across error, statement,
  stage, transaction, and wait families.
- [x] Reused the live-session snapshot and connection-total reset from
  continuation 1401; focused connection-summary regression is green.
- [ ] Memory/component summary dimensions, Enterprise Firewall exact semantics,
  complete I_S/P_S coverage, P2 native binlog/GTID/XA/crash/promotion
  interoperability, the P3 non-Connector-J client matrix, and the P4 official
  MySQL fixture remain open. FULLTEXT remains deferred and non-InnoDB remains
  out of scope.

### Continuation 1403

- [x] P1 Performance Schema memory summaries now have independent global,
  thread, account, host, and user aggregation sources.
- [x] Add explicit derived-memory `TRUNCATE` handlers and connect account/host/
  user connection-table truncation to the corresponding memory dimension plus
  thread reset; global memory truncation resets all dimensions.
- [x] Red/green identity-isolation and connection-dependent reset tests plus
  affected metrics/engine regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-memory-summary-dimension-reset-current-continuation1403.txt`.
- [ ] Enterprise/component memory sources, exact Enterprise Firewall semantics,
  complete I_S/P_S coverage, P2 native binlog/GTID/XA/crash/promotion
  interoperability, the P3 non-Connector-J client matrix, and the P4 official
  MySQL fixture remain open. FULLTEXT remains deferred and non-InnoDB remains
  out of scope.

### Continuation 1404

- [x] Reconfirmed global scope: complete INFORMATION_SCHEMA/PERFORMANCE_SCHEMA
  semantics, the full non-Connector-J client matrix, and XA/native MySQL
  binlog/replication/crash/promotion interoperability remain global backlog
  items at P1, P3, and P2/P4 respectively.
- [x] Reconfirmed that non-InnoDB engines, repair, and conversion are explicitly
  out of scope, while FULLTEXT remains deferred.
- [x] Audited the official MySQL 8.4 Enterprise Firewall documentation. The
  documented columns and meanings are known, but exact plugin metadata types,
  lifecycle, and permission behavior require an Enterprise fixture or build
  unavailable in this workspace; no speculative implementation was added.
  Evidence: `reports/compatibility/p1-global-scope-audit-current-continuation1404.txt`.
- [ ] Complete I_S/P_S table/field/runtime/component/permission semantics,
  P2 native interoperability and recovery, the P3 client matrix, and the P4
  official MySQL fixture remain open.
- [ ] Complete I_S/P_S table/field/runtime/component/permission semantics,
  P2 native interoperability and recovery, the P3 client matrix, and the P4
  official MySQL fixture remain open.

### Continuation 1405

- [x] P1 partial-revoke object visibility: schema/table/column/index metadata
  now honors a restriction on a global `*.*` grant while allowing explicit
  schema/table/column grants to restore limited visibility.
- [x] Red/green regression, related privilege/role tests, full engine regression,
  and full serial repository regression passed. Evidence:
  `reports/compatibility/p1-information-schema-partial-revoke-object-visibility-current-continuation1405.txt`.
- [ ] Complete I_S/P_S table/field/runtime/component/permission semantics,
  P2 native binlog/GTID/XA/crash/promotion interoperability, the P3
  non-Connector-J client matrix, and the P4 official MySQL fixture remain open.
  FULLTEXT remains deferred and non-InnoDB remains out of scope.

### Continuation 1418

- [x] Add and verify a native MySQL binlog source against an official MySQL
  8.4.11 fixture, including raw event preservation, checksum framing,
  Query-event status-variable offsets, and MySQL hex-literal XA XIDs.
- [x] Verify ordinary committed and XA prepared/committed transactions from
  official MySQL with the replication decoder. Evidence:
  `reports/compatibility/p4-official-mysql84-native-binlog-xa-current-continuation1418.txt`.
- [ ] Integrate the native source into the runtime replica/apply path and prove
  xmysql -> official MySQL replica application, GTID restart, crash recovery,
  promotion, and complete bidirectional XA/binlog interoperability.
- [ ] Complete INFORMATION_SCHEMA/PERFORMANCE_SCHEMA field/runtime/component/
  permission semantics and the full non-Connector-J client matrix. FULLTEXT
  remains deferred; non-InnoDB engines, repair, and conversion remain out of
  scope.

### Continuation 1419

- [x] Integrate the `mysql://` native binlog source into the replica runtime.
  Persist native relay frames, prepared-XA state, executed GTIDs, and source
  position together through `ApplyNativeAtSource`; preserve the existing HTTP
  source path and redact native source credentials from status output.
- [x] Verify split native transactions locally and verify official MySQL 8.4.11
  runtime ingestion, ordinary transaction decoding, and XA decoding. Evidence:
  `reports/compatibility/p2-native-runtime-source-apply-current-continuation1419.txt`.
- [ ] Prove xmysql -> official MySQL replica application, GTID auto-position,
  sustained cross-file rotation, external-mysqld crash recovery/promotion, and
  complete bidirectional XA/binlog interoperability.
- [ ] Complete full INFORMATION_SCHEMA/PERFORMANCE_SCHEMA semantics and the
  non-Connector-J client matrix. FULLTEXT remains deferred; non-InnoDB engines,
  repair, and conversion remain out of scope.

### Continuation 1420

- [x] Add native MySQL GTID auto-position reconnect from the durable executed
  GTID interval set, while falling back to file/position during an incomplete
  native relay transaction.
- [x] Handle official MySQL autocommit QUERY_EVENT transactions without an
  XID_EVENT; keep row-based transaction completion strict. Evidence:
  `reports/compatibility/p2-native-gtid-auto-position-current-continuation1420.txt`.
- [x] Verify official MySQL 8.4.11 first-transaction apply, GTID reconnect,
  second-transaction apply, targeted package regressions, and the full serial
  repository suite.
- [ ] Prove full GTID crash restart, sustained cross-file rotation, external
  mysqld promotion, xmysql -> official MySQL application, and bidirectional
  XA/binlog interoperability.
- [ ] Complete INFORMATION_SCHEMA/PERFORMANCE_SCHEMA semantics and the full
  non-Connector-J client matrix. FULLTEXT remains deferred; non-InnoDB engines,
  repair, and conversion remain out of scope.

### Continuation 1421

- [x] Diff official MySQL 8.4.11 Performance Schema field contracts against
  xmysql and add regression coverage for the actionable missing fields.
- [x] Add the three missing `events_waits_*` fields and expand
  `table_lock_waits_summary_by_table` to the official 68-column shape,
  including INFORMATION_SCHEMA metadata and row projection. Evidence:
  `reports/compatibility/p1-performance-schema-field-shapes-current-continuation1421.txt`.
- [x] Run the focused metadata/shape tests and the full
  `server/innodb/engine` regression suite.
- [ ] Complete the remaining I_S/P_S runtime, component, privilege, and
  full-table semantics; the non-Connector-J client matrix; and native
  replication crash/promotion/bidirectional interoperation. FULLTEXT remains
  deferred; non-InnoDB engines, repair, and conversion remain out of scope.

### Continuation 1422

- [x] Persist native TABLE_MAP dictionaries independently from incomplete
  native relay transactions so reconnects can decode later ROW_EVENT frames.
- [x] Add guarded file-position replay after native decode failure to rebuild
  metadata without reapplying executed GTIDs.
- [x] Verify official MySQL 8.4.11 transaction -> `FLUSH BINARY LOGS` ->
  rotated transaction -> xmysql restart -> post-restart transaction. Evidence:
  `reports/compatibility/p2-native-rotation-restart-current-continuation1422.txt`.
- [ ] Prove external mysqld promotion, xmysql -> official replica application,
  and complete bidirectional XA/binlog interoperability; complete remaining
  I_S/P_S semantics and the non-Connector-J client matrix. FULLTEXT remains
  deferred; non-InnoDB engines, repair, and conversion remain out of scope.

### Continuation 1423

- [x] Audit and fix the native binlog Format Description Event: match the
  official MySQL 8.4.11 table including its `UNKNOWN_EVENT` slot, cover the
  published post-header lengths, and emit the CRC32 checksum descriptor.
- [x] Add the FDE shape regression and run the full `server/replication` and
  `server/net` package regressions. Evidence:
  `reports/compatibility/p2-native-format-description-current-continuation1423.txt`.
- [x] Run the full serial repository regression after the FDE change; the
  complete `go test -p 1 ./... -count=1 -timeout 45m` suite passed.
- [ ] Keep official mysqld as the external gate for xmysql -> official replica
  application, bidirectional XA/binlog, external crash recovery and promotion;
  continue the remaining P1 I_S/P_S runtime/component/permission semantics and
  the P3 full client matrix.

### Continuation 1424

- [x] Remove pre-negotiation raw flate from the MySQL TCP handshake path;
  `CompressNone` now preserves ordinary MySQL packet framing and automatic
  Getty compression is disabled until a negotiated MySQL compression path
  exists.
- [x] Add the transport regression and run the focused `server/net` and
  `server/conf` suites. Evidence:
  `reports/compatibility/p0-mysql-transport-compression-current-continuation1424.txt`.
- [ ] Re-run the official mysqld replica probe with a clean authenticated source
  account, then verify row/XA/GTID/rotation and promotion. Keep P4 pending until
  those external observations exist.
- [x] Run the full serial repository regression after the transport change;
  `go test -p 1 ./... -count=1 -timeout 45m` passed, including
  `server/innodb/engine` (262.336s), `server/net` (8.237s), and
  `server/replication` (5.979s).

### Continuation 1425

- [x] Fix native COM_BINLOG_DUMP response sequencing: the first server event
  packet now starts at sequence 1 after the command packet sequence 0.
- [x] Align both native FDE builders with the official MySQL 8.4.11 table,
  including the UNKNOWN_EVENT slot, post-header lengths, and CRC32 descriptor.
- [x] Support replication handshake `SET @...` comma lists and preserve
  `@@GLOBAL.xxx` references; stop bare VARCHAR row metadata from depending on
  the current value length.
- [x] Verify an official MySQL 8.4.11 replica reads xmysql native events and
  applies a fresh `VARCHAR(64)` table plus DML with both replica threads
  running and the source position caught up. Evidence:
  `reports/compatibility/p2-p4-official-replica-dump-current-continuation1425.txt`.
- [x] Run `go test -p 1 ./... -count=1 -timeout 45m`; engine 303.499s,
  net 8.523s, replication 6.298s.
- [ ] Complete official XA event application, GTID crash resume, promotion,
  duplicate-delivery and bidirectional XA/binlog gates; complete remaining
  I_S/P_S runtime/component/permission semantics and the full non-Connector-J
  matrix. FULLTEXT remains deferred; non-InnoDB engines, repair, and
  conversion remain out of scope.

### Continuation 1426

- [x] Diff the existing Performance Schema virtual-column metadata against
  official MySQL 8.4.11 for ENUM/SET lengths and character sets, TEXT-family
  lengths, and the corrected stage/statement/transaction/wait fields.
- [x] Apply the normalization only on the `performance_schema` metadata path
  and update the corresponding compatibility assertions. Evidence:
  `reports/compatibility/p1-performance-schema-official-metadata-current-continuation1426.txt`.
- [x] Run the focused metadata tests and the complete
  `TestInformationSchema|TestPerformanceSchema` engine regression.
- [ ] Complete remaining I_S/P_S runtime, component, lock/wait, and privilege
  semantics; complete the full non-Connector-J client matrix and official
  XA/GTID/crash-recovery/promotion/bidirectional replication gates. FULLTEXT
  remains deferred; non-InnoDB engines, repair, and conversion remain out of
  scope.

### Continuation 1427

- [x] Make native TABLE_MAP installation monotonic by binlog file/position so
  a stale relay map cannot overwrite a newer cached map for the same table ID.
- [x] Reset persisted TABLE_MAP metadata before a forced file replay and filter
  pre-boundary historical ROWS_EVENT/transaction frames while retaining the
  metadata needed to rebuild the decoder dictionary. Evidence:
  `reports/compatibility/p2-native-table-map-replay-current-continuation1427.txt`.
- [x] Verify the official MySQL 8.4.11 ordinary/XA native source tests and the
  runtime rotation/restart sequence.
- [x] Run the complete serial repository suite; engine 241.639s, net 8.051s,
  replication 6.028s.
- [ ] Complete official XA/binlog bidirectional interoperability, external
  mysqld crash recovery/promotion and duplicate-delivery gates; complete the
  remaining I_S/P_S runtime/component/permission semantics and the full
  non-Connector-J client matrix. FULLTEXT remains deferred; non-InnoDB
  engines, repair, and conversion remain out of scope.

### Continuation 1428

- [x] Correct `performance_schema.setup_objects` UPDATE matching so supplied
  `OBJECT_TYPE`, `OBJECT_SCHEMA`, and `OBJECT_NAME` equality/LIKE predicates
  are evaluated together. A schema/name-specific predicate no longer updates
  the default `TABLE/%/%` row. Evidence:
  `reports/compatibility/p1-performance-schema-setup-object-predicate-current-continuation1428.txt`.
- [x] Run the focused setup-object regression and the combined
  `TestInformationSchema|TestPerformanceSchema` engine regression.
- [ ] Implement setup_objects custom-row INSERT/DELETE and complete pattern
  precedence, then continue the remaining I_S/P_S runtime/component/permission
  semantics, P2 native/external replication gates, and the P3 full
  non-Connector-J client matrix. FULLTEXT remains deferred; non-InnoDB
  engines, repair, and conversion remain out of scope.

### Continuation 1429

- [x] Complete the local `performance_schema.setup_objects` lifecycle:
  custom-row INSERT/DELETE, exact/schema/global wildcard precedence, TRUNCATE,
  ENABLED/TIMED update validation, and INSERT/DELETE privilege checks.
- [x] Apply TABLE setup-object ENABLED/TIMED rules to table I/O and table-lock
  summary projections. Evidence:
  `reports/compatibility/p1-performance-schema-setup-object-lifecycle-current-continuation1429.txt`.
- [x] Run the focused setup-object/table-I/O, privilege, and full
  `TestPerformanceSchema` regressions.
- [ ] Continue the remaining I_S/P_S runtime/component/permission semantics,
  then close the P2 native replication commit boundary and P3/P4 gates.
  FULLTEXT remains deferred; non-InnoDB engines, repair, and conversion remain
  out of scope.

### Continuation 1430

- [x] Expand the shared P3 non-Connector/J matrix with metadata shape/case
  handling, auto-increment result metadata, savepoints, and session state;
  apply the same cases to Go, PyMySQL, Node/mysql2, and mysql CLI harnesses.
- [x] Verify the expanded 13-case matrix on an isolated development instance:
  Go, PyMySQL, and Node/mysql2 passed all cases. Evidence:
  `reports/compatibility/p3-client-matrix-current-continuation1430.txt`.
- [ ] Install/provide the mysql CLI and complete the broader client ecosystem,
  TLS/authentication variants, pooling/load-balancer cases, then close the P2
  external XA/binlog/GTID/crash/promotion gates. FULLTEXT remains deferred;
  non-InnoDB engines, repair, and conversion remain out of scope.

### Continuation 1431

- [x] Re-run the official MySQL 8.4.11 source interoperability fixture for
  ordinary transactions, XA transactions, native runtime pull, and
  rotation/restart resume. The four selected integration tests passed in
  1.192s. Evidence:
  `reports/compatibility/p2-official-mysql-source-current-continuation1431.txt`.
- [ ] Complete the reverse xmysql-to-official XA application gate, GTID
  auto-position after crash, promotion, duplicate-delivery, and full
  bidirectional XA/binlog interoperability; continue remaining P1 semantic
  coverage and P3 ecosystem clients. FULLTEXT remains deferred; non-InnoDB
  engines, repair, and conversion remain out of scope.

### Continuation 1435

- [x] Close the official MySQL 8.4.11 shared Performance Schema metadata
  contract: 114 official tables and 114 local shared tables now have zero
  field-level differences across name, type, nullability, key, default,
  extra, charset, and collation. The local-only 16 tables are optional,
  legacy, component, NDB, or thread-pool surfaces. Evidence:
  `reports/compatibility/p1-official-performance-schema-metadata-current-continuation1435.txt`.
- [x] Run the combined `TestInformationSchema|TestPerformanceSchema` engine
  regression; it passed in 73.726s.
- [ ] Continue the full Information Schema official/runtime differential and
  remaining Performance Schema live component/permission semantics. Keep the
  reverse xmysql-to-official XA/binlog/GTID/crash-recovery/promotion gates and
  the full non-Connector-J client ecosystem matrix open. FULLTEXT remains
  deferred; non-InnoDB engines, repair, and conversion remain out of scope.

### Continuation 1442

- [x] Re-run the replication package regression: `go test -p 1
  ./server/replication -count=1 -timeout 45m` passed in 7.218s.
- [x] Correct the stale one-phase XA native-event assertion in
  `server/innodb/engine/xa_compatibility_test.go`: the official lifecycle
  emitted by production is eight events, including `XA START`, `XA END`, and
  type-38 `XA_PREPARE(one_phase=1)`, not the old six-event expectation.
- [x] Verify the corrected XA one-phase path together with MySQL 8.4
  `USER_ATTRIBUTES` visibility and partial-revoke metadata visibility; the
  focused engine gate passed in 3.229s. Evidence:
  `reports/compatibility/p1-p2-focused-gate-current-continuation1442.txt`.
- [ ] Do not count the attempted full engine run as PASS: Windows Go runtime
  `VirtualAlloc errno=1455` aborted it while collecting output. Continue with
  resource-safe serial full-suite verification when the environment permits.
  P1/P2/P3/P4 global items remain partial; FULLTEXT remains deferred and
  non-InnoDB engines/repair/conversion remain out of scope.

### Continuation 1443

- [x] Close the native MySQL GTID auto-position startup/restart configuration
  gap: parse `gtid_set`, allow a GTID-positioned source without
  `binlog_file`, validate the native GTID set, and restore the durable
  replica Executed intervals when constructing a native source.
- [x] Verify the focused GTID startup/restart tests, the native runtime
  split-transaction/reconnect tests, and the full `server/replication` package
  regression. Evidence:
  `reports/compatibility/p2-native-gtid-auto-position-current-continuation1443.txt`.
- [x] Verify the official MySQL 8.4.11 source-to-xmysql transaction, XA,
  runtime, rotation, and restart fixture; remove source URL userinfo from the
  runtime status surface. The temporary Docker fixture was cleaned after the
  run.
- [ ] Continue the unified storage/WAL/native-binlog/GTID/applied-marker crash
  protocol, complete promotion/failover and official bidirectional fixtures,
  the remaining P1 I_S/P_S runtime semantics, and the full P3 client matrix.
  FULLTEXT remains deferred; non-InnoDB engines, repair, and conversion remain
  out of scope.

### Continuation 1444

- [x] Run a fresh xmysql -> official MySQL 8.4.11 GTID auto-position fixture for
  ordinary DML, XA DML, source restart, reconnect, and post-restart DML. The
  official replica ended with exactly three expected rows and healthy I/O/SQL
  threads; evidence:
  `reports/compatibility/p2-p4-xmysql-to-official-reverse-restart-current-continuation1444.txt`.
- [x] Fix and regression-test restart restoration of persisted dictionary tables
  into the in-memory table-storage mapping. The focused engine regression passed
  in 2.039s; the full `server/replication` package passed in 9.156s.
- [x] Run the three-round external process-level crash drill; committed rows,
  rollback of the uncommitted row, redo, and DDL/index metadata all recovered.
  Evidence: `reports/compatibility/p2-external-crash-recovery-current-continuation1445.txt`.
- [x] Extend the same three-round drill with the existing interrupted-commit
  (`race_commit`) workload; every run recovered with the affected primary key
  at most once. Evidence:
  `reports/compatibility/p2-external-crash-race-current-continuation1447.txt`.
- [x] Remove the hard-coded test password from the crash drill script and recovery
  client default DSN; use the explicit development bypass-auth configuration with
  an empty local test password.
- [x] Run an official MySQL 8.4.11 reverse crash/reconnect fixture: force-stop
  xmysql while the official replica is connected, restart the same data directory,
  reconnect the replica, and verify ordinary/XA history plus a new post-restart
  transaction. Evidence:
  `reports/compatibility/p4-official-mysql-reverse-crash-reconnect-current-continuation1446.txt`.
- [x] Re-run the current P1 Information Schema/Performance Schema focused suite;
  it passed in 74.517s. Record the P3 client matrix authentication gate as
  `unverified` because no protected client password was supplied; do not count it
  as a client PASS. Evidence:
  `reports/compatibility/p1-p3-current-gate-continuation1448.txt`.
- [x] Re-run the authenticated P3 single-node and cluster-endpoint baselines;
  mysql CLI, Go, PyMySQL, and Node.js/mysql2 passed on source and promoted
  endpoints. Evidence:
  `reports/compatibility/p3-client-cluster-current-continuation1449.txt`.
- [ ] Attempted the full current engine regression both through `go test` and a
  directly executed compiled test binary; both hit Windows `VirtualAlloc errno=1455`
  before package completion. Keep this as an environment-blocked verification item,
  not a PASS. Evidence:
  `reports/compatibility/full-engine-regression-resource-gate-current-continuation1450.txt`.
- [ ] Continue native replication crash windows, duplicate-event suppression, promotion/failover,
  and complete official bidirectional interoperability. P1 runtime/permission/
  lifecycle semantics and the full P3 non-Connector/J client matrix remain partial.
  FULLTEXT remains deferred; non-InnoDB engines, repair, and conversion remain
  out of scope.

### Continuation 1451

- [x] Close the engine-owned Insert Buffer worker during `XMySQLEngine.Close()`
  and wait for it to exit before storage teardown.
- [x] Make engine shutdown attempt all owned manager closures; fix the
  `IndexManager.Close()` lock/flush deadlock and safe-close partially initialized
  dictionary managers. Evidence:
  `reports/compatibility/engine-lifecycle-current-continuation1451.txt`.
- [x] Re-run the focused Information Schema/Performance Schema suite and the
  full replication package; both passed.
- [ ] Keep the full engine package unverified: the current Windows host still
  hits `VirtualAlloc errno=1455` during the full run. Continue P1 runtime/
  permission/component semantics, P2/P4 crash/promotion/bidirectional gates,
  and the full P3 client boundary matrix. FULLTEXT remains deferred; non-InnoDB
  engines, repair, and conversion remain out of scope.

### Continuation 1452

- [x] Add idempotent legacy BufferPool shutdown that purges cached entries and
  releases preallocated page frames; invoke it from StorageManager shutdown.
- [x] Re-run the complete `server/innodb/engine` serial package after the
  lifecycle fix; it passed in 274.586s. Evidence:
  `reports/compatibility/full-engine-regression-current-continuation1452.txt`.
- [ ] Continue the remaining P1 runtime/permission/component semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and complete P3
  client boundary matrix. FULLTEXT remains deferred; non-InnoDB engines,
  repair, and conversion remain out of scope.

### Continuation 1453

- [x] Synchronize the P3 scope matrix with the authenticated Docker fixture:
  mysql CLI, Go, PyMySQL, and Node.js/mysql2 are implemented for the current
  single-node and promoted-endpoint baseline. Evidence:
  `reports/compatibility/p3-scope-status-current-continuation1453.txt`.
- [ ] Keep the aggregate P3 matrix partial until connection-pool routing,
  old-connection failure/retry, TLS, administrative CLI, and broader client
  version boundaries are covered.

### Continuation 1454

- [x] Run the full Go repository regression after the BufferPool lifecycle fix;
  `go test -p 1 ./... -count=1 -timeout 90m` passed with exit code 0. Evidence:
  `reports/compatibility/full-go-regression-current-continuation1454.txt`.
- [ ] Continue the remaining P1 runtime/permission/component semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1455

- [x] Re-audit the MySQL 8.4.11 Performance Schema shared-table metadata
  differential: all 114 official tables are present locally and all shared
  field contracts match. Correct the scope matrix's stale P_S registry status
  from `partial` to `implemented`; this closes metadata registration only.
  Evidence: `reports/compatibility/p1-official-performance-schema-metadata-current-continuation1435.txt`.
- [x] Regenerate `reports/compatibility/scope-matrix-current-continuation1455.json`.
  The matrix now reports 15 implemented, 8 partial, 1 deferred, and 2
  out-of-scope entries.
- [ ] Continue P_S runtime/permission/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion. FULLTEXT remains deferred; non-InnoDB engines, repair,
  and conversion remain out of scope.

### Continuation 1456

- [x] Re-run the focused Performance Schema regression after the matrix status
  correction; `go test -p 1 ./server/innodb/engine -run 'TestPerformanceSchema'
  -count=1 -timeout 20m` passed in 26.959s. Evidence:
  `reports/compatibility/p1-performance-schema-registry-current-continuation1456.txt`.
- [ ] Continue the broader P1 runtime/permission/component/lifecycle semantics,
  P2/P4 native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1457

- [x] Close the virtual `mysql.*` grant-table privilege-dispatch gap. Require
  table-level SELECT for non-root authenticated sessions on `mysql.user`,
  `db`, `tables_priv`, `columns_priv`, `procs_priv`, `proxies_priv`,
  `global_grants`, `role_edges`, and `default_roles`; preserve nil/internal
  and root behavior.
- [x] Add red/green coverage for `mysql.user` and the full nine-table grant
  table set; related account/role/privilege tests passed in 5.479s and the full
  engine package passed in 238.036s. Evidence:
  `reports/compatibility/p1-mysql-grant-table-privilege-dispatch-current-continuation1457.txt`.
- [ ] Continue the remaining P1 runtime/permission/component/lifecycle
  semantics, P2/P4 native replication crash/promotion/bidirectional gates,
  and P3 client boundary expansion.

### Continuation 1458

- [x] Run the full serial repository regression after the grant-table privilege
  dispatch fix: `go test -p 1 ./... -count=1 -timeout 90m` passed. Engine
  238.342s, manager 7.527s, net 8.138s, replication 5.386s, and metrics
  0.542s. Evidence:
  `reports/compatibility/full-go-regression-current-continuation1458.txt`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1477

- [x] Run a credential-free client environment audit. Go, PyMySQL, and Node
  mysql2 are available; `mysql.exe` is absent, and the protected
  `XMYSQL_CLIENT_PASSWORD` variable is unset, so no authenticated client or
  cluster-endpoint matrix was started. Evidence:
  `reports/compatibility/client-matrix-diagnostic-current-continuation1477/client-matrix-20260927-105556.json`
  and
  `reports/compatibility/client-cluster-endpoint-diagnostic-current-continuation1477/client-cluster-endpoint-20260927-105631.json`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1463

- [x] Regenerate the scope matrix with the `performance_schema.threads`
  UPDATE-privilege evidence. Counts remain 15 implemented, 8 partial, 1
  deferred, and 2 out-of-scope. Evidence:
  `reports/compatibility/scope-matrix-current-continuation1463.json`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1461

- [x] Synchronize the error-log DDL evidence into the scope matrix. The
  aggregate remains 15 implemented, 8 partial, 1 deferred, and 2 out of scope;
  P_S metadata completion is not treated as completion of all P_S runtime or
  component semantics. Evidence:
  `reports/compatibility/scope-matrix-current-continuation1461.json`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1460

- [x] Align `TRUNCATE TABLE performance_schema.error_log` with MySQL 8.4:
  reject it explicitly, while preserving permitted error-summary truncation.
  Red/green coverage, the related Performance Schema tests, and the full
  engine package passed. Evidence:
  `reports/compatibility/p1-performance-schema-error-log-truncate-current-continuation1460.txt`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1462

- [x] Require table-level `SELECT` before reading and `UPDATE` before mutating
  `performance_schema.threads`; pass the authenticated session through both
  dedicated dispatchers and preserve internal/nil-session behavior.
- [x] Add unauthorized/authorized red-green coverage. Performance Schema
  related tests passed and the full engine package passed in 238.314s. Evidence:
  `reports/compatibility/p1-performance-schema-threads-update-privilege-current-continuation1462.txt`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1464

- [x] Correct the 1462 evidence scope to include both `threads` SELECT and
  UPDATE privilege enforcement; matrix totals remain 15 implemented, 8
  partial, 1 deferred, and 2 out-of-scope.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1465

- [x] Regenerate the matrix after the `threads` SELECT/UPDATE permission
  slice; totals remain 15 implemented, 8 partial, 1 deferred, and 2
  out-of-scope. Evidence:
  `reports/compatibility/scope-matrix-current-continuation1465.json`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1466

- [x] Run the full serial repository regression after the threads permission
  slice: `go test -p 1 ./... -count=1 -timeout 90m` passed. Engine 239.029s,
  manager 7.609s, net 8.169s, replication 5.531s, and metrics 0.465s.
  Evidence:
  `reports/compatibility/full-go-regression-current-continuation1466.txt`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1467

- [x] Synchronize the full-regression evidence into the scope matrix; totals
  remain 15 implemented, 8 partial, 1 deferred, and 2 out-of-scope.
  Evidence:
  `reports/compatibility/scope-matrix-current-continuation1467.json`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1468

- [x] Add a common table-level SELECT gate for direct Performance Schema
  reads, while preserving the dedicated PROCESS visibility semantics of
  `performance_schema.processlist`. The full Performance Schema family
  passed in 27.482s and the engine package passed in 238.847s. Evidence:
  `reports/compatibility/p1-performance-schema-select-privilege-current-continuation1468.txt`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1469

- [x] Run the full serial repository regression after the common P_S SELECT
  gate: `go test -p 1 ./... -count=1 -timeout 90m` passed. Engine 239.237s,
  manager 7.731s, net 8.088s, replication 5.476s, and metrics 0.439s.
  Evidence:
  `reports/compatibility/full-go-regression-current-continuation1469.txt`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1470

- [x] Regenerate the scope matrix with the common P_S SELECT gate and the
  full-regression evidence; totals remain 15 implemented, 8 partial, 1
  deferred, and 2 out-of-scope. Evidence:
  `reports/compatibility/scope-matrix-current-continuation1470.json`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1471

- [x] Implement per-routine MySQL visibility for `INFORMATION_SCHEMA.ROUTINES`
  and `PARAMETERS`: DEFINER/global SELECT/SHOW_ROUTINE expose definitions;
  CREATE ROUTINE/ALTER ROUTINE/EXECUTE expose rows with
  `ROUTINE_DEFINITION = NULL`; unauthorized rows are hidden. Evidence:
  `reports/compatibility/p1-information-schema-routines-privilege-current-continuation1471.txt`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1472

- [x] Run the full serial repository regression after routine metadata
  permissions: `go test -p 1 ./... -count=1 -timeout 90m` passed. Engine
  239.726s, manager 7.630s, net 8.225s, replication 5.307s, metrics 0.463s.
  Evidence:
  `reports/compatibility/full-go-regression-current-continuation1472.txt`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1473

- [x] Regenerate the scope matrix with the routine privilege and full-regression
  evidence; totals are 16 implemented, 7 partial, 1 deferred, and 2
  out-of-scope. Evidence:
  `reports/compatibility/scope-matrix-current-continuation1473.json`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1474

- [x] Correct `VIEW_TABLE_USAGE` and `VIEW_ROUTINE_USAGE` to use MySQL's
  “some privilege on the view and referenced object” rule instead of reusing
  the stricter `SHOW VIEW`-only VIEWS handler. Added SELECT-only view and
  source-object regression coverage. Evidence:
  `reports/compatibility/p1-information-schema-view-usage-privilege-current-continuation1474.txt`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1475

- [x] Run the full serial repository regression after view usage permissions:
  `go test -p 1 ./... -count=1 -timeout 90m` passed. Engine 240.253s,
  manager 7.117s, net 8.134s, replication 5.391s, metrics 0.451s. Evidence:
  `reports/compatibility/full-go-regression-current-continuation1475.txt`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1476

- [x] Regenerate the scope matrix with view usage and full-regression evidence;
  totals remain 16 implemented, 7 partial, 1 deferred, and 2 out-of-scope.
  Evidence: `reports/compatibility/scope-matrix-current-continuation1476.json`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.

### Continuation 1459

- [x] Synchronize the grant-table privilege-dispatch and full-regression
  evidence into the compatibility scope matrix. The aggregate counts remain
  15 implemented, 8 partial, 1 deferred, and 2 out-of-scope; the broader
  privilege/role item correctly remains partial. Evidence:
  `reports/compatibility/scope-matrix-current-continuation1459.json`.
- [ ] Continue the remaining P1 runtime/component/lifecycle semantics, P2/P4
  native replication crash/promotion/bidirectional gates, and P3 client
  boundary expansion.
### Continuation 1478

- P1 metadata semantics: include persisted empty database directories in
  `INFORMATION_SCHEMA.SCHEMATA`, then apply the existing privilege and partial
  revoke visibility gate.
- Verification: focused red/green test, all `TestInformationSchema` tests, and
  `go test -p 1 ./... -count=1 -timeout 90m` all passed.
- Evidence: `reports/compatibility/p1-information-schema-schemata-empty-database-current-continuation1478.txt`.

### Continuation 1479

- [x] Regenerate the global compatibility matrix with the empty-schema metadata
  and full Go regression evidence.
- [ ] Keep P1 privilege/runtime/component lifecycle semantics, P2/P4 native
  replication interoperability, and P3 client authentication matrix open until
  their remaining external gates are verified.
- Evidence: `reports/compatibility/scope-matrix-current-continuation1478.json`.

### Continuation 1480

- [x] P1 metadata visibility: route `ST_GEOMETRY_COLUMNS` through the same
  table-level object visibility gate as other INFORMATION_SCHEMA metadata.
- [x] Verify no-privilege hiding, table SELECT visibility, the full
  INFORMATION_SCHEMA family, and the full serial Go regression.
- Evidence: `reports/compatibility/p1-information-schema-st-geometry-visibility-current-continuation1480.txt`.

### Continuation 1481

- [x] Regenerate the scope matrix with the latest P1 geometry visibility and
  full-regression evidence.
- [ ] Continue the remaining P1 runtime/permission/component semantics and all
  P2/P3/P4 external compatibility gates.
- Evidence: `reports/compatibility/scope-matrix-current-continuation1480.json`.

### Continuation 1482

- [x] P1 histogram metadata visibility: apply table-level object visibility
  before projecting `INFORMATION_SCHEMA.COLUMN_STATISTICS` rows.
- [x] Verify no-privilege hiding, table SELECT visibility, the full
  INFORMATION_SCHEMA family, and the full serial Go regression.
- Evidence: `reports/compatibility/p1-information-schema-column-statistics-visibility-current-continuation1482.txt`.

### Continuation 1483

- [x] Regenerate the scope matrix with the latest histogram visibility and
  full-regression evidence.
- [ ] Continue the remaining P1 runtime/permission/component semantics and all
  P2/P3/P4 external compatibility gates.
- Evidence: `reports/compatibility/scope-matrix-current-continuation1482.json`.

### Continuation 1484

- [x] P1 partition metadata visibility: pass the authenticated session into
  `INFORMATION_SCHEMA.PARTITIONS` and apply the table object visibility gate.
- [x] Verify the full INFORMATION_SCHEMA family and the full serial Go suite.
- Evidence: `reports/compatibility/p1-information-schema-partitions-visibility-current-continuation1484.txt`.

### Continuation 1485

- [x] Regenerate the scope matrix with the latest partition visibility and
  full-regression evidence.
- [ ] Continue the remaining P1 runtime/permission/component semantics and all
  P2/P3/P4 external compatibility gates.
- Evidence: `reports/compatibility/scope-matrix-current-continuation1484.json`.

### Continuation 1486

- [x] Add the official MySQL 8.4 `PROCESS` privilege gate for
  `INFORMATION_SCHEMA.FILES` and cover it in the existing InnoDB metadata
  privilege test.
- [x] Verify the focused gate, all `TestInformationSchema` tests, and the full
  serial Go regression.
- [ ] Keep the aggregate P1 I_S/P_S semantics, P2/P4 native replication
  interoperability, and P3 non-Connector-J client matrix open.
- Evidence: `reports/compatibility/p1-information-schema-files-process-privilege-current-continuation1486.txt`.

### Continuation 1487

- [x] Align the tested `INFORMATION_SCHEMA.FILES` field nullability and empty
  catalog semantics with MySQL 8.4.
- [x] Remove the one-page `.ibd` placeholder race and extend physical files
  when an extent is allocated, so registered root pages remain readable.
- [x] Verify the focused FILES/auxiliary regressions, all
  `TestInformationSchema`, the privilege/role slice, the full engine package,
  and the subsequent serial repository run.
- [ ] Continue the remaining P1 aggregate semantics, P2/P4 official MySQL
  interoperability, and P3 non-Connector-J client authentication matrix.
- Evidence: `reports/compatibility/p1-information-schema-files-and-tablespace-pages-current-continuation1487.txt`.

### Continuation 1490

- [x] Re-run the local replication, native binlog/GTID, XA, recovery/promotion,
  and I_S/P_S regression slice. Replication and net passed; the engine slice also
  passed. Evidence:
  `reports/compatibility/global-compatibility-status-current-continuation1490.txt`.
- [x] Re-audit the non-Connector-J client environment. Go, PyMySQL and
  Node/mysql2 are available; mysql CLI and Docker fallback are unavailable.
- [ ] Keep authenticated client execution and the official MySQL full
  bidirectional promotion/crash-window matrix open until their protected test
  environments are supplied and the scenarios are run.

### Continuation 1491

- [x] Add stale file-position recovery through the durable GTID set, prevent
  older ROTATE_EVENT frames from moving the source position backwards, and
  decode the compact official-MySQL `MYSQL_TYPE_STRING` row-image form.
- [x] Verify the local replication regression slice and official single-
  transaction/XA/rotation fixture evidence.
- [ ] Fix the remaining runtime second-transaction state loop where a GTID
  stream exposes only a metadata/rotation preamble before the new transaction;
  keep the official bidirectional, crash-window, promotion and full client
  gates partial until this is closed.
- Evidence: `reports/compatibility/global-compatibility-status-current-continuation1491.txt`.

### Continuation 1492

- [x] Correct the GTID-stream mode decision after loading the durable executed
  set, so valid lower-numbered ROTATE_EVENT preambles are retained.
- [x] Verify official MySQL 8.4.11 ordinary, GTID, XA, continuous-runtime,
  rotation and restart source scenarios; all five passed.
- [ ] Continue the reverse xmysql-to-official GTID/XA/crash/promotion and
  duplicate-delivery matrix, the authenticated non-Connector/J client matrix,
  and the full I_S/P_S runtime/component/permission semantics.
- Evidence: `reports/compatibility/global-compatibility-status-current-continuation1492.txt`.

### Continuation 1493

- [x] Run the authenticated mysql CLI, Go, PyMySQL and Node mysql2 matrix.
- [x] Run the same clients against the source endpoint, catch the replica up,
  promote it, and rerun the matrix against the promoted endpoint.
- [x] Close the aggregate P3 non-Connector-J and cluster-endpoint rows as
  implemented.
- [ ] Continue P1 full I_S/P_S runtime/component/permission semantics and the
  P2/P4 reverse official-MySQL GTID/XA/crash/promotion/duplicate matrix.
- Evidence: `reports/compatibility/global-compatibility-status-current-continuation1493.txt`.

### Continuation 1494

- [x] 补齐原生 MySQL 源 `@@GLOBAL.server_uuid` 到 Runtime 和
  `performance_schema.replication_connection_status.SOURCE_UUID` 的真实投影。
- [x] 修复官方驱动返回 `[]byte` 时 UUID 被错误格式化的问题。
- [x] 通过官方 MySQL 8.4.11 fixture、native runtime 和 P_S 定向回归。
- [ ] 完整 I_S/P_S 运行时、组件生命周期、权限语义仍保持 partial。
- [ ] P2/P4 双向复制、提升、重复投递和全崩溃窗口矩阵仍待完成。

Evidence: `reports/compatibility/p1-source-uuid-performance-schema-current-continuation1494.txt`。

### Continuation 1495

- [x] 将同一权威复制运行时的 `Source_UUID` 投影到当前实现的
  `SHOW REPLICA STATUS` / `SHOW SLAVE STATUS` 结果。
- [x] 通过 engine 定向回归，且保持完整列集合、顺序、channel/error 元数据和
  生命周期/权限语义未完成的边界声明。

Evidence: `reports/compatibility/p1-show-replica-status-source-uuid-current-continuation1495.txt`。

### Continuation 1496

- [x] 保留 mysql CLI、Go mysql driver、PyMySQL、Node mysql2 及 promoted endpoint
  的已验证子集。
- [x] 将“完整非 Connector/J 客户端矩阵”聚合项校正为 partial，等待更多驱动、
  ORM、认证/TLS、协议边界和版本组合的证据。

Evidence: `reports/compatibility/p3-client-scope-boundary-current-continuation1496.txt`。

### Continuation 1497

- [x] 持久化 native upstream `server_uuid`，并在 replica Runtime 重启时提前恢复。
- [x] 独立身份文件不保存 source URL/凭据，source 变更会清除旧身份。
- [x] 通过 focused red/green、replication 全量包和 P_S/SHOW REPLICA 定向回归。

Evidence: `reports/compatibility/p1-source-identity-persistence-current-continuation1497.txt`。

### Continuation 1498

- [x] 在 fresh MySQL 8.4.11 fixture 上复验普通事务、SOURCE_UUID、GTID、XA、
  连续拉取和轮转/重启 source matrix。
- [ ] 保持 reverse promotion/failover、全崩溃窗口、重复投递和完整双向 XA/binlog
  互操作为未关闭项。

Evidence: `reports/compatibility/p2-p4-official-source-regression-current-continuation1498.txt`。

### Continuation 1499

- [x] 持久化本地 promotion 后的 `source` 角色，使原副本配置重启时恢复为 promoted source。
- [x] 通过 focused promotion 回归、replication 全量包和 SHOW REPLICA/P_S 定向回归。
- [ ] 保持官方 MySQL 反向提升、全部崩溃窗口、重复投递、多节点仲裁和完整双向
  GTID/XA/binlog 互操作为未关闭项。

Evidence: `reports/compatibility/p2-promotion-role-persistence-current-continuation1499.txt`。

### Continuation 1500

- [x] `RESET REPLICA ALL` 清除旧 native upstream identity，避免重置后 SOURCE_UUID 串源。
- [x] 通过 focused reset 回归和 replication 全量包。
- [ ] 完整 MySQL RESET REPLICA 选项/元数据与 I_S/P_S 全量权限、生命周期语义仍保持 partial。

Evidence: `reports/compatibility/p1-replica-reset-clears-source-identity-current-continuation1500.txt`。

### Continuation 1501

- [x] 通过新鲜串行全仓回归：`go test -p 1 ./... -count=1 -timeout 45m`。
- [x] 通过 cluster smoke：单源、双副本、DML/DDL、回滚隔离和副本提升。
- [x] 通过三次重复的本地 crash-recovery manager 门禁。
- [ ] 这些本地门禁不关闭官方 MySQL 反向提升/故障切换、全部崩溃窗口、重复投递和
  完整双向 XA/binlog 互操作；P1 全量 I_S/P_S 运行时/组件/权限语义与完整非
  Connector-J 客户端矩阵也继续保持 partial。

Evidence: `reports/compatibility/p2-p0-local-gates-current-continuation1501.txt`。

### Continuation 1502

- [x] 对照新鲜 MySQL 8.4.11 fixture，补齐 `SHOW REPLICA STATUS` / `SHOW SLAVE STATUS`
  的 60 列官方顺序。
- [x] 投影当前真实拥有的 source file、position、GTID、错误和运行状态；未维护的
  relay/TLS/filter/worker 组件保持 NULL/空值。
- [x] 通过 SHOW/P_S 定向回归和 replication 全量包。
- [ ] P1 其它运行时/组件/权限语义、P2/P4 双向复制与完整故障窗口、P3 完整非
  Connector-J 客户端矩阵仍未关闭。

Evidence: `reports/compatibility/p1-show-replica-status-column-contract-current-continuation1502.txt`。

### Continuation 1503

- [x] 对照 MySQL 8.4.11，补齐 native `mysql://` source 在
  `replication_connection_configuration` 中的 HOST、PORT、USER、AUTO_POSITION、
  SSL_ALLOWED 投影。
- [x] 保证 source 用户不进入 HTTP status JSON，并保留 HTTP xmysql source 的既有
  endpoint 语义。
- [x] 通过 native P_S 定向回归。
- [ ] P1 其它组件生命周期、重试/TLS/压缩配置、权限和 worker/filter 语义仍未关闭。

Evidence: `reports/compatibility/p1-performance-schema-native-connection-fields-current-continuation1503.txt`。

### Continuation 1504

- [x] 让 `CHANGE REPLICATION SOURCE TO` / `CHANGE MASTER TO` 在保留 HTTP source
  行为的同时生成 native `mysql://` source URL，并解析用户、日志文件、日志位置和
  `SOURCE_AUTO_POSITION`。
- [x] 要求 source 变更发生在复制线程停止后；变更时立即重建 native source，并验证
  持久化配置可在 Runtime 重启后恢复。
- [x] 通过 focused red/green、replication 全量包和复制管理/SHOW/P_S engine 定向回归。
- [ ] source password 的安全注入、TLS/压缩、完整官方 MySQL 双向 promotion/failover、
  全崩溃窗口与重复投递仍未关闭。
- [ ] 完整 I_S/P_S 运行时/权限/组件语义和完整非 Connector-J 客户端生态矩阵仍未关闭。
- [x] 保持 FULLTEXT deferred，以及非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换
  out of scope。

Evidence: `reports/compatibility/p2-p4-native-source-change-sql-current-continuation1504.txt`。

### Continuation 1505

- [x] 保留 native replica 首次接入时显式配置的 `gtid_set` 基线；本地尚无已执行
  GTID 时不再错误地清空该基线。
- [x] 将官方标准 `TABLE_MAP` 的合成列名 `column_N` 按本地持久化表列顺序重绑定，
  覆盖行事件的 Before/After/列类型等映射，并通过回归测试。
- [x] 通过 replication 全包和 native source-change/列映射 engine 定向回归。
- [ ] 官方 MySQL 8.4.11 反向 promotion fixture 因 Docker Desktop CLI 在容器启动时
  无响应，本轮未记为通过；P2/P4 反向 XA/binlog/GTID、崩溃恢复、promotion/failover
  和重复投递仍保持 partial。
- [ ] P1 全量 I_S/P_S 运行时/权限/组件语义、P3 完整非 Connector-J 客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p2-p4-official-mysql-reverse-promotion-current-continuation1505.txt`。

### Continuation 1506

- [x] 支持首次 native replica 接入时空 GTID 集的 `SOURCE_AUTO_POSITION=1`，不再
  强制要求 binlog file/position。
- [x] 让 source URL、COM_BINLOG_DUMP_GTID 和 Runtime 首次拉取保持一致。
- [x] 通过 replication 全包和 `CHANGE REPLICATION SOURCE` engine 定向回归。
- [ ] 官方 MySQL 网络 fixture、双向 XA/binlog/GTID、promotion/failover、崩溃窗口和
  重复投递仍未关闭；Docker CLI 当前不可用。

Evidence: `reports/compatibility/p2-native-empty-gtid-auto-position-current-continuation1506.txt`。

### Continuation 1507

- [x] 支持 native source URL 与 `CHANGE REPLICATION SOURCE` 的 SSL/TLS、CA、客户端
  证书、key 和服务端证书校验配置。
- [x] 将 TLS 配置传递给 COM_BINLOG_DUMP 与 SOURCE_UUID 元数据连接，并投影真实的
  `SSL_ALLOWED`。
- [x] 通过 replication、engine P_S 和 source-change 定向回归。
- [ ] 官方 MySQL TLS 网络 fixture、完整双向 XA/binlog/GTID、promotion/failover、
  崩溃窗口和重复投递仍未关闭；Docker CLI 当前不可用。

Evidence: `reports/compatibility/p2-p3-native-replication-tls-current-continuation1507.txt`。

### Continuation 1508

- [x] 保存 native source 的连接重试间隔/次数、心跳周期、压缩算法和 zstd 等级。
- [x] 让 CHANGE SOURCE / CHANGE MASTER 生成这些配置，并由 P_S 真实投影。
- [x] 通过 source、管理 SQL 和 P_S 定向回归。
- [ ] 实际网络重试、心跳、压缩协商及官方 MySQL 双向故障窗口仍需外部 fixture；
  Docker CLI 当前不可用。

Evidence: `reports/compatibility/p1-p2-native-replication-runtime-options-current-continuation1508.txt`。

### Continuation 1509

- [x] 让 native replica 在 source 拉取失败后使用 `SOURCE_CONNECT_RETRY` 配置的
  下一轮运行时重试间隔。
- [x] 保持 go-mysql 同步器的重连次数、heartbeat 配置真实映射，并通过 red/green
  测试、replication 全包、复制相关 engine 定向回归和完整 engine 回归。
- [ ] go-mysql 依赖内部每次重连之间固定 1 秒；压缩协商以及官方 MySQL 双向
  XA/binlog/GTID、promotion/failover、崩溃窗口仍需依赖层/外部 fixture，未宣称关闭。

Evidence: `reports/compatibility/p1-p2-native-replication-retry-runtime-current-continuation1509.txt`。

### Continuation 1510

- [x] 统计 native `HEARTBEAT_EVENT`，推进 source position 但排除事务解码，记录
  最后心跳时间。
- [x] 将心跳计数和时间投影到 `performance_schema.replication_connection_status`，
  并在更换/重置 source 时清理 channel 状态。
- [x] 通过 runtime/P_S focused 回归、replication 全包和复制相关 engine 定向回归。
- [ ] 官方 MySQL 网络 fixture、压缩协商、完整 P_S 组件生命周期以及双向
  XA/binlog/GTID、promotion/failover、崩溃窗口仍未关闭。

Evidence: `reports/compatibility/p1-performance-schema-replication-heartbeat-current-continuation1510.txt`。

### Continuation 1511

- [x] 记录 native replication 最近一次 MySQL 错误号与 UTC 时间，并在 runtime reset/
  CHANGE SOURCE 时清理 channel 状态。
- [x] 将错误号、错误消息和错误时间投影到 P_S connection status 及单线程
  coordinator/worker applier 视图。
- [x] 通过 red/green runtime/P_S 回归、replication 全包和复制相关 engine 定向回归。
- [ ] 完整 I_S/P_S 表与权限语义、P3 非 Connector/J 客户端矩阵、官方 MySQL 双向
  XA/binlog/GTID、promotion/failover 和崩溃窗口仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-replication-errors-current-continuation1511.txt`。

### Continuation 1512

- [x] 记录认证失败的首次/最近时间，并按 host 聚合到 host-cache 状态。
- [x] 投影 `FIRST_ERROR_SEEN` / `LAST_ERROR_SEEN`，保留成功认证清理活动失败计数的
  既有行为。
- [x] 通过 metrics、host-cache、完整 P_S 测试；完整 engine 回归仍需本 slice 后复核。
- [ ] 完整 I_S/P_S 表、组件、生命周期、权限语义、P3 非 Connector/J 客户端矩阵，
  以及官方 MySQL 双向 XA/binlog/GTID、promotion/failover、崩溃窗口仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-host-cache-lifecycle-current-continuation1512.txt`。

### Continuation 1513

- [x] 将 native 单线程 applier 的默认配置投影到
  `replication_applier_configuration`。
- [x] 覆盖 privilege-check、row-format、primary-key-check 和 anonymous-GTID
  配置列，并通过 focused P_S replication 回归。
- [ ] 完整复制过滤器/Group Replication 语义以及官方 MySQL 双向
  XA/binlog/GTID、promotion/failover、崩溃窗口仍未关闭。

Evidence: `reports/compatibility/p1-performance-schema-applier-configuration-current-continuation1513.txt`。

### Continuation 1514

- [x] 将 native source pull/connection error 与 native apply error 分别归类为
  IO 与 SQL/applier 错误，并保留各自错误号、消息和 UTC 时间。
- [x] 将分类状态贯通到 `SHOW REPLICA STATUS` 的 `Last_IO_*`/
  `Last_SQL_*` 以及 P_S connection/coordinator/worker status。
- [x] 通过 focused red/green、replication 全包、复制管理/SHOW/P_S 定向回归和
  完整 engine 回归。
- [ ] 官方 MySQL 双向 XA/binlog/GTID、崩溃恢复、promotion/failover、完整
  I_S/P_S 权限生命周期和非 Connector/J 完整客户端矩阵仍未关闭。

Evidence: `reports/compatibility/p1-p2-replication-error-classification-current-continuation1514.txt`。

### Continuation 1515

- [x] 实现并持久化本地 replica 过滤器，覆盖 DB、精确表和通配表规则。
- [x] 接通 `CHANGE REPLICATION FILTER`，要求 applier 停止后修改，并投影到
  `replication_applier_filters` / `replication_applier_global_filters`。
- [x] 保证完全过滤的事务仍推进 GTID/relay 边界，row image 过滤后不绕过规则执行
  statement image。
- [x] 通过 replication 全包、复制管理/P_S 定向回归和完整 engine 回归。
- [ ] 完整 MySQL 过滤语法、官方双向 XA/binlog/GTID、崩溃恢复、promotion/failover、
  P_S 权限生命周期和非 Connector/J 完整矩阵仍未关闭。

Evidence: `reports/compatibility/p2-replication-filters-current-continuation1515.txt`。

### Continuation 1516

- [x] 实现并持久化有序 `REPLICATE_REWRITE_DB` 规则。
- [x] 支持官方双括号 `CHANGE REPLICATION FILTER` 语法，并在其他过滤规则前改写
  row table / statement 默认数据库。
- [x] native prepared-XA commit replay 复用过滤器，P_S 过滤器视图投影重写规则。
- [x] 通过 replication 全包、复制相关 engine 定向回归和完整 engine 回归。
- [ ] channel-specific/Group Replication 限制、完整 SQL 文本改写、官方双向
  XA/binlog/GTID、崩溃恢复、promotion/failover 和非 Connector-J 完整矩阵仍未关闭。

Evidence: `reports/compatibility/p2-replication-rewrite-db-current-continuation1516.txt`。

### Continuation 1517

- [x] 将 `REPLICATE_REWRITE_DB` 投影到 `SHOW REPLICA STATUS.Replicate_Rewrite_DB`。
- [x] 使用 MySQL `(from_db,to_db),(from_db2,to_db2)` 输出格式并保留未配置时的 NULL。
- [x] 通过 SHOW/P_S/复制定向回归和完整 engine 回归。
- [ ] channel-specific SHOW、完整复制权限语义、官方双向 XA/binlog/GTID、崩溃
  恢复、promotion/failover 和非 Connector-J 完整矩阵仍未关闭。

Evidence: `reports/compatibility/p1-show-replica-rewrite-db-current-continuation1517.txt`。

### Continuation 1518

- [x] 为 `SHOW REPLICA STATUS` / `SHOW SLAVE STATUS` 增加已认证会话权限检查，要求
  `REPLICATION CLIENT` 或兼容的 `SUPER`。
- [x] 将持久化账号授权、激活角色授权和认证会话全局权限快照纳入判定，并保留
  nil/internal session 的内部投影行为。
- [x] 通过红绿权限回归、SHOW/账号相关定向回归和完整 engine 回归。
- [ ] 完整 I_S/P_S 表、生命周期和权限语义、channel-specific 复制权限、非
  Connector/J 完整矩阵、官方双向 XA/binlog/GTID、崩溃恢复、promotion/failover
  仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-show-replica-status-permission-current-continuation1518.txt`。

### Continuation 1519

- [x] 为 SHOW 状态/日志查询补齐 `REPLICATION CLIENT` 与 `REPLICATION SLAVE`
  的区分权限。
- [x] 为 replica 控制补齐 `REPLICATION_SLAVE_ADMIN`/`SUPER`，并为
  `RESET REPLICA` 补齐严格的 `RELOAD` 门槛。
- [x] 为 source binlog 管理补齐 `RELOAD`（flush/reset）和 `BINLOG_ADMIN`
  （purge）门槛。
- [x] 通过红绿权限回归、相关 SHOW/账号/admin 定向回归和完整 engine 回归。
- [ ] 完整 I_S/P_S 表、生命周期和权限语义、channel-specific 复制、非
  Connector/J 完整矩阵、官方双向 XA/binlog/GTID、崩溃恢复、promotion/failover
  仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-p2-replication-permissions-current-continuation1519.txt`。

### Continuation 1520

- [x] 为 `SHOW REPLICAS`、`SHOW SLAVE HOSTS` 和 `SHOW REPLICA HOSTS` 补齐
  `REPLICATION SLAVE` 权限门禁，并复用现有账号/角色/会话权限解析。
- [x] 保持成功授权后的五列 replica registration 结果形状与数据源不变。
- [x] 通过红绿权限回归、既有结果形状测试和完整 engine 回归。
- [ ] 完整 I_S/P_S 表、生命周期和权限语义、channel-specific 复制、非
  Connector/J 完整矩阵、官方双向 XA/binlog/GTID、崩溃恢复、promotion/failover
  仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-show-replica-hosts-permission-current-continuation1520.txt`。

### Continuation 1521

- [x] 为 `SHOW ENGINE INNODB STATUS` 补齐全局 `PROCESS` 权限门禁。
- [x] 保持成功授权后的三列结果形状和实时 InnoDB 状态投影不变。
- [x] 通过权限回归、既有结果形状测试和完整 engine 回归。
- [ ] 完整 I_S/P_S 表、生命周期和权限语义、channel-specific 复制、非
  Connector/J 完整矩阵、官方双向 XA/binlog/GTID、崩溃恢复、promotion/failover
  仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-show-engine-process-permission-current-continuation1521.txt`。

### Continuation 1522

- [x] 将 Performance Schema 多表读取从“首表权限检查”扩展为所有直接表引用逐表
  检查 `SELECT`。
- [x] 保留 `processlist` 的 PROCESS 规则、反引号表名和 nil/internal/root 行为。
- [x] 通过红绿多表权限回归、完整 P_S 测试族和完整 engine 回归。
- [ ] 完整 I_S/P_S 运行时值、组件生命周期、剩余权限/角色语义、P2/P4 官方复制
  互操作和 P3 客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-multi-table-select-current-continuation1522.txt`。

### Continuation 1523

- [x] 将 INFORMATION_SCHEMA 的 `PROCESS` 门禁从首个 registry 表扩展为查询中所有
  直接引用的 `TABLESPACES`、`FILES` 和 InnoDB 诊断表。
- [x] 保留 processlist 行级可见性、反引号表名以及 nil/internal session 行为。
- [x] 通过 I_S 多表红绿权限回归、既有 InnoDB PROCESS 回归、P_S 权限回归和完整
  I_S/P_S 测试族。
- [ ] 完整 I_S/P_S 运行时值、组件生命周期、剩余权限/角色语义、P2/P4 官方复制
  互操作和 P3 客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-information-schema-multi-table-process-current-continuation1523.txt`。

### Continuation 1524

- [x] 将 `mysql.*` 虚拟授权表的 SELECT 权限检查从首表扩展为查询中的全部直接表引用。
- [x] 保留单表 handler、持久化账号/角色/会话授权解析及 nil/internal session 行为。
- [x] 通过 mysql 授权表红绿回归、既有授权表回归和完整 I_S/P_S 测试族。
- [ ] 完整 I_S/P_S 运行时值、组件生命周期、剩余权限/角色语义、P2/P4 官方复制
  互操作和 P3 客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-mysql-metadata-multi-table-process-current-continuation1524.txt`。

### Continuation 1525

- [x] 为 `COM_BINLOG_DUMP` / `COM_BINLOG_DUMP_GTID` 增加已认证会话的
  `REPLICATION SLAVE` 权限门禁，并保留兼容的 `SUPER/ALL` 放行。
- [x] 保持未绑定认证会话的内部投影、GTID 解析、轮转和非阻塞 dump 行为不变。
- [x] 通过 binlog 权限红绿回归、完整 `server/net` 与 `server/replication` 测试。
- [ ] 官方 MySQL 双向 XA/binlog/GTID、崩溃恢复、promotion/failover、完整 I_S/P_S
  聚合和 P3 客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p2-binlog-dump-replication-privilege-current-continuation1525.txt`。

### Continuation 1526

- [x] 为 `COM_REGISTER_SLAVE` 增加已认证会话的 `REPLICATION SLAVE` 权限门禁，兼容
  保留 `SUPER/ALL` 放行。
- [x] 确保无权限时不会写入 replica registry，授权后保留既有注册响应和元数据。
- [x] 通过注册权限红绿回归、完整 `server/net` 与 `server/replication` 测试。
- [ ] 官方 MySQL 双向 XA/binlog/GTID、崩溃恢复、promotion/failover、完整 I_S/P_S
  聚合和 P3 客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p2-register-replica-replication-privilege-current-continuation1526.txt`。

### Continuation 1527

- [x] 将 `performance_schema.status_by_account`、`status_by_host` 和 `status_by_user`
  的 `Queries`/`Com_*` 投影接入 recorder 的生命周期汇总，保留断开后的历史身份统计。
- [x] 保持 `Threads_connected`/`Threads_running` 使用实时会话和活动语句，不把历史连接
  总数误当作当前连接数。
- [x] 通过断开身份红绿回归和完整 I_S/P_S 测试族。
- [ ] 全 engine 回归仍需诊断全包顺序下的单个 P_S 测试失败；完整 I_S/P_S 权限、组件
  生命周期、P2/P4 官方复制互操作和 P3 客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-status-summary-lifecycle-current-continuation1527.txt`。

### Continuation 1528

- [x] 将 `GRANT ... (column) ... WITH GRANT OPTION` 的控制项保持在列授权范围，
  不再为纯列授权持久化空的表级 grant scope。
- [x] 让 `mysql.columns_priv.Column_priv` 只投影实际列权限，并让
  `INFORMATION_SCHEMA.COLUMN_PRIVILEGES.IS_GRANTABLE` 单独反映列级授权能力。
- [x] 通过列级权限红绿回归，以及账号、角色、GRANT/REVOKE、动态权限和部分撤销权限
  相关回归。
- [ ] 完整 I_S/P_S 表覆盖、运行时生命周期、剩余角色/权限语义、P2/P4 官方复制
  互操作和 P3 客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-column-grant-option-scope-current-continuation1528.txt`。

### Continuation 1529

- [x] 支持列级 `REVOKE GRANT OPTION FOR`，只移除列授权的 delegation 能力并保留
  原始列权限。
- [x] 保证 grant-option-only revoke 不改动 partial revoke 状态。
- [x] 通过表级/列级撤销红绿回归和账号、角色、GRANT/REVOKE、动态权限及部分撤销
  权限回归。
- [x] 通过完整 `Test(InformationSchema|PerformanceSchema)` 测试族。
- [ ] 完整 I_S/P_S 表与运行时语义、剩余角色/权限覆盖、P2/P4 官方复制互操作和
  P3 客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-column-revoke-grant-option-current-continuation1529.txt`。

### Continuation 1530

- [x] 按 MySQL 语法输出列级 `SHOW GRANTS`，将 `GRANT OPTION` 放在语句尾部的
  `WITH GRANT OPTION`，不再混入列权限列表。
- [x] 通过列级展示、撤销、元数据和权限族回归。
- [ ] 完整 I_S/P_S 语义、剩余角色/权限覆盖、P2/P4 官方复制互操作和 P3 客户端
  矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-column-show-grants-format-current-continuation1530.txt`。

### Continuation 1531

- [x] 将 recorder 的 account 维度内存高水位接入 P_S `accounts/hosts/users`，投影
  `MAX_SESSION_CONTROLLED_MEMORY` 和 `MAX_SESSION_TOTAL_MEMORY`。
- [x] 保持连接摘要断开后历史值和 accounts/hosts/users 的 TRUNCATE 重置语义。
- [x] 通过连接摘要定向回归和隔离运行回归。
- [ ] P_S 全包仍需诊断测试间共享历史身份；完整 I_S/P_S 语义、组件权限生命周期、
  P2/P4 官方复制互操作和 P3 客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-connection-memory-current-continuation1531.txt`。

### Continuation 1532

- [x] 将 I_S 的表、库、列、全局权限视图及 `ROLE_*_GRANTS` 中的
  `ALL/ALL PRIVILEGES` 投影拆分为 MySQL 的逐项权限；`GRANT OPTION` 不作为独立
  权限行，grantor provenance 仍可回溯到原始 ALL grant。
- [x] 通过新增 ALL 权限和角色权限回归、权限族回归、完整 P_S 回归。
- [x] 修复两个依赖 process-wide recorder 历史身份的测试顺序问题；engine 全包
  回归通过，`GO_EXIT=0`。
- [ ] 完整 I_S/P_S 运行时、组件生命周期和权限语义，P2/P4 官方复制互操作以及
  P3 全量客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-privilege-all-expansion-and-engine-regression-current-continuation1532.txt`。

### Continuation 1533

- [x] 将 runtime recorder 的语句内存仪表接入 statement event，捕获 live
  `memory/sql/THD::main_mem_root` 的 current/语句级 high-water 值，并以语句开始基线
  防止前一条语句的生命周期峰值泄漏。
- [x] 将 `MAX_CONTROLLED_MEMORY` / `MAX_TOTAL_MEMORY` 投影到 statement history、
  按维度 summary 和 digest summary；不估算 CPU、锁时间、临时表或 secondary-engine
  指标。
- [x] 通过 metrics 定向测试、Performance Schema 定向测试、完整 I_S/P_S 族和全仓串行
  Go 回归，`GO_EXIT=0`。
- [ ] 完整 I_S/P_S 运行时与权限语义、P2/P4 官方 XA/binlog/GTID/复制/崩溃互操作、
  P3 全量客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-statement-memory-current-continuation1533.txt`。

### Continuation 1534

- [x] 在握手能力中声明 `CLIENT_COMPRESS`，并在认证成功后按客户端协商结果启用 MySQL
  传输压缩；握手包本身保持普通 MySQL 包。
- [x] 将压缩统一收口到 Session 发送层，覆盖即时发送、异步发送和批量发送，避免
  handler/Session 双重压缩。
- [x] 支持标准压缩帧解压、半包等待，以及单个压缩帧中多个普通 MySQL 包的排队派发。
- [x] 通过握手、认证协商、读写和多包排队定向回归；当前环境缺少 `mysql`/`mariadb`
  CLI，真实客户端压缩矩阵仍待外部环境验证。
- [ ] 完整 I_S/P_S 运行时与权限语义、P2/P4 官方 XA/binlog/GTID/复制/崩溃互操作、
  P3 全量客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p3-mysql-transport-compression-current-continuation1534.txt`。

### Continuation 1535

- [x] 让 `REPLICATE_REWRITE_DB` 同时改写 statement 的默认数据库和 SQL 文本中的
  `source_db.table` 限定名，覆盖普通、反引号和安全双引号标识符。
- [x] 跳过字符串字面量、行注释、块注释和未限定表名，避免对 SQL 文本做错误替换。
- [x] 通过复制过滤器改写、storage callback、完整 replication 包、引擎复制过滤/P_S
  定向回归和全仓串行 Go 回归，`GO_EXIT=0`。
- [ ] 完整 MySQL filter grammar、channel-specific/Group Replication 限制、官方双向
  XA/binlog/GTID、崩溃恢复、promotion/failover、重复投递、完整 I_S/P_S 语义和 P3
  全量客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p2-rewrite-qualified-statement-db-current-continuation1535.txt`。

### Continuation 1536

- [x] 为 statement-based/native replay 补齐 `REPLICATE_DO_TABLE`、
  `REPLICATE_IGNORE_TABLE`、`REPLICATE_WILD_DO_TABLE` 和
  `REPLICATE_WILD_IGNORE_TABLE` 的表引用识别，支持默认库、库表限定名和多表语句。
- [x] 词法扫描跳过字符串、注释和无法可靠解析的子查询目标，避免误拦截；新增表级
  过滤回归，完整 `server/replication` 包通过。
- [ ] 完整 MySQL filter grammar、channel-specific/Group Replication 限制、官方双向
  XA/binlog/GTID、崩溃恢复、promotion/failover、重复投递、完整 I_S/P_S 语义和 P3
  全量客户端矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p2-statement-table-filters-current-continuation1536.txt`。

### Continuation 1537

- [x] 复核 continuation1493 客户端矩阵：mysql CLI（Docker MySQL 8.4.11 客户端）、Go
  mysql driver、PyMySQL、Node mysql2 在源端和晋升端的完整用例均 PASS。
- [x] 将 P3 `non-connector-j-matrix` 聚合状态从 partial 修正为 implemented，并保留
  单客户端与集群晋升报告作为证据。
- [ ] 当前机器因 Docker daemon 和受保护密码不可用，不能重新执行外部客户端矩阵；这不
  改变 continuation1493 的同一工作区 PASS 证据。P1 I_S/P_S 完整语义、P2/P4 官方
  XA/binlog/GTID/复制互操作和崩溃晋升矩阵仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/global-compatibility-status-current-continuation1493.txt`。

### Continuation 1540

- [x] 将 `performance_schema.setup_threads` 从单一客户端线程行扩展为线程类别注册表，覆盖
  `thread/performance_schema/setup`、`thread/sql/event_scheduler`、`thread/sql/main` 和
  `thread/sql/one_connection`，保持按 NAME 排序、MySQL 的 `PROPERTIES`/`VOLATILITY` 形状和
  `UPDATE ... WHERE name` 的逐类修改语义。
- [x] 按 MySQL 语义拒绝 `TRUNCATE TABLE performance_schema.setup_threads`；实际运行线程仍由
  `performance_schema.threads` 根据真实客户端/复制线程产生，不用注册表伪造后台实例。
- [x] 通过 setup_threads 定向回归、完整 Performance Schema 回归和全仓串行 Go 回归，均为
  `GO_EXIT=0`。
- [ ] 这只关闭 setup_threads 注册表这一局部缺口；完整 I_S/P_S 组件清单、所有插件/后台线程
  生命周期、P2/P4 官方 XA/binlog/GTID/复制/崩溃互操作仍未关闭。非 Connector/J 客户端矩阵
  已按既有 continuation1493 证据标记为 implemented，但当前 Docker 不可用，未重新执行外部矩阵。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-setup-threads-registry-current-continuation1540.txt`。

### Continuation 1541

- [x] 补齐 `RESET REPLICA`/`RESET SLAVE` 的可选 `ALL FOR CHANNEL <channel>` 和
  `FOR CHANNEL <channel>` 解析，并保持原有 `RELOAD` 权限门槛。
- [x] 单通道 runtime 接受空引号默认 channel 并委托 reset/reset-all；非空 channel
  明确拒绝，避免把未实现的多通道语义当成默认 channel。
- [x] 通过 reset admin 定向回归以及 engine/replication reset-control 回归，均为
  `GO_EXIT=0`。
- [ ] 多通道 runtime、官方 XA/binlog/GTID、反向/双向复制、崩溃恢复和晋升互操作仍为
  partial；P1 I_S/P_S 完整组件与生命周期仍为 partial。非 Connector/J 客户端矩阵
  沿用 continuation1493 的 implemented 证据，当前 Docker 不可用，未在本轮复跑。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p2-reset-replica-channel-current-continuation1541.txt`。

### Continuation 1542

- [x] 补齐 `START/STOP REPLICA` 与 `START/STOP SLAVE` 的 `FOR CHANNEL <channel>`
  解析，并保持原有 `REPLICATION_SLAVE_ADMIN` 权限门槛及线程选项拒绝边界。
- [x] 单通道 runtime 接受空引号默认 channel 并复用既有 start/stop 回调；非空 channel
  明确拒绝，不伪装成多通道已实现。
- [x] 通过 admin 定向回归以及 engine/replication 复制控制回归，均为 `GO_EXIT=0`。
- [ ] 多通道 runtime、线程级控制选项、官方 XA/binlog/GTID、反向/双向复制、崩溃恢复、
  重复投递和晋升互操作仍为 partial；P1 I_S/P_S 完整组件与生命周期仍为 partial。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p2-replication-channel-control-current-continuation1542.txt`。

### Continuation 1543

- [x] 补齐 `SHOW REPLICA STATUS` 与 `SHOW SLAVE STATUS` 的 `FOR CHANNEL <channel>`
  解析，保持 `REPLICATION CLIENT` 权限检查和既有 MySQL 8.4 状态列投影。
- [x] 单通道 runtime 对空引号默认 channel 复用 status provider；非空 channel 明确
  拒绝，不伪装成多通道已实现。
- [x] 通过 SHOW 状态列契约、运行时投影和 channel 边界回归。
- [ ] 多通道状态、`NONBLOCKING`、官方 XA/binlog/GTID、反向/双向复制、崩溃恢复、
  重复投递和晋升互操作仍为 partial；P1 I_S/P_S 完整组件与生命周期仍为 partial。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p2-show-replica-status-channel-current-continuation1543.txt`。

### Continuation 1544

- [x] 补齐 `CHANGE REPLICATION SOURCE TO`、`CHANGE MASTER TO` 和
  `CHANGE REPLICATION FILTER` 的 `FOR CHANNEL <channel>` 解析。
- [x] 单通道 runtime 接受空引号默认 channel 并复用既有 callback；非空 channel
  明确拒绝，保留 source/filter 的权限和 stopped-replica 生命周期校验。
- [x] 通过 source/filter/admin/status 复制控制回归，均为 `GO_EXIT=0`。
- [ ] 多通道 source/filter 状态、统一存储/WAL/binlog/GTID 提交协议、官方 XA/binlog/GTID、
  反向/双向复制、崩溃恢复、重复投递和晋升互操作仍为 partial；P1 I_S/P_S 完整组件与
  生命周期仍为 partial。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p2-replication-source-filter-channel-current-continuation1544.txt`。

### Continuation 1545

- [x] 补齐 MySQL 8.4 `SHOW BINARY LOG STATUS`，复用已有 native source 的文件位点和
  `Executed_Gtid_Set` 投影；既有 `SHOW MASTER STATUS`/`SHOW SOURCE STATUS` 别名保持不变。
- [x] 先验证缺口：新增回归在生产代码修改前命中 `unsupported SHOW type: binary log`；
  加入 dispatch 后 `TestShowBinaryLogStatusUsesTheSourceStatusContract` 与相关 master-status
  回归通过，`GO_EXIT=0`。
- [ ] 该切片不改变 P2 聚合状态；统一 storage/WAL/native-binlog/GTID 提交协议、官方
  双向 XA/binlog/GTID、崩溃恢复、重复投递和 promotion/failover 仍为 partial；P1
  I_S/P_S 完整组件与生命周期仍为 partial。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p2-show-binary-log-status-current-continuation1545.txt`。

### Continuation 1546

- [x] 实现 MySQL 8.4 `RESET BINARY LOGS AND GTIDS` 及可选 `TO <file index>`，并保留
  `RESET MASTER` 默认重置到序号 1 的兼容路径。
- [x] 打通 executor、runtime、source 和 native binlog writer：清空 Executed GTID 和
  logical binlog，按目标序号重建 native 文件，并验证关闭/重启后的 durable 状态。
- [x] 复用 source-binlog `RELOAD` 权限门槛，补充默认/显式序号、权限拒绝/允许、native
  文件名、物理位点和重启持久化回归。
- [x] 受影响包回归通过：engine 381.565s、replication 7.055s、net 8.368s；全仓串行
  回归完成且各测试包均为 `ok`，engine 449.704s、replication 8.503s。
- [ ] 该局部切片不改变 P2/P4 官方 XA/binlog/GTID、统一提交协议、多通道/多源、崩溃
  恢复、重复投递、promotion/failover 的 partial 聚合；P1 I_S/P_S 完整语义仍为 partial。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p2-reset-binary-logs-gtids-current-continuation1546.txt`。

### Continuation 1547

- [x] 补齐 MySQL 8.4 对 `performance_schema.setup_consumers` 和
  `performance_schema.setup_instruments` 的 `TRUNCATE TABLE` 拒绝语义，返回
  `Invalid performance_schema usage`。
- [x] 先用红测确认旧实现错误落入 reserved-database 校验，再在真实 DDL 分发路径修复；
  `setup_actors`/`setup_objects` 允许截断、`setup_threads` 禁止截断的既有边界保持不变。
- [x] `TestPerformanceSchema*` 专项通过（46.472s），engine 全包通过（380.473s）。
- [ ] P1 完整 I_S/P_S 组件、插件、运行时计数、锁/等待/线程字段精度和权限生命周期仍为
  partial；P2/P4 官方 XA/binlog/GTID、崩溃恢复和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-setup-truncate-boundary-current-continuation1547.txt`。

### Continuation 1548

- [x] 将 MySQL 8.4 `statements_digest` 注册到 `performance_schema.setup_consumers`，默认
  `ENABLED='YES'`，并保留标准 UPDATE 配置路径。
- [x] 让 `events_statements_summary_by_digest` 读取该 consumer gate；关闭后不再生成
  digest 汇总，statement history consumer 不受影响。
- [x] 通过红测、完整 `TestPerformanceSchema*` 专项（47.108s）和 engine 全包（378.043s）。
- [ ] P1 完整 consumer/instrument 类别、组件/插件生命周期、运行时计数、锁/等待/线程
  字段精度和权限生命周期仍为 partial；P2/P4 官方 XA/binlog/GTID 及崩溃晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-statements-digest-consumer-current-continuation1548.txt`。

### Continuation 1549

- [x] 按 MySQL 8.4 官方值修正 `setup_consumers` 默认矩阵：statement 当前/历史、transaction
  当前/历史、`statements_digest` 及 global/thread instrumentation 开启；history-long、
  stage/wait consumer 关闭。
- [x] 保持 runtime gate 与默认值一致；历史、stage、wait 相关测试显式开启所需 consumer，
  不用测试便利状态污染生产默认。
- [x] 默认矩阵测试、完整 P_S 专项（47.498s）、engine 全包（378.005s）和全仓串行回归
  均通过；全仓 engine 378.921s、manager 11.776s、net 8.452s、metrics 0.451s、
  protocol 0.755s、replication 6.456s。
- [ ] P1 完整 instrument/consumer 类别、组件/插件生命周期、运行时计数、锁/等待/线程
  字段精度和权限生命周期仍为 partial；P2/P4 官方 XA/binlog/GTID 及崩溃晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-consumer-defaults-current-continuation1549.txt`。

### Continuation 1550

- [x] 在 `setup_instruments` 注册 MySQL 8.4 `transaction` instrument，默认
  `ENABLED='YES'`、`TIMED='YES'`。
- [x] 让事务当前/历史采集遵守 instrument 的 `ENABLED` gate；关闭后停止追加，
  既有历史仍可查询；`TIMED='NO'` 时事务计时列为空。
- [x] 通过红测、事务 instrument 聚焦回归、完整 `TestPerformanceSchema*`（47.380s）、
  engine 全包（380.447s）和全仓串行回归；全仓 engine 396.011s、net 8.483s、
  metrics 0.493s、protocol 0.763s、replication 6.554s。
- [ ] P1 完整 I_S/P_S 表覆盖、组件/插件生命周期、运行时统计、锁/等待/线程字段精度和
  权限生命周期仍为 partial；P2/P4 官方 XA/binlog/GTID、崩溃恢复、重复投递和晋升互操作
  仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-transaction-instrument-current-continuation1550.txt`。

### Continuation 1551

- [x] 为前台连接增加 `setup_threads` 快照：新连接读取
  `thread/sql/one_connection` 的 ENABLED/HISTORY，既有连接不被后续更新追溯修改。
- [x] 让 `performance_schema.threads` 和语句级 Performance Schema 设置共享线程 gate，
  并补充新连接/既有连接边界红测。
- [x] 通过相关 setup_threads/threads 测试（3.530s）、完整 `TestPerformanceSchema*`
  专项（47.944s）、engine 全包（379.053s）和全仓串行回归；全仓 engine 378.522s，
  integration 0.999s，manager 11.636s，net 8.778s，metrics 0.485s，protocol 0.795s，
  replication 6.409s。
- [ ] P1 完整 I_S/P_S 表覆盖、所有线程类别、组件/插件生命周期、运行时统计、锁/等待/
  线程字段精度和权限生命周期仍为 partial；P2/P4 官方 XA/binlog/GTID、崩溃恢复、重复
  投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-setup-threads-runtime-current-continuation1551.txt`。

### Continuation 1552

- [x] 在 executor 和 engine 对外执行路径统一应用
  `statement/sql/<type>` instrument 的 ENABLED gate。
- [x] 补充关闭 `statement/sql/select` 后不产生 statement history、summary、digest 和
  相关汇总的红测与回归。
- [x] 通过完整 `TestPerformanceSchema*` 专项（50.166s）、engine 全包（379.158s）和
  全仓串行回归；全仓 engine 379.360s，integration 1.130s，manager 11.568s，net
  8.571s，metrics 0.450s，protocol 0.764s，replication 6.541s。
- [ ] P1 完整 I_S/P_S 表覆盖、所有 instrument TIMED/生命周期、组件/插件生命周期、运行时
  统计、锁/等待/线程字段精度和权限生命周期仍为 partial；P2/P4 官方 XA/binlog/GTID、
  崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-statement-instrument-gate-current-continuation1552.txt`。

### Continuation 1553

- [x] 为 RuntimeRecorder 增加 timer-aware statement 记录 API，并保持既有调用默认
  `TIMED=YES` 的兼容行为。
- [x] 将 `statement/sql/<type>` 的 TIMED 设置传入 executor 与 engine 两条执行路径；
  `TIMED=NO` 时保留 count/业务计数但将 summary、digest、stage/table-I/O timer 清零。
- [x] 通过 focused 重复测试（3 次）、完整 `TestPerformanceSchema*`（47.513s）、metrics
  包（0.454s）、engine 全包（380.831s）和全仓串行回归；全仓 engine 385.916s，
  integration 0.977s，manager 12.063s，net 8.542s，metrics 0.444s，protocol 0.735s，
  replication 6.528s。
- [ ] P1 完整 I_S/P_S 表覆盖、所有 instrument 家族 TIMED/生命周期、组件/插件生命周期、
  运行时统计、锁/等待/线程字段精度和权限生命周期仍为 partial；P2/P4 官方 XA/binlog/GTID、
  崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-statement-instrument-timed-current-continuation1553.txt`。

### Continuation 1554

- [x] 为 RuntimeRecorder 增加独立 `stageInstrumented` gate，区分 statement 与 stage
  summary/history 生命周期。
- [x] 将 `stage/sql/execute` ENABLED 设置传入 executor 和 engine 两条记录路径；关闭期间
  不创建 stage 汇总/历史，重新开启不回填。
- [x] 通过 lifecycle/statement-timer 聚焦重复测试、相关 stage/statement/setup 测试
  （23.559s）、完整 `TestPerformanceSchema*`（48.785s）、engine 全包（382.703s）和
  全仓串行回归；全仓 engine 384.911s，integration 1.023s，manager 11.985s，net
  8.470s，metrics 0.509s，protocol 0.793s，replication 8.559s。
- [ ] P1 完整 I_S/P_S 表覆盖、所有 stage/instrument 类别、组件/插件生命周期、运行时
  统计、锁/等待/线程字段精度和权限生命周期仍为 partial；P2/P4 官方 XA/binlog/GTID、
  崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-stage-instrument-lifecycle-current-continuation1554.txt`。

### Continuation 1555

- [x] 为 statement runtime event 保存 `StageTimed`，分离 statement 与
  `stage/sql/execute` 的计时状态。
- [x] stage summary 使用独立 stage timer；`TIMED=NO` 时保留 count、计时归零，重新开启
  TIMED 不给关闭期间事件补回计时。
- [x] 通过 stage-timer 聚焦测试、相关 stage/statement/setup 测试（16.678s）、完整
  `TestPerformanceSchema*`（48.466s）、engine 全包（383.572s）和全仓串行回归；全仓
  engine 383.004s，integration 0.956s，manager 12.087s，net 8.524s，metrics 0.530s，
  protocol 0.757s，replication 5.921s。
- [ ] P1 完整 I_S/P_S 表覆盖、所有 instrument TIMED/生命周期、组件/插件生命周期、运行时
  统计、锁/等待/线程字段精度和权限生命周期仍为 partial；P2/P4 官方 XA/binlog/GTID、
  崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-stage-instrument-timed-current-continuation1555.txt`。

### Continuation 1556

- [x] 为支持的 `REPLACE` 语句族注册 `statement/sql/replace` setup instrument，默认
  `ENABLED=YES`、`TIMED=YES`，并接入 setup 表投影。
- [x] 增加真实执行回归：REPLACE 会进入 statement summary；关闭 instrument 后不再
  增加新的 summary 计数。
- [x] 通过定向测试、完整 `TestPerformanceSchema*`（55.680s）、engine 全包（393.672s）
  和全仓串行回归；全仓 engine 386.430s，integration 0.946s，manager 11.696s，net
  8.665s，metrics 0.524s，protocol 0.774s，replication 5.890s。
- [ ] P1 完整 I_S/P_S instrument 家族、运行时/组件/权限生命周期仍为 partial；P2/P4
  多通道复制、官方 XA/binlog/GTID、崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-replace-instrument-current-continuation1556.txt`。

### Continuation 1557

- [x] 在 statement event 保存事件产生时的 `Timed` 快照，history 使用该快照生成
  `TIMER_START/TIMER_END/TIMER_WAIT`。
- [x] 增加双向生命周期回归：关闭 TIMED 不改写已有 timed 事件，重新开启 TIMED 不给
  已有 untimed 事件补回计时。
- [x] 修复 `server/net` 测试 MockSession 属性 map 的并发读写，并通过 server/net
  回归；完整 P_S 49.063s，engine 383.692s，全仓 engine 445.903s、integration
  0.898s、manager 13.872s、net 8.921s、metrics 0.510s、protocol 0.759s、
  replication 9.602s。
- [ ] P1 完整 I_S/P_S instrument、stage history、组件/权限生命周期仍为 partial；
  P2/P4 多通道复制、官方 XA/binlog/GTID、崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-statement-history-timed-snapshot-current-continuation1557.txt`。

### Continuation 1558

- [x] stage history 使用事件产生时的 `StageTimed` 快照，不再读取当前 setup instrument
  的 TIMED 值来反向改写既有 `TIMER_START/TIMER_END/TIMER_WAIT`。
- [x] stage instrument 关闭期间不回填历史事件；已捕获的 stage history 在后续关闭
  instrument 后仍保留。
- [x] 通过 stage history 生命周期专项测试、完整 `TestPerformanceSchema*`（57.913s）
  和 engine 全包（444.247s）。
- [ ] 完整 I_S/P_S 表与组件/权限语义、P2/P4 多通道复制、官方 XA/binlog/GTID、
  崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-stage-history-snapshot-current-continuation1558.txt`。

### Continuation 1559

- [x] statement history 使用事件产生时的 `Instrumented` 快照，不再读取当前
  `statement/sql/*` setup instrument 的 ENABLED 值。
- [x] 增加双向生命周期回归：关闭期间不回填，后续关闭 instrument 不隐藏已有 history。
- [x] 通过 statement history 专项测试、完整 `TestPerformanceSchema*`（59.811s）和
  engine 全包（447.555s）。
- [ ] 完整 I_S/P_S 表、字段、组件/权限语义，P2/P4 多通道复制及官方 XA/binlog/GTID、
  崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-statement-history-instrument-snapshot-current-continuation1559.txt`。

### Continuation 1560

- [x] `setup_actors.ENABLED` 现在同时约束 statement 与 `stage/sql/execute` instrumentation。
- [x] 增加 actor-disabled stage summary/history 回归，确认禁用 actor 不产生 stage 观测。
- [x] 通过 actor stage 专项测试、完整 `TestPerformanceSchema*`（58.795s）和 engine 全包
  （445.943s）。
- [ ] 完整 I_S/P_S 表、字段、组件/权限语义，P2/P4 多通道复制及官方 XA/binlog/GTID、
  崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-setup-actors-stage-current-continuation1560.txt`。

### Continuation 1561

- [x] record-lock 与 metadata-lock wait edge 保存事件完成时的 `Instrumented/TIMED` 快照。
- [x] history/history_long 查询使用事件时快照，current wait 继续使用当前 setup 配置。
- [x] 通过两类 wait history 专项测试、manager 全包（14.324s）、完整
  `TestPerformanceSchema*`（61.000s）和 engine 全包（453.842s）。
- [ ] 完整 I_S/P_S 表、字段、组件/权限语义，剩余 wait 类别，P2/P4 多通道复制及官方
  XA/binlog/GTID、崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-wait-history-instrument-snapshot-current-continuation1561.txt`。
