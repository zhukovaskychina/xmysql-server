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
- 当前执行顺序固定为：先完成 P0（启动、核心 CRUD、集群、Connector/J），再收口 P2 本地复制/XA/binlog/GTID/恢复；P1、P3、P4 后续依次推进。
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

## Scope reconciliation (2026-09-29)

- Full INFORMATION_SCHEMA/PERFORMANCE_SCHEMA remains a global P1 deliverable. Existing table
  shapes, selected runtime paths, and permission slices are evidence-backed partial progress,
  not completion of all MySQL 8.4 runtime/component semantics.
- The complete non-Connector/J client matrix remains a global P3 deliverable. The continuation1493
  evidence marks the defined smoke/cluster endpoint slice implemented: MySQL CLI through the Docker
  MySQL 8.4.11 client image, Go mysql driver, PyMySQL, and Node mysql2 passed against both source
  and promoted endpoints; the full edge matrix remains partial.
- XA/native binlog/GTID/replication/crash/promotion interoperability remains global P2/P4 work;
  local state-machine and official-source read evidence do not close reverse, bidirectional,
  duplicate-delivery, crash-window, or promotion acceptance gates.
- FULLTEXT remains deferred. MyISAM, ARCHIVE, CSV, non-InnoDB REPAIR TABLE, and engine
  conversion remain explicitly out of scope.

## Scope reconciliation (latest continuation 1718)

- The authoritative matrix is `reports/compatibility/scope-matrix-current-continuation1705.json`:
  29 total entries, 18 implemented, 8 partial, 1 deferred, and 2 out of scope.
- Continuation 1705 makes `performance_schema.variables_info.VARIABLE_SOURCE` reflect the
  authoritative global-variable manager: definitions start as `COMPILED`, `SET GLOBAL` changes
  report `GLOBAL`, and persisted metadata continues to override the row as `PERSISTED`. The new
  regression first reproduced the old incorrect `COMPILED` result, then the focused variable/
  persistence gate and full Performance Schema targeted family passed (705.749 seconds).
  Evidence: `reports/compatibility/p1-variables-info-global-source-current-continuation1705.txt`.
- Continuation 1706 makes `performance_schema.setup_instruments.FLAGS` return SQL `NULL` when
  no authoritative runtime flag exists, matching the nullable `SET('controlled')` contract;
  it keeps the empty `PROPERTIES` SET and absent `DOCUMENTATION` as-is. The regression first
  reproduced the old empty-string value, then setup/variables gates and the full Performance
  Schema targeted family passed (60.200 seconds). Evidence:
  `reports/compatibility/p1-performance-schema-setup-instruments-null-flags-current-continuation1706.txt`.
- Continuation 1707 removes fabricated Performance Schema replication rows when no replica
  channel is configured; the full Performance Schema targeted family passed (60.963 seconds).
  Evidence: `reports/compatibility/p1-performance-schema-replication-empty-runtime-current-continuation1707.txt`.
- Continuation 1708 maps the authoritative `PSI_FLAG_USER` registration for
  `wait/io/socket/sql/client_connection` to `setup_instruments.PROPERTIES='user'`; the full
  Performance Schema targeted family passed (61.417 seconds). Evidence:
  `reports/compatibility/p1-performance-schema-client-socket-property-current-continuation1708.txt`.
- Continuation 1709 maps the authoritative memory registration for
  `memory/sql/THD::main_mem_root` to `PROPERTIES='controlled_by_default'`,
  `FLAGS='controlled'`, `VOLATILITY=0`, and its documentation; the full Performance Schema
  targeted family passed (60.333 seconds). Evidence:
  `reports/compatibility/p1-performance-schema-memory-instrument-metadata-current-continuation1709.txt`.
- Continuation 1710 adds the official `statement/abstract/Query` setup instrument with
  `PROPERTIES='mutable'`, `VOLATILITY=0`, and its official documentation; the focused gate and
  full Performance Schema targeted family passed (786.386 seconds with test temporary storage
  redirected to D:). Evidence:
  `reports/compatibility/p1-performance-schema-abstract-query-instrument-current-continuation1710.txt`.
- Continuation 1711 adds the official `statement/abstract/new_packet` and
  `statement/abstract/relay_log` setup instruments with their documented default
  ENABLED/TIMED state; the focused gate and full Performance Schema targeted family passed
  (785.895 seconds with test temporary storage redirected to D:). Evidence:
  `reports/compatibility/p1-performance-schema-abstract-instrument-registry-current-continuation1711.txt`.
- Continuation 1712 maps the authoritative metadata for those two abstract instruments:
  `PROPERTIES='mutable'`, nullable `FLAGS`, `VOLATILITY=0`, and the official documentation;
  the focused gate and full Performance Schema targeted family passed (780.342 seconds with
  test temporary storage redirected to D:). Evidence:
  `reports/compatibility/p1-performance-schema-abstract-instrument-metadata-current-continuation1712.txt`.
- Continuation 1713 adds the official MySQL 8.4 InnoDB file instrument registry rows
  `innodb_tablespace_open_file`, `innodb_temp_file`, `innodb_arch_file`, and
  `innodb_clone_file`, all with the default `ENABLED=YES,TIMED=YES` state. The focused
  gate and full Performance Schema targeted family passed (750.467 seconds with test
  temporary storage redirected to D:). This closes registry coverage only; file instances,
  I/O lifecycle, component lifecycle, permissions, and the full I_S/P_S runtime contract
  remain partial. Evidence:
  `reports/compatibility/p1-performance-schema-innodb-file-instrument-registry-current-continuation1713.txt`.
- Continuation 1714 adds the six Unix server thread classes exposed by the official MySQL 8.4
  PSI registry: `admin_interface`, `bootstrap`, `compress_gtid_table`, `manager`,
  `parser_service`, and `signal_handler`. The expanded `setup_threads` registry preserves
  `singleton/user` properties and the focused gate plus full Performance Schema family passed
  (1023.278 seconds with test temporary storage redirected to D:). Thread creation/lifecycle,
  component lifecycle, permissions, and the full I_S/P_S runtime contract remain partial.
  Evidence: `reports/compatibility/p1-performance-schema-thread-instrument-registry-current-continuation1714.txt`.
- Continuation 1715 adds the four Windows-conditional MySQL 8.4 thread classes
  `con_named_pipes`, `con_shared_mem`, `con_sockets`, and `shutdown_restart`, selected by
  `runtime.GOOS`. The focused Windows gate and full Performance Schema family passed
  (1139.440 seconds with test temporary storage redirected to D:). Listener, shutdown,
  thread lifecycle, permissions, and the full I_S/P_S runtime contract remain partial.
  Evidence: `reports/compatibility/p1-performance-schema-windows-thread-registry-current-continuation1715.txt`.
- Continuation 1716 changes the no-session `performance_schema.threads` fallback from the
  project-specific `xmysql/FOREGROUND/Sleep` row to the official server-main shape
  `thread/sql/main/BACKGROUND` with `PROCESSLIST_ID=NULL` and
  `PROCESSLIST_COMMAND=NULL`. The focused regression and full Performance Schema family passed
  (711.962 seconds with test temporary storage redirected to D:). Thread lifecycle, runtime
  statistics, permissions, components, and the full I_S/P_S contract remain partial. Evidence:
  `reports/compatibility/p1-performance-schema-main-thread-fallback-current-continuation1716.txt`.
- Continuation 1717 adds runtime UPDATE semantics for the official server-main
  `performance_schema.threads` row (`THREAD_ID=1`): `INSTRUMENTED` and `HISTORY` updates now
  affect one row and are projected by subsequent SELECTs. The focused regression and full
  Performance Schema family passed (741.468 seconds with test temporary storage redirected to
  D:). Background-thread lifecycle, runtime statistics, permissions, components, and the full
  I_S/P_S contract remain partial. Evidence:
  `reports/compatibility/p1-performance-schema-main-thread-update-current-continuation1717.txt`.
- Continuation 1718 projects three additional runtime fields for the server-main
  `performance_schema.threads` fallback: `PROCESSLIST_DB=mysql`, server-uptime-based
  `PROCESSLIST_TIME`, and `RESOURCE_GROUP=SYS_default`, on both executor fallback paths.
  The focused regression and full Performance Schema family passed (734.861 seconds with
  test temporary storage redirected to D:). Real OS-thread identity, main-thread memory,
  lifecycle, permissions, components, and the full I_S/P_S contract remain partial. Evidence:
  `reports/compatibility/p1-performance-schema-main-thread-runtime-fields-current-continuation1718.txt`.
- Continuation 1704 replaces the small handwritten session-variable fallback with the existing
  `SystemVariablesManager` inventory and applies live session overrides. Focused regressions and
  the full Performance Schema targeted family pass (737.921 seconds); the complete MySQL 8.4
  variable catalog and exact dynamic semantics remain partial.
  Evidence: `reports/compatibility/p1-session-variable-manager-inventory-current-continuation1704.txt`.
- Continuation 1703 adds live `character_set_database` and `character_set_server` rows to the
  session variable projection. The focused red/green regression passes; the complete charset/
  collation and system-variable requirement remains partial.
  Evidence: `reports/compatibility/p1-session-character-set-variable-scope-current-continuation1703.txt`.
- Continuation 1702 adds live `transaction_read_only` and `tx_read_only` rows to the session
  variable projection, with `OFF` defaults and current-session overrides. The focused red/green
  regression and the complete `TestInformationSchema|TestPerformanceSchema` engine family pass;
  this closes two variable rows only and does not promote the aggregate P1 I_S/P_S requirement.
  Evidence: `reports/compatibility/p1-session-read-only-variable-aliases-current-continuation1702.txt`.
- Continuation 1701 corrects the official MySQL 8.4 `performance_schema.log_status.REPLICATION`
  JSON shape: it is a JSON array of channel objects, so the no-channel runtime now returns `[]`
  instead of the previous project-specific `{"channels":[]}` object. The focused log-status and
  P_S regression passed, and the evidence report records the deliberate boundary that the runtime
  still has no authoritative local relay-log file/position source; no channel coordinates are
  fabricated. This closes one observable P1 shape slice only, not full replication-channel,
  XA/native-binlog/GTID, or complete P_S lifecycle semantics.
- Continuation 1700 adds a source-backed `performance_schema.log_status.STORAGE_ENGINES`
  projection for InnoDB: `LSN` and `LSN_checkpoint` come from the live redo manager and match
  the MySQL 8.4 JSON contract. This closes one P1 runtime slice only; the complete I_S/P_S
  runtime/component/lifecycle/permission requirement remains partial.
- The global P1 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA requirement remains partial: full table and
  column coverage, runtime/component lifecycle, and complete permission/role visibility are not
  closed.
- Continuation 1663 re-ran the existing one-shot TCP fault fixture for Go mysql-driver, PyMySQL,
  and Node.js/mysql2 after the persistent partition proxy extension; all old-connection-failure
  and reconnect cases passed. This is a regression guard, not completion of the full P3 matrix.
- Continuation 1664 ran the persistent source-network-partition fixture against official MySQL
  8.0.41; ordinary, two-phase XA, and one-phase XA rows all caught up after heal with no duplicates.
  This extends the version slice but does not complete distributed fencing, multi-node promotion,
  or the full P2/P4 topology matrix.
- Continuation 1665 fresh-ran the targeted INFORMATION_SCHEMA/PERFORMANCE_SCHEMA engine tests and
  passed. This is a regression gate for existing slices, not completion of full MySQL 8.4 metadata,
  runtime component, or permission semantics.
- Continuation 1666 binds replication/XA identity and ordinary-client commit keys into the same
  physical `LOG_TYPE_TXN_COMMIT` WAL record before storage commit; manager and engine full-package
  regressions passed. This closes the WAL identity-binding slice, but the aggregate physical
  storage/WAL/native-binlog/GTID/applied-state commit protocol remains partial.
- Continuation 1667 confirms that one-phase and prepared XA commit paths bind the canonical XA XID
  into the same physical `LOG_TYPE_TXN_COMMIT` WAL record; focused identity tests and the full
  engine package regression passed. This closes only the XA-specific WAL identity slice; the
  aggregate physical commit protocol remains partial.
- Continuation 1668 confirms that the durable transaction-journal commit record also uses the
  canonical XA XID after a publication failure, so crash recovery will not republish that XA
  transaction under an ordinary client key. Focused identity tests and the full engine package
  regression passed; the aggregate physical commit protocol remains partial.
- Continuation 1669 confirms that XA DML journal entries use the canonical XA XID before physical
  commit as well, covering the earlier crash window where no commit record exists yet. Focused
  identity tests and the full engine package regression passed; the aggregate physical commit
  protocol remains partial.
- Continuation 1673 closes the `performance_schema.session_account_connect_attrs` account-scope
  slice: the runtime projection reads the live session snapshot, includes sessions for the current
  user/host account, excludes other accounts, and retains current-session-only behavior for
  unauthenticated sessions. The focused regression and complete I_S/P_S targeted family passed;
  the aggregate P1 requirement remains partial because full table/column, optional-component
  lifecycle, and permission semantics are still open.
- Continuation 1674 closes the implicit `USAGE` row slice of
  `information_schema.user_privileges`: an account with no explicit global privilege now
  produces the official default row, while explicitly privileged accounts retain their actual
  privilege rows. The official MySQL 8.4.11 Docker probe, red/green regression, and complete
  I_S/P_S targeted family passed; full privilege/role and component semantics remain partial.
- Continuation 1675 passes the defined local non-Connector/J client matrix for Docker
  `mysql:8.4.11`, Go mysql-driver, PyMySQL, and Node.js mysql2 on an isolated xmysql-server
  instance. The matrix covers the current SQL, metadata, prepared-statement, transaction,
  savepoint, pool, error, wire-type, and reconnect cases. The aggregate P3 requirement remains
  partial because broader client versions, ORM/pool variants, TLS/auth-plugin permutations,
  negative protocol cases, and cluster endpoint/failover coverage are still open.
- Continuation 1676 makes named replication channels inherit the parent runtime's
  `NativeEndpoint`, `Peers`, `AutoFailover`, `FailureTimeout`, and `PollInterval` while keeping
  source URLs and native source instances channel-local. The red/green channel regression and
  full replication package regression pass; this closes only the channel failover-configuration
  inheritance slice, so the aggregate P2/P4 requirement remains partial.
- Continuation 1677 closes one replication applied/GTID marker recovery window: after a durable
  storage commit and commit record, recovery retries the marker for `replication-*` journals and
  removes the active journal only after marker success. Marker failure remains visible, and no
  upstream publisher is invoked for local replay recovery. The focused and replication/client
  recovery regressions pass; the aggregate physical commit and official-MySQL matrix remains
  partial.
- Continuation 1678 closes one P1 role-visibility slice: role privilege views now use persisted
  default roles only when a synthetic session omits `active_roles`; an explicit empty active-role
  set remains authoritative. The role regression set and complete I_S/P_S targeted family pass;
  full MySQL 8.4 metadata/component/permission semantics remain partial.
- Continuation 1679 extends that role slice to `INFORMATION_SCHEMA.ENABLED_ROLES`: an omitted
  `active_roles` parameter now projects persisted default roles and mandatory roles, while an
  explicit empty set remains authoritative. The focused role/P1 regression passes; the global P1
  requirement remains partial.
- Continuation 1680 closes one local native-promotion relay slice: when native decoding provides
  an aggregated committed event, the replica now persists synthetic BEGIN/ROW relay events so a
  promoted source regenerates a complete GTID/BEGIN/ROW/XID transaction instead of only an
  XID_EVENT. Valid MySQL UUID identity is preserved, and the focused promotion plus full
  replication regressions pass. P2/P4 remain partial for unified physical commit atomicity,
  crash-kill/topology coverage, and complete official-MySQL bidirectional interoperability.
- Continuation 1681 closes one local native-materialization retry slice: if logical relay import is
  durable but native append fails, a same-process retry now detects the logical/native mismatch and
  rebuilds the physical native stream. The focused regression, full replication package, and engine
  replication/XA/source regressions pass; the aggregate P2/P4 requirement remains partial.
- Continuation 1682 closes one local XA PREPARE retry slice: a durable matching logical
  `XA_PREPARE` is detected by GTID/XID/key after native append failure, the native stream is rebuilt,
  and a retry does not duplicate the prepare before XA COMMIT. Focused, replication-package, and
  engine replication/XA/source regressions pass; the aggregate P2/P4 requirement remains partial.
- Continuation 1683 closes one local partial-batch relay retry slice: if logical `BEGIN/ROW` are
  durable but native append fails and a later batch supplies only `COMMIT`, `ImportEvents` now
  validates the complete logical/native projection and rebuilds the complete native transaction
  instead of appending an orphan terminal event. Focused, replication-package, and engine
  replication/XA/source regressions pass; the aggregate P2/P4 requirement remains partial.
- Continuation 1684 closes one local native GTID identity-integrity slice: the logical/native
  projection check now includes the actual ordinary/XA/tagged GTID frame layout and compares each
  physical SID, tag, and sequence with the logical identity. A checksum-valid GTID mutation is
  repaired from the durable logical stream. Focused, replication-package, and engine
  replication/XA/source regressions pass; the aggregate P2/P4 requirement remains partial.
- Continuation 1685 closes one local native payload-integrity slice: the logical/native projection
  check regenerates deterministic physical bodies and compares TABLE_MAP/ROWS/QUERY/XA payloads,
  while ignoring physical header positions and checksums. A checksum-valid ROWS_EVENT mutation is
  repaired from the durable logical stream. Focused, replication-package, and engine
  replication/XA/source regressions pass; the aggregate P2/P4 requirement remains partial.
- Fresh full serial regression after continuation 1685 passes with
  `go test -p 1 ./... -count=1 -timeout 90m`; evidence is
  `reports/compatibility/full-go-regression-current-continuation1685.txt`.
- Continuation 1686 closes one local native physical-header integrity slice:
  timestamp/server-id/flags and `log_pos == EndPosition` are checked in addition
  to type, deterministic body, checksum, and GTID identity. A checksum-valid
  server-id mutation is repaired from the durable logical stream. Focused,
  replication-package, and engine replication/XA/source regressions pass; the
  aggregate P2/P4 requirement remains partial.
- Fresh full serial regression after continuation 1686 also passes with
  `go test -p 1 ./... -count=1 -timeout 90m`; evidence is
  `reports/compatibility/full-go-regression-current-continuation1686.txt`.
- Continuation 1687 closes one local first-file native header payload slice:
  deterministic FORMAT_DESCRIPTION_EVENT/PREVIOUS_GTIDS_EVENT bodies,
  server-id, flags, and `log_pos` are validated while creation timestamps remain
  physical metadata. A checksum-valid PREVIOUS_GTIDS body mutation is repaired
  from the durable logical stream. Focused, replication-package, and engine
  replication/XA/source regressions pass; the aggregate P2/P4 requirement
  remains partial.
- Fresh full serial regression after continuation 1687 passes with
  `go test -p 1 ./... -count=1 -timeout 90m`; evidence is
  `reports/compatibility/full-go-regression-current-continuation1687.txt`.
- Continuation 1688 closes one local rotated native-header integrity slice:
  executed GTIDs are tracked across ROTATE boundaries and each retained file's
  deterministic FDE/PREVIOUS_GTIDS body, server-id, flags, and `log_pos` are
  checked. Semantic frame assertions avoid confusing regenerated timestamps
  with corruption. Focused, replication-package, engine replication/XA, and
  full serial regressions pass; the aggregate P2/P4 requirement remains
  partial.
- Continuation 1689 closes one local rotated native-event integrity slice:
  checksum-valid `ROTATE_EVENT` target-file mutations are detected by rebuilding
  the expected next filename at each logical rotation boundary. The same
  deterministic body builder is used by normal append and recovery rebuild;
  focused, replication-package, and engine replication/XA regressions pass.
  The aggregate P2/P4 requirement remains partial.
- Continuation 1690 closes one local native-frame short-write slice: native
  file initialization, ROTATE_EVENT, and transaction frame writes now reject
  short writes with `io.ErrShortWrite`, making partial physical writes visible
  to the existing recovery/rebuild path. Focused, replication-package, and
  engine replication/XA regressions pass; the aggregate P2/P4 requirement
  remains partial.
- Continuation 1691 closes one local pending-ROTATE retry slice: when the
  logical ROTATE is durable but native ROTATE_EVENT append fails, the next
  `Rotate()` repairs the native projection and returns the original event
  without appending a second logical boundary. Focused, rotation/restart/dump,
  replication-package, and engine replication/XA regressions pass; the
  aggregate P2/P4 requirement remains partial.
- Continuation 1692 closes one local ROTATE/index publication retry slice: when
  the logical ROTATE, native ROTATE_EVENT, and next native file are durable but
  binlog.index replacement fails, the next `Rotate()` repairs the durable index
  and returns the original event without appending a second logical boundary or
  creating a third native file. Focused, replication-package, engine
  replication/XA, and full serial regressions pass; the aggregate P2/P4
  requirement remains partial.
- Continuation 1693 closes one local relay/import GTID-index publication retry
  slice: when logical relay and native files are durable but binlog.gtid.index
  replacement fails, the next `ImportEvents()` rebuilds and persists the GTID
  index before acknowledging the idempotent retry. Focused, replication-package,
  engine replication/XA, and full serial regressions pass; the aggregate P2/P4
  requirement remains partial.
- Continuation 1694 closes one local promotion/imported-relay native-dump GTID
  identity slice: when a promoted source retains an upstream wire SID while its
  local source UUID changes, `NativeDumpFileWithIntervals` records the canonical
  UUID represented by that SID in the caller's executed intervals. Focused,
  replication-package, engine replication/XA, and full serial regressions pass;
  the aggregate P2/P4 requirement remains partial.
- Continuation 1695 closes one local native-dump partial-position slice: a GTID
  observed before a requested mid-transaction position is no longer recorded
  as executed until a complete XID or XA terminal event is reached. Focused,
  net binlog/GTID, replication-package, engine replication/XA, and full serial
  regressions pass; the aggregate P2/P4 requirement remains partial.
- Continuation 1696 closes one local native-dump XA terminal mid-position slice:
  when a terminal query is deliberately included after its GTID_EVENT, the
  terminal GTID is recorded at that completion boundary while ordinary skipped
  partial transactions remain unrecorded. Focused, net binlog/GTID,
  replication-package, engine replication/XA, and full serial regressions pass;
  the aggregate P2/P4 requirement remains partial.

- Continuation 1697 closes one same-process replication marker retry slice:
  when physical WAL commit has completed but applied/GTID marker publication
  fails transiently, retrying the same committed context now finishes the
  journal/marker boundary without calling the already-completed storage commit
  again. Focused, related engine, replication, net, and engine replication/XA
  tests pass; the P2/P4 aggregate remains partial pending the unified commit
  protocol and full crash-kill, topology, and official-MySQL matrices.
  Evidence:
  `reports/compatibility/p2-replication-committed-context-marker-retry-current-continuation1697.txt`.
- Continuation 1698 closes one P1 SQL EVENT/Performance Schema publication race:
  INSERT/UPDATE/DELETE results from an event can notify the event runtime before
  delayed executeQuery metrics cleanup, so `events_statements_summary_by_program`
  becomes observable with the committed event effect. Event lifecycle and
  Performance Schema program/setup/component regressions pass; the full P1
  INFORMATION_SCHEMA/PERFORMANCE_SCHEMA aggregate remains partial.
  Evidence:
  `reports/compatibility/p1-event-program-summary-publication-current-continuation1698.txt`.
- The global P3 non-Connector/J requirement remains partial: the server fast-auth slice and the
  Go/PyMySQL/mysql CLI/Node.js mysql2 authentication-plugin probes pass; a historical Node timeout
  was not reproduced in two independent fresh-process reruns. Broader client/version/ORM/network/
  failover coverage remains open. A fresh process-level old-connection/restart/reconnect gate also
  passes for Go, PyMySQL, and Node.js/mysql2.
- The global P2/P4 XA/native binlog/GTID/replication/crash-recovery requirement remains partial:
  local durable-marker and recovery slices pass, but the single physical commit protocol, full
  crash-kill/topology matrix, and complete official-MySQL bidirectional fixture remain open. Fresh
  official 8.4.11 reverse-promotion and source crash/reconnect sub-gates pass, without closing the
  aggregate requirement.
- FULLTEXT remains deferred. MyISAM, ARCHIVE, CSV, non-InnoDB REPAIR TABLE, and engine conversion
  remain explicitly out of scope.

### Continuation 1652

- [x] Add a post-rename directory durability barrier to the shared atomic
  replication-state writer: Unix directory `Sync()` and Windows directory
  `FlushFileBuffers()`.
- [x] Add fault-injection coverage for directory-sync invocation and error
  propagation; verify native Windows compilation.
- [ ] Keep P2/P4 partial until storage/WAL/native-binlog/applied-marker commit
  atomicity, crash-kill/topology coverage, and complete official bidirectional
  XA/binlog/GTID fixtures are closed.

### Continuation 1653

- [x] Run the fresh serial repository regression after the replication directory
  durability change: `go test -p 1 ./... -count=1 -timeout 90m`.
- [x] Record package-level PASS evidence for engine, manager, net, replication,
  and the remaining Go packages.
- [ ] Keep the aggregate compatibility items open; a Go regression cannot prove
  official-MySQL interoperability, full client-version/network coverage,
  component runtime semantics, or distributed topology behavior.

### Continuation 1654

- [x] Add native partial-batch transport-reset recovery coverage: persist the
  relay boundary before apply, reconnect with the remaining frame, and assert
  exactly-once row application plus durable GTID/source position advancement.
- [x] Verify the focused retry tests and the complete `server/replication`
  package.
- [ ] Keep P2/P4 partial until the complete network-partition, topology, and
  official bidirectional XA/binlog/GTID gates are closed.

### Continuation 1655

- [x] Add a real-process TCP fault proxy that closes active connections while
  xmysql remains running, then verify Go mysql-driver, PyMySQL, and Node.js/mysql2
  observe old-connection failure and recover through a new connection.
- [ ] Keep P3 partial until more client versions, ORM/pool variants, negative
  protocol cases, and cluster-client topologies are covered.

### Continuation 1656

- [x] Run the defined MySQL CLI matrix against Docker clients `mysql:5.7.44`
  and `mysql:8.0.41`; both passed all 16 defined cases, extending the existing
  `mysql:8.4.11` CLI evidence to three major-version lines.
- [ ] Keep P3 partial until programmatic-client versions, ORM/pool variants,
  protected credential gates, negative protocol cases, and cluster-client
  topologies are covered.

### Continuation 1657

- [x] Freshly run the official MySQL 8.4.11 reverse-promotion fixture: ordinary,
  two-phase XA, and one-phase XA apply from official source to xmysql; restart
  remains duplicate-free; after xmysql promotion, all three transaction forms
  apply to the official target.
- [ ] Keep P2/P4 partial until complete crash-kill, network-partition, version,
  and bidirectional GTID/XA topology gates are closed.

### Continuation 1658

- [x] Freshly run the official MySQL 8.4.11 source crash/reconnect fixture;
  recovery restored pre-crash and crash-window ordinary/XA transactions,
  source reconnect applied later ordinary/XA transactions, and GTID progress
  remained continuous without duplicate rows.
- [ ] Keep P2/P4 partial until complete crash-kill, network-partition, version,
  and bidirectional GTID/XA topology gates are closed.

### Continuation 1659

- [x] Freshly run the official MySQL 8.0.41 reverse-promotion fixture after
  making its binlog-status query version-aware (`SHOW MASTER STATUS` for the
  official 8.0 source, native `SHOW BINARY LOG STATUS` for xmysql). Ordinary,
  two-phase XA, and one-phase XA apply in both directions, with duplicate-free
  xmysql restart, all passed.
- [ ] Keep P2/P4 partial until more official versions, complete crash-kill and
  network-partition topology gates, and bidirectional GTID/XA coverage close.

### Continuation 1660

- [x] Freshly run the official MySQL 8.0.41 source crash/reconnect fixture:
  xmysql recovered ordinary, two-phase XA, and one-phase XA transactions after
  forced termination, then resumed ordinary/XA delivery after the official
  source restarted; GTID progress was continuous and duplicate-free.
- [ ] Keep P2/P4 partial until more official versions, network-partition/fencing
  gates, and bidirectional GTID/XA topology coverage close.

### Continuation 1661

- [x] Re-run the official MySQL 8.4.11 reverse-promotion fixture after the
  MySQL 8.0 status-query compatibility change; ordinary, two-phase XA,
  one-phase XA, restart deduplication, and post-promotion delivery all passed.
- [ ] Keep P2/P4 partial until network-partition/fencing and complete
  bidirectional GTID/XA topology coverage close.

### Continuation 1662

- [x] Add and run a real-process persistent network-partition fixture: the
  official MySQL 8.4.11 source stays online, the proxy closes active native
  replication connections and rejects new ones, source writes continue during
  the partition, and heal resumes all ordinary/XA delivery exactly once.
- [ ] Keep P2/P4 partial until distributed fencing/consensus, multi-node
  partition promotion, and the complete version/topology matrix close.

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

### Continuation 1562

- [x] completed record-lock 与 metadata-lock wait 的 table summary 使用事件时
  `Instrumented/TIMED` 快照。
- [x] 增加 `table_lock_waits_summary_by_table` 回归：后续 `TIMED=NO` 保留原计时，
  后续 `ENABLED=NO` 保留已完成汇总。
- [x] 通过专项测试（2.388s）、manager 全包（14.780s）、完整
  `TestPerformanceSchema*`（58.553s）和 engine 全包（685.693s）。
- [ ] 完整 I_S/P_S 表、字段、组件/权限语义，剩余 wait 类别，P2/P4 多通道复制及官方
  XA/binlog/GTID、崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-wait-summary-instrument-snapshot-current-continuation1562.txt`。

### Continuation 1563

- [x] 为已支持的 DDL、SHOW、SET、事务、授权和 XA 语句增加 MySQL 风格的
  canonical `statement/sql/*` instrument 名称，并保持既有 DML instrument 名称稳定。
- [x] `setup_instruments` 暴露 canonical statement 行，按行更新 `ENABLED/TIMED` 可控制
  对应语句运行时采集。
- [x] 通过 canonical mapping/setup/DDL 生命周期专项测试（5.176s）、完整 `TestPerformanceSchema*`
  （60.666s）和 engine 全包（430.503s）。
- [ ] 完整 I_S/P_S 表、字段、组件/权限语义，剩余 instrument/stage/wait 类别，P2/P4
  多通道复制及官方 XA/binlog/GTID、崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-canonical-statement-instruments-current-continuation1563.txt`。

### Continuation 1564

- [x] 为已有执行路径补齐 MySQL 风格 statement instrument：存储对象、视图、SQL
  PREPARE/EXECUTE/DEALLOCATE、CALL、RENAME TABLE、LOCK/UNLOCK TABLES、FLUSH/KILL、表维护、
  复制控制、SHOW 扩展以及用户/角色语句；`INSERT ... SELECT`/`REPLACE ... SELECT` 使用
  独立的 `insert_select`/`replace_select` 名称。
- [x] 将新增 canonical 名称加入 `setup_instruments`，并复用现有 `ENABLED/TIMED` runtime gate。
- [x] 通过扩展 mapping/setup 回归、完整 `TestPerformanceSchema`（75.181s）和 engine 全包
  （446.339s）。
- [ ] 没有对应 xmysql runtime 的 Clone、Firewall、Keyring、NDB、Enterprise scheduler 等
  组件表不以合成数据宣称完成；完整 I_S/P_S 语义、剩余 instrument/stage/wait 类别、P2/P4
  官方 XA/binlog/GTID、崩溃恢复、重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续 out of scope。

Evidence: `reports/compatibility/p1-performance-schema-expanded-statement-instruments-current-continuation1564.txt`。

### Continuation 1565

- [x] 补齐 MySQL 8.4 `performance_schema.setup_consumers` 的
  `events_statements_cpu` 行，默认 `ENABLED=NO`。
- [x] 支持该 consumer 的查询和动态 `ENABLED` 更新，并沿用 setup 表的权限校验。
- [x] 明确 CPU consumer 开关与 CPU 指标实现边界：当前未采集 CPU 时间，不能把
  `CPU_TIME` 的完整运行时语义宣称为已实现。
- [x] 通过 consumer 专项回归（2.126s）和完整 `TestPerformanceSchema`（73.137s）。
- [ ] 完整 I_S/P_S 表、字段、组件/权限语义，P2/P4 官方 XA/binlog/GTID、崩溃恢复、
  重复投递和晋升互操作仍未关闭。
- [x] FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续
  out of scope。

Evidence: `reports/compatibility/p1-performance-schema-statement-cpu-consumer-current-continuation1565.txt`。

### Continuation 1566

- [x] 通过官方 MySQL 8.4.11 反向复制 fixture：普通事务和 XA 事务从官方源同步到
  xmysql，运行时 Promote 后 xmysql 产生的 native GTID/BEGIN/TABLE_MAP/ROWS/XID
  事件被官方目标完整应用。
- [x] 修复该门禁暴露的权限传播、Promote 后动态 source 生命周期、运行时复制 hooks、
  官方 QUERY_EVENT(BEGIN)、GTID 元数据和 VARCHAR TABLE_MAP 字节宽度，并通过
  `server/net` binlog 回归与 `server/replication` native 回归。
- [ ] P2/P4 聚合项继续保持 partial：崩溃窗口恢复、重复投递幂等、官方双向 XA 和
  更广泛 topology/failover 仍需独立门禁。
- [ ] 完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 与非 Connector/J 客户端矩阵仍是
  全局任务；FULLTEXT deferred，非 InnoDB 引擎、非 InnoDB REPAIR/转换 out of scope。

Evidence: `reports/compatibility/p2-p4-official-reverse-replication-current-continuation1566.txt`。

### Continuation 1567

- [x] 外部进程级 crash/recovery 矩阵连续 3 次通过：已提交数据保留、未提交数据回滚、
  半提交最多应用一次、DDL/索引元数据重启后可用。
- [ ] 官方 MySQL 重启后的重复投递、native GTID/applied-state 崩溃窗口、双向 XA 和
  更广泛 promotion/failover 拓扑仍未关闭，因此 P2/P4 聚合项保持 partial。

Evidence: `reports/compatibility/p2-external-process-crash-recovery-current-continuation1567.txt`。

### Continuation 1568

- [x] 官方 MySQL 8.4.11 反向复制 fixture 增加 xmysql 进程重启：普通事务和 XA
  事务重启后保持且未重复，随后 Promote 成 source，官方目标成功应用 Promote 后行。
- [ ] 官方双向 XA、native GTID/applied-state crash-kill、任意重复投递和更广泛拓扑仍未
  关闭，P2/P4 聚合项保持 partial。

Evidence: `reports/compatibility/p2-p4-official-reverse-restart-promotion-current-continuation1568.txt`。

### Continuation 1569

- [x] 实现 `events_statements_cpu` 的 Windows/Linux OS-thread CPU 时间采集，并沿
  两条执行路径传递到 Performance Schema statement event。
- [x] `events_statements_history*` 在 consumer 开启时返回 `CPU_TIME`，关闭时返回
  `NULL`；statement summary/digest 返回实际 `SUM_CPU_TIME`。
- [x] 通过 CPU_TIME 专项、metrics 包、完整 `TestPerformanceSchema`、engine 全包和 Linux 目标编译。
- [ ] 该切片不关闭完整 I_S/P_S 组件、权限、instrument/stage/wait/thread/runtime 语义，
  也不关闭 P2/P4 官方双向 XA、crash-kill/applied-state、重复投递和拓扑故障切换。

Evidence: `reports/compatibility/p1-performance-schema-statement-cpu-time-current-continuation1569.txt`。

### Continuation 1570

- [x] SQL `PREPARE/EXECUTE` 将嵌套执行上下文的 CPU 时间累计到
  `prepared_statements_instances.SUM_CPU_TIME`。
- [x] prepared statement 专项回归通过。
- [ ] 协议 `COM_STMT_EXECUTE` CPU 传播和完整 P_S prepared-statement 语义仍需独立门禁。

Evidence: `reports/compatibility/p1-performance-schema-prepared-cpu-time-current-continuation1570.txt`。

### Continuation 1571

- [x] 协议 `COM_STMT_EXECUTE` 在 `events_statements_cpu` 开启时采集 OS-thread CPU 时间。
- [x] 协议 `PreparedStatementManager` 保留 `CPUTimeTotal`，并使
  `prepared_statements_instances.SUM_CPU_TIME` 覆盖 Connector/J 风格二进制预处理语句。
- [x] 通过协议级专项和 net/protocol/compatibility 回归。
- [ ] P_S 其他组件、权限、stage/wait、非 CPU prepared-statement 语义仍需继续收口。

Evidence: `reports/compatibility/p1-performance-schema-com-stmt-cpu-time-current-continuation1571.txt`。

### Continuation 1572

- [x] `events_statements_summary_by_program.SUM_CPU_TIME` 接入真实 CPU 聚合，
  覆盖存储过程子语句、存储函数、EVENT 和 TRIGGER。
- [x] 修复普通 `executeQuery` statement summary 未传递 CPU 字段的问题，并通过
  存储程序专项和完整 `TestPerformanceSchema` 回归。
- [ ] P_S 其他组件、权限、stage/wait、非 CPU prepared-statement 语义仍需继续收口；
  P2/P4 官方 XA/binlog/GTID、崩溃恢复和重复投递仍保持 partial。

Evidence: `reports/compatibility/p1-performance-schema-program-cpu-time-current-continuation1572.txt`。

### Continuation 1573

- [x] 扩展官方 MySQL 反向复制 fixture：官方源的 XA 事务同步到 xmysql，xmysql
  重启并 Promote 后，官方目标继续成功应用 xmysql 发出的 XA 事务。
- [x] 通过普通事务、官方源 XA、xmysql 重启、Promote、xmysql XA、官方目标
  应用的完整子门禁。
- [ ] native GTID/applied-state crash-kill、任意重复投递和更广泛拓扑/故障切换仍
  未关闭，P2/P4 聚合项保持 partial。

Evidence: `reports/compatibility/p2-p4-official-bidirectional-xa-current-continuation1573.txt`。

### Continuation 1574

- [x] 补齐 replica applied-marker 在最终 state replacement 失败后的同进程重试收敛：
  成功持久化的 relay、native relay、table map、prepared-XA、已应用行和 marker 会
  更新内存重试视图。
- [x] 重试开始前提升持久化 marker 到 Executed，避免同进程或重启后的 native 物理
  binlog 重放再次调用存储回调。
- [x] 新增 native applied-marker 故障窗口回归，并通过完整
  `go test ./server/replication -count=1 -timeout 45m`。
- [ ] 官方 MySQL crash-kill 期间的 native GTID/applied-state 发布、任意重复投递和
  更广泛 topology/failover 仍未关闭，P2/P4 聚合项保持 partial。

Evidence: `reports/compatibility/p2-native-applied-marker-same-process-current-continuation1574.txt`。

### Continuation 1575

- [x] 重新执行官方 MySQL 双向 XA/晋升门禁：官方源普通事务与 XA 事务同步到
  xmysql；xmysql 重启后保留两条记录并完成 Promote；官方目标应用 xmysql 的普通
  事务和 XA 事务。
- [x] 保留官方 fixture 的进程存活检查，避免把 xmysql 重启阶段的进程退出误判为
  “尚未就绪”。
- [ ] 官方 crash-kill 期间的 native GTID/applied-state 发布、任意重复投递和更广泛
  topology/failover 仍未关闭，P2/P4 聚合项保持 partial。

Evidence: `reports/compatibility/p2-p4-official-bidirectional-xa-current-continuation1575.txt`。

### Continuation 1576

- [x] 修复 `performance_schema.table_lock_waits_summary_by_table` 在 reset/filter
  快照行之后收到首条真实等待时的 `MIN_TIMER_*` 初始化错误。
- [x] 通过最小回归和完整 `TestPerformanceSchema` 专项回归。
- [ ] 完整 P_S 组件生命周期、等待分类、权限语义及 I_S/P_S 聚合项仍保持
  `partial`。

Evidence: `reports/compatibility/p1-performance-schema-table-lock-minimum-current-continuation1576.txt`。

### Continuation 1577

- [x] 为全局及按实例的 `events_waits_summary_*` 增加显式首条等待状态，修复
  TRUNCATE/reset 空行与后续真实等待合并时的最小/最大计时初始化风险。
- [x] 通过 `TestPerformanceSchemaWaitSummaryTruncateResetsRows` 及完整
  `TestPerformanceSchema` 专项回归和 `server/innodb/engine` 全包回归。
- [ ] 完整 P_S 组件生命周期、等待分类、权限语义及 I_S/P_S 聚合项仍保持
  `partial`。

Evidence: `reports/compatibility/p1-performance-schema-wait-minimum-current-continuation1577.txt`。

### Continuation 1578

- [x] 修复 account/host/user 维度重新聚合等待统计时 reset 空线程行污染
  `MIN_TIMER_WAIT`/`MAX_TIMER_WAIT` 的问题。
- [x] 通过 account 维度专项、完整 `TestPerformanceSchema` 和
  `server/innodb/engine` 全包回归。
- [ ] 完整 P_S 组件生命周期、等待分类、权限语义及 I_S/P_S 聚合项仍保持
  `partial`。

Evidence: `reports/compatibility/p1-performance-schema-wait-dimension-minimum-current-continuation1578.txt`。

### Continuation 1579

- [x] 实现 `INFORMATION_SCHEMA.COLUMN_PRIVILEGES UNION ALL TABLE_PRIVILEGES` 的
  真实行合并，保留两个权限视图的可见性、过滤和投影语义。
- [x] 通过联合查询专项、权限/角色专项、完整 `TestInformationSchema` 和引擎
  全包回归。
- [ ] 完整权限/角色语义、所有 SQL set-operation 形状以及 I_S/P_S 聚合项仍保持
  `partial`。

Evidence: `reports/compatibility/p1-information-schema-privileges-union-current-continuation1579.txt`。

### Continuation 1580

- [x] 补齐 one-phase XA 在逻辑事件已持久化、native binlog append 失败后的重试
  恢复；重试重建 native 文件且不追加第二条 one-phase 事务。
- [x] 新增故障注入回归，并通过原生/XA/崩溃/晋升专项与完整
  `go test ./server/replication -count=1 -timeout 45m`。
- [ ] P2/P4 完整 native binlog、GTID、crash-kill、promotion 和官方 MySQL
  互操作聚合项仍保持 `partial`。

Evidence: `reports/compatibility/p2-one-phase-xa-native-retry-current-continuation1580.txt`。

### Continuation 1581

- [x] 扩展官方 MySQL 双向 fixture，覆盖官方源和晋升后 xmysql 源的 one-phase XA。
- [x] 通过官方源 -> xmysql、xmysql 重启、Promote、xmysql -> 官方目标的普通、
  两阶段 XA、one-phase XA 完整门禁，结果为 PASS。
- [ ] P2/P4 完整 crash-kill、GTID/位置、拓扑故障切换和全量官方互操作仍保持
  `partial`。

Evidence: `reports/compatibility/p2-p4-official-bidirectional-one-phase-xa-current-continuation1581.txt`。

### Continuation 1582

- [x] 补齐权限视图多分支 `UNION ALL` 以及 `UNION`/`UNION DISTINCT` 去重语义。
- [x] 通过两段/三段权限 UNION 回归、权限/角色专项和完整
  `TestInformationSchema`。
- [ ] 完整权限/角色可见性、全部 SQL set-operation 组合及 I_S/P_S 聚合项仍保持
  `partial`。

Evidence: `reports/compatibility/p1-information-schema-privileges-union-distinct-current-continuation1582.txt`。

### Continuation 1583

- [x] 连续运行 3 次外部进程崩溃恢复矩阵，验证 redo/undo、半提交至多一次、
  DDL 元数据和索引重建元数据。
- [x] 三次运行全部 PASS。
- [ ] P2/P4 native GTID/applied-state、官方 MySQL crash、promotion 和拓扑矩阵仍
  保持 `partial`。

Evidence: `reports/compatibility/p2-external-process-crash-recovery-current-continuation1583.txt`。

### Continuation 1584

- [x] 为权限视图补齐 `INTERSECT`/`EXCEPT`，并保留每个分支的账户可见性、
  过滤、投影与重复行语义。
- [x] 通过受限账户集合运算回归、权限/角色专项、完整
  `TestInformationSchema` 和 engine 包全量回归。
- [ ] 完整权限/角色可见性、全部 SQL set-operation 组合及 I_S/P_S 聚合项仍保持
  `partial`。

Evidence: `reports/compatibility/p1-information-schema-privileges-intersect-except-current-continuation1584.txt`。

### P3 全量客户端矩阵边界澄清

- [x] 保留现有 mysql CLI、Go、PyMySQL、Node.js 以及 source/promoted endpoint
  的定义 smoke gate 证据。
- [ ] 将非 Connector/J 客户端的负向、重连、协议边界、类型/字符集、事务/预处理、
  元数据和集群故障场景扩展为全量兼容矩阵；该聚合项保持 `partial`，实现顺序晚于
  P1/P2/P4 主线。

Evidence: `reports/compatibility/global-scope-reconciliation-current-continuation1538.txt`；
`reports/compatibility/client-matrix-current-continuation1493/client-matrix-20260927-133157.json`。

### Continuation 1586

- [x] 修复解耦处理器只在 `CLIENT_CONNECT_WITH_DB` 协商时读取默认库，避免无默认
  库客户端把 `mysql_native_password` 误存为 session database。
- [x] 新增 no-default/empty-default handshake 回归，并通过 `server/net`、
  `server/protocol` 和 `server/replication` 专项回归。
- [x] 官方 MySQL -> xmysql 的进程崩溃重连子门禁已闭环：native recovery 在持久化
  文件/位置可用时优先从精确边界恢复，避免从空文件名 GTID dump 起点反复重放旧
  metadata；停机期间产生的普通事务行 3 和 one-phase XA 行 4 均在重启后追平，且
  `@@GLOBAL.gtid_executed` 返回运行时 applied GTID 集。
- [ ] P2/P4 聚合项仍未闭环：双向 XA/binlog/GTID、提升/切换、重复投递以及存储/WAL/
  applied-state 的其他崩溃边界仍需独立门禁。

Evidence: `reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1586.txt`。

### Continuation 1591

- [x] 修正 Go mysql-driver 客户端矩阵的 reconnect 检查：关闭旧连接并重新打开
  `database/sql` 连接后执行 `Ping` 和 `SELECT 1`，不再把复用连接的 Ping 当作物理
  重连证据。
- [x] 在隔离 xmysql 进程上重跑官方 MySQL CLI、Go mysql driver、PyMySQL、Node
  mysql2 四类客户端，DDL/DML、预处理、事务、类型/字符集、元数据、savepoint、
  多结果/错误和物理重连全部 PASS。
- [ ] P3 全量客户端聚合仍未闭环：负向/协议、TLS/认证插件、更多类型/字符集、ORM
  以及集群故障恢复矩阵仍需独立门禁。

Evidence: `reports/compatibility/p3-non-connector-client-matrix-current-continuation1591.txt`。

### Continuation 1592

- [x] 扩展官方 MySQL -> xmysql crash/reconnect fixture，覆盖 xmysql 停机期间提交的
  普通事务、one-phase XA 和两阶段 XA；三类事务均在重启后恢复，源端 GTID `...:1-14`
  追平到 xmysql `...:6-14`。
- [ ] P2/P4 聚合项仍未闭环：任意重复投递、全部存储/WAL/native-binlog/applied-marker
  崩溃点、提升/故障切换拓扑及剩余双向官方 MySQL 矩阵仍需独立门禁。

Evidence: `reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1592.txt`。

### Continuation 1594

- [x] 官方 MySQL 反向提升 fixture 通过：官方源的普通事务、两阶段 XA、one-phase XA
  已应用到 xmysql；xmysql 重启后无重复并完成 Promote；官方目标随后成功应用
  xmysql 提升后的普通、两阶段 XA 和 one-phase XA。
- [ ] P2/P4 聚合项仍未闭环：任意重复投递、全部存储/WAL/applied-marker 崩溃点、完整
  拓扑故障切换和剩余官方双向矩阵仍需独立门禁。

Evidence: `reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1594.txt`。

### Continuation 1595

- [x] 新增 native duplicate-prefix retry 回归：副本先持久化事务前缀，重启后再次收到
  已持久化前缀和提交帧，事务只应用一次；提交后的同一物理重试继续为 no-op。
- [x] 通过专项测试及 `server/replication`、`server/net` 回归。
- [ ] P2/P4 聚合项仍未闭环：官方传输层重复投递、全部存储/WAL/applied-marker 崩溃点
  和完整拓扑/官方互操作矩阵仍需独立门禁。

Evidence: `reports/compatibility/p2-native-duplicate-prefix-retry-current-continuation1595.txt`。

### Continuation 1597

- [x] 当前版本真实外部进程崩溃恢复矩阵连续 3 轮通过：已提交数据保留、未提交数据
  回滚、半提交事务至多应用一次，DDL 和索引重建元数据在重启后仍可查询。
- [ ] P2/P4 聚合项仍未闭环：native replication/applied-marker 的全部传输边界、完整
  提升拓扑和官方互操作矩阵仍需独立门禁。

Evidence: `reports/compatibility/p2-external-crash-recovery-current-continuation1597.txt`。

### Continuation 1600

- [x] 客户端集群故障切换 fixture 通过：MySQL CLI、Go mysql driver、PyMySQL、Node mysql2
  均能访问源端；副本追平 GTID 后源端停止并完成 Promote；四类客户端再次访问提升节点
  全部通过。
- [ ] P3 全量客户端和 P2/P4 聚合项仍未闭环：负向/协议/TLS/认证插件/ORM、native
  传输崩溃边界和完整官方拓扑矩阵仍需独立门禁。

Evidence: `reports/compatibility/p2-p3-client-cluster-failover-current-continuation1600.txt`。

### Continuation 1602

- [x] 本地 replication runtime 的 quorum/fencing 拓扑专项通过：单副本法定人数选举、
  重启后 fencing 状态和旧 epoch 拒绝、提升前隔离可达旧源均通过。
- [ ] P2/P4 聚合项仍未闭环：多节点进程级 native 传输中断和完整官方 MySQL 拓扑矩阵
  仍需独立门禁。

Evidence: `reports/compatibility/p2-local-topology-failover-current-continuation1602.txt`。

### Continuation 1604

- [x] 扩展官方 MySQL -> xmysql fixture：xmysql 保持运行时，官方源停止并重新启动；源
  恢复后 xmysql 自动重连并继续应用普通事务和两阶段 XA，行 1-7 均只出现一次。
- [ ] P2/P4 聚合项仍未闭环：native 传输在全部 applied-marker/storage 崩溃边界和完整
  官方拓扑矩阵仍需独立门禁。

Evidence: `reports/compatibility/p2-p4-official-source-transport-reconnect-current-continuation1604.txt`。

### Continuation 1607

- [x] 补齐 native 一次批量包含多个事务时的 source-position/applied-marker 故障窗口：
  最终状态替换失败后，重启使用同一 `ApplyNativeAtSource` 路径重试，两个事务均不重复
  执行，source position 只在整批状态替换成功后发布。
- [x] 通过该专项以及 `server/replication`、`server/net` 全包回归。
- [ ] 所有 storage/WAL/native relay/applied-marker 交错故障、完整多节点拓扑和官方
  MySQL 矩阵仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-native-multi-transaction-source-position-current-continuation1607.txt`。

### Continuation 1606

- [x] 补齐 P_S 组件型虚拟表的逐表 SELECT 权限回归：普通持久化账户访问
  `component_scheduler_tasks`、`clone_status`、`keyring_keys` 时，未授予对应表级
  SELECT 前拒绝，逐表授权后可查询注册列。
- [x] 通过组件权限专项，以及 P_S/I_S、角色默认激活、mandatory role、`SET ROLE` 和
  角色可见性专项回归。
- [ ] Clone、Keyring、Enterprise Firewall、component scheduler 的完整运行时生命周期
  仍需权威组件或官方 Enterprise fixture；P1 聚合项保持 `partial`。

Evidence: `reports/compatibility/p1-component-table-permissions-current-continuation1606.txt`。

### Continuation 1608

- [x] 补齐本地 HTTP replication source 重启重连：replica 保持运行，source 使用同一
  数据目录和 replication UUID 重启后，普通事务与两阶段 XA 均继续应用，重启前事务不
  重复。
- [x] 通过 source/replica engine 专项、`server/replication` 和 `server/net` 回归。
- [ ] 多节点进程级 transport 中断、全部 native relay/applied-marker/storage 崩溃交错和
  官方 MySQL 完整拓扑矩阵仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-engine-source-reconnect-current-continuation1608.txt`。

### Continuation 1609

- [x] 补齐带真实数据的三成员 quorum 自动故障切换：source 与两个 replica 先同步同一
  事务，source 停止后低 server-id 候选唯一晋升，另一 replica 保持从属，两个副本均只
  保留一条已应用记录。
- [x] 通过该拓扑专项以及 `server/replication`、`server/net` 全包回归。
- [ ] 进程/容器级网络分区、完整 fencing/storage 崩溃交错、晋升后其他 replica 自动改指向
  和官方 MySQL 拓扑矩阵仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-runtime-multi-replica-failover-data-current-continuation1609.txt`。

### Continuation 1612

- [x] 补齐非 Connector/J 客户端的负向协议错误契约：不存在表查询在服务端错误分类
  修复后，Go mysql driver、PyMySQL 和 Node mysql2 均收到 MySQL `ER_NO_SUCH_TABLE`
 （1146，SQLSTATE `42S02`）；mysql CLI 用例已加入同一门禁。
- [x] 通过 `go test ./server/protocol -run
  '^TestClassifyGoErrorMapsMissingTableVariantsToMySQL1146$'` 以及客户端矩阵；当前主机
  没有 mysql 可执行文件或 Docker CLI，mysql CLI 结果记录为 `SKIPPED_ENVIRONMENT`。
- [ ] P3 全量客户端聚合仍未闭环：连接池、旧连接故障重试、TLS/认证插件、更多类型/字符集、
  ORM、客户端版本边界和完整集群客户端矩阵仍需独立门禁。P2/P4、FULLTEXT 和非 InnoDB
  范围边界不变。

Evidence: `reports/compatibility/p3-client-negative-error-code-current-continuation1612.txt`；
`reports/compatibility/client-matrix-current-continuation1612`。

### Continuation 1613

- [x] 补齐 P3 多会话/连接池子门禁：Go 使用 `database/sql` 的两个连接，Node 使用
  `mysql2` 原生 pool，PyMySQL 使用两个独立连接，三者均能同时执行查询并返回正确结果。
- [x] mysql CLI 没有连接池 API，因此不纳入该专用 case；当前主机仍没有 mysql 可执行文件或
  Docker CLI，环境缺失不计为 PASS。
- [ ] P3 全量客户端聚合仍未闭环：旧连接故障重试、TLS/认证插件、更多类型/字符集、ORM、
  客户端版本边界和完整集群客户端矩阵仍需独立门禁。

Evidence: `reports/compatibility/p3-client-multi-session-pool-current-continuation1613.txt`；
`reports/compatibility/client-matrix-current-continuation1613`。

### Continuation 1615

- [x] 补齐 P3 常见类型/元数据子门禁：跨客户端创建包含 `BIGINT`、`DECIMAL`、`DOUBLE`、
  `TEXT`、`BLOB`、`TIMESTAMP` 的 InnoDB 表，并通过 `INFORMATION_SCHEMA.COLUMNS` 验证类型序列。
- [x] Go mysql driver、PyMySQL、Node mysql2 通过；mysql CLI 因本机缺少可执行文件和 Docker
  记录为 `SKIPPED_ENVIRONMENT`。
- [ ] P3 全量客户端聚合仍未闭环：真实值编码、更多类型/字符集、旧连接故障重试、TLS/认证
  插件、ORM、客户端版本边界和完整集群客户端矩阵仍需独立门禁。

Evidence: `reports/compatibility/p3-client-extended-types-metadata-current-continuation1615.txt`；
`reports/compatibility/client-matrix-current-continuation1615`。

### Continuation 1624

- [x] 补齐 P3 客户端真实值编码子门禁：通过 InnoDB 表实际写入并读取 `DECIMAL(10,2)`、
  `DOUBLE`、`BLOB` 和 `DATE`，Go mysql driver、PyMySQL、Node.js/mysql2 均通过；mysql CLI
  因环境缺少客户端和 Docker 记录为 `SKIPPED_ENVIRONMENT`。
- [x] 修复 Go 客户端矩阵与独立重连夹具的构建边界：重连夹具使用
  `reconnectfixture` build tag，普通客户端包可独立构建。
- [ ] P3 全量客户端聚合仍未闭环：mysql CLI、TLS/认证插件、更多值/字符集、ORM、客户端
  版本边界和完整集群客户端矩阵仍需独立门禁。

Evidence: `reports/compatibility/p3-client-wire-values-current-continuation1624.txt`；
`reports/compatibility/client-matrix-current-continuation1624`；
`reports/compatibility/client-restart-reconnect-current-continuation1624`。

### Continuation 1625

- [x] 补齐 P2 原生复制长连接在提升后的 source 刷新：执行
  `COM_REGISTER_SLAVE`、`COM_BINLOG_DUMP` 或 `COM_BINLOG_DUMP_GTID` 前，从实时引擎重新
  取得当前 source，避免复用 Promote/Demote 前的连接级旧指针。
- [ ] 完整 native 拓扑、崩溃窗口、网络分区和官方双向 XA/binlog/GTID 互操作仍未闭环。

Evidence: `reports/compatibility/p2-native-session-source-refresh-current-continuation1625.txt`。

### Continuation 1626

- [x] 在 Docker 可用环境中通过官方 MySQL 8.4.11 源崩溃/重连专项：xmysql 重启后恢复
  普通事务与 XA，官方源重启后继续拉取且无重复。
- [x] 通过官方 MySQL 源 → xmysql 提升 → 官方 MySQL 目标反向提升专项：普通事务、两阶段
  XA、一阶段 XA 均经 native binlog 继续复制。
- [ ] P2/P4 全量聚合仍未闭环：完整双向拓扑、网络分区、更多官方版本和所有故障窗口仍需
  继续验证。

Evidence: `reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1626.txt`；
`reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1626.txt`。

P1 回归证据：`reports/compatibility/p1-privilege-role-component-regression-current-continuation1626.txt`；
该回归只确认现有权限、角色和组件生命周期切片通过，完整 I_S/P_S 仍为 partial。

### Continuation 1627

- [x] 在受控免密配置下补齐 mysql CLI（Docker MySQL 8.4.11）、Go mysql driver、PyMySQL、
  Node.js/mysql2 的共享协议矩阵。
- [x] 在两节点 source/replica 拓扑中验证四类客户端的 source 端点、GTID 追平、source 停止后
  promoted 端点访问，全部通过。
- [ ] P3 全量客户端仍需真实密码插件/TLS、ORM/版本边界、网络分区和完整负向矩阵。

Evidence: `reports/compatibility/p3-client-matrix-dev-auth-current-continuation1627.txt`；
`reports/compatibility/p3-client-cluster-endpoint-current-continuation1627.txt`。

### Continuation 1628

- [x] 补齐正式 engine 配置到 replication runtime 的自动故障切换 wiring：解析并传入
  `[replication] peers`、`auto_failover`、`failure_timeout`。
- [x] 通过三节点 engine 集成回归：source 失效后按 server-id 选举副本，另一副本保持
  replica 角色。
- [ ] native endpoint 广播、网络分区 fencing、完整 native 拓扑和官方双向故障矩阵仍需
  独立门禁。

Evidence: `reports/compatibility/p2-engine-replication-config-autofailover-current-continuation1628.txt`。

### Continuation 1629

- [x] 具体 SQL listener 未配置 `native_endpoint` 时，自动发布 `mysql://host:port`；wildcard
  listener 不发布不可连接地址并保留显式配置要求。
- [x] 通过 engine endpoint discovery 和 quorum auto-failover 回归。
- [ ] 容器/NAT、网络分区、完整 native 拓扑及官方双向矩阵仍需独立门禁。

Evidence: `reports/compatibility/p2-native-endpoint-advertisement-current-continuation1629.txt`。

### Continuation 1630

- [x] 增加 engine 配置级 quorum 安全回归：配置一个不可达 peer 后停止 source，存活副本
  在超过 `failure_timeout` 后保持 replica，不得单节点自提升。
- [ ] 真实网络分区注入、并发 fencing race、完整 native 拓扑和官方双向 XA/binlog/GTID
  矩阵仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-engine-replication-config-quorum-fencing-current-continuation1630.txt`。

同轮串行回归 `go test -p 1 ./server/conf ./server/net ./server/replication ./server/innodb/engine
-count=1 -timeout 45m` 全部通过；engine 包耗时 448.330s。Evidence:
`reports/compatibility/p2-p1-config-cluster-regression-current-continuation1630.txt`。

### Continuation 1631

- [x] 重新执行官方 MySQL 8.4.11 source crash/reconnect 门禁，普通事务、两阶段 XA 和一阶段
  XA 均在 xmysql 重启及官方源重连后恢复且无重复。
- [x] 重新执行官方 MySQL → xmysql 重启/Promote → 官方 MySQL 反向门禁，晋升后的 xmysql
  native source 继续输出普通事务、两阶段 XA 和一阶段 XA，官方目标全部应用成功。
- [ ] 完整双向拓扑、网络分区 fencing race、全部 storage/WAL/applied-marker 崩溃交错和
  更多官方版本矩阵仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1631.txt`；
`reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1631.txt`。

### Continuation 1632

- [x] 增加复制成员配置的精确自节点 endpoint 安全校验：启动时和运行时更新 peer 时均拒绝
  本地 replication listen endpoint，避免自节点被计为远端成员并错误放大自动故障转移 quorum。
- [x] 拒绝发生在成员持久化前；专项测试与完整 `go test ./server/replication -count=1`
  均通过。
- [ ] host alias/NAT、真实网络分区、并发 fencing race、完整多通道拓扑、native
  GTID/binlog 全部崩溃交错及官方版本广度仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-replication-self-peer-quorum-validation-current-continuation1632.txt`。

### Continuation 1633

- [x] 修复晋升 fencing 的本地状态覆盖竞态：更高 epoch 或其他候选 UUID 已围栏本节点时，
  晋升 fencing round 不能在收集 ACK 后清除该围栏；无 peer 路径同样受保护。
- [x] 通过 fencing 专项回归，旧的持久化围栏、过期 epoch 和可达 source 晋升回归保持通过。
- [ ] 该实现仍不是共识协议；任意网络分区、进程暂停、同时外部围栏、完整多节点拓扑和官方
  双向 XA/binlog/GTID 矩阵仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-fencing-epoch-race-guard-current-continuation1633.txt`。

### Continuation 1634

- [x] 将 fencing epoch 校验、内存状态更新和持久化替换放入同一临界区，防止并发 fencing
  请求用旧 epoch 回写磁盘；并发回归重启后仍得到最高 epoch。
- [ ] 该修复只覆盖本地持久化顺序，不等价于分布式共识、quorum lease、网络分区 fencing
  或完整多节点拓扑；P2/P4 聚合项继续保持 `partial`。

Evidence: `reports/compatibility/p2-fencing-persistence-order-current-continuation1634.txt`。

### Continuation 1635

- [x] fencing 文件持久化失败时回滚内存 epoch/owner；直接 fence 与两条 promotion fencing
  路径使用相同规则，避免本进程和重启状态分裂。
- [ ] 文件写入原子性仍不等价于分布式共识、quorum lease、网络分区 fencing 或完整拓扑；
  P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-fencing-persistence-rollback-current-continuation1635.txt`。

### Continuation 1636

- [x] `CHANGE REPLICATION SOURCE` 改为先完成 source 配置持久化和 identity 清理，再发布
  新内存 source；持久化失败时保持旧 source URL。
- [ ] 命名复制通道、分布式共识和完整拓扑仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-source-config-persistence-rollback-current-continuation1636.txt`。

### Continuation 1637

- [x] persisted members 启动时拒绝本节点 endpoint；`UpdatePeers` 先完成 members 文件持久化
  再发布内存 peer 集合，失败时保留旧配置。
- [ ] host/NAT alias、分布式共识、命名通道和完整拓扑仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-members-persistence-rollback-current-continuation1637.txt`。

### Continuation 1638

- [x] 为命名复制通道建立隔离 runtime、source/relay 持久化目录和通道清单恢复。
- [x] 接入命名通道的 CHANGE SOURCE/FILTER、START/STOP/RESET 和 SHOW REPLICA STATUS SQL
  控制面；default channel 既有回调保持兼容。
- [x] 通过命名通道专项、复制包全量和引擎包全量回归。
- [ ] 每通道 native 注册/binlog dump 路由、多源调度、分布式 fencing、网络分区及完整
  crash/XA/GTID 拓扑仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-named-replication-channels-current-continuation1638.txt`。

### Continuation 1639

- [x] Performance Schema 复制连接、applier、filter、failover 视图按 default/命名通道
  输出独立行并保留 `CHANNEL_NAME`；server-wide 视图保持单行。
- [x] 通过复制视图专项回归和 runtime 回归。
- [ ] 完整 P1 系统表/组件语义、native endpoint 发现、分布式故障转移和官方多版本矩阵
  仍未闭环，聚合项保持 `partial`。

Evidence: `reports/compatibility/p1-p2-performance-schema-named-channel-views-current-continuation1639.txt`。

### Continuation 1640

- [x] 通过双上游 native 回归验证命名通道分别维护 source UUID、GTID、文件位置、applier 和
  source URL，west/east 各自只应用自己的 native 事务。
- [x] 明确 native MySQL 注册/拉取协议按连接隔离，不新增不存在于协议中的 channel id；通道
  身份由本地 named runtime 管理。
- [ ] native endpoint 发现、多节点多源调度、网络分区、分布式 fencing、完整 crash-kill
  交错和官方 XA/binlog/GTID 全拓扑仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-named-replication-channels-current-continuation1640.txt`。

### Continuation 1641

- [x] native 晋升后 source repoint 先完成脱敏 source URL 和 identity 持久化，再发布内存
  endpoint/native source；写失败时恢复旧 source URL。
- [x] 增加 identity 持久化失败回归，证明当前进程和重启 source 配置不会分裂。
- [ ] 分布式共识、外部租约 fencing、网络分区、完整 crash-kill 交错和官方多节点 native
  MySQL 故障转移仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-native-source-repoint-persistence-rollback-current-continuation1641.txt`。

### Continuation 1642

- [x] keyed transaction 与 native/XA 的幂等重试在 state 写失败后重新持久化 durable state，
  并保持 binlog/GTID 不重复。
- [x] 增加同进程普通 transaction、XA PREPARE、XA COMMIT 的 state repair 回归，并通过复制包
  全量测试。
- [ ] storage、binlog writer、外部表提交的统一物理原子性，以及分布式 crash/network-partition
  拓扑仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-source-idempotent-retry-state-repair-current-continuation1642.txt`。

### Continuation 1643

- [x] 提交前同步 client/replication transaction journal，再进入物理 storage commit；保留提交后
  commit record 和 marker 屏障。
- [x] 故障注入验证 journal sync 失败时不发生物理提交，重试路径可恢复；复制包、引擎包全量
  与全仓 compile-only 通过。
- [ ] 统一 physical commit protocol、完整 crash-kill 交错、分布式网络分区拓扑和官方完整
  XA/binlog/GTID 互操作仍需后续专项，不能因本切片改为 `implemented`。

Evidence: `reports/compatibility/p2-precommit-journal-sync-current-continuation1643.txt`。

### Continuation 1644

- [x] 当前工作树通过官方 MySQL 8.4.11 反向晋升、官方源 crash/reconnect（含 XA/one-phase XA）
  和本地外部进程 crash-recovery 三次重复矩阵。
- [x] 将上述结果作为 P2/P4 子门禁证据写入矩阵。
- [ ] 统一 storage/WAL/binlog/applied-marker 提交、完整 crash-kill 交错、网络分区、全版本和
  完整 XA/GTID 拓扑仍未闭环，聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-p4-external-crash-official-current-continuation1644.txt`。

### Continuation 1645

- [x] Docker mysql CLI、Go mysql-driver、PyMySQL、Node.js/mysql2 通过当前定义的 17 个客户端
  兼容用例，包括连接认证、DDL/DML、预处理、事务、类型/字符集、元数据、savepoint、多结果/错误、
  多会话池和重连。
- [ ] 认证插件/TLS、更多客户端版本/ORM、扩展负向/网络故障和受保护认证仍需独立门禁，P3
  全量聚合保持 `partial`。

Evidence: `reports/compatibility/p3-client-full-defined-matrix-current-continuation1645.txt`。

### Continuation 1646

- [x] 服务端 TLS/认证协议切片通过，覆盖 TLS 状态记录、`REQUIRE SSL/X509`、
  `caching_sha2_password` 和 `sha256_password` 的 TLS/RSA 分支。
- [ ] 四类非 Connector/J 客户端的真实端到端 TLS、受保护认证、更多客户端版本/ORM 和负向
  网络故障仍需独立门禁；P3 全量聚合继续保持 `partial`。

Evidence: `reports/compatibility/p3-tls-auth-slice-current-continuation1646.txt`。

### Continuation 1647

- [x] 实现 MySQL 协议级 SSLRequest/TLS 升级，并保留同一连接上的后续包解析。
- [x] 增加 `ssl`、`ssl-cert`、`ssl-key`、`ssl-ca`、`ssl_require_client_cert` 配置解析，且
  只有证书/私钥成功加载时才声明 `CLIENT_SSL`。
- [x] 通过服务端 TLS/认证定向测试、会话级 SSLRequest 回归和全仓 compile-only。
- [ ] 继续运行四类非 Connector/J 客户端的真实 TLS、受保护认证及更多网络故障场景，暂不将
  P3 聚合项改为 `implemented`。

Evidence: `reports/compatibility/p3-mysql-protocol-tls-upgrade-current-continuation1647.txt`。

- [x] Go mysql-driver、PyMySQL、Node.js/mysql2 和 Docker mysql:8.4.11 的 mysql CLI 已通过
  CA 校验的真实 TLS 客户端矩阵。
- [ ] 更多客户端版本/ORM、认证插件、网络故障和集群客户端拓扑仍需后续门禁；P3 聚合项保持
  `partial`。

Evidence: `reports/compatibility/p3-client-tls-e2e-current-continuation1647.txt`。

- [x] 以 `dev_bypass_password_auth=false` 和受保护 root 凭据重跑四类客户端 TLS 矩阵，全部通过。
- [ ] 更多客户端版本/ORM、认证插件、网络故障和集群客户端拓扑仍需后续门禁；P3 聚合项保持
  `partial`。

Evidence: `reports/compatibility/p3-client-tls-protected-e2e-current-continuation1647.txt`。

### Continuation 1648

- [x] 修复 `caching_sha2_password` fast-auth 的 MySQL AuthenticationMoreData 包顺序：
  `0x01 0x03` 后跟最终 OK，并通过服务端 AuthSwitch/TLS 定向回归。
- [x] 在一次性临时账号和 TLS 隔离服务器上验证 Go mysql-driver、PyMySQL、Docker mysql CLI
  的 `caching_sha2_password` 与 `sha256_password` 登录及最小查询。
- [ ] Node mysql2 在 caching_sha2_password AuthSwitch 场景仍发生 `connect ETIMEDOUT`；P3
  全量客户端聚合继续保持 `partial`，同时保留更多版本/ORM、负向/网络故障和集群端点门禁。

Evidence: `reports/compatibility/p3-client-auth-plugin-matrix-current-continuation1648.txt`。

### Continuation 1649

- [x] 修正 Windows 开发免密夹具中 PyMySQL 对空密码环境变量的读取，使合法空密码不会被误判为
  缺失配置。
- [x] 真实进程级旧连接失效/服务重启/新连接恢复门禁通过：Go mysql-driver、PyMySQL、
  Node.js/mysql2 均通过。
- [ ] P3 聚合项仍保持 `partial`：更多客户端版本/ORM、负向网络故障和完整集群客户端拓扑
  仍需独立门禁。

Evidence: `reports/compatibility/p3-client-restart-reconnect-current-continuation1649.txt`。

### Continuation 1650

- [x] 重新执行官方 MySQL 8.4.11 反向晋升 fixture：普通事务、两阶段 XA、一阶段 XA 从官方
  source 应用到 xmysql，xmysql 重启后无重复；xmysql 晋升为 source 后，三类事务继续应用到
  官方 MySQL target。
- [ ] P2/P4 聚合项仍保持 `partial`：完整 crash-kill、网络分区/fencing、更多官方版本及
  全量 GTID/XA 拓扑矩阵仍需独立门禁。

Evidence: `reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1650.txt`。

### Continuation 1651

- [x] 通过官方 MySQL 8.4.11 source crash/reconnect fixture：xmysql 崩溃窗口前、窗口中（含
  一阶段/两阶段 XA）及官方 source 重连后的事务均恢复，最终 exactly-once 且无重复行。
- [ ] P2/P4 聚合项仍保持 `partial`：全部 storage/WAL/native-binlog/applied-marker 交错、
  网络分区/fencing、更多官方版本和完整双向拓扑仍需独立门禁。

Evidence: `reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1651.txt`。

### Continuation 1620

- [x] 补齐 P3 旧连接故障与重连子门禁：控制器对同一 xmysql 数据目录执行停止/重启，Go
  mysql driver、PyMySQL、Node.js/mysql2 的旧物理连接均按预期失败，重新建立连接后均能
  成功执行 `SELECT 1`。
- [ ] P3 全量客户端聚合仍未闭环：mysql CLI 当前因环境缺少客户端和 Docker 而未验证，
  TLS/认证插件、更多值/字符集、ORM、客户端版本边界和完整集群客户端矩阵仍需独立门禁。

Evidence: `reports/compatibility/p3-client-restart-reconnect-current-continuation1620.txt`；
`reports/compatibility/client-restart-reconnect-current-continuation1620`。

### Continuation 1621

- [x] 补齐 P2 晋升后的 HTTP 副本自动重指向：低 server-id 副本晋升后，其他存活副本通过
  控制面发现新的 source，自动更新并持久化 source URL，并继续应用晋升后事务。
- [x] 增加 native endpoint 状态契约和重指向单元门禁：替换晋升节点的主机/端口，保留运行
  时认证参数，持久化 URL 不包含密码。
- [x] 通过该专项及 `go test ./server/replication -count=1 -timeout 45m`。
- [ ] native MySQL binlog 的晋升后 endpoint 发现、完整崩溃窗口、网络分区和官方双向拓扑
  仍未闭环，P2/P4 聚合项保持 `partial`。

Evidence: `reports/compatibility/p2-promotion-auto-repoint-current-continuation1621.txt`。

### Continuation 1707

- [x] 修正 P_S 复制视图在没有已配置 replica channel 时的空集语义：standalone/source
  runtime 不再返回合成的 `StatusSnapshot` 行；真实 replica channel 的普通状态和已配置
  的过滤规则仍保持可见。
- [x] 通过专项先红后绿回归，以及完整 `TestPerformanceSchema` 定向门禁（60.963 秒）。
- [ ] P1 完整 I_S/P_S 逐表运行时、P2 native binlog/GTID/XA/crash recovery、P3 全量
  非 Connector/J 客户端矩阵和 P4 官方 MySQL 双向互操作仍未完成；FULLTEXT deferred，
  非 InnoDB 明确 out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-replication-empty-runtime-current-continuation1707.txt`。

### Continuation 1708

- [x] 补齐官方 PSI 注册可确认的 `setup_instruments.PROPERTIES`：
  `wait/io/socket/sql/client_connection` 返回 `user`。
- [x] 通过 setup/instrument 聚焦回归和完整 `TestPerformanceSchema` 定向门禁（61.417 秒）。
- [ ] 完整 instrument metadata、I_S/P_S 运行时/组件生命周期、P2 复制互操作、P3 全量
  客户端矩阵和 P4 官方双向互操作仍未完成；FULLTEXT deferred，非 InnoDB 明确
  out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-client-socket-property-current-continuation1708.txt`。

### Continuation 1709

- [x] 补齐官方 PSI memory 注册可确认的 `memory/sql/THD::main_mem_root` metadata：
  `PROPERTIES='controlled_by_default'`、`FLAGS='controlled'`、`VOLATILITY=0` 和官方
  documentation。
- [x] 通过专项 metadata 回归和完整 `TestPerformanceSchema` 定向门禁（60.333 秒）。
- [ ] 完整 instrument metadata、I_S/P_S 逐表运行时/组件生命周期、P2 复制互操作、P3
  全量客户端矩阵和 P4 官方双向互操作仍未完成；FULLTEXT deferred，非 InnoDB 明确
  out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-memory-instrument-metadata-current-continuation1709.txt`。

### Continuation 1710

- [x] 补齐官方 `statement/abstract/Query` setup instrument 注册和 metadata：
  `PROPERTIES='mutable'`、`FLAGS=NULL`、`VOLATILITY=0` 及官方 documentation。
- [x] 通过专项 setup metadata 回归和完整 `TestPerformanceSchema` 定向门禁（786.386 秒，
  测试临时目录切换到 D:）。
- [ ] 完整 instrument catalog、abstract statement 生命周期、I_S/P_S 逐表运行时/组件
  生命周期、P2 复制互操作、P3 全量客户端矩阵和 P4 官方双向互操作仍未完成；FULLTEXT
  deferred，非 InnoDB 明确 out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-abstract-query-instrument-current-continuation1710.txt`。

### Continuation 1711

- [x] 补齐官方 `statement/abstract/new_packet` 和 `statement/abstract/relay_log` setup
  instrument 注册行，默认 `ENABLED=YES`、`TIMED=YES`。
- [x] 通过专项 setup 回归和完整 `TestPerformanceSchema` 定向门禁（785.895 秒，测试
  临时目录切换到 D:）。
- [ ] 完整 abstract statement refinement/lifecycle、instrument catalog、I_S/P_S 逐表
  运行时/组件生命周期、P2 复制互操作、P3 全量客户端矩阵和 P4 官方双向互操作仍未
  完成；FULLTEXT deferred，非 InnoDB 明确 out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-abstract-instrument-registry-current-continuation1711.txt`。

### Continuation 1712

- [x] 补齐 `statement/abstract/new_packet` 和 `statement/abstract/relay_log` 的官方
  metadata：`PROPERTIES='mutable'`、`FLAGS=NULL`、`VOLATILITY=0` 和官方 documentation。
- [x] 通过专项 metadata 回归和完整 `TestPerformanceSchema` 定向门禁（780.342 秒，
  测试临时目录切换到 D:）。
- [ ] 完整 statement refinement/lifecycle、instrument catalog、I_S/P_S 逐表运行时/组件
  生命周期、P2 复制互操作、P3 全量客户端矩阵和 P4 官方双向互操作仍未完成；FULLTEXT
  deferred，非 InnoDB 明确 out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-abstract-instrument-metadata-current-continuation1712.txt`。

### Continuation 1713

- [x] 补齐官方 MySQL 8.4 `setup_instruments` 的四个 InnoDB 文件注册行：
  `innodb_tablespace_open_file`、`innodb_temp_file`、`innodb_arch_file` 和
  `innodb_clone_file`，默认均为 `ENABLED=YES,TIMED=YES`。
- [x] 先红后绿专项门禁和完整 `TestPerformanceSchema` 族（750.467 秒，D: 临时目录）通过。
- [ ] 不把这四个 registry 行扩展为完整 file instances、I/O lifecycle 或 P1 聚合项完成；
  完整 I_S/P_S、P2/P3/P4 继续按全局任务推进。

Evidence: `reports/compatibility/p1-performance-schema-innodb-file-instrument-registry-current-continuation1713.txt`。

### Continuation 1714

- [x] 补齐官方 MySQL 8.4 Unix server thread registry 的六个 `setup_threads` 行：
  `admin_interface`、`bootstrap`、`compress_gtid_table`、`manager`、`parser_service` 和
  `signal_handler`，并按 PSI flags 投影 `singleton/user` 属性。
- [x] 先红后绿专项门禁和完整 `TestPerformanceSchema` 族（1023.278 秒，D: 临时目录）通过。
- [ ] 不把 registry 行扩展为完整后台线程创建、生命周期、权限或 P1 聚合项完成；完整
  I_S/P_S、P2/P3/P4 继续按全局任务推进。

Evidence: `reports/compatibility/p1-performance-schema-thread-instrument-registry-current-continuation1714.txt`。

### Continuation 1715

- [x] 补齐 Windows 条件下官方 MySQL 8.4 `setup_threads` 的四个线程类：
  `con_named_pipes`、`con_shared_mem`、`con_sockets` 和 `shutdown_restart`，并按
  `runtime.GOOS` 进行平台条件注册。
- [x] 先红后绿 Windows 专项门禁和完整 `TestPerformanceSchema` 族（1139.440 秒，D: 临时目录）通过。
- [ ] 不把平台 registry 行扩展为 listener/shutdown 生命周期、权限或 P1 聚合项完成；完整
  I_S/P_S、P2/P3/P4 继续按全局任务推进。

Evidence: `reports/compatibility/p1-performance-schema-windows-thread-registry-current-continuation1715.txt`。

### Continuation 1734

- [x] 补齐二进制预处理协议对应的 P_S 状态变量：
  `Com_stmt_prepare`、`Com_stmt_execute`、`Com_stmt_close`、`Com_stmt_reset`、
  `Com_stmt_send_long_data` 和 `Com_stmt_fetch`；协议处理器记录成功/失败及延迟，
  `global_status/session_status` 统一投影。
- [x] 先红后绿引擎状态回归通过（17.920 秒），预处理协议专项回归通过（1.859 秒）。
- [ ] 完整 P_S 字段/组件/权限语义、非 Connector/J 全客户端矩阵、P2/P4 XA/native
  binlog/crash/promotion 聚合仍未完成；FULLTEXT deferred，非 InnoDB 明确 out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-prepared-command-status-current-continuation1734.txt`。

### Continuation 1735

- [x] 将 `Com_stmt_prepare`、`Com_stmt_execute`、`Com_stmt_close`、`Com_stmt_reset`、
  `Com_stmt_send_long_data`、`Com_stmt_fetch` 的协议调用接入真实 session identity，
  使 `status_by_thread` 与 `status_by_account/status_by_user` 能看到生命周期计数。
- [x] 无法解析身份时只保留 global status，不创建伪造的 thread/account 记录。
- [x] 引擎身份维度专项通过（13.470 秒），网络预处理专项通过（1.985 秒），完整
  `server/net` 包通过（36.452 秒）。
- [ ] 完整 I_S/P_S 表/字段/组件/权限语义、P2/P3/P4 聚合仍需继续；FULLTEXT deferred，
  非 InnoDB out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-prepared-command-session-status-current-continuation1735.txt`。

### Continuation 1736

- [x] 审计原生 binlog/GTID、XA、relay 晋升和存储提交恢复链路：事务键幂等、native
  投影重建、GTID/index 恢复、XA 状态恢复和稳定身份重试均已有实现。
- [x] `server/replication` 全包回归通过（162.876 秒）；存储引擎复制/XA/恢复专项通过
  （247.133 秒）。
- [ ] 不把局部恢复能力误报为统一物理提交完成；跨 storage/WAL、logical/native binlog、
  GTID/applied marker、外部 topology/fencing 以及完整官方 kill/partition/双向互操作仍
  保持 P2/P4 `partial`。FULLTEXT deferred，非 InnoDB out_of_scope。

Evidence: `reports/compatibility/p2-native-binlog-gtid-recovery-audit-current-continuation1736.txt`。

### Continuation 1737

- [x] 注册 MySQL 兼容的 `statement/sql/error` Performance Schema instrument，
  并将 executor/worker 两条指标路径中的 SQL parser failure 归入专用 instrument；
  普通执行错误继续按其 statement instrument 统计。
- [x] 先红后绿的解析错误专项与聚焦 Performance Schema instrument 回归通过，
  `server/observability/metrics` 全包通过。
- [ ] 该切片只关闭一个 parser-error instrument 边界；完整 I_S/P_S 表覆盖、字段精度、
  运行时统计、锁/等待/线程生命周期、组件/插件生命周期、权限语义及 P1 聚合仍未完成。

Evidence: `reports/compatibility/p1-performance-schema-parse-error-instrument-current-continuation1737.txt`。

### Continuation 1738

- [x] 补齐六个 prepared protocol command instrument：`statement/com/Prepare`、
  `Execute`、`Close stmt`、`Reset stmt`、`Long Data`、`Fetch`，并保持 `Com_stmt_*`
  状态变量映射。
- [x] 让 command instrument 的 `setup_instruments` enabled/timed 设置影响事件采集；
  instrument 关闭时保留全局命令计数，且无 engine 测试夹具不发生空指针。
- [x] 通过引擎专项、metrics 全包、预处理命令单测和完整 `server/net` 回归。
- [ ] 其它 COM command、完整 command lifecycle、全量 I_S/P_S 表字段和 P1 聚合仍未完成。

Evidence: `reports/compatibility/p1-performance-schema-command-instruments-current-continuation1738.txt`。

### Continuation 1739

- [x] 注册并分类普通协议 command instrument：Ping、Init DB、Field List、Refresh、
  Statistics、Processlist、Kill、Debug、Time、Change user、Binlog Dump、Table Dump、
  Connect、Connect Out、Register Slave、Set option、Daemon、Sleep、Quit 和 Error。
- [x] 保持 COM_QUERY 与 COM_STMT_* 的专用执行/统计路径，避免普通 command defer 与 SQL/
  prepared event 重复计数；引擎专项、`server/net` 映射测试、`server/net` 全包和 metrics
  全包通过。
- [ ] `statement/com/Query` 父级生命周期、完整 command history/current 语义、全量 I_S/P_S
  表字段和 P1 聚合仍未完成；非 Connector/J 全客户端矩阵、P2/P4 聚合、FULLTEXT deferred，
  非 InnoDB out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-common-command-instruments-current-continuation1739.txt`。

### Continuation 1740

- [x] 将真实 COM_QUERY 协议上下文传给 `events_statements_current`，使活动请求以
  `statement/com/Query` 暴露；完成后由 SQL executor 保留最终 `statement/sql/*` 或
  `statement/sql/error` 统计，不产生重复历史事件。
- [x] active statement 的 instrumented/history/timed 状态遵循当前 actor、thread、consumer
  和 instrument 设置；Performance Schema 当前事件专项及 `server/net` 全包通过。
- [ ] 同一物理事件的完整可变生命周期、完整 command history/current 语义、全量 I_S/P_S
  表字段和 P1 聚合仍未完成；非 Connector/J 全客户端矩阵、P2/P4 聚合、FULLTEXT deferred，
  非 InnoDB out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-com-query-current-instrument-current-continuation1740.txt`。

### Continuation 1741

- [x] 修正合法 `COM_RESET_CONNECTION` 的 command instrument：注册并路由到
  `statement/com/Reset connection`，不再错误归入 `statement/com/Error`。
- [x] 通过协议映射、setup row、statement summary、`server/net` 全包和 metrics 全包。
- [ ] 剩余命令矩阵、完整 command lifecycle/current-history 语义、全量 I_S/P_S 表字段和
  P1 聚合仍未完成；非 Connector/J 全客户端矩阵、P2/P4 聚合、FULLTEXT deferred，非
  InnoDB out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-reset-connection-command-current-continuation1741.txt`。

### Continuation 1742

- [x] 注册 `performance_schema_setup_actors_size` 和
  `performance_schema_setup_objects_size`，按 MySQL 8.4 约定暴露 `-1` autosizing。
- [x] 实现 setup_actors/setup_objects 有效默认 100 行容量和溢出拒绝，并通过
  Performance Schema 全专项、manager、`server/net` 与 metrics 回归。
- [ ] 完整 I_S/P_S 表、组件、运行时和权限语义以及 P1 聚合仍未完成；非 Connector/J
  全客户端矩阵、P2/P4 聚合、FULLTEXT deferred，非 InnoDB out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-setup-capacity-current-continuation1742.txt`。

### Continuation 1743

- [x] 注册 `performance_schema_show_processlist` 全局动态变量，默认 `OFF`，并规范化
  `SET GLOBAL` 的 ON/OFF 值。
- [x] 开启变量后让 `SHOW PROCESSLIST` 从 Performance Schema processlist 数据源生成
  传统结果形状；processlist、manager、`server/net` 和 metrics 回归通过。
- [ ] 完整 I_S/P_S 表字段、运行时统计、锁/等待/线程、组件/插件生命周期和权限语义仍未完成；
  非 Connector/J 全客户端矩阵、P2/P4 聚合、FULLTEXT deferred，非 InnoDB out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-show-processlist-current-continuation1743.txt`。

### Continuation 1744

- [x] 注册并加载 `performance_schema_accounts_size`、
  `performance_schema_hosts_size`、`performance_schema_users_size` 启动变量。
- [x] 实现连接摘要及对应 `status_by_*` 表的 0 禁用、正值限行和 -1 autosizing 语义；
  专项、完整 Performance Schema 族、manager 与 `server/net` 回归通过。
- [ ] 完整 I_S/P_S 运行时、组件/插件生命周期、线程/锁/等待和权限语义仍未完成；
  非 Connector/J 全客户端矩阵、P2/P4 聚合、FULLTEXT deferred，非 InnoDB out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-connection-summary-capacity-current-continuation1744.txt`。

### Continuation 1745

- [x] 注册并加载 `performance_schema_digests_size` 启动变量，实现 digest summary 的
  0 禁用、正值限行和 -1 autosizing。
- [x] 在查询过滤后应用 digest 容量，避免无关 schema 影响带条件查询；digest 专项和
  完整 Performance Schema 族回归通过。
- [ ] digest 算法完全一致性、lost 计数器以及完整 I_S/P_S 运行时、组件和权限语义仍未完成；
  非 Connector/J 全客户端矩阵、P2/P4 聚合、FULLTEXT deferred，非 InnoDB out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-digest-capacity-current-continuation1745.txt`。

### Continuation 1746

- [x] 注册并加载 `performance_schema_max_sql_text_length` 启动变量，默认 1024 字节。
- [x] 将字节长度限制应用到 statement event `SQL_TEXT` 和 digest
  `QUERY_SAMPLE_TEXT`，保持 UTF-8 有效；专项和完整 Performance Schema 族回归通过。
- [ ] 完整 digest 算法、其它 P_S 容量变量、I_S/P_S 运行时和权限语义仍未完成；
  非 Connector/J 全客户端矩阵、P2/P4 聚合、FULLTEXT deferred，非 InnoDB out_of_scope。

Evidence: `reports/compatibility/p1-performance-schema-sql-text-length-current-continuation1746.txt`。

### Continuation 1747

- [x] 增加 `performance_schema_error_size` 只读启动变量，默认值 5377，校验范围 0..1048576。
- [x] 从 `[performance_schema] error_size` 加载启动配置。
- [x] 实现 0 禁用错误摘要、正值容量限制，并在查询过滤后应用容量。
- [x] 通过专项、完整 P_S 族、manager、server/net、metrics 回归。
- [ ] 保持全量 I_S/P_S 表字段、运行时统计、锁/等待/线程、组件/权限语义为后续 P1 global task。

Evidence: `reports/compatibility/p1-performance-schema-error-summary-capacity-current-continuation1747.txt`。

### Continuation 1748

- [x] 增加 `performance_schema_max_metadata_locks` 只读启动变量，默认 `-1`，范围 `-1..10485760`。
- [x] 从 `[performance_schema] max_metadata_locks` 加载配置，实现 0 禁用和正值容量限制。
- [x] 通过 metadata lock owner/waiter 专项回归；1748/1749 批次后的完整 P_S、manager、server/net、metrics release-gate 均通过。
- [ ] 完整 metadata-lock lost 计数器、instrument allocation 以及全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-metadata-lock-capacity-current-continuation1748.txt`。

### Continuation 1749

- [x] 增加 `performance_schema_max_table_handles` 只读启动变量，默认 `-1`，范围 `-1..1048576`。
- [x] 从 `[performance_schema] max_table_handles` 加载配置，实现 0 禁用和正值容量限制。
- [x] 通过 table handle 专项、显式锁和隐式事务 table handle 回归；完整 P_S、manager、server/net、metrics 批次 release-gate 均通过。
- [ ] 完整 table-handle lost 计数器、instrument allocation 以及全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-table-handle-capacity-current-continuation1749.txt`。

### Continuation 1750

- [x] 增加 `performance_schema_session_connect_attrs_size` 只读启动变量，默认 `-1`，范围 `-1..1048576`。
- [x] 从 `[performance_schema] session_connect_attrs_size` 加载配置，并限制两个连接属性表的 per-session 属性值字节数。
- [x] 通过专项、既有连接属性回归、完整 P_S、manager、server/net、metrics 回归。
- [x] 暴露并验证 `Performance_schema_session_connect_attrs_lost`，按每个截断会话计数一次。
- [ ] 完成 `_truncated`、精确协议字节预算和 error-log 副作用。

Evidence: `reports/compatibility/p1-performance-schema-session-connect-attrs-capacity-current-continuation1750.txt`。

### Continuation 1751

- [x] 增加 `performance_schema_max_prepared_statements_instances` 只读启动变量，默认 `-1`，范围 `-1..4194304`。
- [x] 从 `[performance_schema] max_prepared_statements_instances` 加载配置，实现 0 禁用和正值限行。
- [x] 通过预处理语句专项、完整 P_S、manager、server/net、metrics 回归。
- [x] 完成 `Performance_schema_prepared_statements_lost` 状态投影及容量溢出计数，并验证重复读取不重复累加。
- [ ] 完成精确 `max_prepared_stmt_count` autosizing 和全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-prepared-capacity-current-continuation1751.txt`。

### Continuation 1753

- [x] 增加 `Performance_schema_prepared_statements_lost` 状态投影和容量溢出计数。
- [x] 通过 prepared 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成精确的 prepared-statement 分配时机、`max_prepared_stmt_count` 自动扩容和全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-prepared-lost-current-continuation1753.txt`。

### Continuation 1752

- [x] 增加 `performance_schema_max_program_instances` 只读启动变量，默认 `-1`，范围 `-1..1048576`。
- [x] 从 `[performance_schema] max_program_instances` 加载配置，实现 0 禁用和正值限行。
- [x] 通过程序汇总专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成 program instrument allocation/lost 语义以及全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-program-capacity-current-continuation1752.txt`。

### Continuation 1754

- [x] 增加 `performance_schema_max_thread_instances` 只读启动变量，默认 `-1`，范围 `-1..1048576`。
- [x] 从 `[performance_schema] max_thread_instances` 加载配置，实现 `threads` 表 0 禁用和正值限行。
- [x] 增加并验证 `Performance_schema_thread_instances_lost`，重复读取不重复计数。
- [x] 通过线程专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成精确 thread-instrument 分配时机、自动扩容和全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-thread-capacity-current-continuation1754.txt`。

### Continuation 1755

- [x] 增加 `performance_schema_max_file_instances` 只读启动变量，默认 `-1`，范围 `-1..1048576`。
- [x] 从 `[performance_schema] max_file_instances` 加载配置，实现 `file_instances` 表 0 禁用和正值限行。
- [x] 增加并验证 `Performance_schema_file_instances_lost`，重复读取不重复计数。
- [x] 通过 file 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成精确 file-instrument 分配时机、自动扩容和全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-file-capacity-current-continuation1755.txt`。

### Continuation 1756

- [x] 增加 `performance_schema_max_socket_instances` 只读启动变量，默认 `-1`，范围 `-1..1048576`。
- [x] 从 `[performance_schema] max_socket_instances` 加载配置，实现 `socket_instances` 表 0 禁用和正值限行。
- [x] 增加并验证 `Performance_schema_socket_instances_lost`，重复读取不重复计数。
- [x] 通过 socket 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成精确 socket-instrument 分配时机、自动扩容和全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-socket-capacity-current-continuation1756.txt`。

### Continuation 1757

- [x] 增加 `Performance_schema_program_lost` 状态投影和程序汇总容量溢出计数。
- [x] 验证同一溢出程序重复读取不会重复累加。
- [x] 通过 program 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成精确 program-instrument 分配时机、自动扩容和全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-program-lost-current-continuation1757.txt`。

### Continuation 1758

- [x] 增加 `performance_schema_max_table_lock_stat` 只读启动变量，默认 `-1`，范围 `-1..1048576`。
- [x] 从 `[performance_schema] max_table_lock_stat` 加载配置，实现 table-lock summary 0 禁用和正值限行。
- [x] 增加并验证 `Performance_schema_table_lock_stat_lost`，重复读取不重复计数。
- [x] 通过 table-lock 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成精确 table-lock-stat 分配时机、自动扩容和全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-table-lock-stat-capacity-current-continuation1758.txt`。

### Continuation 1759

- [x] 增加 `performance_schema_max_index_stat` 只读启动变量，默认 `-1`，范围 `-1..1048576`。
- [x] 从 `[performance_schema] max_index_stat` 加载配置，实现 index-usage summary 0 禁用和正值限行。
- [x] 增加并验证 `Performance_schema_index_stat_lost`，重复读取不重复计数，并覆盖 recorder-backed 与 statement-summary 两条投影路径。
- [x] 通过 index-stat 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成精确 index-stat 分配时机、真实索引身份解析、自动扩容和全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-index-stat-capacity-current-continuation1759.txt`。

### Continuation 1760

- [x] 增加 `performance_schema_max_digest_length` 只读启动变量，默认 `1024`，范围 `0..1048576`。
- [x] 从 `[performance_schema] max_digest_length` 加载配置，并统一限制 digest 聚合、直方图及 statement-event 的 `DIGEST_TEXT`。
- [x] 通过 digest 长度专项、既有 digest/histogram/SQL text 回归、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成精确 MySQL digest tokenizer/hash 算法、全部字段副作用和全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-digest-length-current-continuation1760.txt`。

### Continuation 1761

- [x] 增加 MySQL 8.4 剩余 Performance Schema 容量变量及 startup 配置范围校验。
- [x] 补齐官方 lost-status 名称表面，并实现 `Performance_schema_digest_lost` 稳定计数。
- [x] 通过官方容量配置、digest lost 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成真实 instrument allocator/lifecycle、各类 lost 计数、digest age-based resampling 和全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-official-capacity-surface-current-continuation1761.txt`。

### Continuation 1762

- [x] 将 `performance_schema_max_digest_sample_age` 接入 digest recorder 的真实 age-based
  resampling 逻辑，保留慢样本优先，并支持 startup 配置与动态 `SET GLOBAL` 同步。
- [x] 更新既有 digest 回归以覆盖配置年龄过期后的样本替换语义。
- [x] 通过 digest/age 专项、完整 P_S、manager、`server/net` 和 metrics 回归。
- [ ] 完成精确 MySQL digest tokenizer/hash、全部 digest 字段副作用、剩余 instrument
  allocator/lifecycle/lost 计数和全量 I_S/P_S 运行时语义。

Evidence: `reports/compatibility/p1-performance-schema-digest-sample-age-current-continuation1762.txt`。

### Continuation 1763

- [x] 实现 `Performance_schema_session_connect_attrs_longest_seen` 的真实运行时最大值，
  按 MySQL 编码 key/value 缓冲区字节数统计，并在 P_S 截断前记录。
- [x] 通过连接属性专项、完整 P_S、manager、`server/net` 和 metrics 回归。
- [ ] 完成全量 I_S/P_S 表字段、allocator/lifecycle/lost 计数、运行时统计、锁/等待/线程、
  组件/权限语义，以及全局 P3/P2/P4 兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-connect-attrs-longest-seen-current-continuation1763.txt`。

### Continuation 1764

- [x] 实现 `Performance_schema_metadata_lock_lost` 的真实容量溢出计数，按稳定锁身份去重，
  并覆盖 `max_metadata_locks=0` 与查询过滤后的容量语义。
- [x] 通过 metadata-lock/等待专项、完整 P_S、manager、`server/net` 和 metrics 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/lost 家族、全量 I_S/P_S 表字段、运行时
  统计、锁/等待/线程精度和组件/权限语义，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-metadata-lock-lost-current-continuation1764.txt`。

### Continuation 1765

- [x] 实现 `Performance_schema_accounts_lost`、`hosts_lost`、`users_lost` 的真实容量溢出
  计数，支持稳定身份去重、过滤后限容和重复读取不累加。
- [x] 排除内部无用户身份的合成 connection-summary 行，避免伪造匿名账号/主机/用户。
- [x] 通过连接汇总专项、完整 P_S、manager、`server/net` 和 metrics 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/lost 家族、全量 I_S/P_S 表字段、运行时
  统计、锁/等待/线程精度和组件/权限语义，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-connection-summary-lost-current-continuation1765.txt`。

### Continuation 1766

- [x] 将 `performance_schema_max_statement_classes` 接入 statement instrument 注册容量，支持 0
  容量、正值限容和稳定注册顺序。
- [x] 将未注册的 statement instrument 从运行时启用判断中排除，并实现
  `Performance_schema_statement_classes_lost` 实际计数。
- [x] 通过 statement-class 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/lost 家族、全量 I_S/P_S 表字段、运行时
  统计、锁/等待/线程精度和组件/权限语义，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-statement-classes-current-continuation1766.txt`。

### Continuation 1767

- [x] 将当前 stage/file/socket/memory instrument 接入对应的 `max_*_classes` 注册容量，支持
  容量为 0 时不注册以及正值限容。
- [x] 将超出容量的四类 instrument 从运行时启用判断中排除，并实现对应 `*_classes_lost`
  实际计数。
- [x] 通过 instrument-class 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/lost 家族、全量 I_S/P_S 表字段、运行时
  统计、锁/等待/线程精度和组件/权限语义，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-instrument-class-capacity-current-continuation1767.txt`。

### Continuation 1768

- [x] 将 `performance_schema_max_thread_classes` 接入 `setup_threads` 注册容量，支持容量为 0
  时不暴露 thread class。
- [x] 让前台/后台 thread instrument 状态遵守 class 注册结果，并实现
  `Performance_schema_thread_classes_lost` 实际计数。
- [x] 通过 thread-class 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/lost 家族、全量 I_S/P_S 表字段、运行时
  统计、锁/等待/线程精度和组件/权限语义，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-thread-classes-current-continuation1768.txt`。

### Continuation 1769

- [x] 将 `max_meter_classes`、`max_metric_classes` 接入 telemetry setup registry 的容量限制。
- [x] 实现 `Performance_schema_meter_lost`、`Performance_schema_metric_lost` 实际计数，并保留
  既有 telemetry 更新与字段形状。
- [x] 通过 telemetry 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/lost 家族、全量 I_S/P_S 表字段、运行时
  统计、锁/等待/线程精度和组件/权限语义，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-telemetry-classes-current-continuation1769.txt`。

### Continuation 1770

- [x] 将 file/socket/memory instrument class capacity 投影到 instances/summary 运行时查询，容量
  为 0 时不返回未注册类行，并保持默认容量路径。
- [x] 通过 instrument runtime-capacity 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/lost 家族、全量 I_S/P_S 表字段、运行时
  统计、锁/等待/线程精度和组件/权限语义，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-instrument-runtime-capacity-current-continuation1770.txt`。

### Continuation 1771

- [x] 为真实显式锁和隐式事务 table handle 增加按容量淘汰的稳定身份跟踪。
- [x] 实现 `Performance_schema_table_handles_lost` 实际计数，并在句柄重新可见时清理 lost 身份。
- [x] 通过 table-handles 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/lost 家族、全量 I_S/P_S 表字段、运行时
  统计、锁/等待/线程精度和组件/权限语义，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-table-handles-lost-current-continuation1771.txt`。

### Continuation 1772

- [x] 使用 RuntimeRecorder 的真实 active file-handle 数量实现 `max_file_handles` 溢出识别。
- [x] 实现 `Performance_schema_file_handles_lost` 累计计数，并通过 global_status 直接刷新验证。
- [x] 通过 file-handle 专项、相关 file 回归、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/lost 家族、全量 I_S/P_S 表字段、运行时
  统计、锁/等待/线程精度和组件/权限语义，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-file-handles-lost-current-continuation1772.txt`。

### Continuation 1773

- [x] 将真实 `ExecutionContext.triggerStack` 的 statement-stack 容量溢出接入
  `Performance_schema_nested_statement_lost` 累计计数。
- [x] 保持既有触发器递归保护行为不变，并恢复官方 rwlock lost 状态行的零值投影。
- [x] 通过 statement-stack/AFTER trigger 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/lost 家族、全量 I_S/P_S 表字段、运行时
  统计、锁/等待/线程精度和组件/权限语义，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-statement-stack-lost-current-continuation1773.txt`。

### Continuation 1774

- [x] 将 `performance_schema_max_table_instances` 接入现有 table-I/O recorder 的真实对象集合，
  对 table/index I/O 汇总执行确定性的容量投影。
- [x] 实现 `Performance_schema_table_instances_lost` 累计计数，并覆盖 setup_objects、生命周期
  与截断回归。
- [x] 通过 table-instance 专项、完整 P_S、manager、server/net、metrics 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/lost 家族、全量 I_S/P_S 表字段、运行时
  统计、锁/等待/线程精度和组件/权限语义，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-table-instances-lost-current-continuation1774.txt`。

### Continuation 1775

- [x] 将 SELECT 执行器已确认的物理索引身份传递到 RuntimeRecorder，并按真实索引身份
  投影 `table_io_waits_summary_by_index_usage`。
- [x] 保持 table scan/无索引身份调用的空 `INDEX_NAME` 兼容语义，并覆盖 table-I/O 截断回归。
- [x] 通过索引身份专项、metrics 全量回归和受影响 Performance Schema table-I/O 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/lost 家族、全量 I_S/P_S 表字段、运行时
  统计、锁/等待/线程精度和组件/权限语义，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-index-identity-current-continuation1775.txt`。

### Continuation 1776

- [x] 为可截断的 `setup_actors`/`setup_objects` 增加认证会话的 DROP 权限检查。
- [x] 保持 `setup_consumers`/`setup_instruments`/`setup_threads` 的 TRUNCATE 拒绝边界。
- [x] 通过 setup 生命周期/权限专项和完整 Performance Schema 回归。
- [ ] 完成其余 Performance Schema allocator/lifecycle/runtime/component/permission 语义、
  全量 I_S/P_S 运行时契约，以及 P2/P3/P4 全局兼容矩阵。

Evidence: `reports/compatibility/p1-performance-schema-truncate-drop-privilege-current-continuation1776.txt`。

### Continuation 1777

- [x] 使用当前有效的临时 TLS 证书重跑 Node.js/mysql2 的
  `caching_sha2_password` / `sha256_password` 认证插件矩阵。
- [x] 通过既有客户端功能、连接池、重连和认证插件场景；旧失败归因于过期证书。
- [ ] 继续补齐 P3 的更多客户端版本、ORM/连接池、协议负例、集群拓扑和长期故障矩阵；
  P1 全量 I_S/P_S、P2/P4 XA/binlog/GTID/恢复仍未完成，Fulltext 延后，非 InnoDB 排除。

Evidence: `reports/compatibility/p3-client-node-auth-rerun-current-continuation1777.txt`。

### Continuation 1778

- [x] 在网络协议层补齐已完成 `COM_QUERY` 的 `statement/com/Query` 父级命令 instrument，
  并保持引擎 `statement/sql/*` 事件独立记录。
- [x] 通过 COM_QUERY 命令 instrument 专项和 `server/net` 受影响测试。
- [ ] engine 包级回归在 45 分钟上限超时；不能据此宣称完整 engine 回归通过。
- [ ] 继续补齐全量 I_S/P_S、组件/权限/角色语义、P3 非 Connector/J 矩阵和 P2/P4
  XA/binlog/GTID/崩溃恢复/提升；Fulltext 延后，非 InnoDB 排除。

Evidence: `reports/compatibility/p1-performance-schema-com-query-command-lifecycle-current-continuation1778.txt`。

### Continuation 1779

- [x] 通过 `server/replication` 全量回归，覆盖 native binlog、GTID、XA、relay/restart、
  promotion/fencing、rotation、purge 与 exactly-once 状态转换。
- [x] 审计官方 MySQL 互操作门禁；6 项官方 fixture 测试因缺少
  `XMYSQL_OFFICIAL_BINLOG_URL` / `XMYSQL_OFFICIAL_BINLOG_DSN` 全部跳过。
- [ ] 官方 MySQL XA/binlog/GTID/崩溃恢复/提升互操作仍需受保护的官方 fixture；
  P1/P3、Fulltext 延后、非 InnoDB 排除边界不变。

Evidence: `reports/compatibility/p2-replication-regression-official-fixture-audit-current-continuation1779.txt`。

### Continuation 1780

- [x] 重新执行 I_S/P_S、权限和角色定向回归，engine 包通过，耗时 1669.007s。
- [ ] 该回归只证明已有实现切片稳定；继续补齐组件运行时来源、字段权威值、锁/等待/线程精度、
  完整权限/角色生命周期，以及 P2/P3/P4 剩余矩阵。

Evidence: `reports/compatibility/p1-information-performance-regression-current-continuation1780.txt`。

### Continuation 1781

- [x] 刷新非 Connector/J 客户端环境矩阵：Go、PyMySQL、Node.js/mysql2 运行时可用。
- [x] 记录当前主机缺少 `mysql.exe`；诊断结果为 `SKIPPED_ENVIRONMENT`，不将 CLI 缺失误判为通过。
- [ ] 继续执行受保护 live-server 条件下的 Go/PyMySQL/Node.js、MySQL CLI、连接池、协议负例、
  版本兼容和集群故障矩阵；P3 聚合项仍为 `partial`。

Evidence: `reports/compatibility/p3-client-environment-matrix-audit-current-continuation1781.txt`。

### Continuation 1782

- [x] 通过 `mutex_instances`/`rwlock_instances` 形状与无运行时组件空集行为专项回归。
- [x] 审计确认当前没有权威同步对象生命周期来源；不生成伪造锁实例或把 Go `sync` 原语映射为
  MySQL Performance Schema instrument。
- [ ] 先建立同步对象身份、owner/wait、历史和 allocator/lost 来源，再补 capacity、instance、
  wait、thread-owner、history/lost 的红绿回归；P1 全量 I_S/P_S、P2/P3/P4 仍未完成。

Evidence: `reports/compatibility/p1-performance-schema-sync-instance-source-audit-current-continuation1782.txt`。

### Continuation 1783

- [x] 在隔离 xmysql-server 实例上执行 Go/go-sql-driver/mysql、PyMySQL、Node.js/mysql2
  live runner matrix；全部定义用例通过。
- [x] 覆盖连接认证、DDL/DML、prepared statement、事务、savepoint、类型、元数据、多结果、
  错误码、多会话池和重连。
- [ ] MySQL CLI、版本组合、生产 TLS/认证、长期故障切换、协议负例和集群端点仍未完成，
  P3 聚合项保持 `partial`；P1/P2/P4 也仍未完成。

Evidence: `reports/compatibility/p3-client-live-matrix-go-python-node-current-continuation1783.txt`。

### Continuation 1784

- [x] 从 `tableDDLCoordinator` 的真实表级 DDL `sync.RWMutex` 注册表投影
  `performance_schema.rwlock_instances` 的对象身份、写锁 owner 和读锁计数。
- [x] 补齐 `performance_schema_max_rwlock_instances`、`Performance_schema_rwlock_instances_lost` 和
  `setup_instruments` 开关的运行时测试，并通过完整 `^TestPerformanceSchema` 回归（965.358s）。
- [ ] 继续建立 mutex/condition 的权威同步对象生命周期来源；完整 I_S/P_S、组件/权限语义、P3
  全量客户端矩阵和 P2/P4 官方 XA/binlog/GTID/崩溃恢复/提升互操作仍未完成；非 InnoDB 排除。

Evidence: `reports/compatibility/p1-performance-schema-rwlock-instances-current-continuation1784.txt`。

### Continuation 1785

- [x] 在非 Connector/J 客户端矩阵加入 `COM_RESET_CONNECTION`：PyMySQL 原始协议命令和
  Node.js/mysql2 `connection.reset()` 均通过，reset 后用户变量正确清理。
- [x] 修复服务端动态用户变量残留在直接 session parameter、导致 `ResetSession` 无法清理的
  根因，并通过失败优先的 engine 回归测试。
- [ ] Go 驱动不纳入本协议 slice 的假覆盖：其公开 `driver.SessionResetter` 只做连接存活检查，
  不发送 `COM_RESET_CONNECTION`；MySQL CLI 缺少 `mysql.exe`，P3 全量矩阵仍为 `partial`。

Evidence: `reports/compatibility/p3-client-session-reset-current-continuation1785.txt`。

### Continuation 1786

- [x] 在非 Connector/J 客户端矩阵加入真实 `COM_PING`：Go、PyMySQL、Node.js/mysql2
  均通过；新增用例不替代此前的 `COM_RESET_CONNECTION` 验证。
- [ ] MySQL CLI、更多驱动版本、TLS/认证组合、ORM/连接池、协议负例、长期故障切换和
  集群端点仍未完成，P3 聚合项保持 `partial`。

Evidence: `reports/compatibility/p3-client-protocol-ping-current-continuation1786.txt`。

### Continuation 1787

- [x] 在 PyMySQL runner 中加入真实 `COM_INIT_DB`：调用 `connection.select_db()` 后校验
  `SELECT DATABASE()`，最终 Go/PyMySQL/Node.js live 矩阵保持通过。
- [x] `go test ./server/net -count=1 -timeout 10m` 通过；MySQL CLI 仍因缺少 `mysql.exe`
  且 Docker fallback 不可用而跳过。
- [ ] P3 全量非 Connector/J 客户端矩阵仍需 CLI、版本/TLS/认证、协议负例、连接池、
  故障切换和集群端点覆盖。

Evidence: `reports/compatibility/p3-client-protocol-init-db-current-continuation1787.txt`。

### Continuation 1788

- [x] 在 Node.js/mysql2 runner 中加入真实 `COM_CHANGE_USER`：调用 `connection.changeUser()`
  后校验数据库切换，Node 专项和全量 Go/PyMySQL/Node 矩阵通过。
- [ ] MySQL CLI 因缺少 `mysql.exe` 且 Docker fallback 不可用而跳过，P3 全量矩阵仍为
  `partial`；版本/TLS/认证、连接池、协议负例、故障切换和集群端点仍待完成。

Evidence: `reports/compatibility/p3-client-protocol-change-user-current-continuation1788.txt`。

### Continuation 1789

- [x] 为 `performance_schema.mutex_instances` 接入五个执行器真实 `sync.Mutex` 实例，
  验证对象身份、容量、lost 计数和 instrument 开关；补齐 mutex/rwlock class 容量与
  class lost 统计。
- [x] 通过 mutex 专项测试及 rwlock/辅助视图/注册表/容量交叉回归。
- [ ] condition 实例、完整 I_S/P_S 字段和运行时统计、等待/线程精度、权限语义仍未完成，
  P1 聚合项保持 `partial`。

Evidence: `reports/compatibility/p1-performance-schema-mutex-instances-current-continuation1789.txt`。

### Continuation 1790

- [x] 原生 binlog 解码器支持并消费 `BEGIN_LOAD_QUERY_EVENT(17)` 和
  `EXECUTE_LOAD_QUERY_EVENT(18)`，覆盖顶层、事务 payload 内嵌和 row-image 解码路径。
- [x] 专项测试通过，完整 `go test ./server/replication -count=1 -timeout 20m` 通过。
- [ ] 官方 MySQL XA/binlog/GTID、崩溃恢复和提升互操作仍需真实 fixture 验证。

Evidence: `reports/compatibility/p2-native-control-events-current-continuation1790.txt`。

### Continuation 1791

- [x] 为 `performance_schema.cond_instances` 接入全局读锁 gate 的真实 `sync.Cond`，并验证
  MySQL 列形状、对象身份、`max_cond_instances`、`max_cond_classes`、lost 计数和 instrument
  开关。
- [x] 失败优先 condition 专项及完整 `^TestPerformanceSchema` 回归通过（968.176s）。
- [ ] 完整 I_S/P_S 等待生产者、线程 owner 精度、组件生命周期和权限语义仍未完成；P1
  聚合项保持 `partial`，P2/P4/P3 边界不变。

Evidence: `reports/compatibility/p1-performance-schema-condition-instances-current-continuation1791.txt`。

### Continuation 1792

- [x] 为全局读锁 gate 接入真实等待生命周期，覆盖 `events_waits_current`、短/长历史以及
  按线程和全局 wait summary，并保留 session/thread owner。
- [x] 通过 live blocker、相关等待/MDL 回归和完整 `^TestPerformanceSchema` 回归（85.824s）。
- [ ] 完整 I_S/P_S 等待生产者、线程 owner 精度、组件生命周期和权限语义仍未完成；P1
  聚合项保持 `partial`，P2/P4/P3 边界不变。

Evidence: `reports/compatibility/p1-performance-schema-global-read-lock-waits-current-continuation1792.txt`。

### Continuation 1793

- [x] 为全局读锁 condition wait 补齐 `events_waits_summary_by_instance` 当前/历史实例汇总，
  并复用 `cond_instances` 的稳定实例身份。
- [x] 失败优先实例汇总测试通过，完整 `^TestPerformanceSchema` 回归通过（84.678s）。
- [ ] 完整 I_S/P_S 等待生产者、字段、线程 owner 精度、组件生命周期和权限语义仍未完成；
  P1 聚合项保持 `partial`，P2/P4/P3 边界不变。

Evidence: `reports/compatibility/p1-performance-schema-global-read-lock-instance-summary-current-continuation1793.txt`。

### Continuation 1794

- [x] 按 MySQL 8.4 synchronization wait 契约修正全局读锁 condition wait 的对象字段：
  `OBJECT_SCHEMA`、`OBJECT_NAME`、`OBJECT_TYPE` 为 NULL，实例地址与 `cond_instances` 一致。
- [x] 失败优先对象契约测试和完整 `^TestPerformanceSchema` 回归通过（86.832s）。
- [ ] P1 全量 I_S/P_S、其余等待生产者、线程 owner 精度、组件生命周期和权限语义仍未完成；
  聚合项保持 `partial`，P2/P4/P3 边界不变。

Evidence: `reports/compatibility/p1-performance-schema-global-read-lock-wait-object-contract-current-continuation1794.txt`。

### Continuation 1795

- [x] 为全局读锁 condition wait 补齐 `events_waits_history_size` 的 per-thread 容量投影，
  并保持 `history_long` 的全局完成顺序。
- [x] 失败优先历史容量测试和完整 `^TestPerformanceSchema` 回归通过（84.901s）。
- [ ] 短/长历史独立底层存储、P1 全量 I_S/P_S、其余等待生产者、线程 owner、组件生命周期
  和权限语义仍未完成；聚合项保持 `partial`，P2/P4/P3 边界不变。

Evidence: `reports/compatibility/p1-performance-schema-global-read-lock-history-capacity-current-continuation1795.txt`。

### Continuation 1796

- [x] 为全局读锁 condition wait 实现独立的 per-thread short history 与 global long history，
  支持容量差异并保持完成顺序。
- [x] 失败优先独立容量测试和完整 `^TestPerformanceSchema` 回归通过（86.198s）。
- [ ] P1 全量 I_S/P_S、其余等待生产者、线程 owner 精度、组件生命周期和权限语义仍未完成；
  聚合项保持 `partial`，P2/P4/P3 边界不变。

Evidence: `reports/compatibility/p1-performance-schema-global-read-lock-independent-history-current-continuation1796.txt`。

### Continuation 1797

- [x] 补齐全局读锁 condition wait 的短/长历史独立 truncate 作用域，并保持 lock manager/MDL
  历史 reset 行为。
- [x] 失败优先 truncate 专项和完整 `^TestPerformanceSchema` 回归通过（86.732s）。
- [ ] P1 全量 I_S/P_S、其余等待生产者、线程 owner 精度、组件生命周期和权限语义仍未完成；
  聚合项保持 `partial`，P2/P4/P3 边界不变。

Evidence: `reports/compatibility/p1-performance-schema-global-read-lock-history-truncate-current-continuation1797.txt`。

### Continuation 1798

- [x] 补齐 `events_waits_summary_by_instance` 独立 truncate 分发及全局读锁实例摘要 reset；
  by-instance truncate 不影响 global/thread 摘要，global summary truncate 的既有零行投影保持不变。
- [x] 失败优先专项、相关摘要回归及完整 `^TestPerformanceSchema` 回归通过（87.267s）。
- [ ] P1 全量 I_S/P_S、其余等待生产者、线程 owner 精度、组件生命周期和权限/角色语义仍未完成；
  聚合项保持 `partial`，P2/P4/P3 边界不变。

Evidence: `reports/compatibility/p1-performance-schema-global-read-lock-summary-instance-truncate-current-continuation1798.txt`。

### Continuation 1799

- [x] 将 `events_waits_summary_by_instance` 的 reset key 快照扩展到完成的 record-lock、MDL
  和全局读锁等待；global/thread 摘要保持独立。
- [x] 相关专项和完整 `^TestPerformanceSchema` 回归通过（86.872s）。
- [ ] P1 全量 I_S/P_S、其余等待生产者、线程 owner 精度、组件生命周期和权限/角色语义仍未完成；
  聚合项保持 `partial`，P2/P4/P3 边界不变。

Evidence: `reports/compatibility/p1-performance-schema-wait-summary-instance-reset-current-continuation1799.txt`。

### Continuation 1800

- [x] 固化嵌套角色的可见性边界：`SET ROLE ALL` 后，
  `INFORMATION_SCHEMA.ENABLED_ROLES` 只展示直接启用的父角色；父角色继承的子角色权限仍
  通过 `ROLE_TABLE_GRANTS` 进入有效权限图。
- [x] 新增回归测试并通过（5.742s）。
- [ ] 完整 I_S/P_S、角色管理生命周期、全部权限视图和组件语义仍未完成；P1 聚合项保持
  `partial`。

Evidence: `reports/compatibility/p1-nested-role-enabled-vs-inherited-privilege-current-continuation1800.txt`。

### Continuation 1801

- [x] 将全局读锁 condition wait 的 `OBJECT_INSTANCE_BEGIN` 统一到真实
  `sync.Cond` 对象身份，覆盖 `cond_instances`、current/history、by-instance summary 和
  summary truncate 零行。
- [x] 失败优先测试先红后绿；专项回归通过（19.132s），完整 `^TestPerformanceSchema`
  回归通过（1007.013s）。
- [ ] P1 全量 I_S/P_S、组件生命周期、权限语义和其它等待生产者仍未完成；聚合项保持
  `partial`。

Evidence: `reports/compatibility/p1-performance-schema-global-read-lock-summary-instance-identity-current-continuation1801.txt`。

### Continuation 1802

- [x] 按 length-encoded 键和值字节计算 `performance_schema_session_connect_attrs_size`，
  在缓冲区有空间时投影 `_truncated` 及丢弃字节数；过小缓冲区只保留 lost 计数。
- [x] 连接属性专项通过（9.960s），账户属性/全量连接属性子集通过（19.434s）。
- [ ] P1 全量 I_S/P_S、组件生命周期和权限语义仍未完成；P1 聚合项保持 `partial`。

Evidence: `reports/compatibility/p1-performance-schema-session-connect-attrs-truncated-current-continuation1802.txt`。

### Continuation 1803

- [x] `CURRENT_ROLE()` 默认按 `sql_quote_show_create=ON` 输出反引号限定角色，关闭该变量后
  输出未加引号的限定角色；保留无 host 的合成角色兼容路径。
- [x] 会话元数据和系统变量相关回归通过（engine 28.553s，manager 0.212s）。
- [ ] 完整角色图、权限视图、I_S/P_S、P2/P3/P4 聚合仍未完成。

Evidence: `reports/compatibility/p1-current-role-account-format-current-continuation1803.txt`。

### Continuation 1804

- [x] 增加 `ROLES_GRAPHML()`：普通账户隐藏角色图，`ROLE_ADMIN` 会话输出持久化账户节点和直接角色边。
- [x] Dispatcher 保留权威引擎准备的 GraphML 会话值；相关 engine/plan/dispatcher 回归通过。
- [ ] 完整 I_S/P_S、权限/组件语义、非 Connector/J 客户端矩阵、P2/P4 复制互操作仍需继续。

Evidence: `reports/compatibility/p1-roles-graphml-role-admin-boundary-current-continuation1804.txt`。

### Continuation 1805

- [x] Dispatcher 与引擎入口统一 `CURRENT_ROLE()` 的默认反引号账号格式及 OFF 分支。
- [x] 失败优先角色回归和 Dispatcher 全包回归通过。
- [ ] 完整 I_S/P_S、角色/权限/组件语义、P2/P3/P4 聚合仍需继续。

Evidence: `reports/compatibility/p1-current-role-dispatcher-format-current-continuation1805.txt`。

### Continuation 1806

- [x] 补齐角色元数据视图对 `USER`、`HOST`、`GRANTEE_HOST` 和 `DEFAULT_ROLE`
  的等值/LIKE 过滤；错误账号、主机和默认角色条件不再被忽略。
- [x] 失败优先专项及角色相关回归通过（217.132s）。engine 全包回归运行超过十分钟未
  产生退出码，已中止且不记录为 PASS。
- [ ] 完整 I_S/P_S、角色/权限/组件生命周期、非 Connector/J 客户端矩阵和 P2/P4
  官方复制互操作仍未完成；聚合状态保持 `partial`。

Evidence: `reports/compatibility/p1-role-metadata-account-filters-current-continuation1806.txt`。

### Continuation 1807

- [x] 为 `performance_schema.mutex_instances` 增加 owner sidecar；`active_query` 和账户变更
  路径在有真实连接线程号时投影 `LOCKED_BY_THREAD_ID`，未知 owner 继续返回 `NULL`。
- [x] 失败优先专项及同步实例、账户、KILL QUERY 回归通过（37.407s）。
- [ ] 完整 I_S/P_S、等待/线程生命周期、组件/权限语义以及 P2/P3/P4 聚合仍未完成；状态
  保持 `partial`。

Evidence: `reports/compatibility/p1-performance-schema-mutex-owner-current-continuation1807.txt`。

### Continuation 1808

- [x] 补齐 `performance_schema.host_cache` 的 `FIRST_SEEN/LAST_SEEN`：对认证失败已有
  权威时间源的 host 行进行投影，无时间源时继续返回 `NULL`。
- [x] 失败优先专项及 host-cache 相关回归通过（9.300s）。
- [ ] 完整 I_S/P_S 运行时、组件/权限语义以及 P2/P3/P4 聚合仍未完成；状态保持 `partial`。

Evidence: `reports/compatibility/p1-performance-schema-host-cache-seen-times-current-continuation1808.txt`。

### Continuation 1809

- [x] 补齐 `performance_schema.setup_actors` 的 `ROLE` 运行时匹配：按 `HOST`、`USER` 和
  会话 `active_roles` 选择，具体角色规则不再错误作用于无角色会话。
- [x] 失败优先专项、setup_actors/setup_threads 相关回归及完整 `TestPerformanceSchema`
  族通过（923.700s）。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵和 P2/P4
  官方复制互操作仍未完成；P1 聚合状态保持 `partial`。

Evidence: `reports/compatibility/p1-performance-schema-setup-actors-active-role-current-continuation1809.txt`。

### Continuation 1810

- [x] 当会话未显式设置 `active_roles` 时，`setup_actors` 复用 `ENABLED_ROLES` 的角色来源，
  使用持久化默认角色并合并 mandatory roles；显式空切片继续表示 `SET ROLE NONE`。
- [x] 失败优先专项、相关 setup/ENABLED_ROLES 回归及完整 `TestPerformanceSchema` 族通过
  （915.818s）。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵和 P2/P4
  官方复制互操作仍未完成；P1 聚合状态保持 `partial`。

Evidence: `reports/compatibility/p1-performance-schema-setup-actors-default-role-current-continuation1810.txt`。

### Continuation 1811

- [x] 补齐 `INFORMATION_SCHEMA.EVENTS.STATUS` 的事件禁用状态投影；`ALTER EVENT ... DISABLE/ENABLE`
  后分别返回 `DISABLED`/`ENABLED`。
- [x] 失败优先专项和事件相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍是后续全局任务。

Evidence: `reports/compatibility/p1-information-schema-events-disabled-status-current-continuation1811.txt`。

### Continuation 1812

- [x] 补齐 `INFORMATION_SCHEMA.EVENTS.STARTS/ENDS` 的事件调度边界投影。
- [x] 失败优先专项和事件相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍是后续全局任务。

Evidence: `reports/compatibility/p1-information-schema-events-start-end-current-continuation1812.txt`。

### Continuation 1813

- [x] 补齐 `INFORMATION_SCHEMA.EVENTS.ON_COMPLETION` 的 `PRESERVE`/`NOT PRESERVE` 投影。
- [x] 失败优先专项和事件相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍是后续全局任务。

Evidence: `reports/compatibility/p1-information-schema-events-completion-policy-current-continuation1813.txt`。

### Continuation 1814

- [x] 补齐 `INFORMATION_SCHEMA.EVENTS.DEFINER` 与 `EVENT_COMMENT` 的持久化元数据投影。
- [x] 失败优先专项和事件相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍是后续全局任务。

Evidence: `reports/compatibility/p1-information-schema-events-definer-comment-current-continuation1814.txt`。

### Continuation 1815

- [x] 补齐 `INFORMATION_SCHEMA.EVENTS.CREATED/LAST_ALTERED` 的持久化时间投影。
- [x] 失败优先专项和事件相关回归通过。
- [ ] `LAST_EXECUTED` 等运行时字段、完整 I_S/P_S 字段精度、组件/权限生命周期、
  非 Connector/J 客户端矩阵以及 P2/P4 官方复制互操作仍是后续全局任务。

Evidence: `reports/compatibility/p1-information-schema-events-timestamps-current-continuation1815.txt`。

### Continuation 1816

- [x] 将 `INFORMATION_SCHEMA.EVENTS.LAST_EXECUTED` 接入真实 SQL EVENT scheduler 执行回调。
- [x] 失败优先专项及完整事件调度相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍是后续全局任务。

Evidence: `reports/compatibility/p1-information-schema-events-last-executed-current-continuation1816.txt`。

### Continuation 1817

- [x] 补齐 `INFORMATION_SCHEMA.ROUTINES.CREATED/LAST_ALTERED` 的持久化时间投影。
- [x] 失败优先专项及 routine 相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍是后续全局任务。

Evidence: `reports/compatibility/p1-information-schema-routines-timestamps-current-continuation1817.txt`。

### Continuation 1818

- [x] 持久化并投影存储对象创建会话的 `sql_mode/time_zone/character_set_client/collation_connection`。
- [x] 失败优先专项及共享事件/routine/trigger 回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍是后续全局任务。

Evidence: `reports/compatibility/p1-stored-object-session-metadata-current-continuation1818.txt`。

### Continuation 1819

- [x] `INFORMATION_SCHEMA.EVENTS.ORIGINATOR` 使用持久化事件创建者 `server_id`，并为旧元数据
  提供当前实例 `server_id` 回退。
- [x] 失败优先专项和事件/routine/trigger 共享回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍未完成；P1 聚合状态保持 `partial`。

Evidence: `reports/compatibility/p1-information-schema-events-originator-current-continuation1819.txt`。

### Continuation 1820

- [x] 修正 `INFORMATION_SCHEMA.ROUTINES.ROUTINE_DEFINITION` 为 body-only 投影，保留
  `SHOW CREATE` 的完整定义。
- [x] 失败优先专项和 routine 相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍未完成；P1 聚合状态保持 `partial`。

Evidence: `reports/compatibility/p1-information-schema-routines-definition-current-continuation1820.txt`。

### Continuation 1821

- [x] `INFORMATION_SCHEMA.TRIGGERS.ACTION_ORDER` 使用 FOLLOWS/PRECEDES 实际执行顺序投影，且
  `ACTION_STATEMENT` 不包含顺序前缀。
- [x] 失败优先专项和触发器/存储对象回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍未完成；P1 聚合状态保持 `partial`。

Evidence: `reports/compatibility/p1-information-schema-triggers-action-order-current-continuation1821.txt`。

### Continuation 1822

- [x] 视图创建会话的 `character_set_client/collation_connection` 已持久化并投影到 I_S/SHOW。
- [x] 视图专项及相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍未完成；P1 聚合状态保持 `partial`。

Evidence: `reports/compatibility/p1-information-schema-views-session-metadata-current-continuation1822.txt`。

### Continuation 1823

- [x] `INFORMATION_SCHEMA.VIEWS.IS_UPDATABLE` 按可证明的单表直接投影规则返回 YES/NO，复杂视图
  保守返回 NO。
- [x] 失败优先专项和 view 相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍未完成；P1 聚合状态保持 `partial`。

Evidence: `reports/compatibility/p1-information-schema-views-updatability-current-continuation1823.txt`。

### Continuation 1824

- [x] `INFORMATION_SCHEMA.EVENTS.EVENT_DEFINITION` 投影 DO 后事件体，保留 SHOW CREATE 的完整定义。
- [x] 失败优先专项和严格事件回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍未完成；P1 聚合状态保持 `partial`。

Evidence: `reports/compatibility/p1-information-schema-events-definition-current-continuation1824.txt`。

### Continuation 1825

- [x] 本地 XA、native binlog、GTID、crash recovery、promotion 和 fencing 专项通过。
- [x] 已确认官方 MySQL 互操作测试当前因 fixture 环境变量缺失而跳过，不能把跳过当作通过。
- [ ] 继续推进官方 MySQL XA/binlog/GTID/恢复/提升互操作；在官方拓扑可用前保持 `partial`。

Evidence: `reports/compatibility/p2-native-replication-local-regression-current-continuation1825.txt`。

### Continuation 1826

- [x] 修正 native `INFORMATION_SCHEMA.PARAMETERS` 函数返回值行的 `PARAMETER_NAME=NULL`。
- [x] 保留 JDBC `COLUMN_NAME=RETURN_VALUE` 适配，并通过参数/例程回归。
- [ ] 继续收敛完整 I_S/P_S 字段精度及 P2/P3/P4 剩余聚合项。

Evidence: `reports/compatibility/p1-information-schema-parameters-return-name-current-continuation1826.txt`。

### Continuation 1827

- [x] 补齐 native `INFORMATION_SCHEMA.PARAMETERS` 的 ordinal/name/mode 过滤语义。
- [x] 失败优先专项和参数/例程回归通过。
- [ ] 继续收敛剩余完整 I_S/P_S、P2、P3、P4 聚合能力。

Evidence: `reports/compatibility/p1-information-schema-parameters-filters-current-continuation1827.txt`。

### Continuation 1828

- [x] 收紧 Performance Schema setup 表的 `ENABLED/TIMED/HISTORY` 更新列语义。
- [x] 失败优先专项及全部 setup 回归通过。
- [ ] 继续推进完整 P_S 组件生命周期、字段精度和权限聚合。

Evidence: `reports/compatibility/p1-performance-schema-setup-column-semantics-current-continuation1828.txt`。

### Continuation 1829

- [x] 补齐 native `INFORMATION_SCHEMA.PARAMETERS` 的 `IS NULL`/`IS NOT NULL` 过滤语义，区分函数返回值
  行与普通参数的 NULL/非 NULL 名称和模式。
- [x] 失败优先专项以及参数/例程/存储对象相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期和 P2/P3/P4 聚合项仍未完成。

Evidence: `reports/compatibility/p1-information-schema-parameters-null-predicates-current-continuation1829.txt`。

### Continuation 1830

- [x] XA 业务路径、XA 持久化恢复路径统一使用 owner-aware mutex 封装。
- [x] XA PREPARE 持锁期间，`performance_schema.mutex_instances.LOCKED_BY_THREAD_ID`
  可观察到实际会话线程 ID；失败优先、XA 和同步实例回归通过。
- [ ] 完整 P_S 锁/等待/线程生命周期、字段精度、组件/权限聚合和 P2/P3/P4 互操作仍未完成。

Evidence: `reports/compatibility/p1-performance-schema-xa-mutex-owner-current-continuation1830.txt`。

### Continuation 1831

- [x] INFORMATION_SCHEMA 四类权限视图支持 `IS NULL` / `IS NOT NULL` 谓词，按实际投影值的 NULL
  状态进行过滤。
- [x] 失败优先权限视图专项和权限视图回归通过。
- [ ] 完整角色/权限图、I_S/P_S 字段精度和运行时语义、P2/P3/P4 互操作仍未完成。

Evidence: `reports/compatibility/p1-information-schema-privilege-null-predicates-current-continuation1831.txt`。

### Continuation 1832

- [x] executor-owned mutex wait lifecycle now projects current, short-history and long-history rows.
- [x] `events_waits_summary_by_instance` now aggregates mutex waits using stable mutex instance identity.
- [x] Truncating the per-instance wait summary now resets retained mutex-history contributions.
- [x] XA mutex owner and related Performance Schema regression tests pass.
- [ ] Continue the remaining I_S/P_S component/permission/thread semantics and P2/P3/P4 interoperability.

Evidence: `reports/compatibility/p1-performance-schema-mutex-wait-lifecycle-current-continuation1832.txt`。

### Continuation 1833

- [x] Mutex waits now honor global and thread summary truncate/reset boundaries.
- [x] The thread wait-summary dispatcher passes its explicit dimension and returns the expected zero row after reset.
- [ ] Continue remaining I_S/P_S, P2, P3 and P4 aggregate compatibility work.

Evidence: `reports/compatibility/p1-performance-schema-mutex-summary-dimension-reset-current-continuation1833.txt`。

### Continuation 1834

- [x] Verified mutex wait projection into account summary with mapped session identity.
- [x] Verified account-summary reset isolation from the global summary.
- [ ] Continue remaining I_S/P_S and replication/client interoperability gates.

Evidence: `reports/compatibility/p1-performance-schema-mutex-account-summary-current-continuation1834.txt`。

### Continuation 1835

- [x] Re-ran the local native binlog/GTID/XA/recovery/promotion gate.
- [x] Confirmed official MySQL fixture tests are skipped because the required environment variables are absent.
- [ ] Keep P2/P4 official interoperability partial until the external fixture is available.

Evidence: `reports/compatibility/p2-native-local-and-official-fixture-audit-current-continuation1835.txt`。

### Continuation 1836

- [x] Verified independent short/long wait-history capacities for executor-owned mutex waits.
- [x] Verified dynamic long-history trimming after reducing its configured capacity.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial until the remaining table/field, component, permission and lifecycle semantics are independently verified.

Evidence: `reports/compatibility/p1-performance-schema-mutex-history-capacity-current-continuation1836.txt`。

### Continuation 1837

- [x] Added statement-lifetime `performance_schema.table_handles` projection for ordinary table accesses.
- [x] Verified cleanup, explicit-lock, transaction-lease, capacity, and metadata self-observation regressions.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial until full table/field and runtime lifecycle coverage is independently verified.

Evidence: `reports/compatibility/p1-performance-schema-table-handles-statement-lease-current-continuation1837.txt`。

### Continuation 1838

- [x] Added metadata-lock duration provenance for statement, transaction, and explicit lock lifetimes.
- [x] Verified granted and pending metadata-lock projections preserve the duration class.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial until full table/field and runtime lifecycle coverage is independently verified.

Evidence: `reports/compatibility/p1-performance-schema-metadata-lock-duration-current-continuation1838.txt`。

### Continuation 1839

- [x] Applied metadata-lock capacity before SQL filtering and made lost accounting use the full runtime collection.
- [x] Added a filtered-read regression proving the retained row stays visible, the evicted row does not reappear, and `Performance_schema_metadata_lock_lost` is one.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial until full table/field and runtime lifecycle coverage is independently verified.

Evidence: `reports/compatibility/p1-performance-schema-metadata-lock-capacity-filter-current-continuation1839.txt`。

### Continuation 1840

- [x] Applied table-handle capacity before SQL filtering and kept lost accounting based on the full live handle set.
- [x] Added the evicted-row non-resurrection regression and passed the combined table-handle/metadata-lock suite.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial until full table/field and runtime lifecycle coverage is independently verified.

Evidence: `reports/compatibility/p1-performance-schema-table-handle-capacity-filter-current-continuation1840.txt`。

### Continuation 1841

- [x] Applied `performance_schema.prepared_statements_instances` capacity before SQL filtering.
- [x] Verified an evicted prepared statement cannot be resurrected by a selective query while
  lost accounting remains based on the complete live inventory.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial until full table/field and runtime lifecycle
  coverage is independently verified.

Evidence: `reports/compatibility/p1-performance-schema-prepared-capacity-filter-current-continuation1841.txt`。

### Continuation 1842

- [x] Applied socket/file instance capacity before SQL filtering and retained full-set lost accounting.
- [x] Verified filtered reads cannot resurrect evicted socket or file instances.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial until full table/field and runtime lifecycle
  coverage is independently verified.

Evidence: `reports/compatibility/p1-performance-schema-file-socket-capacity-filter-current-continuation1842.txt`。

### Continuation 1843

- [x] Applied program-summary capacity before SQL filtering and retained full-set lost accounting.
- [x] Verified a selective query cannot resurrect an evicted program summary.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial until full table/field and runtime lifecycle
  coverage is independently verified.

Evidence: `reports/compatibility/p1-performance-schema-program-capacity-filter-current-continuation1843.txt`。

### Continuation 1844

- [x] Applied connection-summary capacity before SQL filtering and kept lost accounting based on the
  complete runtime identity set.
- [x] Applied the same ordering to table-lock, table-I/O, and index-usage summaries.
- [x] Added and passed selective-read regressions for retained and evicted rows.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial until full table/field and runtime lifecycle
  coverage is independently verified.

Evidence: `reports/compatibility/p1-performance-schema-summary-io-capacity-filter-current-continuation1844.txt`。

### Continuation 1845

- [x] Applied error-summary capacity before SQL filtering across global and identity dimensions.
- [x] Applied digest capacity before SQL filtering and retained complete-source lost accounting.
- [x] Added and passed selective-read regressions for evicted error and digest rows.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial until full table/field and runtime lifecycle
  coverage is independently verified.

Evidence: `reports/compatibility/p1-performance-schema-error-digest-capacity-filter-current-continuation1845.txt`。

### Continuation 1846

- [x] Applied `max_thread_instances` before SQL filtering on the complete sorted thread inventory.
- [x] Added and passed the evicted-thread non-resurrection regression.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial until complete field and runtime lifecycle
  coverage is independently verified.

Evidence: `reports/compatibility/p1-performance-schema-thread-capacity-filter-current-continuation1846.txt`。

### Continuation 1847

- [x] Re-verified the P1 Performance Schema capacity-before-filter regressions.
- [x] Re-verified local P2 native binlog/GTID/XA/recovery/promotion behavior.
- [x] Preserved the matrix boundary: implemented=18, partial=8, deferred=1, out_of_scope=2.
- [ ] Continue only with evidence-backed P1/P2 work; official MySQL fixtures, missing component
  runtimes, and the broader non-Connector/J client matrix remain external or incomplete inputs.

Evidence: `reports/compatibility/p1-p2-capacity-and-native-regression-current-continuation1847.txt`。

### Continuation 1848

- [x] Closed the role metadata predicate boundary for `IN`, `NOT IN`, `IS NULL`, and `IS NOT NULL`.
- [x] Verified focused, role/privilege, and full I_S/P_S targeted regressions.
- [ ] Continue the remaining P1 field/runtime/component audit and then the P2/P3/P4 boundaries.

Evidence: `reports/compatibility/p1-information-schema-role-predicate-filters-current-continuation1848.txt`。

### Continuation 1849

- [x] Added `IN` and `NOT IN` matching to the generic INFORMATION_SCHEMA privilege views.
- [x] Preserved equality, LIKE, and NULL predicate behavior.
- [x] Passed focused and role/permission regression families.
- [ ] Keep the aggregate P1 requirement partial pending complete official field/runtime/
  component evidence.

Evidence: `reports/compatibility/p1-information-schema-privilege-predicate-filters-current-continuation1849.txt`。

### Continuation 1850

- [x] Added `IN`, `NOT IN`, `IS NULL`, and `IS NOT NULL` matching to shared metadata filters.
- [x] Passed focused metadata and related tables/columns/constraints/catalog regressions.
- [ ] Keep the aggregate P1 requirement partial pending complete official field/runtime/
  component evidence.

Evidence: `reports/compatibility/p1-information-schema-metadata-predicate-filters-current-continuation1850.txt`。

### Continuation 1851

- [x] Added shared `IN`/`NOT IN`/NULL matching for mysql privilege metadata tables.
- [x] Preserved equality and LIKE behavior and passed the account/permission regression family.
- [ ] Keep complete mysql.* coverage and the aggregate P1 requirement partial pending
  full field/runtime/component and official MySQL evidence.

Evidence: `reports/compatibility/p1-mysql-system-privilege-predicate-filters-current-continuation1851.txt`。

### Continuation 1852

- [x] Added `IN`, `NOT IN`, and NULL predicate matching to mysql.role_edges and mysql.default_roles.
- [x] Preserved equality/LIKE behavior and passed the role-focused regression family.
- [ ] Keep complete mysql.* and aggregate P1 lifecycle/field/runtime coverage partial.

Evidence: `reports/compatibility/p1-mysql-role-table-predicate-filters-current-continuation1852.txt`。

### Continuation 1853

- [x] Added `IN` and `NOT IN` matching to INFORMATION_SCHEMA.PARAMETERS fields.
- [x] Applied it to ordinary parameters and function return rows while retaining equality/LIKE/NULL.
- [x] Passed parameter/routine regression coverage.
- [ ] Keep complete I_S/P_S field/runtime/component and lifecycle coverage partial.

Evidence: `reports/compatibility/p1-information-schema-parameters-in-predicates-current-continuation1853.txt`。

### Continuation 1854

- [x] Added `IN`, `NOT IN`, and NULL matching to mysql.procs_priv routine filters.
- [x] Preserved equality/LIKE and JDBC-compatible projections.
- [x] Passed the parameter/routine regression family.
- [ ] Keep complete mysql.* and aggregate I_S/P_S runtime/lifecycle coverage partial.

Evidence: `reports/compatibility/p1-mysql-procs-priv-predicate-filters-current-continuation1854.txt`。

### Continuation 1855

- [x] Re-ran the complete targeted INFORMATION_SCHEMA/PERFORMANCE_SCHEMA regression family.
- [x] Confirmed the 1849-1854 predicate changes do not regress existing P1 role, permission,
  runtime, capacity, and metadata behavior.
- [ ] Keep the aggregate P1 requirement partial pending complete component/runtime/field/lifecycle
  and official MySQL evidence.

Evidence: `reports/compatibility/p1-information-performance-predicate-regression-current-continuation1855.txt`。

### Continuation 1856

- [x] Ran the full serial Go command and captured all post-timeout package results.
- [ ] Do not claim repository-wide PASS: `server/innodb/engine` timed out at 2702.580s;
  the targeted I_S/P_S gate remains the authoritative evidence for continuations 1849-1855.

Evidence: `reports/compatibility/full-go-regression-current-continuation1856.txt`。

### Continuation 1865

- [x] Added global and account-level password verification policy:
  `password_require_current` and `PASSWORD REQUIRE CURRENT/OPTIONAL/DEFAULT`.
- [x] Enforced `REPLACE` for self-service password changes and projected the
  policy through `mysql.user` and `SHOW CREATE USER`.
- [ ] Keep the aggregate P1 requirement partial pending complete mysql.user,
  I_S/P_S, component, permission, and official interoperability evidence.

Evidence: `reports/compatibility/p1-password-verification-policy-current-continuation1865.txt`。

### Continuation 1866

- [x] Added durable `password_last_changed` and account `password_lifetime`,
  including `PASSWORD EXPIRE INTERVAL N DAY` and NEVER/DEFAULT reset behavior.
- [x] Added the dynamic `default_password_lifetime` variable and projected the
  lifetime metadata through `mysql.user` and `SHOW CREATE USER`.
- [ ] Password expiration scheduling/enforcement and full mysql.user parity
  remain open.

Evidence: `reports/compatibility/p1-mysql-user-password-lifetime-current-continuation1866.txt`。

### Continuation 1867

- [x] Expanded the default `mysql.user` projection to all MySQL 8.4 static
  privilege columns, backed by persisted grants and ALL expansion.
- [x] Passed the static privilege-column regression and related account/system
  table regression family.
- [ ] Dynamic privilege/role edge semantics, full I_S/P_S field/runtime parity,
  official replication/XA interoperability, and the full non-Connector-J
  client matrix remain open or externally blocked.

Evidence: `reports/compatibility/p1-mysql-user-static-privilege-columns-current-continuation1867.txt`。

### Continuation 1868

- [x] Connected account password lifetime metadata to authentication and made
  account-specific lifetime override the global default.
- [x] Rejected authentication after the configured lifetime elapses while
  preserving explicit password-expired state.
- [ ] Continue expired-password warning/change-flow protocol details and the
  remaining global I_S/P_S, P2, P3, and P4 gates.

Evidence: `reports/compatibility/p1-password-lifetime-auth-enforcement-current-continuation1868.txt`。
### Continuation 1869

- 完成 `SHOW CREATE USER` 权限门禁、当前账户哈希脱敏、`CURRENT_USER()` 目标解析和 MySQL
  8.4 默认账户子句输出。
- 验证：`go test ./server/innodb/engine -run '^TestShowCreateUserEnforcesSystemSchemaAndHashVisibility$|^TestShowCreateUserReflectsPersistedAuthenticationState$' -count=1 -timeout 10m` 通过。
- 全局 partial 项、Fulltext deferred 和非 InnoDB out_of_scope 边界保持不变。
### Continuation 1870

- 完成 `CREATE/ALTER USER ... ATTRIBUTE` JSON 账户属性生命周期及 COMMENT 键更新，支持 `ATTRIBUTE DEFAULT` 清除和 USER()/CURRENT_USER() 账户目标。
- 验证：`go test ./server/innodb/engine -run '^TestUserAttributesAccountSyntaxPersistsAndProjects$|^TestInformationSchemaUserAttributes' -count=1 -timeout 15m` 通过。
- 全局 partial、Fulltext deferred 和非 InnoDB out_of_scope 边界保持不变。

### Continuation 1871

- 完成账户 TLS 元数据和资源限制持久化：`REQUIRE CIPHER/ISSUER/SUBJECT` 及四类 `WITH`
  资源限制；补齐 `mysql.user` 投影、`SHOW CREATE USER` 回显和认证侧读取。
- 验证：`go test ./server/innodb/engine -run '^TestAccountTLSAttributesAndResourceLimitsPersistAndProject$' -count=1 -timeout 15m` 通过。
- 已将四类账户资源限制接入解耦协议的认证、普通查询和预处理执行路径并验证超限拒绝；其他
  协议适配器、警告/重试边界、完整 I_S/P_S、非 Connector-J 客户端矩阵和官方 XA/binlog
  互操作仍保持未完成；
  Fulltext deferred，非 InnoDB out_of_scope。

### Continuation 1872

- [x] Implemented `FAILED_LOGIN_ATTEMPTS` and `PASSWORD_LOCK_TIME N/UNBOUNDED`
  persistence, projection, and authentication enforcement.
- [x] Reset failed-login state on successful authentication, password change, and
  `ACCOUNT UNLOCK`; verified targeted and account-family regressions.
- [ ] Keep the aggregate P1/P2/P3/P4 gates open pending full system-schema/runtime
  parity, official replication/XA interoperability, and the non-Connector-J
  client matrix.

Evidence: `reports/compatibility/p1-failed-login-password-lock-current-continuation1872.txt`。

### Continuation 1873

- [x] Implemented expired-password capability negotiation and `ER_MUST_CHANGE_PASSWORD_LOGIN` default rejection.
- [x] Implemented restricted-session allowlist for current-account `SET PASSWORD` and `ALTER USER USER()/CURRENT_USER()` across COM_QUERY and prepared statements.
- [x] Clear the restriction after successful password change, including explicit and lifetime expiry paths.
- [x] Verified focused and full auth/net/protocol/dispatcher regressions; engine regression evidence is recorded separately because it is a long-running command.
- [ ] Keep global I_S/P_S, non-Connector-J client matrix, official XA/binlog interoperability, Fulltext, and out-of-scope non-InnoDB boundaries unchanged.

Evidence: `reports/compatibility/p1-expired-password-restricted-session-current-continuation1873.txt`。

### Continuation 1874

- [x] Added `IS NULL` and `IS NOT NULL` semantics to the shared Performance
  Schema summary filters.
- [x] Preserved NULL-aware filtering in the generic Performance Schema row
  projection path.
- [x] Verified the focused regression, the full Performance Schema test family,
  and the repository compile sweep.
- [ ] Keep complete I_S/P_S component/runtime/permission parity, official
  XA/binlog/GTID/crash/promotion interoperability, and the full non-
  Connector-J client matrix open pending their remaining evidence.

Evidence: `reports/compatibility/p1-performance-schema-summary-null-predicates-current-continuation1874.txt`。

### Continuation 1875

- [x] Fixed empty-result restoration for Performance Schema global/session
  variable filters.
- [x] Covered `IN`, `NOT IN`, `IS NULL`, and `IS NOT NULL` variable-name
  predicates while preserving unfiltered inventory behavior.
- [x] Verified focused, related, full P_S, and repository compile checks.
- [ ] Keep complete I_S/P_S component/runtime/permission parity and official
  XA/binlog/GTID/crash/promotion plus full non-Connector-J client evidence open.

Evidence: `reports/compatibility/p1-performance-schema-variable-filter-empty-result-current-continuation1875.txt`。

### Continuation 1876

- [x] Fixed MySQL system-table `IN`/`NOT IN` behavior for real NULL values in
  `mysql.server_cost` and `mysql.engine_cost`; NULL membership predicates now
  evaluate as UNKNOWN and are filtered from WHERE results.
- [x] Preserved explicit NULL predicates and existing equality/LIKE behavior;
  focused red/green and related optimizer-cost regressions pass.
- [ ] Keep complete I_S/P_S component/runtime/permission parity, official
  XA/binlog/GTID/crash/promotion interoperability, and the full non-
  Connector-J client matrix open.

Evidence: `reports/compatibility/p1-mysql-system-table-null-not-in-current-continuation1876.txt`。

### Continuation 1877

- [x] Distinguished absent metadata predicates from empty-string `=`/`LIKE`
  predicates in role and MySQL system-table projections.
- [x] Added regressions for empty role-edge and optimizer-cost filters; focused
  role/account regressions pass.
- [ ] Keep complete I_S/P_S component/runtime/permission parity, official
  XA/binlog/GTID/crash/promotion interoperability, and the full non-
  Connector-J client matrix open.

Evidence: `reports/compatibility/p1-mysql-metadata-empty-string-predicates-current-continuation1877.txt`。

### Continuation 1878

- [x] Fixed empty-string `=`/`LIKE` predicates for `INFORMATION_SCHEMA.PARAMETERS`
  schema/routine filters while preserving absent-filter behavior.
- [x] Added the empty-string regression to the parameter metadata family; the
  focused routine regression passes.
- [ ] Keep complete I_S/P_S component/runtime/permission parity, official
  XA/binlog/GTID/crash/promotion interoperability, and the full non-
  Connector-J client matrix open.

Evidence: `reports/compatibility/p1-information-schema-parameters-empty-string-predicates-current-continuation1878.txt`。

### Continuation 1883

- [x] 修正 `INFORMATION_SCHEMA.ST_SPATIAL_REFERENCE_SYSTEMS.SRS_ID` 的 `LIKE` 谓词语义，
  使 `srs_id LIKE '4%'` 能命中内置 4326，同时保留精确数值和显式空字符串过滤。
- [x] 添加失败优先回归，并通过完整 INFORMATION_SCHEMA 测试族和全仓编译。
- [ ] 保持完整 I_S/P_S 运行时、组件/权限生命周期、官方 XA/binlog/GTID/恢复互操作及
  非 Connector-J 全量客户端矩阵为后续全局任务；Fulltext 继续后置，非 InnoDB 继续排除。

Evidence: `reports/compatibility/p1-information-schema-srs-id-like-filter-current-continuation1883.txt`。

### Continuation 1884

- [x] 重新执行完整 P_S 回归和本地 native binlog/GTID/XA/recovery/promotion 回归，结果通过。
- [x] 实际启动非 Connector-J 客户端矩阵入口；由于当前安全调用未提供受保护密码，矩阵在
  客户端执行前停止，未猜测或写入凭据。
- [ ] 保持完整 P1 组件/权限语义、官方 P2/P4 互操作和 P3 全量客户端矩阵为未完成；
  MySQL CLI、Docker 和官方 fixture 的环境缺口继续记录为未验证。

Evidence: `reports/compatibility/p1-p2-p3-fresh-gate-audit-current-continuation1884.txt`。

### Continuation 1885

- [x] 重新执行本地账户、角色、授权表、权限视图和 partial revoke 回归，18.135 秒通过。
- [ ] 不因本地权限族通过而宣称完整 P1；完整 `mysql.*` 生命周期、组件运行时、官方
  MySQL XA/binlog/恢复/提升互操作和非 Connector-J 全量客户端矩阵仍保持未完成。

Evidence: `reports/compatibility/p1-local-privilege-role-fresh-audit-current-continuation1885.txt`。

### Continuation 1886-1892

- [x] 恢复 Docker 官方 MySQL 8.4 fixture，完成官方源端崩溃/重启/断线重连、网络分区、
  xmysql 反向 promotion，以及普通/XA 两阶段/XA one-phase 双向复制验证。
- [x] 完成 mysql CLI、Go、PyMySQL、Node mysql2 的基础协议/事务/元数据/类型/错误/连接池
  矩阵，并完成服务重启、TCP 故障和集群端点切换矩阵。
- [ ] P1 完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 的组件运行时、字段精度和权限生命周期
  仍未全部实现；Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

Evidence: `reports/compatibility/p2-p4-official-mysql-fresh-interoperability-current-continuation1886.txt`、
`reports/compatibility/p3-non-connector-client-fresh-matrix-current-continuation1888.txt`、
`reports/compatibility/p3-client-fault-and-cluster-fresh-current-continuation1892.txt`。

### Continuation 1894

- [x] 重新执行完整 I_S/P_S 测试族，192.762 秒通过。
- [x] 完成当前 P1 边界审计：共享官方字段合同无差异，未发现新的本地失败回归。
- [ ] 仅剩未实现的原生组件运行时/分配器、完整权限生命周期聚合语义；Fulltext 继续
  deferred，非 InnoDB 继续 out_of_scope。

Evidence: `reports/compatibility/p1-final-boundary-audit-current-continuation1894.txt`。

### Continuation 1900-1901

- [x] 完成官方 MySQL 8.4 与 xmysql 的 `mysql.*` 权限/角色生命周期对照，覆盖用户、库、
  表、列、动态权限、代理、角色边和默认角色；修正 `mysql.role_edges` 的 FROM/TO 官方方向。
- [x] 完成 P_S 原生运行时可支撑的组件权限、setup actors、同步实例容量/instrument 开关、
  官方容量变量、digest sample age 和 statement/table/file lost 计数切片。
- [x] 通过 I_S/P_S 联合回归、全仓编译、20 次解析错误测试隔离回归和串行全仓回归。
- [ ] 完整 I_S/P_S 仍不包含没有 xmysql 原生实现的 Scheduler、Clone、Keyring、Firewall、
  Thread Pool、NDB、Group Replication 组件运行时，以及没有原生 allocator 的精确 lost 统计；
  Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

Evidence: `reports/compatibility/official-mysql-privilege-lifecycle-current/privilege-lifecycle-20261003-204707.json`、
`reports/compatibility/p1-performance-schema-native-lifecycle-current-continuation1900.txt`、
`reports/compatibility/full-go-regression-current-continuation1901.txt`、
`reports/compatibility/scope-matrix-current-final.json`。
