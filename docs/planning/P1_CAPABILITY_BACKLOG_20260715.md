# XMySQL P1 Capability Backlog - 2026-07-15

## Scope

P1 items are important for broad MySQL compatibility, query quality, performance, and maintainability. They should not block the first production-grade core CRUD/JDBC path if every P0 item is closed and the limitation is documented.

## Current Global Scope Decision

- 纳入全局任务：完整 `INFORMATION_SCHEMA`、完整 `PERFORMANCE_SCHEMA`、非 Connector/J 客户端兼容矩阵、XA 与原生 binlog/复制/崩溃恢复互操作。
- 保持 P0 基线：MySQL 可启动、InnoDB 核心 CRUD、集群复制/故障切换、Connector/J 当前 139 项门禁。
- 本轮暂缓：FULLTEXT 及全文检索生态。
- 明确不处理：MyISAM、ARCHIVE、CSV 等非 InnoDB 引擎、非 InnoDB `REPAIR TABLE`、非 InnoDB 引擎转换；这些不再作为全局任务的未完成项。
- 实施计划：[`docs/superpowers/plans/2026-09-21-global-compatibility-completion-plan.md`](../superpowers/plans/2026-09-21-global-compatibility-completion-plan.md)。

## P1 Backlog

| ID | Area | Capability | Current State | Completion Definition |
|---|---|---|---|---|
| P1-OPT-001 | Optimizer | Complete predicate pushdown | CNF normalization, inner-join/aggregation-safe pushdown, INNER JOIN cross-table equality placement and single-column or same-arity tuple equality-key placement/constant transitivity, including equality keys nested inside conjunctive `ON`/`JoinConds` expressions, and predicates above LEFT/RIGHT JOIN now push only to the preserved-side child; simple and expression predicates with qualified `table.column` references are recognized from explicit schemas or TableScan metadata; direct-column projection aliases now rewrite outer predicates to child columns when no LIMIT/ORDER/DISTINCT semantic barrier exists; predicate pushdown, expression normalization, index-access selection, aggregation elimination, and MIN/MAX projection simplification now traverse `LogicalSubquery.Subplan` and CTE definition/query/anchor/recursive/body fields; CTE/UNION output-column discovery is available for join ownership analysis; predicates on the NULL-extended side remain above the join, while cross-child predicates remain above the join for outer joins; safe preserved-side predicates over LEFT/RIGHT equi-joins can also be inferred onto the nullable side for all compatible single-column comparisons and structured predicates, including multiple range predicates, `IS NULL`/`IS NOT NULL`, `IN`, `LIKE`, and constant-bound `BETWEEN`; correlated INNER/SEMI/ANTI Apply now carries constant/range/pattern/null-safe transitive predicates across equality `JoinConds`, while LEFT Apply keeps nullable-side inference disabled; standalone and explicit decorrelation `LogicalSubquery` boundaries are preserved so the optimizer cannot create an invalid one-child Apply; node-only IN/EXISTS rewrites now also preserve standalone subquery boundaries instead of producing one-child Apply plans; uncorrelated SEMI/ANTI Apply is preserved instead of being weakened to INNER; generic subquery predicate inference and richer outer-join expression inference remain incomplete | Predicates are pushed only when semantically safe and verified by plan/execution tests |
| P1-OPT-002 | Optimizer | Column pruning end-to-end | Logical projection/selection/join/aggregation pruning rewrites scan schemas and retains predicate/join dependencies; direct-column projection aliases are translated from required output names back to their child columns; `LogicalSubquery.Subplan` is traversed and correlated outer references are retained at the subquery boundary; `LogicalApply.JoinConds` contributes left/right join-key requirements; `LogicalCTE.Query`, recursive `Anchor/Recursive`, and CTE Statement `Definitions/Body` fields are traversed; `LogicalCTEScan` schemas are pruned by required columns; physical table scans and index scans now carry required columns and project row output; WHERE-only dependency columns are retained through storage-integrated scans even when absent from the projection; parallel table and index scans now project `RequiredColumns` from full real chunk rows; parallel index scans also accept a real `PlanRowReader` for the original physical node; clustered-index SELECT scans now expose a real `ScanProjected` path and retain predicate dependencies while dropping unused row-map columns; clustered record decoding now skips value conversion for unrequested encoded fields while still validating field boundaries; physical plan conversion now preserves Projection/Selection/Aggregation/Join/Apply child trees, and the parallel executor executes physical Values/Selection/Projection nodes; broader heterogeneous row-source integration, complex index-range partitioning, and full output-column lineage remain incomplete | Unused columns are removed through logical and physical plans without breaking metadata |
| P1-OPT-003 | Optimizer | Statistics, NDV, histogram refresh | ANALYZE persists row/column/index statistics; DML invalidates table stats, carries ModifyCount through the information-schema/table-manager cache boundary, refreshes histograms/NDV, reloads durable `.frm` statistics into the optimizer after engine restart, and now writes a versioned, metadata-fingerprinted, checksummed `.stats.json` sidecar with atomic publication; exact row counts use B+Tree leaf pages when available, and the integration layer now wires a production `StorageAccessor` that supplies exact decoded row counts, real data/index space sizes, decoded clustered-record samples through `ClusteredIndexScanner`, and index cardinality/B+Tree shape from `IndexManager`; enhanced histograms now use exact sample bucket distinct counts, non-overlapping numeric boundaries, deterministic frequency ordering, safe zero-bucket handling, and scaled bucket totals; non-unique single/composite index cardinality now derives distinct keys from decoded row samples with conservative sample scaling; invalid or unreadable sampled pages no longer become synthetic rows; a temporary space-manager lookup failure now preserves real row-count and space-size values supplied by the accessor; JOIN cost estimation now consumes column NDV and NULL ratios instead of a fixed 10% output assumption; `INFORMATION_SCHEMA.TABLES` and `SHOW TABLE STATUS` now project analyzed row/average-row/data/index sizes, with index length estimated from durable index page counters, and ANALYZE persists the same page-derived index byte estimate into `InfoTableStats.IndexSize`; full storage sampling breadth across every table/index variant and upstream physical stats-file fidelity remain incomplete | CBO uses real table/index stats with refresh, restart reload, invalidation behavior, and stats-aware JOIN estimates |
  | P1-OPT-004 | Optimizer | Multi-column index prefix matching | Continuous leading equality prefixes on composite secondary indexes use bounded index ranges; a following single range predicate now uses that prefix with residual filtering; structured `BETWEEN`, `NOT BETWEEN`, `IN`, `LIKE`, and `IS NULL` expressions now produce index conditions; constant single-column `IN` lists now execute as real secondary-index equality branches with primary-key deduplication and residual predicate validation; constant single-column `NOT IN` lists now scan the secondary-index namespace and reapply exact NULL-aware residual filtering; simple single-column `LIKE 'prefix%'` and constant single-column `NOT LIKE` now use the secondary-index namespace with exact residual LIKE filtering; simple single-column `IS NULL` now uses an exact NULL equality range with residual validation; simple single-column `IS NOT NULL` now uses the secondary-index namespace with residual NULL filtering; composite equality prefixes now also cover `IN`/`NOT IN`, `BETWEEN`/`NOT BETWEEN`, `LIKE`/`NOT LIKE`, `IS NULL`/`IS NOT NULL`, and NULL-safe equality `<=>` with bounded prefix scans plus residual filtering; OR branches made of equality predicates on the same composite leading prefix now scan each prefix range with primary-key deduplication and residual filtering; OR branches with a composite equality prefix followed by a simple range, `BETWEEN`/`NOT BETWEEN`, constant `IN`, or `<=>` now use the same prefix scan and exact residual filtering; parser-generated `LIKE`/`NOT LIKE`/`IN`/`NOT IN` binary predicates are recognized by index-condition extraction, with only prefix `LIKE`/`NOT LIKE` eligible for pushdown, fuzzy patterns conservatively falling back, and `IN`/`NOT IN` requiring a non-empty constant list; constant-left comparison predicates are normalized by operator reversal before index candidate generation; richer predicate forms remain incomplete | Equality/range predicates use valid prefixes of composite indexes |
| P1-OPT-005 | Optimizer | DNF and expression simplification | Bounded DNF conversion, NULL-safe structural simplification, recursive double-negation elimination, De Morgan rewrites, and associative flattening for boolean AND/OR are available through the plan package; arithmetic reassociation is intentionally excluded to preserve rounding and coercion semantics; expression normalization now also covers Apply join conditions, nested subquery plans, VALUES expressions, and CTE definition/query/anchor/recursive/body fields; plan numeric coercion now accepts both textual strings and textual `[]byte` values for comparison and arithmetic; compiled/runtime expression coverage now includes MySQL `CONV()` for bases 2..36 with signed values, `ORD()`, `MAKE_SET()`, `EXPORT_SET()`, `SOUNDEX()`, `INTERVAL()` boundary lookup, session read/set semantics for `LAST_INSERT_ID()` and `ROW_COUNT()`, `REGEXP_LIKE`/`REGEXP_INSTR`/`REGEXP_REPLACE`/`REGEXP_SUBSTR` match_type options (`c/i/m/n/u`), `GET_FORMAT()` with DATE/DATETIME/TIME format matrices, `CONVERT_TZ()` with fixed offsets, UTC/GMT aliases, named IANA zones and daylight-saving transitions, common `EXTRACT()` date units including `MICROSECOND`, common `DATE_FORMAT()` week/day/time specifiers (`%j/%w/%U/%u/%V/%v/%X/%x/%T/%r/%D`), and matching `STR_TO_DATE()` weekday/month/day-of-year/time specifiers (`%W/%a/%b/%j/%r/%T/%f`); executor-wide SQL expression coverage remains incomplete | Boolean expressions are normalized and simplified without changing NULL semantics |
| P1-OPT-006 | Optimizer | Rule ordering and conflict control | Fixed seven-stage logical rule pipeline; duplicate/nil rules are rejected and traceable | RBO rules run in deterministic order with conflict tests |
| P1-EXE-001 | Executor | SortMergeJoin execution | Physical merge join builder executes direct sort-merge for supported equi-keys and retains nested-loop fallback for unsafe predicates | Merge join executes directly when inputs are sorted or can be sorted efficiently |
| P1-EXE-002 | Executor | Batch processing | VolcanoExecutor exposes bounded-memory ExecuteBatches with Open/Next/Close lifecycle and callback backpressure; optional `BatchOperator` dispatch now propagates bounded batches through values, table/index scans, projection, filter, sort, limit, hash join, hash aggregation, CTE/recursive CTE/CTE Scan, window output, and IN/ANY/ALL subquery materialization while preserving row-oriented fallback; window functions still materialize their input once for frame semantics, and broader vectorized execution remains row-oriented | Scan/filter/project/aggregate paths support batch execution where useful |
| P1-EXE-003 | Executor | Parallel scan scheduling | Parallel scan workers are bounded by a semaphore, cancellation-aware, and scan/sort/hash-join results are assembled deterministically by chunk/partition; `PhysicalTableScan` and `PhysicalIndexScan` now support cancellation-aware chunk readers, required-column projection, and explicit failure when no real row source is configured; parallel index scans also preserve the original physical node for a `PlanRowReader`; supported parallel equi-hash joins accept a `PlanRowReader` plus explicit key pairs to execute real child rows, parallel sort accepts a real child row reader with stable chunk sorting/merge and now compares text/byte-string sort keys lexically instead of coercing non-numeric text to zero, and parallel hash aggregation accepts a real child row reader with grouped COUNT/SUM/AVG/MIN/MAX and NULL semantics; parallel HashAggregate now binds ordered multi-column inputs and supports multi-column `COUNT(DISTINCT ...)` and multi-input `JSON_OBJECTAGG`; a fully parallelized tree recursively wires scan children and auto-executes them through a shared row reader, while missing row contracts and unsupported physical nodes fail explicitly instead of returning synthetic rows; the parallel physical adapter now executes Values/Selection/Projection/Union, condition-driven INNER/LEFT MergeJoin, expression-derived HashAgg/StreamAgg, non-correlated and outer-context-bound correlated SCALAR/EXISTS/IN Subquery, SEMI/ANTI/INNER/LEFT Apply, and non-recursive/iterative recursive CTE materialization; correlated Apply re-executes the right plan per outer row and propagates qualified outer columns into nested Selection/Projection/Values expressions; Volcano HashJoin now derives single- and multi-column keys from the actual equality columns instead of assuming the first output column; the storage-integrated nested-loop JOIN path now evaluates range comparisons, boolean OR/NOT, BETWEEN/NOT BETWEEN, IS NULL/IS NOT NULL, and supported scalar function expressions with MySQL-style numeric-string coercion; complex index-range partitioning and broader physical-plan integration remain partial | Parallel workers have bounded resource control and deterministic result assembly |
| P1-EXE-004 | Executor | Expression evaluation optimization | Volcano projection/predicate hot paths compile common constant/column/binary/UNARY/NOT/CASE/tuple-membership/IS TRUE/FALSE/IS NULL/BETWEEN expressions and a whitelist of deterministic scalar functions once per operator; legacy SelectExecutor caches parsed projection ASTs and compiles safe arithmetic/predicate forms while retaining AST fallback for legacy CASE/coercion semantics and unsupported/non-deterministic functions; structured `InExpression` and `LikeExpression` ASTs now also use the compiled evaluator with preserved NULL/UNKNOWN behavior; `STR_TO_DATE`, `CONV`, `ORD`, `MAKE_SET`, `EXPORT_SET`, `SOUNDEX`, `INTERVAL`, session-aware `LAST_INSERT_ID` and `ROW_COUNT` are explicitly covered by the compiled-function whitelist regressions; broader compiled-expression/vectorized evaluation remains | Hot expression paths reduce allocations and repeated conversions |
| P1-SQL-001 | SQL | Subquery breadth | Uncorrelated scalar/IN/quantified rewrites, correlated scalar/predicate/EXISTS paths, correlated scalar projections with common outer `COALESCE`/arithmetic wrappers (including multiple correlated scalar subqueries in one projection expression), correlated scalar projection over a bounded single-layer derived outer source, correlated scalar projections with outer `LIMIT/OFFSET`, `GROUP BY/HAVING`, and `DISTINCT` applied after scalar evaluation with common post-DISTINCT `LIMIT/OFFSET`, correlated scalar aliases in outer `ORDER BY` with post-scalar numeric ordering and pagination, including mixed scalar-alias and qualified outer-column sort keys plus projection expressions such as `COALESCE(max_score, 0)`, and correlated scalar aliases in outer `HAVING` after `GROUP BY` with mixed qualified-column predicates, correlated EXISTS over single- and multi-source derived outer tables with outer projection/WHERE/multi-term `ORDER BY`, multi-source derived-table JOIN/LEFT/USING execution, non-recursive multi-source/chained/column-named CTE materialization including INSERT SELECT, and the supported recursive CTE main projection/aggregate/INSERT paths pass the current matrix; common single-table correlated `UPDATE/DELETE` with single- or composite-column primary keys now materializes `EXISTS/NOT EXISTS/IN/NOT IN` target keys and reuses the storage-integrated DML path; common single- and multi-assignment correlated scalar UPDATEs now materialize per-key scalar values through CASE rewrites, including empty-set `COUNT(*) = 0`, common outer `COALESCE`/arithmetic wrappers, and multiple correlated scalar subqueries inside one assignment expression; recursive `UNION DISTINCT` duplicate termination, common multi-column recursive anchor/member projections with main-query projection/filter/order, single-column `UPDATE/DELETE ... IN (SELECT ...)` materialization, common single-column recursive AST expressions, common multi-column recursive hierarchy expansion from an anchor table through `CTE JOIN` to a base table, multiple independent recursive definitions, a bounded inner JOIN over independent single-/multi-column recursive definitions, common `COUNT/SUM` aggregation with `GROUP BY` over that recursive CTE JOIN, and a bounded dual/literal-anchor recursive member inner/left/right-join graph over persisted base tables with `ON/WHERE`, null-extension and expression projection are now covered; complex nested/multiple correlated expressions beyond the supported wrapper form and broader correlated outer-query AST execution remain incomplete | Scalar, IN, EXISTS, and correlated subqueries pass a defined compatibility matrix |
| P1-SQL-002 | SQL | HAVING, DISTINCT, UNION variants | Common HAVING/DISTINCT/outer UNION ordering and limits pass; explicit `UNION DISTINCT` is consumed and follows default duplicate elimination, mixed UNION ALL/UNION duplicate semantics and unparenthesized INTERSECT precedence are implemented; INTERSECT/EXCEPT core DISTINCT/ALL paths and parenthesized SELECT branches with branch-local `ORDER BY/LIMIT` are covered; nested parenthesized set branches, including nested UNION/INTERSECT/EXCEPT branches, now materialize through the same executor; mixed INTERSECT/EXCEPT expressions used as derived-table sources now materialize through a raw compatibility path with outer projection, WHERE, aggregate/GROUP BY, DISTINCT, multi-column ORDER BY, INNER/LEFT/RIGHT/USING joins and LIMIT/OFFSET, including use as the right source of ordinary INNER/LEFT/RIGHT JOINs; set-operation outer `ORDER BY` now parses and evaluates complete expressions (including function/arithmetic expressions) with multi-term ordering; unsupported grammar remains for broader set expressions mixed with arbitrary query tails and full general AST combinations | Common aggregate and set-operation queries match MySQL semantics |
| P1-SQL-003 | SQL | Wider ALTER TABLE support | Common ADD/DROP/MODIFY/CHANGE/RENAME column/index metadata paths exist; `ALTER COLUMN ... SET/DROP DEFAULT` now updates durable `.frm` metadata, table-level `CREATE TABLE ... COMMENT`/`ALTER TABLE ... COMMENT` persists and is exposed through `SHOW CREATE TABLE` and `INFORMATION_SCHEMA.TABLES`, `ALTER TABLE ... DEFAULT CHARACTER SET/CHARSET ... COLLATE ...`, `ALTER TABLE ... CONVERT TO CHARACTER SET ... COLLATE ...`, `ALTER TABLE ... DEFAULT COLLATE=...`, and `ALTER TABLE ... ROW_FORMAT=...` update durable table options and metadata views, common `CREATE TABLE` `ENGINE`/`CHARSET`/`COLLATE`/`COMMENT`/`ROW_FORMAT` options are normalized into durable table metadata, `CREATE TABLE ... AUTO_INCREMENT=n` initializes the durable next-value state and `INFORMATION_SCHEMA.TABLES.AUTO_INCREMENT` reflects allocation, ALTER index success refreshes dictionary metadata and rebuilds existing rows before following DML, raw multi-operation/single-operation paths support `ADD/DROP ... IF [NOT] EXISTS`, ADD/MODIFY/CHANGE column paths preserve `FIRST/AFTER` order, common `ALGORITHM`/`LOCK` options, `ALTER TABLE ... FORCE`, and `ENGINE=InnoDB` are accepted, and the in-process compatibility path now coordinates same-table readers/writers: `LOCK=SHARED` blocks DML while allowing SELECT, `LOCK=EXCLUSIVE/DEFAULT` waits for both, and `LOCK=NONE` skips the DDL barrier; `CREATE TABLE ... LIKE ...` copies persisted definitions without rows, compound `ADD/DROP/MODIFY/CHANGE` clauses can be combined with common UNIQUE/PRIMARY/FOREIGN KEY/CHECK constraints with rollback on clause failure, named CHECK/foreign-key symbols can be removed with `DROP CONSTRAINT`, local and referenced foreign-key column references, CHECK expressions/definitions, and generated-column expressions are synchronized across same-database `.frm` metadata by `RENAME/CHANGE COLUMN`, and common `ON DUPLICATE KEY UPDATE`/`REPLACE` DML paths are implemented; background online rebuild/data movement, full MDL semantics, non-InnoDB engine conversion, and complex AST variants remain incomplete | Add/drop/modify column and index operations preserve dictionary, data, and indexes; same-table DDL lock behavior is deterministic |
| P1-SQL-004 | SQL | Foreign key and CHECK constraints | Implemented for CREATE/ALTER metadata, runtime validation, named/table-level CHECK enforcement, common inline column `REFERENCES` enforcement/cascade behavior, common inline column CHECK syntax including `CONSTRAINT <symbol> CHECK (...)`, CREATE/ALTER validation that referenced columns are covered by a parent-side index, compatible child/referenced column type checks, DDL validation that `ON DELETE/UPDATE SET NULL` uses nullable child columns, explicit rejection of unsupported `ON DELETE/UPDATE SET DEFAULT`, schema-wide/same-table foreign-key symbol uniqueness, rejection of missing local/referenced columns, and fallback preservation of explicit constraint symbols/actions; column-level `UNIQUE` now persists its implicit unique index metadata; automatic foreign-key indexes are removed only when no remaining foreign key needs them; foreign-key local/referenced column metadata and CHECK expressions remain consistent after same-database column rename; broader MySQL edge-case matrix remains | FK metadata, FK enforcement, CHECK evaluation, referenced-key index/type/nullability/column validation, symbol/action uniqueness, automatic-index lifecycle, and clear error behavior pass JDBC tests |
| P1-SQL-005 | SQL | Cascade update/delete actions | Implemented for RESTRICT/NO ACTION, SET NULL, CASCADE, including multi-level cascade propagation and rollback metadata | `ON DELETE` and `ON UPDATE` actions are enforced consistently with transaction semantics |
| P1-SQL-006 | SQL | BIT column compatibility | `BIT` is now treated as an integer-compatible metadata/storage type; `BIT(n)` values support insert, read, update, equality predicates, durable metadata validation, `INFORMATION_SCHEMA.COLUMNS.COLUMN_TYPE`, and JDBC `java.sql.Types.BIT` reporting | BIT columns preserve numeric value semantics through storage, predicates, result conversion, and JDBC metadata |
| P1-SQL-007 | SQL | CREATE TABLE AS SELECT | Common raw CTAS now executes the source SELECT, derives a target schema from result columns/types or honors explicit target-column definitions, accepts verified `ENGINE=InnoDB`/`CHARSET`/`COLLATE`/`ROW_FORMAT` options, creates a durable InnoDB table, materializes rows through the storage-integrated INSERT path, and removes the target on materialization failure; broader CTAS grammar remains incomplete | Common `CREATE TABLE target AS SELECT ...`, explicit-column CTAS, and supported InnoDB table options persist the projected schema and rows with rollback on failure |
| P1-SQL-008 | SQL | Temporary table lifecycle | Basic `CREATE TEMPORARY TABLE` definitions, temporary `AS SELECT` (including explicit target columns with and without the `AS` keyword), verified `ENGINE=InnoDB`/`CHARSET`/`COLLATE`/`ROW_FORMAT` options, and temporary `LIKE` are materialized into connection-owned physical tables; same-session name shadowing, cross-session isolation, qualified references, ordinary/temporary `DROP TABLE` including multi-table `DROP TEMPORARY TABLE` with `IF EXISTS`, `TRUNCATE TABLE`, session-local `ALTER/RENAME TABLE ... RENAME TO` mapping, basic `SHOW TABLES`/`SHOW FULL TABLES` visibility, session-aware `INFORMATION_SCHEMA.TABLES/COLUMNS/STATISTICS/TABLE_CONSTRAINTS/KEY_COLUMN_USAGE/CHECK_CONSTRAINTS/REFERENTIAL_CONSTRAINTS`, session-aware `SHOW CREATE TABLE`/`SHOW INDEX`, COM_RESET_CONNECTION cleanup, network disconnect cleanup, startup orphan scavenging, and common temporary-table DML commit/rollback semantics are implemented; full MySQL temporary-table grammar remains incomplete | Temporary tables are isolated to one connection and cannot leak into the persistent catalog after reset, disconnect, or engine restart |
  | P1-IDX-001 | Index | Index merge | Equality and simple range OR branches now merge secondary-index scans with primary-key deduplication and residual filtering; bounded DNF expansion also handles shared predicates around OR branches (for example `a = 1 AND (b = 2 OR c = 3)`); optimizer AND candidates now carry explicit intersection mode and multiplicative selectivity, with deterministic row-identity intersection helper; single-column and composite-secondary-index-leading equality/range predicates now execute primary-key intersection, clustered lookup, and residual filtering; composite OR branches now also accept trailing constant `IN`, `<>`, `!=`, and NULL-safe equality `<=>` predicates after a leading equality prefix, using prefix candidate scans plus exact residual filtering; range branches conservatively scan the index namespace because key encoding is not type-sortable; unbounded boolean expressions and richer multi-column forms remain partial | OR and multi-index predicates can combine index scans safely |
| P1-IDX-002 | Index | SHOW INDEX and index statistics | Durable definitions, exact index filtering, persisted index `Cardinality/LeafPages/NonLeafPages` preference, cardinality fallback, DML invalidation, `ANALYZE TABLE` logical `DataSize/AvgRowSize` persistence, and JDBC/INFORMATION_SCHEMA `PAGES` estimates from the clustered leaf chain or durable secondary-index page counter are covered; physical page-level estimator fidelity, index byte sizing, and full storage sampling remain partial | Metadata reflects current durable index definitions and cardinality estimates |
| P1-TXN-001 | Transaction | Deadlock and lock-wait diagnostics | Structured wait edges now include resource, lock type/mode, start time and duration; latest deadlock snapshot includes cycle, victim and wait duration, and SHOW ENGINE exposes the summary | Reports include wait graph, victim, wait time, and related transaction identifiers |
| P1-TXN-002 | Transaction | Long transaction management | Warning/critical thresholds, lock/undo limits, optional critical auto-rollback, sorted immutable diagnostics, and restartable monitoring are implemented; TransactionManager now registers RR/RC ReadViews with UndoPurger on Begin (including late purger attachment) and unregisters on commit/rollback/cleanup; purge protection now uses each active snapshot's low/high transaction-ID interval, allowing segments older than the low watermark to be reclaimed while retaining segments a snapshot can still distinguish | Long transactions are visible, bounded by policy, and safe for purge behavior |
| P1-TXN-003 | Transaction | Read-only transaction enforcement | `SET TRANSACTION READ ONLY`/session read-only state rejects client INSERT/UPDATE/DELETE, including JOIN DML, while allowing SELECT; replication replay is explicitly exempted so replicas can apply source changes; `SET TRANSACTION` can combine one isolation-level and one access-mode characteristic; duplicate/conflicting characteristics are rejected before any session/global state mutation; `START TRANSACTION` accepts comma-separated `WITH CONSISTENT SNAPSHOT` plus READ ONLY/READ WRITE in either order and rejects duplicate/unknown characteristics; broader isolation/transaction-variable lifecycle semantics remain incomplete | Read-only transactions reject writes with MySQL-compatible error behavior without blocking reads or replication apply |
| P1-SYS-003 | System variables | Global `read_only` enforcement | `SET GLOBAL read_only` now updates the shared system-variable store; `SHOW GLOBAL VARIABLES LIKE 'read_only'` reads the same state; the engine rejects ordinary client DML while retaining SELECT, replication replay, and SUPER/CONNECTION_ADMIN bypasses; system-variable enumeration no longer re-enters the manager read lock; temporary-table and finer MySQL privilege matrix semantics remain incomplete | Global read-only mode has an observable effect on real client DML and does not block replication apply or authorized administration |
| P1-SEC-001 | Security | Hardened auth and privilege model | Persistent user/host/plugin/password, global/database/table/column grants, roles and active-role checks are data-backed; host selection prefers exact and most-specific wildcard accounts; REQUIRE NONE/SSL/TLS/X509 transport requirements are replaceable and client-certificate checks are enforced; `mysql.user.ssl_type` reflects the persisted transport policy; caching_sha2 cleartext full-auth is accepted over TLS and non-TLS RSA public-key exchange; stored-function EXECUTE checks now cover ordinary projection/WHERE/nested arithmetic and active/default role inheritance; GRANT OPTION, REVOKE GRANT OPTION FOR, REVOKE ALL, multiple role grants and WITH ADMIN OPTION are persisted and exposed; object and PROXY grants/revokes support multiple grantees with full prevalidation; table/schema/column privilege metadata reports `IS_GRANTABLE` as YES/NO without emitting GRANT OPTION as a business privilege row; official static `PROXY` grants are accepted and projected through `mysql.proxies_priv`; multi-target CREATE USER/ROLE, ALTER USER, SET DEFAULT ROLE, DROP USER/ROLE and RENAME USER are prevalidated/updated atomically, with role binding cleanup; `CURRENT_ROLE()` and `INFORMATION_SCHEMA.ENABLED_ROLES` expose connection active-role state; `INFORMATION_SCHEMA.APPLICABLE_ROLES` and `ADMINISTRABLE_ROLE_AUTHORIZATIONS` expose role grants and admin options; `ROLE_TABLE_GRANTS`, `ROLE_COLUMN_GRANTS`, and `ROLE_ROUTINE_GRANTS` expose role table/column/routine privileges; `SET ROLE ALL EXCEPT` is supported; the 46 MySQL 8.4 built-in dynamic privileges now have a centralized case-insensitive registry, global-scope validation, unknown-name rejection, complete static/dynamic `SHOW PRIVILEGES` discovery, `GRANT ALL` expansion, and concurrency-safe runtime component registration/unregistration APIs, `mysql.global_grants` rows, correct grant-option lifecycle, and an additive `CheckDynamicPrivilege` API; account-management statements require the corresponding global privilege or self-service password change; common `INSERT ... SELECT` routes `INSERT` to the destination and `SELECT` to source tables, while `INSERT ... ON DUPLICATE KEY UPDATE` also checks destination `UPDATE`; common `CREATE VIEW` uses database-level `CREATE VIEW` plus source-table `SELECT`; `CREATE TABLE ... AS SELECT`/`LIKE` use target `CREATE` plus source `SELECT`; single-target `UPDATE JOIN` and `DELETE JOIN/USING` use target DML plus source `SELECT`; common `REPLACE` uses target `INSERT + DELETE`, and `REPLACE ... SELECT` also checks source `SELECT`; `CREATE OR REPLACE TRIGGER` routes through `TRIGGER` privilege; full grant-table lifecycle semantics and feature-specific enforcement remain | User/host/plugin/password, static privileges, registered dynamic privilege checks, complete privilege discovery, `GRANT ALL` dynamic expansion, runtime dynamic registry, active-role metadata, applicable/admin role metadata, role table/column/routine metadata, ALL EXCEPT activation, and common DML/DDL privilege routing are data-backed and test-covered |
| P1-OPS-001 | Operations | Backup and restore workflow | Versioned logical export/import now supports version-2 committed schema/data statements, cross-directory restore, commit-position PITR replay, and callback-based schema statement replay; XMySQL-specific physical snapshots now have a versioned tar/gzip format, file manifest, SHA-256 verification, atomic publication, safe restore staging, and engine-side sharp-checkpoint/flush integration; upstream-MySQL physical file compatibility and online hot-backup semantics remain out of scope | Logical and physical backup/restore can be run repeatedly with validation output and corrupted/path-unsafe archives are rejected |
| P1-OPS-002 | Operations | Performance benchmark suite | Stable PowerShell harness records repeatable engine/manager benchmark samples, allocations and exit status in JSON | Read/write/query benchmarks produce comparable latency and throughput reports |

### Continuation 142

- `server/innodb/storage/io/io_optimizer.go` no longer fabricates zero-filled pages or silently discards batch writes. `PageIO` is now an explicit durable backend contract; reads/writes delegate to it, failed batch entries remain buffered for retry, and page caches are isolated by `(spaceID,pageNo)`.
- Regression coverage: `io_optimizer_compatibility_test.go` verifies backend reads, cross-space cache isolation, batch persistence, and explicit failure when the backend is absent.
- P1-06 provider path is closed for the legacy page wrappers covered by this backlog: `page_impl.go`, `page/page_wrapper_base.go`, `page.BasePage`, `types.BasePage`, `types.BasePageWrapper`, `page.InodePageWrapper`, legacy `page.IndexPage`, `wrapper/system` FSP/XDES/IBuf/Dict/Trx, FSP/XDES/IBuf bitmap, Blob/Compressed/IBuf free-list, Undo, TrxSys and Encrypted wrappers now expose real provider-backed paths while preserving legacy constructors. `page.Allocated` remains intentionally manager-owned: its `Read/Write` lifecycle updates in-memory allocation state and the owning allocator persists `ToBytes()`.

### Continuation 109

- `server/innodb/engine/derived_table_compatibility.go` and `cte_compatibility.go` now apply every materialized `ORDER BY` term in sequence, preserving per-term ASC/DESC semantics before `LIMIT` in derived, aggregate-derived, and CTE compatibility paths.
- Regression coverage: `TestDerivedTablePreservesMultiColumnOrderBeforeLimit` and `TestDerivedAggregatePreservesMultiColumnOrderBeforeLimit`.

### Continuation 110

- Session metadata expressions now read connection state in the plan and engine paths: `DATABASE()`/`SCHEMA()`, `USER()`/`CURRENT_USER()`/`SESSION_USER()`/`SYSTEM_USER()`, `CURRENT_ROLE()`, `VERSION()`, and `CONNECTION_ID()`.
- The engine supplies these values to constant SELECTs, storage-backed projections, and compiled expressions; `SCHEMA()` is rewritten only when it is a function call so the parser's DDL keyword reservation does not break the SQL-standard alias.
- Regression coverage: `TestSessionMetadataFunctionsThroughSessionSQL` and `TestCompiledSessionMetadataFunctionsReadSessionValues`.
- Verification: full engine, full repository Go tests, cluster smoke, and the mandatory release candidate gate all passed; release report `reports/compatibility/release-candidate-current-continuation110/release-candidate.json` is `GO` with Connector/J `139` tests and no failures/errors/skips.

### Continuation 111

- Session metadata context now propagates through derived-table materialization and recursive CTE anchor/member/main projection paths, keeping `DATABASE()`/`USER()`/`CURRENT_ROLE()`/`CONNECTION_ID()` consistent with the owning client session instead of falling back to empty values or `NONE/0`.
- Regression coverage: `TestSessionMetadataFunctionsPropagateThroughDerivedProjection`, `TestSessionMetadataFunctionsPropagateThroughCTEProjection`, and `TestSessionMetadataFunctionsPropagateThroughRecursiveCTE`; targeted engine tests pass.

### Continuation 112

- Session expression context now also propagates through raw set-operation materialization and derived JOIN sources, so outer projections and predicates keep connection-local `DATABASE()`/`CURRENT_ROLE()`/`CONNECTION_ID()` semantics.
- Regression coverage: `TestSessionMetadataFunctionsPropagateThroughSetDerivedProjection` and `TestSessionMetadataFunctionsPropagateThroughDerivedJoinProjection`; targeted engine tests pass.

### Continuation 113

- `SET NAMES <charset> COLLATE <collation>` now synchronizes all three connection character-set parameters and `collation_connection` in both engine and dispatcher SET paths; dispatcher handles `names` as a session-only connection setting instead of silently ignoring it as an unknown system variable.
- Regression coverage: `TestSetNamesCollationUpdatesConnectionSession` and `TestSystemVariableEngine_SetNamesCollationSyncsSessionState`.
- Verification: engine/dispatcher full package tests, full repository Go tests, cluster smoke, and release candidate gate all passed; continuation113 release report is `GO` with Connector/J `139` tests and no failures/errors/skips.

### Continuation 114

- Global `read_only` enforcement now permits DML against connection-owned temporary tables while retaining the block for persistent-table writes, including after temporary logical names are rewritten to internal physical names.
- Regression coverage: `TestGlobalReadOnlyRejectsClientWritesButAllowsAdminAndReplay` now covers temporary-table write/read under global read-only mode.

### Continuation 115

- Read-only transaction enforcement now permits DML against the owning session's temporary tables while retaining rejection for persistent-table writes; the DML guard recognizes both logical and rewritten internal temporary names.
- Regression coverage: `TestReadOnlyTransactionRejectsSQLDML` covers temporary-table INSERT alongside persistent INSERT/UPDATE/DELETE rejection.
- Verification: engine full regression, cluster smoke, and continuation115 release candidate gate passed; release report is `GO` with Connector/J `139` tests and no failures/errors/skips.

### Continuation 116

- P1-SYS-003 新增全局 `super_read_only`：启用自动设置 `read_only=ON`，禁止包括 `SUPER`/`CONNECTION_ADMIN` 在内的客户端持久表 DML；复制回放与连接自有临时表例外保留；关闭后不清除已有 `read_only`，`read_only=OFF` 在 `super_read_only=ON` 时显式失败。
- engine/dispatcher `SET GLOBAL`、`SHOW GLOBAL VARIABLES` 和 DML 回归均已覆盖；新增测试通过。完整只读 DDL/锁等待语义仍是剩余边界。

### Continuation 117

- P1-SQL-003/P0-Connector.J 清理生命周期补齐：`DROP DATABASE` 对受管 tablespace 删除增加 Windows 文件句柄有界重试（50×20ms），避免批量 JDBC 事务后最后一个 `.ibd` 短暂占用导致 `Access is denied`。
- 新增 active tablespace 删除回归并通过；全仓 Go、集群 smoke 及最新 Connector/J 发布候选均通过。`release-candidate-current-continuation117.json`：`GO`、9/9 PASS、139 tests、0 failures/errors/skips。
- 完整 online DDL/MDL 仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 118

- 修正 P1-SYS-003 与官方 MySQL 的变量联动：`SET GLOBAL read_only=OFF` 会隐式关闭 `super_read_only`，engine/dispatcher 均已覆盖；`super_read_only=ON` 仍强制 `read_only=ON` 并阻止包括管理员在内的持久表 DML。
- 定向 `super_read_only` engine/dispatcher 回归通过。

### Continuation 119

- P1-TXN-003 补齐一段隐式事务生命周期：`autocommit=0` 首个 DML 建立活动事务，`SET autocommit=1` 对其执行隐式提交并清理状态；新增跨连接可见性回归通过。
- 更广泛的 isolation/transaction-variable 生命周期语义仍保留为剩余边界。

### Continuation 120

- 最新验证：全仓 Go 回归通过；cluster smoke `p1-cluster-current-continuation119` 为 `PASS`；release candidate `release-candidate-current-continuation119` 为 `GO`，9/9 PASS，Connector/J 139 tests 且无 failures/errors/skips。
- 这只证明当前兼容子集和 P0 主链路；FULLTEXT、非 Connector/J 全量客户端及完整 online DDL/MDL 等边界仍未完成。

### Continuation 121

- P1-SYS-003 扩展到持久 DDL：`read_only` 拦截普通客户端的持久 CREATE/ALTER/DROP/TRUNCATE，`super_read_only` 进一步拦截管理员；临时表 DDL、`ANALYZE TABLE` 与 replication replay 保持例外。
- 定向只读 DDL/临时表/复制回放回归通过；完整 MDL/锁等待和更细的 MySQL 管理语句矩阵仍是剩余边界。

### Continuation 122

- P1-SYS-003 继续收口全局只读切换边界：启用 `read_only`/`super_read_only` 时，若当前连接仍有活动事务或持有 `LOCK TABLES` 显式表锁，engine/dispatcher 现在拒绝切换并返回明确错误。
- 修正多表 `DROP TABLE` 的只读判断：混合临时表与持久表的语句不再只检查第一个目标而误放行；在当前支持范围内按持久写入保守拦截。
- 回归覆盖未提交事务、显式表锁和混合多表 DROP；engine 全量回归通过（`go test ./server/innodb/engine -count=1 -timeout 30m`，111.798s）。
- 全仓 Go 回归通过（engine 117.310s）；cluster smoke `p1-cluster-current-continuation122` 为 `PASS`；release candidate `release-candidate-current-continuation122` 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`（11:26）。
- 完整 MDL/锁等待、复杂多目标 DDL 的精确 MySQL 矩阵仍是剩余边界；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 123

- P1-TXN-003 将 `START TRANSACTION` 的已有事务边界改为显式隐式提交：提交账户变更和 replication statement，记录事务历史，更新 active-transaction 计数，再建立新事务；避免仅清理旧 journal 状态。
- 新增跨连接回归 `TestStartTransactionCommitsPreviousActiveTransaction`；engine 定向与全量 Go 回归通过（engine 110.208s，全仓 engine 116.604s）。
- cluster smoke `p1-cluster-current-continuation123` 为 `PASS`；release candidate `release-candidate-current-continuation123` 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`（11:12）。
- 更广泛的 isolation/transaction-variable 生命周期、完整 MDL/锁等待及复杂 DDL 精确矩阵仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 124

- P1-SYS-003 精确化临时表例外：全目标均为当前连接临时表时，多表 `DROP TABLE` 在 `read_only` 下允许执行；临时表与持久表混合时继续阻断，避免只检查首目标造成绕过。
- 定向与 engine 全量回归通过（110.656s）；cluster smoke `p1-cluster-current-continuation124` 为 `PASS`；release candidate `release-candidate-current-continuation124` 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`（11:18）。
- 完整 MDL/锁等待、复杂多目标 DDL 精确矩阵、剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 125

- P1-SQL-003 补齐锁定读的元数据锁分类：`SELECT ... FOR UPDATE`、`SELECT ... LOCK IN SHARE MODE` 及 `SKIP LOCKED` 现在进入 DML 级锁路径，DDL 会等待其释放；普通 SELECT 仍走读锁路径。
- 定向锁协调器与 engine 全量回归通过（111.160s）；cluster smoke `p1-cluster-current-continuation125` 为 `PASS`；release candidate `release-candidate-current-continuation125` 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`（11:10）。
- 完整 MDL/在线 DDL 的更多锁模式、剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 126

- P1-SQL-003 再补齐 MySQL 8 锁定读写法：`SELECT ... FOR SHARE` 与既有 `FOR UPDATE`、`LOCK IN SHARE MODE` 统一进入 DML 级元数据锁路径。
- 先行回归确认 `FOR SHARE` 缺口，修复后定向与 engine 全量回归通过（110.602s）；cluster smoke `p1-cluster-current-continuation126` 为 `PASS`；release candidate `release-candidate-current-continuation126` 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`（11:09）。
- 完整 MDL/在线 DDL 的更多锁模式、剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 127

- P1-SQL-003 继续补齐事务边界：显式事务或 `autocommit=0` 下获取的表元数据锁现在保留到 `COMMIT`/`ROLLBACK`/连接重置；DDL 不再在事务内的 SELECT 返回后提前穿透。
- 新增 `TestExplicitTransactionHoldsMetadataLockUntilCommit`，先行回归验证旧行为确实会让 DDL 提前完成，修复后通过；同时修正 `LOCK TABLES` lease 的 SQL 请求模式与物理协调锁模式分离，避免释放 READ lease 时解错 RWMutex。
- engine 全量回归通过（111.189s）；cluster smoke `p1-cluster-current-continuation127` 为 `PASS`；release candidate `release-candidate-current-continuation127` 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 更完整的 MDL 锁兼容矩阵、online DDL/多表 DDL 精确语义、剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。本轮未重复执行全仓 `go test ./...`，以 engine、release gate 的 go-core 和 Connector/J 证据为准。

### Continuation 128-129

- 将 CREATE/DROP/TRUNCATE 的解析 DDL 与 raw 多表 DROP 接入共享 DDL 写锁；engine 全量回归（111.119s）和 cluster smoke continuation128 通过。
- continuation128/129 的 release gate 分别暴露同一连接 DDL 的隐式提交缺口：Connector/J 139 项中各有 11 个错误，原因是 `autocommit=0` 下普通 SELECT 的事务级 MDL lease 在未 materialize `in_transaction` 时未被 `SET autocommit=1` 清理；两轮均为 `NO-GO`，未作为通过证据使用。

### Continuation 130

- P1-SQL-003/P1-TXN-003 修复上述边界：`SET autocommit=1`、同连接 CREATE/DROP/TRUNCATE DDL 会清理“有 lease 但尚未 materialize 活动事务”的元数据锁；显式活动事务仍通过隐式提交路径提交并释放锁。
- 新增 `TestAutocommitOnReleasesReadMetadataLeaseWithoutMaterializedTransaction`，覆盖 `autocommit=0` 普通 SELECT、`SET autocommit=1` 和后续 TRUNCATE；定向回归通过。
- cluster smoke `p1-cluster-current-continuation130` 为 `PASS`；release candidate `release-candidate-current-continuation130` 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`；本轮 gate 的 integration/go-core engine 均通过。
- 更完整的 MDL 锁兼容矩阵、ALTER/RENAME/数据库级 DDL 的全部隐式提交边界、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 131-132

- continuation131 尝试把 ALTER/RENAME 也纳入 DDL 隐式提交时，engine 全量回归发现 raw ALTER 识别条件误排除了 `RENAME TABLE`，因此该轮未作为通过证据。
- continuation132 恢复 RENAME TABLE raw 路径并保留真正 ALTER/RENAME 的隐式提交；engine 全量回归通过（110.496s），cluster smoke `p1-cluster-current-continuation132` 为 `PASS`，release candidate `release-candidate-current-continuation132` 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 数据库级 DDL 的全部隐式提交边界、更完整的 MDL 锁兼容矩阵、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 133-134

- P1-SQL-003 扩展 DDL 隐式提交到 CREATE/DROP DATABASE；新增数据库级 DDL 事务 lease 回归，engine 全量回归通过（110.469s），cluster smoke continuation133 为 `PASS`，release candidate continuation133 为 `GO`（Connector/J 139 tests、0 failures/errors/skips）。
- P1-SQL-003 再补 `RENAME TABLE` 多表源/目标 MDL：同一张表的事务内读会阻塞跨连接 rename，新增 `TestExplicitTransactionBlocksRenameUntilCommit`；engine 全量回归通过（110.897s），全仓 `go test ./... -count=1 -timeout 30m` 通过（engine 114.726s），cluster smoke continuation134 为 `PASS`，release candidate continuation134 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- continuation135 重新验证 `RENAME TABLE` 多表源/目标锁 key 去重：engine 全量回归通过（109.666s），cluster smoke 为 `PASS`，release candidate 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- continuation136 补齐 `DROP DATABASE` 的数据库级 MDL：删除数据库前按稳定顺序获取库内受管表的 DDL 写锁，跨连接事务内普通 SELECT 持有表级 lease 时会阻塞 DROP DATABASE，COMMIT 后继续完成；新增 `TestExplicitTransactionBlocksDropDatabaseUntilCommit`，事务/DDL 锁专项回归和 engine 全量通过（110.632s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation136/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation136/cluster-report.json) 为 `PASS`，release candidate [`reports/compatibility/release-candidate-current-continuation136/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation136/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- continuation137 按 MySQL 8.4 隐式提交语义补齐已实现的存储对象/视图 DDL：`CREATE/DROP/ALTER PROCEDURE/FUNCTION/EVENT/TRIGGER` 与 `CREATE/ALTER/DROP VIEW` 在执行前结束当前事务级 MDL lease；临时表路径不受影响。新增 `TestStoredObjectAndViewDDLImplicitlyCommitTransaction`，engine 全量回归通过（110.075s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation137/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation137/cluster-report.json) 为 `PASS`，release candidate [`reports/compatibility/release-candidate-current-continuation137/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation137/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- continuation138/139 继续收口表空间 DDL：`CREATE/ALTER/DROP TABLESPACE` 与 `ALTER TABLE ... TABLESPACE`/`DISCARD|IMPORT TABLESPACE` 在已实现路径上统一执行隐式提交；表级迁移、discard/import 现在还获取 DDL 写锁，事务内读会阻塞跨连接表空间迁移。新增 `TestTablespaceDDLImplicitlyCommitsTransaction` 与 `TestExplicitTransactionBlocksAlterTableTablespaceUntilCommit`；engine 全量回归通过（109.803s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation139/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation139/cluster-report.json) 为 `PASS`，release candidate [`reports/compatibility/release-candidate-current-continuation139/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation139/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- continuation140 补齐 `LOCK TABLES` 的隐式提交：获取显式表锁前提交活动 DML 并释放旧事务级 MDL lease，新增 `TestLockTablesImplicitlyCommitsActiveTransaction` 覆盖跨连接可见性和会话状态；engine 全量回归通过（110.796s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation140/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation140/cluster-report.json) 为 `PASS`，release candidate [`reports/compatibility/release-candidate-current-continuation140/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation140/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- continuation141 补齐 admin DDL 的隐式提交：`FLUSH TABLES` 与 `ANALYZE/CHECK/OPTIMIZE/REPAIR TABLE` 执行前结束活动事务并释放事务级 MDL lease，新增 `TestFlushAndTableMaintenanceImplicitlyCommitActiveTransaction`；engine 全量回归通过（111.128s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation141/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation141/cluster-report.json) 为 `PASS`，release candidate [`reports/compatibility/release-candidate-current-continuation141/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation141/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- continuation142 首次补账户管理 DDL 隐式提交时误把 `GRANT/REVOKE` 也提前提交，触发现有 `TestAccountGrantChangesCommitAndRollbackWithSessionTransaction` 的暂存/回滚契约；该轮 engine 为 `NO-GO`，不作为通过证据。
- continuation143 收窄账户边界：`CREATE/ALTER/DROP USER/ROLE`、`SET PASSWORD`、`RENAME USER` 在执行前隐式提交，项目既有会话暂存的 `GRANT/REVOKE` 保持 COMMIT/ROLLBACK 语义；新增 `TestAccountManagementDDLImplicitlyCommitsActiveTransaction`，engine 全量回归通过（110.515s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation143/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation143/cluster-report.json) 为 `PASS`，release candidate [`reports/compatibility/release-candidate-current-continuation143/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation143/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 更完整的 MDL 锁兼容矩阵、数据库级/存储对象 DDL 的所有隐式提交边界、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 144

- P1-SQL-003 继续补齐 View 的 MDL 协调：`CREATE/ALTER/DROP VIEW` 现在对已解析的视图对象获取有序 DDL 写锁；跨连接事务内普通 SELECT 或显式表锁持有视图元数据 lease 时，View DDL 会等待其释放，避免视图对象变更绕过已有 MDL 协议。
- 新增 `TestViewDDLWaitsForExplicitViewMetadataLock`，先行回归验证 `DROP VIEW` 在另一连接的显式视图锁释放前保持等待；修复后通过。engine 全量回归通过（113.131s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation144/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation144/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation144/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation144/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 更完整的 View 依赖对象/跨库对象协调、MDL 锁兼容矩阵、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 145

- P1-SQL-003 继续收口 View 依赖对象 MDL：`CREATE/ALTER VIEW` 在持有 View 自身 DDL 写锁之外，现在递归收集嵌套 View 的源对象，并对嵌套 View 与最终基表获取有序共享元数据锁；源对象被另一连接以 `LOCK TABLES ... WRITE` 持有时，View DDL 会等待释放。
- 新增 `TestCreateViewWaitsForExplicitSourceMetadataLock` 与 `TestCreateViewWaitsForNestedViewSourceMetadataLock`；后者先行复现旧实现会提前完成，修复后定向回归通过。engine 全量回归通过（112.530s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation147/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation147/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation147/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation147/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 更完整的跨库 View 依赖、循环/复杂 AST 依赖解析、MDL 锁兼容矩阵、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 146

- P1-SQL-003 补齐 View 元数据读取路径：`SHOW CREATE VIEW` 现在获取会话级共享 MDL；另一连接持有 `LOCK TABLES view WRITE` 时，查询会等待释放，避免专用 metadata handler 绕过 View 锁协议。
- 新增 `TestShowCreateViewWaitsForExplicitViewMetadataLock`；先行回归确认旧实现会在锁持有期间直接返回，修复后 View/嵌套 View 定向回归通过。engine 全量回归通过（111.505s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation148/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation148/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation148/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation148/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 更完整的 View 依赖/metadata handler 矩阵、跨库对象协调、MDL 锁兼容矩阵、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 147

- P1-SQL-003 补齐表元数据读取路径：`SHOW CREATE TABLE` 现在对持久表获取会话级共享 MDL；另一连接持有 `LOCK TABLES table WRITE` 时，查询会等待释放，避免专用 SHOW handler 绕过表锁协议；临时表路径保持原有逻辑。
- 新增 `TestShowCreateTableWaitsForExplicitTableMetadataLock`；先行回归确认旧实现会在锁持有期间直接返回，修复后通过。engine 全量回归通过（111.360s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation154/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation154/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation154/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation154/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括其他专用 metadata handler 的完整 MDL 矩阵、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 1011

- P1/P0 客户端元数据补齐 `INFORMATION_SCHEMA.COLUMN_STATISTICS`、`TABLESPACES`、`FILES` 的合法路由和 MySQL 列形状；当前返回明确的空结果集，因为 XMySQL 尚未持久化直方图及通用 tablespace-file 元数据，不将空壳冒充真实统计数据。
- P1-SQL-001 扩展递归 CTE：`WITH RECURSIVE` 后续普通 CTE 可以消费前面已物化的递归 CTE，例如 `nums -> doubled -> 主查询`；校验逻辑只把实际自引用定义标记为 recursive，避免把 statement-level `RECURSIVE` 错误施加到所有定义。
- P1-OPT 分区裁剪：同一谓词的 `OR` 分支求并、`AND` 分支求交，并同步 SELECT 候选行裁剪；不支持的表达式继续保守保留全量分区。
- 回归：`TestInformationSchemaAuxiliaryTablesReturnStableMetadataShapes`、`TestRecursiveCTECompatibilitySupportsDependentNonRecursiveDefinition`、`TestPartitionedBTreeManagerUnionsTopLevelOrRanges`；engine 全量 113.768s、全仓 Go 124.196s；集群 [`reports/compatibility/p1-cluster-current-continuation1011/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1011/cluster-report.json) PASS；发布候选 [`reports/compatibility/release-candidate-current-continuation1011/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1011/release-candidate.json) GO，9/9 checks PASS，Connector/J 139/0/0/0。

### Continuation 1012

- P1-OPT 分区访问继续支持数值常量 `IN (...)`：RANGE/LIST 分区对每个等值点求并，和已有 `AND` 求交、`OR` 求并组合；包含函数、动态值、类型转换或无法证明的谓词仍全量扫描。
- 新增 `TestPartitionedBTreeManagerPrunesConstantInValues`；最新 engine 全量 113.768s，集群 [`reports/compatibility/p1-cluster-current-continuation1012/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1012/cluster-report.json) PASS；发布候选 [`reports/compatibility/release-candidate-current-continuation1012/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1012/release-candidate.json) GO，9/9 checks PASS，Connector/J 139/0/0/0。
- 仍未完成：完整动态插件生命周期、全量 I_S/P_S、递归 CTE 任意复杂 AST/相关递归、函数和复杂类型分区剪枝、物理分区路由、完整 online DDL/MDL、XA 原生日志与更细权限矩阵及其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 179

- P1-OPT-001 继续补齐外连接表达式推导：LEFT/RIGHT 等值连接现在可识别两侧同形、单侧、确定性的算术连接键（例如 `a.id + 1 = b.id + 1`），将保留侧对该完整表达式的比较/结构化谓词复制到 nullable 侧；含无关列、非确定性/聚合函数和不受支持表达式仍保守跳过。
- 新增 `TestPredicatePushdownInfersOuterJoinWrappedJoinKeyExpression`，先行验证旧实现无法从表达式连接键推导过滤，修复后计划包与已有外连接结构化谓词回归通过；原始保留侧条件保持不变。
- 本轮验证：`go test ./server/innodb/plan -count=1` 通过；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（115.843s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation179/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation179/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation179/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation179/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括通用子查询谓词推导、更广泛 outer-join 表达式/函数推导、并行/异构 row-source 的完整列裁剪、统计采样全覆盖、复杂 SQL AST、触发器跨表副作用/递归依赖的更细 MDL 矩阵、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 180

- P1-OPT-001 继续扩大外连接等值键推导：在算术表达式基础上，现支持白名单内的确定性标量函数包裹连接键（如 `ABS(a.id) = ABS(b.id)`），并可复制保留侧的比较谓词；`RAND` 等非确定性函数、聚合函数和含跨侧/无关列表达式仍明确拒绝。
- 新增 `TestPredicatePushdownInfersOuterJoinFunctionWrappedJoinKeyExpression` 与 `TestPredicatePushdownRejectsNondeterministicOuterJoinFunctionKey`，先行失败后转绿，覆盖正向推导和安全拒绝边界。
- 本轮验证：`go test ./server/innodb/plan -count=1` 通过；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（117.394s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation180/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation180/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation180/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation180/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括通用子查询谓词推导、更广泛函数/类型和 NULL 敏感表达式推导、并行/异构 row-source 的完整列裁剪、统计采样全覆盖、复杂 SQL AST、触发器跨表副作用/递归依赖的更细 MDL 矩阵、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 181

- P1-OPT-004 补齐索引边界常量表达式：索引条件提取现在可折叠不引用列、无副作用的算术常量（例如 `col1 >= 1 + 2`），再按 `col1 >= 3` 生成索引候选；含列引用、非算术函数或求值错误的表达式仍不折叠。
- 新增 `TestConstantArithmeticExpressionUsesIndexPushdown`，覆盖先失败后通过的候选生成与折叠值断言。
- 本轮验证：`go test ./server/innodb/plan -count=1` 通过；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（115.903s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation181/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation181/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation181/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation181/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range partitioning、通用子查询谓词推导、并行/异构 row-source 的完整列裁剪、统计采样全覆盖、复杂 SQL AST、真正 online DDL/多表 DDL 精确语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 183

- P1-OPT-001/P1-SQL-001 继续补齐关联 Apply 推导：INNER/SEMI/ANTI Apply 的等值关联键现在可使用两侧同形、确定性的算术/标量函数表达式（例如 `ABS(left.id) = ABS(right.id)`），把一侧的安全比较谓词复制到另一侧；LEFT Apply 仍保持 nullable-side 禁止推导。
- 新增 `TestPredicatePushdownInfersApplyFunctionWrappedCorrelation`，先行验证旧实现无法推导函数包裹关联键，修复后与直接列、NULL-safe、嵌套选择条件回归一起通过。
- 本轮验证：`go test ./server/innodb/plan -count=1` 通过；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（115.987s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation183/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation183/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation183/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation183/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括通用子查询谓词推导、复杂/多层相关 AST、完整 Apply 语义矩阵、复杂 index-range partitioning、并行/异构 row-source 的完整列裁剪、统计采样全覆盖、真正 online DDL/多表 DDL 精确语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 184

- P1-OPT-002 补齐派生投影的输出列裁剪：当上层 row-source 只请求投影中的部分输出时，当前投影现在会删除未使用的直接/计算表达式，并只向子计划保留实际依赖列；`DISTINCT`、`ORDER BY`、`LIMIT`、`OFFSET` 语义屏障下保持完整投影。
- 新增 `TestColumnPruningDropsUnusedComputedProjectionOutputs` 与 `TestColumnPruningRetainsDistinctProjectionOutputs`，覆盖计算列血缘和 DISTINCT 保留边界。
- 本轮验证：`go test ./server/innodb/plan -count=1` 通过；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.227s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation184/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation184/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation184/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation184/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛异构 row-source/复杂输出血缘、复杂 index-range partitioning、通用子查询谓词推导、统计采样全覆盖、复杂 SQL AST、真正 online DDL/多表 DDL 精确语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 182

- P1-OPT-004 继续扩展索引边界常量折叠：索引条件提取现在也支持白名单内确定性标量函数对常量的计算（例如 `col1 >= ABS(-3)`），并复用相同的无列/无副作用/求值成功约束。
- 新增 `TestConstantScalarFunctionExpressionUsesIndexPushdown`；算术常量、函数常量和既有字面量索引条件回归均通过。
- 本轮验证：`go test ./server/innodb/plan -count=1` 通过；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（115.471s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation182/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation182/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation182/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation182/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range partitioning、通用子查询谓词推导、并行/异构 row-source 的完整列裁剪、统计采样全覆盖、复杂 SQL AST、真正 online DDL/多表 DDL 精确语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 149

- P1-SQL-003 补齐 `ANALYZE/CHECK/OPTIMIZE TABLE` 专用维护路径的 MDL：维护语句现在先解析全部目标，按稳定顺序获取共享表元数据锁，再执行统计/一致性维护，避免绕过公共 statement lock 协调。
- 新增 `TestAnalyzeTableWaitsForExplicitTableMetadataLock`；先行回归确认旧实现会直接完成，修复后维护、隐式提交与锁等待矩阵通过。engine 全量回归通过（112.086s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation160/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation160/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation160/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation160/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括 `REPAIR TABLE`/其他专用 metadata handler 的完整 MDL 矩阵、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 148

- P1-SQL-003 补齐 `DESCRIBE/DESC table` 专用 metadata 路径：持久表现在获取会话级共享 MDL；另一连接持有 `LOCK TABLES table WRITE` 时，描述查询会等待释放；临时表继续沿用原有物理名称解析。
- 新增 `TestDescribeWaitsForExplicitTableMetadataLock`；先行回归确认旧实现会直接返回，修复后通过。engine 全量回归通过（111.929s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation157/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation157/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation157/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation157/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括其他专用 metadata handler 的完整 MDL 矩阵、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 150

- P1-SQL-003 补齐已实现存储对象的元数据锁协调：`SHOW CREATE PROCEDURE/FUNCTION/TRIGGER/EVENT` 现在获取共享元数据锁，`CREATE/DROP/ALTER` 对应对象获取 DDL 写锁；另一连接持有同名显式锁时，读取和对象 DDL 都会等待释放，避免存储对象专用路径绕过公共锁协议。
- 新增 `TestShowCreateStoredObjectWaitsForExplicitMetadataLock` 与 `TestStoredObjectDDLWaitsForExplicitMetadataLock`；先行回归确认旧实现会在锁持有期间直接完成，修复后定向回归通过。engine 全量回归通过（113.035s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation163/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation163/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation163/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation163/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括存储对象引用依赖的完整 MDL 矩阵、`REPAIR TABLE`/其他专用 metadata handler、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 151

- P1-SQL-003 继续补齐专用 metadata handler 的 MDL：`SHOW TABLE STATUS`、`SHOW COLUMNS/FIELDS`、`SHOW FULL COLUMNS/FIELDS` 获取共享表元数据锁；`SHOW PROCEDURE STATUS`、`SHOW EVENT(S)`、`SHOW TRIGGERS` 对已实现存储对象获取共享对象锁，并按稳定顺序避免多对象读取死锁。
- 新增 `TestShowTableStatusWaitsForExplicitTableMetadataLock`、`TestShowColumnsWaitsForExplicitTableMetadataLock`、`TestShowFullColumnsWaitsForExplicitTableMetadataLock` 与 `TestShowRoutineStatusWaitsForExplicitMetadataLock`；同时修复 `SHOW COLUMNS` 对真实持久化 `.frm` 元数据的读取优先级。专项目标回归通过，engine 全量回归通过（113.590s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation164/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation164/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation164/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation164/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括 `REPAIR TABLE` 的 InnoDB 语义、存储对象引用依赖的完整 MDL 矩阵、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 152

- 明确 `REPAIR TABLE` 的 InnoDB 边界：已实现的 InnoDB 路径现在返回可识别的“不支持 repair”错误，而不是落到通用 `unsupported statement type`；`CHECK/ANALYZE/OPTIMIZE TABLE` 既有执行路径保持不变。该行为符合 MySQL 对 InnoDB 不提供 REPAIR 的边界。
- 新增 `TestRepairTableReportsInnoDBUnsupported`；engine 全量回归通过（113.197s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation165/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation165/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation165/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation165/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括 MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、存储对象引用依赖的完整 MDL 矩阵、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 153

- P1-SQL-003 继续补齐存储对象执行路径的 MDL：顶层 `CALL procedure` 与存储函数 `SELECT function(...)` 在读取持久化定义前获取共享对象锁；另一连接持有同名对象显式锁时，例程执行等待释放，避免执行路径绕过对象元数据协议。
- 新增 `TestCallStoredObjectWaitsForExplicitMetadataLock` 与 `TestStoredFunctionCallWaitsForExplicitMetadataLock`；定向回归通过，engine 全量回归通过（118.962s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation166/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation166/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation166/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation166/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括存储对象内部嵌套调用/触发器依赖的完整锁矩阵、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 154

- P1-SQL-003 补齐 `INFORMATION_SCHEMA.ROUTINES/TRIGGERS/EVENTS` 的对象级 MDL：这些查询现在在扫描持久化对象定义前按稳定顺序获取共享对象锁，锁等待错误向上层返回；不再绕过 `SHOW ... STATUS` 与 `SHOW CREATE` 已使用的对象锁协议。
- 新增 `TestInformationSchemaRoutinesWaitsForExplicitMetadataLock`；定向回归通过，engine 全量回归通过（110.508s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation167/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation167/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation167/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation167/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括存储对象触发器执行期/内部嵌套调用的完整锁矩阵、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 155

- P1-SQL-003 继续补齐 `INFORMATION_SCHEMA.PARAMETERS` 的对象级 MDL：参数元数据扫描现在在读取 procedure/function 持久化定义前按稳定顺序获取共享对象锁，锁等待错误向上层返回。
- 新增 `TestInformationSchemaParametersWaitsForExplicitMetadataLock`；定向回归通过，engine 全量回归通过（114.545s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation168/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation168/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation168/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation168/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括存储对象触发器执行期/内部嵌套调用的完整锁矩阵、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 156

- P1-SQL-003 补齐两项存储对象依赖边界：跨 schema 的 `CREATE VIEW` 源表依赖现在参与递归共享 MDL；嵌套 `CALL` 在进入被调用 procedure 前也获取对应对象共享锁，不再只依赖顶层 procedure 的锁。
- 新增 `TestCreateViewWaitsForCrossDatabaseSourceMetadataLock` 与 `TestNestedCallStoredObjectWaitsForExplicitMetadataLock`；专项回归通过，engine 全量回归通过（115.386s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation169/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation169/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation169/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation169/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括触发器执行期对象锁的完整矩阵、复杂跨库/循环 View 依赖、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 170

- P1-SQL-003 补齐触发器执行期的对象级 MDL：storage-integrated INSERT/UPDATE/DELETE 在解析目标表后，按稳定顺序对该表全部已持久化 Trigger 获取共享对象锁，并持有到 AFTER Trigger 副作用完成；另一连接持有 Trigger 对象写锁时，DML 会等待释放，避免执行期间 DROP/ALTER TRIGGER 与触发器定义读取竞态。
- 新增 `TestDMLWaitsForExplicitTriggerMetadataLock`；专项回归通过，证明 `LOCK TABLES app.trigger_lock_before WRITE` 会阻塞目标 INSERT，释放后 INSERT 和 BEFORE Trigger 改写均成功。`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.758s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation170/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation170/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation170/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation170/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括触发器跨表副作用/递归触发器与更细锁兼容矩阵、复杂跨库/循环 View 依赖、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 171

- P1-SQL-003 收口循环 View 依赖：`CREATE/ALTER/CREATE OR REPLACE VIEW` 在获取目标 View DDL 写锁前先递归解析源 View 依赖；检测到自引用或多级循环立即返回明确错误，避免通用语句读锁与目标 View 写锁形成约 50 秒等待超时。
- 新增 `TestCreateOrReplaceViewRejectsDependencyCycle`；循环检测修复后，自引用 View、跨 schema 源依赖和嵌套 View 源锁专项回归通过。`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（114.881s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation171/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation171/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation171/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation171/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、触发器跨表副作用/递归触发器、更细 MDL 锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 172

- P1-SQL-003 补齐触发器失败语义：AFTER INSERT/UPDATE/DELETE 现在建立触发器级原子日志边界；触发器失败会补偿基础行写入和触发器副作用，递归触发器链会在进入重复 frame 前返回明确错误并回滚本次 DML。
- 新增 `TestAfterTriggerRejectsRecursiveTriggerCycle` 与 `TestAfterUpdateAndDeleteTriggerFailuresRestoreBaseRows`，并强化 `TestAfterTriggerRunsAtPostWriteBoundaryAndRejectsNewMutation` 的副作用回滚断言；触发器递归、UPDATE/DELETE 补偿和 INSERT 副作用回滚专项回归通过。`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（114.996s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation172/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation172/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation172/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation172/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、更细触发器跨表锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 1058

- P1/I_S 元数据目录继续收口：`TABLES`、`COLUMNS`、`STATISTICS`、`PARTITIONS`、`KEY_COLUMN_USAGE`、`TABLE_CONSTRAINTS`、`CHECK_CONSTRAINTS`、`REFERENTIAL_CONSTRAINTS` 和 `VIEWS` 对已有真实字段统一执行等值/LIKE 过滤；新增真实表结构、索引和 CHECK 元数据回归，覆盖 `ENGINE`、`TABLE_TYPE`、`DATA_TYPE`、`IS_NULLABLE`、`ORDINAL_POSITION`、`INDEX_TYPE`、`CONSTRAINT_TYPE`、`ENFORCED` 和分区/View 属性。
- 通用目录匹配现在不会把 NULL 投影值错误视为满足显式 `=`/`LIKE` 条件；`IS NULL`/`IS NOT NULL` 仍由对应扩展/表达式路径处理。
- I_S/P_S 专项通过（24.462s）；engine 全包通过（141.294s）；全仓 `go test ./...` exit 0（engine 147.390s、manager 22.562s、net 8.349s、replication 2.836s）。
- 仍未完成的全局边界：完整 I_S/P_S 表和字段覆盖、缺少权威 recorder 的锁/等待/线程/长期统计、非 Connector/J 全客户端矩阵、XA/binlog 与官方 MySQL 双向互操作；FULLTEXT、非 InnoDB 引擎及其 `REPAIR TABLE`/转换明确排除，P3/P4 继续后置。

### Continuation 1059

- P1/I_S 存储对象目录继续收口：`ROUTINES`、`TRIGGERS`、`EVENTS` 对已有真实持久化定义的 routine 类型、trigger 事件/时序、event 调度类型/间隔等字段统一执行条件过滤；新增存储对象红测。
- 存储对象专项通过（14.194s）；engine 全包通过（140.901s）；随后全仓 `go test ./...` exit 0（engine 141.712s、net 7.628s，其余包通过或缓存通过）。
- 仍未完成的全局边界：完整 I_S/P_S 表和字段覆盖、缺少权威 recorder 的锁/等待/线程/长期统计、非 Connector/J 全客户端矩阵、XA/binlog 与官方 MySQL 双向互操作；FULLTEXT、非 InnoDB 引擎及其 `REPAIR TABLE`/转换明确排除，P3/P4 继续后置。

### Continuation 1060

- P1/I_S/P_S 真实来源字段继续收口：`COLUMN_STATISTICS.HISTOGRAM`、`USER_ATTRIBUTES.ATTRIBUTE`、`INNODB_VIRTUAL` 的已有字段、`performance_schema.host_cache` 的认证失败计数现在执行条件过滤；复制 P_S 视图统一采用 NULL-aware 条件匹配。
- I_S/P_S 专项通过（24.841s）；engine 全包通过（142.553s）。缺少 recorder/存储引擎来源的字段仍保持 NULL 或空集，不将局部实现冒充完整 MySQL 运行时语义。
- 仍未完成的全局边界：完整 I_S/P_S 表和字段覆盖、缺少权威 recorder 的锁/等待/线程/长期统计、非 Connector/J 全客户端矩阵、XA/binlog 与官方 MySQL 双向互操作；FULLTEXT、非 InnoDB 引擎及其 `REPAIR TABLE`/转换明确排除，P3/P4 继续后置。

### Continuation 1061

- P1/I_S `INNODB_VIRTUAL` 继续收口：`BASE_POS` 和 `M_COLS` 不再固定为空/1，而是从持久化生成列表达式解析实际依赖的基础列位置；无可解析依赖时保持 NULL/0，不制造虚假依赖。
- 新增生成列表达式依赖解析回归，覆盖多列引用和字符串字面量排除；I_S/P_S 专项通过（24.688s），engine 全量通过（140.397s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` 通过（engine 141.072s、integration 0.748s、manager 6.700s、net 7.515s、metrics 0.383s、protocol 0.541s、replication 1.922s）。
- 仍未完成的全局边界：完整 I_S/P_S 表和字段覆盖、缺少权威 recorder 的锁/等待/线程/长期统计、非 Connector/J 全客户端矩阵、XA/binlog 与官方 MySQL 双向互操作；FULLTEXT、非 InnoDB 引擎及其 `REPAIR TABLE`/转换明确排除，P3/P4 继续后置。

### Continuation 1062

- P1/I_S `INNODB_COLUMNS` 继续收口：`HAS_DEFAULT` 现在只对持久化记录为 `ALTER TABLE ... ALGORITHM=INSTANT` 新增的列返回 1；普通 SQL `DEFAULT` 不会被错误当作 instant-add 标记。`MTYPE` 对可从持久化 SQL 类型无歧义推导的 INT/VARCHAR/CHAR/浮点/DECIMAL/TEXT/GEOMETRY 类型返回官方主类型编码。
- 新增 instant-add 与 MTYPE 回归；相关 engine 定向测试通过（1.670s）。`PRTYPE` 的字符集/精确类型位编码、instant default 的内部二进制值仍保持 NULL，避免伪造 InnoDB 内部字典语义。
- 仍未完成的全局边界：完整 I_S/P_S 表和字段覆盖、缺少权威 recorder 的锁/等待/线程/长期统计、非 Connector/J 全客户端矩阵、XA/binlog 与官方 MySQL 双向互操作；FULLTEXT、非 InnoDB 引擎及其 `REPAIR TABLE`/转换明确排除，P3/P4 继续后置。

### 全局任务优先级与权限视图过滤（2026-09-22）

- 全局任务正式纳入：完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA、非 Connector/J 客户端兼容矩阵、XA 与原生 binlog/复制/崩溃恢复官方互操作。
- 明确排除：MyISAM、ARCHIVE、CSV 等非 InnoDB 引擎，非 InnoDB `REPAIR TABLE` 和引擎转换；FULLTEXT 继续延期，不作为当前 release blocker。
- P0 口径保持为单机核心运行、Connector/J 和 xmysql 自身集群闭环；P1 继续推进有权威运行时来源的 I_S/P_S 语义、其他客户端矩阵和官方互操作；P2 再处理低频管理扩展。
- 本轮修复 I_S `TABLE_PRIVILEGES`、`COLUMN_PRIVILEGES`、`SCHEMA_PRIVILEGES`、`USER_PRIVILEGES` 的 `GRANTEE`、对象字段、`PRIVILEGE_TYPE`、`IS_GRANTABLE` 条件过滤，新增 `TestInformationSchemaPrivilegeViewsApplyPrivilegePredicates`。
- 证据：权限/I_S 专项通过；Go/PyMySQL/Node 实连矩阵通过，MySQL CLI 仍因缺少 `mysql.exe` 为环境未验证；xmysql 自身 XA/binlog/复制/恢复通过，官方 MySQL 双向互操作仍需官方 fixture。

- 本轮 I_S View usage 修复：`VIEW_TABLE_USAGE` 与 `VIEW_ROUTINE_USAGE` 不再把源对象的 `TABLE_SCHEMA`/`TABLE_NAME` 条件错误下推到 View 元数据；两类视图现在分别过滤 View 字段和源表/源函数字段。新增 `TestInformationSchemaViewTableUsageFiltersSourceTables`、`TestInformationSchemaViewRoutineUsageFiltersSourceFunctions`；I_S 专项、engine 全量和串行全仓 Go 均通过。
- 本轮继续收口 View 依赖：依赖收集通过 SQL AST 排除 CTE 名称，只保留 CTE 内实际访问的物理表；新增 `TestInformationSchemaViewTableUsageExcludesCTENames`。复杂 View AST、循环依赖与完整 MDL/online DDL 仍为 partial。
- 本轮 I_S 目录过滤补充：`KEYWORDS` 支持 `WORD`/`RESERVED` 等值和 LIKE 条件；`ST_GEOMETRY_COLUMNS` 支持 catalog/schema/table/column/geometry type/SRS 条件。完整官方关键词词表与空间 SRS 元数据仍受 xmysql 数据源范围限制。

### Continuation 1037

- P1-SEC-001 / I_S 角色视图过滤继续收口：`ROLE_TABLE_GRANTS`、`ROLE_COLUMN_GRANTS`、`ROLE_ROUTINE_GRANTS` 现在执行 `TABLE_CATALOG`、`SPECIFIC_CATALOG`、`ROUTINE_CATALOG` 等目录字段的等值/LIKE 条件；旧行为的错误 catalog 条件会返回全量行，已由红测确认并修复。
- 新增角色 catalog 过滤回归；engine 全量 `go test ./server/innodb/engine -count=1 -timeout 30m` 通过（137.069s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 137.807s、manager 7.610s、net 7.456s、replication 1.908s）。
- 完整授权图、角色管理员行为、metadata visibility 和官方 MySQL 全量 I_S/P_S 运行时语义仍未闭环。

### Continuation 1038

- P1-A / `INFORMATION_SCHEMA.INNODB_TRX` 继续补齐真实字段过滤：`TRX_STARTED` 与 `TRX_LOCK_STRUCTS` 现在执行等值/LIKE 条件；先由红测确认旧实现会忽略错误条件，修复后定向、engine 全量和串行全仓回归通过（engine 135.744s；全仓 engine 138.227s，manager 7.707s，net 7.654s，replication 1.935s）。
- `TRX_MYSQL_THREAD_ID`、`TRX_QUERY`、等待锁、行统计等字段当前没有 xmysql 权威来源，继续返回 `NULL`，不使用推测值填充。

### Continuation 1039

- P1-SEC-001 / 角色授权视图继续补齐已有字段过滤：`ROLE_TABLE_GRANTS`、`ROLE_COLUMN_GRANTS`、`ROLE_ROUTINE_GRANTS` 现在执行 `GRANTOR`、`GRANTOR_HOST` 的等值/LIKE 条件；错误 grantor 条件先由红测复现，修复后 engine 全量与串行全仓回归通过（engine 137.630s；全仓 engine 149.410s，manager 8.982s，net 7.441s，metrics 0.366s，replication 3.887s）。
- 本次仍未虚构角色授权图或管理员/metadata visibility 数据；这些完整语义继续列为 P1 partial。

### Continuation 1040

- P1-OPS-001：`events_statements_summary_by_digest` 的 `QUANTILE_95`、`QUANTILE_99`、`QUANTILE_999` 改为基于 bounded statement history 的真实 timer 样本 nearest-rank quantile；新增 100 个同 digest 样本的 95/99/99.9 分位回归。

### Continuation 1041

- P1-A / `INFORMATION_SCHEMA.SCHEMATA`：在已有 schema 会话可见性和 `SCHEMA_NAME` 过滤基础上，补齐 `CATALOG_NAME`、`DEFAULT_CHARACTER_SET_NAME`、`DEFAULT_COLLATION_NAME`、`DEFAULT_ENCRYPTION` 的等值/LIKE 条件。红测先确认错误条件会返回系统库，修复后 I_S/P_S 专项、engine 全包和串行全仓 Go 门禁通过。
- 本次只扩展已有真实投影字段的过滤，不虚构完整 schema 默认属性目录；组件表运行时、官方 MySQL fixture、MySQL CLI 和官方 XA/binlog 双向互操作仍按全局边界保持 partial/unverified。

### Continuation 1042

- P1-A / `INFORMATION_SCHEMA.PROFILING`：基于已有 bounded statement history，补齐 `QUERY_ID`、`SEQ`、`STATE`、`DURATION` 的等值/LIKE 过滤；红测先确认错误 `STATE` 会返回全部历史，修复后 I_S/P_S 专项、engine 全包和串行全仓 Go 门禁通过。
- CPU、系统调用、source-location 等没有 xmysql recorder 权威来源的 profiling 字段继续保持 NULL，不用合成数据标记为完整实现。

### Continuation 1043

- P1-A / `INFORMATION_SCHEMA.CHARACTER_SETS`：补齐 `DEFAULT_COLLATE_NAME`、`DESCRIPTION`、`MAXLEN` 对已有内置字符集目录行的过滤；红测先确认错误排序规则会放行全部字符集，修复后 I_S/P_S 专项、engine 全包和串行全仓 Go 门禁通过。
- 本次只完善现有字符集子集的字段过滤，不宣称官方完整字符集/排序规则目录已经导入。

### Continuation 1044

- P1-A / `INFORMATION_SCHEMA.PLUGINS`：基于已有内置插件目录，扩展版本、作者、描述、许可证等非空字段过滤；红测先确认错误作者条件会返回全部插件，修复后 I_S/P_S 专项、engine 全包和串行全仓 Go 门禁通过。
- 插件动态加载/卸载生命周期及未接入组件的真实运行时数据仍保持 partial，不用内置目录行替代完整插件生态。

### Continuation 1045

- P1-A / I_S extension views：`SCHEMATA_EXTENSIONS`、`COLUMNS_EXTENSIONS`、`TABLES_EXTENSIONS`、`TABLE_CONSTRAINTS_EXTENSIONS` 及相关扩展路径现在执行已有 NULL 属性的 `IS NULL`/`IS NOT NULL` 过滤；红测先确认错误 `IS NOT NULL` 条件会放行，修复后 I_S/P_S 专项、engine 全包和串行全仓 Go 门禁通过。
- 本次只实现已有 NULL 语义，不为没有 xmysql 来源的 engine/secondary/options JSON 属性合成数据。
- P_S 专项通过（12.398s），engine 全量通过（137.424s），串行全仓 Go 通过（engine 136.596s、manager 9.004s、net 7.794s、metrics 0.427s、replication 1.999s）。完整长期持久化 summary、官方 digest token 规则和 bounded history 之外的统计仍未闭环。

### Continuation 217

- P1-SQL/Optimizer：普通 SELECT 的 schema 限定符现在贯穿手工 UnifiedExecutor、SelectExecutor JOIN 和 PlanBuilder；优化器计划优先按 `schema.table` 查询，并兼容仅支持裸表名的旧 metadata provider。新增 qualified table plan、UnifiedExecutor 和兼容元数据回归。
- 本轮验证：plan 全量通过；engine 全量 `go test ./server/innodb/engine -count=1 -timeout 30m` 通过（136.213s）；串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` 通过（engine 139.125s，integration 0.801s，manager 8.218s，net 7.534s，metrics 0.381s，protocol 0.551s，replication 1.910s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation217/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation217/cluster-report.json) 为 `PASS`。
- 仍未完成：复杂 View AST/跨库循环依赖与完整 MDL、完整 I_S/P_S 运行时保真度、XA/binlog 官方 MySQL 双向互操作、完整非 Connector/J 客户端矩阵、剩余复杂优化器/SQL、online DDL 和 P3/P4；FULLTEXT 与非 InnoDB 引擎/REPAIR 明确不纳入当前范围。

### Continuation 178

- P1-SEC-001/全局权限兼容继续收口：认证层角色继承现在合并持久化的
  `partial_revokes` schema restrictions，与 engine 的有效授权计算一致，
  防止角色授予的 `*.*` 权限绕过库级 REVOKE 限制。新增角色继承回归，
  auth、partial revoke 和最终串行全仓门禁通过。
- 客户端矩阵补充真实隔离实例连接：Go `go-mysql-driver`、PyMySQL、
  Node.js/mysql2 均 PASS；MySQL CLI 因环境缺少 `mysql.exe` 保持跳过。
- 仍未完成边界保持：完整 I_S/P_S 运行时表族、官方 XA/binlog/复制/崩溃恢复互操作、
  完整 online DDL/MDL、动态插件生命周期、FULLTEXT，以及非 Connector/J
  全量客户端矩阵的 MySQL CLI 部分。

### Continuation 179

- 刷新 P0 本地交付证据：集群 smoke 通过；release candidate 的 build、unit、
  integration、Go core、crash recovery、concurrency、observability 和
  Connector/J 139 项全部通过，JDBC 无失败/错误/跳过，Maven BUILD SUCCESS。
- release candidate 仍为 NO-GO，唯一失败是完整客户端矩阵因当前环境缺少
  `mysql.exe` 返回 `SKIPPED_ENVIRONMENT`；Go、PyMySQL、Node.js/mysql2
  实连均 PASS。该环境缺口不改变 Connector/J P0 通过证据。

### Continuation 185

- P1-OPT-004 继续补齐常量类型转换边界：索引条件提取现在会折叠不引用行列的确定性 `CAST`/`CONVERT` 常量表达式，例如 `col1 >= CAST('42' AS SIGNED)` 可生成可下推的 `col1 >= 42` 条件；含列、随机或时间函数仍保守拒绝。
- 新增 `TestConstantCastExpressionUsesIndexPushdown`，并通过计划包全量回归；全仓 `go test ./... -count=1 -timeout 30m` 通过，engine 回归通过（118.730s）。
- 本轮门禁：集群 smoke [`reports/compatibility/p1-cluster-current-continuation185/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation185/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation185/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation185/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛的动态/类型转换常量、复杂 index-range 分区、异构 row-source 完整列裁剪、通用子查询谓词推导、更复杂 View/触发器 MDL、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 200

- P1-SQL-001 补齐嵌套派生表中的混合集合表达式：普通派生表内再嵌套的 `INTERSECT/EXCEPT` 现在递归先物化最内层集合结果，再逐层按内层/外层 SELECT 顺序应用投影、过滤、排序和分页；新增 `TestNestedDerivedTableSupportsMixedSetExpression` 与 `TestDeeplyNestedDerivedTableSupportsMixedSetExpression`，避免落回 `unsupported derived table set operation` 或解析失败。
- 本轮验证：复杂 SELECT/派生表/相关子查询/CTE 专项通过；全仓 `go test ./... -count=1 -timeout 30m` 通过，engine 121.741s；集群 smoke [`reports/compatibility/p1-cluster-current-continuation200/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation200/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation200/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation200/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更深层/任意 AST 组合的通用子查询执行、复杂 index-range 分区、异构 row-source 完整列裁剪、统计采样全覆盖、复杂 View/触发器 MDL、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 201

- P1-SQL-001 继续补齐嵌套混合集合的 JOIN 边界：每一层物化派生结果如果带有 JOIN，现在转入既有物化派生 JOIN 执行器，保留外表列、ON 条件和排序语义；新增 JOIN 回归并通过。
- 本轮验证：engine 全包 `go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.611s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation201/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation201/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation201/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation201/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更深层/任意 AST 组合的通用子查询执行、复杂 index-range 分区、异构 row-source 完整列裁剪、统计采样全覆盖、复杂 View/触发器 MDL、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 186

- P1-OPT-004 继续补齐结构化范围条件：`BETWEEN` 的上下界现在复用确定性常量表达式折叠，算术或 `CAST/CONVERT` 边界可生成精确的下界/上界索引条件；非确定性或含行列依赖的边界仍不下推。
- 新增 `TestStructuredRangeWithConstantExpressionsUsesIndexes`，验证 `BETWEEN (2 + 3) AND CAST('20' AS SIGNED)` 生成 `5`/`20` 两个范围条件；计划包与 engine 全量回归通过（116.471s）。
- 本轮门禁：集群 smoke [`reports/compatibility/p1-cluster-current-continuation186/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation186/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation186/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation186/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range 分区、通用子查询谓词推导、异构 row-source 完整列裁剪、复杂 SQL AST、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 190

- P1-OPT-005/P1-OPT-004 继续补齐确定性函数常量：`MD5/SHA/SHA2/CRC32/INET_*`、UUID 编解码和 Base64/HEX 等纯输入函数现在可参与索引常量折叠；`UUID()`、`RAND()`、时间/会话函数仍明确排除。
- 新增 `TestConstantDigestFunctionExpressionUsesIndexPushdown`，验证 `col1 = MD5('abc')` 生成可下推的摘要等值条件；计划包与 engine 全量回归通过（117.292s）。
- 本轮门禁：集群 smoke [`reports/compatibility/p1-cluster-current-continuation190/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation190/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation190/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation190/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括通用子查询谓词推导、复杂 index-range 分区、异构 row-source 完整列裁剪、复杂 SQL AST、真正 online DDL/多表 DDL、触发器跨表副作用/递归依赖的完整 MDL 矩阵；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 189

- P1-OPT-004 继续补齐确定性日期函数常量：`YEAR/MONTH/DAY/.../DATE_FORMAT/TIME_FORMAT/STR_TO_DATE` 等无会话状态函数现在可参与常量索引边界折叠；时间、随机、会话读取函数仍不在白名单。
- 新增 `TestConstantDateFunctionExpressionUsesIndexPushdown`，验证 `DATE_FORMAT('2024-01-15','%Y-%m-%d')` 生成可下推的字符串索引边界；计划包与 engine 全量回归通过（117.825s）。
- 本轮门禁：集群 smoke [`reports/compatibility/p1-cluster-current-continuation189/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation189/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation189/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation189/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range 分区、通用子查询谓词推导、异构 row-source 完整列裁剪、复杂 SQL AST、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 188

- P1-OPT-004 继续补齐常量边界表达式：无行列依赖的确定性比较/逻辑表达式和 `CASE` 分支现在可折叠为索引边界；求值失败、含列或会话/时间/随机函数的表达式仍拒绝下推。
- 新增 `TestConstantCaseExpressionUsesIndexPushdown`，验证 `col1 >= CASE WHEN 1 = 1 THEN 7 ELSE 8 END` 生成可下推的 `col1 >= 7`；计划包与 engine 全量回归通过（115.428s）。
- 本轮门禁：集群 smoke [`reports/compatibility/p1-cluster-current-continuation188/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation188/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation188/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation188/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range 分区、通用子查询谓词推导、异构 row-source 完整列裁剪、复杂 SQL AST、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 187

- P1-OPT-004 继续补齐索引列表常量：`IN` 列表中的算术/`CAST` 等表达式只要全部为确定性无行列依赖表达式，现在会折叠为常量列表并走索引下推；混入列引用或不确定函数时仍保守回退。
- 新增 `TestConstantExpressionTupleUsesInIndexPushdown`，验证 `col1 IN (2 + 3, CAST('20' AS SIGNED))` 生成可下推的 `(5, 20)` 列表；计划包与 engine 全量回归通过（115.915s）。
- 本轮门禁：集群 smoke [`reports/compatibility/p1-cluster-current-continuation187/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation187/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation187/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation187/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range 分区、通用子查询谓词推导、异构 row-source 完整列裁剪、复杂 SQL AST、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 178

- P1-SEC-001/P1-SQL-003 补齐表级 `TRIGGER` 权限：Trigger DDL、`SHOW CREATE TRIGGER`、`SHOW TRIGGERS` 与 `INFORMATION_SCHEMA.TRIGGERS` 现在按触发器所属表检查 `TRIGGER`，同时保留数据库级 `schema.*` 授权和无权限隐藏/拒绝语义。
- 新增 `TestTriggerOperationsAcceptTableScopedTriggerPrivilege`，覆盖表级授权下的创建、SHOW、`INFORMATION_SCHEMA` 查询和删除；原有数据库级权限矩阵保持通过。
- 本轮验证：`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.200s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation178/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation178/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation178/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation178/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括触发器跨表副作用/递归依赖的更细 MDL 矩阵、更复杂 View AST/跨库循环锁兼容矩阵、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 176

- P1-SQL-005 补齐自引用外键边界：CREATE TABLE 现在允许引用自身已定义的主键/唯一键，并校验自引用列存在、类型兼容和父侧索引覆盖；随后 `TRUNCATE` 仍按 MySQL 语义返回 `ER_TRUNCATE_ILLEGAL_FK`（1701/42000）。
- 新增 `TestTruncateSelfReferencingTableIsRejected`；自引用外键 CREATE/TRUNCATE、普通父表引用和错误码专项回归通过。`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（115.496s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation176/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation176/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation176/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation176/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、更细触发器跨表锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 1046

- P1/I_S 收口：`INFORMATION_SCHEMA.OPTIMIZER_TRACE` 现在使用真实的物理计划追踪历史，并对 `QUERY`、`MISSING_BYTES_BEYOND_MAX_MEM_SIZE`、`INSUFFICIENT_PRIVILEGES` 已投影字段应用等值/LIKE 过滤；新增错误查询与权限条件回归测试，先复现后修复。
- 本轮 I_S/P_S 专项通过（25.119s），engine 全量通过（138.181s）。
- 仍未完成的边界：完整官方 optimizer-trace 配置/持久化/跨实例语义、完整 I_S/P_S 表与字段保真度、XA/binlog 原生互操作、非 Connector/J 全量客户端矩阵、FULLTEXT；非 InnoDB 引擎及其专用 `REPAIR TABLE` 按用户确认不纳入范围。

### Continuation 1047

- P1/I_S 收口：`INFORMATION_SCHEMA.INNODB_METRICS` 对已有真实计数/说明字段补齐等值/LIKE 过滤，新增错误 `COUNT` 条件回归；不为缺少 recorder 来源的时间字段构造数据。
- 本轮 I_S/P_S 专项通过（24.743s），engine 全量通过（137.144s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 137.050s、manager 7.102s、net 7.583s、replication 1.932s）。
- 仍未完成的边界：完整 I_S/P_S 表与字段保真度、optimizer-trace 持久化/跨实例语义、XA/binlog 原生互操作、非 Connector/J 全量客户端矩阵、FULLTEXT；非 InnoDB 引擎及其专用 `REPAIR TABLE` 按用户确认不纳入范围。

### Continuation 1048

- P1/I_S 收口：启用压缩管理器时，`INFORMATION_SCHEMA.INNODB_CMP`、`INNODB_CMPMEM` 及 reset 视图对已有压缩统计字段补齐等值/LIKE 过滤；`CMP_PER_INDEX` 继续因无真实 per-index 来源不生成行。
- 新增启用 zlib 的 engine 回归；本轮 I_S/P_S 专项通过（25.155s），engine 全量通过（137.057s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 138.748s、manager 7.093s、net 7.510s、replication 1.864s）。
- 仍未完成的边界：完整 I_S/P_S 表与字段保真度、optimizer-trace 持久化/跨实例语义、XA/binlog 原生互操作、非 Connector/J 全量客户端矩阵、FULLTEXT；非 InnoDB 引擎及其专用 `REPAIR TABLE` 按用户确认不纳入范围。

### Continuation 1049

- P1/P_S 收口：`performance_schema.innodb_redo_log_files` 对已有 redo manager 快照的 `START_LSN`、`END_LSN`、`SIZE_IN_BYTES`、`IS_FULL`、`CONSUMER_LEVEL` 补齐查询过滤；单一 append-only redo 文件的实现边界保持不变。
- 新增 redo manager 实例回归；本轮 P_S/I_S 专项通过（26.093s），engine 全量通过（139.118s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 137.719s、manager 8.220s、net 8.241s、replication 2.690s）。
- 仍未完成的边界：MySQL 多文件循环 redo/consumer 语义、完整 I_S/P_S 表与字段保真度、XA/binlog 原生互操作、非 Connector/J 全量客户端矩阵、FULLTEXT；非 InnoDB 引擎及其专用 `REPAIR TABLE` 按用户确认不纳入范围。

### Continuation 1050

- P1/P_S 收口：`table_io_waits_summary_by_table` 与 `table_io_waits_summary_by_index_usage` 对真实 statement history 聚合出的非空计数、计时摘要字段补齐条件过滤；新增错误 `COUNT_STAR/COUNT_READ` 回归。
- 本轮 P_S 专项通过（13.312s），engine 全量通过（138.431s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 139.383s、manager 7.178s、net 7.819s、replication 2.195s）。
- 仍未完成的边界：完整 P_S 计数器/长期持久化语义、完整 I_S/P_S 表字段保真度、MySQL 多文件循环 redo、XA/binlog 原生互操作、非 Connector/J 全量客户端矩阵、FULLTEXT；非 InnoDB 引擎及其专用 `REPAIR TABLE` 按用户确认不纳入范围。

### Continuation 1051

- P1/P_S 收口：`setup_meters` 对已有 `DESCRIPTION`、`setup_metrics` 对已有 `UNIT`/`DESCRIPTION` 补齐条件过滤；不扩展到没有 xmysql 运行时来源的完整 MySQL metric catalog。
- 新增错误描述/单位回归；完整 `TestPerformanceSchema` 通过（13.010s），engine 全量通过（137.285s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 137.406s、manager 7.123s、net 7.566s、replication 1.916s）。
- 仍未完成的边界：完整 telemetry catalog/采集导出、完整 I_S/P_S 表字段保真度、MySQL 多文件循环 redo、XA/binlog 原生互操作、非 Connector/J 全量客户端矩阵、FULLTEXT；非 InnoDB 引擎及其专用 `REPAIR TABLE` 按用户确认不纳入范围。

### Continuation 1052

- P1/P_S 收口：session runtime 视图 `variables_by_thread`、`user_variables_by_thread`、`status_by_account`、`status_by_host`、`status_by_user` 对已有 `VARIABLE_VALUE` 补齐过滤，覆盖明细和聚合状态行。
- 新增错误状态值/用户变量值回归；完整 `TestPerformanceSchema` 通过（13.203s），engine 全量通过（137.529s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 139.630s、manager 7.208s、net 7.683s、replication 2.016s）。
- 仍未完成的边界：完整 P_S session/status counters 与长期持久化、完整 telemetry catalog、完整 I_S/P_S 表字段保真度、MySQL 多文件循环 redo、XA/binlog 原生互操作、非 Connector/J 全量客户端矩阵、FULLTEXT；非 InnoDB 引擎及其专用 `REPAIR TABLE` 按用户确认不纳入范围。

### Continuation 1053

- P1/P_S 收口：`prepared_statements_instances` 对已有 prepared-statement inventory 的非空字段补齐通用条件过滤，覆盖执行次数、计时、错误/告警和行数等投影字段；不虚构 inventory 未提供的执行历史。
- 新增错误 `COUNT_EXECUTE` 回归；完整 `TestPerformanceSchema` 通过（17.101s），engine 全量通过（136.646s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 138.069s、manager 7.327s、net 7.446s、replication 1.892s）。
- 仍未完成的边界：prepared statement 完整执行历史 accounting、完整 P_S session/status counters 与长期持久化、完整 telemetry catalog、完整 I_S/P_S 表字段保真度、XA/binlog 原生互操作、非 Connector/J 全量客户端矩阵、FULLTEXT；非 InnoDB 引擎及其专用 `REPAIR TABLE` 按用户确认不纳入范围。

### Continuation 1054

- P1/P_S 收口：`performance_schema.processlist` 对实时 session 的 `TIME_MS`、`ROWS_SENT`、`ROWS_EXAMINED` 补齐过滤。
- 新增错误 `ROWS_SENT` 回归；完整 `TestPerformanceSchema` 通过（13.120s），engine 全量通过（137.047s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 138.111s、manager 7.075s、net 7.486s、replication 1.915s）。
- 仍未完成的边界：完整 P_S processlist/session accounting、prepared statement 执行历史、完整 telemetry catalog、完整 I_S/P_S 表字段保真度、XA/binlog 原生互操作、非 Connector/J 全量客户端矩阵、FULLTEXT；非 InnoDB 引擎及其专用 `REPAIR TABLE` 按用户确认不纳入范围。

### Continuation 1055

- P1/P_S 收口：`performance_schema.variables_info` 对持久化变量已有的 `VARIABLE_SOURCE`、`VARIABLE_PATH`、`SET_TIME`、`SET_USER`、`SET_HOST` 元数据补齐条件过滤。
- 新增错误 source/user 回归；完整 `TestPerformanceSchema` 通过（13.054s），engine 全量通过（136.315s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 137.776s、manager 7.094s、net 7.556s、replication 1.908s）。
- 仍未完成的边界：完整 P_S processlist/session accounting、prepared statement 执行历史、完整 telemetry catalog、完整 I_S/P_S 表字段保真度、XA/binlog 原生互操作、非 Connector/J 全量客户端矩阵、FULLTEXT；非 InnoDB 引擎及其专用 `REPAIR TABLE` 按用户确认不纳入范围。

### Continuation 1056

- P1/P_S 收口：`tls_channel_status` 对真实 session/TLS 参数的 `VALUE` 补齐等值/LIKE/IN 过滤；服务器级完整 TLS channel 属性目录仍不伪造。
- 新增错误 `VALUE` 回归；完整 `TestPerformanceSchema` 通过（13.088s），engine 全量通过（136.168s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 138.087s、manager 7.365s、net 7.530s、replication 2.219s）。
- 仍未完成的边界：完整 P_S processlist/session accounting、prepared statement 执行历史、完整 telemetry/TLS catalog、完整 I_S/P_S 表字段保真度、XA/binlog 原生互操作、非 Connector/J 全量客户端矩阵、FULLTEXT；非 InnoDB 引擎及其专用 `REPAIR TABLE` 按用户确认不纳入范围。

### Continuation 1057

- P1/P_S 收口：`performance_schema.error_log` 对真实 recorder 事件已有的 `DATA`、`TIMESTAMP` 等字段补齐条件过滤。
- 新增错误 `DATA` 回归；完整 `TestPerformanceSchema` 通过（13.273s），engine 全量通过（137.482s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 138.097s、manager 8.797s、net 7.511s、replication 1.835s）。
- 仍未完成的边界：完整 P_S error-log 生命周期/持久化、processlist/session accounting、prepared statement 执行历史、完整 telemetry/TLS catalog、完整 I_S/P_S 表字段保真度、XA/binlog 原生互操作、非 Connector/J 全量客户端矩阵、FULLTEXT；非 InnoDB 引擎及其专用 `REPAIR TABLE` 按用户确认不纳入范围。

### Continuation 215

- P1-SQL-001：递归 CTE 主查询现在支持 CTE 与多张基表组成的嵌套 JOIN 链；单列/多列 CTE 均复用物化派生 JOIN 执行器，保留 ON/WHERE、外连接 NULL、投影、排序和分页语义。
- 新增 `TestRecursiveCTECompatibilitySupportsMainQueryJoinChainWithBaseTables`；旧实现先复现 `unsupported FROM expression`，修复后通过。engine 全量、全仓 Go、集群 smoke 和 Connector/J 发布候选均重新验证。
- fresh 证据：engine 117.825s、全仓 engine 123.488s；集群 [`reports/compatibility/p1-cluster-current-continuation214/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation214/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation214/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation214/release-candidate.json) 为 `GO`，9/9 checks PASS，Connector/J 139/0/0/0，Maven BUILD SUCCESS。

### Continuation 216

- P1-SQL-003 修复跨库 View schema 绑定：单表 SELECT/JOIN 现在使用表自身的 schema 限定；View 重写将未限定底层表绑定到 View 所属库，并保留 CTE 名称与已有 schema 限定。
- 新增 `TestCrossDatabaseViewPreservesQualifiedSource`、`TestViewBindsUnqualifiedSourcesToItsOwningDatabase` 与 `TestCrossDatabaseViewJoinUsesEachSourceSchema`，红测先复现 `other.users`，修复后 View/CTE 专项、engine 全量和串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` 均通过（engine 135.364s）。
- 仍未完成：复杂 View AST/跨库循环依赖与完整 MDL 矩阵、真正 online DDL/多表 DDL、完整 I_S/P_S 运行时保真度、XA/binlog 与官方 MySQL 原生双向互操作、完整非 Connector/J 客户端矩阵（当前 mysql CLI 缺失）、剩余复杂优化器/SQL 与 P3/P4；FULLTEXT 和非 InnoDB 引擎/REPAIR 明确不纳入当前范围。
- 仍未完成：通用复杂 SQL AST/相关子查询谓词推导、统计采样全覆盖、复杂 View/触发器依赖 MDL、真正 online DDL/多表 DDL、MyISAM/ARCHIVE/CSV 专用 REPAIR TABLE 和更高阶集群/存储互操作；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 216

- P1 XA：`XA RECOVER CONVERT XID` 已支持；保留 `XA_RECOVER_ADMIN` 权限门槛，`data` 返回 XID 字节拼接后的十六进制文本。
- 回归与证据：engine 118.928s、全仓 engine 123.084s；集群 [`reports/compatibility/p1-cluster-current-continuation1001/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1001/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation1001/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1001/release-candidate.json) 为 `GO`，9/9 checks PASS，Connector/J 139/0/0/0，Maven BUILD SUCCESS。
- 仍未完成：XA 原生二进制日志/更细权限矩阵、完整 online DDL/MDL、全量 I_S/P_S、通用复杂 SQL AST、物理分区路由、真正多引擎 REPAIR TABLE 和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 218

- P1-SQL-001：同一个递归 CTE 现在可以在最终查询中用不同别名被引用两次，支持自连接、ON 条件、WHERE 过滤、ORDER BY 和 LIMIT；物化结果按每个别名挂载到派生 JOIN 执行器，避免重复执行递归成员。
- 新增 `TestRecursiveCTECompatibilitySupportsMainQuerySelfJoin`；旧实现先返回 `recursive CTE main query has unsupported FROM expression`，修复后通过。engine 115.353s、全仓 engine 130.123s；集群 [`reports/compatibility/p1-cluster-current-continuation1003/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1003/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation1003/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1003/release-candidate.json) 为 `GO`，9/9 checks PASS，Connector/J 139/0/0/0，Maven BUILD SUCCESS。
- 仍未完成：递归 CTE 更复杂的多层/任意 AST 组合、通用相关子查询谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、全量 I_S/P_S、XA 原生二进制日志/更细权限矩阵、物理分区路由和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 220

- P1-SQL-003/P0-Connector.J：`SHOW PLUGINS` 现在返回 MySQL 六列结果形状（`Name/Status/Type/Library/License/Load_option`），暴露当前 InnoDB 与认证插件，并支持 `LIKE` 过滤；此前 parser 虽可识别语句，但执行器返回 `unsupported SHOW type: plugins`。
- 新增 `TestXMySQLExecutor_ShowPluginsReturnsBuiltinsAndSupportsLike`；engine 114.747s、全仓 engine 120.322s；集群 [`reports/compatibility/p1-cluster-current-continuation1005/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1005/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation1005/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1005/release-candidate.json) 为 `GO`，9/9 checks PASS，Connector/J 139/0/0/0，Maven BUILD SUCCESS。
- 仍未完成：更完整的插件动态装载/卸载生命周期、递归 CTE 更复杂的多层/任意 AST 组合、通用相关子查询谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、全量 I_S/P_S、XA 原生二进制日志/更细权限矩阵、物理分区路由和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 221

- P1/客户端元数据：补齐 `SHOW OPEN TABLES`，并覆盖 parser 的 `SHOW OPEN SCHEMAS` 别名；返回 MySQL 四列 `Database/Table/In_use/Name_locked`，支持 `FROM/IN` 和 `LIKE`。
- 结果来源为当前会话可见的持久化 `.frm` 表与视图元数据；由于没有 MySQL table-cache 计数器，`In_use` 与 `Name_locked` 明确为 0。
- 新增 `TestXMySQLExecutor_ShowOpenTablesReturnsVisibleTablesAndSupportsLike`；engine 115.003s、全仓 Go 119.257s；集群 [`reports/compatibility/p1-cluster-current-continuation1006/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1006/cluster-report.json) PASS；发布候选 [`reports/compatibility/release-candidate-current-continuation1006/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1006/release-candidate.json) GO，9/9 checks PASS，Connector/J 139/0/0/0。

### Continuation 222

- P1/客户端元数据：补齐 `SHOW STORAGE ENGINES` 执行路由，复用已验证的 `SHOW ENGINES` 六列结果形状和 InnoDB 能力声明。
- 新增 `TestXMySQLExecutor_ShowStorageEnginesUsesShowEnginesShape`；fresh engine 113.306s、全仓 Go 120.062s；集群 [`reports/compatibility/p1-cluster-current-continuation1007/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1007/cluster-report.json) PASS；发布候选 [`reports/compatibility/release-candidate-current-continuation1007/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1007/release-candidate.json) GO，9/9 checks PASS，Connector/J 139/0/0。
- 仍未完成：真实动态插件装载/卸载生命周期、SHOW OPEN TABLES 的 table-cache 使用计数、递归 CTE 更复杂的多层/任意 AST 组合、通用相关子查询谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、全量 I_S/P_S、XA 原生二进制日志/更细权限矩阵、物理分区路由和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 223

- P1-SQL-001：多个独立递归 CTE 的最终查询现在支持与普通持久表混合组成嵌套 JOIN；CTE 结果通过派生 JOIN 执行器注入，普通表仍走 storage-integrated 路径。
- 新增 `TestRecursiveCTECompatibilitySupportsIndependentDefinitionsJoinedWithBaseTable`；旧实现先返回 `multiple recursive CTE join source is unsupported`，修复后通过。engine 114.466s、全仓 Go 119.903s；集群 [`reports/compatibility/p1-cluster-current-continuation1008/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1008/cluster-report.json) PASS；发布候选 [`reports/compatibility/release-candidate-current-continuation1008/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1008/release-candidate.json) GO，9/9 checks PASS，Connector/J 139/0/0。
- 仍未完成：递归 CTE 更复杂的多层/任意 AST、相关子查询通用谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、全量 I_S/P_S、XA 原生二进制日志/更细权限矩阵、物理分区路由、动态插件生命周期和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 224

- P1-SYS/I_S：新增 `INFORMATION_SCHEMA.PLUGINS` 路由，返回 MySQL 14 列结果形状；支持列投影及 `PLUGIN_NAME`、`PLUGIN_STATUS`、`PLUGIN_TYPE`、`LOAD_OPTION` 的等值/LIKE 过滤，内置插件与 `SHOW PLUGINS` 对齐。
- 新增 `TestXMySQLExecutor_InformationSchemaPluginsSupportsProjectionAndFilter`；旧路径先落入普通 SELECT 并触发空指针，修复后通过。engine 114.283s、全仓 Go 123.039s；集群 [`reports/compatibility/p1-cluster-current-continuation1009/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1009/cluster-report.json) PASS；发布候选 [`reports/compatibility/release-candidate-current-continuation1009/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1009/release-candidate.json) GO，9/9 checks PASS，Connector/J 139/0/0。
- 仍未完成：动态插件装载/卸载生命周期、完整 I_S/P_S 表覆盖、递归 CTE 更复杂的多层/任意 AST、相关子查询通用谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、XA 原生二进制日志/更细权限矩阵、物理分区路由和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 225

- P1-OPT/分区访问：分区路由对多个同一分区表达式的常量范围谓词求交，`id >= 100 AND id < 200` 可只保留命中的 RANGE 分区；无法证明的谓词保守保留全部分区，DML 候选行裁剪同步该逻辑。
- 新增 `TestPartitionedBTreeManagerIntersectsMultipleConstantRanges`；engine 118.512s、全仓 Go 122.137s；集群 [`reports/compatibility/p1-cluster-current-continuation1010/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1010/cluster-report.json) PASS；发布候选 [`reports/compatibility/release-candidate-current-continuation1010/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1010/release-candidate.json) GO，9/9 checks PASS，Connector/J 139/0/0。
- 仍未完成：更复杂表达式/OR/函数分区剪枝、复杂 index-range partitioning 的完整类型矩阵、动态插件生命周期、完整 I_S/P_S、递归 CTE 任意 AST、相关子查询通用谓词推导、完整 online DDL/MDL、XA 原生二进制日志/更细权限矩阵和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 219

- P1-SQL-001：多个独立递归 CTE 现在支持三路及更深的嵌套 INNER JOIN 物化，统一复用派生 JOIN 执行器；原有两路 CTE JOIN、聚合、排序和别名语义保持兼容。
- 新增 `TestRecursiveCTECompatibilitySupportsThreeIndependentDefinitionsInJoinChain`；旧实现先返回 `multiple recursive CTE join source is unsupported`，修复后通过。engine 117.434s、全仓 engine 131.548s；集群 [`reports/compatibility/p1-cluster-current-continuation1004/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1004/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation1004/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1004/release-candidate.json) 为 `GO`，9/9 checks PASS，Connector/J 139/0/0/0，Maven BUILD SUCCESS。
- 仍未完成：递归 CTE 更复杂的多层/任意 AST 组合、通用相关子查询谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、全量 I_S/P_S、XA 原生二进制日志/更细权限矩阵、物理分区路由和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 217

- P1 XA：`XA BEGIN xid` 现在归一化到既有 `XA START` 状态机，支持 BEGIN、END、COMMIT ONE PHASE 的完整事务路径；新增 `TestParseXAStatementSupportsBasicXIDForms` 与 `TestXACompatibilitySupportsBeginAlias`，旧实现先将 BEGIN 判为 unsupported，修复后提交并校验数据成功。
- fresh 验证：engine 116.848s、全仓 engine 126.303s；集群 [`reports/compatibility/p1-cluster-current-continuation1002/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1002/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation1002/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1002/release-candidate.json) 为 `GO`，9/9 checks PASS，Connector/J 139/0/0/0，Maven BUILD SUCCESS。
- 仍未完成：XA 原生二进制日志/更细权限矩阵、完整 online DDL/MDL、全量 I_S/P_S、通用复杂 SQL AST、物理分区路由、真正多引擎 REPAIR TABLE 和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 175

- P1-SQL-005 完善 `TRUNCATE TABLE` 外键错误契约：被外键引用的父表在 `FOREIGN_KEY_CHECKS=1` 时返回 MySQL `ER_TRUNCATE_ILLEGAL_FK`（1701/42000），而不是普通文本错误；关闭该会话开关时仍允许执行。
- `TestTruncateReferencedParentTableIsRejected` 现在同时校验原数据保留与 errno 1701；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.529s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation175/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation175/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation175/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation175/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、更细触发器跨表锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 174

- P1-SQL-005 补齐 `TRUNCATE TABLE` 的外键依赖边界：`FOREIGN_KEY_CHECKS=1` 时，存在子表外键引用的父表不能被 TRUNCATE，且原表数据保持不变；关闭该会话开关时保留导入/重建场景的放行语义。
- 新增 `TestTruncateReferencedParentTableIsRejected`；外键 TRUNCATE 专项与 `TRUNCATE` 原有元数据重建回归通过。`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.689s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation174/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation174/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation174/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation174/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、更细触发器跨表锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 1015

- P1/客户端元数据继续收口：`SHOW OPEN TABLES` 的 `In_use`/`Name_locked` 不再固定返回 `0,0`；执行器现在从当前 MDL 协调器快照投影已授予锁、等待状态和写模式，覆盖同一连接持有 `LOCK TABLES ... WRITE` 时的实时兼容视图。该实现明确是锁支持的兼容视图，不宣称复制 MySQL 私有 table-cache 内部计数。
- 新增 `TestXMySQLExecutor_ShowOpenTablesProjectsLiveLockUsage`；验证持锁时返回 `app/users/1/1`，解锁后恢复 `0/0`。定向 SHOW 测试通过；engine 全包 116.529s；全仓 Go `go test ./... -count=1 -timeout 30m` 通过（engine 121.535s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1015/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1015/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1015/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1015/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:15。
- 仍未完成的边界：完整 table-cache 私有计数、复杂分区表达式/类型转换与物理路由覆盖、`NOT BETWEEN` 等更广区间推导、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1016

- P1/分区优化：数值 RANGE 分区支持安全的常量 `NOT BETWEEN lower AND upper` 剪枝，将结果转换为 `< lower OR > upper` 的分区集合并集；无法证明的表达式、类型转换和其他分区方法继续保守保留全分区扫描。
- 新增 `TestPartitionedBTreeManagerUnionsConstantNotBetweenRanges`，并在真实 RANGE 分区查询中覆盖 `WHERE id NOT BETWEEN 10 AND 20`；专项回归通过。engine 全包 117.502s；全仓 Go 通过（engine 119.316s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1016/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1016/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1016/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1016/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:06。
- 仍未完成的边界：完整 table-cache 私有计数、复杂分区表达式/类型转换和物理路由覆盖、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1017

- P1/分区优化继续收口：LIST 分区支持字符串常量 `NOT IN` 的安全剪枝；仅当某分区的所有已枚举值都在排除集合中时移除该分区，未枚举/default、动态表达式、RANGE 及其他无法证明的情况保持保守扫描。
- 新增 `TestPartitionedBTreeManagerPrunesConstantNotInListValues`，并在真实字符串 LIST 分区查询覆盖 `WHERE region NOT IN ('east','west')`；分区兼容测试组通过。engine 全包 116.880s；全仓 Go 通过（engine 120.102s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1017/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1017/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1017/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1017/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:17。
- 仍未完成的边界：完整 table-cache 私有计数、复杂分区表达式/类型转换和物理路由覆盖、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1018

- P0/运行时可观测性：把已有 metrics recorder 接入三条真实运行路径。owner-aware MDL 等待记录 lock-wait counter/histogram；崩溃恢复成功/失败记录 recovery counters；检查点记录更新 dirty-pages gauge 和 checkpoint counter。
- 新增 `TestTableDDLCoordinatorRecordsRuntimeLockWaitMetric` 与 `TestCrashRecoveryAndCheckpointPathsRecordRuntimeMetrics`；相关 engine/manager/metrics/net 包通过，engine 116.850s、manager 10.903s、全仓 Go 通过（engine 120.320s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1018/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1018/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1018/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1018/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:18。
- 仍未完成的边界：完整 table-cache 私有计数、复杂分区表达式/类型转换和物理路由覆盖、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1019

- P1/分区物理路由：补齐单列字符串 `RANGE COLUMNS` 的持久化行路由；`VALUES LESS THAN ('m')` 等边界现在可正确把字符串行写入对应物理分区并读回。数值 `RANGE COLUMNS` 既有行为保持不变。
- 新增 `TestRangeColumnsStringPartitionRoutingAndReadback`，并保留 `TestRangeColumnsPartitionRoutingAndReadback` 覆盖数值路径；定向分区回归通过，engine 全包 115.440s；全仓 Go 通过（engine 120.170s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1019/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1019/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1019/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1019/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:12。
- 仍未完成的边界：多列 `RANGE/LIST COLUMNS`、完整 MySQL 字符集/校对序排序和字符串分区剪枝、复杂分区表达式/类型转换及更广物理路由、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1020

- P1/分区优化继续收口：单列字符串 `RANGE COLUMNS` 现在支持计划层常量 `=`、`<`、`<=`、`>`、`>=` 的安全剪枝；字符串边界使用 `LessThanText`，遇到混合数值/文本边界、复杂表达式或无法证明的类型转换时保守保留全分区扫描。1019 已补齐的字符串物理行路由与本轮计划层裁剪保持一致。
- 新增 `TestPartitionedBTreeManagerPrunesStringRangeValues` 与 plan 矩阵用例，覆盖 `'alpha' < 'm'` 和 `'zulu' >= 'm'`；定向 plan/engine 测试通过。engine 全包 115.842s；全仓 Go `go test ./... -count=1 -timeout 45m` 通过，engine 120.293s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1020/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1020/cluster-report.json) 为 `PASS`：一主两从、提交 DML/DDL、回滚隔离和从库提升场景通过。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1020/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1020/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:09。
- 仍未完成的边界：多列 `RANGE/LIST COLUMNS`、完整 MySQL 字符集/校对序排序和复杂字符串类型转换、复杂分区表达式及更广物理路由、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1021

- P1/分区剪枝继续收口：单列字符串 `RANGE COLUMNS` 支持常量 `BETWEEN` 的上下界求交和 `NOT BETWEEN` 的 `< lower OR > upper` 集合并；文本边界解析只接受明确字符串字面量，未知/混合类型继续保守扫描。
- 新增 `TestPartitionedBTreeManagerIntersectsStringBetweenRange` 与 `TestPartitionedBTreeManagerUnionsStringNotBetweenRange`；engine 全包 115.095s；全仓 Go 通过，engine 119.592s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1021/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1021/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1021/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1021/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:13。

### Continuation 1022

- P1/分区物理路由继续推进：`RANGE COLUMNS` 支持单列之外的多列元组边界，按 MySQL 风格字典序比较复合数值/字符串键；建表、插入和持久化分区读回已覆盖。多列查询剪枝仍保守走全分区扫描，不宣称已完成。
- 新增 `TestRangeColumnsMultiColumnPartitionRoutingAndReadback`，覆盖 `(tenant_id, region)` 对 `(10, 'm')` 边界的路由；engine 全包 115.778s；全仓 Go 通过，engine 119.850s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1022/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1022/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1022/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1022/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:18。
- 仍未完成的边界：多列 `LIST COLUMNS`、多列 RANGE 查询剪枝、分区 ALTER/REORGANIZE 的多列边界验证、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及更广物理路由，以及其余 P1/P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1023

- P1/分区物理路由继续推进：`LIST COLUMNS` 支持多列元组枚举值，例如 `VALUES IN ((1, 'east'), (2, 'west'))`；插入按复合键精确匹配到独立物理分区并读回。未知元组、混合类型和多列 LIST 查询剪枝仍保守处理。
- 新增 `TestListColumnsMultiColumnPartitionRoutingAndReadback`；engine 全包 116.143s；全仓 Go 通过，engine 121.176s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1023/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1023/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1023/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1023/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:13。
- 仍未完成的边界：多列 LIST/RANGE 查询剪枝、分区 ALTER/REORGANIZE 的多列边界验证、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及更广物理路由，以及其余 P1/P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1024

- P1/分区查询剪枝继续推进：多列 `RANGE COLUMNS` 支持 `(col1, col2) =/>=/<... (constant1, constant2)` 的字典序边界裁剪，多列 `LIST COLUMNS` 支持 tuple `IN` 精确裁剪；无法解析的元组、混合类型和复杂表达式继续保守保留全分区。
- 新增 `TestPartitionedBTreeManagerPrunesMultiColumnRangeValues`、`TestPartitionedBTreeManagerPrunesMultiColumnListValues` 与 plan 层多列边界回归；engine 全包 118.786s；全仓 Go 通过，engine 122.587s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1024/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1024/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1024/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1024/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:12。
- 仍未完成的边界：多列分区 `BETWEEN/NOT BETWEEN` 与更广谓词推导、分区 ALTER/REORGANIZE 的多列边界验证、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及更广物理路由，以及其余 P1/P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1025

- P1/分区查询剪枝继续收口：多列 `RANGE COLUMNS` 支持 tuple `BETWEEN`/`NOT BETWEEN`，分别通过上下界集合求交和 `< lower OR > upper` 集合求并裁剪；首分区无下界、末分区无上界时保持正确保守语义。
- 新增 `TestPartitionedBTreeManagerIntersectsMultiColumnBetweenRange` 与 `TestPartitionedBTreeManagerUnionsMultiColumnNotBetweenRange`；engine 全包 115.185s；全仓 Go 通过，engine 120.615s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1025/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1025/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1025/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1025/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:14。
- 仍未完成的边界：多列分区更广谓词推导、分区 ALTER/REORGANIZE 的多列边界验证、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及更广物理路由，以及其余 P1/P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1026

- P1/分区查询剪枝继续收口：多列 `LIST COLUMNS` 支持 tuple `NOT IN` 的安全裁剪；只有分区内所有已枚举元组均在排除集合时才移除该分区，未知/default 元组和不完整排除集合保持保守扫描。
- 新增 `TestPartitionedBTreeManagerPrunesMultiColumnNotInListValues`；engine 全包 115.873s；全仓 Go 通过，engine 120.077s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1026/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1026/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1026/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1026/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:14。
- 仍未完成的边界：分区 ALTER/REORGANIZE 的多列边界验证、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及更广物理路由，以及其余 P1/P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1028

- P1/分区维护继续收口：多列 `RANGE COLUMNS` 的 `ALTER TABLE ... TRUNCATE PARTITION` 与 `DROP PARTITION` 现在按完整 tuple 表达式定位行，确保目标分区数据被真实删除而不是只更新元数据；`ADD PARTITION` 现在对新增后的完整 RANGE 定义统一校验 tuple 边界升序、`MAXVALUE` 位置以及 scalar/tuple 不能混用。
- 新增 `TestMultiColumnDropAndTruncatePartitionDeletesRows` 与 `TestAddMultiColumnRangePartitionExtendsTupleDescriptor`；engine 全包通过（114.995s），全仓 Go 通过（engine 120.589s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1028/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1028/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1028/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1028/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:14。
- 仍未完成的边界：更广分区 ALTER/REORGANIZE 语义、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及物理路由覆盖、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1029

- P1/分区维护继续收口：多列 `LIST COLUMNS` 现在支持 `REORGANIZE PARTITION` 的 tuple 物理搬迁与元数据替换；CREATE/ADD/REORGANIZE 共用 LIST 定义校验，拒绝跨分区重复 tuple、tuple 元数不一致以及 scalar/tuple 混用。
- 新增 `TestReorganizeMultiColumnListPartitionsPreservesRows` 与 `TestReorganizeMultiColumnListPartitionsRejectsDuplicateTuple`；engine 全包通过（116.414s）；全仓 Go 通过（engine 125.829s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1029/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1029/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1029/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1029/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:13。
- 仍未完成的边界：更广分区 ALTER/REORGANIZE 语义、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及物理路由覆盖、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1030

- P1/分区物理路由继续推进：支持确定性日期函数分区表达式 `YEAR(date_col)`、`MONTH(date_col)`、`DAY(date_col)`/`DAYOFMONTH(date_col)` 的行路由；`RANGE (YEAR(event_date))` 已覆盖建表、插入、物理分区读回以及 `WHERE YEAR(event_date)=...` 的安全分区剪枝。
- 新增 `TestRangeFunctionPartitionRoutingAndReadback` 与 `TestPartitionedBTreeManagerPrunesYearFunctionRange`；engine 全包通过（116.340s）；全仓 Go 通过（engine 122.865s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1030/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1030/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1030/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1030/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:18。
- 仍未完成的边界：更多日期/算术/类型转换分区表达式、完整字符集/校对序排序、更广物理路由和 ALTER 维护语义、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1031

- P1/分区物理路由继续推进：增加 `TO_DAYS(date_col)` 确定性日期分区表达式，使用 MySQL 日序计算参与 RANGE 路由；`RANGE (TO_DAYS(event_date))` 已覆盖建表、插入和物理分区读回。
- 新增 `TestRangeToDaysFunctionPartitionRoutingAndReadback`；engine 全包通过（116.190s）；全仓 Go 通过（engine 121.881s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1031/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1031/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1031/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1031/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:18。
- 仍未完成的边界：更多日期/算术/类型转换分区表达式、完整字符集/校对序排序、更广物理路由和 ALTER 维护语义、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1032

- P1/分区物理路由继续推进：支持单列确定性算术分区表达式 `column + constant`、`-`、`*`、`/`、`DIV` 和 `%` 的 RANGE 路由，并保留表达式边界的安全剪枝。
- 新增 `TestRangeArithmeticPartitionRoutingAndReadback` 与 `TestPartitionedBTreeManagerPrunesArithmeticRange`；engine 全包通过（121.125s）；全仓 Go 通过（engine 126.266s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1032/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1032/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1032/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1032/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:15。
- 仍未完成的边界：更复杂/多列算术、类型转换和非确定性表达式、完整字符集/校对序排序、更广物理路由和 ALTER 维护语义、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1033

- P1/分区物理路由继续推进：支持单列 `CAST(column AS SIGNED/UNSIGNED)` 类型转换分区表达式，字符串列可在 RANGE 分区写入时按数值转换结果路由。
- 新增 `TestRangeCastPartitionRoutingAndReadback`；engine 全包通过（119.576s）；全仓 Go 通过（engine 121.281s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1033/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1033/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1033/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1033/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:13。
- 仍未完成的边界：更复杂/多列类型转换和非确定性表达式、完整字符集/校对序排序、更广物理路由和 ALTER 维护语义、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 173

- P1-SQL-003 修正 AFTER UPDATE 触发器的 `OLD`/`NEW` 行语义：副作用语句现在分别使用更新前与更新后的行值，避免 `OLD.col` 被错误替换为新值；新增 `TestAfterUpdateTriggerReceivesDistinctOldAndNewRows`，并通过触发器递归/失败补偿专项回归。
- 本轮验证：`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.768s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation173/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation173/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation173/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation173/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、更细触发器跨表锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Suggested Order

1. Optimizer correctness before executor performance.
2. SQL compatibility breadth before optional storage features.
3. Security hardening before broader deployment.
4. Benchmarks after P0 correctness is stable, otherwise numbers are misleading.

### Continuation 1013

- P1/partition compatibility continues to close the constant-pruning boundary: LIST partitions now retain quoted text values in planner rules and support safe constant string `=`/`IN` pruning; quoted LIST values are normalized consistently for physical row routing. Numeric RANGE/LIST behavior and unsupported/dynamic predicates remain conservative.
- Added `TestStringListPartitionRoutingAndPruning`, `TestPartitionedBTreeManagerPrunesStringListValues`, and the plan-level string LIST case. Targeted plan/engine tests passed; engine full package passed in 114.924s; full Go `go test ./... -count=1 -timeout 30m` passed with engine 119.430s.
- Fresh cluster smoke [`reports/compatibility/p1-cluster-current-continuation1013/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1013/cluster-report.json) is `PASS`.
- Fresh release candidate [`reports/compatibility/release-candidate-current-continuation1013/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1013/release-candidate.json) is `GO`: 9/9 checks `PASS`, Connector/J 139 tests with 0 failures/errors/skips, Maven `BUILD SUCCESS`. The JDBC performance stage took 23:43 and remains a performance-risk observation, not a functional failure.
- Remaining boundaries are still explicit: complex partition expressions/type conversion and physical routing breadth, arbitrary recursive/correlated SQL ASTs, complete online DDL/MDL, full I_S/P_S fidelity, native XA/binlog interoperability, specialized non-InnoDB repair, and P3/P4 semantics. FULLTEXT and non-Connector/J full-client coverage remain intentionally postponed per the current priority decision.

### Continuation 1014

- 分区剪枝继续补齐：RANGE 分区现在支持数值常量 `BETWEEN lower AND upper`，通过上下界分区集合求交实现安全剪枝；不支持的表达式仍保持全分区扫描，避免错误裁剪。
- 新增 `TestPartitionedBTreeManagerIntersectsConstantBetweenRange`，并在真实分区表查询中覆盖 `WHERE id BETWEEN 10 AND 20`。plan/engine 定向测试通过；engine 全包 115.429s；全仓 Go 通过，engine 120.680s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1014/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1014/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1014/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1014/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:13。
- 仍未完成的边界：复杂分区表达式/类型转换、`NOT BETWEEN` 等更广的区间推导、物理分区路由覆盖、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1034

- P1/分区维护继续推进：实现 `ALTER TABLE ... EXCHANGE PARTITION ... WITH TABLE`，支持普通表与分区表分区之间的列元数据校验、分区归属校验、行交换和 `WITHOUT VALIDATION` 选项；交换失败时保持原元数据不变。
- 新增 `TestExchangePartitionSwapsRowsWithOrdinaryTable`；定向测试与 engine 全量回归通过（119.025s），全仓 Go 回归通过（engine 124.283s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1034/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1034/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1034/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1034/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:13。
- 仍未完成的边界：无主键表的安全行级分区迁移、完整字符集/校对序排序、更复杂分区表达式和物理路由、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1035

- P1/分区 ALTER 维护继续推进：实现 `ALTER TABLE ... REMOVE PARTITIONING`，把已有分区行搬回普通表存储，清除持久化分区描述并验证转换后的普通表继续支持读写；同时修复 `.frm` 索引元数据回填 `TableMeta.PrimaryKey` 的缺口。
- 新增 `TestRemovePartitioningMigratesRowsToOrdinaryTable`；定向测试、engine 全量回归通过（117.361s），全仓 Go 回归通过（engine 122.220s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1035/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1035/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1035/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1035/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:31。
- 仍未完成的边界：无主键表的重复行保持型分区迁移、更复杂分区 ALTER/REORGANIZE 和类型/校对序语义、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 1036

- P1/分区 ALTER 迁移继续收口：`REMOVE PARTITIONING` 的行迁移不再要求主键；无主键表按完整行条件逐条 `DELETE ... LIMIT 1`，可保留完全重复的多行记录。
- 新增 `TestRemovePartitioningPreservesDuplicateRowsWithoutPrimaryKey`；engine 全量回归通过（119.350s），全仓 Go 回归通过（engine 124.584s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1036/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1036/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1036/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1036/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:34。
- 仍未完成的边界：更复杂分区 ALTER/REORGANIZE 和类型/校对序语义、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

### Continuation 177

- P1-SQL-004/P1-SEC-001 补齐外键 DDL 的 `REFERENCES` 权限：`CREATE TABLE` 与 `ALTER TABLE ... ADD FOREIGN KEY` 现在均检查被引用表权限；原始 ALTER 兼容路径也纳入检查，自引用 CREATE 保留数据库级 CREATE 语义。
- 新增 `TestForeignKeyDDLRequiresReferencesPrivilegeOnParent`，覆盖无授权拒绝、授权后 CREATE 成功、撤权后 ALTER 拒绝和重新授权后 ALTER 成功。
- 本轮验证：`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（117.079s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation177/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation177/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation177/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation177/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、更细触发器跨表锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

### Continuation 1063

- 本轮继续收口 `INFORMATION_SCHEMA.INNODB_INDEXES`：现在从持久化 `.frm` 定义投影二级索引；用户定义 `PRIMARY` 返回官方 `TYPE=3`，唯一二级索引返回 `TYPE=2`，普通二级索引返回 `TYPE=0`；无用户主键表补出合成 `GEN_CLUST_INDEX`，返回 `TYPE=1`。二级索引暂无可信物理根页来源，因此 `PAGE_NO` 保持 `NULL`，不伪造页号。
- 新增并通过 `TestInformationSchemaInnoDBIndexesProjectsPersistedSecondaryIndexes`、`TestInformationSchemaInnoDBIndexesProjectsSyntheticClusteredIndex`，并将既有聚簇索引回归更新为校验 `TYPE=3`；I_S/P_S 专项通过（24.584s），engine 全量通过（141.150s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` 通过（engine 141.526s、integration 0.783s、manager 6.593s、net 7.543s、metrics 0.384s、protocol 0.539s、replication 3.878s）。
- 官方语义依据：[MySQL `INFORMATION_SCHEMA.INNODB_INDEXES`](https://dev.mysql.com/doc/mysql-infoschema-excerpt/8.0/en/information-schema-innodb-indexes-table.html)。
- 全局未完成边界仍包括：完整 `INFORMATION_SCHEMA` / `PERFORMANCE_SCHEMA` 表与字段覆盖、完整运行时统计/锁等待/线程语义、XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作、完整非 Connector/J 客户端矩阵，以及更复杂 SQL/online DDL/MDL 和 P3/P4；FULLTEXT 与 MyISAM/ARCHIVE/CSV、非 InnoDB `REPAIR TABLE`/引擎转换按范围明确后置或排除。

### Continuation 1064

- 本轮继续收口 InnoDB 字典：`INFORMATION_SCHEMA.INNODB_FIELDS` 现在按持久化索引定义投影主键和二级索引的全部字段，`INDEX_ID` 与 `INNODB_INDEXES` 共用规范化后的索引身份，`POS` 从 0 连续编号；同时补齐字典视图对 `IN (...)` 条件的过滤。
- `INFORMATION_SCHEMA.INNODB_TABLES` 按官方语义将 `N_COLS` 修正为用户列加 3 个隐藏 InnoDB 列，并投影可由持久化 instant 元数据证明的 `INSTANT_COLS`、`TOTAL_ROW_VERSIONS`；同时从持久化表选项投影 `ROW_FORMAT`，从表存储映射区分 file-per-table 的 `Single` 与共享表空间的 `General`。新增 `TestInformationSchemaInnoDBFieldsProjectsPersistedSecondaryIndexColumns`、`TestInformationSchemaInnoDBTablesReportsHiddenAndInstantColumnMetadata` 与 `TestInformationSchemaInnoDBTablesProjectsRowFormatAndTablespaceType`。
- I_S/P_S 专项通过（26.515s），engine 全量通过（144.186s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` 通过（engine 144.972s、integration 0.762s、manager 6.729s、net 7.861s、metrics 0.421s、protocol 0.811s、replication 1.994s）。
- 官方语义依据：[MySQL `INNODB_FIELDS`](https://dev.mysql.com/doc/mysql-infoschema-excerpt/8.0/en/information-schema-innodb-fields-table.html) 与 [MySQL `INNODB_TABLES`](https://dev.mysql.com/doc/mysql-infoschema-excerpt/8.0/en/information-schema-innodb-tables-table.html)。
- 仍未完成：`INNODB_TABLES` 的完整 FLAG/ROW_FORMAT/压缩页和 tablespace 类型编码、完整 I_S/P_S 表与运行时统计、官方 XA/binlog/复制/崩溃恢复双向互操作、完整非 Connector/J 客户端矩阵；FULLTEXT 与非 InnoDB 引擎/REPAIR 继续按范围后置或排除。

### Continuation 1065

- `INFORMATION_SCHEMA.INNODB_TABLESTATS.AUTOINC` 现在读取持久化自增序列状态；新增 `TestInformationSchemaInnoDBTableStatsProjectsPersistedAutoIncrement`，验证创建后下一个值为 1、插入一行后推进为 2。
- I_S/P_S 专项通过（26.240s），engine 全量通过（145.487s）。`OTHER_INDEX_SIZE`、`MODIFIED_COUNTER`、`REF_COUNT` 等仍没有完整的 InnoDB 统计采样来源，保持现有保守值，不伪造完整运行时统计。

### Continuation 1066

- `INFORMATION_SCHEMA.INNODB_COLUMNS.LEN` 现在按 InnoDB 字典的最大字节长度语义投影字符列长度：项目默认 `utf8mb4` 下 `VARCHAR(32)` 返回 `128`，数值列继续返回其持久化定义长度；新增回归更新 `TestInformationSchemaInnoDBColumnsReflectsDurableDefinition`。
- I_S/P_S 专项通过（27.321s），engine 全量通过（146.387s），随后串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` 通过（engine 145.138s、integration 0.750s、manager 6.748s、net 7.430s、metrics 0.556s、protocol 0.613s、replication 1.933s）。
- 官方语义依据：[MySQL `INNODB_COLUMNS`](https://dev.mysql.com/doc/mysql-infoschema-excerpt/8.0/en/information-schema-innodb-columns-table.html)。`PRTYPE` 的内部精确类型编码、`DEFAULT_VALUE` 的内部二进制默认值仍没有完整可信来源，继续保持保守投影。

### Continuation 1067

- `INFORMATION_SCHEMA.INNODB_COLUMNS.MTYPE` 继续收口：`BINARY`/`VARBINARY` 现在分别投影官方 `FIXBINARY/BINARY` 主类型编码 `3/4`，同时保留持久化长度；新增 `TestInformationSchemaInnoDBColumnsProjectsBinaryMainTypes`，覆盖两个字段和 `IN (...)` 条件。
- I_S/P_S 专项通过（27.363s），engine 全量通过（145.049s），随后串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` 通过（engine 145.138s、integration 0.750s、manager 6.748s、net 7.430s、metrics 0.556s、protocol 0.613s、replication 1.933s）。
- 官方语义依据：[MySQL `INNODB_COLUMNS`](https://dev.mysql.com/doc/mysql-infoschema-excerpt/8.0/en/information-schema-innodb-columns-table.html)。`PRTYPE`、instant 默认值的内部编码，以及完整 I_S/P_S 表/字段和运行时采样仍未完成。

### Continuation 1068

- `INFORMATION_SCHEMA.INNODB_FOREIGN.TYPE` 现在从持久化外键动作重建官方位标志：删除 `CASCADE/SET NULL/NO ACTION` 分别使用 `1/2/16`，更新 `CASCADE/SET NULL/NO ACTION` 分别使用 `4/8/32`，动作按位 OR 组合；默认 `RESTRICT` 保持 `0`。
- 新增 `TestInformationSchemaInnoDBForeignProjectsReferentialActionTypeFlags`，覆盖组合值 `9`、`20`，并将默认外键回归明确校验为 `0`。
- I_S/P_S 专项通过（26.980s），engine 全量通过（146.068s），随后串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` 通过（engine 147.067s、integration 0.850s、manager 7.826s、net 7.904s、metrics 0.364s、protocol 0.541s、replication 1.935s）。
- 官方语义依据：[MySQL `INNODB_FOREIGN`](https://dev.mysql.com/doc/refman/8.0/en/information-schema-innodb-foreign-table.html)。完整 I_S/P_S 覆盖、运行时采样、官方 XA/binlog/复制/崩溃恢复互操作等全局事项仍未完成。

### Continuation 1069

- `INFORMATION_SCHEMA.INNODB_TABLESPACES` 现在从持久化表空间 page 0 读取真实 FSP flags，并投影 `PAGE_SIZE`、`ZIP_PAGE_SIZE`、`FLAG`、`SPACE_FLAGS`、`SPACE_FLAGS2`；同时根据压缩状态投影 `ROW_FORMAT`，普通 16 KiB 表空间返回 `PAGE_SIZE=16384`、`ZIP_PAGE_SIZE=0`。
- 更新 `TestInformationSchemaInnoDBTablespacesReflectsDurableCatalog`，验证 page size、压缩页大小、row format 和 space flags；相关 tablespace/datafile 回归通过。
- I_S/P_S 专项通过（26.313s），engine 全量通过（149.350s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` 通过（engine 145.493s、integration 0.856s、manager 6.715s、net 7.468s、metrics 0.368s、protocol 0.558s、replication 1.868s）。
- 官方语义依据：[MySQL `INNODB_TABLESPACES`](https://dev.mysql.com/doc/mysql-infoschema-excerpt/8.0/en/information-schema-innodb-tablespaces-table.html)。`AUTOEXTEND_SIZE`、加密/SDI 等字段及完整上游 page-flag 解码仍缺少 xmysql 的权威来源，继续保守返回 `NULL` 或有限投影；完整 I_S/P_S 表与运行时统计、官方 XA/binlog/复制/崩溃恢复互操作、完整非 Connector/J 客户端矩阵仍未完成或未验证。

### Continuation 1070

- 本轮继续收口 I_S 权限视图：`COLUMN_PRIVILEGES`、`TABLE_PRIVILEGES`、`SCHEMA_PRIVILEGES`、`USER_PRIVILEGES` 现在按当前会话执行账号可见性判断；普通账号只看自身授权行，启用具备显式 `mysql.user` 读取权限的角色后可查看全局授权行，root/复制回放保持全局可见，并支持 `CREATE USER + SYSTEM_USER` 管理员组合。有效角色通过当前会话的 `active_roles` 合并，未启用的角色不会泄漏授权视图。
- 权限视图的 `GRANTEE` 等值/LIKE 过滤现在支持 MySQL SQL 字符串中的双单引号转义；新增 `TestInformationSchemaPrivilegeViewsHonorAccountVisibilityAndEffectiveRolePrivileges`，先以失败测试锁定角色可见性和转义过滤缺口，再完成实现。
- 权限/角色回归通过（4.870s），I_S/P_S 专项通过（28.130s），engine 全量通过（145.959s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 144.730s、integration 0.815s、manager 7.561s、net 7.247s、metrics 0.432s、protocol 0.605s、replication 3.939s）。
- 语义依据：[MySQL `USER_PRIVILEGES`](https://dev.mysql.com/doc/refman/8.0/en/information-schema-user-privileges-table.html)、[SCHEMA_PRIVILEGES](https://dev.mysql.com/doc/refman/8.0/en/information-schema-schema-privileges-table.html)、[COLUMN_PRIVILEGES](https://dev.mysql.com/doc/refman/8.0/en/information-schema-column-privileges-table.html)、[TABLE_PRIVILEGES](https://dev.mysql.com/doc/refman/8.0/en/information-schema-table-privileges-table.html)。
- 全局剩余项仍包括：完整 I_S/P_S 表和字段注册、字段精度与运行时统计/锁等待/线程语义；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端兼容矩阵。FULLTEXT 继续后置，MyISAM/ARCHIVE/CSV 及非 InnoDB `REPAIR TABLE`/引擎转换继续明确排除。

### Continuation 1071

- 本轮补齐 `PERFORMANCE_SCHEMA.events_statements_current`、`events_statements_history`、`events_statements_history_long` 的官方事件字段形状：新增 `END_EVENT_ID`、`SOURCE`、`TIMER_START/END`、`LOCK_TIME`、`DIGEST`、`DIGEST_TEXT`、错误/告警、行数、排序/扫描计数、嵌套事件、`STATEMENT_ID`、CPU/内存上限和 `EXECUTION_ENGINE` 等字段；现有 recorder 能提供的字段使用真实值，暂无权威来源的内部计数保持 `NULL`。
- `ROWS_AFFECTED`、`ROWS_SENT`、`WARNINGS`、`ERRORS`、`DIGEST_TEXT` 和 `EXECUTION_ENGINE` 已接入 statement history 行投影；current 事件保持未完成事件的 `END_EVENT_ID=NULL`，history 事件保留结束 ID。新增 `TestPerformanceSchemaStatementHistoryProjectsOfficialEventFields`，并将既有回归改为按列名定位，避免依赖旧的简化列序号。
- 完整 `TestPerformanceSchema` 通过（13.418s），engine 全量通过（148.548s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 146.829s、integration 0.739s、manager 6.685s、net 7.375s、metrics 0.372s、protocol 0.608s、replication 1.961s）。
- 官方语义依据：[MySQL `events_statements_current`](https://dev.mysql.com/doc/refman/8.0/en/performance-schema-events-statements-current-table.html)；MySQL 明确将 current/history/history_long 视为同一事件表结构的不同生命周期视图。
- 全局剩余项仍包括：其他 I_S/P_S 表的完整字段精度与真实运行时采样、锁/等待/线程完整语义；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端矩阵。FULLTEXT 继续后置，非 InnoDB 引擎与相关修复/转换继续排除。

### Continuation 1079

- 本轮补齐 `PERFORMANCE_SCHEMA.table_handles` 的运行时行生产：查询现在读取 live session 的 `sessionTableLockLease`，真实投影 `OBJECT_TYPE`、schema/table、`OWNER_THREAD_ID`、读写 `INTERNAL_LOCK/EXTERNAL_LOCK` 和稳定的实例标识；`UNLOCK TABLES` 后对应句柄消失，并支持已有列过滤/投影。
- 新增 `TestPerformanceSchemaTableHandlesExposeExplicitTableLocks`，验证显式 `LOCK TABLES ... READ` 返回句柄、观察会话可查询且释放后为空。
- 验证结果：定向测试通过；完整 `TestPerformanceSchema` 通过（15.530s）；engine 全量通过（151.291s）。
- 当前仍未完成：未持有显式表锁的隐式句柄、完整 `table_handles` 事件/内部锁语义，以及其他 I_S/P_S 表的全量字段精度和运行时采样；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作、完整非 Connector/J 客户端矩阵仍未完成或未验证。FULLTEXT 后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1080

- 全局任务重新按交付层次梳理：P0 保持 MySQL 启动、InnoDB 核心 CRUD、xmysql 集群复制/故障切换和 Connector/J 139 项门禁；P1-A 继续补完整 I_S/P_S 的字段精度、真实运行时统计、锁等待和线程生命周期；P1-B 纳入完整非 Connector/J 客户端矩阵；P1-C 纳入 XA 与官方 MySQL binlog/GTID/复制/崩溃恢复双向互操作。
- `INFORMATION_SCHEMA` / `PERFORMANCE_SCHEMA` 的“表名已注册”与“运行时语义已实现”分开统计：插件、NDB、线程池、UDF、Group Replication 专属表如果没有 xmysql 对应组件，只能标记为组件依赖或形状/空结果，不得冒充完整实现；同理，`table_handles` 当前已覆盖显式 `LOCK TABLES` lease，隐式句柄和完整内部锁事件仍是 P1-A 未完成项。
- 非 Connector/J 矩阵中，Go MySQL driver、PyMySQL、Node.js/mysql2 已有本地证据；MySQL CLI 因当前环境缺少 `mysql.exe` 仍为环境未验证。XA/xmysql-native binlog/恢复已有本地回归，但官方 MySQL 双向互操作必须使用官方 fixture 单独验收。
- MyISAM/ARCHIVE/CSV、非 InnoDB `REPAIR TABLE` 和引擎转换继续明确排除；FULLTEXT 继续后置，不作为当前 release blocker。
- 本轮文档复核通过；带详细输出的 `go test ./server/innodb/engine -run 'TestPerformanceSchema' -count=1 -timeout 120s -v` 通过，P_S 专项耗时 15.790s，exit 0。

### Continuation 1081

- P1-B 客户端矩阵补齐真实协议断言：runner 现在必须返回完整 `cases` 且每个计划用例均为 `PASS`，避免仅凭进程 exit 0 把未执行/漏报的客户端误判为通过。
- 修复多语句结果集的 MySQL wire packet sequence：拆分执行的后续结果集继续沿用同一响应序列，不再从 sequence 1 重启；新增 `TestHandleQueryMultiStatementContinuesPacketSequenceAcrossResults`，定向 `server/net` 回归通过。
- 修复 `SELECT 1 AS alias` 快捷路径丢失列别名：`HandleQueryWithRealSession` 临时消息现在保留 SQL/数据库，硬编码响应通过 SQL AST 投影别名；新增 dispatcher 回归，并保留 engine 层别名回归。
- Go runner 改用 `multiStatements=true` 和 `Rows.NextResultSet` 验证两个结果集的列名/值，再验证缺表错误；PyMySQL、Go MySQL driver、Node.js/mysql2 均完成同一 8-case 本地矩阵并通过：分别使用报告 `reports/compatibility/client-matrix-current-python-final/`、`client-matrix-current-go-final/`、`client-matrix-current-node-final2/`。
- 本轮局部证据：dispatcher 两项别名测试、engine 别名测试、net 多结果 sequence 测试均 exit 0；三个客户端矩阵均 `passed=true`。MySQL CLI 仍因当前环境缺少 `mysql.exe` 保持 `SKIPPED_ENVIRONMENT`，不能据此宣称完整跨客户端矩阵完成。
- 全局剩余项保持不变：完整 I_S/P_S 字段精度及运行时统计、隐式句柄/锁等待/线程完整语义；XA 与官方 MySQL binlog/GTID/复制/崩溃恢复双向互操作；剩余客户端和集群场景矩阵。FULLTEXT 继续后置，MyISAM/ARCHIVE/CSV 及非 InnoDB `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1078

- 本轮继续收口 `PERFORMANCE_SCHEMA` statement-event 的扫描计数：真实 `table_scan` 查询现在投影 `SELECT_SCAN=1`，并汇总到 `events_statements_summary_by_digest.SUM_SELECT_SCAN`；只对执行器明确选择聚簇全表扫描的路径计数，二级索引、join、临时表和其他算子仍不伪造该字段。
- 更新 `TestPerformanceSchemaStatementHistoryProjectsRowsExaminedFromClusteredScan`，验证同一条查询同时得到 `ROWS_EXAMINED=2`、`ROWS_SENT=1`、`SELECT_SCAN=1`，digest 汇总为 `SUM_ROWS_EXAMINED=2`、`SUM_SELECT_SCAN=1`。
- P_S 全套通过（16.036s），engine 全量通过（157.944s）；串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` 最终明确 `XMYSQL_TEST_EXIT_CODE=0`，engine 150.410s、integration 0.782s、manager 7.708s、net 7.608s、metrics 0.372s、protocol 0.577s、replication 1.954s。
- 全局剩余项仍包括：完整 I_S/P_S 表和字段覆盖、字段精度、所有运行时统计及锁/等待/线程完整语义；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端矩阵。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1076

- 本轮继续收口 `INFORMATION_SCHEMA.INNODB_TABLESTATS`：`CLUST_INDEX_SIZE` 优先读取聚簇索引叶页链，`OTHER_INDEX_SIZE` 现在汇总持久化二级索引管理器维护的页数，不再固定返回 0；`MODIFIED_COUNTER` 改为读取 `InfoSchemaManager` 的真实 DML invalidation 计数，并在旧持久化统计 reload 时保留待刷新计数；聚簇索引页链不可用时才回退到表空间总页数。
- 新增 `TestInformationSchemaInnoDBTableStatsProjectsSecondaryIndexPages`、`TestInformationSchemaInnoDBTableStatsProjectsModifiedCounter` 和 manager reload 回归，先复现固定 0/计数丢失，再验证修复后来自真实存储/索引管理器和 DML 计数源。
- I_S/P_S 专项通过（18.752s），engine 全量通过（146.605s）；串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` 明确 `XMYSQL_TEST_EXIT_CODE=0`，engine 145.961s、manager 7.318s、net 7.645s、replication 2.073s。
- 全局剩余项仍包括：其他 I_S/P_S 表的完整字段精度与真实运行时采样、锁/等待/线程完整语义；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端矩阵。FULLTEXT 继续后置，非 InnoDB 引擎与相关修复/转换继续排除。

### Continuation 1077

- 本轮继续收口 `PERFORMANCE_SCHEMA` statement-event 运行时统计：`ROWS_EXAMINED` 现在由 `ClusteredIndexScanner` 在实际检查每条有效聚簇记录时计数，沿 `SelectExecutor -> ExecutionContext -> RuntimeRecorder` 传递，并在 `events_statements_history_long` 与 `events_statements_summary_by_digest.SUM_ROWS_EXAMINED` 中投影；外层 `XMySQLEngine.ExecuteQuery` 的 statement 汇总通道同步保留该计数。
- 新增 `TestPerformanceSchemaStatementHistoryProjectsRowsExaminedFromClusteredScan`，先复现真实全表扫描返回 1 行但 `ROWS_EXAMINED=0` 的红测，再验证两条聚簇记录实际检查后 `ROWS_EXAMINED=2`、`ROWS_SENT=1`，digest 汇总为 2。二级索引访问路径同步记录实际取回的候选记录数；尚未把该字段扩展为完整 join、临时表、物化子查询和所有执行算子的 MySQL 等价统计。
- P_S/I_S 聚焦回归通过（20.015s），engine 全量通过（159.737s）；串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` 明确 `XMYSQL_TEST_EXIT_CODE=0`，engine 148.615s、integration 0.758s、manager 7.204s、net 7.476s、metrics 0.373s、protocol 0.530s、replication 1.852s。
- 全局剩余项仍包括：完整 I_S/P_S 表和字段覆盖、字段精度、所有运行时统计及锁/等待/线程完整语义；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端矩阵。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1072

- 本轮继续收口 Performance Schema stage-event 语义：`events_stages_current` 对运行中的 stage 保留基于执行开始时间的 `TIMER_START`，将 `END_EVENT_ID`、`TIMER_END`、`TIMER_WAIT` 保持为 `NULL`；`events_stages_history` / `events_stages_history_long` 根据 recorder 的完成时间和耗时生成单调的 `TIMER_START`、`TIMER_END`、`TIMER_WAIT` 窗口，并保留结束事件 ID。
- 新增 `TestPerformanceSchemaStageHistoryProjectsMonotonicTimerWindow` 与 `TestPerformanceSchemaCurrentStageLeavesCompletionTimersNull`，覆盖历史事件的时间窗口一致性以及 current/history 的结束语义。
- 完整 Performance Schema 套件通过（14.045s），engine 全量通过（148.923s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 147.667s、integration 0.765s、manager 7.646s、net 7.493s、metrics 0.390s、protocol 0.544s、replication 1.966s）。
- 语义依据：[MySQL `events_stages_current`](https://dev.mysql.com/doc/refman/8.0/en/performance-schema-events-stages-current-table.html)；官方说明 current 只保留当前 stage，history/history_long 只收集已结束 stage。
- 全局剩余项仍包括：其他 I_S/P_S 表的完整字段精度与真实运行时采样、锁/等待/线程完整语义；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端矩阵。FULLTEXT 继续后置，非 InnoDB 引擎与相关修复/转换继续排除。

### Continuation 1073

- 本轮继续收口 Performance Schema transaction-event 生命周期：`events_transactions_current` 对活动事务使用持久化的事务开始时间生成 `TIMER_START`，并将 `END_EVENT_ID`、`TIMER_END`、`TIMER_WAIT` 保持为 `NULL`；commit/rollback 写入 history/history_long 时使用相同单调时钟生成 `TIMER_START`、`TIMER_END`、`TIMER_WAIT`，并确保 `TIMER_WAIT = TIMER_END - TIMER_START`。
- 新增 `TestPerformanceSchemaTransactionEventsProjectLifecycleTimers`，覆盖活动事务和完成事务的 current/history 差异、结束 ID 以及时间窗口一致性。
- 完整 Performance Schema 套件通过（13.464s），engine 全量通过（146.065s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 145.839s、integration 0.807s、manager 6.878s、net 7.535s、metrics 0.367s、protocol 0.556s、replication 1.867s）。
- 全局剩余项仍包括：其他 I_S/P_S 表的完整字段精度与真实运行时采样、锁/等待/线程完整语义；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端矩阵。FULLTEXT 继续后置，非 InnoDB 引擎与相关修复/转换继续排除。

### Continuation 1074

- 本轮继续收口 Performance Schema wait-event 生命周期：`events_waits_current` 现在从真实锁等待的 `Since/WaitStarted` 生成非零 `EVENT_ID`、`TIMER_START`、当前 `TIMER_END` 和 `TIMER_WAIT`，并保持 `END_EVENT_ID=NULL`；`events_waits_history/history_long` 对已完成等待生成结束事件 ID和一致的计时窗口。
- `TIMED=NO` 现在按 MySQL 语义将 `TIMER_START/TIMER_END/TIMER_WAIT` 全部投影为 `NULL`，同步更新旧回归断言；未观测的对象字段仍保持 NULL，不伪造底层锁对象地址。
- 新增 `TestPerformanceSchemaWaitEventsProjectLifecycleTimers`，覆盖 current/history 的生命周期、事件 ID和 `TIMER_WAIT = TIMER_END - TIMER_START`；完整 P_S 套件通过（14.291s），engine 全量通过（147.592s），串行全仓 `go test -p 1 ./... -count=1 -timeout 35m` exit 0（engine 149.247s、integration 0.832s、manager 10.202s、net 7.633s、metrics 0.390s、protocol 0.603s、replication 1.920s）。
- 语义依据：[MySQL `events_waits_current`](https://dev.mysql.com/doc/refman/8.0/en/performance-schema-events-waits-current-table.html)，官方定义未结束事件的计时字段及 `TIMED=NO` 时三个计时字段均为 NULL。
- 全局剩余项仍包括：其他 I_S/P_S 表的完整字段精度与真实运行时采样、锁/等待/线程完整语义；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端矩阵。FULLTEXT 继续后置，非 InnoDB 引擎与相关修复/转换继续排除。

### Continuation 1075

- 本轮继续收口 `INFORMATION_SCHEMA.INNODB_TABLES.FLAG`：不再固定返回 0，改为从对应表空间页 0 的持久化 FSP flags 读取，并与 `INNODB_TABLESPACES.FLAG` 使用同一权威来源；缺少空间或页数据时仍保守返回 0。
- 新增 `TestInformationSchemaInnoDBTablesProjectsPersistedTablespaceFlag`，先修改持久化页标志形成红测，再验证 `INNODB_TABLES.FLAG` 与 `INNODB_TABLESPACES.FLAG` 同步投影为 7；普通 `ROW_FORMAT=COMPRESSED` 元数据不会被误判为已写入 FSP 压缩标志。
- 定向 InnoDB 字典回归通过；完整 P_S、engine 全量和串行全仓回归沿用本轮 wait-event 验证结果，后续需在最终交付门禁中重新采集最新全量证据。
- 全局剩余项仍包括：其他 I_S/P_S 表的完整字段精度与真实运行时采样、锁/等待/线程完整语义；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端矩阵。FULLTEXT 继续后置，非 InnoDB 引擎与相关修复/转换继续排除。

### Continuation 1082

- P1-A 继续收口 INFORMATION_SCHEMA 的权威时间字段：`TABLES.CREATE_TIME` 与 `PARTITIONS.CREATE_TIME` 现在都读取持久化 `.frm` 元数据中的 `created_at`，并投影为 MySQL datetime；`UPDATE_TIME`、`CHECK_TIME` 仍在没有权威来源时返回 `NULL`。
- 新增 `TestInformationSchemaPartitionsProjectsPersistedCreateTime`，与既有 `TABLES.CREATE_TIME` 回归一起验证表级和分区视图的持久化创建时间。
- P_S 运行时覆盖已扩展：`table_handles` 已验证显式 `LOCK TABLES` lease 和事务作用域隐式 lease 的出现/清理；`global_status`/`session_status` 的 `Uptime` 由 executor/server 启动时间计算，engine 启动时刷新起始时间。
- 本轮新增的 I_S/P_S 定向测试通过；最终交付前仍需重新采集 engine 全量和串行全仓回归证据。
- 全局剩余项仍包括：其他 I_S/P_S 表的完整字段精度与真实运行时采样、锁/等待/线程完整语义；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端矩阵。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1083

- P1-A 收口 INFORMATION_SCHEMA 账号授权视图的对象可见性：普通账号仅能看到自己以及其有对象权限的范围；拥有 `SELECT`/`UPDATE` 覆盖 `mysql.user` 的全局或 `mysql.*` 授权账号，可以看到其他账号的授权行。
- `informationSchemaPrivilegeAccountVisible` 改用统一的 scope 覆盖判断，保留 partial revoke 限制，不再把 `*.*`/`mysql.*` 错误地当作无法覆盖 `mysql.user` 的授权。
- 新增 `TestInformationSchemaPrivilegeViewsHonorGlobalMySQLUserVisibility`，并调整普通 schema 授权回归，覆盖普通账号与全局授权账号的差异；相关三项 privilege visibility 测试通过。
- 官方语义依据：[INFORMATION_SCHEMA 权限说明](https://dev.mysql.com/doc/refman/8.4/en/information-schema-introduction.html)、[TABLE_PRIVILEGES](https://dev.mysql.com/doc/refman/8.4/en/information-schema-table-privileges-table.html)、[SHOW GRANTS](https://dev.mysql.com/doc/refman/8.4/en/show-grants.html)。
- 全局剩余项仍包括：grant option、角色继承、partial revoke、管理员行为的完整权限矩阵；其他 I_S/P_S 字段精度与真实运行时采样、锁/等待/线程完整语义；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端矩阵。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1084

- P1-A 继续补齐 `PERFORMANCE_SCHEMA.global_status/session_status` 的运行时计数：`Connections` 现在由现有 metrics recorder 的认证连接累计值计算，不再遗漏 MySQL 常用连接总数状态项。
- 新增 `TestPerformanceSchemaGlobalStatusProjectsConnectionTotals`，以 recorder 当前累计值为基线，验证新增连接后 `global_status.Connections` 增量准确为 2；既有 `Threads_*`、`Queries`、`Uptime` 回归保持通过。
- 全局剩余项仍包括：P_S 其他运行时计数与精确 event/lock/thread 语义、I_S grant option/角色继承/partial revoke/管理员完整权限矩阵；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端矩阵。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1085

- P1-A 继续收口 INFORMATION_SCHEMA.FILES 的时间字段：对能映射到逻辑 schema/table 的 InnoDB tablespace，`CREATION_TIME` 与 `CREATE_TIME` 读取对应 `.frm` 的持久化 `created_at`；无法映射的系统/物理空间继续保留 `NULL`。
- 新增 `TestInformationSchemaFilesProjectsPersistedCreateTime`，与 `TABLES`、`PARTITIONS` 创建时间回归一起验证同一权威元数据源在三个 I_S 视图中的投影。
- 定向三项创建时间测试通过；最终引擎全量回归需在本次代码变更后重新采集。
- 全局剩余项仍包括：其他 I_S 字段精度与权威运行时值、P_S 其他运行时计数与精确 event/lock/thread 语义、I_S grant option/角色继承/partial revoke/管理员完整权限矩阵；XA 与官方 MySQL binlog/复制/崩溃恢复双向互操作；完整非 Connector/J 客户端矩阵。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1086

- P1-B 扩展非 Connector/J 客户端规范：在原有 8 个 case 基础上增加独立 `charset` case，分别验证 Go MySQL driver、PyMySQL 和 Node.js/mysql2 对 `_utf8mb4` 字符串的返回值。
- `client_matrix.ps1` 仍从 `client_matrix_cases.json` 动态读取必需 case，因此新增 case 会进入 runner 完整性校验；Go 编译、Python 语法检查、Node 语法检查和规范 case 列表校验通过。
- 当前仍未把单机 runner 误标为完整矩阵：MySQL CLI 仍因环境没有 `mysql.exe` 而为 `SKIPPED_ENVIRONMENT`，集群 endpoint case 及官方客户端交叉验证仍待对应运行环境。

### Continuation 1087

- 本轮验证证据刷新：`go test -p 1 ./... -count=1 -timeout 45m` exit 0；engine 149.276s、net 7.712s、replication 1.901s，其他 Go 包同样通过。
- xmysql 集群 smoke 通过：`reports/compatibility/p1-cluster-current-20260923/cluster-report.json`；内部 crash-recovery 三次重复通过；外部进程级 crash/recovery 三次通过：`reports/compatibility/external-crash-current-20260923/`。
- 客户端诊断刷新于 2026-09-23：Go/PyMySQL/Node 可用，MySQL CLI 因没有 `mysql.exe` 为 `SKIPPED_ENVIRONMENT`；实际功能矩阵因当前环境未设置受保护的 `XMYSQL_CLIENT_PASSWORD`，未运行且不能宣称通过。
- 全局剩余项仍包括：完整 I_S/P_S 语义与组件运行时来源、完整客户端/集群 endpoint 矩阵、XA 与官方 MySQL 双向 binlog/GTID/XA/崩溃恢复互操作。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1088

- P1-A 继续补齐 `PERFORMANCE_SCHEMA.global_status/session_status`：新增 `Aborted_connects`，由 metrics recorder 的认证失败累计值投影；与 `host_cache` 的当前失败计数分离，成功认证不会回退历史累计值。
- 新增 `TestPerformanceSchemaGlobalStatusProjectsAbortedConnections`，与 `Connections` 状态回归一起验证连接总数和认证失败总数均从运行时基线增量计算。
- 定向状态测试通过；最终引擎全量门禁需在本次变更后重新采集。
- 全局剩余项仍包括：其他 P_S 运行时计数与精确 event/lock/thread 语义、组件运行时来源；完整 I_S 权限矩阵、客户端/集群 endpoint 矩阵；XA 与官方 MySQL 双向 binlog/GTID/XA/崩溃恢复互操作。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1089

- `SHOW STATUS` / `SHOW GLOBAL STATUS` 现在复用 `PERFORMANCE_SCHEMA` 状态运行时投影，不再返回旧的硬编码 `Threads_connected=1`、`Uptime=3600`、`Questions=100`；`Connections`、`Aborted_connects`、`Uptime` 等状态项与 recorder/server 启动时间保持一致，LIKE/WHERE 和 AST filter 回归通过。
- 新增 SHOW 状态路由与运行时计数回归；状态测试改用独立 recorder，避免测试之间共享默认 runtime recorder 造成跨用例污染。
- 本轮最终串行全仓回归通过：`go test -p 1 ./... -count=1 -timeout 45m` exit 0；engine 149.007s、net 8.539s、replication 1.868s、metrics 0.377s、protocol 0.555s，其余 Go 包通过。
- 当前全局剩余项不变：完整 I_S/P_S 表/字段精度与所有组件运行时来源、锁/等待/线程完整语义；完整非 Connector/J 客户端及集群 endpoint 矩阵；XA 与官方 MySQL 双向 binlog/GTID/XA/崩溃恢复互操作。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1090

- P0 请求分发路径移除 `SELECT 1` 硬编码 shortcut：`HandleQueryWithRealSession` 和认证后的 `handleQueryMessage` 现在都进入真实 `SQLDispatcher`/引擎执行，保留 `SELECT 1` 作为无表连接探测时的权限例外，但不再伪造列和值。
- 新增 dispatcher 回归，使用独立测试引擎返回非 `1` 的结果，验证真实分发被调用并保留列别名；dispatcher 全量测试通过。
- 该变更未改变 FULLTEXT、非 InnoDB、完整 I_S/P_S、非 Connector/J 全量矩阵或官方 MySQL XA 互操作的范围判断；这些仍按当前全局任务继续推进或保留为环境/组件依赖。

### Continuation 1091

- P1-A 新增 `PERFORMANCE_SCHEMA.global_status/session_status` 的常用命令计数投影：`Questions`、`Com_select`、`Com_insert`、`Com_update`、`Com_delete`、`Com_begin`、`Com_commit`、`Com_rollback`、`Com_show`。
- 计数来自 `RuntimeRecorder` 的服务生命周期累计 query totals；session scope 则按真实 statement history 的 thread ID 过滤，避免把有界 history 当作 global lifetime counter。
- 新增 `TestPerformanceSchemaGlobalStatusProjectsStatementCommandCounters`；P_S 专项通过（15.587s），metrics 专项通过（0.431s）。本次变更后的全仓门禁仍需在最终交付前重新采集。
- 全局剩余项仍包括：完整 I_S/P_S 表/字段精度与所有组件运行时来源、锁/等待/线程完整语义；完整非 Connector/J 客户端及集群 endpoint 矩阵；XA 与官方 MySQL 双向 binlog/GTID/XA/崩溃恢复互操作。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1092

- 本轮最终串行全仓回归通过：`go test -p 1 ./... -count=1 -timeout 45m` exit 0；engine 172.048s、net 7.520s、replication 3.895s、metrics 0.553s、protocol 0.542s，其余 Go 包通过。
- 本轮涉及的 P0 dispatcher、P_S 状态计数和 runtime recorder 变更均已纳入该全仓证据；未执行 reset、clean、commit 或 push，保留现有用户工作区改动。
- 当前全局剩余项仍是：完整 I_S/P_S 表/字段精度与组件运行时来源、锁/等待/线程完整语义；完整 9-case 非 Connector/J 功能矩阵及集群 endpoint；XA 与官方 MySQL 双向 binlog/GTID/XA/崩溃恢复互操作。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1093

- 修复真实协议查询的 P_S 记账重复：增强网络路径已经由 InnoDB SQL 引擎记录完成查询，网络层不再二次写入 `StatementHistory`、`Questions` 和 `Com_*`；非增强回退路径仍保留协议层记账。
- 非增强协议记账现在从真实会话读取 `connection_id`、用户和主机，语句历史不再把客户端语句统一归到线程 `0`；新增真实引擎网络回归，验证同一 `select 1` 只产生一条事件且线程号等于连接 ID。
- 相关定向回归通过：`go test ./server/net ./server/dispatcher ./server/observability/metrics -count=1 -timeout 240s`；`git diff --check` 无错误（仅保留现有换行格式提示）。全仓门禁需在本轮最终交付前重新采集。
- 当前全局剩余项仍是：完整 I_S/P_S 表/字段精度与组件运行时来源、锁/等待/线程完整语义；完整 9-case 非 Connector/J 功能矩阵及集群 endpoint；XA 与官方 MySQL 双向 binlog/GTID/XA/崩溃恢复互操作。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1094

- P1-A 扩展 `PERFORMANCE_SCHEMA.status_by_account/status_by_host/status_by_user`：复用真实 statement history 和命令分类，补齐 `Com_select`、`Com_insert`、`Com_update`、`Com_delete`、`Com_begin`、`Com_commit`、`Com_rollback`、`Com_show` 以及 `Com_replace`、`Com_truncate`、`Com_set_option`、`Com_flush`、`Com_call_procedure` 等状态行。
- `global_status/session_status` 同步支持上述新增常用命令计数；新增账号状态聚合回归和全量 P_S 专项回归通过。
- 当前全局剩余项仍是：完整 I_S/P_S 表/字段精度与组件运行时来源、锁/等待/线程完整语义；完整 9-case 非 Connector/J 功能矩阵及集群 endpoint；XA 与官方 MySQL 双向 binlog/GTID/XA/崩溃恢复互操作。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1095

- 修复 P_S 客户端统计污染：认证和权限检查使用的内部临时 SQL 会话现在标记为 internal query，仍正常执行但不写入 `Questions`、`Com_*`、statement history 或错误摘要。
- 新增 `TestPerformanceSchemaIgnoresMarkedInternalEngineQueries`，验证内部 `SELECT` 不改变 runtime recorder 的 query totals 和 statement history；认证包与 P_S 定向回归通过。
- 当前全局剩余项仍是：完整 I_S/P_S 表/字段精度与组件运行时来源、锁/等待/线程完整语义；完整 9-case 非 Connector/J 功能矩阵及集群 endpoint；XA 与官方 MySQL 双向 binlog/GTID/XA/崩溃恢复互操作。FULLTEXT 继续后置，非 InnoDB 引擎及 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1096

- 本轮串行全仓门禁通过：`go test -p 1 ./... -count=1 -timeout 45m` exit 0；engine 154.563s、net 8.033s、replication 1.935s、metrics 0.372s、protocol 0.560s，其他 Go 包同样通过。
- 本轮新增的 P_S 状态聚合、常用 `Com_*` 计数和内部认证 SQL 过滤均包含在该全仓证据中；未执行 reset、clean、commit 或 push，保留现有用户工作区改动。

### Continuation 1097

- P1-A 继续扩展无歧义的常用 `Com_*` 状态：`Com_change_db`、`Com_explain`、`Com_describe`、`Com_analyze`、`Com_savepoint`、`Com_grant`、`Com_revoke`、`Com_lock_tables`、`Com_unlock_tables`、`Com_kill`、`Com_reset`。
- 这些状态同时出现在 global/session status 和按账号/主机/用户聚合的 P_S 状态视图中；P_S 专项回归通过（14.914s）。

### Continuation 1098

- 最新串行全仓门禁通过：`go test -p 1 ./... -count=1 -timeout 45m` exit 0；engine 149.792s、net 7.533s、replication 3.896s、metrics 0.434s、protocol 0.562s，其他 Go 包同样通过。
- 本轮改动未执行 reset、clean、commit 或 push，继续保留用户工作区现有改动。

### Continuation 1099

- P1-A 收口 `INFORMATION_SCHEMA.COLUMNS` 的一组真实字段精度：从持久化列定义读取整数/DECIMAL 的 `NUMERIC_PRECISION`、`NUMERIC_SCALE`，读取 `TIME/DATETIME/TIMESTAMP` 的 fractional `DATETIME_PRECISION`，保留 `UNSIGNED` 的 `COLUMN_TYPE`，并按 `utf8mb4` 计算 `CHARACTER_OCTET_LENGTH`；显式列字符集与排序规则优先于默认值。
- 新增 `TestInformationSchemaColumnsProjectsNumericTemporalAndCharacterPrecision`，先行失败后转绿；I_S 定向套件 `go test ./server/innodb/engine -run '^(TestInformationSchema|Test.*InformationSchema)' -count=1 -timeout 300s` 通过（23.393s）。
- 最新串行全仓回归 `go test -p 1 ./... -count=1 -timeout 45m` 通过；engine 151.251s、net 7.763s、replication 1.871s、metrics 0.525s、protocol 0.542s，其他 Go 包同样通过；`git diff --check` 无实际错误。
- 客户端诊断仍显示 Go/PyMySQL/Node.js 可用，MySQL CLI 因当前环境没有 `mysql.exe` 保持 `SKIPPED_ENVIRONMENT`；未设置 `XMYSQL_CLIENT_PASSWORD`，因此本轮不伪造功能矩阵通过。全局剩余项仍包括其他 I_S/P_S 字段与组件运行时语义、完整客户端/集群 endpoint 矩阵、XA 与官方 MySQL 双向 binlog/GTID/XA/复制/崩溃恢复互操作；FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1100

- 继续收口 `INFORMATION_SCHEMA.COLUMNS`：未显式指定精度的 `DECIMAL/NUMERIC` 现在按 MySQL 默认 `DECIMAL(10,0)` 投影 `NUMERIC_PRECISION=10`、`NUMERIC_SCALE=0`；新增默认精度回归，先行失败后转绿。
- I_S 定向套件通过（19.760s），最新串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过；engine 165.136s、net 8.078s、replication 1.971s、metrics 0.362s、protocol 0.558s，其余 Go 包通过。
- 当前仍不能宣称全局完成：I_S/P_S 的组件依赖表与剩余完整字段/运行时语义、完整非 Connector/J 客户端及集群 endpoint 矩阵、官方 MySQL 双向 XA/binlog/GTID/复制/崩溃恢复互操作仍需继续实现或外部 fixture 验证；FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1101

- 根据全局范围确认，完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 继续纳入 P1；需要继续补齐未覆盖表的字段精度、NULL/type/filter 语义、权威运行时值，以及组件依赖的锁/等待/线程和状态来源，不能仅以表名和空结果视为完成。
- 非 Connector/J 的完整客户端兼容矩阵继续纳入全局 backlog，但优先级后置为 P3；当前 Go/PyMySQL/Node.js 只有 runner 和局部证据，MySQL CLI 因环境没有 `mysql.exe` 仍未验证，完整功能矩阵和集群 endpoint 场景不能宣称通过。
- XA 继续拆分：xmysql-to-xmysql 的本地 XA/binlog/GTID/复制/崩溃恢复收口为 P2；与官方 MySQL 的双向 XA/binlog/GTID/复制/崩溃恢复互操作列为 P4 外部 fixture 门禁，当前没有官方 MySQL fixture，保持未验证。
- MyISAM、ARCHIVE、CSV、非 InnoDB `REPAIR TABLE` 和非 InnoDB 引擎转换不纳入全局剩余任务；FULLTEXT 继续按范围决策后置，不阻塞 P0。

### Continuation 1102

- P1-A 继续收口 `INFORMATION_SCHEMA.COLUMNS` 字符集元数据：表级 `DEFAULT CHARACTER SET/CHARSET` 在没有列级覆盖时现在继承到字符列，表级 collation 同步继承；无 `=` 的标准 MySQL 表选项写法也会被解析。
- `CHARACTER_OCTET_LENGTH` 现在按常见字符集最大字节数计算，覆盖 `utf8mb4`、`utf8/utf8mb3`、`utf16/utf16le`、`utf32`、`ucs2` 及常见双字节字符集；数值列不会错误继承字符集属性。
- 新增 `TestInformationSchemaColumnsInheritTableCharacterSetForOctetLength`，先行失败后转绿；I_S 定向套件通过（19.411s）。串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过，engine 153.204s、net 7.762s、metrics 0.372s、protocol 0.536s、replication 1.915s，其余 Go 包通过。
- 当前未完成项不变：完整 I_S/P_S 组件运行时和剩余字段语义、P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作；FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1103

- P1-A 继续收口视图元数据：`INFORMATION_SCHEMA.COLUMNS` 对普通视图现在保留源列的 `Scale`、`UNSIGNED`、字符集、排序规则、默认值、生成表达式和枚举值；源表的表级 charset/collation 会在列级未覆盖时传递到视图列。
- 新增 `TestInformationSchemaViewColumnsPreserveSourcePrecisionAndCharacterSet`，先行失败后转绿；覆盖视图中的 `VARCHAR`、`DECIMAL(8,2)` 和 `DATETIME(3)` 元数据。
- I_S 定向套件通过（19.190s）；串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过，engine 148.569s、net 7.462s、metrics 0.388s、protocol 0.590s、replication 3.847s，其余 Go 包通过。
- 当前未完成项仍为完整 I_S/P_S 组件运行时和剩余字段语义、P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作；FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1104

- P1-A 继续补齐 `INFORMATION_SCHEMA.PARAMETERS`：存储过程参数和函数返回值现在复用统一类型元数据计算，投影字符长度/字节长度、数值精度/scale、时间精度、字符集和排序规则；没有对应类型来源的字段继续保留 `NULL`。
- 新增 `TestInformationSchemaParametersProjectTypePrecision`，先行失败后转绿；I_S 定向套件通过（19.661s）。
- 最新串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过，engine 150.669s、net 7.581s、metrics 0.566s、protocol 0.547s、replication 1.912s，其余 Go 包通过。
- 当前未完成项仍为完整 I_S/P_S 组件运行时和剩余字段语义、P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作；FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1105

- P1-A 继续收口 `INFORMATION_SCHEMA.ROUTINES`：函数返回值现在投影字符长度/字节长度、数值 precision/scale、时间精度、字符集和排序规则；`RETURNS DECIMAL(8,2)` 等带逗号的返回类型会完整持久化，不再被旧正则截断。
- 新增 `TestInformationSchemaRoutinesProjectReturnTypePrecision`，先行失败后转绿；存储对象和 I_S 相关回归通过（21.054s）。
- 最新串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过，engine 151.151s、manager 8.476s、net 7.781s、metrics 0.359s、protocol 0.601s、replication 1.913s，其余 Go 包通过。
- 当前未完成项仍为完整 I_S/P_S 组件运行时和剩余字段语义、P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作；FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1106

- P1-A 修复 `PERFORMANCE_SCHEMA.events_statements_summary_*` 的累计来源：summary 视图现在读取 `RuntimeRecorder` 的实例生命周期聚合，不再仅从 256 条有界 statement history 重新计算；超过历史窗口后，`COUNT_STAR`、计时、错误、warning、affected/sent/examined rows 和 `SELECT_SCAN` 仍保持累计值。
- 新增 `TestPerformanceSchemaStatementSummaryRetainsLifetimeTotalsBeyondHistory`，先行失败并准确暴露 256/300 截断，改为生命周期聚合后转绿；metrics 与 engine 定向测试通过，engine 全套 `go test ./server/observability/metrics ./server/innodb/engine -count=1 -timeout 300s` 通过（engine 150.806s）。
- 最新串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过：engine 152.005s、manager 8.228s、net 7.859s、metrics 0.368s、protocol 0.550s、replication 1.971s，其余 Go 包通过。
- 本轮明确留下的 P1 子项：digest summary、statement histogram、部分 stage/status 投影仍直接依赖有界 history 或组件专用历史，尚未统一为 MySQL 级别的生命周期累计；完整 I_S/P_S 字段精度、NULL/type/filter 语义及锁/等待/线程权威来源仍需继续补齐。P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作仍未验证；FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1107

- P1-A 继续修复生命周期来源：`events_statements_summary_by_digest` 和 `events_statements_histogram_{global,by_digest}` 现在读取 `RuntimeRecorder` 的实例生命周期 statement summary；digest 的 `COUNT_STAR`、计时、错误、warning、行数、首末时间和 quantile 输入，以及 histogram 的 bucket 累计值不再被 256 条 history 截断。
- 新增 `TestPerformanceSchemaStatementDigestRetainsLifetimeTotalsBeyondHistory` 与 `TestPerformanceSchemaStatementHistogramRetainsLifetimeTotalsBeyondHistory`，均先行失败并暴露 256/300 截断，改为生命周期来源后转绿；P_S/metrics 专项 `go test ./server/observability/metrics ./server/innodb/engine -run '^(TestPerformanceSchema|TestRuntime)' -count=1 -timeout 300s` 通过（metrics 0.440s，engine 16.425s）。
- 最新串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过：engine 173.778s、manager 7.601s、net 7.547s、metrics 0.392s、protocol 0.588s、replication 5.908s，其余 Go 包通过。
- 当前 P1 剩余重点收敛为：stage/status 等仍依赖有界 history 的投影、其他 P_S/I_S 表的完整字段精度/NULL/type/filter 语义、锁/等待/线程及组件权威运行时来源。P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作仍未验证；FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1108

- P1-A 收口 statement-derived 生命周期来源：`events_stages_summary_by_{user,host,account,thread}_by_event_name`、`session_status` 以及 `status_by_account/status_by_host/status_by_user` 的命令计数现在复用实例生命周期 statement summary；有界 history 继续只服务 current/history 事件视图。
- 新增 `TestPerformanceSchemaStageSummaryRetainsLifetimeTotalsBeyondHistory` 与 `TestPerformanceSchemaSessionStatusRetainsLifetimeCommandTotalsBeyondHistory`，覆盖用户/全局/线程 stage summary 及 `Queries`/`Com_select`，均先行失败并准确暴露 256/300 截断，修复后转绿。
- 最新 P_S/metrics 专项通过：engine 16.188s、metrics 0.464s；最新串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过：engine 171.389s、manager 7.130s、net 7.531s、metrics 0.355s、protocol 0.551s、replication 2.030s，其余 Go 包通过。
- 当前 P1 重点进一步收敛为：其他 I_S/P_S 表的完整字段精度/NULL/type/filter 语义、锁/等待/线程及组件权威运行时来源；statement-derived summary 的生命周期累计已覆盖本轮范围。P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作仍未验证；FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1109

- 对 `RuntimeRecorder.StatementSummary()` 做并发安全收口：返回值现在在读锁内深拷贝 latency samples，避免与执行线程追加生命周期样本时发生数据竞争；不改变 P_S 投影结果。
- 并发修复后的专项回归通过：engine 16.555s、metrics 0.397s；最终串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过：engine 171.023s、manager 7.152s、net 7.570s、metrics 0.393s、protocol 0.594s、replication 3.912s，其余 Go 包通过；`git diff --check` 无实际错误，仅有既存换行格式提示。
- 当前全局任务仍未完成：P1 还需补齐其他 I_S/P_S 表的字段精度、NULL/type/filter 语义、锁/等待/线程及组件权威来源；P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作仍未完成或验证。FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1110

- P1-A 收口虚拟元数据：`INFORMATION_SCHEMA.COLUMNS` 对 `PERFORMANCE_SCHEMA` 虚拟表列现在复用统一类型精度计算；数值列投影 `NUMERIC_PRECISION/NUMERIC_SCALE`，字符列投影字符最大长度和字符集字节长度，`COLUMN_SIZE` 同步采用数值 precision 或字符长度，字符列以外的字符集/排序规则保持 NULL。
- 新增 `TestInformationSchemaVirtualPerformanceSchemaColumnsProjectTypePrecision`，先行失败并暴露 `COUNT_STAR.NUMERIC_PRECISION` 缺失，修复后转绿；I_S/P_S 定向套件通过（engine 34.911s）。
- 最新串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过：engine 170.593s、manager 7.095s、net 9.767s、metrics 0.409s、protocol 0.546s、replication 1.883s，其余 Go 包通过。
- 当前 P1 仍需继续补齐其他 I_S/P_S 表的完整字段精度、NULL/type/filter 语义、锁/等待/线程及组件权威来源；P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作仍未完成或验证。FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1111

- P1-A 继续收口生命周期 summary：`events_statements_summary_by_program` 新增独立实例生命周期累计，超过原 1024 条 program history 后仍保留 count、timer、statement wait、warning 和 rows；transaction summary 不再使用 1024 条 history 窗口截断；record-lock wait summary 新增不截断来源，`events_waits_summary_*` 和 table-lock summary 改用该来源，history 视图仍保持 bounded。
- 新增 `TestPerformanceSchemaProgramSummaryRetainsLifetimeTotalsBeyondHistory`、`TestPerformanceSchemaTransactionSummaryRetainsLifetimeTotalsBeyondHistory`、`TestPerformanceSchemaWaitSummaryRetainsLifetimeTotalsBeyondHistory`，分别覆盖 1100/1100/130 次执行或等待，均先行暴露 1024/128 截断后转绿。
- 同步补齐 `INFORMATION_SCHEMA.COLUMNS` 对虚拟 P_S 列的 numeric/character precision、octet length、`COLUMN_SIZE` 和 NULL 字符元数据；I_S/P_S、manager 定向测试及最新串行全仓门禁通过：engine 225.249s、manager 39.715s、net 7.674s、metrics 0.371s、protocol 0.580s、replication 3.881s，其余 Go 包通过。
- 当前 P1 剩余重点：其他 I_S/P_S 表的完整字段精度、NULL/type/filter 语义、线程/组件权威运行时来源，以及必要的 bounded-history 与 lifetime-summary 分层。P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作仍未完成或验证；FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1112

- P1-A 补齐元数据锁 summary 的生命周期来源：`tableDDLCoordinator` 现在将已完成的 owner-aware metadata-lock waits 分为有界 `MetadataLockWaitHistory()` 与不截断的 `MetadataLockWaitSummary()`；`events_waits_summary_by_*`、`events_waits_summary_by_instance` 和 `table_lock_waits_summary_by_table` 的 MDL 聚合改读 lifetime summary，`events_waits_history*` 继续只读 bounded history。
- 新增 `TestMetadataLockWaitSummaryRetainsLifetimeTotalsBeyondHistory`，先行验证 135 次真实 owner-aware MDL wait 后 history 保持 128 条而 summary 保留全部事件；元数据锁定向回归通过。
- 当前 P1 剩余重点仍是其他 I_S/P_S 表的完整字段精度、NULL/type/filter 语义、线程/组件权威运行时来源，以及必要的 bounded-history 与 lifetime-summary 分层。P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作仍未完成或验证；FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1113

- P1-A 补齐 `performance_schema.threads` 的内存运行时来源：`CONTROLLED_MEMORY`、`MAX_CONTROLLED_MEMORY`、`TOTAL_MEMORY` 和 `MAX_TOTAL_MEMORY` 现在按线程聚合 `RuntimeRecorder.MemorySummary()` 的当前字节数和高水位字节数，不再对有真实 recorder 数据的线程固定返回 0；无来源时仍保持 0，不推断 Go 进程堆内存。
- 新增 `TestPerformanceSchemaThreadsExposeRuntimeMemoryCounters`，先行验证旧实现返回 0，接入 recorder 后通过；线程视图定向回归通过。
- `UPDATE performance_schema.threads SET INSTRUMENTED/HISTORY=... WHERE THREAD_ID=...` 现在更新当前连接线程的监控与历史开关，并由同一线程快照读取；新增回归覆盖 engine dispatcher 路径。
- P3 客户端矩阵 runner 同步补齐 MySQL CLI 的 9 个场景：连接、DDL/DML、SQL PREPARE、事务回滚、NULL/类型、字符集、元数据、多结果/错误和重连；当前环境仍没有 `mysql.exe`，因此只能完成 runner 代码审计，不能伪造 CLI 功能通过。
- 当前 P1 剩余重点仍是其他 I_S/P_S 表的完整字段精度、NULL/type/filter 语义、线程 OS ID/组件权威来源和非 statement 组件数据；P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量实测及集群 endpoint、P4 官方 MySQL 双向互操作仍未完成或验证。FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1114

- P1-A 收口 statement-derived table I/O summary：`performance_schema.table_io_waits_summary_by_{table,index_usage}` 现在读取 `StatementSummary()` 的成功执行累计，并保留成功执行的 count/timer min/max；超过 256 条 bounded statement history 后不再回退到 256 条，错误执行不会被误计入 I/O summary。
- 新增 `TestPerformanceSchemaTableIOSummaryRetainsLifetimeTotalsBeyondHistory`，先行验证旧实现 300 次只返回 256 次，改为 lifetime success aggregate 后转绿；engine 与 metrics 定向回归通过。
- 最新串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过：engine 173.818s、manager 7.312s、net 7.666s、metrics 0.359s、protocol 0.541s、replication 1.923s，其余 Go 包通过。
- 当前 P1 剩余重点仍是其他 I_S/P_S 表的完整字段精度、NULL/type/filter 语义、线程 OS ID/组件权威来源和非 statement 组件数据；stored-program 内部语句统计、socket/file 非零 I/O 计数及组件依赖表仍需真实来源。P2 xmysql-native XA/复制边界、P3 非 Connector/J 全量实测及集群 endpoint、P4 官方 MySQL 双向互操作仍未完成或验证。FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1115

- P2 崩溃恢复验证补齐：`scripts/compatibility/crash_recovery_matrix.ps1 -Repeat 3` 三轮通过，覆盖故障注入/恢复回归；报告为 `reports/compatibility/crash-recovery-current-continuation1114/crash-recovery.json`。
- 外部进程级恢复演练补齐：`scripts/compatibility/external_crash_recovery_matrix.ps1 -Repeat 1 -Port 3312` 通过，覆盖 committed 数据保留、未提交事务回滚、DDL 元数据和重建索引元数据恢复；报告位于 `reports/compatibility/external-crash-current-continuation1114/`。
- 本轮结论：P2 xmysql-native 的本地 XA/binlog/复制/崩溃恢复测试已有通过证据，但 P4 与官方 MySQL 的双向 XA、binlog、GTID/复制、崩溃恢复互操作仍未完成；P1 的完整 I_S/P_S 字段/来源语义、P3 非 Connector/J 客户端功能实测和完整集群 endpoint 矩阵仍待继续。

### Continuation 1116

- P1-A 补齐 socket 运行时来源：`RuntimeRecorder` 新增按连接线程聚合的 socket read/write 计数、传输耗时和字节数；真实 `MysqlTCPConn.recv/send` 边界现在写入该 recorder。
- `performance_schema.socket_summary_by_instance` 与 `socket_summary_by_event_name` 读取 recorder 数据；无实际网络 I/O 的兼容性测试仍保持零值，有真实传输时返回 `COUNT_*`、`SUM_TIMER_*` 和 `SUM_NUMBER_OF_BYTES_*`。
- 新增 `TestRuntimeRecorderTracksSocketReadWriteLifecycle`、`TestPerformanceSchemaSocketSummariesExposeRuntimeTraffic` 和 `TestMysqlTCPConnRecordsSocketTrafficAtTransportBoundary`；metrics、engine、net 定向测试通过。
- 当前 P1 剩余重点收敛为其他 I_S/P_S 字段精度/NULL/type/filter 语义、`THREAD_OS_ID`/组件权威来源、stored-program 内部统计及组件依赖表；P2/P3/P4 的剩余边界保持上一轮结论不变。FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1117

- P1-A 补齐 stored-program 内部语句统计的生命周期来源：procedure 调用现在在执行前后对 `RuntimeRecorder.StatementSummary()` 做线程范围增量，`COUNT_STATEMENTS`、`SUM_STATEMENTS_WAIT`、错误、warning、affected/sent rows 和计时边界不再从 256 条 bounded statement history 截断计算。
- 新增 `TestPerformanceSchemaStoredProcedureStatementStatsRetainLifetimeDeltaBeyondHistory`，300 条内部 `SELECT` 先行暴露旧实现只返回 256，改为生命周期增量后返回完整 300；普通 procedure/function program summary 回归通过。
- 最新串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过：engine 174.657s、manager 7.126s、net 7.734s、metrics 2.174s、protocol 0.540s、replication 1.898s，其余 Go 包通过；P0 集群 smoke 也通过，报告为 `reports/compatibility/p1-socket-program-current-continuation1117/cluster-report.json`。
- 当前 P1 剩余重点进一步收敛为其他 I_S/P_S 字段精度/NULL/type/filter 语义、`THREAD_OS_ID`/组件权威来源及组件依赖表；P2/P3/P4 的剩余边界保持上一轮结论不变。FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1118

- P1-A 补齐 `INFORMATION_SCHEMA.PARTITIONS` 的真实行数来源：分区表按对应物理 partition B-tree `FullScan` 统计 `TABLE_ROWS`；非分区表复用现有表级物理/ANALYZE 统计，并同步 `AVG_ROW_LENGTH`、`DATA_LENGTH`、`INDEX_LENGTH`。
- 新增 `TestInformationSchemaPartitionsProjectPhysicalPartitionRowCounts`，覆盖两个 range 分区的 1/2 行和普通表的 2 行；旧实现的固定 0 结果已被回归测试捕获，修复后通过。`COLUMN_SIZE` 元数据回归也通过，确认 DECIMAL/时间/字符列分别投影 8/3/7。
- 最新串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过：engine 179.791s、manager 8.032s、net 7.720s、metrics 0.371s、protocol 0.550s、replication 1.923s，其余 Go 包通过；P0 集群 smoke 通过，报告为 `reports/compatibility/p1-partition-current-continuation1118/cluster-report.json`。
- 当前 P1 剩余重点进一步收敛为其他 I_S/P_S 字段精度/NULL/type/filter 语义、`THREAD_OS_ID`/组件权威来源及组件依赖表；P2/P3/P4 的剩余边界保持上一轮结论不变。FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1119

- P1-A 补齐普通 `BufferPoolManager` 的运行时统计来源：`GetStats()` 现在暴露真实 `hit_rate`、`young_hits`、`old_hits` 和 resident `cache_size`，`INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS` 不再因旧 buffer-pool 实现缺少这些键而把命中率和驻留页投影为固定 0。
- 新增 `TestBufferPoolManagerGetStatsExposesRuntimeHitCounters`；旧实现先行复现 `hit_rate=nil`，接入已有命中/分代计数后转绿。manager 定向回归通过；最新串行全仓门禁 `go test -p 1 ./... -count=1 -timeout 45m` 通过：engine 175.993s、manager 7.170s、net 7.569s、metrics 0.366s、protocol 0.535s、replication 1.897s，其余 Go 包通过；P0 集群 smoke 通过，报告为 `reports/compatibility/p1-buffer-pool-stats-current-continuation1119/cluster-report.json`。
- 当前 P1 剩余重点进一步收敛为其他 I_S/P_S 字段精度/NULL/type/filter 语义、`THREAD_OS_ID`/组件权威来源及组件依赖表；P2/P3/P4 的剩余边界保持上一轮结论不变。FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1120

- P1-A 补齐 `performance_schema.threads.THREAD_OS_ID` 的真实协议边界来源：Windows 使用 `GetCurrentThreadId`，Linux 使用 `gettid`；网络任务执行时锁定当前 OS 线程并将真实号码写入对应会话，`threads` 和 `setup_threads` 从会话来源投影，无法取得时保留 `NULL`，不使用连接 ID 冒充。
- 新增 `TestCurrentOSThreadIDIsAvailableAtProtocolBoundary`、`TestRecordOSThreadIDStoresIDOnMySQLSession` 和 `TestPerformanceSchemaThreadsExposeAuthoritativeOSThreadID`；平台/会话/线程视图定向回归通过，`go test ./server/net -count=1 -timeout 300s` 通过（7.842s），线程专项 engine 测试通过（4.628s）。
- 当前 P1 剩余重点进一步收敛为其他 I_S/P_S 字段精度/NULL/type/filter 语义、组件权威来源及组件依赖表；P2/P3/P4 的剩余边界保持上一轮结论不变。FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1121

- P1-A 补齐 `INFORMATION_SCHEMA.INNODB_METRICS` 的 Buffer Pool 运行时来源：当当前 buffer-pool manager 提供统计时，新增 `buffer_pool_pages_total/data/free/dirty`、`buffer_pool_read_requests`、`buffer_pool_reads` 和 `buffer_pool_write_requests`，分别来自真实驻留页、命中/未命中、脏页和物理页写计数；没有 provider 时不生成伪造行。
- 新增 `TestInformationSchemaInnoDBMetricsExposeBufferPoolRuntimeCounters`；旧实现先行复现 `buffer_pool_pages_%` 结果为空，接入 manager 统计后通过，并修正原有类型过滤回归以限定测试 metric 范围。
- 当前 P1 剩余重点进一步收敛为其他 I_S/P_S 字段精度/NULL/type/filter 语义、组件权威来源及组件依赖表；P2/P3/P4 的剩余边界保持上一轮结论不变。FULLTEXT 后置，非 InnoDB 引擎及相关 `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1122

- P0 Connector/J 元数据查询收口：修复 `INFORMATION_SCHEMA.TABLES/COLUMNS` 专用执行器把 Connector/J `SELECT` 投影中的 `CASE WHEN ...` 表达式误当作行过滤条件的问题；过滤器现在只解析 `FROM` 之后的条件，保留合法的 `WHERE/HAVING` 语义。新增 TABLES/COLUMNS 回归测试，覆盖 Connector/J 的 `BASE TABLE`、JDBC 类型 CASE 和 `HAVING TABLE_TYPE IN (...)` 形状。
- 当前源码构建的完整 Connector/J 套件通过：139 tests、0 failures、0 errors、0 skipped；其中 `DDLOperationsTest` 18/18 通过。结果证据在 `jdbc_client/target/surefire-reports/`，性能测试因 10,000 条批量数据耗时约 620 秒但最终通过。
- 修复后的串行全仓 Go 门禁通过：`go test -p 1 ./... -count=1 -timeout 45m`，engine 174.802s、manager 7.247s、net 7.654s、metrics 0.401s、protocol 0.566s、replication 1.890s，其余包通过；P0 集群 smoke 通过，报告为 `reports/compatibility/p1-jdbc-metadata-current-continuation1122/cluster-report.json`。
- 需要区分：此前 release-candidate JSON 仍是修复前的 NO-GO 快照，原因包括旧的 2 个 JDBC 元数据失败，以及当前环境未设置受保护的 `XMYSQL_CLIENT_PASSWORD` 导致非 Connector/J 客户端矩阵未执行；该旧报告不能覆盖本轮修复后的 139 项结果，也不能伪造 CLI/其他客户端或官方 MySQL 互操作通过。
- 全局剩余任务仍为：P1 其他 I_S/P_S 表的完整字段精度、NULL/type/filter 语义和组件权威运行时来源；P2 xmysql-native XA/binlog/GTID/复制/崩溃边界的进一步收口；P3 非 Connector/J 客户端完整矩阵与集群 endpoint；P4 官方 MySQL 双向互操作。完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 与非 Connector/J 客户端已纳入全局任务；MyISAM/ARCHIVE/CSV、非 InnoDB `REPAIR TABLE`/引擎转换不纳入；FULLTEXT 继续后置。

### Continuation 1123

- P1-A 补齐 `prepared_statements_instances` 的执行期统计来源：`PreparedStatementManager` 现在在每次实际 COM_STMT_EXECUTE 后累计执行耗时、最小/最大/平均耗时、失败次数、warning、affected/sent rows；Performance Schema 投影使用该 live snapshot，计时单位与其他 P_S timer 一致为皮秒（纳秒×1000）。
- 当前网络 handler 已覆盖成功结果、解析/绑定失败、执行失败和 server-side cursor 路径；新增 manager、Performance Schema 和真实 decoupled handler 回归，验证成功执行与失败执行的计数行为。Rows examined、reprepare、lock time、临时表、join/range/scan/sort 等字段仅在存在权威来源时投影，当前无来源字段保持 0，不返回伪造统计。
- 本轮验证：`go test ./server/net -run '^TestHandleComStmtExecute' -count=1 -timeout 120s` 通过；`go test -p 1 ./server/innodb/engine -count=1 -timeout 45m` 通过（180.328s）；最新串行全仓 `go test -p 1 ./... -count=1 -timeout 45m` 通过（engine 173.057s、net 7.640s、protocol 0.565s、replication 1.875s，其余包通过）；集群 smoke `reports/compatibility/p1-prepared-execution-20260923/cluster-report.json` 为 `PASS`。
- 全局剩余任务仍为：P1 其他 I_S/P_S 表的完整字段精度、NULL/type/filter 语义和组件权威运行时来源；P2 xmysql-native XA/binlog/GTID/复制/崩溃边界的进一步收口；P3 非 Connector/J 客户端完整矩阵与集群 endpoint；P4 官方 MySQL 双向互操作。完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 与非 Connector/J 客户端已纳入全局任务；MyISAM/ARCHIVE/CSV、非 InnoDB `REPAIR TABLE`/引擎转换不纳入；FULLTEXT 继续后置。

### Continuation 1124

- P1-A 补齐 `events_statements_summary_by_program.SUM_ROWS_EXAMINED`：program event 现在保存并聚合 `RowsExamined`；存储过程执行通过 child statement summary 的生命周期 delta 传入真实扫描行数，trigger/event/function 等当前没有同等权威来源的路径保持 0。
- 新增/扩展 program summary 回归：1100 次生命周期累计校验 `SUM_ROWS_EXAMINED`，真实表扫描的 stored procedure 校验 child rows-examined；engine 与全仓门禁均通过。
- 本轮验证：`go test ./server/innodb/engine -run '^TestPerformanceSchemaProgramSummary(TracksStoredProcedureCalls|RetainsLifetimeTotalsBeyondHistory)$' -count=1 -timeout 180s` 通过；`go test -p 1 ./server/innodb/engine -count=1 -timeout 45m` 通过（216.721s）；最新串行全仓 `go test -p 1 ./... -count=1 -timeout 45m` 通过（engine 172.867s、manager 7.854s、net 7.645s、metrics 0.385s、protocol 0.558s、replication 5.925s，其余包通过）；集群 smoke `reports/compatibility/p1-program-rows-examined-20260923/cluster-report.json` 为 `PASS`。
- 全局剩余任务仍为：P1 其他 I_S/P_S 表的完整字段精度、NULL/type/filter 语义和组件权威运行时来源；P2 xmysql-native XA/binlog/GTID/复制/崩溃边界的进一步收口；P3 非 Connector/J 客户端完整矩阵与集群 endpoint；P4 官方 MySQL 双向互操作。完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 与非 Connector/J 客户端已纳入全局任务；MyISAM/ARCHIVE/CSV、非 InnoDB `REPAIR TABLE`/引擎转换不纳入；FULLTEXT 继续后置。

### Continuation 1125

- P2 收口一处本地 XA/复制 exactly-once 崩溃窗口：`server/replication/source.go` 新增原子 `source_state.json`，将已执行 GTID 集合与 keyed transaction 的 XID/GTID 映射作为同一份 durable state 替换；既有 `source_gtid.json` 和 `source_transaction_keys.json` 保留为兼容镜像。启动时优先恢复原子状态，并继续从 binlog 重建提交 GTID，兼容旧数据目录和状态文件丢失恢复。
- 新增 `TestSourceCommittedTransactionKeySurvivesKeyStateCrashWindow`，模拟事务/binlog 与 GTID 已落盘、旧 key 镜像尚未替换即重启；重试同一 key 不再分配第二个 GTID。原有 keyed restart、GTID state loss、replication rotate/duplicate/XA 回归均通过。
- 本轮验证：`go test -p 1 ./server/replication -count=1 -timeout 30m` 通过（1.891s）；`go test -p 1 ./... -count=1 -timeout 45m` 通过（engine 178.801s、manager 10.205s、net 7.718s、replication 1.990s，其余包通过）；cluster smoke `reports/compatibility/p2-atomic-source-state-20260923/cluster-report.json` 为 `PASS`。
- P2 仍需继续覆盖 XA prepare/commit 与 native binlog/GTID/replica promotion 的全部故障注入边界；P1 完整 I_S/P_S 字段/来源语义、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作仍未完成或验证。完整 I_S/P_S、非 Connector/J 客户端和 XA/native 互操作已纳入全局任务；FULLTEXT 后置，非 InnoDB 引擎、非 InnoDB `REPAIR TABLE`/引擎转换不纳入。

### Continuation 1126

- P2 promotion 持久化再收口：`Runtime.Promote()` 现在使用 `Source.persistDurableState()` 写入副本已执行 GTID 集合，而不是只更新旧的 `source_gtid.json` 镜像；晋升后在第一次新写入前重启，也不会丢失副本已经应用的 GTID。
- 新增 `TestRuntimePromotionPersistsReplicaGTIDsBeforeNewWrite`，先行复现旧实现丢失 `upstream-source:7` 的晋升后重启窗口，修复后通过；既有 source/replica promote、fencing 和 auto-failover 回归保持通过。
- 本轮验证：`go test ./server/replication -count=1 -timeout 30m` 通过（1.871s）；最新 `go test -p 1 ./... -count=1 -timeout 45m` 通过（engine 176.411s、manager 7.886s、net 7.550s、replication 2.002s，其余包通过）；cluster smoke `reports/compatibility/p2-promotion-durable-state-20260923/cluster-report.json` 为 `PASS`。P2 仍需继续覆盖 XA prepare/commit、native binlog/GTID dump、replica reconnect 和 promotion 的完整故障注入；P1 完整 I_S/P_S 字段/来源语义、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作仍未完成或验证。

### Continuation 1127

- P2 补齐事务中途断线恢复：`Replica.Apply()` 现在从持久化 `relayEvents` 重建尚未提交的 `BEGIN/ROW` pending transaction；后续只收到 `COMMIT` 时仍会以完整 row/statement batch 原子应用，并通过 event identity 避免重放 relay event 造成重复。
- 新增 `TestReplicaResumesTransactionFromDurableRelayEvents`，覆盖 `BEGIN/ROW` 首次接收、replica 重启、仅接收 `COMMIT`、GTID 标记和已提交事务重放抑制；旧实现先行复现“提交后 applied rows 仍为空”，修复后通过。
- P2 仍需继续覆盖 XA prepare/commit、native binlog/GTID dump、replica reconnect 的更多 partial-event 形状、promotion 以及完整故障注入；P1 完整 I_S/P_S 字段/来源语义、P3 非 Connector/J 全量客户端及集群 endpoint、P4 官方 MySQL 双向互操作仍未完成或验证。

### Continuation 1128

- P2 收口最深的 keyed-XA 崩溃窗口：`BinlogEvent` 的逻辑 JSONL 记录现在携带内部 `TransactionKey`，native MySQL event 编码不包含该字段；`NewSource()` 在恢复已提交 COMMIT 事件时重建 key→GTID 映射，即使 `source_state.json`、`source_gtid.json` 和 `source_transaction_keys.json` 同时丢失，也不会因重试同一 key 产生第二个事务。
- 新增 `TestSourceCommittedTransactionKeyRecoversFromBinlogWhenStateFilesAreLost`，模拟 binlog 已落盘但所有 source state 尚未落盘的崩溃边界；旧实现先行复现重复 GTID，修复后通过。既有 native checksum、native dump、keyed restart 和 replication 回归保持通过。
- 本轮验证：`go test -p 1 ./server/replication -count=1 -timeout 30m` 通过（3.892s）；`go test -p 1 ./... -count=1 -timeout 45m` 通过（engine 172.984s、manager 7.196s、net 7.718s、replication 2.011s，其余包通过）；cluster smoke `reports/compatibility/p2-reconnect-key-recovery-20260923/cluster-report.json` 为 `PASS`。P2 仍需继续覆盖 XA PREPARE/COMMIT 原生事件、native GTID resume、replica reconnect 的完整故障注入和 promotion 互操作；P1/P3/P4 的剩余项仍未完成或验证。

### Continuation 1129

- P3 环境诊断重新验证：`scripts/compatibility/client_matrix.ps1 -DiagnosticOnly -ReportDirectory reports/compatibility/client-matrix-diagnostic-20260923` 明确报告 Go、PyMySQL、Node.js/mysql2 可用，`mysql.exe` 缺失并标记 `SKIPPED_ENVIRONMENT`，脚本以非零状态拒绝把诊断误报成全矩阵通过；本轮未设置或写入任何客户端密码。
- 因此 P3 的本地 runner 代码具备环境，但完整功能矩阵仍未完成实测；MySQL CLI 运行时和受保护认证凭据仍是外部环境门禁。P4 官方 MySQL fixture 也仍不可用；FULLTEXT 与非 InnoDB 范围维持既定后置/排除结论。

### Continuation 1130

- P1-A 补齐 `PERFORMANCE_SCHEMA.events_transactions_current/history` 的已有事务身份来源：当会话提供权威 `transaction_id` 时投影 `TRX_ID`；XA 会话的内部 XID key 现在解码为 `XID_FORMAT`、`XID_GTRID`、`XID_BQUAL`，并在 XA START 时记录真实事务计时起点。
- P1-A 补齐 `INFORMATION_SCHEMA.INNODB_TRX` 的 record-lock 运行时来源：`LockManager.HeldLockSnapshots()` 以已授予锁表为来源，按事务汇总 distinct table 数和 record-lock 数，投影 `TRX_TABLES_LOCKED`、`TRX_ROWS_LOCKED`；无对应锁管理器来源时继续保留 NULL。
- 新增 `TestPerformanceSchemaTransactionCurrentProjectsSessionTransactionID`、`TestPerformanceSchemaTransactionProjectsXAIdentity`、`TestInformationSchemaInnoDBTrxProjectsHeldLockCounts`；engine 和 manager 定向回归通过，既有 XA、事务生命周期和锁等待回归保持通过。
- P1 剩余仍是其他 I_S/P_S 字段精度、NULL/type/filter 语义、组件权威来源和完整 MySQL 8.4 表行为；P2 native GTID resume/故障注入、P3 全量客户端/集群 endpoint、P4 官方 MySQL fixture 仍未完成或验证。FULLTEXT 后置，非 InnoDB 引擎及相关修复/转换继续排除。

### Continuation 1131

- P2 补齐 native 复制事务中途断线的 durable relay：`NativeBinlogDecoder` 将“流在 XID_EVENT 前结束”区分为可恢复的 `ErrNativeTransactionIncomplete`；`Replica.ApplyNative` 把未完成 physical event 帧写入 `replica_gtid.json` 的 `native_relay_events`，重启后与下一段帧拼接，再以原有 GTID/row/statement exactly-once 路径提交。
- 新增 `TestReplicaResumesNativeTransactionFromDurableRelayEvents`，覆盖 native frame 分段、状态落盘、replica 重启、仅补交 XID_EVENT、单次应用和 GTID 标记；replication 全包通过。
- P2 仍待 native GTID 任意 file/position 的完整故障注入、XA PREPARE/COMMIT 与 promotion 边界；P1 完整 I_S/P_S、P3 非 Connector/J 客户端/集群 endpoint、P4 官方 MySQL 双向互操作仍未完成或验证。FULLTEXT 后置，非 InnoDB 引擎及相关修复/转换继续排除。

### Continuation 1132

- 发布候选门禁新增独立的 `p1-schema-observability`、`p2-xmysql-replication` 和 `cluster-smoke` 证据项；P1/P2 不再只依赖 integration 总包的汇总结果。PowerShell 语法检查通过，P2 专项回归通过，P1 的 `InformationSchema|PerformanceSchema` 组合回归重跑通过，集群 smoke 保持通过。
- P1 专项门禁首次运行出现一次 `TestPerformanceSchemaStageSummaryByIdentityHonorsInstrumentSettings` 的全局测试顺序波动；单测、Performance Schema 测试组和组合门禁随后均通过，当前未确认生产代码缺陷。该门禁仍保留完整测试命令，不以跳过测试规避波动。
- 全局剩余实现边界不变：P1 仍缺完整 I_S/P_S 字段精度、NULL/type/filter 语义及组件权威运行时来源；P2 仍缺 native GTID 任意 file/position、XA PREPARE/COMMIT 原生事件和 promotion 故障注入收口；P3 仍缺 `mysql.exe` 及完整非 Connector/J 客户端/集群 endpoint 证据；P4 仍等待官方 MySQL fixture。FULLTEXT 后置，非 InnoDB 引擎、非 InnoDB `REPAIR TABLE`/引擎转换继续排除。

### Continuation 1133

- P2 增加 native GTID 任意物理位置回归：从一个事务的 TABLE_MAP 之后断线时，`NativeDumpFileWithIntervals` 不发送残缺事务；从旋转文件的事务中途恢复时，`NativeDumpFrom` 保留 ROTATE 并从下一个完整 GTID 继续。两个新测试通过，现有 native dump/GTID/replica 回归未改变。
- 该项验证了当前实现的事务边界过滤，但不等于已经完成 XA 原生 PREPARE/COMMIT 事件发射或官方 MySQL 双向互操作；这两项仍是 P2/P4 未完成边界。

### Continuation 1134

- P2 补齐 xmysql-native XA 事件发射链路：`Source`/`BinlogWriter` 现在持久化 `XA_PREPARE_EVENT`、终态 `XA COMMIT`/`XA ROLLBACK`，prepared GTID 只在 COMMIT 后进入 executed 集合；Source 重启可从逻辑 binlog 和 durable state 恢复 prepared XID，重复 COMMIT 不追加第二个 native terminal event。
- engine XA 生命周期新增独立的 prepare/commit/rollback replication hooks；实际 source-role 接入后，XA PREPARE 不再误用普通提交事件，XA COMMIT/ROLLBACK 与本地 XA 状态机保持同一终态。replica 逻辑流和 native decoder/apply 均增加 prepared-XA 终态幂等处理。
- 新增/通过 `TestSourceEmitsAndRecoversNativeXAPrepareAndCommit`、`TestSourceEmitsNativeXARollbackWithoutExecutingGTID`、`TestReplicaAppliesLogicalXATransactionOnlyAtCommit`、`TestXAEngineHooksPublishNativePrepareAndTerminalBoundary`；replication 全包、engine XA/事务专项和 net replication/binlog 专项均通过。
- P2 尚未全部结束：仍需故障注入覆盖 prepare/append/terminal marker、prepared-XA promotion/restart、任意 file/position 和官方 MySQL native XA 互操作；P1 完整 I_S/P_S、P3 全量非 Connector/J 客户端/集群 endpoint、P4 官方 MySQL fixture 仍未完成或验证。FULLTEXT 后置，非 InnoDB 范围继续排除。

### Continuation 1135

- P2 收口 prepared-XA 身份幂等和 native resume：`Source` 将 prepared transaction key 与 reserved GTID 一起写入 `source_state.json`，重启后重复 `XA PREPARE` 不再追加第二个 PREPARE 或重新分配 GTID；native decoder 保留 `XA_PREPARE/XA_COMMIT/XA_ROLLBACK` 和完整 XID，而不是把终态降级为普通 COMMIT。
- `Replica.ReplicateNativeFrom` 现在可在 `XA_PREPARE` 后的物理 position 恢复并提交 XA；prepared-XA 状态跨 replica 重启保持，提交 GTID 进入正确的 source UUID 执行集，重复物理流不重复应用 row image。新增并通过 `TestSourcePrepareXAIsIdempotentByTransactionKey`、`TestReplicaPersistsNativePreparedXAAcrossRestart`，既有 native position/XA/replication 回归保持通过。
- 本轮定向验证：replication XA、native position、prepared-key 和 replica restart 测试通过。P2 仍需完整 failure-injection（prepare/append/terminal marker）、prepared-XA promotion/recovery 及官方 MySQL native XA 互操作；P1 完整 I_S/P_S、P3 全量非 Connector/J 客户端/集群 endpoint、P4 官方 MySQL fixture 仍未完成或验证。FULLTEXT 后置，非 InnoDB 范围继续排除。
- 最新 release-candidate gate 为 `NO-GO`：build、unit、integration、P1 schema/observability、P2 replication、go-core、cluster smoke、crash-recovery、concurrency、observability 和 JDBC 全部通过；JDBC 当前为 139/0/0/0。唯一失败项是 `client-matrix`，因为本机未提供受保护的 `XMYSQL_CLIENT_PASSWORD`，runner 在真正执行 Go/Python/Node/CLI 矩阵前拒绝运行；当前 `mysql.exe` 也不可用。该失败是环境门禁，不代表客户端行为已通过。报告：`reports/compatibility/release-candidate-continuation1135/release-candidate.json`。

### Continuation 1136

- P2 补齐 prepared-XA promotion：`Runtime.Promote()` 现在从副本 durable state 导入未决 XA 分支，保留 upstream GTID/XID、row/statement batch 和 transaction key；晋升后的 Source 可以继续 `XA COMMIT`，不会丢失 prepared transaction 或重新分配本地 GTID。
- 新增 source state failure-injection：模拟 XA PREPARE/COMMIT 已追加 binlog、但 source durable state 替换失败；重启时从逻辑 binlog 重建 prepared/committed 状态，并保证终态重试幂等。新增并通过 `TestRuntimePromotionPreservesPreparedXA`、`TestSourceXARecoversAfterPrepareStatePersistFailure`、`TestSourceXARecoversAfterCommitStatePersistFailure`，replication 全包通过。
- 当前 P2 剩余收敛为更细的 terminal-marker/replica apply failure 注入、promotion 后 native file/position 与多副本场景；P1 完整 I_S/P_S、P3 全量非 Connector/J 客户端/集群 endpoint、P4 官方 MySQL fixture 仍未完成或验证。FULLTEXT 后置，非 InnoDB 范围继续排除。

### Continuation 1137

- P2 增加 promotion 后 native prepared-XA 回归和双副本 exactly-once 回归：副本通过 `ReplicateNativeFrom` 接收 PREPARE 后晋升，仍可由新 Source 完成 COMMIT；两个独立副本重复消费同一 XA stream 各只应用一次 row image。
- 新增并通过 `TestRuntimePromotionPreservesNativePreparedXA`、`TestMultipleReplicasApplyXATransactionExactlyOnce`；本轮 replication 定向回归保持通过。剩余边界仍是 terminal-marker/replica-apply crash window 的存储引擎级故障注入，以及 P1/P3/P4 的未完成或外部验证项。

### Continuation 1138

- 本轮受影响范围验证通过：`go test -p 1 ./server/innodb/engine ./server/net ./server/replication -run 'Test.*(XA|Native|Replica|Replication|Binlog|Recovery|Promotion)' -count=1 -timeout 30m` 通过；随后 `go test -p 1 ./... -count=1 -timeout 45m` 全仓通过，其中 engine 177.156s、manager 6.829s、net 7.806s、replication 2.462s。
- 当前集群 smoke 重新执行并通过：`reports/compatibility/p2-promotion-current-continuation1138/cluster-report.json`，覆盖一主两从、提交 DML/DDL、回滚隔离和副本提升场景。
- P2 的 native promotion、多副本 exactly-once、source XA 状态持久化失败恢复已进入“本地代码已实现并验证”；仍未关闭的是 storage-engine apply 成功到 `replica_gtid.json` 状态提交之间的 crash window，以及 P4 官方 MySQL 双向互操作。P1 完整 I_S/P_S 运行时字段、P3 全量非 Connector/J 客户端/集群 endpoint 仍是全局剩余任务；FULLTEXT 后置，非 InnoDB 引擎及相关修复/转换继续排除。

### Continuation 1139

- P1/PERFORMANCE_SCHEMA 补齐一条有真实执行来源的运行时语义：实际 clustered/partitioned table scan 现在记录 `NO_INDEX_USED=1`，并贯穿 `events_statements_history*`、digest、thread/account/host/user 汇总；有二级索引的访问路径不会误报。
- 新增并先失败后修复 `TestPerformanceSchemaStatementHistoryProjectsNoIndexUsedFromTableScan`；`NO_GOOD_INDEX_USED` 仍因没有可靠 xmysql 优化器来源保持 0，未构造伪数据。完整 Performance Schema 与 metrics 定向回归通过。
- 受影响 engine 全包在单测顺序波动用例单独复跑确认通过后再次通过（`go test -p 1 ./server/innodb/engine -count=1 -timeout 30m`，174.892s）；当前 cluster smoke 也重新通过，报告为 `reports/compatibility/p1-schema-no-index-current-continuation1139/cluster-report.json`。此前那轮四包并行回归只因既有的 `TestPerformanceSchemaStageSummaryByIdentityHonorsInstrumentSettings` 顺序波动返回失败，不作为本轮代码失败结论。
- P1 仍需继续补齐其他有权威来源的 scan/sort/tmp/lock/thread 计数和剩余 I_S/P_S 字段语义；组件依赖表保持 shape/empty 或 component-dependent。P2 storage-engine apply/state commit crash window、P3 全量客户端/集群 endpoint、P4 官方 MySQL fixture 仍未关闭；FULLTEXT 后置，非 InnoDB 引擎及相关修复/转换继续排除。

### Continuation 1140

- P1/PERFORMANCE_SCHEMA 补齐语句事件计时窗口：`events_statements_current/history/history_long` 现在在计时开启时投影同一单调时间基准上的 `TIMER_START`、`TIMER_END` 和 `TIMER_WAIT`，完成事件满足 `TIMER_END - TIMER_START = TIMER_WAIT`；当前事件保持 `TIMER_END/TIMER_WAIT` 为 NULL，未计时事件继续保持零等待兼容行为。
- 新增并先失败后修复 `TestPerformanceSchemaStatementHistoryProjectsTimerBounds`；语句历史、当前事件、stage history、setup instrument/consumer 配置回归均通过；engine、net、replication、metrics 四包回归通过，其中 engine 184.786s。
- P1 仍需继续补齐其他有权威来源的 scan/sort/tmp/lock/thread 计数和剩余 I_S/P_S 字段语义；组件依赖表保持 shape/empty 或 component-dependent。P2 storage-engine apply/state commit crash window、P3 全量客户端/集群 endpoint、P4 官方 MySQL fixture 仍未关闭；FULLTEXT 后置，非 InnoDB 引擎及相关修复/转换继续排除。

### Continuation 1141

- P1/PERFORMANCE_SCHEMA 再补一条真实执行来源：`ORDER BY` 排序器现在记录实际进入排序的行数，并贯穿 `events_statements_history*` 的 `SORT_ROWS`、digest 的 `SUM_SORT_ROWS` 以及 thread/account/host/user 语句汇总；没有排序的语句仍为 0。
- 新增并先失败后修复 `TestPerformanceSchemaStatementHistoryProjectsSortRowsFromOrderBy`，覆盖真实表排序、历史事件和 digest 汇总；完整 Performance Schema/metrics 定向回归通过，engine、net、replication、metrics 四包回归通过，其中 engine 183.323s；当前 cluster smoke 通过，报告为 `reports/compatibility/p1-sort-rows-current-continuation1141/cluster-report.json`。
- P1 仍需继续补齐其他有权威来源的 sort/tmp/lock/thread 计数和剩余 I_S/P_S 字段语义；组件依赖表保持 shape/empty 或 component-dependent。P2 storage-engine apply/state commit crash window、P3 全量客户端/集群 endpoint、P4 官方 MySQL fixture 仍未关闭；FULLTEXT 后置，非 InnoDB 引擎及相关修复/转换继续排除。

### Continuation 1142

- P1/PERFORMANCE_SCHEMA 继续补齐排序来源：真实 table scan 上的 `ORDER BY` 现在同时记录 `SORT_SCAN=1`，并贯穿 statement history、digest 及 thread/account/host/user 汇总；`SORT_RANGE` 仍保持 0，直到执行器能提供可靠的 range-sort 分类。
- `TestPerformanceSchemaStatementHistoryProjectsSortRowsFromOrderBy` 扩展为校验 `SORT_ROWS/SORT_SCAN` 与 `SUM_SORT_ROWS/SUM_SORT_SCAN`；受影响四包回归通过：engine 175.416s、net 8.176s、replication 2.496s、metrics 0.367s；cluster smoke 通过，报告为 `reports/compatibility/p1-sort-scan-current-continuation1142/cluster-report.json`；之前全仓串行回归也通过（engine 180.385s）。
- P1 仍需继续补齐其他有权威来源的 sort/tmp/lock/thread 计数和剩余 I_S/P_S 字段语义；组件依赖表保持 shape/empty 或 component-dependent。P2 storage-engine apply/state commit crash window、P3 全量客户端/集群 endpoint、P4 官方 MySQL fixture 仍未关闭；FULLTEXT 后置，非 InnoDB 引擎及相关修复/转换继续排除。

### Continuation 1143

- P3 客户端矩阵环境诊断重新执行：Go、PyMySQL、Node.js/mysql2 均可用；`mysql.exe` 仍不存在，报告明确标记 `SKIPPED_ENVIRONMENT`，因此诊断命令以非零状态返回，不能误报为完整矩阵通过。报告：`reports/compatibility/client-matrix-diagnostic-current-continuation1142/`。
- 本机仍未配置受保护的 `XMYSQL_CLIENT_PASSWORD`，所以未启动真实认证矩阵，也没有把空密码或临时凭据写入代码、报告或日志。P3 功能结果继续保持 `partial`，等待可用的 CLI 和受保护认证环境；P4 官方 MySQL fixture 仍不可用。

### Continuation 1144

- P1-A 继续补齐有可靠执行来源的 Performance Schema 语义：真实二级索引范围访问现在记录 `SELECT_RANGE=1`；同一语句包含 `ORDER BY` 时记录 `SORT_RANGE=1`，并同步投影到 `events_statements_history_long`、`events_statements_summary_by_digest` 及各 statement summary 维度。
- 新增 `TestPerformanceSchemaStatementHistoryProjectsRangeCounters`，覆盖二级索引 `price >= ? ORDER BY price DESC`；新增回归通过。
- 影响包回归通过：`go test -p 1 ./server/innodb/engine ./server/observability/metrics ./server/net ./server/replication -count=1 -timeout 45m`，engine 181.379s、metrics 0.392s、net 6.939s、replication 2.596s。
- 尚未把 `LOCK_TIME`、tmp-table、range-join、reprepare 或组件依赖表填成推测值；它们仍需真实 xmysql 来源或保持 `NULL/0/partial`。客户端 CLI/受保护密码和官方 MySQL fixture 仍是外部验证条件；FULLTEXT 与非 InnoDB 引擎继续按范围排除。

### Continuation 1145

- 全仓串行门禁在修复 P_S 测试 recorder 隔离后通过：`go test -p 1 ./... -count=1 -timeout 45m`；engine 175.725s、manager 7.082s、net 7.663s、replication 4.497s，其他 Go 包同样通过。
- `TestPerformanceSchemaStageSummaryByIdentityHonorsInstrumentSettings` 增加独立 RuntimeRecorder，避免进程级 recorder 的历史账号统计污染本测试；该测试以 `-count=3` 稳定通过，engine 全包随后通过 175.574s。
- 集群 smoke 重新通过：`reports/compatibility/p1-range-counters-current-continuation1144/cluster-report.json`；scope matrix 重新生成并通过校验，当前为 25 项、23 项纳入范围、2 项明确 out_of_scope：`reports/compatibility/scope-matrix-current-continuation1144.json`。
- 优先级标注已同步：I_S/P_S=P1，xmysql-native XA/binlog/GTID/复制/崩溃恢复=P2，非 Connector/J 客户端=P3，官方 MySQL 双向互操作=P4；FULLTEXT=deferred，非 InnoDB 引擎/修复/转换=out_of_scope。

### Continuation 1146

- P1/PERFORMANCE_SCHEMA 补齐 JOIN 运行时来源：当前物化 JOIN 执行器对首表聚簇全扫描记录 `SELECT_SCAN=1`，对每个后续无索引嵌套匹配记录 `SELECT_FULL_JOIN=1`，并把参与扫描的实际行数累计到 `ROWS_EXAMINED`；`SELECT_FULL_RANGE_JOIN`、`SELECT_RANGE_CHECK` 没有对应的真实 range-join 规划来源，继续明确保持 0。
- 新增并通过 `TestPerformanceSchemaStatementHistoryProjectsJoinCounters`，同时校验 statement history 和 digest 的 `ROWS_EXAMINED`、`SELECT_SCAN`、`SELECT_FULL_JOIN`、`SELECT_FULL_RANGE_JOIN`、`SELECT_RANGE_CHECK`；SQL 文本使用的别名归一化也纳入 digest 断言。
- 本轮受影响包回归通过：`go test -p 1 ./server/innodb/engine ./server/observability/metrics ./server/net ./server/replication -count=1 -timeout 45m`，engine 176.116s、metrics 0.397s、net 8.235s、replication 2.505s；随后 `go test -p 1 ./... -count=1 -timeout 45m` 全仓通过，其中 engine 175.190s、net 7.852s、replication 2.521s、metrics 0.356s。cluster smoke 通过，报告为 `reports/compatibility/p1-join-counters-current-continuation1146/cluster-report.json`；scope matrix 25 项、23 项纳入范围、2 项 out_of_scope，报告为 `reports/compatibility/scope-matrix-current-continuation1146.json`。没有把旧 release-candidate 报告当作本轮证据。
- 剩余边界保持明确：完整 I_S/P_S 表字段精度、NULL/type/filter 语义及组件权威来源仍是 P1；存储引擎 apply/state commit crash window 是 P2；非 Connector/J 完整客户端/集群 endpoint 是 P3；官方 MySQL 双向 XA/binlog/GTID/复制/崩溃恢复是 P4。FULLTEXT 继续 deferred，非 InnoDB 引擎、非 InnoDB `REPAIR TABLE` 和引擎转换继续 out_of_scope。

### Continuation 1147

- P1/PERFORMANCE_SCHEMA 补齐 `NO_GOOD_INDEX_USED` 的真实执行来源：带过滤条件且最终选择 clustered/partitioned table scan 的语句现在由执行器记录 `NO_GOOD_INDEX_USED=1`，并贯穿 `events_statements_history*`、digest 及 thread/account/host/user 汇总；无过滤条件的普通全表扫描和实际二级索引访问不会因文本猜测而误报。
- 新增并先失败后修复 `TestPerformanceSchemaStatementHistoryProjectsNoIndexUsedFromTableScan` 的 `NO_GOOD_INDEX_USED` 断言；本轮先完成针对性回归，随后继续执行受影响包、全仓、cluster smoke 和 scope matrix 刷新。
- 剩余边界保持明确：完整 I_S/P_S 表字段精度、NULL/type/filter 语义及组件权威来源仍是 P1；存储引擎 apply/state commit crash window 是 P2；非 Connector/J 完整客户端/集群 endpoint 是 P3；官方 MySQL 双向 XA/binlog/GTID/复制/崩溃恢复是 P4。FULLTEXT 继续 deferred，非 InnoDB 引擎、非 InnoDB `REPAIR TABLE` 和引擎转换继续 out_of_scope。

### Continuation 1148

- P2 收口一部分 replica apply/state crash window：`Replica` 新增 identity-aware row/statement apply 回调，将已提交 GTID 传入存储回放层；xmysql row-image replay 在每个变更前检查真实 durable post-image，INSERT/REPLACE/UPDATE 的已完成后镜像和 DELETE 的已不存在前镜像会被识别为重试并跳过，避免存储已提交但 `replica_gtid.json` 尚未替换时重复写入。
- 新增并先失败后修复 `TestReplicaPassesCommittedGTIDToIdentityAwareRowApply`；扩展 `TestEngineAppliesReplicationRowImagesWithoutStatementText` 和 `TestEngineAppliesPartialJSONRowImageWithoutFullAfterValue`，验证普通 row image 与 partial JSON update 的重复回放收敛。
- 本轮已通过 replication 定向测试及两个 engine 定向回归；下一步仍需受影响包和全仓回归。剩余边界是多行事务的跨变更原子性、statement-only replay 的存储幂等、replica 状态提交与存储页提交的真正同一提交点，以及 P1 完整 I_S/P_S、P3 客户端/集群 endpoint、P4 官方 MySQL fixture。FULLTEXT 继续 deferred，非 InnoDB 引擎、非 InnoDB `REPAIR TABLE` 和引擎转换继续 out_of_scope。

### Continuation 1149

- P2 row-image replay 改动完成受影响包与全仓回归：`go test -p 1 ./server/innodb/engine ./server/observability/metrics ./server/net ./server/replication -count=1 -timeout 45m` 通过；随后 `go test -p 1 ./... -count=1 -timeout 45m` 全仓通过，engine 176.018s、net 7.802s、replication 2.480s、metrics 0.360s。
- 集群 smoke 通过：`reports/compatibility/p2-row-image-replay-current-continuation1148/cluster-report.json`；范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1148.json`，仍为 25 项、23 项纳入范围、2 项明确 out_of_scope。
- 当前仍未关闭：多行事务跨变更原子性、statement-only replay 的存储幂等、存储页提交与 `replica_gtid.json` 状态提交的同一提交点、P1 完整 I_S/P_S 表注册/字段精度/运行时来源、P3 完整非 Connector/J 客户端及 cluster endpoint、P4 官方 MySQL 双向 XA/binlog/GTID/复制/崩溃恢复 fixture。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1150

- P2 进一步收口 replica apply/state crash window：事务终端事件现在先将完整 relay 边界持久化，再调用 identity-aware row/statement apply；存储 apply 成功但 `replica_gtid.json` 替换失败时，重启仍可从 relay 重新进入回放。新增并通过 `TestReplicaPersistsRelayBeforeIdentityAwareApply`、`TestReplicaRetriesAfterStatePersistFailureWithDurableRelay`。
- 受影响包回归通过：`go test -p 1 ./server/replication ./server/innodb/engine ./server/observability/metrics ./server/net -count=1 -timeout 45m`，engine 182.853s、replication 2.567s、net 7.034s、metrics 0.635s。
- 仍未宣称 P2 完成：statement-only replay 的通用幂等、真正同一存储提交点、多行事务的全引擎原子提交、prepared-XA terminal-marker 故障注入和官方 MySQL native XA/binlog 互操作仍待完成；P1/P3/P4 边界保持不变。

### Continuation 1151

- 本轮验证完成：`go test -p 1 ./... -count=1 -timeout 45m` 全仓通过，engine 176.231s、net 7.856s、replication 4.510s、metrics 0.377s；集群 smoke 通过，报告为 `reports/compatibility/p2-durable-relay-current-continuation1150/cluster-report.json`。
- 范围矩阵已刷新为 `reports/compatibility/scope-matrix-current-continuation1150.json`，共 25 项，其中 23 项纳入范围、2 项明确 out_of_scope；当前状态为 implemented=10、partial=10、pending_external=1、unverified=1、deferred=1、out_of_scope=2。
- 本轮没有把全仓测试或集群 smoke 误记为 P2 完成。仍未关闭的 P2 是 statement-only replay 通用幂等、真正同一存储提交点、多行事务全引擎原子提交、prepared-XA terminal-marker 故障注入，以及与官方 MySQL native XA/binlog/GTID/复制/崩溃恢复的双向互操作；P1 完整 I_S/P_S、P3 非 Connector/J 客户端/集群 endpoint 和 P4 官方 fixture 继续保留。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1152

- P2 新增 prepared-XA 终端状态替换失败回归：`TestReplicaRetriesPreparedXAAfterTerminalStatePersistFailure` 验证存储 apply 已执行但最终 replica state 替换失败时，重启可依据 durable relay 中的 PREPARE/COMMIT 再次回放，最终清理 prepared-XA 并执行 GTID。
- 定向验证通过：`go test -p 1 ./server/replication -run '^TestReplicaRetriesPreparedXAAfterTerminalStatePersistFailure$' -count=1 -timeout 10m`。
- 这只补齐了故障注入证据，不代表 XA 已与官方 MySQL native binlog/GTID/复制/崩溃恢复互操作；statement-only 幂等、真正同一存储提交点、多行事务全引擎原子提交仍未关闭。

### Continuation 1153

- P2 实现 statement-only replay 的 durable applied marker：`replicaState` 新增 `applied_transactions`，存储 apply 回调成功后、executed-GTID 最终替换前先持久化事务身份；重启回放时若发现该 marker，则补齐 GTID 状态而跳过重复 SQL。新增并通过 `TestReplicaStatementReplaySkipsRetryAfterStatePersistFailure`、`TestReplicaStatementMarkersSurviveMultipleTransactionsInOneApply`，同时更新 row/XA 崩溃窗口测试以验证 marker 避免重复 apply。
- 全仓验证通过：`go test -p 1 ./... -count=1 -timeout 45m`，engine 178.818s、net 7.595s、replication 2.555s、metrics 0.366s；集群 smoke 通过，报告为 `reports/compatibility/p2-statement-marker-current-continuation1153/cluster-report.json`。
- 范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1153.json`，仍为 25 项、23 项纳入范围、2 项 out_of_scope；状态统计仍是 implemented=10、partial=10、pending_external=1、unverified=1、deferred=1、out_of_scope=2。
- 该 marker 缩小了 statement-only 的重复回放窗口，但没有制造存储页与 replica state 的同一提交点；marker 写入本身失败或进程恰在存储提交与 marker 写入之间崩溃时，仍需下一阶段的存储事务协调。P2 的多行全引擎原子提交及官方 MySQL native XA/binlog/GTID/复制/崩溃恢复互操作继续未完成。

### Continuation 1154

- P3 客户端环境诊断已刷新：`reports/compatibility/client-matrix-diagnostic-current-continuation1153/client-matrix-20260923-181958.json`。Go、PyMySQL、Node.js 运行时可用；MySQL CLI 因当前环境不存在 `mysql.exe` 仍为 `SKIPPED_ENVIRONMENT`，因此不能把完整非 Connector/J 矩阵标记为通过。
- 范围矩阵已同步更新到该诊断证据：`reports/compatibility/scope-matrix-current-continuation1153.json`；P3 仍保持 partial/unverified，未把环境可用性诊断误记为功能兼容性通过。

### Continuation 1155

- 最终修正后的全仓回归再次通过：`go test -p 1 ./... -count=1 -timeout 45m`，engine 197.952s、net 7.562s、replication 2.601s、metrics 0.365s，退出码 0。
- 本轮仍不宣称全局任务完成：P1 I_S/P_S 语义仍有 component/runtime partial，P2 仍有 marker 写入与存储页提交之间的真实 crash window，P3 完整功能矩阵受 MySQL CLI/受保护凭据和 cluster endpoint 证据限制，P4 仍等待官方 MySQL fixture；FULLTEXT deferred，非 InnoDB 引擎/修复/转换 out_of_scope。

### Continuation 1156

- P3 新增真实集群端点客户端验证脚本 `scripts/compatibility/client_cluster_endpoint.ps1`：准备独立 source/replica 数据目录和 SQL/复制控制端口，先通过客户端矩阵连接 source，等待 replica GTID 追平，停止 source，调用 `/replication/promote`，再从 promoted SQL 端点运行同一客户端矩阵。
- 脚本只从受保护的 `XMYSQL_CLIENT_PASSWORD` 读取认证信息，不把密码写入配置、报告或日志；`-DiagnosticOnly` 会分别报告 MySQL CLI、Go、PyMySQL、Node.js/mysql2 和受保护凭据的环境状态。当前诊断报告为 `reports/compatibility/client-cluster-endpoint-diagnostic-current/client-cluster-endpoint-20260923-133209.json`：Go/PyMySQL/Node 可用，`mysql.exe` 和受保护密码不可用，因此脚本正确返回非零并保持 `passed=false`。
- 全局矩阵已增加 `clients/cluster-endpoint-client-scenarios`，状态保持 `partial`；这项真实端到端门禁与 P3 全量客户端验证仍待受保护认证环境和 MySQL CLI，不能用引擎内集群 smoke 替代。

### Continuation 1157

- P1 收口一批虚拟元数据精度：Performance Schema 事件/摘要列现在按 table/column 映射 `EVENT_NAME VARCHAR(128) NOT NULL`、`DIGEST CHAR(64)`、语句文本 `LONGTEXT` 和非负计数/计时器 `BIGINT UNSIGNED`；`information_schema.columns`、`tables`、`schemata`、`statistics` 的常用字段也补齐了 64 字符标识列、unsigned 数值、索引统计和 NULL 语义。未知或组件专用字段继续使用保守 fallback，未伪造完整 MySQL 来源。
- 新增并通过 `TestInformationSchemaVirtualPerformanceSchemaColumnsProjectTypePrecision`、`TestInformationSchemaVirtualStatementEventColumnsUseNativeTextAndDigestShapes`、`TestInformationSchemaVirtualColumnsTableUsesNativeMetadataShapes`、`TestInformationSchemaVirtualTablesAndSchemataUseNativeMetadataShapes`、`TestInformationSchemaVirtualStatisticsUsesNativeIndexMetadataShapes`；engine 全包通过：`go test -p 1 ./server/innodb/engine -count=1 -timeout 30m`，177.412s；最新全仓通过：`go test -p 1 ./... -count=1 -timeout 45m`，engine 177.479s、manager 7.125s、net 7.558s、replication 2.597s，退出码 0。
- 集群 smoke 通过：`reports/compatibility/p1-column-metadata-current-continuation1157/cluster-smoke/cluster-report.json`；范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1157.json`，仍为 26 项、24 项纳入范围、2 项 out_of_scope。
- 本轮没有把 P1 标记为完成：剩余仍包括更多 I_S/P_S 表级精度、权限/角色完整可见性、组件运行时来源和未实现计数；P2 存储页与复制状态同提交点、P3 受保护客户端/CLI 矩阵、P4 官方 MySQL fixture 继续保留。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1158

- P1 继续补齐 I_S 字符集契约：I_S 虚拟字符列现在使用 MySQL 兼容的 `utf8mb3`，并按字符集计算 `CHARACTER_OCTET_LENGTH`；P_S 仍保持自身 `utf8mb4` 映射。新增并通过 `TestInformationSchemaVirtualInformationSchemaCharactersUseUtf8mb3`，同时保留 routines/parameters、statistics、tables/schemata 和 P_S 事件元数据回归。
- 最新 engine 全包通过：`go test -p 1 ./server/innodb/engine -count=1 -timeout 30m`，181.999s；最新全仓通过：`go test -p 1 ./... -count=1 -timeout 45m`，engine 177.427s、manager 7.208s、net 7.726s、replication 2.670s，退出码 0。集群 smoke 通过：`reports/compatibility/p1-column-metadata-current-continuation1158/cluster-smoke/cluster-report.json`。
- 本轮仍不宣称 P1 完成：完整 I_S/P_S 表覆盖、更多字段的精确 NULL/type/filter 语义、权限/角色边界、组件运行时来源和剩余计数仍待收口；P2 的存储/复制同提交点、P3 外部客户端认证矩阵、P4 官方 MySQL fixture 继续未完成。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1159

- P1 再补齐常用 I_S 目录元数据：`COLLATIONS`、`CHARACTER_SETS`、`PROCESSLIST`、`ENGINES` 的常用字段现在按 MySQL 语义投影长度、NULL、unsigned 数值和文本类型；新增并通过 `TestInformationSchemaVirtualCharacterCatalogsUseNativeShapes`、`TestInformationSchemaVirtualProcesslistAndEnginesUseNativeShapes`。
- 最新 engine 全包通过：`go test -p 1 ./server/innodb/engine -count=1 -timeout 30m`，198.522s；最新全仓通过：`go test -p 1 ./... -count=1 -timeout 45m`，engine 182.842s、manager 11.053s、net 7.585s、replication 2.765s，退出码 0。集群 smoke 通过：`reports/compatibility/p1-catalog-metadata-current-continuation1159/cluster-smoke/cluster-report.json`。
- P1 仍为 partial：组件依赖表、更多 I_S/P_S 字段精度和运行时来源、权限/角色完整矩阵仍需继续；P2 同提交点、P3 受保护客户端/CLI、P4 官方 MySQL fixture 仍未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1160

- P1 继续补齐已有真实行来源的 INFORMATION_SCHEMA 目录元数据：`KEY_COLUMN_USAGE`、`TABLE_CONSTRAINTS`、`CHECK_CONSTRAINTS`、`REFERENTIAL_CONSTRAINTS`、`VIEWS`、`PARTITIONS` 和 `PLUGINS` 现在按 MySQL 语义投影常用字段的长度、`LONGTEXT`、unsigned 序号/统计值及 NULL 语义；`CHECK_CONSTRAINTS` 的 `ENFORCED` 同步加入默认投影和 `INFORMATION_SCHEMA.COLUMNS` 目录，修复了注册形状与实际行结果不一致。
- 新增并先失败后修复 `TestInformationSchemaVirtualConstraintMetadataUsesNativeShapes`、`TestInformationSchemaVirtualViewAndPartitionMetadataUsesNativeShapes`、`TestInformationSchemaVirtualPluginsUseNativeShapes`；约束目录注册形状回归也通过。
- 验证通过：`go test ./server/innodb/engine -run 'TestXMySQLExecutor_InformationSchemaRegisteredShapesMatchColumnsCatalog|TestInformationSchemaVirtual(Constraint|ViewAndPartition)MetadataUsesNativeShapes|TestInformationSchemaVirtualPluginsUseNativeShapes' -count=1`；`go test -p 1 ./server/innodb/engine -count=1 -timeout 30m`（181.314s）；`go test -p 1 ./... -count=1 -timeout 45m`（Engine 181.045s，net 7.661s，replication 2.665s，退出码 0）；集群 Smoke 通过，报告为 `reports/compatibility/p1-constraint-metadata-current-continuation1160/cluster-smoke/cluster-report.json`。
- 本轮仍不宣称 P1 全部完成：完整 I_S/P_S 表注册、剩余字段精度/过滤/运行时来源、权限角色完整矩阵和组件依赖表仍需继续；P2 存储提交与复制状态同提交点、P3 非 Connector/J 外部认证矩阵、P4 官方 MySQL 双向 fixture 仍未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1161

- P1 继续补齐存储对象目录元数据：`TRIGGERS` 和 `EVENTS` 的常用字段现在按 MySQL 8.4 语义投影标识符、事件/动作枚举长度、`LONGTEXT` 定义、时间列、`TRIGGERS.CREATED TIMESTAMP(2)`、unsigned `ACTION_ORDER/ORIGINATOR` 及 NULL 语义。
- 新增并先失败后修复 `TestInformationSchemaVirtualStoredObjectMetadataUsesNativeShapes`；该测试覆盖触发器动作列、事件调度列、时间精度和复制来源字段。
- 验证通过：`go test ./server/innodb/engine -run '^TestInformationSchemaVirtualStoredObjectMetadataUsesNativeShapes$' -count=1`；`go test -p 1 ./server/innodb/engine -count=1 -timeout 30m`（185.764s）；`go test -p 1 ./... -count=1 -timeout 45m`（Engine 193.206s，manager 7.248s，net 7.606s，replication 2.636s，退出码 0）；集群 Smoke 通过，报告为 `reports/compatibility/p1-stored-object-metadata-current-continuation1161/cluster-smoke/cluster-report.json`。
- P1 仍为 partial：剩余完整 I_S/P_S 表注册、组件表和运行时来源、字段过滤/权限矩阵仍未关闭；P2 复制状态与存储提交同提交点、P3 外部客户端认证/CLI、P4 官方 MySQL 双向 fixture 继续未完成。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1162

- P1 补齐 `INFORMATION_SCHEMA.INFORMATION_SCHEMA_CATALOG_NAME`：新增完整目录注册、`CATALOG_NAME VARCHAR(64) NOT NULL` 元数据，以及返回 MySQL 约定单行 `def` 的真实查询分支；`TABLES/COLUMNS` 现在可以发现该表和字段。
- 新增并先失败后修复 `TestInformationSchemaCatalogNameIsRegisteredAndReturnsNativeRow`；定向元数据回归通过。受影响 engine 全包通过：`go test ./server/innodb/engine -count=1 -timeout 30m`，177.436s；全仓串行回归第二次通过：`go test -p 1 ./... -count=1 -timeout 45m`，engine 177.317s、manager 7.169s、net 7.726s、replication 4.591s，退出码 0。第一次全仓运行仅因既有顺序敏感的 `TestPerformanceSchemaStageSummaryByIdentityHonorsInstrumentSettings` 失败，随后该测试单独连续两次通过，第二次全仓作为最终证据。
- 集群 Smoke 通过：`reports/compatibility/p1-catalog-name-current-continuation1162/cluster-smoke/cluster-report.json`；`git diff --check` 通过（仅报告 Windows 换行提示）。范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1162.json`，仍为 26 项、24 项纳入范围、2 项 out_of_scope。
- P1 仍未全部完成：完整 I_S/P_S 表注册与运行时来源、剩余字段精度/过滤/权限角色矩阵仍为 partial；P2 原生 XA/binlog/GTID/复制/崩溃恢复互操作、P3 非 Connector/J 外部认证/CLI/集群端点全量矩阵、P4 官方 MySQL 双向 fixture 仍未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1163

- P1 修正 `VIEW_ROUTINE_USAGE` 的官方目录语义：注册列改为 `TABLE_CATALOG/TABLE_SCHEMA/TABLE_NAME/SPECIFIC_CATALOG/SPECIFIC_SCHEMA/SPECIFIC_NAME`，行过滤、排序和投影同步修正；补齐 `VIEW_ROUTINE_USAGE` 与 `VIEW_TABLE_USAGE` 的 `VARCHAR(64) NOT NULL` 元数据。官方 MySQL 8.4 文档将这两个视图作为 INFORMATION_SCHEMA 通用目录的一部分，且 `VIEW_ROUTINE_USAGE` 的列顺序与当前实现已对齐。
- 新增并先失败后修复 `TestInformationSchemaViewRoutineUsageUsesNativeCatalogShape`、`TestInformationSchemaVirtualViewUsageMetadataUsesNativeShapes`；`go test ./server/innodb/engine -run 'TestInformationSchemaViewRoutineUsage|TestInformationSchemaVirtualViewUsageMetadataUsesNativeShapes' -count=1` 通过，完整 `TestInformationSchema` 回归通过（19.384s），engine 全包通过（182.075s）。
- 修正 P_S 阶段计时的零时长边界：`RuntimeRecorder` 对已完成事件使用最小正计时值，避免极短语句出现 `COUNT_STAR=1` 但 `SUM_TIMER_WAIT=0`；新增零时长回归，metrics 与该阶段汇总测试各连续 20 次通过。第一次全仓回归暴露该既有间歇失败，修正后最终 `go test -p 1 ./... -count=1 -timeout 45m` 通过，退出码 0；engine 180.138s、manager 7.228s、net 7.651s、replication 2.597s。
- 集群 Smoke 通过：`reports/compatibility/p1-view-routine-usage-current-continuation1163/cluster-smoke/cluster-report.json`；范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1163.json`。
- P1 仍未全部完成：完整 I_S/P_S 表注册、组件/运行时来源、剩余字段精度/过滤和权限角色完整矩阵仍为 partial；P2 原生 XA/binlog/GTID/复制/崩溃恢复双向互操作与存储同提交点、P3 非 Connector/J 外部认证/CLI/集群端点全量矩阵、P4 官方 MySQL fixture 仍未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1164

- P1 修正 `TABLESPACES_EXTENSIONS` 的官方 8.4 注册形状：移除多出的 `SE_PRIVATE_DATA`，现在 `SELECT *` 只返回 `TABLESPACE_NAME`、`ENGINE_ATTRIBUTE`；保留 `INFORMATION_SCHEMA.TABLESPACES` 自身的引擎私有字段，不与扩展目录混淆。
- 扩展辅助目录回归先失败后修复：`TestInformationSchemaAuxiliaryTablesReturnStableMetadataShapes` 现在同时验证显式投影与 `SELECT *` 的官方列集合；定向回归及注册形状一致性测试通过。官方文档明确 `TABLESPACES_EXTENSIONS` 是保留扩展表且只有这两个字段。
- 当前工作树验证通过：`go test ./server/innodb/engine -count=1 -timeout 30m`（177.302s）；`go test -p 1 ./... -count=1 -timeout 45m`（engine 179.109s、manager 7.137s、net 7.586s、replication 2.596s、metrics 0.369s，退出码 0）；集群 Smoke 报告为 `reports/compatibility/p1-tablespaces-extensions-current-continuation1164/cluster-smoke/cluster-report.json`，范围矩阵为 `reports/compatibility/scope-matrix-current-continuation1164.json`；`git diff --check` 无错误。
- P1 仍未全部完成：完整 I_S/P_S 表注册、组件/运行时来源、剩余字段精度/过滤和权限角色完整矩阵仍为 partial；P2 原生 XA/binlog/GTID/复制/崩溃恢复双向互操作与存储同提交点、P3 非 Connector/J 外部认证/CLI/集群端点全量矩阵、P4 官方 MySQL fixture 仍未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1165

- P1 继续补齐 INFORMATION_SCHEMA 元数据精度：`COLLATION_CHARACTER_SET_APPLICABILITY` 的 `COLLATION_NAME`、`CHARACTER_SET_NAME` 现在按 MySQL 8.4 目录语义返回 `VARCHAR(64) NOT NULL`，不再落入 `VARCHAR(255) NULL` 通用兜底。
- P1 补齐五个扩展目录的可发现字段形状：`COLUMNS_EXTENSIONS`、`TABLES_EXTENSIONS`、`TABLE_CONSTRAINTS_EXTENSIONS` 的标识符列为 `VARCHAR(64) NOT NULL`、引擎属性列为可空 `JSON`；`SCHEMATA_EXTENSIONS.OPTIONS` 为 `VARCHAR(256) NOT NULL`；`TABLESPACES_EXTENSIONS` 的 `TABLESPACE_NAME` 为 `VARCHAR(64) NOT NULL`、`ENGINE_ATTRIBUTE` 为可空 `JSON`。扩展属性的实际行值仍保持 NULL，未虚构 InnoDB 未提供的 JSON 属性。
- 新增并先失败后修复 `TestInformationSchemaVirtualCollationApplicabilityUsesNativeShapes`、`TestInformationSchemaExtensionMetadataUsesNativeShapes`；定向回归通过：`go test ./server/innodb/engine -run 'TestInformationSchema(VirtualCollationApplicabilityUsesNativeShapes|ExtensionMetadataUsesNativeShapes|ExtensionViews|AuxiliaryTablesReturnStableMetadataShapes)' -count=1 -timeout 10m`。
- 官方 MySQL 8.4 文档将这些扩展表列为目录契约；`COLUMNS_EXTENSIONS`、`TABLES_EXTENSIONS`、`TABLE_CONSTRAINTS_EXTENSIONS` 的属性字段保留给存储引擎扩展，当前 xmysql 继续只承诺 NULL 值，不把“字段可发现”误报成“引擎属性已实现”。
- 本轮之后 P1 仍为 partial：完整 I_S/P_S 行来源、组件/运行时实例、权限角色全矩阵仍未关闭；P2 原生 XA/binlog/GTID/复制/崩溃恢复双向互操作、P3 非 Connector/J 客户端和集群端点全量矩阵、P4 官方 MySQL fixture 仍未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1166

- 重新梳理全局范围：完整 INFORMATION_SCHEMA、完整 PERFORMANCE_SCHEMA、非 Connector/J 客户端兼容矩阵，以及 XA 与原生 binlog/GTID/复制/崩溃恢复互操作均纳入全局任务；FULLTEXT 继续后置，MyISAM/ARCHIVE/CSV、非 InnoDB 的 REPAIR/转换明确不纳入任务。
- 复制幂等边界完成源码审计：`Replica` 当前先持久化 relay，再调用存储 apply，随后写 `appliedTransactions` 中间标记，最后替换 executed GTID；这能覆盖状态文件替换失败和重放窗口，但不是存储页与 GTID 状态的同一提交点。
- 存储引擎审计确认：`applyReplicationStatementsWithID`/`applyReplicationRowsWithID` 通过会话执行 BEGIN/多条 DML/COMMIT，但普通 DML 内部仍分别调用 `beginStorageTransaction`/`commitStorageTransaction`；外层 transaction journal 是补偿回滚机制，不是跨多条 DML 的原子存储事务。因此，P2 剩余的“marker-write/storage-page same-commit window”需要后续设计统一的存储事务提交协议，不能仅靠继续增加 JSON marker 关闭。
- P3 环境诊断结果：Go、PyMySQL、Node.js 运行时可用；本机未发现 `mysql.exe`，且未设置受保护的 `XMYSQL_CLIENT_PASSWORD`，所以非 Connector/J 全量功能矩阵和集群端点矩阵仍只能记为 partial/unverified，不得据此宣称客户端兼容已完成。
- 本轮不把上述架构缺口伪装成实现；下一阶段顺序固定为：先完成统一存储事务/复制提交协议设计与失败注入，再补齐有权威运行时来源的 I_S/P_S 表，最后在受保护认证环境中跑 P3，并用官方 MySQL fixture 完成 P4。

### Continuation 1167

- 角色兼容性补齐一个真实语义缺口：`mandatory_roles` 现在会在会话权限路径中保持启用；`SET ROLE NONE`、`SET ROLE DEFAULT` 和 `SET ROLE ... EXCEPT` 不再能够把强制角色从 `active_roles` 或 `INFORMATION_SCHEMA.ENABLED_ROLES` 中移除。
- 新增并先失败后修复 `TestMandatoryRolesRemainActiveAfterSetRoleNone`；角色/账户/权限相关 engine 回归通过：`go test ./server/innodb/engine -run 'Role|Account|Privilege' -count=1 -timeout 10m`。
- 这只是 P1 权限/角色矩阵的一项收口，不代表完整角色语义已完成；完整 I_S/P_S 表覆盖、字段/过滤精度、组件运行时来源及权限可见性边界仍保持 `partial`。P2 存储页与复制状态同提交点、P3 非 Connector/J 客户端/集群端点真实认证矩阵、P4 官方 MySQL fixture 继续未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1168

- P2 增加复制事务提交恢复协议：带事务 ID 的复制 apply 现在使用稳定的 replication journal ID；提交前同步 journal，存储提交成功后写 durable committed marker，再清理 journal。若 marker 已存在，statement-only 或 row-image 重放会直接跳过，避免 replica 状态文件晚于存储提交时重复执行事务。
- 新增并先失败后修复 `TestReplicationStatementApplyIsIdempotentAcrossCommitStateRecovery`，覆盖同一事务 ID 的重复 apply；新增 `TestReplicationCommitMarkerFailureLeavesJournalRecoverable`，注入 marker 写失败并验证 orphan journal 能回滚、随后事务可以重新 apply。
- 修复 `RecoverOrphanedTransactions` 删除 journal 前未关闭 append 文件句柄的问题；Windows 下的失败恢复现在不会留下锁定 journal。replication/XA 回归、engine 全包和集群 smoke 均通过；集群报告为 `reports/compatibility/p2-commit-protocol-current-continuation1168/cluster-smoke/cluster-report.json`。
- 该协议收口的是 xmysql 内部崩溃恢复和 exactly-once 重放窗口，不等于存储页、binlog、GTID、官方 MySQL XA 的同一物理提交点；P2 仍需原生 MySQL 双向 fixture 和更深的存储 WAL/复制状态协调验证。P1 完整 I_S/P_S、P3 外部客户端、P4 官方 fixture 继续未关闭。

### Continuation 1169

- 对照 MySQL 8.4 官方目录重新核对表名边界：当前 I_S 注册已覆盖通用目录、权限/角色目录、InnoDB 非 FULLTEXT 目录、线程池/连接控制/防火墙目录；缺失的 NDB 专用目录属于明确排除的非 InnoDB 范围，`INNODB_FT_*` 继续属于 FULLTEXT deferred。
- 当前 P_S 注册和 dedicated shape 覆盖官方 8.4 表目录；剩余 `clone`、keyring、firewall、Group Replication、NDB、component scheduler 等表没有 xmysql 对应组件或协议来源，因此保持空行/保守 shape，不虚构运行时数据。P1 的剩余工作从“补表名”转为字段精度、过滤权限和真实运行时来源收口。
- 全仓串行回归再次通过：`go test -p 1 ./... -count=1 -timeout 45m`，engine 180.004s、manager 7.560s、net 7.735s、replication 2.709s；P2 集群 smoke 仍为 PASS。P3 真实客户端矩阵继续等待受保护认证环境，P4 等待官方 MySQL fixture。

### Continuation 1170

- P1 收口 `INFORMATION_SCHEMA.KEYWORDS` 的真实值语义：`RESERVED` 从旧的 `YES/NO` 字符串改为 MySQL 8.4 约定的整数 `1/0`；`WHERE RESERVED`、`WHERE NOT RESERVED`、`RESERVED = 1/0` 以及旧客户端的 `YES/NO` 过滤均有回归覆盖。
- P1 补齐三个已有真实行来源目录的字段契约：`KEYWORDS.WORD` 为 `VARCHAR(128)`、`KEYWORDS.RESERVED` 为可空 `INT`；`ST_UNITS_OF_MEASURE` 的 `VARCHAR(255)/VARCHAR(7)/DOUBLE/VARCHAR(255)` 及 NULL 语义；`ST_SPATIAL_REFERENCE_SYSTEMS` 的 `VARCHAR(80)/INT UNSIGNED/VARCHAR(256)/INT UNSIGNED/VARCHAR(4096)/VARCHAR(2048)` 及 NULL 语义。
- 采用先失败后修复的测试推进：新增字段元数据和布尔/数值过滤回归；完整 INFORMATION_SCHEMA 回归通过：`go test ./server/innodb/engine -run 'TestInformationSchema' -count=1 -timeout 15m`，退出码 0，19.520s。
- 继续修正 `ST_GEOMETRY_COLUMNS` 的官方 8.4 列契约：使用 `SRS_NAME`、`SRS_ID`、`GEOMETRY_TYPE_NAME`，移除此前错误的 `MIN_X/MAX_X/MIN_Y/MAX_Y` 形状；`SELECT *` 精确列序和空间元数据回归已覆盖。
- P1 再收口 `RESOURCE_GROUPS` 的字段精度：资源组名/CPU 列表为 `VARCHAR(64/1024)`，类型为 `ENUM('SYSTEM','USER')`，启用标记为 `TINYINT(1)`，线程优先级为 `INT`；同时修正虚拟 I_S 元数据的 `DATA_TYPE` 基础类型投影，使带参数的 `ENUM`/`TINYINT` 与 `COLUMN_TYPE` 分离。
- `COLUMN_STATISTICS` 的三个对象标识列收口为 `VARCHAR(64) NOT NULL`，`HISTOGRAM` 收口为 `JSON NOT NULL`；现有 `ANALYZE TABLE` 生成和持久化 histogram 行为保持不变，并新增元数据回归。
- `PROFILING` 按 MySQL 8.4 官方 `ST_FIELD_INFO` 收口：查询/序号及各计数为 `INT`，状态/源码字段为 `VARCHAR(30/20)`，时间字段为 `DECIMAL(9,6)`，NULL 语义与官方一致；性能采样能力本身仍是现有 statement history 的兼容投影。
- Continuation 1173 回归证据：`go test ./server/innodb/engine -run 'Test(InformationSchema|PerformanceSchema)' -count=1 -timeout 25m` 通过（41.761s）；`go test -p 1 ./... -count=1 -timeout 45m` 通过，engine 181.356s、manager 7.338s、net 7.885s、replication 2.657s。最新范围矩阵为 `reports/compatibility/scope-matrix-current-continuation1173.json`，仍为 24 项 in-scope、2 项 out-of-scope。
- 这些改动继续只收口可由 parser、空间目录和现有运行时来源证明的语义；组件专用空表、完整权限矩阵、剩余 P_S 字段/运行时来源仍保持 partial，不把 shape-correct 空结果误报成完整实现。P2 存储/WAL 同提交点与官方 XA/binlog/GTID 互操作、P3 外部客户端矩阵、P4 官方 MySQL fixture 继续未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1174

- P1 补齐 `INFORMATION_SCHEMA.USER_ATTRIBUTES` 的官方 8.4 元数据契约：`USER CHAR(32) NOT NULL`、`HOST CHAR(255) NOT NULL`、`ATTRIBUTE JSON NULL`；已有账户属性持久化、权限可见性和属性过滤行为保持不变。
- 采用先失败后修复的元数据回归：专项 `TestInformationSchemaUserAttributesUsesNativeMetadata` 先确认三列错误地使用 `VARCHAR(255) NULL`，修复后通过；I_S/P_S 合并专项通过，`go test ./server/innodb/engine -run 'Test(InformationSchema|PerformanceSchema)' -count=1 -timeout 25m` 用时 42.655s。
- 当前 P1 的“完整表注册”仍保持 partial：组件专用表的精确字段、完整权限矩阵、运行时统计来源和未实现组件不能仅凭空结果关闭。P2 存储/WAL 同提交点与官方 XA/binlog/GTID 互操作、P3 外部客户端矩阵、P4 官方 MySQL fixture 继续未关闭；FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1175

- P1 收口 Performance Schema 配置表的官方 8.4 形状：`accounts/hosts/users` 的连接与内存计数使用 BIGINT（内存字段为 unsigned），用户/主机字段分别使用 `CHAR(32)`、ASCII `CHAR(255)`；`setup_consumers`、`setup_instruments`、`setup_actors`、`setup_objects`、`setup_meters`、`setup_metrics` 的枚举、集合、VARCHAR 长度和 NULL 语义已补齐。
- 修正 `setup_instruments` 的真实列投影，从旧的 `DOCUMENT` 改为官方 `FLAGS`、`DOCUMENTATION`；补齐 `setup_threads` 的官方配置表形状（`NAME/ENABLED/HISTORY/PROPERTIES/VOLATILITY/DOCUMENTATION`），并保留 `ENABLED/HISTORY` 更新语义。
- 新增 I_S 元数据和 P_S 行行为回归，先由红测暴露旧列名、旧的 setup_threads 行语义及通用 `VARCHAR(255)` 兜底，再完成实现；专项测试通过：`go test ./server/innodb/engine -run 'TestPerformanceSchemaSetupThreadsExposeThreadInstrumentation|TestInformationSchemaVirtualPerformanceSchemaConnectionAndSetupMetadataUsesNativeShapes' -count=1 -timeout 10m`，I_S/P_S 合并专项通过（43.548s，退出码 0）。
- 本轮仅收口已有运行时来源和官方字段契约；未接入的组件运行时表、完整权限矩阵、P2 统一物理提交、P3 外部客户端和 P4 官方 MySQL fixture 仍保持未关闭，不把 shape-correct 空行视为完整实现。

### Continuation 1176

- P1/P2 复制诊断目录继续对齐 MySQL 8.4：`replication_applier_status` 收口为 `CHANNEL_NAME/SERVICE_STATE/REMAINING_DELAY/COUNT_TRANSACTIONS_RETRIES`；连接配置和连接状态表补齐官方列集合，`AUTO_POSITION` 改为 `ENUM('1','0')` 语义。
- `replication_applier_status_by_coordinator/worker` 补齐官方事务时间戳、事务标识、错误和重试列；xmysql 当前没有来源的字段明确返回 NULL，单线程 coordinator/worker 的已有状态仍来自同一复制状态快照。
- 新增 INFORMATION_SCHEMA 元数据和 P_S 运行时回归，覆盖官方 CHAR/ENUM/TIMESTAMP/unsigned BIGINT 形状；相关专项通过（2.168s）。这仍不等同于官方 MySQL 双向复制互操作，P2/P4 的物理提交和外部 fixture 门禁继续未关闭。

### Continuation 1177

- 修复 `ExecuteWithQuery` 的 statement history 竞态：结果转发 goroutine 负责的 `rows_sent` 累加可能晚于 worker 的指标 defer，导致 `select 8` 偶发记录为 0。现在该直接执行路径会先完成结果归集，再发布 statement history；查询、内存释放和错误指标仍由 worker 保持原有生命周期。
- 回归证据：`TestPerformanceSchemaDirectExecutorRecordsMemoryAndStatementLifecycle` 连续 10 次通过；`go test ./server/innodb/engine -run 'Test(InformationSchema|PerformanceSchema)' -count=1 -timeout 30m` 通过，42.078s，退出码 0；随后 `go test -p 1 ./... -count=1 -timeout 45m` 全仓通过，engine 180.065s、manager 7.151s、net 7.696s、replication 2.644s，退出码 0。
- 当前范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1176.json`：10 项 implemented、11 项 partial、1 项 unverified、1 项 pending_external、1 项 deferred、2 项 out_of_scope。P1 完整 I_S/P_S、P2 官方 XA/binlog/GTID/崩溃恢复互操作、P3 外部客户端真实认证矩阵、P4 官方 fixture 仍未关闭；FULLTEXT 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1178

- P1 权限/角色可见性补齐 Host 通配匹配：`sessionAccount` 原先将 `%` 的合法匹配得分为 `-1`，但初始最佳分也是 `-1`，导致只有 `%` 账户定义时无法选中账户；现在保留精确 Host 优先级，同时允许通配 Host 作为唯一匹配。
- `APPLICABLE_ROLES`、`ADMINISTRABLE_ROLE_AUTHORIZATIONS`、`ROLE_TABLE_GRANTS`、`ROLE_COLUMN_GRANTS`、`ROLE_ROUTINE_GRANTS` 统一复用 host-aware `sessionAccount`，不再各自要求会话 Host 与账户 Host 字符串完全相等。
- 新增先失败后修复的 `TestRoleMetadataResolvesWildcardHostAccountLikePrivilegeChecks`；专项通过：`go test ./server/innodb/engine -run '^TestRoleMetadataResolvesWildcardHostAccountLikePrivilegeChecks$' -count=1 -timeout 10m`；角色/账户/权限及 I_S/P_S 回归通过：`go test ./server/innodb/engine -run 'Test(InformationSchema|PerformanceSchema|Role|Account|Privilege)' -count=1 -timeout 30m`，45.758s，退出码 0。
- 该修复只关闭一个 P1 Host 可见性边界，不代表完整权限角色矩阵、P1 完整 I_S/P_S、P2 统一物理提交、P3 外部客户端真实认证矩阵或 P4 官方 MySQL fixture 已完成；FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1179

- P1 继续收口 InnoDB 目录表的字段精度：`INNODB_FIELDS` 的 `INDEX_ID` 为 `BIGINT UNSIGNED`、`NAME` 为 `VARCHAR(64) NOT NULL`、`POS` 为 `INT UNSIGNED`；`INNODB_VIRTUAL` 的 `TABLE_ID/POS/BASE_POS/M_COLS` 使用官方 unsigned 数值形状。
- `INNODB_FOREIGN` 的 `ID/FOR_NAME/REF_NAME` 收口为 `VARCHAR(193) NOT NULL`，`N_COLS/TYPE` 为 `INT UNSIGNED`；`INNODB_FOREIGN_COLS` 的外键 ID 为 `VARCHAR(193)`、列名为 `VARCHAR(64)`、位置为 `INT UNSIGNED`。这些字段仍由现有持久化 InnoDB 目录行提供，未虚构额外运行时数据。
- 新增先失败后修复的 `TestInformationSchemaInnoDBDictionaryColumnsUseNativeMetadata`；`TestInformationSchema` 全量回归通过（23.574s）。范围矩阵仍保持 10 项 implemented、11 项 partial、1 项 unverified、1 项 pending_external、1 项 deferred、2 项 out_of_scope；完整 I_S/P_S 表来源、权限矩阵、P2 原生互操作、P3 外部客户端和 P4 官方 fixture 仍未关闭。

### Continuation 1180

- 修复全仓回归暴露的并发缺陷：`EnhancedMockMySQLServerSession.params` 原先由异步消息处理路径无锁读写，`TestMessageBusIntegration` 在全仓顺序下可触发 `concurrent map read/write`；现在使用 `sync.RWMutex` 保护参数访问。
- 新增先失败后修复的 `TestEnhancedMockMySQLServerSessionConcurrentParamAccess`；该测试在修复前稳定触发 `concurrent map writes`，修复后连续 20 次通过。消息总线测试连续 10 次通过，dispatcher 与 net 相关回归通过。
- `go test -p 1 ./... -count=1 -timeout 45m` 全仓串行回归完成，dispatcher、engine、net、metrics、protocol、replication 等包均通过。`go test -race` 在当前环境因 `CGO_ENABLED=0` 无法启动，未将其结果计入通过证据。

### Continuation 1181

- P1 继续收口 `INFORMATION_SCHEMA.INNODB_INDEXES` 的字段契约：`INDEX_ID/TABLE_ID` 为 `BIGINT UNSIGNED`，`NAME` 为 `VARCHAR(193) NOT NULL`，`TYPE/N_FIELDS/PAGE_NO/SPACE/MERGE_THRESHOLD` 为 `INT NOT NULL`。
- 新增并先失败后修复 `TestInformationSchemaInnoDBDictionaryColumnsUseNativeMetadata` 的 `innodb_indexes` 子用例；InnoDB 目录、Information Schema、Native/Virtual 元数据回归通过：`go test ./server/innodb/engine -run '^TestInformationSchema(InnoDB|Native|Virtual|Columns|Tables|Indexes|Auxiliary)' -count=1 -timeout 30m`，10.175s。
- `INNODB_COLUMNS` 的 DEFAULT_VALUE/DEFAULT_VALUE_UTF8/版本字段精度仍单独保留为下一切片，避免在没有同等强度类型证据时扩大承诺；完整 I_S/P_S、P2/P3/P4 兼容边界仍未关闭。

### Continuation 1182

- 对照 MySQL 8.4 官方源码和动态表契约，收敛 `INNODB_COLUMNS` 为官方 8 列：`TABLE_ID/NAME/POS/MTYPE/PRTYPE/LEN/HAS_DEFAULT/DEFAULT_VALUE`；移除项目此前虚构的 `DEFAULT_VALUE_UTF8/VERSION/HAS_NO_DEFAULT` 输出，避免把数据字典内部字段误报为 INFORMATION_SCHEMA 动态表字段。
- `INNODB_COLUMNS` 元数据形状已补齐：`TABLE_ID/POS BIGINT UNSIGNED NOT NULL`、`NAME VARCHAR(64) NOT NULL`、`MTYPE/PRTYPE/LEN/HAS_DEFAULT INT NOT NULL`、`DEFAULT_VALUE BLOB NULL`。当前持久化目录仍能可靠提供 `TABLE_ID/NAME/POS/MTYPE/LEN/HAS_DEFAULT`，`PRTYPE` 和 instant add 的二进制 `DEFAULT_VALUE` 仍是运行时值语义的下一步，不把 NULL 兜底误记为完整实现。
- 红测先暴露 `INNODB_COLUMNS` 仍为通用 `VARCHAR(255)`/可 NULL 且包含 11 列；修正后 `go test ./server/innodb/engine -run '^TestInformationSchemaInnoDBDictionaryColumnsUseNativeMetadata$/innodb_columns$' -count=1 -timeout 10m` 通过，随后 `go test ./server/innodb/engine -run '^Test(InformationSchema|PerformanceSchema)' -count=1 -timeout 30m` 通过（42.690s）。范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1182.json`，状态仍为 implemented=10、partial=11、unverified=1、pending_external=1、deferred=1、out_of_scope=2。
- 完整 I_S/P_S 表来源、精确运行时统计、权限矩阵、P2 XA/binlog/GTID 与官方 MySQL 崩溃恢复互操作、P3 非 Connector/J 客户端全量矩阵、P4 官方 fixture 仍未关闭；FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1183

- 继续补齐 `INNODB_COLUMNS.PRTYPE` 的值语义：按 InnoDB 官方编码使用 MySQL 字段类型低字节、`DATA_NOT_NULL`、`DATA_UNSIGNED`、`DATA_BINARY_TYPE` 标志以及已知 collation ID，覆盖当前持久化定义中的常见整数、浮点、时间、字符、二进制、LOB、DECIMAL、ENUM/SET 和 GEOMETRY 类型。
- 新增 `TestInformationSchemaInnoDBColumnsProjectsPreciseTypeFlags`，先确认旧实现对 `PRTYPE` 全部返回空值，再验证 `INT NOT NULL`、`BIGINT UNSIGNED NOT NULL`、`VARCHAR`、`VARBINARY` 的编码值；专测通过，`go test ./server/innodb/engine -run '^Test(InformationSchema|PerformanceSchema)' -count=1 -timeout 30m` 通过（44.969s）。
- 未知字段类型或未知 collation 保守返回 NULL；instant add 的 `DEFAULT_VALUE` 仍不能用 SQL 默认值字符串替代 MySQL 的内部二进制格式，继续列为 P1 运行时值语义缺口。范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1183.json`，全局状态未因单字段值语义而虚增为 implemented。
- 随后 `go test -p 1 ./... -count=1 -timeout 45m` 全仓串行回归通过，engine 186.087s、integration 0.813s、manager 7.876s、net 9.504s、replication 2.962s，退出码 0；该证据只说明当前变更未回归，不改变完整 I_S/P_S、官方 XA/binlog/GTID 互操作或外部客户端矩阵仍为 partial/pending 的结论。

### Continuation 1184

- P2 收口 xmysql 内部复制提交的 marker-write/recovery 窗口：带事务 ID 的复制事务在存储提交后、最终 committed marker 之前，先向同一 active transaction journal 追加并同步 `replication_commit` 记录。marker 写入失败或进程重启时，恢复逻辑识别该记录并保留已提交 DML，不再把它当作 orphan journal 回滚。
- 更新并先失败后修复 `TestReplicationCommitMarkerFailureLeavesJournalRecoverable`：覆盖 marker 故障后的恢复、同事务重试、引擎重启后的重复投递抑制；引擎关闭时关闭 active journal 文件句柄但保留恢复文件，避免 Windows 无法清理数据目录且不丢失恢复证据。
- 复制/XA/事务日志专项通过：`go test ./server/innodb/engine -run '^(TestReplication|Test.*TransactionJournal|Test.*Orphaned|Test.*XA)' -count=1 -timeout 30m`、`go test ./server/replication -run '^(TestReplica|TestReplication|Test.*XA)' -count=1 -timeout 20m` 均退出码 0。
- 该切片关闭的是 xmysql 自身 committed-marker 晚于存储提交时的恢复误判窗口，不等于 InnoDB 存储页、native binlog、GTID/applied state 和官方 MySQL XA 的同一物理提交点；P2 仍需统一存储/WAL/复制状态协议及官方双向 fixture。P1 完整 I_S/P_S、P3 非 Connector/J 全量客户端矩阵、P4 官方 MySQL fixture 继续未关闭；FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。
- P0 连接证据同步刷新：隔离服务上的 `JdbcConnectionTest` + `SystemVariableTest` 通过 26/0/0/0；完整 release gate 生成 `reports/compatibility/release-candidate-current-continuation1184/release-candidate.json`，但因 `PerformanceTest` 长时间协议循环导致 Surefire fork 异常终止而为 `NO-GO`。因此 Connector/J 仍按 P0 保留，当前完整门禁记为 partial，不能宣称 139 项全量通过。

### Continuation 1185

- 重新执行 Connector/J P0 门禁并隔离运行器影响：全新数据目录、全新端口 `3312` 上的 10 个非性能测试类通过 `131/131`，失败/错误/跳过均为 `0`；独立长耗时 `PerformanceTest` 通过 `8/8`，耗时 `614.011s`。
- 两部分合计 `139/139`，`0` 失败、`0` 错误、`0` 跳过；证据记录在 `reports/compatibility/connectorj-full-current-continuation1185/connectorj-report.json`，因此 Connector/J 从 `partial` 收口为 P0 `implemented`。此前 `NO-GO` 的根因是门禁运行时过早终止，而非 Connector/J 协议断言失败。
- 当前全局范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1185.json`：24 项纳入范围、2 项明确不纳入；完整 I_S/P_S 表注册仍为 partial，P2 原生 XA/binlog/GTID/崩溃恢复互操作仍为 partial，P3 非 Connector/J 客户端仍为 partial/unverified，P4 官方 MySQL fixture 仍为 pending_external；FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1186-1187

- P3 单节点非 Connector/J 真实认证矩阵已执行：Go MySQL Driver、PyMySQL、Node mysql2 均通过全部必需场景；mysql CLI 因当前环境没有 `mysql` 可执行文件保持 `unverified`，不能把环境缺失误记为协议失败。证据为 `reports/compatibility/client-matrix-current-continuation1186/client-matrix-20260923-194100.json` 和诊断报告 `reports/compatibility/client-matrix-diagnostic-current-continuation1186/client-matrix-20260923-194050.json`。
- 集群端点矩阵脚本修复了 `last_error` 使用 `omitempty` 时的严格模式异常；此前复制追平成功也会因读取不存在的空字段而进入错误路径。修复后 Go、PyMySQL、Node mysql2 均完成源节点测试、GTID 追平、停止源节点、副本提升和提升后客户端测试，三份报告位于 `reports/compatibility/client-cluster-endpoint-current-continuation1187/`。
- 范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1187.json`：Connector/J P0 仍为 implemented；Go/PyMySQL/Node P3 子项收口为 implemented；完整非 Connector/J 矩阵和完整集群客户端矩阵仍因 mysql CLI 缺失保持 partial，mysql CLI 保持 unverified。

### Continuation 1188

- P1 继续收口 InnoDB 辅助表的字段契约：`INNODB_CACHED_INDEXES` 的 `SPACE_ID/INDEX_ID/N_CACHED_PAGES` 改为 `BIGINT UNSIGNED NOT NULL`，`INDEX_NAME` 改为 `VARCHAR(193) NOT NULL`。
- 新增 `TestInformationSchemaInnoDBDictionaryColumnsUseNativeMetadata` 的 `innodb_cached_indexes` 红绿子用例；修复前确认通用 BIGINT 兜底，修复后 `go test -p 1 ./server/innodb/engine -run '^TestInformationSchemaInnoDBDictionaryColumnsUseNativeMetadata$' -count=1 -timeout 10m` 通过。
- `go test -p 1 ./server/innodb/engine -run '^(TestInformationSchema|TestPerformanceSchema)' -count=1 -timeout 30m` 通过（44.994s）。该切片只关闭一个精确元数据缺口；其余 InnoDB 辅助表的类型/值语义、完整 P_S 运行时来源和权限矩阵仍保持 P1 partial。
- 范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1188.json`：26 项总计，24 项纳入范围；状态为 13 implemented、8 partial、1 unverified、1 pending_external、1 deferred、2 out_of_scope。

### Continuation 1189

- P1 继续收口 InnoDB 目录/文件元数据：`INNODB_TABLES` 现在按官方 8.4 形状暴露 `TABLE_ID BIGINT UNSIGNED`、`NAME VARCHAR(655)`、`FLAG/N_COLS/INSTANT_COLS/TOTAL_ROW_VERSIONS INT`、`SPACE BIGINT`、`ROW_FORMAT VARCHAR(12)`、`ZIP_PAGE_SIZE INT UNSIGNED`、`SPACE_TYPE VARCHAR(10)`；`INNODB_DATAFILES` 收口为 `SPACE BIGINT UNSIGNED` 和 `PATH VARCHAR(512)`。
- `TestInformationSchemaInnoDBDictionaryColumnsUseNativeMetadata` 新增两组红绿断言；修复前由通用 BIGINT/VARCHAR 兜底触发失败，修复后通过。P1 合并回归 `go test -p 1 ./server/innodb/engine -run '^(TestInformationSchema|TestPerformanceSchema)' -count=1 -timeout 30m` 通过（45.090s）。
- 当前仍不把 `INNODB_TABLESPACES_BRIEF`、临时表空间、压缩统计、缓冲池页、指标/事务锁等剩余字段和值语义标为完成；它们继续进入下一轮 P1 收口。

### Continuation 1190

- P1 收口临时表相关的 MySQL 8.4 字段契约：`INNODB_SESSION_TEMP_TABLESPACES` 修正为 `ID/SPACE/PATH/SIZE/STATE/PURPOSE`，移除旧的 `ALLOCATED_SIZE`；`INNODB_TEMP_TABLE_INFO` 修正为 `TABLE_ID/NAME/N_COLS/SPACE`，移除 5.7 时代的 `PER_TABLE_TABLESPACE`。
- 新增临时表元数据红绿测试，锁定 `INT UNSIGNED`、`BIGINT UNSIGNED`、`VARCHAR(4000)`、`VARCHAR(64)` 和 `NAME VARCHAR(64) NULL` 等官方 8.4 契约。临时表创建与运行时测试同时验证：会话 ID、实际 `.ibd` 路径、文件大小、`ACTIVE/USER` 状态、稳定内部 `TABLE_ID`，以及由 `.frm` 列数加三个 InnoDB 隐藏列计算出的 `N_COLS`。
- 红测先确认旧注册表仍返回错误列集合和通用类型；修复后 `go test -p 1 ./server/innodb/engine -run '^(TestInformationSchemaTemporaryInnoDBTablesUseMySQL84Metadata|TestInformationSchemaTemporaryInnoDBViewsFilterLiveSession)$' -count=1 -timeout 10m` 通过，P1 合并回归 `go test -p 1 ./server/innodb/engine -run '^(TestInformationSchema|TestPerformanceSchema)' -count=1 -timeout 30m` 通过（44.352s）。
- 当前 `TABLE_ID` 是 xmysql 临时表会话生命周期内的稳定内部 ID；因当前普通表路径未接入官方 InnoDB 数据字典表 ID，不能把它宣称为官方永久字典 ID。完整 P1 仍需继续收口 `INNODB_TABLESPACES_BRIEF`、压缩/缓冲池/指标/事务锁和权限可见性语义。
- 范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1190.json`：26 项总计，24 项纳入范围；状态仍为 13 implemented、8 partial、1 unverified、1 pending_external、1 deferred、2 out_of_scope。

### Continuation 1191

- P1 继续收口 `INNODB_TABLESPACES_BRIEF.FLAG` 的运行时值语义：不再固定返回 `0`，改为读取持久化表空间头中的 FSP flags，与 `INNODB_TABLESPACES.FLAG` 使用同一可靠来源。
- 新增 `TestInformationSchemaInnoDBTablespacesReflectsDurableCatalog` 的交叉断言，验证 `INNODB_TABLESPACES_BRIEF.FLAG` 与 `INNODB_TABLESPACES.FLAG` 一致，并验证 `SPACE_TYPE=Single`；专项通过，随后 `go test -p 1 ./server/innodb/engine -run '^(TestInformationSchema|TestPerformanceSchema)' -count=1 -timeout 30m` 通过（44.476s）。
- 该切片只关闭 BRIEF 的一个真实值语义缺口；BRIEF 全部字段的精确 INFORMATION_SCHEMA 元数据、其余辅助表的运行时统计/权限语义仍未完成。矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1191.json`，状态仍为 13 implemented、8 partial、1 unverified、1 pending_external、1 deferred、2 out_of_scope。

### Continuation 1192

- P1 收口 `INNODB_METRICS` 的官方字段精度：名称/子系统/状态/类型/注释为 `VARCHAR(64) NOT NULL`；`COUNT/COUNT_RESET` 为非 unsigned `BIGINT NOT NULL`；最大值、最小值、耗时及 reset 计数为可 NULL 的 `BIGINT`；平均值为可 NULL 的 `FLOAT`；启用/禁用/reset 时间为可 NULL 的 `DATETIME`。
- 新增 `TestInformationSchemaInnoDBDictionaryColumnsUseNativeMetadata` 的 `innodb_metrics` 红绿子用例；修复前捕获到全部字段被通用 `VARCHAR(255)` 兜底，修复后专项及 `TestInformationSchema|TestPerformanceSchema|TestRuntimeMetrics|TestInformationSchemaInnoDBMetrics` 回归通过（43.720s）。
- 该切片只关闭 INNODB_METRICS 元数据精度，不等于完整 InnoDB 监控器数量、启停/reset 状态机或权限行为已与官方一致；完整 I_S/P_S 表注册和运行时来源仍为 P1 partial。
- 范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1192.json`：26 项总计，24 项纳入范围；状态仍为 13 implemented、8 partial、1 unverified、1 pending_external、1 deferred、2 out_of_scope。

### Continuation 1193

- P1 补齐 `INNODB_TRX` 的 MySQL 8.4 尾部字段：`TRX_ADAPTIVE_HASH_LATCHED INT NOT NULL`、`TRX_ADAPTIVE_HASH_TIMEOUT BIGINT UNSIGNED NOT NULL`、`TRX_SCHEDULE_WEIGHT BIGINT UNSIGNED NULL`；注册表、运行时投影和 `INFORMATION_SCHEMA.COLUMNS` 元数据均已同步。
- 新增 `TestInformationSchemaInnoDBTrxUsesMySQL84TailColumns`，活跃事务测试同时验证三个字段的值形状：自适应哈希字段为 0，调度权重在非锁等待事务中为 NULL。专项与合并 P1 回归通过（43.670s）。
- 这只关闭 `INNODB_TRX` 的三列缺失和对应元数据缺口，不等于完整事务锁状态、CATS 调度权重、PROCESS 权限和所有锁视图语义已完成。矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1193.json`，状态仍为 13 implemented、8 partial、1 unverified、1 pending_external、1 deferred、2 out_of_scope。

### Continuation 1194

- P1 补上 `INNODB_TRX` 和 `INNODB_METRICS` 的 PROCESS 权限门禁：真实会话没有 `PROCESS`、`SUPER` 或 `ALL` 时返回访问拒绝；已有 PROCESS 会话和内部 nil-session 测试路径不受影响。
- 新增 `TestInformationSchemaInnoDBProcessTablesRequireProcessPrivilege`；无权限专项和合并 P1 回归通过（44.562s）。这只覆盖两个明确要求 PROCESS 的表，其他 InnoDB 动态表和 Performance Schema 的权限边界仍需逐表核对。
- 范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1194.json`：26 项总计，24 项纳入范围；状态仍为 13 implemented、8 partial、1 unverified、1 pending_external、1 deferred、2 out_of_scope。

### Continuation 1195

- P1 收口缓冲池动态表的 MySQL 8.4 字段形状：`INNODB_BUFFER_PAGE` 增加 `IS_STALE`，`INNODB_BUFFER_PAGE_LRU` 将旧的 `PAGE_STATE` 修正为 `COMPRESSED`，`INNODB_BUFFER_POOL_STATS` 修正为 `YOUNG_MAKE_PER_THOUSAND_GETS` 和 `NOT_YOUNG_MAKE_PER_THOUSAND_GETS`；缓冲池统计计数器、命中率和速率字段的 unsigned/nullable/type 元数据已按官方契约补齐。
- 缓冲池页运行时投影不再把未知状态伪装成数值：`IS_OLD` 使用 `YES/NO`，未知的哈希、压缩和 stale 状态保持 NULL；官方要求的非 NULL 数值字段用可证明的零值或真实页信息填充，并修复空页快照读取访问时间的潜在 panic。
- 真实会话访问 `INNODB_BUFFER_POOL_STATS`、`INNODB_BUFFER_PAGE`、`INNODB_BUFFER_PAGE_LRU` 前统一执行 `PROCESS` 权限校验；新增字段、运行时值和无权限回归均通过，P1 合并回归 `go test -p 1 ./server/innodb/engine -run '^(TestInformationSchema|TestPerformanceSchema|TestRuntimeMetrics|TestInformationSchemaInnoDBMetrics)' -count=1 -timeout 30m` 通过（46.115s）。
- 范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1195.json`：26 项总计，24 项纳入范围；状态为 13 implemented、8 partial、1 unverified、1 pending_external、1 deferred、2 out_of_scope。完整 I_S/P_S 注册与运行时覆盖、P2 原生 XA/binlog/GTID/崩溃恢复互操作、P3 全量非 Connector/J 矩阵和 P4 官方 fixture 仍未关闭；FULLTEXT deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1196

- P1 收口 InnoDB 压缩统计动态表：`INNODB_CMP`/`INNODB_CMP_RESET` 和 `INNODB_CMP_PER_INDEX`/`_RESET` 的数值、字符串字段已从通用兜底改为官方 8.4 的 `INT`/`VARCHAR(192)`；`INNODB_CMPMEM`/`_RESET` 补齐 `PAGES_USED`、`PAGES_FREE`、`RELOCATION_OPS`、`RELOCATION_TIME` 四列，并同步运行时投影。
- 压缩统计动态表加入真实会话 `PROCESS` 权限门禁；新增元数据列数、精度、过滤和无权限回归。专项通过，P1 合并回归 `go test -p 1 ./server/innodb/engine -run '^(TestInformationSchema|TestPerformanceSchema|TestRuntimeMetrics|TestInformationSchemaInnoDBMetrics)' -count=1 -timeout 30m` 通过（46.855s）。
- 范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1196.json`：26 项总计，24 项纳入范围；状态仍为 13 implemented、8 partial、1 unverified、1 pending_external、1 deferred、2 out_of_scope。压缩表真实多实例统计/reset 与完整 I_S/P_S 覆盖仍未宣称完成；P2/P3/P4、FULLTEXT 和非 InnoDB 范围保持原结论。

### Continuation 1197

- P1 补齐 `INNODB_TABLESPACES_BRIEF` 的官方元数据形状：`SPACE BIGINT UNSIGNED`、`NAME VARCHAR(655)`、`PATH VARCHAR(512)`、`FLAG INT`、`SPACE_TYPE VARCHAR(10)`，并保留现有持久化 FSP flags/space type 值来源。
- `INNODB_TABLESPACES_BRIEF` 加入真实会话 `PROCESS` 权限门禁；专项元数据和权限回归通过。该切片没有把所有 InnoDB 表的权限/运行时语义误报为完成。
- 范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1197.json`：26 项总计，24 项纳入范围；状态仍为 13 implemented、8 partial、1 unverified、1 pending_external、1 deferred、2 out_of_scope。完整 I_S/P_S、P2/P3/P4、FULLTEXT 与非 InnoDB 的范围边界不变。

### Continuation 1198

- P1 补齐 Performance Schema 线程/进程列表的 MySQL 8.4 契约：`performance_schema.threads` 的 24 列现在按官方源码区分 `VARCHAR`、`ENUM`、有符号/无符号 `BIGINT` 及精确长度和可空性；`performance_schema.processlist` 补上此前遗漏的 `EXECUTION_ENGINE` 列，并将官方列的 `HOST` 长度收敛到 `VARCHAR(261) ASCII`。
- `performance_schema.processlist` 运行时投影现在返回真实来源可证明的 `EXECUTION_ENGINE=PRIMARY`，同时保留项目已有的 `TIME_MS/ROWS_SENT/ROWS_EXAMINED` 扩展列；新增线程/进程列表全字段元数据回归，先由缺失列红测暴露，再修复注册、投影和元数据。
- `go test -p 1 ./server/innodb/engine -run '^(TestPerformanceSchemaVirtualThreadsAndProcesslistUseNativeShapes|TestInformationSchemaVirtualProcesslistAndEnginesUseNativeShapes|TestPerformanceSchemaThreadsExposeMySQL84ColumnsAndLiveAttributes|TestPerformanceSchemaProcesslist)' -count=1 -timeout 20m` 通过；随后 P1 合并回归 `go test -p 1 ./server/innodb/engine -run '^(TestInformationSchema|TestPerformanceSchema|TestRuntimeMetrics|TestInformationSchemaInnoDBMetrics)' -count=1 -timeout 30m` 通过（46.942s）。
- 全仓串行回归 `go test -p 1 ./... -count=1 -timeout 45m` 通过，engine 187.861s、integration 1.006s、manager 7.040s、net 7.781s、replication 4.637s，退出码 0；该证据只证明本轮修改未引入仓库级回归。
- 范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1198.json`：26 项总计，24 项纳入范围；完整 I_S/P_S 表注册和值语义、P2 原生 XA/binlog/GTID/崩溃恢复互操作、P3 全量非 Connector/J 客户端和 P4 官方 fixture 仍未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1199

- P1 补充 `performance_schema` 权限/可见性回归：无 `PROCESS` 的真实会话在 `performance_schema.processlist` 中只能看到自身和同用户会话；授予 `PROCESS` 后可看到其他用户；`performance_schema.threads` 不因缺少 `PROCESS` 隐藏其他用户线程，符合 MySQL 8.4 的表级可见性规则。
- 回归同时验证 `EXECUTION_ENGINE=PRIMARY` 的运行时投影；测试先因使用 nil session 无法证明用户边界而失败，改为真实会话路径后通过：`go test -p 1 ./server/innodb/engine -run '^(TestPerformanceSchemaProcesslistAndThreadsHonorNativeVisibilityRules|TestPerformanceSchemaProcesslistProjectsLiveSessions|TestPerformanceSchemaThreadsExposeMySQL84ColumnsAndLiveAttributes)$' -count=1 -timeout 20m`。
- 范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1199.json`：26 项总计，24 项纳入范围。该切片只关闭线程/进程列表的一组权限证据；组件专用表权威运行时来源、完整 I_S/P_S 字段/过滤矩阵、P2/P3/P4 仍未关闭，FULLTEXT 与非 InnoDB 范围边界不变。

### Continuation 1200

- P1 修正 `performance_schema.binary_log_transaction_compression_stats` 的 MySQL 8.4 表形状：从旧的 5 个占位列改为官方 14 列，包括 `TRANSACTION_COUNTER`、压缩百分比、首末事务 ID/字节计数/时间戳。
- `INFORMATION_SCHEMA.COLUMNS` 现在精确反映官方的 `ENUM('BINARY','RELAY')`、`VARCHAR(64)`、unsigned `BIGINT`、signed `SMALLINT`、可空 `TEXT` 和 `TIMESTAMP(6)` 类型；`SELECT *` 列顺序也加入回归保护。
- 官方表定义要求真实 binlog/relay-log 压缩监控上下文，当前 xmysql 尚未提供该运行时统计来源，因此本次只关闭 schema/discovery 兼容切片，不宣称压缩统计值、`TRUNCATE TABLE` 重置和 binlog 互操作完成。依据：[MySQL 8.4 表说明](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-binary-log-transaction-compression-stats-table.html) 和 [官方表定义源码](https://raw.githubusercontent.com/mysql/mysql-server/8.4/storage/perfschema/table_binary_log_transaction_compression_stats.cc)。
- 专项回归通过：`go test -p 1 ./server/innodb/engine -run '^(TestInformationSchemaVirtualBinaryLogCompressionStatsUsesMySQL84Shape|TestPerformanceSchemaRegistryCoversMySQL84VirtualTables)$' -count=1 -timeout 20m`。
- 全仓串行回归通过：`go test -p 1 ./... -count=1 -timeout 45m`；`server/innodb/engine` 184.159 秒，integration、manager、net、replication 及其余包均通过，退出码 0。
- 范围矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1200.json`：26 项总计，24 项纳入范围；完整 I_S/P_S 运行时来源、P2 原生 XA/binlog/GTID/崩溃恢复互操作、P3 全量非 Connector/J 客户端和 P4 官方 fixture 仍未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1201

- P1 修正 `performance_schema.log_status` 的 MySQL 8.4 契约：列从项目自定义的 `RELAY_LOG/BINLOG_SUMMARY` 改为官方 `SERVER_UUID/LOCAL/REPLICATION/STORAGE_ENGINES`，后三列均按 `JSON NOT NULL` 暴露。
- 运行时投影改为官方 JSON 字段名；当前 xmysql 没有可证明的本地 binlog 文件位置、relay-log 文件和 InnoDB log-resource collector，因此只返回已知的 GTID 状态和空的 replication/storage-engine 结构，不伪造文件位置或 LSN。
- 真实会话增加 `BACKUP_ADMIN` 门禁；无该动态权限的会话被拒绝，拥有该权限的会话可读取状态。官方表还要求 `SELECT`，现有 Performance Schema 元数据授权路径仍需继续逐表收口。
- 新增/更新元数据、运行时和权限回归均通过：`go test -p 1 ./server/innodb/engine -run '^(TestPerformanceSchemaReplicationExtendedViewsProjectRuntimeState|TestInformationSchemaVirtualLogStatusUsesMySQL84Shape|TestInformationSchemaVirtualBinaryLogCompressionStatsUsesMySQL84Shape|TestPerformanceSchemaProcesslistAndThreadsHonorNativeVisibilityRules)$' -count=1 -timeout 20m`。
- 全仓串行回归通过：`go test -p 1 ./... -count=1 -timeout 45m`；`server/innodb/engine` 187.063 秒，integration、manager、net、replication 及其余包均通过，退出码 0。
- 官方依据：[log_status 表说明](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-log-status-table.html) 和 [MySQL 8.4 表定义源码](https://raw.githubusercontent.com/mysql/mysql-server/8.4/storage/perfschema/table_log_status.cc)。矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1201.json`：26 项总计，24 项纳入范围；完整 I_S/P_S 运行时来源、P2/P3/P4 仍未关闭。
- 同一切片补齐专用 `TRUNCATE` 分派：`TRUNCATE performance_schema.binary_log_transaction_compression_stats` 按 MySQL 契约成功返回，`TRUNCATE performance_schema.log_status` 明确返回“不允许”；元数据兼容路由不再截获 DDL。该行为由 `TestPerformanceSchemaReplicationExtendedViewsProjectRuntimeState` 回归锁定，但压缩统计重置目前仍无真实采集器可清空。

### Continuation 1202

- 修复元数据路由边界：`executeInformationSchemaMetadataSelect` 现在只接收 `SELECT/SHOW/DESCRIBE/EXPLAIN/WITH` 读语句，避免包含 `performance_schema.*` 的 DDL 被错误当成虚拟查询；该修复由 `TRUNCATE log_status` 的红测暴露并闭环。
- 专项回归通过：P_S/I_S 切片 `16.423s`；`go test -p 1 ./server/replication ./server/net ./server/dispatcher -count=1 -timeout 20m` 分别为 `23.039s/17.546s/4.203s`。
- 全仓串行回归第一次因 C 盘空间不足在 engine 集成阶段失败；随后将 TEMP/TMP/GOCACHE/GOTMPDIR 指向 D 盘重跑，但 engine.test 超过 29 分钟仍未完成，已主动停止，未将其计为通过。当前只确认专项、P1 合并回归和相关包级回归通过。
- 矩阵刷新为 `reports/compatibility/scope-matrix-current-continuation1202.json`：26 项总计，24 项纳入范围；I_S/P_S 完整运行时来源、P2 原生 XA/binlog/GTID/崩溃恢复互操作、P3 全量非 Connector/J 客户端和 P4 官方 fixture 仍未关闭；FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1203

- P1 补齐 `performance_schema.innodb_redo_log_files` 的 MySQL 8.4 INFORMATION_SCHEMA 元数据：`FILE_ID/START_LSN/END_LSN/SIZE_IN_BYTES BIGINT NOT NULL`、`FILE_NAME VARCHAR(2000) NOT NULL`、`IS_FULL TINYINT NOT NULL`、`CONSUMER_LEVEL INT NOT NULL`；现有 redo 文件运行时快照投影保持不变。
- 新增 `TestInformationSchemaVirtualRedoLogFilesUsesMySQL84Shape`，先锁定 7 列精度/长度/可空性，再通过专项和 P1 合并回归；P1 合并命令 `go test -p 1 ./server/innodb/engine -run '^(TestInformationSchemaVirtualRedoLogFilesUsesMySQL84Shape|TestPerformanceSchemaInnoDBRedoLogFilesProjectsLiveRedoSnapshot|TestInformationSchema|TestPerformanceSchema|TestRuntimeMetrics|TestInformationSchemaInnoDBMetrics)' -count=1 -timeout 30m` 通过（45.774s）。
- 官方依据：[MySQL 8.4 Performance Schema miscellaneous tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-miscellaneous-tables.html) 和 [MySQL 8.4 `log0pfs.cc` 表定义源码](https://raw.githubusercontent.com/mysql/mysql-server/mysql-8.4.11/storage/innobase/log/log0pfs.cc)。矩阵状态边界不变：完整 I_S/P_S 运行时来源、P2/P3/P4 仍未关闭；FULLTEXT deferred，非 InnoDB 引擎/修复/转换 out_of_scope。

### Continuation 1204

- P1 修正 `performance_schema.error_log` 的官方 8.4 形状：移除项目自定义的 `TIMESTAMP` 尾列，补回 `THREAD_ID BIGINT UNSIGNED`；`LOGGED` 为 `TIMESTAMP(6)`，`PRIO` 为 `ENUM('System','Error','Warning','Note')`，`ERROR_CODE VARCHAR(10)`、`SUBSYSTEM VARCHAR(7)` 可空，`DATA TEXT NOT NULL`。
- 错误事件记录现在保存调用线程 ID，并将现有数值优先级投影为官方枚举标签；过滤同时保留历史数值输入兼容。新增元数据和运行时回归先红后绿，专项通过（2.460s），P1 合并回归 `go test -p 1 ./server/innodb/engine -run '^(TestInformationSchema|TestPerformanceSchema|TestRuntimeMetrics|TestInformationSchemaInnoDBMetrics)' -count=1 -timeout 30m` 通过（45.569s）。
- 官方依据：[error_log 表说明](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-log-table.html) 和 [MySQL 8.4 `table_error_log.cc` 表定义源码](https://raw.githubusercontent.com/mysql/mysql-server/mysql-8.4.11/storage/perfschema/table_error_log.cc)。完整错误日志 ring buffer、后台线程来源和所有 P_S 表仍未宣称完成。

### Continuation 1205

- P1 修正 `performance_schema.component_scheduler_tasks` 的发现契约：移除项目自定义的 `TASK_NAME/TASK_TYPE/INTERVAL/EXECUTION_COUNT/ERROR_COUNT/LAST_EXECUTED/LAST_ERROR`，改为 MySQL 8.4 的 `NAME/STATUS/COMMENT/INTERVAL_SECONDS/TIMES_RUN/TIMES_FAILED` 六列。
- `INFORMATION_SCHEMA.COLUMNS` 现在覆盖调度器表的官方可查询元数据：`NAME VARCHAR(64)`、`STATUS ENUM('RUNNING','WAITING')`、`COMMENT VARCHAR(1024)`、`INTERVAL_SECONDS INT`、`TIMES_RUN/TIMES_FAILED BIGINT UNSIGNED`，均为 `NOT NULL`；新增 `SELECT *` 和元数据回归。
- 本仓库没有 MySQL Enterprise `scheduler` 组件及其注册任务源，因此当前只完成表名/列名/类型/发现层兼容，运行时返回空任务集；不伪造审计插件或其他组件任务行。依据：[MySQL 8.4 component_scheduler_tasks 表说明](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-component-scheduler-tasks-table.html) 和 [Scheduler Component 说明](https://dev.mysql.com/doc/refman/8.0/en/scheduler-component.html)。
- 专项回归通过：`go test -p 1 ./server/innodb/engine -run '^(TestInformationSchemaVirtualComponentSchedulerTasksUsesMySQL84Shape|TestPerformanceSchemaRegistryCoversMySQL84VirtualTables)$' -count=1 -timeout 20m`。
- 该切片仍不改变全局边界：完整 I_S/P_S 运行时来源、P2 原生 XA/binlog/GTID/崩溃恢复互操作、P3 全量非 Connector/J 客户端和 P4 官方 fixture 仍未关闭；FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1206

- P1/P2 收紧 `performance_schema.log_status` 的运行时投影：当存在本地 `replication.Source` 时，`LOCAL.binary_log_file` 和 `LOCAL.binary_log_position` 现在来自真实 native binlog 的持久化文件与物理结束位置，不再固定返回空文件和 `0`。
- 官方 MySQL 8.4 的 `REPLICATION` JSON 是包含 `channels` 数组的对象；当前运行时尚未保存本地 relay-log 文件/位置和 InnoDB log-resource 统计，因此保持官方空 channel 形状 `{"channels":[]}`，不伪造 relay 元数据。新增源文件/位置回归并通过：`TestPerformanceSchemaReplicationExtendedViewsProjectRuntimeState`。
- P1 合并回归通过：`go test -p 1 ./server/innodb/engine -run '^(TestInformationSchema|TestPerformanceSchema|TestRuntimeMetrics|TestInformationSchemaInnoDBMetrics)' -count=1 -timeout 30m`；P2 本地 XA/native binlog/复制回归通过：`go test -p 1 ./server/innodb/engine ./server/net ./server/replication -run 'Test.*(XA|Native|Replica|Replication|Binlog|Recovery|Promotion)' -count=1 -timeout 30m`。崩溃恢复管理器矩阵连续重复 3 次通过，报告为 `reports/compatibility/crash-recovery-current-continuation1206/crash-recovery.json`。
- 当前环境没有 `mysql/mysqld/mysqladmin`，Docker CLI 也无法连接 Docker Desktop daemon，因此官方 MySQL XA/binlog/复制互操作仍为外部待验证，不把本地回归写成官方互操作完成。完整 I_S/P_S 运行时来源、P2 官方互操作、P3 全量非 Connector/J 客户端仍未关闭；FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1207

- 复核 MySQL 8.4 官方 `storage/perfschema/table_log_status.cc` 后纠正上一轮试探性判断：`log_status` 的原生读取门禁只检查 `BACKUP_ADMIN`，因此不额外加入独立 `SELECT` 门禁；`REPLICATION` 也保留官方 `{"channels":[]}` 对象形状，而不是数组顶层值。
- 本轮只保留已由官方源码和本地回归共同支持的实现：真实 native binlog 文件/物理位置投影、官方 JSON 形状和 `BACKUP_ADMIN` 拒绝路径。完整 I_S/P_S 字段、运行时来源、逐表权限/过滤矩阵，以及 P2 官方互操作、P3 全量非 Connector/J 客户端继续保持未关闭；FULLTEXT deferred，非 InnoDB 引擎/修复/转换 out_of_scope。

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

### Continuation 1212

- P1 补齐未知错误的官方聚合规则：无法映射到本地 MySQL 错误码的错误现在按汇总维度合并为 `ERROR_NUMBER=0`、`ERROR_NAME=NULL`、`SQL_STATE=NULL` 的单行；`SUM_ERROR_RAISED`、`SUM_ERROR_HANDLED` 以及首末时间继续聚合，已知内部错误继续投影官方错误名和错误码。
- 新增未知错误红测后转绿，并纳入 P1 合并回归；P1 门禁通过（47.535s）。P2 XA/native-binlog/复制本地门禁通过：engine 14.675s、net 2.395s、replication 2.602s。
- 当前仍未关闭：`SUM_ERROR_HANDLED` 没有 SQL handler 运行时来源；五类错误汇总表的逐维度截断、连接断开和身份生命周期尚未完成 MySQL 级证据；完整 I_S/P_S 运行时来源、P2 官方互操作、P3 非 Connector/J 全量矩阵和 P4 官方 fixture 仍保持 partial/pending。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1213

- P1 补齐 `SUM_ERROR_HANDLED` 的仓库内运行时来源：存储过程捕获嵌套 SQL 异常、以及捕获 `SIGNAL SQLSTATE '45000'`，现在都按 MySQL 语义各记录一次 `SUM_ERROR_RAISED` 和一次 `SUM_ERROR_HANDLED`；`SQLWARNING` 不计入错误汇总。
- 运行时记录器新增独立 handled 计数，不再复用 warning 计数；同时修复了第一次实现对存储过程子语句统计的回归，子语句仍会进入 `events_statements_summary_by_program`。
- 红测转绿：`TestPerformanceSchemaErrorSummaryCountsHandledSignalAsHandled`；错误摘要/程序摘要专项通过；P1 合并回归 `go test -p 1 ./server/innodb/engine -run '^(TestInformationSchema|TestPerformanceSchema|TestRuntimeMetrics|TestInformationSchemaInnoDBMetrics)' -count=1 -timeout 30m` 通过（46.755s）。证据：`reports/compatibility/p1-error-summary-current-continuation1213.txt`。
- 当前仍未关闭：完整 I_S/P_S 运行时来源、组件专用表和全部权限/过滤/生命周期边界；P2 官方 XA/binlog/GTID/崩溃恢复互操作、P3 非 Connector/J 全量矩阵、P4 官方 fixture 仍为 partial/pending。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1214

- 修正 P0 Connector/J 的过期状态：最新隔离完整门禁为 139/139，通过 0 failures、0 errors、0 skipped；`PerformanceTest` 8 项在隔离服务中完成，不再把旧的 Surefire 长耗时误写成 NO-GO。
- P1 `performance_schema.data_locks` 接入真实 `LockManager` 快照，已能展示无等待场景下的 granted record lock，也保留 waiting 请求；`data_lock_waits` 使用带事务 ID 的唯一 lock ID，能够关联到 `data_locks` 行。
- 红测转绿：`TestPerformanceSchemaDataLocksExposeGrantedRecordLocks`；manager 锁测试和 P_S wait/lock/data-lock/metadata 回归均通过。证据：`reports/compatibility/p1-data-locks-current-continuation1214.txt`。
- 当前仍未关闭：锁管理器尚未提供原生 schema/table/index/record 元数据，完整 P1 组件运行时来源、P2 统一 storage/WAL/binlog 提交协议、P3 全量客户端证据和 P4 官方 MySQL fixture 仍为 partial/pending。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1215

- P2 本地回归复核通过：`server/replication` 的 XA/native/recovery/GTID/replica/source 专项 4.754s，`server/innodb/engine` 复制专项 2.432s。该证据只证明 xmysql 自身复制状态机与恢复路径，不等于官方 MySQL 双向互操作。
- P3 客户端环境诊断刷新：Go、PyMySQL、Node.js/mysql2 可用，当前环境未发现 `mysql.exe`；本次未提供受保护密码，因此没有启动客户端功能矩阵，聚合矩阵继续 partial，MySQL CLI 继续 unverified。证据：`reports/compatibility/client-matrix-diagnostic-current-continuation1215/client-matrix-20260923-233348.json`。
- P2 仍有架构性缺口：普通 DML 的存储事务按语句提交，外层事务依赖 journal/逆操作补偿；storage/WAL、native binlog、GTID 发布和崩溃恢复尚未统一到一个物理提交协议，不能标记 P2 完成。

### Continuation 1216

- P1 Performance Schema 的 `events_statements_history` 与
  `events_stages_history` 现在按当前 session/thread id 隔离，不再把其他
  连接的历史行返回给当前连接；`events_*_history_long` 继续保持实例级
  历史。runtime recorder 保存有界的逐线程历史，executor 使用当前连接 id
  选择对应历史。
- 红测转绿、metrics 专项、完整 P1 engine 门禁和 metrics 全包均通过；证据：
  `reports/compatibility/p1-thread-history-current-continuation1216.txt`。
- 该切片只关闭逐线程来源/过滤缺口。MySQL 精确 history 容量与自动调整、系统变量、线程结束丢弃、wait history 生命周期及完整 P_S 表/运行时语义仍为 partial；P2 统一物理提交协议、P3 全量非 Connector/J 客户端证据、P4 官方 MySQL 互操作仍未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。官方依据：[statement history 表说明](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-events-statements-history-table.html)。

### Continuation 1217

- P1 `performance_schema.events_waits_history` 现在按当前 session/thread id
  过滤已完成的表锁和元数据锁等待；`events_waits_history_long` 仍是实例级
  历史，current/consumer/instrument/timer/summary 回归保持通过。
- 红测转绿，并通过等待/锁专项和完整 P1 engine 门禁；证据：
  `reports/compatibility/p1-wait-history-current-continuation1217.txt`。
- 该切片只关闭 wait history 的 session 过滤缺口。MySQL 精确容量/生命周期
  以及所有 wait instrument 的完整 ownership 映射仍为 partial；P2 统一物理
  提交协议、P3 全量非 Connector/J 客户端证据、P4 官方 MySQL 互操作仍未关闭。
  FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1218

- P2 本地复制回放现在为一条包含多条 SQL 或 row-image change 的 source
  transaction 建立并复用一个物理 storage transaction；逐条 replay DML 不再
  单独 commit/flush，由外层 COMMIT/ROLLBACK 统一负责。证据：
  `reports/compatibility/p2-replication-storage-transaction-current-continuation1218.txt`。
- 红测转绿，并通过 engine replication、replication/net XA-binlog 相关门禁及
  完整 P1 engine 门禁。
- 这只关闭本地 replication replay 的 storage transaction 边界。普通客户端事务、
  storage/WAL、native binlog、GTID/applied-state 和崩溃恢复仍未统一为一个原子
  物理提交点；官方 MySQL 互操作仍是 P4 外部门禁。

### Continuation 1219

- P1 修正 `INFORMATION_SCHEMA.INNODB_COLUMNS.DEFAULT_VALUE` 的 ALTER fallback 元数据边界：显式 `DEFAULT ''` 不再被当成“无默认值”，带空格的引号默认值也不再被 `strings.Fields` 截断；支持的字符串默认值以字节载荷投影，整数继续使用 InnoDB 符号位翻转/大端编码。
- 新增显式空字符串、带空格字符串和整数默认值回归；完整 P1 engine 门禁通过（49.421s）。证据：`reports/compatibility/p1-innodb-default-value-current-continuation1219.txt`。
- 该切片只关闭持久化默认值解析的具体边界；时间/DECIMAL/表达式/排序规则相关的完整内部编码仍需 authoritative Field 来源，完整 I_S/P_S、P2 统一提交协议、P3 全量客户端和 P4 官方 fixture 仍保持 partial/pending。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1220

- 普通客户端 `autocommit=0`/显式事务现在复用单一 shared storage transaction；逐语句 DML 不再结束外层边界，COMMIT、ROLLBACK、`SET autocommit=1` 和连接重置负责完成边界。
- 当前 B+Tree writer 仍是 eager page writer，因此增加会话级 pending DML visibility fence：INSERT/UPDATE/DELETE 写入会话可读自己的未提交变更，其他客户端会话看到重建后的已提交视图；二级索引条件会回到聚簇扫描后再应用谓词，COMMIT 后解除，ROLLBACK 后通过物理回滚加既有 inverse DML 清理。pending registry 按数据目录隔离。
- XA COMMIT/ROLLBACK 也会关闭会话持有的 shared storage transaction，避免 XA 结束后遗留 ACTIVE physical transaction。
- 红测转绿；普通事务、P0 rollback/savepoint、XA、复制事务组合回归通过；受限 P1 engine 门禁通过。证据：`reports/compatibility/p2-client-transaction-boundary-current-continuation1220.txt`。
- 仍未关闭：UPDATE/DELETE 与索引范围的完整 MVCC 可见性、storage/WAL/native binlog/GTID/crash recovery 的统一原子提交点。完整 engine 门禁本次被 Windows Go runtime 的 VirtualAlloc errno=1455 内存不足中止，不作为代码失败证据；P1/事务受限门禁已通过。

### Continuation 1221

- 普通客户端 REPEATABLE READ/SERIALIZABLE 现在在第一次一致性 `SELECT` 时捕获提交 epoch；之后其他会话提交的 INSERT/UPDATE/DELETE 通过有界 committed-change history 重建为快照视图，COMMIT/ROLLBACK/连接重置会清除快照。新增红测转绿，并通过 P1、普通事务、XA、复制组合门禁。
- 该实现关闭的是当前 eager B+Tree writer 上的逻辑客户端可见性缺口，不是物理页级 MVCC；版本历史不会跨重启持久化，storage/WAL/native binlog/GTID/crash recovery 仍未统一到一个原子提交点，因此 P2 继续 partial。完整 I_S/P_S 表注册与运行时来源、P3 非 Connector/J 全量矩阵、P4 官方 MySQL fixture 仍未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1221a

- 对长事务补充了历史保留策略：存在活跃快照时不裁剪其所需的旧提交记录，快照结束后才恢复有界裁剪；空 epoch 和长快照回归均通过。仍然只是逻辑可见性，不是物理页级 MVCC，也不跨重启持久化。

### Continuation 1222

- 重新执行仓库侧 native binlog、GTID、replica apply、XA/binlog、recovery、promotion 专项：`server/replication` 6.492s、`server/net` 2.171s、`server/innodb/engine` 5.492s，均通过。证据：`reports/compatibility/p2-replication-binlog-current-continuation1222.txt`。
- 该结果只证明 xmysql 自身协议和恢复路径；真实 MySQL 8.4 上下游互操作、storage pages/WAL/native binlog/GTID/crash recovery 单一物理提交点仍未完成。P2/P4 继续 partial/pending_external。

### Continuation 1223

- 刷新客户端环境诊断：Go、PyMySQL、Node.js/mysql2 均可用；当前机器仍没有 `mysql.exe`。本次未提供受保护客户端密码，因此只做环境发现，不把未执行的真实连接矩阵误报为通过。证据：`reports/compatibility/client-matrix-diagnostic-current-continuation1223/client-matrix-20260924-004657.json`。
- 非 Connector/J 矩阵继续保持“已实现客户端有历史通过证据、当前环境可运行但本次未做认证实测”；MySQL CLI 继续 `unverified`，官方 MySQL fixture 继续 `pending_external`。

### Continuation 1224

- 当前 INFORMATION_SCHEMA 权限/角色专项回归通过（3.145s），覆盖 account、active/applicable/administrable role 和 role grant 过滤。证据：`reports/compatibility/p1-privilege-role-current-continuation1224.txt`。
- 这不等于 MySQL 全部授权生命周期已完成；动态 GRANT/REVOKE 可见性、完整角色继承边界、PROCESS 作用域及所有账户/plugin 权限来源仍是 P1 partial。

@@
### Continuation 1225

- 补齐普通 JOIN 物化路径的事务快照边界：JOIN 两侧的聚簇扫描现在复用
  单表 SELECT 的 session-aware visibility fence，REPEATABLE READ 不会因
  走 JOIN 专用执行器而看到快照之后提交的行。
- 同时修正 JOIN `COUNT(*)` 聚合器未设置 `countStar` 导致结果为 0 的兼容性
  缺口；新增直接 JOIN、派生 JOIN、聚合和 COMMIT 回归并通过。证据：
  `reports/compatibility/p2-client-transaction-boundary-current-continuation1220.txt`。
- 这仍是 eager B+Tree writer 上的逻辑可见性重建，不是物理页级 MVCC，也不
  关闭 storage/WAL/native binlog/GTID/crash recovery 单一原子提交点。

### Continuation 1226

- 重新执行 P1 INFORMATION_SCHEMA、Performance Schema、账户权限和角色元数据
  回归：`go test -p 1 ./server/innodb/engine -run
  '^(TestInformationSchema|TestPerformanceSchema|TestRole|TestApplicableRoles|TestMandatoryRoles|TestInformationSchemaPrivilege)'
  -count=1 -timeout 60m`，通过（59.778s）。证据：
  `reports/compatibility/p1-metadata-privilege-regression-current-continuation1226.txt`。
- 该结果确认当前已实现切片仍稳定，但不关闭完整 MySQL 8.4 I_S/P_S 表、字段、
  运行时来源、容量/生命周期、权限和角色继承矩阵；这些继续保持 P1 partial。
- P2 物理统一提交协议与官方 XA/binlog/GTID/崩溃恢复互操作、P3 非 Connector/J
  全量客户端矩阵和 MySQL CLI、P4 官方 MySQL fixture 仍未关闭；FULLTEXT 继续
  deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1227

- 修正普通客户端提交顺序：先提交 shared storage transaction，再发布
  replication/binlog；为客户端事务分配稳定 commit key，使发布后报错可以重试而不
  生成第二个 GTID/native transaction。自动提交 DML/DDL 成功发布后会清理 journal、
  changes 和 key，避免后续语句复用旧事务身份。
- storage redo 已提交但脏页 FlushPage 失败时，现在保留 `COMMITTED` 状态并返回明确
  durability error；客户端提升已提交的 pending changes，只重试 replication 发布，
  不重复提交物理 storage transaction。故障注入、普通事务、XA、source/replica DML/DDL
  回归通过，证据：`reports/compatibility/p2-commit-order-and-retry-current-continuation1227.txt`。
- 本次关闭的是本地提交顺序/幂等缺口，不等于 storage pages/WAL/native binlog/GTID/
  crash recovery 的统一物理提交点，也不等于官方 MySQL 8.4 互操作；P2/P4 继续 partial/
  pending_external。P1 全量 I_S/P_S、P3 非 Connector/J 全量矩阵仍在全局任务中，FULLTEXT
  继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1228

- 完成全局 release-candidate gate 的本地接线：新增兼容性 scope-matrix 校验，扩大 P1
  门禁覆盖 Information Schema、Performance Schema、账户权限和角色回归，P2 门禁加入
  stable client commit/retry 故障注入测试。矩阵校验通过（26 项、24 项在范围内、2 项
  明确 out_of_scope），P1 门禁通过 51.403s，P2 门禁通过（engine 14.183s、net 1.823s、
  replication 6.629s）。证据：`reports/compatibility/gate-wiring-current-continuation1228.txt`。
- P3 client-matrix、client-cluster-endpoint 和 P4 官方 MySQL fixture 仍是门禁必检项；
  缺少 `mysql.exe` 或官方 fixture 时保持环境阻塞/`pending_external`，不会伪造为通过。

### Continuation 1229

- 刷新 P3 客户端环境诊断：Go、PyMySQL、Node.js/mysql2 可用，但 `mysql.exe` 不存在，
  且未提供受保护的 `XMYSQL_CLIENT_PASSWORD`；功能矩阵未启动，MySQL CLI 继续
  `unverified`，全量非 Connector/J 矩阵继续 `partial`。证据：
  `reports/compatibility/client-matrix-diagnostic-current-continuation1229.txt`。

### Continuation 1232/1233

- 低资源参数下完整本地 Go integration 通过：engine 213.441s、net 9.637s、
  protocol 0.537s、replication 4.498s；用于规避 Windows 提交额度峰值，不能
  降低功能覆盖范围。
- 完整门禁推进到 Connector/J；前 6 个测试类 0 failures/0 errors，但
  `PerformanceTest` 在 10,000 条批量装载阶段未结束，故 Connector/J 完整门禁
  暂不标记为本轮通过。门禁相对路径缺陷已修复，下一轮需要重新取得最终 JSON。
- 第二轮门禁发现并修复 scope-matrix 对绝对输出路径的重复拼接问题；独立矩阵诊断
  已重新通过，完整 release-candidate 仍需在该修复后重新执行。

### Continuation 1238

- 修复普通 Connector/J 重复事务序列中只读 Repeatable Read 快照在
  `SET autocommit=1` 后残留的问题；新增红测
  `TestRepeatedJdbcTransactionSequenceKeepsDecimalRowsVisible`，并通过事务、聚合、
  GROUP BY 相关回归。
- 修复后 Connector/J 权威完整门禁通过 `139/139`，`0` failures、`0` errors、`0`
  skipped；`TransactionTest` 8/8 和 `PerformanceTest` 全部 8/8 通过。证据：
  `reports/compatibility/release-candidate-current-continuation1238/jdbc.log`。
- 完整 release-candidate 的本地必需链路全部通过：P1、P2、Go core、集群 smoke、
  崩溃恢复（3 次）、并发和 observability 均为 `PASS`。权威汇总：
  `reports/compatibility/release-candidate-current-continuation1238/release-candidate.json`。
- 总体仍为 `NO-GO`，仅因 P3 客户端矩阵的受保护认证环境缺失：没有
  `XMYSQL_CLIENT_PASSWORD`，且没有 `mysql.exe`。这不是 Connector/J 功能失败；Go、
  PyMySQL、Node.js/mysql2 的可用性已发现，但完整非 Connector/J 矩阵不能据此宣称完成。
- 全局范围不变：完整 I_S/P_S 仍为 P1 partial；XA 与 xmysql-native binlog/复制/崩溃
  恢复本地路径纳入 P2，官方 MySQL 双向互操作仍为 P4 pending_external；FULLTEXT
  deferred；非 InnoDB 引擎、专用 REPAIR TABLE 和引擎转换 out_of_scope。

### Continuation 1239

- 压缩管理器新增按 tablespace 的 `CompressionStats`，全局统计与失败计数统一受锁保护，
  为 InnoDB 动态视图提供真实来源。
- `INFORMATION_SCHEMA.INNODB_CMP_PER_INDEX` 不再直接返回空结果：对 `.frm` 确认的
  PRIMARY clustered index，投影该 tablespace 的真实压缩操作数；二级索引没有独立来源
  时不复制全局计数，避免伪造 per-index 语义。
- 红测转绿并通过相关回归：
  `TestCompressionManagerKeepsPerSpaceStatistics`、
  `TestInformationSchemaInnoDBCompressionPerIndexProjectsPrimarySpaceStats`，以及
  `TestInformationSchemaInnoDBCompression*`、Buffer Pool 和 cached-index 切片。
- 这只关闭一个 P1 InnoDB runtime source 缺口；完整 I_S/P_S 表、字段、权限、筛选和
  生命周期仍保持 partial，P2/P3/P4、FULLTEXT 与非 InnoDB 范围边界不变。

### Continuation 1240

- `CompressionManager` 新增聚合和按 tablespace 的原子 snapshot-and-reset 接口，
  `INNODB_CMP_RESET`、`INNODB_CMPMEM_RESET`、`INNODB_CMP_PER_INDEX_RESET` 不再把
  reset 视图当作普通只读视图。
- 新增并通过 `TestCompressionManagerResetSnapshotsAndClearsCounters` 和
  `TestInformationSchemaInnoDBCompressionResetViewsClearTheirCounters`；聚合 reset 后
  普通视图归零，按表空间 reset 不影响其他表空间。证据：
  `reports/compatibility/p1-compression-reset-current-continuation1240.txt`。
- 这只关闭压缩统计 reset 生命周期；完整 I_S/P_S 表/字段/权限/筛选/运行时来源，及
  压缩耗时、解压计数、二级索引精确归属仍为 P1 partial。

### Continuation 1241

- `DecompressPage` 成功解压后现在累计 aggregate 和 tablespace 的
  `UncompressedPages`，`INNODB_CMP`/`INNODB_CMP_PER_INDEX` 投影真实
  `UNCOMPRESS_OPS`，reset 同步清零。
- 新增并通过 `TestCompressionManagerTracksSuccessfulDecompression` 和
  `TestInformationSchemaInnoDBCompressionViewsExposeDecompressionCount`；证据：
  `reports/compatibility/p1-compression-uncompress-current-continuation1241.txt`。
- 当前没有权威的 MySQL 兼容时间单位，因此 `COMPRESS_TIME`/`UNCOMPRESS_TIME` 仍为 0；
  二级索引精确归属和完整 I_S/P_S 仍为 P1 partial。

### Continuation 1242

- 压缩管理器在源头累计实际 `time.Duration`，I_S 压缩视图按完整秒投影
  `COMPRESS_TIME`/`UNCOMPRESS_TIME`；亚秒操作显示 0，符合 MySQL 8.4 的单位语义。
- manager 耗时来源和 `TestInformationSchemaInnoDBCompression*` 回归通过；证据：
  `reports/compatibility/p1-compression-timing-current-continuation1242.txt`。
- `innodb_cmp_per_index_enabled`、二级索引精确归属、完整 I_S/P_S 表/字段/权限/筛选和
  组件运行时语义仍未完成，继续保持 P1 partial。

### Continuation 1243

- 新增全局动态 innodb_cmp_per_index_enabled，默认 OFF；关闭时只保留聚合压缩
  统计，开启后从后续操作开始保留 per-index tablespace 统计，切换会重置该采集窗口。
- SET GLOBAL 和持久化变量恢复路径均同步运行时压缩管理器；相关 manager/I_S 回归
  通过。证据：
  reports/compatibility/p1-compression-per-index-switch-current-continuation1243.txt。
- 当前 per-index 仍只能投影 source-backed PRIMARY；二级索引精确归属和完整 I_S/P_S
  表、字段、权限、筛选及生命周期继续为 P1 partial。

### Continuation 1244

- `INFORMATION_SCHEMA.INNODB_METRICS` 新增官方名称
  `compress_pages_compressed` 和 `compress_pages_decompressed`，使用
  `CompressionManager` 的真实压缩/解压来源，归属 `compression` 子系统，类型为
  `status_counter`，默认禁用。
- `innodb_monitor_enable`、`innodb_monitor_disable`、`innodb_monitor_reset` 和
  `innodb_monitor_reset_all` 支持官方 `module_compress` 模块及指标；现有
  `INNODB_CMP` 聚合统计不受监控开关影响，持久化变量启动恢复会同步运行时状态。
- 新增并通过
  `TestInformationSchemaInnoDBMetricsExposeCompressionRuntimeCounters` 及相关
  InnoDB/manager/system-variable 回归；证据：
  `reports/compatibility/p1-innodb-metrics-compression-current-continuation1244.txt`。
- 该切片只关闭两个官方压缩指标的 runtime-source 缺口，完整 I_S/P_S 表、字段、权限、
  筛选、生命周期和组件运行时语义仍为 P1 partial。

### Continuation 1245

- 对齐 `INFORMATION_SCHEMA.INNODB_METRICS` 的 Buffer Pool 官方语义：新增
  `buffer_pool_bytes_data`、`buffer_pool_bytes_dirty`、`buffer_pool_size`，由真实页数和
  页大小计算；已有页面指标使用 `value`，读写请求使用 `status_counter`，子系统使用
  官方的 `buffer`/`server`。
- `buffer_pool_pages_misc`、read-ahead、read-ahead-evicted 和 wait-free 暂不注册，因当前
  Buffer Pool 管理器没有能证明这些值的权威来源，不能用固定零值填充。
- 新增并通过 `TestInformationSchemaInnoDBMetricsExposeBufferPoolByteAndSizeValues`；
  证据：`reports/compatibility/p1-innodb-metrics-buffer-pool-current-continuation1245.txt`。
- 该切片只关闭三个 Buffer Pool runtime-source 缺口，完整 I_S/P_S 仍为 P1 partial。

### Continuation 1246

- `innodb_monitor_enable/disable/reset` 现在支持官方 `%` 通配符选择器；
  `compress_pages_%` 会匹配本次已实现的两个 compression counters，同时保留精确名、
  `module_compress`、`all` 和兼容别名。
- 新增并通过压缩指标通配符回归；证据：
  `reports/compatibility/p1-innodb-metrics-monitor-pattern-current-continuation1246.txt`。
- 完整 `INNODB_METRICS` 注册表及生命周期时间戳、reset 列仍为 P1 partial。

### Continuation 1247

- 两个已有真实来源的压缩指标现在维护官方生命周期字段：
  `COUNT_RESET`、`TIME_ENABLED`、`TIME_DISABLED`、`TIME_ELAPSED` 和
  `TIME_RESET`；`time.Time` 结果也按 DATETIME 投影。
- `innodb_monitor_reset_all` 对仍启用的压缩指标返回错误；先按官方流程
  disable 后才允许 reset。
- 新增并通过生命周期与 reset-all 红绿回归、相关 engine/manager 回归；证据：
  `reports/compatibility/p1-innodb-metrics-lifecycle-current-continuation1247.txt`。
- 该切片只关闭已实现压缩指标的生命周期/reset 缺口；完整注册表、所有指标的
  权威来源和 max/min/avg 采样语义仍为 P1 partial。

### Continuation 1248

- 使用现有 LockManager wait graph 提供官方
  `lock_row_lock_current_waits` 和 `lock_threads_waiting` 指标；新增的
  `trx_active_transactions` 默认 disabled、类型为 `counter`，通过
  `module_trx` 后返回真实活动事务数。
- 事务/锁指标支持已实现范围内的精确名、subsystem、`module_trx`、
  `module_lock`、`all` 和 `%` 选择器；保留原有自定义诊断行。
- 新增并通过官方事务/锁指标及受影响 engine/manager 回归；证据：
  `reports/compatibility/p1-innodb-metrics-transaction-lock-current-continuation1248.txt`。
- 该切片只关闭三个 source-backed 指标缺口，完整 314 行注册表、所有模块和
  全部采样语义仍为 P1 partial。

### Continuation 1249

- 新增官方 `dml_inserts`、`dml_updates`、`dml_deletes` 运行时行，来源是成功
  INSERT/REPLACE、UPDATE、DELETE 的真实 affected-row 结果；默认启用，类型为
  `status_counter`，子系统为 `dml`。
- `module_dml`、精确名、通配符、disable、reset 和 disabled-counter 的
  `reset_all` 均已接入；普通 reset 保留累计 `COUNT`，清零 `COUNT_RESET`，
  `reset_all` 清除累计值和生命周期字段。
- 新增并通过 `TestInformationSchemaInnoDBMetricsExposeDMLRowCounters` 和
  `TestInformationSchemaInnoDBMetricsDMLLifecycleAndReset`；相关
  `INNODB_METRICS`、system-variable、persisted-variable 回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-dml-current-continuation1249.txt`。
- `dml_reads`、system-DML 及完整 314 行 registry 仍保持 P1 partial，不能用
  无权威来源的固定值冒充实现。

### Continuation 1250

- 新增官方 `buffer_pages_read`、`buffer_pages_written` 和 `innodb_page_size`；
  前两者来源于 Buffer Pool manager 的真实 page read/write 计数，后者来源于
  当前 InnoDB 页大小配置，分别使用 `buffer/status_counter` 和
  `server/value` 语义。
- 新增并通过 `TestInformationSchemaInnoDBMetricsExposeBufferPageCounters`，
  同时通过已有 Buffer Pool 指标回归。证据：
  `reports/compatibility/p1-innodb-metrics-buffer-page-current-continuation1250.txt`。
- `buffer_pool_pages_misc`、read-ahead、read-ahead-evicted、wait-free 和完整
  314 行 registry 仍为 P1 partial，不使用无来源的零值填充。

### Continuation 1251

- 使用 LockManager 的完整等待摘要新增官方
  `lock_row_lock_waits`、`lock_row_lock_time`、`lock_row_lock_time_avg` 和
  `lock_row_lock_time_max`，时间单位为毫秒，类型为 `status_counter`。
- `module_lock`、精确名和通配符的开关，以及 disabled-counter 的
  `reset_all` 基线均已接入；新增并通过
  `TestInformationSchemaInnoDBMetricsExposeCompletedLockWaitCounters`，
  `INNODB_METRICS` 专项回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-lock-summary-current-continuation1251.txt`。
- 死锁、超时、记录锁创建/删除/请求等仍没有权威累计来源，完整 lock 模块和
  314 行 registry 继续保持 P1 partial。

### Continuation 1252

- 四个 source-backed lock-summary 指标新增普通 `innodb_monitor_reset` 的
  reset-window 语义：累计 `COUNT` 保留，`COUNT_RESET` 及 reset min/max/avg
  从 reset 基线重新计算，并暴露 `TIME_RESET`。
- 新增并通过
  `TestInformationSchemaInnoDBMetricsLockSummaryResetProjectsResetColumns`；
  完整 `INNODB_METRICS` registry、死锁/超时/记录锁生命周期仍为 P1 partial。
  证据：
  `reports/compatibility/p1-innodb-metrics-lock-reset-current-continuation1252.txt`。

### Continuation 1253

- Buffer Pool 新增官方 `buffer_pool_pages_misc`、`buffer_pool_read_ahead` 和
  `buffer_pool_read_ahead_evicted`；分别来源于实时页数关系、成功的后台预读和
  预读页在命中前被 LRU 驱逐的事件。
- 新增并通过 manager/engine 聚焦回归；证据：
  `reports/compatibility/p1-innodb-metrics-buffer-runtime-current-continuation1253.txt`。
- `buffer_pool_wait_free`、剩余指标模块及完整 I_S/P_S 注册表仍为 P1 partial，
  因为当前引擎仍缺少可证明的权威运行时来源或完整表投影。

### Continuation 1254

- `INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS` 的
  `NUMBER_PAGES_READ_AHEAD` 与 `NUMBER_READ_AHEAD_EVICTED` 现在投影实时预读和
  预读驱逐计数，不再固定返回 0。
- 红测先因视图返回 0 超时，接入来源后 engine 专项回归通过；证据：
  `reports/compatibility/p1-innodb-buffer-pool-stats-current-continuation1254.txt`。
- 对应 rate 列仍缺少时间窗口来源，完整 I_S/P_S 仍为 P1 partial。

### Continuation 1255

- Buffer Pool 新增 page read/write、read-ahead 和 read-ahead-evicted 的实时每秒
  delta 速率；来源为连续统计快照，不使用固定值。
- `INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS` 已投影这些 rate 列；manager/engine
  回归通过。证据：
  `reports/compatibility/p1-innodb-buffer-pool-rate-current-continuation1255.txt`。
- page-create 与 LRU I/O 速率仍无权威来源，完整 I_S/P_S 继续为 P1 partial。

### Continuation 1256

- LockManager 新增真实累计事件来源，并接入官方
  `lock_rec_lock_requests`、`lock_rec_lock_created`、
  `lock_rec_lock_removed`、`lock_deadlocks` 四行 `INNODB_METRICS`。
- `module_lock`、精确名和通配符开关，以及 `reset`/`reset_all` 基线均已
  覆盖；锁管理器、InnoDB metrics 全量回归和 XA/binlog/复制筛选回归通过。
  证据：`reports/compatibility/p1-innodb-metrics-lock-lifecycle-current-continuation1256.txt`。
- 完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 注册表、完整非
  Connector/J 客户端矩阵、官方 MySQL XA/binlog/复制/崩溃恢复互操作仍纳入
  全局任务；非 InnoDB 引擎及 repair/conversion 按范围明确排除。

### Continuation 1257

- `INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS` 的
  `NUMBER_PAGES_CREATED` 与 `PAGES_CREATE_RATE` 改为使用成功的实时页分配
  事件和连续快照速率；新增 `INNODB_METRICS.buffer_pages_created`。
- engine/manager 聚焦回归通过，证据：
  `reports/compatibility/p1-innodb-buffer-pool-create-rate-current-continuation1257.txt`。
- page-young、LRU I/O 及完整 I_S/P_S 注册表仍未完成；Connector/J 已属 P0，
  非 Connector/J 全矩阵仍为 P3，官方 MySQL XA/binlog/复制/崩溃恢复互操作
  仍在 P2/P4；非 InnoDB 引擎按范围排除。

### Continuation 1258

- 客户端环境审计确认 Go、PyMySQL、Node/mysql2 可用，但当前环境没有
  `mysql` CLI；这不是连接兼容性通过证据。
- 非 Connector/J 全矩阵继续 P3 partial，`mysql-cli` 继续 unverified；证据：
  `reports/compatibility/client-matrix-diagnostic-continuation1257/client-matrix-20260924-073703.json`。
- 集群端点真实双节点客户端运行还需要受保护认证变量；当前只完成环境审计，
  不宣称 P3 集群客户端场景通过。证据：
  `reports/compatibility/client-cluster-endpoint-diagnostic-continuation1258/client-cluster-endpoint-20260924-074328.json`。

### Continuation 1260

- Buffer Pool old-list 访问和真实 old→young 晋升现在驱动
  `PAGES_MADE_YOUNG`、`PAGES_NOT_MADE_YOUNG`、速率及千次命中比例；不再使用
  generic hit 或固定零值替代。
- 真实 520 页重组的 engine 测试、manager Buffer Pool 回归和 metrics 回归通过。
  证据：`reports/compatibility/p1-innodb-buffer-pool-young-pages-current-continuation1260.txt`。
- LRU I/O、剩余 Buffer Pool 采样字段及完整 I_S/P_S 注册表仍未完成。

### Continuation 1261

- `INNODB_BUFFER_POOL_STATS.PENDING_READS` 现在来源于实时预读队列，
  `PENDING_FLUSH_LIST` 来源于实时 dirty-page 注册表。
- manager 与 engine Buffer Pool 回归通过，证据：
  `reports/compatibility/p1-innodb-buffer-pool-pending-work-current-continuation1261.txt`。
- `PENDING_FLUSH_LRU`、LRU I/O 及剩余采样字段仍缺少权威运行时来源。

### Continuation 1262

- `INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS.UNCOMPRESS_TOTAL` 现在使用
  CompressionManager 的真实累计成功解压页计数；`UNCOMPRESS_CURRENT` 使用
  独立的当前窗口计数，读取视图只消费当前窗口，不清除累计值。
- 首次快照验证 `UNCOMPRESS_TOTAL/UNCOMPRESS_CURRENT=1/1`，下一次快照为
  `1/0`；engine、manager、XA/binlog/GTID/recovery/promotion 回归通过。
  证据：
  `reports/compatibility/p1-innodb-buffer-pool-uncompress-current-continuation1262.txt`。
- `PENDING_FLUSH_LRU`、`LRU_IO_TOTAL`、`LRU_IO_CURRENT` 仍未实现；不能用普通
  page read/write 总数冒充 MySQL 独立的 LRU-policy I/O 统计。

### Continuation 1263

- P1 `INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS` 新增独立的 LRU I/O
  运行时来源：成功的缓冲池页加载和脏页刷盘分别记录到专用累计/当前窗口
  计数器；当前窗口由视图快照原子消费，累计值保留，不再使用普通
  `page_reads/page_writes` 推导。
- manager 专项与 engine 真实 tablespace 视图回归通过；证据：
  `reports/compatibility/p1-innodb-buffer-pool-lru-io-current-continuation1263.txt`。
- `PENDING_FLUSH_LRU` 仍缺少独立 pending-LRU 队列来源；完整 I_S/P_S、P2
  官方 XA/binlog/GTID/复制/崩溃恢复互操作、P3 全量非 Connector/J 客户端
  矩阵仍保持 partial/pending。非 InnoDB 引擎及 repair/conversion 继续
  明确排除。
### Continuation 1264

- P1 `INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS.PENDING_FLUSH_LRU` 现在
  来源于真实的 LRU dirty-page flush queue：dirty page 被淘汰时进入独立
  队列，后台 worker 持久化并从 dirty registry 移除，关闭流程会先排空该
  队列；不再用 dirty page 总数或固定 0 代替。
- 阻塞存储红测观察到 `pending_flush_lru=1`，释放写入后归零且页面内容已
  持久化；manager、engine 和完整 InnoDB 回归通过。证据：
  `reports/compatibility/p1-innodb-buffer-pool-pending-lru-current-continuation1264.txt`。
- 完整 I_S/P_S 的其他字段/组件/权限/生命周期、P2 官方互操作、P3 全量
  客户端矩阵和 P4 fixture 仍保持未完成；非 InnoDB 范围继续排除。

### Continuation 1265

- P1 `INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS.PENDING_DECOMPRESS` 现在
  来源于 CompressionManager 的真实在途解压计数；不再固定返回 0。
- manager 红测/绿测、engine 视图生命周期测试、manager 全量和 engine 全量
  回归均通过；证据：
  `reports/compatibility/p1-innodb-buffer-pool-pending-decompress-current-continuation1265.txt`。
- 这只关闭 `PENDING_DECOMPRESS` 一个字段来源；完整 I_S/P_S、P2 官方
  XA/binlog/GTID/复制/崩溃恢复互操作、P3 全量非 Connector/J 客户端矩阵和
  P4 fixture 仍保持未完成，非 InnoDB 继续排除。

### Continuation 1266

- 修正 `INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS.HIT_RATE` 的尺度：
  manager 保持 `0..1` 比例，I_S 按 InnoDB 约定转换为每千次值，`0.75`
  暴露为 `750`，不再错误返回 `75`。
- 红测、Buffer Pool/I_S/P_S 相关 engine 回归通过；证据：
  `reports/compatibility/p1-innodb-buffer-pool-hit-rate-current-continuation1266.txt`。
- 完整 I_S/P_S 的其他字段/组件/权限/生命周期、P2/P3/P4 仍保持未完成。

### Continuation 1267

- `INFORMATION_SCHEMA.COLUMNS.PRIVILEGES` 不再对所有列固定返回
  `select,insert,update,references`；现在按当前会话的有效表级、全局、列级
  授权投影，支持活动角色和通配列范围。
- 列级 SELECT 红测、权限/角色/I_S 回归通过；证据：
  `reports/compatibility/p1-information-schema-column-privileges-current-continuation1267.txt`。
- 完整 I_S/P_S 权限可见性和运行时语义、P2/P3/P4 仍保持未完成。

### Continuation 1268

- 客户端事务在存储提交后、复制发布前，现在向同一 active journal 写入并
  `fsync` durable `replication_commit` 记录；发布器返回错误并重启时，恢复
  不再把已提交行作为孤儿回滚。
- 重启窗口红测、P2 XA/binlog/GTID/recovery 回归和完整 engine 回归通过；证据：
  `reports/compatibility/p2-client-commit-recovery-current-continuation1268.txt`。
- 这只关闭本地 client commit recovery 窗口；跨 storage/WAL、native binlog、
  GTID、replica applied state 的统一物理提交及官方 MySQL 互操作仍保持未完成。

### Continuation 1269

- P1 `INFORMATION_SCHEMA.INNODB_METRICS` 新增 3 个真实 RedoLogManager
  来源：`log_lsn_current`、`log_lsn_last_checkpoint`、
  `log_lsn_checkpoint_age`；统一由 `innodb_monitor_enable=module_log`
  控制，禁用时不泄露运行时值。
- 指标专项、I_S/P_S 相关回归和完整 engine 回归通过；证据：
  `reports/compatibility/p1-innodb-metrics-redo-lsn-current-continuation1269.txt`。
- 完整 I_S/P_S 组件/字段/权限/生命周期语义、P2/P3/P4 仍保持未完成；
  非 InnoDB 引擎、repair 和 engine conversion 明确排除。

### Continuation 1272

- 完整 release-candidate gate 已结束并判定 `NO-GO`：build、unit、integration、
  scope matrix、P1/P2、go-core、cluster smoke、3 次 crash recovery、并发、
  observability 及 139 个 Connector/J 测试全部通过。
- 仅非 Connector/J client matrix 和 cluster endpoint 认证因环境阻塞失败：
  没有受保护的 `XMYSQL_CLIENT_PASSWORD`，且没有 `mysql.exe`。证据：
  `reports/compatibility/global-audit-current-continuation1273.txt`、
  `reports/compatibility/release-candidate-current-continuation1272/release-candidate.json`。
- 这不是代码门禁失败，也不关闭完整 I_S/P_S、P2 统一物理提交和 P4 官方
  MySQL 互操作；FULLTEXT 及非 InnoDB 范围边界不变。

### Continuation 1273

- `INFORMATION_SCHEMA.INNODB_METRICS` 新增两个真实 Buffer Pool 来源：
  `buffer_data_reads` 使用 `page_reads`，`buffer_data_written` 使用
  `page_writes`；两者均为 `buffer/status_counter`，没有新增伪造计数源。
- 红测、指标专项和 Buffer Pool 相关回归通过；证据：
  `reports/compatibility/p1-innodb-metrics-buffer-data-io-current-continuation1273.txt`。
- MySQL 314 行完整 metrics registry、组件运行时来源、P2/P3/P4 仍未完成；
  非 InnoDB 引擎、repair 和 engine conversion 明确排除。

### Continuation 1271

- 刷新全局证据：`go test -p 1 ./... -count=1 -timeout 45m` 全仓串行回归
  通过（exit 0），当前集群 smoke 也通过。证据：
  `reports/compatibility/global-audit-current-continuation1271.txt`。
- P3 环境诊断确认 Go、PyMySQL、Node.js/mysql2 可用；当前没有
  `mysql.exe`，且受保护的 `XMYSQL_CLIENT_PASSWORD` 未提供，因此不把
  全量客户端矩阵或集群端点认证场景误记为通过。
- 当前矩阵仍为 13 implemented、8 partial、1 unverified、1
  pending_external、1 deferred、2 out_of_scope。完整 I_S/P_S 表/字段/运行时/
  权限语义、storage/WAL/native binlog/GTID/replica-applied/crash recovery
  的统一物理提交点、全量非 Connector/J 矩阵、官方 MySQL XA/binlog/复制/
  崩溃恢复互操作仍未关闭；FULLTEXT deferred，非 InnoDB 引擎/修复/转换
  按范围排除。

### Continuation 1270

- P1 `INFORMATION_SCHEMA.INNODB_METRICS.dml_reads` 现在来源于真实
  `SelectExecutor.rowsExamined`，成功扫描三行表后返回 3，并接入
  `module_dml` 的启停/reset 生命周期。
- 红测、指标/I_S 回归和完整 engine 回归通过；证据：
  `reports/compatibility/p1-innodb-metrics-dml-reads-current-continuation1270.txt`。
- 完整 I_S/P_S 组件/字段/权限/生命周期语义、P2/P3/P4 仍保持未完成；
  非 InnoDB 引擎、repair 和 engine conversion 明确排除。

### Continuation 1274

- `INFORMATION_SCHEMA.INNODB_METRICS.buffer_data_reads` 和
  `buffer_data_written` 按 MySQL 字节计数语义投影为页 I/O 次数乘以实时
  InnoDB 页大小；新增 `os_data_reads`、`os_data_writes`，来源为同一真实
  Buffer Pool 页 I/O 计数。
- 新增 MySQL 8.4 InnoDB 静态监控目录，共 331 个唯一名称，包含显式源码
  条目和 buffer-page/wait 宏展开名称；没有 xmysql 权威来源的条目只以
  `disabled/0` 暴露，动态真实来源优先覆盖，不构造伪造运行值。
- 指标专项、完整 engine、net/replication 及全仓串行回归通过；证据：
  `reports/compatibility/p1-innodb-metrics-catalog-current-continuation1274.txt`。
- 该切片只补齐注册可发现性、Buffer I/O 单位和两个 OS I/O 来源；完整
  I_S/P_S 运行时、生命周期、组件和权限语义，以及 P2/P3/P4 仍未完成。

### Continuation 1275

- `INFORMATION_SCHEMA.INNODB_METRICS.file_num_open_files` 现在来源于已有
  file lifecycle recorder 的实时 `OpenCount` 汇总，投影为
  `file_system/status_counter`；回归覆盖打开和关闭句柄后的基线增量。
- 指标专项、完整 engine 和全仓串行回归通过；证据：
  `reports/compatibility/p1-innodb-metrics-open-files-current-continuation1275.txt`。
- 该切片只关闭一个真实 file-system 指标；完整 I_S/P_S 运行时/组件/权限
  语义、P2/P3/P4 仍未完成。

### Continuation 1276

- `INFORMATION_SCHEMA.INNODB_METRICS.trx_rollbacks` 接入事务完成边界的真实
  rollback 累计计数，并通过同步的 `RuntimeRecorder.TransactionTotals()`
  投影为 `transaction/counter`。
- 红测确认静态目录的零值不会随 rollback 事件增长；engine 指标专项和
  RuntimeRecorder 回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-transaction-rollback-current-continuation1276.txt`。
- 当前完成边界没有可靠的读写分类，因此不把总 commit 计数伪造为
  `trx_rw_commits` 或 `trx_ro_commits`；完整 I_S/P_S、P2/P3/P4 仍未完成。

### Continuation 1277

- `INFORMATION_SCHEMA.INNODB_METRICS.os_log_fsyncs` 接入 RedoLogManager
  group-commit 的真实 `TotalFsyncs`，投影为 `os/status_counter`。
- 红测通过真实 `FlushAsync` 触发 fsync 计数，绿测及后续回归通过；证据：
  `reports/compatibility/p1-innodb-metrics-redo-fsync-current-continuation1277.txt`。
- 该切片只关闭一个 OS/redo 指标；完整 I_S/P_S 运行时/组件/权限语义及
  P2/P3/P4 仍未完成。

### Continuation 1278

- `INFORMATION_SCHEMA.INNODB_METRICS` 新增六个已有执行器锁租约来源的实时指标：
  `metadata_table_handles_opened`、`metadata_table_handles_closed` 和
  `metadata_table_reference_count`，分别覆盖显式 `LOCK TABLES`、语句级锁和事务级锁
  的获取、释放与当前引用数；并以 `lock_table_lock_created`、
  `lock_table_lock_removed`、`lock_table_locks` 投影同一表锁租约生命周期。
- 红测先确认会话中的显式表锁已经存在但静态目录无法反映生命周期；接入
  `RuntimeRecorder` 后，专项测试、完整 engine 和全仓串行回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-metadata-handles-current-continuation1278.txt`。
- 这只关闭六个有真实来源的 metadata/lock 指标；完整 I_S/P_S 表/字段/权限/筛选/运行时统计、
  P2/P3/P4 仍未完成。

### Continuation 1279

- `INFORMATION_SCHEMA.INNODB_METRICS.lock_rec_locks` 现在从 LockManager 的
  实时锁表扫描当前已授予的记录锁数量，不再把当前记录锁数作为静态目录的
  `disabled/0` 行返回。
- 红测先确认已授予一把记录锁但指标仍为 0；接入实时快照后，manager/engine
  聚焦测试、完整 engine 和全仓串行回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-record-lock-current-continuation1279.txt`。
- 这只关闭一个当前记录锁指标；剩余锁等待/超时/授予/释放指标、完整 I_S/P_S
  运行时语义以及 P2/P3/P4 仍未完成。

### Continuation 1280

- `INFORMATION_SCHEMA.INNODB_METRICS.lock_rec_lock_waits` 现在投影
  LockManager 的真实已完成记录锁等待历史，并接入 `module_lock` 的默认启用、禁用和
  reset baseline 处理。
- 红测先确认真实冲突/释放已经产生一次完成等待但指标仍为 0；接入来源后，manager/engine
  聚焦测试、完整 engine 和全仓串行回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-record-lock-waits-current-continuation1280.txt`。
- 这只关闭一个已完成等待指标；锁超时、授予/释放尝试计数、完整 I_S/P_S、P2/P3/P4
  仍未完成。

### Continuation 1281

- `INFORMATION_SCHEMA.INNODB_METRICS.lock_rec_grant_attempts` 和
  `lock_rec_release_attempts` 现在分别接入 LockManager 的真实记录锁授予尝试、等待锁
  再授予以及匹配释放操作来源，并支持 `module_lock` 的启用/禁用/reset baseline。
- 红测先补出缺失来源字段，再发现等待锁被释放后重新授予未计数；修正后 manager/engine
  聚焦测试和完整 engine 回归通过，全仓串行回归输出无失败包。证据：
  `reports/compatibility/p1-innodb-metrics-record-lock-attempts-current-continuation1281.txt`。
- 这只关闭两个记录锁尝试指标；锁超时、table-lock waits、完整 I_S/P_S、P2/P3/P4
  仍未完成。

### Continuation 1282

- `INFORMATION_SCHEMA.INNODB_METRICS.lock_table_lock_waits` 现在接入
  `tableDDLCoordinator` 的真实完成 metadata/table-lock wait history，并使用独立的
  enable/disable/reset baseline。
- 红测先用真实 owner/waiter/release 流程确认静态值为 0；接入后专项测试、完整 engine
  和全仓串行回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-table-lock-waits-current-continuation1282.txt`。
- 这只关闭一个 table-lock wait 指标；锁超时、完整 I_S/P_S、P2/P3/P4 仍未完成。

### Continuation 1283

- `INFORMATION_SCHEMA.INNODB_METRICS.lock_timeouts` 现在接入真实的
  `context.DeadlineExceeded` table-lock 超时来源；普通 `ErrLockConflict` 不计入，且
  支持独立的启用/禁用/reset baseline。
- 红测先用真实 table-lock deadline 确认缺少运行时来源；接入后专项测试和全仓串行回归
  通过，engine/manager/net/replication 均无失败。证据：
  `reports/compatibility/p1-innodb-metrics-lock-timeouts-current-continuation1283.txt`。
- 这只关闭 table-lock deadline 来源；record-lock 原生等待超时、完整 I_S/P_S、P2/P3/P4
  仍未完成。

### Continuation 1284

- `INFORMATION_SCHEMA.INNODB_METRICS.trx_rw_commits` 和 `trx_ro_commits` 现在
  接入真实事务完成边界的读写/只读提交计数，并支持 `module_trx` 的启用、禁用
  和 reset baseline。
- 专项测试、完整 engine 回归和全仓串行回归通过；证据：
  `reports/compatibility/p1-innodb-metrics-transaction-commit-modes-current-continuation1284.txt`。
- `trx_nl_ro_commits` 暂不宣称完成，原因是当前没有权威的 MySQL 非锁定只读分类来源；
  完整 I_S/P_S、P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

### Continuation 1285

- `INFORMATION_SCHEMA.INNODB_METRICS.trx_commits_insert_update` 现在接入真实
  事务 DML 变更记录：包含 INSERT 或 UPDATE 后提交的事务递增，DELETE-only 和
  READ ONLY 提交不递增。
- 专项测试、完整 engine 回归和全仓串行回归通过；证据：
  `reports/compatibility/p1-innodb-metrics-transaction-insert-update-commits-current-continuation1285.txt`。
- 该切片只关闭一个 source-backed P1 指标；`trx_nl_ro_commits`、完整 I_S/P_S、
  P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

### Continuation 1286

- `INFORMATION_SCHEMA.INNODB_METRICS.trx_rollbacks_savepoint` 现在接入成功的
  `ROLLBACK TO SAVEPOINT` 事务语句边界，并支持 `module_trx` 的启用、禁用和
  reset baseline。
- 专项测试、完整 engine 回归和全仓串行回归通过；证据：
  `reports/compatibility/p1-innodb-metrics-savepoint-rollbacks-current-continuation1286.txt`。
- 该切片只关闭一个 source-backed P1 指标；`trx_nl_ro_commits`、完整 I_S/P_S、
  P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

### Continuation 1287

- `INFORMATION_SCHEMA.INNODB_METRICS.trx_nl_ro_commits` 现在接入真实的表查询自动
  提交边界：成功的非锁定表查询递增；常量查询、元数据查询和显式只读事务不递增。
- 该指标支持 `module_trx` 的启用、禁用和 reset baseline；专项测试、完整 engine
  回归和全仓串行回归通过，证据见
  `reports/compatibility/p1-innodb-metrics-nonlocking-autocommit-readonly-current-continuation1287.txt`。
- 完整 I_S/P_S、P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

### Continuation 1288

- `INFORMATION_SCHEMA.INNODB_METRICS.trx_allocations` 现在接入
  `TransactionManager.Begin` 的真实逻辑事务对象分配计数，并支持
  `module_trx` 的启用、禁用和 reset baseline。
- 红测先确认事务已经分配但静态目录值仍为 0；接入后事务指标聚焦测试、完整
  engine 回归和全仓串行回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-transaction-allocations-current-continuation1288.txt`。
- 该切片只关闭一个 source-backed P1 指标；完整 I_S/P_S、P2/P3/P4 仍未完成，
  FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

### Continuation 1319

- P1 实现 Performance Schema statement、stage、transaction、wait history 和
  history_long 表的 `TRUNCATE TABLE` 生命周期。
- statement/stage 使用独立历史缓冲；transaction history 使用 long-history
  清理和会话代际失效；record-lock wait 支持 history/history_long 分离清理。
- 红测/绿测、Performance Schema 全套、受影响包和
  `go test -p 1 ./... -count=1 -timeout 35m` 全部通过。证据见：
  `reports/compatibility/p1-performance-schema-history-truncate-current-continuation1319.txt`。
- 该切片只关闭 history 表清理语义；完整 I_S/P_S、原生 XA/binlog/复制/崩溃
  互操作、全量非 Connector/J 客户端和官方 MySQL fixture 仍未完成。FULLTEXT
  继续 deferred，非 InnoDB 继续 out of scope。

### Continuation 1317

- P1 实现 `performance_schema.socket_summary_by_event_name` 和
  `performance_schema.socket_summary_by_instance` 的 truncate 生命周期：保留
  socket/线程行身份，清零读写次数、计时和字节数；reset 后 SQL 投影不再用活动
  连接数回填 `COUNT_STAR`。
- 官方依据为 [MySQL socket summary tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-socket-summary-tables.html)。
  红测、全部 Performance Schema 测试、受影响包和 `go test -p 1 ./...`
  全部通过，证据见
  `reports/compatibility/p1-performance-schema-socket-summary-truncate-current-continuation1317.txt`。
- 该切片只关闭 socket summary 的生命周期语义；完整 I_S/P_S 表/字段/运行时/权限、
  全量非 Connector/J 客户端矩阵、P2 原生 XA/binlog/复制/崩溃互操作和 P4 官方
  fixture 仍未完成。FULLTEXT 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1310

- P1 实现 `performance_schema.events_transactions_summary_global_by_event_name` 的
  `TRUNCATE TABLE` 生命周期：全局事务汇总行及依赖的账户、主机、用户、线程投影保留
  身份并清零，后续新事务可继续累加且不会把 reset 占位行计入 `COUNT_STAR`。
- 红测、P_S 专项、受影响包和全仓串行回归全部通过。证据见：
  `reports/compatibility/p1-performance-schema-transaction-summary-truncate-current-continuation1310.txt`。
- 完整 P_S 表/字段/运行时/权限语义、P2/P3/P4 仍未完成；FULLTEXT 继续 deferred，
  非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1313

- P1 实现 `performance_schema.table_io_waits_summary_by_table` 和
  `table_io_waits_summary_by_index_usage` 的独立汇总生命周期：表级截断保留
  表/索引身份并清零计数，同时隐式清空索引使用汇总；索引级截断只清空索引使用
  汇总；statement summary 不受影响。
- 新增表 I/O 红测，受影响包与全仓串行回归全部通过。证据见：
  `reports/compatibility/p1-performance-schema-table-io-summary-truncate-current-continuation1313.txt`。
- 完整 P_S 表/字段/运行时/权限语义、P2/P3/P4 仍未完成；FULLTEXT 继续 deferred，
  非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1314

- P1 实现 `performance_schema.table_lock_waits_summary_by_table` 的独立汇总
  生命周期：截断保留表身份并清零 table-lock 汇总，`events_waits` 汇总、bounded
  history 和 live/current wait projection 不受影响。
- 新增隔离红测，P_S 专项、受影响包和全仓串行回归全部通过。证据见：
  `reports/compatibility/p1-performance-schema-table-lock-summary-truncate-current-continuation1314.txt`。
- 完整 P_S 表/字段/运行时/权限语义、P2/P3/P4 仍未完成；FULLTEXT 继续 deferred，
  非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1315

- P1 实现 `performance_schema.memory_summary_global_by_event_name` 的基线重置：
  保留 memory event 身份和当前占用，不释放内存；分配/释放计数、字节累计重新设基线，
  高低水位设为当前使用量。
- 新增 memory summary 红测，P_S 专项、受影响包和全仓串行回归全部通过。证据见：
  `reports/compatibility/p1-performance-schema-memory-summary-truncate-current-continuation1315.txt`。
- 完整 P_S 表/字段/运行时/权限语义、P2/P3/P4 仍未完成；FULLTEXT 继续 deferred，
  非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1316

- P1 实现 `performance_schema.file_summary_by_event_name` 和
  `file_summary_by_instance` 的截断生命周期：保留文件身份、实例 ID 和打开句柄状态，
  清零读写/杂项计数与计时。
- 新增 file summary 红测，P_S 专项和全仓串行回归全部通过。证据见：
  `reports/compatibility/p1-performance-schema-file-summary-truncate-current-continuation1316.txt`。
- 完整 P_S 表/字段/运行时/权限语义、P2/P3/P4 仍未完成；FULLTEXT 继续 deferred，
  非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1312

- P1 实现 `performance_schema.events_waits_summary_global_by_event_name` 的
  `TRUNCATE TABLE` 生命周期：record-lock/metadata-lock 的 lifetime summary 清空，
  global、thread、instance 及身份派生行保留并归零，history/live wait state 保持独立。
- 红测、P_S 专项、受影响包和全仓串行回归全部通过。证据见：
  `reports/compatibility/p1-performance-schema-wait-summary-truncate-current-continuation1312.txt`。
- 完整 P_S 表/字段/运行时/权限语义、P2/P3/P4 仍未完成；FULLTEXT 继续 deferred，
  非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1311

- P1 实现 `performance_schema.events_stages_summary_global_by_event_name` 的
  `TRUNCATE TABLE` 生命周期：stage/sql/execute 独立于 statement/digest summary，
  保留身份行并清零计数和计时，后续语句继续从零累加。
- 红测、P_S 专项、受影响包和全仓串行回归全部通过。证据见：
  `reports/compatibility/p1-performance-schema-stage-summary-truncate-current-continuation1311.txt`。
- 完整 P_S 表/字段/运行时/权限语义、P2/P3/P4 仍未完成；FULLTEXT 继续 deferred，
  非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1293

- `INFORMATION_SCHEMA.INNODB_METRICS.trx_rollback_active` 现在接入
  `TransactionManager` 的真实活动回滚计数，覆盖完整事务回滚和回滚到保存点，
  并支持 `module_trx` 目标路由。
- 完整回滚在 Undo 执行期间释放事务管理器写锁，同时以进行中集合拒绝同一事务的
  并发 COMMIT/重复回滚；这样 INFORMATION_SCHEMA 可以在回滚进行中读取该指标。
- 红测先确认指标仍为 disabled；接入后管理器、指标专项、完整 engine 和全仓串行
  回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-rollback-active-current-continuation1293.txt`。
- 该切片只关闭一个 source-backed P1 指标；完整 I_S/P_S、P2/P3/P4 仍未完成，
  FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

### Continuation 1294

- P2 收口复制状态文件的持久化边界：source/replica/runtime 共用的
  `writeReplicationFileAtomic` 现在先写临时文件并 `File.Sync`，关闭后再原子
  rename；注入同步失败时，目标文件和临时文件都不会被安装或残留。
- 复制专项、P2 选择性回归和全仓串行回归均通过，证据：
  `reports/compatibility/p2-replication-state-fsync-current-continuation1294.txt`。
- 这只关闭状态文件替换前的 fsync 边界；storage pages/WAL、native binlog、GTID、
  replica-applied state 与崩溃恢复的一致物理提交点仍是 P2 partial，官方 MySQL
  互操作仍是 P4 pending_external。完整 I_S/P_S 与全量非 Connector/J 矩阵继续纳入
  全局任务；FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1295

- P2 补齐逻辑 binlog 已持久化但 native binlog append 失败时的进程内重试边界：
  相同 commit key 重试会从逻辑恢复流重建 native 文件，再幂等返回，不会重复追加
  第二个逻辑事务。
- 新增 native append 故障注入红绿测试；复制包、受影响的 engine/net/replication
  选择性门禁和全仓串行回归均通过，证据：
  `reports/compatibility/p2-native-binlog-retry-recovery-current-continuation1295.txt`。
- 这只覆盖普通 keyed transaction 的恢复窗口；XA terminal retry、统一的
  storage/WAL/binlog/GTID/replica-applied/crash physical commit point 和官方 MySQL
  互操作仍未关闭。完整 I_S/P_S 与全量非 Connector/J 矩阵继续纳入全局任务；
  FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1296

- P2 将逻辑/native retry 恢复边界扩展到 XA terminal：已同步的 XA COMMIT 或
  XA ROLLBACK 在 native append 失败后，按同一 terminal 重试会从逻辑恢复流重建
  native 文件，避免重复追加 XA terminal。
- 红测先复现 XA terminal 从 1 条变成 2 条；修复后普通/XA retry 专项、复制包、
  受影响的 engine/net/replication 选择性门禁和全仓串行回归通过，证据：
  `reports/compatibility/p2-xa-terminal-retry-recovery-current-continuation1296.txt`。
- 统一 storage/WAL/binlog/GTID/replica-applied/crash physical commit point 与官方
  MySQL 互操作仍未关闭。完整 I_S/P_S、全量非 Connector/J 矩阵继续纳入全局任务；
  FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1292

### Continuation 1297

- P1 补齐 `performance_schema.events_waits_current`、`events_waits_history` 和
  `events_waits_history_long` 的 INFORMATION_SCHEMA 元数据：THREAD/EVENT 标识为
  非空 unsigned BIGINT，END_EVENT、计时、SPINS、字节数和 FLAGS 为可空 unsigned
  BIGINT，SOURCE/对象字段/OPERATION 使用精确 VARCHAR 长度。
- 红测先确认上述列仍使用错误的 NULLability 或 `VARCHAR(255)` 兜底；修正后
  `TestInformationSchemaPerformanceSchemaWaitEventsUseNativeMetadata`、完整
  I_S/P_S 专项、engine 包、受影响包和全仓串行回归通过。证据：
  `reports/compatibility/p1-performance-schema-wait-events-metadata-current-continuation1297.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；完整运行时/组件/权限
  语义、P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换
  继续 out_of_scope。

### Continuation 1298

- P1 补齐 `performance_schema.events_stages_current`、`events_stages_history` 和
  `events_stages_history_long` 的 INFORMATION_SCHEMA 元数据：THREAD/EVENT 标识
  为非空 unsigned BIGINT，END_EVENT、计时、WORK、嵌套事件 ID 为可空 unsigned
  BIGINT，SOURCE 与 NESTING_EVENT_TYPE 使用精确 VARCHAR 长度。
- 红测先确认上述列仍使用错误的 NULLability、signed BIGINT 或 `VARCHAR(255)`
  兜底；修正后 `TestInformationSchemaPerformanceSchemaStageEventsUseNativeMetadata`、
  完整 I_S/P_S 专项、engine 包、受影响包和全仓串行回归通过。证据：
  `reports/compatibility/p1-performance-schema-stage-events-metadata-current-continuation1298.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；完整运行时/组件/权限
  语义、P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换
  继续 out_of_scope。

### Continuation 1299

- P1 对齐 `performance_schema.events_transactions_current`、`events_transactions_history`
  和 `events_transactions_history_long` 的 MySQL 8.4 官方 24 列形状，补齐
  `XA_STATE`、`SOURCE`，将 `XID_FORMAT` 正名为 `XID_FORMAT_ID`，并补齐事务事件
  字段的精确类型、长度、unsigned 与 NULLability 元数据。
- `XA_STATE` 来自真实 session XA 状态机；旧 `XID_FORMAT` 仅保留为显式查询别名，
  不进入 `SELECT *` 或 `INFORMATION_SCHEMA.COLUMNS`。
- 红测、事务/Performance Schema 专项、engine 包、受影响包和全仓串行回归通过。证据：
  `reports/compatibility/p1-performance-schema-transaction-events-metadata-current-continuation1299.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；完整运行时/组件/权限
  语义、P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换
  继续 out_of_scope。

### Continuation 1300

- P1 对齐 `performance_schema.events_statements_current`、`events_statements_history`
  和 `events_statements_history_long` 的 MySQL 8.4 语句事件元数据，覆盖标识、计时、
  SQL/digest 文本、错误字段、语句计数、嵌套事件、CPU/内存和执行引擎字段。
- 将官方字段名 `NESTING_EVENT_LEVEL` 纳入注册表和运行时投影，移除 `SELECT *` 中
  的仓库自定义 `NESTING_LEVEL`。
- 红测、I_S/P_S 专项、engine 包、受影响包和全仓串行回归通过。证据：
  `reports/compatibility/p1-performance-schema-statement-events-metadata-current-continuation1300.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；完整运行时/组件/权限
  语义、P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换
  继续 out_of_scope。

### Continuation 1301

- P1 对齐 `performance_schema.events_statements_summary_by_digest` 的采样字段元数据：
  `QUERY_SAMPLE_SEEN` 现在是可空 `TIMESTAMP(6)`，`QUERY_SAMPLE_TIMER_WAIT` 现在是
  可空 `BIGINT UNSIGNED`，不再使用 `VARCHAR(255)` 和有符号 `BIGINT` 通用兜底。
- 红测、P1 I_S/P_S 专项、engine/manager/net/replication 受影响包和全仓串行回归通过，
  证据见 `reports/compatibility/p1-performance-schema-digest-sampling-metadata-current-continuation1301.txt`。
- 该切片不改变 P1 complete-table-registry 的 partial 状态；digest 容量/淘汰、完整采样
  生命周期、组件运行时、P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/
  修复/转换继续 out_of_scope。

### Continuation 1302

- P1 对齐 `performance_schema.events_statements_histogram_by_digest` 和
  `events_statements_histogram_global` 的 `BUCKET_QUANTILE` 元数据：现在返回非空
  `DOUBLE`，不再使用 `VARCHAR(255)` 可空通用兜底。官方依据为
  [Statement Histogram Summary Tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-statement-histogram-summary-tables.html)。
- 红测先确认两张表的字段均为 `VARCHAR(255)`/`YES`；修正后聚焦测试、P1
  I_S/P_S 专项、engine/manager/net/replication 受影响包和 `go test -p 1 ./...`
  全部通过。证据见
  `reports/compatibility/p1-performance-schema-histogram-quantile-metadata-current-continuation1302.txt`。
- 该切片只关闭直方图分位字段的元数据精度缺口；完整 P_S 注册、直方图运行时/容量/生命周期、
  组件/权限语义、P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换
  继续 out_of_scope。

### Continuation 1303

- P1 对齐两张 statement histogram 表的默认 `SELECT *` 形状：
  `events_statements_histogram_by_digest` 为官方 9 列，
  `events_statements_histogram_global` 为官方 6 列；`COUNT_BUCKET_AND_UPPER` 和
  `COUNT_STAR` 不再进入默认注册表，但内部显式列投影值仍保留。
- 红测先确认 digest/global 实际返回 11/8 列；修正后聚焦 histogram、P1 I_S/P_S 专项、
  engine/manager/net/replication 受影响包和 `go test -p 1 ./...` 全部通过。官方依据为
  [Statement Histogram Summary Tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-statement-histogram-summary-tables.html)，
  证据见 `reports/compatibility/p1-performance-schema-histogram-column-shape-current-continuation1303.txt`。
- 该切片只关闭默认列形状缺口；完整 P_S 注册、直方图运行时/容量/生命周期、组件/权限语义、
  P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1304

- P1 对齐两张 statement histogram 表的官方字段元数据：`DIGEST` 为可空
  `VARCHAR(64)`，`BUCKET_NUMBER` 为非空 `INT UNSIGNED`，计时/计数列为非空
  `BIGINT UNSIGNED`，`BUCKET_QUANTILE` 为非空 `DOUBLE(7,6)`；依据为 MySQL 8.4
  [直方图表文档](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-statement-histogram-summary-tables.html)
  和官方 8.4 表定义源码。
- 红测先暴露 `DIGEST` 的 CHAR/VARCHAR 差异及其它字段的通用可空元数据；修正后
  聚焦 histogram、P1 I_S/P_S 专项、engine/manager/net/replication 受影响包和
  `go test -p 1 ./...` 全部通过。证据见
  `reports/compatibility/p1-performance-schema-histogram-column-metadata-current-continuation1304.txt`。
- 该切片关闭该直方图家族的官方元数据契约；完整 P_S 注册、运行时/容量/生命周期、组件/权限
  语义、P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1305

- P1 支持两张 statement histogram 表的直接 `TRUNCATE TABLE`，并将 global 与 by-digest
  的运行时样本分离，保证一张表清零不会误清另一张表。官方依据为
  [Statement Histogram Summary Tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-statement-histogram-summary-tables.html)。
- 红测先确认该 DDL 被错误送入普通 InnoDB 路径并因 `performance_schema` 保留库名失败；
  修正后 histogram 聚焦测试、P1 I_S/P_S 专项、受影响包和 `go test -p 1 ./...` 全部通过。
  证据见 `reports/compatibility/p1-performance-schema-histogram-truncate-current-continuation1305.txt`。
- 该切片只关闭直方图直接清零语义；摘要表隐式 reset、完整 P_S 生命周期/组件/权限语义、
  P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1306

- P1 对齐 `performance_schema.events_statements_summary_by_digest.DIGEST` 的官方元数据：
  现在为可空 `VARCHAR(64)`；红测确认旧通用分支返回了错误的 `CHAR(64)`。
- 修正后 digest/statement/histogram 元数据测试、P1 I_S/P_S 专项、受影响包和
  `go test -p 1 ./...` 全部通过。官方依据为
  [Statement Summary Tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-statement-summary-tables.html)，
  证据见 `reports/compatibility/p1-performance-schema-digest-summary-metadata-current-continuation1306.txt`。
- 该切片只关闭一个摘要表字段的精度缺口；完整 P_S 字段/运行时/组件/权限语义、P2/P3/P4
  仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1307

- P1 对齐 `performance_schema.events_statements_summary_by_digest` 的时间与采样字段元数据：
  `FIRST_SEEN`、`LAST_SEEN`、`QUERY_SAMPLE_SEEN` 为 `TIMESTAMP(6) NOT NULL`，
  `QUERY_SAMPLE_TIMER_WAIT` 为 `BIGINT UNSIGNED NOT NULL`；此前摘要时间字段还会落入
  通用 `VARCHAR(255)` fallback。
- 红测、P1 I_S/P_S 专项、metrics/engine/manager/net/replication 受影响包和全仓串行回归均通过。
  证据见 `reports/compatibility/p1-performance-schema-digest-summary-time-metadata-current-continuation1307.txt`。
- 该切片只关闭摘要表字段元数据缺口；完整 P_S 字段/运行时/组件/权限语义、P2/P3/P4
  仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1308

- P1 将 `events_statements_summary_by_digest` 从普通 statement summary 的临时聚合中拆出，
  建立独立的 schema/digest 聚合状态；global statement summary 保持独立。
- 实现摘要表截断联动：digest 摘要截断会删除 digest 行并清空 digest histogram，但保留
  global 摘要；global 摘要截断会清零普通摘要并清空 global histogram，但保留 digest 摘要
  和 digest histogram。红测、P1 I_S/P_S 专项、metrics/engine/manager/net/replication
  受影响包和全仓串行回归均通过。证据见
  `reports/compatibility/p1-performance-schema-summary-truncate-isolation-current-continuation1308.txt`。
- 该切片只关闭摘要表截断隔离语义；完整 P_S 字段/运行时/组件/权限语义、P2/P3/P4
  仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1309

- P1 实现 `performance_schema.events_statements_summary_by_program` 的 `TRUNCATE TABLE`：
  保留 program 行身份，清零计数、计时、statement 子统计、错误、告警和行计数，并清理
  program history fallback。
- 红测、P1 I_S/P_S 专项、metrics/engine/manager/net/replication 受影响包和全仓串行回归均通过。
  证据见 `reports/compatibility/p1-performance-schema-program-summary-truncate-current-continuation1309.txt`。
- 该切片只关闭 program summary 的截断生命周期；完整 P_S 字段/运行时/组件/权限语义、P2/P3/P4
  仍未完成，FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1292

- `INFORMATION_SCHEMA.INNODB_METRICS.os_log_pending_fsyncs` 现在接入
  redo manager 对异步 `FlushAsync` 请求的原子 pending 计数，并支持
  `module_os` 目标路由。
- 红测先确认受控 group commit 窗口内指标行仍为 disabled；接入后 manager、
  指标专项、完整 engine 和全仓串行回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-pending-redo-fsyncs-current-continuation1292.txt`。
- 该切片只关闭一个 source-backed P1 指标；完整 I_S/P_S、P2/P3/P4 仍未完成，
  FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

### Continuation 1291

- `INFORMATION_SCHEMA.INNODB_METRICS.os_log_pending_writes` 现在接入
  `RedoLogStats.BufferedLogs` 的真实待写 redo 记录数，并支持 `module_os`
  的启用、禁用和 reset target routing。
- 红测先确认新增 redo 记录后指标行仍为 disabled；接入后指标专项测试、完整
  engine 回归和全仓串行回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-pending-redo-writes-current-continuation1291.txt`。
- `os_log_pending_fsyncs` 仍缺少直接 fsync 请求来源；完整 I_S/P_S、P2/P3/P4
  仍未完成，FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

### Continuation 1290

- `INFORMATION_SCHEMA.INNODB_METRICS.trx_undo_slots_used` 现在接入
  `UndoLogManager.GetStats().ActiveTxns` 的真实 undo slot 使用来源，并支持
  `module_trx` 的启用、禁用和 reset baseline。
- 红测先确认真实 undo 日志写入后投影值仍为 0；接入后指标专项测试、完整
  engine 回归和全仓串行回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-undo-slots-current-continuation1290.txt`。
- `trx_rollback_active` 和 `trx_rseg_history_len` 暂无同等权威的本地来源；完整
  I_S/P_S、P2/P3/P4 仍未完成，FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

### Continuation 1289

- `INFORMATION_SCHEMA.INNODB_METRICS.os_log_bytes_written` 现在接入
  `RedoLogManager.GetFileSnapshot().SizeInBytes` 的真实 redo 日志字节来源，并支持
  `module_os` 的启用、禁用和 reset baseline。
- 红测先确认真实 redo append/flush 后投影值仍为 0；接入后指标专项测试、完整
  engine 回归和全仓串行回归通过。证据：
  `reports/compatibility/p1-innodb-metrics-redo-log-bytes-current-continuation1289.txt`。

### Continuation 1318

- P1 为 `performance_schema.objects_summary_global_by_type` 增加独立的已完成
  对象等待汇总来源：同一对象的多次等待聚合为一行，并补齐该表的
  `TRUNCATE TABLE` 生命周期，保留对象行身份、清零计数和计时。
- 官方依据为 [MySQL object summary global-by-type table](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-objects-summary-global-by-type-table.html)。
  聚合/截断红测、全部 Performance Schema 测试、受影响包和
  `go test -p 1 ./...` 全部通过，证据见
  `reports/compatibility/p1-performance-schema-object-summary-current-continuation1318.txt`。
- 该切片只关闭对象汇总的一组生命周期语义；完整 I_S/P_S 表/字段/运行时/权限、
  全量非 Connector/J 客户端矩阵、P2 原生 XA/binlog/复制/崩溃互操作和 P4 官方
  fixture 仍未完成。FULLTEXT 继续 deferred，非 InnoDB 继续 out_of_scope。
- 该切片只关闭一个 source-backed P1 指标；完整 I_S/P_S、P2/P3/P4 仍未完成，
  FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。
### Continuation 1320

- P1 将元数据锁等待分别接入 `events_waits_history` 与
  `events_waits_history_long` 的独立缓冲区、查询和截断生命周期；截断其中一张
  history 表不再清空另一张表。
- 红测、全部 Performance Schema 测试、metrics/manager/engine/net/replication
  受影响包和全仓串行回归均通过。证据见
  `reports/compatibility/p1-performance-schema-mdl-history-truncate-current-continuation1320.txt`。
- 该切片不改变完整 I_S/P_S 表/字段/运行时/组件/权限语义的 partial 状态；P2
  原生 XA/binlog/复制/崩溃互操作、P3 全量非 Connector-J 客户端矩阵和 P4 官方
  MySQL fixture 仍未完成。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续
  out_of_scope。
### Continuation 1321

- P1 将五类 Performance Schema 错误汇总表拆为独立的 global/account/host/user/thread
  聚合源；account、host、user、thread 单表截断只重置自身，global 截断按官方规则
  联动重置派生维度。
- 红测、错误汇总专项、全部 Performance Schema、metrics/manager/engine/net/replication
  受影响包和全仓串行回归均通过。官方依据为
  [MySQL 8.4 Error Summary Tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-error-summary-tables.html)，
  证据见 `reports/compatibility/p1-performance-schema-error-summary-dimension-truncate-current-continuation1321.txt`。
- 该切片不改变完整 I_S/P_S 表/字段/运行时/组件/权限语义的 partial 状态；P2 原生
  XA/binlog/复制/崩溃互操作、P3 全量非 Connector-J 客户端矩阵和 P4 官方 fixture
  仍未完成。FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1322

- P1 在 `performance_schema.setup_instruments` 暴露 `error` instrument，默认
  `ENABLED='YES'`、`TIMED='NO'`。
- 执行器错误汇总采集现在遵守该 instrument 以及全局/线程 instrumentation gate；关闭
  instrument 不再产生新的错误汇总事件，重新开启后恢复采集。
- 红测、全部 Performance Schema、受影响包和全仓串行回归均通过。证据见
  `reports/compatibility/p1-performance-schema-error-instrument-control-current-continuation1322.txt`。
- 完整 I_S/P_S 运行时、组件、权限和连接生命周期语义；P2 原生
  XA/binlog/复制/崩溃互操作；P3 全量非 Connector-J 客户端矩阵；以及官方 MySQL
  fixture 仍未完成。FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

### Continuation 1323

- P1 在 `performance_schema.setup_instruments` 暴露真实 SQL 内存 instrument
  `memory/sql/THD::main_mem_root`；默认 `ENABLED='YES'`、`TIMED=NULL`，符合 MySQL
  内存 instrument 不支持计时的语义。
- 执行器和引擎查询路径都遵守该 instrument 以及全局/线程 instrumentation gate；关闭后
  不再记录新的内存汇总事件，重新开启后恢复采集。
- 红测、全部 Performance Schema、受影响包和全仓串行回归均通过。证据见
  `reports/compatibility/p1-performance-schema-memory-instrument-control-current-continuation1323.txt`。
- 其他内存 instrument、完整 I_S/P_S 运行时/组件/权限/连接生命周期语义、P2 原生
  XA/binlog/复制/崩溃互操作、P3 全量非 Connector-J 客户端矩阵和官方 MySQL fixture
  仍未完成。FULLTEXT 继续 deferred，非 InnoDB 继续 out of scope。

### Continuation 1324

- P1 为真实文件/socket recorder 接入 `setup_instruments` 控制，新增
  `wait/io/file/innodb/innodb_data_file`、`wait/io/file/innodb/innodb_log_file`、
  `wait/io/file/sql/FRM`、`wait/io/file/sql/file` 和
  `wait/io/socket/sql/client_connection` 的 `ENABLED/TIMED` 语义。
- `ENABLED='NO'` 不再记录新的底层文件/socket 事件；`TIMED='NO'` 保留计数和字节数，
  但不累计 timer。红测、metrics/net/engine 受影响包和全仓串行回归通过。证据：
  `reports/compatibility/p1-performance-schema-file-socket-instrument-control-current-continuation1324.txt`。
- 完整 I_S/P_S 运行时/组件/权限/连接生命周期语义、P2 原生 XA/binlog/复制/崩溃互操作、
  P3 全量非 Connector-J 客户端矩阵和官方 MySQL fixture 仍未完成；FULLTEXT 继续 deferred，
  非 InnoDB 继续 out of scope。

### Continuation 1325

- P2 修复 `Replica.ReplicateNativeFrom` 的物理 relay 持久化缺口，使 native pull
  路径与 `ApplyNative` 共用 `applyWithPreparedXAAndNativeRelay` 边界。
- native XA PREPARE 的物理帧和 prepared-XA 状态现在都能在 `NewReplica` 重启后保留；
  replication、net、engine 的 native/binlog/GTID/XA/recovery/promotion 测试通过。证据：
  `reports/compatibility/p2-native-replica-relay-persistence-current-continuation1325.txt`。
- storage/WAL、native binlog、GTID、replica-applied state 的统一物理提交协议及官方
  MySQL fixture 仍未完成；P3 客户端矩阵、FULLTEXT 和非 InnoDB 范围边界不变。

### Continuation 1326

- P2 修复复制 replay 的事务上下文隔离：`autocommit=0` 不再把同一个共享存储事务
  同时登记为 client transaction，复制提交不会提前返回。
- 覆盖存储已提交但后续脏页 flush 失败的恢复窗口：返回错误前先持久化并同步
  `replication_commit` 记录，避免恢复阶段误回滚已提交页面。新增红测转绿，复制/事务/
  恢复相关回归和全仓串行回归通过。证据：
  `reports/compatibility/p2-replication-storage-commit-record-current-continuation1326.txt`。
- 该切片仍不等于统一 storage/WAL、native binlog、GTID、relay/applied state 的单一
  物理提交协议，也不等于官方 MySQL 互操作；P3 全量客户端矩阵、FULLTEXT 和非
  InnoDB 范围边界不变。

### Continuation 1327

- P1 补齐 `mysql.role_edges` 和 `mysql.default_roles` 的 source-backed 查询投影。
  角色授权关系、WITH ADMIN OPTION 和默认角色此前已经写入账号兼容层，但两张
  官方授权表没有专用 SELECT handler。
- 新 handler 将 `Roles`、`RoleAdminOptions`、`DefaultRoles` 投影为 MySQL 8.4
  的列形状，支持 SELECT 列投影、授权表字段过滤和稳定排序。红测先确认
  `mysql.role_edges` 报表不存在，修复后聚焦角色表测试与账号/权限/元数据回归通过。
  证据见 `reports/compatibility/p1-role-grant-tables-current-continuation1327.txt`。
- 该切片只补齐两张授权表的读取可见性；role grantor provenance、完整 I_S/P_S
  组件/runtime/字段精度/权限生命周期、P2 原生 XA/binlog/复制/崩溃互操作、P3
  全量非 Connector/J 矩阵和 P4 官方 fixture 仍未完成。FULLTEXT 继续 deferred，
  非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1328

- P1 补齐新授权的 grantor provenance：`persistedAccount` 现在保留表级、列级、
  routine 级和角色边的授权者；`ROLE_TABLE_GRANTS`、`ROLE_COLUMN_GRANTS`、
  `ROLE_ROUTINE_GRANTS` 优先返回真实 `GRANTOR/GRANTOR_HOST`。
- 红测先以非 root 账号执行 `GRANT SELECT ... TO role` 并观察到视图错误返回
  `root@localhost`；修复后聚焦测试和账号/角色/I_S 回归通过。证据见
  `reports/compatibility/p1-role-grantor-provenance-current-continuation1328.txt`。
- 随后全仓串行回归 `go test -p 1 ./... -count=1 -timeout 45m` 通过，engine
  用时 228.558s。
- 旧账号文件缺少 provenance 时继续兼容性回退到 `root@localhost`；完整 I_S/P_S
  组件/runtime/字段精度/权限生命周期、P2/P3/P4 仍未完成。FULLTEXT 继续 deferred，
  非 InnoDB 引擎/修复/转换继续 out_of_scope。

### Continuation 1329

- P2 补齐 native pull replica 的 source file/position 持久化。此前 GTID、物理
  relay 和 prepared-XA 已能恢复，但 `LastSourcePosition` 只存在于内存；现在
  `NewReplica` 重启后恢复 source file/position，并可从该位置继续拉取而不重复
  应用已执行 GTID。
- 中间 relay/applied-marker 持久化保留旧 position，最终完整状态替换才推进
  position，避免存储提交前崩溃造成跳过源事务。replication 包和全仓串行回归通过。
- 新增最终 state replacement 失败的故障注入回归，确认 source position 不提前推进，
  重启后由 durable applied marker 收敛且不重复存储应用。
  证据见 `reports/compatibility/p2-native-source-position-persistence-current-continuation1329.txt`。
- 统一 storage/WAL/native binlog/GTID/relay/applied-state 物理提交协议及官方
  MySQL fixture 仍是 P2/P4 未完成项；P3 全量非 Connector/J 客户端矩阵、FULLTEXT
  和非 InnoDB 范围边界不变。

### Continuation 1330

- P2 修复后台 HTTP replica runtime 的 source position 持久化缺口：`pullOnce` 之前
  在 Apply 后只改内存，重启会回到旧位置；现在完整 HTTP response 通过 replica
  最终状态替换一起持久化 position，空事件但 next_position 前进时也覆盖。
- 新增 runtime 重启红测并转绿；replication 包和全仓串行回归通过。证据见
  `reports/compatibility/p2-runtime-pull-source-position-current-continuation1330.txt`。
- P2 统一 storage/WAL/native binlog/GTID/relay/applied-state 物理提交协议及官方
  MySQL fixture 仍未完成；P3/P4、FULLTEXT 和非 InnoDB 范围边界不变。

### Continuation 1331

- HTTP position-based replication now returns the source binlog filename and
  persists it with the source position during the final replica-state replacement.
- `TestSourceBinlogResponseAdvancesPastLastEvent` and
  `TestRuntimePullPersistsSourcePositionAcrossRestart` pass, together with the
  full replication package regression. Evidence:
  `reports/compatibility/p2-runtime-source-file-persistence-current-continuation1331.txt`.
- The unified storage/WAL/native-binlog/GTID/relay/applied-state commit protocol
  and official MySQL fixture remain open; P3/P4, FULLTEXT, and non-InnoDB scope
  boundaries are unchanged.

### Continuation 1332

- Refreshed the non-Connector-J client environment diagnostic. Go, PyMySQL, and
  Node/mysql2 are available; mysql CLI remains `SKIPPED_ENVIRONMENT` because
  `mysql.exe` is absent.
- No live client PASS was claimed without a protected server password. Evidence:
  `reports/compatibility/p3-client-matrix-diagnostic-current-continuation1332.txt`.

### Continuation 1333

- P2 added durable `PURGE BINARY LOGS TO` support. The source/runtime path
  removes older native files, prunes the native GTID position index, persists
  the first retained file number, and prevents restart/rebuild from resurrecting
  purged files.
- The SQL admin path delegates the operation with source-role and fencing
  checks. Focused replication and engine tests pass. Evidence:
  `reports/compatibility/p2-purge-binary-logs-current-continuation1333.txt`.
- This closes only the local filename-target purge path. The unified physical
  commit protocol, `PURGE ... BEFORE` time semantics, official MySQL fixture
  interoperability, complete I_S/P_S semantics, and the full non-Connector-J
  client matrix remain open; FULLTEXT remains deferred and non-InnoDB remains
  out of scope.

### Continuation 1334

- P2 added timestamp-based `PURGE BINARY LOGS BEFORE` and the legacy
  `PURGE MASTER LOGS BEFORE` alias. The event-time boundary, durable retention,
  and native rebuild behavior were covered by focused red/green tests.
- This slice does not change P2 partial, P1/P3 incomplete, FULLTEXT deferred,
  or non-InnoDB out-of-scope boundaries.

### Continuation 1335

- Added restart-time recovery for ordinary client commit journals whose storage
  commit completed but source binlog/GTID publication failed. Recovery republishes
  durable row images with the stable transaction key, writes the committed marker,
  and removes the active journal after success.
- Replication replay journals remain excluded from source republishing. The focused
  red/green test, affected package tests, and full serial repository regression all
  passed. Evidence:
  `reports/compatibility/p2-replication-commit-recovery-current-continuation1335.txt`.
- Unified storage/WAL/native-binlog/GTID/relay/applied-state commit protocol, full
  I_S/P_S semantics, full non-Connector-J client matrix, and official MySQL fixture
  interoperability remain open. FULLTEXT is deferred and non-InnoDB remains out of
  scope.

### Continuation 1336

- Audited the schema registries against the MySQL 8.4 INFORMATION_SCHEMA and
  PERFORMANCE_SCHEMA table references. Core table registration, dedicated runtime
  handlers, and SELECT/COLUMNS shape regressions are present; component-specific
  tables keep discoverable native shapes without fabricated rows when the component
  is absent.
- This is an audit, not a completion claim. Exact column metadata, privilege
  visibility, and complete lifecycle/runtime semantics remain required before the
  two P1 registry rows can become implemented. Evidence:
  `reports/compatibility/p1-official-schema-registry-audit-current-continuation1336.txt`.

### Continuation 1337

- P2 durable commit recovery now persists and restores original SQL statements
  together with row images and the stable transaction key. This preserves the
  statement-based source publication payload across a restart.
- The focused red/green test, affected packages, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p2-replication-commit-statements-recovery-current-continuation1337.txt`.
- The unified physical commit protocol and official MySQL fixture interoperability
  remain open; P1/P3 remain partial, FULLTEXT remains deferred, and non-InnoDB
  remains out of scope.

### Continuation 1343

- P2 now assigns separate phase GTIDs to two-phase XA: PREPARE keeps the first
  GTID, while COMMIT/ROLLBACK receives an independent terminal GTID. Logical
  events carry both identities and native output emits the terminal GTID before
  the XA query; decoder, replica, relay import, restart recovery, promotion,
  and retries preserve the boundary.
- Replication, engine XA/recovery, and the full serial Go regression passed.
  Evidence: `reports/compatibility/p2-xa-separate-phase-gtids-current-continuation1343.txt`.
- This does not close the broader P2 atomic commit protocol or official MySQL
  fixture interoperability. Complete I_S/P_S semantics and the full
  non-Connector-J client matrix remain global tasks; FULLTEXT stays deferred
  and non-InnoDB stays out of scope.
### Continuation 1344 boundary note

The latest verified slice is P2 XA rollback retry idempotency. It does not close
the P1 backlog. Complete INFORMATION_SCHEMA/PERFORMANCE_SCHEMA coverage still
requires exact per-table column metadata, privilege visibility, component
behavior, and lifecycle/runtime semantics; table-name registration alone is not
completion evidence. See
`reports/compatibility/p2-xa-rollback-retry-idempotency-current-continuation1344.txt`
for the separate P2 evidence.
### Continuation 1345

- Added exact MySQL 8.4 metadata contracts for Performance Schema synchronization
  instance tables: `cond_instances`, `mutex_instances`, and `rwlock_instances`.
- The focused red/green test, I_S/P_S regression, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-sync-instance-metadata-current-continuation1345.txt`.
- This closes only the synchronization-instance metadata fallback. Complete
  INFORMATION_SCHEMA/PERFORMANCE_SCHEMA runtime semantics, row lifecycle,
  privileges, and component behavior remain global P1 work.
### Continuation 1346

- Added exact MySQL 8.4 metadata contracts for Performance Schema
  `file_instances` and `socket_instances`.
- The focused red/green test, I_S/P_S regression, and full serial repository
  regression passed. Evidence:
  `reports/compatibility/p1-performance-schema-file-socket-instance-metadata-current-continuation1346.txt`.
- This closes only the file/socket metadata fallback. Complete runtime
  lifecycle, instance rows, privileges, and component behavior remain global
  P1 work.
