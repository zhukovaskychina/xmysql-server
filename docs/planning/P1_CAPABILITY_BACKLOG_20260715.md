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
