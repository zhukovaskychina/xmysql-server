# XMySQL P0 Capability Backlog - 2026-07-15

## Scope

P0 contains only production-blocking capabilities. These are not nice-to-have items; each one can cause incorrect data, failed JDBC/MySQL compatibility, unrecoverable state, or unsafe release decisions.

Current tested baseline after the 2026-07-22 P0 remaining-capability closure run:

- Focused Go packages pass: `./server/innodb/engine ./server/innodb/manager ./server/innodb/plan`.
- JDBC focused matrix passes on clean local data: `DMLOperationsTest`, `PreparedStatementTest`, `TransactionTest`, `DDLOperationsTest`, `SystemVariableTest`, and `IndexAndConstraintTest`; 77 tests, 0 failures, 0 errors.
- Storage recovery now includes focused B+Tree delete/reuse/free-list behavior, redo/page checksum evidence, and restart read-back for clustered-record paths.
- Undo purge now protects active read views and `UndoSpaceReclaimer` reclaims prepared cached segments into reusable cached segments.
- Index `ValidateIndex` and `CompactIndex` are no longer no-ops; they verify active B+Tree metadata, root page, leaf-chain shape, duplicate leaf pages, and compact stale page counters.
- CBO exact row count now parses InnoDB index page `PAGE_N_RECS`; empty pages no longer generate synthetic sample rows.
- DDL metadata now passes the focused JDBC matrix for `SHOW DATABASES LIKE`, `DROP DATABASE IF EXISTS`, `SHOW FULL TABLES`, `DatabaseMetaData.getTables()`, `ALTER TABLE ADD COLUMN`, and `TRUNCATE TABLE`.
- Transaction commands now pass the focused JDBC matrix for commit, rollback, savepoint, multiple savepoints, isolation-level setter, `BEGIN`, and `START TRANSACTION`.
- Core constraints pass for PRIMARY KEY, composite PRIMARY KEY, UNIQUE, NOT NULL, INDEX, composite index, and UNIQUE INDEX; FK/CHECK/FULLTEXT/cascade syntax smoke paths pass, while complete advanced semantics are tracked outside P0.

P0 is the remaining work required to turn the green CRUD/prepared-statement baseline into a production-grade database path.

## P0 Backlog

| ID | Area | Capability | Current State | Completion Definition | Evidence |
|---|---|---|---|---|---|
| P0-STG-001 | Storage | Unified durable clustered row/page path | Closed for the focused P0 path: DML write, SELECT scan, restart read-back, and page decode share the clustered-record codec | Keep regression coverage current; full byte-for-byte InnoDB compatibility is not a P0 requirement | Go storage tests and focused restart coverage |
| P0-STG-002 | Storage | Clustered B+Tree full scan and range scan | Closed for focused CRUD/restart paths; full production stress breadth is tracked as P1 | Full table scan, PK lookup, and ordered scan stay backed by durable B+Tree pages | Go tests plus JDBC CRUD matrix |
| P0-STG-003 | Storage | B+Tree split, merge, delete, and reuse correctness | Closed for focused delete/reuse/free-list coverage; scale and crash-drill stress move to P1 | Deletes and page reuse cannot resurrect stale focused-test records | Split/delete/reuse Go tests |
| P0-IDX-001 | Index | Durable secondary index read path | Closed for focused optimizer/executor tests; broader cost-model and workload tuning move to P1 | Optimizer can choose secondary indexes and executor can return correct rows | Index manager and plan tests |
| P0-IDX-002 | Index | Durable UNIQUE enforcement | Closed for focused restart/concurrency coverage and JDBC constraint matrix | UNIQUE checks are enforced from current index state for supported DML paths | Go tests and JDBC DML/index suite |
| P0-IDX-003 | Index | Index rebuild, validate, and repair | Closed for focused rebuild/validate/repair evidence; deeper corruption-injection coverage moves to P1 | Rebuild produces equivalent state; validator detects stale entries; repair is explicit | Rebuild/validate/repair Go tests |
| P0-TXN-001 | Transaction | JDBC transaction semantics | Closed for focused JDBC matrix: commit, rollback, savepoint, multiple savepoints, isolation setter, `BEGIN`, and `START TRANSACTION` pass | JDBC transaction suite remains green on clean local data | `TransactionTest` |
| P0-TXN-002 | Transaction | MVCC visibility and isolation | Closed for focused P0 rollback/savepoint/read-view tests; broad anomaly testing remains P1 | Supported RC/RR-style visibility paths do not regress under focused tests | Go transaction tests and JDBC transaction suite |
| P0-TXN-003 | Recovery | Recovery with row/page/WAL state proof | Closed for focused Go recovery evidence; external kill-and-replay drill moves to P1 release evidence | Redo, undo, page checksum, and restart-read focused tests remain green | Go recovery tests |
| P0-TXN-004 | Recovery | Undo purge and deleted-record lifecycle | Closed for active snapshot protection and reusable cached segment reclaim; long workload proof moves to P1 | Deleted versions are retained while visible and reclaim does not break focused tests | Undo purge/reclaim Go tests |
| P0-SQL-001 | SQL | Core DML compatibility | Closed for focused JDBC CRUD; `ON DUPLICATE KEY UPDATE` and `REPLACE` move to P1 compatibility backlog | `INSERT`, `UPDATE`, `DELETE`, and `SELECT` pass JDBC tests | `DMLOperationsTest` |
| P0-SQL-002 | SQL | Core DDL compatibility | Closed for focused JDBC DDL and metadata: CREATE/DROP/TRUNCATE/ALTER ADD COLUMN and metadata lookups pass | Core DDL forms keep dictionary, storage, and JDBC metadata consistent for tested scope | `DDLOperationsTest` |
| P0-JDBC-001 | Protocol | Prepared statement and metadata fidelity | Closed for focused JDBC prepared and metadata surface | Common JDBC prepared statements, generated keys, metadata queries, and system variable bootstrap pass | `PreparedStatementTest`, `DDLOperationsTest`, `SystemVariableTest` |
| P0-JDBC-002 | Protocol | Auth and error contract for JDBC clients | Authentication and error mapping include simplified paths | Supported auth plugin behavior, SQLState, vendor codes, and connection/session errors are deterministic | Protocol/auth tests and JDBC negative tests |
| P0-OBS-001 | Operations | Live runtime observability | Metrics/logging foundations exist | Live server exposes QPS, latency, errors, connections, transactions, lock waits, slow queries, and storage health from real traffic | Live `/metrics` probe and generated slow-query log |
| P0-REL-001 | Release | Production candidate evidence gate | Candidate/evidence tooling exists | Full current-run candidate passes with no stale artifacts and no unresolved required gaps | Delivery candidate report, evidence bundle, delivery audit |
| P0-GOV-001 | Governance | Risk owner sign-off | Governance tooling exists; owner approval must be real | Risk register has owners, open P0 risks are closed/deferred, and owner sign-off is valid | Governance gate report |

## 2026-07-21 Implementation Notes

Closed in code during the P0 gap-closure pass:

- `UndoSpaceReclaimer.reclaimSpace()` scans cached undo segments, purges reclaimable `SEGMENT_PREPARED` segments, and returns them to `SEGMENT_CACHED` so `AllocateSegment()` can reuse them.
- `IndexManager.ValidateIndex()` now rejects missing root pages, inactive/non-B+Tree indexes, zero pages, missing columns, empty/duplicate leaf chains, inconsistent first-leaf pointers, and stale leaf/page counters.
- `IndexManager.CompactIndex()` now recomputes leaf/non-leaf/page counters from the B+Tree leaf chain instead of only updating `UpdateTime`.
- `EnhancedStatisticsCollector` exact row counts now sum parsed InnoDB index page `PAGE_N_RECS` values, and empty pages no longer synthesize column sample rows.

## 2026-07-22 Implementation Notes

Closed in code and evidence during the P0 remaining-capability closure pass:

- JDBC transaction matrix now passes for commit, rollback, savepoint, multiple savepoints, isolation-level setter, `BEGIN`, and `START TRANSACTION`.
- Durable secondary-index maintenance and focused read-path coverage were closed with optimizer/executor tests, validation, rebuild, and repair coverage.
- No-primary-key indexed table DML and composite-primary-key update/delete focused gaps were closed in the engine tests.
- JDBC DDL metadata gaps were closed for `SHOW DATABASES LIKE`, `DROP DATABASE IF EXISTS`, `SHOW FULL TABLES`, `DatabaseMetaData.getTables()`, `ALTER TABLE ADD COLUMN`, and `TRUNCATE TABLE`.
- Evidence run `docs/planning/P0_EVIDENCE_RUN_20260722.md` records the clean focused Go and JDBC matrix results.

Moved out of P0 after this pass:

- `ON DUPLICATE KEY UPDATE` and `REPLACE`: P1 SQL compatibility breadth.
- Full foreign-key referential enforcement matrix, CHECK expression semantics, and FULLTEXT query/ranking behavior: P1/P* compatibility breadth.
- External kill-and-replay crash drill, production-scale concurrent workload, live metrics drill, rollback drill, owner sign-off: P1 release-readiness gates rather than code-level P0 closure.

## P0 Exit Criteria

P0 is complete for the focused 2026-07-22 scope when:

- all P0 backlog items above either have current evidence or are explicitly moved to P1/P* with a reason;
- no P0 item relies on stale evidence from an older run;
- focused Go engine/manager/plan tests pass;
- focused JDBC CRUD, prepared statement, transaction, DDL metadata, system variable, and index/constraint suites pass on clean local data;
- generated runtime files are removed or restored before merge.
