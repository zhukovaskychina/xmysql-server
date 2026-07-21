# XMySQL P0 Capability Backlog - 2026-07-15

## Scope

P0 contains only production-blocking capabilities. These are not nice-to-have items; each one can cause incorrect data, failed JDBC/MySQL compatibility, unrecoverable state, or unsafe release decisions.

Current tested baseline after the 2026-07-21 P0 gap-closure implementation run:

- Focused Go packages pass: `./server/innodb/engine ./server/innodb/manager ./server/innodb/plan`.
- Earlier JDBC CRUD/prepared-statement focused suites passed on clean local data; rerun is still required for every release candidate.
- Storage recovery now includes focused B+Tree delete/reuse/free-list behavior, redo/page checksum evidence, and restart read-back for clustered-record paths.
- Undo purge now protects active read views and `UndoSpaceReclaimer` reclaims prepared cached segments into reusable cached segments.
- Index `ValidateIndex` and `CompactIndex` are no longer no-ops; they verify active B+Tree metadata, root page, leaf-chain shape, duplicate leaf pages, and compact stale page counters.
- CBO exact row count now parses InnoDB index page `PAGE_N_RECS`; empty pages no longer generate synthetic sample rows.
- DDL is still partial: JDBC metadata and unsupported foreign key DDL require a current matrix rerun before any readiness claim.
- Transaction command support has focused Go coverage, but production-grade JDBC rollback/savepoint/isolation evidence is still required.
- Core constraints pass for PRIMARY KEY, composite PRIMARY KEY, UNIQUE, NOT NULL, INDEX, composite index, and UNIQUE INDEX; FK/CHECK/FULLTEXT/cascade remain explicitly unsupported.

P0 is the remaining work required to turn the green CRUD/prepared-statement baseline into a production-grade database path.

## P0 Backlog

| ID | Area | Capability | Current State | Completion Definition | Evidence |
|---|---|---|---|---|---|
| P0-STG-001 | Storage | Unified InnoDB-style row/page format | Clustered rows persist through the current codec, but project-specific record blocks and sidecar-style transition paths still exist | One canonical on-disk row/page format is used for DML write, SELECT scan, recovery, and restart read-back | Format-level tests, restart tests, page dump/parse tests |
| P0-STG-002 | Storage | Clustered B+Tree full scan and range scan | Focused full scan works for CRUD tests; complete leaf-chain/range behavior is not closed | Full table scan, PK lookup, range scan, and ordered leaf traversal read from durable B+Tree pages only | Go tests plus JDBC range queries before and after restart |
| P0-STG-003 | Storage | B+Tree split, merge, delete, and reuse correctness | Split/merge/delete paths contain simplified logic and need durable verification | Inserts trigger stable splits, deletes mark/purge correctly, page reuse cannot resurrect stale records | Split/merge/delete stress tests and page-level verification |
| P0-IDX-001 | Index | Durable secondary index read path | Durable key encoding exists, but SELECT does not fully depend on secondary indexes | Optimizer can choose secondary index, execution can scan it, and row lookup returns correct rows | Query plan assertions and JDBC SELECT tests |
| P0-IDX-002 | Index | Durable UNIQUE enforcement | Runtime checks and partial durable mappings exist | UNIQUE checks are enforced from durable index state across restart and concurrent inserts | Duplicate/non-duplicate tests across restart and concurrent JDBC clients |
| P0-IDX-003 | Index | Index rebuild, validate, and repair | Validate/compact now has a real metadata and leaf-chain check; rebuild and repair still need corruption-injection proof | Rebuild produces equivalent index state; validator detects missing/stale entries; repair path is explicit | Rebuild/validate tests and corruption-injection tests |
| P0-TXN-001 | Transaction | JDBC transaction semantics | `BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT` commands have focused coverage, but JDBC multi-connection rollback/savepoint/isolation matrix still must be proven current | `BEGIN`, `COMMIT`, `ROLLBACK`, autocommit, and savepoint behavior are correct over JDBC | JDBC transaction matrix |
| P0-TXN-002 | Transaction | MVCC visibility and isolation | MVCC structures exist; visibility is still simplified in parts of storage | RC/RR visibility works for concurrent readers/writers and restart boundaries | Multi-connection anomaly tests |
| P0-TXN-003 | Recovery | Crash recovery with row/page/WAL state proof | Recovery tests and evidence tooling exist; full disk-state proof remains incomplete | Redo, undo, interrupted commit, and replay boundary checks prove expected row/page/WAL state | Recovery drill with generated state diff artifacts |
| P0-TXN-004 | Recovery | Undo purge and deleted-record lifecycle | Active snapshot protection and prepared cached segment reclaim are implemented; deleted-record purge/restart lifecycle still needs full workload proof | Deleted versions are retained while visible, purged when safe, and never reappear after restart | Long transaction plus delete/purge/restart tests |
| P0-SQL-001 | SQL | Core DML compatibility | Basic DML works; `ON DUPLICATE KEY UPDATE` and `REPLACE` remain unsupported | `INSERT`, `UPDATE`, `DELETE`, `SELECT`, `ON DUPLICATE KEY UPDATE`, and `REPLACE` pass JDBC tests | JDBC DML suite |
| P0-SQL-002 | SQL | Core DDL compatibility | CREATE/DROP/TRUNCATE/ALTER ADD COLUMN smoke paths work; JDBC metadata visibility is still incomplete | CREATE/DROP/TRUNCATE/ALTER core forms keep dictionary, storage, and indexes consistent | DDL integration tests with restart |
| P0-JDBC-001 | Protocol | Prepared statement and metadata fidelity | Common prepared DML now passes, including generated keys, IN, LIKE, DECIMAL, BOOLEAN, NULL, and transaction use; JDBC metadata result sets are still incomplete | Common JDBC prepared statements, parameter types, result metadata, generated keys, and errors match MySQL expectations | JDBC compatibility suite |
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

Still P0 after this pass:

- JDBC transaction matrix: rollback, savepoint, autocommit, isolation, and multi-connection visibility must be rerun and fixed from current failures.
- Durable secondary-index SELECT path: optimizer choice and executor row lookup must be proven to depend on durable secondary index state.
- Rebuild/repair: validation exists, but repair and corruption-injection evidence are still missing.
- DML breadth: no-primary-key indexed tables and composite-primary-key update/delete remain explicit compatibility gaps until tests prove otherwise.
- Production evidence: crash drill, concurrent workload, metrics, rollback drill, and owner sign-off are still required.

## P0 Exit Criteria

P0 is complete only when:

- all P0 backlog items are implemented or explicitly accepted as time-boxed deferrals;
- no P0 item relies on stale evidence from an older run;
- CRUD and JDBC tests pass before and after server restart;
- crash recovery evidence includes row/page/WAL state checks;
- concurrent JDBC workload evidence shows no lost write, dirty read, duplicate row, stale index entry, or resurrected deleted row;
- delivery readiness and governance gates pass.
