# XMySQL P* Capability Backlog - 2026-07-15

## Scope

P* covers P2/P3/future work. These items are valuable, but they should not be used to block the P0 core CRUD/JDBC production path unless a product decision explicitly promotes one of them.

## P* Backlog

| ID | Area | Capability | Priority | Notes |
|---|---|---|---|---|
| PX-SQL-001 | SQL | Stored procedures and functions | P2 | Common CREATE/ALTER/DROP/CALL, including qualified cross-schema `CALL schema.routine(...)`, routine `SQL SECURITY`/`COMMENT` metadata, persistence, parameter/return metadata, routine execution privilege checks, and routine information-schema rows are implemented; broader routine grammar, dynamic SHOW_ROUTINE enforcement, and full MySQL routine semantics remain |
| PX-SQL-002 | SQL | Triggers | P2 | Common CREATE/DROP trigger definitions, persistence, trigger metadata and row-event execution paths are implemented; same-table/event/timing `FOLLOWS`/`PRECEDES` ordering is honored by BEFORE/AFTER execution and ordered AFTER bodies strip the ordering clause; broader trigger grammar/order/error compatibility remains |
| PX-SQL-003 | SQL | Views and materialized views | P2 | Common CREATE/ALTER/DROP view definitions, persistence, SHOW CREATE/INFORMATION_SCHEMA metadata and view query expansion are implemented; materialized-view refresh semantics remain |
| PX-SQL-004 | SQL | Partitioned tables and partition pruning | P2 | Logical RANGE/LIST/HASH/KEY descriptors, routing, metadata, pruning, ADD/DROP/TRUNCATE, and RANGE REORGANIZE are implemented; physical independent partition tablespaces, data movement, and full optimizer/storage semantics remain |
| PX-SQL-005 | SQL | Window functions | P2 | Common ROW_NUMBER/RANK/DENSE_RANK/NTILE/LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE/CUME_DIST/PERCENT_RANK and window aggregate/frame semantics are implemented through the compatibility executor; broader optimizer-native physical planning and uncommon grammar remain |
| PX-IDX-001 | Index | FULLTEXT index | P2 | Explicitly unsupported in the current JDBC closure run; requires separate tokenizer/storage/query semantics |
| PX-IDX-002 | Index | Spatial index / R-Tree | P2 | Separate data types and operator semantics |
| PX-IDX-003 | Index | Index skip scan | P2 | Composite secondary indexes now support bounded trailing-column equality, `IS NULL`, `IS NOT NULL`, and range skip scans by enumerating persisted one-or-more-column leading prefixes and reusing prefix ranges; a basic cost guard falls back when distinct leading combinations are not fewer than index entries; range and non-NULL scans use residual WHERE filtering because the durable key encoding is not type-sortable, and richer cost/selectivity modeling remains |
| PX-STG-001 | Storage | Compression | P2 | `IOOptimizer` now supports an injectable page-compression boundary (including batch writes, reads, cache and prefetch), and `StorageManager` can wire the zlib provider through buffer-pool and direct Space/FileTableSpace paths with compression-before-encryption composition, default new-space policy, explicit per-space policy, and `SpaceInfo.IsCompressed`; compression policy is durably persisted and restored across restart, including settings and excluded spaces; compressed-page buffer lifetime, truncated-header failures, config parsing, direct-space round trips, restart recovery, and composition are tested. Compressed tablespace metadata/recovery migration beyond the XMySQL provider envelope and upstream InnoDB compressed-page compatibility remain |
| PX-STG-002 | Storage | Encryption at rest | P2 | Master-key configuration now wraps buffer-pool, direct Space and FileTableSpace paths; new tablespaces receive keys, are encrypted before use, and persist the keyring; restart reconstruction, existing-page re-encryption/rollback, and startup migration of pre-keyring plaintext spaces are lifecycle-tested. Native encrypted-page headers/integrity remain |
| PX-STG-003 | Storage | Tablespace shrink/defragmentation | P2 | `StorageManager.ReclaimSpace` now actually releases already-empty segment extents back to the allocator, `OptimizeStorage` no longer expands a tablespace implicitly and invokes optional `IBDSpace.ShrinkToFit`, and `CHECK TABLE` validates the clustered index page record block; record relocation, cross-layer extent reconciliation, and full online defragmentation remain |
| PX-STG-004 | Storage | External/off-page large object optimization | P2 | Requires row format decisions |
| PX-LOG-001 | Log | Log archival | P2 | Rotation/gzip archival, provider-mirrored flushed redo, and configurable PITR retention by age/count are lifecycle-tested; `redo.log` remains the recovery source; upstream binlog/PITR format compatibility remains out of scope |
| PX-LOG-002 | Log | Group commit optimization | P2 | Asynchronous commit requests are coalesced by a configurable time window and batch limit; the worker performs one flush for the batch, drains accepted requests on close, and exposes real commit/batch/fsync/latency statistics. Further WAL scheduling and production throughput tuning remain |
| PX-REPL-001 | Replication | Binlog / GTID / replication protocol | P2 | Internal source/replica GTID state, transaction filtering, promotion, COM_BINLOG_DUMP/COM_BINLOG_DUMP_GTID and native event framing are implemented and tested; full upstream binlog file/GTID/PITR interoperability remains out of scope |
| PX-OPS-001 | Operations | Performance Schema compatibility | P2 | Summary/variables/status/threads/setup views, stateful consumers/instruments/actors/objects configuration, bounded statement/stage/wait history, wait consumer and wait-instrument visibility/timing controls, lock-backed `data_locks/data_lock_waits/events_waits_current` views, bounded completed row-lock and metadata-lock wait history, cross-session bounded commit/rollback `events_transactions_history_long`, savepoint counters, and a bounded memory summary view are implemented; full allocator/file-page IO and upstream event fidelity remain deferred |
| PX-OPS-002 | Operations | Online DDL | P2 | Requires mature locks, metadata, and index rebuild behavior |
| PX-PERF-001 | Performance | SIMD/vectorized hot paths | P3 | Optimize only after correctness and batching are stable |
| PX-PERF-002 | Performance | Advanced buffer-pool tuning | P3 | Requires workload benchmark baseline |

## Promotion Rule

Move a P* item to P1 or P0 only when there is a concrete release requirement and a defined compatibility matrix. Do not promote broad feature names without testable acceptance criteria.
