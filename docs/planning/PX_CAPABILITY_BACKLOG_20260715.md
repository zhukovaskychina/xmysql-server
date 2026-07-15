# XMySQL P* Capability Backlog - 2026-07-15

## Scope

P* covers P2/P3/future work. These items are valuable, but they should not be used to block the P0 core CRUD/JDBC production path unless a product decision explicitly promotes one of them.

## P* Backlog

| ID | Area | Capability | Priority | Notes |
|---|---|---|---|---|
| PX-SQL-001 | SQL | Stored procedures and functions | P2 | Requires parser, execution, metadata, and security design |
| PX-SQL-002 | SQL | Triggers | P2 | Depends on transaction semantics and DDL metadata stability |
| PX-SQL-003 | SQL | Views and materialized views | P2 | Views need parser/planner support; materialized views need refresh semantics |
| PX-SQL-004 | SQL | Partitioned tables and partition pruning | P2 | Storage layout and optimizer changes required |
| PX-SQL-005 | SQL | Window functions | P2 | Requires executor and planner support |
| PX-IDX-001 | Index | FULLTEXT index | P2 | Separate tokenizer/storage/query semantics |
| PX-IDX-002 | Index | Spatial index / R-Tree | P2 | Separate data types and operator semantics |
| PX-IDX-003 | Index | Index skip scan | P2 | Useful after composite index basics are stable |
| PX-STG-001 | Storage | Compression | P2 | Page format and recovery must be stable first |
| PX-STG-002 | Storage | Encryption at rest | P2 | Needs key management and recovery integration |
| PX-STG-003 | Storage | Tablespace shrink/defragmentation | P2 | Depends on stable page allocation and purge |
| PX-STG-004 | Storage | External/off-page large object optimization | P2 | Requires row format decisions |
| PX-LOG-001 | Log | Log archival | P2 | Useful for point-in-time recovery and operations |
| PX-LOG-002 | Log | Group commit optimization | P2 | Performance feature after WAL correctness is proven |
| PX-REPL-001 | Replication | Binlog / GTID / replication protocol | P2 | Large compatibility surface; not part of single-node P0 |
| PX-OPS-001 | Operations | Performance Schema compatibility | P2 | Useful for tooling, not required for first core path |
| PX-OPS-002 | Operations | Online DDL | P2 | Requires mature locks, metadata, and index rebuild behavior |
| PX-PERF-001 | Performance | SIMD/vectorized hot paths | P3 | Optimize only after correctness and batching are stable |
| PX-PERF-002 | Performance | Advanced buffer-pool tuning | P3 | Requires workload benchmark baseline |

## Promotion Rule

Move a P* item to P1 or P0 only when there is a concrete release requirement and a defined compatibility matrix. Do not promote broad feature names without testable acceptance criteria.

