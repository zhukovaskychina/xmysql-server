# XMySQL P1 Capability Backlog - 2026-07-15

## Scope

P1 items are important for broad MySQL compatibility, query quality, performance, and maintainability. They should not block the first production-grade core CRUD/JDBC path if every P0 item is closed and the limitation is documented.

## P1 Backlog

| ID | Area | Capability | Current State | Completion Definition |
|---|---|---|---|---|
| P1-OPT-001 | Optimizer | Complete predicate pushdown | Basic rules exist; JOIN/aggregation/subquery boundaries still need hardening | Predicates are pushed only when semantically safe and verified by plan/execution tests |
| P1-OPT-002 | Optimizer | Column pruning end-to-end | Framework and tests exist; full execution path needs coverage | Unused columns are removed through logical and physical plans without breaking metadata |
| P1-OPT-003 | Optimizer | Statistics, NDV, histogram refresh | Exact row count can parse `PAGE_N_RECS`, but NDV, histograms, column-value extraction, refresh, and invalidation are still incomplete | CBO uses real table/index stats with refresh and invalidation behavior |
| P1-OPT-004 | Optimizer | Multi-column index prefix matching | Not complete | Equality/range predicates use valid prefixes of composite indexes |
| P1-OPT-005 | Optimizer | DNF and expression simplification | Partial expression tooling exists | Boolean expressions are normalized and simplified without changing NULL semantics |
| P1-OPT-006 | Optimizer | Rule ordering and conflict control | Not complete | RBO rules run in deterministic order with conflict tests |
| P1-EXE-001 | Executor | SortMergeJoin execution | Physical plan exists; execution path can degrade to nested loop | Merge join executes directly when inputs are sorted or can be sorted efficiently |
| P1-EXE-002 | Executor | Batch processing | Not complete | Scan/filter/project/aggregate paths support batch execution where useful |
| P1-EXE-003 | Executor | Parallel scan scheduling | Partial | Parallel workers have bounded resource control and deterministic result assembly |
| P1-EXE-004 | Executor | Expression evaluation optimization | Not complete | Hot expression paths reduce allocations and repeated conversions |
| P1-SQL-001 | SQL | Subquery breadth | Partial | Scalar, IN, EXISTS, and correlated subqueries pass a defined compatibility matrix |
| P1-SQL-002 | SQL | HAVING, DISTINCT, UNION variants | Partial | Common aggregate and set-operation queries match MySQL semantics |
| P1-SQL-003 | SQL | Wider ALTER TABLE support | Partial | Add/drop/modify column and index operations preserve dictionary, data, and indexes |
| P1-SQL-004 | SQL | Foreign key and CHECK constraints | Explicitly unsupported in the current JDBC closure run | FK metadata, FK enforcement, CHECK evaluation, and clear error behavior pass JDBC tests |
| P1-SQL-005 | SQL | Cascade update/delete actions | Explicitly unsupported in the current JDBC closure run | `ON DELETE` and `ON UPDATE` actions are enforced consistently with transaction semantics |
| P1-IDX-001 | Index | Index merge | Partial | OR and multi-index predicates can combine index scans safely |
| P1-IDX-002 | Index | SHOW INDEX and index statistics | Validate/compact can correct basic page counters; SHOW INDEX/cardinality fidelity remains partial | Metadata reflects current durable index definitions and cardinality estimates |
| P1-TXN-001 | Transaction | Deadlock and lock-wait diagnostics | Detection exists in parts; reporting is incomplete | Reports include wait graph, victim, wait time, and related transaction identifiers |
| P1-TXN-002 | Transaction | Long transaction management | Partial | Long transactions are visible, bounded by policy, and safe for purge behavior |
| P1-SEC-001 | Security | Hardened auth and privilege model | Several paths are simplified or fallback to root/default behavior | User/host/plugin/password and privilege checks are data-backed and test-covered |
| P1-OPS-001 | Operations | Backup and restore workflow | Scripts and runbooks exist; production workflow needs hardening | Backup/restore can be run repeatedly with validation output |
| P1-OPS-002 | Operations | Performance benchmark suite | No stable release benchmark baseline | Read/write/query benchmarks produce comparable latency and throughput reports |

## Suggested Order

1. Optimizer correctness before executor performance.
2. SQL compatibility breadth before optional storage features.
3. Security hardening before broader deployment.
4. Benchmarks after P0 correctness is stable, otherwise numbers are misleading.
