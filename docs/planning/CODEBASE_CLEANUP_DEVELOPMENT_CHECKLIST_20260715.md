# XMySQL Codebase Cleanup Development Checklist - 2026-07-15

## Purpose

This checklist turns the current codebase confusion points into executable cleanup work.

Current baseline:

- Basic CRUD and JDBC DML work in the focused path.
- The code still contains multiple historical paths for storage format, DML, SELECT, B+Tree, transactions, and auth.
- Cleanup must preserve the working CRUD/JDBC baseline while reducing ambiguous runtime paths.

## Priority Rule

| Priority | Meaning |
|---|---|
| P0 | Can return wrong data, lose data, hide storage/transaction errors, or make CRUD/JDBC behavior depend on initialization state |
| P1 | Creates maintenance confusion or blocks broader compatibility, but is not expected to break the focused CRUD path immediately |
| P* | Long-term cleanup or optional hardening after the main path is stable |

## P0 Cleanup Checklist

| ID | Area | Problem | Files | Completion Definition | Verification |
|---|---|---|---|---|---|
| CLEAN-P0-01 | Storage format | `XIR1`, `XBTREC1`, sidecar JSON, and pseudo-InnoDB page layout coexist | `server/innodb/engine/record_codec.go`, `server/innodb/manager/enhanced_btree_index.go`, `server/innodb/manager/enhanced_btree_adapter.go`, `server/innodb/engine/btree_sidecar_cleanup.go` | **Completed:** `XIR1` is the clustered-record codec, `XBTREC1` is the durable B+Tree record block, and legacy row/B+Tree JSON is not a runtime read/write fallback; cleanup remains migration hygiene only | Go restart tests, page parse tests, JDBC CRUD after restart; continuation140 |
| CLEAN-P0-02 | DML routing | `XMySQLExecutor` can run either `StorageIntegratedDMLExecutor` or legacy `DMLExecutor` | `server/innodb/engine/executor.go`, `server/innodb/integration/execution_engine_integration.go`, `server/innodb/engine/dml_executor.go`, `server/innodb/engine/storage_integrated_dml_executor.go` | **Completed:** production executor and integration wiring use one storage-integrated DML entry; missing storage managers fail fast; legacy executor remains only for isolated compatibility tests | engine/integration DML tests; continuation140 |
| CLEAN-P0-03 | SELECT correctness | Legacy `SelectExecutor` still has string WHERE and simulated even-row filtering | `server/innodb/engine/select_executor.go`, `server/innodb/engine/unified_executor.go`, `server/innodb/engine/clustered_index_scanner.go` | **Completed for active SELECT paths:** UnifiedExecutor now applies parsed AST predicates through `FilterOperator` with typed row values; production SelectExecutor already applies the authoritative parsed `sqlparser.Expr` after storage scan; simulated filtering is not used by active paths | UnifiedExecutor predicate regression, SELECT WHERE positive/negative tests, JDBC SELECT tests |
| CLEAN-P0-04 | Transaction behavior | Storage DML can continue with simplified transaction context when `txManager` is nil | `server/innodb/engine/storage_integrated_dml_helper.go`, `server/innodb/manager/transaction_manager.go` | **Completed for production DML:** storage-integrated transaction begin fails fast without a real `TransactionManager`, and commit/rollback operate on the real transaction object; nil-manager constructions remain test fixtures | JDBC transaction tests, rollback tests, nil-manager regression; continuation140 |
| CLEAN-P0-05 | B+Tree persistence | Legacy `DefaultBPlusTreeManager` and `EnhancedBTree*` use different page/write semantics | `server/innodb/manager/bplus_tree_manager.go`, `server/innodb/manager/enhanced_btree_index.go`, `server/innodb/manager/enhanced_btree_adapter.go`, `server/innodb/manager/table_storage_mapping.go` | **Completed for production wiring:** user/system table paths expose `basic.BPlusTreeManager` backed by `EnhancedBTreeAdapter`; legacy manager constructors and helpers are isolated to compatibility tests and no longer appear in production table wiring | B+Tree insert/delete/range/restart tests; continuation140 |
| CLEAN-P0-06 | Auth shortcuts | `mysql.user` hardcoded response, root bypass, default host, and one-privilege check remain in request path | `server/dispatcher/enhanced_message_handler.go`, `server/auth/password_validator.go`, `server/auth/engine_access.go` | Test/demo shortcuts are behind explicit test mode; normal path is data-backed and checks all required privileges | Auth negative tests and JDBC connection tests |

## P1 Cleanup Checklist

| ID | Area | Problem | Files | Completion Definition | Verification |
|---|---|---|---|---|---|
| CLEAN-P1-01 | Metadata | Commented-out conflicting `Table` model remains | `server/innodb/metadata/table.go`, `server/innodb/metadata/schema.go` | Completed: removed the dead file; `schema.go` is the single runtime owner of `metadata.Table` | `go test ./server/innodb/metadata ./server/innodb/engine -count=1`, continuation134 full/release gates |
| CLEAN-P1-02 | Index helpers | Index performance metrics return constants | `server/innodb/engine/storage_integrated_index_helper.go`, `server/innodb/manager/enhanced_btree_manager.go` | Completed: update latency uses measured DML index-sync time, cache hit rate uses atomic B+Tree cache counters, and active index count uses `IndexManager` statistics; empty state returns zero | `go test ./server/innodb/engine -run 'TestIndexPerformanceMetrics|TestRecordIndexUpdate' -count=1`, continuation135 full/release gates |
| CLEAN-P1-03 | Optimizer stats | CBO/statistics contain mock or heuristic paths | `server/innodb/plan/statistics_collector_helpers.go`, `server/innodb/plan/cost_estimator.go`, `server/innodb/plan/selectivity_estimator.go` | Mock stats are marked test-only; production stats come from table/index scans | Optimizer plan tests |
| CLEAN-P1-04 | Page wrappers | Multiple page abstractions remain active | `server/innodb/storage/store/pages`, `server/innodb/storage/wrapper/page`, `server/innodb/storage/wrapper/types` | Document and enforce one page abstraction entrypoint | Wrapper and manager tests |
| CLEAN-P1-05 | Error contract | Some fallback paths still return broad `ExecutionErrorCodeUnknown` | `server/innodb/engine/executor.go`, `server/innodb/engine/unified_executor.go` | Storage, validation, duplicate key, auth, and transaction errors map to stable codes | Negative JDBC and engine tests |

## P* Cleanup Checklist

| ID | Area | Problem | Completion Definition |
|---|---|---|---|
| CLEAN-PX-01 | Demo/test code | Many tests construct production executors with nil managers | Move to explicit test builders so nil-manager behavior cannot be confused with production behavior |
| CLEAN-PX-02 | Logging | Logs mix Chinese symbols, English text, and decorative markers | Standardize runtime logs after functional cleanup |
| CLEAN-PX-03 | File naming | Historical files do not reveal production vs legacy status | Rename or add package comments after main cleanup lands |
| CLEAN-PX-04 | Documentation | Historical completion reports conflict with current capability state | Keep current priority docs as canonical and mark historical reports as evidence-only |

## Recommended Execution Order

1. CLEAN-P0-03: remove SELECT simulated filtering first because it can return wrong rows.
2. CLEAN-P0-02: force one DML runtime path.
3. CLEAN-P0-04: require real transaction semantics in production DML.
4. CLEAN-P0-01 and CLEAN-P0-05: unify storage format and B+Tree persistence together.
5. CLEAN-P0-06: isolate auth shortcuts before broader JDBC compatibility work.
6. P1/P* cleanup after the focused CRUD/JDBC and restart tests stay green.

## Continuation 140

- `CLEAN-P0-01`, `CLEAN-P0-02`, `CLEAN-P0-04`, and `CLEAN-P0-05` were re-audited against production call sites.
- The integration execution layer no longer constructs the legacy `DMLExecutor`; only `StorageIntegratedDMLExecutor` is initialized for production DML.
- `verifyUserDataBTree` now accepts the public `basic.BPlusTreeManager` interface instead of binding a production helper to `DefaultBPlusTreeManager`.
- Evidence: `go test ./server/innodb/integration ./server/innodb/engine -run 'Test|DML|StorageIntegrated' -count=1` and manager production-boundary tests pass.
- `CLEAN-P0-03` is closed for the active paths: the main SELECT path applies the parsed `sqlparser.Expr` after storage scan, and UnifiedExecutor applies the same AST predicate through `FilterOperator`; both retain WHERE-only column dependencies and NULL-aware evaluation. Remaining string condition helpers are isolated compatibility utilities, not active production filtering.

## Minimum Regression Gate

Run these after each P0 cleanup item:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -count=1
mvn test -Dtest=DMLOperationsTest
mvn test -Pjdbc-connectivity
```

If a change touches protocol/auth, also run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher ./server/auth ./server/net -count=1
```

