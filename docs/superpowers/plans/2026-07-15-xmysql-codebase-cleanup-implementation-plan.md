# XMySQL Codebase Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce confusing legacy runtime paths while preserving the working CRUD/JDBC baseline.

**Architecture:** Make the storage-integrated path the production path, isolate or remove legacy fallbacks, and add regression tests before each cleanup. Keep storage format and B+Tree cleanup coupled because they affect the same persisted bytes.

**Tech Stack:** Go 1.24.3, XMySQL engine/manager packages, Maven JDBC test suite.

---

## File Structure

- `server/innodb/engine/select_executor.go`: remove legacy simulated WHERE behavior or isolate it outside production routing.
- `server/innodb/engine/unified_executor.go`: keep SELECT routing through the authoritative planner/operator path.
- `server/innodb/engine/executor.go`: remove silent DML fallback to legacy `DMLExecutor`.
- `server/innodb/engine/dml_executor.go`: mark as test-only or retire from production path after callers are removed.
- `server/innodb/engine/storage_integrated_dml_executor.go`: keep as production DML entrypoint.
- `server/innodb/engine/storage_integrated_dml_helper.go`: require real transaction behavior for production DML.
- `server/innodb/engine/record_codec.go`: keep or replace as the single clustered-record codec.
- `server/innodb/manager/enhanced_btree_index.go`: remove sidecar fallback after canonical page record persistence is complete.
- `server/innodb/manager/enhanced_btree_adapter.go`: remove sidecar full-scan/delete fallback after page scan passes.
- `server/innodb/manager/bplus_tree_manager.go`: demote legacy implementation or wrap it behind a non-production boundary.
- `server/dispatcher/enhanced_message_handler.go`: remove hardcoded auth/user shortcuts from normal path.
- `server/auth/password_validator.go`: separate unsupported auth plugin behavior from supported plugin behavior.

### Task 1: Remove SELECT Simulated Filtering

**Files:**
- Modify: `server/innodb/engine/select_executor.go`
- Modify: `server/innodb/engine/unified_executor.go`
- Test: `server/innodb/engine/select_executor_projection_test.go`

- [x] **Step 1: Add a failing test for WHERE correctness**

Add a test that inserts or builds three rows where only one row matches `id = 2`, then asserts the result is exactly that row. The failure must prove that returning even-indexed rows is not acceptable.

- [x] **Step 2: Run the focused test**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Select|Where' -count=1
```

Expected before the fix: a WHERE correctness failure or unsupported-path failure that points to the legacy path.

- [x] **Step 3: Remove or bypass `applyWhereFilter` simulated logic**

Change production SELECT routing so WHERE predicates are evaluated by the planner/operator path or `ClusteredIndexScanner` predicate evaluation. Do not keep `i%2 == 0` behavior in any production method.

- [x] **Step 4: Re-run focused and baseline tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Select|Where|Clustered' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -count=1
```

- [x] **Step 5: Commit**

```bash
git add server/innodb/engine/select_executor.go server/innodb/engine/unified_executor.go server/innodb/engine/*select*_test.go
git commit -m "fix: remove simulated select filtering"
```

### Task 2: Make Storage-Integrated DML the Only Production DML Path

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/engine/dml_executor.go`
- Test: `server/innodb/engine/unified_executor_test.go`
- Test: `server/innodb/engine/storage_integrated_dml_executor_p0_test.go`

- [x] **Step 1: Add a failing test for missing manager behavior**

Create a test that constructs an executor without `storageManager` or `tableStorageManager`, runs INSERT/UPDATE/DELETE, and expects a structured error instead of fallback to `DMLExecutor`.

- [x] **Step 2: Run the focused DML tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'DML|StorageIntegrated|ExecuteInsert|ExecuteUpdate|ExecuteDelete' -count=1
```

- [x] **Step 3: Remove silent fallback in `executor.go`**

Replace fallback construction of `NewDMLExecutor` with a clear validation error when production managers are missing. Keep `DMLExecutor` only for parser/unit helper tests if still needed.

- [x] **Step 4: Re-run JDBC DML**

```bash
mvn test -Dtest=DMLOperationsTest
```

- [x] **Step 5: Commit**

```bash
git add server/innodb/engine/executor.go server/innodb/engine/dml_executor.go server/innodb/engine/*dml*_test.go server/innodb/engine/unified_executor_test.go
git commit -m "refactor: require storage integrated dml path"
```

### Task 3: Require Real Transaction Semantics for Production DML

**Files:**
- Modify: `server/innodb/engine/storage_integrated_dml_helper.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Test: `server/innodb/engine/transaction_integration_test.go`

- [x] **Step 1: Add failing tests for nil transaction manager**

Add tests that production INSERT/UPDATE/DELETE with missing `txManager` either uses an explicit autocommit implementation with WAL/undo boundaries or returns a structured error. The test must reject timestamp-only fake transaction IDs.

- [x] **Step 2: Run transaction tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Transaction|Rollback|StorageIntegrated' -count=1
```

- [x] **Step 3: Remove simplified transaction continuation**

Update `beginStorageTransaction` so production DML cannot silently proceed without `txManager`. If autocommit is kept, implement it through the real transaction manager.

- [x] **Step 4: Re-run engine and JDBC tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -count=1
mvn test -Dtest=DMLOperationsTest
```

- [x] **Step 5: Commit**

```bash
git add server/innodb/engine/storage_integrated_dml_helper.go server/innodb/engine/storage_integrated_dml_executor.go server/innodb/engine/transaction_integration_test.go
git commit -m "fix: require real transactions for storage dml"
```

### Task 4: Unify Clustered Record and B+Tree Page Persistence

**Files:**
- Modify: `server/innodb/engine/record_codec.go`
- Modify: `server/innodb/manager/enhanced_btree_index.go`
- Modify: `server/innodb/manager/enhanced_btree_adapter.go`
- Modify: `server/innodb/engine/clustered_index_scanner.go`
- Test: `server/innodb/engine/clustered_index_scanner_test.go`
- Test: `server/innodb/manager/enhanced_btree_index_test.go`

- [x] **Step 1: Add restart and page-parse tests**

Add tests that insert multiple rows, flush, restart/reload, scan from pages, and assert all rows are present without reading sidecar JSON.

- [x] **Step 2: Run storage tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -run 'Clustered|BTree|Persistence|Restart' -count=1
```

- [x] **Step 3: Choose the canonical persisted format**

Use one record block for clustered row payloads and B+Tree scan. If `XIR1` remains the row codec, B+Tree pages must store and scan `XIR1` payloads directly. Do not persist the same records through sidecar JSON.

- [x] **Step 4: Remove sidecar fallback**

Delete or isolate `loadAllIndexRecordsSidecars`, `loadIndexRecordsSidecar`, `saveIndexRecordsSidecar`, and sidecar delete fallback from production scan/delete paths. Keep migration cleanup only if needed for old local data.

- [x] **Step 5: Re-run restart and JDBC tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -count=1
mvn test -Dtest=DMLOperationsTest
mvn test -Pjdbc-connectivity
```

- [x] **Step 6: Commit**

```bash
git add server/innodb/engine/record_codec.go server/innodb/engine/clustered_index_scanner.go server/innodb/manager/enhanced_btree_index.go server/innodb/manager/enhanced_btree_adapter.go server/innodb/engine/*clustered*_test.go server/innodb/manager/*btree*_test.go
git commit -m "refactor: unify clustered btree persistence format"
```

### Task 5: Isolate Legacy B+Tree Implementation

**Files:**
- Modify: `server/innodb/manager/bplus_tree_manager.go`
- Modify: `server/innodb/manager/table_storage_mapping.go`
- Modify: `server/innodb/manager/enhanced_btree_adapter.go`
- Test: `server/innodb/manager/btree_improvements_test.go`

- [x] **Step 1: Add production-construction assertions**

Add tests that table storage mapping creates the selected production B+Tree implementation and does not accidentally return the legacy manager for user tables.

- [x] **Step 2: Run B+Tree tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager -run 'BTree|TableStorage' -count=1
```

- [x] **Step 3: Mark legacy manager boundary**

Either remove legacy production wiring or add explicit package comments and constructor names showing that it is test/legacy only.

- [x] **Step 4: Re-run manager and engine tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager ./server/innodb/engine -count=1
```

- [x] **Step 5: Commit**

```bash
git add server/innodb/manager/bplus_tree_manager.go server/innodb/manager/table_storage_mapping.go server/innodb/manager/enhanced_btree_adapter.go server/innodb/manager/*btree*_test.go
git commit -m "refactor: isolate legacy btree manager"
```

### Task 6: Remove Runtime Auth Shortcuts

**Files:**
- Modify: `server/dispatcher/enhanced_message_handler.go`
- Modify: `server/auth/password_validator.go`
- Modify: `server/auth/engine_access.go`
- Test: `server/dispatcher`
- Test: `server/auth`

- [x] **Step 1: Add negative auth tests**

Add tests that non-root users cannot read `mysql.user` without privilege, root bypass is not used for missing privilege, and all required privileges are checked.

- [x] **Step 2: Run auth tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher ./server/auth -count=1
```

- [x] **Step 3: Move shortcuts behind explicit test mode**

Remove hardcoded `mysql.user` responses and root bypass from normal runtime. If a shortcut is needed for tests, guard it behind a test-only constructor or config flag that defaults off.

- [x] **Step 4: Re-run protocol/JDBC connectivity**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher ./server/auth ./server/net -count=1
mvn test -Pjdbc-connectivity
```

- [x] **Step 5: Commit**

```bash
git add server/dispatcher/enhanced_message_handler.go server/auth/password_validator.go server/auth/engine_access.go server/dispatcher/*test.go server/auth/*test.go
git commit -m "fix: remove runtime auth shortcuts"
```

## Final Regression Gate

Run this before claiming the cleanup work complete:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager ./server/dispatcher ./server/auth ./server/net -count=1
mvn test -Dtest=DMLOperationsTest
mvn test -Pjdbc-connectivity
git diff --check
```

## Completion Evidence

- Task 1-2: `6df0386 fix: remove legacy select and dml fallbacks`
- Task 3: `5dae75c fix: require real storage dml transactions`
- Task 4: `7791d10 refactor: remove btree sidecar fallback`
- Task 5: `13554c6 refactor: isolate legacy btree manager`
- Task 6: `fee0709 fix: remove runtime auth shortcuts`
- Follow-up auth storage lookup closure: `b788e1a fix: complete mysql user btree lookup`

Latest verified regression commands:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager ./server/innodb/engine ./server/auth ./server/dispatcher ./server/net -count=1
git diff --check
```
