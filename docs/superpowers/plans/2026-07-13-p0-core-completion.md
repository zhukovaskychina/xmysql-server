# XMySQL P0 Core Completion Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining P0 correctness gaps for DML, checkpoint write blocking, buffer pool eviction, Enhanced B+Tree rebuild/drop, SHOW SQL wiring, and repeatable P0 validation.

**Architecture:** Keep changes narrow and test-first. Prefer extending existing managers and executors over introducing new storage layers. Every task must produce a runnable, isolated verification command and must not rely on full-package `go test ./server/innodb/engine` as the only signal.

**Tech Stack:** Go 1.24.3 (`/Users/zhukovasky/sdk/go1.24.3/bin/go`), existing InnoDB engine packages, existing sqlparser, existing metadata/manager/buffer_pool modules, shell scripts under `scripts/`.

---

## Current P0 Status

Already has minimum implementation from the previous pass:

- `server/innodb/engine/dml_operators.go`: `parseInsertRows()` parses `INSERT ... VALUES`.
- `server/innodb/engine/storage_integrated_dml_helper.go`: no-WHERE UPDATE/DELETE helper returns explicit error; row delete no longer clears the whole page.
- `server/innodb/engine/show_executor.go`: SHOW executor can read from `metadata.InfoSchemaManager`.
- `server/innodb/engine/storage_integrated_checkpoint.go`: Sharp Checkpoint has a write gate API and releases it on success/error.
- `server/innodb/buffer_pool/buffer_lru_optimized.go`: optimized LRU `Evict()` returns correct page identity and can fall back to young list.
- `server/innodb/manager/buffer_pool_manager_optimized.go`: optimized manager counts LRU evictions.
- `server/innodb/manager/enhanced_btree_manager.go`: `RebuildIndex()` and `DropIndex()` are no longer direct stubs.
- `scripts/verify_p0_core.sh`: runs the current narrow P0 regression tests.

Still P0 incomplete:

- `InsertOperator`, `UpdateOperator`, and `DeleteOperator` in `dml_operators.go` still contain non-writing simplified implementations.
- `StorageIntegratedDMLExecutor` still has write/read format consistency, update overwrite semantics, and nil-manager safety gaps.
- Checkpoint write gate is not yet called by all storage write paths.
- `server/innodb/manager/buffer_pool_manager.go::evictPage()` is still TODO.
- Enhanced B+Tree rebuild is reload/stat-refresh, not rebuild-from-table-data; DropIndex page freeing is incomplete for non-trivial trees.
- SHOW executor needs SQL-entry wiring verification for `schemaName/tableName/current DB`.
- P0 test split is not complete; `verify_p0_core.sh` is only a core smoke suite.

---

## File Map

### DML

- Modify: `server/innodb/engine/dml_operators.go`
  - Responsibility: Volcano-style INSERT/UPDATE/DELETE operators.
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
  - Responsibility: Storage-integrated DML implementation.
- Modify: `server/innodb/engine/storage_integrated_dml_helper.go`
  - Responsibility: row serialization/deserialization, page-slot helpers, row delete/update helpers.
- Create/extend tests:
  - `server/innodb/engine/dml_operators_p0_test.go`
  - `server/innodb/engine/storage_integrated_dml_executor_p0_test.go`
  - Existing: `server/innodb/engine/dml_operators_insert_parse_test.go`
  - Existing: `server/innodb/engine/storage_integrated_dml_helper_p0_test.go`

### Checkpoint

- Modify: `server/innodb/engine/storage_integrated_checkpoint.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Possibly modify: `server/innodb/engine/storage_integrated_persistence.go`
- Test: `server/innodb/engine/storage_integrated_checkpoint_p0_test.go`

### Buffer Pool

- Modify: `server/innodb/manager/buffer_pool_manager.go`
- Possibly modify: `server/innodb/buffer_pool/buffer_pool.go`
- Test: `server/innodb/manager/buffer_pool_manager_p0_test.go`

### Enhanced B+Tree

- Modify: `server/innodb/manager/enhanced_btree_manager.go`
- Modify: `server/innodb/manager/enhanced_btree_index.go`
- Possibly modify: `server/innodb/manager/index_metadata.go`
- Test: `server/innodb/manager/enhanced_btree_manager_p0_test.go`

### SHOW

- Modify: `server/innodb/engine/show_executor.go`
- Modify: `server/innodb/engine/executor.go` or `server/innodb/engine/enginx.go` if SHOW builder does not pass current DB/table.
- Test: `server/innodb/engine/show_executor_p0_test.go`

### Validation

- Modify: `scripts/verify_p0_core.sh`
- Create: `scripts/verify_p0_engine_suites.sh`
- Create: `docs/superpowers/plans/P0_ENGINE_TEST_SPLIT_MANIFEST.md`

---

## Chunk 1: DML Operators Must Perform Real Writes

### Task 1: INSERT operator writes one row through storage adapter or storage-integrated DML

**Files:**

- Modify: `server/innodb/engine/dml_operators.go`
- Test: `server/innodb/engine/dml_operators_p0_test.go`

- [ ] **Step 1: Write the failing INSERT test**

Add a test that builds an `InsertOperator` with a fake storage adapter and asserts:

- parsed row is sent to the write path exactly once;
- returned affected rows is 1;
- insert error propagates and does not report success.

Example target behavior:

```go
func TestInsertOperatorExecuteWritesParsedRows(t *testing.T) {
    // fake adapter records inserted rows
    // stmt: INSERT INTO users (id, name) VALUES (1, 'alice')
    // assert one persisted row: id=1, name="alice"
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
GO_BIN=${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}
$GO_BIN test ./server/innodb/engine -run 'TestInsertOperatorExecuteWritesParsedRows' -count=1 -v
```

Expected: FAIL because `insertRow()` only logs and returns `nil`.

- [ ] **Step 3: Implement the minimal write path**

Implementation requirements:

- Resolve schema from real metadata if available.
- Validate row against schema:
  - unknown column is error;
  - missing NOT NULL column without default is error;
  - type conversion failure is error.
- Convert row to the storage adapter row format already used elsewhere in engine.
- Call actual storage insertion path.
- Return explicit errors; do not log-only.

- [ ] **Step 4: Add duplicate key test**

Test:

```go
func TestInsertOperatorDuplicateKeyReturnsError(t *testing.T) {
    // fake primary key index says id=1 already exists
    // assert duplicate error is returned
}
```

- [ ] **Step 5: Implement `findDuplicateRecord()`**

Minimum behavior:

- Check primary key.
- Check unique indexes present in schema metadata.
- Return the conflicting record for ON DUPLICATE KEY UPDATE.
- Return `basic.ErrDuplicateKey` or SQL duplicate error for plain INSERT.

- [ ] **Step 6: Run INSERT tests**

Run:

```bash
$GO_BIN test ./server/innodb/engine -run 'TestInsertOperator(ParseInsertRows|ExecuteWritesParsedRows|DuplicateKey)' -count=1 -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/innodb/engine/dml_operators.go server/innodb/engine/dml_operators_p0_test.go server/innodb/engine/dml_operators_insert_parse_test.go
git commit -m "fix: implement p0 insert operator write path"
```

### Task 2: UPDATE operator applies SET and persists target rows

**Files:**

- Modify: `server/innodb/engine/dml_operators.go`
- Test: `server/innodb/engine/dml_operators_p0_test.go`

- [ ] **Step 1: Write failing SET application test**

Test:

```go
func TestUpdateOperatorApplySetClauseChangesTargetColumn(t *testing.T) {
    // old row: id=1, name="alice"
    // SET name='bob'
    // assert new row has name="bob"
}
```

- [ ] **Step 2: Run it**

```bash
$GO_BIN test ./server/innodb/engine -run 'TestUpdateOperatorApplySetClauseChangesTargetColumn' -count=1 -v
```

Expected: FAIL because `applySetClause()` returns old values.

- [ ] **Step 3: Implement SET expression evaluation**

Minimum behavior:

- Support literal assignment: string, int, float, NULL, bool.
- Support simple arithmetic where current evaluator already supports it.
- Resolve column indexes by schema column names.
- Return explicit error for unsupported expression.

- [ ] **Step 4: Write failing persistence test**

Test:

```go
func TestUpdateOperatorPersistsUpdatedRecord(t *testing.T) {
    // fake scan returns one row
    // fake storage records update call
    // assert updated row is passed to write path
}
```

- [ ] **Step 5: Implement `updateInPlace()` and `updateWithIndexChange()`**

Minimum behavior:

- For non-index column changes: update target physical record.
- For index column changes: delete old index entries, write new record, insert new index entries.
- Write undo metadata if the existing transaction/undo interface is present; otherwise return a clear unsupported error rather than silently succeeding.

- [ ] **Step 6: Run UPDATE tests**

```bash
$GO_BIN test ./server/innodb/engine -run 'TestUpdateOperator' -count=1 -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/innodb/engine/dml_operators.go server/innodb/engine/dml_operators_p0_test.go
git commit -m "fix: implement p0 update operator semantics"
```

### Task 3: DELETE operator marks only target records deleted

**Files:**

- Modify: `server/innodb/engine/dml_operators.go`
- Test: `server/innodb/engine/dml_operators_p0_test.go`

- [ ] **Step 1: Write failing delete test**

Test:

```go
func TestDeleteOperatorDeletesOnlyScannedRecords(t *testing.T) {
    // scan returns row id=1 only
    // fake storage records deleted row IDs
    // assert only id=1 deleted
}
```

- [ ] **Step 2: Run it**

```bash
$GO_BIN test ./server/innodb/engine -run 'TestDeleteOperatorDeletesOnlyScannedRecords' -count=1 -v
```

Expected: FAIL because `deleteRecord()` only logs.

- [ ] **Step 3: Implement `deleteRecord()`**

Minimum behavior:

- Resolve physical row location from record metadata or index lookup.
- Mark delete flag only for target record.
- Delete related secondary index entries.
- Do not physically wipe page content.

- [ ] **Step 4: Run DELETE tests**

```bash
$GO_BIN test ./server/innodb/engine -run 'TestDeleteOperator' -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/innodb/engine/dml_operators.go server/innodb/engine/dml_operators_p0_test.go
git commit -m "fix: implement p0 delete operator semantics"
```

---

## Chunk 2: StorageIntegratedDML Consistency

### Task 4: Unify insert page format with row-slot read/delete format

**Files:**

- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/engine/storage_integrated_dml_helper.go`
- Test: `server/innodb/engine/storage_integrated_dml_executor_p0_test.go`

- [ ] **Step 1: Write failing round-trip test**

Test:

```go
func TestStorageIntegratedDMLInsertCanBeReadBackByPageSlot(t *testing.T) {
    // insert row via insertRowToStorage()
    // btree search returns pageNo, slot
    // readRowFromStorage(pageNo, slot) returns inserted values
}
```

- [ ] **Step 2: Run it**

```bash
$GO_BIN test ./server/innodb/engine -run 'TestStorageIntegratedDMLInsertCanBeReadBackByPageSlot' -count=1 -v
```

Expected: FAIL if insert stores bytes in a format `readRowFromStorage()` cannot decode.

- [ ] **Step 3: Implement shared row collection write helper**

Add or reuse helpers:

```go
func appendDMLPageRow(content []byte, rowData []byte) ([]byte, int, error)
func replaceDMLPageRow(content []byte, slot int, rowData []byte) ([]byte, error)
```

Rules:

- Existing legacy single-row page is converted to row collection.
- Append returns slot.
- Replace preserves other rows.
- Deleted rows remain readable only as explicit deleted error.

- [ ] **Step 4: Update `insertRowToStorage()`**

Requirements:

- Use the shared helper to write page content.
- Update B+Tree with key -> page/slot mapping compatible with `Search()`.
- Flush only if managers are non-nil; nil manager is explicit error for required managers.

- [ ] **Step 5: Run helper and executor tests**

```bash
$GO_BIN test ./server/innodb/engine -run 'TestStorageIntegratedDMLExecutor_(InsertCanBeReadBackByPageSlot|MarkRowDeletedInPageContentPreservesOtherRows|DataSerialization)' -count=1 -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/innodb/engine/storage_integrated_dml_executor.go server/innodb/engine/storage_integrated_dml_helper.go server/innodb/engine/storage_integrated_dml_executor_p0_test.go server/innodb/engine/storage_integrated_dml_helper_p0_test.go
git commit -m "fix: unify storage integrated dml row format"
```

### Task 5: Update/delete index synchronization must use old and new row values

**Files:**

- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/engine/storage_integrated_dml_helper.go`
- Test: `server/innodb/engine/storage_integrated_dml_executor_p0_test.go`

- [ ] **Step 1: Write failing index update test**

Test:

```go
func TestStorageIntegratedDMLUpdateIndexesUsesOldAndNewValues(t *testing.T) {
    // old: email=a@example.com
    // update: email=b@example.com
    // fake index manager asserts old and new maps are different and correct
}
```

- [ ] **Step 2: Run it**

```bash
$GO_BIN test ./server/innodb/engine -run 'TestStorageIntegratedDMLUpdateIndexesUsesOldAndNewValues' -count=1 -v
```

Expected: FAIL if old values are reconstructed from incomplete row info.

- [ ] **Step 3: Preserve old row data in `RowUpdateInfo`**

Minimum behavior:

- `findRowsToUpdateInStorage()` fills old column values from storage.
- `findRowsToDeleteInStorage()` fills values needed by secondary index delete.
- Update path computes new row values once and passes both old/new to index sync.

- [ ] **Step 4: Add nil-manager guard tests**

Tests:

```go
func TestStorageIntegratedDMLInsertFailsWhenBufferPoolMissing(t *testing.T)
func TestStorageIntegratedDMLIndexSyncFailsWhenIndexManagerMissing(t *testing.T)
```

- [ ] **Step 5: Implement explicit manager checks**

Rules:

- Required manager missing => return error.
- Optional persistence manager missing => log or skip only if durability is explicitly optional in that path.
- No nil pointer panics.

- [ ] **Step 6: Run DML executor tests**

```bash
$GO_BIN test ./server/innodb/engine -run 'TestStorageIntegratedDMLExecutor_' -count=1 -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/innodb/engine/storage_integrated_dml_executor.go server/innodb/engine/storage_integrated_dml_helper.go server/innodb/engine/storage_integrated_dml_executor_p0_test.go
git commit -m "fix: preserve row values for dml index sync"
```

---

## Chunk 3: Checkpoint Write Gate Integration

### Task 6: Call checkpoint write gate from DML write paths

**Files:**

- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/engine/storage_integrated_persistence.go` if page writes can bypass DML.
- Test: `server/innodb/engine/storage_integrated_checkpoint_p0_test.go`

- [ ] **Step 1: Write failing integration test**

Test:

```go
func TestDMLWriteWaitsForSharpCheckpointGate(t *testing.T) {
    // checkpoint gate blocked
    // start DML write with context
    // assert it does not proceed until gate unblocks
}
```

- [ ] **Step 2: Run it**

```bash
$GO_BIN test ./server/innodb/engine -run 'TestDMLWriteWaitsForSharpCheckpointGate' -count=1 -v
```

Expected: FAIL because DML write path does not call `WaitForWritePermit()`.

- [ ] **Step 3: Add checkpoint manager dependency to DML executor path**

Rules:

- Do not add a global variable.
- Pass `CheckpointManager` through existing engine/executor construction if already available.
- If unavailable in unit tests, allow nil only when the path does not require checkpoint coordination.

- [ ] **Step 4: Call `WaitForWritePermit(ctx)` before page mutation**

Call before:

- insert row page write;
- update row page write;
- delete mark page write;
- direct persistence page flush if it mutates page state.

- [ ] **Step 5: Add context cancellation test**

Test:

```go
func TestDMLWriteReturnsContextErrorWhenCheckpointGateTimesOut(t *testing.T)
```

- [ ] **Step 6: Run checkpoint tests**

```bash
$GO_BIN test ./server/innodb/engine -run 'Test(CheckpointManager_|DMLWrite.*Checkpoint)' -count=1 -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/innodb/engine/storage_integrated_checkpoint.go server/innodb/engine/storage_integrated_dml_executor.go server/innodb/engine/storage_integrated_checkpoint_p0_test.go
git commit -m "fix: enforce checkpoint write gate on dml writes"
```

### Task 7: Define concurrent Sharp Checkpoint behavior

**Files:**

- Modify: `server/innodb/engine/storage_integrated_checkpoint.go`
- Test: `server/innodb/engine/storage_integrated_checkpoint_p0_test.go`

- [ ] **Step 1: Write failing concurrent checkpoint test**

Choose the behavior: serialize or reject. Prefer reject to avoid unclear nested gates.

Test:

```go
func TestCheckpointManagerRejectsConcurrentSharpCheckpoint(t *testing.T)
```

- [ ] **Step 2: Implement state guard**

Add:

```go
sharpRunning bool
```

Guard under `cm.mutex` or a dedicated mutex. Return explicit error if another Sharp Checkpoint is running.

- [ ] **Step 3: Verify failure path clears state**

Test:

```go
func TestCheckpointManagerClearsSharpRunningAfterFailure(t *testing.T)
```

- [ ] **Step 4: Run tests**

```bash
$GO_BIN test ./server/innodb/engine -run 'TestCheckpointManager.*Sharp' -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/innodb/engine/storage_integrated_checkpoint.go server/innodb/engine/storage_integrated_checkpoint_p0_test.go
git commit -m "fix: define concurrent sharp checkpoint behavior"
```

---

## Chunk 4: Legacy BufferPoolManager Eviction

### Task 8: Implement `BufferPoolManager.evictPage()`

**Files:**

- Modify: `server/innodb/manager/buffer_pool_manager.go`
- Possibly modify: `server/innodb/buffer_pool/buffer_pool.go`
- Test: `server/innodb/manager/buffer_pool_manager_p0_test.go`

- [ ] **Step 1: Write failing clean-page eviction test**

Test:

```go
func TestBufferPoolManagerEvictPageRemovesCleanUnpinnedPage(t *testing.T)
```

Assert:

- page is removed from buffer pool;
- evictions stat increments;
- no flush occurs for clean page.

- [ ] **Step 2: Run it**

```bash
$GO_BIN test ./server/innodb/manager -run 'TestBufferPoolManagerEvictPageRemovesCleanUnpinnedPage' -count=1 -v
```

Expected: FAIL because `evictPage()` returns nil.

- [ ] **Step 3: Implement clean unpinned eviction**

Minimum implementation:

- Ask underlying buffer pool/LRU for a candidate.
- Skip pinned pages.
- Remove candidate from page table/LRU.
- Update stats.

If underlying buffer pool does not expose candidates cleanly, add a narrow method there:

```go
func (bp *BufferPool) EvictCandidate() (*buffer_pool.BufferBlock, error)
```

- [ ] **Step 4: Write dirty-page eviction test**

Test:

```go
func TestBufferPoolManagerEvictPageFlushesDirtyUnpinnedPage(t *testing.T)
```

Assert:

- dirty page is written before removal;
- flush and eviction stats increment;
- write error aborts eviction.

- [ ] **Step 5: Implement dirty flush before eviction**

Rules:

- Flush before remove.
- If flush fails, keep page cached and return nil/error.
- Do not clear dirty state until write succeeds.

- [ ] **Step 6: Write pinned-page test**

Test:

```go
func TestBufferPoolManagerEvictPageSkipsPinnedPages(t *testing.T)
```

- [ ] **Step 7: Run buffer pool tests**

```bash
$GO_BIN test ./server/innodb/manager -run 'TestBufferPoolManager.*Evict|TestOptimizedBufferPoolManagerCountsLRUSetEvictions' -count=1 -v
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add server/innodb/manager/buffer_pool_manager.go server/innodb/manager/buffer_pool_manager_p0_test.go
git commit -m "fix: implement p0 buffer pool eviction"
```

---

## Chunk 5: Enhanced B+Tree True Rebuild and Complete Drop

### Task 9: Rebuild index from source table rows

**Files:**

- Modify: `server/innodb/manager/enhanced_btree_manager.go`
- Possibly modify: `server/innodb/manager/enhanced_btree_index.go`
- Test: `server/innodb/manager/enhanced_btree_manager_p0_test.go`

- [ ] **Step 1: Write failing rebuild-with-data test**

Test:

```go
func TestEnhancedBTreeManagerRebuildIndexPreservesSearchResults(t *testing.T) {
    // create index
    // insert keys 1,2,3
    // rebuild
    // search keys 1,2,3 still succeeds with same values
}
```

- [ ] **Step 2: Run it**

```bash
$GO_BIN test ./server/innodb/manager -run 'TestEnhancedBTreeManagerRebuildIndexPreservesSearchResults' -count=1 -v
```

Expected: FAIL if rebuild only reloads metadata and does not reconstruct entries.

- [ ] **Step 3: Add rebuild source abstraction**

Do not hardwire table scanning into manager internals. Add a narrow source:

```go
type IndexRebuildSource interface {
    ScanIndexRows(ctx context.Context, metadata *IndexMetadata) ([]IndexRecord, error)
}
```

If no source is configured:

- return explicit error for rebuild-from-table;
- allow metadata-only reload only through a separate private helper, not as `RebuildIndex()` success.

- [ ] **Step 4: Implement safe rebuild flow**

Required order:

1. Read existing metadata.
2. Scan source rows into memory or a bounded temp stream.
3. Allocate new root/page set.
4. Build new index.
5. Validate search/count.
6. Swap metadata root page atomically under manager lock.
7. Mark old pages for free only after success.

Failure rule:

- old metadata and old loaded index remain usable if any rebuild step fails.

- [ ] **Step 5: Add failure-preserves-old-index test**

Test:

```go
func TestEnhancedBTreeManagerRebuildFailurePreservesOldIndex(t *testing.T)
```

- [ ] **Step 6: Run rebuild tests**

```bash
$GO_BIN test ./server/innodb/manager -run 'TestEnhancedBTreeManagerRebuild' -count=1 -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/innodb/manager/enhanced_btree_manager.go server/innodb/manager/enhanced_btree_manager_p0_test.go
git commit -m "fix: rebuild enhanced btree indexes from source rows"
```

### Task 10: DropIndex frees all index-owned pages

**Files:**

- Modify: `server/innodb/manager/enhanced_btree_manager.go`
- Modify: `server/innodb/manager/enhanced_btree_index.go`
- Test: `server/innodb/manager/enhanced_btree_manager_p0_test.go`

- [ ] **Step 1: Write failing multi-page drop test**

Test:

```go
func TestEnhancedBTreeManagerDropIndexFreesAllLeafAndInternalPages(t *testing.T)
```

Set up an index with root + multiple leaf pages. Assert all are freed.

- [ ] **Step 2: Run it**

```bash
$GO_BIN test ./server/innodb/manager -run 'TestEnhancedBTreeManagerDropIndexFreesAllLeafAndInternalPages' -count=1 -v
```

Expected: FAIL because current drop mostly covers root/leaf traversal and not complete page ownership.

- [ ] **Step 3: Implement page collection**

Add:

```go
func (idx *EnhancedBTreeIndex) CollectOwnedPages(ctx context.Context) ([]uint32, error)
```

Rules:

- Include root.
- Include internal pages.
- Include leaf chain.
- Include overflow pages if metadata/page format exposes them.
- Deduplicate pages.

- [ ] **Step 4: Use page collection in DropIndex**

Rules:

- Set metadata state to Dropping.
- Free pages.
- Remove loaded index cache entry.
- Remove metadata only after page free succeeds.
- On free failure, keep metadata with Dropping or Corrupted state and return error.

- [ ] **Step 5: Run drop tests**

```bash
$GO_BIN test ./server/innodb/manager -run 'TestEnhancedBTreeManagerDropIndex' -count=1 -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/innodb/manager/enhanced_btree_manager.go server/innodb/manager/enhanced_btree_index.go server/innodb/manager/enhanced_btree_manager_p0_test.go
git commit -m "fix: free all enhanced btree pages on drop"
```

---

## Chunk 6: SHOW SQL Entry Wiring

### Task 11: Verify executor passes current DB/table to ShowExecutor

**Files:**

- Modify: `server/innodb/engine/show_executor.go`
- Modify: `server/innodb/engine/executor.go` or `server/innodb/engine/enginx.go`
- Test: `server/innodb/engine/show_executor_p0_test.go`

- [ ] **Step 1: Write failing SQL-level SHOW TABLES test**

Test:

```go
func TestXMySQLExecutorShowTablesUsesCurrentDatabase(t *testing.T)
```

Assert:

- current DB is passed into `ShowExecutor.schemaName`;
- result column name is `Tables_in_<db>`;
- rows come from info schema for that DB.

- [ ] **Step 2: Run it**

```bash
$GO_BIN test ./server/innodb/engine -run 'TestXMySQLExecutorShowTablesUsesCurrentDatabase' -count=1 -v
```

Expected: FAIL if builder does not set schema name.

- [ ] **Step 3: Wire schema/table names**

Rules:

- `SHOW TABLES` uses current database unless explicit database is parsed.
- `SHOW COLUMNS FROM table` sets table name and current/explicit schema name.
- Missing current database returns explicit error.

- [ ] **Step 4: Add SQL-level SHOW COLUMNS test**

Test:

```go
func TestXMySQLExecutorShowColumnsPassesTableAndSchema(t *testing.T)
```

- [ ] **Step 5: Run SHOW tests**

```bash
$GO_BIN test ./server/innodb/engine -run 'Test.*Show|Test.*SHOW' -count=1 -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/innodb/engine/show_executor.go server/innodb/engine/executor.go server/innodb/engine/enginx.go server/innodb/engine/show_executor_p0_test.go
git commit -m "fix: wire show executor to sql metadata context"
```

---

## Chunk 7: P0 Verification Suite Split

### Task 12: Split engine P0 tests into explicit suites

**Files:**

- Modify: `scripts/verify_p0_core.sh`
- Create: `scripts/verify_p0_engine_suites.sh`
- Create: `docs/superpowers/plans/P0_ENGINE_TEST_SPLIT_MANIFEST.md`

- [ ] **Step 1: Create manifest**

Create `docs/superpowers/plans/P0_ENGINE_TEST_SPLIT_MANIFEST.md` with:

- suite name;
- package;
- regex;
- expected max runtime;
- what failure means;
- owner files.

- [ ] **Step 2: Create suite script**

Script behavior:

```bash
#!/usr/bin/env bash
set -euo pipefail
GO_BIN="${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}"

run_suite() {
  local name="$1"
  local pkg="$2"
  local regex="$3"
  echo "==> ${name}: ${pkg} ${regex}"
  "${GO_BIN}" test "${pkg}" -run "${regex}" -count=1 -timeout=60s -v
}
```

Suites:

- engine-dml-operators
- engine-storage-dml
- engine-show
- engine-checkpoint
- buffer-pool
- manager-btree

- [ ] **Step 3: Run script**

```bash
scripts/verify_p0_engine_suites.sh
```

Expected: PASS with output per suite.

- [ ] **Step 4: Replace/extend `verify_p0_core.sh`**

Make `verify_p0_core.sh` call the suite script or document why it remains a smaller smoke suite.

- [ ] **Step 5: Commit**

```bash
git add scripts/verify_p0_core.sh scripts/verify_p0_engine_suites.sh docs/superpowers/plans/P0_ENGINE_TEST_SPLIT_MANIFEST.md
git commit -m "test: split p0 verification suites"
```

---

## Final P0 Verification

After all chunks are complete, run:

```bash
GO_BIN=${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}
scripts/verify_p0_engine_suites.sh
scripts/verify_p0_core.sh
$GO_BIN test ./server/innodb/buffer_pool -count=1
$GO_BIN test ./server/innodb/manager -run 'Test.*BufferPool|Test.*EnhancedBTree|Test.*RebuildIndex|Test.*DropIndex' -count=1 -v
$GO_BIN test ./server/innodb/engine -run 'Test.*DML|Test.*Insert|Test.*Update|Test.*Delete|Test.*Show|Test.*Checkpoint' -count=1 -v
```

Do not use a bare `go test ./server/innodb/engine` as the only P0 gate until the slow/hanging tests are isolated.

---

## Completion Checklist

- [ ] INSERT writes real data and duplicate key behavior is tested.
- [ ] UPDATE applies SET and persists changed rows.
- [ ] DELETE marks only target records deleted.
- [ ] StorageIntegratedDML uses one row-slot format for insert/read/update/delete.
- [ ] Secondary index sync receives correct old/new row data.
- [ ] DML write paths call checkpoint write gate.
- [ ] Concurrent Sharp Checkpoint behavior is explicit and tested.
- [ ] Legacy `BufferPoolManager.evictPage()` is implemented and tested.
- [ ] Enhanced B+Tree `RebuildIndex()` rebuilds from source rows or returns explicit unsupported error without corrupting old index.
- [ ] Enhanced B+Tree `DropIndex()` frees all owned pages before metadata removal.
- [ ] SHOW SQL entry passes current schema/table to `ShowExecutor`.
- [ ] P0 suite scripts provide repeatable, bounded verification.

---

## Execution Notes

- Use `python3.12` for any helper scripts if Python is needed.
- Do not stage unrelated dirty files.
- Before each commit, run the task-specific test command.
- Before claiming final P0 completion, run the final verification commands and inspect full output.
- If a task reveals a broader architectural mismatch, stop and update this plan rather than patching around it silently.
