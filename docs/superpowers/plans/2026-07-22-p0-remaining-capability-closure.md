# XMySQL Remaining P0 Capability Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining production-blocking P0 gaps after the 2026-07-21 validation/reclaim work: JDBC transaction evidence, durable secondary-index SELECT, index rebuild/repair proof, composite/no-primary-key DML breadth, JDBC metadata/DDL edges, and release evidence.

**Architecture:** Start from the committed branch `codex/p0-important-gap-closure` at `b6d3ddf` so the current undo reclaim, index validate/compact, and page-stat fixes are present. Keep each P0 gap behind focused Go tests first, then prove the externally visible behavior with JDBC tests where the gap affects Connector/J. Do not introduce a new storage format or rewrite the executor; extend existing executor, storage adapter, index manager, and metadata paths.

**Tech Stack:** Go 1.24.3 from `/Users/zhukovasky/sdk/go1.24.3/bin/go`, Maven/JUnit under `jdbc_client`, MySQL Connector/J 8.0.33, existing SQL parser under `server/innodb/sqlparser`, existing engine packages under `server/innodb`.

## Global Constraints

- Do not use Alibaba-style jargon in docs, comments, commit messages, or test names.
- Use `/Users/zhukovasky/sdk/go1.24.3/bin/go` and `/Users/zhukovasky/sdk/go1.24.3/bin/gofmt` for Go commands.
- Use `python3.12` when a Python helper is needed.
- Use TDD: write a failing test, run it, implement the minimal code, rerun it, then run focused regression.
- Keep changes scoped to the listed files unless a failing test proves another call path owns the behavior.
- Preserve current focused baseline: `./server/innodb/engine ./server/innodb/manager ./server/innodb/plan`.
- Clean generated runtime files before every commit: `server/innodb/engine/data`, `server/innodb/manager/data/mysql/user.ibd`, `server/innodb/engine/testdata/innodb/mysql/user.ibd`, `server/innodb/manager/testdata/storage_opt/mysql/user.ibd`, `jdbc_client/target`.
- Every task ends with a local commit on the feature branch.

---

## Current Baseline

Source of truth before executing this plan:

- `codex/p0-important-gap-closure` commit `b6d3ddf` contains:
  - `UndoSpaceReclaimer.reclaimSpace()` implementation.
  - `IndexManager.ValidateIndex()` and `CompactIndex()` implementation.
  - InnoDB index page `PAGE_N_RECS` row-count parsing.
  - Updated `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`.
- Remaining P0 gaps:
  - JDBC transaction matrix: rollback, savepoint, autocommit, isolation, and multi-connection visibility.
  - Durable secondary-index SELECT path and optimizer/executor proof.
  - Index rebuild/repair with corruption-injection evidence.
  - No-primary-key indexed table DML and composite-primary-key update/delete.
  - JDBC metadata/DDL edge cases.
  - Production-readiness evidence bundle.

## File Structure

- Modify: `server/innodb/engine/executor.go`
  - Session transaction routing and JDBC statement dispatch.
- Modify: `server/innodb/engine/select_executor.go`
  - Secondary-index access-method selection for simple equality/range predicates.
- Modify: `server/innodb/engine/storage_adapter.go`
  - Primary-key lookup and row read helpers used after secondary-index scan.
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
  - Composite-primary-key UPDATE/DELETE and no-primary-key indexed table fallback.
- Modify: `server/innodb/engine/storage_integrated_index_helper.go`
  - Composite-key encoding helpers and durable index validation/rebuild hooks.
- Modify: `server/innodb/manager/index_manager.go`
  - Add explicit `RepairIndex(indexID uint64) error` and strengthen rebuild validation.
- Modify: `server/dispatcher/system_variable_engine.go`
  - Route JDBC metadata queries away from invalid system-variable handling.
- Modify: `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`
  - Mark items complete only with fresh evidence from this plan.
- Create: `server/innodb/engine/p0_transaction_jdbc_semantics_test.go`
- Create: `server/innodb/engine/p0_secondary_index_select_test.go`
- Create: `server/innodb/engine/p0_composite_pk_dml_test.go`
- Create: `server/innodb/manager/p0_index_repair_test.go`
- Create: `docs/planning/P0_EVIDENCE_RUN_20260722.md`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/TransactionTest.java`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/DDLOperationsTest.java`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/IndexAndConstraintTest.java`

---

### Task 0: Prepare the Execution Branch

**Files:**
- Modify: no source files

**Interfaces:**
- Consumes: `codex/p0-important-gap-closure` at commit `b6d3ddf`.
- Produces: isolated branch `codex/p0-remaining-capability-closure` with the 2026-07-21 P0 gap-closure work included.

- [ ] **Step 1: Create a fresh worktree from the latest P0 branch**

Run from `/Users/zhukovasky/GolandProjects/xmysql-server`:

```bash
git worktree add .worktrees/p0-remaining-capability-closure -b codex/p0-remaining-capability-closure codex/p0-important-gap-closure
cd .worktrees/p0-remaining-capability-closure
```

Expected:

```text
HEAD is now at b6d3ddf fix: close p0 validation and reclaim gaps
```

- [ ] **Step 2: Verify the starting baseline**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager ./server/innodb/plan -count=1
```

Expected:

```text
ok  	github.com/zhukovaskychina/xmysql-server/server/innodb/engine
ok  	github.com/zhukovaskychina/xmysql-server/server/innodb/manager
ok  	github.com/zhukovaskychina/xmysql-server/server/innodb/plan
```

- [ ] **Step 3: Commit only if branch preparation changed files**

Run:

```bash
git status --short
```

Expected: no source changes. Do not commit in this task if the tree is clean.

---

### Task 1: JDBC Transaction and MVCC Matrix

**Files:**
- Create: `server/innodb/engine/p0_transaction_jdbc_semantics_test.go`
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/TransactionTest.java`

**Interfaces:**
- Consumes: existing helper `mustExecSQL(t, executor, database, sql)` from engine tests.
- Consumes: existing session state methods `SetParamByName`, `GetParamByName`, `SetAutocommit`.
- Produces: transaction-scoped DML changes are visible inside the same session and reverted by `ROLLBACK`.
- Produces: `SAVEPOINT name` and `ROLLBACK TO SAVEPOINT name` restore only changes after the savepoint.

- [ ] **Step 1: Write failing Go tests for rollback and savepoint**

Create `server/innodb/engine/p0_transaction_jdbc_semantics_test.go`:

```go
package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestP0RollbackRestoresInsertedRows(t *testing.T) {
	executor := newTestExecutor(t)
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key, name varchar(50))")

	mustExecSQL(t, executor, "app", "set autocommit=0")
	mustExecSQL(t, executor, "app", "insert into accounts (id, name) values (1, 'alice')")
	require.Equal(t, 1, mustCountRows(t, executor, "app", "accounts"))

	mustExecSQL(t, executor, "app", "rollback")
	require.Equal(t, 0, mustCountRows(t, executor, "app", "accounts"))
}

func TestP0RollbackToSavepointKeepsEarlierRows(t *testing.T) {
	executor := newTestExecutor(t)
	mustExecSQL(t, executor, "app", "create table accounts (id int primary key, name varchar(50))")

	mustExecSQL(t, executor, "app", "set autocommit=0")
	mustExecSQL(t, executor, "app", "insert into accounts (id, name) values (1, 'alice')")
	mustExecSQL(t, executor, "app", "savepoint sp1")
	mustExecSQL(t, executor, "app", "insert into accounts (id, name) values (2, 'bob')")

	mustExecSQL(t, executor, "app", "rollback to savepoint sp1")
	mustExecSQL(t, executor, "app", "commit")
	require.Equal(t, 1, mustCountRows(t, executor, "app", "accounts"))
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestP0RollbackRestoresInsertedRows|TestP0RollbackToSavepointKeepsEarlierRows' -count=1
```

Expected before implementation: at least one test fails because row changes survive rollback or savepoint rollback.

- [ ] **Step 3: Implement transaction-scoped DML buffering**

In `server/innodb/engine/storage_integrated_dml_executor.go`, add a transaction operation log type near the executor type:

```go
type transactionDMLChange struct {
	tableName string
	kind      string
	before    map[string]interface{}
	after     map[string]interface{}
}
```

Wire DML write paths so `INSERT`, `UPDATE`, and `DELETE` append this change to the active session transaction before committing physical row state. Use the existing rollback helpers to replay inverse operations:

```go
func rollbackDMLChange(dml *StorageIntegratedDMLExecutor, change transactionDMLChange) error {
	switch change.kind {
	case "insert":
		return dml.deleteRowByValues(change.tableName, change.after)
	case "update":
		return dml.updateRowByValues(change.tableName, change.after, change.before)
	case "delete":
		return dml.insertRowByValues(change.tableName, change.before)
	default:
		return fmt.Errorf("unknown transaction DML change kind %s", change.kind)
	}
}
```

The helper names above must be implemented in this task with concrete row-value operations over existing storage APIs. The rollback operation must update secondary indexes through the same sync paths as normal DML.

- [ ] **Step 4: Implement savepoint slicing**

In `server/innodb/engine/executor.go`, store savepoint offsets in the session transaction state:

```go
type sessionSavepoint struct {
	name   string
	offset int
}
```

`SAVEPOINT sp1` records the current operation-log length. `ROLLBACK TO SAVEPOINT sp1` reverts changes from the end of the log down to the saved offset and truncates the log to that offset.

- [ ] **Step 5: Run Go transaction tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestP0RollbackRestoresInsertedRows|TestP0RollbackToSavepointKeepsEarlierRows|TestTransaction' -count=1
```

Expected: all selected tests pass.

- [ ] **Step 6: Run JDBC transaction suite**

Start the server with a temp config copied from `conf/jdbc_local.ini`, then run:

```bash
cd jdbc_client
mvn test -Dtest=TransactionTest
```

Expected:

```text
Tests run: 8, Failures: 0, Errors: 0, Skipped: 0
```

- [ ] **Step 7: Commit**

Run:

```bash
git add server/innodb/engine/p0_transaction_jdbc_semantics_test.go server/innodb/engine/executor.go server/innodb/engine/storage_integrated_dml_executor.go jdbc_client/src/test/java/com/xmysql/server/test/TransactionTest.java
git commit -m "fix: close p0 jdbc transaction semantics"
```

---

### Task 2: Durable Secondary-Index SELECT Path

**Files:**
- Create: `server/innodb/engine/p0_secondary_index_select_test.go`
- Modify: `server/innodb/engine/select_executor.go`
- Modify: `server/innodb/engine/volcano_executor.go`
- Modify: `server/innodb/engine/storage_adapter.go`
- Modify: `server/innodb/manager/index_manager.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/IndexAndConstraintTest.java`

**Interfaces:**
- Consumes: `IndexManager.RangeSearch(indexID uint64, startKey, endKey interface{}) ([]basic.Row, error)`.
- Consumes: `StorageAdapter.GetRecordByPrimaryKey(ctx context.Context, tableInfo *TableStorageInfo, primaryKey interface{})`.
- Produces: `SELECT ... WHERE indexed_col = ?` uses the durable secondary index to fetch primary keys and then reads clustered rows.

- [ ] **Step 1: Write failing Go test for secondary-index equality lookup**

Create `server/innodb/engine/p0_secondary_index_select_test.go`:

```go
package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestP0SelectUsesDurableSecondaryIndexAfterRestart(t *testing.T) {
	tmp := t.TempDir()
	first := newTestExecutorWithDataDir(t, tmp)
	mustExecSQL(t, first, "app", "create table users (id int primary key, email varchar(100), name varchar(50), index idx_email (email))")
	mustExecSQL(t, first, "app", "insert into users (id, email, name) values (1, 'a@example.com', 'alice')")
	mustExecSQL(t, first, "app", "insert into users (id, email, name) values (2, 'b@example.com', 'bob')")
	require.NoError(t, first.Close())

	second := newTestExecutorWithDataDir(t, tmp)
	rows := mustQueryRows(t, second, "app", "select id, name from users where email = 'b@example.com'")
	require.Equal(t, [][]interface{}{{int64(2), "bob"}}, rows)
	require.True(t, second.lastAccessPathWasSecondaryIndex("idx_email"))
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run TestP0SelectUsesDurableSecondaryIndexAfterRestart -count=1
```

Expected before implementation: query falls back to full scan or cannot prove `idx_email` access.

- [ ] **Step 3: Add access-path marker for testability**

In `server/innodb/engine/select_executor.go`, add an unexported field to `SelectExecutor`:

```go
lastAccessPath string
```

Set it in `chooseAccessMethod`:

```go
se.lastAccessPath = "table_scan"
```

When an index is selected:

```go
se.lastAccessPath = "secondary_index:" + index.Name
```

Expose it only through a test helper in `_test.go`, not production API.

- [ ] **Step 4: Implement index selection for simple predicates**

In `server/innodb/engine/select_executor.go`, match predicates of these forms:

```sql
indexed_col = literal
indexed_col >= literal
indexed_col <= literal
indexed_col between literal and literal
```

Resolve candidate indexes from table metadata. Choose a secondary index when:

```go
len(index.Columns) > 0 &&
strings.EqualFold(index.Columns[0].Name, predicate.Column) &&
index.State == manager.IndexStateActive
```

- [ ] **Step 5: Implement secondary-index row lookup**

In `server/innodb/engine/volcano_executor.go`, update `IndexScanOperator.fetchPrimaryKeys` so non-covering secondary-index scans return primary keys from durable index values and then call the existing clustered lookup path:

```go
rows, err := i.indexAdapter.RangeSearch(ctx, encodedStartKey, encodedEndKey)
if err != nil {
	return fmt.Errorf("secondary index range search failed: %w", err)
}
for _, row := range rows {
	pk, err := decodePrimaryKeyFromSecondaryIndexRow(row)
	if err != nil {
		return err
	}
	i.primaryKeys = append(i.primaryKeys, pk)
}
```

`decodePrimaryKeyFromSecondaryIndexRow` must accept the row/value format currently produced by `IndexManager.SyncSecondaryIndexesOnInsert`.

- [ ] **Step 6: Run Go and JDBC index tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -run 'TestP0SelectUsesDurableSecondaryIndexAfterRestart|TestSecondaryIndex|TestIndexScan' -count=1
cd jdbc_client && mvn test -Dtest=IndexAndConstraintTest
```

Expected: selected Go tests pass and JDBC index/constraint suite reports zero failures for supported index cases.

- [ ] **Step 7: Commit**

Run:

```bash
git add server/innodb/engine/p0_secondary_index_select_test.go server/innodb/engine/select_executor.go server/innodb/engine/volcano_executor.go server/innodb/engine/storage_adapter.go server/innodb/manager/index_manager.go jdbc_client/src/test/java/com/xmysql/server/test/IndexAndConstraintTest.java
git commit -m "fix: route select through durable secondary indexes"
```

---

### Task 3: Index Rebuild and Repair Evidence

**Files:**
- Create: `server/innodb/manager/p0_index_repair_test.go`
- Modify: `server/innodb/manager/index_manager.go`
- Modify: `server/innodb/engine/storage_integrated_index_helper.go`

**Interfaces:**
- Consumes: `IndexManager.ValidateIndex(indexID uint64) error`.
- Consumes: `IndexManager.RebuildIndex(indexID uint64) error`.
- Produces: `IndexManager.RepairIndex(indexID uint64) error`.
- Produces: repair restores leaf/page counters and rebuilds the B+Tree from clustered records when an index entry is stale or missing.

- [ ] **Step 1: Write failing corruption-injection tests**

Create `server/innodb/manager/p0_index_repair_test.go`:

```go
package manager

import "testing"

func TestP0ValidateIndexDetectsMissingSecondaryEntry(t *testing.T) {
	fixture := newIndexRepairFixture(t)
	indexID := fixture.createUsersEmailIndex()
	fixture.insertClusteredUser(1, "a@example.com")
	fixture.removeSecondaryEntry(indexID, "a@example.com")

	if err := fixture.indexManager.ValidateIndex(indexID); err == nil {
		t.Fatalf("ValidateIndex accepted index with missing secondary entry")
	}
}

func TestP0RepairIndexRebuildsMissingSecondaryEntry(t *testing.T) {
	fixture := newIndexRepairFixture(t)
	indexID := fixture.createUsersEmailIndex()
	fixture.insertClusteredUser(1, "a@example.com")
	fixture.removeSecondaryEntry(indexID, "a@example.com")

	if err := fixture.indexManager.RepairIndex(indexID); err != nil {
		t.Fatalf("RepairIndex failed: %v", err)
	}
	if err := fixture.indexManager.ValidateIndex(indexID); err != nil {
		t.Fatalf("ValidateIndex after repair failed: %v", err)
	}
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager -run 'TestP0ValidateIndexDetectsMissingSecondaryEntry|TestP0RepairIndexRebuildsMissingSecondaryEntry' -count=1
```

Expected before implementation: compile fails because `RepairIndex` is missing, or validation does not detect the injected missing entry.

- [ ] **Step 3: Add `RepairIndex`**

In `server/innodb/manager/index_manager.go`, add:

```go
func (im *IndexManager) RepairIndex(indexID uint64) error {
	if err := im.RebuildIndex(indexID); err != nil {
		return fmt.Errorf("rebuild index %d failed during repair: %w", indexID, err)
	}
	if err := im.ValidateIndex(indexID); err != nil {
		return fmt.Errorf("validate index %d failed after repair: %w", indexID, err)
	}
	return nil
}
```

Then extend `RebuildIndex` to repopulate keys from clustered records through `storage_integrated_index_helper.go`; it must not leave `KeyCount` at zero when source rows exist.

- [ ] **Step 4: Add row-source contract for rebuild**

In `server/innodb/engine/storage_integrated_index_helper.go`, expose a helper used by the repair path:

```go
func BuildSecondaryIndexEntries(tableMeta *metadata.TableMeta, rows []*InsertRowData, index *manager.Index) ([]manager.SecondaryIndexEntry, error)
```

The helper returns deterministic encoded keys using `manager.EncodeSecondaryIndexKey` and primary-key payloads using the existing primary-key encoder.

- [ ] **Step 5: Run manager regression**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager -run 'TestP0.*Index|TestEnhancedBTreeManagerRebuildIndex|TestValidateIndex|TestCompactIndex' -count=1
```

Expected: all selected manager tests pass.

- [ ] **Step 6: Commit**

Run:

```bash
git add server/innodb/manager/p0_index_repair_test.go server/innodb/manager/index_manager.go server/innodb/engine/storage_integrated_index_helper.go
git commit -m "fix: add p0 index repair evidence"
```

---

### Task 4: Composite Primary Key and No-Primary-Key DML Breadth

**Files:**
- Create: `server/innodb/engine/p0_composite_pk_dml_test.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/engine/storage_integrated_index_helper.go`
- Modify: `server/innodb/engine/storage_adapter.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/IndexAndConstraintTest.java`

**Interfaces:**
- Consumes: `generatePrimaryKeyFromRow(tableMeta, rowData)` and composite-key encoding helpers.
- Produces: composite-primary-key `UPDATE` and `DELETE` use the encoded composite key for lookup.
- Produces: indexed tables without a declared primary key use a stable hidden row identifier for DML and secondary-index payloads.

- [ ] **Step 1: Write failing tests for composite-primary-key DML**

Create `server/innodb/engine/p0_composite_pk_dml_test.go`:

```go
package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestP0CompositePrimaryKeyUpdateAndDelete(t *testing.T) {
	executor := newTestExecutor(t)
	mustExecSQL(t, executor, "app", "create table memberships (user_id int, group_id int, score int, primary key (user_id, group_id))")
	mustExecSQL(t, executor, "app", "insert into memberships values (1, 100, 5)")
	mustExecSQL(t, executor, "app", "update memberships set score = 9 where user_id = 1 and group_id = 100")
	require.Equal(t, [][]interface{}{{int64(9)}}, mustQueryRows(t, executor, "app", "select score from memberships where user_id = 1 and group_id = 100"))

	mustExecSQL(t, executor, "app", "delete from memberships where user_id = 1 and group_id = 100")
	require.Equal(t, 0, mustCountRows(t, executor, "app", "memberships"))
}
```

- [ ] **Step 2: Write failing test for indexed no-primary-key table**

Add to the same file:

```go
func TestP0IndexedTableWithoutPrimaryKeySupportsDML(t *testing.T) {
	executor := newTestExecutor(t)
	mustExecSQL(t, executor, "app", "create table events (tenant_id int, event_name varchar(50), index idx_tenant (tenant_id))")
	mustExecSQL(t, executor, "app", "insert into events values (10, 'created')")
	mustExecSQL(t, executor, "app", "update events set event_name = 'updated' where tenant_id = 10")
	require.Equal(t, [][]interface{}{{"updated"}}, mustQueryRows(t, executor, "app", "select event_name from events where tenant_id = 10"))
	mustExecSQL(t, executor, "app", "delete from events where tenant_id = 10")
	require.Equal(t, 0, mustCountRows(t, executor, "app", "events"))
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestP0CompositePrimaryKeyUpdateAndDelete|TestP0IndexedTableWithoutPrimaryKeySupportsDML' -count=1
```

Expected before implementation: failures include `unsupported composite primary key UPDATE`, `unsupported composite primary key DELETE`, or `unsupported indexed table without primary key`.

- [ ] **Step 4: Implement composite-primary-key lookup**

In `server/innodb/engine/storage_integrated_dml_executor.go`, replace the explicit composite-primary-key unsupported branches with:

```go
primaryKey, err := dml.generatePrimaryKeyFromRow(tableMeta, rowData)
if err != nil {
	return nil, err
}
```

Use the returned encoded key in UPDATE and DELETE lookup. The encoded composite key must match the insert path in `storage_integrated_index_helper.go`.

- [ ] **Step 5: Implement hidden row identifier for no-primary-key indexed tables**

In `server/innodb/engine/storage_integrated_index_helper.go`, add:

```go
func generateHiddenRowID(schemaName, tableName string, rowData map[string]interface{}) []byte {
	h := fnv.New64a()
	_, _ = h.Write([]byte(schemaName))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(tableName))
	_, _ = h.Write([]byte{0})
	keys := make([]string, 0, len(rowData))
	for key := range rowData {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		_, _ = h.Write([]byte(key))
		_, _ = h.Write([]byte("="))
		_, _ = h.Write([]byte(fmt.Sprint(rowData[key])))
		_, _ = h.Write([]byte{0})
	}
	return []byte(fmt.Sprintf("__xmysql_hidden_pk_%016x", h.Sum64()))
}
```

Use this hidden row id only when the table has no primary key. Secondary index payloads must store this id so UPDATE/DELETE can locate the clustered row consistently.

- [ ] **Step 6: Run Go and JDBC constraint tests**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestP0CompositePrimaryKeyUpdateAndDelete|TestP0IndexedTableWithoutPrimaryKeySupportsDML|TestStorageIntegrated.*Constraint' -count=1
cd jdbc_client && mvn test -Dtest=IndexAndConstraintTest
```

Expected: selected Go tests pass and JDBC supported constraints pass. FK/CHECK/FULLTEXT remain documented as unsupported unless implemented in a later task.

- [ ] **Step 7: Commit**

Run:

```bash
git add server/innodb/engine/p0_composite_pk_dml_test.go server/innodb/engine/storage_integrated_dml_executor.go server/innodb/engine/storage_integrated_index_helper.go server/innodb/engine/storage_adapter.go jdbc_client/src/test/java/com/xmysql/server/test/IndexAndConstraintTest.java
git commit -m "fix: support p0 composite and no primary key dml"
```

---

### Task 5: JDBC Metadata and Core DDL Edges

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/dispatcher/system_variable_engine.go`
- Modify: `server/innodb/manager/info_schema_manager.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/DDLOperationsTest.java`

**Interfaces:**
- Consumes: existing information-schema table metadata generator.
- Produces: `SHOW DATABASES LIKE`, `DatabaseMetaData.getTables()`, `DROP DATABASE IF EXISTS`, `TRUNCATE TABLE`, and core `ALTER TABLE ADD COLUMN` match JDBC expectations.

- [ ] **Step 1: Add or re-enable failing JDBC metadata assertions**

In `jdbc_client/src/test/java/com/xmysql/server/test/DDLOperationsTest.java`, ensure these assertions exist and are active:

```java
@Test
@DisplayName("SHOW DATABASES LIKE returns navigable result set")
public void testShowDatabasesLikeNavigable() throws SQLException {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("SHOW DATABASES LIKE 'test_%'")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString(1)).startsWith("test_");
    }
}

@Test
@DisplayName("DROP DATABASE IF EXISTS missing database succeeds")
public void testDropDatabaseIfExistsMissing() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
        assertThatCode(() -> stmt.executeUpdate("DROP DATABASE IF EXISTS missing_p0_db"))
            .doesNotThrowAnyException();
    }
}

@Test
@DisplayName("DatabaseMetaData getTables sees created table")
public void testDatabaseMetaDataGetTables() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS p0_meta_db");
        stmt.executeUpdate("USE p0_meta_db");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS meta_users (id INT PRIMARY KEY, name VARCHAR(50))");
    }
    DatabaseMetaData meta = connection.getMetaData();
    try (ResultSet rs = meta.getTables(null, "p0_meta_db", "meta_users", new String[]{"TABLE"})) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("TABLE_NAME")).isEqualTo("meta_users");
    }
}
```

- [ ] **Step 2: Run JDBC DDL suite and verify failure**

Run:

```bash
cd jdbc_client
mvn test -Dtest=DDLOperationsTest
```

Expected before implementation: current metadata/DDL gaps fail.

- [ ] **Step 3: Route `information_schema.tables` queries correctly**

In `server/dispatcher/system_variable_engine.go`, before the system-variable parser returns `invalid system variable query`, add a guard:

```go
func isInformationSchemaMetadataQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(query))
	return strings.Contains(lower, "information_schema.tables") ||
		strings.Contains(lower, "information_schema.columns") ||
		strings.Contains(lower, "information_schema.schemata")
}
```

When this function returns true, pass the query to the engine SQL path rather than the system-variable path.

- [ ] **Step 4: Normalize metadata result columns**

In `server/innodb/manager/info_schema_manager.go`, ensure `getTables` returns at least these columns with JDBC-compatible names:

```text
TABLE_CAT
TABLE_SCHEM
TABLE_NAME
TABLE_TYPE
REMARKS
```

For normal base tables, set:

```text
TABLE_TYPE = TABLE
REMARKS = ""
```

- [ ] **Step 5: Harden DROP/TRUNCATE/ALTER paths**

In `server/innodb/engine/executor.go`:

- `DROP DATABASE IF EXISTS missing_db` returns DDL OK without removing mappings.
- `TRUNCATE TABLE db.table` preserves the table metadata mapping and resets only row/index contents.
- `ALTER TABLE table ADD COLUMN col type` updates dictionary metadata and leaves existing rows readable with `NULL` for the new column.

- [ ] **Step 6: Run DDL and prepared statement suites**

Run:

```bash
cd jdbc_client
mvn test -Dtest=DDLOperationsTest,PreparedStatementTest,SystemVariableTest
```

Expected:

```text
Failures: 0, Errors: 0
```

- [ ] **Step 7: Commit**

Run:

```bash
git add server/innodb/engine/executor.go server/dispatcher/system_variable_engine.go server/innodb/manager/info_schema_manager.go jdbc_client/src/test/java/com/xmysql/server/test/DDLOperationsTest.java
git commit -m "fix: close p0 jdbc metadata ddl edges"
```

---

### Task 6: Production Evidence Bundle

**Files:**
- Create: `docs/planning/P0_EVIDENCE_RUN_20260722.md`
- Modify: `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`
- Modify: `docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md`

**Interfaces:**
- Consumes: evidence from Tasks 1-5.
- Produces: current-run evidence document with commands, dates, pass/fail status, and remaining accepted deferrals.

- [ ] **Step 1: Create evidence document**

Create `docs/planning/P0_EVIDENCE_RUN_20260722.md`:

```markdown
# P0 Evidence Run - 2026-07-22

## Scope

This evidence run covers the P0 remaining capability closure branch.

## Commands

| Area | Command | Result |
|---|---|---|
| Go focused baseline | `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager ./server/innodb/plan -count=1` | PASS |
| JDBC DML | `cd jdbc_client && mvn test -Dtest=DMLOperationsTest` | PASS |
| JDBC Prepared | `cd jdbc_client && mvn test -Dtest=PreparedStatementTest` | PASS |
| JDBC Transaction | `cd jdbc_client && mvn test -Dtest=TransactionTest` | PASS |
| JDBC DDL Metadata | `cd jdbc_client && mvn test -Dtest=DDLOperationsTest,SystemVariableTest` | PASS |
| JDBC Index/Constraint | `cd jdbc_client && mvn test -Dtest=IndexAndConstraintTest` | PASS for supported P0 cases |

## Remaining Deferrals

| Capability | Priority | Reason |
|---|---|---|
| Foreign key cascade enforcement | P1 | Not required for core CRUD/JDBC P0 exit after explicit sign-off |
| CHECK expression enforcement | P1 | Not required for core CRUD/JDBC P0 exit after explicit sign-off |
| FULLTEXT index | P* | Advanced MySQL feature outside core CRUD |
```

- [ ] **Step 2: Run final verification commands**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager ./server/innodb/plan -count=1
cd jdbc_client && mvn test -Dtest=DMLOperationsTest,PreparedStatementTest,TransactionTest,DDLOperationsTest,SystemVariableTest,IndexAndConstraintTest
git diff --check
```

Expected:

```text
Go packages: ok
Maven: Failures: 0, Errors: 0
git diff --check: no output
```

- [ ] **Step 3: Update P0 docs from evidence only**

In `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`, move a P0 item to closed language only if the command in `P0_EVIDENCE_RUN_20260722.md` passed in this task. Keep remaining unsupported items in P1/P* docs rather than claiming P0 completion.

- [ ] **Step 4: Commit**

Run:

```bash
git add docs/planning/P0_EVIDENCE_RUN_20260722.md docs/planning/P0_CAPABILITY_BACKLOG_20260715.md docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md
git commit -m "docs: record p0 closure evidence"
```

---

## Final Verification

After all tasks are complete, run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager ./server/innodb/plan -count=1
cd jdbc_client && mvn test -Dtest=DMLOperationsTest,PreparedStatementTest,TransactionTest,DDLOperationsTest,SystemVariableTest,IndexAndConstraintTest
git status --short
```

Expected final state:

```text
Go focused baseline: PASS
JDBC focused matrix: PASS
git status --short: clean
```

Do not claim P0 complete if:

- any JDBC suite above fails;
- restart read-back was not run after the DML/index changes;
- secondary-index SELECT cannot prove it used durable secondary-index state;
- rollback/savepoint behavior only passes Go tests but fails JDBC;
- generated runtime files remain in git status.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-22-p0-remaining-capability-closure.md`. Two execution options:

1. Subagent-Driven (recommended) - dispatch a fresh subagent per task, review between tasks, fast iteration.
2. Inline Execution - execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
