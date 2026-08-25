# XMySQL P0 Capability Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining P0 correctness gaps so JDBC CRUD, restart recovery, durable storage scans, indexes, transactions, and release evidence no longer depend on sidecar files or snapshot-only transaction behavior.

**Architecture:** Move the row source of truth to one canonical InnoDB-style clustered record format stored in durable B+Tree pages. Keep SQL/JDBC compatibility tests as the outer contract, and add page/index/transaction tests beneath it so each green JDBC result is backed by durable page, index, undo, and recovery evidence.

**Tech Stack:** Go 1.24.3 server code, Maven/JUnit JDBC compatibility suite, MySQL protocol implementation under `server/net`, storage/engine packages under `server/innodb`, Python 3.12 only for optional local evidence scripts.

---

## Current Baseline

- Branch: `dev`
- Verified merged commit: `454451b`
- Known local dirty file to ignore during implementation unless explicitly requested: `.idea/workspace.xml`
- Last verified JDBC baseline before this plan: full `jdbc_client` Maven test suite passed, including CRUD, prepared statements, DDL smoke paths, transactions, constraints, joins, and performance.
- Current risk: basic JDBC behavior is green, but storage and transaction correctness still have bypasses:
  - table row sidecar: `server/innodb/engine/table_rows_sidecar.go`
  - SELECT sidecar read path: `server/innodb/engine/select_executor.go`
  - DML sidecar write/update/delete path: `server/innodb/engine/storage_integrated_dml_executor.go`
  - B+Tree record sidecar fallback: `server/innodb/manager/enhanced_btree_index.go`
  - transaction snapshot/restore path: `server/innodb/engine/executor.go`
  - undo purge active snapshot and reclaim gaps: `server/innodb/manager/undo_purge.go`

## File Structure

- Modify `server/innodb/record/`: canonical clustered record encoding and decoding helpers.
- Modify `server/innodb/storage/wrapper/page/`: durable page record slot layout, page parse/dump helpers, checksum validation.
- Modify `server/innodb/manager/enhanced_btree_index.go`: remove JSON sidecar fallback, enforce page-contained record persistence.
- Modify `server/innodb/manager/enhanced_btree_adapter.go`: leaf-chain/range scan behavior and restart reconstruction.
- Modify `server/innodb/engine/storage_integrated_dml_executor.go`: write canonical records and update/delete through clustered B+Tree only.
- Modify `server/innodb/engine/storage_integrated_dml_helper.go`: scan/update/delete through clustered B+Tree only.
- Modify `server/innodb/engine/select_executor.go`: read from clustered B+Tree pages, not table sidecar.
- Delete or quarantine `server/innodb/engine/table_rows_sidecar.go` after replacement tests pass.
- Modify `server/innodb/engine/executor.go`: replace transaction data-directory snapshot behavior with undo/MVCC-backed behavior.
- Modify `server/innodb/manager/undo_purge.go`: active snapshot check and real reclaim bookkeeping.
- Modify `server/net/decoupled_handler.go`: P0 protocol gaps for JDBC long data/reset/fetch behavior where required.
- Add Go tests under `server/innodb/engine`, `server/innodb/manager`, and `server/innodb/storage/wrapper/page`.
- Add JDBC tests under `jdbc_client/src/test/java/com/xmysql/server/test`.
- Update `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`, `docs/planning/P0_CURRENT_STATUS_SUMMARY.md`, and `docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md` after implementation evidence is generated.

---

### Task 1: Freeze the P0 Regression Baseline

**Files:**
- Create: `server/innodb/engine/p0_storage_contract_test.go`
- Create: `jdbc_client/src/test/java/com/xmysql/server/test/P0DurabilityContractTest.java`
- Modify: `docs/planning/P0_CURRENT_STATUS_SUMMARY.md`

- [ ] **Step 1: Add Go contract tests that fail on sidecar dependence**

Create `server/innodb/engine/p0_storage_contract_test.go` with tests that insert rows through the storage-integrated DML path, remove table-row sidecar files, restart engine state, and assert rows are still readable from durable pages.

```go
package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestP0SelectDoesNotDependOnTableRowsSidecar(t *testing.T) {
	dataDir := t.TempDir()
	h := newP0EngineHarness(t, dataDir)
	h.Exec(t, "CREATE DATABASE p0_sidecar")
	h.Exec(t, "USE p0_sidecar")
	h.Exec(t, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT)")
	h.Exec(t, "INSERT INTO users(id, name, age) VALUES (1, 'alice', 20), (2, 'bob', 30)")

	sidecar := filepath.Join(dataDir, "p0_sidecar", "users.xrows.json")
	if err := os.Remove(sidecar); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove sidecar: %v", err)
	}

	h.Restart(t)
	rows := h.QueryRows(t, "SELECT id, name, age FROM users ORDER BY id")
	assertP0Rows(t, rows, [][]any{{1, "alice", 20}, {2, "bob", 30}})
}
```

- [ ] **Step 2: Add JDBC restart contract tests**

Create `jdbc_client/src/test/java/com/xmysql/server/test/P0DurabilityContractTest.java`.

```java
package com.xmysql.server.test;

import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

class P0DurabilityContractTest extends BaseIntegrationTest {
    @Test
    void crudSurvivesServerRestartWithoutSidecarRows() throws Exception {
        try (Connection c = getConnection()) {
            try (Statement s = c.createStatement()) {
                s.execute("CREATE DATABASE IF NOT EXISTS p0_durable");
                s.execute("USE p0_durable");
                s.execute("DROP TABLE IF EXISTS users");
                s.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT)");
                s.execute("INSERT INTO users VALUES (1, 'alice', 20), (2, 'bob', 30)");
            }
        }

        restartServer();

        try (Connection c = getConnection()) {
            try (Statement s = c.createStatement()) {
                s.execute("USE p0_durable");
                try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM users")) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                }
            }
        }
    }
}
```

- [ ] **Step 3: Run tests and capture expected failures**

Run:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run TestP0SelectDoesNotDependOnTableRowsSidecar -count=1
cd jdbc_client && mvn test -Dtest=P0DurabilityContractTest
```

Expected before implementation: at least one test fails because SELECT/DML still relies on `users.xrows.json` or helper harness methods are missing.

- [ ] **Step 4: Commit only the failing tests**

```bash
git add server/innodb/engine/p0_storage_contract_test.go jdbc_client/src/test/java/com/xmysql/server/test/P0DurabilityContractTest.java docs/planning/P0_CURRENT_STATUS_SUMMARY.md
git commit -m "test: add p0 durability contract coverage"
```

---

### Task 2: Make Clustered Records the Single Row Format

**Files:**
- Modify: `server/innodb/record/row_cluster_index_leaf_row.go`
- Modify: `server/innodb/storage/wrapper/page/index_page_wrapper.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/engine/storage_integrated_dml_helper.go`
- Modify: `server/innodb/engine/select_executor.go`
- Test: `server/innodb/storage/wrapper/page/clustered_record_format_test.go`

- [ ] **Step 1: Add format round-trip tests**

Create `server/innodb/storage/wrapper/page/clustered_record_format_test.go`.

```go
package page

import "testing"

func TestClusteredRecordFormatRoundTrip(t *testing.T) {
	record := ClusteredRecord{
		PrimaryKey: []byte{0, 0, 0, 1},
		Columns: [][]byte{
			[]byte("alice"),
			[]byte{0, 0, 0, 20},
		},
		TransactionID: 101,
		RollPointer:  202,
		DeleteMarked: false,
	}

	encoded, err := EncodeClusteredRecord(record)
	if err != nil {
		t.Fatalf("encode clustered record: %v", err)
	}
	decoded, err := DecodeClusteredRecord(encoded)
	if err != nil {
		t.Fatalf("decode clustered record: %v", err)
	}
	if string(decoded.Columns[0]) != "alice" || decoded.TransactionID != 101 || decoded.RollPointer != 202 || decoded.DeleteMarked {
		t.Fatalf("decoded record mismatch: %+v", decoded)
	}
}
```

- [ ] **Step 2: Implement the canonical record API**

Add one exported API used by DML, SELECT, recovery, and tests:

```go
type ClusteredRecord struct {
	PrimaryKey    []byte
	Columns       [][]byte
	NullBitmap    []byte
	TransactionID uint64
	RollPointer   uint64
	DeleteMarked  bool
}

func EncodeClusteredRecord(record ClusteredRecord) ([]byte, error)
func DecodeClusteredRecord(data []byte) (ClusteredRecord, error)
```

Encoding rules:
- fixed header: magic `XICR`, version `1`, flags, transaction id, roll pointer;
- primary key length and bytes;
- null bitmap length and bytes;
- column count;
- per-column length and bytes;
- CRC32 over header and body.

- [ ] **Step 3: Route INSERT through `EncodeClusteredRecord`**

In `server/innodb/engine/storage_integrated_dml_executor.go`, replace custom row bytes written by `insertRowToStorage` with canonical clustered record bytes. Keep `appendTableRowSidecar` call removed from the normal path.

- [ ] **Step 4: Route SELECT through `DecodeClusteredRecord`**

In `server/innodb/engine/select_executor.go`, make durable page scan call `DecodeClusteredRecord` for every user record. Remove `decodeTableRowsSidecar` from the main scan path.

- [ ] **Step 5: Run format and CRUD tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/storage/wrapper/page ./server/innodb/engine -run 'TestClusteredRecordFormatRoundTrip|TestP0SelectDoesNotDependOnTableRowsSidecar' -count=1
```

Expected: both tests pass, and no `*.xrows.json` file is required for SELECT correctness.

- [ ] **Step 6: Commit**

```bash
git add server/innodb/record server/innodb/storage/wrapper/page server/innodb/engine
git commit -m "feat: use canonical clustered record format"
```

---

### Task 3: Close Clustered B+Tree Full Scan, Range Scan, and Restart

**Files:**
- Modify: `server/innodb/manager/enhanced_btree_adapter.go`
- Modify: `server/innodb/manager/enhanced_btree_index.go`
- Modify: `server/innodb/engine/select_executor.go`
- Test: `server/innodb/manager/clustered_btree_scan_test.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/P0ClusteredBTreeScanTest.java`

- [ ] **Step 1: Add leaf-chain and range tests**

Create `server/innodb/manager/clustered_btree_scan_test.go`.

```go
package manager

import "testing"

func TestClusteredBTreeFullAndRangeScanAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	tree := newP0ClusteredTree(t, dataDir, "p0_scan", "events")
	for i := 1; i <= 2500; i++ {
		tree.InsertIntKey(t, i, []byte{byte(i % 251)})
	}

	tree.Restart(t)

	full := tree.ScanAllKeys(t)
	if len(full) != 2500 || full[0] != 1 || full[2499] != 2500 {
		t.Fatalf("full scan mismatch: len=%d first=%d last=%d", len(full), full[0], full[len(full)-1])
	}

	ranged := tree.ScanIntRange(t, 900, 1100)
	if len(ranged) != 201 || ranged[0] != 900 || ranged[200] != 1100 {
		t.Fatalf("range scan mismatch: %+v", ranged[:min(5, len(ranged))])
	}
}
```

- [ ] **Step 2: Remove index record JSON fallback from normal reads**

In `server/innodb/manager/enhanced_btree_adapter.go`, remove successful reads from `readIndexRecordsSidecar` in production scan paths. Keep a migration-only helper behind an explicit test-only or recovery command if old artifacts must be imported.

- [ ] **Step 3: Enforce page-contained B+Tree records**

In `server/innodb/manager/enhanced_btree_index.go`, change oversized record handling from sidecar write to page split or overflow-page reference. P0 accepts overflow pages only when they are inside the tablespace and covered by checksum/restart tests.

- [ ] **Step 4: Add JDBC range scan restart test**

Create `jdbc_client/src/test/java/com/xmysql/server/test/P0ClusteredBTreeScanTest.java` with:

```java
@Test
void primaryKeyRangeScanSurvivesRestart() throws Exception {
    try (Connection c = getConnection(); Statement s = c.createStatement()) {
        s.execute("CREATE DATABASE IF NOT EXISTS p0_scan");
        s.execute("USE p0_scan");
        s.execute("DROP TABLE IF EXISTS events");
        s.execute("CREATE TABLE events (id INT PRIMARY KEY, payload VARCHAR(32))");
        for (int i = 1; i <= 1200; i++) {
            s.execute("INSERT INTO events VALUES (" + i + ", 'v" + i + "')");
        }
    }
    restartServer();
    try (Connection c = getConnection(); Statement s = c.createStatement()) {
        s.execute("USE p0_scan");
        try (ResultSet rs = s.executeQuery("SELECT id FROM events WHERE id BETWEEN 500 AND 510 ORDER BY id")) {
            for (int expected = 500; expected <= 510; expected++) {
                assertTrue(rs.next());
                assertEquals(expected, rs.getInt(1));
            }
            assertFalse(rs.next());
        }
    }
}
```

- [ ] **Step 5: Run scan tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager ./server/innodb/engine -run 'TestClusteredBTreeFullAndRangeScanAcrossRestart|TestP0SelectDoesNotDependOnTableRowsSidecar' -count=1
cd jdbc_client && mvn test -Dtest=P0ClusteredBTreeScanTest,P0DurabilityContractTest
```

Expected: all tests pass with no B+Tree sidecar JSON files created.

- [ ] **Step 6: Commit**

```bash
git add server/innodb/manager server/innodb/engine jdbc_client/src/test/java/com/xmysql/server/test/P0ClusteredBTreeScanTest.java
git commit -m "feat: scan clustered btree pages across restart"
```

---

### Task 4: Prove B+Tree Split, Delete Mark, Merge, and Reuse Correctness

**Files:**
- Modify: `server/innodb/manager/enhanced_btree_index.go`
- Modify: `server/innodb/manager/bplus_tree_manager.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Test: `server/innodb/manager/btree_split_merge_delete_test.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/P0BTreeMutationTest.java`

- [ ] **Step 1: Add mutation stress tests**

Create `server/innodb/manager/btree_split_merge_delete_test.go`:

```go
package manager

import "testing"

func TestBTreeSplitDeleteMergeReuseDoesNotResurrectRows(t *testing.T) {
	tree := newP0ClusteredTree(t, t.TempDir(), "p0_mutation", "items")
	for i := 1; i <= 5000; i++ {
		tree.InsertIntKey(t, i, []byte("live"))
	}
	for i := 1000; i <= 3000; i++ {
		tree.DeleteIntKey(t, i)
	}
	for i := 6000; i <= 7500; i++ {
		tree.InsertIntKey(t, i, []byte("new"))
	}
	tree.Restart(t)

	for i := 1000; i <= 3000; i++ {
		if tree.ExistsIntKey(t, i) {
			t.Fatalf("deleted key resurrected: %d", i)
		}
	}
	if !tree.ExistsIntKey(t, 7500) {
		t.Fatalf("new key missing after page reuse")
	}
}
```

- [ ] **Step 2: Implement durable delete markers**

Persist delete markers in the canonical record header, not in memory-only structures. SELECT must skip delete-marked records unless the transaction visibility rules in Task 6 require an older version.

- [ ] **Step 3: Implement stable page split and merge metadata**

Persist sibling page ids, page level, high key, record count, free space, and page LSN. On restart, reconstruct traversal from root page metadata and verify leaf sibling links.

- [ ] **Step 4: Run mutation tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager ./server/innodb/engine -run 'TestBTreeSplitDeleteMergeReuseDoesNotResurrectRows' -count=1
```

Expected: deleted keys never reappear after restart, and newly inserted keys remain readable.

- [ ] **Step 5: Commit**

```bash
git add server/innodb/manager server/innodb/engine
git commit -m "feat: harden btree mutation persistence"
```

---

### Task 5: Make Secondary Index and UNIQUE Enforcement Durable

**Files:**
- Modify: `server/innodb/engine/storage_integrated_index_helper.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/manager/enhanced_btree_index.go`
- Test: `server/innodb/engine/p0_index_durability_test.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/P0IndexDurabilityTest.java`

- [ ] **Step 1: Add durable UNIQUE restart test**

Create `server/innodb/engine/p0_index_durability_test.go`:

```go
package engine

import "testing"

func TestUniqueIndexEnforcedFromDurableStateAfterRestart(t *testing.T) {
	h := newP0EngineHarness(t, t.TempDir())
	h.Exec(t, "CREATE DATABASE p0_idx")
	h.Exec(t, "USE p0_idx")
	h.Exec(t, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(128) UNIQUE)")
	h.Exec(t, "INSERT INTO users VALUES (1, 'a@example.com')")
	h.Restart(t)

	err := h.ExecErr("INSERT INTO users VALUES (2, 'a@example.com')")
	if err == nil {
		t.Fatalf("expected duplicate unique key error after restart")
	}
	assertMySQLDuplicateKeyError(t, err)
}
```

- [ ] **Step 2: Add optimizer-visible secondary index test**

Add a JDBC test that inserts 10,000 rows, creates `INDEX idx_email(email)`, runs `SELECT id FROM users WHERE email = ?`, and asserts either an EXPLAIN/index evidence hook or engine trace shows `idx_email` was selected.

- [ ] **Step 3: Implement secondary index key format**

Use deterministic key bytes:

```text
secondary-key = index-column-1 || 0x00 || ... || index-column-N || 0x00 || primary-key
unique-key    = index-column-1 || 0x00 || ... || index-column-N
```

UNIQUE check must query the durable secondary index before inserting the clustered row.

- [ ] **Step 4: Implement index rebuild/validate/repair commands**

Expose internal functions used by tests:

```go
func RebuildTableIndexes(ctx context.Context, schemaName string, tableName string) error
func ValidateTableIndexes(ctx context.Context, schemaName string, tableName string) ([]IndexConsistencyIssue, error)
func RepairTableIndexes(ctx context.Context, schemaName string, tableName string) error
```

The validator must detect missing secondary entries, stale secondary entries, and duplicate UNIQUE keys.

- [ ] **Step 5: Run index tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -run 'TestUniqueIndexEnforcedFromDurableStateAfterRestart|Test.*Index.*' -count=1
cd jdbc_client && mvn test -Dtest=IndexAndConstraintTest,P0IndexDurabilityTest
```

Expected: duplicate checks still fail correctly after restart, and SELECT can use durable secondary indexes.

- [ ] **Step 6: Commit**

```bash
git add server/innodb/engine server/innodb/manager jdbc_client/src/test/java/com/xmysql/server/test/P0IndexDurabilityTest.java
git commit -m "feat: enforce durable secondary indexes"
```

---

### Task 6: Replace Snapshot-Only Transactions with Undo/MVCC Semantics

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/manager/undo_purge.go`
- Modify: `server/innodb/storage/store/mvcc/trx_sys.go`
- Test: `server/innodb/engine/p0_transaction_mvcc_test.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/P0TransactionIsolationTest.java`

- [ ] **Step 1: Add rollback/savepoint tests that inspect row versions**

Create `server/innodb/engine/p0_transaction_mvcc_test.go`:

```go
package engine

import "testing"

func TestRollbackAndSavepointUseUndoRecords(t *testing.T) {
	h := newP0EngineHarness(t, t.TempDir())
	h.Exec(t, "CREATE DATABASE p0_txn")
	h.Exec(t, "USE p0_txn")
	h.Exec(t, "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)")
	h.Exec(t, "INSERT INTO accounts VALUES (1, 100)")

	h.Exec(t, "BEGIN")
	h.Exec(t, "UPDATE accounts SET balance = 80 WHERE id = 1")
	h.Exec(t, "SAVEPOINT s1")
	h.Exec(t, "UPDATE accounts SET balance = 50 WHERE id = 1")
	h.Exec(t, "ROLLBACK TO SAVEPOINT s1")
	h.Exec(t, "COMMIT")

	rows := h.QueryRows(t, "SELECT balance FROM accounts WHERE id = 1")
	assertP0Rows(t, rows, [][]any{{80}})
	h.AssertUndoChainContains(t, "accounts", 1)
}
```

- [ ] **Step 2: Add two-connection JDBC isolation tests**

Create `jdbc_client/src/test/java/com/xmysql/server/test/P0TransactionIsolationTest.java` with READ COMMITTED and REPEATABLE READ cases:

```java
@Test
void repeatableReadDoesNotSeeConcurrentCommitUntilTransactionEnds() throws Exception {
    try (Connection a = getConnection(); Connection b = getConnection()) {
        try (Statement s = a.createStatement()) {
            s.execute("CREATE DATABASE IF NOT EXISTS p0_iso");
            s.execute("USE p0_iso");
            s.execute("DROP TABLE IF EXISTS t");
            s.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
            s.execute("INSERT INTO t VALUES (1, 10)");
        }
        a.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        a.setAutoCommit(false);
        b.setAutoCommit(false);
        try (Statement sa = a.createStatement(); Statement sb = b.createStatement()) {
            sa.execute("USE p0_iso");
            sb.execute("USE p0_iso");
            assertSingleInt(sa, "SELECT v FROM t WHERE id = 1", 10);
            sb.execute("UPDATE t SET v = 20 WHERE id = 1");
            b.commit();
            assertSingleInt(sa, "SELECT v FROM t WHERE id = 1", 10);
            a.commit();
            assertSingleInt(sa, "SELECT v FROM t WHERE id = 1", 20);
        }
    }
}
```

- [ ] **Step 3: Remove data-directory snapshot from the normal transaction path**

In `server/innodb/engine/executor.go`, stop using `captureDataDirSnapshot` and `restoreDataDirSnapshot` for normal `BEGIN`, `SAVEPOINT`, and `ROLLBACK`. Keep a test-only hook only if a specific recovery test needs filesystem comparison.

- [ ] **Step 4: Write undo records for INSERT/UPDATE/DELETE**

Each DML must write an undo record containing:
- transaction id;
- table id;
- primary key;
- before image for UPDATE/DELETE;
- insert marker for INSERT rollback;
- previous roll pointer.

- [ ] **Step 5: Implement MVCC visibility checks**

SELECT must filter record versions by transaction read view:
- READ COMMITTED creates a read view per statement;
- REPEATABLE READ creates a read view per transaction;
- own writes are visible inside the transaction;
- rollback walks undo records back to the savepoint or transaction start.

- [ ] **Step 6: Complete purge active snapshot checks**

In `server/innodb/manager/undo_purge.go`, make `canPurge` consult the oldest active read view. A version is purgeable only when no active read view can still see it.

- [ ] **Step 7: Run transaction tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager ./server/innodb/storage/store/mvcc -run 'TestRollbackAndSavepointUseUndoRecords|Test.*MVCC.*|Test.*Purge.*' -count=1
cd jdbc_client && mvn test -Dtest=TransactionTest,P0TransactionIsolationTest
```

Expected: rollback/savepoint pass without data-dir snapshot restore, and two-connection visibility matches MySQL semantics for covered isolation levels.

- [ ] **Step 8: Commit**

```bash
git add server/innodb/engine server/innodb/manager server/innodb/storage/store/mvcc jdbc_client/src/test/java/com/xmysql/server/test/P0TransactionIsolationTest.java
git commit -m "feat: implement undo backed transaction semantics"
```

---

### Task 7: Add Crash Recovery Evidence for Rows, Pages, WAL, and Undo

**Files:**
- Modify: `server/innodb/manager/recovery_manager.go`
- Modify: `server/innodb/manager/wal_manager.go`
- Modify: `server/innodb/engine/storage_integrated_checkpoint.go`
- Create: `server/innodb/engine/p0_crash_recovery_test.go`
- Modify: `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`

- [ ] **Step 1: Add interrupted commit recovery test**

Create `server/innodb/engine/p0_crash_recovery_test.go`:

```go
package engine

import "testing"

func TestCrashRecoveryReplaysCommittedAndRollsBackUncommitted(t *testing.T) {
	dataDir := t.TempDir()
	h := newP0EngineHarness(t, dataDir)
	h.Exec(t, "CREATE DATABASE p0_recovery")
	h.Exec(t, "USE p0_recovery")
	h.Exec(t, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	h.Exec(t, "INSERT INTO t VALUES (1, 10)")
	h.Exec(t, "BEGIN")
	h.Exec(t, "INSERT INTO t VALUES (2, 20)")
	h.SimulateCrashBeforeCommit(t)

	h.Restart(t)
	rows := h.QueryRows(t, "SELECT id FROM t ORDER BY id")
	assertP0Rows(t, rows, [][]any{{1}})
	h.AssertRecoveryEvidence(t, "redo_applied", "undo_rollback_applied", "page_checksum_verified")
}
```

- [ ] **Step 2: Persist WAL records for page changes**

Every clustered page mutation must write WAL with page id, before/after LSN, record operation, and transaction id. Recovery must be idempotent.

- [ ] **Step 3: Add recovery manifest generation**

Update `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md` with generated evidence fields:

```json
{
  "commit": "454451b-or-newer",
  "tests": ["TestCrashRecoveryReplaysCommittedAndRollsBackUncommitted"],
  "redo_applied": true,
  "undo_rollback_applied": true,
  "page_checksum_verified": true,
  "sidecar_files_required": false
}
```

- [ ] **Step 4: Run recovery tests**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -run 'TestCrashRecoveryReplaysCommittedAndRollsBackUncommitted|Test.*Recovery.*' -count=1
```

Expected: committed rows survive restart, uncommitted rows disappear, and recovery evidence shows redo/undo/page checksum checks.

- [ ] **Step 5: Commit**

```bash
git add server/innodb/manager server/innodb/engine docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md
git commit -m "feat: add p0 crash recovery evidence"
```

---

### Task 8: Close P0 SQL, DDL, and JDBC Protocol Gaps

**Files:**
- Modify: `server/innodb/engine/storage_integrated_dml_helper.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/net/decoupled_handler.go`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/P0MysqlCompatibilityEdgeTest.java`

- [ ] **Step 1: Add JDBC edge tests**

Create `jdbc_client/src/test/java/com/xmysql/server/test/P0MysqlCompatibilityEdgeTest.java` with covered cases:
- `INSERT ... SELECT`;
- `INSERT ... ON DUPLICATE KEY UPDATE`;
- `REPLACE INTO`;
- `UPDATE` by composite primary key;
- `DELETE` by composite primary key;
- `ALTER TABLE DROP COLUMN`;
- `ALTER TABLE ADD INDEX`;
- `PreparedStatement.setCharacterStream` or `setBinaryStream` large payload path.

- [ ] **Step 2: Implement `INSERT ... SELECT` through the SELECT executor**

For `sqlparser.SelectStatement` rows, execute the SELECT inside the same transaction, map returned columns to INSERT target columns, validate constraints, then write clustered rows and secondary index entries.

- [ ] **Step 3: Implement `ON DUPLICATE KEY UPDATE` in the storage-integrated path**

Use durable UNIQUE lookup from Task 5. On duplicate, update the existing clustered row and all affected secondary indexes inside the same transaction.

- [ ] **Step 4: Implement `REPLACE INTO`**

Use MySQL semantics for covered P0 cases:
- if no duplicate key exists, insert;
- if duplicate primary or unique key exists, delete conflicting row version and insert the new row;
- affected row count follows MySQL-compatible JDBC assertions.

- [ ] **Step 5: Implement composite primary key UPDATE/DELETE**

Make row identity use encoded composite primary key bytes instead of single-column assumptions in `storage_integrated_dml_executor.go`.

- [ ] **Step 6: Extend core ALTER TABLE**

Support P0 forms:
- `ALTER TABLE t ADD COLUMN c TYPE`;
- `ALTER TABLE t DROP COLUMN c`;
- `ALTER TABLE t ADD INDEX idx(c)`;
- `ALTER TABLE t DROP INDEX idx`;
- `ALTER TABLE t MODIFY COLUMN c TYPE` where stored values can be converted deterministically.

- [ ] **Step 7: Implement JDBC long data command**

In `server/net/decoupled_handler.go`, implement `COM_STMT_SEND_LONG_DATA` by buffering parameter chunks per prepared statement id and parameter index, then consuming them in `COM_STMT_EXECUTE`.

- [ ] **Step 8: Run compatibility tests**

```bash
cd jdbc_client && mvn test -Dtest=P0MysqlCompatibilityEdgeTest,DDLOperationsTest,DMLOperationsTest,PreparedStatementTest
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/net ./server/protocol ./server/innodb/engine -count=1
```

Expected: P0 compatibility edge tests pass and existing JDBC suites remain green.

- [ ] **Step 9: Commit**

```bash
git add server/innodb/engine server/net server/protocol jdbc_client/src/test/java/com/xmysql/server/test/P0MysqlCompatibilityEdgeTest.java
git commit -m "feat: close p0 mysql compatibility edges"
```

---

### Task 9: Refresh P0 Documents and Release Evidence

**Files:**
- Modify: `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`
- Modify: `docs/planning/P0_CURRENT_STATUS_SUMMARY.md`
- Modify: `docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md`
- Modify: `docs/planning/P0_EVIDENCE_BUNDLE.md`
- Modify: `docs/planning/P0_RISK_REGISTER.md`

- [ ] **Step 1: Run the full local evidence suite**

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/net ./server/conf ./server/dispatcher ./server/innodb/manager ./server/innodb/engine ./server/protocol ./server/innodb/storage/store/mvcc -count=1
cd jdbc_client && mvn test
```

Expected:
- Go package tests pass.
- Maven JDBC suite passes.
- No result depends on old `target/surefire-reports` output.

- [ ] **Step 2: Run sidecar absence check**

```bash
find . -path '*/_xmysql_btree_records/*.json' -o -name '*.xrows.json'
```

Expected: no files are required by passing tests. Migration fixtures may exist only in explicitly named testdata directories.

- [ ] **Step 3: Update P0 backlog status**

Update P0 documents so they reflect the current commit and test evidence:
- mark completed items with current commit hash;
- keep unresolved items open with failing test names;
- remove stale claims that DDL/transaction/index suites fail if the current full suite passes.

- [ ] **Step 4: Update risk register**

For every remaining open P0 risk, include:
- owner;
- failing test or missing evidence;
- exact code path;
- accepted deferral record if not fixed in this release.

- [ ] **Step 5: Commit documents**

```bash
git add docs/planning/P0_CAPABILITY_BACKLOG_20260715.md docs/planning/P0_CURRENT_STATUS_SUMMARY.md docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md docs/planning/P0_EVIDENCE_BUNDLE.md docs/planning/P0_RISK_REGISTER.md
git commit -m "docs: refresh p0 capability evidence"
```

---

## Final Verification

Run from repository root:

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/net ./server/conf ./server/dispatcher ./server/innodb/manager ./server/innodb/engine ./server/protocol ./server/innodb/storage/store/mvcc -count=1
cd jdbc_client && mvn test
```

Required final state:
- JDBC CRUD works before and after server restart.
- SELECT reads from durable clustered B+Tree pages, not `*.xrows.json`.
- B+Tree record persistence does not require `_xmysql_btree_records/*.json`.
- UNIQUE checks use durable secondary index state across restart.
- Transactions use undo/MVCC behavior for rollback, savepoint, and covered isolation cases.
- Crash recovery proves redo, undo, page checksum, and row state.
- P0/P1/P* planning docs no longer contain stale conclusions for the current commit.

## Execution Options

1. **Subagent-Driven (recommended)** - dispatch a fresh subagent per task, review between tasks, faster parallel progress.
2. **Inline Execution** - execute tasks in this session using `executing-plans`, with checkpoints after each task.
