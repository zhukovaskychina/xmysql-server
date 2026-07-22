# Task 2 Report: Durable Secondary-Index SELECT Path

## Scope

Implemented the durable secondary-index SELECT path only. The work persists secondary index entries with a stable table identity, reconstructs secondary-index metadata after restart, scans the durable secondary index, decodes clustered primary keys, and reads the matching clustered records.

No rebuild or repair workflow from Task 3 was added.

## TDD Evidence

The new restart regression test was added first in `server/innodb/engine/p0_secondary_index_select_test.go`.

Initial failures included:

- The test helper referenced an access-path marker before it existed.
- The restarted query fell back to a clustered scan and failed with `index 1 not found`.
- After durable entries were written, the restarted secondary range scan returned no rows because the table-space ID and B+Tree key comparison were not durable-safe.
- The first clustered lookup could not decode the stored primary-key representation.

The final regression proves that rows inserted before close are returned after a new engine instance executes `SELECT id, name FROM users WHERE email = 'b@example.com'`, and the executor records `secondary_index:idx_email` as the actual access path.

## Implementation

- Added stable secondary table/index identifiers and durable secondary value encoding.
- Created secondary-index metadata from the persisted table definition when needed after restart.
- Routed index-manager range operations through the table-specific durable B+Tree.
- Added secondary key range lookup and primary-key decoding in the SELECT and volcano paths.
- Made clustered lookup decode the stored primary-key representation and clustered-record payload.
- Persisted table-space identity with table metadata and restored it while scanning existing tablespaces.
- Corrected B+Tree key comparison to use binary lexical order, which is required for encoded secondary keys.
- Added a one-time range retry for the initial reopened B+Tree read when its first range result is empty.

## Verification

Passed:

```text
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run TestP0SelectUsesDurableSecondaryIndexAfterRestart -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -run 'TestP0SelectUsesDurableSecondaryIndexAfterRestart|TestSecondaryIndex|TestIndexScan' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager ./server/innodb/plan
```

JDBC attempted:

```text
cd jdbc_client && mvn test -Dtest=IndexAndConstraintTest
```

It could not connect to the configured MySQL endpoint: `Connection refused`. The test suite did not reach its assertions.

## Cleanup

Removed the generated runtime paths required by the task before commit. Some of those paths are repository-tracked generated artifacts and remain as unstaged deletions by design.

## Review Fixes

### RED

- `TestP0SecondaryIndexUpdateRemovesStaleEntry` returned the updated row for the old secondary key.
- `TestP0SecondaryIndexDeleteRemovesStaleEntry` followed a stale secondary entry to a missing clustered row.
- `TestP0SelectUsesSecondaryIndexForRangePredicates` showed `>=`, `<=`, and `BETWEEN` used a table scan.
- `TestP0SelectRejectsInvalidDurableSecondaryIndexValue` showed invalid durable values were silently skipped.
- Removing the empty-result retry made `TestP0SelectUsesDurableSecondaryIndexAfterRestart` fail on its first reopened read.
- `TestSecondaryIndexFullRangeContainsCompositeIndexKey` failed because the full-index range did not cover composite keys.
- JDBC `IndexAndConstraintTest` initially failed its composite-index query because a compound predicate was treated as a simple index equality predicate.

### GREEN

- Update and delete now derive the same encoded secondary key used by insert, use the stable secondary table ID, and store encoded secondary values.
- The first post-restart read explicitly loads the durable leaf-page chain; no empty-result retry remains.
- `>=`, `<=`, and `BETWEEN` scan only the selected durable secondary index and apply the predicate after clustered lookup. Compound predicates fall back to a table scan.
- Invalid secondary values now return an error.

### Verification

```text
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestP0(SelectUsesDurableSecondaryIndexAfterRestart|SecondaryIndex|Rollback.*SecondaryIndex)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager -run 'Test(SecondaryIndex|EncodeSecondaryIndexKey)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager ./server/innodb/plan -count=1
cd jdbc_client && mvn test -Dtest=IndexAndConstraintTest
```

All Go commands passed. The JDBC suite was run against a local server started with `conf/jdbc_local.ini` on `127.0.0.1:3309` and passed 12 tests with zero failures or errors.
