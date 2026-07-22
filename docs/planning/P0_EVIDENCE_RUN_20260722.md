# P0 Evidence Run - 2026-07-22

## Scope

This evidence run covers branch `codex/p0-remaining-capability-closure`.

The run verifies the P0 closure work for clustered-record durability, secondary-index maintenance, transaction command behavior, JDBC metadata/DDL edges, and core JDBC CRUD compatibility.

## Commands

| Area | Command | Result |
|---|---|---|
| Go focused baseline | `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager ./server/innodb/plan -count=1` | PASS: engine, manager, and plan passed on 2026-07-22 |
| JDBC focused matrix | `cd jdbc_client && mvn test -Dtest=DMLOperationsTest,PreparedStatementTest,TransactionTest,DDLOperationsTest,SystemVariableTest,IndexAndConstraintTest` | PASS: 77 tests, 0 failures, 0 errors, 0 skipped |
| JDBC DML | Included in focused matrix: `DMLOperationsTest` | PASS: 13 tests |
| JDBC prepared statements | Included in focused matrix: `PreparedStatementTest` | PASS: 12 tests |
| JDBC transaction commands | Included in focused matrix: `TransactionTest` | PASS: 8 tests |
| JDBC DDL metadata | Included in focused matrix: `DDLOperationsTest` and `SystemVariableTest` | PASS: 17 DDL tests and 15 system-variable tests |
| JDBC index/constraint smoke | Included in focused matrix: `IndexAndConstraintTest` | PASS: 12 tests for the supported compatibility surface |

## Verified Behaviors

| Area | Verified Behavior |
|---|---|
| Core CRUD | JDBC `INSERT`, `SELECT`, `UPDATE`, and `DELETE` pass the focused DML suite |
| PreparedStatement | Insert, batch insert, select, update, delete, generated keys, parameter reuse, `IN`, `LIKE`, `NULL`, and transaction use pass |
| Transactions | JDBC `COMMIT`, `ROLLBACK`, savepoints, multiple savepoints, isolation-level setter, `BEGIN`, and `START TRANSACTION` pass |
| DDL metadata | `SHOW DATABASES LIKE`, `DROP DATABASE IF EXISTS`, `SHOW FULL TABLES`, and `DatabaseMetaData.getTables()` pass |
| Core DDL | `CREATE DATABASE`, `DROP DATABASE`, `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE ADD COLUMN`, and `TRUNCATE TABLE` pass |
| Constraints and indexes | Primary key, composite primary key, unique, not null, index creation, composite index, unique index, FK syntax smoke, CHECK syntax smoke, FULLTEXT syntax smoke, and cascade syntax smoke pass |
| Storage baseline | Focused Go engine/manager/plan packages pass after the P0 storage/index/transaction changes |

## Important Boundary

This evidence proves the current focused P0 compatibility surface. It does not claim full MySQL/InnoDB production equivalence.

The following remain outside this evidence run:

| Capability | Priority | Reason |
|---|---|---|
| Full foreign-key referential enforcement under all update/delete plans | P1 | Current JDBC suite proves syntax and smoke behavior, not a full referential-integrity matrix |
| Full CHECK expression evaluation semantics | P1 | Current JDBC suite proves syntax and smoke behavior, not full expression compatibility |
| FULLTEXT query and ranking behavior | P* | Advanced MySQL feature outside core CRUD/JDBC P0 |
| Full crash drill with killed process and row/page/WAL diff artifacts | P1 | Go recovery coverage exists, but this run did not execute an external kill-and-replay drill |
| Production-scale concurrent workload and owner sign-off | P1 | Required before production release, but not a code-level P0 closure item |

## Notes

The JDBC matrix was run against a clean local `server/net/data` state. A previous run against stale data failed because old `test_ddl_db_*` databases already existed; after cleaning runtime data and restarting the server, the same focused matrix passed.
