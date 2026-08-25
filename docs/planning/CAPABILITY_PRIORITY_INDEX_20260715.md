# XMySQL Capability Priority Index - 2026-07-15

## Purpose

This document is the current priority index for missing XMySQL capabilities.

It supersedes older completion summaries when they conflict on priority or readiness. Historical reports remain useful as evidence of past work, but current planning should start here.

## Current Baseline

Basic single-node CRUD through the JDBC path is currently working for the tested scope:

- `INSERT`
- `SELECT`
- `UPDATE`
- `DELETE`
- basic JDBC connection and DML tests
- common JDBC prepared statements, including generated keys, `IN`, `LIKE`, `DECIMAL`, `BOOLEAN`, and `NULL`
- JDBC transaction commands, including commit, rollback, savepoints, isolation-level setter, `BEGIN`, and `START TRANSACTION`
- JDBC DDL metadata edges, including `SHOW DATABASES LIKE`, `SHOW FULL TABLES`, `DROP DATABASE IF EXISTS`, and `DatabaseMetaData.getTables()`
- core DDL smoke paths: `CREATE DATABASE`, `DROP DATABASE`, `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE ADD COLUMN`, and `TRUNCATE TABLE`
- restart read-back for the focused durable clustered-record path
- page-level redo/checksum and B+Tree delete/reuse focused Go coverage
- undo purge now protects active read views and reclaims prepared cached undo segments into reusable cached segments
- index validation/compaction now has a concrete metadata and leaf-chain verification path instead of a no-op
- CBO row count collection now parses InnoDB index page `PAGE_N_RECS` for exact page-level counts instead of always using a fixed row estimate
- focused JDBC matrix on 2026-07-22: 77 tests, 0 failures, 0 errors

This does not mean the project is production-ready. The current boundary is:

- usable for focused local integration tests and the current P0 closure evidence;
- not yet complete for advanced MySQL compatibility such as full FK enforcement matrix, full CHECK expression semantics, FULLTEXT query/ranking behavior, `ON DUPLICATE KEY UPDATE`, and `REPLACE`;
- not yet complete as a MySQL/InnoDB-compatible production database;
- not approved for production release without P1 release-readiness evidence such as crash drill, concurrent workload, metrics drill, rollback drill, and owner sign-off.

## Priority Definitions

| Priority | Meaning | Release Rule |
|---|---|---|
| P0 | Blocks production-grade correctness, durability, or JDBC/MySQL compatibility for core CRUD workloads | Must be implemented or explicitly accepted as a time-boxed deferral before production gray release |
| P1 | Important for broad compatibility, performance, maintainability, or operational confidence | Should be scheduled after P0; can be released only if documented as non-blocking |
| P* | P2/P3/future work: advanced MySQL features, performance expansions, ecosystem compatibility, or optional storage features | Track separately; do not mix with P0 readiness claims |

## Canonical Priority Documents

| Document | Scope |
|---|---|
| `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md` | Production-blocking missing capabilities |
| `docs/planning/P1_CAPABILITY_BACKLOG_20260715.md` | Important next capabilities after P0 |
| `docs/planning/PX_CAPABILITY_BACKLOG_20260715.md` | P2/P3/future capability backlog |
| `docs/planning/CODEBASE_CLEANUP_DEVELOPMENT_CHECKLIST_20260715.md` | Development checklist for cleaning confusing code paths |

## P0 Summary

| Area | Current State | P0 Completion Boundary |
|---|---|---|
| Storage page and B+Tree format | Focused CRUD persistence, restart read-back, delete cleanup, page reuse, page checksum, and clustered-record scan coverage pass | Keep focused storage evidence green; move scale/crash stress breadth to P1 |
| Secondary indexes and constraints | Durable key mapping, focused optimizer/executor read-path tests, validate/compact, rebuild, repair, and JDBC constraint smoke tests pass | Keep supported DML/SELECT/UNIQUE/index paths backed by durable index state; move deeper corruption and advanced constraint semantics to P1/P* |
| Transactions, MVCC, and recovery | Focused Go transaction/recovery tests and JDBC transaction suite pass | Keep commit/rollback/savepoint/isolation setter behavior green; move external crash drill and broad anomaly workload to P1 |
| SQL and JDBC compatibility | Focused JDBC DML, prepared statement, transaction, DDL metadata, system variable, and index/constraint matrix passes | Keep current core JDBC matrix green; move `ON DUPLICATE KEY UPDATE`, `REPLACE`, and advanced MySQL features to P1/P* |
| Production validation | Evidence bundle exists for focused P0 closure, but focused local evidence is not production readiness | Full candidate run, concurrent SQL workload, live metrics evidence, rollback drill, risk sign-off, and owner approval are P1 release gates |

## Immediate Execution Order

1. Keep the 2026-07-22 P0 focused matrix green on every merge candidate.
2. Schedule P1 release-readiness evidence: external crash drill, concurrent workload, metrics drill, rollback drill, and owner sign-off.
3. Schedule P1 SQL compatibility breadth: `ON DUPLICATE KEY UPDATE`, `REPLACE`, full FK enforcement matrix, and CHECK expression semantics.
4. Schedule P1/P* storage stress: large B+Tree split/merge/reuse workloads, corruption injection, and long-running purge/restart tests.
5. Keep production readiness claims tied to `docs/planning/P0_EVIDENCE_RUN_20260722.md` or newer evidence documents.
