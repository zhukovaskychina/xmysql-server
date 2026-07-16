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
- restart read-back for the focused durable clustered-record path

This does not mean the project is production-ready. The current boundary is:

- usable for focused local integration tests;
- not yet complete for JDBC DDL metadata, transaction undo/savepoint semantics, or advanced constraints;
- not yet complete as a MySQL/InnoDB-compatible production database;
- not approved for production gray release without the P0 items below.

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
| Storage page and B+Tree format | CRUD persistence is working through the current clustered-record path, but page/record format still has project-specific transition layers | A single canonical on-disk record/page format, restart scan, range scan, split/merge, delete cleanup, and recovery all work from disk pages |
| Secondary indexes and constraints | Durable key mapping exists, but query and constraint enforcement are not fully index-backed | DML, SELECT, UNIQUE checks, rebuild, validation, and optimizer selection are all backed by durable secondary indexes |
| Transactions, MVCC, and recovery | Transaction commands are accepted and some commit paths pass, but rollback/savepoint undo still leaves row changes visible | COMMIT/ROLLBACK, isolation, undo purge, crash replay, half-commit handling, and row/page/WAL state evidence pass |
| SQL and JDBC compatibility | JDBC DML and PreparedStatement suites are green; DDL metadata result sets, full transaction semantics, and advanced constraints remain incomplete | Core MySQL/JDBC compatibility matrix passes, including prepared statements, metadata, transaction commands, and common DML extensions |
| Production validation | Evidence tooling exists, but focused local evidence is not the same as production readiness | Full candidate run, concurrent SQL workload, live metrics evidence, rollback drill, risk sign-off, and owner approval are complete |

## Immediate Execution Order

1. Close storage format and B+Tree scan/split/merge persistence.
2. Close durable secondary index query and constraint enforcement.
3. Close JDBC DDL metadata result sets and `DatabaseMetaData.getTables()` visibility.
4. Close transaction/MVCC/recovery correctness under JDBC multi-connection workloads.
5. Run production-readiness evidence and governance gates against the completed runtime behavior.
