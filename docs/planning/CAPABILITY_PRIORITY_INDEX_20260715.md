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
- page-level redo/checksum and B+Tree delete/reuse focused Go coverage
- undo purge now protects active read views and reclaims prepared cached undo segments into reusable cached segments
- index validation/compaction now has a concrete metadata and leaf-chain verification path instead of a no-op
- CBO row count collection now parses InnoDB index page `PAGE_N_RECS` for exact page-level counts instead of always using a fixed row estimate

This does not mean the project is production-ready. The current boundary is:

- usable for focused local integration tests;
- not yet complete for full JDBC transaction matrix evidence, durable secondary-index SELECT selection, composite/no-PK DML breadth, or advanced constraints;
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
| Storage page and B+Tree format | CRUD persistence, focused restart read-back, delete cleanup, page reuse, and page checksum coverage exist; full InnoDB-compatible page lifecycle breadth still needs stress evidence | A single canonical on-disk record/page format, restart scan, range scan, split/merge, delete cleanup, and recovery all work from disk pages |
| Secondary indexes and constraints | Durable key mapping exists; validate/compact now checks metadata and leaf-chain shape, but SELECT and all constraints are not yet fully index-backed | DML, SELECT, UNIQUE checks, rebuild, validation, and optimizer selection are all backed by durable secondary indexes |
| Transactions, MVCC, and recovery | Transaction commands and focused rollback/savepoint Go paths exist; undo purge protects active snapshots and reclaims reusable segments, but JDBC multi-connection/isolation evidence is still incomplete | COMMIT/ROLLBACK, isolation, undo purge, crash replay, half-commit handling, and row/page/WAL state evidence pass |
| SQL and JDBC compatibility | JDBC DML and PreparedStatement suites are green; DDL metadata result sets, full transaction semantics, and advanced constraints remain incomplete | Core MySQL/JDBC compatibility matrix passes, including prepared statements, metadata, transaction commands, and common DML extensions |
| Production validation | Evidence tooling exists, but focused local evidence is not the same as production readiness | Full candidate run, concurrent SQL workload, live metrics evidence, rollback drill, risk sign-off, and owner approval are complete |

## Immediate Execution Order

1. Close JDBC transaction/MVCC/recovery correctness under multi-connection workloads.
2. Close durable secondary-index query selection and constraint enforcement evidence.
3. Close no-primary-key indexed table and composite-primary-key update/delete DML breadth.
4. Close JDBC metadata/DDL edge cases that still fail MySQL client expectations.
5. Run production-readiness evidence and governance gates against the completed runtime behavior.
