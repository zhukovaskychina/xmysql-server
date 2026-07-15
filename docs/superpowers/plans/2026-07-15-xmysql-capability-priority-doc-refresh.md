# XMySQL Capability Priority Documentation Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a current P0/P1/P* capability backlog and make older planning documents point to the new priority source of truth.

**Architecture:** Keep historical reports as append-only evidence while adding a current canonical priority index. Update only document conclusions and navigation so old completion claims do not override the 2026-07-15 capability assessment.

**Tech Stack:** Markdown documentation, repository planning docs, existing Go/JDBC verification evidence.

---

### Task 1: Add Canonical Priority Documents

**Files:**
- Create: `docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md`
- Create: `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`
- Create: `docs/planning/P1_CAPABILITY_BACKLOG_20260715.md`
- Create: `docs/planning/PX_CAPABILITY_BACKLOG_20260715.md`

- [x] **Step 1: Define current baseline**

Record that focused CRUD/JDBC is working, but production-grade InnoDB/MySQL compatibility is not complete.

- [x] **Step 2: Define priority meanings**

Use P0 for production-blocking correctness/durability/JDBC gaps, P1 for important next work, and P* for P2/P3/future work.

- [x] **Step 3: Split backlog by priority**

Group work into storage, indexes, transactions/recovery, SQL/JDBC, observability, release, security, and future enhancements.

### Task 2: Update Old Planning Conclusions

**Files:**
- Modify: `README.md`
- Modify: `docs/planning/DEVELOPMENT_ROADMAP.md`
- Modify: `docs/planning/P0_CURRENT_STATUS_SUMMARY.md`
- Modify: `docs/planning/P0_PRODUCTION_CHECKLIST.md`
- Modify: `docs/planning/P0_PRODUCTION_GAP_ANALYSIS.md`
- Modify: `docs/planning/P0_REMAINING_ENGINEERING_BACKLOG.md`
- Modify: `docs/planning/P0_NEXT_ACTIONS.md`
- Modify: `docs/planning/P0_PRODUCTION_TASKS.md`
- Modify: `docs/未实现功能梳理.md`
- Modify: `docs/development/QUERY_MODULE_REMAINING_TASKS.md`

- [x] **Step 1: Add current-status notices**

Add a 2026-07-15 notice that the new priority documents supersede older conclusions when they conflict.

- [x] **Step 2: Update readiness wording**

Separate "basic CRUD/JDBC works" from "production-ready".

- [x] **Step 3: Update next actions**

Point next work toward P0 storage/index/transaction/JDBC closure before evidence-only release work.

### Task 3: Verify Documentation Consistency

**Files:**
- Verify: `docs/planning/*.md`
- Verify: `docs/development/QUERY_MODULE_REMAINING_TASKS.md`
- Verify: `docs/未实现功能梳理.md`

- [x] **Step 1: List changed files**

Run:

```bash
git status --short
```

Expected: only documentation files are modified or added.

- [x] **Step 2: Check references**

Run:

```bash
rg "CAPABILITY_PRIORITY_INDEX_20260715|P0_CAPABILITY_BACKLOG_20260715|P1_CAPABILITY_BACKLOG_20260715|PX_CAPABILITY_BACKLOG_20260715" README.md docs
```

Expected: old planning entry points reference the new canonical documents.
