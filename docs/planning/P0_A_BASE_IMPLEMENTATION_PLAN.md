# P0-A-Base Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the minimum `server/innodb/engine` test baseline needed to start P0 A work, then implement structured duplicate-key detection with TDD.

**Architecture:** Start with the narrowest compile-time blockers in `engine` tests that prevent focused execution, fixing API drift in small slices. Once the package can run the duplicate-key tests reliably, implement `T-A1-03` by removing message-pattern fallback and relying on structured error typing/codes.

**Tech Stack:** Go 1.20+, `go test`, existing engine/unit test suite

---

## Chunk 1: G-01 Minimum Baseline Recovery

### Task 1: Confirm and isolate the first compile blocker cluster

**Files:**
- Read: `server/innodb/engine/index_lookup_test.go`
- Read: `server/innodb/engine/index_reading_test.go`
- Read: `server/innodb/engine/volcano_executor.go`
- Read: `server/innodb/basic/record.go`

- [ ] **Step 1: Re-run the narrow failing command**

Run: `go test ./server/innodb/engine -run 'TestIndexScanOperator|TestIndexReading'`
Expected: compile failure around `NewIndexScanOperator` and stale helper methods

- [ ] **Step 2: Document exact API drift**

Capture:
- old constructor signature used by tests
- current constructor signature in production
- stale helper methods expected by tests but missing in production

- [ ] **Step 3: Choose the minimum compatibility direction**

Rule:
- prefer updating tests if production API is already coherent
- prefer adding compatibility code only if multiple current call sites still need the old behavior

### Task 2: Fix the first compile blocker slice with red-green verification

**Files:**
- Modify: `server/innodb/engine/index_lookup_test.go`
- Modify: `server/innodb/engine/index_reading_test.go`
- Optionally modify: `server/innodb/engine/volcano_executor.go`

- [ ] **Step 1: Run the focused failing command**

Run: `go test ./server/innodb/engine -run 'TestIndexScanOperator_CoveringIndex|TestIndexReading_NextFromIndex'`
Expected: FAIL to compile

- [ ] **Step 2: Make the minimal change to resolve constructor drift**

Keep scope limited to:
- aligning constructor invocations with current API, or
- adding one compatibility shim if strongly justified

- [ ] **Step 3: Re-run the same focused command**

Run: `go test ./server/innodb/engine -run 'TestIndexScanOperator_CoveringIndex|TestIndexReading_NextFromIndex'`
Expected: compile moves forward; if new failure appears, it should be the next real blocker

- [ ] **Step 4: Repeat for the next blocker in the same cluster only**

Stop when the index-related focused tests compile and run, even if they still fail logically.

## Chunk 2: T-A1-03 Structured Duplicate-Key Detection

### Task 3: Create the failing duplicate-key regression test

**Files:**
- Read: `server/innodb/engine/dml_operators.go`
- Read: `server/innodb/engine/dml_operators_duplicate_test.go`
- Modify: `server/innodb/engine/dml_operators_duplicate_test.go`

- [ ] **Step 1: Add or tighten a test proving message-pattern fallback is not accepted**

Target behavior:
- typed duplicate errors return `true`
- SQL duplicate codes return `true`
- plain message-only errors return `false`

- [ ] **Step 2: Run only the duplicate-key tests**

Run: `go test ./server/innodb/engine -run 'Test.*Duplicate.*'`
Expected: FAIL because message-based duplicate detection still exists

### Task 4: Implement the minimal structured-error fix

**Files:**
- Modify: `server/innodb/engine/dml_operators.go`
- Test: `server/innodb/engine/dml_operators_duplicate_test.go`

- [ ] **Step 1: Remove message/regex fallback from duplicate detection**

Allowed signals:
- `errors.Is(err, basic.ErrDuplicateKey)`
- `errors.As(err, *common.SQLError)` with duplicate-entry codes

- [ ] **Step 2: Re-run duplicate-key tests**

Run: `go test ./server/innodb/engine -run 'Test.*Duplicate.*'`
Expected: PASS

- [ ] **Step 3: Re-run a broader engine subset**

Run: `go test ./server/innodb/engine -run 'Test.*Duplicate.*|TestIndexScanOperator.*|TestIndexReading.*'`
Expected: no regression in the slices touched during this plan

## Chunk 3: Verification and Handoff

### Task 5: Verify current completion boundary honestly

**Files:**
- None

- [ ] **Step 1: Run fresh verification commands**

Run:
- `go test ./server/innodb/engine -run 'Test.*Duplicate.*'`
- `go test ./server/innodb/engine -run 'TestIndexScanOperator.*|TestIndexReading.*'`

- [ ] **Step 2: Record actual remaining blockers**

If full `go test ./server/innodb/engine` still fails, document the next failing cluster instead of claiming baseline fully restored.
