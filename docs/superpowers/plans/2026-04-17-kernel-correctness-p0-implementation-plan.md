# Kernel Correctness P0 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore a trustworthy P0 correctness baseline for the kernel by fixing configuration/test panics, removing error-masking fallback paths, aligning stale engine tests with the current entrypoints, hardening wrapper critical paths, and eliminating targeted flaky tests.

**Architecture:** Work from the failure chain inward: first restore safe configuration and a reproducible baseline, then tighten `engine` execution paths so failures are explicit, then collapse page/wrapper ambiguity to a single production entry, and finally remove timing-based test noise. The plan avoids new SQL or protocol features and focuses only on correctness-critical paths that currently pollute regression signals.

**Tech Stack:** Go 1.20 module, Go test, internal executor/storage/wrapper packages, existing docs under `docs/planning/` and `docs/superpowers/specs/`.

---

## Files and Responsibilities

- Modify: `server/conf/config.go`
  - Make config access nil-safe for tests and default configs.
- Modify: `server/auth/auth_integration_test.go`
  - Stop constructing invalid empty configs that panic engine initialization; assert failure modes intentionally.
- Create: `server/conf/config_test.go`
  - Add focused regression tests for nil/empty config getters and defaults.
- Modify: `server/innodb/engine/storage_adapter.go`
  - Replace silent fallback behavior with explicit, typed or well-scoped errors.
- Modify: `server/innodb/engine/storage_adapter_test.go`
  - Assert explicit failures instead of fallback results.
- Modify: `server/innodb/engine/unified_executor.go`
  - Remove or narrow manual operator-tree fallback that masks planner/build failures.
- Modify: `server/innodb/engine/executor.go`
  - Align SHOW execution routing with current behavior; avoid stale helper assumptions and stringly fallback decisions.
- Modify: `server/innodb/engine/executor_show_routing_test.go`
  - Rebase failing tests onto current entrypoints and preserve intended SHOW filtering behavior.
- Modify: `server/innodb/engine/hash_operators_test.go`
  - Stop depending on removed constructor APIs; test current aggregate operator contract.
- Modify: `server/innodb/storage/wrapper/page/page_wrapper_base.go`
  - Harden base wrapper behavior and clarify deprecated path role.
- Modify: `server/innodb/storage/wrapper/page/rollback_page_wrapper.go`
  - Implement minimal parse/validation behavior instead of TODO pass-through.
- Create: `server/innodb/storage/wrapper/page/rollback_page_wrapper_test.go`
  - Add direct tests for rollback page parse/validation behavior.
- Modify: `server/innodb/storage/wrapper/types/base_page.go`
  - If still on production path, reduce to thin compatibility layer or document its limited role.
- Modify: `server/innodb/storage/store/pages/page.go`
  - Ensure it is not competing as a parallel production entry for wrapper responsibilities.
- Modify: `server/innodb/buffer_pool/prefetch_test.go`
  - Keep condition-based assertions and remove remaining timing assumptions if any.
- Modify: `server/innodb/manager/btree_cache_limit_test.go`
  - Ensure cache cleanup assertions do not rely on background timing.
- Modify: `server/innodb/manager/space_expansion_concurrent_test.go`
  - Replace any timing-sensitive checks with deterministic synchronization.
- Modify: `docs/planning/PRODUCTION_GAP_EXECUTION_TABLE.md`
  - Update status/evidence placeholders for the gaps directly addressed in this plan after verification.

**Spec Reference:**
- `docs/superpowers/specs/2026-04-17-kernel-correctness-p0-design.md`

---

## Chunk 1: Safe Baseline and Failure Classification

### Task 1: Make Config Access Nil-Safe and Stop Test Panics

**Files:**
- Create: `server/conf/config_test.go`
- Modify: `server/conf/config.go`
- Modify: `server/auth/auth_integration_test.go`

- [ ] **Step 1: Write the failing config tests**

Add tests that prove `Cfg.GetString` and `Cfg.GetInt` do not panic when `cfg.Raw` is nil and that empty/default configs behave deterministically in test setup.

```go
func TestCfgGetString_NilRawReturnsEmpty(t *testing.T) {
	cfg := &Cfg{}
	if got := cfg.GetString("innodb.redo_log_dir"); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestCfgGetInt_NilRawReturnsZero(t *testing.T) {
	cfg := &Cfg{}
	if got := cfg.GetInt("innodb.page_size"); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}
```

- [ ] **Step 2: Run tests to verify RED**

Run: `go test ./server/conf ./server/auth -run 'TestCfgGetString_NilRawReturnsEmpty|TestCfgGetInt_NilRawReturnsZero|TestAuthServiceIntegration' -count=1`

Expected:
- config tests fail or `TestAuthServiceIntegration` still panics due to nil `cfg.Raw`

- [ ] **Step 3: Write minimal implementation**

In `server/conf/config.go`, guard `cfg == nil` and `cfg.Raw == nil` in `GetString` / `GetInt`.  
In `server/auth/auth_integration_test.go`, stop using `&conf.Cfg{}` directly; construct a valid test config with `conf.NewCfg()` or set `Raw` explicitly so engine initialization does not panic before the auth path is exercised.

- [ ] **Step 4: Run focused tests to verify GREEN**

Run: `go test ./server/conf ./server/auth -run 'TestCfgGetString_NilRawReturnsEmpty|TestCfgGetInt_NilRawReturnsZero|TestAuthServiceIntegration|TestEngineAccess' -count=1`

Expected:
- no panic
- config tests pass
- auth integration tests either pass or fail with explicit auth/engine errors rather than initialization panic

- [ ] **Step 5: Commit**

```bash
git add server/conf/config.go server/conf/config_test.go server/auth/auth_integration_test.go
git commit -m "test: harden config access for kernel correctness baseline"
```

### Task 2: Freeze the Failure Clusters in the Plan’s Target Scope

**Files:**
- Modify: `docs/planning/PRODUCTION_GAP_EXECUTION_TABLE.md`

- [ ] **Step 1: Record current failure clusters before code fixes spread**

Add a short note under the relevant P0 rows capturing the current failure groups:
- config/init panic cluster
- SHOW routing API drift
- hash aggregate constructor drift
- storage adapter fallback masking
- wrapper TODO / flaky tests

- [ ] **Step 2: Run targeted verification command and save output locally**

Run: `go test ./server/innodb/engine ./server/auth ./server/conf -count=1`

Expected:
- failures group into the known clusters above; output is used to guide subsequent tasks

- [ ] **Step 3: Commit**

```bash
git add docs/planning/PRODUCTION_GAP_EXECUTION_TABLE.md
git commit -m "docs: capture kernel correctness failure clusters"
```

---

## Chunk 2: Engine Mainline Correctness

### Task 3: Remove Error-Masking Fallbacks from `storage_adapter.go`

**Files:**
- Modify: `server/innodb/engine/storage_adapter.go`
- Modify: `server/innodb/engine/storage_adapter_test.go`

- [ ] **Step 1: Write failing tests for explicit error behavior**

Extend `storage_adapter_test.go` to cover current fallback sites:
- missing table info
- missing B+ tree manager
- search/read/parse failures
- invalid slot access

Example shape:

```go
func TestStorageAdapter_GetRecordByPrimaryKey_ReturnsExplicitErrorWhenBTreeLookupFails(t *testing.T) {
	// build adapter with deterministic failing dependency
	// assert returned error contains module context and does not silently fallback
}
```

- [ ] **Step 2: Run adapter tests to verify RED**

Run: `go test ./server/innodb/engine -run 'TestStorageAdapter_' -count=1`

Expected:
- new tests fail because current code logs and falls back

- [ ] **Step 3: Write minimal implementation**

In `storage_adapter.go`:
- replace `using fallback` branches with explicit returns
- keep logging, but make the return path deterministic
- include enough context to identify space ID/page/slot/table path

Do **not** add broad error wrapping everywhere; only the fallback points identified by tests.

- [ ] **Step 4: Run adapter tests to verify GREEN**

Run: `go test ./server/innodb/engine -run 'TestStorageAdapter_' -count=1`

Expected:
- adapter tests pass
- fallback sites no longer silently return synthetic results

- [ ] **Step 5: Commit**

```bash
git add server/innodb/engine/storage_adapter.go server/innodb/engine/storage_adapter_test.go
git commit -m "fix: remove storage adapter fallback masking"
```

### Task 4: Tighten `unified_executor.go` Fallback Behavior

**Files:**
- Modify: `server/innodb/engine/unified_executor.go`
- Modify: `server/innodb/engine/unified_executor_test.go`
- Modify: `server/innodb/engine/unified_executor_build_tree_test.go`

- [ ] **Step 1: Write failing tests that distinguish explicit planner/build errors from fallback success**

Add or extend tests so failures in:
- logical plan creation
- optimizer application
- physical operator tree build

lead to explicit failure rather than manual operator-tree fallback, unless a specific fallback is intentionally retained and documented.

- [ ] **Step 2: Run focused unified executor tests to verify RED**

Run: `go test ./server/innodb/engine -run 'TestUnifiedExecutor|TestBuildOperatorTree' -count=1`

Expected:
- one or more tests fail because current code falls back to manual operator trees

- [ ] **Step 3: Write minimal implementation**

In `unified_executor.go`:
- remove broad fallback paths that hide planner/build failures
- if a fallback must remain temporarily, guard it behind a narrow, test-documented case
- ensure returned errors identify which phase failed

- [ ] **Step 4: Run focused tests to verify GREEN**

Run: `go test ./server/innodb/engine -run 'TestUnifiedExecutor|TestBuildOperatorTree' -count=1`

Expected:
- failures are explicit and tests pass against the intended contract

- [ ] **Step 5: Commit**

```bash
git add server/innodb/engine/unified_executor.go server/innodb/engine/unified_executor_test.go server/innodb/engine/unified_executor_build_tree_test.go
git commit -m "fix: make unified executor failures explicit"
```

### Task 5: Rebase SHOW Routing and Hash Aggregate Tests onto Current APIs

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/engine/executor_show_routing_test.go`
- Modify: `server/innodb/engine/hash_operators_test.go`
- Optional modify: `server/innodb/engine/volcano_executor.go`

- [ ] **Step 1: Write or update failing tests against current intended entrypoints**

For SHOW routing:
- keep coverage for LIKE/WHERE filtering and `SHOW CREATE TABLE`
- stop relying on removed helper signatures unless the current implementation genuinely still needs a helper

For hash aggregate:
- use the current constructor contract, or add a narrow compatibility shim only if the missing constructor reflects real supported behavior

- [ ] **Step 2: Run targeted tests to verify RED**

Run: `go test ./server/innodb/engine -run 'TestXMySQLExecutor_ExecuteShow|TestHashAggregateOperator_' -count=1`

Expected:
- tests fail due to stale signatures or outdated constructor assumptions

- [ ] **Step 3: Write minimal implementation**

SHOW path:
- prefer updating tests to current `executeShowStatement` / `executeQuery` behavior
- only add a helper in `executor.go` if raw-query-aware filtering is still part of the supported contract

Hash aggregate path:
- prefer updating tests to `NewHashAggregateOperator`
- only reintroduce a helper if multiple active call sites still require column index injection and tests prove it is part of current behavior

- [ ] **Step 4: Run targeted tests to verify GREEN**

Run: `go test ./server/innodb/engine -run 'TestXMySQLExecutor_ExecuteShow|TestHashAggregateOperator_' -count=1`

Expected:
- SHOW routing tests pass against current behavior
- hash aggregate tests pass against current constructor/API

- [ ] **Step 5: Commit**

```bash
git add server/innodb/engine/executor.go server/innodb/engine/executor_show_routing_test.go server/innodb/engine/hash_operators_test.go server/innodb/engine/volcano_executor.go
git commit -m "test: align engine tests with current correctness contracts"
```

---

## Chunk 3: Wrapper Mainline and Flaky Tests

### Task 6: Make Rollback/Page Wrapper Critical Paths Minimally Viable

**Files:**
- Modify: `server/innodb/storage/wrapper/page/page_wrapper_base.go`
- Modify: `server/innodb/storage/wrapper/page/rollback_page_wrapper.go`
- Modify: `server/innodb/storage/wrapper/types/base_page.go`
- Modify: `server/innodb/storage/store/pages/page.go`
- Create: `server/innodb/storage/wrapper/page/rollback_page_wrapper_test.go`

- [ ] **Step 1: Write failing wrapper tests**

Add tests for:
- invalid rollback page size/data returns explicit error
- valid parse preserves page metadata
- deprecated `BasePageWrapper` does not compete with the wrapper/types production path

Example:

```go
func TestRollbackPageWrapper_ParseFromBytes_RejectsShortData(t *testing.T) {
	w := NewRollbackPageWrapper(1, 1)
	if err := w.ParseFromBytes(make([]byte, common.PageSize-1)); err == nil {
		t.Fatal("expected invalid rollback data error")
	}
}
```

- [ ] **Step 2: Run wrapper tests to verify RED**

Run: `go test ./server/innodb/storage/wrapper/page -run 'TestRollbackPageWrapper|TestPageChecksum|TestNew' -count=1`

Expected:
- new rollback wrapper tests fail because parse path is still TODO / pass-through

- [ ] **Step 3: Write minimal implementation**

In `rollback_page_wrapper.go`:
- implement minimal structural validation
- fail loudly on invalid or incomplete data
- avoid TODO pass-through behavior

In `page_wrapper_base.go` / `types/base_page.go` / `store/pages/page.go`:
- make the production entrypoint unambiguous
- if deprecated wrapper remains, ensure it is a thin compatibility layer instead of a competing logic owner

- [ ] **Step 4: Run wrapper tests to verify GREEN**

Run: `go test ./server/innodb/storage/wrapper/page -count=1`

Expected:
- rollback/page wrapper tests pass
- no ambiguous behavior between deprecated and production paths in the targeted surface

- [ ] **Step 5: Commit**

```bash
git add server/innodb/storage/wrapper/page/page_wrapper_base.go server/innodb/storage/wrapper/page/rollback_page_wrapper.go server/innodb/storage/wrapper/page/rollback_page_wrapper_test.go server/innodb/storage/wrapper/types/base_page.go server/innodb/storage/store/pages/page.go
git commit -m "fix: harden rollback wrapper critical path"
```

### Task 7: Eliminate Timing-Based Flakiness in Target Tests

**Files:**
- Modify: `server/innodb/buffer_pool/prefetch_test.go`
- Modify: `server/innodb/manager/btree_cache_limit_test.go`
- Modify: `server/innodb/manager/space_expansion_concurrent_test.go`
- Optional modify: `server/innodb/buffer_pool/prefetch.go`

- [ ] **Step 1: Add or refine deterministic assertions**

For each target test:
- replace fixed sleep assumptions with `waitUntil`, explicit cleanup calls, channel synchronization, or bounded polling
- add helper(s) locally in tests if needed

- [ ] **Step 2: Run target tests to verify RED where behavior is still timing-dependent**

Run:

```bash
go test ./server/innodb/buffer_pool -run 'TestPrefetch' -count=1
go test ./server/innodb/manager -run 'TestBTREE006|TestConcurrent(CheckAndExpand|ExpandSpace|AsyncExpand|GetStats|RaceConditionDetection|ExpansionLockEffectiveness|NoDataOverwrite)' -count=1
```

Expected:
- at least one test or assertion still depends on non-deterministic timing and needs conversion

- [ ] **Step 3: Write minimal implementation**

Prefer test-only changes:
- condition polling
- helper methods
- explicit flush/cleanup/sync calls

Only touch production code such as `prefetch.go` if a tiny synchronization hook is required to make tests deterministic.

- [ ] **Step 4: Run repeated stability checks**

Run:

```bash
go test ./server/innodb/buffer_pool -run 'TestPrefetch' -count=10
go test ./server/innodb/manager -run 'TestBTREE006|TestConcurrent(CheckAndExpand|ExpandSpace|AsyncExpand|GetStats|RaceConditionDetection|ExpansionLockEffectiveness|NoDataOverwrite)' -count=10
```

Expected:
- no random failures across repeated runs

- [ ] **Step 5: Commit**

```bash
git add server/innodb/buffer_pool/prefetch_test.go server/innodb/manager/btree_cache_limit_test.go server/innodb/manager/space_expansion_concurrent_test.go server/innodb/buffer_pool/prefetch.go
git commit -m "test: remove timing-based flakiness from kernel correctness suite"
```

### Task 8: Final Verification and Gap Table Update

**Files:**
- Modify: `docs/planning/PRODUCTION_GAP_EXECUTION_TABLE.md`

- [ ] **Step 1: Update execution-table statuses and evidence placeholders**

For addressed rows (`GAP-01`, `GAP-06`, `GAP-07` and any directly advanced row), update:
- `当前状态`
- `证据路径`
- short note on what was actually verified

- [ ] **Step 2: Run the full targeted verification suite**

Run:

```bash
go test ./server/conf ./server/auth -count=1
go test ./server/innodb/storage/wrapper/page -count=1
go test ./server/innodb/buffer_pool -run 'TestPrefetch' -count=1
go test ./server/innodb/manager -run 'TestBTREE006|TestConcurrent(CheckAndExpand|ExpandSpace|AsyncExpand|GetStats|RaceConditionDetection|ExpansionLockEffectiveness|NoDataOverwrite)' -count=1
go test ./server/innodb/engine -count=1
```

Expected:
- target packages pass or any remaining failures are narrowed to non-scope clusters and documented

- [ ] **Step 3: If `./server/innodb/engine` still fails, document the remaining failure cluster before stopping**

Capture:
- failing test names
- whether each is stale test, missing implementation, or out-of-scope feature
- why the remaining failure is outside this plan’s P0 boundary

- [ ] **Step 4: Commit**

```bash
git add docs/planning/PRODUCTION_GAP_EXECUTION_TABLE.md
git commit -m "docs: record kernel correctness p0 verification evidence"
```

---

Plan complete and saved to `docs/superpowers/plans/2026-04-17-kernel-correctness-p0-implementation-plan.md`. Ready to execute?

