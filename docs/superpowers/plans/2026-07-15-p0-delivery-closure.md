# P0 Delivery Closure Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining P0 delivery gate locally by generating current-run evidence for P0-B/C/D/E, final regression, evidence bundle, delivery readiness, and release review.

**Architecture:** Keep existing PowerShell delivery tooling intact and add a macOS/Linux shell delivery candidate path because this workspace has no `pwsh`. The shell path must generate auditable report artifacts under `reports/`, run existing Go and bash validation where available, and fail on missing required evidence rather than marking documentation-only work as complete.

**Tech Stack:** Go 1.24.3, POSIX shell, existing `scripts/*.sh`, JSON/Markdown report artifacts.

---

### Task 1: Baseline Current Delivery Blockers

**Files:**
- Read: `scripts/run_p0_evidence_suite.ps1`
- Read: `scripts/run_p0_delivery_candidate.ps1`
- Read: `docs/planning/P0_REMAINING_ENGINEERING_BACKLOG.md`
- Read: `docs/planning/P0_CURRENT_STATUS_SUMMARY.md`

- [ ] Confirm `pwsh` is unavailable in this environment.
- [ ] Map the PowerShell-only gates to shell-executable evidence generation.
- [ ] Keep existing user modifications to `docs/planning/P0_CURRENT_STATUS_SUMMARY.md` untouched unless the final evidence update intentionally changes it.

### Task 2: Add Shell Delivery Candidate

**Files:**
- Create: `scripts/run_p0_delivery_candidate.sh`
- Create or modify: focused helper scripts only if needed.

- [ ] Write a shell runner that creates a timestamped run id and report directory.
- [ ] Run `scripts/verify_p0_core.sh`.
- [ ] Run P0-B focused recovery audit/selftest and current crash recovery evidence.
- [ ] Run P0-C concurrency validation or focused consistency evidence.
- [ ] Run P0-D metrics/logging evidence, including a local metrics endpoint probe if supported.
- [ ] Run P0-E rollback/canary selftest and timed rollback evidence.
- [ ] Run final regression using Go 1.24.3.
- [ ] Generate JSON and Markdown reports for suite summary, evidence bundle, approval packet, delivery readiness, and remediation if blocked.

### Task 3: Verify And Commit

**Files:**
- Modify: generated scripts/docs as needed.
- Do not commit generated bulky runtime artifacts unless they are small required evidence reports.

- [ ] Run the new shell delivery candidate.
- [ ] Run existing P0 core and focused package tests.
- [ ] Check `git status` for unintended files.
- [ ] Commit code/tooling and small evidence/docs updates.
