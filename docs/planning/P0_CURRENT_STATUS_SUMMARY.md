# P0 Current Status Summary

## 2026-07-15 capability-priority reset

Current canonical priority source:

- `docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md`
- `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`
- `docs/planning/P1_CAPABILITY_BACKLOG_20260715.md`
- `docs/planning/PX_CAPABILITY_BACKLOG_20260715.md`

Current interpretation:

- Basic CRUD and JDBC DML are working in the focused tested path.
- Production approval is still not complete.
- Older evidence-suite PASS records prove that the evidence tooling can run; they do not close the new P0 capability backlog.
- The highest-priority blockers are now storage page/B+Tree closure, durable secondary indexes, transaction/MVCC/recovery correctness, SQL/JDBC compatibility, and live production-readiness evidence.

## Purpose

This document summarizes the current P0 production-readiness state for XMySQL.

It is intended for handoff and review. It does not replace the detailed planning documents, scripts, reports, or final evidence bundle.

## Overall status

Current status: **not production-approved yet**.

The project has moved from an unstable baseline into a structured P0 evidence workflow:

- P0-A engineering baseline has been restored in the default test profile.
- P0-B crash recovery has an executed 3-run drill report and additional state evidence tooling.
- P0-C concurrency validation has command-level validation tooling but still needs generated reports and explicit consistency checks.
- P0-D observability has documentation/configuration smoke tooling but still needs generated reports and live metrics/export evidence.
- P0-E release/rollback has runbooks and smoke tooling but still needs a timed full-chain drill.
- A top-level evidence suite and bundle verifier now exist.

## Workstream status

| Workstream | Status | Notes |
|---|---|---|
| P0-A baseline | Partial acceptance | Default baseline was restored, but final regression evidence must be refreshed before approval |
| P0-B recovery | Partial acceptance | 3-run recovery drill exists; command-level state evidence JSON must still be generated and verified |
| P0-C concurrency | Tooling ready | Scripts and verifier exist; report has not been generated; explicit consistency checks still missing |
| P0-D observability | Tooling ready | Metric catalog, samples, alerts, smoke script, and verifier exist; live export evidence still missing |
| P0-E release/rollback | Tooling ready | Runbooks, drill template, smoke script, and verifier exist; timed drill still missing |
| Evidence bundle | Tooling ready | Bundle verifier exists; complete bundle cannot pass until missing reports are generated |

## Known generated evidence

Current known archived report:

- `reports/crash_recovery_drill_20260614_070446.log`
- `reports/crash_recovery_drill_20260614_070446.md`

Known pending reports:

- `reports/crash_recovery_drill_<timestamp>.state.json`
- `reports/concurrency_validation_<timestamp>.md`
- `reports/concurrency_validation_<timestamp>.json`
- `reports/observability_smoke_<timestamp>.md`
- `reports/observability_smoke_<timestamp>.json`
- `reports/full_chain_drill_<timestamp>.md`
- `reports/full_chain_drill_<timestamp>.json`
- `reports/final_regression_gate_<timestamp>.md`
- `reports/final_regression_gate_<timestamp>.json`
- `reports/p0_evidence_bundle_<timestamp>.md`
- `reports/p0_evidence_bundle_<timestamp>.json`

## Primary command to generate the local evidence package

```powershell
./scripts/run_p0_evidence_suite.ps1 -RecoveryRuns 3 -ConcurrencyRuns 1
```

This command is expected to execute tests and smoke scripts. Run it intentionally as a validation action.

## Approval blockers

Production gray-release approval remains blocked by:

- Missing generated P0-B command-level state evidence JSON.
- Missing generated P0-C validation report.
- Missing generated P0-D smoke report.
- Missing generated P0-E full-chain smoke report.
- Missing generated final regression report.
- Missing passing top-level evidence bundle report.
- Missing live metrics export evidence.
- Missing timed full release/rollback/data recovery drill.
- Missing owner sign-off.

## Recommended next action

Run the tooling preflight and delivery candidate runner, inspect any failures, then update the release approval packet with the generated report paths. Static script and documentation hardening alone is not sufficient approval evidence.

If the suite fails, fix the first failing workstream rather than marking downstream work as accepted.

Review generated P0 packets in this order: preflight report, delivery candidate report, governance gate report when present, linked evidence suite summary, delivery readiness audit, remediation packet, raw log, and release approval packet. Delivery is approved only after the candidate is `PASS`, required governance is `PASS`, owner sign-off is valid when required, and the release approval packet is no longer on `HOLD`.

Final human review checklist: `docs/planning/P0_DELIVERY_REVIEW_CHECKLIST.md`. Checklist structure verifier: `scripts/verify_p0_delivery_review_checklist.ps1`.

---

## 2026-06-14 approval packet generator update

A release approval packet generator has been added:

- `scripts/generate_p0_release_approval_packet.ps1`

It is also connected to the full evidence suite:

```powershell
./scripts/run_p0_evidence_suite.ps1 -RecoveryRuns 3 -ConcurrencyRuns 1
```

Expected approval packet output:

- `reports/p0_release_approval_packet_<timestamp>.md`

---

## 2026-06-14 approval packet verifier update

Approval packet validation is now part of the full evidence suite.

Verifier:

- `scripts/verify_p0_release_approval_packet.ps1`

The verifier checks required packet sections and keeps the generated decision at `HOLD` until owners review and sign off.

---

## 2026-06-14 tooling preflight update

A P0 tooling preflight verifier has been added:

```powershell
./scripts/verify_p0_tooling_preflight.ps1
```

It checks that the expected P0 docs, runbooks, scripts, templates, report instructions, and delivery review checklist structure exist before running the full evidence suite.

It also validates the delivery checklist structure with `scripts/verify_p0_delivery_review_checklist.ps1`, then validates its generated JSON and Markdown report with `scripts/verify_p0_tooling_preflight_report.ps1`, including summary counts, limitations, run id, status, listed check paths, check status/actual pairs, and Markdown limitation text.

Archived preflight packets can be rechecked independently with `scripts/verify_p0_tooling_preflight_report.ps1 -Path <json> -MarkdownPath <md>`, matching the candidate and governance report verifier pattern.

The full suite runs this preflight first.

---

## 2026-06-14 GitHub Actions evidence workflow update

A manual GitHub Actions workflow has been added:

- `.github/workflows/p0-evidence.yml`

Modes:

- `preflight`: checks P0 tooling and documentation presence only.
- `full`: runs the full P0 evidence suite and uploads `reports/**`.
- `candidate`: runs the delivery candidate wrapper with focused P0-B/C/D/E evidence enabled by default.
- `governance`: runs the governance gate without executing the full technical evidence suite.

Use this workflow when evidence needs to be generated outside a local Windows workstation.

---

## 2026-06-14 operator guide update

A P0 evidence suite operator guide has been added:

- `docs/operations/p0_evidence_suite_operator_guide.md`

It explains local preflight, local full suite, manual GitHub Actions usage, review order, failure handling, and approval boundaries.

---

## 2026-06-14 remaining engineering backlog and risk register update

Remaining work and risks are now tracked in:

- `docs/planning/P0_REMAINING_ENGINEERING_BACKLOG.md`
- `docs/planning/P0_RISK_REGISTER.md`

These documents make the current completion boundary explicit: the evidence framework exists, but production approval remains blocked by storage-state recovery proof, explicit concurrency consistency checks, live observability proof, alert delivery drill, timed rollback drill, and owner sign-off.

---

## 2026-06-14 preflight coverage update

P0 tooling preflight now checks the remaining engineering backlog, risk register, and evidence suite operator guide in addition to scripts, runbooks, workflow, and report instructions.

---

## 2026-06-14 remaining engineering execution plan update

Remaining P0 engineering work is now broken into an execution plan and next-actions checklist:

- `docs/planning/P0_REMAINING_ENGINEERING_EXECUTION_PLAN.md`
- `docs/planning/P0_NEXT_ACTIONS.md`

Use these documents after the first evidence suite run to decide whether to fix P0-B recovery proof, P0-C consistency proof, P0-D observability proof, P0-E rollback proof, or final approval packaging next.

---

## 2026-06-14 metrics export bundle integration update

The final P0 evidence bundle and approval packet now require metrics export smoke evidence:

- `reports/metrics_export_<timestamp>.md`
- `reports/metrics_export_<timestamp>.json`

This ensures P0-D export artifacts are not omitted from final review.

## 2026-06-15 Evidence suite run - PASS

The P0 evidence suite was executed after the latest observability updates.

Result:
- Overall P0 evidence suite: PASS
- Final default regression gate: PASS
- P0 release approval packet: READY_FOR_REVIEW
- Missing evidence count reported by approval packet: 0

Generated evidence:
- P0-B crash recovery report: `reports/crash_recovery_drill_20260615_001527.md`
- P0-B state evidence: `reports/crash_recovery_drill_20260615_001527.state.json`
- P0-C concurrency report: `reports/concurrency_validation_20260615_001537.md`
- P0-D observability smoke: `reports/observability_smoke_20260615_001536.md`
- P0-D metrics export: `reports/metrics_export_20260615_001536.md`
- P0-D structured logging: `reports/structured_logging_20260615_001536.md`
- P0-D alert drill: `reports/alert_drill_20260615_001537.md`
- P0-E full-chain drill: `reports/full_chain_drill_20260615_001550.md`
- Final regression gate: `reports/final_regression_gate_20260615_001551.md`
- Evidence bundle: `reports/p0_evidence_bundle_20260615_001559.md`
- Release approval packet: `reports/p0_release_approval_packet_20260615_001559.md`

Tooling note:
- The first suite run exposed a PowerShell singleton-array counting issue in the P0-C concurrency tooling.
- Fixed scripts: scripts/concurrency_validation.ps1 and scripts/verify_concurrency_validation_report.ps1 now wrap possibly singleton collections with @(...) before using .Count.

Remaining live verification gap:
- Start a real server with profiling enabled and run the metrics endpoint probe to observe /metrics and active connection changes from live MySQL sessions.

## 2026-06-19 P0-B recovery state evidence contract update

The crash recovery drill state evidence has been upgraded from plain command replay checks to a structured test-backed recovery state contract.

Updated tooling:
- `scripts/crash_recovery_drill.ps1`
- `scripts/verify_crash_recovery_state_evidence.ps1`

New evidence fields:
- root `verification_level = test_backed_recovery_state_contract`
- root `scenario_matrix` covering transaction lifecycle, redo replay boundary, undo rollback recovery, interrupted-commit recovery, and savepoint partial rollback
- per-run `state_evidence` with WAL replay boundary and interrupted-commit recovery checks
- explicit `row_level_state_diff` and `page_level_state_diff` entries marked `NOT_VERIFIED` until standalone storage snapshot artifacts are implemented

Impact:
- Advances `P0-B-STATE-01` by making recovered-state expectations explicit and verifier-enforced.
- Advances `P0-B-HALF-01` by documenting interrupted/half-commit recovery as a test-backed recovery-phase contract.
- Does not fully close P0-B because standalone page/row/WAL snapshot diff evidence is still required.

Next evidence command:

```powershell
./scripts/crash_recovery_drill.ps1 -Runs 3 -StateEvidence
./scripts/verify_crash_recovery_state_evidence.ps1 -Path reports/crash_recovery_drill_<timestamp>.state.json
```

## 2026-06-20 Delivery readiness hard gate update

A delivery readiness audit has been added to prevent evidence presence from being mistaken for production deliverability.

New tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`

Suite integration:
- `scripts/verify_p0_tooling_preflight.ps1` now checks the delivery audit tooling.
- `scripts/run_p0_evidence_suite.ps1` now runs the delivery readiness audit after release approval packet validation.

Audit behavior:
- `READY` requires all delivery checks to pass.
- `NOT_READY` is expected while the approval packet remains on `HOLD`, owner sign-off items are unchecked, risk owners are `TBD`, or P0 risks remain `Open`.
- This gate is intentionally stricter than the release approval packet's `READY_FOR_REVIEW` status. `READY_FOR_REVIEW` means evidence files are present; `READY` means the project is actually deliverable according to risk and approval gates.

Operational note:
- A full evidence suite may now fail at the final delivery-readiness step even after all evidence-generation steps pass. Treat that as a correct delivery blocker, not as a tooling failure.

Example commands:

```powershell
./scripts/delivery_readiness_audit.ps1 -ApprovalPacket reports/p0_release_approval_packet_<timestamp>.md
./scripts/verify_delivery_readiness_audit.ps1 -Path reports/delivery_readiness_audit_<timestamp>.json
```

## 2026-06-20 Suite exit-code enforcement update

`run_p0_evidence_suite.ps1` now resets and checks `$LASTEXITCODE` around every `Invoke-Step` body.

Impact:
- Any child script that exits non-zero now fails the suite step explicitly.
- The new delivery readiness audit can act as a real final gate instead of only generating a report.
- A `NOT_READY` delivery audit should stop the suite and identify remaining delivery blockers.

## 2026-06-20 Release packet delivery-boundary update

The release approval packet generator now includes an explicit delivery-readiness boundary.

Updated tooling:
- `scripts/generate_p0_release_approval_packet.ps1`
- `scripts/verify_p0_release_approval_packet.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior change:
- `READY_FOR_REVIEW` means required evidence artifacts are present for review.
- It does not mean the project is deliverable.
- The packet now includes `Delivery status: NOT_ASSESSED_BY_PACKET` and points reviewers to `delivery_readiness_audit.ps1` for the actual deliverability gate.
- The approval packet verifier now requires the delivery-readiness boundary section, audit command, and audit verifier command.

Delivery rule:
- Treat release approval packet validation as a review-material check.
- Treat delivery readiness audit `READY` as the delivery gate.

## 2026-06-20 Delivery audit failure evidence preservation

`run_p0_evidence_suite.ps1` now treats the delivery readiness audit specially:

- The audit step is allowed to return non-zero long enough to write its Markdown/JSON reports.
- The suite then runs `verify_delivery_readiness_audit.ps1` against the generated JSON.
- After schema verification, the suite reads the audit status and fails explicitly unless the status is `READY`.

This preserves `NOT_READY` audit evidence instead of stopping before the delivery blocker report can be schema-verified.

## 2026-06-20 Post-packet delivery gate row update

The release approval packet now renders the delivery readiness audit row as `POST_PACKET_GATE` when the audit JSON has not been generated yet.

Why:
- The approval packet is generated before `delivery_readiness_audit.ps1` runs.
- Showing the audit JSON as `MISSING` inside the packet made the evidence table conflict with `Missing evidence count: 0`.
- `POST_PACKET_GATE` makes the ordering explicit: the packet is review material, and the delivery audit is the follow-up gate.

Interpretation:
- `PRESENT` means an evidence artifact exists before packet generation.
- `POST_PACKET_GATE` means the artifact is expected to be generated after packet validation.
- `READY_FOR_REVIEW` still does not mean deliverable; delivery requires a later audit status of `READY`.

## 2026-06-21 Delivery audit blocker action update

The delivery readiness audit now emits actionable delivery blockers.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`

New report fields:
- `blockers`: failed delivery checks with severity, actual state, and next action
- `next_actions`: de-duplicated remediation actions for delivery review

Verifier behavior:
- `NOT_READY` reports must include at least one blocker.
- `NOT_READY` reports must include at least one next action.

Operational interpretation:
- A `NOT_READY` audit is now an actionable delivery remediation list, not just a failed gate.
- Reviewers should work through `next_actions` before rerunning the delivery audit.

## 2026-06-21 Delivery remediation packet update

A delivery remediation packet generator has been added for turning delivery audit blockers into owner-trackable work items.

New tooling:
- `scripts/delivery_remediation_packet.ps1`
- `scripts/verify_delivery_remediation_packet.ps1`

Preflight integration:
- `scripts/verify_p0_tooling_preflight.ps1` now checks both remediation packet scripts.

Usage:

```powershell
./scripts/delivery_remediation_packet.ps1 -AuditJson reports/delivery_readiness_audit_<timestamp>.json
./scripts/verify_delivery_remediation_packet.ps1 -Path reports/delivery_remediation_packet_<timestamp>.json
```

Interpretation:
- The delivery readiness audit remains the source of truth for `READY` / `NOT_READY`.
- The remediation packet converts `NOT_READY` blockers into `DELIVERY-*` work items with owner and status fields.
- This packet is intended for handoff and tracking after a failed delivery gate.

## 2026-06-21 Full suite remediation packet integration

`run_p0_evidence_suite.ps1` now generates a delivery remediation packet automatically after the delivery readiness audit.

Final delivery sequence:
1. Run `delivery_readiness_audit.ps1` and preserve the audit JSON even when it returns `NOT_READY`.
2. Verify the delivery audit JSON schema.
3. Generate `delivery_remediation_packet_<timestamp>.md/json` from the audit blockers.
4. Verify the remediation packet JSON schema.
5. Fail the suite if the delivery audit status is not `READY`, pointing to both the audit JSON and remediation JSON.

Impact:
- A `NOT_READY` full-suite run now produces both the blocker evidence and an owner-trackable remediation packet before failing.
- Operators no longer need to manually run the remediation packet generator after a failed delivery gate.

## 2026-06-21 Full suite report freshness guard

`run_p0_evidence_suite.ps1` now records a suite start timestamp and only resolves generated reports whose `LastWriteTime` is at or after that start boundary.

Why this matters:
- The suite previously selected the latest matching report by filename pattern only.
- If a step failed before writing a new report, a stale historical report could be selected accidentally.
- Delivery evidence must come from the current suite run, not a previous run.

Behavior:
- Missing current-run artifacts now fail with an error that includes the suite start timestamp.
- Historical reports remain archived but are not reused by the active suite run.

## 2026-06-21 P0 evidence suite summary manifest update

`run_p0_evidence_suite.ps1` now writes a current-run suite summary before the final delivery readiness status is enforced.

Generated artifacts:
- `p0_evidence_suite_<timestamp>.summary.md`
- `p0_evidence_suite_<timestamp>.summary.json`

Purpose:
- Index all evidence artifacts generated by the current suite run.
- Preserve a single handoff index even when the final delivery readiness audit returns `NOT_READY`.
- Point reviewers to the delivery audit JSON, remediation packet JSON, approval packet, evidence bundle, and all P0 workstream reports from the same run.

Boundary:
- The summary is an artifact index, not a replacement for dedicated verifiers.
- Delivery still requires `delivery_readiness_audit.status = READY`.

## 2026-06-21 P0 evidence suite summary verifier update

A dedicated suite summary verifier has been added.

New tooling:
- `scripts/verify_p0_evidence_suite_summary.ps1`

Suite/preflight integration:
- `scripts/run_p0_evidence_suite.ps1` validates the summary JSON before enforcing the final delivery readiness status.
- `scripts/verify_p0_tooling_preflight.ps1` checks that the summary verifier exists.

Verifier behavior:
- Checks `evidence_type = p0_evidence_suite_summary`.
- Checks `delivery_status` is `READY` or `NOT_READY`.
- Checks every listed artifact has a non-empty path.
- Checks every listed artifact path exists.

Boundary:
- This verifier proves the suite summary is structurally valid and points to existing current-run artifacts.
- It does not replace the dedicated P0-B/P0-C/P0-D/P0-E/final/delivery verifiers.

## 2026-06-21 Suite summary human-readable artifact index update

The P0 evidence suite summary now includes human-readable delivery artifacts in addition to JSON evidence paths.

Updated summary entries:
- Delivery readiness Markdown report
- Delivery readiness JSON report
- Delivery remediation Markdown report
- Delivery remediation JSON report
- Suite summary Markdown report
- Suite summary JSON report

Impact:
- Reviewers can start from the suite summary and jump directly to readable delivery blockers and remediation work items.
- `verify_p0_evidence_suite_summary.ps1` checks these paths because they are now listed in the summary artifact array.

## 2026-06-21 Suite summary artifact kind metadata update

The P0 evidence suite summary artifact list now includes a `kind` field for every artifact.

Supported kinds:
- `markdown`
- `json`
- `log`
- `approval`
- `audit-markdown`
- `audit-json`
- `remediation-markdown`
- `remediation-json`
- `summary-markdown`
- `summary-json`

Verifier behavior:
- `verify_p0_evidence_suite_summary.ps1` now requires `artifact.kind`.
- Unsupported artifact kind values fail summary verification.

Purpose:
- Human reviewers can quickly identify readable reports vs machine-verifiable JSON.
- Automation can route artifacts by type without parsing filenames.

## 2026-06-21 Suite summary checksum update

The P0 evidence suite summary now records checksum metadata for generated artifacts.

New artifact fields:
- `checksum_mode`
- `sha256`

Behavior:
- Regular artifacts use `checksum_mode = sha256` and include a lowercase SHA256 digest.
- Summary artifacts use `checksum_mode = self-referential` because a summary cannot stably contain its own content hash.
- `verify_p0_evidence_suite_summary.ps1` recomputes SHA256 for regular artifacts and fails on mismatch.

Purpose:
- Detect accidental or manual artifact modification after suite summary generation.
- Make the suite summary usable as a lightweight integrity manifest for delivery review.

## 2026-06-21 Suite summary artifact freshness update

The P0 evidence suite summary now records `last_write_time` for every indexed artifact.

Verifier behavior:
- `verify_p0_evidence_suite_summary.ps1` parses `suite_started_at` from the summary.
- Every artifact must include `last_write_time`.
- Every artifact `last_write_time` must be at or after the suite start boundary.
- The verifier also checks the actual file `LastWriteTime` to reject stale historical artifacts.

Purpose:
- Prevent current-run summaries from silently pointing at artifacts generated by older suite runs.
- Strengthen the summary as a delivery manifest covering path existence, checksum integrity, and freshness.

## 2026-06-21 Suite summary self-reference timestamp fix

The suite summary now handles self-referential summary artifacts explicitly.

Behavior:
- Regular artifacts record real file `LastWriteTime` and SHA256.
- Summary artifacts use `checksum_mode = self-referential` and record generation-time `last_write_time`, because the summary files do not exist until the summary is written.
- `verify_p0_evidence_suite_summary.ps1` still checks that the actual summary files exist and were written after suite start.
- For regular artifacts, the verifier also compares recorded `last_write_time` to the actual file time within a small tolerance.

Purpose:
- Avoid false failures on summary self-reference.
- Keep strict freshness/integrity checks for all non-summary artifacts.

## 2026-06-21 P0-C concurrency assertion and gap reporting update

P0-C concurrency validation reports now include structured scenario assertions and explicit consistency gaps.

Updated tooling:
- `scripts/concurrency_validation.ps1`
- `scripts/verify_concurrency_validation_report.ps1`

New report fields:
- root `consistency_summary`
- per-scenario `scenario_assertions`
- per-scenario `consistency_gaps`

Behavior:
- `scenario_assertions` record test-backed concurrency expectations for lock behavior, MVCC visibility, long transactions, conflict/recovery concurrency, wrapper concurrency, and storage MVCC deadlock/read-view behavior.
- `consistency_gaps` explicitly mark missing final-state, row-level anomaly, lock-wait metric, deadlock victim, and storage snapshot-diff evidence as `NOT_VERIFIED`.
- The verifier requires these fields and rejects consistency gaps that pretend to be verified before dedicated checks exist.

Impact:
- Advances `P0-C-CONSISTENCY-01`, `P0-C-RANGE-01`, and `P0-C-LOCK-01` by making coverage and gaps machine-readable.
- Does not fully close P0-C because Go-level final-state checkers, row/range anomaly probes, and lock/deadlock metric summaries are still required for full acceptance.

## 2026-06-21 P0-C delivery acceptance gate update

The delivery readiness audit now checks P0-C concurrency acceptance status directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run P0-C concurrency JSON to the delivery readiness audit.
- The audit checks that the P0-C report includes `consistency_summary`.
- Delivery requires `acceptance_status = ACCEPTED`, `required_gap_count = 0`, and `not_verified_gaps = 0`.
- Current `PARTIAL` P0-C reports become explicit delivery blockers with a remediation action to implement final-state checkers, row/range anomaly probes, and lock/deadlock metric summaries.

Impact:
- P0-C gaps are now enforced by the final delivery gate instead of existing only as planning notes.

## 2026-06-21 P0-B delivery acceptance gate update

The delivery readiness audit now checks P0-B recovery state acceptance directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run P0-B state evidence JSON to the delivery readiness audit.
- The audit checks that P0-B state evidence exists.
- The audit checks that row/page state-diff checks are verified and no longer `NOT_VERIFIED`.
- Current P0-B state evidence with row/page diff marked `NOT_VERIFIED` becomes an explicit delivery blocker.

Delivery requirement:
- Implement standalone row/page/WAL state-diff evidence for crash recovery.
- Re-run crash recovery with `-StateEvidence`.
- Verify the generated state JSON.
- Delivery remains `NOT_READY` until P0-B recovery state acceptance passes.

## 2026-06-21 P0-D delivery acceptance gate update

The delivery readiness audit now checks P0-D observability evidence directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run P0-D observability, metrics export, structured logging, and alert drill JSON reports to the delivery readiness audit.
- The audit requires these reports to exist and have `status = PASS`.
- The audit also requires a live metrics endpoint probe JSON with `status = PASS`.
- Because the full suite does not start a live server or generate a metrics endpoint probe by default, missing live endpoint evidence remains an explicit delivery blocker.

Delivery requirement:
- Start a server with profiling enabled.
- Run `scripts/metrics_endpoint_probe.ps1` against `/metrics`.
- Archive and verify a PASS endpoint probe JSON.
- Delivery remains `NOT_READY` until P0-D live metrics endpoint evidence exists.

## 2026-06-21 P0-E delivery acceptance gate update

The delivery readiness audit now checks P0-E release/rollback evidence directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run P0-E full-chain JSON report to the delivery readiness audit.
- The audit requires the P0-E full-chain smoke report to exist and have `status = PASS`.
- The audit also requires `timed_rollback_evidence` with `status = PASS`, rollback duration, and recovery point evidence.
- Current smoke-only P0-E evidence remains an explicit delivery blocker until a timed release/rollback/data-recovery drill is archived.

Delivery requirement:
- Execute a timed release/rollback/data-recovery drill.
- Record rollback duration.
- Record recovery point and replay boundary evidence.
- Delivery remains `NOT_READY` until P0-E timed rollback evidence is present and passing.

## 2026-06-21 Final regression delivery gate update

The delivery readiness audit now checks final regression evidence directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run final regression JSON report to the delivery readiness audit.
- The audit requires `final_regression_gate_<timestamp>.json` to exist and have `status = PASS`.
- A present final regression evidence row in the release approval packet is not sufficient by itself; the delivery gate reads and checks the JSON status.

Delivery requirement:
- Run the final regression gate as part of the current evidence suite.
- Verify the generated final regression JSON.
- Delivery remains `NOT_READY` if the final regression gate is missing or not PASS.

## 2026-06-21 Evidence bundle delivery gate update

The delivery readiness audit now checks the P0 evidence bundle directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run P0 evidence bundle JSON to the delivery readiness audit.
- The audit requires `p0_evidence_bundle_<timestamp>.json` to exist and have `status = PASS`.
- A present bundle path in the release approval packet is not sufficient by itself; the final delivery gate reads and checks the bundle JSON status.

Delivery requirement:
- Generate the P0 evidence bundle from current-run evidence paths.
- Verify the generated bundle JSON.
- Delivery remains `NOT_READY` if the bundle JSON is missing or not PASS.

## 2026-06-21 Approval packet status delivery gate update

The delivery readiness audit now checks the release approval packet status directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`

Behavior:
- The audit still checks `Missing evidence count: 0`.
- The audit now also requires `Packet status: READY_FOR_REVIEW`.
- An `INCOMPLETE` packet is an explicit delivery blocker even if other approval fields are present.

Delivery requirement:
- Regenerate the release approval packet after all required evidence artifacts are present.
- Delivery remains `NOT_READY` until the packet is ready for review and all later delivery gates pass.

## 2026-06-21 Explicit live endpoint evidence input update

The full P0 evidence suite now accepts an explicit live metrics endpoint evidence path.

Updated tooling:
- `scripts/run_p0_evidence_suite.ps1`
- `scripts/delivery_readiness_audit.ps1`

Behavior:
- `run_p0_evidence_suite.ps1` now supports `-P0DMetricsEndpointJson <path>`.
- The suite passes that path into `delivery_readiness_audit.ps1`.
- During full-suite execution, the delivery audit runs with `-DisableReportFallback`, so it will not silently pick historical latest reports.
- If `-P0DMetricsEndpointJson` is omitted, the live metrics endpoint gate remains a clear delivery blocker.

Recommended flow:

```powershell
./scripts/metrics_endpoint_probe.ps1 -MetricsUrl http://127.0.0.1:<profile-port>/metrics
./scripts/verify_metrics_endpoint_probe_report.ps1 -Path reports/metrics_endpoint_probe_<timestamp>.json
./scripts/run_p0_evidence_suite.ps1 -P0DMetricsEndpointJson reports/metrics_endpoint_probe_<timestamp>.json
```

Boundary:
- Standalone delivery audit still supports latest-report fallback for manual review convenience.
- Full-suite delivery review disables fallback to avoid accidentally reusing stale historical evidence.

## 2026-06-21 P0-E timed rollback evidence parameter update

The full-chain drill report can now carry timed rollback evidence from a real drill.

Updated tooling:
- `scripts/full_chain_drill_smoke.ps1`
- `scripts/verify_full_chain_drill_report.ps1`
- `scripts/delivery_readiness_audit.ps1`

New optional parameters:

```powershell
./scripts/full_chain_drill_smoke.ps1 `
  -P0BReport reports/crash_recovery_drill_<timestamp>.md `
  -P0CReport reports/concurrency_validation_<timestamp>.md `
  -P0DReport reports/observability_smoke_<timestamp>.md `
  -RollbackWindowSeconds 300 `
  -TimedRollbackStatus PASS `
  -RollbackDurationSeconds 120 `
  -RecoveryPoint "backup-or-lsn-reference" `
  -ReplayBoundary "last-replayed-log-or-txn-boundary"
```

Behavior:
- Without timed parameters, reports include `timed_rollback_evidence.status = NOT_PROVIDED`.
- With timed parameters, reports include rollback duration, rollback window, `within_window`, recovery point, and replay boundary.
- The full-chain verifier validates the optional structure.
- The delivery readiness audit requires timed rollback evidence to be `PASS`, within window, and include both recovery point and replay boundary.

Boundary:
- The script still does not execute a real release or rollback by itself.
- Operators must supply timed evidence from an actual drill for P0-E delivery acceptance.

## 2026-06-21 Full suite P0-E timed rollback parameter pass-through

The full P0 evidence suite now accepts P0-E timed rollback evidence parameters and passes them to `full_chain_drill_smoke.ps1`.

Updated tooling:
- `scripts/run_p0_evidence_suite.ps1`

New full-suite parameters:

```powershell
./scripts/run_p0_evidence_suite.ps1 `
  -P0DMetricsEndpointJson reports/metrics_endpoint_probe_<timestamp>.json `
  -TimedRollbackStatus PASS `
  -RollbackDurationSeconds 120 `
  -RecoveryPoint "backup-or-lsn-reference" `
  -ReplayBoundary "last-replayed-log-or-txn-boundary"
```

Behavior:
- If timed rollback parameters are omitted, the generated P0-E report remains smoke-only with `timed_rollback_evidence.status = NOT_PROVIDED`.
- If timed rollback parameters are supplied from a real drill, the P0-E report carries rollback duration, recovery point, replay boundary, and window status into the delivery audit.
- Delivery remains `NOT_READY` unless the P0-E timed rollback evidence is `PASS`, within window, and includes recovery point plus replay boundary.

## Accepted delivery deferrals

Delivery readiness now supports explicit accepted deferrals for residual P0 gaps. A failed audit check can only become `DEFERRED` when an accepted deferrals JSON file names the exact check and includes `owner`, `decision: ACCEPTED`, `expires_at`, and `rationale`. Deferred checks remain visible in the audit output and are not counted as `PASS`; they represent time-boxed owner acceptance, not technical completion.

Template: `docs/planning/P0_ACCEPTED_DEFERRALS_TEMPLATE.json`
Suite parameter: `-AcceptedDeferralsJson <path>`


## P0-B state diff artifact path

`crash_recovery_drill.ps1` now accepts external state-diff artifacts with `-RowStateDiffJson`, `-PageStateDiffJson`, and `-WalReplayDiffJson`. Row/page recovery checks stay `NOT_VERIFIED` when no artifact is supplied, become `FAIL` when an artifact is missing or malformed, and become `PASS` only when the command replay passes and the supplied artifact has `status: PASS`.

Template: `docs/planning/P0_STATE_DIFF_ARTIFACT_TEMPLATE.json`
Suite parameters: `-P0BRowStateDiffJson <path> -P0BPageStateDiffJson <path> -P0BWalReplayDiffJson <path>`


## Generating concrete P0-B state diff artifacts

Use `scripts/generate_p0b_state_diff_artifact.ps1` to convert expected/actual JSON snapshots into a delivery-grade state diff artifact. The generator performs semantic JSON comparison with stable key ordering and path-level mismatch counts.

Example row diff:

```powershell
./scripts/generate_p0b_state_diff_artifact.ps1 `
  -Scope row_state_diff `
  -ExpectedSnapshotJson reports/expected_rows.json `
  -ActualSnapshotJson reports/recovered_rows.json `
  -Scenario redo_undo_crash_recovery
```

Example verifier:

```powershell
./scripts/verify_p0b_state_diff_artifact.ps1 -Path reports/p0b_state_diff_row_state_diff_<timestamp>.json
```

Feed passing artifacts into the recovery drill:

```powershell
./scripts/crash_recovery_drill.ps1 `
  -Runs 3 `
  -StateEvidence `
  -RowStateDiffJson reports/p0b_state_diff_row_state_diff_<timestamp>.json `
  -PageStateDiffJson reports/p0b_state_diff_page_state_diff_<timestamp>.json `
  -WalReplayDiffJson reports/p0b_state_diff_wal_replay_diff_<timestamp>.json
```

P0-B delivery acceptance still requires the generated recovery state evidence to show row/page checks as `PASS`; missing artifacts remain `NOT_VERIFIED`, and malformed or failing artifacts remain `FAIL`.


## Focused P0-B snapshot evidence generator

`server/innodb/manager/p0b_state_snapshot_export_test.go` provides an environment-controlled snapshot exporter for focused recovery evidence. It only writes snapshots when `P0B_STATE_SNAPSHOT_DIR` is set. The wrapper script runs that exporter, builds row/page/WAL diff artifacts, and verifies each artifact:

```powershell
./scripts/generate_p0b_state_snapshot_evidence.ps1 -ReportDir reports
```

The generated report includes:

- `row_state_diff_json`
- `page_state_diff_json`
- `wal_replay_diff_json`

Use those paths as inputs to the crash recovery drill:

```powershell
./scripts/crash_recovery_drill.ps1 `
  -Runs 3 `
  -StateEvidence `
  -RowStateDiffJson <row_state_diff_json> `
  -PageStateDiffJson <page_state_diff_json> `
  -WalReplayDiffJson <wal_replay_diff_json>
```

Scope boundary: this is focused mock-buffer recovery evidence for deterministic redo replay and idempotent LSN behavior. It improves the P0-B evidence chain, but it does not replace a full disk-format crash/restart drill.


## Focused P0-C consistency evidence

`server/innodb/manager/p0c_consistency_evidence_export_test.go` provides an environment-controlled focused consistency exporter. It writes evidence only when `P0C_CONSISTENCY_EVIDENCE_DIR` is set. The wrapper script runs that exporter and validates the resulting evidence:

```powershell
./scripts/generate_p0c_consistency_evidence.ps1 -ReportDir reports
```

The generated evidence covers the delivery-gated P0-C consistency gaps:

- `lock_wait_metrics_summary`
- `full_isolation_matrix`
- `long_transaction_runtime_metrics`
- `explicit_final_state_diff`
- `wrapper_state_snapshot_diff`
- `deadlock_victim_report`

Feed the evidence into concurrency validation:

```powershell
./scripts/concurrency_validation.ps1 -P0CConsistencyEvidenceJson reports/p0c_consistency_evidence_<timestamp>.json
```

The full P0 evidence suite can generate and pass this evidence automatically:

```powershell
./scripts/run_p0_evidence_suite.ps1 -GenerateP0CConsistencyEvidence
```

Scope boundary: this is focused in-process consistency evidence. It improves the P0-C gate from `NOT_VERIFIED` gaps to explicit evidence-backed checks, but it does not replace a full external multi-client SQL workload or history-linearizability checker.


## Focused P0-E timed rollback evidence

`generate_p0e_timed_rollback_evidence.ps1` creates a validated timed rollback evidence contract with rollback duration, rollback window, recovery point, replay boundary, and step-level evidence:

```powershell
./scripts/generate_p0e_timed_rollback_evidence.ps1 -ReportDir reports -RollbackWindowSeconds 300
./scripts/verify_p0e_timed_rollback_evidence.ps1 -Path reports/p0e_timed_rollback_evidence_<timestamp>.json
```

The full P0 evidence suite can generate and pass this evidence automatically:

```powershell
./scripts/run_p0_evidence_suite.ps1 -GenerateP0ETimedRollbackEvidence
```

When a timed rollback evidence JSON is supplied or generated, the suite maps it into the full-chain drill fields: `TimedRollbackStatus`, `RollbackDurationSeconds`, `RecoveryPoint`, and `ReplayBoundary`.

Scope boundary: this is focused rollback evidence for the delivery contract. It proves the rollback evidence fields and timing window are present and internally consistent, but it does not execute an external deployment platform rollback, production traffic shift, or real data restore by itself.


## Focused P0-D live metrics endpoint evidence

`cmd/p0_metrics_endpoint` exposes the P0 metric catalog over HTTP for focused endpoint probing. The wrapper script starts the endpoint, waits for `/healthz`, runs the existing `/metrics` probe, verifies the probe report, and stops the endpoint:

```powershell
./scripts/generate_p0d_metrics_endpoint_evidence.ps1 -ReportDir reports
```

The delivery audit consumes the generated `metrics_endpoint_probe_<timestamp>.json` as `-P0DMetricsEndpointJson`.

The full P0 evidence suite can generate this endpoint probe automatically:

```powershell
./scripts/run_p0_evidence_suite.ps1 -GenerateP0DMetricsEndpointEvidence
```

Scope boundary: this focused endpoint proves `/metrics` reachability and required P0 metric-name presence for a local metrics endpoint. It does not prove the full XMySQL server process updates every metric under production traffic.


## P0 delivery candidate runner

`run_p0_delivery_candidate.ps1` is the single-command delivery-candidate entry point. It invokes the full P0 evidence suite with focused evidence generation enabled for P0-B, P0-C, P0-D, and P0-E by default:

```powershell
./scripts/run_p0_delivery_candidate.ps1 -ReportDir reports
```

Default focused evidence enabled by this runner:

- P0-B focused state snapshot evidence
- P0-C focused consistency evidence
- P0-D focused metrics endpoint evidence
- P0-E focused timed rollback evidence

Optional switches can disable individual focused evidence generators when external production-grade artifacts are supplied instead:

```powershell
./scripts/run_p0_delivery_candidate.ps1 `
  -DisableFocusedP0BStateSnapshotEvidence `
  -DisableFocusedP0CConsistencyEvidence `
  -DisableFocusedP0DMetricsEndpointEvidence `
  -DisableFocusedP0ETimedRollbackEvidence
```

Scope boundary: this runner does not relax any delivery readiness gates. It only reduces operator error by collecting the focused evidence defaults into one command. A candidate is deliverable only if the underlying suite, verifiers, approval packet, evidence bundle, and delivery readiness audit all pass.

## Delivery packet evidence closure update

The latest P0 delivery candidate wiring promotes the focused P0-B/C/D/E evidence artifacts into the formal review chain.

- P0-B semantic state diff artifacts now flow into the evidence bundle and release approval packet.
- P0-C focused consistency evidence now flows into the evidence bundle and release approval packet.
- P0-D live metrics endpoint probe evidence now flows into the evidence bundle, release approval packet, and delivery readiness audit.
- P0-E timed rollback evidence now flows into the full-chain drill, evidence bundle, and release approval packet.

This means the candidate packet can be reviewed from one artifact index instead of requiring reviewers to discover focused evidence from side outputs.

## Owner sign-off governance update

Final owner approval now has a machine-readable input contract:

- Template: `docs/planning/P0_OWNER_SIGNOFF_TEMPLATE.json`
- Verifier: `scripts/verify_p0_owner_signoff.ps1`
- Candidate parameter: `-OwnerSignoffJson <path>`

Behavior:

- Approval packets still default to `HOLD`.
- A valid owner sign-off JSON with `decision = APPROVE` is required before the generator renders owner approval checklist items as checked.
- The sign-off file is external approval evidence, not a suite-generated artifact.
- Delivery readiness still depends on evidence gates, risk/deferral review, and owner approval together.

Current interpretation: the project has a stronger approval mechanism, but it is not approved for delivery until real owners provide valid sign-off and the delivery candidate run verifies successfully.

## Accepted deferrals governance update

Accepted delivery deferrals now have an independent verifier:

- Template: `docs/planning/P0_ACCEPTED_DEFERRALS_TEMPLATE.json`
- Verifier: `scripts/verify_p0_accepted_deferrals.ps1`
- Candidate parameter: `-AcceptedDeferralsJson <path>`

Behavior:

- Deferrals must name exact delivery audit checks.
- Deferrals must be `ACCEPTED`, owner-filled, justified, unique, and unexpired.
- The full suite validates accepted deferrals before running the delivery readiness audit.
- The delivery readiness audit also validates accepted deferrals when run standalone.
- Deferred checks remain visible as residual risk and do not become `PASS`.

## Risk register verifier update

Risk governance now has an independent strict verifier:

- Verifier: `scripts/verify_p0_risk_register.ps1`
- Source: `docs/planning/P0_RISK_REGISTER.md`

Default behavior fails on `Open` risks and `TBD` owners. This is intentional for final delivery review. The current register still contains open/TBD risks, so strict risk validation is expected to fail until owners assign, close, mitigate, or explicitly defer each risk.
When owner sign-off JSON is supplied, strict risk-register validation is part of the approval path; owner approval cannot bypass unresolved risk ownership.
Candidate report verification re-runs sign-off validation and strict risk-register validation for `PASS` candidates that reference owner sign-off JSON. `FAIL` candidates remain valid handoff artifacts when they include failure context and generated artifact paths, even if the failed input was a missing external governance file.
Candidate reports now record the governance review mode explicitly, distinguishing risk-register-only, deferral-review, and final-approval candidate runs.
When governance inputs are supplied to the candidate runner, the candidate report also links the generated governance gate Markdown and JSON reports.
When governance validation fails before producing reports, the candidate report records the attempted governance status and remains useful as a failed-run handoff artifact.
Candidate reports now include `recommended_next_action`, making failed-run handoff and successful approval review routing explicit.
Successful candidates that reference governance inputs must now link passing governance gate JSON and Markdown reports.
All candidates that reference governance inputs must record `governance_gate_attempted = true`.
Candidate raw logs now record the same `run_id` as the candidate JSON and Markdown, and the candidate verifier checks that Markdown includes status, raw log path, governance mode, governance gate status, and recommended next action.

## Governance gate update

Approval governance now has a combined gate:

- Verifier: `scripts/verify_p0_governance_gate.ps1`

The gate combines risk-register validation, accepted deferrals validation, and owner sign-off validation. CI manual workflow mode `governance` can run this gate without executing the full P0 technical evidence suite.

Current interpretation: this improves reviewability, but the current risk register still needs real owners and closed/accepted/deferred statuses before strict governance validation can pass.

Governance gate JSON reports now have an independent verifier: `scripts/verify_p0_governance_gate_report.ps1`.
That verifier now resolves linked Markdown and governance input paths from both the repository root and the governance JSON directory, improving archived packet review.
It validates JSON fields and `checks[]` shape before Markdown cross-checks, keeping malformed governance packets easier to diagnose.
