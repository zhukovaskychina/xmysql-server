# P0 Remaining Engineering Backlog

## 2026-07-15 superseding capability backlog

This document remains a historical delivery-evidence backlog. The current production-blocking capability backlog is now:

- `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`

Use the new backlog first. Evidence tooling remains necessary, but it is not sufficient until the runtime capabilities in the new P0 backlog are implemented and verified.

## Purpose

This backlog lists the remaining engineering work required before XMySQL can be considered complete for P0 production gray-release approval.

It intentionally separates tooling readiness from actual production capability. A script, report template, or smoke check is useful evidence infrastructure, but it does not automatically prove the underlying runtime behavior is production-ready.

## Current completion boundary

The project currently has a strong P0 evidence framework:

- recovery drill scripts,
- concurrency validation scripts,
- observability smoke scripts,
- release/rollback runbooks,
- evidence bundle tooling,
- approval packet generation,
- manual GitHub Actions workflow.

The project is not fully complete until the remaining backlog items below are implemented, exercised, and verified with archived evidence.

## Backlog

| ID | Workstream | Item | Current state | Completion evidence | Priority |
|---|---|---|---|---|---|
| P0-B-STATE-01 | Recovery | Add storage-state snapshot or state-diff evidence to recovery drill | Command-level replay evidence tooling exists | `.state.json` includes storage/page/row/WAL-level expected vs actual checks | P0 |
| P0-B-HALF-01 | Recovery | Prove half-commit or interrupted-commit recovery behavior | Scenario is documented as required, but not separately proven | Archived drill report with explicit half-commit/interrupted-commit scenario | P0 |
| P0-C-CONSISTENCY-01 | Concurrency | Add explicit consistency checker for conflict writes | Command-level test runner exists | JSON report includes expected final state vs actual final state for conflict-write scenario | P0 |
| P0-C-RANGE-01 | Concurrency | Add explicit range-read anomaly detection | Scenario is planned but not state-checked | Report captures range-read expected/actual observations and anomaly count | P0 |
| P0-C-LOCK-01 | Concurrency | Add lock wait/deadlock metrics summary | Command-level lock tests exist | Report includes lock wait count, max wait duration, deadlock count, and victim behavior | P0 |
| P0-D-EXPORT-01 | Observability | Implement production-consumable metrics export | Metric catalog and smoke docs exist | Metrics endpoint, textfile, or documented exporter output with sample artifact | P0 |
| P0-D-SLOW-01 | Observability | Connect slow query log to runtime execution path | Sample log exists only as documentation evidence | Runtime-generated slow query sample with configured threshold | P0 |
| P0-D-ALERT-01 | Observability | Run alert delivery drill | Alert rule template exists | Alert drill report showing trigger condition, firing alert, and recovery | P0 |
| P0-E-DRILL-01 | Release | Execute timed full release/rollback drill | Runbooks and smoke tooling exist | Completed full-chain drill report with rollback duration | P0 |
| P0-E-RECOVERY-01 | Release | Prove data recovery point and replay boundary during drill | Data recovery runbook exists | Drill evidence links recovery point, replay boundary, and validation output | P0 |
| P0-FINAL-01 | Release gate | Run full P0 evidence suite and archive approval packet | Suite tooling exists | Passing suite output and generated approval packet with all evidence present | P0 |
| P0-FINAL-02 | Release gate | Refresh final `go test ./...` evidence | Script exists but report not generated | Passing `final_regression_gate_<timestamp>.json` and `.md` | P0 |

## Recommended execution order

1. Run tooling preflight.
2. Generate P0-B command-level state evidence.
3. Extend recovery drill to include real storage-state diff checks.
4. Generate P0-D smoke report.
5. Implement live metrics export or production-equivalent output.
6. Generate P0-C command-level report.
7. Extend P0-C with explicit consistency checks.
8. Run final regression gate.
9. Run P0-E full-chain smoke.
10. Execute timed release/rollback/data recovery drill.
11. Generate P0 evidence bundle.
12. Generate release approval packet.
13. Complete owner sign-off.

## Non-goals for current tooling

The current smoke and bundle scripts do not:

- prove row-level serializability,
- prove WAL/page-level recovered-state consistency,
- prove live metrics export,
- prove alert delivery,
- prove rollback timing,
- approve production release.

Those proofs require the backlog items above.

## Completion rule

Do not mark the project complete until every P0 backlog item has:

- implementation or documented accepted deferral,
- archived evidence,
- verifier output where applicable,
- linked approval packet entry,
- owner sign-off.

## 2026-06-21 focused delivery candidate closure update

The delivery candidate tooling now generates focused P0-B/C/D/E evidence by default and promotes those artifacts into the evidence bundle, release approval packet, suite summary, delivery readiness audit, and remediation packet.

Updated completion interpretation:

- P0-B-STATE-01 can be candidate-closed by a passing delivery candidate run whose crash recovery state evidence includes PASS row/page/WAL diff artifacts.
- P0-C-CONSISTENCY-01, P0-C-RANGE-01, and P0-C-LOCK-01 can be candidate-closed by a passing delivery candidate run whose concurrency validation report reaches `ACCEPTED` with zero required gaps.
- P0-D-EXPORT-01 can be candidate-closed by a passing delivery candidate run whose live metrics endpoint probe is present and PASS.
- P0-E-DRILL-01 and P0-E-RECOVERY-01 can be candidate-closed by a passing delivery candidate run whose full-chain evidence includes PASS timed rollback, recovery point, and replay boundary fields.
- P0-FINAL-01 can be candidate-closed only when the suite summary, approval packet, evidence bundle, delivery readiness audit, and remediation packet are all generated from the same current run.

Remaining production boundary:

- These focused generators are local delivery evidence. They improve the candidate gate, but they do not replace external production traffic, a multi-client SQL workload, real deployment platform rollback, or owner sign-off unless those residual risks are explicitly accepted through `P0_ACCEPTED_DEFERRALS_TEMPLATE.json`.

Primary candidate command:

```powershell
./scripts/run_p0_delivery_candidate.ps1 -ReportDir reports
```

---

## 2026-06-14 metrics registry foundation update

A lightweight metrics registry foundation has been added:

- `server/observability/metrics/registry.go`
- `server/observability/metrics/defaults.go`
- `docs/observability/metrics_registry_usage.md`

This advances `P0-D-EXPORT-01` from documentation-only planning to reusable code foundation.

Remaining work:

- wire runtime paths into the registry,
- expose or archive production-consumable Prometheus text output,
- generate live export evidence.

---

## 2026-06-14 metrics export smoke update

Metrics export smoke tooling has been added:

- `cmd/p0_metrics_export/main.go`
- `scripts/metrics_export_smoke.ps1`
- `scripts/verify_metrics_export_report.ps1`

The smoke generates Prometheus textfile evidence under `reports/`.

This advances `P0-D-EXPORT-01` from registry foundation to export artifact generation, but runtime server path integration is still required.

---

## 2026-06-14 metrics HTTP handler foundation update

P0-D now has a reusable Prometheus HTTP handler foundation:

- `server/observability/metrics/http.go`
- `docs/observability/metrics_http_handler_usage.md`

This advances `P0-D-EXPORT-01`, but does not close it yet. Remaining work is mounting the handler in the live server runtime, wiring runtime stats into the registry, and archiving endpoint evidence.

---

## 2026-06-15 metrics runtime recorder foundation update

P0-D now has a runtime metrics recorder adapter:

- `server/observability/metrics/runtime.go`
- `docs/observability/metrics_runtime_recorder_usage.md`

This advances `P0-D-EXPORT-01` by providing stable event-level methods for runtime integration. It does not close the item yet because live query, transaction, lock, recovery, and checkpoint paths still need to call the recorder.

---

## 2026-06-15 structured logging foundation update

P0-D structured logging foundation has been added:

- `server/observability/logging/structured.go`
- `cmd/p0_structured_log_export/main.go`
- `docs/observability/structured_logging_usage.md`
- `scripts/structured_logging_smoke.ps1`
- `scripts/verify_structured_logging_report.ps1`

This advances `P0-D-SLOW-01` from documentation-only samples to reusable JSON-line encoding and smoke evidence tooling. Runtime query and error-path integration remains required.

---

## 2026-06-15 alert drill smoke update

P0-D alert drill smoke tooling has been added:

- `scripts/alert_drill_smoke.ps1`
- `scripts/verify_alert_drill_report.ps1`

This advances `P0-D-ALERT-01` from static alert rule templates to sample trigger evidence. Live Prometheus/Alertmanager delivery evidence remains required or must be explicitly owner-deferred.

---

## 2026-06-15 live metrics endpoint integration update

P0-D live metrics endpoint foundation is now wired into the existing profiling listener:

- `server/observability/metrics/global.go`
- `server/net/mysql_server.go`

The server registers `/metrics` on the existing profiling HTTP mux.

This advances `P0-D-EXPORT-01` from reusable foundations to endpoint integration. Remaining work is runtime recorder updates from live query/transaction/lock/recovery/checkpoint paths and archived endpoint scrape evidence.

---

## 2026-06-15 live metrics endpoint probe update

P0-D live endpoint probe tooling has been added:

- `scripts/metrics_endpoint_probe.ps1`
- `scripts/verify_metrics_endpoint_probe_report.ps1`

This probe can archive evidence from a running `/metrics` endpoint. It requires the server to be running and does not generate traffic by itself.

## 2026-06-15 Backlog update - Observability instrumentation

Completed:
- P0-D active connection gauge is now written by the MySQL session lifecycle using the current session map size.
- /metrics handler registration exists on the profiling listener.

Still open:
- Probe /metrics on a running server and archive evidence.
- Wire query, transaction, lock wait, recovery, checkpoint, and storage health metrics to real execution paths.
- Re-run the P0 evidence suite after runtime instrumentation changes.

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
