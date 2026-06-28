# P0 Evidence Reports

This directory stores auditable reports generated during P0 production-readiness work.

Current report producers:

- `scripts/crash_recovery_drill.ps1`
- `scripts/crash_recovery_drill.sh`

Expected crash-recovery outputs:

- `crash_recovery_drill_<timestamp>.log`
- `crash_recovery_drill_<timestamp>.md`

Repeated replay evidence can be generated with:

- PowerShell: `./scripts/crash_recovery_drill.ps1 -Runs 3`
- Bash: `CR_RUNS=3 ./scripts/crash_recovery_drill.sh`

Command-level state evidence can be generated with:

- PowerShell: `./scripts/crash_recovery_drill.ps1 -Runs 3 -StateEvidence`
- Bash: `CR_RUNS=3 CR_STATE_EVIDENCE=1 ./scripts/crash_recovery_drill.sh`

The `.state.json` file records replay status and scenario checks. It is useful evidence, but it is not yet page-level, row-level, or WAL-level state-diff proof.

These reports are evidence artifacts, not production approval by themselves. P0-B still requires storage-state snapshot or state-diff evidence, scenario-matrix coverage, and final regression validation before it can be marked fully accepted.

---

## P0-C concurrency validation reports

Concurrency validation artifacts can be generated with:

- PowerShell: `./scripts/concurrency_validation.ps1 -Runs 3`
- Bash: `CONCURRENCY_RUNS=3 ./scripts/concurrency_validation.sh`

Expected outputs:

- `concurrency_validation_<timestamp>.log`
- `concurrency_validation_<timestamp>.md`
- `concurrency_validation_<timestamp>.json`

Validate the JSON report with:

```powershell
./scripts/verify_concurrency_validation_report.ps1 -Path reports/concurrency_validation_<timestamp>.json
```

These reports are command-level P0-C evidence. They do not replace explicit consistency checks, row-level anomaly detection, or storage-state diff evidence.

---

## P0-D observability smoke reports

Observability smoke artifacts can be generated with:

```powershell
./scripts/observability_smoke.ps1
```

Expected outputs:

- `observability_smoke_<timestamp>.log`
- `observability_smoke_<timestamp>.md`
- `observability_smoke_<timestamp>.json`

Validate the JSON report with:

```powershell
./scripts/verify_observability_report.ps1 -Path reports/observability_smoke_<timestamp>.json
```

These reports validate documentation/configuration artifacts. They do not replace live metrics export or alert delivery evidence.

---

## P0-E full-chain release/rollback drill reports

P0-E runbooks now exist:

- `docs/planning/P0_E_RELEASE_ROLLBACK_PLAN.md`
- `docs/operations/gray_release_runbook.md`
- `docs/operations/rollback_runbook.md`
- `docs/operations/data_recovery_runbook.md`
- `docs/operations/full_chain_drill_record_template.md`

Expected future drill outputs:

- `full_chain_drill_<timestamp>.md`
- `full_chain_drill_<timestamp>.json`

These reports must link P0-B, P0-C, and P0-D evidence before P0-E can be accepted.

---

## P0-E full-chain drill smoke reports

P0-E drill-readiness smoke artifacts can be generated with:

```powershell
./scripts/full_chain_drill_smoke.ps1 `
  -P0BReport reports/crash_recovery_drill_20260614_070446.md `
  -P0CReport reports/concurrency_validation_<timestamp>.md `
  -P0DReport reports/observability_smoke_<timestamp>.md
```

Expected outputs:

- `full_chain_drill_<timestamp>.log`
- `full_chain_drill_<timestamp>.md`
- `full_chain_drill_<timestamp>.json`

Validate the JSON report with:

```powershell
./scripts/verify_full_chain_drill_report.ps1 -Path reports/full_chain_drill_<timestamp>.json
```

These reports validate drill readiness and linked evidence paths. They do not replace an executed timed release, rollback, and data recovery drill.

---

## P0 evidence bundle reports

A top-level P0 evidence bundle can be generated with:

```powershell
./scripts/verify_p0_evidence_bundle.ps1 `
  -P0BReport reports/crash_recovery_drill_20260614_070446.md `
  -P0BLog reports/crash_recovery_drill_20260614_070446.log `
  -P0BState reports/crash_recovery_drill_<timestamp>.state.json `
  -P0CReport reports/concurrency_validation_<timestamp>.md `
  -P0CJson reports/concurrency_validation_<timestamp>.json `
  -P0DReport reports/observability_smoke_<timestamp>.md `
  -P0DJson reports/observability_smoke_<timestamp>.json `
  -P0EReport reports/full_chain_drill_<timestamp>.md `
  -P0EJson reports/full_chain_drill_<timestamp>.json
```

Expected outputs:

- `p0_evidence_bundle_<timestamp>.md`
- `p0_evidence_bundle_<timestamp>.json`

The bundle verifier checks evidence presence only. It does not replace dedicated report verifiers, live drills, or final regression validation.

---

## P0 evidence suite runner

A top-level local evidence suite can be run with:

```powershell
./scripts/run_p0_evidence_suite.ps1 -RecoveryRuns 3 -ConcurrencyRuns 1
```

The suite generates and verifies P0-B, P0-C, P0-D, P0-E, and bundle reports.

This command executes tests and smoke scripts. It should be run intentionally as a validation action.

---

## Final regression gate reports

Final default regression evidence can be generated with:

```powershell
./scripts/final_regression_gate.ps1
```

Expected outputs:

- `final_regression_gate_<timestamp>.log`
- `final_regression_gate_<timestamp>.md`
- `final_regression_gate_<timestamp>.json`

Validate the JSON report with:

```powershell
./scripts/verify_final_regression_gate_report.ps1 -Path reports/final_regression_gate_<timestamp>.json
```

The full P0 evidence suite runs this gate automatically before producing the evidence bundle.

---

## P0 handoff and approval documents

Top-level handoff documents:

- `docs/planning/P0_CURRENT_STATUS_SUMMARY.md`
- `docs/planning/P0_RELEASE_APPROVAL_PACKET_TEMPLATE.md`

The approval packet should be filled after the P0 evidence suite generates concrete report paths.

---

## P0 release approval packet reports

Generate a filled release approval packet with:

```powershell
./scripts/generate_p0_release_approval_packet.ps1 <report path arguments>
```

The full P0 evidence suite generates this packet automatically.

Expected output:

- `p0_release_approval_packet_<timestamp>.md`

The generated packet defaults to `HOLD` until evidence is reviewed and owners sign off.

---

## P0 release approval packet verifier

Validate a generated approval packet with:

```powershell
./scripts/verify_p0_release_approval_packet.ps1 -Path reports/p0_release_approval_packet_<timestamp>.md
```

The full P0 evidence suite runs this verifier automatically.

---

## P0 tooling preflight reports

Check P0 tooling, documentation, and delivery review checklist structure with:

```powershell
./scripts/verify_p0_tooling_preflight.ps1
```

Expected outputs:

- `p0_tooling_preflight_<timestamp>.md`
- `p0_tooling_preflight_<timestamp>.json`

The runner validates the delivery checklist structure with `scripts/verify_p0_delivery_review_checklist.ps1`, then validates the generated JSON and Markdown with `scripts/verify_p0_tooling_preflight_report.ps1`. The report verifier accepts both passing and failing reports while requiring consistent summary counts, limitations, Markdown title, run id, status, listed check paths, check status/actual pairs, and Markdown limitation text.

Recheck an archived preflight packet with:

```powershell
./scripts/verify_p0_tooling_preflight_report.ps1 `
  -Path reports/p0_tooling_preflight_<timestamp>.json `
  -MarkdownPath reports/p0_tooling_preflight_<timestamp>.md
```

This preflight does not execute tests or drills. It checks whether expected files exist and whether the final delivery checklist keeps the required review structure.

---

## P0 delivery review order

Review generated P0 packets in this order:

1. Recheck the tooling preflight report with `scripts/verify_p0_tooling_preflight_report.ps1`.
2. Review the delivery candidate JSON with `scripts/verify_p0_delivery_candidate.ps1`.
3. If governance inputs are present, recheck the governance gate JSON and Markdown with `scripts/verify_p0_governance_gate_report.ps1`.
4. Confirm the candidate links the evidence suite summary, delivery readiness audit, remediation packet, raw log, and any required governance reports.
5. Treat delivery as approved only after the candidate is `PASS`, required governance is `PASS`, owner sign-off is valid when required, and the release approval packet is no longer on `HOLD`.

Use `docs/planning/P0_DELIVERY_REVIEW_CHECKLIST.md` as the final human review checklist after the machine verifiers pass. Recheck the checklist structure with `scripts/verify_p0_delivery_review_checklist.ps1`.

---

## GitHub Actions P0 evidence workflow

A manual workflow is available at:

- `.github/workflows/p0-evidence.yml`

Modes:

- `preflight`: runs P0 tooling preflight only.
- `full`: runs the full P0 evidence suite.
- `candidate`: runs the delivery candidate wrapper with focused P0-B/C/D/E evidence enabled by default.
- `governance`: runs the governance gate without executing the full technical evidence suite.

The workflow uploads generated `reports/**` as an artifact.

---

## P0 evidence suite operator guide

Operator guide:

- `docs/operations/p0_evidence_suite_operator_guide.md`

Use this guide for local preflight, local full suite runs, manual GitHub Actions runs, artifact review, and failure handling.

---

## P0 remaining work and risks

Track remaining engineering work and risks in:

- `docs/planning/P0_REMAINING_ENGINEERING_BACKLOG.md`
- `docs/planning/P0_RISK_REGISTER.md`

Review these before interpreting generated reports as production approval evidence.

---

## P0 preflight coverage update

The tooling preflight now checks the remaining engineering backlog, risk register, and evidence suite operator guide.

---

## P0 remaining engineering execution plan

Execution plan and next actions:

- `docs/planning/P0_REMAINING_ENGINEERING_EXECUTION_PLAN.md`
- `docs/planning/P0_NEXT_ACTIONS.md`

Use these after an evidence suite run to select the next real engineering fix.

---

## Metrics registry usage

Usage guide:

- `docs/observability/metrics_registry_usage.md`

Code foundation:

- `server/observability/metrics/registry.go`
- `server/observability/metrics/defaults.go`

This registry can render Prometheus text exposition, but runtime integration and live export evidence are still required for P0-D acceptance.

---

## Metrics export smoke reports

Generate Prometheus textfile evidence with:

```powershell
./scripts/metrics_export_smoke.ps1
```

Expected outputs:

- `metrics_export_<timestamp>.prom`
- `metrics_export_<timestamp>.log`
- `metrics_export_<timestamp>.md`
- `metrics_export_<timestamp>.json`

Validate with:

```powershell
./scripts/verify_metrics_export_report.ps1 -Path reports/metrics_export_<timestamp>.json
```

This smoke proves textfile generation from the metrics registry, not live runtime integration.

---

## Metrics export evidence in final bundle

The final evidence bundle and approval packet now require metrics export smoke reports:

- `metrics_export_<timestamp>.md`
- `metrics_export_<timestamp>.json`

The Prometheus textfile artifact is also generated as:

- `metrics_export_<timestamp>.prom`

---

## Metrics HTTP handler usage

Usage guide:

- `docs/observability/metrics_http_handler_usage.md`

Code foundation:

- `server/observability/metrics/http.go`

This is the HTTP export foundation for future live metrics endpoint integration.

---

## Metrics runtime recorder usage

Usage guide:

- `docs/observability/metrics_runtime_recorder_usage.md`

Code foundation:

- `server/observability/metrics/runtime.go`

This adapter provides stable methods for runtime paths to update P0-D metrics.

---

## Structured logging smoke reports

Generate structured slow-query and error-log evidence with:

```powershell
./scripts/structured_logging_smoke.ps1
```

Expected outputs:

- `structured_logging_<timestamp>.slow.log`
- `structured_logging_<timestamp>.error.log`
- `structured_logging_<timestamp>.raw.log`
- `structured_logging_<timestamp>.md`
- `structured_logging_<timestamp>.json`

Validate with:

```powershell
./scripts/verify_structured_logging_report.ps1 -Path reports/structured_logging_<timestamp>.json
```

This smoke proves structured JSON-line generation, not live runtime query/error integration.

---

## Alert drill smoke reports

Generate alert drill smoke evidence with:

```powershell
./scripts/alert_drill_smoke.ps1
```

Expected outputs:

- `alert_drill_<timestamp>.log`
- `alert_drill_<timestamp>.md`
- `alert_drill_<timestamp>.json`

Validate with:

```powershell
./scripts/verify_alert_drill_report.ps1 -Path reports/alert_drill_<timestamp>.json
```

This smoke proves local rule presence and sample trigger logic, not live Alertmanager delivery.

---

## Live metrics endpoint integration

The server now registers `/metrics` on the existing profiling HTTP listener.

Updated code:

- `server/observability/metrics/global.go`
- `server/net/mysql_server.go`

The endpoint uses Prometheus text exposition via `server/observability/metrics.Handler`.

Runtime metric updates and endpoint scrape evidence are still required for P0-D acceptance.

---

## Live metrics endpoint probe reports

After starting the server, probe the live metrics endpoint with:

```powershell
./scripts/metrics_endpoint_probe.ps1 -MetricsUrl http://127.0.0.1:<profile-port>/metrics
```

Expected outputs:

- `metrics_endpoint_probe_<timestamp>.prom`
- `metrics_endpoint_probe_<timestamp>.log`
- `metrics_endpoint_probe_<timestamp>.md`
- `metrics_endpoint_probe_<timestamp>.json`

Validate with:

```powershell
./scripts/verify_metrics_endpoint_probe_report.ps1 -Path reports/metrics_endpoint_probe_<timestamp>.json
```

## 2026-06-15 P0-D active connection metric evidence status

Runtime wiring added:
- /metrics endpoint registration on the profiling listener.
- xmysql_connections_active{listener="mysql"} update on MySQL session open/close/error/COM_QUIT paths.

Evidence not yet generated after this change:
- Metrics endpoint probe report.
- Full P0 evidence suite report.

## 2026-06-15 Evidence suite run - PASS

The P0 evidence suite was executed after the latest observability updates.

Result:
- Overall P0 evidence suite: PASS
- Final default regression gate: PASS
- P0 release approval packet: READY_FOR_REVIEW
- Missing evidence count reported by approval packet: 0

Generated evidence:
- P0-B crash recovery report: eports/crash_recovery_drill_20260615_001527.md
- P0-B state evidence: eports/crash_recovery_drill_20260615_001527.state.json
- P0-C concurrency report: eports/concurrency_validation_20260615_001537.md
- P0-D observability smoke: eports/observability_smoke_20260615_001536.md
- P0-D metrics export: eports/metrics_export_20260615_001536.md
- P0-D structured logging: eports/structured_logging_20260615_001536.md
- P0-D alert drill: eports/alert_drill_20260615_001537.md
- P0-E full-chain drill: eports/full_chain_drill_20260615_001550.md
- Final regression gate: eports/final_regression_gate_20260615_001551.md
- Evidence bundle: eports/p0_evidence_bundle_20260615_001559.md
- Release approval packet: eports/p0_release_approval_packet_20260615_001559.md

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


## Suite shortcut for focused P0-B state snapshot evidence

The full P0 evidence suite can generate focused P0-B snapshot evidence before the crash recovery drill:

```powershell
./scripts/run_p0_evidence_suite.ps1 -GenerateP0BStateSnapshotEvidence
```

When this switch is used, the suite runs `generate_p0b_state_snapshot_evidence.ps1`, locates the generated row/page/WAL diff JSON artifacts from the current run, and passes them into `crash_recovery_drill.ps1` automatically.


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

## P0 focused evidence packet rows

New P0 delivery candidate runs include focused evidence in the formal packet chain:

- `p0b_state_snapshot_evidence_*.row_state_diff.json`
- `p0b_state_snapshot_evidence_*.page_state_diff.json`
- `p0b_state_snapshot_evidence_*.wal_replay_diff.json`
- `p0c_consistency_evidence_*.json`
- `metrics_endpoint_probe_*.json`
- `p0e_timed_rollback_evidence_*.json`

The evidence bundle reports these as focused checks, and the release approval packet lists them with dedicated verifier commands.

## P0 delivery candidate reports

`run_p0_delivery_candidate.ps1` writes:

- `p0_delivery_candidate_<timestamp>.md`
- `p0_delivery_candidate_<timestamp>.json`
- `p0_delivery_candidate_<timestamp>.log`

The JSON report is validated by:

```powershell
./scripts/verify_p0_delivery_candidate.ps1 -Path reports/p0_delivery_candidate_<timestamp>.json
```

`PASS` means the candidate suite and delivery readiness audit completed successfully. `FAIL` can still be useful because the report links raw logs, suite summary, delivery readiness, and remediation outputs when available.
For `PASS` candidate JSON, `verify_p0_delivery_candidate.ps1` revalidates referenced accepted deferrals, owner sign-off, governance gate reports, and strict risk register. For `FAIL` candidate JSON, it checks generated artifact paths and failure context so failed runs remain useful as handoff artifacts, even when a bad external governance path caused the failure.
Candidate JSON includes `governance_review_mode` and `governance_gate_required` to make the governance scope explicit in the report.
When governance inputs are supplied, candidate JSON also links `p0_governance_gate_<timestamp>.json` and `.md`.
If governance validation fails before those reports are produced, candidate JSON records `governance_gate_attempted` and `governance_gate_status` so the failure can still be handed off with the raw log.
Candidate JSON also includes `recommended_next_action` to make the next reviewer action explicit.
For `PASS` candidate reports, any accepted-deferrals or owner-signoff input requires linked governance gate JSON and Markdown reports with `governance_gate_status = PASS`.
Any candidate report that references accepted deferrals or owner sign-off must also record that the governance gate was attempted.
The candidate verifier checks that the raw log has the expected candidate header, records the report `run_id`, and matches the report `started_at` value.
Candidate JSON includes `candidate_markdown`; the verifier checks the Markdown title, run id, status, raw log path, governance status, governance mode, and recommended next action.
The candidate verifier resolves linked artifact paths against the repository root and the candidate JSON directory before reading them, which makes copied or archived report packets easier to recheck.

## Owner sign-off reports

Owner sign-off JSON should be created from:

- `docs/planning/P0_OWNER_SIGNOFF_TEMPLATE.json`

Validate it with:

```powershell
./scripts/verify_p0_owner_signoff.ps1 -Path reports/p0_owner_signoff_<timestamp>.json
```

Then pass it into the candidate runner with `-OwnerSignoffJson`. The file is treated as external approval evidence and is linked from the candidate/approval packet; it is not treated as a suite-generated artifact.

## Accepted deferrals reports

Accepted deferrals should be created from:

- `docs/planning/P0_ACCEPTED_DEFERRALS_TEMPLATE.json`

Validate them with:

```powershell
./scripts/verify_p0_accepted_deferrals.ps1 -Path reports/p0_accepted_deferrals_<timestamp>.json
```

Then pass the file into the candidate runner with `-AcceptedDeferralsJson`. Deferrals are external owner-risk evidence and must be unexpired when used.

## Risk register validation

Validate the release risk register with:

```powershell
./scripts/verify_p0_risk_register.ps1
```

Strict validation is intended for final approval review and fails on open risks or `TBD` owners. Use relaxed switches only for inventory review, not production approval.

## Governance gate reports

Validate governance materials together with:

```powershell
./scripts/verify_p0_governance_gate.ps1
```

For final approval material review:

```powershell
./scripts/verify_p0_governance_gate.ps1 `
  -OwnerSignoffJson reports/p0_owner_signoff_<timestamp>.json `
  -AcceptedDeferralsJson reports/p0_accepted_deferrals_<timestamp>.json `
  -RequireOwnerSignoff
```

Successful governance gate runs write:

- `p0_governance_gate_<timestamp>.md`
- `p0_governance_gate_<timestamp>.json`

The governance gate report verifier resolves linked Markdown and input paths against both the repository root and the governance JSON directory, so archived governance packets can be rechecked without relying on the original shell working directory.
It validates the JSON fields and `checks[]` shape required for Markdown cross-checks before reading the Markdown report, which keeps malformed governance packets easier to diagnose.

Verify the JSON report with:

```powershell
./scripts/verify_p0_governance_gate_report.ps1 `
  -Path reports/p0_governance_gate_<timestamp>.json `
  -MarkdownPath reports/p0_governance_gate_<timestamp>.md
```

`verify_p0_governance_gate.ps1` also runs this report verifier automatically before reporting success.
