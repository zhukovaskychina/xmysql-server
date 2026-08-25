# P0 Production Evidence Bundle

## Purpose

This document defines the evidence bundle required before XMySQL can be considered ready for production gray-release approval.

The bundle does not replace engineering validation. It collects and links the required evidence from P0-A through P0-E so reviewers can audit readiness without chasing scattered files.

## Bundle status

Current status: incomplete.

The project has baseline recovery and several scripts/runbooks in place, but not all required reports have been generated and verified.

## Required evidence

| Workstream | Required evidence | Current known status |
|---|---|---|
| P0-A engineering baseline | Default `go test ./...` result and baseline notes | Previously restored, latest all-package evidence should be refreshed before approval |
| P0-B crash recovery | 3-run recovery drill report, raw log, state evidence JSON, row/page/WAL diff artifacts, verifier output | Tooling exists; current-run candidate evidence must be generated |
| P0-C concurrency | Multi-run concurrency validation report, focused consistency evidence, JSON verifier output | Tooling exists; current-run candidate evidence must be generated |
| P0-D observability | Smoke report, metric catalog, sample logs, alert rules, live metrics endpoint probe, verifier output | Tooling exists; current-run candidate evidence must be generated |
| P0-E release/rollback | Full-chain drill smoke report, timed rollback evidence, recovery point, replay boundary | Tooling exists; current-run candidate evidence must be generated |
| Owner sign-off | Machine-readable owner approval JSON when owners approve release | Optional until real owners approve; required for final delivery approval |
| Accepted deferrals | Machine-readable owner-accepted residual risk deferrals | Optional; must be verified and unexpired when used |
| Risk register | Risk register plus strict verifier output before approval | Required for final delivery approval; current open/TBD risks must be resolved |
| Governance gate | Combined risk, deferral, and sign-off validation | Required for final approval review when owner sign-off is supplied |
| Delivery candidate | Candidate report, raw log, suite summary, delivery audit, remediation packet | Preferred single-command local delivery review path |

## Known evidence paths

Current known P0-B evidence:

- `reports/crash_recovery_drill_20260614_070446.log`
- `reports/crash_recovery_drill_20260614_070446.md`

Evidence still expected:

- `reports/crash_recovery_drill_<timestamp>.state.json`
- `reports/p0b_state_snapshot_evidence_<timestamp>.row_state_diff.json`
- `reports/p0b_state_snapshot_evidence_<timestamp>.page_state_diff.json`
- `reports/p0b_state_snapshot_evidence_<timestamp>.wal_replay_diff.json`
- `reports/concurrency_validation_<timestamp>.log`
- `reports/concurrency_validation_<timestamp>.md`
- `reports/concurrency_validation_<timestamp>.json`
- `reports/p0c_consistency_evidence_<timestamp>.json`
- `reports/observability_smoke_<timestamp>.log`
- `reports/observability_smoke_<timestamp>.md`
- `reports/observability_smoke_<timestamp>.json`
- `reports/metrics_export_<timestamp>.prom`
- `reports/metrics_export_<timestamp>.md`
- `reports/metrics_export_<timestamp>.json`
- `reports/metrics_endpoint_probe_<timestamp>.json`
- `reports/full_chain_drill_<timestamp>.log`
- `reports/full_chain_drill_<timestamp>.md`
- `reports/full_chain_drill_<timestamp>.json`
- `reports/p0e_timed_rollback_evidence_<timestamp>.json`
- `reports/p0_delivery_candidate_<timestamp>.json`
- `reports/p0_delivery_candidate_<timestamp>.md`
- `reports/p0_delivery_candidate_<timestamp>.log`
- owner-provided signoff JSON based on `docs/planning/P0_OWNER_SIGNOFF_TEMPLATE.json`
- owner-provided accepted deferrals JSON based on `docs/planning/P0_ACCEPTED_DEFERRALS_TEMPLATE.json`
- `docs/planning/P0_RISK_REGISTER.md`

## Bundle verification

Use the bundle verifier to produce an auditable summary:

```powershell
./scripts/verify_p0_evidence_bundle.ps1 `
  -P0BReport reports/crash_recovery_drill_20260614_070446.md `
  -P0BLog reports/crash_recovery_drill_20260614_070446.log `
  -P0BState reports/crash_recovery_drill_<timestamp>.state.json `
  -P0BRowStateDiffJson reports/p0b_state_snapshot_evidence_<timestamp>.row_state_diff.json `
  -P0BPageStateDiffJson reports/p0b_state_snapshot_evidence_<timestamp>.page_state_diff.json `
  -P0BWalReplayDiffJson reports/p0b_state_snapshot_evidence_<timestamp>.wal_replay_diff.json `
  -P0CReport reports/concurrency_validation_<timestamp>.md `
  -P0CJson reports/concurrency_validation_<timestamp>.json `
  -P0CConsistencyEvidenceJson reports/p0c_consistency_evidence_<timestamp>.json `
  -P0DReport reports/observability_smoke_<timestamp>.md `
  -P0DJson reports/observability_smoke_<timestamp>.json `
  -P0DMetricsReport reports/metrics_export_<timestamp>.md `
  -P0DMetricsJson reports/metrics_export_<timestamp>.json `
  -P0DMetricsEndpointJson reports/metrics_endpoint_probe_<timestamp>.json `
  -P0EReport reports/full_chain_drill_<timestamp>.md `
  -P0EJson reports/full_chain_drill_<timestamp>.json `
  -P0ETimedRollbackEvidenceJson reports/p0e_timed_rollback_evidence_<timestamp>.json
```

Expected output:

```text
reports/p0_evidence_bundle_<timestamp>.json
reports/p0_evidence_bundle_<timestamp>.md
```

## Approval boundary

Do not approve production gray release until:

- the evidence bundle verifier passes,
- all linked JSON reports pass their own schema verifiers,
- the final default regression gate is freshly executed,
- owners sign off in the production checklist.

---

## 2026-06-14 evidence suite automation update

A top-level P0 evidence suite runner has been added:

```powershell
./scripts/run_p0_evidence_suite.ps1 -RecoveryRuns 3 -ConcurrencyRuns 1
```

It orchestrates P0-B recovery, P0-D observability, P0-C concurrency, P0-E drill-readiness, and final bundle verification.

Use this runner when generating a complete local P0 evidence package.

---

## 2026-06-14 final regression gate update

The P0 evidence bundle now includes final regression evidence.

Additional required evidence:

- `reports/final_regression_gate_<timestamp>.md`
- `reports/final_regression_gate_<timestamp>.json`

Generate it directly with:

```powershell
./scripts/final_regression_gate.ps1
```

Or as part of the full suite:

```powershell
./scripts/run_p0_evidence_suite.ps1 -RecoveryRuns 3 -ConcurrencyRuns 1
```

The bundle verifier now requires final regression report paths before it can pass.

---

## 2026-06-14 GitHub Actions evidence workflow update

A manual CI entrypoint now exists for the evidence bundle:

- `.github/workflows/p0-evidence.yml`

The workflow supports `preflight` and `full` modes and uploads generated `reports/**` artifacts.

The workflow also supports `candidate` mode, which runs the preferred delivery-candidate entry point:

```powershell
./scripts/run_p0_delivery_candidate.ps1
```

Use `candidate` mode for delivery review because it enables focused P0-B/C/D/E evidence generation by default and validates the candidate report.

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
