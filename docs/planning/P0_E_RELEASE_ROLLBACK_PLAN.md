# P0-E Gray Release, Rollback, and Recovery Plan

## Purpose

This plan defines the P0-E operational evidence required before the project can claim production gray-release readiness.

P0-E is not a code-only milestone. It requires runbooks, rollback paths, data recovery boundaries, timed drill records, and approval evidence.

## Current status

Status: open.

Completed by this plan:

- Gray release runbook path is defined.
- Fast rollback runbook path is defined.
- Data recovery runbook path is defined.
- Full drill record template path is defined.

Still required:

- Execute one full release-to-rollback drill.
- Archive timed drill report under `reports/`.
- Prove rollback can complete inside the accepted window.
- Prove data recovery point and replay boundary are understood.
- Link P0-B/P0-C/P0-D evidence before release approval.

## Required runbooks

| Runbook | Path | Purpose |
|---|---|---|
| Gray release | `docs/operations/gray_release_runbook.md` | Defines staged rollout, gates, monitoring, and approval |
| Fast rollback | `docs/operations/rollback_runbook.md` | Defines rollback triggers, steps, timing, and validation |
| Data recovery | `docs/operations/data_recovery_runbook.md` | Defines recovery point, replay boundary, backup and validation rules |
| Drill record | `docs/operations/full_chain_drill_record_template.md` | Captures timed evidence from a full release/rollback drill |

## P0-E acceptance rules

P0-E can only be marked accepted when all of the following are true:

- Gray release runbook exists.
- Fast rollback runbook exists.
- Data recovery runbook exists.
- Full-chain drill template exists.
- One completed drill record is archived under `reports/`.
- Drill record includes start time, end time, rollback time, owner, decision log, and validation evidence.
- Rollback completes within the accepted window.
- Recovery point and replay boundary are documented.
- P0-B recovery evidence is linked.
- P0-C concurrency evidence is linked.
- P0-D observability evidence is linked.
- Final default regression gate passes before production approval.

## Release gates

Do not start gray release unless all gates pass:

- Default engineering baseline is green.
- P0-B recovery evidence exists.
- P0-C concurrency evidence exists.
- P0-D observability evidence exists.
- Rollback owner is assigned.
- Data recovery owner is assigned.
- On-call window is active.
- Release package and previous package are both available.
- Configuration backup is available.

## Rollback triggers

Rollback must be initiated if any of these occur:

- Critical recovery error.
- Error rate exceeds agreed P0 threshold.
- P99 latency exceeds agreed P0 threshold for the configured window.
- Data consistency check fails.
- Lock wait or deadlock behavior exceeds accepted boundary.
- Long transaction alert remains active beyond threshold.
- Manual business owner stop signal.

## Evidence artifacts

Expected future evidence under `reports/`:

```text
reports/full_chain_drill_<timestamp>.md
reports/full_chain_drill_<timestamp>.json
```

The JSON report should include:

```json
{
  "generated_at": "2026-06-14T00:00:00Z",
  "run_id": "full_chain_drill_<timestamp>",
  "status": "PASS",
  "evidence_type": "release_rollback_drill",
  "timing": {
    "release_started_at": "2026-06-14T00:00:00Z",
    "rollback_started_at": "2026-06-14T00:10:00Z",
    "rollback_finished_at": "2026-06-14T00:14:00Z",
    "rollback_duration_seconds": 240
  },
  "linked_evidence": {
    "p0b_recovery": "reports/crash_recovery_drill_<timestamp>.md",
    "p0c_concurrency": "reports/concurrency_validation_<timestamp>.md",
    "p0d_observability": "reports/observability_smoke_<timestamp>.md"
  }
}
```

## Current judgment

P0-E is planned but not accepted.

The runbooks created with this plan are operational prerequisites. They do not replace an executed drill.

---

## 2026-06-14 full-chain drill smoke automation update

P0-E drill-readiness smoke automation has been added.

Scripts:

- `scripts/full_chain_drill_smoke.ps1`
- `scripts/verify_full_chain_drill_report.ps1`

Example smoke command:

```powershell
./scripts/full_chain_drill_smoke.ps1 `
  -P0BReport reports/crash_recovery_drill_20260614_070446.md `
  -P0CReport reports/concurrency_validation_<timestamp>.md `
  -P0DReport reports/observability_smoke_<timestamp>.md
```

Verifier command:

```powershell
./scripts/verify_full_chain_drill_report.ps1 -Path reports/full_chain_drill_<timestamp>.json
```

The smoke report verifies runbook presence and linked evidence paths. It does not replace a timed full release, rollback, and data recovery drill.

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

