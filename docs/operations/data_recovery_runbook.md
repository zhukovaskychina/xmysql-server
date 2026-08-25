# Data Recovery Runbook

## Purpose

This runbook defines the data recovery boundaries required for P0 production-readiness.

## Recovery principles

- Prefer verified recovery points over guesswork.
- Never overwrite evidence before recovery analysis is complete.
- Record every recovery decision and timestamp.
- Validate recovered state before resuming rollout.

## Required inputs

- Last known good backup or snapshot.
- WAL/redo log boundary.
- Undo/rollback boundary.
- Recovery target time.
- Affected database or tablespace.
- Expected consistency checks.
- Recovery owner.

## Recovery steps

1. Stop release expansion.
2. Preserve current data files and logs.
3. Identify the recovery point.
4. Identify replay boundary.
5. Restore from backup or snapshot if required.
6. Replay redo/WAL up to the selected boundary if required.
7. Apply undo or rollback logic if required.
8. Run consistency checks.
9. Compare expected and actual state.
10. Record recovery result and evidence.

## Validation checks

Minimum checks:

- Expected committed data is visible.
- Expected rolled-back data is absent.
- No duplicate committed records are introduced.
- Transaction status is consistent.
- Recovery logs contain no critical failure.
- Application-level smoke checks pass.

## Evidence to archive

Archive:

- Recovery start and finish timestamps.
- Recovery point.
- Replay boundary.
- Input backup or snapshot identifier.
- Recovery command or manual steps.
- Consistency check output.
- Final decision.

## P0-B relationship

This runbook depends on P0-B crash recovery evidence. P0-E cannot be accepted unless P0-B has at least one archived recovery drill report and the recovery point/replay boundary are documented for the release drill.

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

