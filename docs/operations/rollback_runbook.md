# Fast Rollback Runbook

## Purpose

This runbook defines how to quickly return XMySQL to the previously accepted package or configuration.

## Rollback triggers

Initiate rollback when any of these occur:

- Critical recovery failure.
- High error rate.
- High P99 latency.
- Data consistency failure.
- Lock wait or deadlock spike.
- Long transaction alert breach.
- Manual release owner or business owner stop signal.

## Required inputs

- Current package version.
- Previous package version.
- Current configuration backup.
- Previous configuration backup.
- Release start timestamp.
- Rollback start timestamp.
- Rollback owner.

## Rollback steps

1. Announce rollback decision.
2. Record rollback start timestamp.
3. Stop traffic expansion.
4. Route new traffic away from the affected scope if supported.
5. Restore previous package.
6. Restore previous configuration.
7. Restart affected process or node if required.
8. Run rollback smoke checks.
9. Confirm error rate and latency return to acceptable range.
10. Confirm active transactions and lock waits are stable.
11. Record rollback finish timestamp.
12. Archive rollback evidence.

## Rollback success criteria

Rollback succeeds when:

- Previous package is active.
- Previous configuration is active.
- Service accepts basic requests.
- Error rate is within threshold.
- P99 latency is within threshold.
- No new recovery failure appears.
- Rollback duration is within the accepted window.

## Rollback timing

Record:

- Decision time
- Rollback start time
- Package restored time
- Configuration restored time
- Smoke check finished time
- Rollback finish time
- Total rollback duration

## Post-rollback

After rollback:

- Freeze further rollout.
- Preserve logs and reports.
- Open follow-up items for root cause.
- Update P0 checklist with evidence path.

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

