# Gray Release Runbook

## Purpose

This runbook defines the staged release process for XMySQL P0 production-readiness validation.

## Required owners

Before release starts, assign:

- Release owner
- Rollback owner
- Data recovery owner
- Observability owner
- Business approval owner

## Pre-release gates

Do not proceed unless all are true:

- Default engineering baseline has passed.
- P0-B recovery report is linked.
- P0-C concurrency report is linked.
- P0-D observability report is linked.
- Rollback runbook is available.
- Data recovery runbook is available.
- Previous release artifact is available.
- Configuration backup is available.
- Monitoring window is active.

## Release steps

1. Announce release start and owners.
2. Record release start timestamp.
3. Confirm current package version and target package version.
4. Backup current configuration.
5. Deploy to the smallest gray-release scope.
6. Watch P0-D metrics and logs for the configured observation window.
7. Run post-deploy smoke checks.
8. Decide one of: continue, hold, rollback.
9. Record decision and evidence links.

## Expansion steps

Only expand if the previous stage is stable.

Recommended stages:

1. Internal or single-node scope.
2. Small traffic percentage.
3. Medium traffic percentage.
4. Full production scope.

Each stage must record:

- Started at
- Finished at
- Scope
- Error rate
- P99 latency
- Active transaction count
- Lock wait status
- Recovery/consistency status
- Decision

## Stop conditions

Stop expansion and evaluate rollback when:

- Any rollback trigger fires.
- Observability smoke evidence is missing.
- Business owner requests hold.
- Data consistency check fails.

## Completion

Gray release is complete only when:

- All stages have passed.
- No active rollback trigger remains.
- Final validation evidence is archived.
- Owners sign off.
