# P0 Remaining Engineering Execution Plan

## Purpose

This plan turns the remaining P0 engineering backlog into an execution sequence.

It assumes the current repository already has the P0 evidence framework, runbooks, verifiers, CI workflow, and approval packet tooling in place. The remaining work is to generate real evidence, fix failures, and implement runtime capability gaps that cannot be solved by documentation alone.

## Phase 1: Generate the first complete evidence run

Goal: produce the first real suite output and identify the first failing workstream.

Steps:

1. Run local tooling preflight:

```powershell
./scripts/verify_p0_tooling_preflight.ps1
```

2. Run the local suite with conservative concurrency:

```powershell
./scripts/run_p0_evidence_suite.ps1 -RecoveryRuns 3 -ConcurrencyRuns 1
```

3. If the suite fails, stop at the first failing workstream.
4. Archive generated `reports/*`.
5. Update `docs/planning/P0_CURRENT_STATUS_SUMMARY.md` with the first failing workstream and report paths.

Exit criteria:

- Preflight report exists.
- At least one full-suite attempt exists.
- First failing workstream is known.

## Phase 2: Close P0-B recovery proof gaps

Goal: upgrade recovery evidence from command-level replay to storage-state proof.

Backlog items:

- `P0-B-STATE-01`
- `P0-B-HALF-01`

Steps:

1. Identify stable recovery test hooks that expose recovered state.
2. Add expected/actual state capture to the recovery drill.
3. Add state checks for committed data, rolled-back data, and recovery status.
4. Add explicit half-commit or interrupted-commit coverage.
5. Regenerate P0-B reports.
6. Verify state evidence JSON.

Exit criteria:

- P0-B `.state.json` includes real state checks.
- Half-commit or interrupted-commit evidence is archived.
- P0-B verifier passes.

## Phase 3: Close P0-C concurrency proof gaps

Goal: upgrade concurrency evidence from command-level test execution to explicit consistency validation.

Backlog items:

- `P0-C-CONSISTENCY-01`
- `P0-C-RANGE-01`
- `P0-C-LOCK-01`

Steps:

1. Add conflict-write final-state checker.
2. Add range-read anomaly counter.
3. Add lock wait/deadlock metric summary fields to the P0-C JSON report.
4. Run `scripts/concurrency_validation.ps1 -Runs 3`.
5. Verify P0-C JSON report.

Exit criteria:

- P0-C report includes expected vs actual consistency checks.
- Range anomaly count is present.
- Lock wait/deadlock summary is present.
- P0-C verifier passes.

## Phase 4: Close P0-D observability proof gaps

Goal: move observability from documentation/configuration smoke to runtime-consumable evidence.

Backlog items:

- `P0-D-EXPORT-01`
- `P0-D-SLOW-01`
- `P0-D-ALERT-01`

Steps:

1. Implement or document a production-consumable metrics export path.
2. Produce a sample export artifact under `reports/`.
3. Connect slow query log sample to runtime or accepted smoke path.
4. Run an alert delivery drill or document an owner-approved deferral.
5. Regenerate P0-D smoke report.
6. Verify P0-D JSON report.

Exit criteria:

- Metrics export evidence exists.
- Slow query evidence is runtime-generated or explicitly deferred.
- Alert drill evidence exists or is explicitly owner-deferred.
- P0-D verifier passes.

## Phase 5: Close P0-E release and rollback proof gaps

Goal: prove release, rollback, and data recovery processes are executable.

Backlog items:

- `P0-E-DRILL-01`
- `P0-E-RECOVERY-01`

Steps:

1. Fill a full-chain drill record from `docs/operations/full_chain_drill_record_template.md`.
2. Link P0-B, P0-C, and P0-D evidence.
3. Record rollback decision, start, finish, and total duration.
4. Record data recovery point and replay boundary.
5. Generate P0-E full-chain smoke report.
6. Verify P0-E JSON report.

Exit criteria:

- Timed rollback evidence is archived.
- Data recovery point and replay boundary are documented.
- P0-E verifier passes.

## Phase 6: Final approval package

Goal: produce one reviewable package for production gray-release decision.

Backlog items:

- `P0-FINAL-01`
- `P0-FINAL-02`

Steps:

1. Run final regression gate.
2. Verify final regression report.
3. Run evidence bundle verifier.
4. Generate release approval packet.
5. Verify release approval packet.
6. Route packet to owners for decision.

Exit criteria:

- Final regression report passes.
- Evidence bundle passes.
- Approval packet is generated and verified.
- Owner sign-off is recorded.

## Completion rule

The project is complete only when:

- every P0 backlog item is closed or explicitly owner-deferred,
- every required report is archived,
- every applicable verifier passes,
- final regression passes,
- release approval packet is complete,
- owners sign off.
