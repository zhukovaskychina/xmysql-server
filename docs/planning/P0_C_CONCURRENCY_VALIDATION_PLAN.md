# P0-C Concurrency Correctness Validation Plan

## Purpose

This plan defines the evidence required before the project can claim P0-C concurrency correctness readiness.

The current repository has scattered concurrency tests and demos, but it does not yet have a formal stress runner, consistency checker, or archived report package. P0-C remains open until those artifacts exist and pass.

## Current status

Status: open.

Known evidence available:

- Default `go test ./...` baseline has previously passed in the current workspace.
- Some package-level concurrency tests exist in the repository.
- Historical demo command packages are excluded from the default test profile with the `demo` build tag.

Known missing evidence:

- Formal concurrency stress script.
- Repeatable parameter profile.
- Consistency checker.
- Archived stress report.
- Lock wait or deadlock behavior summary.
- Multi-run stability evidence.

## Acceptance target

P0-C can only be marked accepted when all of the following are true:

- A repeatable concurrency validation script exists.
- The script can run locally on Windows.
- The script has a Bash-compatible entrypoint or documented CI equivalent.
- The script archives raw log, Markdown summary, and machine-readable JSON.
- The validation covers conflict writes, range reads, long transactions, and lock behavior.
- The validation includes a consistency check that can fail independently from the test runner.
- A multi-run report is archived under `reports/`.
- Final default regression gate passes after concurrency-related code or test changes.

## Scenario matrix

| Scenario | Purpose | Minimum evidence |
|---|---|---|
| Conflict writes | Prove concurrent writes do not silently lose updates or duplicate committed state | conflicting writer count, operation count, final expected/actual result |
| Range reads | Prove range scans remain consistent under concurrent writes | read pattern, observed anomalies, expected isolation notes |
| Long transactions | Prove long-running transactions do not corrupt visibility or block indefinitely | transaction duration, active transaction count, completion status |
| Lock wait behavior | Prove lock waits are observable and bounded | wait count, max wait duration, timeout count |
| Deadlock handling | Prove deadlocks are detected or bounded | deadlock count, victim behavior, final consistency result |
| Repeated replay | Prove the scenario is not a one-off pass | at least 3 runs with per-run PASS/FAIL |

## Recommended report artifacts

Each P0-C validation run should generate:

```text
reports/concurrency_validation_<timestamp>.log
reports/concurrency_validation_<timestamp>.md
reports/concurrency_validation_<timestamp>.json
```

The JSON artifact should include:

```json
{
  "generated_at": "2026-06-14T00:00:00Z",
  "run_id": "concurrency_validation_<timestamp>",
  "status": "PASS",
  "evidence_type": "concurrency_validation",
  "profile": {
    "runs": 3,
    "workers": 8,
    "operations_per_worker": 100
  },
  "scenarios": [
    {
      "name": "conflict_writes",
      "status": "PASS",
      "expected": "no lost updates",
      "actual": "no lost updates observed",
      "metrics": {
        "operations": 800,
        "errors": 0
      }
    }
  ]
}
```

Minimum required fields:

- `generated_at`
- `run_id`
- `status`
- `evidence_type`
- `profile`
- `scenarios`
- per-scenario `name`
- per-scenario `status`
- per-scenario `expected`
- per-scenario `actual`
- per-scenario `metrics`

## Initial implementation strategy

Start with the smallest reliable validation loop:

1. Build a script wrapper that runs existing concurrency-related tests by pattern.
2. Archive `.log`, `.md`, and `.json` reports under `reports/`.
3. Add a JSON verifier similar to P0-B state evidence verification.
4. Expand from command-level evidence to explicit state consistency checks.
5. Add a multi-run mode for repeated replay stability.

## Recommended first script command

PowerShell entrypoint:

```powershell
./scripts/concurrency_validation.ps1 -Runs 3
```

Bash entrypoint:

```bash
CONCURRENCY_RUNS=3 ./scripts/concurrency_validation.sh
```

The first implementation can start as command-level evidence, but it must clearly label itself as such and must not be treated as final P0-C acceptance until explicit consistency checks are added.

## Final approval boundary

Current status after creating this plan: P0-C is planned but not implemented.

Do not mark P0-C accepted until:

- validation scripts exist,
- reports exist,
- JSON verifier exists,
- at least one multi-run report passes,
- consistency checks are present,
- final `go test ./...` passes after implementation.

---

## 2026-06-14 automation update

P0-C command-level concurrency validation automation has been added.

Scripts:

- `scripts/concurrency_validation.ps1`
- `scripts/concurrency_validation.sh`
- `scripts/verify_concurrency_validation_report.ps1`

PowerShell:

```powershell
./scripts/concurrency_validation.ps1 -Runs 3
./scripts/verify_concurrency_validation_report.ps1 -Path reports/concurrency_validation_<timestamp>.json
```

Bash:

```bash
CONCURRENCY_RUNS=3 ./scripts/concurrency_validation.sh
```

The first implementation records command-level evidence across lock behavior, MVCC isolation, long transactions, conflict/recovery concurrency, wrapper concurrency, and storage MVCC deadlock scenarios.

It does not yet replace explicit consistency checks or row-level anomaly detection.

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

