# P0-B Crash Recovery Drill Runbook

## Purpose

This runbook defines the P0-B crash-recovery evidence required before the project can claim production-readiness for recovery behavior.

The current project has executable crash-recovery drills and one successful 3-run replay report. P0-B is still not fully accepted until the drill also proves recovered state consistency through snapshot or state-diff evidence.

## Current evidence inventory

Generated on 2026-06-14:

- Raw log: `reports/crash_recovery_drill_20260614_070446.log`
- Markdown report: `reports/crash_recovery_drill_20260614_070446.md`
- Command: `./scripts/crash_recovery_drill.ps1 -Runs 3`
- Result: PASS
- Replay runs: 3

Current drill entrypoints:

- Windows PowerShell: `scripts/crash_recovery_drill.ps1`
- Bash-compatible environments: `scripts/crash_recovery_drill.sh`

## Current drill command

PowerShell:

```powershell
./scripts/crash_recovery_drill.ps1 -Runs 3
```

Bash:

```bash
CR_RUNS=3 ./scripts/crash_recovery_drill.sh
```

Default focused test pattern:

```text
TestTXN001|TestCrashRecovery|TestRedo|TestUndoRollback|TestSavepoint
```

## Scenario matrix

| Scenario | Current evidence | Acceptance status | Required next evidence |
|---|---|---|---|
| Transaction lifecycle baseline | Covered by `TestTXN001` in the 3-run drill | Partial | Include state snapshot before and after recovery |
| Crash recovery flow | Covered by `TestCrashRecovery*` in the 3-run drill | Partial | Include recovered-state diff output |
| Redo recovery | Covered by `TestRedo*` in the 3-run drill | Partial | Include expected redo-visible rows or page state |
| Undo rollback | Covered by `TestUndoRollback*` in the 3-run drill | Partial | Include expected rollback absence/presence checks |
| Savepoint behavior | Covered by `TestSavepoint*` in the 3-run drill | Partial | Include final transaction state snapshot |
| Half-commit or interrupted commit | Not separately proven by current report | Open | Add or identify focused coverage and archive report output |
| Repeated replay stability | 3-run report exists | Partial | Keep report and add state-diff evidence |

## Snapshot/state-diff evidence requirement

Each accepted crash-recovery drill should produce a machine-readable evidence file next to the raw log and Markdown report.

Recommended file name:

```text
reports/crash_recovery_drill_<timestamp>.state.json
```

Recommended JSON shape:

```json
{
  "generated_at": "2026-06-14T00:00:00Z",
  "run_id": "crash_recovery_drill_<timestamp>",
  "status": "PASS",
  "runs": [
    {
      "run": 1,
      "status": "PASS",
      "exit_code": 0,
      "checks": [
        {
          "name": "redo_recovery_visible_state",
          "expected": "committed records are visible after recovery",
          "actual": "committed records are visible after recovery",
          "status": "PASS"
        }
      ]
    }
  ]
}
```

Minimum accepted fields:

- `generated_at`
- `run_id`
- `status`
- `runs`
- per-run `status`
- per-run `exit_code`
- per-run `checks`
- per-check `name`
- per-check `expected`
- per-check `actual`
- per-check `status`

## Acceptance rules

P0-B can only be marked fully accepted when all of the following are true:

- Crash-recovery drill automation exists for the target development or CI environment.
- A repeated replay report exists with at least 3 runs.
- Raw log and Markdown report are archived under `reports/`.
- Snapshot or state-diff JSON is archived under `reports/`.
- Every replay run passes.
- Every state-diff check passes.
- The final default regression gate passes after recovery-related code or script changes.

## Current P0-B judgment

Current status: partial.

Completed:

- Local PowerShell drill entrypoint.
- Bash-compatible drill entrypoint.
- 3-run replay report.
- Raw log archive.
- Markdown report archive.

Still required:

- Snapshot or state-diff JSON artifact.
- Explicit half-commit or interrupted-commit evidence.
- Final default regression gate after any recovery-related implementation change.

## Recommended next implementation step

Add snapshot/state-diff support to the drill automation.

Recommended behavior:

- Add a `-StateEvidence` switch to `scripts/crash_recovery_drill.ps1`.
- Add a `CR_STATE_EVIDENCE=1` mode to `scripts/crash_recovery_drill.sh`.
- Emit `reports/crash_recovery_drill_<timestamp>.state.json`.
- Include the state file path in the Markdown report.
- Keep the existing default behavior unchanged when state evidence mode is not enabled.

---

## 2026-06-14 automation update

The drill scripts now support command-level state evidence output:

- PowerShell: `./scripts/crash_recovery_drill.ps1 -Runs 3 -StateEvidence`
- Bash: `CR_RUNS=3 CR_STATE_EVIDENCE=1 ./scripts/crash_recovery_drill.sh`

Generated artifact:

- `reports/crash_recovery_drill_<timestamp>.state.json`

The artifact records replay status and command-level scenario checks. It is intentionally labeled as `evidence_type: command_replay` and does not replace the still-required storage-state snapshot or state-diff evidence.

---

## 2026-06-14 state evidence verifier handoff

Command-level state evidence now has a schema verifier:

```powershell
./scripts/verify_crash_recovery_state_evidence.ps1 -Path reports/crash_recovery_drill_<timestamp>.state.json
```

The verifier intentionally requires `evidence_type: command_replay`, so command-level evidence cannot be mistaken for final storage-state snapshot/diff proof.

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

