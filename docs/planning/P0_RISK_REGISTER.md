# P0 Risk Register

## Purpose

This register tracks production-readiness risks that remain after the current P0 evidence tooling work.

## Risks

| ID | Risk | Impact | Mitigation | Owner | Status |
|---|---|---|---|---|---|
| R-P0-001 | Command-level recovery replay may pass without proving recovered storage state | Recovery defects could escape validation | Add page/row/WAL-level state-diff checks to P0-B | TBD | Open |
| R-P0-002 | Concurrency validation may rely on existing unit tests without explicit final-state checks | Lost update or isolation anomalies may be missed | Add explicit consistency checker and anomaly counters | TBD | Open |
| R-P0-003 | Observability artifacts may remain documentation-only | Production operators may lack live metrics during incident | Implement live metrics export or production-equivalent textfile output | TBD | Open |
| R-P0-004 | Alert rules may not be exercised | Alert routing or thresholds may fail during incident | Run alert delivery drill and archive evidence | TBD | Open |
| R-P0-005 | Release/rollback runbooks may not match runtime reality | Rollback may exceed accepted window | Execute timed full-chain drill | TBD | Open |
| R-P0-006 | Final evidence suite may fail on CI even if local tooling exists | Approval packet may not be reproducible outside one machine | Run manual GitHub Actions `full` workflow and archive artifact | TBD | Open |
| R-P0-007 | Build-tagged demo and legacy conformance profiles are excluded from default baseline | Hidden regressions may remain in excluded profiles | Decide whether those profiles are release-blocking, then fix or formally defer | TBD | Open |

## Review cadence

Review this register after each P0 evidence suite run.

Close a risk only when:

- mitigation evidence is archived,
- the approval packet links the evidence,
- the owner accepts the residual risk.

## Risk register verifier

Use the risk register verifier before owner sign-off:

```powershell
./scripts/verify_p0_risk_register.ps1
```

Default behavior is delivery-strict:

- `TBD` owners fail.
- `Open` risks fail.
- duplicate risk IDs fail.
- unsupported statuses fail.
- `Accepted` or `Deferred` risks must reference acceptance, deferral, evidence, owner, or follow-up in mitigation text.

For inventory-only review, operators can temporarily allow open risks or TBD owners:

```powershell
./scripts/verify_p0_risk_register.ps1 -AllowOpenRisks -AllowTbdOwners
```

Do not use the relaxed mode for final production approval.

For combined governance validation, use:

```powershell
./scripts/verify_p0_governance_gate.ps1
```

When owner sign-off is part of the review, require it explicitly:

```powershell
./scripts/verify_p0_governance_gate.ps1 `
  -OwnerSignoffJson reports/p0_owner_signoff_<timestamp>.json `
  -AcceptedDeferralsJson reports/p0_accepted_deferrals_<timestamp>.json `
  -RequireOwnerSignoff
```

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

## Accepted delivery deferrals

Delivery readiness now supports explicit accepted deferrals for residual P0 gaps. A failed audit check can only become `DEFERRED` when an accepted deferrals JSON file names the exact check and includes `owner`, `decision: ACCEPTED`, `expires_at`, and `rationale`. Deferred checks remain visible in the audit output and are not counted as `PASS`; they represent time-boxed owner acceptance, not technical completion.

Template: `docs/planning/P0_ACCEPTED_DEFERRALS_TEMPLATE.json`
Verifier: `scripts/verify_p0_accepted_deferrals.ps1`
Suite parameter: `-AcceptedDeferralsJson <path>`

Deferral validation rules:

- each deferral must name the exact delivery audit check,
- `decision` must be `ACCEPTED`,
- `owner` must name a real owner or team,
- `expires_at` must be in the future,
- `rationale` must describe concrete residual risk, compensating controls, and follow-up.
- template placeholder text is rejected.

The delivery readiness audit runs this verifier whenever `-AcceptedDeferralsJson` is supplied, including standalone audit runs.

## Owner sign-off evidence

Final production approval now has a machine-readable sign-off input:

- Template: `docs/planning/P0_OWNER_SIGNOFF_TEMPLATE.json`
- Verifier: `scripts/verify_p0_owner_signoff.ps1`
- Suite/candidate parameter: `-OwnerSignoffJson <path>`

Governance boundary:

- Owner sign-off JSON must be supplied by real release owners after evidence review.
- The verifier checks structure and required roles; it does not decide business acceptability.
- The approval packet remains `HOLD` unless a valid owner sign-off JSON with `decision = APPROVE` is supplied.
- Open risks and deferred risks must still be reviewed by owners before sign-off.
- Template placeholders such as `OWNER_NAME`, `BRANCH_OR_COMMIT`, and `RELEASE_DECISION_OWNER` are rejected by the sign-off verifier.

