# P0 Next Actions

## Immediate next actions

1. Run tooling preflight.

```powershell
./scripts/verify_p0_tooling_preflight.ps1
```

2. Run the first complete evidence suite.

```powershell
./scripts/run_p0_evidence_suite.ps1 -RecoveryRuns 3 -ConcurrencyRuns 1
```

3. If the suite fails, fix the first failing workstream only.

4. Re-run the failed workstream verifier before re-running the full suite.

5. Update the approval packet only after generated report paths exist.

## First implementation targets after suite failure

Prioritize in this order:

1. P0-B storage-state recovery proof.
2. P0-C explicit consistency checks.
3. P0-D live metrics export evidence.
4. P0-E timed rollback/data recovery drill evidence.
5. Final regression gate.
6. Evidence bundle and approval packet.

## Do not do

- Do not mark P0-B accepted from command-level replay alone.
- Do not mark P0-C accepted from unit-test execution alone.
- Do not mark P0-D accepted from documentation-only samples alone.
- Do not mark P0-E accepted without timed drill evidence.
- Do not mark the project complete until the approval packet is filled and signed.

## Current best command

The next command to run when validation is allowed is:

```powershell
./scripts/run_p0_delivery_candidate.ps1 -ReportDir reports
```

If owners have reviewed the generated evidence and are ready to approve, provide a machine-readable owner sign-off file based on `docs/planning/P0_OWNER_SIGNOFF_TEMPLATE.json`:

```powershell
./scripts/verify_p0_owner_signoff.ps1 -Path reports/p0_owner_signoff_<timestamp>.json
./scripts/run_p0_delivery_candidate.ps1 `
  -ReportDir reports `
  -OwnerSignoffJson reports/p0_owner_signoff_<timestamp>.json
```

Do not create or pass owner sign-off JSON until real owners have reviewed the evidence bundle, delivery readiness audit, risk register, remediation packet, rollback evidence, and release impact.
The verifier rejects placeholder values such as `BRANCH_OR_COMMIT`, `RELEASE_DECISION_OWNER`, and `OWNER_NAME`.

Before owner sign-off, run the risk register verifier in strict mode:

```powershell
./scripts/verify_p0_risk_register.ps1
```

For a combined governance review, run:

```powershell
./scripts/verify_p0_governance_gate.ps1
```

For final approval materials:

```powershell
./scripts/verify_p0_governance_gate.ps1 `
  -OwnerSignoffJson reports/p0_owner_signoff_<timestamp>.json `
  -AcceptedDeferralsJson reports/p0_accepted_deferrals_<timestamp>.json `
  -RequireOwnerSignoff
```

If owners explicitly accept a residual delivery gap, create an accepted deferrals file from `docs/planning/P0_ACCEPTED_DEFERRALS_TEMPLATE.json`, replace all placeholders, and verify it before use:

```powershell
./scripts/verify_p0_accepted_deferrals.ps1 -Path reports/p0_accepted_deferrals_<timestamp>.json
./scripts/run_p0_delivery_candidate.ps1 `
  -ReportDir reports `
  -AcceptedDeferralsJson reports/p0_accepted_deferrals_<timestamp>.json
```

Use accepted deferrals sparingly. They make residual risk visible and time-boxed; they do not turn deferred checks into `PASS`.
The verifier rejects placeholder owners and template rationale text.

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
