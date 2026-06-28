# P0 Release Approval Packet Template

## Release candidate

- Project:
- Branch or commit:
- Prepared by:
- Prepared at:
- Target environment:
- Proposed gray-release window:

## Evidence links

| Evidence | Path | Status |
|---|---|---|
| P0-B recovery raw log |  | PASS/FAIL/MISSING |
| P0-B recovery Markdown report |  | PASS/FAIL/MISSING |
| P0-B state evidence JSON |  | PASS/FAIL/MISSING |
| P0-B row state diff JSON |  | PASS/FAIL/MISSING |
| P0-B page state diff JSON |  | PASS/FAIL/MISSING |
| P0-B WAL replay diff JSON |  | PASS/FAIL/MISSING |
| P0-C concurrency Markdown report |  | PASS/FAIL/MISSING |
| P0-C concurrency JSON report |  | PASS/FAIL/MISSING |
| P0-C consistency evidence JSON |  | PASS/FAIL/MISSING |
| P0-D observability Markdown report |  | PASS/FAIL/MISSING |
| P0-D observability JSON report |  | PASS/FAIL/MISSING |
| P0-D metrics export Markdown report |  | PASS/FAIL/MISSING |
| P0-D metrics export JSON report |  | PASS/FAIL/MISSING |
| P0-D live metrics endpoint probe JSON |  | PASS/FAIL/MISSING |
| P0-D structured logging Markdown report |  | PASS/FAIL/MISSING |
| P0-D structured logging JSON report |  | PASS/FAIL/MISSING |
| P0-D alert drill Markdown report |  | PASS/FAIL/MISSING |
| P0-D alert drill JSON report |  | PASS/FAIL/MISSING |
| P0-E full-chain smoke Markdown report |  | PASS/FAIL/MISSING |
| P0-E full-chain smoke JSON report |  | PASS/FAIL/MISSING |
| P0-E timed rollback evidence JSON |  | PASS/FAIL/MISSING |
| Final regression Markdown report |  | PASS/FAIL/MISSING |
| Final regression JSON report |  | PASS/FAIL/MISSING |
| P0 evidence bundle Markdown report |  | PASS/FAIL/MISSING |
| P0 evidence bundle JSON report |  | PASS/FAIL/MISSING |
| P0 owner signoff JSON |  | PASS/FAIL/MISSING |
| P0 accepted deferrals JSON |  | PASS/FAIL/MISSING |
| P0 risk register | `docs/planning/P0_RISK_REGISTER.md` | PASS/FAIL/MISSING |

## Dedicated verifier results

| Verifier | Command | Result |
|---|---|---|
| P0-B state evidence verifier | `./scripts/verify_crash_recovery_state_evidence.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0-B row state diff verifier | `./scripts/verify_p0b_state_diff_artifact.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0-B page state diff verifier | `./scripts/verify_p0b_state_diff_artifact.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0-B WAL replay diff verifier | `./scripts/verify_p0b_state_diff_artifact.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0-C concurrency report verifier | `./scripts/verify_concurrency_validation_report.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0-C consistency evidence verifier | `./scripts/verify_p0c_consistency_evidence.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0-D observability report verifier | `./scripts/verify_observability_report.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0-D metrics export report verifier | `./scripts/verify_metrics_export_report.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0-D live metrics endpoint probe verifier | `./scripts/verify_metrics_endpoint_probe_report.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0-D structured logging report verifier | `./scripts/verify_structured_logging_report.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0-D alert drill report verifier | `./scripts/verify_alert_drill_report.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0-E full-chain report verifier | `./scripts/verify_full_chain_drill_report.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0-E timed rollback evidence verifier | `./scripts/verify_p0e_timed_rollback_evidence.ps1 -Path <path>` | PASS/FAIL/MISSING |
| Final regression verifier | `./scripts/verify_final_regression_gate_report.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0 evidence bundle verifier | `./scripts/verify_p0_evidence_bundle.ps1 <args>` | PASS/FAIL/MISSING |
| P0 delivery candidate verifier | `./scripts/verify_p0_delivery_candidate.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0 owner signoff verifier | `./scripts/verify_p0_owner_signoff.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0 accepted deferrals verifier | `./scripts/verify_p0_accepted_deferrals.ps1 -Path <path>` | PASS/FAIL/MISSING |
| P0 risk register verifier | `./scripts/verify_p0_risk_register.ps1` | PASS/FAIL/MISSING |
| P0 governance gate verifier | `./scripts/verify_p0_governance_gate.ps1 -OwnerSignoffJson <path> -AcceptedDeferralsJson <path>` | PASS/FAIL/MISSING |
| P0 governance gate report verifier | `./scripts/verify_p0_governance_gate_report.ps1 -Path <path>` | PASS/FAIL/MISSING |

## Remaining risks

- Risk:
- Impact:
- Mitigation:
- Owner:

## Approval checklist

- [ ] P0-B recovery evidence is linked and verified.
- [ ] P0-C concurrency evidence is linked and verified.
- [ ] P0-D observability evidence is linked and verified.
- [ ] P0-D metrics export evidence is linked and verified.
- [ ] P0-D structured logging evidence is linked and verified.
- [ ] P0-D alert drill evidence is linked and verified.
- [ ] P0-E full-chain drill readiness evidence is linked and verified.
- [ ] Final default regression evidence is linked and verified.
- [ ] Evidence bundle is linked and verified.
- [ ] Live metrics export evidence is available or explicitly deferred by owner.
- [ ] Timed rollback drill evidence is available or explicitly deferred by owner.
- [ ] Data recovery point and replay boundary are documented.
- [ ] Release owner approves.
- [ ] Rollback owner approves.
- [ ] Data recovery owner approves.
- [ ] Observability owner approves.
- [ ] Business owner approves.

## Final decision

- Decision: APPROVE / HOLD / REJECT
- Decision time:
- Decision owner:
- Notes:

---

## 2026-06-14 approval packet generator update

A generator now exists for filled release approval packets:

```powershell
./scripts/generate_p0_release_approval_packet.ps1 <report path arguments>
```

The full P0 evidence suite runs this generator automatically after bundle verification and writes:

```text
reports/p0_release_approval_packet_<timestamp>.md
```

The generated packet defaults to `HOLD` until owners review evidence and sign off.

---

## 2026-06-14 approval packet verifier update

The generated release approval packet can now be validated with:

```powershell
./scripts/verify_p0_release_approval_packet.ps1 -Path reports/p0_release_approval_packet_<timestamp>.md
```

The full P0 evidence suite runs this verifier automatically after generating the approval packet.

## 2026-06-20 Release packet delivery-boundary update

The release approval packet generator now includes an explicit delivery-readiness boundary.

Updated tooling:
- `scripts/generate_p0_release_approval_packet.ps1`
- `scripts/verify_p0_release_approval_packet.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior change:
- `READY_FOR_REVIEW` means required evidence artifacts are present for review.
- It does not mean the project is deliverable.
- The packet now includes `Delivery status: NOT_ASSESSED_BY_PACKET` and points reviewers to `delivery_readiness_audit.ps1` for the actual deliverability gate.
- The approval packet verifier now requires the delivery-readiness boundary section, audit command, and audit verifier command.

Delivery rule:
- Treat release approval packet validation as a review-material check.
- Treat delivery readiness audit `READY` as the delivery gate.

## 2026-06-20 Post-packet delivery gate row update

The release approval packet now renders the delivery readiness audit row as `POST_PACKET_GATE` when the audit JSON has not been generated yet.

Why:
- The approval packet is generated before `delivery_readiness_audit.ps1` runs.
- Showing the audit JSON as `MISSING` inside the packet made the evidence table conflict with `Missing evidence count: 0`.
- `POST_PACKET_GATE` makes the ordering explicit: the packet is review material, and the delivery audit is the follow-up gate.

Interpretation:
- `PRESENT` means an evidence artifact exists before packet generation.
- `POST_PACKET_GATE` means the artifact is expected to be generated after packet validation.
- `READY_FOR_REVIEW` still does not mean deliverable; delivery requires a later audit status of `READY`.
