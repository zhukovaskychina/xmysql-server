# P0 Delivery Review Checklist

Use this checklist after generating local or CI P0 evidence. This document is not approval by itself; it is the review order for deciding whether the generated packet is ready for owner approval.

## Required reports

- Tooling preflight JSON and Markdown are present.
- Tooling preflight report passes `scripts/verify_p0_tooling_preflight_report.ps1`.
- Delivery candidate JSON, Markdown, and raw log are present.
- Delivery candidate report passes `scripts/verify_p0_delivery_candidate.ps1`.
- Evidence suite summary is linked from the delivery candidate.
- Delivery readiness audit is linked from the delivery candidate.
- Delivery remediation packet is linked from the delivery candidate.

## Governance evidence

- Governance gate JSON and Markdown are linked when accepted deferrals or owner sign-off inputs are used.
- Governance gate report passes `scripts/verify_p0_governance_gate_report.ps1` when present.
- Accepted deferrals pass `scripts/verify_p0_accepted_deferrals.ps1` when present.
- Owner sign-off passes `scripts/verify_p0_owner_signoff.ps1` when required.
- Strict risk-register validation passes before final owner approval.

## Approval conditions

- Delivery candidate status is `PASS`.
- Required governance status is `PASS`.
- Release approval packet is present and no longer on `HOLD`.
- Any accepted deferrals are explicit, unexpired, owner-approved, and linked from the candidate packet.
- No unresolved P0 risk is missing an owner or final disposition.

## Non-approval conditions

- A passing tooling preflight alone does not approve delivery.
- A passing governance gate alone does not approve delivery.
- A failed delivery candidate is a handoff artifact, not an approval artifact.
- Static documentation or script hardening without a real candidate run is not approval evidence.
- Placeholder owner sign-off data is not approval evidence.

## Final reviewer note

Record the reviewed report paths in the release approval packet before declaring the project ready for delivery.

Checklist structure can be rechecked with `scripts/verify_p0_delivery_review_checklist.ps1`.
