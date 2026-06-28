param(
    [Parameter(Mandatory = $true)]
    [string]$Path
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Path)) {
    throw "P0 release approval packet does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Content = Get-Content -Raw -LiteralPath $ResolvedPath

function Assert-Contains {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Pattern,
        [Parameter(Mandatory = $true)]
        [string]$Description
    )

    if (-not $Content.Contains($Pattern)) {
        throw "Approval packet is missing required content: $Description"
    }
}

Assert-Contains -Pattern "# P0 Release Approval Packet" -Description "title"
Assert-Contains -Pattern "## Release candidate" -Description "release candidate section"
Assert-Contains -Pattern "## Evidence links" -Description "evidence links section"
Assert-Contains -Pattern "## Delivery readiness boundary" -Description "delivery readiness boundary section"
Assert-Contains -Pattern "## Dedicated verifier commands" -Description "dedicated verifier commands section"
Assert-Contains -Pattern "## Approval checklist" -Description "approval checklist section"
Assert-Contains -Pattern "## Final decision" -Description "final decision section"
Assert-Contains -Pattern "- Packet status:" -Description "packet status field"
Assert-Contains -Pattern "- Missing evidence count:" -Description "missing evidence count field"
Assert-Contains -Pattern "- Delivery status:" -Description "delivery status field"
Assert-Contains -Pattern "- Delivery readiness audit:" -Description "delivery readiness audit field"
Assert-Contains -Pattern "| Evidence | Path | Status |" -Description "evidence table header"
Assert-Contains -Pattern "P0-B recovery raw log" -Description "P0-B raw log evidence row"
Assert-Contains -Pattern "P0-B row state diff JSON" -Description "P0-B row state diff evidence row"
Assert-Contains -Pattern "P0-B page state diff JSON" -Description "P0-B page state diff evidence row"
Assert-Contains -Pattern "P0-B WAL replay diff JSON" -Description "P0-B WAL replay diff evidence row"
Assert-Contains -Pattern "P0-C concurrency JSON report" -Description "P0-C JSON evidence row"
Assert-Contains -Pattern "P0-C consistency evidence JSON" -Description "P0-C focused consistency evidence row"
Assert-Contains -Pattern "P0-D observability JSON report" -Description "P0-D JSON evidence row"
Assert-Contains -Pattern "P0-D metrics export JSON report" -Description "P0-D metrics export JSON evidence row"
Assert-Contains -Pattern "P0-D live metrics endpoint probe JSON" -Description "P0-D live metrics endpoint evidence row"
Assert-Contains -Pattern "P0-D structured logging JSON report" -Description "P0-D structured logging JSON evidence row"
Assert-Contains -Pattern "P0-D alert drill JSON report" -Description "P0-D alert drill JSON evidence row"
Assert-Contains -Pattern "P0-E full-chain smoke JSON report" -Description "P0-E JSON evidence row"
Assert-Contains -Pattern "P0-E timed rollback evidence JSON" -Description "P0-E timed rollback evidence row"
Assert-Contains -Pattern "Final regression JSON report" -Description "final regression JSON evidence row"
Assert-Contains -Pattern "P0 evidence bundle JSON report" -Description "bundle JSON evidence row"
Assert-Contains -Pattern "P0 owner signoff JSON" -Description "owner signoff evidence row"
Assert-Contains -Pattern "P0 accepted deferrals JSON" -Description "accepted deferrals evidence row"
Assert-Contains -Pattern "P0 risk register" -Description "risk register evidence row"
Assert-Contains -Pattern "P0 delivery readiness audit JSON report" -Description "delivery readiness audit JSON evidence row"
Assert-Contains -Pattern "verify_p0b_state_diff_artifact.ps1" -Description "P0-B focused state diff verifier command"
Assert-Contains -Pattern "verify_p0c_consistency_evidence.ps1" -Description "P0-C focused consistency verifier command"
Assert-Contains -Pattern "verify_metrics_endpoint_probe_report.ps1" -Description "P0-D live endpoint probe verifier command"
Assert-Contains -Pattern "verify_p0e_timed_rollback_evidence.ps1" -Description "P0-E timed rollback verifier command"
Assert-Contains -Pattern "verify_p0_owner_signoff.ps1" -Description "owner signoff verifier command"
Assert-Contains -Pattern "verify_p0_accepted_deferrals.ps1" -Description "accepted deferrals verifier command"
Assert-Contains -Pattern "verify_p0_risk_register.ps1" -Description "risk register verifier command"
Assert-Contains -Pattern "verify_p0_governance_gate.ps1" -Description "governance gate verifier command"
Assert-Contains -Pattern "verify_p0_governance_gate_report.ps1" -Description "governance gate report verifier command"
Assert-Contains -Pattern "delivery_readiness_audit.ps1" -Description "delivery readiness audit command"
Assert-Contains -Pattern "verify_delivery_readiness_audit.ps1" -Description "delivery readiness audit verifier command"
Assert-Contains -Pattern "does not mean the project is deliverable" -Description "review-vs-delivery boundary"
Assert-Contains -Pattern "- Decision:" -Description "final decision field"

Write-Host "P0 release approval packet validation passed: $ResolvedPath"
