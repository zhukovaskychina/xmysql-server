param(
    [string]$ReportDir = $env:P0_APPROVAL_REPORT_DIR,
    [string]$OutputPath = "",
    [string]$Project = "xmysql-server",
    [string]$BranchOrCommit = "",
    [string]$TargetEnvironment = "local-validation",
    [string]$GrayReleaseWindow = "",
    [string]$P0BLog = "",
    [string]$P0BReport = "",
    [string]$P0BState = "",
    [string]$P0BRowStateDiffJson = "",
    [string]$P0BPageStateDiffJson = "",
    [string]$P0BWalReplayDiffJson = "",
    [string]$P0CReport = "",
    [string]$P0CJson = "",
    [string]$P0CConsistencyEvidenceJson = "",
    [string]$P0DReport = "",
    [string]$P0DJson = "",
    [string]$P0DMetricsReport = "",
    [string]$P0DMetricsJson = "",
    [string]$P0DMetricsEndpointJson = "",
    [string]$P0DLoggingReport = "",
    [string]$P0DLoggingJson = "",
    [string]$P0DAlertReport = "",
    [string]$P0DAlertJson = "",
    [string]$P0EReport = "",
    [string]$P0EJson = "",
    [string]$P0ETimedRollbackEvidenceJson = "",
    [string]$FinalRegressionReport = "",
    [string]$FinalRegressionJson = "",
    [string]$BundleReport = "",
    [string]$BundleJson = "",
    [string]$DeliveryReadinessJson = "",
    [string]$OwnerSignoffJson = "",
    [string]$AcceptedDeferralsJson = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RepoRoot

if ([string]::IsNullOrWhiteSpace($ReportDir)) {
    $ReportDir = Join-Path $RepoRoot "reports"
}

New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$PreparedAt = Get-Date -Format "o"

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $ReportDir "p0_release_approval_packet_$Timestamp.md"
}

function Get-ArtifactStatus {
    param([string]$Path)

    if ([string]::IsNullOrWhiteSpace($Path)) {
        return "MISSING"
    }

    if (Test-Path -LiteralPath $Path) {
        return "PRESENT"
    }

    return "MISSING"
}

function Row {
    param(
        [string]$Evidence,
        [string]$Path
    )

    return "| $Evidence | ``$Path`` | $(Get-ArtifactStatus -Path $Path) |"
}

function PostPacketGateRow {
    param(
        [string]$Evidence,
        [string]$Path
    )

    if ([string]::IsNullOrWhiteSpace($Path)) {
        return "| $Evidence | generated after this packet | POST_PACKET_GATE |"
    }

    return "| $Evidence | ``$Path`` | $(Get-ArtifactStatus -Path $Path) |"
}

$OwnerSignoff = $null
$OwnerApprovalByRole = @{}
$HasApprovedOwnerSignoff = $false

if (-not [string]::IsNullOrWhiteSpace($OwnerSignoffJson)) {
    & (Join-Path $ScriptDir "verify_p0_owner_signoff.ps1") -Path $OwnerSignoffJson
    & (Join-Path $ScriptDir "verify_p0_risk_register.ps1")

    $OwnerSignoff = Get-Content -Raw -LiteralPath $OwnerSignoffJson | ConvertFrom-Json
    foreach ($Approval in @($OwnerSignoff.approvals)) {
        $OwnerApprovalByRole[[string]$Approval.role] = $Approval
    }

    $HasApprovedOwnerSignoff = $OwnerSignoff.decision -eq "APPROVE"
}

if (-not [string]::IsNullOrWhiteSpace($AcceptedDeferralsJson)) {
    & (Join-Path $ScriptDir "verify_p0_accepted_deferrals.ps1") -Path $AcceptedDeferralsJson
}

function Get-OwnerCheck {
    param([string]$Role)

    if ($script:HasApprovedOwnerSignoff -and $script:OwnerApprovalByRole.ContainsKey($Role) -and $script:OwnerApprovalByRole[$Role].approved -eq $true) {
        return "[x]"
    }

    return "[ ]"
}

$PacketDecision = if ($HasApprovedOwnerSignoff) { "APPROVE" } else { "HOLD" }
$PacketDecisionTime = if ($HasApprovedOwnerSignoff) { [string]$OwnerSignoff.decision_time } else { "" }
$PacketDecisionOwner = if ($HasApprovedOwnerSignoff) { [string]$OwnerSignoff.decision_owner } else { "" }
$PacketDecisionNotes = if ($HasApprovedOwnerSignoff) { "Owner sign-off loaded from $OwnerSignoffJson." } else { "Generated packet defaults to HOLD until owners review evidence and sign off." }

$Rows = @(
    (Row -Evidence "P0-B recovery raw log" -Path $P0BLog),
    (Row -Evidence "P0-B recovery Markdown report" -Path $P0BReport),
    (Row -Evidence "P0-B state evidence JSON" -Path $P0BState),
    (Row -Evidence "P0-B row state diff JSON" -Path $P0BRowStateDiffJson),
    (Row -Evidence "P0-B page state diff JSON" -Path $P0BPageStateDiffJson),
    (Row -Evidence "P0-B WAL replay diff JSON" -Path $P0BWalReplayDiffJson),
    (Row -Evidence "P0-C concurrency Markdown report" -Path $P0CReport),
    (Row -Evidence "P0-C concurrency JSON report" -Path $P0CJson),
    (Row -Evidence "P0-C consistency evidence JSON" -Path $P0CConsistencyEvidenceJson),
    (Row -Evidence "P0-D observability Markdown report" -Path $P0DReport),
    (Row -Evidence "P0-D observability JSON report" -Path $P0DJson),
    (Row -Evidence "P0-D metrics export Markdown report" -Path $P0DMetricsReport),
    (Row -Evidence "P0-D metrics export JSON report" -Path $P0DMetricsJson),
    (Row -Evidence "P0-D live metrics endpoint probe JSON" -Path $P0DMetricsEndpointJson),
    (Row -Evidence "P0-D structured logging Markdown report" -Path $P0DLoggingReport),
    (Row -Evidence "P0-D structured logging JSON report" -Path $P0DLoggingJson),
    (Row -Evidence "P0-D alert drill Markdown report" -Path $P0DAlertReport),
    (Row -Evidence "P0-D alert drill JSON report" -Path $P0DAlertJson),
    (Row -Evidence "P0-E full-chain smoke Markdown report" -Path $P0EReport),
    (Row -Evidence "P0-E full-chain smoke JSON report" -Path $P0EJson),
    (Row -Evidence "P0-E timed rollback evidence JSON" -Path $P0ETimedRollbackEvidenceJson),
    (Row -Evidence "Final regression Markdown report" -Path $FinalRegressionReport),
    (Row -Evidence "Final regression JSON report" -Path $FinalRegressionJson),
    (Row -Evidence "P0 evidence bundle Markdown report" -Path $BundleReport),
    (Row -Evidence "P0 evidence bundle JSON report" -Path $BundleJson),
    (Row -Evidence "P0 owner signoff JSON" -Path $OwnerSignoffJson),
    (Row -Evidence "P0 accepted deferrals JSON" -Path $AcceptedDeferralsJson),
    (Row -Evidence "P0 risk register" -Path "docs/planning/P0_RISK_REGISTER.md"),
    (PostPacketGateRow -Evidence "P0 delivery readiness audit JSON report" -Path $DeliveryReadinessJson)
)

$RequiredPaths = @(
    $P0BLog,
    $P0BReport,
    $P0BState,
    $P0CReport,
    $P0CJson,
    $P0DReport,
    $P0DJson,
    $P0DMetricsReport,
    $P0DMetricsJson,
    $P0DLoggingReport,
    $P0DLoggingJson,
    $P0DAlertReport,
    $P0DAlertJson,
    $P0EReport,
    $P0EJson,
    $FinalRegressionReport,
    $FinalRegressionJson,
    $BundleReport,
    $BundleJson
)

$MissingCount = @($RequiredPaths | Where-Object { [string]::IsNullOrWhiteSpace($_) -or -not (Test-Path -LiteralPath $_) }).Count
$PacketStatus = if ($MissingCount -eq 0) { "READY_FOR_REVIEW" } else { "INCOMPLETE" }

$Packet = @(
    "# P0 Release Approval Packet",
    "",
    "## Release candidate",
    "",
    "- Project: $Project",
    "- Branch or commit: $BranchOrCommit",
    "- Prepared by: Codex",
    "- Prepared at: $PreparedAt",
    "- Target environment: $TargetEnvironment",
    "- Proposed gray-release window: $GrayReleaseWindow",
    "- Packet status: $PacketStatus",
    "- Missing evidence count: $MissingCount",
    "- Delivery status: NOT_ASSESSED_BY_PACKET",
    "- Delivery readiness audit: ``$DeliveryReadinessJson``",
    "",
    "## Delivery readiness boundary",
    "",
    "This packet status only means required evidence artifacts are present for review. It does not mean the project is deliverable.",
    "",
    "Delivery requires a separate `READY` result from:",
    "",
    "````powershell",
    "./scripts/delivery_readiness_audit.ps1 -ApprovalPacket $OutputPath",
    "````",
    "",
    "The delivery audit checks the final decision, owner sign-off, open risks, TBD owners, and remaining delivery blockers.",
    "",
    "## Evidence links",
    "",
    "| Evidence | Path | Status |",
    "|---|---|---|",
    $Rows,
    "",
    "## Dedicated verifier commands",
    "",
    "| Verifier | Command |",
    "|---|---|",
    "| P0-B state evidence verifier | ``./scripts/verify_crash_recovery_state_evidence.ps1 -Path $P0BState`` |",
    "| P0-B row state diff verifier | ``./scripts/verify_p0b_state_diff_artifact.ps1 -Path $P0BRowStateDiffJson`` |",
    "| P0-B page state diff verifier | ``./scripts/verify_p0b_state_diff_artifact.ps1 -Path $P0BPageStateDiffJson`` |",
    "| P0-B WAL replay diff verifier | ``./scripts/verify_p0b_state_diff_artifact.ps1 -Path $P0BWalReplayDiffJson`` |",
    "| P0-C concurrency report verifier | ``./scripts/verify_concurrency_validation_report.ps1 -Path $P0CJson`` |",
    "| P0-C consistency evidence verifier | ``./scripts/verify_p0c_consistency_evidence.ps1 -Path $P0CConsistencyEvidenceJson`` |",
    "| P0-D observability report verifier | ``./scripts/verify_observability_report.ps1 -Path $P0DJson`` |",
    "| P0-D metrics export report verifier | ``./scripts/verify_metrics_export_report.ps1 -Path $P0DMetricsJson`` |",
    "| P0-D live metrics endpoint probe verifier | ``./scripts/verify_metrics_endpoint_probe_report.ps1 -Path $P0DMetricsEndpointJson`` |",
    "| P0-D structured logging report verifier | ``./scripts/verify_structured_logging_report.ps1 -Path $P0DLoggingJson`` |",
    "| P0-D alert drill report verifier | ``./scripts/verify_alert_drill_report.ps1 -Path $P0DAlertJson`` |",
    "| P0-E full-chain report verifier | ``./scripts/verify_full_chain_drill_report.ps1 -Path $P0EJson`` |",
    "| P0-E timed rollback evidence verifier | ``./scripts/verify_p0e_timed_rollback_evidence.ps1 -Path $P0ETimedRollbackEvidenceJson`` |",
    "| Final regression verifier | ``./scripts/verify_final_regression_gate_report.ps1 -Path $FinalRegressionJson`` |",
    "| P0 evidence bundle verifier | ``./scripts/verify_p0_evidence_bundle.ps1 <bundle args>`` |",
    "| P0 owner signoff verifier | ``./scripts/verify_p0_owner_signoff.ps1 -Path $OwnerSignoffJson`` |",
    "| P0 accepted deferrals verifier | ``./scripts/verify_p0_accepted_deferrals.ps1 -Path $AcceptedDeferralsJson`` |",
    "| P0 risk register verifier | ``./scripts/verify_p0_risk_register.ps1`` |",
    "| P0 governance gate verifier | ``./scripts/verify_p0_governance_gate.ps1 -OwnerSignoffJson $OwnerSignoffJson -AcceptedDeferralsJson $AcceptedDeferralsJson`` |",
    "| P0 governance gate report verifier | ``./scripts/verify_p0_governance_gate_report.ps1 -Path reports/p0_governance_gate_<timestamp>.json`` |",
    "| P0 delivery readiness audit | ``./scripts/delivery_readiness_audit.ps1 -ApprovalPacket $OutputPath`` |",
    "| P0 delivery readiness audit verifier | ``./scripts/verify_delivery_readiness_audit.ps1 -Path $DeliveryReadinessJson`` |",
    "",
    "## Approval checklist",
    "",
    "- [ ] P0-B recovery evidence is linked and verified.",
    "- [ ] P0-C concurrency evidence is linked and verified.",
    "- [ ] P0-D observability evidence is linked and verified.",
    "- [ ] P0-D metrics export evidence is linked and verified.",
    "- [ ] P0-D structured logging evidence is linked and verified.",
    "- [ ] P0-D alert drill evidence is linked and verified.",
    "- [ ] P0-E full-chain drill readiness evidence is linked and verified.",
    "- [ ] Final default regression evidence is linked and verified.",
    "- [ ] Evidence bundle is linked and verified.",
    "- [ ] Live metrics export evidence is available or explicitly deferred by owner.",
    "- [ ] Timed rollback drill evidence is available or explicitly deferred by owner.",
    "- [ ] Data recovery point and replay boundary are documented.",
    "- $(Get-OwnerCheck -Role "release_owner") Release owner approves.",
    "- $(Get-OwnerCheck -Role "rollback_owner") Rollback owner approves.",
    "- $(Get-OwnerCheck -Role "data_recovery_owner") Data recovery owner approves.",
    "- $(Get-OwnerCheck -Role "observability_owner") Observability owner approves.",
    "- $(Get-OwnerCheck -Role "business_owner") Business owner approves.",
    "",
    "## Final decision",
    "",
    "- Decision: $PacketDecision",
    "- Decision time: $PacketDecisionTime",
    "- Decision owner: $PacketDecisionOwner",
    "- Notes: $PacketDecisionNotes"
)

$Packet | Set-Content -Path $OutputPath -Encoding utf8

Write-Host "P0 release approval packet status: $PacketStatus"
Write-Host "Missing evidence count: $MissingCount"
Write-Host "Approval packet written: $OutputPath"

if ($MissingCount -eq 0) {
    exit 0
}

exit 1
