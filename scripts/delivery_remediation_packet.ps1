param(
    [string]$ReportDir = $env:P0_DELIVERY_REPORT_DIR,
    [string]$AuditJson = "",
    [string]$OutputPath = ""
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
$GeneratedAt = Get-Date -Format "o"
$RunId = "delivery_remediation_packet_$Timestamp"
$JsonPath = Join-Path $ReportDir "$RunId.json"

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $ReportDir "$RunId.md"
}

function Get-LatestAuditJson {
    $Match = Get-ChildItem -LiteralPath $ReportDir -Filter "delivery_readiness_audit_*.json" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if ($Match) {
        return $Match.FullName
    }

    return ""
}

if ([string]::IsNullOrWhiteSpace($AuditJson)) {
    $AuditJson = Get-LatestAuditJson
}

$AuditExists = -not [string]::IsNullOrWhiteSpace($AuditJson) -and (Test-Path -LiteralPath $AuditJson)
$AuditStatus = "MISSING_AUDIT"
$Blockers = @()
$NextActions = @("Run scripts/delivery_readiness_audit.ps1 after generating the release approval packet.")

function Get-RemediationGuidance {
    param(
        [string]$Check,
        [string]$Action
    )

    $Text = "$Check $Action".ToLowerInvariant()

    if ($Text.Contains("p0-b") -or $Text.Contains("recovery") -or $Text.Contains("state")) {
        return [PSCustomObject]@{
            evidence_focus = "P0-B recovery state diff evidence"
            suggested_command = "./scripts/run_p0_delivery_candidate.ps1 -ReportDir reports"
            verifier = "./scripts/verify_crash_recovery_state_evidence.ps1 -Path reports/crash_recovery_drill_<timestamp>.state.json"
        }
    }

    if ($Text.Contains("p0-c") -or $Text.Contains("concurrency") -or $Text.Contains("consistency")) {
        return [PSCustomObject]@{
            evidence_focus = "P0-C consistency evidence"
            suggested_command = "./scripts/run_p0_delivery_candidate.ps1 -ReportDir reports"
            verifier = "./scripts/verify_concurrency_validation_report.ps1 -Path reports/concurrency_validation_<timestamp>.json"
        }
    }

    if ($Text.Contains("p0-d") -or $Text.Contains("observability") -or $Text.Contains("metrics") -or $Text.Contains("alert") -or $Text.Contains("logging")) {
        return [PSCustomObject]@{
            evidence_focus = "P0-D observability and live metrics endpoint evidence"
            suggested_command = "./scripts/run_p0_delivery_candidate.ps1 -ReportDir reports"
            verifier = "./scripts/verify_metrics_endpoint_probe_report.ps1 -Path reports/metrics_endpoint_probe_<timestamp>.json"
        }
    }

    if ($Text.Contains("p0-e") -or $Text.Contains("rollback") -or $Text.Contains("release") -or $Text.Contains("recovery point") -or $Text.Contains("replay boundary")) {
        return [PSCustomObject]@{
            evidence_focus = "P0-E timed rollback and full-chain evidence"
            suggested_command = "./scripts/run_p0_delivery_candidate.ps1 -ReportDir reports"
            verifier = "./scripts/verify_p0e_timed_rollback_evidence.ps1 -Path reports/p0e_timed_rollback_evidence_<timestamp>.json"
        }
    }

    if ($Text.Contains("final regression")) {
        return [PSCustomObject]@{
            evidence_focus = "Final regression gate evidence"
            suggested_command = "./scripts/final_regression_gate.ps1 -ReportDir reports"
            verifier = "./scripts/verify_final_regression_gate_report.ps1 -Path reports/final_regression_gate_<timestamp>.json"
        }
    }

    if ($Text.Contains("bundle")) {
        return [PSCustomObject]@{
            evidence_focus = "P0 evidence bundle"
            suggested_command = "./scripts/run_p0_delivery_candidate.ps1 -ReportDir reports"
            verifier = "./scripts/verify_p0_evidence_bundle.ps1 <bundle args>"
        }
    }

    if ($Text.Contains("approval") -or $Text.Contains("owner") -or $Text.Contains("decision") -or $Text.Contains("risk")) {
        return [PSCustomObject]@{
            evidence_focus = "Owner approval, risk register, or release decision"
            suggested_command = "Review the generated approval packet, risk register, and accepted deferrals before rerunning delivery_readiness_audit.ps1."
            verifier = "./scripts/verify_delivery_readiness_audit.ps1 -Path reports/delivery_readiness_audit_<timestamp>.json"
        }
    }

    return [PSCustomObject]@{
        evidence_focus = "Delivery readiness blocker"
        suggested_command = "Review the source delivery readiness audit blocker and rerun the relevant evidence generator."
        verifier = "./scripts/verify_delivery_readiness_audit.ps1 -Path reports/delivery_readiness_audit_<timestamp>.json"
    }
}

if ($AuditExists) {
    $Audit = Get-Content -Raw -LiteralPath $AuditJson | ConvertFrom-Json
    $AuditStatus = $Audit.status
    $Blockers = @($Audit.blockers)
    $NextActions = @($Audit.next_actions)
}

$PacketStatus = if ($AuditStatus -eq "READY") { "READY" } else { "ACTION_REQUIRED" }
$ExitCode = if ($PacketStatus -eq "READY") { 0 } else { 1 }

$WorkItems = @()
$Index = 1
foreach ($Blocker in $Blockers) {
    $Guidance = Get-RemediationGuidance -Check $Blocker.check -Action $Blocker.next_action
    $WorkItems += [PSCustomObject]@{
        id = "DELIVERY-$Index"
        source_check = $Blocker.check
        severity = $Blocker.severity
        current_state = $Blocker.actual
        required_action = $Blocker.next_action
        evidence_focus = $Guidance.evidence_focus
        suggested_command = $Guidance.suggested_command
        verifier = $Guidance.verifier
        owner = "TBD"
        status = "OPEN"
    }
    $Index++
}

if (-not $AuditExists) {
    $WorkItems += [PSCustomObject]@{
        id = "DELIVERY-1"
        source_check = "delivery_readiness_audit_missing"
        severity = "P0"
        current_state = "no delivery readiness audit JSON found"
        required_action = "Run scripts/delivery_readiness_audit.ps1 after generating the release approval packet."
        evidence_focus = "Delivery readiness audit"
        suggested_command = "./scripts/delivery_readiness_audit.ps1 -ApprovalPacket reports/p0_release_approval_packet_<timestamp>.md"
        verifier = "./scripts/verify_delivery_readiness_audit.ps1 -Path reports/delivery_readiness_audit_<timestamp>.json"
        owner = "TBD"
        status = "OPEN"
    }
}

$Report = [ordered]@{
    generated_at = $GeneratedAt
    run_id = $RunId
    status = $PacketStatus
    evidence_type = "delivery_remediation_packet"
    repository = "$RepoRoot"
    audit_json = $AuditJson
    audit_status = $AuditStatus
    markdown_report = "$OutputPath"
    work_items = $WorkItems
    next_actions = $NextActions
    limitations = @(
        "This packet translates delivery readiness blockers into remediation work items.",
        "It does not execute tests, evidence suites, release drills, or owner sign-off.",
        "A READY packet only means the source delivery readiness audit was READY."
    )
}

$Report | ConvertTo-Json -Depth 12 | Set-Content -Path $JsonPath -Encoding utf8

$WorkItemLines = if ($WorkItems.Count -gt 0) {
    $WorkItems | ForEach-Object {
        "- $($_.id) [$($_.severity)] $($_.source_check): $($_.required_action) Focus: $($_.evidence_focus). Command: ``$($_.suggested_command)``. Verifier: ``$($_.verifier)``. Owner: $($_.owner). Status: $($_.status)."
    }
} else {
    @("- None")
}

$NextActionLines = if ($NextActions.Count -gt 0) {
    $NextActions | ForEach-Object { "- $_" }
} else {
    @("- None")
}

$Markdown = @(
    "# P0 Delivery Remediation Packet",
    "",
    "## Summary",
    "",
    "- Status: $PacketStatus",
    "- Generated at: $GeneratedAt",
    "- Repository: $RepoRoot",
    "- Source audit JSON: ``$AuditJson``",
    "- Source audit status: $AuditStatus",
    "- JSON report: ``$JsonPath``",
    "- Work item count: $($WorkItems.Count)",
    "",
    "## Remediation work items",
    "",
    $WorkItemLines,
    "",
    "## Next actions",
    "",
    $NextActionLines,
    "",
    "## Result",
    "",
    "- Exit code: $ExitCode",
    "- Result: $PacketStatus"
)

$Markdown | Set-Content -Path $OutputPath -Encoding utf8

Write-Host "P0 delivery remediation packet status: $PacketStatus"
Write-Host "Markdown report written: $OutputPath"
Write-Host "JSON report written: $JsonPath"

exit $ExitCode
