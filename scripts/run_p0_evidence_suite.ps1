param(
    [string]$ReportDir = $env:P0_SUITE_REPORT_DIR,
    [int]$RecoveryRuns = 3,
    [int]$ConcurrencyRuns = 1,
    [int]$RollbackWindowSeconds = 300,
    [string]$P0DMetricsEndpointJson = "",
    [switch]$GenerateP0DMetricsEndpointEvidence,
    [string]$AcceptedDeferralsJson = "",
    [string]$OwnerSignoffJson = "",
    [string]$P0BRowStateDiffJson = "",
    [string]$P0BPageStateDiffJson = "",
    [string]$P0BWalReplayDiffJson = "",
    [switch]$GenerateP0BStateSnapshotEvidence,
    [string]$P0CConsistencyEvidenceJson = "",
    [switch]$GenerateP0CConsistencyEvidence,
    [string]$P0ETimedRollbackEvidenceJson = "",
    [switch]$GenerateP0ETimedRollbackEvidence,
    [ValidateSet("", "PASS", "FAIL")]
    [string]$TimedRollbackStatus = "",
    [int]$RollbackDurationSeconds = -1,
    [string]$RecoveryPoint = "",
    [string]$ReplayBoundary = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RepoRoot

if ([string]::IsNullOrWhiteSpace($ReportDir)) {
    $ReportDir = Join-Path $RepoRoot "reports"
}

if ($RecoveryRuns -lt 1) {
    throw "RecoveryRuns must be greater than or equal to 1."
}

if ($ConcurrencyRuns -lt 1) {
    throw "ConcurrencyRuns must be greater than or equal to 1."
}

if ($RollbackWindowSeconds -lt 1) {
    throw "RollbackWindowSeconds must be greater than or equal to 1."
}

New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$SuiteStartedAt = (Get-Date).AddSeconds(-2)
$SuiteRunId = "p0_evidence_suite_" + (Get-Date -Format "yyyyMMdd_HHmmss")
$GeneratedP0DMetricsEndpointEvidence = $false

function Get-LatestReport {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Pattern
    )

    $Match = Get-ChildItem -LiteralPath $ReportDir -Filter $Pattern |
        Where-Object { $_.LastWriteTime -ge $SuiteStartedAt } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if (-not $Match) {
        throw "Expected report not found with pattern '$Pattern' in '$ReportDir' after suite start '$SuiteStartedAt'."
    }

    return $Match.FullName
}

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [scriptblock]$Body,
        [switch]$AllowNonZeroExit
    )

    Write-Host ""
    Write-Host "=== P0 evidence suite step: $Name ==="
    $global:LASTEXITCODE = 0
    & $Body
    if ($LASTEXITCODE -ne 0 -and -not $AllowNonZeroExit) {
        throw "P0 evidence suite step '$Name' failed with exit code $LASTEXITCODE."
    }
}

Invoke-Step -Name "P0 tooling preflight" -Body {
    & (Join-Path $ScriptDir "verify_p0_tooling_preflight.ps1") -ReportDir $ReportDir
}

if ($GenerateP0BStateSnapshotEvidence) {
    Invoke-Step -Name "P0-B focused state snapshot evidence generation" -Body {
        & (Join-Path $ScriptDir "generate_p0b_state_snapshot_evidence.ps1") -ReportDir $ReportDir
    }

    $P0BRowStateDiffJson = Get-LatestReport -Pattern "p0b_state_snapshot_evidence_*.row_state_diff.json"
    $P0BPageStateDiffJson = Get-LatestReport -Pattern "p0b_state_snapshot_evidence_*.page_state_diff.json"
    $P0BWalReplayDiffJson = Get-LatestReport -Pattern "p0b_state_snapshot_evidence_*.wal_replay_diff.json"
}

if ($GenerateP0CConsistencyEvidence) {
    Invoke-Step -Name "P0-C focused consistency evidence generation" -Body {
        & (Join-Path $ScriptDir "generate_p0c_consistency_evidence.ps1") -ReportDir $ReportDir
    }

    $P0CConsistencyEvidenceJson = Get-LatestReport -Pattern "p0c_consistency_evidence_*.json"
}

if ($GenerateP0ETimedRollbackEvidence) {
    Invoke-Step -Name "P0-E focused timed rollback evidence generation" -Body {
        & (Join-Path $ScriptDir "generate_p0e_timed_rollback_evidence.ps1") `
            -ReportDir $ReportDir `
            -RollbackWindowSeconds $RollbackWindowSeconds
    }

    $P0ETimedRollbackEvidenceJson = Get-LatestReport -Pattern "p0e_timed_rollback_evidence_*.json"
}

if (-not [string]::IsNullOrWhiteSpace($P0ETimedRollbackEvidenceJson)) {
    $P0ETimedRollbackEvidence = Get-Content -Raw -LiteralPath $P0ETimedRollbackEvidenceJson | ConvertFrom-Json
    $TimedRollbackStatus = [string]$P0ETimedRollbackEvidence.status
    $RollbackDurationSeconds = [int]$P0ETimedRollbackEvidence.rollback_duration_seconds
    $RecoveryPoint = [string]$P0ETimedRollbackEvidence.recovery_point
    $ReplayBoundary = [string]$P0ETimedRollbackEvidence.replay_boundary
}

if ($GenerateP0DMetricsEndpointEvidence) {
    Invoke-Step -Name "P0-D focused metrics endpoint evidence generation" -Body {
        & (Join-Path $ScriptDir "generate_p0d_metrics_endpoint_evidence.ps1") -ReportDir $ReportDir
    }

    $P0DMetricsEndpointJson = Get-LatestReport -Pattern "metrics_endpoint_probe_*.json"
    $GeneratedP0DMetricsEndpointEvidence = $true
}

Invoke-Step -Name "P0-B crash recovery with state evidence" -Body {
    & (Join-Path $ScriptDir "crash_recovery_drill.ps1") `
        -ReportDir $ReportDir `
        -Runs $RecoveryRuns `
        -StateEvidence `
        -RowStateDiffJson $P0BRowStateDiffJson `
        -PageStateDiffJson $P0BPageStateDiffJson `
        -WalReplayDiffJson $P0BWalReplayDiffJson
}

$P0BReport = Get-LatestReport -Pattern "crash_recovery_drill_*.md"
$P0BLog = Get-LatestReport -Pattern "crash_recovery_drill_*.log"
$P0BState = Get-LatestReport -Pattern "crash_recovery_drill_*.state.json"

Invoke-Step -Name "P0-B state evidence schema verification" -Body {
    & (Join-Path $ScriptDir "verify_crash_recovery_state_evidence.ps1") -Path $P0BState
}

Invoke-Step -Name "P0-D observability smoke" -Body {
    & (Join-Path $ScriptDir "observability_smoke.ps1") -ReportDir $ReportDir
}

$P0DReport = Get-LatestReport -Pattern "observability_smoke_*.md"
$P0DJson = Get-LatestReport -Pattern "observability_smoke_*.json"

Invoke-Step -Name "P0-D observability report schema verification" -Body {
    & (Join-Path $ScriptDir "verify_observability_report.ps1") -Path $P0DJson
}

Invoke-Step -Name "P0-D metrics export smoke" -Body {
    & (Join-Path $ScriptDir "metrics_export_smoke.ps1") -ReportDir $ReportDir
}

$P0DMetricsReport = Get-LatestReport -Pattern "metrics_export_*.md"
$P0DMetricsJson = Get-LatestReport -Pattern "metrics_export_*.json"

Invoke-Step -Name "P0-D metrics export report schema verification" -Body {
    & (Join-Path $ScriptDir "verify_metrics_export_report.ps1") -Path $P0DMetricsJson
}

Invoke-Step -Name "P0-D structured logging smoke" -Body {
    & (Join-Path $ScriptDir "structured_logging_smoke.ps1") -ReportDir $ReportDir
}

$P0DLoggingReport = Get-LatestReport -Pattern "structured_logging_*.md"
$P0DLoggingJson = Get-LatestReport -Pattern "structured_logging_*.json"

Invoke-Step -Name "P0-D structured logging report schema verification" -Body {
    & (Join-Path $ScriptDir "verify_structured_logging_report.ps1") -Path $P0DLoggingJson
}

Invoke-Step -Name "P0-D alert drill smoke" -Body {
    & (Join-Path $ScriptDir "alert_drill_smoke.ps1") -ReportDir $ReportDir
}

$P0DAlertReport = Get-LatestReport -Pattern "alert_drill_*.md"
$P0DAlertJson = Get-LatestReport -Pattern "alert_drill_*.json"

Invoke-Step -Name "P0-D alert drill report schema verification" -Body {
    & (Join-Path $ScriptDir "verify_alert_drill_report.ps1") -Path $P0DAlertJson
}

Invoke-Step -Name "P0-C concurrency validation" -Body {
    & (Join-Path $ScriptDir "concurrency_validation.ps1") `
        -ReportDir $ReportDir `
        -Runs $ConcurrencyRuns `
        -P0CConsistencyEvidenceJson $P0CConsistencyEvidenceJson
}

$P0CReport = Get-LatestReport -Pattern "concurrency_validation_*.md"
$P0CJson = Get-LatestReport -Pattern "concurrency_validation_*.json"

Invoke-Step -Name "P0-C concurrency report schema verification" -Body {
    & (Join-Path $ScriptDir "verify_concurrency_validation_report.ps1") -Path $P0CJson
}

Invoke-Step -Name "P0-E full-chain drill smoke" -Body {
    & (Join-Path $ScriptDir "full_chain_drill_smoke.ps1") `
        -ReportDir $ReportDir `
        -P0BReport $P0BReport `
        -P0CReport $P0CReport `
        -P0DReport $P0DReport `
        -RollbackWindowSeconds $RollbackWindowSeconds `
        -TimedRollbackStatus $TimedRollbackStatus `
        -RollbackDurationSeconds $RollbackDurationSeconds `
        -RecoveryPoint $RecoveryPoint `
        -ReplayBoundary $ReplayBoundary
}

$P0EReport = Get-LatestReport -Pattern "full_chain_drill_*.md"
$P0EJson = Get-LatestReport -Pattern "full_chain_drill_*.json"

Invoke-Step -Name "P0-E full-chain drill report schema verification" -Body {
    & (Join-Path $ScriptDir "verify_full_chain_drill_report.ps1") -Path $P0EJson
}

Invoke-Step -Name "Final default regression gate" -Body {
    & (Join-Path $ScriptDir "final_regression_gate.ps1") -ReportDir $ReportDir
}

$FinalRegressionReport = Get-LatestReport -Pattern "final_regression_gate_*.md"
$FinalRegressionJson = Get-LatestReport -Pattern "final_regression_gate_*.json"

Invoke-Step -Name "Final default regression gate report schema verification" -Body {
    & (Join-Path $ScriptDir "verify_final_regression_gate_report.ps1") -Path $FinalRegressionJson
}

Invoke-Step -Name "P0 evidence bundle verification" -Body {
    & (Join-Path $ScriptDir "verify_p0_evidence_bundle.ps1") `
        -ReportDir $ReportDir `
        -P0BReport $P0BReport `
        -P0BLog $P0BLog `
        -P0BState $P0BState `
        -P0BRowStateDiffJson $P0BRowStateDiffJson `
        -P0BPageStateDiffJson $P0BPageStateDiffJson `
        -P0BWalReplayDiffJson $P0BWalReplayDiffJson `
        -P0CReport $P0CReport `
        -P0CJson $P0CJson `
        -P0CConsistencyEvidenceJson $P0CConsistencyEvidenceJson `
        -P0DReport $P0DReport `
        -P0DJson $P0DJson `
        -P0DMetricsReport $P0DMetricsReport `
        -P0DMetricsJson $P0DMetricsJson `
        -P0DMetricsEndpointJson $P0DMetricsEndpointJson `
        -P0DLoggingReport $P0DLoggingReport `
        -P0DLoggingJson $P0DLoggingJson `
        -P0DAlertReport $P0DAlertReport `
        -P0DAlertJson $P0DAlertJson `
        -P0EReport $P0EReport `
        -P0EJson $P0EJson `
        -P0ETimedRollbackEvidenceJson $P0ETimedRollbackEvidenceJson `
        -FinalRegressionReport $FinalRegressionReport `
        -FinalRegressionJson $FinalRegressionJson
}

$BundleReport = Get-LatestReport -Pattern "p0_evidence_bundle_*.md"
$BundleJson = Get-LatestReport -Pattern "p0_evidence_bundle_*.json"

Invoke-Step -Name "P0 release approval packet generation" -Body {
    & (Join-Path $ScriptDir "generate_p0_release_approval_packet.ps1") `
        -ReportDir $ReportDir `
        -P0BReport $P0BReport `
        -P0BLog $P0BLog `
        -P0BState $P0BState `
        -P0BRowStateDiffJson $P0BRowStateDiffJson `
        -P0BPageStateDiffJson $P0BPageStateDiffJson `
        -P0BWalReplayDiffJson $P0BWalReplayDiffJson `
        -P0CReport $P0CReport `
        -P0CJson $P0CJson `
        -P0CConsistencyEvidenceJson $P0CConsistencyEvidenceJson `
        -P0DReport $P0DReport `
        -P0DJson $P0DJson `
        -P0DMetricsReport $P0DMetricsReport `
        -P0DMetricsJson $P0DMetricsJson `
        -P0DMetricsEndpointJson $P0DMetricsEndpointJson `
        -P0DLoggingReport $P0DLoggingReport `
        -P0DLoggingJson $P0DLoggingJson `
        -P0DAlertReport $P0DAlertReport `
        -P0DAlertJson $P0DAlertJson `
        -P0EReport $P0EReport `
        -P0EJson $P0EJson `
        -P0ETimedRollbackEvidenceJson $P0ETimedRollbackEvidenceJson `
        -FinalRegressionReport $FinalRegressionReport `
        -FinalRegressionJson $FinalRegressionJson `
        -BundleReport $BundleReport `
        -BundleJson $BundleJson `
        -DeliveryReadinessJson "" `
        -OwnerSignoffJson $OwnerSignoffJson `
        -AcceptedDeferralsJson $AcceptedDeferralsJson
}

$ApprovalPacket = Get-LatestReport -Pattern "p0_release_approval_packet_*.md"

Invoke-Step -Name "P0 release approval packet validation" -Body {
    & (Join-Path $ScriptDir "verify_p0_release_approval_packet.ps1") -Path $ApprovalPacket
}

if (-not [string]::IsNullOrWhiteSpace($OwnerSignoffJson)) {
    Invoke-Step -Name "P0 risk register strict validation for owner signoff" -Body {
        & (Join-Path $ScriptDir "verify_p0_risk_register.ps1")
    }
}

if (-not [string]::IsNullOrWhiteSpace($AcceptedDeferralsJson)) {
    Invoke-Step -Name "Accepted delivery deferrals validation" -Body {
        & (Join-Path $ScriptDir "verify_p0_accepted_deferrals.ps1") -Path $AcceptedDeferralsJson
    }
}

Invoke-Step -Name "P0 delivery readiness audit" -AllowNonZeroExit -Body {
    & (Join-Path $ScriptDir "delivery_readiness_audit.ps1") `
        -ReportDir $ReportDir `
        -ApprovalPacket $ApprovalPacket `
        -P0BState $P0BState `
        -P0CJson $P0CJson `
        -P0DJson $P0DJson `
        -P0DMetricsJson $P0DMetricsJson `
        -P0DLoggingJson $P0DLoggingJson `
        -P0DAlertJson $P0DAlertJson `
        -P0DMetricsEndpointJson $P0DMetricsEndpointJson `
        -P0EJson $P0EJson `
        -FinalRegressionJson $FinalRegressionJson `
        -BundleJson $BundleJson `
        -AcceptedDeferralsJson $AcceptedDeferralsJson `
        -DisableReportFallback
}

$DeliveryReadinessJson = Get-LatestReport -Pattern "delivery_readiness_audit_*.json"
$DeliveryReadinessReport = Get-LatestReport -Pattern "delivery_readiness_audit_*.md"

Invoke-Step -Name "P0 delivery readiness audit report schema verification" -Body {
    & (Join-Path $ScriptDir "verify_delivery_readiness_audit.ps1") -Path $DeliveryReadinessJson
}

Invoke-Step -Name "P0 delivery remediation packet generation" -AllowNonZeroExit -Body {
    & (Join-Path $ScriptDir "delivery_remediation_packet.ps1") `
        -ReportDir $ReportDir `
        -AuditJson $DeliveryReadinessJson
}

$DeliveryRemediationJson = Get-LatestReport -Pattern "delivery_remediation_packet_*.json"
$DeliveryRemediationReport = Get-LatestReport -Pattern "delivery_remediation_packet_*.md"

Invoke-Step -Name "P0 delivery remediation packet verification" -Body {
    & (Join-Path $ScriptDir "verify_delivery_remediation_packet.ps1") -Path $DeliveryRemediationJson
}

$DeliveryReadiness = Get-Content -Raw -LiteralPath $DeliveryReadinessJson | ConvertFrom-Json

$SuiteFinishedAt = Get-Date -Format "o"
$SuiteSummaryJson = Join-Path $ReportDir "$SuiteRunId.summary.json"
$SuiteSummaryMarkdown = Join-Path $ReportDir "$SuiteRunId.summary.md"
$SuiteArtifacts = @(
    [PSCustomObject]@{ name = "P0-B recovery report"; kind = "markdown"; path = $P0BReport },
    [PSCustomObject]@{ name = "P0-B recovery log"; kind = "log"; path = $P0BLog },
    [PSCustomObject]@{ name = "P0-B state evidence"; kind = "json"; path = $P0BState },
    [PSCustomObject]@{ name = "P0-C concurrency report"; kind = "markdown"; path = $P0CReport },
    [PSCustomObject]@{ name = "P0-C concurrency JSON"; kind = "json"; path = $P0CJson },
    [PSCustomObject]@{ name = "P0-D observability report"; kind = "markdown"; path = $P0DReport },
    [PSCustomObject]@{ name = "P0-D observability JSON"; kind = "json"; path = $P0DJson },
    [PSCustomObject]@{ name = "P0-D metrics export report"; kind = "markdown"; path = $P0DMetricsReport },
    [PSCustomObject]@{ name = "P0-D metrics export JSON"; kind = "json"; path = $P0DMetricsJson },
    [PSCustomObject]@{ name = "P0-D structured logging report"; kind = "markdown"; path = $P0DLoggingReport },
    [PSCustomObject]@{ name = "P0-D structured logging JSON"; kind = "json"; path = $P0DLoggingJson },
    [PSCustomObject]@{ name = "P0-D alert drill report"; kind = "markdown"; path = $P0DAlertReport },
    [PSCustomObject]@{ name = "P0-D alert drill JSON"; kind = "json"; path = $P0DAlertJson },
    [PSCustomObject]@{ name = "P0-E full-chain report"; kind = "markdown"; path = $P0EReport },
    [PSCustomObject]@{ name = "P0-E full-chain JSON"; kind = "json"; path = $P0EJson },
    [PSCustomObject]@{ name = "Final regression report"; kind = "markdown"; path = $FinalRegressionReport },
    [PSCustomObject]@{ name = "Final regression JSON"; kind = "json"; path = $FinalRegressionJson },
    [PSCustomObject]@{ name = "P0 evidence bundle report"; kind = "markdown"; path = $BundleReport },
    [PSCustomObject]@{ name = "P0 evidence bundle JSON"; kind = "json"; path = $BundleJson },
    [PSCustomObject]@{ name = "P0 release approval packet"; kind = "approval"; path = $ApprovalPacket },
    [PSCustomObject]@{ name = "Delivery readiness report"; kind = "audit-markdown"; path = $DeliveryReadinessReport },
    [PSCustomObject]@{ name = "Delivery readiness JSON"; kind = "audit-json"; path = $DeliveryReadinessJson },
    [PSCustomObject]@{ name = "Delivery remediation report"; kind = "remediation-markdown"; path = $DeliveryRemediationReport },
    [PSCustomObject]@{ name = "Delivery remediation JSON"; kind = "remediation-json"; path = $DeliveryRemediationJson },
    [PSCustomObject]@{ name = "P0 evidence suite summary report"; kind = "summary-markdown"; path = $SuiteSummaryMarkdown },
    [PSCustomObject]@{ name = "P0 evidence suite summary JSON"; kind = "summary-json"; path = $SuiteSummaryJson }
)

if (-not [string]::IsNullOrWhiteSpace($P0CConsistencyEvidenceJson)) {
    $SuiteArtifacts += [PSCustomObject]@{ name = "P0-C focused consistency evidence"; kind = "json"; path = $P0CConsistencyEvidenceJson }
}

if ($GenerateP0BStateSnapshotEvidence) {
    $SuiteArtifacts += [PSCustomObject]@{ name = "P0-B focused row state diff"; kind = "json"; path = $P0BRowStateDiffJson }
    $SuiteArtifacts += [PSCustomObject]@{ name = "P0-B focused page state diff"; kind = "json"; path = $P0BPageStateDiffJson }
    $SuiteArtifacts += [PSCustomObject]@{ name = "P0-B focused WAL replay diff"; kind = "json"; path = $P0BWalReplayDiffJson }
}

if (-not [string]::IsNullOrWhiteSpace($P0ETimedRollbackEvidenceJson)) {
    $SuiteArtifacts += [PSCustomObject]@{ name = "P0-E focused timed rollback evidence"; kind = "json"; path = $P0ETimedRollbackEvidenceJson }
}

if ($GeneratedP0DMetricsEndpointEvidence -and -not [string]::IsNullOrWhiteSpace($P0DMetricsEndpointJson)) {
    $SuiteArtifacts += [PSCustomObject]@{ name = "P0-D metrics endpoint probe"; kind = "json"; path = $P0DMetricsEndpointJson }
}

foreach ($Artifact in $SuiteArtifacts) {
    $ChecksumMode = if ($Artifact.kind -in @("summary-markdown", "summary-json")) { "self-referential" } else { "sha256" }
    $Sha256 = ""
    $LastWriteTime = ""
    if ($ChecksumMode -eq "sha256") {
        $Sha256 = ((Get-FileHash -Algorithm SHA256 -LiteralPath $Artifact.path).Hash).ToLowerInvariant()
    }
    if (Test-Path -LiteralPath $Artifact.path) {
        $LastWriteTime = (Get-Item -LiteralPath $Artifact.path).LastWriteTime.ToString("o")
    } elseif ($ChecksumMode -eq "self-referential") {
        $LastWriteTime = (Get-Date).ToString("o")
    }

    $Artifact | Add-Member -NotePropertyName "checksum_mode" -NotePropertyValue $ChecksumMode -Force
    $Artifact | Add-Member -NotePropertyName "sha256" -NotePropertyValue $Sha256 -Force
    $Artifact | Add-Member -NotePropertyName "last_write_time" -NotePropertyValue $LastWriteTime -Force
}

$SuiteSummary = [ordered]@{
    generated_at = $SuiteFinishedAt
    run_id = $SuiteRunId
    evidence_type = "p0_evidence_suite_summary"
    repository = "$RepoRoot"
    report_dir = "$ReportDir"
    suite_started_at = $SuiteStartedAt.ToString("o")
    suite_finished_at = $SuiteFinishedAt
    delivery_status = $DeliveryReadiness.status
    artifacts = $SuiteArtifacts
    limitations = @(
        "This summary indexes artifacts generated by the current P0 evidence suite run.",
        "It does not replace dedicated report verifiers or the delivery readiness audit.",
        "Delivery requires the delivery readiness audit status to be READY."
    )
}
$SuiteSummary | ConvertTo-Json -Depth 12 | Set-Content -Path $SuiteSummaryJson -Encoding utf8
$SuiteArtifactLines = $SuiteArtifacts | ForEach-Object {
    "- $($_.name) [$($_.kind)]: ``$($_.path)``"
}
$SuiteSummaryMarkdownContent = @(
    "# P0 Evidence Suite Summary",
    "",
    "## Summary",
    "",
    "- Run ID: $SuiteRunId",
    "- Generated at: $SuiteFinishedAt",
    "- Repository: $RepoRoot",
    "- Report directory: ``$ReportDir``",
    "- Suite started at: $($SuiteStartedAt.ToString("o"))",
    "- Suite finished at: $SuiteFinishedAt",
    "- Delivery status: $($DeliveryReadiness.status)",
    "- JSON summary: ``$SuiteSummaryJson``",
    "",
    "## Current-run artifacts",
    "",
    $SuiteArtifactLines
)
$SuiteSummaryMarkdownContent | Set-Content -Path $SuiteSummaryMarkdown -Encoding utf8

Invoke-Step -Name "P0 evidence suite summary verification" -Body {
    & (Join-Path $ScriptDir "verify_p0_evidence_suite_summary.ps1") -Path $SuiteSummaryJson
}

if ($DeliveryReadiness.status -ne "READY") {
    throw "P0 delivery readiness audit status is $($DeliveryReadiness.status). See $DeliveryReadinessJson for delivery blockers, $DeliveryRemediationJson for remediation work items, and $SuiteSummaryJson for the current-run artifact index."
}

Write-Host ""
Write-Host "P0 evidence suite completed."
Write-Host "P0-B report: $P0BReport"
Write-Host "P0-B state: $P0BState"
Write-Host "P0-C report: $P0CReport"
Write-Host "P0-D report: $P0DReport"
Write-Host "P0-D metrics export report: $P0DMetricsReport"
Write-Host "P0-D structured logging report: $P0DLoggingReport"
Write-Host "P0-D alert drill report: $P0DAlertReport"
Write-Host "P0-E report: $P0EReport"
Write-Host "Final regression report: $FinalRegressionReport"
Write-Host "Bundle report: $BundleReport"
Write-Host "Bundle JSON: $BundleJson"
Write-Host "Approval packet: $ApprovalPacket"
Write-Host "Delivery readiness report: $DeliveryReadinessReport"
Write-Host "Delivery readiness JSON: $DeliveryReadinessJson"
Write-Host "Delivery remediation report: $DeliveryRemediationReport"
Write-Host "Delivery remediation JSON: $DeliveryRemediationJson"
Write-Host "Suite summary report: $SuiteSummaryMarkdown"
Write-Host "Suite summary JSON: $SuiteSummaryJson"
