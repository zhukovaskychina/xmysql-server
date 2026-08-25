param(
    [string]$ReportDir = $env:P0_BUNDLE_REPORT_DIR,
    [string]$P0BReport = "reports/crash_recovery_drill_20260614_070446.md",
    [string]$P0BLog = "reports/crash_recovery_drill_20260614_070446.log",
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
    [string]$FinalRegressionJson = ""
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
$StartedAt = Get-Date -Format "o"
$RunId = "p0_evidence_bundle_$Timestamp"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"

$Checks = @()

function Add-Check {
    param(
        [string]$Name,
        [string]$Workstream,
        [string]$Expected,
        [string]$Path,
        [bool]$Required
    )

    $Provided = -not [string]::IsNullOrWhiteSpace($Path)
    $Exists = $false

    if ($Provided) {
        $Exists = Test-Path -LiteralPath $Path
    }

    $Status = if ($Provided -and $Exists) {
        "PASS"
    } elseif ($Required) {
        "FAIL"
    } else {
        "WARN"
    }

    $Actual = if (-not $Provided) {
        "not provided"
    } elseif ($Exists) {
        "exists"
    } else {
        "missing"
    }

    $script:Checks += [PSCustomObject]@{
        name = $Name
        workstream = $Workstream
        expected = $Expected
        path = $Path
        required = $Required
        actual = $Actual
        status = $Status
    }
}

Add-Check -Name "p0b_recovery_markdown_report" -Workstream "P0-B" -Expected "3-run recovery drill Markdown report exists" -Path $P0BReport -Required $true
Add-Check -Name "p0b_recovery_raw_log" -Workstream "P0-B" -Expected "3-run recovery drill raw log exists" -Path $P0BLog -Required $true
Add-Check -Name "p0b_state_evidence_json" -Workstream "P0-B" -Expected "command-level state evidence JSON exists" -Path $P0BState -Required $true
Add-Check -Name "p0b_row_state_diff_json" -Workstream "P0-B" -Expected "focused row-state semantic diff artifact exists when generated" -Path $P0BRowStateDiffJson -Required $false
Add-Check -Name "p0b_page_state_diff_json" -Workstream "P0-B" -Expected "focused page-state semantic diff artifact exists when generated" -Path $P0BPageStateDiffJson -Required $false
Add-Check -Name "p0b_wal_replay_diff_json" -Workstream "P0-B" -Expected "focused WAL replay semantic diff artifact exists when generated" -Path $P0BWalReplayDiffJson -Required $false
Add-Check -Name "p0c_concurrency_markdown_report" -Workstream "P0-C" -Expected "concurrency validation Markdown report exists" -Path $P0CReport -Required $true
Add-Check -Name "p0c_concurrency_json_report" -Workstream "P0-C" -Expected "concurrency validation JSON report exists" -Path $P0CJson -Required $true
Add-Check -Name "p0c_consistency_evidence_json" -Workstream "P0-C" -Expected "focused consistency evidence JSON exists when generated" -Path $P0CConsistencyEvidenceJson -Required $false
Add-Check -Name "p0d_observability_markdown_report" -Workstream "P0-D" -Expected "observability smoke Markdown report exists" -Path $P0DReport -Required $true
Add-Check -Name "p0d_observability_json_report" -Workstream "P0-D" -Expected "observability smoke JSON report exists" -Path $P0DJson -Required $true
Add-Check -Name "p0d_metrics_export_markdown_report" -Workstream "P0-D" -Expected "metrics export smoke Markdown report exists" -Path $P0DMetricsReport -Required $true
Add-Check -Name "p0d_metrics_export_json_report" -Workstream "P0-D" -Expected "metrics export smoke JSON report exists" -Path $P0DMetricsJson -Required $true
Add-Check -Name "p0d_live_metrics_endpoint_probe_json" -Workstream "P0-D" -Expected "focused live metrics endpoint probe JSON exists when generated" -Path $P0DMetricsEndpointJson -Required $false
Add-Check -Name "p0d_structured_logging_markdown_report" -Workstream "P0-D" -Expected "structured logging smoke Markdown report exists" -Path $P0DLoggingReport -Required $true
Add-Check -Name "p0d_structured_logging_json_report" -Workstream "P0-D" -Expected "structured logging smoke JSON report exists" -Path $P0DLoggingJson -Required $true
Add-Check -Name "p0d_alert_drill_markdown_report" -Workstream "P0-D" -Expected "alert drill smoke Markdown report exists" -Path $P0DAlertReport -Required $true
Add-Check -Name "p0d_alert_drill_json_report" -Workstream "P0-D" -Expected "alert drill smoke JSON report exists" -Path $P0DAlertJson -Required $true
Add-Check -Name "p0e_full_chain_markdown_report" -Workstream "P0-E" -Expected "full-chain drill smoke Markdown report exists" -Path $P0EReport -Required $true
Add-Check -Name "p0e_full_chain_json_report" -Workstream "P0-E" -Expected "full-chain drill smoke JSON report exists" -Path $P0EJson -Required $true
Add-Check -Name "p0e_timed_rollback_evidence_json" -Workstream "P0-E" -Expected "focused timed rollback evidence JSON exists when generated" -Path $P0ETimedRollbackEvidenceJson -Required $false
Add-Check -Name "final_regression_markdown_report" -Workstream "Final" -Expected "final regression Markdown report exists" -Path $FinalRegressionReport -Required $true
Add-Check -Name "final_regression_json_report" -Workstream "Final" -Expected "final regression JSON report exists" -Path $FinalRegressionJson -Required $true

$FinishedAt = Get-Date -Format "o"
$FailedChecks = @($Checks | Where-Object { $_.status -eq "FAIL" })
$WarningChecks = @($Checks | Where-Object { $_.status -eq "WARN" })
$Status = if ($FailedChecks.Count -eq 0) { "PASS" } else { "FAIL" }
$ExitCode = if ($Status -eq "PASS") { 0 } else { 1 }

$Report = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = $Status
    evidence_type = "p0_evidence_bundle"
    repository = "$RepoRoot"
    started_at = $StartedAt
    finished_at = $FinishedAt
    checks = $Checks
    summary = [ordered]@{
        total = $Checks.Count
        passed = @($Checks | Where-Object { $_.status -eq "PASS" }).Count
        failed = $FailedChecks.Count
        warnings = $WarningChecks.Count
    }
    limitations = @(
        "This bundle verifier checks evidence presence only.",
        "It does not execute tests, smoke scripts, or schema verifiers.",
        "Production approval still requires each linked report to pass its dedicated verifier and a fresh final regression gate."
    )
}

$Report | ConvertTo-Json -Depth 12 | Set-Content -Path $JsonPath -Encoding utf8

$CheckLines = $Checks | ForEach-Object {
    "- $($_.workstream) $($_.name): $($_.status) ($($_.actual))"
}

$Markdown = @(
    "# P0 Evidence Bundle Report",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- JSON report: ``$JsonPath``",
    "- Total checks: $($Checks.Count)",
    "- Passed: $(@($Checks | Where-Object { $_.status -eq "PASS" }).Count)",
    "- Failed: $($FailedChecks.Count)",
    "- Warnings: $($WarningChecks.Count)",
    "",
    "## Checks",
    "",
    $CheckLines,
    "",
    "## Acceptance note",
    "",
    "This report checks evidence presence only. It does not replace dedicated report verifiers, live drills, or final regression validation.",
    "",
    "## Result",
    "",
    "- Exit code: $ExitCode",
    "- Result: $Status"
)

$Markdown | Set-Content -Path $MarkdownPath -Encoding utf8

Write-Host "P0 evidence bundle status: $Status"
Write-Host "Markdown report written: $MarkdownPath"
Write-Host "JSON report written: $JsonPath"

exit $ExitCode
