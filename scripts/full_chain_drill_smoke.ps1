param(
    [string]$ReportDir = $env:FULL_CHAIN_REPORT_DIR,
    [string]$P0BReport = $env:P0B_REPORT,
    [string]$P0CReport = $env:P0C_REPORT,
    [string]$P0DReport = $env:P0D_REPORT,
    [int]$RollbackWindowSeconds = 300,
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

if ($RollbackWindowSeconds -lt 1) {
    throw "RollbackWindowSeconds must be greater than or equal to 1."
}

if (-not [string]::IsNullOrWhiteSpace($TimedRollbackStatus)) {
    if ($RollbackDurationSeconds -lt 0) {
        throw "RollbackDurationSeconds must be greater than or equal to 0 when TimedRollbackStatus is provided."
    }

    if ([string]::IsNullOrWhiteSpace($RecoveryPoint)) {
        throw "RecoveryPoint is required when TimedRollbackStatus is provided."
    }

    if ([string]::IsNullOrWhiteSpace($ReplayBoundary)) {
        throw "ReplayBoundary is required when TimedRollbackStatus is provided."
    }
}

New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$StartedAt = Get-Date -Format "o"
$RunId = "full_chain_drill_$Timestamp"
$LogPath = Join-Path $ReportDir "$RunId.log"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"

$RequiredRunbooks = @(
    [PSCustomObject]@{ Name = "p0e_plan"; Path = "docs/planning/P0_E_RELEASE_ROLLBACK_PLAN.md" },
    [PSCustomObject]@{ Name = "gray_release_runbook"; Path = "docs/operations/gray_release_runbook.md" },
    [PSCustomObject]@{ Name = "rollback_runbook"; Path = "docs/operations/rollback_runbook.md" },
    [PSCustomObject]@{ Name = "data_recovery_runbook"; Path = "docs/operations/data_recovery_runbook.md" },
    [PSCustomObject]@{ Name = "full_chain_drill_template"; Path = "docs/operations/full_chain_drill_record_template.md" }
)

$EvidenceLinks = @(
    [PSCustomObject]@{ Name = "p0b_recovery"; Path = $P0BReport },
    [PSCustomObject]@{ Name = "p0c_concurrency"; Path = $P0CReport },
    [PSCustomObject]@{ Name = "p0d_observability"; Path = $P0DReport }
)

$Checks = @()

function Add-Check {
    param(
        [string]$Name,
        [string]$Expected,
        [string]$Actual,
        [string]$Status
    )

    $script:Checks += [PSCustomObject]@{
        name = $Name
        expected = $Expected
        actual = $Actual
        status = $Status
    }
}

"=== full-chain drill smoke $StartedAt ===" | Set-Content -Path $LogPath -Encoding utf8
"repo: $RepoRoot" | Add-Content -Path $LogPath -Encoding utf8
"rollback_window_seconds: $RollbackWindowSeconds" | Add-Content -Path $LogPath -Encoding utf8
"timed_rollback_status: $TimedRollbackStatus" | Add-Content -Path $LogPath -Encoding utf8
"rollback_duration_seconds: $RollbackDurationSeconds" | Add-Content -Path $LogPath -Encoding utf8
"recovery_point: $RecoveryPoint" | Add-Content -Path $LogPath -Encoding utf8
"replay_boundary: $ReplayBoundary" | Add-Content -Path $LogPath -Encoding utf8

foreach ($Runbook in $RequiredRunbooks) {
    $Exists = Test-Path -LiteralPath $Runbook.Path
    Add-Check -Name "$($Runbook.Name)_exists" -Expected "$($Runbook.Path) exists" -Actual $(if ($Exists) { "exists" } else { "missing" }) -Status $(if ($Exists) { "PASS" } else { "FAIL" })
}

foreach ($Evidence in $EvidenceLinks) {
    if ([string]::IsNullOrWhiteSpace($Evidence.Path)) {
        Add-Check -Name "$($Evidence.Name)_linked" -Expected "$($Evidence.Name) report path is provided" -Actual "not provided" -Status "FAIL"
        continue
    }

    $Exists = Test-Path -LiteralPath $Evidence.Path
    Add-Check -Name "$($Evidence.Name)_exists" -Expected "$($Evidence.Path) exists" -Actual $(if ($Exists) { "exists" } else { "missing" }) -Status $(if ($Exists) { "PASS" } else { "FAIL" })
}

$FailedChecks = @($Checks | Where-Object { $_.status -ne "PASS" })
$FinishedAt = Get-Date -Format "o"
$Status = if ($FailedChecks.Count -eq 0) { "PASS" } else { "FAIL" }
$ExitCode = if ($Status -eq "PASS") { 0 } else { 1 }

foreach ($Check in $Checks) {
    "$($Check.status) $($Check.name): $($Check.actual)" | Add-Content -Path $LogPath -Encoding utf8
}

$TimedRollbackEvidence = [ordered]@{
    status = "NOT_PROVIDED"
    rollback_duration_seconds = $RollbackDurationSeconds
    rollback_window_seconds = $RollbackWindowSeconds
    recovery_point = $RecoveryPoint
    replay_boundary = $ReplayBoundary
    within_window = $false
}

if (-not [string]::IsNullOrWhiteSpace($TimedRollbackStatus)) {
    $TimedRollbackEvidence.status = $TimedRollbackStatus
    $TimedRollbackEvidence.within_window = $RollbackDurationSeconds -le $RollbackWindowSeconds
}

$Report = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = $Status
    evidence_type = "release_rollback_drill_smoke"
    repository = "$RepoRoot"
    raw_log = "$LogPath"
    markdown_report = "$MarkdownPath"
    rollback_window_seconds = $RollbackWindowSeconds
    linked_evidence = [ordered]@{
        p0b_recovery = $P0BReport
        p0c_concurrency = $P0CReport
        p0d_observability = $P0DReport
    }
    timed_rollback_evidence = $TimedRollbackEvidence
    checks = $Checks
    limitations = @(
        "This smoke report verifies runbook presence and linked evidence paths.",
        "It does not execute a real release, rollback, or data recovery drill by itself.",
        "Timed rollback evidence is only authoritative when TimedRollbackStatus, RollbackDurationSeconds, RecoveryPoint, and ReplayBoundary are supplied from an actual drill.",
        "Full P0-E acceptance still requires a timed full-chain drill with rollback and recovery evidence."
    )
}

$Report | ConvertTo-Json -Depth 12 | Set-Content -Path $JsonPath -Encoding utf8

$CheckLines = $Checks | ForEach-Object {
    "- $($_.name): $($_.status) ($($_.actual))"
}

$TimedRollbackLines = @(
    "- Status: $($TimedRollbackEvidence.status)",
    "- Rollback duration seconds: $($TimedRollbackEvidence.rollback_duration_seconds)",
    "- Rollback window seconds: $($TimedRollbackEvidence.rollback_window_seconds)",
    "- Within window: $($TimedRollbackEvidence.within_window)",
    "- Recovery point: ``$($TimedRollbackEvidence.recovery_point)``",
    "- Replay boundary: ``$($TimedRollbackEvidence.replay_boundary)``"
)

$Markdown = @(
    "# Full Chain Release/Rollback Drill Smoke Report",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- Rollback window seconds: $RollbackWindowSeconds",
    "- Raw log: ``$LogPath``",
    "- JSON report: ``$JsonPath``",
    "",
    "## Linked evidence",
    "",
    "- P0-B recovery: ``$P0BReport``",
    "- P0-C concurrency: ``$P0CReport``",
    "- P0-D observability: ``$P0DReport``",
    "",
    "## Checks",
    "",
    $CheckLines,
    "",
    "## Timed rollback evidence",
    "",
    $TimedRollbackLines,
    "",
    "## Acceptance note",
    "",
    "This is P0-E drill-readiness smoke evidence. It does not replace a timed full release, rollback, and data recovery drill.",
    "",
    "## Result",
    "",
    "- Exit code: $ExitCode",
    "- Result: $Status"
)

$Markdown | Set-Content -Path $MarkdownPath -Encoding utf8

Write-Host "Full-chain drill smoke status: $Status"
Write-Host "Raw log written: $LogPath"
Write-Host "Markdown report written: $MarkdownPath"
Write-Host "JSON report written: $JsonPath"

exit $ExitCode
