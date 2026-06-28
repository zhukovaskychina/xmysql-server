param(
    [string]$ReportDir = $env:ALERT_DRILL_REPORT_DIR
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
$RunId = "alert_drill_$Timestamp"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"
$RawLogPath = Join-Path $ReportDir "$RunId.log"

$AlertRulesPath = "deploy/alerts/xmysql-p0-alerts.yml"
$RulesText = if (Test-Path -LiteralPath $AlertRulesPath) { Get-Content -Raw -LiteralPath $AlertRulesPath } else { "" }

$SampleMetrics = [ordered]@{
    query_error_rate = 0.08
    p99_latency_ms = 1500
    long_transactions_active = 1
    recovery_failures_increase = 1
    checkpoint_dirty_pages = 150000
    lock_wait_rate = 150
}

$AlertChecks = @(
    [PSCustomObject]@{
        name = "XMySQLHighErrorRate"
        expected = "fires when query error rate is greater than 0.05"
        actual = "query_error_rate=$($SampleMetrics.query_error_rate)"
        triggered = $SampleMetrics.query_error_rate -gt 0.05
    },
    [PSCustomObject]@{
        name = "XMySQLHighP99Latency"
        expected = "fires when p99 latency is greater than 1000ms"
        actual = "p99_latency_ms=$($SampleMetrics.p99_latency_ms)"
        triggered = $SampleMetrics.p99_latency_ms -gt 1000
    },
    [PSCustomObject]@{
        name = "XMySQLLongTransaction"
        expected = "fires when long transactions are active"
        actual = "long_transactions_active=$($SampleMetrics.long_transactions_active)"
        triggered = $SampleMetrics.long_transactions_active -gt 0
    },
    [PSCustomObject]@{
        name = "XMySQLRecoveryFailure"
        expected = "fires when recovery failures increase"
        actual = "recovery_failures_increase=$($SampleMetrics.recovery_failures_increase)"
        triggered = $SampleMetrics.recovery_failures_increase -gt 0
    },
    [PSCustomObject]@{
        name = "XMySQLCheckpointDirtyPagesHigh"
        expected = "fires when checkpoint dirty pages are greater than 100000"
        actual = "checkpoint_dirty_pages=$($SampleMetrics.checkpoint_dirty_pages)"
        triggered = $SampleMetrics.checkpoint_dirty_pages -gt 100000
    },
    [PSCustomObject]@{
        name = "XMySQLLockWaitSpike"
        expected = "fires when lock wait rate is greater than 100"
        actual = "lock_wait_rate=$($SampleMetrics.lock_wait_rate)"
        triggered = $SampleMetrics.lock_wait_rate -gt 100
    }
)

$Checks = @()

foreach ($Alert in $AlertChecks) {
    $RulePresent = $RulesText.Contains($Alert.name)
    $Pass = $RulePresent -and $Alert.triggered
    $Checks += [PSCustomObject]@{
        name = $Alert.name
        expected = $Alert.expected
        actual = "$($Alert.actual); rule_present=$RulePresent; triggered=$($Alert.triggered)"
        status = if ($Pass) { "PASS" } else { "FAIL" }
    }
}

@(
    "=== alert drill smoke $StartedAt ===",
    "repo: $RepoRoot",
    "rules: $AlertRulesPath",
    "sample_metrics: $($SampleMetrics | ConvertTo-Json -Compress)",
    ""
) | Set-Content -Path $RawLogPath -Encoding utf8

foreach ($Check in $Checks) {
    "$($Check.status) $($Check.name): $($Check.actual)" | Add-Content -Path $RawLogPath -Encoding utf8
}

$FailedChecks = @($Checks | Where-Object { $_.status -ne "PASS" })
$FinishedAt = Get-Date -Format "o"
$Status = if ($FailedChecks.Count -eq 0) { "PASS" } else { "FAIL" }
$ExitCode = if ($Status -eq "PASS") { 0 } else { 1 }

$Report = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = $Status
    evidence_type = "alert_drill_smoke"
    repository = "$RepoRoot"
    alert_rules = $AlertRulesPath
    raw_log = "$RawLogPath"
    markdown_report = "$MarkdownPath"
    sample_metrics = $SampleMetrics
    checks = $Checks
    limitations = @(
        "This report validates local alert rule presence and sample trigger logic.",
        "It does not prove Prometheus rule evaluation or Alertmanager delivery.",
        "Full P0-D acceptance still requires live alert delivery evidence or explicit owner deferral."
    )
}

$Report | ConvertTo-Json -Depth 12 | Set-Content -Path $JsonPath -Encoding utf8

$CheckLines = $Checks | ForEach-Object {
    "- $($_.name): $($_.status) ($($_.actual))"
}

$Markdown = @(
    "# Alert Drill Smoke Report",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- Alert rules: ``$AlertRulesPath``",
    "- Raw log: ``$RawLogPath``",
    "- JSON report: ``$JsonPath``",
    "",
    "## Checks",
    "",
    $CheckLines,
    "",
    "## Acceptance note",
    "",
    "This smoke validates rule presence and sample trigger logic. It does not replace live Alertmanager delivery evidence.",
    "",
    "## Result",
    "",
    "- Exit code: $ExitCode",
    "- Result: $Status"
)

$Markdown | Set-Content -Path $MarkdownPath -Encoding utf8

Write-Host "Alert drill smoke status: $Status"
Write-Host "Raw log written: $RawLogPath"
Write-Host "Markdown report written: $MarkdownPath"
Write-Host "JSON report written: $JsonPath"

exit $ExitCode
