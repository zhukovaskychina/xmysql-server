param(
    [string]$ReportDir = $env:OBSERVABILITY_REPORT_DIR
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
$RunId = "observability_smoke_$Timestamp"
$LogPath = Join-Path $ReportDir "$RunId.log"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"

$RequiredFiles = @(
    [PSCustomObject]@{ Name = "metrics_catalog"; Path = "docs/observability/metrics_catalog.md" },
    [PSCustomObject]@{ Name = "sample_slow_query_log"; Path = "docs/observability/sample_slow_query.log" },
    [PSCustomObject]@{ Name = "sample_error_log"; Path = "docs/observability/sample_error.log" },
    [PSCustomObject]@{ Name = "alert_rules"; Path = "deploy/alerts/xmysql-p0-alerts.yml" },
    [PSCustomObject]@{ Name = "p0d_plan"; Path = "docs/planning/P0_D_OBSERVABILITY_PLAN.md" }
)

$RequiredMetrics = @(
    "xmysql_queries_total",
    "xmysql_query_errors_total",
    "xmysql_query_latency_ms",
    "xmysql_connections_active",
    "xmysql_transactions_active",
    "xmysql_transactions_committed_total",
    "xmysql_transactions_rolled_back_total",
    "xmysql_lock_waits_total",
    "xmysql_lock_wait_duration_ms",
    "xmysql_long_transactions_active",
    "xmysql_recovery_runs_total",
    "xmysql_recovery_failures_total",
    "xmysql_checkpoint_dirty_pages",
    "xmysql_checkpoint_runs_total"
)

$RequiredSlowFields = @("timestamp", "trace_id", "connection_id", "transaction_id", "database", "sql_digest", "duration_ms", "rows_examined", "rows_returned", "error_code", "status")
$RequiredErrorFields = @("timestamp", "level", "trace_id", "component", "operation", "error_code", "error_class", "message", "duration_ms", "connection_id", "transaction_id")
$RequiredAlerts = @("XMySQLHighErrorRate", "XMySQLHighP99Latency", "XMySQLLongTransaction", "XMySQLRecoveryFailure", "XMySQLCheckpointDirtyPagesHigh", "XMySQLLockWaitSpike")

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

"=== observability smoke $StartedAt ===" | Set-Content -Path $LogPath -Encoding utf8
"repo: $RepoRoot" | Add-Content -Path $LogPath -Encoding utf8

foreach ($File in $RequiredFiles) {
    $Exists = Test-Path -LiteralPath $File.Path
    Add-Check -Name "$($File.Name)_exists" -Expected "$($File.Path) exists" -Actual $(if ($Exists) { "exists" } else { "missing" }) -Status $(if ($Exists) { "PASS" } else { "FAIL" })
}

$MetricsCatalog = if (Test-Path -LiteralPath "docs/observability/metrics_catalog.md") { Get-Content -Raw -LiteralPath "docs/observability/metrics_catalog.md" } else { "" }
foreach ($Metric in $RequiredMetrics) {
    $Present = $MetricsCatalog.Contains($Metric)
    Add-Check -Name "metric_$Metric" -Expected "$Metric is documented" -Actual $(if ($Present) { "documented" } else { "missing" }) -Status $(if ($Present) { "PASS" } else { "FAIL" })
}

$SlowLog = if (Test-Path -LiteralPath "docs/observability/sample_slow_query.log") { Get-Content -Raw -LiteralPath "docs/observability/sample_slow_query.log" } else { "" }
foreach ($Field in $RequiredSlowFields) {
    $Present = $SlowLog.Contains("""$Field""")
    Add-Check -Name "slow_query_field_$Field" -Expected "$Field appears in slow query sample" -Actual $(if ($Present) { "present" } else { "missing" }) -Status $(if ($Present) { "PASS" } else { "FAIL" })
}

$ErrorLog = if (Test-Path -LiteralPath "docs/observability/sample_error.log") { Get-Content -Raw -LiteralPath "docs/observability/sample_error.log" } else { "" }
foreach ($Field in $RequiredErrorFields) {
    $Present = $ErrorLog.Contains("""$Field""")
    Add-Check -Name "error_log_field_$Field" -Expected "$Field appears in error log sample" -Actual $(if ($Present) { "present" } else { "missing" }) -Status $(if ($Present) { "PASS" } else { "FAIL" })
}

$AlertRules = if (Test-Path -LiteralPath "deploy/alerts/xmysql-p0-alerts.yml") { Get-Content -Raw -LiteralPath "deploy/alerts/xmysql-p0-alerts.yml" } else { "" }
foreach ($Alert in $RequiredAlerts) {
    $Present = $AlertRules.Contains($Alert)
    Add-Check -Name "alert_$Alert" -Expected "$Alert rule exists" -Actual $(if ($Present) { "present" } else { "missing" }) -Status $(if ($Present) { "PASS" } else { "FAIL" })
}

$FailedChecks = @($Checks | Where-Object { $_.status -ne "PASS" })
$FinishedAt = Get-Date -Format "o"
$Status = if ($FailedChecks.Count -eq 0) { "PASS" } else { "FAIL" }
$ExitCode = if ($Status -eq "PASS") { 0 } else { 1 }

foreach ($Check in $Checks) {
    "$($Check.status) $($Check.name): $($Check.actual)" | Add-Content -Path $LogPath -Encoding utf8
}

$Report = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = $Status
    evidence_type = "observability_smoke"
    repository = "$RepoRoot"
    raw_log = "$LogPath"
    markdown_report = "$MarkdownPath"
    checks = $Checks
    limitations = @(
        "This report verifies P0-D documentation, sample logs, and alert rule artifacts.",
        "It does not prove live metrics export, dashboard integration, or runtime alert delivery.",
        "Full P0-D acceptance still requires production-consumable metric export and alert drill evidence."
    )
}

$Report | ConvertTo-Json -Depth 12 | Set-Content -Path $JsonPath -Encoding utf8

$CheckLines = $Checks | ForEach-Object {
    "- $($_.name): $($_.status) ($($_.actual))"
}

$Markdown = @(
    "# Observability Smoke Report",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- Raw log: ``$LogPath``",
    "- JSON report: ``$JsonPath``",
    "",
    "## Checks",
    "",
    $CheckLines,
    "",
    "## Acceptance note",
    "",
    "This is P0-D documentation/configuration smoke evidence. It does not replace live metrics export or alert delivery evidence.",
    "",
    "## Result",
    "",
    "- Exit code: $ExitCode",
    "- Result: $Status"
)

$Markdown | Set-Content -Path $MarkdownPath -Encoding utf8

Write-Host "Observability smoke status: $Status"
Write-Host "Raw log written: $LogPath"
Write-Host "Markdown report written: $MarkdownPath"
Write-Host "JSON report written: $JsonPath"

exit $ExitCode
