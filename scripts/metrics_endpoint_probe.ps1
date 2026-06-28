param(
    [string]$ReportDir = $env:METRICS_ENDPOINT_REPORT_DIR,
    [string]$MetricsUrl = $env:P0_METRICS_URL,
    [int]$TimeoutSeconds = 10
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RepoRoot

if ([string]::IsNullOrWhiteSpace($ReportDir)) {
    $ReportDir = Join-Path $RepoRoot "reports"
}

if ($TimeoutSeconds -lt 1) {
    throw "TimeoutSeconds must be greater than or equal to 1."
}

New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$StartedAt = Get-Date -Format "o"
$RunId = "metrics_endpoint_probe_$Timestamp"
$PromPath = Join-Path $ReportDir "$RunId.prom"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"
$RawLogPath = Join-Path $ReportDir "$RunId.log"

$RequiredMetrics = @(
    "xmysql_queries_total",
    "xmysql_query_errors_total",
    "xmysql_query_latency_ms",
    "xmysql_connections_active",
    "xmysql_transactions_active",
    "xmysql_lock_waits_total",
    "xmysql_recovery_runs_total",
    "xmysql_checkpoint_dirty_pages"
)

@(
    "=== metrics endpoint probe $StartedAt ===",
    "repo: $RepoRoot",
    "metrics_url: $MetricsUrl",
    "timeout_seconds: $TimeoutSeconds",
    ""
) | Set-Content -Path $RawLogPath -Encoding utf8

$Checks = @()
$ProbeStatus = "FAIL"
$ProbeError = ""
$Content = ""

if ([string]::IsNullOrWhiteSpace($MetricsUrl)) {
    $ProbeError = "MetricsUrl is required. Pass -MetricsUrl or set P0_METRICS_URL."
} else {
    try {
        $Response = Invoke-WebRequest -Uri $MetricsUrl -TimeoutSec $TimeoutSeconds -UseBasicParsing
        $Content = [string]$Response.Content
        $Content | Set-Content -Path $PromPath -Encoding utf8
        $ProbeStatus = "PASS"
    } catch {
        $ProbeError = $_.Exception.Message
    }
}

if ($ProbeError) {
    "probe_error: $ProbeError" | Add-Content -Path $RawLogPath -Encoding utf8
}

$Checks += [PSCustomObject]@{
    name = "metrics_endpoint_reachable"
    expected = "metrics endpoint returns Prometheus text"
    actual = if ($ProbeStatus -eq "PASS") { "reachable" } else { $ProbeError }
    status = $ProbeStatus
}

foreach ($Metric in $RequiredMetrics) {
    $Present = $Content.Contains($Metric)
    $Checks += [PSCustomObject]@{
        name = "metric_$Metric"
        expected = "$Metric appears in endpoint output"
        actual = if ($Present) { "present" } else { "missing" }
        status = if ($Present) { "PASS" } else { "FAIL" }
    }
}

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
    evidence_type = "metrics_endpoint_probe"
    repository = "$RepoRoot"
    metrics_url = $MetricsUrl
    prometheus_textfile = "$PromPath"
    raw_log = "$RawLogPath"
    markdown_report = "$MarkdownPath"
    checks = $Checks
    limitations = @(
        "This report proves that a running metrics endpoint can be scraped from the provided URL.",
        "It does not start the XMySQL server.",
        "It does not prove all runtime paths update metric values unless traffic was exercised before the probe."
    )
}

$Report | ConvertTo-Json -Depth 12 | Set-Content -Path $JsonPath -Encoding utf8

$CheckLines = $Checks | ForEach-Object {
    "- $($_.name): $($_.status) ($($_.actual))"
}

$Markdown = @(
    "# Metrics Endpoint Probe Report",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- Metrics URL: ``$MetricsUrl``",
    "- Prometheus textfile: ``$PromPath``",
    "- Raw log: ``$RawLogPath``",
    "- JSON report: ``$JsonPath``",
    "",
    "## Checks",
    "",
    $CheckLines,
    "",
    "## Acceptance note",
    "",
    "This probe proves endpoint reachability and metric-name presence for a provided live URL. It does not start the server or generate traffic.",
    "",
    "## Result",
    "",
    "- Exit code: $ExitCode",
    "- Result: $Status"
)

$Markdown | Set-Content -Path $MarkdownPath -Encoding utf8

Write-Host "Metrics endpoint probe status: $Status"
Write-Host "Prometheus textfile written: $PromPath"
Write-Host "Raw log written: $RawLogPath"
Write-Host "Markdown report written: $MarkdownPath"
Write-Host "JSON report written: $JsonPath"

exit $ExitCode
