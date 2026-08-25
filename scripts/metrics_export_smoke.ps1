param(
    [string]$ReportDir = $env:METRICS_EXPORT_REPORT_DIR
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
$RunId = "metrics_export_$Timestamp"
$PromPath = Join-Path $ReportDir "$RunId.prom"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"
$LogPath = Join-Path $ReportDir "$RunId.log"

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

@(
    "=== metrics export smoke $StartedAt ===",
    "repo: $RepoRoot",
    "command: go run ./cmd/p0_metrics_export -output $PromPath",
    ""
) | Set-Content -Path $LogPath -Encoding utf8

& go run ./cmd/p0_metrics_export -output $PromPath 2>&1 | Tee-Object -FilePath $LogPath -Append
$ExitCode = $LASTEXITCODE

$Checks = @()
$PromText = if (Test-Path -LiteralPath $PromPath) { Get-Content -Raw -LiteralPath $PromPath } else { "" }

foreach ($Metric in $RequiredMetrics) {
    $Present = $PromText.Contains($Metric)
    $Checks += [PSCustomObject]@{
        name = "metric_$Metric"
        expected = "$Metric appears in Prometheus export"
        actual = if ($Present) { "present" } else { "missing" }
        status = if ($Present) { "PASS" } else { "FAIL" }
    }
}

$FailedChecks = @($Checks | Where-Object { $_.status -ne "PASS" })
$FinishedAt = Get-Date -Format "o"
$Status = if ($ExitCode -eq 0 -and $FailedChecks.Count -eq 0) { "PASS" } else { "FAIL" }

$Report = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = $Status
    evidence_type = "metrics_export_smoke"
    repository = "$RepoRoot"
    command = "go run ./cmd/p0_metrics_export -output $PromPath"
    prometheus_textfile = "$PromPath"
    raw_log = "$LogPath"
    markdown_report = "$MarkdownPath"
    exit_code = $ExitCode
    checks = $Checks
    limitations = @(
        "This report proves the metrics registry can produce Prometheus textfile output.",
        "It uses sample metric observations from cmd/p0_metrics_export.",
        "It does not prove runtime server paths are wired to live metrics yet."
    )
}

$Report | ConvertTo-Json -Depth 12 | Set-Content -Path $JsonPath -Encoding utf8

$CheckLines = $Checks | ForEach-Object {
    "- $($_.name): $($_.status) ($($_.actual))"
}

$Markdown = @(
    "# Metrics Export Smoke Report",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- Prometheus textfile: ``$PromPath``",
    "- Raw log: ``$LogPath``",
    "- JSON report: ``$JsonPath``",
    "",
    "## Checks",
    "",
    $CheckLines,
    "",
    "## Acceptance note",
    "",
    "This report proves Prometheus textfile generation from the metrics registry. It does not prove live runtime integration yet.",
    "",
    "## Result",
    "",
    "- Exit code: $ExitCode",
    "- Result: $Status"
)

$Markdown | Set-Content -Path $MarkdownPath -Encoding utf8

Write-Host "Metrics export smoke status: $Status"
Write-Host "Prometheus textfile written: $PromPath"
Write-Host "Raw log written: $LogPath"
Write-Host "Markdown report written: $MarkdownPath"
Write-Host "JSON report written: $JsonPath"

if ($Status -eq "PASS") {
    exit 0
}

exit 1
