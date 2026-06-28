param(
    [string]$ReportDir = $env:STRUCTURED_LOGGING_REPORT_DIR
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
$RunId = "structured_logging_$Timestamp"
$SlowLogPath = Join-Path $ReportDir "$RunId.slow.log"
$ErrorLogPath = Join-Path $ReportDir "$RunId.error.log"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"
$RawLogPath = Join-Path $ReportDir "$RunId.raw.log"

$RequiredSlowFields = @("timestamp", "trace_id", "connection_id", "transaction_id", "database", "sql_digest", "duration_ms", "rows_examined", "rows_returned", "error_code", "status")
$RequiredErrorFields = @("timestamp", "level", "trace_id", "component", "operation", "error_code", "error_class", "message", "duration_ms", "connection_id", "transaction_id")

@(
    "=== structured logging smoke $StartedAt ===",
    "repo: $RepoRoot",
    "command: go run ./cmd/p0_structured_log_export -slow-output $SlowLogPath -error-output $ErrorLogPath",
    ""
) | Set-Content -Path $RawLogPath -Encoding utf8

& go run ./cmd/p0_structured_log_export -slow-output $SlowLogPath -error-output $ErrorLogPath 2>&1 | Tee-Object -FilePath $RawLogPath -Append
$ExitCode = $LASTEXITCODE

$Checks = @()
$SlowText = if (Test-Path -LiteralPath $SlowLogPath) { Get-Content -Raw -LiteralPath $SlowLogPath } else { "" }
$ErrorText = if (Test-Path -LiteralPath $ErrorLogPath) { Get-Content -Raw -LiteralPath $ErrorLogPath } else { "" }

foreach ($Field in $RequiredSlowFields) {
    $Present = $SlowText.Contains("""$Field""")
    $Checks += [PSCustomObject]@{
        name = "slow_query_field_$Field"
        expected = "$Field appears in generated slow query log"
        actual = if ($Present) { "present" } else { "missing" }
        status = if ($Present) { "PASS" } else { "FAIL" }
    }
}

foreach ($Field in $RequiredErrorFields) {
    $Present = $ErrorText.Contains("""$Field""")
    $Checks += [PSCustomObject]@{
        name = "error_log_field_$Field"
        expected = "$Field appears in generated error log"
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
    evidence_type = "structured_logging_smoke"
    repository = "$RepoRoot"
    command = "go run ./cmd/p0_structured_log_export -slow-output $SlowLogPath -error-output $ErrorLogPath"
    slow_query_log = "$SlowLogPath"
    error_log = "$ErrorLogPath"
    raw_log = "$RawLogPath"
    markdown_report = "$MarkdownPath"
    exit_code = $ExitCode
    checks = $Checks
    limitations = @(
        "This report proves the structured logging package can generate P0-D JSON-line log records.",
        "It uses sample events from cmd/p0_structured_log_export.",
        "It does not prove live query or engine error paths are wired to structured logging yet."
    )
}

$Report | ConvertTo-Json -Depth 12 | Set-Content -Path $JsonPath -Encoding utf8

$CheckLines = $Checks | ForEach-Object {
    "- $($_.name): $($_.status) ($($_.actual))"
}

$Markdown = @(
    "# Structured Logging Smoke Report",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- Slow query log: ``$SlowLogPath``",
    "- Error log: ``$ErrorLogPath``",
    "- Raw log: ``$RawLogPath``",
    "- JSON report: ``$JsonPath``",
    "",
    "## Checks",
    "",
    $CheckLines,
    "",
    "## Acceptance note",
    "",
    "This report proves structured JSON-line log generation from the logging package. It does not prove live runtime integration yet.",
    "",
    "## Result",
    "",
    "- Exit code: $ExitCode",
    "- Result: $Status"
)

$Markdown | Set-Content -Path $MarkdownPath -Encoding utf8

Write-Host "Structured logging smoke status: $Status"
Write-Host "Slow query log written: $SlowLogPath"
Write-Host "Error log written: $ErrorLogPath"
Write-Host "Markdown report written: $MarkdownPath"
Write-Host "JSON report written: $JsonPath"

if ($Status -eq "PASS") {
    exit 0
}

exit 1
