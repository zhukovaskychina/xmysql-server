param(
    [string]$ReportDir = $env:FINAL_REGRESSION_REPORT_DIR,
    [int]$TimeoutSeconds = 600,
    [switch]$VerboseTests
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
$RunId = "final_regression_gate_$Timestamp"
$LogPath = Join-Path $ReportDir "$RunId.log"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"

try {
    $GoVersion = (& go version) 2>$null
} catch {
    $GoVersion = "go version unavailable: $($_.Exception.Message)"
}

$GoArgs = @("test", "./...", "-timeout=$($TimeoutSeconds)s")
if ($VerboseTests) {
    $GoArgs += "-v"
}

@(
    "=== final regression gate $StartedAt ===",
    "repo: $RepoRoot",
    "go: $GoVersion",
    "command: go $($GoArgs -join ' ')",
    ""
) | Set-Content -Path $LogPath -Encoding utf8

& go @GoArgs 2>&1 | Tee-Object -FilePath $LogPath -Append
$ExitCode = $LASTEXITCODE
$FinishedAt = Get-Date -Format "o"
$Status = if ($ExitCode -eq 0) { "PASS" } else { "FAIL" }

$Report = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = $Status
    evidence_type = "final_regression_gate"
    repository = "$RepoRoot"
    go_version = $GoVersion
    command = "go $($GoArgs -join ' ')"
    timeout_seconds = $TimeoutSeconds
    raw_log = "$LogPath"
    markdown_report = "$MarkdownPath"
    exit_code = $ExitCode
    limitations = @(
        "This report records the default Go regression gate.",
        "It does not validate excluded build-tag profiles such as demo or legacy SQL parser conformance.",
        "Production approval still requires P0-B/P0-C/P0-D/P0-E evidence and owner sign-off."
    )
}

$Report | ConvertTo-Json -Depth 8 | Set-Content -Path $JsonPath -Encoding utf8

$Markdown = @(
    "# Final Regression Gate Report",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- Go version: $GoVersion",
    "- Command: ``go $($GoArgs -join ' ')``",
    "- Timeout: $($TimeoutSeconds)s",
    "- Raw log: ``$LogPath``",
    "- JSON report: ``$JsonPath``",
    "",
    "## Acceptance note",
    "",
    "This is the default final regression gate. It does not cover build-tagged demo or legacy conformance profiles.",
    "",
    "## Result",
    "",
    "- Exit code: $ExitCode",
    "- Result: $Status"
)

$Markdown | Set-Content -Path $MarkdownPath -Encoding utf8

Write-Host "Final regression gate status: $Status"
Write-Host "Raw log written: $LogPath"
Write-Host "Markdown report written: $MarkdownPath"
Write-Host "JSON report written: $JsonPath"

exit $ExitCode
