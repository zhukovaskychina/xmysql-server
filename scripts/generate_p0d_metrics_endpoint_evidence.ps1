param(
    [string]$ReportDir = $env:P0D_METRICS_ENDPOINT_REPORT_DIR,
    [string]$Addr = "127.0.0.1:18080",
    [int]$StartupTimeoutSeconds = 20,
    [int]$ProbeTimeoutSeconds = 10
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RepoRoot

if ([string]::IsNullOrWhiteSpace($ReportDir)) {
    $ReportDir = Join-Path $RepoRoot "reports"
}

if ($StartupTimeoutSeconds -lt 1) {
    throw "StartupTimeoutSeconds must be greater than or equal to 1."
}

if ($ProbeTimeoutSeconds -lt 1) {
    throw "ProbeTimeoutSeconds must be greater than or equal to 1."
}

New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$RunId = "p0d_metrics_endpoint_evidence_$Timestamp"
$ServerStdoutPath = Join-Path $ReportDir "$RunId.server.stdout.log"
$ServerStderrPath = Join-Path $ReportDir "$RunId.server.stderr.log"
$EvidenceJsonPath = Join-Path $ReportDir "$RunId.json"
$EvidenceMarkdownPath = Join-Path $ReportDir "$RunId.md"
$MetricsUrl = "http://$Addr/metrics"
$HealthUrl = "http://$Addr/healthz"

$Process = $null
$ProbeJson = ""
$ProbeMarkdown = ""

try {
    $Process = Start-Process `
        -FilePath "go" `
        -ArgumentList @("run", "./cmd/p0_metrics_endpoint", "-addr", $Addr) `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $ServerStdoutPath `
        -RedirectStandardError $ServerStderrPath `
        -WindowStyle Hidden `
        -PassThru

    $Deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    $Ready = $false
    while ((Get-Date) -lt $Deadline) {
        if ($Process.HasExited) {
            throw "Focused metrics endpoint exited before readiness. See $ServerStderrPath."
        }

        try {
            $Response = Invoke-WebRequest -Uri $HealthUrl -TimeoutSec 2 -UseBasicParsing
            if ([int]$Response.StatusCode -eq 200) {
                $Ready = $true
                break
            }
        }
        catch {
            Start-Sleep -Milliseconds 500
        }
    }

    if (-not $Ready) {
        throw "Focused metrics endpoint was not ready within $StartupTimeoutSeconds seconds at $HealthUrl."
    }

    & (Join-Path $ScriptDir "metrics_endpoint_probe.ps1") `
        -ReportDir $ReportDir `
        -MetricsUrl $MetricsUrl `
        -TimeoutSeconds $ProbeTimeoutSeconds

    if ($LASTEXITCODE -ne 0) {
        throw "metrics_endpoint_probe.ps1 failed with exit code $LASTEXITCODE."
    }

    $Probe = Get-ChildItem -LiteralPath $ReportDir -Filter "metrics_endpoint_probe_*.json" |
        Where-Object { $_.LastWriteTime -ge (Get-Date).AddMinutes(-10) } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if (-not $Probe) {
        throw "metrics endpoint probe JSON was not found after probe execution."
    }

    $ProbeJson = $Probe.FullName
    $ProbeMarkdown = [System.IO.Path]::ChangeExtension($ProbeJson, ".md")

    & (Join-Path $ScriptDir "verify_metrics_endpoint_probe_report.ps1") -Path $ProbeJson
}
finally {
    if ($Process -and -not $Process.HasExited) {
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
    }
}

$FinishedAt = Get-Date -Format "o"
$Evidence = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = "PASS"
    evidence_type = "p0d_metrics_endpoint_evidence"
    repository = "$RepoRoot"
    metrics_url = $MetricsUrl
    health_url = $HealthUrl
    server_stdout_log = $ServerStdoutPath
    server_stderr_log = $ServerStderrPath
    probe_json = $ProbeJson
    probe_markdown = $ProbeMarkdown
    limitations = @(
        "This starts a focused local P0 metrics endpoint and probes /metrics.",
        "It proves endpoint reachability and required metric-name presence for the focused endpoint.",
        "It does not prove the full XMySQL server process updates every metric during production traffic."
    )
}

$Evidence | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $EvidenceJsonPath -Encoding utf8

$Markdown = @(
    "# P0-D Metrics Endpoint Evidence",
    "",
    "## Summary",
    "",
    "- Status: PASS",
    "- Generated at: $FinishedAt",
    "- Metrics URL: ``$MetricsUrl``",
    "- Health URL: ``$HealthUrl``",
    "- Probe JSON: ``$ProbeJson``",
    "- Server stdout log: ``$ServerStdoutPath``",
    "- Server stderr log: ``$ServerStderrPath``",
    "- Evidence JSON: ``$EvidenceJsonPath``",
    "",
    "## Use with delivery audit",
    "",
    "Pass the probe JSON as the P0-D metrics endpoint evidence:",
    "",
    "````powershell",
    "./scripts/run_p0_evidence_suite.ps1 -P0DMetricsEndpointJson `"$ProbeJson`"",
    "````"
)

$Markdown | Set-Content -LiteralPath $EvidenceMarkdownPath -Encoding utf8

Write-Host "P0-D metrics endpoint evidence status: PASS"
Write-Host "Evidence JSON written: $EvidenceJsonPath"
Write-Host "Probe JSON written: $ProbeJson"
