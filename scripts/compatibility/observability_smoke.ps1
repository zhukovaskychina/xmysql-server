param(
    [string]$ReportDir = "reports/compatibility/observability"
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null
$started = Get-Date
$output = & go test ./server/observability/metrics ./server/innodb/engine -run "Test.*PerformanceSchema|Test.*Metrics|Test.*Slow" -count=1 2>&1
$exitCode = $LASTEXITCODE
$report = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    started_at = $started.ToUniversalTime().ToString("o")
    git_revision = (git rev-parse HEAD)
    command = "go test ./server/observability/metrics ./server/innodb/engine -run Test.*PerformanceSchema|Test.*Metrics|Test.*Slow -count=1"
    exit_code = $exitCode
    output_tail = (($output | Select-Object -Last 80) -join "`n")
    status = if ($exitCode -eq 0) { "PASS" } else { "FAIL" }
}
$report | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $ReportDir "observability.json")
$report.status
if ($exitCode -ne 0) { exit $exitCode }
