param(
    [string]$ReportDir = "reports/compatibility/crash-recovery",
    [int]$Repeat = 3
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null
$runs = @()
for ($i = 1; $i -le $Repeat; $i++) {
    $started = Get-Date
    & go test ./server/innodb/manager -run "Test(Crash|FullCrash|FaultInjector|Recovery)" -count=1 2>&1 | Tee-Object -Variable output
    $code = $LASTEXITCODE
    $runs += [ordered]@{
        run = $i
        started_at = $started.ToUniversalTime().ToString("o")
        exit_code = $code
        output_tail = (($output | Select-Object -Last 40) -join "`n")
    }
    if ($code -ne 0) { break }
}
$report = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    git_revision = (git rev-parse HEAD)
    runs = $runs
    status = if (($runs | Where-Object { $_.exit_code -ne 0 }).Count -eq 0 -and $runs.Count -eq $Repeat) { "PASS" } else { "FAIL" }
}
$report | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $ReportDir "crash-recovery.json")
$report.status
if ($report.status -ne "PASS") { exit 1 }
