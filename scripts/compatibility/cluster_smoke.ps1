param(
    [string]$ReportDir = "reports/compatibility/p1-cluster-current"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
$absoluteReportDir = Join-Path $repoRoot $ReportDir
New-Item -ItemType Directory -Force -Path $absoluteReportDir | Out-Null

$testPattern = 'TestEngineSourceReplicaReplicates(Committed(DML|DDL)|StoredObjectDefinition)'
$testOutput = & go test ./server/innodb/engine -run $testPattern -count=1 -json 2>&1
$exitCode = $LASTEXITCODE
$logPath = Join-Path $absoluteReportDir "cluster-engine-test.jsonl"
$testOutput | Set-Content -Encoding utf8 $logPath

$passed = $exitCode -eq 0 -and (($testOutput -join "`n") -match '"Action":"pass"')
$report = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    scenario = "one source, two replicas, committed DML/DDL, rollback isolation, replica promotion"
    command = "go test ./server/innodb/engine -run $testPattern -count=1 -json"
    result = if ($passed) { "PASS" } else { "FAIL" }
    exit_code = $exitCode
    evidence = $logPath
}
$report | ConvertTo-Json | Set-Content -Encoding utf8 (Join-Path $absoluteReportDir "cluster-report.json")

if (-not $passed) {
    throw "Cluster smoke test failed. See $logPath"
}

Write-Output (Join-Path $absoluteReportDir "cluster-report.json")
