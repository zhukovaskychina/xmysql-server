param(
    [string]$ReportDir = $env:P0C_CONSISTENCY_REPORT_DIR,
    [int]$TimeoutSeconds = 180
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
$RunId = "p0c_consistency_evidence_$Timestamp"
$EvidenceDir = Join-Path $ReportDir $RunId
$JsonPath = Join-Path $ReportDir "$RunId.json"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$LogPath = Join-Path $ReportDir "$RunId.log"

New-Item -ItemType Directory -Force -Path $EvidenceDir | Out-Null

$env:P0C_CONSISTENCY_EVIDENCE_DIR = $EvidenceDir

try {
    & go test ./server/innodb/manager -run TestP0CConsistencyEvidenceExport -count=1 -timeout "$($TimeoutSeconds)s" 2>&1 |
        Tee-Object -FilePath $LogPath
    $GoTestExitCode = $LASTEXITCODE
}
finally {
    Remove-Item Env:\P0C_CONSISTENCY_EVIDENCE_DIR -ErrorAction SilentlyContinue
}

if ($GoTestExitCode -ne 0) {
    throw "P0-C consistency evidence export test failed with exit code $GoTestExitCode. See $LogPath."
}

$GeneratedEvidence = Join-Path $EvidenceDir "p0c_consistency_evidence.json"
if (-not (Test-Path -LiteralPath $GeneratedEvidence)) {
    throw "Expected generated P0-C consistency evidence was not found: $GeneratedEvidence"
}

Copy-Item -LiteralPath $GeneratedEvidence -Destination $JsonPath -Force

& (Join-Path $ScriptDir "verify_p0c_consistency_evidence.ps1") -Path $JsonPath | Tee-Object -FilePath $LogPath -Append

$Evidence = Get-Content -Raw -LiteralPath $JsonPath | ConvertFrom-Json
$CheckLines = @($Evidence.checks | ForEach-Object {
    "- $($_.name): $($_.status) ($($_.actual))"
})

$Markdown = @(
    "# P0-C Consistency Evidence",
    "",
    "## Summary",
    "",
    "- Status: $($Evidence.status)",
    "- Generated at: $($Evidence.generated_at)",
    "- Scenario: $($Evidence.scenario)",
    "- Evidence JSON: ``$JsonPath``",
    "- Raw log: ``$LogPath``",
    "",
    "## Checks",
    "",
    $CheckLines,
    "",
    "## Use with concurrency validation",
    "",
    "````powershell",
    "./scripts/concurrency_validation.ps1 -P0CConsistencyEvidenceJson `"$JsonPath`"",
    "````"
)

$Markdown | Set-Content -LiteralPath $MarkdownPath -Encoding utf8

Write-Host "P0-C consistency evidence status: $($Evidence.status)"
Write-Host "JSON report written: $JsonPath"
Write-Host "Markdown report written: $MarkdownPath"
