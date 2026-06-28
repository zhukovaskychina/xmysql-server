param(
    [string]$ReportDir = $env:P0_STATE_SNAPSHOT_REPORT_DIR,
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
$RunId = "p0b_state_snapshot_evidence_$Timestamp"
$SnapshotDir = Join-Path $ReportDir $RunId
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"
$LogPath = Join-Path $ReportDir "$RunId.log"

New-Item -ItemType Directory -Force -Path $SnapshotDir | Out-Null

$env:P0B_STATE_SNAPSHOT_DIR = $SnapshotDir

try {
    & go test ./server/innodb/manager -run TestP0BStateSnapshotExport -count=1 -timeout "$($TimeoutSeconds)s" 2>&1 |
        Tee-Object -FilePath $LogPath
    $GoTestExitCode = $LASTEXITCODE
}
finally {
    Remove-Item Env:\P0B_STATE_SNAPSHOT_DIR -ErrorAction SilentlyContinue
}

if ($GoTestExitCode -ne 0) {
    throw "P0-B state snapshot export test failed with exit code $GoTestExitCode. See $LogPath."
}

$ExpectedRows = Join-Path $SnapshotDir "expected_rows.json"
$ActualRows = Join-Path $SnapshotDir "actual_rows.json"
$ExpectedPages = Join-Path $SnapshotDir "expected_pages.json"
$ActualPages = Join-Path $SnapshotDir "actual_pages.json"
$ExpectedWal = Join-Path $SnapshotDir "expected_wal.json"
$ActualWal = Join-Path $SnapshotDir "actual_wal.json"

$RowDiffJson = Join-Path $ReportDir "$RunId.row_state_diff.json"
$PageDiffJson = Join-Path $ReportDir "$RunId.page_state_diff.json"
$WalDiffJson = Join-Path $ReportDir "$RunId.wal_replay_diff.json"

& (Join-Path $ScriptDir "generate_p0b_state_diff_artifact.ps1") -Scope row_state_diff -ExpectedSnapshotJson $ExpectedRows -ActualSnapshotJson $ActualRows -Scenario redo_idempotent_page_replay -OutJson $RowDiffJson | Tee-Object -FilePath $LogPath -Append
if ($LASTEXITCODE -ne 0) { throw "Row state diff artifact failed. See $RowDiffJson." }
& (Join-Path $ScriptDir "verify_p0b_state_diff_artifact.ps1") -Path $RowDiffJson | Tee-Object -FilePath $LogPath -Append

& (Join-Path $ScriptDir "generate_p0b_state_diff_artifact.ps1") -Scope page_state_diff -ExpectedSnapshotJson $ExpectedPages -ActualSnapshotJson $ActualPages -Scenario redo_idempotent_page_replay -OutJson $PageDiffJson | Tee-Object -FilePath $LogPath -Append
if ($LASTEXITCODE -ne 0) { throw "Page state diff artifact failed. See $PageDiffJson." }
& (Join-Path $ScriptDir "verify_p0b_state_diff_artifact.ps1") -Path $PageDiffJson | Tee-Object -FilePath $LogPath -Append

& (Join-Path $ScriptDir "generate_p0b_state_diff_artifact.ps1") -Scope wal_replay_diff -ExpectedSnapshotJson $ExpectedWal -ActualSnapshotJson $ActualWal -Scenario redo_idempotent_page_replay -OutJson $WalDiffJson | Tee-Object -FilePath $LogPath -Append
if ($LASTEXITCODE -ne 0) { throw "WAL replay diff artifact failed. See $WalDiffJson." }
& (Join-Path $ScriptDir "verify_p0b_state_diff_artifact.ps1") -Path $WalDiffJson | Tee-Object -FilePath $LogPath -Append

$FinishedAt = Get-Date -Format "o"
$Report = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = "PASS"
    evidence_type = "p0b_state_snapshot_evidence"
    repository = "$RepoRoot"
    snapshot_dir = $SnapshotDir
    raw_log = $LogPath
    row_state_diff_json = $RowDiffJson
    page_state_diff_json = $PageDiffJson
    wal_replay_diff_json = $WalDiffJson
    snapshots = [ordered]@{
        expected_rows = $ExpectedRows
        actual_rows = $ActualRows
        expected_pages = $ExpectedPages
        actual_pages = $ActualPages
        expected_wal = $ExpectedWal
        actual_wal = $ActualWal
    }
    limitations = @(
        "This evidence is generated from a focused Go test-side recovery snapshot exporter.",
        "It exercises deterministic redo page replay and idempotent LSN behavior through existing CrashRecovery and mock buffer pool components.",
        "It is not a substitute for a full disk-format crash/restart integration drill."
    )
}

$Report | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $JsonPath -Encoding utf8

$Markdown = @(
    "# P0-B State Snapshot Evidence",
    "",
    "## Summary",
    "",
    "- Status: PASS",
    "- Generated at: $FinishedAt",
    "- Snapshot dir: ``$SnapshotDir``",
    "- Raw log: ``$LogPath``",
    "- Row state diff JSON: ``$RowDiffJson``",
    "- Page state diff JSON: ``$PageDiffJson``",
    "- WAL replay diff JSON: ``$WalDiffJson``",
    "- JSON report: ``$JsonPath``",
    "",
    "## Use with crash recovery drill",
    "",
    "````powershell",
    "./scripts/crash_recovery_drill.ps1 -Runs 3 -StateEvidence -RowStateDiffJson `"$RowDiffJson`" -PageStateDiffJson `"$PageDiffJson`" -WalReplayDiffJson `"$WalDiffJson`"",
    "````"
)

$Markdown | Set-Content -LiteralPath $MarkdownPath -Encoding utf8

Write-Host "P0-B state snapshot evidence status: PASS"
Write-Host "JSON report written: $JsonPath"
Write-Host "Markdown report written: $MarkdownPath"
