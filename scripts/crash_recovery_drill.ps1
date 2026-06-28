param(
    [string]$ReportDir = $env:CR_REPORT_DIR,
    [string]$RunPattern = "TestTXN001|TestCrashRecovery|TestRedo|TestUndoRollback|TestSavepoint",
    [int]$TimeoutSeconds = 180,
    [int]$Runs = 1,
    [switch]$StateEvidence,
    [string]$RowStateDiffJson = "",
    [string]$PageStateDiffJson = "",
    [string]$WalReplayDiffJson = "",
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

if ($Runs -lt 1) {
    throw "Runs must be greater than or equal to 1."
}

New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$StartedAt = Get-Date -Format "o"
$LogPath = Join-Path $ReportDir "crash_recovery_drill_$Timestamp.log"
$MarkdownPath = Join-Path $ReportDir "crash_recovery_drill_$Timestamp.md"
$StatePath = Join-Path $ReportDir "crash_recovery_drill_$Timestamp.state.json"

$Package = "./server/innodb/manager"
$GoArgs = @(
    "test",
    $Package,
    "-run",
    $RunPattern,
    "-count=1",
    "-timeout=$($TimeoutSeconds)s"
)

if ($VerboseTests) {
    $GoArgs += "-v"
}

try {
    $GoVersion = (& go version) 2>$null
} catch {
    $GoVersion = "go version unavailable: $($_.Exception.Message)"
}

$ScenarioMatrix = @(
    [ordered]@{
        id = "txn_lifecycle_replay"
        selector = "TestTXN001"
        proof_type = "test_backed_command_replay"
        expected = "transaction lifecycle and recovery statistics remain valid across replay"
        state_layers = @("transaction", "recovery_statistics")
    },
    [ordered]@{
        id = "redo_replay_boundary"
        selector = "TestRedo*"
        proof_type = "test_backed_command_replay"
        expected = "redo entries are replayed idempotently and respect LSN boundaries"
        state_layers = @("redo_log", "page_lsn")
    },
    [ordered]@{
        id = "undo_rollback_recovery"
        selector = "TestUndoRollback*"
        proof_type = "test_backed_command_replay"
        expected = "undo rollback and CLR behavior remain valid after replay"
        state_layers = @("undo_log", "record_state")
    },
    [ordered]@{
        id = "interrupted_commit_recovery"
        selector = "TestCrashRecovery*"
        proof_type = "test_backed_command_replay"
        expected = "analysis, redo, and undo phases recover committed work and roll back incomplete work"
        state_layers = @("recovery_phase", "active_transaction_table", "undo_log")
    },
    [ordered]@{
        id = "savepoint_partial_rollback"
        selector = "TestSavepoint*"
        proof_type = "test_backed_command_replay"
        expected = "savepoint rollback preserves pre-savepoint state and removes post-savepoint changes"
        state_layers = @("savepoint", "undo_log", "record_state")
    }
)

function New-ExternalStateEvidenceCheck {
    param(
        [string]$Path,
        [string]$Expected,
        [string]$MissingActual,
        [string]$CommandReplayStatus
    )

    if ([string]::IsNullOrWhiteSpace($Path)) {
        return [ordered]@{
            expected = $Expected
            actual = $MissingActual
            status = "NOT_VERIFIED"
            artifact = ""
        }
    }

    if (-not (Test-Path -LiteralPath $Path)) {
        return [ordered]@{
            expected = $Expected
            actual = "configured artifact is missing: $Path"
            status = "FAIL"
            artifact = $Path
        }
    }

    try {
        $Artifact = Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
        $ArtifactStatus = [string]$Artifact.status
        if ($ArtifactStatus -notin @("PASS", "FAIL")) {
            $ArtifactStatus = "FAIL"
        }

        $FinalStatus = if ($CommandReplayStatus -eq "PASS" -and $ArtifactStatus -eq "PASS") { "PASS" } else { "FAIL" }
        return [ordered]@{
            expected = $Expected
            actual = "artifact=$Path; artifact_status=$ArtifactStatus; command_replay_status=$CommandReplayStatus"
            status = $FinalStatus
            artifact = $Path
        }
    }
    catch {
        return [ordered]@{
            expected = $Expected
            actual = "artifact could not be parsed: $Path; error=$($_.Exception.Message)"
            status = "FAIL"
            artifact = $Path
        }
    }
}

$Header = @(
    "=== crash recovery drill $StartedAt ===",
    "repo: $RepoRoot",
    "go: $GoVersion",
    "package: $Package",
    "pattern: $RunPattern",
    "timeout: $($TimeoutSeconds)s",
    "runs: $Runs",
    "",
    "--- go $($GoArgs -join ' ') ---"
)

$Header | Set-Content -Path $LogPath -Encoding utf8

$RunResults = @()
$ExitCode = 0

for ($Run = 1; $Run -le $Runs; $Run++) {
    $RunStartedAt = Get-Date -Format "o"
    @(
        "",
        "=== replay run $Run/$Runs started at $RunStartedAt ==="
    ) | Add-Content -Path $LogPath -Encoding utf8

    & go @GoArgs 2>&1 | Tee-Object -FilePath $LogPath -Append
    $RunExitCode = $LASTEXITCODE
    $RunFinishedAt = Get-Date -Format "o"
    $RunStatus = if ($RunExitCode -eq 0) { "PASS" } else { "FAIL" }

    @(
        "=== replay run $Run/$Runs finished at ${RunFinishedAt}: $RunStatus (exit code $RunExitCode) ==="
    ) | Add-Content -Path $LogPath -Encoding utf8

    $RunResults += [PSCustomObject]@{
        Run = $Run
        Status = $RunStatus
        ExitCode = $RunExitCode
        StartedAt = $RunStartedAt
        FinishedAt = $RunFinishedAt
    }

    if ($RunExitCode -ne 0 -and $ExitCode -eq 0) {
        $ExitCode = $RunExitCode
    }
}

$FinishedAt = Get-Date -Format "o"
$Status = if ($ExitCode -eq 0) { "PASS" } else { "FAIL" }
$RunResultLines = $RunResults | ForEach-Object {
    "- Run $($_.Run): $($_.Status) (exit code $($_.ExitCode), started $($_.StartedAt), finished $($_.FinishedAt))"
}
$StateEvidenceLine = if ($StateEvidence) { "- State evidence: ``$StatePath``" } else { "- State evidence: not requested" }
$ScenarioMatrixLines = $ScenarioMatrix | ForEach-Object {
    "- $($_.id): selector ``$($_.selector)``; proof ``$($_.proof_type)``; expected $($_.expected)"
}

if ($StateEvidence) {
    $StateRuns = @($RunResults | ForEach-Object {
        $StateEvidenceStatus = if ($_.Status -eq "PASS") { "PASS" } else { "FAIL" }
        $WalReplayEvidence = New-ExternalStateEvidenceCheck `
            -Path $WalReplayDiffJson `
            -Expected "redo replay respects LSN ordering, idempotency, and persisted WAL replay boundary evidence" `
            -MissingActual "TestRedo* and TestTXN001* selectors completed in this replay run; no standalone WAL replay diff artifact was provided" `
            -CommandReplayStatus $StateEvidenceStatus
        if ([string]::IsNullOrWhiteSpace($WalReplayDiffJson)) {
            $WalReplayEvidence.status = $StateEvidenceStatus
        }

        $RowStateEvidence = New-ExternalStateEvidenceCheck `
            -Path $RowStateDiffJson `
            -Expected "expected row set equals actual recovered row set" `
            -MissingActual "no standalone row snapshot diff artifact was provided to this drill" `
            -CommandReplayStatus $StateEvidenceStatus

        $PageStateEvidence = New-ExternalStateEvidenceCheck `
            -Path $PageStateDiffJson `
            -Expected "expected page contents and LSNs equal actual recovered page contents and LSNs" `
            -MissingActual "no standalone page snapshot diff artifact was provided to this drill" `
            -CommandReplayStatus $StateEvidenceStatus

        [ordered]@{
            run = $_.Run
            status = $_.Status
            exit_code = $_.ExitCode
            started_at = $_.StartedAt
            finished_at = $_.FinishedAt
            state_evidence = [ordered]@{
                verification_level = "test_backed_command_replay"
                expected_final_state = "Committed redo-backed state is recoverable; incomplete or rolled-back work is not visible after recovery selectors complete."
                actual_observation = "go test selector '$RunPattern' completed with exit code $($_.ExitCode)"
                wal_replay_boundary = $WalReplayEvidence
                interrupted_commit_recovery = [ordered]@{
                    expected = "incomplete transactions are recovered through analysis/redo/undo phase handling"
                    actual = "TestCrashRecovery* selectors completed in this replay run"
                    status = $StateEvidenceStatus
                }
                row_level_state_diff = $RowStateEvidence
                page_level_state_diff = $PageStateEvidence
            }
            checks = @(
                [ordered]@{
                    name = "transaction_lifecycle_baseline"
                    expected = "focused transaction lifecycle tests pass during replay"
                    actual = "go test exit code $($_.ExitCode)"
                    status = $_.Status
                },
                [ordered]@{
                    name = "crash_recovery_command_replay"
                    expected = "focused crash recovery tests pass during replay"
                    actual = "go test exit code $($_.ExitCode)"
                    status = $_.Status
                },
                [ordered]@{
                    name = "redo_undo_savepoint_command_replay"
                    expected = "focused redo, undo, and savepoint tests pass during replay"
                    actual = "go test exit code $($_.ExitCode)"
                    status = $_.Status
                }
            )
        }
    })

    $StateEvidenceDocument = [ordered]@{
        generated_at = $FinishedAt
        run_id = "crash_recovery_drill_$Timestamp"
        status = $Status
        evidence_type = "command_replay"
        verification_level = "test_backed_recovery_state_contract"
        package = $Package
        test_pattern = $RunPattern
        timeout_seconds = $TimeoutSeconds
        raw_log = $LogPath
        markdown_report = $MarkdownPath
        state_diff_artifacts = [ordered]@{
            row_state_diff_json = $RowStateDiffJson
            page_state_diff_json = $PageStateDiffJson
            wal_replay_diff_json = $WalReplayDiffJson
        }
        scenario_matrix = $ScenarioMatrix
        limitations = @(
            "This file records command-level replay evidence.",
            "WAL/recovery-phase/undo/redo/savepoint expectations are backed by focused Go test selectors.",
            "Standalone row/page/WAL diff artifacts must be supplied through -RowStateDiffJson, -PageStateDiffJson, and -WalReplayDiffJson when available.",
            "Full P0-B acceptance requires row-level and page-level state-diff checks to be PASS instead of NOT_VERIFIED."
        )
        runs = $StateRuns
    }

    $StateEvidenceDocument | ConvertTo-Json -Depth 12 | Set-Content -Path $StatePath -Encoding utf8
}

$Markdown = @(
    "# Crash Recovery Drill Report",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- Go version: $GoVersion",
    "- Package: $Package",
    "- Test pattern: ``$RunPattern``",
    "- Timeout: $($TimeoutSeconds)s",
    "- Runs: $Runs",
    "- Row state diff JSON: ``$RowStateDiffJson``",
    "- Page state diff JSON: ``$PageStateDiffJson``",
    "- WAL replay diff JSON: ``$WalReplayDiffJson``",
    "- Raw log: ``$LogPath``",
    $StateEvidenceLine,
    "",
    "## Command",
    "",
    "````powershell",
    "go $($GoArgs -join ' ')",
    "````",
    "",
    "## Replay results",
    "",
    $RunResultLines,
    "",
    "## Scenario coverage represented by this drill",
    "",
    "- Transaction lifecycle baseline: ``TestTXN001``",
    "- Crash recovery tests: ``TestCrashRecovery*``",
    "- Redo recovery tests: ``TestRedo*``",
    "- Undo rollback tests: ``TestUndoRollback*``",
    "- Savepoint recovery behavior: ``TestSavepoint*``",
    "",
    "## Structured state evidence contract",
    "",
    $ScenarioMatrixLines,
    "",
    "State evidence JSON records WAL replay boundary and interrupted-commit recovery as test-backed checks. Row/page snapshot diff checks remain ``NOT_VERIFIED`` unless dedicated diff artifacts are supplied with ``-RowStateDiffJson`` and ``-PageStateDiffJson``.",
    "",
    "## Acceptance note",
    "",
    "This report is P0-B crash-recovery evidence, but it is not the full production acceptance package by itself.",
    "",
    "Remaining evidence required before P0-B can be marked fully accepted:",
    "",
    "- snapshot or state-diff evidence,",
    "- scenario matrix for redo, undo, half-commit, and consistency checks,",
    "- final regression gate using ``go test ./...`` after recovery-related code changes.",
    "",
    "## Result",
    "",
    "- Exit code: $ExitCode",
    "- Result: $Status"
)

$Markdown | Set-Content -Path $MarkdownPath -Encoding utf8

Write-Host "Crash recovery drill status: $Status"
Write-Host "Raw log written: $LogPath"
Write-Host "Markdown report written: $MarkdownPath"

exit $ExitCode
