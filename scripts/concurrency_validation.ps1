param(
    [string]$ReportDir = $env:CONCURRENCY_REPORT_DIR,
    [int]$Runs = 1,
    [int]$TimeoutSeconds = 180,
    [string]$P0CConsistencyEvidenceJson = "",
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
$RunId = "concurrency_validation_$Timestamp"
$LogPath = Join-Path $ReportDir "$RunId.log"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"

try {
    $GoVersion = (& go version) 2>$null
} catch {
    $GoVersion = "go version unavailable: $($_.Exception.Message)"
}

$Scenarios = @(
    [PSCustomObject]@{
        Name = "lock_behavior"
        Package = "./server/innodb/manager"
        Pattern = "TestLockManager_.*|TestGapLock.*|TestNextKeyLock.*|TestLockCompatibilityMatrix|TestExplainLockConflict"
        Expected = "lock compatibility, lock release, gap lock, next-key lock, and deadlock behavior tests pass"
        AssertionChecks = @(
            [PSCustomObject]@{ Name = "lock_compatibility_matrix"; Expected = "incompatible locks are rejected and compatible locks are allowed"; Evidence = "TestLockCompatibilityMatrix and lock manager selectors"; Coverage = "test_backed" },
            [PSCustomObject]@{ Name = "gap_next_key_lock_behavior"; Expected = "gap and next-key lock behavior follows scenario expectations"; Evidence = "TestGapLock* and TestNextKeyLock* selectors"; Coverage = "test_backed" },
            [PSCustomObject]@{ Name = "deadlock_behavior"; Expected = "deadlock/conflict behavior remains bounded by focused tests"; Evidence = "TestExplainLockConflict selector"; Coverage = "test_backed" }
        )
        ConsistencyGaps = @(
            [PSCustomObject]@{ Name = "lock_wait_metrics_summary"; Expected = "lock wait count, max wait duration, deadlock count, and victim behavior are measured"; RequiredForAcceptance = $true }
        )
    },
    [PSCustomObject]@{
        Name = "mvcc_isolation"
        Package = "./server/innodb/manager"
        Pattern = "TestMVCC_.*|TestVersionChainConcurrency"
        Expected = "MVCC visibility and concurrent transaction tests pass"
        AssertionChecks = @(
            [PSCustomObject]@{ Name = "mvcc_visibility"; Expected = "read views and version chains preserve visibility rules"; Evidence = "TestMVCC_* selectors"; Coverage = "test_backed" },
            [PSCustomObject]@{ Name = "version_chain_concurrency"; Expected = "concurrent version chain operations complete without test-detected anomaly"; Evidence = "TestVersionChainConcurrency selector"; Coverage = "test_backed" }
        )
        ConsistencyGaps = @(
            [PSCustomObject]@{ Name = "full_isolation_matrix"; Expected = "explicit dirty-read, non-repeatable-read, phantom, and lost-update matrix is reported"; RequiredForAcceptance = $true }
        )
    },
    [PSCustomObject]@{
        Name = "long_transactions"
        Package = "./server/innodb/manager"
        Pattern = "TestLongTransaction.*|TestConcurrentLongTransactionDetection"
        Expected = "long transaction detection and concurrent long transaction tests pass"
        AssertionChecks = @(
            [PSCustomObject]@{ Name = "long_transaction_detection"; Expected = "long-running transactions are detected by focused tests"; Evidence = "TestLongTransaction* selectors"; Coverage = "test_backed" },
            [PSCustomObject]@{ Name = "concurrent_long_transaction_detection"; Expected = "concurrent long transaction detection remains stable"; Evidence = "TestConcurrentLongTransactionDetection selector"; Coverage = "test_backed" }
        )
        ConsistencyGaps = @(
            [PSCustomObject]@{ Name = "long_transaction_runtime_metrics"; Expected = "runtime transaction age and active transaction metrics are exported"; RequiredForAcceptance = $true }
        )
    },
    [PSCustomObject]@{
        Name = "conflict_and_recovery_concurrency"
        Package = "./server/innodb/manager"
        Pattern = "TestConcurrentRollback|TestConcurrentInsert|TestCrashRecoveryConcurrentTransactions"
        Expected = "concurrent rollback, insert, and recovery tests pass"
        AssertionChecks = @(
            [PSCustomObject]@{ Name = "concurrent_rollback"; Expected = "concurrent rollback completes with test-expected state"; Evidence = "TestConcurrentRollback selector"; Coverage = "test_backed" },
            [PSCustomObject]@{ Name = "concurrent_insert"; Expected = "concurrent insert completes without focused test failure"; Evidence = "TestConcurrentInsert selector"; Coverage = "test_backed" },
            [PSCustomObject]@{ Name = "concurrent_crash_recovery"; Expected = "crash recovery handles concurrent transaction scenario"; Evidence = "TestCrashRecoveryConcurrentTransactions selector"; Coverage = "test_backed" }
        )
        ConsistencyGaps = @(
            [PSCustomObject]@{ Name = "explicit_final_state_diff"; Expected = "expected final row/storage state is compared with actual final state for conflict writes"; RequiredForAcceptance = $true }
        )
    },
    [PSCustomObject]@{
        Name = "wrapper_concurrency"
        Package = "./server/innodb/storage/wrapper/..."
        Pattern = "Test.*Concurrent.*|Test.*Concurrency.*"
        Expected = "wrapper-level concurrency tests pass"
        AssertionChecks = @(
            [PSCustomObject]@{ Name = "wrapper_concurrency_selectors"; Expected = "wrapper-level concurrent/concurrency selectors pass across packages"; Evidence = "Test.*Concurrent.* and Test.*Concurrency.* selectors"; Coverage = "test_backed" }
        )
        ConsistencyGaps = @(
            [PSCustomObject]@{ Name = "wrapper_state_snapshot_diff"; Expected = "wrapper-level storage state snapshots are compared before and after concurrent operations"; RequiredForAcceptance = $true }
        )
    },
    [PSCustomObject]@{
        Name = "storage_mvcc_deadlock"
        Package = "./server/innodb/storage/store/mvcc"
        Pattern = "TestDeadlockScenarios|TestReadView_MultipleTransactions|TestReadView_TransactionCanSeeOwnChanges"
        Expected = "storage MVCC deadlock and read-view tests pass"
        AssertionChecks = @(
            [PSCustomObject]@{ Name = "deadlock_scenarios"; Expected = "storage MVCC deadlock scenarios complete as expected"; Evidence = "TestDeadlockScenarios selector"; Coverage = "test_backed" },
            [PSCustomObject]@{ Name = "read_view_multiple_transactions"; Expected = "read views behave across multiple transactions"; Evidence = "TestReadView_MultipleTransactions selector"; Coverage = "test_backed" },
            [PSCustomObject]@{ Name = "read_view_own_changes"; Expected = "transactions can see their own changes"; Evidence = "TestReadView_TransactionCanSeeOwnChanges selector"; Coverage = "test_backed" }
        )
        ConsistencyGaps = @(
            [PSCustomObject]@{ Name = "deadlock_victim_report"; Expected = "deadlock victim and wait-for graph evidence are reported"; RequiredForAcceptance = $true }
        )
    }
)

$Header = @(
    "=== concurrency validation $StartedAt ===",
    "repo: $RepoRoot",
    "go: $GoVersion",
    "runs: $Runs",
    "timeout: $($TimeoutSeconds)s",
    "evidence_type: concurrency_validation",
    ""
)

$Header | Set-Content -Path $LogPath -Encoding utf8

$ConsistencyEvidenceChecks = @{}
$ConsistencyEvidenceStatus = "NOT_PROVIDED"
if (-not [string]::IsNullOrWhiteSpace($P0CConsistencyEvidenceJson)) {
    if (-not (Test-Path -LiteralPath $P0CConsistencyEvidenceJson)) {
        throw "P0-C consistency evidence JSON does not exist: $P0CConsistencyEvidenceJson"
    }

    $ConsistencyEvidence = Get-Content -Raw -LiteralPath $P0CConsistencyEvidenceJson | ConvertFrom-Json
    if ($ConsistencyEvidence.evidence_type -ne "p0c_consistency_evidence") {
        throw "P0-C consistency evidence JSON must have evidence_type=p0c_consistency_evidence."
    }

    $ConsistencyEvidenceStatus = [string]$ConsistencyEvidence.status
    foreach ($Check in @($ConsistencyEvidence.checks)) {
        $ConsistencyEvidenceChecks[[string]$Check.name] = $Check
    }
}

function Resolve-ConsistencyGap {
    param(
        [object]$Gap
    )

    $Name = [string]$Gap.Name
    if ($ConsistencyEvidenceChecks.ContainsKey($Name)) {
        $EvidenceCheck = $ConsistencyEvidenceChecks[$Name]
        return [PSCustomObject]@{
            name = $Name
            expected = $Gap.Expected
            status = [string]$EvidenceCheck.status
            required_for_acceptance = [bool]$Gap.RequiredForAcceptance
            evidence = [PSCustomObject]@{
                source = $P0CConsistencyEvidenceJson
                expected = [string]$EvidenceCheck.expected
                actual = [string]$EvidenceCheck.actual
                metrics = $EvidenceCheck.metrics
            }
        }
    }

    return [PSCustomObject]@{
        name = $Name
        expected = $Gap.Expected
        status = "NOT_VERIFIED"
        required_for_acceptance = [bool]$Gap.RequiredForAcceptance
        evidence = $null
    }
}

$ScenarioResults = @()
$OverallExitCode = 0

foreach ($Scenario in $Scenarios) {
    $ScenarioRuns = @()

    for ($Run = 1; $Run -le $Runs; $Run++) {
        $RunStartedAt = Get-Date -Format "o"
        $GoArgs = @(
            "test",
            $Scenario.Package,
            "-run",
            $Scenario.Pattern,
            "-count=1",
            "-timeout=$($TimeoutSeconds)s"
        )

        if ($VerboseTests) {
            $GoArgs += "-v"
        }

        @(
            "",
            "=== scenario $($Scenario.Name) run $Run/$Runs started at $RunStartedAt ===",
            "--- go $($GoArgs -join ' ') ---"
        ) | Add-Content -Path $LogPath -Encoding utf8

        & go @GoArgs 2>&1 | Tee-Object -FilePath $LogPath -Append
        $RunExitCode = $LASTEXITCODE
        $RunFinishedAt = Get-Date -Format "o"
        $RunStatus = if ($RunExitCode -eq 0) { "PASS" } else { "FAIL" }

        "=== scenario $($Scenario.Name) run $Run/$Runs finished at ${RunFinishedAt}: $RunStatus (exit code $RunExitCode) ===" | Add-Content -Path $LogPath -Encoding utf8

        $ScenarioRuns += [PSCustomObject]@{
            run = $Run
            status = $RunStatus
            exit_code = $RunExitCode
            started_at = $RunStartedAt
            finished_at = $RunFinishedAt
        }

        if ($RunExitCode -ne 0 -and $OverallExitCode -eq 0) {
            $OverallExitCode = $RunExitCode
        }
    }

    $FailedRuns = @($ScenarioRuns | Where-Object { $_.status -ne "PASS" })
    $ScenarioStatus = if ($FailedRuns.Count -eq 0) { "PASS" } else { "FAIL" }
    $ScenarioAssertions = @($Scenario.AssertionChecks | ForEach-Object {
        [PSCustomObject]@{
            name = $_.Name
            expected = $_.Expected
            evidence = $_.Evidence
            coverage = $_.Coverage
            status = $ScenarioStatus
        }
    })
    $ScenarioGaps = @($Scenario.ConsistencyGaps | ForEach-Object { Resolve-ConsistencyGap -Gap $_ })
    $ScenarioResults += [PSCustomObject]@{
        name = $Scenario.Name
        status = $ScenarioStatus
        package = $Scenario.Package
        pattern = $Scenario.Pattern
        expected = $Scenario.Expected
        actual = "go test scenario completed with status $ScenarioStatus"
        metrics = [PSCustomObject]@{
            runs = $Runs
            failed_runs = $FailedRuns.Count
        }
        scenario_assertions = $ScenarioAssertions
        consistency_gaps = $ScenarioGaps
        runs = $ScenarioRuns
    }
}

$FinishedAt = Get-Date -Format "o"
$Status = if ($OverallExitCode -eq 0) { "PASS" } else { "FAIL" }
$AllAssertions = @($ScenarioResults | ForEach-Object { $_.scenario_assertions } | ForEach-Object { $_ })
$AllGaps = @($ScenarioResults | ForEach-Object { $_.consistency_gaps } | ForEach-Object { $_ })
$RequiredOpenGaps = @($AllGaps | Where-Object { $_.required_for_acceptance -eq $true -and $_.status -ne "PASS" })

$Report = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = $Status
    evidence_type = "concurrency_validation"
    profile = [ordered]@{
        runs = $Runs
        timeout_seconds = $TimeoutSeconds
        scenario_count = $Scenarios.Count
    }
    repository = "$RepoRoot"
    go_version = $GoVersion
    raw_log = "$LogPath"
    markdown_report = "$MarkdownPath"
    p0c_consistency_evidence_json = "$P0CConsistencyEvidenceJson"
    p0c_consistency_evidence_status = "$ConsistencyEvidenceStatus"
    consistency_summary = [ordered]@{
        test_backed_assertions = $AllAssertions.Count
        not_verified_gaps = @($AllGaps | Where-Object { $_.status -eq "NOT_VERIFIED" }).Count
        failed_gaps = @($AllGaps | Where-Object { $_.status -eq "FAIL" }).Count
        passed_gaps = @($AllGaps | Where-Object { $_.status -eq "PASS" }).Count
        required_gap_count = $RequiredOpenGaps.Count
        acceptance_status = if ($RequiredOpenGaps.Count -eq 0 -and $Status -eq "PASS") { "ACCEPTED" } else { "PARTIAL" }
    }
    limitations = @(
        "This report records command-level concurrency validation evidence.",
        "It records test-backed scenario assertions and explicit NOT_VERIFIED consistency gaps.",
        "It does not yet prove row-level serializability, full isolation semantics, or storage-state diff consistency.",
        "Full P0-C acceptance still requires explicit consistency checks and final regression validation."
    )
    scenarios = $ScenarioResults
}

$Report | ConvertTo-Json -Depth 16 | Set-Content -Path $JsonPath -Encoding utf8

$ScenarioLines = $ScenarioResults | ForEach-Object {
    "- $($_.name): $($_.status) ($($_.metrics.failed_runs)/$Runs failed runs)"
}

$AssertionLines = $ScenarioResults | ForEach-Object {
    $ScenarioName = $_.name
    $_.scenario_assertions | ForEach-Object {
        "- ${ScenarioName} / $($_.name): $($_.status) ($($_.coverage); $($_.evidence))"
    }
}

$GapLines = $ScenarioResults | ForEach-Object {
    $ScenarioName = $_.name
    $_.consistency_gaps | ForEach-Object {
        "- ${ScenarioName} / $($_.name): $($_.status) ($($_.expected))"
    }
}

$Markdown = @(
    "# Concurrency Validation Report",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- Go version: $GoVersion",
    "- Runs: $Runs",
    "- Timeout: $($TimeoutSeconds)s",
    "- Raw log: ``$LogPath``",
    "- JSON report: ``$JsonPath``",
    "- P0-C consistency evidence JSON: ``$P0CConsistencyEvidenceJson``",
    "- P0-C consistency evidence status: $ConsistencyEvidenceStatus",
    "",
    "## Scenario results",
    "",
    $ScenarioLines,
    "",
    "## Test-backed scenario assertions",
    "",
    $AssertionLines,
    "",
    "## Explicit consistency gaps",
    "",
    $GapLines,
    "",
    "## Acceptance note",
    "",
    "This is command-level P0-C evidence. It does not replace explicit consistency checks, row-level anomaly detection, or storage-state diff evidence.",
    "",
    "## Result",
    "",
    "- Exit code: $OverallExitCode",
    "- Result: $Status"
)

$Markdown | Set-Content -Path $MarkdownPath -Encoding utf8

Write-Host "Concurrency validation status: $Status"
Write-Host "Raw log written: $LogPath"
Write-Host "Markdown report written: $MarkdownPath"
Write-Host "JSON report written: $JsonPath"

exit $OverallExitCode
