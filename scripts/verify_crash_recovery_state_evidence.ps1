param(
    [Parameter(Mandatory = $true)]
    [string]$Path
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Assert-Property {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Object,
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    if (-not ($Object.PSObject.Properties.Name -contains $Name)) {
        throw "$Context is missing required property '$Name'."
    }
}

function Assert-NonEmptyString {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Value,
        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    if (-not ($Value -is [string]) -or [string]::IsNullOrWhiteSpace($Value)) {
        throw "$Context must be a non-empty string."
    }
}

if (-not (Test-Path -LiteralPath $Path)) {
    throw "State evidence file does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Evidence = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @("generated_at", "run_id", "status", "evidence_type", "verification_level", "package", "test_pattern", "timeout_seconds", "raw_log", "markdown_report", "state_diff_artifacts", "scenario_matrix", "limitations", "runs")) {
    Assert-Property -Object $Evidence -Name $Name -Context "root"
}

Assert-NonEmptyString -Value $Evidence.generated_at -Context "generated_at"
Assert-NonEmptyString -Value $Evidence.run_id -Context "run_id"
Assert-NonEmptyString -Value $Evidence.status -Context "status"
Assert-NonEmptyString -Value $Evidence.evidence_type -Context "evidence_type"
Assert-NonEmptyString -Value $Evidence.verification_level -Context "verification_level"
Assert-NonEmptyString -Value $Evidence.package -Context "package"
Assert-NonEmptyString -Value $Evidence.test_pattern -Context "test_pattern"
Assert-NonEmptyString -Value $Evidence.raw_log -Context "raw_log"
Assert-NonEmptyString -Value $Evidence.markdown_report -Context "markdown_report"

foreach ($Name in @("row_state_diff_json", "page_state_diff_json", "wal_replay_diff_json")) {
    Assert-Property -Object $Evidence.state_diff_artifacts -Name $Name -Context "state_diff_artifacts"
}

if ($Evidence.evidence_type -ne "command_replay") {
    throw "evidence_type must be 'command_replay' for this verifier. Actual: $($Evidence.evidence_type)"
}

if ($Evidence.status -notin @("PASS", "FAIL")) {
    throw "status must be PASS or FAIL. Actual: $($Evidence.status)"
}

if (-not ($Evidence.timeout_seconds -is [int] -or $Evidence.timeout_seconds -is [long]) -or $Evidence.timeout_seconds -lt 1) {
    throw "timeout_seconds must be a positive integer."
}

if ($Evidence.verification_level -ne "test_backed_recovery_state_contract") {
    throw "verification_level must be 'test_backed_recovery_state_contract'. Actual: $($Evidence.verification_level)"
}

if (-not $Evidence.scenario_matrix -or @($Evidence.scenario_matrix).Count -lt 1) {
    throw "scenario_matrix must include at least one scenario."
}

foreach ($Scenario in @($Evidence.scenario_matrix)) {
    foreach ($Name in @("id", "selector", "proof_type", "expected", "state_layers")) {
        Assert-Property -Object $Scenario -Name $Name -Context "scenario_matrix"
    }

    Assert-NonEmptyString -Value $Scenario.id -Context "scenario_matrix.id"
    Assert-NonEmptyString -Value $Scenario.selector -Context "scenario_matrix.selector"
    Assert-NonEmptyString -Value $Scenario.proof_type -Context "scenario_matrix.proof_type"
    Assert-NonEmptyString -Value $Scenario.expected -Context "scenario_matrix.expected"

    if (-not $Scenario.state_layers -or @($Scenario.state_layers).Count -lt 1) {
        throw "scenario_matrix.state_layers must include at least one state layer."
    }
}

if (-not $Evidence.limitations -or @($Evidence.limitations).Count -lt 1) {
    throw "limitations must include at least one entry."
}

if (-not $Evidence.runs -or @($Evidence.runs).Count -lt 1) {
    throw "runs must include at least one replay run."
}

$AnyRunFailed = $false

foreach ($Run in @($Evidence.runs)) {
    foreach ($Name in @("run", "status", "exit_code", "started_at", "finished_at", "state_evidence", "checks")) {
        Assert-Property -Object $Run -Name $Name -Context "run"
    }

    if (-not ($Run.run -is [int] -or $Run.run -is [long]) -or $Run.run -lt 1) {
        throw "run.run must be a positive integer."
    }

    if ($Run.status -notin @("PASS", "FAIL")) {
        throw "run.status must be PASS or FAIL. Actual: $($Run.status)"
    }

    if ($Run.status -eq "FAIL") {
        $AnyRunFailed = $true
    }

    if (-not ($Run.exit_code -is [int] -or $Run.exit_code -is [long]) -or $Run.exit_code -lt 0) {
        throw "run.exit_code must be a non-negative integer."
    }

    Assert-NonEmptyString -Value $Run.started_at -Context "run.started_at"
    Assert-NonEmptyString -Value $Run.finished_at -Context "run.finished_at"

    foreach ($Name in @("verification_level", "expected_final_state", "actual_observation", "wal_replay_boundary", "interrupted_commit_recovery", "row_level_state_diff", "page_level_state_diff")) {
        Assert-Property -Object $Run.state_evidence -Name $Name -Context "run.state_evidence"
    }

    Assert-NonEmptyString -Value $Run.state_evidence.verification_level -Context "run.state_evidence.verification_level"
    Assert-NonEmptyString -Value $Run.state_evidence.expected_final_state -Context "run.state_evidence.expected_final_state"
    Assert-NonEmptyString -Value $Run.state_evidence.actual_observation -Context "run.state_evidence.actual_observation"

    foreach ($StateCheckName in @("wal_replay_boundary", "interrupted_commit_recovery", "row_level_state_diff", "page_level_state_diff")) {
        $StateCheck = $Run.state_evidence.$StateCheckName
        foreach ($Name in @("expected", "actual", "status")) {
            Assert-Property -Object $StateCheck -Name $Name -Context "run.state_evidence.$StateCheckName"
        }

        Assert-NonEmptyString -Value $StateCheck.expected -Context "run.state_evidence.$StateCheckName.expected"
        Assert-NonEmptyString -Value $StateCheck.actual -Context "run.state_evidence.$StateCheckName.actual"

        if ($StateCheck.status -notin @("PASS", "FAIL", "NOT_VERIFIED")) {
            throw "run.state_evidence.$StateCheckName.status must be PASS, FAIL, or NOT_VERIFIED. Actual: $($StateCheck.status)"
        }
    }

    if (-not $Run.checks -or @($Run.checks).Count -lt 1) {
        throw "run.checks must include at least one check."
    }

    foreach ($Check in @($Run.checks)) {
        foreach ($Name in @("name", "expected", "actual", "status")) {
            Assert-Property -Object $Check -Name $Name -Context "run.check"
        }

        Assert-NonEmptyString -Value $Check.name -Context "run.check.name"
        Assert-NonEmptyString -Value $Check.expected -Context "run.check.expected"
        Assert-NonEmptyString -Value $Check.actual -Context "run.check.actual"

        if ($Check.status -notin @("PASS", "FAIL")) {
            throw "run.check.status must be PASS or FAIL. Actual: $($Check.status)"
        }
    }
}

if ($AnyRunFailed -and $Evidence.status -ne "FAIL") {
    throw "root status must be FAIL when any replay run failed."
}

Write-Host "State evidence schema validation passed: $ResolvedPath"
