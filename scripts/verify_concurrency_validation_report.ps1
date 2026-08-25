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
    throw "Concurrency validation report does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Report = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @("generated_at", "run_id", "status", "evidence_type", "profile", "repository", "go_version", "raw_log", "markdown_report", "p0c_consistency_evidence_json", "p0c_consistency_evidence_status", "consistency_summary", "limitations", "scenarios")) {
    Assert-Property -Object $Report -Name $Name -Context "root"
}

Assert-NonEmptyString -Value $Report.generated_at -Context "generated_at"
Assert-NonEmptyString -Value $Report.run_id -Context "run_id"
Assert-NonEmptyString -Value $Report.status -Context "status"
Assert-NonEmptyString -Value $Report.evidence_type -Context "evidence_type"
Assert-NonEmptyString -Value $Report.repository -Context "repository"
Assert-NonEmptyString -Value $Report.go_version -Context "go_version"
Assert-NonEmptyString -Value $Report.raw_log -Context "raw_log"
Assert-NonEmptyString -Value $Report.markdown_report -Context "markdown_report"

if ($Report.evidence_type -ne "concurrency_validation") {
    throw "evidence_type must be 'concurrency_validation'. Actual: $($Report.evidence_type)"
}

if ($Report.status -notin @("PASS", "FAIL")) {
    throw "status must be PASS or FAIL. Actual: $($Report.status)"
}

foreach ($Name in @("runs", "timeout_seconds", "scenario_count")) {
    Assert-Property -Object $Report.profile -Name $Name -Context "profile"
}

if ($Report.profile.runs -lt 1) {
    throw "profile.runs must be greater than or equal to 1."
}

if ($Report.profile.timeout_seconds -lt 1) {
    throw "profile.timeout_seconds must be greater than or equal to 1."
}

if ($Report.profile.scenario_count -lt 1) {
    throw "profile.scenario_count must be greater than or equal to 1."
}

if (-not $Report.limitations -or @($Report.limitations).Count -lt 1) {
    throw "limitations must include at least one entry."
}

foreach ($Name in @("test_backed_assertions", "not_verified_gaps", "failed_gaps", "passed_gaps", "required_gap_count", "acceptance_status")) {
    Assert-Property -Object $Report.consistency_summary -Name $Name -Context "consistency_summary"
}

if ($Report.consistency_summary.test_backed_assertions -lt 1) {
    throw "consistency_summary.test_backed_assertions must be greater than or equal to 1."
}

if ($Report.consistency_summary.not_verified_gaps -lt 0) {
    throw "consistency_summary.not_verified_gaps must be greater than or equal to 0."
}

if ($Report.consistency_summary.failed_gaps -lt 0) {
    throw "consistency_summary.failed_gaps must be greater than or equal to 0."
}

if ($Report.consistency_summary.passed_gaps -lt 0) {
    throw "consistency_summary.passed_gaps must be greater than or equal to 0."
}

if ($Report.consistency_summary.required_gap_count -lt 0) {
    throw "consistency_summary.required_gap_count must be greater than or equal to 0."
}

if ($Report.consistency_summary.acceptance_status -notin @("ACCEPTED", "PARTIAL")) {
    throw "consistency_summary.acceptance_status must be ACCEPTED or PARTIAL. Actual: $($Report.consistency_summary.acceptance_status)"
}

if (-not $Report.scenarios -or @($Report.scenarios).Count -lt 1) {
    throw "scenarios must include at least one scenario."
}

$AnyScenarioFailed = $false

foreach ($Scenario in @($Report.scenarios)) {
    foreach ($Name in @("name", "status", "package", "pattern", "expected", "actual", "metrics", "scenario_assertions", "consistency_gaps", "runs")) {
        Assert-Property -Object $Scenario -Name $Name -Context "scenario"
    }

    Assert-NonEmptyString -Value $Scenario.name -Context "scenario.name"
    Assert-NonEmptyString -Value $Scenario.status -Context "scenario.status"
    Assert-NonEmptyString -Value $Scenario.package -Context "scenario.package"
    Assert-NonEmptyString -Value $Scenario.pattern -Context "scenario.pattern"
    Assert-NonEmptyString -Value $Scenario.expected -Context "scenario.expected"
    Assert-NonEmptyString -Value $Scenario.actual -Context "scenario.actual"

    if ($Scenario.status -notin @("PASS", "FAIL")) {
        throw "scenario.status must be PASS or FAIL. Actual: $($Scenario.status)"
    }

    if ($Scenario.status -eq "FAIL") {
        $AnyScenarioFailed = $true
    }

    foreach ($Name in @("runs", "failed_runs")) {
        Assert-Property -Object $Scenario.metrics -Name $Name -Context "scenario.metrics"
    }

    if ($Scenario.metrics.runs -lt 1) {
        throw "scenario.metrics.runs must be greater than or equal to 1."
    }

    if ($Scenario.metrics.failed_runs -lt 0) {
        throw "scenario.metrics.failed_runs must be greater than or equal to 0."
    }

    if (-not $Scenario.scenario_assertions -or @($Scenario.scenario_assertions).Count -lt 1) {
        throw "scenario.scenario_assertions must include at least one assertion."
    }

    foreach ($Assertion in @($Scenario.scenario_assertions)) {
        foreach ($Name in @("name", "expected", "evidence", "coverage", "status")) {
            Assert-Property -Object $Assertion -Name $Name -Context "scenario.assertion"
        }

        Assert-NonEmptyString -Value $Assertion.name -Context "scenario.assertion.name"
        Assert-NonEmptyString -Value $Assertion.expected -Context "scenario.assertion.expected"
        Assert-NonEmptyString -Value $Assertion.evidence -Context "scenario.assertion.evidence"
        Assert-NonEmptyString -Value $Assertion.coverage -Context "scenario.assertion.coverage"

        if ($Assertion.coverage -ne "test_backed") {
            throw "scenario.assertion.coverage must be test_backed. Actual: $($Assertion.coverage)"
        }

        if ($Assertion.status -notin @("PASS", "FAIL")) {
            throw "scenario.assertion.status must be PASS or FAIL. Actual: $($Assertion.status)"
        }
    }

    if (-not $Scenario.consistency_gaps -or @($Scenario.consistency_gaps).Count -lt 1) {
        throw "scenario.consistency_gaps must include at least one gap."
    }

    foreach ($Gap in @($Scenario.consistency_gaps)) {
        foreach ($Name in @("name", "expected", "status", "required_for_acceptance", "evidence")) {
            Assert-Property -Object $Gap -Name $Name -Context "scenario.consistency_gap"
        }

        Assert-NonEmptyString -Value $Gap.name -Context "scenario.consistency_gap.name"
        Assert-NonEmptyString -Value $Gap.expected -Context "scenario.consistency_gap.expected"

        if ($Gap.status -notin @("PASS", "FAIL", "NOT_VERIFIED")) {
            throw "scenario.consistency_gap.status must be PASS, FAIL, or NOT_VERIFIED. Actual: $($Gap.status)"
        }

        if ($Gap.status -eq "PASS") {
            if (-not $Gap.evidence) {
                throw "PASS consistency gap '$($Gap.name)' must include evidence metadata."
            }
            foreach ($Name in @("source", "expected", "actual", "metrics")) {
                Assert-Property -Object $Gap.evidence -Name $Name -Context "scenario.consistency_gap.evidence"
            }
            Assert-NonEmptyString -Value $Gap.evidence.source -Context "scenario.consistency_gap.evidence.source"
            Assert-NonEmptyString -Value $Gap.evidence.expected -Context "scenario.consistency_gap.evidence.expected"
            Assert-NonEmptyString -Value $Gap.evidence.actual -Context "scenario.consistency_gap.evidence.actual"
        }
    }

    if (-not $Scenario.runs -or @($Scenario.runs).Count -lt 1) {
        throw "scenario.runs must include at least one replay run."
    }

    foreach ($Run in @($Scenario.runs)) {
        foreach ($Name in @("run", "status", "exit_code", "started_at", "finished_at")) {
            Assert-Property -Object $Run -Name $Name -Context "scenario.run"
        }

        if ($Run.run -lt 1) {
            throw "scenario.run.run must be greater than or equal to 1."
        }

        if ($Run.status -notin @("PASS", "FAIL")) {
            throw "scenario.run.status must be PASS or FAIL. Actual: $($Run.status)"
        }

        if ($Run.exit_code -lt 0) {
            throw "scenario.run.exit_code must be greater than or equal to 0."
        }

        Assert-NonEmptyString -Value $Run.started_at -Context "scenario.run.started_at"
        Assert-NonEmptyString -Value $Run.finished_at -Context "scenario.run.finished_at"
    }
}

if ($AnyScenarioFailed -and $Report.status -ne "FAIL") {
    throw "root status must be FAIL when any scenario failed."
}

if ($Report.consistency_summary.acceptance_status -eq "ACCEPTED") {
    if ($Report.status -ne "PASS") {
        throw "ACCEPTED consistency summary requires root status PASS."
    }
    if ($Report.consistency_summary.required_gap_count -ne 0) {
        throw "ACCEPTED consistency summary requires required_gap_count=0."
    }
    if ($Report.consistency_summary.not_verified_gaps -ne 0) {
        throw "ACCEPTED consistency summary requires not_verified_gaps=0."
    }
    if ($Report.consistency_summary.failed_gaps -ne 0) {
        throw "ACCEPTED consistency summary requires failed_gaps=0."
    }
}

Write-Host "Concurrency validation report schema validation passed: $ResolvedPath"
