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
    throw "Observability report does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Report = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @("generated_at", "run_id", "status", "evidence_type", "repository", "raw_log", "markdown_report", "checks", "limitations")) {
    Assert-Property -Object $Report -Name $Name -Context "root"
}

Assert-NonEmptyString -Value $Report.generated_at -Context "generated_at"
Assert-NonEmptyString -Value $Report.run_id -Context "run_id"
Assert-NonEmptyString -Value $Report.status -Context "status"
Assert-NonEmptyString -Value $Report.evidence_type -Context "evidence_type"
Assert-NonEmptyString -Value $Report.repository -Context "repository"
Assert-NonEmptyString -Value $Report.raw_log -Context "raw_log"
Assert-NonEmptyString -Value $Report.markdown_report -Context "markdown_report"

if ($Report.evidence_type -ne "observability_smoke") {
    throw "evidence_type must be 'observability_smoke'. Actual: $($Report.evidence_type)"
}

if ($Report.status -notin @("PASS", "FAIL")) {
    throw "status must be PASS or FAIL. Actual: $($Report.status)"
}

if (-not $Report.limitations -or $Report.limitations.Count -lt 1) {
    throw "limitations must include at least one entry."
}

if (-not $Report.checks -or $Report.checks.Count -lt 1) {
    throw "checks must include at least one check."
}

$AnyCheckFailed = $false

foreach ($Check in @($Report.checks)) {
    foreach ($Name in @("name", "expected", "actual", "status")) {
        Assert-Property -Object $Check -Name $Name -Context "check"
    }

    Assert-NonEmptyString -Value $Check.name -Context "check.name"
    Assert-NonEmptyString -Value $Check.expected -Context "check.expected"
    Assert-NonEmptyString -Value $Check.actual -Context "check.actual"

    if ($Check.status -notin @("PASS", "FAIL")) {
        throw "check.status must be PASS or FAIL. Actual: $($Check.status)"
    }

    if ($Check.status -eq "FAIL") {
        $AnyCheckFailed = $true
    }
}

if ($AnyCheckFailed -and $Report.status -ne "FAIL") {
    throw "root status must be FAIL when any check failed."
}

Write-Host "Observability report schema validation passed: $ResolvedPath"
