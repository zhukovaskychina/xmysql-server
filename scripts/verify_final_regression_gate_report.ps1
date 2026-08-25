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
    throw "Final regression gate report does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Report = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @("generated_at", "run_id", "status", "evidence_type", "repository", "go_version", "command", "timeout_seconds", "raw_log", "markdown_report", "exit_code", "limitations")) {
    Assert-Property -Object $Report -Name $Name -Context "root"
}

Assert-NonEmptyString -Value $Report.generated_at -Context "generated_at"
Assert-NonEmptyString -Value $Report.run_id -Context "run_id"
Assert-NonEmptyString -Value $Report.status -Context "status"
Assert-NonEmptyString -Value $Report.evidence_type -Context "evidence_type"
Assert-NonEmptyString -Value $Report.repository -Context "repository"
Assert-NonEmptyString -Value $Report.go_version -Context "go_version"
Assert-NonEmptyString -Value $Report.command -Context "command"
Assert-NonEmptyString -Value $Report.raw_log -Context "raw_log"
Assert-NonEmptyString -Value $Report.markdown_report -Context "markdown_report"

if ($Report.evidence_type -ne "final_regression_gate") {
    throw "evidence_type must be 'final_regression_gate'. Actual: $($Report.evidence_type)"
}

if ($Report.status -notin @("PASS", "FAIL")) {
    throw "status must be PASS or FAIL. Actual: $($Report.status)"
}

if ($Report.timeout_seconds -lt 1) {
    throw "timeout_seconds must be greater than or equal to 1."
}

if ($Report.exit_code -lt 0) {
    throw "exit_code must be greater than or equal to 0."
}

if ($Report.exit_code -ne 0 -and $Report.status -ne "FAIL") {
    throw "status must be FAIL when exit_code is non-zero."
}

if ($Report.exit_code -eq 0 -and $Report.status -ne "PASS") {
    throw "status must be PASS when exit_code is zero."
}

if (-not $Report.limitations -or $Report.limitations.Count -lt 1) {
    throw "limitations must include at least one entry."
}

Write-Host "Final regression gate report schema validation passed: $ResolvedPath"
