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
        [object]$Value,
        [string]$Context
    )

    if (-not ($Value -is [string]) -or [string]::IsNullOrWhiteSpace($Value)) {
        throw "$Context must be a non-empty string."
    }
}

if (-not (Test-Path -LiteralPath $Path)) {
    throw "P0-C consistency evidence does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Evidence = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @("evidence_type", "generated_at", "status", "scenario", "checks", "limitations")) {
    Assert-Property -Object $Evidence -Name $Name -Context "root"
}

if ($Evidence.evidence_type -ne "p0c_consistency_evidence") {
    throw "evidence_type must be p0c_consistency_evidence. Actual: $($Evidence.evidence_type)"
}

if ($Evidence.status -notin @("PASS", "FAIL")) {
    throw "status must be PASS or FAIL. Actual: $($Evidence.status)"
}

Assert-NonEmptyString -Value $Evidence.generated_at -Context "generated_at"
Assert-NonEmptyString -Value $Evidence.scenario -Context "scenario"

$RequiredChecks = @(
    "lock_wait_metrics_summary",
    "full_isolation_matrix",
    "long_transaction_runtime_metrics",
    "explicit_final_state_diff",
    "wrapper_state_snapshot_diff",
    "deadlock_victim_report"
)

if (-not $Evidence.checks -or @($Evidence.checks).Count -lt $RequiredChecks.Count) {
    throw "checks must include all required P0-C consistency checks."
}

foreach ($RequiredCheck in $RequiredChecks) {
    $Match = @($Evidence.checks | Where-Object { $_.name -eq $RequiredCheck })
    if ($Match.Count -ne 1) {
        throw "Expected exactly one check named '$RequiredCheck'."
    }
}

$AnyFailed = $false
foreach ($Check in @($Evidence.checks)) {
    foreach ($Name in @("name", "status", "expected", "actual", "metrics")) {
        Assert-Property -Object $Check -Name $Name -Context "check"
    }

    Assert-NonEmptyString -Value $Check.name -Context "check.name"
    Assert-NonEmptyString -Value $Check.expected -Context "check.expected"
    Assert-NonEmptyString -Value $Check.actual -Context "check.actual"

    if ($Check.status -notin @("PASS", "FAIL")) {
        throw "check.status must be PASS or FAIL. Actual: $($Check.status)"
    }

    if ($Check.status -eq "FAIL") {
        $AnyFailed = $true
    }
}

if ($AnyFailed -and $Evidence.status -ne "FAIL") {
    throw "root status must be FAIL when any check failed."
}

if (-not $AnyFailed -and $Evidence.status -ne "PASS") {
    throw "root status must be PASS when all checks pass."
}

if (-not $Evidence.limitations -or @($Evidence.limitations).Count -lt 1) {
    throw "limitations must include at least one entry."
}

Write-Host "P0-C consistency evidence schema validation passed: $ResolvedPath"
