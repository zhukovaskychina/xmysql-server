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
    throw "P0-E timed rollback evidence does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Evidence = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @(
    "evidence_type",
    "generated_at",
    "run_id",
    "status",
    "scenario",
    "rollback_window_seconds",
    "rollback_duration_seconds",
    "within_window",
    "recovery_point",
    "replay_boundary",
    "steps",
    "limitations"
)) {
    Assert-Property -Object $Evidence -Name $Name -Context "root"
}

if ($Evidence.evidence_type -ne "p0e_timed_rollback_evidence") {
    throw "evidence_type must be p0e_timed_rollback_evidence. Actual: $($Evidence.evidence_type)"
}

if ($Evidence.status -notin @("PASS", "FAIL")) {
    throw "status must be PASS or FAIL. Actual: $($Evidence.status)"
}

foreach ($Name in @("generated_at", "run_id", "scenario", "recovery_point", "replay_boundary")) {
    Assert-NonEmptyString -Value $Evidence.$Name -Context $Name
}

if ($Evidence.rollback_window_seconds -lt 1) {
    throw "rollback_window_seconds must be greater than or equal to 1."
}

if ($Evidence.rollback_duration_seconds -lt 0) {
    throw "rollback_duration_seconds must be greater than or equal to 0."
}

$ExpectedWithinWindow = $Evidence.rollback_duration_seconds -le $Evidence.rollback_window_seconds
if ($Evidence.within_window -ne $ExpectedWithinWindow) {
    throw "within_window must match rollback_duration_seconds <= rollback_window_seconds."
}

if ($Evidence.status -eq "PASS" -and -not $Evidence.within_window) {
    throw "PASS evidence requires within_window=true."
}

if (-not $Evidence.steps -or @($Evidence.steps).Count -lt 3) {
    throw "steps must include at least recovery point, replay boundary, and rollback window checks."
}

$AnyStepFailed = $false
foreach ($Step in @($Evidence.steps)) {
    foreach ($Name in @("name", "status", "evidence")) {
        Assert-Property -Object $Step -Name $Name -Context "step"
    }

    Assert-NonEmptyString -Value $Step.name -Context "step.name"
    Assert-NonEmptyString -Value $Step.evidence -Context "step.evidence"

    if ($Step.status -notin @("PASS", "FAIL")) {
        throw "step.status must be PASS or FAIL. Actual: $($Step.status)"
    }

    if ($Step.status -eq "FAIL") {
        $AnyStepFailed = $true
    }
}

if ($AnyStepFailed -and $Evidence.status -ne "FAIL") {
    throw "root status must be FAIL when any step failed."
}

if (-not $AnyStepFailed -and $Evidence.status -ne "PASS") {
    throw "root status must be PASS when all steps passed."
}

if (-not $Evidence.limitations -or @($Evidence.limitations).Count -lt 1) {
    throw "limitations must include at least one entry."
}

Write-Host "P0-E timed rollback evidence schema validation passed: $ResolvedPath"
