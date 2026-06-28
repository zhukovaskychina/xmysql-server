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

function Assert-NonNegativeInteger {
    param(
        [object]$Value,
        [string]$Context
    )

    if (-not ($Value -is [int] -or $Value -is [long]) -or $Value -lt 0) {
        throw "$Context must be a non-negative integer."
    }
}

if (-not (Test-Path -LiteralPath $Path)) {
    throw "P0-B state diff artifact does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Artifact = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @(
    "evidence_type",
    "generated_at",
    "run_id",
    "status",
    "scope",
    "scenario",
    "repository",
    "expected_snapshot",
    "actual_snapshot",
    "expected_sha256",
    "actual_sha256",
    "diff_summary",
    "samples",
    "limitations"
)) {
    Assert-Property -Object $Artifact -Name $Name -Context "root"
}

if ($Artifact.evidence_type -ne "p0b_state_diff_artifact") {
    throw "evidence_type must be p0b_state_diff_artifact. Actual: $($Artifact.evidence_type)"
}

if ($Artifact.status -notin @("PASS", "FAIL")) {
    throw "status must be PASS or FAIL. Actual: $($Artifact.status)"
}

if ($Artifact.scope -notin @("row_state_diff", "page_state_diff", "wal_replay_diff")) {
    throw "scope must be row_state_diff, page_state_diff, or wal_replay_diff. Actual: $($Artifact.scope)"
}

foreach ($Name in @("generated_at", "run_id", "scope", "scenario", "repository", "expected_snapshot", "actual_snapshot", "expected_sha256", "actual_sha256")) {
    Assert-NonEmptyString -Value $Artifact.$Name -Context $Name
}

foreach ($Name in @("missing_expected_items", "unexpected_actual_items", "mismatched_values", "mismatches")) {
    Assert-Property -Object $Artifact.diff_summary -Name $Name -Context "diff_summary"
    Assert-NonNegativeInteger -Value $Artifact.diff_summary.$Name -Context "diff_summary.$Name"
}

$ComputedMismatchCount = $Artifact.diff_summary.missing_expected_items + $Artifact.diff_summary.unexpected_actual_items + $Artifact.diff_summary.mismatched_values
if ($Artifact.diff_summary.mismatches -ne $ComputedMismatchCount) {
    throw "diff_summary.mismatches must equal missing_expected_items + unexpected_actual_items + mismatched_values."
}

if ($Artifact.status -eq "PASS" -and $Artifact.diff_summary.mismatches -ne 0) {
    throw "PASS artifact must have zero mismatches."
}

if ($Artifact.status -eq "FAIL" -and $Artifact.diff_summary.mismatches -eq 0) {
    throw "FAIL artifact must include at least one mismatch."
}

foreach ($Name in @("missing_expected_paths", "unexpected_actual_paths", "mismatched_values")) {
    Assert-Property -Object $Artifact.samples -Name $Name -Context "samples"
}

if (-not $Artifact.limitations -or @($Artifact.limitations).Count -lt 1) {
    throw "limitations must include at least one entry."
}

Write-Host "P0-B state diff artifact schema validation passed: $ResolvedPath"
