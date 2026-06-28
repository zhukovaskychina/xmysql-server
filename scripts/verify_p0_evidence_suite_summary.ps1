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
    throw "P0 evidence suite summary does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Summary = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @("generated_at", "run_id", "evidence_type", "repository", "report_dir", "suite_started_at", "suite_finished_at", "delivery_status", "artifacts", "limitations")) {
    Assert-Property -Object $Summary -Name $Name -Context "root"
}

Assert-NonEmptyString -Value $Summary.generated_at -Context "generated_at"
Assert-NonEmptyString -Value $Summary.run_id -Context "run_id"
Assert-NonEmptyString -Value $Summary.repository -Context "repository"
Assert-NonEmptyString -Value $Summary.report_dir -Context "report_dir"
Assert-NonEmptyString -Value $Summary.suite_started_at -Context "suite_started_at"
Assert-NonEmptyString -Value $Summary.suite_finished_at -Context "suite_finished_at"

if ($Summary.evidence_type -ne "p0_evidence_suite_summary") {
    throw "evidence_type must be 'p0_evidence_suite_summary'. Actual: $($Summary.evidence_type)"
}

if ($Summary.delivery_status -notin @("READY", "NOT_READY")) {
    throw "delivery_status must be READY or NOT_READY. Actual: $($Summary.delivery_status)"
}

if (-not $Summary.artifacts -or @($Summary.artifacts).Count -lt 1) {
    throw "artifacts must include at least one artifact."
}

$SuiteStartedAt = [datetime]::Parse($Summary.suite_started_at)

foreach ($Artifact in @($Summary.artifacts)) {
    foreach ($Name in @("name", "kind", "path", "checksum_mode", "sha256", "last_write_time")) {
        Assert-Property -Object $Artifact -Name $Name -Context "artifact"
    }

    Assert-NonEmptyString -Value $Artifact.name -Context "artifact.name"
    Assert-NonEmptyString -Value $Artifact.kind -Context "artifact.kind"
    Assert-NonEmptyString -Value $Artifact.path -Context "artifact.path"
    Assert-NonEmptyString -Value $Artifact.checksum_mode -Context "artifact.checksum_mode"
    Assert-NonEmptyString -Value $Artifact.last_write_time -Context "artifact.last_write_time"

    if ($Artifact.kind -notin @("markdown", "json", "log", "approval", "audit-markdown", "audit-json", "remediation-markdown", "remediation-json", "summary-markdown", "summary-json")) {
        throw "artifact.kind has unsupported value for '$($Artifact.name)': $($Artifact.kind)"
    }

    if (-not (Test-Path -LiteralPath $Artifact.path)) {
        throw "artifact path does not exist for '$($Artifact.name)': $($Artifact.path)"
    }

    $ArtifactLastWriteTime = [datetime]::Parse($Artifact.last_write_time)
    if ($ArtifactLastWriteTime -lt $SuiteStartedAt) {
        throw "artifact '$($Artifact.name)' was last written before suite start. Artifact time: $($Artifact.last_write_time), suite start: $($Summary.suite_started_at)"
    }

    $ActualLastWriteTime = (Get-Item -LiteralPath $Artifact.path).LastWriteTime
    if ($ActualLastWriteTime -lt $SuiteStartedAt) {
        throw "artifact path for '$($Artifact.name)' points to a stale file. Actual file time: $($ActualLastWriteTime.ToString("o")), suite start: $($Summary.suite_started_at)"
    }

    if ($Artifact.checksum_mode -eq "sha256") {
        Assert-NonEmptyString -Value $Artifact.sha256 -Context "artifact.sha256"
        if ($Artifact.sha256 -notmatch "^[a-f0-9]{64}$") {
            throw "artifact.sha256 must be a lowercase SHA256 hex digest for '$($Artifact.name)'. Actual: $($Artifact.sha256)"
        }

        $ActualHash = ((Get-FileHash -Algorithm SHA256 -LiteralPath $Artifact.path).Hash).ToLowerInvariant()
        if ($ActualHash -ne $Artifact.sha256) {
            throw "artifact.sha256 mismatch for '$($Artifact.name)'. Expected $($Artifact.sha256), actual $ActualHash"
        }

        if ([math]::Abs(($ActualLastWriteTime - $ArtifactLastWriteTime).TotalSeconds) -gt 2) {
            throw "artifact.last_write_time mismatch for '$($Artifact.name)'. Recorded $($Artifact.last_write_time), actual $($ActualLastWriteTime.ToString("o"))"
        }
    } elseif ($Artifact.checksum_mode -eq "self-referential") {
        if ($Artifact.kind -notin @("summary-markdown", "summary-json")) {
            throw "self-referential checksum mode is only supported for summary artifacts. Artifact: $($Artifact.name)"
        }
    } else {
        throw "artifact.checksum_mode has unsupported value for '$($Artifact.name)': $($Artifact.checksum_mode)"
    }
}

if (-not $Summary.limitations -or @($Summary.limitations).Count -lt 1) {
    throw "limitations must include at least one entry."
}

Write-Host "P0 evidence suite summary validation passed: $ResolvedPath"
