param(
    [Parameter(Mandatory = $true)]
    [string]$Path,
    [string]$MarkdownPath = ""
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

function Resolve-PreflightReportPath {
    param(
        [string]$Value,
        [string]$Description
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return ""
    }

    $CandidatePaths = @()
    if ([System.IO.Path]::IsPathRooted($Value)) {
        $CandidatePaths += $Value
    } else {
        if ($null -ne $Report -and ($Report.PSObject.Properties.Name -contains "repository") -and -not [string]::IsNullOrWhiteSpace([string]$Report.repository)) {
            $CandidatePaths += (Join-Path ([string]$Report.repository) $Value)
        }

        if ($null -ne $ResolvedPath) {
            $CandidatePaths += (Join-Path (Split-Path -Parent $ResolvedPath.Path) $Value)
        }

        $CandidatePaths += $Value
    }

    foreach ($CandidatePath in $CandidatePaths) {
        if (Test-Path -LiteralPath $CandidatePath) {
            return (Resolve-Path -LiteralPath $CandidatePath).Path
        }
    }

    throw "$Description path does not exist: $Value"
}

if (-not (Test-Path -LiteralPath $Path)) {
    throw "P0 tooling preflight report does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Report = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @(
    "generated_at",
    "run_id",
    "status",
    "evidence_type",
    "repository",
    "started_at",
    "finished_at",
    "checks",
    "summary",
    "limitations"
)) {
    Assert-Property -Object $Report -Name $Name -Context "root"
}

if ($Report.evidence_type -ne "p0_tooling_preflight") {
    throw "evidence_type must be p0_tooling_preflight. Actual: $($Report.evidence_type)"
}

if ($Report.status -notin @("PASS", "FAIL")) {
    throw "status must be PASS or FAIL. Actual: $($Report.status)"
}

$Checks = @($Report.checks)
if ($Checks.Count -eq 0) {
    throw "checks must contain at least one preflight check."
}

foreach ($Check in $Checks) {
    foreach ($Name in @("workstream", "type", "path", "status", "actual")) {
        Assert-Property -Object $Check -Name $Name -Context "check"
    }

    if ($Check.status -notin @("PASS", "FAIL")) {
        throw "check '$($Check.path)' has unsupported status '$($Check.status)'."
    }

    $Actual = [string]$Check.actual
    if ($Actual -notin @("exists", "missing", "validator passed") -and -not $Actual.StartsWith("validator failed:")) {
        throw "check '$($Check.path)' has unsupported actual value '$($Check.actual)'."
    }

    if ($Check.status -eq "PASS" -and $Actual -notin @("exists", "validator passed")) {
        throw "PASS check '$($Check.path)' has inconsistent actual value '$($Check.actual)'."
    }

    if ($Check.status -eq "FAIL" -and $Actual -ne "missing" -and -not $Actual.StartsWith("validator failed:")) {
        throw "FAIL check '$($Check.path)' has inconsistent actual value '$($Check.actual)'."
    }
}

foreach ($Name in @("total", "passed", "failed")) {
    Assert-Property -Object $Report.summary -Name $Name -Context "summary"
}

$PassedCount = @($Checks | Where-Object { $_.status -eq "PASS" }).Count
$FailedCount = @($Checks | Where-Object { $_.status -eq "FAIL" }).Count

if ([int]$Report.summary.total -ne $Checks.Count) {
    throw "summary.total does not match checks count."
}

if ([int]$Report.summary.passed -ne $PassedCount) {
    throw "summary.passed does not match PASS checks count."
}

if ([int]$Report.summary.failed -ne $FailedCount) {
    throw "summary.failed does not match FAIL checks count."
}

if ($Report.status -eq "PASS" -and $FailedCount -ne 0) {
    throw "PASS preflight report must not contain failed checks."
}

if ($Report.status -eq "FAIL" -and $FailedCount -eq 0) {
    throw "FAIL preflight report must contain at least one failed check."
}

if (@($Report.limitations).Count -eq 0) {
    throw "limitations must contain at least one limitation."
}

if (-not [string]::IsNullOrWhiteSpace($MarkdownPath)) {
    $ResolvedMarkdownPath = Resolve-PreflightReportPath -Value $MarkdownPath -Description "P0 tooling preflight Markdown report"
    $MarkdownContent = Get-Content -Raw -LiteralPath $ResolvedMarkdownPath

    foreach ($ExpectedMarkdownText in @(
        "# P0 Tooling Preflight Report",
        "## Summary",
        "## Checks",
        "## Acceptance note",
        "## Limitations",
        [string]$Report.run_id,
        [string]$Report.status
    )) {
        if (-not $MarkdownContent.Contains($ExpectedMarkdownText)) {
            throw "P0 tooling preflight Markdown report is missing expected text '$ExpectedMarkdownText'."
        }
    }

    foreach ($Check in $Checks) {
        if (-not $MarkdownContent.Contains([string]$Check.path)) {
            throw "P0 tooling preflight Markdown report is missing check path '$($Check.path)'."
        }
    }

    foreach ($Limitation in @($Report.limitations)) {
        if (-not [string]::IsNullOrWhiteSpace([string]$Limitation) -and -not $MarkdownContent.Contains([string]$Limitation)) {
            throw "P0 tooling preflight Markdown report is missing limitation '$Limitation'."
        }
    }
}

Write-Host "P0 tooling preflight report validation passed: $ResolvedPath"
