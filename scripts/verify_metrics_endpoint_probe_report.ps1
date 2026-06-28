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

if (-not (Test-Path -LiteralPath $Path)) {
    throw "Metrics endpoint probe report does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Report = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @("generated_at", "run_id", "status", "evidence_type", "repository", "metrics_url", "prometheus_textfile", "raw_log", "markdown_report", "checks", "limitations")) {
    Assert-Property -Object $Report -Name $Name -Context "root"
}

if ($Report.evidence_type -ne "metrics_endpoint_probe") {
    throw "evidence_type must be 'metrics_endpoint_probe'. Actual: $($Report.evidence_type)"
}

if ($Report.status -notin @("PASS", "FAIL")) {
    throw "status must be PASS or FAIL. Actual: $($Report.status)"
}

if (-not $Report.checks -or $Report.checks.Count -lt 1) {
    throw "checks must include at least one check."
}

$AnyCheckFailed = $false
foreach ($Check in @($Report.checks)) {
    foreach ($Name in @("name", "expected", "actual", "status")) {
        Assert-Property -Object $Check -Name $Name -Context "check"
    }
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

Write-Host "Metrics endpoint probe report schema validation passed: $ResolvedPath"
