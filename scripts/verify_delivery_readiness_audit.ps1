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
    throw "Delivery readiness audit report does not exist: $Path"
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
    "approval_packet",
    "accepted_deferrals_json",
    "accepted_deferrals",
    "p0b_state_json",
    "p0c_concurrency_json",
    "p0d_observability_json",
    "p0d_metrics_json",
    "p0d_logging_json",
    "p0d_alert_json",
    "p0d_metrics_endpoint_json",
    "p0e_full_chain_json",
    "final_regression_json",
    "bundle_json",
    "risk_register",
    "backlog",
    "checks",
    "blockers",
    "next_actions",
    "summary",
    "limitations"
)) {
    Assert-Property -Object $Report -Name $Name -Context "root"
}

if ($Report.evidence_type -ne "delivery_readiness_audit") {
    throw "evidence_type must be 'delivery_readiness_audit'. Actual: $($Report.evidence_type)"
}

if ($Report.status -notin @("READY", "NOT_READY")) {
    throw "status must be READY or NOT_READY. Actual: $($Report.status)"
}

if (-not $Report.checks -or @($Report.checks).Count -lt 1) {
    throw "checks must include at least one check."
}

$FailedChecks = @()
$DeferredChecks = @()
foreach ($Check in @($Report.checks)) {
    foreach ($Name in @("name", "expected", "actual", "status", "severity")) {
        Assert-Property -Object $Check -Name $Name -Context "check"
    }

    if ($Check.status -notin @("PASS", "FAIL", "DEFERRED")) {
        throw "check.status must be PASS, FAIL, or DEFERRED. Actual: $($Check.status)"
    }

    if ($Check.status -eq "FAIL") {
        $FailedChecks += $Check
    }

    if ($Check.status -eq "DEFERRED") {
        $DeferredChecks += $Check
        Assert-Property -Object $Check -Name "accepted_deferral" -Context "deferred check '$($Check.name)'"

        foreach ($Field in @("owner", "decision", "expires_at", "rationale", "source")) {
            Assert-Property -Object $Check.accepted_deferral -Name $Field -Context "deferred check '$($Check.name)' accepted_deferral"
            Assert-NonEmptyString -Value $Check.accepted_deferral.$Field -Context "deferred check '$($Check.name)' accepted_deferral.$Field"
        }

        if ($Check.accepted_deferral.decision -ne "ACCEPTED") {
            throw "Deferred check '$($Check.name)' accepted_deferral decision must be ACCEPTED."
        }
    }
}

foreach ($Name in @("total", "passed", "failed", "deferred", "open_risks", "tbd_owners", "unchecked_approval_items")) {
    Assert-Property -Object $Report.summary -Name $Name -Context "summary"
}

if ($FailedChecks.Count -gt 0 -and $Report.status -ne "NOT_READY") {
    throw "root status must be NOT_READY when any check failed."
}

if ($FailedChecks.Count -eq 0 -and $Report.status -ne "READY") {
    throw "root status must be READY when there are no FAIL checks."
}

if ($DeferredChecks.Count -gt 0 -and @($Report.accepted_deferrals).Count -lt 1) {
    throw "accepted_deferrals must include at least one entry when checks are DEFERRED."
}

if ($Report.summary.total -ne @($Report.checks).Count) {
    throw "summary.total does not match checks count."
}
if ($Report.summary.failed -ne $FailedChecks.Count) {
    throw "summary.failed does not match FAIL checks count."
}
if ($Report.summary.deferred -ne $DeferredChecks.Count) {
    throw "summary.deferred does not match DEFERRED checks count."
}

if ($Report.status -eq "NOT_READY" -and @($Report.blockers).Count -lt 1) {
    throw "blockers must include at least one blocker when status is NOT_READY."
}

foreach ($Blocker in @($Report.blockers)) {
    foreach ($Name in @("check", "severity", "actual", "next_action")) {
        Assert-Property -Object $Blocker -Name $Name -Context "blocker"
    }
}

if ($Report.status -eq "NOT_READY" -and @($Report.next_actions).Count -lt 1) {
    throw "next_actions must include at least one action when status is NOT_READY."
}

Write-Host "Delivery readiness audit report schema validation passed: $ResolvedPath"
