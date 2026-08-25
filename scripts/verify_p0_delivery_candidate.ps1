param(
    [Parameter(Mandatory = $true)]
    [string]$Path
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")

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

function Resolve-ReportPath {
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

    if (-not (Test-Path -LiteralPath $Value)) {
        throw "$Description path does not exist: $Value"
    }
}

if (-not (Test-Path -LiteralPath $Path)) {
    throw "P0 delivery candidate report does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Report = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @(
    "generated_at",
    "run_id",
    "status",
    "evidence_type",
    "repository",
    "report_dir",
    "started_at",
    "finished_at",
    "exit_code",
    "enabled_focused_evidence",
    "disabled_focused_evidence",
    "accepted_deferrals_json",
    "owner_signoff_json",
    "governance_review_mode",
    "governance_gate_required",
    "governance_gate_attempted",
    "governance_gate_status",
    "governance_gate_json",
    "governance_gate_markdown",
    "recommended_next_action",
    "suite_summary_json",
    "suite_summary_markdown",
    "delivery_readiness_json",
    "delivery_readiness_markdown",
    "delivery_remediation_json",
    "delivery_remediation_markdown",
    "raw_log",
    "candidate_markdown",
    "failure",
    "limitations"
)) {
    Assert-Property -Object $Report -Name $Name -Context "root"
}

if ($Report.evidence_type -ne "p0_delivery_candidate") {
    throw "evidence_type must be 'p0_delivery_candidate'. Actual: $($Report.evidence_type)"
}

if ($Report.status -notin @("PASS", "FAIL")) {
    throw "status must be PASS or FAIL. Actual: $($Report.status)"
}

if ($Report.status -eq "PASS" -and [int]$Report.exit_code -ne 0) {
    throw "PASS candidate must have exit_code 0. Actual: $($Report.exit_code)"
}

if ($Report.status -eq "FAIL" -and [int]$Report.exit_code -eq 0) {
    throw "FAIL candidate must have a non-zero exit_code."
}

if ($Report.governance_review_mode -notin @("risk_register_only", "deferral_review", "final_approval")) {
    throw "governance_review_mode must be risk_register_only, deferral_review, or final_approval. Actual: $($Report.governance_review_mode)"
}

if ($Report.governance_gate_required -isnot [bool]) {
    throw "governance_gate_required must be a boolean."
}

if ($Report.governance_gate_attempted -isnot [bool]) {
    throw "governance_gate_attempted must be a boolean."
}

if ($Report.governance_gate_status -notin @("NOT_RUN", "PASS", "FAILED_OR_NOT_COMPLETED", "MISSING")) {
    throw "governance_gate_status must be NOT_RUN, PASS, FAILED_OR_NOT_COMPLETED, or MISSING. Actual: $($Report.governance_gate_status)"
}

if ([string]::IsNullOrWhiteSpace([string]$Report.recommended_next_action)) {
    throw "recommended_next_action must not be empty."
}

if (-not [string]::IsNullOrWhiteSpace([string]$Report.owner_signoff_json)) {
    if ($Report.governance_review_mode -ne "final_approval") {
        throw "Candidates with owner_signoff_json must use governance_review_mode final_approval."
    }

    if ($Report.governance_gate_required -ne $true) {
        throw "Candidates with owner_signoff_json must set governance_gate_required to true."
    }
}

if ([string]::IsNullOrWhiteSpace([string]$Report.owner_signoff_json) -and $Report.governance_gate_required -eq $true) {
    throw "governance_gate_required cannot be true without owner_signoff_json."
}

if ($Report.governance_gate_required -eq $true -and $Report.governance_gate_attempted -ne $true) {
    throw "Candidates requiring governance gate must set governance_gate_attempted to true."
}

if ($Report.status -eq "PASS" -and $Report.governance_gate_required -eq $true -and [string]::IsNullOrWhiteSpace([string]$Report.governance_gate_json)) {
    throw "PASS candidates requiring governance gate must include governance_gate_json."
}

if ($Report.status -eq "PASS" -and (-not [string]::IsNullOrWhiteSpace([string]$Report.owner_signoff_json) -or -not [string]::IsNullOrWhiteSpace([string]$Report.accepted_deferrals_json))) {
    if ($Report.governance_gate_status -ne "PASS") {
        throw "PASS candidates with governance input files must have governance_gate_status PASS."
    }

    if ([string]::IsNullOrWhiteSpace([string]$Report.governance_gate_json)) {
        throw "PASS candidates with governance input files must include governance_gate_json."
    }

    if ([string]::IsNullOrWhiteSpace([string]$Report.governance_gate_markdown)) {
        throw "PASS candidates with governance input files must include governance_gate_markdown."
    }
}

if ($Report.status -eq "PASS" -and $Report.governance_gate_status -eq "FAILED_OR_NOT_COMPLETED") {
    throw "PASS candidates cannot have governance_gate_status FAILED_OR_NOT_COMPLETED."
}

if ($Report.status -eq "PASS" -and $Report.governance_gate_status -eq "MISSING") {
    throw "PASS candidates cannot have governance_gate_status MISSING."
}

if ($Report.governance_gate_status -eq "PASS" -and [string]::IsNullOrWhiteSpace([string]$Report.governance_gate_json)) {
    throw "governance_gate_status PASS requires governance_gate_json."
}

if ($Report.governance_gate_status -eq "PASS" -and [string]::IsNullOrWhiteSpace([string]$Report.governance_gate_markdown)) {
    throw "governance_gate_status PASS requires governance_gate_markdown."
}

if ($Report.governance_gate_status -eq "NOT_RUN" -and $Report.governance_gate_attempted -eq $true) {
    throw "governance_gate_status NOT_RUN is inconsistent with governance_gate_attempted true."
}

if ((-not [string]::IsNullOrWhiteSpace([string]$Report.owner_signoff_json) -or -not [string]::IsNullOrWhiteSpace([string]$Report.accepted_deferrals_json)) -and $Report.governance_gate_attempted -ne $true) {
    throw "Candidates with governance input files must set governance_gate_attempted to true."
}

if ([string]::IsNullOrWhiteSpace([string]$Report.owner_signoff_json) -and -not [string]::IsNullOrWhiteSpace([string]$Report.accepted_deferrals_json) -and $Report.governance_review_mode -ne "deferral_review") {
    throw "Candidates with accepted_deferrals_json and no owner_signoff_json must use governance_review_mode deferral_review."
}

if ([string]::IsNullOrWhiteSpace([string]$Report.owner_signoff_json) -and [string]::IsNullOrWhiteSpace([string]$Report.accepted_deferrals_json) -and $Report.governance_review_mode -ne "risk_register_only") {
    throw "Candidates without governance input files must use governance_review_mode risk_register_only."
}

$RawLogPath = Resolve-ReportPath -Value $Report.raw_log -Description "raw_log"
$CandidateMarkdownPath = Resolve-ReportPath -Value $Report.candidate_markdown -Description "candidate_markdown"
$GovernanceGateJsonPath = Resolve-ReportPath -Value $Report.governance_gate_json -Description "governance_gate_json"
$GovernanceGateMarkdownPath = Resolve-ReportPath -Value $Report.governance_gate_markdown -Description "governance_gate_markdown"
$SuiteSummaryJsonPath = Resolve-ReportPath -Value $Report.suite_summary_json -Description "suite_summary_json"
$SuiteSummaryMarkdownPath = Resolve-ReportPath -Value $Report.suite_summary_markdown -Description "suite_summary_markdown"
$DeliveryReadinessJsonPath = Resolve-ReportPath -Value $Report.delivery_readiness_json -Description "delivery_readiness_json"
$DeliveryReadinessMarkdownPath = Resolve-ReportPath -Value $Report.delivery_readiness_markdown -Description "delivery_readiness_markdown"
$DeliveryRemediationJsonPath = Resolve-ReportPath -Value $Report.delivery_remediation_json -Description "delivery_remediation_json"
$DeliveryRemediationMarkdownPath = Resolve-ReportPath -Value $Report.delivery_remediation_markdown -Description "delivery_remediation_markdown"
$AcceptedDeferralsPath = ""
$OwnerSignoffPath = ""

if ($Report.status -eq "PASS") {
    $AcceptedDeferralsPath = Resolve-ReportPath -Value $Report.accepted_deferrals_json -Description "accepted_deferrals_json"
    $OwnerSignoffPath = Resolve-ReportPath -Value $Report.owner_signoff_json -Description "owner_signoff_json"
}

if ($Report.status -eq "PASS" -and -not [string]::IsNullOrWhiteSpace([string]$Report.accepted_deferrals_json)) {
    & (Join-Path $ScriptDir "verify_p0_accepted_deferrals.ps1") -Path $AcceptedDeferralsPath
}

if ($Report.status -eq "PASS" -and -not [string]::IsNullOrWhiteSpace([string]$Report.owner_signoff_json)) {
    & (Join-Path $ScriptDir "verify_p0_owner_signoff.ps1") -Path $OwnerSignoffPath
    & (Join-Path $ScriptDir "verify_p0_risk_register.ps1") -Path (Join-Path $RepoRoot "docs/planning/P0_RISK_REGISTER.md")
}

if ($Report.status -eq "PASS" -and -not [string]::IsNullOrWhiteSpace([string]$Report.governance_gate_json)) {
    & (Join-Path $ScriptDir "verify_p0_governance_gate_report.ps1") -Path $GovernanceGateJsonPath -MarkdownPath $GovernanceGateMarkdownPath
}

if ($Report.status -eq "PASS") {
    foreach ($Name in @("raw_log", "suite_summary_json", "suite_summary_markdown", "delivery_readiness_json", "delivery_readiness_markdown", "delivery_remediation_json", "delivery_remediation_markdown")) {
        if ([string]::IsNullOrWhiteSpace([string]$Report.$Name)) {
            throw "PASS candidate must include $Name."
        }
    }
}

if ([string]::IsNullOrWhiteSpace([string]$Report.raw_log)) {
    throw "Candidate report must include raw_log."
}

if ([string]::IsNullOrWhiteSpace([string]$Report.candidate_markdown)) {
    throw "Candidate report must include candidate_markdown."
}

$CandidateMarkdownContent = Get-Content -Raw -LiteralPath $CandidateMarkdownPath
if (-not $CandidateMarkdownContent.Contains("# P0 Delivery Candidate Run")) {
    throw "Candidate Markdown report is missing expected title."
}

if (-not $CandidateMarkdownContent.Contains([string]$Report.run_id)) {
    throw "Candidate Markdown report does not match run_id '$($Report.run_id)'."
}

foreach ($ExpectedMarkdownValue in @(
    [string]$Report.status,
    [string]$Report.raw_log,
    [string]$Report.recommended_next_action,
    [string]$Report.governance_review_mode,
    [string]$Report.governance_gate_status
)) {
    if (-not [string]::IsNullOrWhiteSpace($ExpectedMarkdownValue) -and -not $CandidateMarkdownContent.Contains($ExpectedMarkdownValue)) {
        throw "Candidate Markdown report is missing expected value '$ExpectedMarkdownValue'."
    }
}

$RawLogContent = Get-Content -Raw -LiteralPath $RawLogPath
foreach ($ExpectedRawLogText in @(
    "=== P0 delivery candidate",
    "run_id:",
    "repo:",
    "report_dir:",
    "suite_args:",
    "governance_review_mode:",
    "accepted_deferrals_json:",
    "owner_signoff_json:"
)) {
    if (-not $RawLogContent.Contains($ExpectedRawLogText)) {
        throw "Candidate raw log is missing expected header field '$ExpectedRawLogText'."
    }
}

if (-not $RawLogContent.Contains([string]$Report.started_at)) {
    throw "Candidate raw log does not match candidate started_at '$($Report.started_at)'."
}

foreach ($ExpectedRawLogValue in @(
    [string]$Report.run_id,
    [string]$Report.repository,
    [string]$Report.report_dir,
    [string]$Report.governance_review_mode,
    [string]$Report.accepted_deferrals_json,
    [string]$Report.owner_signoff_json
)) {
    if (-not [string]::IsNullOrWhiteSpace($ExpectedRawLogValue) -and -not $RawLogContent.Contains($ExpectedRawLogValue)) {
        throw "Candidate raw log is missing expected value '$ExpectedRawLogValue'."
    }
}

$FocusedEvidenceEntries = @($Report.enabled_focused_evidence) + @($Report.disabled_focused_evidence)
foreach ($FocusedEvidence in $FocusedEvidenceEntries) {
    if (-not [string]::IsNullOrWhiteSpace([string]$FocusedEvidence) -and -not $RawLogContent.Contains([string]$FocusedEvidence)) {
        throw "Candidate raw log is missing focused evidence entry '$FocusedEvidence'."
    }
}

if ($Report.status -eq "FAIL") {
    $HasFailureContext = -not [string]::IsNullOrWhiteSpace([string]$Report.failure) -or
        -not [string]::IsNullOrWhiteSpace([string]$Report.suite_summary_json) -or
        -not [string]::IsNullOrWhiteSpace([string]$Report.delivery_readiness_json) -or
        -not [string]::IsNullOrWhiteSpace([string]$Report.delivery_remediation_json)

    if (-not $HasFailureContext) {
        throw "FAIL candidate must include failure context, suite summary, delivery readiness, or remediation output."
    }
}

if (@($Report.enabled_focused_evidence).Count -eq 0 -and @($Report.disabled_focused_evidence).Count -eq 0) {
    throw "Candidate report must record enabled or disabled focused evidence."
}

Write-Host "P0 delivery candidate report validation passed: $ResolvedPath"
