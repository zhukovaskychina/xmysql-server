param(
    [string]$ReportDir = $env:P0_DELIVERY_CANDIDATE_REPORT_DIR,
    [int]$RecoveryRuns = 3,
    [int]$ConcurrencyRuns = 1,
    [int]$RollbackWindowSeconds = 300,
    [string]$AcceptedDeferralsJson = "",
    [string]$OwnerSignoffJson = "",
    [switch]$DisableFocusedP0BStateSnapshotEvidence,
    [switch]$DisableFocusedP0CConsistencyEvidence,
    [switch]$DisableFocusedP0DMetricsEndpointEvidence,
    [switch]$DisableFocusedP0ETimedRollbackEvidence
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RepoRoot

if ([string]::IsNullOrWhiteSpace($ReportDir)) {
    $ReportDir = Join-Path $RepoRoot "reports"
}

if ($RecoveryRuns -lt 1) {
    throw "RecoveryRuns must be greater than or equal to 1."
}

if ($ConcurrencyRuns -lt 1) {
    throw "ConcurrencyRuns must be greater than or equal to 1."
}

if ($RollbackWindowSeconds -lt 1) {
    throw "RollbackWindowSeconds must be greater than or equal to 1."
}

New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$StartedAt = Get-Date -Format "o"
$RunId = "p0_delivery_candidate_$Timestamp"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"
$RawLogPath = Join-Path $ReportDir "$RunId.log"

$SuiteArgs = @(
    "-ReportDir", $ReportDir,
    "-RecoveryRuns", "$RecoveryRuns",
    "-ConcurrencyRuns", "$ConcurrencyRuns",
    "-RollbackWindowSeconds", "$RollbackWindowSeconds"
)

$EnabledEvidence = @()
$DisabledEvidence = @()

if ($DisableFocusedP0BStateSnapshotEvidence) {
    $DisabledEvidence += "P0-B focused state snapshot evidence"
}
else {
    $SuiteArgs += "-GenerateP0BStateSnapshotEvidence"
    $EnabledEvidence += "P0-B focused state snapshot evidence"
}

if ($DisableFocusedP0CConsistencyEvidence) {
    $DisabledEvidence += "P0-C focused consistency evidence"
}
else {
    $SuiteArgs += "-GenerateP0CConsistencyEvidence"
    $EnabledEvidence += "P0-C focused consistency evidence"
}

if ($DisableFocusedP0DMetricsEndpointEvidence) {
    $DisabledEvidence += "P0-D focused metrics endpoint evidence"
}
else {
    $SuiteArgs += "-GenerateP0DMetricsEndpointEvidence"
    $EnabledEvidence += "P0-D focused metrics endpoint evidence"
}

if ($DisableFocusedP0ETimedRollbackEvidence) {
    $DisabledEvidence += "P0-E focused timed rollback evidence"
}
else {
    $SuiteArgs += "-GenerateP0ETimedRollbackEvidence"
    $EnabledEvidence += "P0-E focused timed rollback evidence"
}

if (-not [string]::IsNullOrWhiteSpace($AcceptedDeferralsJson)) {
    $SuiteArgs += @("-AcceptedDeferralsJson", $AcceptedDeferralsJson)
}

if (-not [string]::IsNullOrWhiteSpace($OwnerSignoffJson)) {
    $SuiteArgs += @("-OwnerSignoffJson", $OwnerSignoffJson)
}

$InitialGovernanceReviewMode = if (-not [string]::IsNullOrWhiteSpace($OwnerSignoffJson)) {
    "final_approval"
} elseif (-not [string]::IsNullOrWhiteSpace($AcceptedDeferralsJson)) {
    "deferral_review"
} else {
    "risk_register_only"
}

@(
    "=== P0 delivery candidate $StartedAt ===",
    "run_id: $RunId",
    "repo: $RepoRoot",
    "report_dir: $ReportDir",
    "suite_args: $($SuiteArgs -join ' ')",
    "enabled_focused_evidence: $($EnabledEvidence -join '; ')",
    "disabled_focused_evidence: $($DisabledEvidence -join '; ')",
    "governance_review_mode: $InitialGovernanceReviewMode",
    "accepted_deferrals_json: $AcceptedDeferralsJson",
    "owner_signoff_json: $OwnerSignoffJson",
    ""
) | Set-Content -LiteralPath $RawLogPath -Encoding utf8

$ExitCode = 0
$Failure = ""
$GovernanceGateAttempted = $false
try {
    if (-not [string]::IsNullOrWhiteSpace($OwnerSignoffJson) -or -not [string]::IsNullOrWhiteSpace($AcceptedDeferralsJson)) {
        $GovernanceGateAttempted = $true
        $GovernanceArgs = @("-ReportDir", $ReportDir)
        if (-not [string]::IsNullOrWhiteSpace($AcceptedDeferralsJson)) {
            $GovernanceArgs += @("-AcceptedDeferralsJson", $AcceptedDeferralsJson)
        }
        if (-not [string]::IsNullOrWhiteSpace($OwnerSignoffJson)) {
            $GovernanceArgs += @("-OwnerSignoffJson", $OwnerSignoffJson, "-RequireOwnerSignoff")
        }

        & (Join-Path $ScriptDir "verify_p0_governance_gate.ps1") @GovernanceArgs 2>&1 |
            Tee-Object -FilePath $RawLogPath -Append
    }

    & (Join-Path $ScriptDir "run_p0_evidence_suite.ps1") @SuiteArgs 2>&1 |
        Tee-Object -FilePath $RawLogPath -Append
    $ExitCode = $LASTEXITCODE
}
catch {
    $ExitCode = if ($LASTEXITCODE -ne 0) { $LASTEXITCODE } else { 1 }
    $Failure = $_.Exception.Message
    "candidate_error: $Failure" | Add-Content -LiteralPath $RawLogPath -Encoding utf8
}

$FinishedAt = Get-Date -Format "o"
$Status = if ($ExitCode -eq 0) { "PASS" } else { "FAIL" }

if ($Status -eq "FAIL" -and [string]::IsNullOrWhiteSpace($Failure)) {
    $Failure = "Underlying P0 evidence suite exited with code $ExitCode. See raw log for details."
}

$LatestSummary = Get-ChildItem -LiteralPath $ReportDir -Filter "p0_evidence_suite_*.summary.json" -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -ge ([DateTime]::Parse($StartedAt)).AddSeconds(-2) } |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

$SuiteSummaryJson = if ($LatestSummary) { $LatestSummary.FullName } else { "" }
$SuiteSummaryMarkdown = if ($SuiteSummaryJson) { [System.IO.Path]::ChangeExtension($SuiteSummaryJson, ".md") } else { "" }

function Get-LatestCurrentRunArtifact {
    param([string]$Pattern)

    $Match = Get-ChildItem -LiteralPath $ReportDir -Filter $Pattern -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -ge ([DateTime]::Parse($StartedAt)).AddSeconds(-2) } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if ($Match) {
        return $Match.FullName
    }

    return ""
}

$DeliveryReadinessJson = Get-LatestCurrentRunArtifact -Pattern "delivery_readiness_audit_*.json"
$DeliveryReadinessMarkdown = if ($DeliveryReadinessJson) { [System.IO.Path]::ChangeExtension($DeliveryReadinessJson, ".md") } else { "" }
$DeliveryRemediationJson = Get-LatestCurrentRunArtifact -Pattern "delivery_remediation_packet_*.json"
$DeliveryRemediationMarkdown = if ($DeliveryRemediationJson) { [System.IO.Path]::ChangeExtension($DeliveryRemediationJson, ".md") } else { "" }
$GovernanceGateJson = Get-LatestCurrentRunArtifact -Pattern "p0_governance_gate_*.json"
$GovernanceGateMarkdown = if ($GovernanceGateJson) { [System.IO.Path]::ChangeExtension($GovernanceGateJson, ".md") } else { "" }
$GovernanceGateStatus = if (-not $GovernanceGateAttempted) {
    "NOT_RUN"
} elseif (-not [string]::IsNullOrWhiteSpace($GovernanceGateJson)) {
    "PASS"
} elseif ($Status -eq "FAIL") {
    "FAILED_OR_NOT_COMPLETED"
} else {
    "MISSING"
}
$GovernanceReviewMode = if (-not [string]::IsNullOrWhiteSpace($OwnerSignoffJson)) {
    "final_approval"
} elseif (-not [string]::IsNullOrWhiteSpace($AcceptedDeferralsJson)) {
    "deferral_review"
} else {
    "risk_register_only"
}
$GovernanceGateRequired = -not [string]::IsNullOrWhiteSpace($OwnerSignoffJson)
$RecommendedNextAction = if ($Status -eq "PASS") {
    "Proceed to release approval review with the generated approval packet, delivery readiness audit, governance evidence, and suite summary."
} elseif ($GovernanceGateStatus -eq "FAILED_OR_NOT_COMPLETED") {
    "Fix governance inputs or risk register blockers, then rerun the delivery candidate with the corrected signoff or deferral files."
} elseif (-not [string]::IsNullOrWhiteSpace($DeliveryRemediationJson)) {
    "Review the delivery remediation packet and resolve the listed work items before rerunning the delivery candidate."
} elseif (-not [string]::IsNullOrWhiteSpace($SuiteSummaryJson)) {
    "Review the suite summary and raw log to identify the failing workstream before rerunning the delivery candidate."
} else {
    "Review the raw candidate log and failure field, then rerun the delivery candidate after fixing the first reported failure."
}

$Report = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = $Status
    evidence_type = "p0_delivery_candidate"
    repository = "$RepoRoot"
    report_dir = "$ReportDir"
    started_at = $StartedAt
    finished_at = $FinishedAt
    exit_code = $ExitCode
    enabled_focused_evidence = $EnabledEvidence
    disabled_focused_evidence = $DisabledEvidence
    accepted_deferrals_json = $AcceptedDeferralsJson
    owner_signoff_json = $OwnerSignoffJson
    governance_review_mode = $GovernanceReviewMode
    governance_gate_required = $GovernanceGateRequired
    governance_gate_attempted = $GovernanceGateAttempted
    governance_gate_status = $GovernanceGateStatus
    governance_gate_json = $GovernanceGateJson
    governance_gate_markdown = $GovernanceGateMarkdown
    recommended_next_action = $RecommendedNextAction
    suite_summary_json = $SuiteSummaryJson
    suite_summary_markdown = $SuiteSummaryMarkdown
    delivery_readiness_json = $DeliveryReadinessJson
    delivery_readiness_markdown = $DeliveryReadinessMarkdown
    delivery_remediation_json = $DeliveryRemediationJson
    delivery_remediation_markdown = $DeliveryRemediationMarkdown
    raw_log = $RawLogPath
    candidate_markdown = $MarkdownPath
    failure = $Failure
    limitations = @(
        "This runner invokes the P0 evidence suite with focused evidence generation defaults.",
        "It does not relax delivery readiness gates.",
        "A PASS result means the underlying suite and delivery readiness audit completed successfully for this candidate run."
    )
}

$Report | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $JsonPath -Encoding utf8

$EnabledLines = if ($EnabledEvidence.Count -gt 0) { $EnabledEvidence | ForEach-Object { "- $_" } } else { @("- None") }
$DisabledLines = if ($DisabledEvidence.Count -gt 0) { $DisabledEvidence | ForEach-Object { "- $_" } } else { @("- None") }

$Markdown = @(
    "# P0 Delivery Candidate Run",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Run ID: $RunId",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- Report directory: ``$ReportDir``",
    "- Exit code: $ExitCode",
    "- Suite summary JSON: ``$SuiteSummaryJson``",
    "- Delivery readiness JSON: ``$DeliveryReadinessJson``",
    "- Delivery remediation JSON: ``$DeliveryRemediationJson``",
    "- Accepted deferrals JSON: ``$AcceptedDeferralsJson``",
    "- Owner signoff JSON: ``$OwnerSignoffJson``",
    "- Governance review mode: $GovernanceReviewMode",
    "- Governance gate required: $GovernanceGateRequired",
    "- Governance gate attempted: $GovernanceGateAttempted",
    "- Governance gate status: $GovernanceGateStatus",
    "- Governance gate JSON: ``$GovernanceGateJson``",
    "- Governance gate Markdown: ``$GovernanceGateMarkdown``",
    "- Recommended next action: $RecommendedNextAction",
    "- Raw log: ``$RawLogPath``",
    "- JSON report: ``$JsonPath``",
    "",
    "## Enabled focused evidence",
    "",
    $EnabledLines,
    "",
    "## Disabled focused evidence",
    "",
    $DisabledLines,
    "",
    "## Result",
    "",
    "- Result: $Status"
)

$Markdown | Set-Content -LiteralPath $MarkdownPath -Encoding utf8

& (Join-Path $ScriptDir "verify_p0_delivery_candidate.ps1") -Path $JsonPath

Write-Host "P0 delivery candidate status: $Status"
Write-Host "Candidate report written: $MarkdownPath"
Write-Host "Candidate JSON written: $JsonPath"
Write-Host "Candidate log written: $RawLogPath"

exit $ExitCode
