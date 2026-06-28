param(
    [string]$ReportDir = $env:P0_DELIVERY_REPORT_DIR,
    [string]$ApprovalPacket = "",
    [string]$P0BState = "",
    [string]$P0CJson = "",
    [string]$P0DJson = "",
    [string]$P0DMetricsJson = "",
    [string]$P0DLoggingJson = "",
    [string]$P0DAlertJson = "",
    [string]$P0DMetricsEndpointJson = "",
    [string]$P0EJson = "",
    [string]$FinalRegressionJson = "",
    [string]$BundleJson = "",
    [string]$AcceptedDeferralsJson = "",
    [string]$RiskRegister = "docs/planning/P0_RISK_REGISTER.md",
    [string]$Backlog = "docs/planning/P0_REMAINING_ENGINEERING_BACKLOG.md",
    [switch]$DisableReportFallback
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RepoRoot

if (-not [string]::IsNullOrWhiteSpace($AcceptedDeferralsJson)) {
    & (Join-Path $ScriptDir "verify_p0_accepted_deferrals.ps1") -Path $AcceptedDeferralsJson
}

if ([string]::IsNullOrWhiteSpace($ReportDir)) {
    $ReportDir = Join-Path $RepoRoot "reports"
}

New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$StartedAt = Get-Date -Format "o"
$RunId = "delivery_readiness_audit_$Timestamp"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"

function Add-Check {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [string]$Expected,
        [Parameter(Mandatory = $true)]
        [string]$Actual,
        [Parameter(Mandatory = $true)]
        [ValidateSet("PASS", "FAIL", "DEFERRED")]
        [string]$Status,
        [string]$Severity = "P0"
    )

    [PSCustomObject]@{
        name = $Name
        expected = $Expected
        actual = $Actual
        status = $Status
        severity = $Severity
    }
}

function Get-LatestReportFile {
    param([string]$Pattern)

    $Match = Get-ChildItem -LiteralPath $ReportDir -Filter $Pattern -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if ($Match) {
        return $Match.FullName
    }

    return ""
}

function Add-ReportStatusCheck {
    param(
        [string]$Name,
        [string]$Path,
        [string]$Expected
    )

    $Exists = -not [string]::IsNullOrWhiteSpace($Path) -and (Test-Path -LiteralPath $Path)
    $Actual = if ($Exists) { "exists" } else { "missing" }
    $Pass = $false

    if ($Exists) {
        $ReportJson = Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
        $Actual = "status=$($ReportJson.status)"
        $Pass = $ReportJson.status -eq "PASS"
    }

    $script:Checks += Add-Check `
        -Name $Name `
        -Expected $Expected `
        -Actual $Actual `
        -Status $(if ($Pass) { "PASS" } else { "FAIL" })
}

if (-not $DisableReportFallback) {
    if ([string]::IsNullOrWhiteSpace($ApprovalPacket)) { $ApprovalPacket = Get-LatestReportFile -Pattern "p0_release_approval_packet_*.md" }
    if ([string]::IsNullOrWhiteSpace($P0BState)) { $P0BState = Get-LatestReportFile -Pattern "crash_recovery_drill_*.state.json" }
    if ([string]::IsNullOrWhiteSpace($P0CJson)) { $P0CJson = Get-LatestReportFile -Pattern "concurrency_validation_*.json" }
    if ([string]::IsNullOrWhiteSpace($P0DJson)) { $P0DJson = Get-LatestReportFile -Pattern "observability_smoke_*.json" }
    if ([string]::IsNullOrWhiteSpace($P0DMetricsJson)) { $P0DMetricsJson = Get-LatestReportFile -Pattern "metrics_export_*.json" }
    if ([string]::IsNullOrWhiteSpace($P0DLoggingJson)) { $P0DLoggingJson = Get-LatestReportFile -Pattern "structured_logging_*.json" }
    if ([string]::IsNullOrWhiteSpace($P0DAlertJson)) { $P0DAlertJson = Get-LatestReportFile -Pattern "alert_drill_*.json" }
    if ([string]::IsNullOrWhiteSpace($P0DMetricsEndpointJson)) { $P0DMetricsEndpointJson = Get-LatestReportFile -Pattern "metrics_endpoint_probe_*.json" }
    if ([string]::IsNullOrWhiteSpace($P0EJson)) { $P0EJson = Get-LatestReportFile -Pattern "full_chain_drill_*.json" }
    if ([string]::IsNullOrWhiteSpace($FinalRegressionJson)) { $FinalRegressionJson = Get-LatestReportFile -Pattern "final_regression_gate_*.json" }
    if ([string]::IsNullOrWhiteSpace($BundleJson)) { $BundleJson = Get-LatestReportFile -Pattern "p0_evidence_bundle_*.json" }
}

$Checks = @()
$AcceptedDeferrals = @()
$AcceptedDeferralErrors = @()

if (-not [string]::IsNullOrWhiteSpace($AcceptedDeferralsJson)) {
    if (-not (Test-Path -LiteralPath $AcceptedDeferralsJson)) {
        $AcceptedDeferralErrors += "Accepted deferrals JSON not found: $AcceptedDeferralsJson"
    }
    else {
        try {
            $DeferralDocument = Get-Content -LiteralPath $AcceptedDeferralsJson -Raw | ConvertFrom-Json
            if ($DeferralDocument.evidence_type -ne "accepted_delivery_deferrals") {
                $AcceptedDeferralErrors += "Accepted deferrals JSON must set evidence_type to accepted_delivery_deferrals."
            }

            foreach ($Deferral in @($DeferralDocument.deferrals)) {
                $MissingFields = @()
                foreach ($Field in @("check", "owner", "decision", "expires_at", "rationale")) {
                    if ([string]::IsNullOrWhiteSpace([string]$Deferral.$Field)) {
                        $MissingFields += $Field
                    }
                }

                if ($MissingFields.Count -gt 0) {
                    $AcceptedDeferralErrors += "Accepted deferral is missing required fields: $($MissingFields -join ', ')"
                    continue
                }

                try {
                    $ExpiresAt = [DateTimeOffset]::Parse([string]$Deferral.expires_at)
                }
                catch {
                    $AcceptedDeferralErrors += "Accepted deferral for check '$($Deferral.check)' has invalid expires_at: $($Deferral.expires_at)"
                    continue
                }

                if ([string]$Deferral.decision -ne "ACCEPTED") {
                    $AcceptedDeferralErrors += "Accepted deferral for check '$($Deferral.check)' must set decision to ACCEPTED."
                    continue
                }

                if ($ExpiresAt -le [DateTimeOffset]::UtcNow) {
                    $AcceptedDeferralErrors += "Accepted deferral for check '$($Deferral.check)' expired at $($Deferral.expires_at)."
                    continue
                }

                $AcceptedDeferrals += [PSCustomObject]@{
                    check = [string]$Deferral.check
                    owner = [string]$Deferral.owner
                    decision = [string]$Deferral.decision
                    expires_at = [string]$Deferral.expires_at
                    rationale = [string]$Deferral.rationale
                    source = $AcceptedDeferralsJson
                }
            }
        }
        catch {
            $AcceptedDeferralErrors += "Failed to parse accepted deferrals JSON: $($_.Exception.Message)"
        }
    }

    $Checks += Add-Check `
        -Name "accepted_deferrals_document_valid" `
        -Expected "accepted deferrals JSON is parseable, unexpired, and uses explicit ACCEPTED decisions" `
        -Actual $(if ($AcceptedDeferralErrors.Count -eq 0) { "$($AcceptedDeferrals.Count) accepted deferrals loaded" } else { $AcceptedDeferralErrors -join "; " }) `
        -Status $(if ($AcceptedDeferralErrors.Count -eq 0) { "PASS" } else { "FAIL" })
}

$ApprovalExists = -not [string]::IsNullOrWhiteSpace($ApprovalPacket) -and (Test-Path -LiteralPath $ApprovalPacket)
$Checks += Add-Check -Name "approval_packet_exists" -Expected "latest or provided P0 release approval packet exists" -Actual $(if ($ApprovalExists) { $ApprovalPacket } else { "missing" }) -Status $(if ($ApprovalExists) { "PASS" } else { "FAIL" })

$ApprovalContent = ""
if ($ApprovalExists) {
    $ApprovalContent = Get-Content -Raw -LiteralPath $ApprovalPacket
}

$MissingEvidencePass = $ApprovalExists -and $ApprovalContent.Contains("- Missing evidence count: 0")
$Checks += Add-Check -Name "approval_packet_has_no_missing_evidence" -Expected "approval packet reports zero missing evidence artifacts" -Actual $(if ($MissingEvidencePass) { "missing evidence count is 0" } else { "missing evidence count is not 0 or packet unavailable" }) -Status $(if ($MissingEvidencePass) { "PASS" } else { "FAIL" })

$PacketReadyForReview = $ApprovalExists -and $ApprovalContent.Contains("- Packet status: READY_FOR_REVIEW")
$Checks += Add-Check -Name "approval_packet_ready_for_review" -Expected "approval packet status is READY_FOR_REVIEW" -Actual $(if ($PacketReadyForReview) { "packet status is READY_FOR_REVIEW" } else { "packet status is not READY_FOR_REVIEW or packet unavailable" }) -Status $(if ($PacketReadyForReview) { "PASS" } else { "FAIL" })

$DecisionReleased = $ApprovalExists -and -not $ApprovalContent.Contains("- Decision: HOLD")
$Checks += Add-Check -Name "approval_packet_not_on_hold" -Expected "approval packet final decision is not HOLD" -Actual $(if ($DecisionReleased) { "decision is not HOLD" } else { "decision is HOLD or packet unavailable" }) -Status $(if ($DecisionReleased) { "PASS" } else { "FAIL" })

$UncheckedApprovalItems = @()
if ($ApprovalExists) {
    $UncheckedApprovalItems = @($ApprovalContent -split "`r?`n" | Where-Object { $_ -match "^- \[ \]" })
}
$OwnerSignoffPass = $ApprovalExists -and $UncheckedApprovalItems.Count -eq 0
$Checks += Add-Check -Name "owner_signoff_complete" -Expected "approval checklist has no unchecked sign-off items" -Actual $(if ($OwnerSignoffPass) { "all approval checklist items checked" } else { "$($UncheckedApprovalItems.Count) unchecked approval checklist items" }) -Status $(if ($OwnerSignoffPass) { "PASS" } else { "FAIL" })

$RiskExists = Test-Path -LiteralPath $RiskRegister
$Checks += Add-Check -Name "risk_register_exists" -Expected "risk register exists" -Actual $(if ($RiskExists) { $RiskRegister } else { "missing" }) -Status $(if ($RiskExists) { "PASS" } else { "FAIL" })

$OpenRiskRows = @()
$TbdOwnerRows = @()
if ($RiskExists) {
    $RiskLines = Get-Content -LiteralPath $RiskRegister
    $OpenRiskRows = @($RiskLines | Where-Object { $_ -match "^\| R-P0-[0-9]+ \|" -and $_ -match "\|\s*Open\s*\|" })
    $TbdOwnerRows = @($RiskLines | Where-Object { $_ -match "^\| R-P0-[0-9]+ \|" -and $_ -match "\|\s*TBD\s*\|" })
}
$Checks += Add-Check -Name "risk_register_has_no_open_p0_risks" -Expected "risk register has no Open P0 risks" -Actual "$($OpenRiskRows.Count) open risk rows" -Status $(if ($RiskExists -and $OpenRiskRows.Count -eq 0) { "PASS" } else { "FAIL" })
$Checks += Add-Check -Name "risk_register_has_named_owners" -Expected "risk register has no TBD owners" -Actual "$($TbdOwnerRows.Count) TBD owner rows" -Status $(if ($RiskExists -and $TbdOwnerRows.Count -eq 0) { "PASS" } else { "FAIL" })

$BacklogExists = Test-Path -LiteralPath $Backlog
$Checks += Add-Check -Name "remaining_backlog_exists" -Expected "remaining engineering backlog exists" -Actual $(if ($BacklogExists) { $Backlog } else { "missing" }) -Status $(if ($BacklogExists) { "PASS" } else { "FAIL" })

$P0BStateExists = -not [string]::IsNullOrWhiteSpace($P0BState) -and (Test-Path -LiteralPath $P0BState)
$Checks += Add-Check -Name "p0b_state_evidence_exists" -Expected "P0-B recovery state evidence JSON exists" -Actual $(if ($P0BStateExists) { $P0BState } else { "missing" }) -Status $(if ($P0BStateExists) { "PASS" } else { "FAIL" })

$P0BStateActual = "missing"
$P0BStateAccepted = $false
if ($P0BStateExists) {
    $P0BStateReport = Get-Content -Raw -LiteralPath $P0BState | ConvertFrom-Json
    $StateChecks = @()
    foreach ($Run in @($P0BStateReport.runs)) {
        if ($Run.PSObject.Properties.Name -contains "state_evidence") {
            $StateChecks += $Run.state_evidence.row_level_state_diff
            $StateChecks += $Run.state_evidence.page_level_state_diff
        }
    }
    $NotVerifiedStateChecks = @($StateChecks | Where-Object { $_.status -eq "NOT_VERIFIED" })
    $FailedStateChecks = @($StateChecks | Where-Object { $_.status -ne "PASS" })
    $P0BStateActual = "verification_level=$($P0BStateReport.verification_level); state_checks=$($StateChecks.Count); not_verified_state_checks=$($NotVerifiedStateChecks.Count); failed_state_checks=$($FailedStateChecks.Count)"
    $P0BStateAccepted = $P0BStateReport.verification_level -eq "test_backed_recovery_state_contract" -and $StateChecks.Count -gt 0 -and $NotVerifiedStateChecks.Count -eq 0 -and $FailedStateChecks.Count -eq 0
}
$Checks += Add-Check -Name "p0b_recovery_state_acceptance_complete" -Expected "P0-B recovery state evidence includes verified row/page state diff checks with zero NOT_VERIFIED state checks" -Actual $P0BStateActual -Status $(if ($P0BStateAccepted) { "PASS" } else { "FAIL" })

$P0CExists = -not [string]::IsNullOrWhiteSpace($P0CJson) -and (Test-Path -LiteralPath $P0CJson)
$Checks += Add-Check -Name "p0c_concurrency_report_exists" -Expected "P0-C concurrency JSON report exists" -Actual $(if ($P0CExists) { $P0CJson } else { "missing" }) -Status $(if ($P0CExists) { "PASS" } else { "FAIL" })

$P0CAcceptanceActual = "missing"
$P0CAcceptancePass = $false
if ($P0CExists) {
    $P0CReport = Get-Content -Raw -LiteralPath $P0CJson | ConvertFrom-Json
    if ($P0CReport.PSObject.Properties.Name -contains "consistency_summary") {
        $P0CAcceptanceActual = "acceptance_status=$($P0CReport.consistency_summary.acceptance_status); required_gap_count=$($P0CReport.consistency_summary.required_gap_count); not_verified_gaps=$($P0CReport.consistency_summary.not_verified_gaps)"
        $P0CAcceptancePass = $P0CReport.consistency_summary.acceptance_status -eq "ACCEPTED" -and $P0CReport.consistency_summary.required_gap_count -eq 0 -and $P0CReport.consistency_summary.not_verified_gaps -eq 0
    }
    else {
        $P0CAcceptanceActual = "consistency_summary missing"
    }
}
$Checks += Add-Check -Name "p0c_concurrency_acceptance_complete" -Expected "P0-C concurrency consistency summary is ACCEPTED with zero required gaps and zero NOT_VERIFIED gaps" -Actual $P0CAcceptanceActual -Status $(if ($P0CAcceptancePass) { "PASS" } else { "FAIL" })

Add-ReportStatusCheck -Name "p0d_observability_smoke_passed" -Path $P0DJson -Expected "P0-D observability smoke JSON exists and has status PASS"
Add-ReportStatusCheck -Name "p0d_metrics_export_passed" -Path $P0DMetricsJson -Expected "P0-D metrics export JSON exists and has status PASS"
Add-ReportStatusCheck -Name "p0d_structured_logging_passed" -Path $P0DLoggingJson -Expected "P0-D structured logging JSON exists and has status PASS"
Add-ReportStatusCheck -Name "p0d_alert_drill_passed" -Path $P0DAlertJson -Expected "P0-D alert drill JSON exists and has status PASS"

$P0DMetricsEndpointActual = "missing"
$P0DMetricsEndpointPass = $false
if (-not [string]::IsNullOrWhiteSpace($P0DMetricsEndpointJson) -and (Test-Path -LiteralPath $P0DMetricsEndpointJson)) {
    $EndpointReport = Get-Content -Raw -LiteralPath $P0DMetricsEndpointJson | ConvertFrom-Json
    $P0DMetricsEndpointActual = "status=$($EndpointReport.status)"
    $P0DMetricsEndpointPass = $EndpointReport.status -eq "PASS"
}
$Checks += Add-Check -Name "p0d_live_metrics_endpoint_verified" -Expected "P0-D live metrics endpoint probe JSON exists and has status PASS" -Actual $P0DMetricsEndpointActual -Status $(if ($P0DMetricsEndpointPass) { "PASS" } else { "FAIL" })

Add-ReportStatusCheck -Name "p0e_full_chain_smoke_passed" -Path $P0EJson -Expected "P0-E full-chain smoke JSON exists and has status PASS"

$P0ETimedActual = "missing"
$P0ETimedPass = $false
if (-not [string]::IsNullOrWhiteSpace($P0EJson) -and (Test-Path -LiteralPath $P0EJson)) {
    $P0EReportJson = Get-Content -Raw -LiteralPath $P0EJson | ConvertFrom-Json
    if ($P0EReportJson.PSObject.Properties.Name -contains "timed_rollback_evidence") {
        $TimedEvidence = $P0EReportJson.timed_rollback_evidence
        $P0ETimedActual = "status=$($TimedEvidence.status); rollback_duration_seconds=$($TimedEvidence.rollback_duration_seconds); recovery_point=$($TimedEvidence.recovery_point); replay_boundary=$($TimedEvidence.replay_boundary); within_window=$($TimedEvidence.within_window)"
        $P0ETimedPass = $TimedEvidence.status -eq "PASS" -and $TimedEvidence.rollback_duration_seconds -ge 0 -and $TimedEvidence.within_window -eq $true -and -not [string]::IsNullOrWhiteSpace([string]$TimedEvidence.recovery_point) -and -not [string]::IsNullOrWhiteSpace([string]$TimedEvidence.replay_boundary)
    }
    else {
        $P0ETimedActual = "timed_rollback_evidence missing"
    }
}
$Checks += Add-Check -Name "p0e_timed_rollback_evidence_complete" -Expected "P0-E timed rollback evidence exists with PASS status, rollback duration within window, recovery point, and replay boundary" -Actual $P0ETimedActual -Status $(if ($P0ETimedPass) { "PASS" } else { "FAIL" })

Add-ReportStatusCheck -Name "final_regression_gate_passed" -Path $FinalRegressionJson -Expected "Final regression gate JSON exists and has status PASS"
Add-ReportStatusCheck -Name "p0_evidence_bundle_passed" -Path $BundleJson -Expected "P0 evidence bundle JSON exists and has status PASS"

$AppliedDeferrals = @()
if ($AcceptedDeferrals.Count -gt 0) {
    foreach ($Check in $Checks) {
        if ($Check.status -ne "FAIL") { continue }

        $MatchingDeferral = @($AcceptedDeferrals | Where-Object { $_.check -eq $Check.name } | Select-Object -First 1)
        if ($MatchingDeferral.Count -eq 0) { continue }

        $Deferral = $MatchingDeferral[0]
        $DeferralEvidence = [PSCustomObject]@{
            owner = $Deferral.owner
            decision = $Deferral.decision
            expires_at = $Deferral.expires_at
            rationale = $Deferral.rationale
            source = $Deferral.source
        }

        $Check.status = "DEFERRED"
        $Check | Add-Member -NotePropertyName "accepted_deferral" -NotePropertyValue $DeferralEvidence -Force
        $AppliedDeferrals += [PSCustomObject]@{
            check = $Check.name
            original_status = "FAIL"
            accepted_deferral = $DeferralEvidence
        }
    }
}

$FinishedAt = Get-Date -Format "o"
$FailedChecks = @($Checks | Where-Object { $_.status -eq "FAIL" })
$DeferredChecks = @($Checks | Where-Object { $_.status -eq "DEFERRED" })
$Status = if ($FailedChecks.Count -eq 0) { "READY" } else { "NOT_READY" }
$ExitCode = if ($Status -eq "READY") { 0 } else { 1 }

$Blockers = @($FailedChecks | ForEach-Object {
    $NextAction = switch ($_.name) {
        "approval_packet_exists" { "Generate a release approval packet with scripts/generate_p0_release_approval_packet.ps1." }
        "approval_packet_has_no_missing_evidence" { "Run the P0 evidence suite or provide all required evidence artifacts before approval packet generation." }
        "approval_packet_ready_for_review" { "Regenerate the release approval packet after all required evidence artifacts are present so packet status becomes READY_FOR_REVIEW." }
        "approval_packet_not_on_hold" { "Have the decision owner review evidence, record a non-HOLD final decision, and regenerate or update the approval packet." }
        "owner_signoff_complete" { "Complete every approval checklist item and owner sign-off entry in the release approval packet." }
        "risk_register_exists" { "Restore docs/planning/P0_RISK_REGISTER.md before delivery review." }
        "risk_register_has_no_open_p0_risks" { "Close, mitigate, or explicitly owner-accept every Open P0 risk in the risk register." }
        "risk_register_has_named_owners" { "Replace every TBD risk owner with a named accountable owner." }
        "remaining_backlog_exists" { "Restore docs/planning/P0_REMAINING_ENGINEERING_BACKLOG.md before delivery review." }
        "p0b_state_evidence_exists" { "Run scripts/crash_recovery_drill.ps1 -Runs 3 -StateEvidence and verify the generated state JSON before delivery review." }
        "p0b_recovery_state_acceptance_complete" { "Implement standalone P0-B row/page/WAL state-diff evidence so recovery state checks are verified instead of NOT_VERIFIED." }
        "p0c_concurrency_report_exists" { "Run scripts/concurrency_validation.ps1 and verify the generated JSON before delivery review." }
        "p0c_concurrency_acceptance_complete" { "Implement P0-C final-state checkers, row/range anomaly probes, and lock/deadlock metric summaries until the concurrency report is ACCEPTED." }
        "p0d_observability_smoke_passed" { "Run scripts/observability_smoke.ps1 and verify the generated P0-D observability JSON." }
        "p0d_metrics_export_passed" { "Run scripts/metrics_export_smoke.ps1 and verify the generated metrics export JSON." }
        "p0d_structured_logging_passed" { "Run scripts/structured_logging_smoke.ps1 and verify the generated structured logging JSON." }
        "p0d_alert_drill_passed" { "Run scripts/alert_drill_smoke.ps1 and verify the generated alert drill JSON." }
        "p0d_live_metrics_endpoint_verified" { "Start a server with profiling enabled, run scripts/metrics_endpoint_probe.ps1 against /metrics, and archive a PASS endpoint probe JSON." }
        "p0e_full_chain_smoke_passed" { "Run scripts/full_chain_drill_smoke.ps1 with current P0 evidence links and verify the generated P0-E JSON." }
        "p0e_timed_rollback_evidence_complete" { "Execute a timed release/rollback/data-recovery drill and archive rollback duration plus recovery point evidence." }
        "final_regression_gate_passed" { "Run scripts/final_regression_gate.ps1 and verify the generated final regression JSON before delivery review." }
        "p0_evidence_bundle_passed" { "Run scripts/verify_p0_evidence_bundle.ps1 with current-run evidence paths and verify the generated bundle JSON before delivery review." }
        "accepted_deferrals_document_valid" { "Fix accepted deferrals JSON shape, required fields, decision values, or expired entries." }
        default { "Resolve this failed delivery-readiness check before marking the project deliverable." }
    }

    [PSCustomObject]@{
        check = $_.name
        severity = $_.severity
        actual = $_.actual
        next_action = $NextAction
    }
})

$NextActions = @($Blockers | ForEach-Object { $_.next_action } | Select-Object -Unique)

$Report = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = $Status
    evidence_type = "delivery_readiness_audit"
    repository = "$RepoRoot"
    started_at = $StartedAt
    finished_at = $FinishedAt
    approval_packet = $ApprovalPacket
    accepted_deferrals_json = $AcceptedDeferralsJson
    accepted_deferrals = $AppliedDeferrals
    p0b_state_json = $P0BState
    p0c_concurrency_json = $P0CJson
    p0d_observability_json = $P0DJson
    p0d_metrics_json = $P0DMetricsJson
    p0d_logging_json = $P0DLoggingJson
    p0d_alert_json = $P0DAlertJson
    p0d_metrics_endpoint_json = $P0DMetricsEndpointJson
    p0e_full_chain_json = $P0EJson
    final_regression_json = $FinalRegressionJson
    bundle_json = $BundleJson
    risk_register = $RiskRegister
    backlog = $Backlog
    checks = $Checks
    blockers = $Blockers
    next_actions = $NextActions
    summary = [ordered]@{
        total = $Checks.Count
        passed = @($Checks | Where-Object { $_.status -eq "PASS" }).Count
        failed = $FailedChecks.Count
        deferred = $DeferredChecks.Count
        open_risks = $OpenRiskRows.Count
        tbd_owners = $TbdOwnerRows.Count
        unchecked_approval_items = $UncheckedApprovalItems.Count
    }
    limitations = @(
        "This audit is a delivery gate over existing reports and planning artifacts.",
        "It does not execute the P0 evidence suite, Go tests, live metrics probes, CI, or release drills.",
        "READY requires zero FAIL checks. DEFERRED checks require explicit accepted-deferral metadata and remain visible as residual risk."
    )
}

$Report | ConvertTo-Json -Depth 12 | Set-Content -Path $JsonPath -Encoding utf8

$CheckLines = $Checks | ForEach-Object { "- $($_.name): $($_.status) ($($_.actual))" }
$DeferredLines = if ($DeferredChecks.Count -gt 0) {
    $DeferredChecks | ForEach-Object { "- $($_.name): owner=$($_.accepted_deferral.owner); expires_at=$($_.accepted_deferral.expires_at); rationale=$($_.accepted_deferral.rationale)" }
}
else {
    @("- None")
}
$BlockerLines = if ($Blockers.Count -gt 0) {
    $Blockers | ForEach-Object { "- $($_.check): $($_.actual). Next action: $($_.next_action)" }
}
else {
    @("- None")
}
$NextActionLines = if ($NextActions.Count -gt 0) {
    $NextActions | ForEach-Object { "- $_" }
}
else {
    @("- None")
}

$Markdown = @(
    "# P0 Delivery Readiness Audit",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- Approval packet: ``$ApprovalPacket``",
    "- Accepted deferrals JSON: ``$AcceptedDeferralsJson``",
    "- P0-B state JSON: ``$P0BState``",
    "- P0-C concurrency JSON: ``$P0CJson``",
    "- P0-D observability JSON: ``$P0DJson``",
    "- P0-D metrics JSON: ``$P0DMetricsJson``",
    "- P0-D logging JSON: ``$P0DLoggingJson``",
    "- P0-D alert JSON: ``$P0DAlertJson``",
    "- P0-D metrics endpoint JSON: ``$P0DMetricsEndpointJson``",
    "- P0-E full-chain JSON: ``$P0EJson``",
    "- Final regression JSON: ``$FinalRegressionJson``",
    "- P0 evidence bundle JSON: ``$BundleJson``",
    "- Risk register: ``$RiskRegister``",
    "- Backlog: ``$Backlog``",
    "- JSON report: ``$JsonPath``",
    "- Total checks: $($Checks.Count)",
    "- Passed: $(@($Checks | Where-Object { $_.status -eq "PASS" }).Count)",
    "- Failed: $($FailedChecks.Count)",
    "- Deferred: $($DeferredChecks.Count)",
    "- Open risks: $($OpenRiskRows.Count)",
    "- TBD owners: $($TbdOwnerRows.Count)",
    "- Unchecked approval items: $($UncheckedApprovalItems.Count)",
    "",
    "## Checks",
    "",
    $CheckLines,
    "",
    "## Accepted residual-risk deferrals",
    "",
    $DeferredLines,
    "",
    "## Delivery blockers",
    "",
    $BlockerLines,
    "",
    "## Next actions",
    "",
    $NextActionLines,
    "",
    "## Result",
    "",
    "- Exit code: $ExitCode",
    "- Result: $Status"
)

$Markdown | Set-Content -Path $MarkdownPath -Encoding utf8

Write-Host "P0 delivery readiness audit status: $Status"
Write-Host "Markdown report written: $MarkdownPath"
Write-Host "JSON report written: $JsonPath"

exit $ExitCode
