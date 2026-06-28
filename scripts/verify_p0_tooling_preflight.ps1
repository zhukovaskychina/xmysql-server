param(
    [string]$ReportDir = $env:P0_PREFLIGHT_REPORT_DIR
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RepoRoot

if ([string]::IsNullOrWhiteSpace($ReportDir)) {
    $ReportDir = Join-Path $RepoRoot "reports"
}

New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$StartedAt = Get-Date -Format "o"
$RunId = "p0_tooling_preflight_$Timestamp"
$MarkdownPath = Join-Path $ReportDir "$RunId.md"
$JsonPath = Join-Path $ReportDir "$RunId.json"

$RequiredArtifacts = @(
    [PSCustomObject]@{ Workstream = "P0-A"; Type = "doc"; Path = "docs/planning/P0_A_BASE_IMPLEMENTATION_PLAN.md" },
    [PSCustomObject]@{ Workstream = "P0-B"; Type = "doc"; Path = "docs/planning/P0_B_CRASH_RECOVERY_DRILL_RUNBOOK.md" },
    [PSCustomObject]@{ Workstream = "P0-B"; Type = "script"; Path = "scripts/crash_recovery_drill.ps1" },
    [PSCustomObject]@{ Workstream = "P0-B"; Type = "script"; Path = "scripts/crash_recovery_drill.sh" },
    [PSCustomObject]@{ Workstream = "P0-B"; Type = "script"; Path = "scripts/verify_crash_recovery_state_evidence.ps1" },
    [PSCustomObject]@{ Workstream = "P0-B"; Type = "script"; Path = "scripts/generate_p0b_state_diff_artifact.ps1" },
    [PSCustomObject]@{ Workstream = "P0-B"; Type = "script"; Path = "scripts/verify_p0b_state_diff_artifact.ps1" },
    [PSCustomObject]@{ Workstream = "P0-B"; Type = "script"; Path = "scripts/generate_p0b_state_snapshot_evidence.ps1" },
    [PSCustomObject]@{ Workstream = "P0-B"; Type = "test"; Path = "server/innodb/manager/p0b_state_snapshot_export_test.go" },
    [PSCustomObject]@{ Workstream = "P0-B"; Type = "template"; Path = "docs/planning/P0_STATE_DIFF_ARTIFACT_TEMPLATE.json" },
    [PSCustomObject]@{ Workstream = "P0-C"; Type = "doc"; Path = "docs/planning/P0_C_CONCURRENCY_VALIDATION_PLAN.md" },
    [PSCustomObject]@{ Workstream = "P0-C"; Type = "script"; Path = "scripts/concurrency_validation.ps1" },
    [PSCustomObject]@{ Workstream = "P0-C"; Type = "script"; Path = "scripts/concurrency_validation.sh" },
    [PSCustomObject]@{ Workstream = "P0-C"; Type = "script"; Path = "scripts/verify_concurrency_validation_report.ps1" },
    [PSCustomObject]@{ Workstream = "P0-C"; Type = "script"; Path = "scripts/generate_p0c_consistency_evidence.ps1" },
    [PSCustomObject]@{ Workstream = "P0-C"; Type = "script"; Path = "scripts/verify_p0c_consistency_evidence.ps1" },
    [PSCustomObject]@{ Workstream = "P0-C"; Type = "test"; Path = "server/innodb/manager/p0c_consistency_evidence_export_test.go" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "doc"; Path = "docs/planning/P0_D_OBSERVABILITY_PLAN.md" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "doc"; Path = "docs/observability/metrics_catalog.md" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "doc"; Path = "docs/observability/metrics_registry_usage.md" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "doc"; Path = "docs/observability/metrics_http_handler_usage.md" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "doc"; Path = "docs/observability/metrics_runtime_recorder_usage.md" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "sample"; Path = "docs/observability/sample_slow_query.log" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "sample"; Path = "docs/observability/sample_error.log" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "config"; Path = "deploy/alerts/xmysql-p0-alerts.yml" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "script"; Path = "scripts/observability_smoke.ps1" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "script"; Path = "scripts/verify_observability_report.ps1" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "command"; Path = "cmd/p0_metrics_export/main.go" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "command"; Path = "cmd/p0_metrics_endpoint/main.go" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "script"; Path = "scripts/metrics_export_smoke.ps1" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "script"; Path = "scripts/verify_metrics_export_report.ps1" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "script"; Path = "scripts/generate_p0d_metrics_endpoint_evidence.ps1" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "script"; Path = "scripts/metrics_endpoint_probe.ps1" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "script"; Path = "scripts/verify_metrics_endpoint_probe_report.ps1" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "command"; Path = "cmd/p0_structured_log_export/main.go" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "doc"; Path = "docs/observability/structured_logging_usage.md" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "script"; Path = "scripts/structured_logging_smoke.ps1" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "script"; Path = "scripts/verify_structured_logging_report.ps1" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "script"; Path = "scripts/alert_drill_smoke.ps1" },
    [PSCustomObject]@{ Workstream = "P0-D"; Type = "script"; Path = "scripts/verify_alert_drill_report.ps1" },
    [PSCustomObject]@{ Workstream = "P0-E"; Type = "doc"; Path = "docs/planning/P0_E_RELEASE_ROLLBACK_PLAN.md" },
    [PSCustomObject]@{ Workstream = "P0-E"; Type = "runbook"; Path = "docs/operations/gray_release_runbook.md" },
    [PSCustomObject]@{ Workstream = "P0-E"; Type = "runbook"; Path = "docs/operations/rollback_runbook.md" },
    [PSCustomObject]@{ Workstream = "P0-E"; Type = "runbook"; Path = "docs/operations/data_recovery_runbook.md" },
    [PSCustomObject]@{ Workstream = "P0-E"; Type = "template"; Path = "docs/operations/full_chain_drill_record_template.md" },
    [PSCustomObject]@{ Workstream = "P0-E"; Type = "script"; Path = "scripts/full_chain_drill_smoke.ps1" },
    [PSCustomObject]@{ Workstream = "P0-E"; Type = "script"; Path = "scripts/verify_full_chain_drill_report.ps1" },
    [PSCustomObject]@{ Workstream = "P0-E"; Type = "script"; Path = "scripts/generate_p0e_timed_rollback_evidence.ps1" },
    [PSCustomObject]@{ Workstream = "P0-E"; Type = "script"; Path = "scripts/verify_p0e_timed_rollback_evidence.ps1" },
    [PSCustomObject]@{ Workstream = "Bundle"; Type = "doc"; Path = "docs/planning/P0_EVIDENCE_BUNDLE.md" },
    [PSCustomObject]@{ Workstream = "Bundle"; Type = "script"; Path = "scripts/verify_p0_evidence_bundle.ps1" },
    [PSCustomObject]@{ Workstream = "Backlog"; Type = "doc"; Path = "docs/planning/P0_REMAINING_ENGINEERING_BACKLOG.md" },
    [PSCustomObject]@{ Workstream = "Backlog"; Type = "doc"; Path = "docs/planning/P0_REMAINING_ENGINEERING_EXECUTION_PLAN.md" },
    [PSCustomObject]@{ Workstream = "Backlog"; Type = "doc"; Path = "docs/planning/P0_NEXT_ACTIONS.md" },
    [PSCustomObject]@{ Workstream = "Delivery"; Type = "doc"; Path = "docs/planning/P0_DELIVERY_REVIEW_CHECKLIST.md" },
    [PSCustomObject]@{ Workstream = "Delivery"; Type = "script"; Path = "scripts/verify_p0_delivery_review_checklist.ps1" },
    [PSCustomObject]@{ Workstream = "Risk"; Type = "doc"; Path = "docs/planning/P0_RISK_REGISTER.md" },
    [PSCustomObject]@{ Workstream = "Risk"; Type = "script"; Path = "scripts/verify_p0_risk_register.ps1" },
    [PSCustomObject]@{ Workstream = "Risk"; Type = "script"; Path = "scripts/verify_p0_governance_gate.ps1" },
    [PSCustomObject]@{ Workstream = "Risk"; Type = "script"; Path = "scripts/verify_p0_governance_gate_report.ps1" },
    [PSCustomObject]@{ Workstream = "Final"; Type = "script"; Path = "scripts/final_regression_gate.ps1" },
    [PSCustomObject]@{ Workstream = "Final"; Type = "script"; Path = "scripts/verify_final_regression_gate_report.ps1" },
    [PSCustomObject]@{ Workstream = "Approval"; Type = "doc"; Path = "docs/planning/P0_CURRENT_STATUS_SUMMARY.md" },
    [PSCustomObject]@{ Workstream = "Approval"; Type = "doc"; Path = "docs/planning/P0_RELEASE_APPROVAL_PACKET_TEMPLATE.md" },
    [PSCustomObject]@{ Workstream = "Approval"; Type = "template"; Path = "docs/planning/P0_OWNER_SIGNOFF_TEMPLATE.json" },
    [PSCustomObject]@{ Workstream = "Approval"; Type = "script"; Path = "scripts/generate_p0_release_approval_packet.ps1" },
    [PSCustomObject]@{ Workstream = "Approval"; Type = "script"; Path = "scripts/verify_p0_release_approval_packet.ps1" },
    [PSCustomObject]@{ Workstream = "Approval"; Type = "script"; Path = "scripts/verify_p0_owner_signoff.ps1" },
    [PSCustomObject]@{ Workstream = "Delivery"; Type = "script"; Path = "scripts/delivery_readiness_audit.ps1" },
    [PSCustomObject]@{ Workstream = "Delivery"; Type = "script"; Path = "scripts/verify_delivery_readiness_audit.ps1" },
    [PSCustomObject]@{ Workstream = "Delivery"; Type = "template"; Path = "docs/planning/P0_ACCEPTED_DEFERRALS_TEMPLATE.json" },
    [PSCustomObject]@{ Workstream = "Delivery"; Type = "script"; Path = "scripts/verify_p0_accepted_deferrals.ps1" },
    [PSCustomObject]@{ Workstream = "Delivery"; Type = "script"; Path = "scripts/delivery_remediation_packet.ps1" },
    [PSCustomObject]@{ Workstream = "Delivery"; Type = "script"; Path = "scripts/verify_delivery_remediation_packet.ps1" },
    [PSCustomObject]@{ Workstream = "Operator"; Type = "doc"; Path = "docs/operations/p0_evidence_suite_operator_guide.md" },
    [PSCustomObject]@{ Workstream = "Suite"; Type = "script"; Path = "scripts/run_p0_evidence_suite.ps1" },
    [PSCustomObject]@{ Workstream = "Suite"; Type = "script"; Path = "scripts/verify_p0_evidence_suite_summary.ps1" },
    [PSCustomObject]@{ Workstream = "Delivery"; Type = "script"; Path = "scripts/run_p0_delivery_candidate.ps1" },
    [PSCustomObject]@{ Workstream = "Delivery"; Type = "script"; Path = "scripts/verify_p0_delivery_candidate.ps1" },
    [PSCustomObject]@{ Workstream = "Preflight"; Type = "script"; Path = "scripts/verify_p0_tooling_preflight_report.ps1" },
    [PSCustomObject]@{ Workstream = "CI"; Type = "workflow"; Path = ".github/workflows/p0-evidence.yml" },
    [PSCustomObject]@{ Workstream = "Reports"; Type = "doc"; Path = "reports/README.md" }
)

$ValidationChecks = @(
    [PSCustomObject]@{
        Workstream = "Delivery"
        Type = "validation"
        Path = "docs/planning/P0_DELIVERY_REVIEW_CHECKLIST.md"
        Validator = "scripts/verify_p0_delivery_review_checklist.ps1"
    }
)

$Checks = @()

foreach ($Artifact in $RequiredArtifacts) {
    $Exists = Test-Path -LiteralPath $Artifact.Path
    $Checks += [PSCustomObject]@{
        workstream = $Artifact.Workstream
        type = $Artifact.Type
        path = $Artifact.Path
        status = if ($Exists) { "PASS" } else { "FAIL" }
        actual = if ($Exists) { "exists" } else { "missing" }
    }
}

foreach ($Validation in $ValidationChecks) {
    $Status = "PASS"
    $Actual = "validator passed"

    try {
        & (Join-Path $RepoRoot $Validation.Validator) -Path $Validation.Path *> $null
    } catch {
        $Status = "FAIL"
        $Actual = "validator failed: $($_.Exception.Message)"
    }

    $Checks += [PSCustomObject]@{
        workstream = $Validation.Workstream
        type = $Validation.Type
        path = $Validation.Path
        status = $Status
        actual = $Actual
    }
}

$FinishedAt = Get-Date -Format "o"
$FailedChecks = @($Checks | Where-Object { $_.status -ne "PASS" })
$Status = if ($FailedChecks.Count -eq 0) { "PASS" } else { "FAIL" }
$ExitCode = if ($Status -eq "PASS") { 0 } else { 1 }

$Report = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = $Status
    evidence_type = "p0_tooling_preflight"
    repository = "$RepoRoot"
    started_at = $StartedAt
    finished_at = $FinishedAt
    checks = $Checks
    summary = [ordered]@{
        total = $Checks.Count
        passed = @($Checks | Where-Object { $_.status -eq "PASS" }).Count
        failed = $FailedChecks.Count
    }
    limitations = @(
        "This preflight checks P0 tooling and documentation presence plus delivery review checklist structure.",
        "It does not execute recovery drills, concurrency validation, observability smoke, release drills, or Go tests.",
        "A passing preflight does not mean production approval is complete."
    )
}

$Report | ConvertTo-Json -Depth 10 | Set-Content -Path $JsonPath -Encoding utf8

$CheckLines = $Checks | ForEach-Object {
    "- $($_.workstream) $($_.type) ``$($_.path)``: $($_.status) ($($_.actual))"
}

$LimitationLines = $Report.limitations | ForEach-Object {
    "- $_"
}

$Markdown = @(
    "# P0 Tooling Preflight Report",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Run ID: $RunId",
    "- Started at: $StartedAt",
    "- Finished at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- JSON report: ``$JsonPath``",
    "- Total checks: $($Checks.Count)",
    "- Passed: $(@($Checks | Where-Object { $_.status -eq "PASS" }).Count)",
    "- Failed: $($FailedChecks.Count)",
    "",
    "## Checks",
    "",
    $CheckLines,
    "",
    "## Acceptance note",
    "",
    "This report checks tooling and documentation presence plus delivery review checklist structure. It does not execute technical validation or prove production readiness.",
    "",
    "## Limitations",
    "",
    $LimitationLines,
    "",
    "## Result",
    "",
    "- Exit code: $ExitCode",
    "- Result: $Status"
)

$Markdown | Set-Content -Path $MarkdownPath -Encoding utf8

& (Join-Path $ScriptDir "verify_p0_tooling_preflight_report.ps1") -Path $JsonPath -MarkdownPath $MarkdownPath

Write-Host "P0 tooling preflight status: $Status"
Write-Host "Markdown report written: $MarkdownPath"
Write-Host "JSON report written: $JsonPath"

exit $ExitCode
