param(
    [string]$RiskRegister = "docs/planning/P0_RISK_REGISTER.md",
    [string]$AcceptedDeferralsJson = "",
    [string]$OwnerSignoffJson = "",
    [string]$ReportDir = $env:P0_GOVERNANCE_REPORT_DIR,
    [string]$OutputJson = "",
    [string]$OutputMarkdown = "",
    [switch]$RequireOwnerSignoff,
    [switch]$AllowOpenRisks,
    [switch]$AllowTbdOwners
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

$StartedAt = Get-Date -Format "o"
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$RunId = "p0_governance_gate_$Timestamp"

if ([string]::IsNullOrWhiteSpace($OutputJson)) {
    $OutputJson = Join-Path $ReportDir "$RunId.json"
}

if ([string]::IsNullOrWhiteSpace($OutputMarkdown)) {
    $OutputMarkdown = Join-Path $ReportDir "$RunId.md"
}

if ($RequireOwnerSignoff -and [string]::IsNullOrWhiteSpace($OwnerSignoffJson)) {
    throw "Owner signoff is required for this governance gate run. Provide -OwnerSignoffJson."
}

$RiskArgs = @("-Path", $RiskRegister)
if ($AllowOpenRisks) {
    $RiskArgs += "-AllowOpenRisks"
}
if ($AllowTbdOwners) {
    $RiskArgs += "-AllowTbdOwners"
}

& (Join-Path $ScriptDir "verify_p0_risk_register.ps1") @RiskArgs

if (-not [string]::IsNullOrWhiteSpace($AcceptedDeferralsJson)) {
    & (Join-Path $ScriptDir "verify_p0_accepted_deferrals.ps1") -Path $AcceptedDeferralsJson
}

if (-not [string]::IsNullOrWhiteSpace($OwnerSignoffJson)) {
    & (Join-Path $ScriptDir "verify_p0_owner_signoff.ps1") -Path $OwnerSignoffJson

    if ($AllowOpenRisks -or $AllowTbdOwners) {
        throw "Owner signoff cannot be combined with relaxed risk register validation."
    }
}

$FinishedAt = Get-Date -Format "o"
$Mode = if (-not [string]::IsNullOrWhiteSpace($OwnerSignoffJson)) {
    "final_approval"
} elseif (-not [string]::IsNullOrWhiteSpace($AcceptedDeferralsJson)) {
    "deferral_review"
} else {
    "risk_register_only"
}

$Report = [ordered]@{
    generated_at = $FinishedAt
    run_id = $RunId
    status = "PASS"
    evidence_type = "p0_governance_gate"
    repository = "$RepoRoot"
    started_at = $StartedAt
    finished_at = $FinishedAt
    governance_review_mode = $Mode
    risk_register = $RiskRegister
    accepted_deferrals_json = $AcceptedDeferralsJson
    owner_signoff_json = $OwnerSignoffJson
    require_owner_signoff = [bool]$RequireOwnerSignoff
    allow_open_risks = [bool]$AllowOpenRisks
    allow_tbd_owners = [bool]$AllowTbdOwners
    checks = @(
        [PSCustomObject]@{ name = "risk_register"; status = "PASS"; path = $RiskRegister },
        [PSCustomObject]@{ name = "accepted_deferrals"; status = if ([string]::IsNullOrWhiteSpace($AcceptedDeferralsJson)) { "NOT_PROVIDED" } else { "PASS" }; path = $AcceptedDeferralsJson },
        [PSCustomObject]@{ name = "owner_signoff"; status = if ([string]::IsNullOrWhiteSpace($OwnerSignoffJson)) { "NOT_PROVIDED" } else { "PASS" }; path = $OwnerSignoffJson }
    )
    limitations = @(
        "This gate validates governance material shape and strict risk ownership.",
        "It does not run technical evidence suites, owner interviews, or production drills.",
        "Final delivery still requires a passing delivery readiness audit and valid technical evidence."
    )
}

$Report | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $OutputJson -Encoding utf8

$CheckLines = $Report.checks | ForEach-Object {
    "- $($_.name): $($_.status) ``$($_.path)``"
}

$LimitationLines = $Report.limitations | ForEach-Object {
    "- $_"
}

$Markdown = @(
    "# P0 Governance Gate Report",
    "",
    "## Summary",
    "",
    "- Status: PASS",
    "- Run ID: $RunId",
    "- Generated at: $FinishedAt",
    "- Repository: $RepoRoot",
    "- Governance review mode: $Mode",
    "- JSON report: ``$OutputJson``",
    "- Risk register: ``$RiskRegister``",
    "- Accepted deferrals JSON: ``$AcceptedDeferralsJson``",
    "- Owner signoff JSON: ``$OwnerSignoffJson``",
    "",
    "## Checks",
    "",
    $CheckLines,
    "",
    "## Limitations",
    "",
    $LimitationLines,
    "",
    "## Result",
    "",
    "- Result: PASS"
)

$Markdown | Set-Content -LiteralPath $OutputMarkdown -Encoding utf8

& (Join-Path $ScriptDir "verify_p0_governance_gate_report.ps1") -Path $OutputJson -MarkdownPath $OutputMarkdown

Write-Host "P0 governance gate validation passed."
Write-Host "Markdown report written: $OutputMarkdown"
Write-Host "JSON report written: $OutputJson"
