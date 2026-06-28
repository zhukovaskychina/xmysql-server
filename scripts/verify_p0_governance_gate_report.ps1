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

function Resolve-GovernanceReportPath {
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
    throw "P0 governance gate report does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Report = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @("repository", "run_id", "risk_register", "accepted_deferrals_json", "owner_signoff_json", "checks", "limitations")) {
    Assert-Property -Object $Report -Name $Name -Context "root"
}

foreach ($Name in @(
    "generated_at",
    "run_id",
    "status",
    "evidence_type",
    "repository",
    "started_at",
    "finished_at",
    "governance_review_mode",
    "risk_register",
    "accepted_deferrals_json",
    "owner_signoff_json",
    "require_owner_signoff",
    "allow_open_risks",
    "allow_tbd_owners",
    "checks",
    "limitations"
)) {
    Assert-Property -Object $Report -Name $Name -Context "root"
}

if ($Report.evidence_type -ne "p0_governance_gate") {
    throw "evidence_type must be p0_governance_gate. Actual: $($Report.evidence_type)"
}

if ($Report.status -ne "PASS") {
    throw "Governance gate report status must be PASS. Actual: $($Report.status)"
}

if ($Report.governance_review_mode -notin @("risk_register_only", "deferral_review", "final_approval")) {
    throw "governance_review_mode must be risk_register_only, deferral_review, or final_approval. Actual: $($Report.governance_review_mode)"
}

foreach ($Name in @("require_owner_signoff", "allow_open_risks", "allow_tbd_owners")) {
    if ($Report.$Name -isnot [bool]) {
        throw "$Name must be a boolean."
    }
}

Resolve-GovernanceReportPath -Value $Report.risk_register -Description "risk_register" | Out-Null
Resolve-GovernanceReportPath -Value $Report.accepted_deferrals_json -Description "accepted_deferrals_json" | Out-Null
Resolve-GovernanceReportPath -Value $Report.owner_signoff_json -Description "owner_signoff_json" | Out-Null

$Checks = @($Report.checks)
if ($Checks.Count -lt 3) {
    throw "checks must include risk_register, accepted_deferrals, and owner_signoff."
}

$CheckByName = @{}
foreach ($Check in $Checks) {
    foreach ($Name in @("name", "status", "path")) {
        Assert-Property -Object $Check -Name $Name -Context "check"
    }

    if ($Check.status -notin @("PASS", "NOT_PROVIDED")) {
        throw "check '$($Check.name)' has unsupported status '$($Check.status)'."
    }

    $CheckByName[[string]$Check.name] = $Check
}

foreach ($RequiredCheck in @("risk_register", "accepted_deferrals", "owner_signoff")) {
    if (-not $CheckByName.ContainsKey($RequiredCheck)) {
        throw "checks is missing '$RequiredCheck'."
    }
}

if (-not [string]::IsNullOrWhiteSpace($MarkdownPath)) {
    $CandidateMarkdownPath = Resolve-GovernanceReportPath -Value $MarkdownPath -Description "P0 governance gate Markdown report"
    $MarkdownContent = Get-Content -Raw -LiteralPath $CandidateMarkdownPath
    if (-not $MarkdownContent.Contains("# P0 Governance Gate Report")) {
        throw "P0 governance gate Markdown report is missing expected title: $MarkdownPath"
    }

    if (-not $MarkdownContent.Contains("## Limitations")) {
        throw "P0 governance gate Markdown report is missing limitations section: $MarkdownPath"
    }

    if (-not $MarkdownContent.Contains([string]$Report.run_id)) {
        throw "P0 governance gate Markdown report does not match JSON run_id '$($Report.run_id)': $MarkdownPath"
    }

    foreach ($ExpectedValue in @($Report.risk_register, $Report.accepted_deferrals_json, $Report.owner_signoff_json)) {
        if (-not [string]::IsNullOrWhiteSpace([string]$ExpectedValue) -and -not $MarkdownContent.Contains([string]$ExpectedValue)) {
            throw "P0 governance gate Markdown report is missing expected input path '$ExpectedValue': $MarkdownPath"
        }
    }

    foreach ($Limitation in @($Report.limitations)) {
        if (-not [string]::IsNullOrWhiteSpace([string]$Limitation) -and -not $MarkdownContent.Contains([string]$Limitation)) {
            throw "P0 governance gate Markdown report is missing limitation '$Limitation': $MarkdownPath"
        }
    }

    foreach ($Check in $Checks) {
        if (-not [string]::IsNullOrWhiteSpace([string]$Check.name) -and -not $MarkdownContent.Contains([string]$Check.name)) {
            throw "P0 governance gate Markdown report is missing check '$($Check.name)': $MarkdownPath"
        }

        if (-not [string]::IsNullOrWhiteSpace([string]$Check.status) -and -not $MarkdownContent.Contains([string]$Check.status)) {
            throw "P0 governance gate Markdown report is missing check status '$($Check.status)' for '$($Check.name)': $MarkdownPath"
        }
    }
}

if ($CheckByName["risk_register"].status -ne "PASS") {
    throw "risk_register check must be PASS."
}

if ($Report.governance_review_mode -eq "deferral_review" -and $CheckByName["accepted_deferrals"].status -ne "PASS") {
    throw "deferral_review reports must have accepted_deferrals PASS."
}

if ($Report.governance_review_mode -eq "final_approval") {
    if ($CheckByName["owner_signoff"].status -ne "PASS") {
        throw "final_approval reports must have owner_signoff PASS."
    }

    if ($Report.require_owner_signoff -ne $true) {
        throw "final_approval reports must set require_owner_signoff to true."
    }

    if ($Report.allow_open_risks -or $Report.allow_tbd_owners) {
        throw "final_approval reports must not allow open risks or TBD owners."
    }
}

Write-Host "P0 governance gate report validation passed: $ResolvedPath"
