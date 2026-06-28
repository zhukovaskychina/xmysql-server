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
        [string]$Description
    )

    if ([string]::IsNullOrWhiteSpace([string]$Value)) {
        throw "$Description must not be empty."
    }
}

function Assert-IsoDate {
    param(
        [object]$Value,
        [string]$Description
    )

    Assert-NonEmptyString -Value $Value -Description $Description
    try {
        [DateTimeOffset]::Parse([string]$Value) | Out-Null
    }
    catch {
        throw "$Description must be parseable as a date/time. Actual: $Value"
    }
}

if (-not (Test-Path -LiteralPath $Path)) {
    throw "P0 owner signoff JSON does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Signoff = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @("evidence_type", "generated_at", "release_candidate", "decision", "decision_owner", "decision_time", "approvals")) {
    Assert-Property -Object $Signoff -Name $Name -Context "root"
}

if ($Signoff.evidence_type -ne "p0_owner_signoff") {
    throw "evidence_type must be 'p0_owner_signoff'. Actual: $($Signoff.evidence_type)"
}

if ($Signoff.decision -notin @("APPROVE", "HOLD", "REJECT")) {
    throw "decision must be APPROVE, HOLD, or REJECT. Actual: $($Signoff.decision)"
}

Assert-IsoDate -Value $Signoff.generated_at -Description "generated_at"
Assert-NonEmptyString -Value $Signoff.release_candidate -Description "release_candidate"
Assert-NonEmptyString -Value $Signoff.decision_owner -Description "decision_owner"
Assert-IsoDate -Value $Signoff.decision_time -Description "decision_time"

if ($Signoff.release_candidate -eq "BRANCH_OR_COMMIT") {
    throw "release_candidate must name the real branch or commit."
}

if ($Signoff.decision_owner -eq "RELEASE_DECISION_OWNER") {
    throw "decision_owner must name the real release decision owner."
}

$RequiredRoles = @("release_owner", "rollback_owner", "data_recovery_owner", "observability_owner", "business_owner")
$Approvals = @($Signoff.approvals)

if ($Approvals.Count -lt 1) {
    throw "approvals must include at least one owner approval."
}

foreach ($Approval in $Approvals) {
    foreach ($Name in @("role", "name", "approved", "approved_at", "notes")) {
        Assert-Property -Object $Approval -Name $Name -Context "approval"
    }

    Assert-NonEmptyString -Value $Approval.role -Description "approval.role"
    Assert-NonEmptyString -Value $Approval.name -Description "approval.name"
    Assert-IsoDate -Value $Approval.approved_at -Description "approval.approved_at"
    Assert-NonEmptyString -Value $Approval.notes -Description "approval.notes"

    if ($Approval.name -eq "OWNER_NAME") {
        throw "approval.name must name a real owner for role '$($Approval.role)'."
    }

    if ($Approval.approved -isnot [bool]) {
        throw "approval.approved must be a boolean for role '$($Approval.role)'."
    }
}

if ($Signoff.decision -eq "APPROVE") {
    foreach ($Role in $RequiredRoles) {
        $RoleApprovals = @($Approvals | Where-Object { $_.role -eq $Role })

        if ($RoleApprovals.Count -ne 1) {
            throw "APPROVE signoff requires exactly one approval for role '$Role'. Actual count: $($RoleApprovals.Count)"
        }

        if ($RoleApprovals[0].approved -ne $true) {
            throw "APPROVE signoff requires role '$Role' to have approved=true."
        }
    }
}

Write-Host "P0 owner signoff validation passed: $ResolvedPath"
