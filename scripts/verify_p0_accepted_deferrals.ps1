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

function Parse-DateTimeOffset {
    param(
        [object]$Value,
        [string]$Description
    )

    Assert-NonEmptyString -Value $Value -Description $Description

    try {
        return [DateTimeOffset]::Parse([string]$Value)
    }
    catch {
        throw "$Description must be parseable as a date/time. Actual: $Value"
    }
}

if (-not (Test-Path -LiteralPath $Path)) {
    throw "Accepted deferrals JSON does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Document = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @("evidence_type", "generated_at", "deferrals")) {
    Assert-Property -Object $Document -Name $Name -Context "root"
}

if ($Document.evidence_type -ne "accepted_delivery_deferrals") {
    throw "evidence_type must be 'accepted_delivery_deferrals'. Actual: $($Document.evidence_type)"
}

Parse-DateTimeOffset -Value $Document.generated_at -Description "generated_at" | Out-Null

$Deferrals = @($Document.deferrals)
if ($Deferrals.Count -lt 1) {
    throw "deferrals must contain at least one accepted deferral."
}

$SeenChecks = @{}
$Now = [DateTimeOffset]::Now

foreach ($Deferral in $Deferrals) {
    foreach ($Name in @("check", "owner", "decision", "expires_at", "rationale")) {
        Assert-Property -Object $Deferral -Name $Name -Context "deferral"
    }

    Assert-NonEmptyString -Value $Deferral.check -Description "deferral.check"
    Assert-NonEmptyString -Value $Deferral.owner -Description "deferral.owner"
    Assert-NonEmptyString -Value $Deferral.rationale -Description "deferral.rationale"

    if ([string]$Deferral.rationale -like "*Replace this text*") {
        throw "deferral.rationale must replace the template placeholder for check '$($Deferral.check)'."
    }

    if ($Deferral.owner -eq "OWNER_OR_TEAM") {
        throw "deferral.owner must name a real owner or team for check '$($Deferral.check)'."
    }

    if ($Deferral.decision -ne "ACCEPTED") {
        throw "deferral.decision must be ACCEPTED for check '$($Deferral.check)'. Actual: $($Deferral.decision)"
    }

    $ExpiresAt = Parse-DateTimeOffset -Value $Deferral.expires_at -Description "deferral.expires_at"
    if ($ExpiresAt -le $Now) {
        throw "deferral for check '$($Deferral.check)' is expired at $($Deferral.expires_at)."
    }

    $Check = [string]$Deferral.check
    if ($SeenChecks.ContainsKey($Check)) {
        throw "Duplicate deferral check found: $Check"
    }

    $SeenChecks[$Check] = $true
}

Write-Host "Accepted delivery deferrals validation passed: $ResolvedPath"
