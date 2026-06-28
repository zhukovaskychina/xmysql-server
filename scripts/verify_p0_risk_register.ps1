param(
    [string]$Path = "docs/planning/P0_RISK_REGISTER.md",
    [switch]$AllowOpenRisks,
    [switch]$AllowTbdOwners
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Path)) {
    throw "P0 risk register does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Content = Get-Content -Raw -LiteralPath $ResolvedPath
$Lines = $Content -split '\r?\n'

if (-not $Content.Contains("# P0 Risk Register")) {
    throw "Risk register is missing title '# P0 Risk Register'."
}

$RiskRows = @(
    $Lines |
        Where-Object { $_ -match '^\|\s*R-P0-\d+\s*\|' } |
        ForEach-Object {
            $Cells = @($_.Trim().Trim("|").Split("|") | ForEach-Object { $_.Trim() })
            if ($Cells.Count -lt 6) {
                throw "Risk row must include ID, Risk, Impact, Mitigation, Owner, and Status: $_"
            }

            [PSCustomObject]@{
                id = $Cells[0]
                risk = $Cells[1]
                impact = $Cells[2]
                mitigation = $Cells[3]
                owner = $Cells[4]
                status = $Cells[5]
            }
        }
)

if ($RiskRows.Count -lt 1) {
    throw "Risk register must contain at least one R-P0 risk row."
}

$SeenIds = @{}
$AllowedStatuses = @("Open", "Closed", "Accepted", "Deferred", "Mitigated")

foreach ($Risk in $RiskRows) {
    foreach ($Name in @("id", "risk", "impact", "mitigation", "owner", "status")) {
        if ([string]::IsNullOrWhiteSpace([string]$Risk.$Name)) {
            throw "Risk $($Risk.id) has empty field '$Name'."
        }
    }

    if ($SeenIds.ContainsKey($Risk.id)) {
        throw "Duplicate risk id found: $($Risk.id)"
    }
    $SeenIds[$Risk.id] = $true

    if ($Risk.status -notin $AllowedStatuses) {
        throw "Risk $($Risk.id) has unsupported status '$($Risk.status)'. Allowed: $($AllowedStatuses -join ', ')"
    }

    if (-not $AllowTbdOwners -and $Risk.owner -eq "TBD") {
        throw "Risk $($Risk.id) has owner TBD. Assign a real owner or use -AllowTbdOwners for inventory-only review."
    }

    if (-not $AllowOpenRisks -and $Risk.status -eq "Open") {
        throw "Risk $($Risk.id) is still Open. Close, mitigate, defer, or use -AllowOpenRisks for inventory-only review."
    }

    if ($Risk.status -in @("Accepted", "Deferred") -and $Risk.mitigation -notmatch "deferral|accept|accepted|owner|evidence|follow-up|followup") {
        throw "Risk $($Risk.id) is $($Risk.status) but mitigation does not reference acceptance, deferral, evidence, owner, or follow-up."
    }
}

Write-Host "P0 risk register validation passed: $ResolvedPath"
