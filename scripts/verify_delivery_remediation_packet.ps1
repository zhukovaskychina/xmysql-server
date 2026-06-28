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

if (-not (Test-Path -LiteralPath $Path)) {
    throw "Delivery remediation packet does not exist: $Path"
}

$ResolvedPath = Resolve-Path -LiteralPath $Path
$Packet = Get-Content -Raw -LiteralPath $ResolvedPath | ConvertFrom-Json

foreach ($Name in @("generated_at", "run_id", "status", "evidence_type", "repository", "audit_json", "audit_status", "markdown_report", "work_items", "next_actions", "limitations")) {
    Assert-Property -Object $Packet -Name $Name -Context "root"
}

if ($Packet.evidence_type -ne "delivery_remediation_packet") {
    throw "evidence_type must be 'delivery_remediation_packet'. Actual: $($Packet.evidence_type)"
}

if ($Packet.status -notin @("READY", "ACTION_REQUIRED")) {
    throw "status must be READY or ACTION_REQUIRED. Actual: $($Packet.status)"
}

if ($Packet.status -eq "ACTION_REQUIRED" -and @($Packet.work_items).Count -lt 1) {
    throw "ACTION_REQUIRED packets must include at least one work item."
}

foreach ($WorkItem in @($Packet.work_items)) {
    foreach ($Name in @("id", "source_check", "severity", "current_state", "required_action", "evidence_focus", "suggested_command", "verifier", "owner", "status")) {
        Assert-Property -Object $WorkItem -Name $Name -Context "work_item"
    }

    foreach ($Name in @("evidence_focus", "suggested_command", "verifier")) {
        if ([string]::IsNullOrWhiteSpace([string]$WorkItem.$Name)) {
            throw "work_item.$Name must not be empty."
        }
    }

    if ($WorkItem.status -notin @("OPEN", "IN_PROGRESS", "DONE", "DEFERRED")) {
        throw "work_item.status must be OPEN, IN_PROGRESS, DONE, or DEFERRED. Actual: $($WorkItem.status)"
    }
}

if ($Packet.status -eq "ACTION_REQUIRED" -and @($Packet.next_actions).Count -lt 1) {
    throw "ACTION_REQUIRED packets must include at least one next action."
}

Write-Host "Delivery remediation packet validation passed: $ResolvedPath"
