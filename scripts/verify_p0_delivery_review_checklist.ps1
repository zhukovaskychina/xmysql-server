param(
    [string]$Path = "docs/planning/P0_DELIVERY_REVIEW_CHECKLIST.md"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Path)) {
    throw "P0 delivery review checklist does not exist: $Path"
}

$Content = Get-Content -Raw -LiteralPath $Path

foreach ($ExpectedText in @(
    "# P0 Delivery Review Checklist",
    "## Required reports",
    "## Governance evidence",
    "## Approval conditions",
    "## Non-approval conditions",
    "## Final reviewer note",
    "Delivery candidate status is `PASS`.",
    "Required governance status is `PASS`.",
    "Release approval packet is present and no longer on `HOLD`.",
    "A passing tooling preflight alone does not approve delivery.",
    "A passing governance gate alone does not approve delivery.",
    "A failed delivery candidate is a handoff artifact, not an approval artifact.",
    "Static documentation or script hardening without a real candidate run is not approval evidence."
)) {
    if (-not $Content.Contains($ExpectedText)) {
        throw "P0 delivery review checklist is missing expected text: $ExpectedText"
    }
}

Write-Host "P0 delivery review checklist validation passed: $Path"
