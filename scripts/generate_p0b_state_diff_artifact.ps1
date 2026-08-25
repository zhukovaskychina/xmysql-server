param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("row_state_diff", "page_state_diff", "wal_replay_diff")]
    [string]$Scope,
    [Parameter(Mandatory = $true)]
    [string]$ExpectedSnapshotJson,
    [Parameter(Mandatory = $true)]
    [string]$ActualSnapshotJson,
    [string]$Scenario = "p0b_recovery_state",
    [string]$ReportDir = $env:P0_STATE_DIFF_REPORT_DIR,
    [string]$OutJson = "",
    [string]$OutMarkdown = ""
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

if (-not (Test-Path -LiteralPath $ExpectedSnapshotJson)) {
    throw "Expected snapshot JSON does not exist: $ExpectedSnapshotJson"
}

if (-not (Test-Path -LiteralPath $ActualSnapshotJson)) {
    throw "Actual snapshot JSON does not exist: $ActualSnapshotJson"
}

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$GeneratedAt = Get-Date -Format "o"
$RunId = "p0b_state_diff_${Scope}_$Timestamp"

if ([string]::IsNullOrWhiteSpace($OutJson)) {
    $OutJson = Join-Path $ReportDir "$RunId.json"
}

if ([string]::IsNullOrWhiteSpace($OutMarkdown)) {
    $OutMarkdown = Join-Path $ReportDir "$RunId.md"
}

function ConvertTo-CanonicalJson {
    param([object]$Value)

    if ($null -eq $Value) {
        return $null
    }

    if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string]) -and -not ($Value -is [System.Management.Automation.PSCustomObject])) {
        return @($Value | ForEach-Object { ConvertTo-CanonicalJson -Value $_ })
    }

    if ($Value -is [System.Management.Automation.PSCustomObject]) {
        $Ordered = [ordered]@{}
        foreach ($PropertyName in @($Value.PSObject.Properties.Name | Sort-Object)) {
            $Ordered[$PropertyName] = ConvertTo-CanonicalJson -Value $Value.$PropertyName
        }
        return $Ordered
    }

    return $Value
}

function Get-StableJson {
    param([object]$Value)
    return (ConvertTo-CanonicalJson -Value $Value | ConvertTo-Json -Depth 100 -Compress)
}

function Get-Sha256Text {
    param([string]$Text)

    $Sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $Bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
        return ([System.BitConverter]::ToString($Sha.ComputeHash($Bytes))).Replace("-", "").ToLowerInvariant()
    }
    finally {
        $Sha.Dispose()
    }
}

function Add-JsonLeaf {
    param(
        [hashtable]$Leaves,
        [string]$Path,
        [object]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Path)) {
        $Path = "$"
    }

    $Leaves[$Path] = Get-StableJson -Value $Value
}

function Get-JsonLeaves {
    param(
        [object]$Value,
        [string]$Path = "$"
    )

    $Leaves = @{}

    if ($null -eq $Value) {
        Add-JsonLeaf -Leaves $Leaves -Path $Path -Value $null
        return $Leaves
    }

    if ($Value -is [System.Management.Automation.PSCustomObject]) {
        $Names = @($Value.PSObject.Properties.Name | Sort-Object)
        if ($Names.Count -eq 0) {
            Add-JsonLeaf -Leaves $Leaves -Path $Path -Value $Value
            return $Leaves
        }

        foreach ($Name in $Names) {
            $ChildLeaves = Get-JsonLeaves -Value $Value.$Name -Path "$Path.$Name"
            foreach ($Key in $ChildLeaves.Keys) {
                $Leaves[$Key] = $ChildLeaves[$Key]
            }
        }
        return $Leaves
    }

    if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string])) {
        $Array = @($Value)
        if ($Array.Count -eq 0) {
            Add-JsonLeaf -Leaves $Leaves -Path $Path -Value $Value
            return $Leaves
        }

        for ($Index = 0; $Index -lt $Array.Count; $Index++) {
            $ChildLeaves = Get-JsonLeaves -Value $Array[$Index] -Path "$Path[$Index]"
            foreach ($Key in $ChildLeaves.Keys) {
                $Leaves[$Key] = $ChildLeaves[$Key]
            }
        }
        return $Leaves
    }

    Add-JsonLeaf -Leaves $Leaves -Path $Path -Value $Value
    return $Leaves
}

$ExpectedRaw = Get-Content -Raw -LiteralPath $ExpectedSnapshotJson
$ActualRaw = Get-Content -Raw -LiteralPath $ActualSnapshotJson
$ExpectedObject = $ExpectedRaw | ConvertFrom-Json
$ActualObject = $ActualRaw | ConvertFrom-Json
$ExpectedCanonical = Get-StableJson -Value $ExpectedObject
$ActualCanonical = Get-StableJson -Value $ActualObject
$ExpectedLeaves = Get-JsonLeaves -Value $ExpectedObject
$ActualLeaves = Get-JsonLeaves -Value $ActualObject

$ExpectedKeys = @($ExpectedLeaves.Keys | Sort-Object)
$ActualKeys = @($ActualLeaves.Keys | Sort-Object)
$MissingExpected = @($ExpectedKeys | Where-Object { -not $ActualLeaves.ContainsKey($_) })
$UnexpectedActual = @($ActualKeys | Where-Object { -not $ExpectedLeaves.ContainsKey($_) })
$CommonKeys = @($ExpectedKeys | Where-Object { $ActualLeaves.ContainsKey($_) })
$MismatchedValues = @($CommonKeys | Where-Object { $ExpectedLeaves[$_] -ne $ActualLeaves[$_] })

$MismatchExamples = @($MismatchedValues | Select-Object -First 20 | ForEach-Object {
    [PSCustomObject]@{
        path = $_
        expected = $ExpectedLeaves[$_]
        actual = $ActualLeaves[$_]
    }
})

$Status = if ($MissingExpected.Count -eq 0 -and $UnexpectedActual.Count -eq 0 -and $MismatchedValues.Count -eq 0) { "PASS" } else { "FAIL" }

$Artifact = [ordered]@{
    evidence_type = "p0b_state_diff_artifact"
    generated_at = $GeneratedAt
    run_id = $RunId
    status = $Status
    scope = $Scope
    scenario = $Scenario
    repository = "$RepoRoot"
    expected_snapshot = $ExpectedSnapshotJson
    actual_snapshot = $ActualSnapshotJson
    expected_sha256 = Get-Sha256Text -Text $ExpectedCanonical
    actual_sha256 = Get-Sha256Text -Text $ActualCanonical
    diff_summary = [ordered]@{
        missing_expected_items = $MissingExpected.Count
        unexpected_actual_items = $UnexpectedActual.Count
        mismatched_values = $MismatchedValues.Count
        mismatches = $MissingExpected.Count + $UnexpectedActual.Count + $MismatchedValues.Count
    }
    samples = [ordered]@{
        missing_expected_paths = @($MissingExpected | Select-Object -First 20)
        unexpected_actual_paths = @($UnexpectedActual | Select-Object -First 20)
        mismatched_values = $MismatchExamples
    }
    limitations = @(
        "This artifact compares JSON snapshots semantically after stable key ordering.",
        "Array order is significant.",
        "Only the first 20 paths per sample category are embedded; summary counts include all paths."
    )
}

$Artifact | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $OutJson -Encoding utf8

$Markdown = @(
    "# P0-B State Diff Artifact",
    "",
    "## Summary",
    "",
    "- Status: $Status",
    "- Scope: $Scope",
    "- Scenario: $Scenario",
    "- Generated at: $GeneratedAt",
    "- Expected snapshot: ``$ExpectedSnapshotJson``",
    "- Actual snapshot: ``$ActualSnapshotJson``",
    "- JSON artifact: ``$OutJson``",
    "- Missing expected items: $($MissingExpected.Count)",
    "- Unexpected actual items: $($UnexpectedActual.Count)",
    "- Mismatched values: $($MismatchedValues.Count)",
    "",
    "## Result",
    "",
    "- Result: $Status"
)

$Markdown | Set-Content -LiteralPath $OutMarkdown -Encoding utf8

Write-Host "P0-B state diff artifact status: $Status"
Write-Host "JSON artifact written: $OutJson"
Write-Host "Markdown report written: $OutMarkdown"

if ($Status -eq "PASS") {
    exit 0
}

exit 1
