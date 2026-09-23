[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$ReferenceDsn,
    [Parameter(Mandatory = $true)][string]$TargetDsn,
    [string]$MysqlBinary = "mysql",
    [string]$ReportDirectory = "reports/compatibility"
)

$ErrorActionPreference = "Stop"
$reportPath = Join-Path $ReportDirectory ("mysql84-matrix-{0}.json" -f (Get-Date -Format "yyyyMMdd-HHmmss"))
New-Item -ItemType Directory -Force -Path $ReportDirectory | Out-Null

$cases = @(
    @{ name = "compat-parent-child"; sql = "CREATE TABLE compat_parent(id INT PRIMARY KEY, code VARCHAR(20) UNIQUE); CREATE TABLE compat_child(id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk_parent FOREIGN KEY(parent_id) REFERENCES compat_parent(id));" },
    @{ name = "foreign-key-read"; sql = "INSERT INTO compat_parent VALUES (1, 'p1'); INSERT INTO compat_child VALUES (1, 1); SELECT id, parent_id FROM compat_child ORDER BY id;" },
    @{ name = "full-tables"; sql = "SHOW FULL TABLES;" },
    @{ name = "columns"; sql = "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME, ORDINAL_POSITION;" }
)

function Invoke-Mysql([string]$dsn, [string]$sql) {
    $output = & $MysqlBinary $dsn --batch --raw --skip-column-names -e $sql 2>&1
    if ($LASTEXITCODE -ne 0) { throw "mysql execution failed for $dsn`: $output" }
    return @($output)
}

$started = Get-Date
$results = @()
try {
    foreach ($case in $cases) {
        $reference = Invoke-Mysql $ReferenceDsn $case.sql
        $target = Invoke-Mysql $TargetDsn $case.sql
        $results += [ordered]@{ name = $case.name; sql = $case.sql; reference_rows = $reference; target_rows = $target; passed = (($reference -join "`n") -eq ($target -join "`n")); error = $null }
    }
} catch {
    $results += [ordered]@{ name = "runner"; sql = $null; reference_rows = @(); target_rows = @(); passed = $false; error = $_.Exception.Message }
}

$report = [ordered]@{ generated_at = (Get-Date).ToUniversalTime().ToString("o"); elapsed_ms = [int]((Get-Date) - $started).TotalMilliseconds; cases = $results; passed = (($results | Where-Object { -not $_.passed }).Count -eq 0) }
$report | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $reportPath
if (-not $report.passed) { exit 1 }
