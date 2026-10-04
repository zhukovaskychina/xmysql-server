[CmdletBinding()]
param(
    [int]$OfficialSourcePort = 33431,
    [int]$ProxyPort = 33432,
    [int]$XMySQLPort = 33433,
    [int]$XMySQLControlPort = 4431,
    [string]$MySQLImage = "mysql:8.4.11",
    [string]$XMySQLBinary = "",
    [string]$ReportDirectory = "reports/compatibility/official-mysql-source-network-partition"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$workspaceRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "../.."))
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$taskDir = Join-Path ([IO.Path]::GetTempPath()) ("xmysql-official-network-partition-" + $runId)
$resolvedReport = [IO.Path]::GetFullPath($ReportDirectory)
$sourceContainer = "xmysql-network-partition-official-$runId"
$databaseName = "official_network_partition_$($runId.Replace('-', '_'))"
$xmysqlProcess = $null
$proxyProcess = $null

New-Item -ItemType Directory -Force -Path $taskDir | Out-Null
New-Item -ItemType Directory -Force -Path $resolvedReport | Out-Null

function Invoke-ContainerSql([string]$container, [string]$sql, [switch]$ToXMySQL) {
    if ($ToXMySQL) {
        $arguments = @('exec', $container, 'mysql', '--protocol=tcp', '-h', 'host.docker.internal', '-P', "$XMySQLPort", '-uroot', '-Nse', $sql)
    } else {
        $arguments = @('exec', $container, 'mysql', '--protocol=tcp', '-uroot', '-Nse', $sql)
    }
    $output = & docker @arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "mysql command failed in ${container}: $($output -join ' ')"
    }
    return @($output | ForEach-Object { [string]$_ })
}

function Wait-OfficialMySQL([string]$container) {
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        $probeErrorAction = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        & docker exec $container mysqladmin --protocol=tcp -uroot ping 2>$null | Out-Null
        $probeExitCode = $LASTEXITCODE
        $ErrorActionPreference = $probeErrorAction
        if ($probeExitCode -eq 0) { return }
        Start-Sleep -Milliseconds 500
    }
    throw "official MySQL container did not become ready: $container"
}

function Wait-Port([int]$port, [int]$seconds = 30) {
    for ($attempt = 0; $attempt -lt ($seconds * 10); $attempt++) {
        if ($null -ne (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)) { return }
        Start-Sleep -Milliseconds 100
    }
    throw "port $port did not reach LISTEN state"
}

function Wait-XMySQL() {
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        if ($null -ne $xmysqlProcess -and $xmysqlProcess.HasExited) {
            throw "xmysql process exited before becoming ready: $($xmysqlProcess.ExitCode)"
        }
        $sqlReady = $null -ne (Get-NetTCPConnection -LocalPort $XMySQLPort -State Listen -ErrorAction SilentlyContinue)
        try {
            $status = Invoke-RestMethod -Method Get -Uri ("http://127.0.0.1:{0}/replication/status" -f $XMySQLControlPort) -TimeoutSec 2
            if ($sqlReady -and $status.role -eq "replica") { return }
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    throw "xmysql replica did not become ready"
}

function Wait-XMySQLRows([string[]]$Expected, [int]$Attempts = 180) {
    $rows = @()
    for ($attempt = 0; $attempt -lt $Attempts; $attempt++) {
        try {
            $rows = @(Invoke-ContainerSql $sourceContainer ("SELECT id,value FROM {0}.rows ORDER BY id" -f $databaseName) -ToXMySQL)
            $text = (($rows -join "`n") -replace '\s+', ' ').Trim()
            $allFound = $true
            foreach ($expectedRow in $Expected) {
                $expectedText = (($expectedRow -replace '\s+', ' ').Trim())
                if ($text -notmatch [regex]::Escape($expectedText)) { $allFound = $false; break }
            }
            if ($allFound) { return $rows }
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    throw "xmysql did not apply expected rows: $($rows -join ' | ')"
}

function Stop-ProcessSafe($process) {
    if ($null -ne $process) {
        $live = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
        if ($null -ne $live) { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue }
    }
}

try {
    if ([string]::IsNullOrWhiteSpace($XMySQLBinary)) {
        $XMySQLBinary = Join-Path $taskDir "xmysql-server.exe"
        & go build -o $XMySQLBinary $workspaceRoot
        if ($LASTEXITCODE -ne 0) { throw "xmysql build failed" }
    }

    $proxyExe = Join-Path $taskDir "tcp-fault-proxy.exe"
    & go build -o $proxyExe (Join-Path $workspaceRoot "scripts/compatibility/tcp_fault_proxy.go")
    if ($LASTEXITCODE -ne 0) { throw "fault proxy build failed" }

    & docker run -d --name $sourceContainer -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p ("{0}:3306" -f $OfficialSourcePort) $MySQLImage --server-id=3431 --log-bin=binlog --binlog-format=ROW --binlog-row-image=FULL --gtid-mode=ON --enforce-gtid-consistency=ON --log-replica-updates=ON | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "failed to start official source" }
    Wait-OfficialMySQL $sourceContainer
    $initialGtid = [string](@(Invoke-ContainerSql $sourceContainer "SELECT @@GLOBAL.gtid_executed")[0])
    if ([string]::IsNullOrWhiteSpace($initialGtid)) { throw "official source returned empty initial GTID set" }
    Invoke-ContainerSql $sourceContainer ("CREATE DATABASE {0}" -f $databaseName) | Out-Null
    Invoke-ContainerSql $sourceContainer ("CREATE TABLE {0}.rows(id INT PRIMARY KEY, value VARCHAR(128))" -f $databaseName) | Out-Null

    $partitionPath = Join-Path $taskDir "partition.trigger"
    $partitionReadyPath = Join-Path $taskDir "partition.ready"
    $healPath = Join-Path $taskDir "heal.trigger"
    $healReadyPath = Join-Path $taskDir "heal.ready"
    $unusedTrigger = Join-Path $taskDir "unused.trigger"
    $unusedReady = Join-Path $taskDir "unused.ready"
    $proxyStdout = Join-Path $taskDir "proxy.stdout.log"
    $proxyStderr = Join-Path $taskDir "proxy.stderr.log"
    $proxyArgs = @(
        "--listen=127.0.0.1:$ProxyPort", "--target=127.0.0.1:$OfficialSourcePort",
        "--trigger=$unusedTrigger", "--ready=$unusedReady",
        "--partition=$partitionPath", "--partition-ready=$partitionReadyPath",
        "--heal=$healPath", "--heal-ready=$healReadyPath"
    )
    $proxyProcess = Start-Process -FilePath $proxyExe -ArgumentList $proxyArgs -WorkingDirectory $workspaceRoot -RedirectStandardOutput $proxyStdout -RedirectStandardError $proxyStderr -WindowStyle Hidden -PassThru
    Wait-Port $ProxyPort

    $baseConfig = Get-Content (Join-Path $workspaceRoot "conf/jdbc-compat-clean.ini") -Raw
    $dataDirectory = Join-Path $taskDir "xmysql-data"
    $config = [regex]::Replace($baseConfig, '(?m)^\s*port\s*=.*$', "port = $XMySQLPort")
    $config = [regex]::Replace($config, '(?m)^\s*datadir\s*=.*$', "datadir = $dataDirectory")
    $config = [regex]::Replace($config, '(?m)^\s*data_dir\s*=.*$', "data_dir = $dataDirectory")
    $config = [regex]::Replace($config, '(?m)^\s*redo_log_dir\s*=.*$', "redo_log_dir = $dataDirectory\redo")
    $config = [regex]::Replace($config, '(?m)^\s*undo_log_dir\s*=.*$', "undo_log_dir = $dataDirectory\undo")
    $config = [regex]::Replace($config, '(?m)^\s*dev_bypass_password_auth\s*=.*$', "dev_bypass_password_auth = true")
    $config = [regex]::Replace($config, '(?m)^\s*log_level\s*=.*$', "log_level = error")
    $config = [regex]::Replace($config, '(?m)^\s*log_error\s*=.*$', "log_error = $taskDir\xmysql.error.log")
    $replicationConfig = @"

[replication]
role = replica
uuid = official-network-partition-xmysql-$runId
server_id = 3432
listen_address = 127.0.0.1:$XMySQLControlPort
source_url = mysql://root@127.0.0.1:${ProxyPort}?gtid_auto_position=true&gtid_set=$([uri]::EscapeDataString($initialGtid))
poll_interval = 100ms
read_only = true
"@
    $configPath = Join-Path $taskDir "xmysql.ini"
    Set-Content -LiteralPath $configPath -Value ($config + $replicationConfig) -Encoding UTF8
    $xmysqlProcess = Start-Process -FilePath $XMySQLBinary -ArgumentList @('-configPath', $configPath) -WorkingDirectory $workspaceRoot -RedirectStandardOutput (Join-Path $taskDir "xmysql.stdout.log") -RedirectStandardError (Join-Path $taskDir "xmysql.stderr.log") -WindowStyle Hidden -PassThru
    Wait-XMySQL

    Invoke-ContainerSql $sourceContainer ("INSERT INTO {0}.rows VALUES (1, 'before-partition')" -f $databaseName) | Out-Null
    Invoke-ContainerSql $sourceContainer ("XA START 'network-partition-xa-$runId', 'branch', 1; INSERT INTO {0}.rows VALUES (2, 'xa-before-partition'); XA END 'network-partition-xa-$runId', 'branch', 1; XA PREPARE 'network-partition-xa-$runId', 'branch', 1; XA COMMIT 'network-partition-xa-$runId', 'branch', 1" -f $databaseName) | Out-Null
    $beforePartitionRows = Wait-XMySQLRows @("1`tbefore-partition", "2`txa-before-partition")

    Set-Content -LiteralPath $partitionPath -Value "partition" -Encoding ascii
    for ($attempt = 0; $attempt -lt 120 -and -not (Test-Path -LiteralPath $partitionReadyPath); $attempt++) { Start-Sleep -Milliseconds 100 }
    if (-not (Test-Path -LiteralPath $partitionReadyPath)) { throw "network partition did not become active" }

    Invoke-ContainerSql $sourceContainer ("INSERT INTO {0}.rows VALUES (3, 'during-partition')" -f $databaseName) | Out-Null
    Invoke-ContainerSql $sourceContainer ("XA START 'network-partition-one-phase-$runId', 'branch', 1; INSERT INTO {0}.rows VALUES (4, 'one-phase-during-partition'); XA END 'network-partition-one-phase-$runId', 'branch', 1; XA COMMIT 'network-partition-one-phase-$runId', 'branch', 1 ONE PHASE" -f $databaseName) | Out-Null
    Invoke-ContainerSql $sourceContainer ("XA START 'network-partition-two-phase-$runId', 'branch', 1; INSERT INTO {0}.rows VALUES (5, 'two-phase-during-partition'); XA END 'network-partition-two-phase-$runId', 'branch', 1; XA PREPARE 'network-partition-two-phase-$runId', 'branch', 1; XA COMMIT 'network-partition-two-phase-$runId', 'branch', 1" -f $databaseName) | Out-Null
    Start-Sleep -Seconds 2
    $partitionRows = @(Invoke-ContainerSql $sourceContainer ("SELECT id,value FROM {0}.rows ORDER BY id" -f $databaseName) -ToXMySQL)
    $partitionText = $partitionRows -join "`n"
    if ($partitionText -match '3\s+|4\s+|5\s+') { throw "rows crossed the active network partition: $partitionText" }

    Set-Content -LiteralPath $healPath -Value "heal" -Encoding ascii
    for ($attempt = 0; $attempt -lt 120 -and -not (Test-Path -LiteralPath $healReadyPath); $attempt++) { Start-Sleep -Milliseconds 100 }
    if (-not (Test-Path -LiteralPath $healReadyPath)) { throw "network partition did not heal" }
    $afterHealRows = Wait-XMySQLRows @("1`tbefore-partition", "2`txa-before-partition", "3`tduring-partition", "4`tone-phase-during-partition", "5`ttwo-phase-during-partition")
    $duplicateRows = @(Invoke-ContainerSql $sourceContainer ("SELECT id,COUNT(*) FROM {0}.rows GROUP BY id HAVING COUNT(*) <> 1" -f $databaseName) -ToXMySQL)
    if ($duplicateRows.Count -gt 0) { throw "network partition recovery produced duplicate rows: $($duplicateRows -join ' | ')" }

    [ordered]@{
        status = "PASS"
        official_source = $MySQLImage
        before_partition_rows = $beforePartitionRows
        rows_visible_during_partition = $partitionRows
        rows_after_heal = $afterHealRows
        duplicate_rows = $duplicateRows
        partition = "persistent TCP partition with new connections rejected"
        report_directory = $taskDir
    } | ConvertTo-Json -Depth 8
} catch {
    Write-Error $_
    if (Test-Path (Join-Path $taskDir "xmysql.stderr.log")) { Get-Content (Join-Path $taskDir "xmysql.stderr.log") -Tail 100 }
    if (Test-Path $proxyStderr) { Get-Content $proxyStderr -Tail 100 }
    exit 1
} finally {
    Stop-ProcessSafe $proxyProcess
    Stop-ProcessSafe $xmysqlProcess
    $cleanupErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & docker rm -f $sourceContainer 2>$null | Out-Null
    $ErrorActionPreference = $cleanupErrorAction
}
