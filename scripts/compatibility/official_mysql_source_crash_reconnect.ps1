[CmdletBinding()]
param(
    [int]$OfficialSourcePort = 33381,
    [int]$XMySQLPort = 3411,
    [int]$XMySQLControlPort = 4411,
    [string]$MySQLImage = "mysql:8.4.11",
    [string]$XMySQLBinary = "",
    [switch]$KeepSourceOnFailure
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$workspaceRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "../.."))
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$taskDir = Join-Path ([IO.Path]::GetTempPath()) ("xmysql-official-source-crash-" + $runId)
$sourceContainer = "xmysql-official-source-crash-$runId"
$databaseName = "official_source_crash_$($runId.Replace('-', '_'))"
$xmysqlProcess = $null
$testFailed = $false

New-Item -ItemType Directory -Force -Path $taskDir | Out-Null

function Invoke-ContainerSql([string]$container, [string]$sql, [switch]$ToXMySQL) {
    if ($ToXMySQL) {
        $arguments = @('exec', $container, 'mysql', '--protocol=tcp', '-h', 'host.docker.internal', '-P', "$XMySQLPort", '-uroot', '-Nse', $sql)
    } else {
        $arguments = @('exec', $container, 'mysql', '--protocol=tcp', '-uroot', '-Nse', $sql)
    }
    $output = & docker @arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "mysql command failed in $($container): $($output -join ' ')"
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

function Wait-XMySQL([System.Diagnostics.Process]$Process) {
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        if ($null -ne $Process -and $Process.HasExited) {
            throw "xmysql process exited before becoming ready: exit_code=$($Process.ExitCode)"
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

function Wait-XMySQLRows([string[]]$Expected) {
    $rows = @()
    for ($attempt = 0; $attempt -lt 180; $attempt++) {
        try {
            $rows = @(Invoke-ContainerSql $sourceContainer ("SELECT id,value FROM {0}.rows ORDER BY id" -f $databaseName) -ToXMySQL)
            # Docker's PowerShell output may carry CRLF or column padding;
            # compare normalized row tokens instead of relying on a literal
            # tab surviving the process boundary.
            $text = (($rows -join "`n") -replace '\s+', ' ').Trim()
            if ($attempt -eq 0) {
                Write-Host ("xmysql row sample: " + $text)
            }
            $allFound = $true
            foreach ($expectedRow in $Expected) {
                $expectedText = (($expectedRow -replace '\s+', ' ').Trim())
                if ($text -notmatch [regex]::Escape($expectedText)) { $allFound = $false; break }
            }
            if ($allFound) { return $rows }
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    try {
        $diagnosticDatabases = @(Invoke-ContainerSql $sourceContainer "SHOW DATABASES" -ToXMySQL)
        $diagnosticTables = @(Invoke-ContainerSql $sourceContainer ("SHOW TABLES FROM {0}" -f $databaseName) -ToXMySQL)
        Write-Host ("xmysql diagnostic databases: " + ($diagnosticDatabases -join ' | '))
        Write-Host ("xmysql diagnostic tables: " + ($diagnosticTables -join ' | '))
    } catch {
        Write-Host ("xmysql diagnostics failed: " + $_.Exception.Message)
    }
    throw "xmysql did not apply expected rows: $($rows -join ' | ')"
}

try {
    if ([string]::IsNullOrWhiteSpace($XMySQLBinary)) {
        $XMySQLBinary = Join-Path $taskDir "xmysql-server.exe"
        & go build -o $XMySQLBinary $workspaceRoot
        if ($LASTEXITCODE -ne 0) { throw "xmysql build failed" }
    }

    & docker run -d --name $sourceContainer -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p ("{0}:3306" -f $OfficialSourcePort) $MySQLImage --server-id=3381 --log-bin=binlog --binlog-format=ROW --binlog-row-image=FULL --gtid-mode=ON --enforce-gtid-consistency=ON --log-replica-updates=ON | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "failed to start official source" }
    Wait-OfficialMySQL $sourceContainer
    $initialGtidRows = @(Invoke-ContainerSql $sourceContainer "SELECT @@GLOBAL.gtid_executed")
    if ($initialGtidRows.Count -lt 1 -or [string]::IsNullOrWhiteSpace($initialGtidRows[0])) {
        throw "official MySQL returned an empty gtid_executed set"
    }
    $initialGtidSet = [string]$initialGtidRows[0]
    Invoke-ContainerSql $sourceContainer ("CREATE DATABASE {0}" -f $databaseName) | Out-Null
    Invoke-ContainerSql $sourceContainer ("CREATE TABLE {0}.rows (id INT PRIMARY KEY, value VARCHAR(128))" -f $databaseName) | Out-Null

    $baseConfig = Get-Content (Join-Path $workspaceRoot "conf/jdbc-compat-clean.ini") -Raw
    $dataDirectory = Join-Path $taskDir "xmysql-data"
    $config = [regex]::Replace($baseConfig, '(?m)^\s*port\s*=.*$', "port = $XMySQLPort")
    $config = [regex]::Replace($config, '(?m)^\s*datadir\s*=.*$', "datadir = $dataDirectory")
    $config = [regex]::Replace($config, '(?m)^\s*data_dir\s*=.*$', "data_dir = $dataDirectory")
    $config = [regex]::Replace($config, '(?m)^\s*redo_log_dir\s*=.*$', "redo_log_dir = $dataDirectory\redo")
    $config = [regex]::Replace($config, '(?m)^\s*undo_log_dir\s*=.*$', "undo_log_dir = $dataDirectory\undo")
    $config = [regex]::Replace($config, '(?m)^\s*dev_bypass_password_auth\s*=.*$', "dev_bypass_password_auth = true")
    $config = [regex]::Replace($config, '(?m)^\s*log_level\s*=.*$', "log_level = error")
    $config = [regex]::Replace($config, '(?m)^\s*log_error\s*=.*$', "log_error = $taskDir\error.log")
    $config = [regex]::Replace($config, '(?m)^\s*log_infos\s*=.*$', "log_infos = $taskDir\info.log")
    $replicationConfig = @"

[replication]
role = replica
uuid = official-source-crash-xmysql-$runId
server_id = 3411
listen_address = 127.0.0.1:$XMySQLControlPort
source_url = mysql://root@127.0.0.1:${OfficialSourcePort}?gtid_auto_position=true&gtid_set=$([uri]::EscapeDataString($initialGtidSet))
poll_interval = 100ms
read_only = true
"@
    $configPath = Join-Path $taskDir "xmysql.ini"
    Set-Content -LiteralPath $configPath -Value ($config + $replicationConfig) -Encoding UTF8

    $stdoutPath = Join-Path $taskDir "xmysql.stdout.log"
    $stderrPath = Join-Path $taskDir "xmysql.stderr.log"
    $xmysqlProcess = Start-Process -FilePath $XMySQLBinary -ArgumentList '-configPath', $configPath -WorkingDirectory $workspaceRoot -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -WindowStyle Hidden -PassThru
    Wait-XMySQL $xmysqlProcess

    Invoke-ContainerSql $sourceContainer ("INSERT INTO {0}.rows VALUES (1, 'before-crash')" -f $databaseName) | Out-Null
    Invoke-ContainerSql $sourceContainer ("XA START 'official-source-xa-$runId', 'branch', 1; INSERT INTO {0}.rows VALUES (2, 'xa-before-crash'); XA END 'official-source-xa-$runId', 'branch', 1; XA PREPARE 'official-source-xa-$runId', 'branch', 1; XA COMMIT 'official-source-xa-$runId', 'branch', 1" -f $databaseName) | Out-Null
    $beforeCrashRows = Wait-XMySQLRows @("1`tbefore-crash", "2`txa-before-crash")

    $live = Get-Process -Id $xmysqlProcess.Id -ErrorAction SilentlyContinue
    if ($null -ne $live) {
        Stop-Process -Id $xmysqlProcess.Id -Force
        $live.WaitForExit(10000) | Out-Null
    }
    if ($null -ne (Get-Process -Id $xmysqlProcess.Id -ErrorAction SilentlyContinue)) {
        throw "xmysql process remained alive after crash kill"
    }

    Invoke-ContainerSql $sourceContainer ("INSERT INTO {0}.rows VALUES (3, 'during-crash')" -f $databaseName) | Out-Null
    Invoke-ContainerSql $sourceContainer ("XA START 'official-source-one-phase-$runId', 'branch', 1; INSERT INTO {0}.rows VALUES (4, 'one-phase-during-crash'); XA END 'official-source-one-phase-$runId', 'branch', 1; XA COMMIT 'official-source-one-phase-$runId', 'branch', 1 ONE PHASE" -f $databaseName) | Out-Null
    Invoke-ContainerSql $sourceContainer ("XA START 'official-source-two-phase-$runId', 'branch', 1; INSERT INTO {0}.rows VALUES (5, 'two-phase-during-crash'); XA END 'official-source-two-phase-$runId', 'branch', 1; XA PREPARE 'official-source-two-phase-$runId', 'branch', 1; XA COMMIT 'official-source-two-phase-$runId', 'branch', 1" -f $databaseName) | Out-Null
    # Make the source-side commit/binlog boundary observable before the replica
    # reconnects. This also prevents the crash fixture from racing a source
    # container that has acknowledged the client before its GTID state is
    # visible to a new replication connection.
    Start-Sleep -Seconds 2
    $sourceGtidDuringCrash = [string](@(Invoke-ContainerSql $sourceContainer "SELECT @@GLOBAL.gtid_executed")[0])
    if ([string]::IsNullOrWhiteSpace($sourceGtidDuringCrash)) { throw "official MySQL returned an empty GTID set after crash-window writes" }

    $restartStdoutPath = Join-Path $taskDir "xmysql.restart.stdout.log"
    $restartStderrPath = Join-Path $taskDir "xmysql.restart.stderr.log"
    $xmysqlProcess = Start-Process -FilePath $XMySQLBinary -ArgumentList '-configPath', $configPath -WorkingDirectory $workspaceRoot -RedirectStandardOutput $restartStdoutPath -RedirectStandardError $restartStderrPath -WindowStyle Hidden -PassThru
    Wait-XMySQL $xmysqlProcess
    $afterRestartRows = Wait-XMySQLRows @("1`tbefore-crash", "2`txa-before-crash", "3`tduring-crash", "4`tone-phase-during-crash", "5`ttwo-phase-during-crash")
    $xmysqlGtidRows = @(Invoke-ContainerSql $sourceContainer "SELECT @@GLOBAL.gtid_executed" -ToXMySQL)
    if ($xmysqlGtidRows.Count -lt 1) { throw "xmysql returned no gtid_executed row after crash recovery" }
    $xmysqlGTID = [string]$xmysqlGtidRows[0]
    if ([string]::IsNullOrWhiteSpace($xmysqlGTID) -or $xmysqlGTID -eq "NULL") {
        throw "xmysql returned an empty gtid_executed set after crash recovery"
    }

    # Exercise the other native transport boundary: keep xmysql alive while
    # the official source is temporarily unavailable, then let the source
    # restart on the same GTID/binlog volume. The replica must reconnect and
    # apply only the post-outage transactions.
    & docker stop $sourceContainer | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "failed to stop official source for reconnect test" }
    Start-Sleep -Seconds 3
    & docker start $sourceContainer | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "failed to restart official source for reconnect test" }
    Wait-OfficialMySQL $sourceContainer
    Invoke-ContainerSql $sourceContainer ("INSERT INTO {0}.rows VALUES (6, 'after-source-reconnect')" -f $databaseName) | Out-Null
    Invoke-ContainerSql $sourceContainer ("XA START 'official-source-reconnect-xa-$runId', 'branch', 1; INSERT INTO {0}.rows VALUES (7, 'xa-after-source-reconnect'); XA END 'official-source-reconnect-xa-$runId', 'branch', 1; XA PREPARE 'official-source-reconnect-xa-$runId', 'branch', 1; XA COMMIT 'official-source-reconnect-xa-$runId', 'branch', 1" -f $databaseName) | Out-Null
    $afterSourceReconnectRows = Wait-XMySQLRows @("1`tbefore-crash", "2`txa-before-crash", "3`tduring-crash", "4`tone-phase-during-crash", "5`ttwo-phase-during-crash", "6`tafter-source-reconnect", "7`txa-after-source-reconnect")
    $duplicateRows = @(Invoke-ContainerSql $sourceContainer ("SELECT id, COUNT(*) FROM {0}.rows GROUP BY id HAVING COUNT(*) <> 1" -f $databaseName) -ToXMySQL)
    if ($duplicateRows.Count -gt 0) { throw "source reconnect produced duplicate rows: $($duplicateRows -join ' | ')" }

    [ordered]@{
        status = "PASS"
        official_source_rows_before_crash = $beforeCrashRows
        official_source_rows_after_xmysql_restart = $afterRestartRows
        official_source_rows_after_source_reconnect = $afterSourceReconnectRows
        official_source_gtid_executed_before_replication = $initialGtidSet
        official_source_gtid_executed_after_crash_window = $sourceGtidDuringCrash
        xmysql_gtid_executed_after_restart = $xmysqlGTID
        report_directory = $taskDir
    } | ConvertTo-Json -Depth 8
} catch {
    $testFailed = $true
    Write-Error $_
    if (Test-Path (Join-Path $taskDir "xmysql.stderr.log")) { Get-Content (Join-Path $taskDir "xmysql.stderr.log") -Tail 100 }
    if (Test-Path (Join-Path $taskDir "error.log")) { Get-Content (Join-Path $taskDir "error.log") -Tail 100 }
    exit 1
} finally {
    if ($null -ne $xmysqlProcess) {
        $live = Get-Process -Id $xmysqlProcess.Id -ErrorAction SilentlyContinue
        if ($null -ne $live) { Stop-Process -Id $xmysqlProcess.Id -Force }
    }
    $cleanupErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    if (-not ($KeepSourceOnFailure -and $testFailed)) {
        & docker rm -f $sourceContainer 2>$null | Out-Null
    } else {
        Write-Host ("preserved official source container: " + $sourceContainer)
    }
    $ErrorActionPreference = $cleanupErrorAction
}
