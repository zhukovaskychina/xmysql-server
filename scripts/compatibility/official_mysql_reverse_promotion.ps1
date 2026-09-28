[CmdletBinding()]
param(
    [int]$OfficialSourcePort = 33371,
    [int]$XMySQLPort = 3401,
    [int]$XMySQLControlPort = 4401,
    [int]$OfficialTargetPort = 33372,
    [string]$MySQLImage = "mysql:8.4.11",
    [string]$XMySQLBinary = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$workspaceRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "../.."))
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$taskDir = Join-Path ([IO.Path]::GetTempPath()) ("xmysql-reverse-promotion-" + $runId)
$sourceContainer = "xmysql-reverse-official-source-$runId"
$targetContainer = "xmysql-reverse-official-target-$runId"
$databaseName = "reverse_promote_$($runId.Replace('-', '_'))"
$xmysqlProcess = $null

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

function Wait-XMySQL([int]$port, [int]$controlPort, [string]$expectedRole = "replica") {
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        $sqlReady = $null -ne (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)
        try {
            $status = Invoke-RestMethod -Method Get -Uri ("http://127.0.0.1:{0}/replication/status" -f $controlPort) -TimeoutSec 2
            if ($sqlReady -and $status.role -eq $expectedRole) { return }
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    throw "xmysql replica did not become ready"
}

function Get-BinlogStatus([string]$container, [switch]$FromXMySQL) {
    $rows = @(Invoke-ContainerSql $container "SHOW BINARY LOG STATUS" $FromXMySQL)
    if ($rows.Count -lt 1) { throw "SHOW BINARY LOG STATUS returned no row" }
    $parts = $rows[0] -split "`t"
    if ($parts.Count -lt 2) { throw "invalid binary log status: $($rows[0])" }
    return [ordered]@{ file = $parts[0]; position = [uint64]$parts[1] }
}

function Get-GtidExecuted([string]$container) {
    $rows = @(Invoke-ContainerSql $container "SELECT @@GLOBAL.gtid_executed")
    if ($rows.Count -lt 1 -or [string]::IsNullOrWhiteSpace($rows[0])) { throw "official MySQL returned an empty gtid_executed set" }
    return [string]$rows[0]
}

try {
    if ([string]::IsNullOrWhiteSpace($XMySQLBinary)) {
        $XMySQLBinary = Join-Path $taskDir "xmysql-server.exe"
        & go build -o $XMySQLBinary $workspaceRoot
        if ($LASTEXITCODE -ne 0) { throw "xmysql build failed" }
    }

    & docker run -d --name $sourceContainer -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p ("{0}:3306" -f $OfficialSourcePort) $MySQLImage --server-id=3371 --log-bin=binlog --binlog-format=ROW --binlog-row-image=FULL --gtid-mode=ON --enforce-gtid-consistency=ON --log-replica-updates=ON | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "failed to start official source" }
    Wait-OfficialMySQL $sourceContainer
    $sourceStart = Get-BinlogStatus $sourceContainer
    $initialGtidSet = Get-GtidExecuted $sourceContainer
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
uuid = reverse-promote-xmysql-$runId
server_id = 3401
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
    Wait-XMySQL $XMySQLPort $XMySQLControlPort "replica"

    Invoke-ContainerSql $sourceContainer ("INSERT INTO {0}.rows VALUES (1, 'official-before-promote')" -f $databaseName) | Out-Null
    Invoke-ContainerSql $sourceContainer ("XA START 'reverse-xa-$runId', 'branch', 1; INSERT INTO {0}.rows VALUES (2, 'official-xa-before-promote'); XA END 'reverse-xa-$runId', 'branch', 1; XA PREPARE 'reverse-xa-$runId', 'branch', 1; XA COMMIT 'reverse-xa-$runId', 'branch', 1" -f $databaseName) | Out-Null

    $xmysqlRows = @()
    for ($attempt = 0; $attempt -lt 160; $attempt++) {
        try {
            $xmysqlRows = Invoke-ContainerSql $sourceContainer ("SELECT id,value FROM {0}.rows ORDER BY id" -f $databaseName) -ToXMySQL
            if (($xmysqlRows -join "`n") -match '1\tofficial-before-promote' -and ($xmysqlRows -join "`n") -match '2\tofficial-xa-before-promote') { break }
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    if (($xmysqlRows -join "`n") -notmatch '1\tofficial-before-promote' -or ($xmysqlRows -join "`n") -notmatch '2\tofficial-xa-before-promote') {
        throw "xmysql did not apply official rows before promotion: $($xmysqlRows -join ' | ')"
    }

    & docker stop $sourceContainer | Out-Null
    $promoted = Invoke-RestMethod -Method Post -Uri ("http://127.0.0.1:{0}/replication/promote" -f $XMySQLControlPort) -TimeoutSec 5
    if ($promoted.role -ne "source") { throw "xmysql promotion did not return role=source" }

    & docker run -d --name $targetContainer -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p ("{0}:3306" -f $OfficialTargetPort) $MySQLImage --server-id=3372 --log-bin=binlog --binlog-format=ROW --binlog-row-image=FULL --gtid-mode=ON --enforce-gtid-consistency=ON --log-replica-updates=ON | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "failed to start official target" }
    Wait-OfficialMySQL $targetContainer
    Invoke-ContainerSql $targetContainer ("CREATE DATABASE {0}" -f $databaseName) | Out-Null
    Invoke-ContainerSql $targetContainer ("CREATE TABLE {0}.rows (id INT PRIMARY KEY, value VARCHAR(128))" -f $databaseName) | Out-Null
    $promotedStatus = Get-BinlogStatus $targetContainer -FromXMySQL
    $changeSource = "CHANGE REPLICATION SOURCE TO SOURCE_HOST='host.docker.internal', SOURCE_PORT=$XMySQLPort, SOURCE_USER='root', SOURCE_PASSWORD='', SOURCE_LOG_FILE='$($promotedStatus.file)', SOURCE_LOG_POS=$($promotedStatus.position), SOURCE_AUTO_POSITION=0"
    Invoke-ContainerSql $targetContainer $changeSource | Out-Null
    Invoke-ContainerSql $targetContainer "START REPLICA" | Out-Null
    Invoke-ContainerSql $targetContainer ("INSERT INTO {0}.rows VALUES (3, 'xmysql-after-promote')" -f $databaseName) -ToXMySQL | Out-Null

    $targetRows = @()
    for ($attempt = 0; $attempt -lt 160; $attempt++) {
        try {
            $targetRows = Invoke-ContainerSql $targetContainer ("SELECT id,value FROM {0}.rows ORDER BY id" -f $databaseName)
            if (($targetRows -join "`n") -match '3\txmysql-after-promote') { break }
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    if (($targetRows -join "`n") -notmatch '3\txmysql-after-promote') {
        $replicaStatus = Invoke-ContainerSql $targetContainer "SHOW REPLICA STATUS"
        throw "official target did not apply post-promotion row: rows=$($targetRows -join ' | '); status=$($replicaStatus -join ' | ')"
    }
    [ordered]@{
        status = "PASS"
        official_source_rows_applied_to_xmysql = $xmysqlRows
        promoted_role = $promoted.role
        official_target_rows_applied_from_xmysql = $targetRows
        report_directory = $taskDir
    } | ConvertTo-Json -Depth 8
} catch {
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
    & docker rm -f $sourceContainer 2>$null | Out-Null
    & docker rm -f $targetContainer 2>$null | Out-Null
    $ErrorActionPreference = $cleanupErrorAction
}
