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
$replicationUpdateOption = if ($MySQLImage -match '(?i)(?:^|:)5\.') {
    # MySQL 5.7 uses the pre-8.0 option spelling. The replication behavior is
    # the same; only the server option name changed in later releases.
    '--log-slave-updates=ON'
} else {
    '--log-replica-updates=ON'
}
$changeSourceStatement = if ($MySQLImage -match '(?i)(?:^|:)5\.') {
    'CHANGE MASTER TO'
} else {
    'CHANGE REPLICATION SOURCE TO'
}
$startReplicaStatement = if ($MySQLImage -match '(?i)(?:^|:)5\.') {
    'START SLAVE'
} else {
    'START REPLICA'
}
$replicaStatusStatement = if ($MySQLImage -match '(?i)(?:^|:)5\.') {
    'SHOW SLAVE STATUS\G'
} else {
    'SHOW REPLICA STATUS\G'
}

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

function Wait-XMySQL([int]$port, [int]$controlPort, [string]$expectedRole = "replica", [System.Diagnostics.Process]$Process = $null) {
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        if ($null -ne $Process -and $Process.HasExited) {
            throw "xmysql process exited before becoming ready: exit_code=$($Process.ExitCode)"
        }
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
    $statusQuery = "SHOW BINARY LOG STATUS"
    try {
        $rows = @(Invoke-ContainerSql $container $statusQuery -ToXMySQL:$FromXMySQL)
    } catch {
        if ($FromXMySQL) { throw }
        # MySQL 8.0 exposes the same file/position contract under its
        # pre-8.4 name. Keep the official-version fixture portable while
        # preserving the native xmysql query above.
        $statusQuery = "SHOW MASTER STATUS"
        $rows = @(Invoke-ContainerSql $container $statusQuery)
    }
    if ($rows.Count -lt 1) { throw "$statusQuery returned no row" }
    $parts = $rows[0] -split "`t"
    if ($parts.Count -lt 2) { throw "invalid $statusQuery result: $($rows[0])" }
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

    & docker run -d --name $sourceContainer -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p ("{0}:3306" -f $OfficialSourcePort) $MySQLImage --server-id=3371 --log-bin=binlog --binlog-format=ROW --binlog-row-image=FULL --gtid-mode=ON --enforce-gtid-consistency=ON $replicationUpdateOption | Out-Null
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
    Wait-XMySQL $XMySQLPort $XMySQLControlPort "replica" $xmysqlProcess

    Invoke-ContainerSql $sourceContainer ("INSERT INTO {0}.rows VALUES (1, 'official-before-promote')" -f $databaseName) | Out-Null
    Invoke-ContainerSql $sourceContainer ("XA START 'reverse-xa-$runId', 'branch', 1; INSERT INTO {0}.rows VALUES (2, 'official-xa-before-promote'); XA END 'reverse-xa-$runId', 'branch', 1; XA PREPARE 'reverse-xa-$runId', 'branch', 1; XA COMMIT 'reverse-xa-$runId', 'branch', 1" -f $databaseName) | Out-Null
    Invoke-ContainerSql $sourceContainer ("XA START 'reverse-one-phase-xa-$runId', 'branch', 1; INSERT INTO {0}.rows VALUES (3, 'official-one-phase-before-promote'); XA END 'reverse-one-phase-xa-$runId', 'branch', 1; XA COMMIT 'reverse-one-phase-xa-$runId', 'branch', 1 ONE PHASE" -f $databaseName) | Out-Null

    $xmysqlRows = @()
    for ($attempt = 0; $attempt -lt 160; $attempt++) {
        try {
            $xmysqlRows = Invoke-ContainerSql $sourceContainer ("SELECT id,value FROM {0}.rows ORDER BY id" -f $databaseName) -ToXMySQL
            $xmysqlText = $xmysqlRows -join "`n"
            if ($xmysqlText -match '1\tofficial-before-promote' -and $xmysqlText -match '2\tofficial-xa-before-promote' -and $xmysqlText -match '3\tofficial-one-phase-before-promote') { break }
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    $xmysqlText = $xmysqlRows -join "`n"
    if ($xmysqlText -notmatch '1\tofficial-before-promote' -or $xmysqlText -notmatch '2\tofficial-xa-before-promote' -or $xmysqlText -notmatch '3\tofficial-one-phase-before-promote') {
        throw "xmysql did not apply official rows before promotion: $($xmysqlRows -join ' | ')"
    }

    # Restart xmysql while the official source is still available. The
    # restarted replica must retain the applied GTID/state and must not
    # duplicate the ordinary or XA rows when it reconnects.
    if ($null -ne $xmysqlProcess) {
        $live = Get-Process -Id $xmysqlProcess.Id -ErrorAction SilentlyContinue
        if ($null -ne $live) {
            Stop-Process -Id $xmysqlProcess.Id -Force
            $live.WaitForExit(10000) | Out-Null
        }
    }
    $restartStdoutPath = Join-Path $taskDir "xmysql.restart.stdout.log"
    $restartStderrPath = Join-Path $taskDir "xmysql.restart.stderr.log"
    $xmysqlProcess = Start-Process -FilePath $XMySQLBinary -ArgumentList '-configPath', $configPath -WorkingDirectory $workspaceRoot -RedirectStandardOutput $restartStdoutPath -RedirectStandardError $restartStderrPath -WindowStyle Hidden -PassThru
    Wait-XMySQL $XMySQLPort $XMySQLControlPort "replica" $xmysqlProcess
    $xmysqlRowsAfterRestart = @()
    for ($attempt = 0; $attempt -lt 160; $attempt++) {
        try {
            $xmysqlRowsAfterRestart = Invoke-ContainerSql $sourceContainer ("SELECT id,value FROM {0}.rows ORDER BY id" -f $databaseName) -ToXMySQL
            $restartText = $xmysqlRowsAfterRestart -join "`n"
            if ($restartText -match '1\tofficial-before-promote' -and $restartText -match '2\tofficial-xa-before-promote' -and $restartText -match '3\tofficial-one-phase-before-promote') { break }
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    $restartText = $xmysqlRowsAfterRestart -join "`n"
    if ($restartText -notmatch '1\tofficial-before-promote' -or $restartText -notmatch '2\tofficial-xa-before-promote' -or $restartText -notmatch '3\tofficial-one-phase-before-promote') {
        throw "xmysql did not retain official rows after restart: $($xmysqlRowsAfterRestart -join ' | ')"
    }

    & docker stop $sourceContainer | Out-Null
    $promoted = Invoke-RestMethod -Method Post -Uri ("http://127.0.0.1:{0}/replication/promote" -f $XMySQLControlPort) -TimeoutSec 5
    if ($promoted.role -ne "source") { throw "xmysql promotion did not return role=source" }

    & docker run -d --name $targetContainer -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p ("{0}:3306" -f $OfficialTargetPort) $MySQLImage --server-id=3372 --log-bin=binlog --binlog-format=ROW --binlog-row-image=FULL --gtid-mode=ON --enforce-gtid-consistency=ON $replicationUpdateOption | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "failed to start official target" }
    Wait-OfficialMySQL $targetContainer
    Invoke-ContainerSql $targetContainer ("CREATE DATABASE {0}" -f $databaseName) | Out-Null
    # Match xmysql's default InnoDB table character set so the official
    # target's TABLE_MAP metadata has the same VARCHAR byte width. MySQL 5.7
    # defaults to latin1 in this fixture, while xmysql persists utf8mb4.
    Invoke-ContainerSql $targetContainer ("CREATE TABLE {0}.rows (id INT PRIMARY KEY, value VARCHAR(128)) DEFAULT CHARACTER SET utf8mb4" -f $databaseName) | Out-Null
    $promotedStatus = Get-BinlogStatus $targetContainer -FromXMySQL
    if ($MySQLImage -match '(?i)(?:^|:)5\.') {
        $changeSource = "$changeSourceStatement MASTER_HOST='host.docker.internal', MASTER_PORT=$XMySQLPort, MASTER_USER='root', MASTER_PASSWORD='', MASTER_LOG_FILE='$($promotedStatus.file)', MASTER_LOG_POS=$($promotedStatus.position), MASTER_AUTO_POSITION=0"
    } else {
        $changeSource = "$changeSourceStatement SOURCE_HOST='host.docker.internal', SOURCE_PORT=$XMySQLPort, SOURCE_USER='root', SOURCE_PASSWORD='', SOURCE_LOG_FILE='$($promotedStatus.file)', SOURCE_LOG_POS=$($promotedStatus.position), SOURCE_AUTO_POSITION=0"
    }
    Invoke-ContainerSql $targetContainer $changeSource | Out-Null
    Invoke-ContainerSql $targetContainer $startReplicaStatement | Out-Null
    Invoke-ContainerSql $targetContainer ("INSERT INTO {0}.rows VALUES (4, 'xmysql-after-promote')" -f $databaseName) -ToXMySQL | Out-Null
    Invoke-ContainerSql $targetContainer ("XA START 'reverse-xa-after-promote-$runId', 'branch', 1; INSERT INTO {0}.rows VALUES (5, 'xmysql-xa-after-promote'); XA END 'reverse-xa-after-promote-$runId', 'branch', 1; XA PREPARE 'reverse-xa-after-promote-$runId', 'branch', 1; XA COMMIT 'reverse-xa-after-promote-$runId', 'branch', 1" -f $databaseName) -ToXMySQL | Out-Null
    Invoke-ContainerSql $targetContainer ("XA START 'reverse-one-phase-xa-after-promote-$runId', 'branch', 1; INSERT INTO {0}.rows VALUES (6, 'xmysql-one-phase-after-promote'); XA END 'reverse-one-phase-xa-after-promote-$runId', 'branch', 1; XA COMMIT 'reverse-one-phase-xa-after-promote-$runId', 'branch', 1 ONE PHASE" -f $databaseName) -ToXMySQL | Out-Null

    $targetRows = @()
    for ($attempt = 0; $attempt -lt 160; $attempt++) {
        try {
            $targetRows = Invoke-ContainerSql $targetContainer ("SELECT id,value FROM {0}.rows ORDER BY id" -f $databaseName)
            $targetText = $targetRows -join "`n"
            if ($targetText -match '4\txmysql-after-promote' -and $targetText -match '5\txmysql-xa-after-promote' -and $targetText -match '6\txmysql-one-phase-after-promote') { break }
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    $targetText = $targetRows -join "`n"
    if ($targetText -notmatch '4\txmysql-after-promote' -or $targetText -notmatch '5\txmysql-xa-after-promote' -or $targetText -notmatch '6\txmysql-one-phase-after-promote') {
        $replicaStatus = Invoke-ContainerSql $targetContainer $replicaStatusStatement
        throw "official target did not apply post-promotion ordinary and XA rows: rows=$($targetRows -join ' | '); status=$($replicaStatus -join ' | ')"
    }
    [ordered]@{
        status = "PASS"
        official_source_rows_applied_to_xmysql = $xmysqlRows
        official_source_rows_after_xmysql_restart = $xmysqlRowsAfterRestart
        promoted_role = $promoted.role
        official_target_rows_applied_from_xmysql = $targetRows
        official_target_xa_row_applied_from_xmysql = $true
        report_directory = $taskDir
    } | ConvertTo-Json -Depth 8
} catch {
    Write-Error $_
    if ($null -ne $targetContainer) {
        $targetExists = & docker ps -a --format '{{.Names}}' | Where-Object { $_ -eq $targetContainer }
        if ($targetExists) {
            Write-Error ("official target docker log: " + ((& docker logs $targetContainer 2>&1 | Select-Object -Last 120) -join ' | '))
            $workerSql = "SELECT WORKER_ID, LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE, LAST_ERROR_TIMESTAMP FROM performance_schema.replication_applier_status_by_worker"
            try {
                Write-Error ("official target worker status: " + ((Invoke-ContainerSql $targetContainer $workerSql) -join ' | '))
            } catch { }
        }
    }
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
