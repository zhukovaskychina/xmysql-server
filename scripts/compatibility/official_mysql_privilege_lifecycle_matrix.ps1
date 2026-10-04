[CmdletBinding()]
param(
    [string]$ServerConfig = "conf/dev.ini",
    [int]$XMySQLPort = 3345,
    [int]$OfficialPort = 3346,
    [string]$MySQLImage = "mysql:8.4.11",
    [string]$ReportDirectory = "reports/compatibility/official-mysql-privilege-lifecycle"
)

$ErrorActionPreference = "Stop"
$workspaceRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "../.."))
$resolvedReport = [IO.Path]::GetFullPath($ReportDirectory)
New-Item -ItemType Directory -Force -Path $resolvedReport | Out-Null
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$reportPath = Join-Path $resolvedReport ("privilege-lifecycle-{0}.json" -f $runId)
$serverProcess = $null
$officialContainer = "xmysql-official-privilege-$runId"
$results = @()
$xmysqlPassword = [guid]::NewGuid().ToString("N")
$oldMySQLPassword = [Environment]::GetEnvironmentVariable("MYSQL_PWD")
[Environment]::SetEnvironmentVariable("MYSQL_PWD", $xmysqlPassword)

function Add-Check([string]$name, [string]$status, [string]$details) {
    $script:results += [ordered]@{ name = $name; status = $status; details = $details }
}

function Assert-Contains([string]$name, [string]$actual, [string]$expected) {
    # MySQL 8.4 exposes boolean grant columns as either N/Y or 0/1 depending
    # on the system-table projection; compare those equivalent wire values.
    $normalizedActual = $actual -replace "`t0(?=\r?$)", "`tN" -replace "`t1(?=\r?$)", "`tY"
    if ($actual -notmatch [regex]::Escape($expected) -and $normalizedActual -notmatch [regex]::Escape($expected)) {
        throw "$name expected output to contain '$expected', actual output: $actual"
    }
}

function Invoke-XMySQL([string]$sql) {
    $dockerArgs = @(
        "run", "--rm", "--add-host", "host.docker.internal:host-gateway", "-e", "MYSQL_PWD",
        $MySQLImage, "mysql",
        "--protocol=TCP", "-h", "host.docker.internal", "-P", "$XMySQLPort", "-uroot",
        "--batch", "--raw", "--skip-column-names", "-e", $sql
    )
    $output = & docker @dockerArgs 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "xmysql SQL failed: $sql`n$(($output | Out-String).Trim())"
    }
    return (($output | ForEach-Object { [string]$_ }) -join "`n").Trim()
}

function Invoke-Official([string]$sql) {
    $output = & docker exec $officialContainer mysql -uroot --batch --raw --skip-column-names -e $sql 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "official MySQL SQL failed: $sql`n$(($output | Out-String).Trim())"
    }
    return (($output | ForEach-Object { [string]$_ }) -join "`n").Trim()
}

function Wait-XMySQL() {
    for ($i = 0; $i -lt 60; $i++) {
        if (Get-NetTCPConnection -LocalPort $XMySQLPort -State Listen -ErrorAction SilentlyContinue) { return }
        Start-Sleep -Milliseconds 500
    }
    throw "xmysql did not listen on port $XMySQLPort"
}

function Wait-Official() {
    # The image first exposes a temporary bootstrap daemon, then restarts the
    # final daemon.  Waiting before the first exec avoids probing that window.
    Start-Sleep -Seconds 20
    for ($i = 0; $i -lt 60; $i++) {
        $output = & docker exec $officialContainer mysql -uroot --batch --raw --skip-column-names -e "SELECT 1" 2>&1
        $probeExit = $LASTEXITCODE
        $probeText = (($output | ForEach-Object { [string]$_ }) -join "`n").Trim()
        if ($probeExit -eq 0 -and $probeText -match "(^|\r?\n)1(\r?\n|$)") { return }
        Start-Sleep -Seconds 1
    }
    throw "official MySQL did not become ready"
}

function New-XMySQLConfig([string]$runDirectory) {
    $config = Get-Content -LiteralPath $ServerConfig -Raw
    $dataDir = Join-Path $runDirectory "data"
    $config = [regex]::Replace($config, '(?m)^\s*port\s*=.*$', "port = $XMySQLPort")
    $config = [regex]::Replace($config, '(?m)^\s*datadir\s*=.*$', "datadir = $dataDir")
    $config = [regex]::Replace($config, '(?m)^\s*data_dir\s*=.*$', "data_dir = $dataDir")
    $config = [regex]::Replace($config, '(?m)^\s*redo_log_dir\s*=.*$', "redo_log_dir = $dataDir\redo")
    $config = [regex]::Replace($config, '(?m)^\s*undo_log_dir\s*=.*$', "undo_log_dir = $dataDir\undo")
    $configPath = Join-Path $runDirectory "xmysql.ini"
    Set-Content -LiteralPath $configPath -Value $config -Encoding UTF8
    return $configPath
}

function Seed-XMySQLAdminAccount([string]$runDirectory) {
    # dev_bypass_password_auth only skips password verification.  The fresh
    # isolated data directory still needs an explicit administrative account
    # so CREATE ROLE/GRANT can be compared through the real protocol path.
    $mysqlDirectory = Join-Path $runDirectory "data\mysql"
    New-Item -ItemType Directory -Force -Path $mysqlDirectory | Out-Null
    $accountPath = Join-Path $mysqlDirectory "accounts.json"
    $accountJson = @'
{
  "accounts": [
    {
      "user": "root",
      "host": "localhost",
      "password": "",
      "plugin": "mysql_native_password",
      "global_grants": ["ALL PRIVILEGES", "GRANT OPTION"],
      "grants": {"*.*": ["ALL PRIVILEGES", "GRANT OPTION"]},
      "updated_at": "compatibility-matrix"
    }
  ]
}
'@
    [IO.File]::WriteAllText($accountPath, $accountJson, [Text.UTF8Encoding]::new($false))
}

$setupSQL = @"
DROP DATABASE IF EXISTS p1_privilege_matrix;
CREATE DATABASE p1_privilege_matrix;
CREATE TABLE p1_privilege_matrix.rows(id INT PRIMARY KEY, label VARCHAR(32));
DROP USER IF EXISTS 'p1_user'@'localhost', 'p1_role'@'localhost', 'p1_proxy'@'localhost', 'p1_target'@'localhost';
CREATE ROLE 'p1_role'@'localhost';
CREATE USER 'p1_user'@'localhost' IDENTIFIED BY 'matrix-secret';
CREATE USER 'p1_proxy'@'localhost' IDENTIFIED BY 'matrix-secret';
CREATE USER 'p1_target'@'localhost' IDENTIFIED BY 'matrix-secret';
GRANT SELECT ON p1_privilege_matrix.* TO 'p1_user'@'localhost';
GRANT SELECT (id) ON p1_privilege_matrix.rows TO 'p1_user'@'localhost';
GRANT INSERT ON p1_privilege_matrix.rows TO 'p1_role'@'localhost';
GRANT BACKUP_ADMIN ON *.* TO 'p1_user'@'localhost';
GRANT PROXY ON 'p1_target'@'localhost' TO 'p1_proxy'@'localhost';
GRANT 'p1_role'@'localhost' TO 'p1_user'@'localhost';
SET DEFAULT ROLE 'p1_role'@'localhost' TO 'p1_user'@'localhost';
"@

try {
    $runDirectory = Join-Path $resolvedReport "xmysql-$runId"
    New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null
    $serverExe = Join-Path $runDirectory "xmysql-privilege-matrix.exe"
    & go build -o $serverExe .
    if ($LASTEXITCODE -ne 0) { throw "xmysql build failed" }
    $configPath = New-XMySQLConfig $runDirectory
    Seed-XMySQLAdminAccount $runDirectory
    $serverProcess = Start-Process -FilePath $serverExe -ArgumentList "-configPath", $configPath -WorkingDirectory $workspaceRoot -RedirectStandardOutput (Join-Path $runDirectory "stdout.log") -RedirectStandardError (Join-Path $runDirectory "stderr.log") -WindowStyle Hidden -PassThru
    Wait-XMySQL

    & docker run -d --name $officialContainer -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p ("{0}:3306" -f $OfficialPort) $MySQLImage --server-id=3991 --log-bin=binlog --binlog-format=ROW --gtid-mode=ON --enforce-gtid-consistency=ON | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "failed to start official MySQL container" }
    Wait-Official

    Invoke-XMySQL $setupSQL | Out-Null
    Invoke-Official $setupSQL | Out-Null
    Add-Check "setup" "PASS" "same account, role, object, dynamic, proxy, and default-role statements accepted"

    $checks = @(
        [ordered]@{ name = "mysql.user accounts"; sql = "SELECT User,Host FROM mysql.user WHERE User IN ('p1_role','p1_user','p1_proxy','p1_target') ORDER BY User"; expected = @("p1_proxy`tlocalhost", "p1_role`tlocalhost", "p1_target`tlocalhost", "p1_user`tlocalhost") },
        [ordered]@{ name = "mysql.db schema grant"; sql = "SELECT Db,User,Select_priv FROM mysql.db WHERE Db='p1_privilege_matrix' AND User='p1_user'"; expected = @("p1_privilege_matrix`tp1_user`tY") },
        [ordered]@{ name = "mysql.tables_priv table grant"; sql = "SELECT Db,User,Table_name,Table_priv FROM mysql.tables_priv WHERE Db='p1_privilege_matrix' AND User='p1_role'"; expected = @("p1_privilege_matrix`tp1_role`trows`tInsert") },
        [ordered]@{ name = "mysql.columns_priv column grant"; sql = "SELECT Db,User,Table_name,Column_name,Column_priv FROM mysql.columns_priv WHERE Db='p1_privilege_matrix' AND User='p1_user'"; expected = @("p1_privilege_matrix`tp1_user`trows`tid`tSelect") },
        [ordered]@{ name = "mysql.global_grants dynamic privilege"; sql = "SELECT User,Host,Priv FROM mysql.global_grants WHERE User='p1_user' AND Priv='BACKUP_ADMIN'"; expected = @("p1_user`tlocalhost`tBACKUP_ADMIN") },
        [ordered]@{ name = "mysql.proxies_priv proxy grant"; sql = "SELECT User,Host,Proxied_user,Proxied_host,With_grant FROM mysql.proxies_priv WHERE User='p1_proxy'"; expected = @("p1_proxy`tlocalhost`tp1_target`tlocalhost`tN") },
        [ordered]@{ name = "mysql.role_edges role grant"; sql = "SELECT From_user,From_host,To_user,To_host,With_admin_option FROM mysql.role_edges WHERE To_user='p1_user'"; expected = @("p1_role`tlocalhost`tp1_user`tlocalhost`tN") },
        [ordered]@{ name = "mysql.default_roles default role"; sql = "SELECT User,Host,Default_role_user,Default_role_host FROM mysql.default_roles WHERE User='p1_user'"; expected = @("p1_user`tlocalhost`tp1_role`tlocalhost") },
        [ordered]@{ name = "information_schema user privileges"; sql = "SELECT GRANTEE,PRIVILEGE_TYPE FROM information_schema.user_privileges WHERE GRANTEE LIKE '%p1_user%' AND PRIVILEGE_TYPE='BACKUP_ADMIN'"; expected = @("'p1_user'@'localhost'`tBACKUP_ADMIN") }
    )
    foreach ($check in $checks) {
        foreach ($target in @("xmysql", "official")) {
            $actual = if ($target -eq "xmysql") { Invoke-XMySQL $check.sql } else { Invoke-Official $check.sql }
            foreach ($expected in $check.expected) { Assert-Contains "$target/$($check.name)" $actual $expected }
            Add-Check "$target/$($check.name)" "PASS" $actual
        }
    }

    $revokeSQL = @"
SET DEFAULT ROLE NONE TO 'p1_user'@'localhost';
REVOKE 'p1_role'@'localhost' FROM 'p1_user'@'localhost';
DROP USER 'p1_proxy'@'localhost', 'p1_target'@'localhost';
DROP ROLE 'p1_role'@'localhost';
"@
    Invoke-XMySQL $revokeSQL | Out-Null
    Invoke-Official $revokeSQL | Out-Null
    foreach ($target in @("xmysql", "official")) {
        $defaultRoles = if ($target -eq "xmysql") { Invoke-XMySQL "SELECT COUNT(*) FROM mysql.default_roles WHERE User='p1_user'" } else { Invoke-Official "SELECT COUNT(*) FROM mysql.default_roles WHERE User='p1_user'" }
        $roleEdges = if ($target -eq "xmysql") { Invoke-XMySQL "SELECT COUNT(*) FROM mysql.role_edges WHERE To_user='p1_user'" } else { Invoke-Official "SELECT COUNT(*) FROM mysql.role_edges WHERE To_user='p1_user'" }
        $dropped = if ($target -eq "xmysql") { Invoke-XMySQL "SELECT COUNT(*) FROM mysql.user WHERE User IN ('p1_role','p1_proxy','p1_target')" } else { Invoke-Official "SELECT COUNT(*) FROM mysql.user WHERE User IN ('p1_role','p1_proxy','p1_target')" }
        if ($defaultRoles.Trim() -ne "0" -or $roleEdges.Trim() -ne "0" -or $dropped.Trim() -ne "0") { throw "$target revoke/drop lifecycle did not clean metadata: defaults=$defaultRoles edges=$roleEdges dropped=$dropped" }
        Add-Check "$target/revoke-default-role-and-drop-cleanup" "PASS" "default_roles=0 role_edges=0 dropped_accounts=0"
    }
    $status = "PASS"
} catch {
    Add-Check "runner" "FAIL" $_.Exception.Message
    $status = "FAIL"
} finally {
    if ($null -ne $serverProcess) {
        $live = Get-Process -Id $serverProcess.Id -ErrorAction SilentlyContinue
        if ($null -ne $live) { Stop-Process -Id $serverProcess.Id -Force }
    }
    & docker rm -f $officialContainer 2>$null | Out-Null
    [Environment]::SetEnvironmentVariable("MYSQL_PWD", $oldMySQLPassword)
}

$report = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    status = $status
    xmysql_port = $XMySQLPort
    official_port = $OfficialPort
    checks = $results
    credentials_persisted = $false
}
$report | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $reportPath -Encoding UTF8
$report | ConvertTo-Json -Depth 10
if ($status -ne "PASS") { exit 1 }
