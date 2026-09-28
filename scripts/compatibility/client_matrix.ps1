[CmdletBinding()]
param(
    [ValidateSet("all", "mysql-cli", "go", "python", "node")][string]$Client = "all",
    [switch]$DiagnosticOnly,
    [switch]$SkipServerStart,
    [string]$ServerConfig = "conf/jdbc-compat-clean.ini",
    [int]$Port = 3312,
    [string]$User = "root",
    [string]$Password = "",
    [string]$ReportDirectory = "reports/compatibility/client-matrix",
    [switch]$UseDockerMySqlCli,
    [string]$MySqlCliDockerImage = "mysql:8.4.11",
    [string]$MySqlCliDockerHost = "host.docker.internal"
)

$ErrorActionPreference = "Stop"
$workspaceRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "../.."))
$resolvedReport = [IO.Path]::GetFullPath($ReportDirectory)
New-Item -ItemType Directory -Force -Path $resolvedReport | Out-Null
$reportPath = Join-Path $resolvedReport ("client-matrix-{0}.json" -f (Get-Date -Format "yyyyMMdd-HHmmss"))
$clients = if ($Client -eq "all") { @("mysql-cli", "go", "python", "node") } else { @($Client) }
$results = @()
$serverProcess = $null
$serverStartedHere = $false
$serverDir = Join-Path $resolvedReport "server"
$caseSpecPath = Join-Path $workspaceRoot "scripts/compatibility/client_matrix_cases.json"
$requiredRunnerCases = @(
    "connection-auth", "database-ddl-dml", "prepared-statements", "transactions",
    "null-and-types", "metadata", "multi-result-and-error", "reconnect"
)
if (Test-Path -LiteralPath $caseSpecPath) {
    $requiredRunnerCases = @((Get-Content -LiteralPath $caseSpecPath -Raw | ConvertFrom-Json) | ForEach-Object { $_.name })
}

function Add-Result([string]$name, [string]$status, [int]$exitCode, [string]$output, [string]$errorMessage = $null) {
    $script:results += [ordered]@{
        client = $name
        status = $status
        exit_code = $exitCode
        output = $output
        error = $errorMessage
    }
}

function Test-CommandAvailable([string]$command) {
    return $null -ne (Get-Command $command -ErrorAction SilentlyContinue)
}

function Test-PythonDependency([string]$module) {
    if (-not (Test-CommandAvailable "python")) { return $false }
    & python -c "import $module" 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}

function Test-NodeDependency([string]$module) {
    if (-not (Test-CommandAvailable "node")) { return $false }
    $nodeRunnerRoot = Join-Path $workspaceRoot "client_compatibility/node"
    Push-Location $nodeRunnerRoot
    try {
        & node -e "try { require.resolve('$module'); process.exit(0) } catch (e) { process.exit(1) }" 2>$null | Out-Null
        return $LASTEXITCODE -eq 0
    } finally {
        Pop-Location
    }
}

function Invoke-Client([string]$name, [string]$command, [string[]]$arguments, [hashtable]$environment = @{}) {
    $old = @{}
    foreach ($key in $environment.Keys) {
        $old[$key] = [Environment]::GetEnvironmentVariable($key)
        [Environment]::SetEnvironmentVariable($key, [string]$environment[$key])
    }
    try {
        $output = & $command @arguments 2>&1
        $code = $LASTEXITCODE
        if ($code -eq 125) {
            Add-Result $name "SKIPPED_ENVIRONMENT" $code (($output | Out-String).Trim())
        } elseif ($code -eq 0) {
            $text = (($output | Out-String).Trim())
            if ($name -in @("go", "python", "node")) {
                try {
                    $runner = $text | ConvertFrom-Json
                    if ($null -eq $runner.cases) {
                        Add-Result $name "FAIL" 1 $text "client runner did not return a cases map"
                        return
                    }
                    $missing = @($requiredRunnerCases | Where-Object {
                        $case = $_
                        $null -eq $runner.cases.$case -or [string]$runner.cases.$case -ne "PASS"
                    })
                    if ($missing.Count -gt 0) {
                        Add-Result $name "FAIL" 1 $text ("client runner missing passing cases: " + ($missing -join ", "))
                        return
                    }
                } catch {
                    Add-Result $name "FAIL" 1 $text ("client runner output is not valid JSON: " + $_.Exception.Message)
                    return
                }
            }
            Add-Result $name "PASS" $code $text
        } else {
            Add-Result $name "FAIL" $code (($output | Out-String).Trim())
        }
    } catch {
        Add-Result $name "FAIL" 1 "" $_.Exception.Message
    } finally {
        foreach ($key in $environment.Keys) {
            [Environment]::SetEnvironmentVariable($key, $old[$key])
        }
    }
}

function Invoke-MySqlCliMatrix([string]$password) {
    $oldPassword = [Environment]::GetEnvironmentVariable("MYSQL_PWD")
    [Environment]::SetEnvironmentVariable("MYSQL_PWD", $password)
    $cliHost = if ($UseDockerMySqlCli) { $MySqlCliDockerHost } else { "127.0.0.1" }
    $commonArguments = @("--protocol=TCP", "-h", $cliHost, "-P", "$Port", "-u", $User, "--batch", "--raw", "--skip-column-names")
    $cases = [ordered]@{}
    try {
        function Invoke-MySqlCommand([string[]]$arguments) {
            $previousErrorAction = $ErrorActionPreference
            $ErrorActionPreference = "Continue"
            try {
            if ($UseDockerMySqlCli) {
                $dockerArguments = @(
                    "run", "--rm", "--add-host", "$MySqlCliDockerHost`:$($MySqlCliDockerHost -replace '\..*$', '')-gateway",
                    "-e", "MYSQL_PWD", $MySqlCliDockerImage, "mysql"
                ) + $arguments
                $output = & docker @dockerArguments 2>&1
                return @($output | ForEach-Object { [string]$_ })
            }
            $output = & mysql @arguments 2>&1
            return @($output | ForEach-Object { [string]$_ })
            } finally {
                $ErrorActionPreference = $previousErrorAction
            }
        }
        function Invoke-MySqlCase([string]$sql, [bool]$expectFailure = $false) {
            $output = Invoke-MySqlCommand ($commonArguments + @("-e", $sql))
            $code = $LASTEXITCODE
            if ($expectFailure) {
                if ($code -eq 0) { throw "expected mysql CLI failure but command succeeded: $sql" }
                return (($output | Out-String).Trim())
            }
            if ($code -ne 0) { throw "mysql CLI command failed ($code): $sql`n$(($output | Out-String).Trim())" }
            return (($output | Out-String).Trim())
        }

        if ((Invoke-MySqlCase "SELECT 1") -notmatch "(^|\r?\n)1(\r?\n|$)") { throw "connection-auth did not return 1" }
        $cases["connection-auth"] = "PASS"

        Invoke-MySqlCase "CREATE DATABASE IF NOT EXISTS client_matrix; CREATE TABLE IF NOT EXISTS client_matrix.matrix_rows(id INT PRIMARY KEY, label VARCHAR(32)); INSERT INTO client_matrix.matrix_rows(id, label) VALUES (1, 'one') ON DUPLICATE KEY UPDATE label=VALUES(label)" | Out-Null
        $cases["database-ddl-dml"] = "PASS"

        $prepared = Invoke-MySqlCase "PREPARE xmysql_stmt FROM 'SELECT ? + 1'; SET @xmysql_value = 41; EXECUTE xmysql_stmt USING @xmysql_value; DEALLOCATE PREPARE xmysql_stmt"
        if ($prepared -notmatch "(^|\r?\n)42(\r?\n|$)") { throw "prepared-statements did not return 42" }
        $cases["prepared-statements"] = "PASS"

        $transaction = Invoke-MySqlCase "START TRANSACTION; INSERT INTO client_matrix.matrix_rows(id, label) VALUES (200000, 'cli-tx'); ROLLBACK; SELECT COUNT(*) FROM client_matrix.matrix_rows WHERE id=200000"
        if ($transaction -notmatch "(^|\r?\n)0(\r?\n|$)") { throw "transactions did not rollback" }
        $cases["transactions"] = "PASS"

        $typed = Invoke-MySqlCase "SELECT IF(NULL IS NULL AND CAST(42 AS SIGNED)=42 AND _utf8mb4'兼容'='兼容', 'PASS', 'FAIL')"
        if ($typed -notmatch "(^|\r?\n)PASS(\r?\n|$)") { throw "null-and-types failed" }
        $cases["null-and-types"] = "PASS"

        $charset = Invoke-MySqlCase "SELECT IF(_utf8mb4'兼容'='兼容', 'PASS', 'FAIL')"
        if ($charset -notmatch "(^|\r?\n)PASS(\r?\n|$)") { throw "charset failed" }
        $cases["charset"] = "PASS"

        $metadata = Invoke-MySqlCase "SELECT IF(COUNT(*) > 0, 'PASS', 'FAIL') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix'"
        if ($metadata -notmatch "(^|\r?\n)PASS(\r?\n|$)") { throw "metadata failed" }
        $cases["metadata"] = "PASS"

        $metadataShape = Invoke-MySqlCase "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, ORDINAL_POSITION AS ordinal_position FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix' ORDER BY TABLE_NAME, ORDINAL_POSITION LIMIT 1"
        if ($metadataShape -notmatch "client_matrix") { throw "metadata-shape failed" }
        $cases["metadata-shape"] = "PASS"

        Invoke-MySqlCase "CREATE TABLE IF NOT EXISTS client_matrix.auto_rows(id INT PRIMARY KEY AUTO_INCREMENT, label VARCHAR(32))" | Out-Null
        $autoResult = Invoke-MySqlCase "INSERT INTO client_matrix.auto_rows(label) VALUES ('mysql-cli'); SELECT LAST_INSERT_ID(), ROW_COUNT()"
        if ($autoResult -notmatch "(?m)^\d+\s+1$") { throw "auto-increment-and-result-metadata failed: $autoResult" }
        $cases["auto-increment-and-result-metadata"] = "PASS"

        $savepoint = Invoke-MySqlCase "START TRANSACTION; INSERT INTO client_matrix.matrix_rows(id, label) VALUES (300001, 'savepoint-before'); SAVEPOINT client_matrix_sp; INSERT INTO client_matrix.matrix_rows(id, label) VALUES (300002, 'savepoint-after'); ROLLBACK TO SAVEPOINT client_matrix_sp; RELEASE SAVEPOINT client_matrix_sp; COMMIT; SELECT COUNT(*) FROM client_matrix.matrix_rows WHERE id IN (300001, 300002); DELETE FROM client_matrix.matrix_rows WHERE id IN (300001, 300002)"
        if ($savepoint -notmatch "(?m)^1$") { throw "savepoints failed: $savepoint" }
        $cases["savepoints"] = "PASS"

        $session = Invoke-MySqlCase "SET @client_matrix_value = 41; SELECT @client_matrix_value + 1"
        if ($session -notmatch "(?m)^42$") { throw "session-state failed: $session" }
        $cases["session-state"] = "PASS"

        $multi = Invoke-MySqlCase "SELECT 1 AS first_col; SELECT 2 AS second_col"
        if ($multi -notmatch "1" -or $multi -notmatch "2") { throw "multi-result did not return both result values" }
        $errorOutput = Invoke-MySqlCommand ($commonArguments + @("-e", "SELECT * FROM table_that_does_not_exist"))
        $errorCode = $LASTEXITCODE
        $errorText = (($errorOutput | Out-String).Trim())
        if ($errorCode -eq 0) { throw "expected mysql CLI failure but command succeeded" }
        if ($errorText -notmatch "(?i)table|doesn't exist|unknown table") { throw "multi-result-and-error returned an unexpected error: $errorText" }
        $cases["multi-result-and-error"] = "PASS"

        if ((Invoke-MySqlCase "SELECT 1") -notmatch "(^|\r?\n)1(\r?\n|$)") { throw "reconnect failed" }
        $cases["reconnect"] = "PASS"
        Add-Result "mysql-cli" "PASS" 0 (([ordered]@{ client = "mysql-cli"; cases = $cases } | ConvertTo-Json -Compress))
    } catch {
        Add-Result "mysql-cli" "FAIL" 1 (([ordered]@{ client = "mysql-cli"; cases = $cases } | ConvertTo-Json -Compress)) $_.Exception.Message
    } finally {
        [Environment]::SetEnvironmentVariable("MYSQL_PWD", $oldPassword)
    }
}

try {
    if ($DiagnosticOnly) {
        foreach ($name in $clients) {
            $diagnostic = switch ($name) {
                "mysql-cli" {
                    if (Test-CommandAvailable "mysql") { @{ ok = $true; message = "mysql executable detected" } }
                    elseif ($UseDockerMySqlCli -and (Test-CommandAvailable "docker")) { @{ ok = $true; message = "mysql CLI will run from $MySqlCliDockerImage" } }
                    else { @{ ok = $false; message = "mysql executable not found" } }
                }
                "go" {
                    if (Test-CommandAvailable "go") { @{ ok = $true; message = "Go runtime detected; runner uses go-sql-driver/mysql from the module" } }
                    else { @{ ok = $false; message = "go executable not found" } }
                }
                "python" {
                    if (Test-PythonDependency "pymysql") { @{ ok = $true; message = "Python runtime and PyMySQL detected" } }
                    elseif (Test-CommandAvailable "python") { @{ ok = $false; message = "Python runtime detected but PyMySQL is not installed" } }
                    else { @{ ok = $false; message = "python executable not found" } }
                }
                "node" {
                    if (Test-NodeDependency "mysql2/promise") { @{ ok = $true; message = "Node runtime and mysql2 detected" } }
                    elseif (Test-CommandAvailable "node") { @{ ok = $false; message = "Node runtime detected but mysql2 is not installed" } }
                    else { @{ ok = $false; message = "node executable not found" } }
                }
            }
            if ($diagnostic.ok) { Add-Result $name "AVAILABLE" 0 $diagnostic.message }
            else { Add-Result $name "SKIPPED_ENVIRONMENT" 125 $diagnostic.message }
        }
    } else {
        if ([string]::IsNullOrWhiteSpace($Password)) { $Password = [Environment]::GetEnvironmentVariable("XMYSQL_CLIENT_PASSWORD") }
        if ([string]::IsNullOrWhiteSpace($Password)) { throw "set XMYSQL_CLIENT_PASSWORD or pass -Password through a protected invocation" }
        if (-not $SkipServerStart) {
            if (-not (Test-Path -LiteralPath $ServerConfig)) { throw "server config not found: $ServerConfig" }
            New-Item -ItemType Directory -Force -Path $serverDir | Out-Null
            $serverExe = Join-Path $serverDir "xmysql-client-matrix.exe"
            go build -o $serverExe .
            if ($LASTEXITCODE -ne 0) { throw "server build failed" }
            $dataDir = Join-Path $serverDir "data"
            $config = Get-Content -LiteralPath $ServerConfig -Raw
            $config = [regex]::Replace($config, '(?m)^\s*port\s*=.*$', "port = $Port")
            $config = [regex]::Replace($config, '(?m)^\s*datadir\s*=.*$', "datadir = $dataDir")
            $config = [regex]::Replace($config, '(?m)^\s*data_dir\s*=.*$', "data_dir = $dataDir")
            $config = [regex]::Replace($config, '(?m)^\s*redo_log_dir\s*=.*$', "redo_log_dir = $dataDir\redo")
            $config = [regex]::Replace($config, '(?m)^\s*undo_log_dir\s*=.*$', "undo_log_dir = $dataDir\undo")
            $configPath = Join-Path $serverDir "client-matrix.ini"
            Set-Content -LiteralPath $configPath -Value $config -Encoding UTF8
            $serverProcess = Start-Process -FilePath $serverExe -ArgumentList "-configPath", $configPath -WorkingDirectory $workspaceRoot -RedirectStandardOutput (Join-Path $serverDir "stdout.log") -RedirectStandardError (Join-Path $serverDir "stderr.log") -WindowStyle Hidden -PassThru
            $serverStartedHere = $true
            $ready = $false
            for ($i = 0; $i -lt 60; $i++) {
                Start-Sleep -Milliseconds 500
                if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) { $ready = $true; break }
            }
            if (-not $ready) { throw "isolated client-matrix server did not listen on port $Port" }
        }

        $dsn = "127.0.0.1:$Port"
        foreach ($name in $clients) {
            switch ($name) {
                "mysql-cli" {
                    if (-not (Test-CommandAvailable "mysql") -and (-not $UseDockerMySqlCli -or -not (Test-CommandAvailable "docker"))) { Add-Result $name "SKIPPED_ENVIRONMENT" 125 "mysql executable not found and Docker CLI fallback is disabled or unavailable"; continue }
                    Invoke-MySqlCliMatrix $Password
                }
                "go" {
                    if (-not (Test-CommandAvailable "go")) { Add-Result $name "SKIPPED_ENVIRONMENT" 125 "go executable not found"; continue }
                    Invoke-Client $name "go" @("run", "./client_compatibility/go") @{ XMYSQL_CLIENT_DSN = "$User`:$Password@tcp(127.0.0.1`:$Port)/mysql?charset=utf8mb4&parseTime=true&multiStatements=true" }
                }
                "python" {
                    if (-not (Test-CommandAvailable "python")) { Add-Result $name "SKIPPED_ENVIRONMENT" 125 "python executable not found"; continue }
                    Invoke-Client $name "python" @("client_compatibility/python/runner.py") @{ XMYSQL_CLIENT_DSN = $dsn; XMYSQL_CLIENT_USER = $User; XMYSQL_CLIENT_PASSWORD = $Password }
                }
                "node" {
                    if (-not (Test-CommandAvailable "node")) { Add-Result $name "SKIPPED_ENVIRONMENT" 125 "node executable not found"; continue }
                    Invoke-Client $name "node" @("client_compatibility/node/runner.js") @{ XMYSQL_CLIENT_HOST = "127.0.0.1"; XMYSQL_CLIENT_PORT = "$Port"; XMYSQL_CLIENT_USER = $User; XMYSQL_CLIENT_PASSWORD = $Password }
                }
            }
        }
    }
} catch {
    Add-Result "runner" "FAIL" 1 "" $_.Exception.Message
} finally {
    if ($serverStartedHere -and $null -ne $serverProcess) {
        $live = Get-Process -Id $serverProcess.Id -ErrorAction SilentlyContinue
        if ($null -ne $live) { Stop-Process -Id $serverProcess.Id -Force }
    }
}

$report = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    clients = $clients
    diagnostic_only = [bool]$DiagnosticOnly
    port = $Port
    results = $results
    passed = (($results | Where-Object { $_.status -notin @("PASS", "AVAILABLE") }).Count -eq 0 -and $results.Count -eq $clients.Count)
}
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $reportPath -Encoding UTF8
Write-Output ($report | ConvertTo-Json -Depth 8)
if (-not $report.passed) { exit 1 }
