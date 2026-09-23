[CmdletBinding()]
param(
    [ValidateSet("all", "mysql-cli", "go", "python", "node")][string]$Client = "all",
    [switch]$DiagnosticOnly,
    [switch]$SkipServerStart,
    [string]$ServerConfig = "conf/jdbc-compat-clean.ini",
    [int]$Port = 3312,
    [string]$User = "root",
    [string]$Password = "",
    [string]$ReportDirectory = "reports/compatibility/client-matrix"
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
            Add-Result $name "PASS" $code (($output | Out-String).Trim())
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

try {
    if ($DiagnosticOnly) {
        foreach ($name in $clients) {
            $diagnostic = switch ($name) {
                "mysql-cli" {
                    if (Test-CommandAvailable "mysql") { @{ ok = $true; message = "mysql executable detected" } }
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
                    if (-not (Test-CommandAvailable "mysql")) { Add-Result $name "SKIPPED_ENVIRONMENT" 125 "mysql executable not found"; continue }
                    Invoke-Client $name "mysql" @("--protocol=TCP", "-h", "127.0.0.1", "-P", "$Port", "-u", $User, "--batch", "--raw", "-e", "SELECT 1; CREATE DATABASE IF NOT EXISTS client_matrix; SELECT NULL, _utf8mb4'兼容'") @{ MYSQL_PWD = $Password }
                }
                "go" {
                    if (-not (Test-CommandAvailable "go")) { Add-Result $name "SKIPPED_ENVIRONMENT" 125 "go executable not found"; continue }
                    Invoke-Client $name "go" @("run", "./client_compatibility/go") @{ XMYSQL_CLIENT_DSN = "$User`:$Password@tcp(127.0.0.1`:$Port)/mysql?charset=utf8mb4&parseTime=true" }
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
