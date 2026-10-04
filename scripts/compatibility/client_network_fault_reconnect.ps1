[CmdletBinding()]
param(
    [ValidateSet("all", "go", "python", "node")][string]$Client = "all",
    [string]$ServerConfig = "conf/dev.ini",
    [int]$Port = 3322,
    [int]$ProxyPort = 3323,
    [string]$User = "root",
    [string]$Password = "",
    [string]$ReportDirectory = "reports/compatibility/client-network-fault-reconnect"
)

$ErrorActionPreference = "Stop"
$workspaceRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "../.."))
$resolvedReport = [IO.Path]::GetFullPath($ReportDirectory)
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$runDirectory = Join-Path $resolvedReport $runId
New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null
$clients = if ($Client -eq "all") { @("go", "python", "node") } else { @($Client) }
$results = @()
$serverProcess = $null
$proxyProcess = $null

function Add-Result([string]$client, [string]$status, [int]$exitCode, [string]$output, [string]$errorMessage = $null) {
    $script:results += [ordered]@{ client = $client; status = $status; exit_code = $exitCode; output = $output; error = $errorMessage }
}

function Wait-Port([int]$port, [int]$seconds = 30) {
    for ($i = 0; $i -lt ($seconds * 10); $i++) {
        if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) { return }
        Start-Sleep -Milliseconds 100
    }
    throw "port $port did not reach LISTEN state"
}

function Stop-ProcessSafe($process) {
    if ($null -ne $process) {
        $live = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
        if ($null -ne $live) { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue }
    }
}

function Read-Text([string]$path) {
    if (-not (Test-Path -LiteralPath $path)) { return "" }
    $value = Get-Content -LiteralPath $path -Raw -ErrorAction SilentlyContinue
    if ($null -eq $value) { return "" }
    return $value.Trim()
}

$serverDir = Join-Path $runDirectory "server"
New-Item -ItemType Directory -Force -Path $serverDir | Out-Null
$serverExe = Join-Path $serverDir "xmysql-client-network-fault.exe"
$proxyExe = Join-Path $serverDir "tcp-fault-proxy.exe"
go build -o $serverExe .
if ($LASTEXITCODE -ne 0) { throw "server build failed" }
go build -o $proxyExe scripts/compatibility/tcp_fault_proxy.go
if ($LASTEXITCODE -ne 0) { throw "fault proxy build failed" }
$dataDir = Join-Path $serverDir "data"
$config = Get-Content -LiteralPath $ServerConfig -Raw
$config = [regex]::Replace($config, '(?m)^\s*port\s*=.*$', "port = $Port")
$config = [regex]::Replace($config, '(?m)^\s*datadir\s*=.*$', "datadir = $dataDir")
$config = [regex]::Replace($config, '(?m)^\s*data_dir\s*=.*$', "data_dir = $dataDir")
$config = [regex]::Replace($config, '(?m)^\s*redo_log_dir\s*=.*$', "redo_log_dir = $dataDir\redo")
$config = [regex]::Replace($config, '(?m)^\s*undo_log_dir\s*=.*$', "undo_log_dir = $dataDir\undo")
$configPath = Join-Path $serverDir "client-network-fault.ini"
Set-Content -LiteralPath $configPath -Value $config -Encoding UTF8

try {
    $serverProcess = Start-Process -FilePath $serverExe -ArgumentList @("-configPath", $configPath) -WorkingDirectory $workspaceRoot -RedirectStandardOutput (Join-Path $serverDir "server.stdout.log") -RedirectStandardError (Join-Path $serverDir "server.stderr.log") -WindowStyle Hidden -PassThru
    Wait-Port $Port
    foreach ($name in $clients) {
        $readyPath = Join-Path $runDirectory "$name.ready"
        $faultPath = Join-Path $runDirectory "$name.trigger"
        $serverReadyPath = Join-Path $runDirectory "$name.fault-ready"
        Remove-Item -LiteralPath $readyPath, $faultPath, $serverReadyPath -Force -ErrorAction SilentlyContinue
        $proxyProcess = Start-Process -FilePath $proxyExe -ArgumentList @("--listen=127.0.0.1:$ProxyPort", "--target=127.0.0.1:$Port", "--trigger=$faultPath", "--ready=$serverReadyPath") -WorkingDirectory $workspaceRoot -RedirectStandardOutput (Join-Path $serverDir "$name-proxy.stdout.log") -RedirectStandardError (Join-Path $serverDir "$name-proxy.stderr.log") -WindowStyle Hidden -PassThru
        Wait-Port $ProxyPort
        $environment = @{
            XMYSQL_CLIENT_USER = $User
            XMYSQL_CLIENT_PASSWORD = $Password
            XMYSQL_RECONNECT_READY_FILE = $readyPath
            XMYSQL_RECONNECT_SERVER_READY_FILE = $serverReadyPath
            XMYSQL_RECONNECT_TIMEOUT_SECONDS = "30"
            XMYSQL_RECONNECT_ENDPOINT = "127.0.0.1:$ProxyPort"
            XMYSQL_RECONNECT_HOST = "127.0.0.1"
            XMYSQL_RECONNECT_PORT = "$ProxyPort"
            XMYSQL_RECONNECT_DSN = "$User`:$Password@tcp(127.0.0.1`:$ProxyPort)/mysql?parseTime=true"
            XMYSQL_RECONNECT_FAULT_MODE = "network"
        }
        $oldEnv = @{}
        foreach ($key in $environment.Keys) { $oldEnv[$key] = [Environment]::GetEnvironmentVariable($key); [Environment]::SetEnvironmentVariable($key, [string]$environment[$key]) }
        $clientOut = Join-Path $runDirectory "$name.stdout.log"
        $clientErr = Join-Path $runDirectory "$name.stderr.log"
        $clientProcess = $null
        try {
            switch ($name) {
                "go" { $clientProcess = Start-Process -FilePath "go" -ArgumentList @("run", "-tags=reconnectfixture", "./client_compatibility/go/reconnect.go") -WorkingDirectory $workspaceRoot -RedirectStandardOutput $clientOut -RedirectStandardError $clientErr -WindowStyle Hidden -PassThru }
                "python" { $clientProcess = Start-Process -FilePath "python" -ArgumentList @("client_compatibility/python/reconnect_runner.py") -WorkingDirectory $workspaceRoot -RedirectStandardOutput $clientOut -RedirectStandardError $clientErr -WindowStyle Hidden -PassThru }
                "node" { $clientProcess = Start-Process -FilePath "node" -ArgumentList @("client_compatibility/node/reconnect.js") -WorkingDirectory $workspaceRoot -RedirectStandardOutput $clientOut -RedirectStandardError $clientErr -WindowStyle Hidden -PassThru }
            }
            $readyDeadline = (Get-Date).AddSeconds(30)
            while (-not (Test-Path -LiteralPath $readyPath)) {
                if ($clientProcess.HasExited) { throw "client exited before ready marker" }
                if ((Get-Date) -gt $readyDeadline) { throw "client ready marker timeout" }
                Start-Sleep -Milliseconds 100
            }
            Set-Content -LiteralPath $faultPath -Value "cut" -Encoding ascii
            $faultDeadline = (Get-Date).AddSeconds(30)
            while (-not (Test-Path -LiteralPath $serverReadyPath)) {
                if ($clientProcess.HasExited) { throw "client exited before network fault marker" }
                if ((Get-Date) -gt $faultDeadline) { throw "network fault marker timeout" }
                Start-Sleep -Milliseconds 100
            }
            $clientExitDeadline = (Get-Date).AddSeconds(30)
            while (-not $clientProcess.HasExited -and (Get-Date) -lt $clientExitDeadline) {
                $candidate = Read-Text $clientOut
                if ($candidate -match 'reconnect-after-network-fault') { break }
                Start-Sleep -Milliseconds 100
            }
            $output = Read-Text $clientOut
            $errorOutput = Read-Text $clientErr
            $exitCode = 0
            if (-not $clientProcess.HasExited) {
                if ($output -notmatch 'reconnect-after-network-fault') { throw "client did not finish after network fault" }
                Stop-ProcessSafe $clientProcess
            } else {
                $clientProcess.Refresh()
                $exitCode = [int]$clientProcess.ExitCode
            }
            if ($exitCode -ne 0) { Add-Result $name "FAIL" $exitCode $output $errorOutput } else { Add-Result $name "PASS" 0 $output $errorOutput }
        } catch { Add-Result $name "FAIL" 1 "" $_.Exception.Message }
        finally {
            foreach ($key in $environment.Keys) { [Environment]::SetEnvironmentVariable($key, $oldEnv[$key]) }
            Stop-ProcessSafe $proxyProcess
            $proxyProcess = $null
        }
    }
} finally {
    Stop-ProcessSafe $proxyProcess
    Stop-ProcessSafe $serverProcess
}

$report = [ordered]@{ generated_at = [DateTime]::UtcNow.ToString("o"); clients = $clients; fault = "active TCP connections closed by local proxy while server remains running"; results = $results; passed = (($results | Where-Object status -ne "PASS").Count -eq 0 -and $results.Count -eq $clients.Count) }
$reportPath = Join-Path $resolvedReport ("client-network-fault-reconnect-{0}.json" -f $runId)
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $reportPath -Encoding UTF8
$report | ConvertTo-Json -Depth 8
if (-not $report.passed) { exit 1 }
