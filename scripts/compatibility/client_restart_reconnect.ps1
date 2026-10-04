[CmdletBinding()]
param(
    [ValidateSet("all", "go", "python", "node")][string]$Client = "all",
    [string]$ServerConfig = "conf/dev.ini",
    [int]$Port = 3322,
    [string]$User = "root",
    [string]$Password = "",
    [string]$ReportDirectory = "reports/compatibility/client-restart-reconnect"
)

$ErrorActionPreference = "Stop"
$workspaceRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "../.."))
$resolvedReport = [IO.Path]::GetFullPath($ReportDirectory)
New-Item -ItemType Directory -Force -Path $resolvedReport | Out-Null
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$runDirectory = Join-Path $resolvedReport $runId
New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null
$clients = if ($Client -eq "all") { @("go", "python", "node") } else { @($Client) }
$results = @()
$serverProcess = $null

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

function Stop-LocalServer {
    if ($null -ne $script:serverProcess) {
        $live = Get-Process -Id $script:serverProcess.Id -ErrorAction SilentlyContinue
        if ($null -ne $live) { Stop-Process -Id $script:serverProcess.Id -Force }
        $script:serverProcess = $null
        Start-Sleep -Milliseconds 250
    }
}

$serverDir = Join-Path $runDirectory "server"
New-Item -ItemType Directory -Force -Path $serverDir | Out-Null
$serverExe = Join-Path $serverDir "xmysql-client-reconnect.exe"
go build -o $serverExe .
if ($LASTEXITCODE -ne 0) { throw "server build failed" }
$dataDir = Join-Path $serverDir "data"
$config = Get-Content -LiteralPath $ServerConfig -Raw
$config = [regex]::Replace($config, '(?m)^\s*port\s*=.*$', "port = $Port")
$config = [regex]::Replace($config, '(?m)^\s*datadir\s*=.*$', "datadir = $dataDir")
$config = [regex]::Replace($config, '(?m)^\s*data_dir\s*=.*$', "data_dir = $dataDir")
$config = [regex]::Replace($config, '(?m)^\s*redo_log_dir\s*=.*$', "redo_log_dir = $dataDir\redo")
$config = [regex]::Replace($config, '(?m)^\s*undo_log_dir\s*=.*$', "undo_log_dir = $dataDir\undo")
$configPath = Join-Path $serverDir "client-reconnect.ini"
Set-Content -LiteralPath $configPath -Value $config -Encoding UTF8

try {
    foreach ($name in $clients) {
        $readyPath = Join-Path $runDirectory "$name.ready"
        $serverReadyPath = Join-Path $runDirectory "$name.server-ready"
        Remove-Item -LiteralPath $readyPath, $serverReadyPath -Force -ErrorAction SilentlyContinue

        $serverProcess = Start-Process -FilePath $serverExe -ArgumentList @("-configPath", $configPath) -WorkingDirectory $workspaceRoot -RedirectStandardOutput (Join-Path $serverDir "$name-initial.stdout.log") -RedirectStandardError (Join-Path $serverDir "$name-initial.stderr.log") -WindowStyle Hidden -PassThru
        Wait-Port $Port

        $oldEnv = @{}
        $environment = @{
            XMYSQL_CLIENT_USER = $User
            XMYSQL_CLIENT_PASSWORD = $Password
            XMYSQL_RECONNECT_READY_FILE = $readyPath
            XMYSQL_RECONNECT_SERVER_READY_FILE = $serverReadyPath
            XMYSQL_RECONNECT_TIMEOUT_SECONDS = "30"
            XMYSQL_RECONNECT_ENDPOINT = "127.0.0.1:$Port"
            XMYSQL_RECONNECT_HOST = "127.0.0.1"
            XMYSQL_RECONNECT_PORT = "$Port"
            XMYSQL_RECONNECT_DSN = "$User`:$Password@tcp(127.0.0.1`:$Port)/mysql?parseTime=true"
        }
        foreach ($key in $environment.Keys) {
            $oldEnv[$key] = [Environment]::GetEnvironmentVariable($key)
            [Environment]::SetEnvironmentVariable($key, [string]$environment[$key])
        }
        $clientOut = Join-Path $runDirectory "$name.stdout.log"
        $clientErr = Join-Path $runDirectory "$name.stderr.log"
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
            Stop-LocalServer
            $serverProcess = Start-Process -FilePath $serverExe -ArgumentList @("-configPath", $configPath) -WorkingDirectory $workspaceRoot -RedirectStandardOutput (Join-Path $serverDir "$name-restart.stdout.log") -RedirectStandardError (Join-Path $serverDir "$name-restart.stderr.log") -WindowStyle Hidden -PassThru
            Wait-Port $Port
            Set-Content -LiteralPath $serverReadyPath -Value "ready" -Encoding ascii
            if (-not $clientProcess.WaitForExit(30000)) {
                Stop-Process -Id $clientProcess.Id -Force
                throw "client did not finish after server restart"
            }
            $output = if (Test-Path -LiteralPath $clientOut) { $rawOutput = Get-Content -LiteralPath $clientOut -Raw; if ($null -eq $rawOutput) { "" } else { $rawOutput.Trim() } } else { "" }
            $errorOutput = if (Test-Path -LiteralPath $clientErr) { $rawError = Get-Content -LiteralPath $clientErr -Raw; if ($null -eq $rawError) { "" } else { $rawError.Trim() } } else { "" }
            $exitCode = 0
            try {
                $clientProcess.Refresh()
                if ($null -ne $clientProcess.ExitCode) { $exitCode = [int]$clientProcess.ExitCode }
            } catch { $exitCode = 0 }
            if ($exitCode -ne 0) { Add-Result $name "FAIL" $exitCode $output $errorOutput }
            else { Add-Result $name "PASS" 0 $output $errorOutput }
        } catch {
            Add-Result $name "FAIL" 1 "" $_.Exception.Message
        } finally {
            foreach ($key in $environment.Keys) { [Environment]::SetEnvironmentVariable($key, $oldEnv[$key]) }
            Stop-LocalServer
        }
    }
} finally {
    Stop-LocalServer
}

$report = [ordered]@{ generated_at = [DateTime]::UtcNow.ToString("o"); clients = $clients; results = $results; passed = (($results | Where-Object status -ne "PASS").Count -eq 0 -and $results.Count -eq $clients.Count) }
$reportPath = Join-Path $resolvedReport ("client-restart-reconnect-{0}.json" -f $runId)
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $reportPath -Encoding UTF8
$report | ConvertTo-Json -Depth 8
if (-not $report.passed) { exit 1 }
