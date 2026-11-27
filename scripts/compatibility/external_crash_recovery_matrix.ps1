[CmdletBinding()]
param(
    [string]$ConfigPath = "conf/external-crash-compat.ini",
    [string]$ReportDirectory = "reports/compatibility/external-crash",
    [int]$Repeat = 3,
    [int]$Port = 3312,
    [string]$DataDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
Set-Location $repo
New-Item -ItemType Directory -Force -Path $ReportDirectory | Out-Null
$serverExe = Join-Path (Resolve-Path $ReportDirectory).Path "xmysql-external-crash.exe"
$clientExe = Join-Path (Resolve-Path $ReportDirectory).Path "recovery_drill_client.exe"
$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$dbStamp = Get-Date -Format "yyyyMMddHHmmss"
$reportPath = Join-Path (Resolve-Path $ReportDirectory).Path ("external-crash-{0}.json" -f $runStamp)
$port = $Port
if ([string]::IsNullOrWhiteSpace($DataDir)) {
    $DataDir = "data/external-crash-compat-$runStamp"
}
$runtimeConfigPath = Join-Path (Resolve-Path $ReportDirectory).Path "external-crash-runtime.ini"
$configText = Get-Content $ConfigPath -Raw
$configText = [regex]::Replace($configText, '(?m)^port\s*=.*$', "port = $port")
$configText = [regex]::Replace($configText, '(?m)^datadir\s*=.*$', "datadir = $DataDir")
$configText = [regex]::Replace($configText, '(?m)^data_dir\s*=.*$', "data_dir = $DataDir")
$configText = [regex]::Replace($configText, '(?m)^redo_log_dir\s*=.*$', "redo_log_dir = $DataDir/redo")
$configText = [regex]::Replace($configText, '(?m)^undo_log_dir\s*=.*$', "undo_log_dir = $DataDir/undo")
Set-Content -Path $runtimeConfigPath -Value $configText -Encoding UTF8
$tableName = "crash_rows"

function Wait-Port([int]$Port, [int]$TimeoutSeconds = 30) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $client = [System.Net.Sockets.TcpClient]::new()
        try {
            $task = $client.ConnectAsync("127.0.0.1", $Port)
            $connected = $task.Wait(500)
            if ($connected -and $client.Connected) { $client.Close(); return }
        } catch { }
        $client.Dispose()
        Start-Sleep -Milliseconds 250
    }
    throw "server did not listen on port $Port"
}

function Invoke-Client([string]$Mode, [string[]]$Extra = @()) {
    $args = @("-dsn", "root:@tcp(127.0.0.1:$port)/mysql?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true", "-db", $dbName, "-table", $tableName, "-mode", $Mode) + $Extra
    $output = & $clientExe @args 2>&1
    if ($LASTEXITCODE -ne 0) { throw "client mode $Mode failed: $output" }
    return @($output)
}

function Start-TestServer {
    $stdout = Join-Path (Resolve-Path $ReportDirectory).Path "server.out.log"
    $stderr = Join-Path (Resolve-Path $ReportDirectory).Path "server.err.log"
    $process = Start-Process -FilePath $serverExe -ArgumentList @("-configPath", $runtimeConfigPath) -PassThru -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    Wait-Port $port
    return $process
}

function Stop-TestServer($Process) {
    if ($null -ne $Process -and -not $Process.HasExited) {
        Stop-Process -Id $Process.Id -Force
        $Process.WaitForExit(10000) | Out-Null
    }
}

& go build -o $serverExe .
if ($LASTEXITCODE -ne 0) { throw "server build failed" }
& go build -o $clientExe ./cmd/recovery_drill_client
if ($LASTEXITCODE -ne 0) { throw "recovery client build failed" }

$runs = @()
for ($run = 1; $run -le $Repeat; $run++) {
	$dbName = "external_crash_db_${dbStamp}_$run"
    $server = $null
    $holder = $null
    $race = $null
    $holderLog = Join-Path (Resolve-Path $ReportDirectory).Path ("holder-$run.log")
    try {
        $server = Start-TestServer
        Invoke-Client "setup_redo" | Out-Null
        Invoke-Client "setup_ddl_index" | Out-Null
        $holder = Start-Process -FilePath $clientExe -ArgumentList @("-dsn", "root:@tcp(127.0.0.1:$port)/mysql?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true", "-db", $dbName, "-table", $tableName, "-mode", "hold_undo", "-hold-seconds", "30") -PassThru -WindowStyle Hidden -RedirectStandardOutput $holderLog -RedirectStandardError ($holderLog + ".err")
        $ready = $false
        for ($i = 0; $i -lt 120; $i++) {
            if (Test-Path $holderLog) {
                $holderOutput = Get-Content $holderLog -Raw
                if ($null -ne $holderOutput -and $holderOutput.Contains("UNDO_HOLD_READY")) { $ready = $true; break }
            }
            Start-Sleep -Milliseconds 250
        }
        if (-not $ready) { throw "uncommitted workload did not reach ready state" }
        Stop-TestServer $server
        $server = Start-TestServer
        Invoke-Client "verify_redo" | Out-Null
        Invoke-Client "verify_undo" | Out-Null
        Invoke-Client "verify_ddl_index" | Out-Null
        $raceLog = Join-Path (Resolve-Path $ReportDirectory).Path ("race-$run.log")
        $race = Start-Process -FilePath $clientExe -ArgumentList @("-dsn", "root:@tcp(127.0.0.1:$port)/mysql?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true", "-db", $dbName, "-table", $tableName, "-mode", "race_commit") -PassThru -WindowStyle Hidden -RedirectStandardOutput $raceLog -RedirectStandardError ($raceLog + ".err")
        $raceReady = $false
        for ($i = 0; $i -lt 120; $i++) {
            if (Test-Path $raceLog) {
                $raceOutput = Get-Content $raceLog -Raw
                if ($null -ne $raceOutput -and $raceOutput.Contains("HALF_COMMIT_READY")) { $raceReady = $true; break }
            }
            Start-Sleep -Milliseconds 25
        }
        if (-not $raceReady) { throw "half-commit workload did not reach ready state" }
        Start-Sleep -Milliseconds 100
        Stop-TestServer $server
        $server = Start-TestServer
        Invoke-Client "verify_half_commit" | Out-Null
        if ($null -ne $race -and -not $race.HasExited) { Stop-Process -Id $race.Id -Force }
        $runs += [ordered]@{ run = $run; status = "PASS"; committed = "present"; uncommitted = "rolled_back"; half_commit = "at_most_once"; ddl_index_metadata = "present" }
    } catch {
        $runs += [ordered]@{ run = $run; status = "FAIL"; error = ($_ | Out-String).Trim() }
    } finally {
        if ($null -ne $holder -and -not $holder.HasExited) { Stop-Process -Id $holder.Id -Force }
        if ($null -ne $race -and -not $race.HasExited) { Stop-Process -Id $race.Id -Force }
        Stop-TestServer $server
    }
}
$status = if ($runs.Count -eq $Repeat -and (@($runs | Where-Object status -ne "PASS").Count -eq 0)) { "PASS" } else { "FAIL" }
$report = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    status = $status
    repeat = $Repeat
    port = $port
    data_dir = $DataDir
    runtime_config = $runtimeConfigPath
    cases = @("committed", "uncommitted", "prepared statement", "half-commit crash", "DDL", "index rebuild")
    verification = @("committed rows present", "uncommitted rows rolled back", "half-commit row count at most one", "DDL metadata queryable", "rebuilt index metadata queryable")
    runs = $runs
}
$report | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $reportPath
Write-Output ("external crash recovery status: {0}" -f $status)
Write-Output ("report: {0}" -f $reportPath)
if ($status -ne "PASS") { exit 1 }
