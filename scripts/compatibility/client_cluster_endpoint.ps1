[CmdletBinding()]
param(
    [ValidateSet("all", "mysql-cli", "go", "python", "node")][string]$Client = "all",
    [switch]$DiagnosticOnly,
    [string]$ServerConfig = "conf/jdbc-compat-clean.ini",
    [int]$SourcePort = 3312,
    [int]$ReplicaPort = 3313,
    [int]$SourceControlPort = 4411,
    [int]$ReplicaControlPort = 4412,
    [string]$User = "root",
    [switch]$UseDockerMySqlCli,
    [string]$MySqlCliDockerImage = "mysql:8.4.11",
    [string]$ReportDirectory = "reports/compatibility/client-cluster-endpoint"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$workspaceRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "../.."))
$resolvedReport = [IO.Path]::GetFullPath($ReportDirectory)
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$runDirectory = Join-Path $resolvedReport $runId
New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null
$reportPath = Join-Path $resolvedReport ("client-cluster-endpoint-{0}.json" -f $runId)
$clients = if ($Client -eq "all") { @("mysql-cli", "go", "python", "node") } else { @($Client) }
$steps = [System.Collections.Generic.List[object]]::new()
$serverProcesses = [System.Collections.Generic.List[object]]::new()
$environmentBlocked = $false

function Add-Step([string]$name, [string]$status, [string]$endpoint = $null, [string]$client = $null, [string]$evidence = $null, [string]$errorMessage = $null) {
    $steps.Add([ordered]@{
        name = $name
        status = $status
        endpoint = $endpoint
        client = $client
        evidence = $evidence
        error = $errorMessage
    })
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

function Get-ClientAvailability([string]$name) {
    switch ($name) {
        "mysql-cli" {
            $native = Test-CommandAvailable "mysql"
            $docker = $UseDockerMySqlCli -and (Test-CommandAvailable "docker")
            $available = $native -or $docker
            return @{ available = $available; message = $(if ($native) { "mysql executable detected" } elseif ($docker) { "mysql CLI will run from $MySqlCliDockerImage" } else { "mysql executable not found and Docker CLI fallback is disabled or unavailable" }) }
        }
        "go" { return @{ available = Test-CommandAvailable "go"; message = "Go runtime detected" } }
        "python" {
            if (Test-PythonDependency "pymysql") { return @{ available = $true; message = "Python runtime and PyMySQL detected" } }
            if (Test-CommandAvailable "python") { return @{ available = $false; message = "Python runtime detected but PyMySQL is not installed" } }
            return @{ available = $false; message = "python executable not found" }
        }
        "node" {
            if (Test-NodeDependency "mysql2/promise") { return @{ available = $true; message = "Node runtime and mysql2 detected" } }
            if (Test-CommandAvailable "node") { return @{ available = $false; message = "Node runtime detected but mysql2 is not installed" } }
            return @{ available = $false; message = "node executable not found" }
        }
    }
    throw "unsupported client: $name"
}

function Wait-ForHttp([string]$url, [int]$attempts = 60) {
    $lastError = $null
    for ($i = 0; $i -lt $attempts; $i++) {
        try {
            return Invoke-RestMethod -Method Get -Uri $url -TimeoutSec 3
        } catch {
            $lastError = $_.Exception.Message
            Start-Sleep -Milliseconds 500
        }
    }
    throw "endpoint did not become ready: $url; last error: $lastError"
}

function New-NodeConfig([string]$role, [string]$uuid, [int]$serverId, [int]$sqlPort, [int]$controlPort, [string]$dataDirectory, [string]$sourceUrl, [string]$nodeDirectory) {
    if (-not (Test-Path -LiteralPath $ServerConfig)) { throw "server config not found: $ServerConfig" }
    $config = Get-Content -LiteralPath $ServerConfig -Raw
    $config = [regex]::Replace($config, '(?m)^\s*port\s*=.*$', "port = $sqlPort")
    $config = [regex]::Replace($config, '(?m)^\s*datadir\s*=.*$', "datadir = $dataDirectory")
    $config = [regex]::Replace($config, '(?m)^\s*data_dir\s*=.*$', "data_dir = $dataDirectory")
    $config = [regex]::Replace($config, '(?m)^\s*redo_log_dir\s*=.*$', "redo_log_dir = $dataDirectory\redo")
    $config = [regex]::Replace($config, '(?m)^\s*undo_log_dir\s*=.*$', "undo_log_dir = $dataDirectory\undo")
    $config = [regex]::Replace($config, '(?m)^\s*log_error\s*=.*$', "log_error = $nodeDirectory\error.log")
    $config = [regex]::Replace($config, '(?m)^\s*log_infos\s*=.*$', "log_infos = $nodeDirectory\info.log")
    $replication = @"

[replication]
role = $role
uuid = $uuid
server_id = $serverId
listen_address = 127.0.0.1:$controlPort
source_url = $sourceUrl
poll_interval = 100ms
read_only = $(if ($role -eq "replica") { "true" } else { "false" })
"@
    $configPath = Join-Path $nodeDirectory "$role.ini"
    Set-Content -LiteralPath $configPath -Value ($config + $replication) -Encoding UTF8
    return $configPath
}

function Start-Node([string]$name, [string]$role, [string]$uuid, [int]$serverId, [int]$sqlPort, [int]$controlPort, [string]$sourceUrl) {
    $nodeDirectory = Join-Path $runDirectory $name
    $dataDirectory = Join-Path $nodeDirectory "data"
    New-Item -ItemType Directory -Force -Path $nodeDirectory | Out-Null
    $configPath = New-NodeConfig $role $uuid $serverId $sqlPort $controlPort $dataDirectory $sourceUrl $nodeDirectory
    $stdoutPath = Join-Path $nodeDirectory "stdout.log"
    $stderrPath = Join-Path $nodeDirectory "stderr.log"
    $process = Start-Process -FilePath (Join-Path $runDirectory "xmysql-cluster-client.exe") -ArgumentList "-configPath", $configPath -WorkingDirectory $workspaceRoot -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -WindowStyle Hidden -PassThru
    $serverProcesses.Add($process)
    $status = Wait-ForHttp ("http://127.0.0.1:{0}/replication/status" -f $controlPort)
    $sqlReady = $false
    for ($i = 0; $i -lt 60; $i++) {
        if (Get-NetTCPConnection -LocalPort $sqlPort -State Listen -ErrorAction SilentlyContinue) { $sqlReady = $true; break }
        Start-Sleep -Milliseconds 500
    }
    if (-not $sqlReady) { throw "$name SQL endpoint did not listen on port $sqlPort" }
    Add-Step "${name}-ready" "PASS" "127.0.0.1:$sqlPort" $null $configPath
    return $status
}

function Stop-Node([object]$process) {
    if ($null -eq $process) { return }
    $live = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
    if ($null -ne $live) { Stop-Process -Id $process.Id -Force }
}

function Invoke-ClientMatrix([string]$name, [string]$endpoint, [int]$port) {
    $clientReportDirectory = Join-Path $runDirectory ("{0}-{1}" -f $endpoint, $name)
    New-Item -ItemType Directory -Force -Path $clientReportDirectory | Out-Null
    $matrixScript = Join-Path $workspaceRoot "scripts/compatibility/client_matrix.ps1"
    $matrixArguments = @("-Client", $name, "-SkipServerStart", "-Port", "$port", "-User", $User, "-ReportDirectory", $clientReportDirectory)
    if ($name -eq "mysql-cli" -and $UseDockerMySqlCli) {
        $matrixArguments += @("-UseDockerMySqlCli", "-MySqlCliDockerImage", $MySqlCliDockerImage)
    }
    & powershell -NoProfile -ExecutionPolicy Bypass -File $matrixScript @matrixArguments 2>&1 | Out-Null
    $exitCode = $LASTEXITCODE
    $matrixReport = Get-ChildItem -LiteralPath $clientReportDirectory -Filter "client-matrix-*.json" -File | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($null -eq $matrixReport) {
        Add-Step "client-matrix-$endpoint" "FAIL" "127.0.0.1:$port" $name $clientReportDirectory "client matrix did not produce a report"
        return $false
    }
    $parsed = Get-Content -LiteralPath $matrixReport.FullName -Raw | ConvertFrom-Json
    $status = if ($parsed.passed -and $exitCode -eq 0) { "PASS" } elseif (@($parsed.results | Where-Object { $_.status -eq "SKIPPED_ENVIRONMENT" }).Count -gt 0) { "SKIPPED_ENVIRONMENT" } else { "FAIL" }
    $errorMessage = if ($status -eq "PASS") { $null } else { "client matrix status: $status" }
    Add-Step "client-matrix-$endpoint" $status "127.0.0.1:$port" $name $matrixReport.FullName $errorMessage
    return $status -eq "PASS"
}

function Wait-ReplicaCaughtUp([int]$sourceControlPort, [int]$replicaControlPort) {
    $lastSource = $null
    $lastReplica = $null

    function Get-StatusProperty([object]$status, [string]$name) {
        if ($null -eq $status) { return "" }
        $property = $status.PSObject.Properties[$name]
        if ($null -eq $property) { return "" }
        return [string]$property.Value
    }

    for ($i = 0; $i -lt 120; $i++) {
        try {
            $lastSource = Invoke-RestMethod -Method Get -Uri ("http://127.0.0.1:{0}/replication/status" -f $sourceControlPort) -TimeoutSec 3
            $lastReplica = Invoke-RestMethod -Method Get -Uri ("http://127.0.0.1:{0}/replication/status" -f $replicaControlPort) -TimeoutSec 3
            $replicaLastError = Get-StatusProperty $lastReplica "last_error"
            $sourceGTIDs = Get-StatusProperty $lastSource "executed_gtids"
            $replicaGTIDs = Get-StatusProperty $lastReplica "executed_gtids"
            if ($lastReplica.role -eq "replica" -and [string]::IsNullOrWhiteSpace($replicaLastError) -and
                -not [string]::IsNullOrWhiteSpace($sourceGTIDs) -and
                $sourceGTIDs -eq $replicaGTIDs) {
                return [ordered]@{ source = $lastSource; replica = $lastReplica }
            }
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    throw "replica did not catch up; source_gtids=$(Get-StatusProperty $lastSource 'executed_gtids'); replica_gtids=$(Get-StatusProperty $lastReplica 'executed_gtids'); replica_error=$(Get-StatusProperty $lastReplica 'last_error')"
}

try {
    if (-not (Test-CommandAvailable "go")) {
        Add-Step "environment" "SKIPPED_ENVIRONMENT" $null $null $null "go executable not found"
        $environmentBlocked = $true
    }
    foreach ($name in $clients) {
        $availability = Get-ClientAvailability $name
        Add-Step "client-availability" $(if ($availability.available) { "AVAILABLE" } else { "SKIPPED_ENVIRONMENT" }) $null $name $null $availability.message
        if (-not $availability.available) { $environmentBlocked = $true }
    }
    $hasPassword = -not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable("XMYSQL_CLIENT_PASSWORD"))
    Add-Step "protected-auth-environment" $(if ($hasPassword) { "AVAILABLE" } else { "SKIPPED_ENVIRONMENT" }) $null $null $null $(if ($hasPassword) { "XMYSQL_CLIENT_PASSWORD is set" } else { "set XMYSQL_CLIENT_PASSWORD for a real authenticated run" })
    if (-not $hasPassword) { $environmentBlocked = $true }

    if (-not $DiagnosticOnly -and -not $environmentBlocked) {
        $serverExe = Join-Path $runDirectory "xmysql-cluster-client.exe"
        & go build -o $serverExe .
        if ($LASTEXITCODE -ne 0) { throw "server build failed" }
        $sourceStatus = Start-Node "source" "source" "client-cluster-source-$runId" 701 $SourcePort $SourceControlPort ""
        $replicaStatus = Start-Node "replica" "replica" "client-cluster-replica-$runId" 702 $ReplicaPort $ReplicaControlPort ("http://127.0.0.1:{0}" -f $SourceControlPort)
        if ($sourceStatus.role -ne "source" -or $replicaStatus.role -ne "replica") { throw "unexpected replication roles at startup" }

        $sourcePassed = $true
        foreach ($name in $clients) { if (-not (Invoke-ClientMatrix $name "source" $SourcePort)) { $sourcePassed = $false } }
        if (-not $sourcePassed) { throw "one or more clients failed against the source endpoint" }

        $catchUp = Wait-ReplicaCaughtUp $SourceControlPort $ReplicaControlPort
        Add-Step "replica-catch-up" "PASS" "127.0.0.1:$ReplicaControlPort" $null $null ("executed_gtids=" + [string]$catchUp.replica.executed_gtids)
        Stop-Node $serverProcesses[0]
        Add-Step "source-stop-before-promote" "PASS" "127.0.0.1:$SourcePort"
        $promoted = Invoke-RestMethod -Method Post -Uri ("http://127.0.0.1:{0}/replication/promote" -f $ReplicaControlPort) -TimeoutSec 5
        if ($promoted.role -ne "source") { throw "replica promotion did not return role=source" }
        Add-Step "replica-promote" "PASS" "127.0.0.1:$ReplicaPort" $null $null ("promoted_uuid=" + [string]$promoted.uuid)
        foreach ($name in $clients) { [void](Invoke-ClientMatrix $name "promoted" $ReplicaPort) }
    }
} catch {
    Add-Step "runner" "FAIL" $null $null $null $_.Exception.Message
} finally {
    foreach ($process in $serverProcesses) { Stop-Node $process }
}

$passStatuses = @("PASS", "AVAILABLE")
$report = [ordered]@{
    generated_at = [DateTime]::UtcNow.ToString("o")
    clients = $clients
    diagnostic_only = [bool]$DiagnosticOnly
    source_endpoint = "127.0.0.1:$SourcePort"
    promoted_endpoint = "127.0.0.1:$ReplicaPort"
    steps = $steps
    passed = (-not $environmentBlocked -and @($steps | Where-Object { $_.status -notin $passStatuses }).Count -eq 0)
}
$report | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $reportPath -Encoding UTF8
Write-Output ($report | ConvertTo-Json -Depth 10)
if (-not $report.passed) { exit 1 }
