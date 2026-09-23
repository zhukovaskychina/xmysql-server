param(
    [string]$ReportDir = "reports/compatibility/release-candidate",
    [switch]$SkipJDBC,
    [switch]$SkipClientMatrix,
    [string]$JdbcServerConfig = "conf/jdbc-compat-clean.ini",
    [string]$JdbcUrl = "jdbc:mysql://localhost:3311?useSSL=false&allowPublicKeyRetrieval=true",
    [string]$JdbcUser = "root",
    [string]$JdbcPassword = "root@1234"
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null
$checks = @()

function Invoke-GateCheck([string]$Name, [string]$Command, [scriptblock]$Action) {
    $started = Get-Date
    $output = & $Action 2>&1
    $code = $LASTEXITCODE
    $testCount = @($output | Where-Object { $_ -match '^(ok\s|--- PASS:)' }).Count
    $mavenCounts = @($output | ForEach-Object {
        if ($_ -match '\[(?:INFO|WARNING)\]\s+Tests run:\s+(\d+)') {
            [int]$Matches[1]
        }
    })
    if ($mavenCounts.Count -gt 0) {
        $testCount = $mavenCounts[-1]
    }
    $script:checks += [ordered]@{
        name = $Name
        command = $Command
        started_at = $started.ToUniversalTime().ToString("o")
        exit_code = $code
        test_count = $testCount
        output_tail = (($output | Select-Object -Last 60) -join "`n")
        status = if ($code -eq 0) { "PASS" } else { "FAIL" }
    }
}

Invoke-GateCheck "clean-data" "validate isolated release report directory" {
    $resolvedReport = [IO.Path]::GetFullPath($ReportDir)
    $resolvedWorkspaceData = [IO.Path]::GetFullPath((Join-Path (Get-Location) "data"))
    if ($resolvedReport.Equals($resolvedWorkspaceData, [StringComparison]::OrdinalIgnoreCase) -or
        $resolvedReport.StartsWith($resolvedWorkspaceData + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        Write-Output "release report directory must be isolated from the live data directory"
        cmd /c exit 1
        return
    }
    Write-Output "isolated report directory: $resolvedReport"
    cmd /c exit 0
}
Invoke-GateCheck "build" "go build ./server/... ./cmd/p0_metrics_export ./cmd/p0_structured_log_export ./cmd/p0_metrics_endpoint ./cmd/persistence_demo" {
    go build ./server/... ./cmd/p0_metrics_export ./cmd/p0_structured_log_export ./cmd/p0_metrics_endpoint ./cmd/persistence_demo
}
Invoke-GateCheck "unit" "go test -p 1 ./server/innodb/sqlparser ./server/innodb/plan ./server/innodb/manager ./server/auth -count=1" {
    go test -p 1 ./server/innodb/sqlparser ./server/innodb/plan ./server/innodb/manager ./server/auth -count=1
}
Invoke-GateCheck "integration" "go test -p 1 ./server/innodb/engine ./server/net ./server/protocol ./server/replication -count=1" {
    go test -p 1 ./server/innodb/engine ./server/net ./server/protocol ./server/replication -count=1
}
Invoke-GateCheck "go-core" "go test -p 1 ./server/innodb/engine ./server/innodb/manager ./server/net ./server/protocol ./server/auth ./server/replication -count=1" {
    go test -p 1 ./server/innodb/engine ./server/innodb/manager ./server/net ./server/protocol ./server/auth ./server/replication -count=1
}
Invoke-GateCheck "crash-recovery" "scripts/compatibility/crash_recovery_matrix.ps1 -Repeat 3" {
    & "$PSScriptRoot/crash_recovery_matrix.ps1" -ReportDir (Join-Path $ReportDir "crash-recovery") -Repeat 3
}
Invoke-GateCheck "concurrency" "go test ./server/innodb/manager -run 'Test(P0C|LockManager|.*Concurrent|.*Concurrency)' -count=100" {
    go test ./server/innodb/manager -run 'Test(P0C|LockManager|.*Concurrent|.*Concurrency)' -count=100
}
Invoke-GateCheck "observability" "scripts/compatibility/observability_smoke.ps1" {
    & "$PSScriptRoot/observability_smoke.ps1" -ReportDir (Join-Path $ReportDir "observability")
}
if (-not $SkipClientMatrix) {
    Invoke-GateCheck "client-matrix" "scripts/compatibility/client_matrix.ps1" {
        & "$PSScriptRoot/client_matrix.ps1" -ReportDirectory (Join-Path $ReportDir "client-matrix")
    }
} else {
    $checks += [ordered]@{
        name = "client-matrix"
        command = "scripts/compatibility/client_matrix.ps1"
        started_at = (Get-Date).ToUniversalTime().ToString("o")
        exit_code = 125
        output_tail = "client matrix was explicitly skipped"
        status = "FAIL"
    }
}
if (-not $SkipJDBC) {
    Invoke-GateCheck "jdbc" "go build .; start isolated xmysql; XMYSQL_JDBC_URL=$JdbcUrl mvn test -Pjdbc-connectivity" {
        $workspaceRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "../.."))
        $serverExe = Join-Path $workspaceRoot (Join-Path $ReportDir "jdbc-server/xmysql-server-jdbc.exe")
        $serverLogDir = Split-Path -Parent $serverExe
		# The JDBC gate is intentionally repeatable. Remove only the generated
		# per-run server directory so stale .frm/.ibd mappings cannot survive a
		# previous failed cursor/drop cleanup into the next run.
		if (Test-Path -LiteralPath $serverLogDir) {
			Remove-Item -LiteralPath $serverLogDir -Recurse -Force
		}
        New-Item -ItemType Directory -Force -Path $serverLogDir | Out-Null

        Push-Location $workspaceRoot
        $serverProcess = $null
        $startedHere = $false
        $oldURL = $env:XMYSQL_JDBC_URL
        $oldUser = $env:XMYSQL_JDBC_USER
        $oldPassword = $env:XMYSQL_JDBC_PASSWORD
        $mvnExitCode = 1
        try {
            go build -o $serverExe .

            $configPath = [IO.Path]::GetFullPath($JdbcServerConfig)
            if (-not (Test-Path -LiteralPath $configPath)) {
                throw "JDBC server config not found: $configPath"
            }

            # Keep each JDBC gate self-contained. Reusing the fixed developer
            # data directory allows a failed DROP/TRUNCATE to poison the next
            # run with stale .frm/.ibd state and makes the gate non-repeatable.
            $isolatedDataDir = [IO.Path]::GetFullPath((Join-Path $serverLogDir "data"))
            $isolatedConfigPath = Join-Path $serverLogDir "jdbc-compat.ini"
            $isolatedConfig = Get-Content -LiteralPath $configPath -Raw
            $isolatedConfig = [regex]::Replace($isolatedConfig, '(?m)^\s*datadir\s*=.*$', "datadir = $isolatedDataDir")
            $isolatedConfig = [regex]::Replace($isolatedConfig, '(?m)^\s*data_dir\s*=.*$', "data_dir = $isolatedDataDir")
            $isolatedConfig = [regex]::Replace($isolatedConfig, '(?m)^\s*redo_log_dir\s*=.*$', "redo_log_dir = $isolatedDataDir\redo")
            $isolatedConfig = [regex]::Replace($isolatedConfig, '(?m)^\s*undo_log_dir\s*=.*$', "undo_log_dir = $isolatedDataDir\undo")
            $isolatedConfig = [regex]::Replace($isolatedConfig, '(?m)^\s*log_error\s*=.*$', "log_error = $(Join-Path $serverLogDir 'error.log')")
            $isolatedConfig = [regex]::Replace($isolatedConfig, '(?m)^\s*log_infos\s*=.*$', "log_infos = $(Join-Path $serverLogDir 'mysql.log')")
            Set-Content -LiteralPath $isolatedConfigPath -Value $isolatedConfig -Encoding UTF8

            $portMatch = [regex]::Match($JdbcUrl, '(?<=:)[0-9]{1,5}(?=[/?]|$)')
            if (-not $portMatch.Success) {
                throw "JDBC URL does not contain a valid TCP port: $JdbcUrl"
            }
            $port = [int]$portMatch.Value
            if ($port -lt 1 -or $port -gt 65535) {
                throw "JDBC URL contains an invalid TCP port: $port"
            }
            $listener = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
            if ($null -ne $listener) {
                $owner = Get-Process -Id $listener[0].OwningProcess -ErrorAction Stop
                if ($owner.Path -ne $serverExe) {
                    throw "JDBC port $port is occupied by $($owner.Path); refusing to reuse an unrelated server"
                }
            } else {
                $serverProcess = Start-Process -FilePath $serverExe -ArgumentList '-configPath', $isolatedConfigPath -WorkingDirectory $workspaceRoot -RedirectStandardOutput (Join-Path $serverLogDir "stdout.log") -RedirectStandardError (Join-Path $serverLogDir "stderr.log") -WindowStyle Hidden -PassThru
                $startedHere = $true
                $ready = $false
                for ($i = 0; $i -lt 60; $i++) {
                    Start-Sleep -Milliseconds 500
                    if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) {
                        $ready = $true
                        break
                    }
                }
                if (-not $ready) {
                    throw "isolated JDBC server did not listen on port $port"
                }
            }

            $env:XMYSQL_JDBC_URL = $JdbcUrl
            $env:XMYSQL_JDBC_USER = $JdbcUser
            $env:XMYSQL_JDBC_PASSWORD = $JdbcPassword
            Push-Location (Join-Path $workspaceRoot "jdbc_client")
            try {
                # Maven/JDK may emit deprecation notices on stderr. Preserve
                # the native Maven exit code instead of letting Strict Stop
                # treat a warning record as a terminating PowerShell error.
                $previousErrorActionPreference = $ErrorActionPreference
                $ErrorActionPreference = "Continue"
                try {
                    mvn test -Pjdbc-connectivity 2>&1
                    $mvnExitCode = $LASTEXITCODE
                } finally {
                    $ErrorActionPreference = $previousErrorActionPreference
                }
            } finally {
                Pop-Location
            }
        } finally {
            $env:XMYSQL_JDBC_URL = $oldURL
            $env:XMYSQL_JDBC_USER = $oldUser
            $env:XMYSQL_JDBC_PASSWORD = $oldPassword
            if ($startedHere -and $null -ne $serverProcess) {
                $live = Get-Process -Id $serverProcess.Id -ErrorAction SilentlyContinue
                if ($null -ne $live -and $live.Path -eq $serverExe) {
                    Stop-Process -Id $serverProcess.Id -Force
                }
            }
            Pop-Location
        }
        cmd /c exit $mvnExitCode
    }
} else {
    $checks += [ordered]@{
        name = "jdbc"
        command = "mvn test -Pjdbc-connectivity"
        started_at = (Get-Date).ToUniversalTime().ToString("o")
        exit_code = 125
        output_tail = "JDBC check was explicitly skipped"
        status = "FAIL"
    }
}

$requiredChecks = @("clean-data", "build", "unit", "integration", "crash-recovery", "concurrency", "observability", "client-matrix", "jdbc")
$missingChecks = @($requiredChecks | Where-Object {
    $requiredName = $_
    -not ($checks | Where-Object { $_.name -eq $requiredName })
})
if ($missingChecks.Count -gt 0) {
    $checks += [ordered]@{
        name = "gate-completeness"
        command = "required checks: $($requiredChecks -join ', ')"
        started_at = (Get-Date).ToUniversalTime().ToString("o")
        exit_code = 1
        test_count = 0
        output_tail = "missing checks: $($missingChecks -join ', ')"
        status = "FAIL"
    }
}

foreach ($check in $checks) {
    if ($check.output_tail -match '(?i)panic:|data diff|stale artifact') {
        $check.status = "FAIL"
        $check.exit_code = 1
    }
}

foreach ($evidencePath in @(
    (Join-Path $ReportDir "crash-recovery/crash-recovery.json"),
    (Join-Path $ReportDir "observability/observability.json")
)) {
    if (-not (Test-Path -LiteralPath $evidencePath)) {
        $checks += [ordered]@{
            name = "evidence-file"
            command = "Test-Path $evidencePath"
            started_at = (Get-Date).ToUniversalTime().ToString("o")
            exit_code = 1
            test_count = 0
            output_tail = "required evidence file missing: $evidencePath"
            status = "FAIL"
        }
    }
}

$report = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    git_revision = (git rev-parse HEAD)
    config_summary = [ordered]@{
        powershell = $PSVersionTable.PSVersion.ToString()
        go = (go version)
        report_dir = [IO.Path]::GetFullPath($ReportDir)
        jdbc_required = (-not $SkipJDBC)
        jdbc_server_config = [IO.Path]::GetFullPath($JdbcServerConfig)
        jdbc_url = $JdbcUrl
    }
    checks = $checks
    status = if (($checks | Where-Object { $_.status -eq "FAIL" }).Count -eq 0) { "GO" } else { "NO-GO" }
    note = "JDBC is mandatory by default; use -SkipJDBC only for local diagnostics, which always produces NO-GO."
}
$report | ConvertTo-Json -Depth 8 | Set-Content (Join-Path $ReportDir "release-candidate.json")
$report.status
if ($report.status -ne "GO") { exit 1 }
