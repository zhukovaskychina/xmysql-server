param(
    [string]$ReportDir = "reports/compatibility/performance-benchmark",
    [int]$Count = 3,
    [string]$BenchRegex = "^(Benchmark(HashJoin_(SmallTables|MediumTables)|HashAgg_(SmallGroups|MediumGroups)|ConcurrentInsert|RangeQuery|MVCC_(IsVisible|BeginCommit))|BenchmarkTableScanOperator|BenchmarkStorageIntegratedDMLExecutor_(Serialization|Deserialization))$",
    [string]$BaselineReport = "",
    [double]$MaxRegressionPercent = 10
)

$ErrorActionPreference = "Stop"
if ($Count -lt 1) {
    throw "Count must be positive"
}
if ($MaxRegressionPercent -lt 0) {
    throw "MaxRegressionPercent must not be negative"
}
New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$command = "go test ./server/innodb/engine ./server/innodb/manager -run ^$ -bench `"$BenchRegex`" -benchmem -count $Count"
$started = (Get-Date).ToUniversalTime()
$output = @(& go test ./server/innodb/engine ./server/innodb/manager -run '^$' -bench $BenchRegex -benchmem -count $Count 2>&1)
$exitCode = $LASTEXITCODE
$output | Set-Content -LiteralPath (Join-Path $ReportDir "benchmark-output.txt") -Encoding UTF8

$samples = @()
foreach ($line in $output) {
    $text = [string]$line
    if ($text -match '^\s*(Benchmark\S+)\s+(\d+)\s+([0-9.]+)\s+ns/op(?:\s+([0-9.]+)\s+B/op\s+([0-9.]+)\s+allocs/op)?') {
        $samples += [ordered]@{
            name = $Matches[1]
            iterations = [int64]$Matches[2]
            ns_per_op = [double]$Matches[3]
            bytes_per_op = if ($Matches[4]) { [double]$Matches[4] } else { $null }
            allocs_per_op = if ($Matches[5]) { [double]$Matches[5] } else { $null }
        }
    }
}

$summary = @()
$sampleNames = @($samples | ForEach-Object { [string]$_.name } | Sort-Object -Unique)
foreach ($sampleName in $sampleNames) {
    $groupSamples = @($samples | Where-Object { [string]$_.name -eq $sampleName })
    $nsValues = @($groupSamples | ForEach-Object { [double]$_.ns_per_op })
    $bytesValues = @($groupSamples | Where-Object { $null -ne $_.bytes_per_op } | ForEach-Object { [double]$_.bytes_per_op })
    $allocValues = @($groupSamples | Where-Object { $null -ne $_.allocs_per_op } | ForEach-Object { [double]$_.allocs_per_op })
    $nsStats = $nsValues | Measure-Object -Minimum -Average -Maximum
    $summary += [ordered]@{
        name = $sampleName
        sample_count = $groupSamples.Count
        min_ns_per_op = [double]$nsStats.Minimum
        mean_ns_per_op = [double]$nsStats.Average
        max_ns_per_op = [double]$nsStats.Maximum
        mean_ops_per_sec = if ($nsStats.Average -gt 0) { [double](1e9 / $nsStats.Average) } else { $null }
        mean_bytes_per_op = if ($bytesValues.Count -gt 0) { [double](($bytesValues | Measure-Object -Average).Average) } else { $null }
        mean_allocs_per_op = if ($allocValues.Count -gt 0) { [double](($allocValues | Measure-Object -Average).Average) } else { $null }
    }
}

$baselineComparison = @()
$baselineStatus = "NOT_CONFIGURED"
if (-not [string]::IsNullOrWhiteSpace($BaselineReport)) {
    if (-not (Test-Path -LiteralPath $BaselineReport -PathType Leaf)) {
        throw "BaselineReport does not exist: $BaselineReport"
    }
    $baseline = Get-Content -LiteralPath $BaselineReport -Raw | ConvertFrom-Json
    if ($null -eq $baseline.summary) {
        throw "BaselineReport does not contain a summary array: $BaselineReport"
    }

    $baselineStatus = "PASS"
    foreach ($current in $summary) {
        $baselineEntry = @($baseline.summary | Where-Object { [string]$_.name -eq [string]$current.name }) | Select-Object -First 1
        if ($null -eq $baselineEntry) {
            $baselineComparison += [ordered]@{
                name = $current.name
                status = "MISSING_BASELINE"
                regression_percent = $null
                baseline_mean_ns_per_op = $null
                current_mean_ns_per_op = $current.mean_ns_per_op
            }
            $baselineStatus = "FAIL"
            continue
        }

        $baselineMean = [double]$baselineEntry.mean_ns_per_op
        $currentMean = [double]$current.mean_ns_per_op
        $regression = $null
        $comparisonStatus = "NOT_COMPARABLE"
        if ($baselineMean -gt 0) {
            $regression = [double](($currentMean - $baselineMean) / $baselineMean * 100)
            $comparisonStatus = if ($regression -le $MaxRegressionPercent) { "PASS" } else { "REGRESSION" }
            if ($comparisonStatus -eq "REGRESSION") {
                $baselineStatus = "FAIL"
            }
        } else {
            $baselineStatus = "FAIL"
        }
        $baselineComparison += [ordered]@{
            name = $current.name
            status = $comparisonStatus
            regression_percent = $regression
            baseline_mean_ns_per_op = $baselineMean
            current_mean_ns_per_op = $currentMean
        }
    }
}

$report = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    started_at = $started.ToString("o")
    command = $command
    count = $Count
    benchmark_regex = $BenchRegex
    exit_code = $exitCode
    sample_count = $samples.Count
    samples = $samples
    summary = $summary
    baseline = [ordered]@{
        report = if ([string]::IsNullOrWhiteSpace($BaselineReport)) { $null } else { [IO.Path]::GetFullPath($BaselineReport) }
        max_regression_percent = $MaxRegressionPercent
        comparisons = $baselineComparison
        status = $baselineStatus
    }
    status = if ($exitCode -eq 0 -and $samples.Count -gt 0 -and $baselineStatus -ne "FAIL") { "PASS" } else { "FAIL" }
    output_file = [IO.Path]::GetFullPath((Join-Path $ReportDir "benchmark-output.txt"))
}
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $ReportDir "benchmark-report.json") -Encoding UTF8
$report.status
if ($report.status -ne "PASS") { exit 1 }
