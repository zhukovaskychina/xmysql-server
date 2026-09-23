[CmdletBinding()]
param(
    [string]$OutputPath = "reports/compatibility/scope-matrix.json",
    [switch]$DiagnosticOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-ScopeMatrixEntries {
    # Keep this list explicit. A source file or a passing unit test alone does
    # not establish MySQL compatibility; each row names the observable gate
    # and the evidence location that can prove its status.
    return @(
        [pscustomobject]@{ area = "server"; feature = "mysql-startup-and-core-crud"; status = "implemented"; priority = "P0"; test_command = "go test ./server/net ./server/innodb/engine -count=1"; evidence_path = "reports/compatibility/release-candidate-current-continuation1036/release-candidate.json"; out_of_scope = $false }
        [pscustomobject]@{ area = "cluster"; feature = "replication-and-failover"; status = "implemented"; priority = "P0"; test_command = "scripts/compatibility/cluster_smoke.ps1"; evidence_path = "reports/compatibility/p1-cluster-current-continuation1036/cluster-report.json"; out_of_scope = $false }
        [pscustomobject]@{ area = "clients"; feature = "connector-j-139-gate"; status = "implemented"; priority = "P0"; test_command = "mvn -f jdbc_client/pom.xml test"; evidence_path = "jdbc_client/TEST_README.md"; out_of_scope = $false }

        [pscustomobject]@{ area = "information_schema"; feature = "complete-table-registry"; status = "partial"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestInformationSchema' -count=1"; evidence_path = "server/innodb/engine/information_schema_registry.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "information_schema"; feature = "core-metadata-shapes-and-filters"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestInformationSchema' -count=1"; evidence_path = "server/innodb/engine/executor_ddl_test.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "information_schema"; feature = "innodb-diagnostics-and-tablespaces"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestInformationSchemaInnoDB' -count=1"; evidence_path = "server/innodb/engine/innodb_tablespaces_compatibility_test.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "information_schema"; feature = "privilege-role-visibility"; status = "partial"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestInformationSchema.*Privilege|TestInformationSchema.*Role' -count=1"; evidence_path = "server/innodb/engine/account_metadata_compatibility.go"; out_of_scope = $false }

        [pscustomobject]@{ area = "performance_schema"; feature = "complete-table-registry"; status = "partial"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestPerformanceSchema' -count=1"; evidence_path = "server/innodb/engine/performance_schema_registry.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "performance_schema"; feature = "statements-stages-transactions"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestPerformanceSchema' -count=1"; evidence_path = "server/innodb/engine/performance_schema_compatibility_test.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "performance_schema"; feature = "waits-locks-and-data-locks"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestPerformanceSchema.*(Wait|Lock)' -count=1"; evidence_path = "server/innodb/engine/executor.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "performance_schema"; feature = "threads-sockets-files-memory"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestPerformanceSchema' -count=1"; evidence_path = "server/innodb/engine/performance_schema_compatibility_test.go"; out_of_scope = $false }

        [pscustomobject]@{ area = "clients"; feature = "non-connector-j-matrix"; status = "partial"; priority = "P1-B"; test_command = "scripts/compatibility/client_matrix.ps1"; evidence_path = "reports/compatibility/client-matrix-final-20260922/client-matrix-20260922-012332.json"; out_of_scope = $false }
        [pscustomobject]@{ area = "clients"; feature = "mysql-cli"; status = "unverified"; priority = "P1-B"; test_command = "scripts/compatibility/client_matrix.ps1 -Client mysql-cli"; evidence_path = "reports/compatibility/client-matrix-final-20260922/client-matrix-20260922-012332.json"; out_of_scope = $false }
        [pscustomobject]@{ area = "clients"; feature = "go-mysql-driver"; status = "implemented"; priority = "P1-B"; test_command = "scripts/compatibility/client_matrix.ps1 -Client go"; evidence_path = "reports/compatibility/client-matrix-final-20260922/client-matrix-20260922-012332.json"; out_of_scope = $false }
        [pscustomobject]@{ area = "clients"; feature = "pymysql"; status = "implemented"; priority = "P1-B"; test_command = "scripts/compatibility/client_matrix.ps1 -Client python"; evidence_path = "reports/compatibility/client-matrix-final-20260922/client-matrix-20260922-012332.json"; out_of_scope = $false }
        [pscustomobject]@{ area = "clients"; feature = "node-mysql2"; status = "implemented"; priority = "P1-B"; test_command = "scripts/compatibility/client_matrix.ps1 -Client node"; evidence_path = "reports/compatibility/client-matrix-final-20260922/client-matrix-20260922-012332.json"; out_of_scope = $false }

        [pscustomobject]@{ area = "replication"; feature = "xa-state-machine-and-recovery"; status = "implemented"; priority = "P1-C"; test_command = "go test ./server/innodb/engine -run 'TestXA' -count=1"; evidence_path = "server/innodb/engine/xa_compatibility_test.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "replication"; feature = "xa-native-binlog-interoperability"; status = "partial"; priority = "P1-C"; test_command = "go test ./server/replication ./server/net ./server/innodb/engine -run 'XA|Binlog|Replication|Recovery' -count=1"; evidence_path = "server/innodb/engine/xa_compatibility_test.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "replication"; feature = "native-binlog-dump-and-gtid"; status = "partial"; priority = "P1-C"; test_command = "go test ./server/replication ./server/net -run 'Binlog|GTID' -count=1"; evidence_path = "server/net/binlog_compatibility_test.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "replication"; feature = "crash-recovery-and-promotion"; status = "partial"; priority = "P1-C"; test_command = "scripts/compatibility/external_crash_recovery_matrix.ps1"; evidence_path = "reports/compatibility/external-crash-current-20260922/external-crash-20260922-010331.json"; out_of_scope = $false }

        [pscustomobject]@{ area = "search"; feature = "fulltext"; status = "deferred"; priority = "P2"; test_command = "not scheduled"; evidence_path = "docs/planning/P1_CAPABILITY_BACKLOG_20260715.md"; out_of_scope = $false }
        [pscustomobject]@{ area = "storage"; feature = "non-innodb-engines"; status = "out_of_scope"; priority = "out_of_scope"; test_command = "not scheduled"; evidence_path = "docs/planning/P1_CAPABILITY_BACKLOG_20260715.md"; out_of_scope = $true }
        [pscustomobject]@{ area = "storage"; feature = "non-innodb-repair-and-engine-conversion"; status = "out_of_scope"; priority = "out_of_scope"; test_command = "not scheduled"; evidence_path = "docs/planning/P1_CAPABILITY_BACKLOG_20260715.md"; out_of_scope = $true }
    )
}

function Test-ScopeMatrix {
    param([object[]]$Entries)

    $requiredFields = @("area", "feature", "status", "priority", "test_command", "evidence_path", "out_of_scope")
    $errors = [System.Collections.Generic.List[string]]::new()
    foreach ($entry in $Entries) {
        foreach ($field in $requiredFields) {
            if ($null -eq $entry.PSObject.Properties[$field]) {
                $errors.Add("missing field '$field' for entry")
            }
        }
    }

    $duplicates = @($Entries | Group-Object area, feature | Where-Object Count -gt 1)
    foreach ($duplicate in $duplicates) {
        $errors.Add("duplicate feature '$($duplicate.Name)'")
    }

    $required = @(
        @{ area = "information_schema"; feature = "complete-table-registry" },
        @{ area = "performance_schema"; feature = "complete-table-registry" },
        @{ area = "clients"; feature = "non-connector-j-matrix" },
        @{ area = "replication"; feature = "xa-native-binlog-interoperability" },
        @{ area = "storage"; feature = "non-innodb-engines" }
    )
    foreach ($item in $required) {
        $matches = @($Entries | Where-Object { $_.area -eq $item.area -and $_.feature -eq $item.feature })
        if ($matches.Count -ne 1) {
            $errors.Add("required feature '$($item.area)/$($item.feature)' is unclassified")
        }
    }

    foreach ($entry in $Entries) {
        if ($entry.out_of_scope -and $entry.status -ne "out_of_scope") {
            $errors.Add("out-of-scope feature '$($entry.area)/$($entry.feature)' must have status out_of_scope")
        }
        if (-not $entry.out_of_scope -and $entry.status -eq "out_of_scope") {
            $errors.Add("in-scope feature '$($entry.area)/$($entry.feature)' cannot have status out_of_scope")
        }
    }

    return $errors
}

$entries = @(Get-ScopeMatrixEntries)
$errors = @(Test-ScopeMatrix -Entries $entries)
if ($errors.Count -gt 0) {
    $errors | ForEach-Object { Write-Error $_ }
    exit 1
}

$report = [ordered]@{
    generated_at = [DateTime]::UtcNow.ToString("o")
    diagnostic_only = [bool]$DiagnosticOnly
    entries = $entries
    summary = [ordered]@{
        total = $entries.Count
        in_scope = @($entries | Where-Object { -not $_.out_of_scope }).Count
        out_of_scope = @($entries | Where-Object { $_.out_of_scope }).Count
    }
}

$resolvedOutput = [IO.Path]::GetFullPath((Join-Path (Get-Location) $OutputPath))
$parent = Split-Path -Parent $resolvedOutput
New-Item -ItemType Directory -Force -Path $parent | Out-Null
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $resolvedOutput -Encoding UTF8
$report | ConvertTo-Json -Depth 8
