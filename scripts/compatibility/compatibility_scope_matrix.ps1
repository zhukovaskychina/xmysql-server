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
        [pscustomobject]@{ area = "server"; feature = "mysql-startup-and-core-crud"; status = "implemented"; priority = "P0"; test_command = "go test ./server/net ./server/innodb/engine -count=1"; evidence_path = "reports/compatibility/release-candidate-current-continuation1238/release-candidate.json"; out_of_scope = $false }
        [pscustomobject]@{ area = "server"; feature = "query-dispatch-real-execution"; status = "implemented"; priority = "P0"; test_command = "go test ./server/dispatcher -count=1"; evidence_path = "server/dispatcher/select_one_response_compatibility_test.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "cluster"; feature = "replication-and-failover"; status = "implemented"; priority = "P0"; test_command = "scripts/compatibility/cluster_smoke.ps1"; evidence_path = "reports/compatibility/release-candidate-current-continuation1238/cluster-smoke/cluster-report.json;reports/compatibility/cluster-current-continuation1599/cluster-report.json"; out_of_scope = $false }
        [pscustomobject]@{ area = "clients"; feature = "connector-j-139-gate"; status = "implemented"; priority = "P0"; test_command = "mvn -f jdbc_client/pom.xml -Pjdbc-connectivity test"; evidence_path = "reports/compatibility/release-candidate-current-continuation1238/jdbc.log"; out_of_scope = $false }

        [pscustomobject]@{ area = "information_schema"; feature = "complete-table-registry"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestInformationSchema' -count=1"; evidence_path = "server/innodb/engine/information_schema_registry.go;reports/compatibility/release-candidate-current-continuation1238/p1-schema-observability.log;reports/compatibility/p1-compression-reset-current-continuation1240.txt;reports/compatibility/p1-compression-uncompress-current-continuation1241.txt;reports/compatibility/p1-compression-timing-current-continuation1242.txt;reports/compatibility/p1-compression-per-index-switch-current-continuation1243.txt;reports/compatibility/p1-innodb-metrics-compression-current-continuation1244.txt;reports/compatibility/p1-innodb-metrics-buffer-pool-current-continuation1245.txt;reports/compatibility/p1-innodb-metrics-monitor-pattern-current-continuation1246.txt;reports/compatibility/p1-innodb-metrics-lifecycle-current-continuation1247.txt;reports/compatibility/p1-innodb-metrics-transaction-lock-current-continuation1248.txt;reports/compatibility/p1-innodb-metrics-dml-current-continuation1249.txt;reports/compatibility/p1-innodb-metrics-buffer-page-current-continuation1250.txt;reports/compatibility/p1-innodb-metrics-lock-summary-current-continuation1251.txt;reports/compatibility/p1-innodb-metrics-lock-reset-current-continuation1252.txt;reports/compatibility/p1-innodb-metrics-buffer-runtime-current-continuation1253.txt;reports/compatibility/p1-innodb-buffer-pool-stats-current-continuation1254.txt;reports/compatibility/p1-innodb-buffer-pool-rate-current-continuation1255.txt;reports/compatibility/p1-official-table-registry-diff-current-continuation1417.txt"; out_of_scope = $false }
        [pscustomobject]@{ area = "information_schema"; feature = "core-metadata-shapes-and-filters"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestInformationSchema' -count=1"; evidence_path = "server/innodb/engine/executor_ddl_test.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "information_schema"; feature = "innodb-diagnostics-and-tablespaces"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestInformationSchemaInnoDB' -count=1"; evidence_path = "server/innodb/engine/innodb_tablespaces_compatibility_test.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "information_schema"; feature = "privilege-role-visibility"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestInformationSchema.*Privilege|TestInformationSchema.*Role' -count=1"; evidence_path = "server/innodb/engine/account_metadata_compatibility.go;reports/compatibility/release-candidate-current-continuation1238/p1-schema-observability.log;reports/compatibility/p1-information-schema-routines-privilege-current-continuation1471.txt;reports/compatibility/full-go-regression-current-continuation1472.txt;reports/compatibility/p1-information-schema-view-usage-privilege-current-continuation1474.txt;reports/compatibility/full-go-regression-current-continuation1475.txt"; out_of_scope = $false }
        [pscustomobject]@{ area = "information_schema"; feature = "runtime-component-and-permission-semantics"; status = "partial"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestInformationSchema|TestPerformanceSchema' -count=1"; evidence_path = "reports/compatibility/p1-official-schema-registry-audit-current-continuation1336.txt;reports/compatibility/p1-global-scope-audit-current-continuation1404.txt;reports/compatibility/p1-performance-schema-setup-actors-lifecycle-current-continuation1539.txt;reports/compatibility/p1-component-table-permissions-current-continuation1606.txt"; out_of_scope = $false }

        [pscustomobject]@{ area = "performance_schema"; feature = "complete-table-registry"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestPerformanceSchema' -count=1"; evidence_path = "server/innodb/engine/performance_schema_registry.go;reports/compatibility/release-candidate-current-continuation1238/p1-schema-observability.log;reports/compatibility/p1-performance-schema-setup-object-predicate-current-continuation1428.txt;reports/compatibility/p1-performance-schema-setup-object-lifecycle-current-continuation1429.txt"; out_of_scope = $false }
        [pscustomobject]@{ area = "performance_schema"; feature = "statements-stages-transactions"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestPerformanceSchema' -count=1"; evidence_path = "server/innodb/engine/performance_schema_compatibility_test.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "performance_schema"; feature = "waits-locks-and-data-locks"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestPerformanceSchema.*(Wait|Lock)' -count=1"; evidence_path = "server/innodb/engine/executor.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "performance_schema"; feature = "threads-sockets-files-memory"; status = "implemented"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestPerformanceSchema' -count=1"; evidence_path = "server/innodb/engine/performance_schema_compatibility_test.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "performance_schema"; feature = "runtime-component-lifecycle-and-permissions"; status = "partial"; priority = "P1-A"; test_command = "go test ./server/innodb/engine -run 'TestPerformanceSchema' -count=1"; evidence_path = "reports/compatibility/p1-official-schema-registry-audit-current-continuation1336.txt;reports/compatibility/p1-global-scope-audit-current-continuation1404.txt;reports/compatibility/p1-performance-schema-setup-actors-lifecycle-current-continuation1539.txt;reports/compatibility/p1-component-table-permissions-current-continuation1606.txt"; out_of_scope = $false }

        [pscustomobject]@{ area = "clients"; feature = "non-connector-j-matrix"; status = "implemented"; priority = "P3"; test_command = "scripts/compatibility/client_matrix.ps1 -Client all -Password <protected> -UseDockerMySqlCli"; evidence_path = "reports/compatibility/client-matrix-current-continuation1493/client-matrix-20260927-133157.json;reports/compatibility/client-cluster-endpoint-current-continuation1493/client-cluster-endpoint-20260927-133235.json;reports/compatibility/global-scope-reconciliation-current-continuation1538.txt"; out_of_scope = $false }
        [pscustomobject]@{ area = "clients"; feature = "non-connector-j-full-compatibility-matrix"; status = "partial"; priority = "P3"; test_command = "scripts/compatibility/client_matrix.ps1 plus client-specific edge/negative/protocol matrix"; evidence_path = "reports/compatibility/global-scope-reconciliation-current-continuation1538.txt;reports/compatibility/client-matrix-current-continuation1493/client-matrix-20260927-133157.json;reports/compatibility/p3-non-connector-full-matrix-boundary-current-continuation1585.txt;reports/compatibility/p3-non-connector-client-matrix-current-continuation1591.txt;reports/compatibility/p3-client-auth-plugin-matrix-current-continuation1648.txt;reports/compatibility/p3-client-environment-matrix-audit-current-continuation1781.txt;reports/compatibility/p3-client-live-matrix-go-python-node-current-continuation1783.txt"; out_of_scope = $false }
        [pscustomobject]@{ area = "clients"; feature = "cluster-endpoint-client-scenarios"; status = "implemented"; priority = "P3"; test_command = "scripts/compatibility/client_cluster_endpoint.ps1 -Client all -UseDockerMySqlCli"; evidence_path = "reports/compatibility/client-cluster-endpoint-current-continuation1449/client-cluster-endpoint-20260927-071500.json;reports/compatibility/p2-p3-client-cluster-failover-current-continuation1600.txt"; out_of_scope = $false }
        [pscustomobject]@{ area = "clients"; feature = "mysql-cli"; status = "implemented"; priority = "P3"; test_command = "scripts/compatibility/client_matrix.ps1 -Client mysql-cli -UseDockerMySqlCli"; evidence_path = "reports/compatibility/client-matrix-current-continuation1449/client-matrix-20260927-071402.json"; out_of_scope = $false }
        [pscustomobject]@{ area = "clients"; feature = "go-mysql-driver"; status = "implemented"; priority = "P3"; test_command = "scripts/compatibility/client_matrix.ps1 -Client go"; evidence_path = "reports/compatibility/client-matrix-current-continuation1449/client-matrix-20260927-071402.json"; out_of_scope = $false }
        [pscustomobject]@{ area = "clients"; feature = "pymysql"; status = "implemented"; priority = "P3"; test_command = "scripts/compatibility/client_matrix.ps1 -Client python"; evidence_path = "reports/compatibility/client-matrix-current-continuation1449/client-matrix-20260927-071402.json"; out_of_scope = $false }
        [pscustomobject]@{ area = "clients"; feature = "node-mysql2"; status = "implemented"; priority = "P3"; test_command = "scripts/compatibility/client_matrix.ps1 -Client node"; evidence_path = "reports/compatibility/client-matrix-current-continuation1449/client-matrix-20260927-071402.json"; out_of_scope = $false }

        [pscustomobject]@{ area = "replication"; feature = "xa-state-machine-and-recovery"; status = "implemented"; priority = "P2"; test_command = "go test ./server/innodb/engine -run 'TestXA' -count=1"; evidence_path = "server/innodb/engine/xa_compatibility_test.go"; out_of_scope = $false }
        [pscustomobject]@{ area = "replication"; feature = "xa-native-binlog-interoperability"; status = "partial"; priority = "P2"; test_command = "go test ./server/replication ./server/net ./server/innodb/engine -run 'XA|Binlog|Replication|Recovery' -count=1"; evidence_path = "server/replication/replication_compatibility_test.go;reports/compatibility/release-candidate-current-continuation1238/p2-xmysql-replication.log;reports/compatibility/p2-p4-official-reverse-restart-promotion-current-continuation1568.txt;reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1592.txt;reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1594.txt;reports/compatibility/p2-native-duplicate-prefix-retry-current-continuation1595.txt;reports/compatibility/p2-external-crash-recovery-current-continuation1597.txt;reports/compatibility/p2-p4-official-source-transport-reconnect-current-continuation1604.txt;reports/compatibility/p2-native-multi-transaction-source-position-current-continuation1607.txt;reports/compatibility/p2-engine-source-reconnect-current-continuation1608.txt;reports/compatibility/p2-runtime-multi-replica-failover-data-current-continuation1609.txt"; out_of_scope = $false }
        [pscustomobject]@{ area = "replication"; feature = "native-binlog-dump-and-gtid"; status = "partial"; priority = "P2"; test_command = "go test ./server/replication ./server/net -run 'Binlog|GTID' -count=1"; evidence_path = "server/net/binlog_compatibility_test.go;reports/compatibility/release-candidate-current-continuation1238/p2-xmysql-replication.log;reports/compatibility/p2-p4-official-reverse-restart-promotion-current-continuation1568.txt;reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1592.txt;reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1594.txt;reports/compatibility/p2-native-duplicate-prefix-retry-current-continuation1595.txt;reports/compatibility/p2-external-crash-recovery-current-continuation1597.txt;reports/compatibility/p2-p4-official-source-transport-reconnect-current-continuation1604.txt;reports/compatibility/p2-native-multi-transaction-source-position-current-continuation1607.txt;reports/compatibility/p2-engine-source-reconnect-current-continuation1608.txt;reports/compatibility/p2-runtime-multi-replica-failover-data-current-continuation1609.txt"; out_of_scope = $false }
        [pscustomobject]@{ area = "replication"; feature = "crash-recovery-and-promotion"; status = "partial"; priority = "P2"; test_command = "go test ./server/replication ./server/innodb/engine -run 'Recovery|Promotion|Replica' -count=1"; evidence_path = "server/replication/replication_compatibility_test.go;reports/compatibility/release-candidate-current-continuation1238/crash-recovery.log;reports/compatibility/p2-external-process-crash-recovery-current-continuation1567.txt;reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1592.txt;reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1594.txt;reports/compatibility/p2-native-duplicate-prefix-retry-current-continuation1595.txt;reports/compatibility/p2-external-crash-recovery-current-continuation1597.txt;reports/compatibility/p2-local-topology-failover-current-continuation1602.txt;reports/compatibility/p2-p4-official-source-transport-reconnect-current-continuation1604.txt;reports/compatibility/p2-native-multi-transaction-source-position-current-continuation1607.txt;reports/compatibility/p2-engine-source-reconnect-current-continuation1608.txt;reports/compatibility/p2-runtime-multi-replica-failover-data-current-continuation1609.txt"; out_of_scope = $false }
        [pscustomobject]@{ area = "replication"; feature = "official-mysql-xa-binlog-interoperability"; status = "partial"; priority = "P4"; test_command = "official MySQL fixture plus reverse/bidirectional crash and promotion matrix"; evidence_path = "reports/compatibility/p2-p4-xmysql-to-official-reverse-restart-current-continuation1444.txt;reports/compatibility/p2-p4-official-reverse-restart-promotion-current-continuation1568.txt;reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1592.txt;reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1594.txt;reports/compatibility/p2-native-duplicate-prefix-retry-current-continuation1595.txt;reports/compatibility/p2-external-crash-recovery-current-continuation1597.txt;reports/compatibility/p2-local-topology-failover-current-continuation1602.txt;reports/compatibility/p2-p4-official-source-transport-reconnect-current-continuation1604.txt;reports/compatibility/p2-native-multi-transaction-source-position-current-continuation1607.txt;reports/compatibility/p2-engine-source-reconnect-current-continuation1608.txt;reports/compatibility/p2-runtime-multi-replica-failover-data-current-continuation1609.txt"; out_of_scope = $false }

        [pscustomobject]@{ area = "search"; feature = "fulltext"; status = "partial"; priority = "P1"; test_command = "go test ./server/innodb/engine -run 'TestFullText' -count=1"; evidence_path = "docs/planning/FULLTEXT_IMPLEMENTATION_20261004.md"; out_of_scope = $false }
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
        @{ area = "clients"; feature = "cluster-endpoint-client-scenarios" },
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
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-lock-lifecycle-current-continuation1256.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-buffer-pool-create-rate-current-continuation1257.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-buffer-pool-young-pages-current-continuation1260.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
	$_.evidence_path += ";reports/compatibility/p1-innodb-buffer-pool-pending-work-current-continuation1261.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
	$_.evidence_path += ";reports/compatibility/p1-innodb-buffer-pool-uncompress-current-continuation1262.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
	$_.evidence_path += ";reports/compatibility/p1-innodb-buffer-pool-lru-io-current-continuation1263.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
	$_.evidence_path += ";reports/compatibility/p1-innodb-buffer-pool-pending-lru-current-continuation1264.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
	$_.evidence_path += ";reports/compatibility/p1-innodb-buffer-pool-pending-decompress-current-continuation1265.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
	$_.evidence_path += ";reports/compatibility/p1-innodb-buffer-pool-hit-rate-current-continuation1266.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
	$_.evidence_path += ";reports/compatibility/p1-information-schema-column-privileges-current-continuation1267.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
	$_.evidence_path += ";reports/compatibility/p1-information-schema-column-privileges-current-continuation1267.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-client-commit-recovery-current-continuation1268.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-redo-lsn-current-continuation1269.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-dml-reads-current-continuation1270.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-buffer-data-io-current-continuation1273.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-catalog-current-continuation1274.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-open-files-current-continuation1275.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-transaction-rollback-current-continuation1276.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-redo-fsync-current-continuation1277.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-metadata-handles-current-continuation1278.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-record-lock-current-continuation1279.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-record-lock-waits-current-continuation1280.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-record-lock-attempts-current-continuation1281.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-table-lock-waits-current-continuation1282.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-lock-timeouts-current-continuation1283.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-transaction-commit-modes-current-continuation1284.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-transaction-insert-update-commits-current-continuation1285.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-savepoint-rollbacks-current-continuation1286.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-nonlocking-autocommit-readonly-current-continuation1287.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-transaction-allocations-current-continuation1288.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-redo-log-bytes-current-continuation1289.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-undo-slots-current-continuation1290.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-pending-redo-writes-current-continuation1291.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-pending-redo-fsyncs-current-continuation1292.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-innodb-metrics-rollback-active-current-continuation1293.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-state-fsync-current-continuation1294.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-binlog-retry-recovery-current-continuation1295.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-xa-terminal-retry-recovery-current-continuation1296.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-wait-events-metadata-current-continuation1297.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-stage-events-metadata-current-continuation1298.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-transaction-events-metadata-current-continuation1299.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-statement-events-metadata-current-continuation1300.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-digest-sampling-metadata-current-continuation1301.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-histogram-quantile-metadata-current-continuation1302.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-histogram-column-shape-current-continuation1303.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-histogram-column-metadata-current-continuation1304.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-histogram-truncate-current-continuation1305.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-digest-summary-metadata-current-continuation1306.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-digest-summary-time-metadata-current-continuation1307.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-summary-truncate-isolation-current-continuation1308.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-program-summary-truncate-current-continuation1309.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-transaction-summary-truncate-current-continuation1310.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-stage-summary-truncate-current-continuation1311.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-wait-summary-truncate-current-continuation1312.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-table-io-summary-truncate-current-continuation1313.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-table-lock-summary-truncate-current-continuation1314.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-memory-summary-truncate-current-continuation1315.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-file-summary-truncate-current-continuation1316.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-socket-summary-truncate-current-continuation1317.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-object-summary-current-continuation1318.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-history-truncate-current-continuation1319.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-mdl-history-truncate-current-continuation1320.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-error-summary-dimension-truncate-current-continuation1321.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-error-instrument-control-current-continuation1322.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-memory-instrument-control-current-continuation1323.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-file-socket-instrument-control-current-continuation1324.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-replica-relay-persistence-current-continuation1325.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-storage-commit-record-current-continuation1326.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-role-grant-tables-current-continuation1327.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-role-grantor-provenance-current-continuation1328.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-source-position-persistence-current-continuation1329.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-runtime-pull-source-position-current-continuation1330.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-runtime-source-file-persistence-current-continuation1331.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-purge-binary-logs-current-continuation1333.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-purge-binary-logs-before-current-continuation1334.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-commit-recovery-current-continuation1335.txt"
}
$entries | Where-Object { ($_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry") -or ($_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-official-schema-registry-audit-current-continuation1336.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-commit-statements-recovery-current-continuation1337.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-keyed-commit-event-return-current-continuation1338.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-autocommit-statement-recovery-current-continuation1339.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-promotion-relay-history-current-continuation1340.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-setup-update-privilege-current-continuation1341.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-xa-one-phase-native-binlog-current-continuation1342.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-xa-separate-phase-gtids-current-continuation1343.txt"
}
$entries | Where-Object { $_.area -eq "cluster" -and $_.feature -eq "replication-and-failover" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-cluster-current-continuation1271/cluster-report.json"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "mysql-cli") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/client-matrix-diagnostic-continuation1257/client-matrix-20260924-073703.json"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "mysql-cli") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/client-matrix-diagnostic-current-continuation1271/client-matrix-20260924-093947.json"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "mysql-cli") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/client-matrix/client-matrix-20260925-010309.json"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "cluster-endpoint-client-scenarios" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/client-cluster-endpoint-diagnostic-continuation1258/client-cluster-endpoint-20260924-074328.json"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "cluster-endpoint-client-scenarios" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/client-cluster-endpoint-diagnostic-current-continuation1271/client-cluster-endpoint-20260924-094009.json"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-xa-rollback-retry-idempotency-current-continuation1344.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-sync-instance-metadata-current-continuation1345.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-sync-instance-metadata-current-continuation1345.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-file-socket-instance-metadata-current-continuation1346.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-file-socket-instance-metadata-current-continuation1346.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-performance-timers-metadata-current-continuation1347.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-connection-attributes-metadata-current-continuation1348.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-thread-variable-metadata-current-continuation1349.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-memory-summary-metadata-current-continuation1350.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-status-dimension-metadata-current-continuation1351.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-variable-metadata-current-continuation1352.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-io-summary-metadata-current-continuation1353.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-dynamic-system-user-visibility-current-continuation1354.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-host-cache-metadata-current-continuation1355.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-table-handle-prepared-metadata-current-continuation1356.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-sql-prepared-statements-current-continuation1357.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-storage-commit-barrier-current-continuation1358.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-applied-gtid-commit-barrier-current-continuation1360.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/local-compatibility-gate-current-continuation1370.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/local-compatibility-gate-current-continuation1370.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/local-compatibility-gate-current-continuation1370.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-ndb-connection-map-registry-current-continuation1371.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-thread-pool-table-shapes-current-continuation1372.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-summary-dimension-truncate-current-continuation1373.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-stage-summary-dimension-truncate-current-continuation1374.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-transaction-summary-dimension-truncate-current-continuation1375.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-wait-summary-dimension-truncate-current-continuation1376.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-boundary-current-continuation1377.txt;reports/compatibility/client-matrix-current-continuation1377"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "cluster-endpoint-client-scenarios" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-boundary-current-continuation1377.txt;reports/compatibility/client-cluster-endpoint-current-continuation1377"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "mysql-cli" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-boundary-current-continuation1377.txt;reports/compatibility/client-matrix-current-continuation1377"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-functional-boundary-current-continuation1378.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-metadata-contracts-current-continuation1398.txt;reports/compatibility/p1-thread-pool-metadata-contracts-current-continuation1399.txt;reports/compatibility/p1-thread-pool-connections-shape-current-continuation1400.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-connection-truncate-current-continuation1401.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-connection-dependent-summary-reset-current-continuation1402.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-memory-summary-dimension-reset-current-continuation1403.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-global-scope-audit-current-continuation1404.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-official-table-registry-diff-current-continuation1417.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-field-shapes-current-continuation1421.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-partial-revoke-object-visibility-current-continuation1405.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-role-admin-grantability-current-continuation1406.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-role-admin-persisted-grant-current-continuation1407.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-nested-role-admin-boundary-current-continuation1408.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-mysql-grant-table-privilege-dispatch-current-continuation1457.txt;reports/compatibility/full-go-regression-current-continuation1458.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-global-scope-audit-current-continuation1404.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "cluster-endpoint-client-scenarios", "mysql-cli") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-environment-current-continuation1409.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "mysql-cli") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-baseline-official-vs-xmysql-current-continuation1416.txt;reports/compatibility/p4-official-mysql84-client-matrix-rerun2/client-matrix-20260925-175836.json;reports/compatibility/xmysql-client-matrix-current-continuation1416/client-matrix-20260925-175849.json"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-baseline-official-vs-xmysql-current-continuation1416.txt;reports/compatibility/p4-official-mysql84-client-matrix-rerun2/client-matrix-20260925-175836.json;reports/compatibility/xmysql-client-matrix-current-continuation1416/client-matrix-20260925-175849.json"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "go-mysql-driver", "pymysql", "node-mysql2", "mysql-cli") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-matrix-current-continuation1430.txt;reports/compatibility/p3-client-matrix-current-continuation1430"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-global-scope-audit-current-continuation1404.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-gtid-tagged-dump-set-current-continuation1410.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-previous-gtid-tagged-set-current-continuation1411.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -eq "native-binlog-dump-and-gtid" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-tagged-gtid-sql-functions-current-continuation1412.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("native-binlog-dump-and-gtid", "xa-native-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-tagged-gtid-text-parser-current-continuation1413.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("native-binlog-dump-and-gtid", "xa-native-binlog-interoperability", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-previous-gtid-binary-v2-current-continuation1414.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("native-binlog-dump-and-gtid", "xa-native-binlog-interoperability", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-gtid-text-invalid-boundary-current-continuation1415.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p4-official-mysql84-native-binlog-xa-current-continuation1418.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-runtime-source-apply-current-continuation1419.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-gtid-auto-position-current-continuation1420.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-rotation-restart-current-continuation1422.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-format-description-current-continuation1423.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-official-mysql-source-current-continuation1431.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-official-performance-schema-metadata-current-continuation1435.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-error-log-truncate-current-continuation1460.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "complete-table-registry" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-threads-update-privilege-current-continuation1462.txt;reports/compatibility/p1-performance-schema-select-privilege-current-continuation1468.txt;reports/compatibility/full-go-regression-current-continuation1466.txt;reports/compatibility/full-go-regression-current-continuation1469.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-xmysql-to-official-reverse-restart-current-continuation1444.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -eq "crash-recovery-and-promotion" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-external-crash-recovery-current-continuation1445.txt;reports/compatibility/external-crash-current-continuation1445-r3/external-crash-20260927-064228.json"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -eq "crash-recovery-and-promotion" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-external-crash-race-current-continuation1447.txt;reports/compatibility/external-crash-race-current-continuation1447-r3/external-crash-20260927-070253.json"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p4-official-mysql-reverse-crash-reconnect-current-continuation1446.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-matrix-current-continuation1437.txt;reports/compatibility/p3-client-cluster-endpoint-current-continuation1437.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-cluster-current-continuation1449.txt;reports/compatibility/client-matrix-current-continuation1449/client-matrix-20260927-071402.json;reports/compatibility/client-cluster-endpoint-current-continuation1449/client-cluster-endpoint-20260927-071500.json"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "mysql-cli") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/client-matrix-diagnostic-current-continuation1477/client-matrix-20260927-105556.json"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "cluster-endpoint-client-scenarios" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/client-cluster-endpoint-diagnostic-current-continuation1477/client-cluster-endpoint-20260927-105631.json"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-schemata-empty-database-current-continuation1478.txt;reports/compatibility/full-go-regression-current-continuation1478.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-st-geometry-visibility-current-continuation1480.txt;reports/compatibility/full-go-regression-current-continuation1480.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-column-statistics-visibility-current-continuation1482.txt;reports/compatibility/full-go-regression-current-continuation1482.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-partitions-visibility-current-continuation1484.txt;reports/compatibility/full-go-regression-current-continuation1484.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-files-process-privilege-current-continuation1486.txt"
}
$entries | Where-Object { $_.feature -in @("mysql-startup-and-core-crud", "connector-j-139-gate", "non-connector-j-matrix", "cluster-endpoint-client-scenarios", "mysql-cli") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p0-mysql-transport-compression-current-continuation1424.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("complete-table-registry", "core-metadata-shapes-and-filters", "innodb-diagnostics-and-tablespaces", "privilege-role-visibility") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-files-and-tablespace-pages-current-continuation1487.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-files-and-tablespace-pages-current-continuation1487.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "cluster-endpoint-client-scenarios") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-files-and-tablespace-pages-current-continuation1487.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("complete-table-registry", "core-metadata-shapes-and-filters", "innodb-diagnostics-and-tablespaces", "privilege-role-visibility") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/global-compatibility-status-current-continuation1490.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/global-compatibility-status-current-continuation1490.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/global-compatibility-status-current-continuation1490.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("complete-table-registry", "core-metadata-shapes-and-filters", "innodb-diagnostics-and-tablespaces", "privilege-role-visibility") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/global-compatibility-status-current-continuation1491.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/global-compatibility-status-current-continuation1491.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/global-compatibility-status-current-continuation1491.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("complete-table-registry", "core-metadata-shapes-and-filters", "innodb-diagnostics-and-tablespaces", "privilege-role-visibility") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/global-compatibility-status-current-continuation1492.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/global-compatibility-status-current-continuation1492.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/global-compatibility-status-current-continuation1492.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("complete-table-registry", "core-metadata-shapes-and-filters", "innodb-diagnostics-and-tablespaces", "privilege-role-visibility") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/global-compatibility-status-current-continuation1493.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/global-compatibility-status-current-continuation1493.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/global-compatibility-status-current-continuation1493.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-scope-boundary-current-continuation1496.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-source-regression-current-continuation1498.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -eq "crash-recovery-and-promotion" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-promotion-role-persistence-current-continuation1499.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-source-uuid-performance-schema-current-continuation1494.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-show-replica-status-source-uuid-current-continuation1495.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-source-identity-persistence-current-continuation1497.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-replica-reset-clears-source-identity-current-continuation1500.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("crash-recovery-and-promotion", "xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p0-local-gates-current-continuation1501.txt"
}
$entries | Where-Object { $_.feature -in @("mysql-startup-and-core-crud", "cluster-endpoint-client-scenarios") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p0-local-gates-current-continuation1501.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-show-replica-status-column-contract-current-continuation1502.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-native-connection-fields-current-continuation1503.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-native-source-change-sql-current-continuation1504.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-mysql-reverse-promotion-current-continuation1505.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-empty-gtid-auto-position-current-continuation1506.txt"
}
$entries | Where-Object { $_.area -in @("replication", "clients") -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "official-mysql-xa-binlog-interoperability", "non-connector-j-matrix") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p3-native-replication-tls-current-continuation1507.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema", "replication") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions", "native-binlog-dump-and-gtid", "xa-native-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-p2-native-replication-runtime-options-current-continuation1508.txt"
}
$entries | Where-Object { $_.area -in @("performance_schema", "replication") -and $_.feature -in @("runtime-component-lifecycle-and-permissions", "native-binlog-dump-and-gtid", "xa-native-binlog-interoperability", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-p2-replication-error-classification-current-continuation1514.txt"
}
$entries | Where-Object { $_.area -in @("performance_schema", "replication") -and $_.feature -in @("runtime-component-lifecycle-and-permissions", "native-binlog-dump-and-gtid", "xa-native-binlog-interoperability", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-filters-current-continuation1515.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("native-binlog-dump-and-gtid", "xa-native-binlog-interoperability", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-p2-native-replication-retry-runtime-current-continuation1509.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-replication-heartbeat-current-continuation1510.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-replication-errors-current-continuation1511.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-host-cache-lifecycle-current-continuation1512.txt"
}
$entries | Where-Object { $_.area -in @("performance_schema", "replication") -and $_.feature -in @("runtime-component-lifecycle-and-permissions", "native-binlog-dump-and-gtid", "xa-native-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-applier-configuration-current-continuation1513.txt"
}
$entries | Where-Object { $_.area -in @("replication", "performance_schema") -and $_.feature -in @("native-binlog-dump-and-gtid", "xa-native-binlog-interoperability", "official-mysql-xa-binlog-interoperability", "crash-recovery-and-promotion", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-rewrite-db-current-continuation1516.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema", "replication") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions", "native-binlog-dump-and-gtid", "xa-native-binlog-interoperability", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-show-replica-rewrite-db-current-continuation1517.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema", "replication") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions", "native-binlog-dump-and-gtid", "xa-native-binlog-interoperability", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-show-replica-status-permission-current-continuation1518.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema", "replication") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions", "native-binlog-dump-and-gtid", "xa-native-binlog-interoperability", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-p2-replication-permissions-current-continuation1519.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema", "replication") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions", "native-binlog-dump-and-gtid", "xa-native-binlog-interoperability", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-show-replica-hosts-permission-current-continuation1520.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema", "replication") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions", "native-binlog-dump-and-gtid", "xa-native-binlog-interoperability", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-show-engine-process-permission-current-continuation1521.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema", "replication") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions", "native-binlog-dump-and-gtid", "xa-native-binlog-interoperability", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-multi-table-select-current-continuation1522.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("privilege-role-visibility", "runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-multi-table-process-current-continuation1523.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-mysql-metadata-multi-table-process-current-continuation1524.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-binlog-dump-replication-privilege-current-continuation1525.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-register-replica-replication-privilege-current-continuation1526.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-status-summary-lifecycle-current-continuation1527.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-column-grant-option-scope-current-continuation1528.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-column-revoke-grant-option-current-continuation1529.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-column-show-grants-format-current-continuation1530.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-connection-memory-current-continuation1531.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("privilege-role-visibility", "runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-privilege-all-expansion-and-engine-regression-current-continuation1532.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-statement-memory-current-continuation1533.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-mysql-transport-compression-current-continuation1534.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-rewrite-qualified-statement-db-current-continuation1535.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-statement-table-filters-current-continuation1536.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-setup-threads-registry-current-continuation1540.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-reset-replica-channel-current-continuation1541.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-channel-control-current-continuation1542.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-show-replica-status-channel-current-continuation1543.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-source-filter-channel-current-continuation1544.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-statement-cpu-time-current-continuation1569.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-prepared-cpu-time-current-continuation1570.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-com-stmt-cpu-time-current-continuation1571.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-program-cpu-time-current-continuation1572.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-bidirectional-xa-current-continuation1573.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-applied-marker-same-process-current-continuation1574.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-bidirectional-xa-current-continuation1575.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-table-lock-minimum-current-continuation1576.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-wait-minimum-current-continuation1577.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-wait-dimension-minimum-current-continuation1578.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("privilege-role-visibility", "runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-privileges-union-current-continuation1579.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-one-phase-xa-native-retry-current-continuation1580.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-bidirectional-one-phase-xa-current-continuation1581.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("privilege-role-visibility", "runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-privileges-union-distinct-current-continuation1582.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("privilege-role-visibility", "runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-privileges-intersect-except-current-continuation1584.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-external-process-crash-recovery-current-continuation1583.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1586.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1592.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema", "replication", "clients") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/full-go-regression-current-continuation1648.txt"
}
$entries | Where-Object { $_.area -in @("server", "cluster", "clients", "information_schema", "performance_schema", "replication", "storage") -and -not $_.out_of_scope } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/full-go-regression-current-continuation1653.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "non-connector-j-full-compatibility-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/client-matrix-current-continuation1591/client-matrix-20260928-165020.json"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "non-connector-j-full-compatibility-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/client-matrix-current-continuation1612"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "non-connector-j-full-compatibility-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-multi-session-pool-current-continuation1613.txt;reports/compatibility/client-matrix-current-continuation1613"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "non-connector-j-full-compatibility-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-extended-types-metadata-current-continuation1615.txt;reports/compatibility/client-matrix-current-continuation1615"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "non-connector-j-full-compatibility-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-restart-reconnect-current-continuation1620.txt;reports/compatibility/client-restart-reconnect-current-continuation1620"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-promotion-auto-repoint-current-continuation1621.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "non-connector-j-full-compatibility-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-wire-values-current-continuation1624.txt;reports/compatibility/client-matrix-current-continuation1624;reports/compatibility/client-restart-reconnect-current-continuation1624"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "non-connector-j-full-compatibility-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-matrix-dev-auth-current-continuation1627.txt;reports/compatibility/client-matrix-current-continuation1627"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "non-connector-j-full-compatibility-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-cluster-endpoint-current-continuation1627.txt;reports/compatibility/client-cluster-endpoint-current-continuation1627"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-matrix", "non-connector-j-full-compatibility-matrix", "cluster-endpoint-client-scenarios", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-full-defined-matrix-current-continuation1645.txt;reports/compatibility/client-matrix-current-continuation1645/client-matrix-20260929-014311.json"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-full-compatibility-matrix") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-tls-auth-slice-current-continuation1646.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-full-compatibility-matrix") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-mysql-protocol-tls-upgrade-current-continuation1647.txt;server/net/mysql_tls_test.go"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-full-compatibility-matrix") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-auth-plugin-node-rerun-current-continuation1648.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-full-compatibility-matrix") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-node-auth-rerun-current-continuation1777.txt;reports/compatibility/p3-client-node-auth-rerun-current-continuation1777-fresh/client-matrix-20260930-230119.json"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-com-query-command-lifecycle-current-continuation1778.txt;server/net/decoupled_handler.go;server/net/decoupled_handler_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-regression-official-fixture-audit-current-continuation1779.txt;server/replication"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -in @("privilege-role-visibility", "runtime-component-and-permission-semantics") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-performance-regression-current-continuation1780.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-performance-regression-current-continuation1780.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-full-compatibility-matrix") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-restart-reconnect-current-continuation1649.txt;reports/compatibility/client-restart-reconnect-current-continuation1649"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-full-compatibility-matrix", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-tls-e2e-current-continuation1647.txt;reports/compatibility/p3-tls-e2e-current-continuation1647/run1"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -in @("non-connector-j-full-compatibility-matrix", "mysql-cli", "go-mysql-driver", "pymysql", "node-mysql2") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-tls-protected-e2e-current-continuation1647.txt;reports/compatibility/p3-tls-e2e-current-continuation1647/run2"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-session-source-refresh-current-continuation1625.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1626.txt;reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1626.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-engine-replication-config-autofailover-current-continuation1628.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-endpoint-advertisement-current-continuation1629.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-engine-replication-config-quorum-fencing-current-continuation1630.txt"
}
$entries | Where-Object { $_.feature -in @("privilege-role-visibility", "runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-privilege-role-component-regression-current-continuation1626.txt"
}
$entries | Where-Object { $_.feature -in @("privilege-role-visibility", "runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions", "xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p1-config-cluster-regression-current-continuation1630.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1631.txt;reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1631.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-self-peer-quorum-validation-current-continuation1632.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-fencing-epoch-race-guard-current-continuation1633.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-fencing-persistence-order-current-continuation1634.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-fencing-persistence-rollback-current-continuation1635.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-source-config-persistence-rollback-current-continuation1636.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-members-persistence-rollback-current-continuation1637.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-named-replication-channels-current-continuation1638.txt"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-p2-performance-schema-named-channel-views-current-continuation1639.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-p2-performance-schema-named-channel-views-current-continuation1639.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-named-replication-channels-current-continuation1640.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-source-repoint-persistence-rollback-current-continuation1641.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-source-idempotent-retry-state-repair-current-continuation1642.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-precommit-journal-sync-current-continuation1643.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-external-crash-official-current-continuation1644.txt;reports/compatibility/external-crash-current-continuation1644/external-crash-20260928-203946.json"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1650.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1651.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-directory-fsync-current-continuation1652.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-partial-transport-retry-current-continuation1654.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-full-compatibility-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-network-fault-reconnect-current-continuation1655.txt;scripts/compatibility/client_network_fault_reconnect.ps1;scripts/compatibility/tcp_fault_proxy.go"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-full-compatibility-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-mysql-cli-version-matrix-current-continuation1656.txt;reports/compatibility/client-matrix-mysql80-current-continuation1656-r2;reports/compatibility/client-matrix-mysql57-current-continuation1656"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-full-compatibility-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/client-network-fault-reconnect-current-continuation1663/client-network-fault-reconnect-20260928-233106.json;scripts/compatibility/client_network_fault_reconnect.ps1;scripts/compatibility/tcp_fault_proxy.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1657.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1658.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-reverse-promotion-mysql80-current-continuation1659.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-source-crash-reconnect-mysql80-current-continuation1660.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-reverse-promotion-script-regression-current-continuation1661.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-source-network-partition-current-continuation1662.txt;scripts/compatibility/official_mysql_source_network_partition.ps1;scripts/compatibility/tcp_fault_proxy.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-p4-official-source-network-partition-current-continuation1664.txt;scripts/compatibility/official_mysql_source_network_partition.ps1;scripts/compatibility/tcp_fault_proxy.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -in @("privilege-role-visibility", "runtime-component-and-permission-semantics") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-performance-schema-targeted-regression-current-continuation1665.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-performance-schema-targeted-regression-current-continuation1665.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-physical-commit-identity-wal-current-continuation1666.txt;server/innodb/manager/transaction_manager.go;server/innodb/engine/replication_integration.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-xa-identity-wal-current-continuation1667.txt;server/innodb/engine/replication_commit_identity_test.go;server/innodb/engine/replication_integration.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-xa-journal-recovery-identity-current-continuation1668.txt;server/innodb/engine/replication_commit_identity_test.go;server/innodb/engine/replication_integration.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-xa-precommit-journal-identity-current-continuation1669.txt;server/innodb/engine/replication_commit_identity_test.go;server/innodb/engine/transaction_journal.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-session-account-attrs-current-continuation1673.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.feature -in @("privilege-role-visibility", "runtime-component-and-permission-semantics") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-user-privileges-usage-current-continuation1674.txt;server/innodb/engine/account_metadata_compatibility.go;server/innodb/engine/information_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-full-compatibility-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-matrix-current-continuation1675.txt;reports/compatibility/p3-client-matrix-current-continuation1675/client-matrix-20260929-024805.json;scripts/compatibility/client_matrix.ps1;scripts/compatibility/client_matrix_cases.json"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-named-channel-failover-config-current-continuation1676.txt;server/replication/runtime.go;server/replication/runtime_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-marker-recovery-current-continuation1677.txt;server/innodb/engine/transaction_journal.go;server/innodb/engine/replication_engine_integration_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-promotion-relay-boundary-current-continuation1680.txt;server/replication/replica.go;server/replication/runtime.go;server/replication/runtime_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-relay-retry-rebuild-current-continuation1681.txt;server/replication/binlog_writer.go;server/replication/native_commit_recovery_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-xa-prepare-retry-rebuild-current-continuation1682.txt;server/replication/binlog_writer.go;server/replication/native_commit_recovery_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-relay-partial-batch-rebuild-current-continuation1683.txt;server/replication/binlog_writer.go;server/replication/native_commit_recovery_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-relay-gtid-integrity-rebuild-current-continuation1684.txt;server/replication/binlog_writer.go;server/replication/native_commit_recovery_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/full-go-regression-current-continuation1684.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-relay-payload-integrity-rebuild-current-continuation1685.txt;server/replication/binlog_writer.go;server/replication/native_commit_recovery_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/full-go-regression-current-continuation1685.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-relay-header-integrity-rebuild-current-continuation1686.txt;server/replication/binlog_writer.go;server/replication/native_commit_recovery_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/full-go-regression-current-continuation1686.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-relay-previous-gtid-integrity-rebuild-current-continuation1687.txt;server/replication/binlog_writer.go;server/replication/native_commit_recovery_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/full-go-regression-current-continuation1687.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-relay-rotated-header-integrity-current-continuation1688.txt;server/replication/binlog_writer.go;server/replication/native_commit_recovery_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/full-go-regression-current-continuation1688.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-relay-rotate-event-integrity-current-continuation1689.txt;server/replication/binlog_writer.go;server/replication/native_commit_recovery_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/full-go-regression-current-continuation1689.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-binlog-short-write-integrity-current-continuation1690.txt;server/replication/binlog_writer.go;server/replication/replication_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-rotate-retry-recovery-current-continuation1691.txt;server/replication/binlog_writer.go;server/replication/native_commit_recovery_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-rotate-index-retry-recovery-current-continuation1692.txt;server/replication/binlog_writer.go;server/replication/native_commit_recovery_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-gtid-index-import-retry-current-continuation1693.txt;server/replication/binlog_writer.go;server/replication/native_commit_recovery_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-role-table-grants-active-role-scope-current-continuation1678.txt;server/innodb/engine/executor.go;server/innodb/engine/account_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-role-default-enabled-scope-current-continuation1679.txt;server/innodb/engine/executor.go;server/innodb/engine/account_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-nested-role-enabled-vs-inherited-privilege-current-continuation1800.txt;server/innodb/engine/account_compatibility_test.go"
}
$entries | Where-Object { $_.area -in @("information_schema", "performance_schema") -and $_.feature -in @("runtime-component-and-permission-semantics", "runtime-component-lifecycle-and-permissions") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-summary-instance-identity-current-continuation1801.txt;server/innodb/engine/performance_schema_sync_instances_compatibility.go;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-dump-imported-gtid-executed-set-current-continuation1694.txt;server/replication/source.go;server/replication/replication_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-dump-partial-gtid-advancement-current-continuation1695.txt;server/replication/source.go;server/replication/replication_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-dump-xa-terminal-mid-resume-current-continuation1696.txt;server/replication/source.go;server/replication/replication_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-replication-committed-context-marker-retry-current-continuation1697.txt;server/innodb/engine/replication_integration.go;server/innodb/engine/replication_engine_integration_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-event-program-summary-publication-current-continuation1698.txt;server/innodb/engine/execution_context.go;server/innodb/engine/executor.go;server/innodb/engine/sql_event_runtime.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-event-program-summary-publication-current-continuation1698.txt;server/innodb/engine/execution_context.go;server/innodb/engine/executor.go;server/innodb/engine/sql_event_runtime.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-log-status-storage-engine-current-continuation1700.txt;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-log-status-replication-json-shape-current-continuation1701.txt;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-session-read-only-variable-aliases-current-continuation1702.txt;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-session-read-only-variable-aliases-current-continuation1702.txt;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-session-character-set-variable-scope-current-continuation1703.txt;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-session-character-set-variable-scope-current-continuation1703.txt;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-session-variable-manager-inventory-current-continuation1704.txt;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go;server/innodb/engine/executor.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-session-variable-manager-inventory-current-continuation1704.txt;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go;server/innodb/engine/executor.go"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-full-compatibility-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-environment-matrix-audit-current-continuation1781.txt;reports/compatibility/client-matrix-current-1781/client-matrix-20260930-194507.json"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-rwlock-instances-current-continuation1784.txt;server/innodb/engine/performance_schema_sync_instances_compatibility.go;server/innodb/engine/performance_schema_sync_instances_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-rwlock-instances-current-continuation1784.txt;server/innodb/engine/performance_schema_sync_instances_compatibility.go;server/innodb/engine/performance_schema_sync_instances_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-mutex-instances-current-continuation1789.txt;server/innodb/engine/performance_schema_sync_instances_compatibility.go;server/innodb/engine/performance_schema_sync_instances_compatibility_test.go;server/innodb/engine/performance_schema_aux_compatibility.go;server/innodb/engine/executor.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-mutex-instances-current-continuation1789.txt;server/innodb/engine/performance_schema_sync_instances_compatibility.go;server/innodb/engine/performance_schema_sync_instances_compatibility_test.go;server/innodb/engine/performance_schema_aux_compatibility.go;server/innodb/engine/executor.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-control-events-current-continuation1790.txt;server/replication/native_decoder.go;server/replication/replication_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-full-compatibility-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-session-reset-current-continuation1785.txt;reports/compatibility/client-matrix-live-1785-session-reset-final/client-matrix-20260930-211845.json;client_compatibility/python/runner.py;client_compatibility/node/runner.js;server/innodb/engine/executor.go;server/innodb/engine/executor_transaction_statement_test.go"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-full-compatibility-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-protocol-ping-current-continuation1786.txt;reports/compatibility/client-matrix-live-1786-ping-final/client-matrix-20260930-213943.json;client_compatibility/go/main.go;client_compatibility/python/runner.py;client_compatibility/node/runner.js;server/net/decoupled_handler.go;server/net/decoupled_handler_test.go"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-full-compatibility-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-protocol-init-db-current-continuation1787.txt;reports/compatibility/client-matrix-live-1786-protocol-final/client-matrix-20261001-024525.json;client_compatibility/python/runner.py;server/net/decoupled_handler.go"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-full-compatibility-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p3-client-protocol-change-user-current-continuation1788.txt;reports/compatibility/client-matrix-live-1787-protocol-final/client-matrix-20261001-025948.json;client_compatibility/node/runner.js;server/net/decoupled_handler.go;server/net/decoupled_handler_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-condition-instances-current-continuation1791.txt;server/innodb/engine/global_read_lock.go;server/innodb/engine/performance_schema_sync_instances_compatibility.go;server/innodb/engine/performance_schema_sync_instances_compatibility_test.go;server/innodb/engine/performance_schema_aux_compatibility.go;server/innodb/engine/executor.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-condition-instances-current-continuation1791.txt;server/innodb/engine/global_read_lock.go;server/innodb/engine/performance_schema_sync_instances_compatibility.go;server/innodb/engine/performance_schema_sync_instances_compatibility_test.go;server/innodb/engine/performance_schema_aux_compatibility.go;server/innodb/engine/executor.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-waits-current-continuation1792.txt;server/innodb/engine/global_read_lock.go;server/innodb/engine/admin_compatibility.go;server/innodb/engine/performance_schema_global_read_lock_wait_compatibility.go;server/innodb/engine/performance_schema_compatibility_test.go;server/innodb/engine/executor.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-waits-current-continuation1792.txt;server/innodb/engine/global_read_lock.go;server/innodb/engine/admin_compatibility.go;server/innodb/engine/performance_schema_global_read_lock_wait_compatibility.go;server/innodb/engine/performance_schema_compatibility_test.go;server/innodb/engine/executor.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-instance-summary-current-continuation1793.txt;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-instance-summary-current-continuation1793.txt;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-wait-object-contract-current-continuation1794.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-wait-object-contract-current-continuation1794.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-history-capacity-current-continuation1795.txt;server/innodb/engine/performance_schema_global_read_lock_wait_compatibility.go;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-history-capacity-current-continuation1795.txt;server/innodb/engine/performance_schema_global_read_lock_wait_compatibility.go;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-independent-history-current-continuation1796.txt;server/innodb/engine/performance_schema_global_read_lock_wait_compatibility.go;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-independent-history-current-continuation1796.txt;server/innodb/engine/performance_schema_global_read_lock_wait_compatibility.go;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-history-truncate-current-continuation1797.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-history-truncate-current-continuation1797.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-summary-instance-truncate-current-continuation1798.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_global_read_lock_wait_compatibility.go;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-global-read-lock-summary-instance-truncate-current-continuation1798.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_global_read_lock_wait_compatibility.go;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-wait-summary-instance-reset-current-continuation1799.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-wait-summary-instance-reset-current-continuation1799.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-session-connect-attrs-truncated-current-continuation1802.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-session-connect-attrs-truncated-current-continuation1802.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-current-role-account-format-current-continuation1803.txt;server/innodb/engine/session_expression_compatibility.go;server/innodb/engine/type_function_compatibility_test.go;server/innodb/manager/system_variables_manager.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-roles-graphml-role-admin-boundary-current-continuation1804.txt;server/innodb/engine/roles_graphml_compatibility.go;server/innodb/engine/executor.go;server/innodb/engine/account_compatibility_test.go;server/dispatcher/system_variable_engine.go;server/dispatcher/system_variable_engine_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-current-role-dispatcher-format-current-continuation1805.txt;server/dispatcher/system_variable_engine.go;server/dispatcher/system_variable_engine_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-role-metadata-account-filters-current-continuation1806.txt;server/innodb/engine/executor.go;server/innodb/engine/account_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-mutex-owner-current-continuation1807.txt;server/innodb/engine/performance_schema_sync_instances_compatibility.go;server/innodb/engine/performance_schema_sync_instances_compatibility_test.go;server/innodb/engine/executor.go;server/innodb/engine/account_compatibility.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-host-cache-seen-times-current-continuation1808.txt;server/innodb/engine/performance_schema_aux_compatibility.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-setup-actors-active-role-current-continuation1809.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-setup-actors-default-role-current-continuation1810.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-events-disabled-status-current-continuation1811.txt;server/innodb/engine/stored_object_compatibility.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-events-start-end-current-continuation1812.txt;server/innodb/engine/stored_object_compatibility.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-events-completion-policy-current-continuation1813.txt;server/innodb/engine/stored_object_compatibility.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-events-definer-comment-current-continuation1814.txt;server/innodb/engine/stored_object_compatibility.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-events-timestamps-current-continuation1815.txt;server/innodb/engine/stored_object_compatibility.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-events-last-executed-current-continuation1816.txt;server/innodb/engine/sql_event_runtime.go;server/innodb/engine/stored_object_compatibility.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-routines-timestamps-current-continuation1817.txt;server/innodb/engine/stored_object_compatibility.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-stored-object-session-metadata-current-continuation1818.txt;server/innodb/engine/stored_object_compatibility.go;server/innodb/engine/stored_object_compatibility_test.go;server/innodb/engine/executor.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-events-originator-current-continuation1819.txt;server/innodb/engine/stored_object_compatibility.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-routines-definition-current-continuation1820.txt;server/innodb/engine/stored_object_compatibility.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-triggers-action-order-current-continuation1821.txt;server/innodb/engine/stored_object_compatibility.go;server/innodb/engine/stored_object_compatibility_test.go;server/innodb/engine/trigger_runtime.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-views-session-metadata-current-continuation1822.txt;server/innodb/engine/executor.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-views-updatability-current-continuation1823.txt;server/innodb/engine/executor.go;server/innodb/engine/executor_ddl_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-events-definition-current-continuation1824.txt;server/innodb/engine/stored_object_compatibility.go;server/innodb/engine/stored_object_compatibility_test.go;server/innodb/engine/sql_event_runtime.go"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-replication-local-regression-current-continuation1825.txt;server/replication/replication_compatibility_test.go;server/replication/runtime_test.go;server/replication/mysql_binlog_source_integration_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-parameters-return-name-current-continuation1826.txt;server/innodb/engine/executor.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-parameters-filters-current-continuation1827.txt;server/innodb/engine/executor.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-parameters-null-predicates-current-continuation1829.txt;server/innodb/engine/executor.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-privilege-null-predicates-current-continuation1831.txt;server/innodb/engine/account_metadata_compatibility.go;server/innodb/engine/information_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-setup-column-semantics-current-continuation1828.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-xa-mutex-owner-current-continuation1830.txt;server/innodb/engine/performance_schema_sync_instances_compatibility.go;server/innodb/engine/xa_compatibility.go;server/innodb/engine/xa_persistence.go;server/innodb/engine/xa_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "waits-locks-and-data-locks" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-mutex-wait-lifecycle-current-continuation1832.txt;reports/compatibility/p1-performance-schema-mutex-summary-dimension-reset-current-continuation1833.txt;reports/compatibility/p1-performance-schema-mutex-account-summary-current-continuation1834.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_mutex_wait_compatibility.go;server/innodb/engine/performance_schema_registry.go;server/innodb/engine/performance_schema_sync_instances_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-summary-null-predicates-current-continuation1874.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-variable-filter-empty-result-current-continuation1875.txt;server/innodb/engine/executor.go;server/innodb/engine/performance_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-mysql-system-table-null-not-in-current-continuation1876.txt;server/innodb/engine/mysql_system_tables_compatibility.go;server/innodb/engine/account_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-mysql-metadata-empty-string-predicates-current-continuation1877.txt;server/innodb/engine/account_metadata_compatibility.go;server/innodb/engine/mysql_system_tables_compatibility.go;server/innodb/engine/executor.go;server/innodb/engine/account_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-parameters-empty-string-predicates-current-continuation1878.txt;server/innodb/engine/executor.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-metadata-empty-string-predicates-current-continuation1879.txt;server/innodb/engine/executor.go;server/innodb/engine/information_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-parameters-empty-string-predicates-current-continuation1880.txt;server/innodb/engine/executor.go;server/innodb/engine/stored_object_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-privilege-empty-string-predicates-current-continuation1881.txt;server/innodb/engine/account_metadata_compatibility.go;server/innodb/engine/information_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-specialized-empty-string-predicates-current-continuation1882.txt;server/innodb/engine/information_schema_registry.go;server/innodb/engine/information_schema_spatial_resource_compatibility.go;server/innodb/engine/information_schema_catalog_compatibility.go;server/innodb/engine/information_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-information-schema-srs-id-like-filter-current-continuation1883.txt;server/innodb/engine/information_schema_spatial_resource_compatibility.go;server/innodb/engine/information_schema_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "runtime-component-and-permission-semantics" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-p2-p3-fresh-gate-audit-current-continuation1884.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-p2-p3-fresh-gate-audit-current-continuation1884.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-full-compatibility-matrix" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-p2-p3-fresh-gate-audit-current-continuation1884.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-p2-p3-fresh-gate-audit-current-continuation1884.txt"
}
$entries | Where-Object { $_.area -eq "clients" -and $_.feature -eq "non-connector-j-full-compatibility-matrix" } | ForEach-Object {
    $_.status = "implemented"
    $_.test_command = "scripts/compatibility/client_matrix.ps1 plus client-specific edge/negative/protocol matrix; scripts/compatibility/client_restart_reconnect.ps1; scripts/compatibility/client_network_fault_reconnect.ps1; scripts/compatibility/client_cluster_endpoint.ps1"
    $_.evidence_path += ";reports/compatibility/p3-non-connector-client-fresh-matrix-current-continuation1888.txt;reports/compatibility/p3-client-fault-and-cluster-fresh-current-continuation1892.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.status = "implemented"
    $_.evidence_path += ";reports/compatibility/p2-p4-official-mysql-fresh-interoperability-current-continuation1886.txt"
}
$entries | Where-Object { $_.area -eq "replication" -and $_.feature -in @("xa-native-binlog-interoperability", "native-binlog-dump-and-gtid", "crash-recovery-and-promotion", "official-mysql-xa-binlog-interoperability") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p2-native-local-and-official-fixture-audit-current-continuation1835.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-local-privilege-role-fresh-audit-current-continuation1885.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -in @("privilege-role-visibility", "runtime-component-and-permission-semantics") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-final-boundary-audit-current-continuation1894.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-final-boundary-audit-current-continuation1894.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -eq "privilege-role-visibility" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/official-mysql-privilege-lifecycle-current/privilege-lifecycle-20261003-204707.json;server/innodb/engine/account_metadata_compatibility.go;server/innodb/engine/account_compatibility_test.go"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/p1-performance-schema-native-lifecycle-current-continuation1900.txt"
}
$entries | Where-Object { $_.area -eq "information_schema" -and $_.feature -in @("privilege-role-visibility", "runtime-component-and-permission-semantics") } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/full-go-regression-current-continuation1901.txt"
}
$entries | Where-Object { $_.area -eq "performance_schema" -and $_.feature -eq "runtime-component-lifecycle-and-permissions" } | ForEach-Object {
    $_.evidence_path += ";reports/compatibility/full-go-regression-current-continuation1901.txt"
}
$entries | Where-Object { $_.status -in @("partial", "deferred", "out_of_scope") } | ForEach-Object {
    $_.evidence_path += ";docs/planning/COMPATIBILITY_REMAINING_BOUNDARIES_20260928.md"
}
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

$resolvedOutput = if ([IO.Path]::IsPathRooted($OutputPath)) {
    [IO.Path]::GetFullPath($OutputPath)
} else {
    [IO.Path]::GetFullPath((Join-Path (Get-Location) $OutputPath))
}
$parent = Split-Path -Parent $resolvedOutput
New-Item -ItemType Directory -Force -Path $parent | Out-Null
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $resolvedOutput -Encoding UTF8
$report | ConvertTo-Json -Depth 8
