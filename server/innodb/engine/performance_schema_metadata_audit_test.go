package engine

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPerformanceSchemaRegistryColumnsHaveExplicitMetadataContracts(t *testing.T) {
	componentOrLegacyTables := map[string]bool{
		"firewall_group_allowlist":  true,
		"firewall_groups":           true,
		"firewall_membership":       true,
		"ndb_sync_excluded_objects": true,
		"ndb_sync_pending_objects":  true,
		"setup_timers":              true, // Removed from MySQL 8.0+; retained only as a legacy compatibility surface.
		// MySQL 8.4 documents the column names and semantics, but the exact
		// Enterprise Thread Pool INFORMATION_SCHEMA.COLUMNS metadata requires
		// an Enterprise fixture or privileged build that is not present here.
		"tp_connections": true,
	}
	missing := make([]string, 0)
	for _, table := range performanceSchemaTableNames() {
		if componentOrLegacyTables[table] {
			continue
		}
		columns := performanceSchemaTableColumns(table)
		for _, column := range columns {
			if _, ok := performanceSchemaVirtualColumnSpecForColumn(table, column); !ok {
				missing = append(missing, table+"."+column)
			}
		}
	}
	sort.Strings(missing)
	require.Empty(t, missing)
}

func TestPerformanceSchemaMySQL84CanonicalShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	cases := map[string][]string{
		"events_statements_histogram_by_digest": {"SCHEMA_NAME", "DIGEST", "BUCKET_NUMBER", "BUCKET_TIMER_LOW", "BUCKET_TIMER_HIGH", "COUNT_BUCKET", "COUNT_BUCKET_AND_LOWER", "BUCKET_QUANTILE"},
		"hosts":                                 {"HOST", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY"},
		"persisted_variables":                   {"VARIABLE_NAME", "VARIABLE_VALUE"},
		"processlist":                           {"ID", "USER", "HOST", "DB", "COMMAND", "TIME", "STATE", "INFO", "EXECUTION_ENGINE"},
		"users":                                 {"USER", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS", "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY"},
	}
	for table, expected := range cases {
		result := mustSelectResultSQL(t, executor, "", "select * from performance_schema."+table)
		require.Equal(t, expected, result.Columns, table)
	}
}
