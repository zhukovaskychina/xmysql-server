package engine

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInformationSchemaMetadataContractProbe(t *testing.T) {
	missing := informationSchemaMissingExplicitMetadataContracts(informationSchemaMetadataTableNames())
	sort.Strings(missing)
	t.Logf("missing explicit INFORMATION_SCHEMA metadata contracts: %v", missing)
}

func TestInformationSchemaCriticalMetadataContractsAreExplicit(t *testing.T) {
	missing := informationSchemaMissingExplicitMetadataContracts([]string{"connection_control_failed_login_attempts", "files", "global_variables", "innodb_lock_waits", "innodb_locks", "innodb_trx", "mysql_firewall_users", "mysql_firewall_whitelist", "parameters", "partitions", "routines", "session_variables", "system_variables", "tablespaces", "tp_thread_group_state", "tp_thread_group_stats", "tp_thread_state"})
	require.Empty(t, missing)
}

func TestInformationSchemaCanonicalMetadataContracts(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	want := map[string][][]interface{}{
		"character_sets": {
			{"CHARACTER_SET_NAME", "VARCHAR", "NO", "VARCHAR(64)", nil, "utf8mb3", "utf8mb3_general_ci", "64", nil},
			{"DEFAULT_COLLATE_NAME", "VARCHAR", "NO", "VARCHAR(64)", nil, "utf8mb3", "utf8mb3_general_ci", "64", nil},
			{"DESCRIPTION", "VARCHAR", "NO", "VARCHAR(2048)", nil, "utf8mb3", "utf8mb3_general_ci", "2048", nil},
			{"MAXLEN", "INT", "NO", "INT UNSIGNED", nil, nil, nil, nil, "10"},
		},
		"applicable_roles": {
			{"USER", "VARCHAR", "YES", "VARCHAR(97)", nil, "utf8mb3", "utf8mb3_general_ci", "97", nil},
			{"HOST", "VARCHAR", "YES", "VARCHAR(256)", nil, "utf8mb3", "utf8mb3_general_ci", "256", nil},
			{"GRANTEE", "VARCHAR", "YES", "VARCHAR(97)", nil, "utf8mb4", "utf8mb4_0900_ai_ci", "97", nil},
			{"GRANTEE_HOST", "VARCHAR", "YES", "VARCHAR(256)", nil, "utf8mb4", "utf8mb4_0900_ai_ci", "256", nil},
			{"ROLE_NAME", "VARCHAR", "YES", "VARCHAR(255)", nil, "utf8mb4", "utf8mb4_0900_ai_ci", "255", nil},
			{"ROLE_HOST", "VARCHAR", "YES", "VARCHAR(256)", nil, "utf8mb4", "utf8mb4_0900_ai_ci", "256", nil},
			{"IS_GRANTABLE", "VARCHAR", "NO", "VARCHAR(3)", "", "utf8mb3", "utf8mb3_general_ci", "3", nil},
			{"IS_DEFAULT", "VARCHAR", "YES", "VARCHAR(3)", nil, "utf8mb3", "utf8mb3_general_ci", "3", nil},
			{"IS_MANDATORY", "VARCHAR", "NO", "VARCHAR(3)", "", "utf8mb3", "utf8mb3_general_ci", "3", nil},
		},
		"check_constraints": {
			{"CONSTRAINT_CATALOG", "VARCHAR", "NO", "VARCHAR(64)", nil, "utf8mb3", "utf8mb3_bin", "64", nil},
			{"CONSTRAINT_SCHEMA", "VARCHAR", "NO", "VARCHAR(64)", nil, "utf8mb3", "utf8mb3_bin", "64", nil},
			{"CONSTRAINT_NAME", "VARCHAR", "NO", "VARCHAR(64)", nil, "utf8mb3", "utf8mb3_tolower_ci", "64", nil},
			{"CHECK_CLAUSE", "LONGTEXT", "NO", "LONGTEXT", nil, "utf8mb3", "utf8mb3_bin", "4294967295", nil},
		},
		"views": {
			{"TABLE_CATALOG", "VARCHAR", "NO", "VARCHAR(64)", nil, "utf8mb3", "utf8mb3_bin", "64", nil},
			{"TABLE_SCHEMA", "VARCHAR", "NO", "VARCHAR(64)", nil, "utf8mb3", "utf8mb3_bin", "64", nil},
			{"TABLE_NAME", "VARCHAR", "NO", "VARCHAR(64)", nil, "utf8mb3", "utf8mb3_bin", "64", nil},
			{"VIEW_DEFINITION", "LONGTEXT", "YES", "LONGTEXT", nil, "utf8mb3", "utf8mb3_bin", "4294967295", nil},
			{"CHECK_OPTION", "ENUM", "YES", "ENUM('NONE','LOCAL','CASCADED')", nil, "utf8mb3", "utf8mb3_bin", "8", nil},
			{"IS_UPDATABLE", "ENUM", "YES", "ENUM('NO','YES')", nil, "utf8mb3", "utf8mb3_bin", "3", nil},
			{"DEFINER", "VARCHAR", "YES", "VARCHAR(288)", nil, "utf8mb3", "utf8mb3_bin", "288", nil},
			{"SECURITY_TYPE", "VARCHAR", "YES", "VARCHAR(7)", nil, "utf8mb3", "utf8mb3_bin", "7", nil},
			{"CHARACTER_SET_CLIENT", "VARCHAR", "NO", "VARCHAR(64)", nil, "utf8mb3", "utf8mb3_general_ci", "64", nil},
			{"COLLATION_CONNECTION", "VARCHAR", "NO", "VARCHAR(64)", nil, "utf8mb3", "utf8mb3_general_ci", "64", nil},
		},
	}
	for table, expected := range want {
		t.Run(table, func(t *testing.T) {
			result := mustSelectResultSQL(t, executor, "", fmt.Sprintf("select column_name, data_type, is_nullable, column_type, column_default, character_set_name, collation_name, character_maximum_length, numeric_precision from information_schema.columns where table_schema='information_schema' and table_name='%s' order by ordinal_position", table))
			expected = normalizeInformationSchemaMetadataTestRows(expected)
			require.Equal(t, expected, selectResultRows(result))
		})
	}
}

func TestInformationSchemaCoreObjectMetadataContracts(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	checks := map[string]map[string][]interface{}{
		"columns": {
			"COLUMN_NAME":           {"COLUMN_NAME", "VARCHAR", "YES", "VARCHAR(64)", "utf8mb3_tolower_ci", "64", ""},
			"ORDINAL_POSITION":      {"ORDINAL_POSITION", "INT", "NO", "INT UNSIGNED", "", "", "10"},
			"COLUMN_DEFAULT":        {"COLUMN_DEFAULT", "TEXT", "YES", "TEXT", "utf8mb3_bin", "65535", ""},
			"NUMERIC_PRECISION":     {"NUMERIC_PRECISION", "BIGINT", "YES", "BIGINT UNSIGNED", "", "", "20"},
			"COLUMN_TYPE":           {"COLUMN_TYPE", "MEDIUMTEXT", "NO", "MEDIUMTEXT", "utf8mb3_bin", "16777215", ""},
			"COLUMN_KEY":            {"COLUMN_KEY", "ENUM", "NO", "ENUM('','PRI','UNI','MUL')", "utf8mb3_bin", "3", ""},
			"GENERATION_EXPRESSION": {"GENERATION_EXPRESSION", "LONGTEXT", "NO", "LONGTEXT", "utf8mb3_bin", "4294967295", ""},
		},
		"tables": {
			"TABLE_TYPE":    {"TABLE_TYPE", "ENUM", "NO", "ENUM('BASE TABLE','VIEW','SYSTEM VIEW')", "utf8mb3_bin", "11", ""},
			"ENGINE":        {"ENGINE", "VARCHAR", "YES", "VARCHAR(64)", "utf8mb3_general_ci", "64", ""},
			"ROW_FORMAT":    {"ROW_FORMAT", "ENUM", "YES", "ENUM('Fixed','Dynamic','Compressed','Redundant','Compact','Paged')", "utf8mb3_bin", "10", ""},
			"TABLE_ROWS":    {"TABLE_ROWS", "BIGINT", "YES", "BIGINT UNSIGNED", "", "", "20"},
			"CREATE_TIME":   {"CREATE_TIME", "TIMESTAMP", "NO", "TIMESTAMP", "", "", ""},
			"TABLE_COMMENT": {"TABLE_COMMENT", "TEXT", "YES", "TEXT", "utf8mb3_general_ci", "65535", ""},
		},
	}
	for table, expectedByColumn := range checks {
		t.Run(table, func(t *testing.T) {
			result := mustSelectResultSQL(t, executor, "", fmt.Sprintf("select column_name, data_type, is_nullable, column_type, collation_name, character_maximum_length, numeric_precision from information_schema.columns where table_schema='information_schema' and table_name='%s' order by ordinal_position", table))
			rows := make(map[string][]interface{}, len(selectResultRows(result)))
			for _, row := range selectResultRows(result) {
				rows[fmt.Sprint(row[0])] = row
			}
			for column, expected := range expectedByColumn {
				require.Equal(t, expected, rows[column], column)
			}
		})
	}
}

func normalizeInformationSchemaMetadataTestRows(rows [][]interface{}) [][]interface{} {
	for _, row := range rows {
		for index, value := range row {
			if value == nil {
				row[index] = ""
			}
		}
	}
	return rows
}

func informationSchemaMissingExplicitMetadataContracts(tables []string) []string {
	missing := make([]string, 0)
	for _, table := range tables {
		columns := informationSchemaDedicatedTableColumns[table]
		if len(columns) == 0 {
			columns = informationSchemaTableRegistry[table]
		}
		for _, column := range columns {
			if _, ok := informationSchemaVirtualColumnSpecForTable(table, column); !ok {
				missing = append(missing, table+"."+column)
			}
		}
	}
	sort.Strings(missing)
	return missing
}

func TestInformationSchemaInnoDBTrxMetadataUsesMySQL84Contracts(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='innodb_trx' order by ordinal_position")

	want := [][]interface{}{
		{"TRX_ID", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"TRX_STATE", "VARCHAR", "4", "", "VARCHAR(13)", "NO"},
		{"TRX_STARTED", "DATETIME", "", "", "DATETIME", "NO"},
		{"TRX_REQUESTED_LOCK_ID", "VARCHAR", "42", "", "VARCHAR(126)", "YES"},
		{"TRX_WAIT_STARTED", "DATETIME", "", "", "DATETIME", "YES"},
		{"TRX_WEIGHT", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"TRX_MYSQL_THREAD_ID", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"TRX_QUERY", "VARCHAR", "341", "", "VARCHAR(1024)", "YES"},
		{"TRX_OPERATION_STATE", "VARCHAR", "21", "", "VARCHAR(64)", "YES"},
		{"TRX_TABLES_IN_USE", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"TRX_TABLES_LOCKED", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"TRX_LOCK_STRUCTS", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"TRX_LOCK_MEMORY_BYTES", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"TRX_ROWS_LOCKED", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"TRX_ROWS_MODIFIED", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"TRX_CONCURRENCY_TICKETS", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"TRX_ISOLATION_LEVEL", "VARCHAR", "5", "", "VARCHAR(16)", "NO"},
		{"TRX_UNIQUE_CHECKS", "INT", "", "", "INT", "NO"},
		{"TRX_FOREIGN_KEY_CHECKS", "INT", "", "", "INT", "NO"},
		{"TRX_LAST_FOREIGN_KEY_ERROR", "VARCHAR", "85", "", "VARCHAR(256)", "YES"},
		{"TRX_ADAPTIVE_HASH_LATCHED", "INT", "", "", "INT", "NO"},
		{"TRX_ADAPTIVE_HASH_TIMEOUT", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"TRX_IS_READ_ONLY", "INT", "", "", "INT", "NO"},
		{"TRX_AUTOCOMMIT_NON_LOCKING", "INT", "", "", "INT", "NO"},
		{"TRX_SCHEDULE_WEIGHT", "BIGINT", "", "", "BIGINT UNSIGNED", "YES"},
	}
	require.Equal(t, want, selectResultRows(result), fmt.Sprintf("unexpected INNODB_TRX metadata: %#v", selectResultRows(result)))
}

func TestThreadPoolMetadataUsesMySQL84Contracts(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	want := map[string][][]interface{}{
		"tp_thread_group_state": {
			{"TP_GROUP_ID", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"CONSUMER_THREADS", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"RESERVE_THREADS", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"CONNECT_THREAD_COUNT", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"CONNECTION_COUNT", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"QUEUED_QUERIES", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"QUEUED_TRANSACTIONS", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"STALL_LIMIT", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"PRIO_KICKUP_TIMER", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"ALGORITHM", "VARCHAR", "20", "", "VARCHAR(20)", "NO"},
			{"THREAD_COUNT", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"ACTIVE_THREAD_COUNT", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"STALLED_THREAD_COUNT", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"WAITING_THREAD_NUMBER", "INT", "", "10", "INT UNSIGNED", "YES"},
			{"OLDEST_QUEUED", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
			{"MAX_THREAD_IDS_IN_GROUP", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"EFFECTIVE_MAX_TRANSACTIONS_LIMIT", "INT", "", "10", "INT UNSIGNED", "YES"},
			{"NUM_QUERY_THREADS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"TIME_OF_LAST_THREAD_CREATION", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
			{"NUM_CONNECT_HANDLER_THREAD_IN_SLEEP", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"THREADS_BOUND_TO_TRANSACTION", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"QUERY_THREADS_COUNT", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"TIME_OF_EARLIEST_CON_EXPIRE", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		},
		"tp_thread_group_stats": {
			{"TP_GROUP_ID", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"CONNECTIONS_STARTED", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"CONNECTIONS_CLOSED", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"QUERIES_EXECUTED", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"QUERIES_QUEUED", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"THREADS_STARTED", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"PRIO_KICKUPS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"STALLED_QUERIES_EXECUTED", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"BECOME_CONSUMER_THREAD", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"BECOME_RESERVE_THREAD", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"BECOME_WAITING_THREAD", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"WAKE_THREAD_STALL_CHECKER", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"SLEEP_WAITS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"DISK_IO_WAITS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"ROW_LOCK_WAITS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"GLOBAL_LOCK_WAITS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"META_DATA_LOCK_WAITS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"TABLE_LOCK_WAITS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"USER_LOCK_WAITS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"BINLOG_WAITS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"GROUP_COMMIT_WAITS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"FSYNC_WAITS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		},
		"tp_thread_state": {
			{"TP_GROUP_ID", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"TP_THREAD_NUMBER", "INT", "", "10", "INT UNSIGNED", "NO"},
			{"PROCESS_COUNT", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"WAIT_TYPE", "VARCHAR", "30", "", "VARCHAR(30)", "YES"},
			{"TP_THREAD_TYPE", "VARCHAR", "32", "", "VARCHAR(32)", "NO"},
			{"THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		},
	}

	for table, expected := range want {
		for _, schema := range []string{"information_schema", "performance_schema"} {
			result := mustSelectResultSQL(t, executor, "", fmt.Sprintf("select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='%s' and table_name='%s' order by ordinal_position", schema, table))
			require.Equal(t, expected, selectResultRows(result), "%s.%s", schema, table)
		}
	}
}
