package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// informationSchemaTableRegistry covers MySQL 8.4 INFORMATION_SCHEMA tables
// that are not backed by one of the richer dedicated compatibility handlers.
// FULLTEXT-only InnoDB tables are intentionally absent because FULLTEXT is a
// deferred feature in the global compatibility scope.
var informationSchemaTableRegistry = map[string][]string{
	"information_schema_catalog_name":          {"CATALOG_NAME"},
	"collation_character_set_applicability":    {"COLLATION_NAME", "CHARACTER_SET_NAME"},
	"columns_extensions":                       {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE"},
	"connection_control_failed_login_attempts": {"USERHOST", "FAILED_ATTEMPTS"},
	"mysql_firewall_users":                     {"USERHOST", "MODE"},
	"mysql_firewall_whitelist":                 {"USERHOST", "RULE"},
	"innodb_buffer_page":                       {"POOL_ID", "BLOCK_ID", "SPACE", "PAGE_NUMBER", "PAGE_TYPE", "FLUSH_TYPE", "FIX_COUNT", "IS_HASHED", "NEWEST_MODIFICATION", "OLDEST_MODIFICATION", "ACCESS_TIME", "TABLE_NAME", "INDEX_NAME", "NUMBER_RECORDS", "DATA_SIZE", "COMPRESSED_SIZE", "PAGE_STATE", "IO_FIX", "IS_OLD", "FREE_PAGE_CLOCK", "IS_STALE"},
	"innodb_buffer_page_lru":                   {"POOL_ID", "LRU_POSITION", "SPACE", "PAGE_NUMBER", "PAGE_TYPE", "FLUSH_TYPE", "FIX_COUNT", "IS_HASHED", "NEWEST_MODIFICATION", "OLDEST_MODIFICATION", "ACCESS_TIME", "TABLE_NAME", "INDEX_NAME", "NUMBER_RECORDS", "DATA_SIZE", "COMPRESSED_SIZE", "COMPRESSED", "IO_FIX", "IS_OLD", "FREE_PAGE_CLOCK"},
	"innodb_buffer_pool_stats":                 {"POOL_ID", "POOL_SIZE", "FREE_BUFFERS", "DATABASE_PAGES", "OLD_DATABASE_PAGES", "MODIFIED_DATABASE_PAGES", "PENDING_DECOMPRESS", "PENDING_READS", "PENDING_FLUSH_LRU", "PENDING_FLUSH_LIST", "PAGES_MADE_YOUNG", "PAGES_NOT_MADE_YOUNG", "PAGES_MADE_YOUNG_RATE", "PAGES_MADE_NOT_YOUNG_RATE", "NUMBER_PAGES_READ", "NUMBER_PAGES_CREATED", "NUMBER_PAGES_WRITTEN", "PAGES_READ_RATE", "PAGES_CREATE_RATE", "PAGES_WRITTEN_RATE", "NUMBER_PAGES_GET", "HIT_RATE", "YOUNG_MAKE_PER_THOUSAND_GETS", "NOT_YOUNG_MAKE_PER_THOUSAND_GETS", "NUMBER_PAGES_READ_AHEAD", "NUMBER_READ_AHEAD_EVICTED", "READ_AHEAD_RATE", "READ_AHEAD_EVICTED_RATE", "LRU_IO_TOTAL", "LRU_IO_CURRENT", "UNCOMPRESS_TOTAL", "UNCOMPRESS_CURRENT"},
	"innodb_cached_indexes":                    {"SPACE_ID", "INDEX_ID", "N_CACHED_PAGES"},
	"innodb_cmp":                               {"PAGE_SIZE", "COMPRESS_OPS", "COMPRESS_OPS_OK", "COMPRESS_TIME", "UNCOMPRESS_OPS", "UNCOMPRESS_TIME"},
	"innodb_cmp_per_index":                     {"DATABASE_NAME", "TABLE_NAME", "INDEX_NAME", "COMPRESS_OPS", "COMPRESS_OPS_OK", "COMPRESS_TIME", "UNCOMPRESS_OPS", "UNCOMPRESS_TIME"},
	"innodb_cmp_per_index_reset":               {"DATABASE_NAME", "TABLE_NAME", "INDEX_NAME", "COMPRESS_OPS", "COMPRESS_OPS_OK", "COMPRESS_TIME", "UNCOMPRESS_OPS", "UNCOMPRESS_TIME"},
	"innodb_cmp_reset":                         {"PAGE_SIZE", "COMPRESS_OPS", "COMPRESS_OPS_OK", "COMPRESS_TIME", "UNCOMPRESS_OPS", "UNCOMPRESS_TIME"},
	"innodb_cmpmem":                            {"PAGE_SIZE", "BUFFER_POOL_INSTANCE", "PAGES_USED", "PAGES_FREE", "RELOCATION_OPS", "RELOCATION_TIME"},
	"innodb_cmpmem_reset":                      {"PAGE_SIZE", "BUFFER_POOL_INSTANCE", "PAGES_USED", "PAGES_FREE", "RELOCATION_OPS", "RELOCATION_TIME"},
	"innodb_fields":                            {"INDEX_ID", "NAME", "POS"},
	"innodb_session_temp_tablespaces":          {"ID", "SPACE", "PATH", "SIZE", "STATE", "PURPOSE"},
	"innodb_tables":                            {"TABLE_ID", "NAME", "FLAG", "N_COLS", "SPACE", "ROW_FORMAT", "ZIP_PAGE_SIZE", "SPACE_TYPE", "INSTANT_COLS", "TOTAL_ROW_VERSIONS"},
	"innodb_tablespaces_brief":                 {"SPACE", "NAME", "PATH", "FLAG", "SPACE_TYPE"},
	"innodb_temp_table_info":                   {"TABLE_ID", "NAME", "N_COLS", "SPACE"},
	"innodb_virtual":                           {"TABLE_ID", "POS", "BASE_POS"},
	"keywords":                                 {"WORD", "RESERVED"},
	"ndb_transid_mysql_connection_map":         {"mysql_connection_id", "node_id", "ndb_transid"},
	"optimizer_trace":                          {"QUERY", "TRACE", "MISSING_BYTES_BEYOND_MAX_MEM_SIZE", "INSUFFICIENT_PRIVILEGES"},
	"profiling":                                {"QUERY_ID", "SEQ", "STATE", "DURATION", "CPU_USER", "CPU_SYSTEM", "CONTEXT_VOLUNTARY", "CONTEXT_INVOLUNTARY", "BLOCK_OPS_IN", "BLOCK_OPS_OUT", "MESSAGES_SENT", "MESSAGES_RECEIVED", "PAGE_FAULTS_MAJOR", "PAGE_FAULTS_MINOR", "SWAPS", "SOURCE_FUNCTION", "SOURCE_FILE", "SOURCE_LINE"},
	"resource_groups":                          {"RESOURCE_GROUP_NAME", "RESOURCE_GROUP_TYPE", "RESOURCE_GROUP_ENABLED", "VCPU_IDS", "THREAD_PRIORITY"},
	"schemata_extensions":                      {"CATALOG_NAME", "SCHEMA_NAME", "OPTIONS"},
	"st_geometry_columns":                      {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "SRS_NAME", "SRS_ID", "GEOMETRY_TYPE_NAME"},
	"st_spatial_reference_systems":             {"SRS_NAME", "SRS_ID", "ORGANIZATION", "ORGANIZATION_COORDSYS_ID", "DEFINITION", "DESCRIPTION"},
	"st_units_of_measure":                      {"UNIT_NAME", "UNIT_TYPE", "CONVERSION_FACTOR", "DESCRIPTION"},
	"tables_extensions":                        {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE"},
	"tablespaces_extensions":                   {"TABLESPACE_NAME", "ENGINE_ATTRIBUTE"},
	"table_constraints_extensions":             {"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_NAME", "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE"},
	"tp_thread_group_state":                    {"TP_GROUP_ID", "CONSUMER_THREADS", "RESERVE_THREADS", "CONNECT_THREAD_COUNT", "CONNECTION_COUNT", "QUEUED_QUERIES", "QUEUED_TRANSACTIONS", "STALL_LIMIT", "PRIO_KICKUP_TIMER", "ALGORITHM", "THREAD_COUNT", "ACTIVE_THREAD_COUNT", "STALLED_THREAD_COUNT", "WAITING_THREAD_NUMBER", "OLDEST_QUEUED", "MAX_THREAD_IDS_IN_GROUP", "EFFECTIVE_MAX_TRANSACTIONS_LIMIT", "NUM_QUERY_THREADS", "TIME_OF_LAST_THREAD_CREATION", "NUM_CONNECT_HANDLER_THREAD_IN_SLEEP", "THREADS_BOUND_TO_TRANSACTION", "QUERY_THREADS_COUNT", "TIME_OF_EARLIEST_CON_EXPIRE"},
	"tp_thread_group_stats":                    {"TP_GROUP_ID", "CONNECTIONS_STARTED", "CONNECTIONS_CLOSED", "QUERIES_EXECUTED", "QUERIES_QUEUED", "THREADS_STARTED", "PRIO_KICKUPS", "STALLED_QUERIES_EXECUTED", "BECOME_CONSUMER_THREAD", "BECOME_RESERVE_THREAD", "BECOME_WAITING_THREAD", "WAKE_THREAD_STALL_CHECKER", "SLEEP_WAITS", "DISK_IO_WAITS", "ROW_LOCK_WAITS", "GLOBAL_LOCK_WAITS", "META_DATA_LOCK_WAITS", "TABLE_LOCK_WAITS", "USER_LOCK_WAITS", "BINLOG_WAITS", "GROUP_COMMIT_WAITS", "FSYNC_WAITS"},
	"tp_thread_state":                          {"TP_GROUP_ID", "TP_THREAD_NUMBER", "PROCESS_COUNT", "WAIT_TYPE", "TP_THREAD_TYPE", "THREAD_ID"},
	"user_attributes":                          {"USER", "HOST", "ATTRIBUTE"},
	"view_routine_usage":                       {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME"},
	"view_table_usage":                         {"VIEW_CATALOG", "VIEW_SCHEMA", "VIEW_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"},
}

// informationSchemaDedicatedTableColumns records the ordered native shape of
// the common INFORMATION_SCHEMA views that have richer row-producing
// handlers.  Keeping the shape here is important because TABLES and COLUMNS
// are also used as the metadata catalog for these virtual views; a synthetic
// NAME column is not compatible with Connector/J DatabaseMetaData discovery.
var informationSchemaDedicatedTableColumns = map[string][]string{
	"schemata": {
		"CATALOG_NAME", "SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME", "SQL_PATH", "DEFAULT_ENCRYPTION",
	},
	"tables": {
		"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "AUTO_INCREMENT", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "TABLE_COLLATION", "CHECKSUM", "CREATE_OPTIONS", "TABLE_COMMENT",
	},
	"columns": {
		"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "COLUMN_TYPE", "COLUMN_KEY", "EXTRA", "PRIVILEGES", "COLUMN_COMMENT", "GENERATION_EXPRESSION", "SRS_ID",
	},
	"routines": {
		"SPECIFIC_NAME", "ROUTINE_CATALOG", "ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "DTD_IDENTIFIER", "ROUTINE_BODY", "ROUTINE_DEFINITION", "EXTERNAL_NAME", "EXTERNAL_LANGUAGE", "PARAMETER_STYLE", "IS_DETERMINISTIC", "SQL_DATA_ACCESS", "SQL_PATH", "SECURITY_TYPE", "CREATED", "LAST_ALTERED", "SQL_MODE", "ROUTINE_COMMENT", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION",
	},
	"triggers": {
		"TRIGGER_CATALOG", "TRIGGER_SCHEMA", "TRIGGER_NAME", "EVENT_MANIPULATION", "EVENT_OBJECT_CATALOG", "EVENT_OBJECT_SCHEMA", "EVENT_OBJECT_TABLE", "ACTION_ORDER", "ACTION_CONDITION", "ACTION_STATEMENT", "ACTION_ORIENTATION", "ACTION_TIMING", "ACTION_REFERENCE_OLD_TABLE", "ACTION_REFERENCE_NEW_TABLE", "ACTION_REFERENCE_OLD_ROW", "ACTION_REFERENCE_NEW_ROW", "CREATED", "SQL_MODE", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION",
	},
	"events": {
		"EVENT_CATALOG", "EVENT_SCHEMA", "EVENT_NAME", "DEFINER", "TIME_ZONE", "EVENT_BODY", "EVENT_DEFINITION", "EVENT_TYPE", "EXECUTE_AT", "INTERVAL_VALUE", "INTERVAL_FIELD", "SQL_MODE", "STARTS", "ENDS", "STATUS", "ON_COMPLETION", "CREATED", "LAST_ALTERED", "LAST_EXECUTED", "EVENT_COMMENT", "ORIGINATOR", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION",
	},
	"parameters": {
		"SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME", "ORDINAL_POSITION", "PARAMETER_MODE", "PARAMETER_NAME", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "DTD_IDENTIFIER", "ROUTINE_TYPE",
	},
	"statistics": {
		"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE", "INDEX_SCHEMA", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME", "COLLATION", "CARDINALITY", "SUB_PART", "PACKED", "NULLABLE", "INDEX_TYPE", "COMMENT", "INDEX_COMMENT", "IS_VISIBLE", "EXPRESSION",
	},
	"key_column_usage": {
		"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
	},
	"table_constraints": {
		"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_TYPE", "ENFORCED",
	},
	"check_constraints": {"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "CHECK_CLAUSE"},
	"referential_constraints": {
		"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "UNIQUE_CONSTRAINT_CATALOG", "UNIQUE_CONSTRAINT_SCHEMA", "UNIQUE_CONSTRAINT_NAME", "MATCH_OPTION", "UPDATE_RULE", "DELETE_RULE", "TABLE_NAME", "REFERENCED_TABLE_NAME",
	},
	"views": {
		"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "VIEW_DEFINITION", "CHECK_OPTION", "IS_UPDATABLE", "DEFINER", "SECURITY_TYPE", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION",
	},
	"partitions": {
		"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "PARTITION_ORDINAL_POSITION", "SUBPARTITION_ORDINAL_POSITION", "PARTITION_METHOD", "SUBPARTITION_METHOD", "PARTITION_EXPRESSION", "SUBPARTITION_EXPRESSION", "PARTITION_DESCRIPTION", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "CHECKSUM", "PARTITION_COMMENT", "NODEGROUP", "TABLESPACE_NAME",
	},
	"collations": {
		"COLLATION_NAME", "CHARACTER_SET_NAME", "ID", "IS_DEFAULT", "IS_COMPILED", "SORTLEN", "PAD_ATTRIBUTE",
	},
	"character_sets": {"CHARACTER_SET_NAME", "DEFAULT_COLLATE_NAME", "DESCRIPTION", "MAXLEN"},
	"engines":        {"ENGINE", "SUPPORT", "COMMENT", "TRANSACTIONS", "XA", "SAVEPOINTS"},
	"processlist":    {"ID", "USER", "HOST", "DB", "COMMAND", "TIME", "STATE", "INFO"},
	"enabled_roles":  {"ROLE_NAME", "ROLE_HOST", "IS_DEFAULT", "IS_MANDATORY"},
	"applicable_roles": {
		"USER", "HOST", "GRANTEE", "GRANTEE_HOST", "ROLE_NAME", "ROLE_HOST", "IS_GRANTABLE", "IS_DEFAULT", "IS_MANDATORY",
	},
	"administrable_role_authorizations": {
		"USER", "HOST", "GRANTEE", "GRANTEE_HOST", "ROLE_NAME", "ROLE_HOST", "IS_GRANTABLE", "IS_DEFAULT", "IS_MANDATORY",
	},
	"role_table_grants": {
		"GRANTOR", "GRANTOR_HOST", "GRANTEE", "GRANTEE_HOST", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE",
	},
	"role_column_grants": {
		"GRANTOR", "GRANTOR_HOST", "GRANTEE", "GRANTEE_HOST", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE",
	},
	"role_routine_grants": {
		"GRANTOR", "GRANTOR_HOST", "GRANTEE", "GRANTEE_HOST", "SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME", "ROUTINE_CATALOG", "ROUTINE_SCHEMA", "ROUTINE_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE",
	},
	// The following tables have dedicated row producers in executor.go and
	// the InnoDB compatibility files. Keep their catalog shapes here as well
	// so INFORMATION_SCHEMA.COLUMNS exposes the same contract instead of the
	// generic one-column NAME fallback.
	"column_privileges": {
		"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE",
	},
	"table_privileges": {
		"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE",
	},
	"schema_privileges": {"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "PRIVILEGE_TYPE", "IS_GRANTABLE"},
	"user_privileges":   {"GRANTEE", "TABLE_CATALOG", "PRIVILEGE_TYPE", "IS_GRANTABLE"},
	"plugins": {
		"PLUGIN_NAME", "PLUGIN_VERSION", "PLUGIN_STATUS", "PLUGIN_TYPE", "PLUGIN_TYPE_VERSION", "PLUGIN_LIBRARY",
		"PLUGIN_LIBRARY_VERSION", "PLUGIN_AUTHOR", "PLUGIN_DESCRIPTION", "PLUGIN_LICENSE", "LOAD_OPTION",
	},
	"column_statistics": {"SCHEMA_NAME", "TABLE_NAME", "COLUMN_NAME", "HISTOGRAM"},
	"tablespaces": {
		"TABLESPACE_NAME", "ENGINE", "TABLESPACE_TYPE", "LOGFILE_GROUP_NAME", "EXTENT_SIZE", "AUTOEXTEND_SIZE",
		"MAXIMUM_SIZE", "NODEGROUP_ID", "TABLESPACE_COMMENT", "FILE_BLOCK_SIZE", "STATUS", "ENCRYPTION",
		"ENGINE_ATTRIBUTE", "SE_PRIVATE_DATA",
	},
	"files": {
		"FILE_ID", "FILE_NAME", "FILE_TYPE", "TABLESPACE_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME",
		"LOGFILE_GROUP_NAME", "LOGFILE_GROUP_NUMBER", "ENGINE", "FULLTEXT_KEYS", "DELETED_ROWS", "UPDATE_COUNT",
		"FREE_EXTENTS", "TOTAL_EXTENTS", "EXTENT_SIZE", "INITIAL_SIZE", "MAXIMUM_SIZE", "AUTOEXTEND_SIZE",
		"CREATION_TIME", "LAST_UPDATE_TIME", "LAST_ACCESS_TIME", "RECOVER_TIME", "TRANSACTION_COUNTER", "VERSION",
		"ROW_FORMAT", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE",
		"CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "CHECKSUM", "STATUS", "EXTRA",
	},
	"innodb_tablespaces": {
		"SPACE", "NAME", "FLAG", "ROW_FORMAT", "PAGE_SIZE", "ZIP_PAGE_SIZE", "SPACE_TYPE", "FS_BLOCK_SIZE", "FILE_SIZE", "ALLOCATED_SIZE", "AUTOEXTEND_SIZE", "SERVER_VERSION",
		"SPACE_VERSION", "ENCRYPTION", "STATE",
	},
	"innodb_datafiles": {"SPACE", "PATH"},
	"innodb_tablestats": {
		"TABLE_ID", "NAME", "STATS_INITIALIZED", "NUM_ROWS", "CLUST_INDEX_SIZE", "OTHER_INDEX_SIZE", "MODIFIED_COUNTER", "AUTOINC", "REF_COUNT",
	},
	"innodb_indexes": {"INDEX_ID", "NAME", "TABLE_ID", "TYPE", "N_FIELDS", "PAGE_NO", "SPACE", "MERGE_THRESHOLD"},
	"innodb_columns": {
		"TABLE_ID", "NAME", "POS", "MTYPE", "PRTYPE", "LEN", "HAS_DEFAULT", "DEFAULT_VALUE",
	},
	"innodb_metrics": {
		"NAME", "SUBSYSTEM", "COUNT", "MAX_COUNT", "MIN_COUNT", "AVG_COUNT", "COUNT_RESET", "MAX_COUNT_RESET", "MIN_COUNT_RESET", "AVG_COUNT_RESET",
		"TIME_ENABLED", "TIME_DISABLED", "TIME_ELAPSED", "TIME_RESET", "STATUS", "TYPE", "COMMENT",
	},
	"innodb_foreign":      {"ID", "FOR_NAME", "REF_NAME", "N_COLS", "TYPE"},
	"innodb_foreign_cols": {"ID", "FOR_COL_NAME", "REF_COL_NAME", "POS"},
	"innodb_trx": {
		"TRX_ID", "TRX_STATE", "TRX_STARTED", "TRX_REQUESTED_LOCK_ID", "TRX_WAIT_STARTED", "TRX_WEIGHT", "TRX_MYSQL_THREAD_ID", "TRX_QUERY",
		"TRX_OPERATION_STATE", "TRX_TABLES_IN_USE", "TRX_TABLES_LOCKED", "TRX_LOCK_STRUCTS", "TRX_LOCK_MEMORY_BYTES", "TRX_ROWS_LOCKED", "TRX_ROWS_MODIFIED",
		"TRX_CONCURRENCY_TICKETS", "TRX_ISOLATION_LEVEL", "TRX_UNIQUE_CHECKS", "TRX_FOREIGN_KEY_CHECKS", "TRX_LAST_FOREIGN_KEY_ERROR", "TRX_ADAPTIVE_HASH_LATCHED", "TRX_ADAPTIVE_HASH_TIMEOUT", "TRX_IS_READ_ONLY", "TRX_AUTOCOMMIT_NON_LOCKING", "TRX_SCHEDULE_WEIGHT",
	},
	"innodb_lock_waits": {"REQUESTING_TRX_ID", "REQUESTED_LOCK_ID", "BLOCKING_TRX_ID", "BLOCKING_LOCK_ID"},
	"innodb_locks": {
		"LOCK_ID", "LOCK_TRX_ID", "LOCK_MODE", "LOCK_TYPE", "LOCK_TABLE", "LOCK_INDEX", "LOCK_SPACE", "LOCK_PAGE", "LOCK_REC", "LOCK_DATA",
	},
	"system_variables":  {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"global_variables":  {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"session_variables": {"VARIABLE_NAME", "VARIABLE_VALUE"},
}

func isInformationSchemaRegistryQuery(query string) bool {
	name := informationSchemaRegistryTableName(query)
	_, ok := informationSchemaTableRegistry[name]
	return ok
}

func informationSchemaRegistryTableName(query string) string {
	lower := strings.ToLower(query)
	marker := "information_schema"
	index := strings.Index(lower, marker)
	if index < 0 {
		return ""
	}
	rest := strings.TrimSpace(query[index+len(marker):])
	if strings.HasPrefix(rest, ".") {
		rest = strings.TrimSpace(rest[1:])
	}
	name := make([]rune, 0, len(rest))
	for _, ch := range rest {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
			name = append(name, ch)
			continue
		}
		break
	}
	return strings.ToLower(string(name))
}

func (e *XMySQLExecutor) executeInformationSchemaRegistrySelect(query string, session server.MySQLServerSession) *SelectResult {
	name := informationSchemaRegistryTableName(query)
	defaults := informationSchemaTableRegistry[name]
	columns := requestedInformationSchemaColumns(query, defaults)
	switch name {
	case "information_schema_catalog_name":
		row := projectInformationSchemaRow(columns, map[string]interface{}{"CATALOG_NAME": "def"})
		return newInformationSchemaSelectResult("information_schema.information_schema_catalog_name", columns, [][]interface{}{row})
	case "collation_character_set_applicability":
		return e.executeInformationSchemaCollationApplicabilitySelect(query)
	case "keywords":
		return e.executeInformationSchemaKeywordsSelect(query)
	case "profiling":
		return e.executeInformationSchemaProfilingSelect(query, session)
	case "optimizer_trace":
		return e.executeInformationSchemaOptimizerTraceSelect(query, session)
	case "connection_control_failed_login_attempts":
		return e.executeInformationSchemaConnectionControlFailedLoginAttemptsSelect(query)
	case "resource_groups":
		return e.executeInformationSchemaResourceGroupsSelect(query)
	case "st_spatial_reference_systems":
		return e.executeInformationSchemaSTSpatialReferenceSystemsSelect(query)
	case "st_units_of_measure":
		return e.executeInformationSchemaSTUnitsOfMeasureSelect(query)
	case "tablespaces_extensions":
		return e.executeInformationSchemaExtensionRows(query, session, "tablespaces_extensions", "tablespaces")
	case "columns_extensions":
		return e.executeInformationSchemaExtensionRows(query, session, "columns_extensions", "columns")
	case "tables_extensions":
		return e.executeInformationSchemaExtensionRows(query, session, "tables_extensions", "tables")
	case "schemata_extensions":
		return e.executeInformationSchemaExtensionRows(query, session, "schemata_extensions", "schemata")
	case "table_constraints_extensions":
		return e.executeInformationSchemaExtensionRows(query, session, "table_constraints_extensions", "table_constraints")
	case "view_table_usage":
		return e.executeInformationSchemaViewTableUsageSelect(query, session)
	case "view_routine_usage":
		return e.executeInformationSchemaViewRoutineUsageSelect(query, session)
	case "st_geometry_columns":
		return e.executeInformationSchemaSTGeometryColumnsSelect(query, session)
	case "user_attributes":
		return e.executeInformationSchemaUserAttributesSelect(query, session)
	case "innodb_buffer_pool_stats":
		return e.executeInformationSchemaInnoDBBufferPoolStatsSelect(query)
	case "innodb_buffer_page":
		return e.executeInformationSchemaInnoDBBufferPagesSelect(query, false)
	case "innodb_buffer_page_lru":
		return e.executeInformationSchemaInnoDBBufferPagesSelect(query, true)
	case "innodb_cached_indexes":
		return e.executeInformationSchemaInnoDBCachedIndexesSelect(query)
	case "innodb_cmp", "innodb_cmp_reset", "innodb_cmp_per_index", "innodb_cmp_per_index_reset", "innodb_cmpmem", "innodb_cmpmem_reset":
		return e.executeInformationSchemaInnoDBCompressionSelect(query, name)
	case "innodb_session_temp_tablespaces", "innodb_temp_table_info":
		return e.executeInformationSchemaTemporaryTablesSelect(query, session, name)
	case "innodb_tablespaces_brief":
		return e.executeInformationSchemaInnoDBTablespacesBriefSelect(query)
	case "innodb_tables":
		return e.executeInformationSchemaInnoDBTablesSelect(query)
	case "innodb_fields":
		return e.executeInformationSchemaInnoDBFieldsSelect(query)
	case "innodb_virtual":
		return e.executeInformationSchemaInnoDBVirtualSelect(query)
	}
	return newInformationSchemaSelectResult("information_schema."+name, columns, nil)
}

func (e *XMySQLExecutor) executeInformationSchemaKeywordsSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry["keywords"])
	infos := sqlparser.KeywordInfos()
	filters := informationSchemaKeywordFilters(query)
	// KeywordInfos is already sorted, but keep the row order explicit at this
	// boundary because INFORMATION_SCHEMA clients commonly compare snapshots.
	sort.Slice(infos, func(i, j int) bool { return infos[i].Word < infos[j].Word })
	rows := make([][]interface{}, 0, len(infos))
	for _, info := range infos {
		reserved := int64(0)
		if info.Reserved {
			reserved = 1
		}
		if !metadataPatternMatches(strings.ToUpper(info.Word), filters["WORD"]) ||
			!informationSchemaKeywordReservedMatches(query, filters["RESERVED"], reserved) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
			"WORD":     strings.ToUpper(info.Word),
			"RESERVED": reserved,
		}))
	}
	return newInformationSchemaSelectResult("information_schema.keywords", columns, rows)
}

var informationSchemaKeywordFilterPattern = regexp.MustCompile(`(?i)\b(word|reserved)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)"|([0-9]+))`)

func informationSchemaKeywordFilters(query string) map[string]string {
	filters := make(map[string]string)
	for _, match := range informationSchemaKeywordFilterPattern.FindAllStringSubmatch(query, -1) {
		value := match[2]
		if value == "" {
			value = match[3]
		}
		if value == "" {
			value = match[4]
		}
		filters[strings.ToUpper(match[1])] = value
	}
	return filters
}

// informationSchemaKeywordReservedMatches follows MySQL's KEYWORDS.RESERVED
// contract: the value is an integer 1/0 and can be used as a boolean in a
// WHERE clause.  YES/NO remain accepted as a compatibility extension for
// older xmysql clients that consumed the pre-8.4 projection.
func informationSchemaKeywordReservedMatches(query, filter string, reserved int64) bool {
	if filter != "" {
		normalized := strings.ToUpper(strings.TrimSpace(filter))
		switch normalized {
		case "1", "TRUE", "YES":
			return reserved == 1
		case "0", "FALSE", "NO":
			return reserved == 0
		default:
			return false
		}
	}
	if regexp.MustCompile(`(?i)\bnot\s+reserved\b`).MatchString(query) {
		return reserved == 0
	}
	if regexp.MustCompile(`(?i)(?:\bwhere\b|\band\b)\s+reserved\b(?:\s|$)`).MatchString(query) {
		return reserved == 1
	}
	return true
}

func (e *XMySQLExecutor) executeInformationSchemaProfilingSelect(query string, session server.MySQLServerSession) *SelectResult {
	const table = "profiling"
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry[table])
	if e == nil || e.metricsRecorder == nil {
		return newInformationSchemaSelectResult("information_schema."+table, columns, nil)
	}
	threadID, _, _ := performanceSchemaSessionIdentity(session)
	rows := make([][]interface{}, 0)
	queryID := int64(0)
	for _, event := range e.metricsRecorder.StatementHistory() {
		if threadID != 0 && event.ThreadID != 0 && event.ThreadID != threadID {
			continue
		}
		queryID++
		values := map[string]interface{}{
			"QUERY_ID":            queryID,
			"SEQ":                 int64(0),
			"STATE":               event.Status,
			"DURATION":            float64(event.Latency) / float64(time.Second),
			"CPU_USER":            nil,
			"CPU_SYSTEM":          nil,
			"CONTEXT_VOLUNTARY":   nil,
			"CONTEXT_INVOLUNTARY": nil,
			"BLOCK_OPS_IN":        nil,
			"BLOCK_OPS_OUT":       nil,
			"MESSAGES_SENT":       nil,
			"MESSAGES_RECEIVED":   nil,
			"PAGE_FAULTS_MAJOR":   nil,
			"PAGE_FAULTS_MINOR":   nil,
			"SWAPS":               nil,
			"SOURCE_FUNCTION":     nil,
			"SOURCE_FILE":         nil,
			"SOURCE_LINE":         nil,
		}
		if !performanceSchemaLockValuesMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema."+table, columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaCollationApplicabilitySelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry["collation_character_set_applicability"])
	base := executeInformationSchemaCollationsSelect(informationSchemaExtensionBaseQuery(query, "collations"))
	rows := make([][]interface{}, 0, len(base.Records))
	for _, record := range base.Records {
		values := record.GetValues()
		if len(values) < 2 {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
			"COLLATION_NAME":     values[0].Raw(),
			"CHARACTER_SET_NAME": values[1].Raw(),
		}))
	}
	return newInformationSchemaSelectResult("information_schema.collation_character_set_applicability", columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaViewTableUsageSelect(query string, session server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry["view_table_usage"])
	filters := informationSchemaViewUsageFilters(query)
	rows := make([][]interface{}, 0)
	for _, view := range e.informationSchemaVisibleViewDefinitions(session) {
		viewSchema := view.Schema
		viewName := view.Name
		definition := view.Definition
		if viewSchema == "" || viewName == "" || definition == "" {
			continue
		}
		if !informationSchemaViewUsageFilterMatches(filters, "VIEW_CATALOG", "def") ||
			!informationSchemaViewUsageFilterMatches(filters, "VIEW_SCHEMA", viewSchema) ||
			!informationSchemaViewUsageFilterMatches(filters, "VIEW_NAME", viewName) {
			continue
		}
		sources, err := e.collectViewSourceTables(viewSchema, viewName, definition)
		if err != nil {
			continue
		}
		for _, source := range sources {
			parts := strings.SplitN(source, ".", 2)
			if len(parts) != 2 {
				continue
			}
			if !informationSchemaViewUsageFilterMatches(filters, "TABLE_CATALOG", "def") ||
				!informationSchemaViewUsageFilterMatches(filters, "TABLE_SCHEMA", parts[0]) ||
				!informationSchemaViewUsageFilterMatches(filters, "TABLE_NAME", parts[1]) {
				continue
			}
			isView := false
			if _, err := os.Stat(filepath.Join(e.getDataDir(), parts[0], parts[1]+".view.json")); err == nil {
				isView = true
			}
			if !e.informationSchemaTableVisible(session, parts[0], parts[1], isView) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
				"VIEW_CATALOG": "def", "VIEW_SCHEMA": viewSchema, "VIEW_NAME": viewName,
				"TABLE_CATALOG": "def", "TABLE_SCHEMA": parts[0], "TABLE_NAME": parts[1],
			}))
		}
	}
	return newInformationSchemaSelectResult("information_schema.view_table_usage", columns, rows)
}

func informationSchemaRawString(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(value)
	}
}

var informationSchemaViewUsageFilterPattern = regexp.MustCompile(`(?i)\b(view_catalog|view_schema|view_name|table_catalog|table_schema|table_name|specific_catalog|specific_schema|specific_name)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)

func informationSchemaViewUsageFilters(query string) map[string]string {
	filters := make(map[string]string)
	for _, match := range informationSchemaViewUsageFilterPattern.FindAllStringSubmatch(query, -1) {
		value := match[2]
		if value == "" {
			value = match[3]
		}
		filters[strings.ToUpper(match[1])] = value
	}
	return filters
}

func informationSchemaViewUsageFilterMatches(filters map[string]string, column, value string) bool {
	return metadataPatternMatches(value, filters[column])
}

// executeInformationSchemaExtensionRows reuses the authoritative native
// metadata handlers and adds the MySQL extension columns. This keeps filters,
// visibility and durable metadata behavior identical to the base views.
func (e *XMySQLExecutor) executeInformationSchemaExtensionRows(query string, session server.MySQLServerSession, extension, base string) *SelectResult {
	baseQuery := informationSchemaExtensionBaseQuery(query, base)
	var baseResult *SelectResult
	switch base {
	case "columns":
		baseResult = e.executeInformationSchemaColumnsSelect(baseQuery, session)
	case "tables":
		baseResult = e.executeInformationSchemaTablesSelect(baseQuery, session)
	case "schemata":
		baseResult = e.executeInformationSchemaSchemataSelect(baseQuery, session)
	case "table_constraints":
		baseResult = e.executeInformationSchemaTableConstraintsSelect(baseQuery, session)
	case "tablespaces":
		baseResult = e.executeInformationSchemaTablespacesSelect(baseQuery)
	}
	columns := requestedInformationSchemaColumns(query, informationSchemaTableRegistry[extension])
	if baseResult == nil {
		return newInformationSchemaSelectResult("information_schema."+extension, columns, nil)
	}
	rows := make([][]interface{}, 0, len(baseResult.Records))
	for _, record := range baseResult.Records {
		values := make(map[string]interface{}, len(baseResult.Columns)+4)
		for index, column := range baseResult.Columns {
			if index < len(record.GetValues()) {
				values[strings.ToUpper(column)] = record.GetValues()[index].Raw()
			}
		}
		// MySQL exposes these extension attributes as nullable JSON/text
		// values when the storage engine does not provide them.
		values["ENGINE_ATTRIBUTE"] = nil
		values["SECONDARY_ENGINE_ATTRIBUTE"] = nil
		values["OPTIONS"] = nil
		if !informationSchemaExtensionNullFiltersMatch(query, values) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("information_schema."+extension, columns, rows)
}

var informationSchemaExtensionNullFilterPattern = regexp.MustCompile(`(?is)\b(engine_attribute|secondary_engine_attribute|options|se_private_data)\b\s+is\s+(not\s+)?null`)

func informationSchemaExtensionNullFiltersMatch(query string, values map[string]interface{}) bool {
	for _, match := range informationSchemaExtensionNullFilterPattern.FindAllStringSubmatch(query, -1) {
		column := strings.ToUpper(match[1])
		value, exists := values[column]
		if !exists {
			continue
		}
		wantNotNull := strings.TrimSpace(match[2]) != ""
		isNull := value == nil
		if wantNotNull == isNull {
			return false
		}
	}
	return true
}

func informationSchemaExtensionBaseQuery(query, base string) string {
	lower := strings.ToLower(query)
	where := ""
	if whereIndex := strings.Index(lower, " where "); whereIndex >= 0 {
		where = query[whereIndex:]
	}
	return "select * from information_schema." + base + where
}

func informationSchemaVirtualTableDefinitions() map[string][]string {
	definitions := make(map[string][]string)
	for _, name := range informationSchemaMetadataTableNames() {
		if columns, ok := informationSchemaDedicatedTableColumns[name]; ok {
			definitions["information_schema."+name] = columns
		} else if columns, ok := informationSchemaTableRegistry[name]; ok {
			definitions["information_schema."+name] = columns
		} else {
			// Dedicated handlers remain authoritative for row values.  This
			// fallback keeps table discovery working for their metadata entry;
			// core shapes are already defined by their dedicated handlers.
			definitions["information_schema."+name] = []string{"NAME"}
		}
	}
	for _, name := range performanceSchemaTableNames() {
		definitions["performance_schema."+name] = performanceSchemaTableColumns(name)
	}
	return definitions
}

func informationSchemaVirtualColumnType(column string) (typeName string, length int, nullable bool) {
	name := strings.ToUpper(strings.TrimSpace(column))
	nullable = true
	if strings.Contains(name, "TEXT") || name == "SQL_TEXT" || name == "TRACE" || name == "DEFINITION" {
		return "LONGTEXT", 0, nullable
	}
	if strings.Contains(name, "ID") || strings.Contains(name, "COUNT") || strings.Contains(name, "NUMBER") || strings.Contains(name, "TIMER") || strings.Contains(name, "POSITION") || strings.Contains(name, "SIZE") || strings.Contains(name, "PORT") || strings.Contains(name, "ORDINAL") || name == "FREQUENCY" || strings.HasSuffix(name, "_LSN") {
		return "BIGINT", 0, nullable
	}
	if strings.HasPrefix(name, "IS_") || name == "ENABLED" || name == "TIMED" || name == "HISTORY" || name == "INSTRUMENTED" || name == "AUTOCOMMIT" {
		return "VARCHAR", 3, nullable
	}
	return "VARCHAR", 255, nullable
}

// informationSchemaVirtualColumnMetadata supplies the small set of
// Performance Schema column contracts whose type and length are stable across
// the event/summary tables.  The registry still intentionally falls back to
// informationSchemaVirtualColumnType for columns for which xmysql does not yet
// have an authoritative MySQL-compatible definition; a discovered column must
// not be presented as more precise than the implementation can support.
func informationSchemaVirtualColumnMetadata(schemaName, tableName, columnName string) frmMetadataColumn {
	typeName, length, nullable := informationSchemaVirtualColumnType(columnName)
	column := frmMetadataColumn{
		name:     columnName,
		typeName: typeName,
		length:   length,
		nullable: nullable,
	}
	if !strings.EqualFold(strings.TrimSpace(schemaName), "performance_schema") {
		if strings.EqualFold(strings.TrimSpace(schemaName), "information_schema") {
			exactSpec, exact := informationSchemaCanonicalVirtualColumnSpec(strings.ToLower(strings.TrimSpace(tableName)), strings.ToUpper(strings.TrimSpace(columnName)))
			spec, ok := exactSpec, exact
			if !ok {
				spec, ok = informationSchemaVirtualColumnSpecForTable(tableName, columnName)
			}
			if ok {
				if !exact && spec.charset != "" {
					spec.charset = "utf8mb3"
				}
				column.typeName = spec.typeName
				column.length = spec.length
				column.scale = spec.scale
				column.unsigned = spec.unsigned
				column.nullable = spec.nullable
				column.charset = spec.charset
				column.collation = spec.collation
				if spec.characterMaximumLengthSet {
					column.characterMaximumLength = spec.characterMaximumLength
					column.characterMaximumLengthSet = true
				}
				if spec.characterOctetLengthSet {
					column.characterOctetLength = spec.characterOctetLength
					column.characterOctetLengthSet = true
				}
				if spec.numericPrecisionSet {
					column.numericPrecision = spec.numericPrecision
					column.numericPrecisionSet = true
				}
				if spec.numericScaleSet {
					column.numericScale = spec.numericScale
					column.numericScaleSet = true
				}
				if spec.datetimePrecisionSet {
					column.datetimePrecision = spec.datetimePrecision
					column.datetimePrecisionSet = true
				}
				column.defaultValue = spec.defaultValue
			}
		}
		return column
	}

	if spec, ok := performanceSchemaVirtualColumnSpecForColumn(tableName, columnName); ok {
		spec = normalizePerformanceSchemaVirtualColumnMetadataSpec(spec)
		if spec.collation == "" {
			switch spec.charset {
			case "utf8mb4":
				spec.collation = "utf8mb4_0900_ai_ci"
			case "ascii":
				spec.collation = "ascii_general_ci"
			}
		}
		column.typeName = spec.typeName
		column.length = spec.length
		column.scale = spec.scale
		column.unsigned = spec.unsigned
		column.nullable = spec.nullable
		column.charset = spec.charset
		column.collation = spec.collation
		column.columnKey = performanceSchemaVirtualColumnKey(tableName, columnName)
		column.defaultValue = performanceSchemaVirtualColumnDefault(tableName, columnName)
	}
	return column
}

// performanceSchemaVirtualColumnKey is the native MySQL 8.4 index contract
// exposed through INFORMATION_SCHEMA.COLUMNS.COLUMN_KEY. The virtual tables
// are not backed by local indexes, but clients still use these values for
// schema discovery and JDBC metadata decisions.
var performanceSchemaVirtualColumnKeys = map[string]map[string]string{
	"accounts":        {"USER": "MUL"},
	"cond_instances":  {"NAME": "MUL", "OBJECT_INSTANCE_BEGIN": "PRI"},
	"data_lock_waits": {"ENGINE": "PRI", "REQUESTING_ENGINE_LOCK_ID": "PRI", "REQUESTING_ENGINE_TRANSACTION_ID": "MUL", "REQUESTING_THREAD_ID": "MUL", "BLOCKING_ENGINE_LOCK_ID": "PRI", "BLOCKING_ENGINE_TRANSACTION_ID": "MUL", "BLOCKING_THREAD_ID": "MUL"},
	"data_locks":      {"ENGINE": "PRI", "ENGINE_LOCK_ID": "PRI", "ENGINE_TRANSACTION_ID": "MUL", "THREAD_ID": "MUL", "OBJECT_SCHEMA": "MUL"},
	"error_log":       {"LOGGED": "PRI", "THREAD_ID": "MUL", "PRIO": "MUL", "ERROR_CODE": "MUL", "SUBSYSTEM": "MUL"},
	"events_errors_summary_by_account_by_error":            {"USER": "MUL"},
	"events_errors_summary_by_host_by_error":               {"HOST": "MUL"},
	"events_errors_summary_by_thread_by_error":             {"THREAD_ID": "MUL"},
	"events_errors_summary_by_user_by_error":               {"USER": "MUL"},
	"events_errors_summary_global_by_error":                {"ERROR_NUMBER": "UNI"},
	"events_stages_current":                                {"THREAD_ID": "PRI", "EVENT_ID": "PRI"},
	"events_stages_history":                                {"THREAD_ID": "PRI", "EVENT_ID": "PRI"},
	"events_stages_summary_by_account_by_event_name":       {"USER": "MUL"},
	"events_stages_summary_by_host_by_event_name":          {"HOST": "MUL"},
	"events_stages_summary_by_thread_by_event_name":        {"THREAD_ID": "PRI", "EVENT_NAME": "PRI"},
	"events_stages_summary_by_user_by_event_name":          {"USER": "MUL"},
	"events_stages_summary_global_by_event_name":           {"EVENT_NAME": "PRI"},
	"events_statements_current":                            {"THREAD_ID": "PRI", "EVENT_ID": "PRI"},
	"events_statements_histogram_by_digest":                {"SCHEMA_NAME": "MUL"},
	"events_statements_histogram_global":                   {"BUCKET_NUMBER": "PRI"},
	"events_statements_history":                            {"THREAD_ID": "PRI", "EVENT_ID": "PRI"},
	"events_statements_summary_by_account_by_event_name":   {"USER": "MUL"},
	"events_statements_summary_by_digest":                  {"SCHEMA_NAME": "MUL"},
	"events_statements_summary_by_host_by_event_name":      {"HOST": "MUL"},
	"events_statements_summary_by_program":                 {"OBJECT_TYPE": "PRI", "OBJECT_SCHEMA": "PRI", "OBJECT_NAME": "PRI"},
	"events_statements_summary_by_thread_by_event_name":    {"THREAD_ID": "PRI", "EVENT_NAME": "PRI"},
	"events_statements_summary_by_user_by_event_name":      {"USER": "MUL"},
	"events_statements_summary_global_by_event_name":       {"EVENT_NAME": "PRI"},
	"events_transactions_current":                          {"THREAD_ID": "PRI", "EVENT_ID": "PRI"},
	"events_transactions_history":                          {"THREAD_ID": "PRI", "EVENT_ID": "PRI"},
	"events_transactions_summary_by_account_by_event_name": {"USER": "MUL"},
	"events_transactions_summary_by_host_by_event_name":    {"HOST": "MUL"},
	"events_transactions_summary_by_thread_by_event_name":  {"THREAD_ID": "PRI", "EVENT_NAME": "PRI"},
	"events_transactions_summary_by_user_by_event_name":    {"USER": "MUL"},
	"events_transactions_summary_global_by_event_name":     {"EVENT_NAME": "PRI"},
	"events_waits_current":                                 {"THREAD_ID": "PRI", "EVENT_ID": "PRI"},
	"events_waits_history":                                 {"THREAD_ID": "PRI", "EVENT_ID": "PRI"},
	"events_waits_summary_by_account_by_event_name":        {"USER": "MUL"},
	"events_waits_summary_by_host_by_event_name":           {"HOST": "MUL"},
	"events_waits_summary_by_instance":                     {"EVENT_NAME": "MUL", "OBJECT_INSTANCE_BEGIN": "PRI"},
	"events_waits_summary_by_thread_by_event_name":         {"THREAD_ID": "PRI", "EVENT_NAME": "PRI"},
	"events_waits_summary_by_user_by_event_name":           {"USER": "MUL"},
	"events_waits_summary_global_by_event_name":            {"EVENT_NAME": "PRI"},
	"file_instances":                                       {"FILE_NAME": "PRI", "EVENT_NAME": "MUL"},
	"file_summary_by_event_name":                           {"EVENT_NAME": "PRI"},
	"file_summary_by_instance":                             {"FILE_NAME": "MUL", "EVENT_NAME": "MUL", "OBJECT_INSTANCE_BEGIN": "PRI"},
	"global_status":                                        {"VARIABLE_NAME": "PRI"},
	"global_variables":                                     {"VARIABLE_NAME": "PRI"},
	"host_cache":                                           {"IP": "PRI", "HOST": "MUL"},
	"hosts":                                                {"HOST": "UNI"},
	"memory_summary_by_account_by_event_name":              {"USER": "MUL"},
	"memory_summary_by_host_by_event_name":                 {"HOST": "MUL"},
	"memory_summary_by_thread_by_event_name":               {"THREAD_ID": "PRI", "EVENT_NAME": "PRI"},
	"memory_summary_by_user_by_event_name":                 {"USER": "MUL"},
	"memory_summary_global_by_event_name":                  {"EVENT_NAME": "PRI"},
	"metadata_locks":                                       {"OBJECT_TYPE": "MUL", "OBJECT_INSTANCE_BEGIN": "PRI", "OWNER_THREAD_ID": "MUL"},
	"mutex_instances":                                      {"NAME": "MUL", "OBJECT_INSTANCE_BEGIN": "PRI", "LOCKED_BY_THREAD_ID": "MUL"},
	"objects_summary_global_by_type":                       {"OBJECT_TYPE": "MUL"},
	"persisted_variables":                                  {"VARIABLE_NAME": "PRI"},
	"prepared_statements_instances":                        {"OBJECT_INSTANCE_BEGIN": "PRI", "STATEMENT_ID": "MUL", "STATEMENT_NAME": "MUL", "OWNER_THREAD_ID": "MUL", "OWNER_OBJECT_TYPE": "MUL"},
	"processlist":                                          {"ID": "PRI"},
	"replication_applier_configuration":                    {"CHANNEL_NAME": "PRI"},
	"replication_applier_status":                           {"CHANNEL_NAME": "PRI"},
	"replication_applier_status_by_coordinator":            {"CHANNEL_NAME": "PRI", "THREAD_ID": "MUL"},
	"replication_applier_status_by_worker":                 {"CHANNEL_NAME": "PRI", "WORKER_ID": "PRI", "THREAD_ID": "MUL"},
	"replication_connection_configuration":                 {"CHANNEL_NAME": "PRI"},
	"replication_connection_status":                        {"CHANNEL_NAME": "PRI", "THREAD_ID": "MUL"},
	"rwlock_instances":                                     {"NAME": "MUL", "OBJECT_INSTANCE_BEGIN": "PRI", "WRITE_LOCKED_BY_THREAD_ID": "MUL"},
	"session_account_connect_attrs":                        {"PROCESSLIST_ID": "PRI", "ATTR_NAME": "PRI"},
	"session_connect_attrs":                                {"PROCESSLIST_ID": "PRI", "ATTR_NAME": "PRI"},
	"session_status":                                       {"VARIABLE_NAME": "PRI"},
	"session_variables":                                    {"VARIABLE_NAME": "PRI"},
	"setup_actors":                                         {"HOST": "PRI", "USER": "PRI", "ROLE": "PRI"},
	"setup_consumers":                                      {"NAME": "PRI"},
	"setup_instruments":                                    {"NAME": "PRI"},
	"setup_meters":                                         {"NAME": "PRI"},
	"setup_metrics":                                        {"NAME": "PRI"},
	"setup_objects":                                        {"OBJECT_TYPE": "MUL"},
	"setup_threads":                                        {"NAME": "PRI"},
	"socket_instances":                                     {"OBJECT_INSTANCE_BEGIN": "PRI", "THREAD_ID": "MUL", "SOCKET_ID": "MUL", "IP": "MUL"},
	"socket_summary_by_event_name":                         {"EVENT_NAME": "PRI"},
	"socket_summary_by_instance":                           {"EVENT_NAME": "MUL", "OBJECT_INSTANCE_BEGIN": "PRI"},
	"status_by_account":                                    {"USER": "MUL"},
	"status_by_host":                                       {"HOST": "MUL"},
	"status_by_thread":                                     {"THREAD_ID": "PRI", "VARIABLE_NAME": "PRI"},
	"status_by_user":                                       {"USER": "MUL"},
	"table_handles":                                        {"OBJECT_TYPE": "MUL", "OBJECT_INSTANCE_BEGIN": "PRI", "OWNER_THREAD_ID": "MUL"},
	"table_io_waits_summary_by_index_usage":                {"OBJECT_TYPE": "MUL"},
	"table_io_waits_summary_by_table":                      {"OBJECT_TYPE": "MUL"},
	"table_lock_waits_summary_by_table":                    {"OBJECT_TYPE": "MUL"},
	"threads":                                              {"THREAD_ID": "PRI", "NAME": "MUL", "PROCESSLIST_ID": "MUL", "PROCESSLIST_USER": "MUL", "PROCESSLIST_HOST": "MUL", "THREAD_OS_ID": "MUL", "RESOURCE_GROUP": "MUL"},
	"user_defined_functions":                               {"UDF_NAME": "PRI"},
	"user_variables_by_thread":                             {"THREAD_ID": "PRI", "VARIABLE_NAME": "PRI"},
	"users":                                                {"USER": "UNI"},
	"variables_by_thread":                                  {"THREAD_ID": "PRI", "VARIABLE_NAME": "PRI"},
}

func performanceSchemaVirtualColumnKey(tableName, columnName string) string {
	return performanceSchemaVirtualColumnKeys[strings.ToLower(strings.TrimSpace(tableName))][strings.ToUpper(strings.TrimSpace(columnName))]
}

var performanceSchemaVirtualColumnDefaults = map[string]map[string]interface{}{
	"replication_applier_filters":                          {"COUNTER": "0"},
	"replication_asynchronous_connection_failover":         {"MANAGED_NAME": ""},
	"replication_asynchronous_connection_failover_managed": {"MANAGED_NAME": "", "MANAGED_TYPE": ""},
	"replication_connection_status":                        {"COUNT_RECEIVED_HEARTBEATS": "0"},
	"setup_actors":                                         {"HOST": "%", "USER": "%", "ROLE": "%", "ENABLED": "YES", "HISTORY": "YES"},
	"setup_objects":                                        {"OBJECT_TYPE": "TABLE", "OBJECT_SCHEMA": "%", "OBJECT_NAME": "%", "ENABLED": "YES", "TIMED": "YES"},
	"variables_info":                                       {"VARIABLE_SOURCE": "COMPILED"},
}

func performanceSchemaVirtualColumnDefault(tableName, columnName string) interface{} {
	return performanceSchemaVirtualColumnDefaults[strings.ToLower(strings.TrimSpace(tableName))][strings.ToUpper(strings.TrimSpace(columnName))]
}

func informationSchemaVirtualColumnSpecForTable(tableName, columnName string) (performanceSchemaVirtualColumnMetadataSpec, bool) {
	table := strings.ToLower(strings.TrimSpace(tableName))
	column := strings.ToUpper(strings.TrimSpace(columnName))
	if spec, ok := informationSchemaCanonicalVirtualColumnSpec(table, column); ok {
		return spec, true
	}
	if table == "ndb_transid_mysql_connection_map" {
		switch column {
		case "MYSQL_CONNECTION_ID", "NDB_TRANSID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "NODE_ID":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_fields" {
		switch column {
		case "INDEX_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "POS":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_virtual" {
		switch column {
		case "TABLE_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "POS", "BASE_POS":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_foreign" {
		switch column {
		case "ID", "FOR_NAME", "REF_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 193, false), true
		case "N_COLS", "TYPE":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_foreign_cols" {
		switch column {
		case "ID":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 193, false), true
		case "FOR_COL_NAME", "REF_COL_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "POS":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_indexes" {
		switch column {
		case "INDEX_ID", "TABLE_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 193, false), true
		case "TYPE", "N_FIELDS", "PAGE_NO", "SPACE", "MERGE_THRESHOLD":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_columns" {
		switch column {
		case "TABLE_ID", "POS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "MTYPE", "PRTYPE", "LEN", "HAS_DEFAULT":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "DEFAULT_VALUE":
			return performanceSchemaVirtualColumnSpecFor("BLOB", 0, true, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_cached_indexes" {
		switch column {
		case "SPACE_ID", "INDEX_ID", "N_CACHED_PAGES":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "INDEX_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 193, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_tables" {
		switch column {
		case "TABLE_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 655, false), true
		case "FLAG", "N_COLS", "SPACE", "INSTANT_COLS", "TOTAL_ROW_VERSIONS":
			if column == "SPACE" {
				return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, false), true
			}
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "ROW_FORMAT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 12, true), true
		case "ZIP_PAGE_SIZE":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		case "SPACE_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 10, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_tablestats" {
		switch column {
		case "TABLE_ID", "NUM_ROWS", "CLUST_INDEX_SIZE", "OTHER_INDEX_SIZE", "MODIFIED_COUNTER", "AUTOINC":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "NAME", "STATS_INITIALIZED":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "REF_COUNT":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_tablespaces" {
		switch column {
		case "SPACE", "FLAG", "PAGE_SIZE", "ZIP_PAGE_SIZE", "FS_BLOCK_SIZE", "SPACE_VERSION":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 655, false), true
		case "ROW_FORMAT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 22, true), true
		case "SPACE_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 10, true), true
		case "FILE_SIZE", "ALLOCATED_SIZE", "AUTOEXTEND_SIZE":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "SERVER_VERSION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 10, true), true
		case "ENCRYPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1, true), true
		case "STATE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 10, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_tablespaces_brief" {
		switch column {
		case "SPACE":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 655, false), true
		case "PATH":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 512, false), true
		case "FLAG":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "SPACE_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 10, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_datafiles" {
		switch column {
		case "SPACE":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "PATH":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 512, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_buffer_pool_stats" {
		switch column {
		case "POOL_ID", "POOL_SIZE", "FREE_BUFFERS", "DATABASE_PAGES", "OLD_DATABASE_PAGES", "MODIFIED_DATABASE_PAGES",
			"PENDING_DECOMPRESS", "PENDING_READS", "PENDING_FLUSH_LRU", "PENDING_FLUSH_LIST", "PAGES_MADE_YOUNG",
			"PAGES_NOT_MADE_YOUNG", "NUMBER_PAGES_READ", "NUMBER_PAGES_CREATED", "NUMBER_PAGES_WRITTEN", "NUMBER_PAGES_GET",
			"HIT_RATE", "YOUNG_MAKE_PER_THOUSAND_GETS", "NOT_YOUNG_MAKE_PER_THOUSAND_GETS", "NUMBER_PAGES_READ_AHEAD",
			"NUMBER_READ_AHEAD_EVICTED", "LRU_IO_TOTAL", "LRU_IO_CURRENT", "UNCOMPRESS_TOTAL", "UNCOMPRESS_CURRENT":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "PAGES_MADE_YOUNG_RATE", "PAGES_MADE_NOT_YOUNG_RATE", "PAGES_READ_RATE", "PAGES_CREATE_RATE",
			"PAGES_WRITTEN_RATE", "READ_AHEAD_RATE", "READ_AHEAD_EVICTED_RATE":
			return performanceSchemaVirtualColumnSpecFor("FLOAT", 0, false, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_buffer_page" || table == "innodb_buffer_page_lru" {
		switch column {
		case "POOL_ID", "BLOCK_ID", "LRU_POSITION", "SPACE", "PAGE_NUMBER", "FLUSH_TYPE", "FIX_COUNT",
			"NEWEST_MODIFICATION", "OLDEST_MODIFICATION", "ACCESS_TIME", "NUMBER_RECORDS", "DATA_SIZE", "COMPRESSED_SIZE", "FREE_PAGE_CLOCK":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "PAGE_TYPE", "PAGE_STATE", "IO_FIX":
			if table == "innodb_buffer_page_lru" && column == "PAGE_STATE" {
				return performanceSchemaVirtualColumnMetadataSpec{}, false
			}
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "IS_HASHED", "IS_OLD", "IS_STALE", "COMPRESSED":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, true), true
		case "TABLE_NAME", "INDEX_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_cmp" || table == "innodb_cmp_reset" {
		switch column {
		case "PAGE_SIZE", "COMPRESS_OPS", "COMPRESS_OPS_OK", "COMPRESS_TIME", "UNCOMPRESS_OPS", "UNCOMPRESS_TIME":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_cmp_per_index" || table == "innodb_cmp_per_index_reset" {
		switch column {
		case "DATABASE_NAME", "TABLE_NAME", "INDEX_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 192, false), true
		case "COMPRESS_OPS", "COMPRESS_OPS_OK", "COMPRESS_TIME", "UNCOMPRESS_OPS", "UNCOMPRESS_TIME":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_cmpmem" || table == "innodb_cmpmem_reset" {
		switch column {
		case "PAGE_SIZE", "BUFFER_POOL_INSTANCE", "PAGES_USED", "PAGES_FREE", "RELOCATION_TIME":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "RELOCATION_OPS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_metrics" {
		switch column {
		case "NAME", "SUBSYSTEM", "STATUS", "TYPE", "COMMENT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "COUNT", "COUNT_RESET":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, false), true
		case "MAX_COUNT", "MIN_COUNT", "TIME_ELAPSED", "MAX_COUNT_RESET", "MIN_COUNT_RESET":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, false), true
		case "AVG_COUNT", "AVG_COUNT_RESET":
			return performanceSchemaVirtualColumnSpecFor("FLOAT", 0, true, false), true
		case "TIME_ENABLED", "TIME_DISABLED", "TIME_RESET":
			return performanceSchemaVirtualColumnSpecFor("DATETIME", 0, true, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_trx" {
		switch column {
		case "TRX_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "TRX_STATE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 13, false), true
		case "TRX_STARTED":
			return performanceSchemaVirtualColumnSpecFor("DATETIME", 0, false, false), true
		case "TRX_REQUESTED_LOCK_ID":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 105, true), true
		case "TRX_WAIT_STARTED":
			return performanceSchemaVirtualColumnSpecFor("DATETIME", 0, true, false), true
		case "TRX_WEIGHT", "TRX_MYSQL_THREAD_ID", "TRX_TABLES_IN_USE", "TRX_TABLES_LOCKED", "TRX_LOCK_STRUCTS", "TRX_LOCK_MEMORY_BYTES", "TRX_ROWS_LOCKED", "TRX_ROWS_MODIFIED", "TRX_CONCURRENCY_TICKETS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "TRX_QUERY":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, true), true
		case "TRX_OPERATION_STATE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "TRX_ISOLATION_LEVEL":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 16, false), true
		case "TRX_UNIQUE_CHECKS", "TRX_FOREIGN_KEY_CHECKS":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "TRX_LAST_FOREIGN_KEY_ERROR":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 256, true), true
		case "TRX_ADAPTIVE_HASH_LATCHED":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "TRX_ADAPTIVE_HASH_TIMEOUT":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "TRX_IS_READ_ONLY", "TRX_AUTOCOMMIT_NON_LOCKING":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "TRX_SCHEDULE_WEIGHT":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_session_temp_tablespaces" {
		switch column {
		case "ID", "SPACE":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		case "PATH":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 4000, false), true
		case "SIZE":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "STATE", "PURPOSE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "innodb_temp_table_info" {
		switch column {
		case "TABLE_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "N_COLS", "SPACE":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "information_schema_catalog_name" {
		if column == "CATALOG_NAME" {
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		}
		return performanceSchemaVirtualColumnMetadataSpec{}, false
	}
	if table == "schemata" {
		switch column {
		case "CATALOG_NAME", "SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "SQL_PATH":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "DEFAULT_ENCRYPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "tables" {
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "TABLE_COLLATION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, column == "ENGINE" || column == "TABLE_COLLATION"), true
		case "ROW_FORMAT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 10, true), true
		case "CREATE_OPTIONS":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 256, true), true
		case "TABLE_COMMENT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 2048, false), true
		case "VERSION", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "AUTO_INCREMENT", "CHECKSUM":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 0, true, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "view_routine_usage" {
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "view_table_usage" {
		switch column {
		case "VIEW_CATALOG", "VIEW_SCHEMA", "VIEW_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "columns_extensions" {
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE":
			return performanceSchemaVirtualColumnSpecFor("JSON", 0, true, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "tables_extensions" {
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE":
			return performanceSchemaVirtualColumnSpecFor("JSON", 0, true, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "schemata_extensions" {
		switch column {
		case "CATALOG_NAME", "SCHEMA_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "OPTIONS":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 256, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "table_constraints_extensions" {
		switch column {
		case "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE":
			return performanceSchemaVirtualColumnSpecFor("JSON", 0, true, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}
	if table == "tablespaces_extensions" {
		switch column {
		case "TABLESPACE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "ENGINE_ATTRIBUTE":
			return performanceSchemaVirtualColumnSpecFor("JSON", 0, true, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "columns" {
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "CHARACTER_SET_NAME", "COLLATION_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, column == "CHARACTER_SET_NAME" || column == "COLLATION_NAME"), true
		case "IS_NULLABLE", "COLUMN_KEY":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, false), true
		case "COLUMN_TYPE", "GENERATION_EXPRESSION":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, false, false), true
		case "COLUMN_DEFAULT":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		case "COLUMN_COMMENT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, false), true
		case "EXTRA":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 256, false), true
		case "PRIVILEGES":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 154, false), true
		case "ORDINAL_POSITION":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "SRS_ID":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "statistics" {
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_SCHEMA", "INDEX_NAME", "COLUMN_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "NON_UNIQUE", "SEQ_IN_INDEX":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "CARDINALITY", "SUB_PART":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "COLLATION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1, true), true
		case "PACKED":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 10, true), true
		case "NULLABLE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, true), true
		case "INDEX_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 16, false), true
		case "COMMENT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 8, true), true
		case "INDEX_COMMENT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, false), true
		case "IS_VISIBLE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, false), true
		case "EXPRESSION":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "collations" || table == "character_sets" {
		switch column {
		case "COLLATION_NAME", "CHARACTER_SET_NAME", "DEFAULT_COLLATE_NAME", "DESCRIPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "IS_DEFAULT", "IS_COMPILED":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, false), true
		case "PAD_ATTRIBUTE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 12, false), true
		case "ID", "SORTLEN", "MAXLEN":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "keywords" {
		switch column {
		case "WORD":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, true), true
		case "RESERVED":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "st_units_of_measure" {
		switch column {
		case "UNIT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 255, false), true
		case "UNIT_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 7, false), true
		case "CONVERSION_FACTOR":
			return performanceSchemaVirtualColumnSpecFor("DOUBLE", 0, false, false), true
		case "DESCRIPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 255, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "st_spatial_reference_systems" {
		switch column {
		case "SRS_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 80, false), true
		case "SRS_ID", "ORGANIZATION_COORDSYS_ID":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, column == "ORGANIZATION_COORDSYS_ID", true), true
		case "ORGANIZATION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 256, true), true
		case "DEFINITION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 4096, false), true
		case "DESCRIPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 2048, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "resource_groups" {
		switch column {
		case "RESOURCE_GROUP_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "RESOURCE_GROUP_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('SYSTEM','USER')", 0, false, false), true
		case "RESOURCE_GROUP_ENABLED":
			return performanceSchemaVirtualColumnSpecFor("TINYINT(1)", 0, false, false), true
		case "VCPU_IDS":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, false), true
		case "THREAD_PRIORITY":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "column_statistics" {
		switch column {
		case "SCHEMA_NAME", "TABLE_NAME", "COLUMN_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "HISTOGRAM":
			return performanceSchemaVirtualColumnSpecFor("JSON", 0, false, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "profiling" {
		switch column {
		case "QUERY_ID", "SEQ":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "STATE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 30, false), true
		case "DURATION":
			return performanceSchemaVirtualDecimalSpec(9, 6, false), true
		case "CPU_USER", "CPU_SYSTEM":
			return performanceSchemaVirtualDecimalSpec(9, 6, true), true
		case "CONTEXT_VOLUNTARY", "CONTEXT_INVOLUNTARY", "BLOCK_OPS_IN", "BLOCK_OPS_OUT", "MESSAGES_SENT", "MESSAGES_RECEIVED", "PAGE_FAULTS_MAJOR", "PAGE_FAULTS_MINOR", "SWAPS":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, false), true
		case "SOURCE_FUNCTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 30, true), true
		case "SOURCE_FILE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 20, true), true
		case "SOURCE_LINE":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "optimizer_trace" {
		switch column {
		case "QUERY", "TRACE":
			return performanceSchemaVirtualColumnSpecFor("MEDIUMTEXT", 0, false, false), true
		case "MISSING_BYTES_BEYOND_MAX_MEM_SIZE":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "INSUFFICIENT_PRIVILEGES":
			return performanceSchemaVirtualColumnSpecFor("TINYINT", 0, false, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "st_geometry_columns" {
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "GEOMETRY_TYPE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "SRS_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 80, true), true
		case "SRS_ID":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, true), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "user_attributes" {
		switch column {
		case "USER":
			return performanceSchemaVirtualCharacterSpec("CHAR", 32, false), true
		case "HOST":
			return performanceSchemaVirtualCharacterSpec("CHAR", 255, false), true
		case "ATTRIBUTE":
			return performanceSchemaVirtualColumnSpecFor("JSON", 0, true, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "collation_character_set_applicability" {
		switch column {
		case "COLLATION_NAME", "CHARACTER_SET_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "processlist" {
		switch column {
		case "ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "TIME":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, false), true
		case "USER":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 32, true), true
		case "HOST":
			return performanceSchemaVirtualCharacterSpecWithCharset("VARCHAR", 261, true, "ascii"), true
		case "DB":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "COMMAND":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 16, true), true
		case "STATE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "INFO":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		case "EXECUTION_ENGINE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('PRIMARY','SECONDARY')", 0, true, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "engines" {
		switch column {
		case "ENGINE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "SUPPORT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 8, false), true
		case "COMMENT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 80, false), true
		case "TRANSACTIONS", "XA", "SAVEPOINTS":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, false), true
		default:
			return performanceSchemaVirtualColumnMetadataSpec{}, false
		}
	}

	if table == "key_column_usage" {
		switch column {
		case "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "ORDINAL_POSITION":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "POSITION_IN_UNIQUE_CONSTRAINT":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		}
	}

	if table == "table_constraints" {
		switch column {
		case "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "ENFORCED":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, false), true
		}
	}

	if table == "check_constraints" {
		switch column {
		case "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "CHECK_CLAUSE":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, false, false), true
		}
	}

	if table == "referential_constraints" {
		switch column {
		case "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "UNIQUE_CONSTRAINT_CATALOG", "UNIQUE_CONSTRAINT_SCHEMA", "UNIQUE_CONSTRAINT_NAME", "TABLE_NAME", "REFERENCED_TABLE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "MATCH_OPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 8, false), true
		case "UPDATE_RULE", "DELETE_RULE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 9, false), true
		}
	}

	if table == "views" {
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "VIEW_DEFINITION":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		case "CHECK_OPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 8, false), true
		case "IS_UPDATABLE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, false), true
		case "DEFINER":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 288, false), true
		case "SECURITY_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 7, false), true
		}
	}

	if table == "partitions" {
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLESPACE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, column == "TABLESPACE_NAME"), true
		case "PARTITION_NAME", "SUBPARTITION_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "PARTITION_ORDINAL_POSITION", "SUBPARTITION_ORDINAL_POSITION", "TABLE_ROWS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "PARTITION_METHOD", "SUBPARTITION_METHOD":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 12, true), true
		case "PARTITION_EXPRESSION", "SUBPARTITION_EXPRESSION", "PARTITION_DESCRIPTION":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		case "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "CHECKSUM":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 0, true, false), true
		case "PARTITION_COMMENT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, false), true
		case "NODEGROUP":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 12, false), true
		}
	}

	if table == "parameters" {
		switch column {
		case "SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "ORDINAL_POSITION":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "PARAMETER_MODE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 5, true), true
		case "PARAMETER_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "DATA_TYPE", "DTD_IDENTIFIER":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		case "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "CHARACTER_SET_NAME", "COLLATION_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "ROUTINE_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('FUNCTION','PROCEDURE')", 0, false, false), true
		}
	}

	if table == "routines" {
		switch column {
		case "SPECIFIC_NAME", "ROUTINE_CATALOG", "ROUTINE_SCHEMA", "ROUTINE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "ROUTINE_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('FUNCTION','PROCEDURE')", 0, false, false), true
		case "DATA_TYPE", "DTD_IDENTIFIER", "ROUTINE_DEFINITION", "SQL_MODE", "ROUTINE_COMMENT":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, column != "ROUTINE_COMMENT", false), true
		case "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "CHARACTER_SET_NAME", "COLLATION_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "ROUTINE_BODY":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 8, false), true
		case "EXTERNAL_NAME", "EXTERNAL_LANGUAGE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "PARAMETER_STYLE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 8, false), true
		case "IS_DETERMINISTIC":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, false), true
		case "SQL_DATA_ACCESS":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "SQL_PATH":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "SECURITY_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 7, false), true
		case "CREATED", "LAST_ALTERED":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 0, false, false), true
		case "DEFINER":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 288, false), true
		case "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		}
	}

	if table == "global_variables" || table == "session_variables" || table == "system_variables" {
		switch column {
		case "VARIABLE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "VARIABLE_VALUE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, true), true
		}
	}

	// INNODB_LOCKS and INNODB_LOCK_WAITS were removed from MySQL 8.0, but
	// xmysql keeps their legacy views for clients that still use the 5.7
	// compatibility surface.  Keep the old opaque-ID/string widths here
	// instead of applying the generic identifier inference.
	if table == "innodb_lock_waits" {
		switch column {
		case "REQUESTING_TRX_ID", "BLOCKING_TRX_ID":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 18, false), true
		case "REQUESTED_LOCK_ID", "BLOCKING_LOCK_ID":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 81, false), true
		}
	}

	if table == "innodb_locks" {
		switch column {
		case "LOCK_ID":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 81, false), true
		case "LOCK_TRX_ID":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 18, false), true
		case "LOCK_MODE", "LOCK_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 32, false), true
		case "LOCK_TABLE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, false), true
		case "LOCK_INDEX":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, true), true
		case "LOCK_SPACE", "LOCK_PAGE", "LOCK_REC":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "LOCK_DATA":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 8192, true), true
		}
	}

	if table == "files" {
		switch column {
		case "FILE_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, false), true
		case "FILE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 512, true), true
		case "FILE_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 20, false), true
		case "TABLESPACE_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "LOGFILE_GROUP_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "LOGFILE_GROUP_NUMBER", "DELETED_ROWS", "UPDATE_COUNT", "FREE_EXTENTS", "TOTAL_EXTENTS", "EXTENT_SIZE", "RECOVER_TIME", "TRANSACTION_COUNTER":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, column != "EXTENT_SIZE", false), true
		case "ENGINE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "FULLTEXT_KEYS":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "INITIAL_SIZE", "MAXIMUM_SIZE", "AUTOEXTEND_SIZE", "VERSION", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "CHECKSUM":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "CREATION_TIME", "LAST_UPDATE_TIME", "LAST_ACCESS_TIME", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME":
			return performanceSchemaVirtualColumnSpecFor("DATETIME", 0, true, false), true
		case "ROW_FORMAT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 10, true), true
		case "STATUS":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 20, true), true
		case "EXTRA":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 256, true), true
		case "NODEGROUP_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, false), true
		case "TABLESPACE_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		}
	}

	if table == "tablespaces" {
		switch column {
		case "TABLESPACE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "ENGINE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "TABLESPACE_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "LOGFILE_GROUP_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "EXTENT_SIZE", "AUTOEXTEND_SIZE", "MAXIMUM_SIZE", "NODEGROUP_ID", "FILE_BLOCK_SIZE":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "TABLESPACE_COMMENT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 2048, false), true
		case "ENCRYPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1, false), true
		case "ENGINE_ATTRIBUTE":
			return performanceSchemaVirtualColumnSpecFor("JSON", 0, true, false), true
		case "SE_PRIVATE_DATA":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		case "STATUS":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		}
	}

	if table == "connection_control_failed_login_attempts" {
		switch column {
		case "USERHOST":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 288, false), true
		case "FAILED_ATTEMPTS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}

	// MySQL exposes the Enterprise Thread Pool tables in both schemas.  The
	// runtime is intentionally absent in xmysql, so these contracts describe
	// the discoverable shape while the row source remains empty.
	if table == "tp_thread_group_state" {
		switch column {
		case "TP_GROUP_ID", "CONSUMER_THREADS", "RESERVE_THREADS", "CONNECT_THREAD_COUNT", "CONNECTION_COUNT", "QUEUED_QUERIES", "QUEUED_TRANSACTIONS", "STALL_LIMIT", "PRIO_KICKUP_TIMER", "THREAD_COUNT", "ACTIVE_THREAD_COUNT", "STALLED_THREAD_COUNT", "MAX_THREAD_IDS_IN_GROUP", "NUM_CONNECT_HANDLER_THREAD_IN_SLEEP", "THREADS_BOUND_TO_TRANSACTION", "QUERY_THREADS_COUNT":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		case "ALGORITHM":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 20, false), true
		case "WAITING_THREAD_NUMBER", "EFFECTIVE_MAX_TRANSACTIONS_LIMIT":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, true), true
		case "OLDEST_QUEUED", "TIME_OF_LAST_THREAD_CREATION", "TIME_OF_EARLIEST_CON_EXPIRE":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "NUM_QUERY_THREADS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}
	if table == "tp_thread_group_stats" {
		switch column {
		case "TP_GROUP_ID":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		case "CONNECTIONS_STARTED", "CONNECTIONS_CLOSED", "QUERIES_EXECUTED", "QUERIES_QUEUED", "THREADS_STARTED", "PRIO_KICKUPS", "STALLED_QUERIES_EXECUTED", "BECOME_CONSUMER_THREAD", "BECOME_RESERVE_THREAD", "BECOME_WAITING_THREAD", "WAKE_THREAD_STALL_CHECKER", "SLEEP_WAITS", "DISK_IO_WAITS", "ROW_LOCK_WAITS", "GLOBAL_LOCK_WAITS", "META_DATA_LOCK_WAITS", "TABLE_LOCK_WAITS", "USER_LOCK_WAITS", "BINLOG_WAITS", "GROUP_COMMIT_WAITS", "FSYNC_WAITS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}
	if table == "tp_thread_state" {
		switch column {
		case "TP_GROUP_ID", "TP_THREAD_NUMBER":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		case "PROCESS_COUNT":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "WAIT_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 30, true), true
		case "TP_THREAD_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 32, false), true
		case "THREAD_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		}
	}

	if table == "plugins" {
		switch column {
		case "PLUGIN_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "PLUGIN_VERSION", "PLUGIN_TYPE_VERSION", "PLUGIN_LIBRARY_VERSION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 20, true), true
		case "PLUGIN_STATUS":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 20, true), true
		case "PLUGIN_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 80, true), true
		case "PLUGIN_LIBRARY", "PLUGIN_AUTHOR", "PLUGIN_DESCRIPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "PLUGIN_LICENSE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 80, true), true
		case "LOAD_OPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		}
	}

	if table == "triggers" {
		switch column {
		case "TRIGGER_CATALOG", "TRIGGER_SCHEMA", "TRIGGER_NAME", "EVENT_OBJECT_CATALOG", "EVENT_OBJECT_SCHEMA", "EVENT_OBJECT_TABLE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "EVENT_MANIPULATION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 6, false), true
		case "ACTION_ORDER":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "ACTION_CONDITION":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		case "ACTION_STATEMENT":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, false, false), true
		case "ACTION_ORIENTATION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 9, false), true
		case "ACTION_TIMING":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 6, false), true
		case "ACTION_REFERENCE_OLD_TABLE", "ACTION_REFERENCE_NEW_TABLE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "ACTION_REFERENCE_OLD_ROW", "ACTION_REFERENCE_NEW_ROW":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, false), true
		case "CREATED":
			return performanceSchemaVirtualColumnMetadataSpec{typeName: "TIMESTAMP", scale: 2, nullable: true}, true
		case "SQL_MODE":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, false, false), true
		case "DEFINER":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 288, false), true
		case "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		}
	}

	if table == "events" {
		switch column {
		case "EVENT_CATALOG", "EVENT_SCHEMA", "EVENT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, column == "EVENT_CATALOG"), true
		case "DEFINER":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 288, false), true
		case "TIME_ZONE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "EVENT_BODY":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 8, false), true
		case "EVENT_DEFINITION", "SQL_MODE":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, false, false), true
		case "EVENT_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 9, false), true
		case "EXECUTE_AT", "STARTS", "ENDS":
			return performanceSchemaVirtualColumnSpecFor("DATETIME", 0, true, false), true
		case "INTERVAL_VALUE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 256, true), true
		case "INTERVAL_FIELD":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 18, true), true
		case "STATUS":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 18, false), true
		case "ON_COMPLETION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 12, false), true
		case "CREATED", "LAST_ALTERED":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 0, false, false), true
		case "LAST_EXECUTED":
			return performanceSchemaVirtualColumnSpecFor("DATETIME", 0, true, false), true
		case "EVENT_COMMENT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "ORIGINATOR":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		}
	}

	if table == "table_privileges" || table == "column_privileges" || table == "schema_privileges" || table == "user_privileges" || strings.HasPrefix(table, "role_") {
		switch column {
		case "GRANTEE", "GRANTOR":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 288, false), true
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME", "ROUTINE_CATALOG", "ROUTINE_SCHEMA", "ROUTINE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "PRIVILEGE_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "IS_GRANTABLE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, false), true
		}
	}

	if table == "applicable_roles" || table == "administrable_role_authorizations" || table == "enabled_roles" || strings.HasPrefix(table, "role_") {
		switch column {
		case "GRANTEE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 288, false), true
		case "USER", "ROLE_NAME":
			return performanceSchemaVirtualCharacterSpec("CHAR", 32, false), true
		case "HOST", "GRANTEE_HOST", "ROLE_HOST", "GRANTOR_HOST":
			return performanceSchemaVirtualCharacterSpec("CHAR", 255, false), true
		case "IS_DEFAULT", "IS_MANDATORY":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, false), true
		}
	}

	// The general I_S views reuse the same 64-character object identifiers.
	// This branch deliberately does not infer arbitrary component-specific
	// columns; those continue to use the conservative fallback above.
	switch column {
	case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "INDEX_SCHEMA", "INDEX_NAME", "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME", "ROUTINE_CATALOG", "ROUTINE_SCHEMA", "ROUTINE_NAME", "VIEW_CATALOG", "VIEW_SCHEMA", "VIEW_NAME", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME":
		return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
	case "ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT":
		return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
	case "IS_GRANTABLE", "IS_DEFAULT", "IS_MANDATORY", "ENFORCED":
		return performanceSchemaVirtualCharacterSpec("VARCHAR", 3, false), true
	}
	return performanceSchemaVirtualColumnMetadataSpec{}, false
}

// informationSchemaCanonicalVirtualColumnSpec contains the MySQL 8.4
// contracts for I_S tables whose columns are virtual in xmysql. These fields
// are deliberately explicit: the generic compatibility fallback cannot infer
// enum/text widths, utf8mb3_bin identifiers, role-account widths, or default
// values from a column name alone.
func informationSchemaCanonicalVirtualColumnSpec(table, column string) (performanceSchemaVirtualColumnMetadataSpec, bool) {
	if spec, ok := informationSchemaInnoDBCanonicalVirtualColumnSpec(table, column); ok {
		return spec, true
	}
	character := func(typeName string, length int, nullable bool, charset, collation string, defaultValue interface{}) performanceSchemaVirtualColumnMetadataSpec {
		return performanceSchemaVirtualColumnMetadataSpec{typeName: typeName, length: length, nullable: nullable, charset: charset, collation: collation, defaultValue: defaultValue}
	}
	characterMax := func(typeName string, length, maximumLength int, nullable bool, charset, collation string, defaultValue interface{}) performanceSchemaVirtualColumnMetadataSpec {
		spec := character(typeName, length, nullable, charset, collation, defaultValue)
		spec.characterMaximumLength = maximumLength
		spec.characterMaximumLengthSet = true
		return spec
	}
	noNumericMetadata := func(spec performanceSchemaVirtualColumnMetadataSpec) performanceSchemaVirtualColumnMetadataSpec {
		spec.numericPrecisionSet = true
		spec.numericScaleSet = true
		return spec
	}
	binaryZero := func(nullable bool) performanceSchemaVirtualColumnMetadataSpec {
		return characterMax("VARBINARY(0)", 0, 0, nullable, "", "", nil)
	}
	integer := func(typeName string, nullable, unsigned bool, defaultValue interface{}) performanceSchemaVirtualColumnMetadataSpec {
		return performanceSchemaVirtualColumnMetadataSpec{typeName: typeName, nullable: nullable, unsigned: unsigned, defaultValue: defaultValue}
	}
	utf8mb3 := "utf8mb3"
	general := "utf8mb3_general_ci"
	binary := "utf8mb3_bin"
	utf8mb4 := "utf8mb4"
	utf8mb4Default := "utf8mb4_0900_ai_ci"

	switch table {
	case "applicable_roles", "administrable_role_authorizations":
		switch column {
		case "USER":
			return character("VARCHAR", 97, true, utf8mb3, general, nil), true
		case "HOST":
			return character("VARCHAR", 256, true, utf8mb3, general, nil), true
		case "GRANTEE":
			return character("VARCHAR", 97, true, utf8mb4, utf8mb4Default, nil), true
		case "GRANTEE_HOST", "ROLE_HOST":
			return character("VARCHAR", 256, true, utf8mb4, utf8mb4Default, nil), true
		case "ROLE_NAME":
			return character("VARCHAR", 255, true, utf8mb4, utf8mb4Default, nil), true
		case "IS_GRANTABLE", "IS_MANDATORY":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		case "IS_DEFAULT":
			return character("VARCHAR", 3, true, utf8mb3, general, nil), true
		}
	case "enabled_roles":
		switch column {
		case "ROLE_NAME", "ROLE_HOST":
			return character("VARCHAR", 255, true, utf8mb4, utf8mb4Default, nil), true
		case "IS_DEFAULT":
			return character("VARCHAR", 3, true, utf8mb3, general, nil), true
		case "IS_MANDATORY":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		}
	case "character_sets":
		switch column {
		case "CHARACTER_SET_NAME", "DEFAULT_COLLATE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, general, nil), true
		case "DESCRIPTION":
			return character("VARCHAR", 2048, false, utf8mb3, general, nil), true
		case "MAXLEN":
			return integer("INT", false, true, nil), true
		}
	case "collations":
		switch column {
		case "COLLATION_NAME", "CHARACTER_SET_NAME":
			return character("VARCHAR", 64, false, utf8mb3, general, nil), true
		case "ID":
			return integer("BIGINT", false, true, "0"), true
		case "IS_DEFAULT", "IS_COMPILED":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		case "SORTLEN":
			return integer("INT", false, true, nil), true
		case "PAD_ATTRIBUTE":
			return character("ENUM('PAD SPACE','NO PAD')", 9, false, utf8mb3, binary, nil), true
		}
	case "engines":
		switch column {
		case "ENGINE":
			return characterMax("VARCHAR", 64, 21, false, utf8mb3, general, ""), true
		case "SUPPORT":
			return characterMax("VARCHAR", 8, 2, false, utf8mb3, general, ""), true
		case "COMMENT":
			return characterMax("VARCHAR", 80, 26, false, utf8mb3, general, ""), true
		case "TRANSACTIONS", "XA", "SAVEPOINTS":
			return characterMax("VARCHAR", 3, 1, true, utf8mb3, general, ""), true
		}
	case "check_constraints":
		switch column {
		case "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "CONSTRAINT_NAME":
			return character("VARCHAR", 64, false, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "CHECK_CLAUSE":
			return character("LONGTEXT", 4294967295, false, utf8mb3, binary, nil), true
		}
	case "user_attributes":
		switch column {
		case "USER":
			return character("CHAR", 32, false, utf8mb3, binary, ""), true
		case "HOST":
			return character("CHAR", 255, false, "ascii", "ascii_general_ci", ""), true
		case "ATTRIBUTE":
			return character("LONGTEXT", 4294967295, true, utf8mb4, "utf8mb4_bin", nil), true
		}
	case "views":
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "VIEW_DEFINITION":
			return character("LONGTEXT", 4294967295, true, utf8mb3, binary, nil), true
		case "CHECK_OPTION":
			return character("ENUM('NONE','LOCAL','CASCADED')", 8, true, utf8mb3, binary, nil), true
		case "IS_UPDATABLE":
			return character("ENUM('NO','YES')", 3, true, utf8mb3, binary, nil), true
		case "DEFINER":
			return character("VARCHAR", 288, true, utf8mb3, binary, nil), true
		case "SECURITY_TYPE":
			return character("VARCHAR", 7, true, utf8mb3, binary, nil), true
		case "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION":
			return character("VARCHAR", 64, false, utf8mb3, general, nil), true
		}
	case "columns":
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "COLUMN_NAME":
			return character("VARCHAR", 64, true, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "ORDINAL_POSITION":
			return integer("INT", false, true, nil), true
		case "COLUMN_DEFAULT":
			return character("TEXT", 65535, true, utf8mb3, binary, nil), true
		case "IS_NULLABLE":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		case "DATA_TYPE":
			return character("LONGTEXT", 4294967295, true, utf8mb3, binary, nil), true
		case "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH":
			return integer("BIGINT", true, false, nil), true
		case "NUMERIC_PRECISION", "NUMERIC_SCALE":
			return integer("BIGINT", true, true, nil), true
		case "DATETIME_PRECISION":
			return integer("INT", true, true, nil), true
		case "CHARACTER_SET_NAME", "COLLATION_NAME":
			return character("VARCHAR", 64, true, utf8mb3, general, nil), true
		case "COLUMN_TYPE":
			return character("MEDIUMTEXT", 16777215, false, utf8mb3, binary, nil), true
		case "COLUMN_KEY":
			return character("ENUM('','PRI','UNI','MUL')", 3, false, utf8mb3, binary, nil), true
		case "EXTRA":
			return character("VARCHAR", 256, true, utf8mb3, general, nil), true
		case "PRIVILEGES":
			return character("VARCHAR", 154, true, utf8mb3, general, nil), true
		case "COLUMN_COMMENT":
			return character("TEXT", 65535, false, utf8mb3, binary, nil), true
		case "GENERATION_EXPRESSION":
			return character("LONGTEXT", 4294967295, false, utf8mb3, binary, nil), true
		case "SRS_ID":
			return integer("INT", true, true, nil), true
		}
	case "tables":
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "TABLE_TYPE":
			return character("ENUM('BASE TABLE','VIEW','SYSTEM VIEW')", 11, false, utf8mb3, binary, nil), true
		case "ENGINE":
			return character("VARCHAR", 64, true, utf8mb3, general, nil), true
		case "VERSION":
			return integer("INT", true, false, nil), true
		case "ROW_FORMAT":
			return character("ENUM('Fixed','Dynamic','Compressed','Redundant','Compact','Paged')", 10, true, utf8mb3, binary, nil), true
		case "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "AUTO_INCREMENT":
			return integer("BIGINT", true, true, nil), true
		case "CREATE_TIME":
			return character("TIMESTAMP", 0, false, "", "", nil), true
		case "UPDATE_TIME", "CHECK_TIME":
			return character("DATETIME", 0, true, "", "", nil), true
		case "TABLE_COLLATION":
			return character("VARCHAR", 64, true, utf8mb3, general, nil), true
		case "CHECKSUM":
			return integer("BIGINT", true, false, nil), true
		case "CREATE_OPTIONS":
			return character("VARCHAR", 256, true, utf8mb3, general, nil), true
		case "TABLE_COMMENT":
			return character("TEXT", 65535, true, utf8mb3, general, nil), true
		}
	case "key_column_usage":
		switch column {
		case "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "CONSTRAINT_NAME":
			return character("VARCHAR", 64, true, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "COLUMN_NAME", "REFERENCED_COLUMN_NAME":
			return character("VARCHAR", 64, true, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "ORDINAL_POSITION":
			return integer("INT", false, true, "0"), true
		case "POSITION_IN_UNIQUE_CONSTRAINT":
			return integer("INT", true, true, nil), true
		case "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME":
			return character("VARCHAR", 64, true, utf8mb3, binary, nil), true
		}
	case "table_constraints":
		switch column {
		case "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "TABLE_SCHEMA", "TABLE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "CONSTRAINT_NAME":
			return character("VARCHAR", 64, true, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "CONSTRAINT_TYPE":
			return character("VARCHAR", 11, false, utf8mb3, binary, ""), true
		case "ENFORCED":
			return character("VARCHAR", 3, false, utf8mb3, binary, ""), true
		}
	case "statistics":
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_SCHEMA":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "NON_UNIQUE":
			return integer("INT", false, false, "0"), true
		case "INDEX_NAME":
			return character("VARCHAR", 64, true, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "SEQ_IN_INDEX":
			return integer("INT", false, true, nil), true
		case "COLUMN_NAME":
			return character("VARCHAR", 64, true, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "COLLATION":
			return character("VARCHAR", 1, true, utf8mb3, general, nil), true
		case "CARDINALITY", "SUB_PART":
			return integer("BIGINT", true, false, nil), true
		case "PACKED":
			return binaryZero(true), true
		case "NULLABLE":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		case "INDEX_TYPE":
			return character("VARCHAR", 11, false, utf8mb3, binary, ""), true
		case "COMMENT":
			return character("VARCHAR", 8, false, utf8mb3, general, ""), true
		case "INDEX_COMMENT":
			return character("VARCHAR", 2048, false, utf8mb3, binary, ""), true
		case "IS_VISIBLE":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		case "EXPRESSION":
			return character("LONGTEXT", 4294967295, true, utf8mb3, binary, nil), true
		}
	case "schemata":
		switch column {
		case "CATALOG_NAME", "SCHEMA_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME":
			return character("VARCHAR", 64, false, utf8mb3, general, nil), true
		case "SQL_PATH":
			return binaryZero(true), true
		case "DEFAULT_ENCRYPTION":
			return character("ENUM('NO','YES')", 3, false, utf8mb3, binary, nil), true
		}
	case "schemata_extensions":
		switch column {
		case "CATALOG_NAME", "SCHEMA_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "OPTIONS":
			return character("VARCHAR", 256, true, utf8mb3, general, nil), true
		}
	case "columns_extensions", "tables_extensions":
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "COLUMN_NAME":
			return character("VARCHAR", 64, true, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE":
			return character("JSON", 0, true, "", "", nil), true
		}
	case "table_constraints_extensions":
		switch column {
		case "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "TABLE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "CONSTRAINT_NAME":
			return character("VARCHAR", 64, false, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE":
			return character("JSON", 0, true, "", "", nil), true
		}
	case "view_routine_usage":
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "SPECIFIC_CATALOG", "SPECIFIC_SCHEMA":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "SPECIFIC_NAME":
			return character("VARCHAR", 64, false, utf8mb3, general, nil), true
		}
	case "view_table_usage":
		switch column {
		case "VIEW_CATALOG", "VIEW_SCHEMA", "VIEW_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		}
	case "parameters":
		switch column {
		case "SPECIFIC_CATALOG", "SPECIFIC_SCHEMA":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "SPECIFIC_NAME":
			return character("VARCHAR", 64, false, utf8mb3, general, nil), true
		case "ORDINAL_POSITION":
			return integer("BIGINT", false, true, "0"), true
		case "PARAMETER_MODE":
			return character("VARCHAR", 5, true, utf8mb3, binary, nil), true
		case "PARAMETER_NAME":
			return character("VARCHAR", 64, true, utf8mb3, general, nil), true
		case "DATA_TYPE":
			return character("LONGTEXT", 4294967295, true, utf8mb3, binary, nil), true
		case "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH":
			return integer("BIGINT", true, false, nil), true
		case "NUMERIC_PRECISION", "DATETIME_PRECISION":
			return integer("INT", true, true, nil), true
		case "NUMERIC_SCALE":
			return integer("BIGINT", true, false, nil), true
		case "CHARACTER_SET_NAME", "COLLATION_NAME":
			return character("VARCHAR", 64, true, utf8mb3, general, nil), true
		case "DTD_IDENTIFIER":
			return character("MEDIUMTEXT", 16777215, false, utf8mb3, binary, nil), true
		case "ROUTINE_TYPE":
			return character("ENUM('FUNCTION','PROCEDURE')", 9, false, utf8mb3, binary, nil), true
		}
	case "partitions":
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "PARTITION_NAME", "SUBPARTITION_NAME":
			return character("VARCHAR", 64, true, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "PARTITION_ORDINAL_POSITION", "SUBPARTITION_ORDINAL_POSITION":
			return integer("INT", true, true, nil), true
		case "PARTITION_METHOD", "SUBPARTITION_METHOD":
			return character("VARCHAR", 13, true, utf8mb3, general, nil), true
		case "PARTITION_EXPRESSION", "SUBPARTITION_EXPRESSION":
			return character("VARCHAR", 2048, true, utf8mb3, binary, nil), true
		case "PARTITION_DESCRIPTION":
			return character("TEXT", 65535, true, utf8mb3, binary, nil), true
		case "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE":
			return integer("BIGINT", true, true, nil), true
		case "CREATE_TIME":
			return character("TIMESTAMP", 0, false, "", "", nil), true
		case "UPDATE_TIME", "CHECK_TIME":
			return character("DATETIME", 0, true, "", "", nil), true
		case "CHECKSUM":
			return integer("BIGINT", true, false, nil), true
		case "PARTITION_COMMENT":
			return character("TEXT", 65535, false, utf8mb3, binary, nil), true
		case "NODEGROUP":
			return character("VARCHAR", 256, true, utf8mb3, general, nil), true
		case "TABLESPACE_NAME":
			return character("VARCHAR", 268, true, utf8mb3, binary, nil), true
		}
	case "routines":
		switch column {
		case "SPECIFIC_NAME":
			return character("VARCHAR", 64, false, utf8mb3, general, nil), true
		case "ROUTINE_CATALOG", "ROUTINE_SCHEMA":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "ROUTINE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, general, nil), true
		case "ROUTINE_TYPE":
			return character("ENUM('FUNCTION','PROCEDURE')", 9, false, utf8mb3, binary, nil), true
		case "DATA_TYPE", "ROUTINE_DEFINITION":
			return character("LONGTEXT", 4294967295, true, utf8mb3, binary, nil), true
		case "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH":
			return integer("BIGINT", true, false, nil), true
		case "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION":
			return integer("INT", true, true, nil), true
		case "CHARACTER_SET_NAME", "COLLATION_NAME":
			return character("VARCHAR", 64, true, utf8mb3, general, nil), true
		case "DTD_IDENTIFIER":
			return character("LONGTEXT", 4294967295, true, utf8mb3, binary, nil), true
		case "ROUTINE_BODY":
			return character("VARCHAR", 8, false, utf8mb3, general, ""), true
		case "EXTERNAL_NAME", "SQL_PATH":
			return binaryZero(true), true
		case "EXTERNAL_LANGUAGE":
			return character("VARCHAR", 64, false, utf8mb3, binary, "SQL"), true
		case "PARAMETER_STYLE", "IS_DETERMINISTIC":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		case "SQL_DATA_ACCESS":
			return character("ENUM('CONTAINS SQL','NO SQL','READS SQL DATA','MODIFIES SQL DATA')", 17, false, utf8mb3, binary, nil), true
		case "SECURITY_TYPE":
			return character("ENUM('DEFAULT','INVOKER','DEFINER')", 7, false, utf8mb3, binary, nil), true
		case "CREATED", "LAST_ALTERED":
			return character("TIMESTAMP", 0, false, "", "", nil), true
		case "SQL_MODE":
			return character("SET('REAL_AS_FLOAT','PIPES_AS_CONCAT','ANSI_QUOTES','IGNORE_SPACE','NOT_USED','ONLY_FULL_GROUP_BY','NO_UNSIGNED_SUBTRACTION','NO_DIR_IN_CREATE','NOT_USED_9','NOT_USED_10','NOT_USED_11','NOT_USED_12','NOT_USED_13','NOT_USED_14','NOT_USED_15','NOT_USED_16','NOT_USED_17','NOT_USED_18','ANSI','NO_AUTO_VALUE_ON_ZERO','NO_BACKSLASH_ESCAPES','STRICT_TRANS_TABLES','STRICT_ALL_TABLES','NO_ZERO_IN_DATE','NO_ZERO_DATE','ALLOW_INVALID_DATES','ERROR_FOR_DIVISION_BY_ZERO','TRADITIONAL','NOT_USED_29','HIGH_NOT_PRECEDENCE','NO_ENGINE_SUBSTITUTION','PAD_CHAR_TO_FULL_LENGTH','TIME_TRUNCATE_FRACTIONAL')", 520, false, utf8mb3, binary, nil), true
		case "ROUTINE_COMMENT":
			return character("TEXT", 65535, false, utf8mb3, binary, nil), true
		case "DEFINER":
			return character("VARCHAR", 288, false, utf8mb3, binary, nil), true
		case "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION":
			return character("VARCHAR", 64, false, utf8mb3, general, nil), true
		}
	case "column_statistics":
		switch column {
		case "SCHEMA_NAME", "TABLE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "COLUMN_NAME":
			return character("VARCHAR", 64, false, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "HISTOGRAM":
			return character("JSON", 0, false, "", "", nil), true
		}
	case "files":
		switch column {
		case "FILE_ID", "LOGFILE_GROUP_NUMBER", "FREE_EXTENTS", "TOTAL_EXTENTS", "EXTENT_SIZE", "INITIAL_SIZE", "MAXIMUM_SIZE", "AUTOEXTEND_SIZE", "VERSION", "DATA_FREE":
			return integer("BIGINT", true, false, nil), true
		case "FILE_NAME":
			return character("TEXT", 65535, true, utf8mb3, binary, nil), true
		case "FILE_TYPE":
			return character("VARCHAR", 256, true, utf8mb3, general, nil), true
		case "TABLESPACE_NAME":
			return character("VARCHAR", 268, false, utf8mb3, binary, nil), true
		case "TABLE_CATALOG":
			return characterMax("VARCHAR(0)", 0, 0, false, utf8mb3, general, ""), true
		case "TABLE_SCHEMA", "TABLE_NAME", "FULLTEXT_KEYS", "DELETED_ROWS", "UPDATE_COUNT", "CREATION_TIME", "LAST_UPDATE_TIME", "LAST_ACCESS_TIME", "RECOVER_TIME", "TRANSACTION_COUNTER", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "CHECKSUM":
			return binaryZero(true), true
		case "LOGFILE_GROUP_NAME", "ROW_FORMAT", "STATUS", "EXTRA":
			return character("VARCHAR", 256, true, utf8mb3, general, nil), true
		case "ENGINE":
			return character("VARCHAR", 64, false, utf8mb3, general, nil), true
		}
	case "st_geometry_columns":
		switch column {
		case "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "COLUMN_NAME":
			return character("VARCHAR", 64, true, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "SRS_NAME":
			return character("VARCHAR", 80, true, utf8mb3, general, nil), true
		case "SRS_ID":
			return integer("INT", true, true, nil), true
		case "GEOMETRY_TYPE_NAME":
			return character("LONGTEXT", 4294967295, true, utf8mb3, binary, nil), true
		}
	case "st_spatial_reference_systems":
		switch column {
		case "SRS_NAME":
			return character("VARCHAR", 80, false, utf8mb3, general, nil), true
		case "SRS_ID", "ORGANIZATION_COORDSYS_ID":
			return integer("INT", column != "SRS_ID", true, nil), true
		case "ORGANIZATION":
			return character("VARCHAR", 256, true, utf8mb3, general, nil), true
		case "DEFINITION":
			return character("VARCHAR", 4096, false, utf8mb3, binary, nil), true
		case "DESCRIPTION":
			return character("VARCHAR", 2048, true, utf8mb3, binary, nil), true
		}
	case "st_units_of_measure":
		switch column {
		case "UNIT_NAME", "DESCRIPTION":
			return character("VARCHAR", 255, true, utf8mb4, utf8mb4Default, nil), true
		case "UNIT_TYPE":
			return character("VARCHAR", 7, true, utf8mb4, utf8mb4Default, nil), true
		case "CONVERSION_FACTOR":
			spec := performanceSchemaVirtualColumnMetadataSpec{typeName: "DOUBLE", nullable: true, numericPrecision: int64(22), numericPrecisionSet: true, numericScaleSet: true}
			return spec, true
		}
	case "keywords":
		switch column {
		case "WORD":
			return character("VARCHAR", 128, true, utf8mb4, utf8mb4Default, nil), true
		case "RESERVED":
			return integer("INT", true, false, nil), true
		}
	case "plugins":
		switch column {
		case "PLUGIN_NAME", "PLUGIN_LIBRARY", "PLUGIN_AUTHOR":
			return characterMax("VARCHAR", 64, 21, column != "PLUGIN_NAME", utf8mb3, general, ""), true
		case "PLUGIN_VERSION", "PLUGIN_TYPE_VERSION", "PLUGIN_LIBRARY_VERSION":
			return characterMax("VARCHAR", 20, 6, column == "PLUGIN_LIBRARY_VERSION", utf8mb3, general, ""), true
		case "PLUGIN_STATUS":
			return characterMax("VARCHAR", 10, 3, false, utf8mb3, general, ""), true
		case "PLUGIN_TYPE":
			return characterMax("VARCHAR", 80, 26, false, utf8mb3, general, ""), true
		case "PLUGIN_DESCRIPTION":
			return characterMax("VARCHAR", 65535, 21845, true, utf8mb3, general, ""), true
		case "PLUGIN_LICENSE":
			return characterMax("VARCHAR", 80, 26, true, utf8mb3, general, ""), true
		case "LOAD_OPTION":
			return characterMax("VARCHAR", 64, 21, false, utf8mb3, general, ""), true
		}
	case "processlist":
		switch column {
		case "ID":
			return noNumericMetadata(integer("BIGINT", false, true, nil)), true
		case "USER":
			return characterMax("VARCHAR", 32, 10, false, utf8mb3, general, ""), true
		case "HOST":
			return characterMax("VARCHAR", 261, 87, false, utf8mb3, general, ""), true
		case "DB":
			return characterMax("VARCHAR", 64, 21, true, utf8mb3, general, ""), true
		case "COMMAND":
			return characterMax("VARCHAR", 16, 5, false, utf8mb3, general, ""), true
		case "TIME":
			return noNumericMetadata(integer("INT", false, false, nil)), true
		case "STATE":
			return characterMax("VARCHAR", 64, 21, true, utf8mb3, general, ""), true
		case "INFO":
			return characterMax("VARCHAR", 65535, 21845, true, utf8mb3, general, ""), true
		}
	case "profiling":
		decimal := func(nullable bool) performanceSchemaVirtualColumnMetadataSpec {
			return performanceSchemaVirtualColumnMetadataSpec{typeName: "DECIMAL", length: 905, nullable: nullable}
		}
		integerNullable := func(nullable bool) performanceSchemaVirtualColumnMetadataSpec {
			return integer("INT", nullable, false, nil)
		}
		switch column {
		case "QUERY_ID", "SEQ":
			return noNumericMetadata(integer("INT", false, false, nil)), true
		case "STATE":
			return characterMax("VARCHAR", 30, 10, false, utf8mb3, general, ""), true
		case "DURATION":
			return noNumericMetadata(decimal(false)), true
		case "CPU_USER", "CPU_SYSTEM":
			return noNumericMetadata(decimal(true)), true
		case "CONTEXT_VOLUNTARY", "CONTEXT_INVOLUNTARY", "BLOCK_OPS_IN", "BLOCK_OPS_OUT", "MESSAGES_SENT", "MESSAGES_RECEIVED", "PAGE_FAULTS_MAJOR", "PAGE_FAULTS_MINOR", "SWAPS", "SOURCE_LINE":
			return noNumericMetadata(integerNullable(true)), true
		case "SOURCE_FUNCTION":
			return characterMax("VARCHAR", 30, 10, true, utf8mb3, general, ""), true
		case "SOURCE_FILE":
			return characterMax("VARCHAR", 20, 6, true, utf8mb3, general, ""), true
		}
	case "resource_groups":
		switch column {
		case "RESOURCE_GROUP_NAME":
			return character("VARCHAR", 64, false, utf8mb3, general, nil), true
		case "RESOURCE_GROUP_TYPE":
			return character("ENUM('SYSTEM','USER')", 6, false, utf8mb3, binary, nil), true
		case "RESOURCE_GROUP_ENABLED":
			return character("TINYINT(1)", 0, false, "", "", nil), true
		case "VCPU_IDS":
			return characterMax("BLOB", 65535, 65535, true, "", "", nil), true
		case "THREAD_PRIORITY":
			return integer("INT", false, false, nil), true
		}
	case "tablespaces_extensions":
		switch column {
		case "TABLESPACE_NAME":
			return character("VARCHAR", 268, false, utf8mb3, binary, nil), true
		case "ENGINE_ATTRIBUTE":
			return character("JSON", 0, true, "", "", nil), true
		}
	case "referential_constraints":
		switch column {
		case "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "UNIQUE_CONSTRAINT_CATALOG", "UNIQUE_CONSTRAINT_SCHEMA", "TABLE_NAME", "REFERENCED_TABLE_NAME":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "CONSTRAINT_NAME", "UNIQUE_CONSTRAINT_NAME":
			return character("VARCHAR", 64, true, utf8mb3, "utf8mb3_tolower_ci", nil), true
		case "MATCH_OPTION":
			return character("ENUM('NONE','PARTIAL','FULL')", 7, false, utf8mb3, binary, nil), true
		case "UPDATE_RULE", "DELETE_RULE":
			return character("ENUM('NO ACTION','RESTRICT','CASCADE','SET NULL','SET DEFAULT')", 11, false, utf8mb3, binary, nil), true
		}
	case "optimizer_trace":
		noNumeric := func(spec performanceSchemaVirtualColumnMetadataSpec) performanceSchemaVirtualColumnMetadataSpec {
			spec.numericPrecisionSet = true
			spec.numericScaleSet = true
			return spec
		}
		switch column {
		case "QUERY", "TRACE":
			return characterMax("VARCHAR", 65535, 21845, false, utf8mb3, general, ""), true
		case "MISSING_BYTES_BEYOND_MAX_MEM_SIZE":
			return noNumeric(integer("INT", false, false, nil)), true
		case "INSUFFICIENT_PRIVILEGES":
			return noNumeric(character("TINYINT(1)", 0, false, "", "", nil)), true
		}
	case "events":
		switch column {
		case "EVENT_CATALOG", "EVENT_SCHEMA", "DEFINER", "TIME_ZONE":
			length := 64
			if column == "DEFINER" {
				length = 288
			}
			return character("VARCHAR", length, false, utf8mb3, binary, nil), true
		case "EVENT_BODY":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		case "EVENT_DEFINITION":
			return character("LONGTEXT", 4294967295, false, utf8mb3, binary, nil), true
		case "INTERVAL_FIELD":
			return character("ENUM('YEAR','QUARTER','MONTH','DAY','HOUR','MINUTE','WEEK','SECOND','MICROSECOND','YEAR_MONTH','DAY_HOUR','DAY_MINUTE','DAY_SECOND','HOUR_MINUTE','HOUR_SECOND','MINUTE_SECOND','DAY_MICROSECOND','HOUR_MICROSECOND','MINUTE_MICROSECOND','SECOND_MICROSECOND')", 18, true, utf8mb3, binary, nil), true
		case "SQL_MODE":
			return character("SET('REAL_AS_FLOAT','PIPES_AS_CONCAT','ANSI_QUOTES','IGNORE_SPACE','NOT_USED','ONLY_FULL_GROUP_BY','NO_UNSIGNED_SUBTRACTION','NO_DIR_IN_CREATE','NOT_USED_9','NOT_USED_10','NOT_USED_11','NOT_USED_12','NOT_USED_13','NOT_USED_14','NOT_USED_15','NOT_USED_16','NOT_USED_17','NOT_USED_18','ANSI','NO_AUTO_VALUE_ON_ZERO','NO_BACKSLASH_ESCAPES','STRICT_TRANS_TABLES','STRICT_ALL_TABLES','NO_ZERO_IN_DATE','NO_ZERO_DATE','ALLOW_INVALID_DATES','ERROR_FOR_DIVISION_BY_ZERO','TRADITIONAL','NOT_USED_29','HIGH_NOT_PRECEDENCE','NO_ENGINE_SUBSTITUTION','PAD_CHAR_TO_FULL_LENGTH','TIME_TRUNCATE_FRACTIONAL')", 520, false, utf8mb3, binary, ""), true
		case "STATUS":
			return character("VARCHAR", 21, false, utf8mb3, binary, ""), true
		case "EVENT_COMMENT":
			return character("VARCHAR", 2048, false, utf8mb3, binary, ""), true
		case "ORIGINATOR":
			return integer("INT", false, true, nil), true
		}
	case "triggers":
		switch column {
		case "TRIGGER_CATALOG", "TRIGGER_SCHEMA", "EVENT_OBJECT_CATALOG", "EVENT_OBJECT_SCHEMA", "EVENT_OBJECT_TABLE":
			return character("VARCHAR", 64, false, utf8mb3, binary, nil), true
		case "EVENT_MANIPULATION":
			return character("ENUM('INSERT','UPDATE','DELETE')", 6, false, utf8mb3, binary, nil), true
		case "ACTION_ORDER":
			return integer("INT", false, true, nil), true
		case "ACTION_CONDITION", "ACTION_REFERENCE_OLD_TABLE", "ACTION_REFERENCE_NEW_TABLE":
			return binaryZero(true), true
		case "ACTION_STATEMENT":
			return character("LONGTEXT", 4294967295, false, utf8mb3, binary, nil), true
		case "ACTION_ORIENTATION":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		case "ACTION_TIMING":
			return character("ENUM('BEFORE','AFTER')", 6, false, utf8mb3, binary, nil), true
		case "CREATED":
			return character("TIMESTAMP", 2, false, "", "", nil), true
		case "SQL_MODE":
			return character("SET('REAL_AS_FLOAT','PIPES_AS_CONCAT','ANSI_QUOTES','IGNORE_SPACE','NOT_USED','ONLY_FULL_GROUP_BY','NO_UNSIGNED_SUBTRACTION','NO_DIR_IN_CREATE','NOT_USED_9','NOT_USED_10','NOT_USED_11','NOT_USED_12','NOT_USED_13','NOT_USED_14','NOT_USED_15','NOT_USED_16','NOT_USED_17','NOT_USED_18','ANSI','NO_AUTO_VALUE_ON_ZERO','NO_BACKSLASH_ESCAPES','STRICT_TRANS_TABLES','STRICT_ALL_TABLES','NO_ZERO_IN_DATE','NO_ZERO_DATE','ALLOW_INVALID_DATES','ERROR_FOR_DIVISION_BY_ZERO','TRADITIONAL','NOT_USED_29','HIGH_NOT_PRECEDENCE','NO_ENGINE_SUBSTITUTION','PAD_CHAR_TO_FULL_LENGTH','TIME_TRUNCATE_FRACTIONAL')", 520, false, utf8mb3, binary, ""), true
		case "DEFINER":
			return character("VARCHAR", 288, false, utf8mb3, binary, nil), true
		}
	case "table_privileges", "column_privileges", "schema_privileges", "user_privileges":
		switch column {
		case "GRANTEE":
			return characterMax("VARCHAR", 292, 97, false, utf8mb3, general, ""), true
		case "TABLE_CATALOG":
			return characterMax("VARCHAR", 512, 170, false, utf8mb3, general, ""), true
		case "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE":
			return characterMax("VARCHAR", 64, 21, false, utf8mb3, general, ""), true
		case "IS_GRANTABLE":
			return characterMax("VARCHAR", 3, 1, false, utf8mb3, general, ""), true
		}
	case "role_table_grants":
		switch column {
		case "GRANTOR":
			return character("VARCHAR", 97, true, utf8mb3, general, nil), true
		case "GRANTOR_HOST":
			return character("VARCHAR", 256, true, utf8mb3, general, nil), true
		case "GRANTEE":
			return character("CHAR", 32, false, utf8mb3, binary, ""), true
		case "GRANTEE_HOST":
			return character("CHAR", 255, false, "ascii", "ascii_general_ci", ""), true
		case "TABLE_CATALOG":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		case "TABLE_SCHEMA", "TABLE_NAME":
			return character("CHAR", 64, false, utf8mb3, binary, ""), true
		case "PRIVILEGE_TYPE":
			return character("SET('Select','Insert','Update','Delete','Create','Drop','Grant','References','Index','Alter','Create View','Show view','Trigger')", 98, false, utf8mb3, general, ""), true
		case "IS_GRANTABLE":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		}
	case "role_column_grants":
		switch column {
		case "GRANTOR":
			return character("VARCHAR", 97, true, utf8mb3, general, nil), true
		case "GRANTOR_HOST":
			return character("VARCHAR", 256, true, utf8mb3, general, nil), true
		case "GRANTEE":
			return character("CHAR", 32, false, utf8mb3, binary, ""), true
		case "GRANTEE_HOST":
			return character("CHAR", 255, false, "ascii", "ascii_general_ci", ""), true
		case "TABLE_CATALOG":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		case "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME":
			return character("CHAR", 64, false, utf8mb3, binary, ""), true
		case "PRIVILEGE_TYPE":
			return character("SET('Select','Insert','Update','References')", 31, false, utf8mb3, general, ""), true
		case "IS_GRANTABLE":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		}
	case "role_routine_grants":
		switch column {
		case "GRANTOR":
			return character("VARCHAR", 97, true, utf8mb3, general, nil), true
		case "GRANTOR_HOST":
			return character("VARCHAR", 256, true, utf8mb3, general, nil), true
		case "GRANTEE":
			return character("CHAR", 32, false, utf8mb3, binary, ""), true
		case "GRANTEE_HOST":
			return character("CHAR", 255, false, "ascii", "ascii_general_ci", ""), true
		case "SPECIFIC_CATALOG", "ROUTINE_CATALOG":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		case "SPECIFIC_SCHEMA", "ROUTINE_SCHEMA":
			return character("CHAR", 64, false, utf8mb3, binary, ""), true
		case "SPECIFIC_NAME", "ROUTINE_NAME":
			return character("CHAR", 64, false, utf8mb3, general, ""), true
		case "PRIVILEGE_TYPE":
			return character("SET('Execute','Alter Routine','Grant')", 27, false, utf8mb3, general, ""), true
		case "IS_GRANTABLE":
			return character("VARCHAR", 3, false, utf8mb3, general, ""), true
		}
	}
	return performanceSchemaVirtualColumnMetadataSpec{}, false
}

func informationSchemaExactVirtualSpec(typeName string, length int, nullable, unsigned bool, charset, collation string, characterMaximumLength, numericPrecision, numericScale, defaultValue interface{}) performanceSchemaVirtualColumnMetadataSpec {
	spec := performanceSchemaVirtualColumnMetadataSpec{
		typeName:             typeName,
		length:               length,
		nullable:             nullable,
		unsigned:             unsigned,
		charset:              charset,
		collation:            collation,
		numericPrecision:     numericPrecision,
		numericPrecisionSet:  true,
		numericScale:         numericScale,
		numericScaleSet:      true,
		datetimePrecisionSet: true,
		defaultValue:         defaultValue,
	}
	if value, ok := characterMaximumLength.(int64); ok {
		spec.characterMaximumLength = int(value)
		spec.characterMaximumLengthSet = true
	}
	return spec
}

func informationSchemaInnoDBCanonicalVirtualColumnSpec(table, column string) (performanceSchemaVirtualColumnMetadataSpec, bool) {
	switch table {
	case "innodb_buffer_page":
		switch column {
		case "POOL_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "BLOCK_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "SPACE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PAGE_NUMBER":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PAGE_TYPE":
			return informationSchemaExactVirtualSpec("VARCHAR", 64, true, false, "utf8mb3", "utf8mb3_general_ci", int64(21), nil, nil, ""), true
		case "FLUSH_TYPE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "FIX_COUNT":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "IS_HASHED":
			return informationSchemaExactVirtualSpec("VARCHAR", 3, true, false, "utf8mb3", "utf8mb3_general_ci", int64(1), nil, nil, ""), true
		case "NEWEST_MODIFICATION":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "OLDEST_MODIFICATION":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "ACCESS_TIME":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TABLE_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 1024, true, false, "utf8mb3", "utf8mb3_general_ci", int64(341), nil, nil, ""), true
		case "INDEX_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 1024, true, false, "utf8mb3", "utf8mb3_general_ci", int64(341), nil, nil, ""), true
		case "NUMBER_RECORDS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "DATA_SIZE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "COMPRESSED_SIZE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PAGE_STATE":
			return informationSchemaExactVirtualSpec("VARCHAR", 64, true, false, "utf8mb3", "utf8mb3_general_ci", int64(21), nil, nil, ""), true
		case "IO_FIX":
			return informationSchemaExactVirtualSpec("VARCHAR", 64, true, false, "utf8mb3", "utf8mb3_general_ci", int64(21), nil, nil, ""), true
		case "IS_OLD":
			return informationSchemaExactVirtualSpec("VARCHAR", 3, true, false, "utf8mb3", "utf8mb3_general_ci", int64(1), nil, nil, ""), true
		case "FREE_PAGE_CLOCK":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "IS_STALE":
			return informationSchemaExactVirtualSpec("VARCHAR", 3, true, false, "utf8mb3", "utf8mb3_general_ci", int64(1), nil, nil, ""), true
		}
	case "innodb_buffer_page_lru":
		switch column {
		case "POOL_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "LRU_POSITION":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "SPACE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PAGE_NUMBER":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PAGE_TYPE":
			return informationSchemaExactVirtualSpec("VARCHAR", 64, true, false, "utf8mb3", "utf8mb3_general_ci", int64(21), nil, nil, ""), true
		case "FLUSH_TYPE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "FIX_COUNT":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "IS_HASHED":
			return informationSchemaExactVirtualSpec("VARCHAR", 3, true, false, "utf8mb3", "utf8mb3_general_ci", int64(1), nil, nil, ""), true
		case "NEWEST_MODIFICATION":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "OLDEST_MODIFICATION":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "ACCESS_TIME":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TABLE_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 1024, true, false, "utf8mb3", "utf8mb3_general_ci", int64(341), nil, nil, ""), true
		case "INDEX_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 1024, true, false, "utf8mb3", "utf8mb3_general_ci", int64(341), nil, nil, ""), true
		case "NUMBER_RECORDS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "DATA_SIZE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "COMPRESSED_SIZE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "COMPRESSED":
			return informationSchemaExactVirtualSpec("VARCHAR", 3, true, false, "utf8mb3", "utf8mb3_general_ci", int64(1), nil, nil, ""), true
		case "IO_FIX":
			return informationSchemaExactVirtualSpec("VARCHAR", 64, true, false, "utf8mb3", "utf8mb3_general_ci", int64(21), nil, nil, ""), true
		case "IS_OLD":
			return informationSchemaExactVirtualSpec("VARCHAR", 3, true, false, "utf8mb3", "utf8mb3_general_ci", int64(1), nil, nil, ""), true
		case "FREE_PAGE_CLOCK":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		}
	case "innodb_buffer_pool_stats":
		switch column {
		case "POOL_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "POOL_SIZE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "FREE_BUFFERS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "DATABASE_PAGES":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "OLD_DATABASE_PAGES":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "MODIFIED_DATABASE_PAGES":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PENDING_DECOMPRESS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PENDING_READS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PENDING_FLUSH_LRU":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PENDING_FLUSH_LIST":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PAGES_MADE_YOUNG":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PAGES_NOT_MADE_YOUNG":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PAGES_MADE_YOUNG_RATE":
			return informationSchemaExactVirtualSpec("FLOAT(12,0)", 0, false, false, "", "", nil, nil, nil, ""), true
		case "PAGES_MADE_NOT_YOUNG_RATE":
			return informationSchemaExactVirtualSpec("FLOAT(12,0)", 0, false, false, "", "", nil, nil, nil, ""), true
		case "NUMBER_PAGES_READ":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "NUMBER_PAGES_CREATED":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "NUMBER_PAGES_WRITTEN":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PAGES_READ_RATE":
			return informationSchemaExactVirtualSpec("FLOAT(12,0)", 0, false, false, "", "", nil, nil, nil, ""), true
		case "PAGES_CREATE_RATE":
			return informationSchemaExactVirtualSpec("FLOAT(12,0)", 0, false, false, "", "", nil, nil, nil, ""), true
		case "PAGES_WRITTEN_RATE":
			return informationSchemaExactVirtualSpec("FLOAT(12,0)", 0, false, false, "", "", nil, nil, nil, ""), true
		case "NUMBER_PAGES_GET":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "HIT_RATE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "YOUNG_MAKE_PER_THOUSAND_GETS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "NOT_YOUNG_MAKE_PER_THOUSAND_GETS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "NUMBER_PAGES_READ_AHEAD":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "NUMBER_READ_AHEAD_EVICTED":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "READ_AHEAD_RATE":
			return informationSchemaExactVirtualSpec("FLOAT(12,0)", 0, false, false, "", "", nil, nil, nil, ""), true
		case "READ_AHEAD_EVICTED_RATE":
			return informationSchemaExactVirtualSpec("FLOAT(12,0)", 0, false, false, "", "", nil, nil, nil, ""), true
		case "LRU_IO_TOTAL":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "LRU_IO_CURRENT":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "UNCOMPRESS_TOTAL":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "UNCOMPRESS_CURRENT":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		}
	case "innodb_cached_indexes":
		switch column {
		case "SPACE_ID":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "INDEX_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "N_CACHED_PAGES":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		}
	case "innodb_cmp":
		switch column {
		case "PAGE_SIZE":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "COMPRESS_OPS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "COMPRESS_OPS_OK":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "COMPRESS_TIME":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "UNCOMPRESS_OPS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "UNCOMPRESS_TIME":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		}
	case "innodb_cmp_per_index":
		switch column {
		case "DATABASE_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 192, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "TABLE_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 192, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "INDEX_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 192, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "COMPRESS_OPS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "COMPRESS_OPS_OK":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "COMPRESS_TIME":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "UNCOMPRESS_OPS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "UNCOMPRESS_TIME":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		}
	case "innodb_cmp_per_index_reset":
		switch column {
		case "DATABASE_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 192, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "TABLE_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 192, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "INDEX_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 192, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "COMPRESS_OPS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "COMPRESS_OPS_OK":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "COMPRESS_TIME":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "UNCOMPRESS_OPS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "UNCOMPRESS_TIME":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		}
	case "innodb_cmp_reset":
		switch column {
		case "PAGE_SIZE":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "COMPRESS_OPS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "COMPRESS_OPS_OK":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "COMPRESS_TIME":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "UNCOMPRESS_OPS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "UNCOMPRESS_TIME":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		}
	case "innodb_cmpmem":
		switch column {
		case "PAGE_SIZE":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "BUFFER_POOL_INSTANCE":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "PAGES_USED":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "PAGES_FREE":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "RELOCATION_OPS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "RELOCATION_TIME":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		}
	case "innodb_cmpmem_reset":
		switch column {
		case "PAGE_SIZE":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "BUFFER_POOL_INSTANCE":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "PAGES_USED":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "PAGES_FREE":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "RELOCATION_OPS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "RELOCATION_TIME":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		}
	case "innodb_columns":
		switch column {
		case "TABLE_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 193, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "POS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "MTYPE":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "PRTYPE":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "LEN":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "HAS_DEFAULT":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "DEFAULT_VALUE":
			return informationSchemaExactVirtualSpec("TEXT", 0, true, false, "utf8mb3", "utf8mb3_general_ci", int64(65535), nil, nil, ""), true
		}
	case "innodb_datafiles":
		switch column {
		case "SPACE":
			return informationSchemaExactVirtualSpec("VARBINARY", 256, true, false, "", "", int64(256), nil, nil, nil), true
		case "PATH":
			return informationSchemaExactVirtualSpec("VARCHAR", 512, false, false, "utf8mb3", "utf8mb3_bin", int64(512), nil, nil, nil), true
		}
	case "innodb_fields":
		switch column {
		case "INDEX_ID":
			return informationSchemaExactVirtualSpec("VARBINARY", 256, true, false, "", "", int64(256), nil, nil, nil), true
		case "NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 64, false, false, "utf8mb3", "utf8mb3_tolower_ci", int64(64), nil, nil, nil), true
		case "POS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, int64(20), int64(0), "0"), true
		}
	case "innodb_foreign":
		switch column {
		case "ID":
			return informationSchemaExactVirtualSpec("VARCHAR", 129, true, false, "utf8mb3", "utf8mb3_bin", int64(129), nil, nil, nil), true
		case "FOR_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 129, true, false, "utf8mb3", "utf8mb3_bin", int64(129), nil, nil, nil), true
		case "REF_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 129, true, false, "utf8mb3", "utf8mb3_bin", int64(129), nil, nil, nil), true
		case "N_COLS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, false, "", "", nil, int64(19), int64(0), "0"), true
		case "TYPE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, int64(20), int64(0), "0"), true
		}
	case "innodb_foreign_cols":
		switch column {
		case "ID":
			return informationSchemaExactVirtualSpec("VARCHAR", 129, true, false, "utf8mb3", "utf8mb3_bin", int64(129), nil, nil, nil), true
		case "FOR_COL_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 64, false, false, "utf8mb3", "utf8mb3_tolower_ci", int64(64), nil, nil, nil), true
		case "REF_COL_NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 64, false, false, "utf8mb3", "utf8mb3_tolower_ci", int64(64), nil, nil, nil), true
		case "POS":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, int64(10), int64(0), nil), true
		}
	case "innodb_indexes":
		switch column {
		case "INDEX_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 193, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "TABLE_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TYPE":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "N_FIELDS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "PAGE_NO":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "SPACE":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "MERGE_THRESHOLD":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		}
	case "innodb_metrics":
		switch column {
		case "NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 193, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "SUBSYSTEM":
			return informationSchemaExactVirtualSpec("VARCHAR", 193, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "COUNT":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "MAX_COUNT":
			return informationSchemaExactVirtualSpec("BIGINT", 0, true, false, "", "", nil, nil, nil, ""), true
		case "MIN_COUNT":
			return informationSchemaExactVirtualSpec("BIGINT", 0, true, false, "", "", nil, nil, nil, ""), true
		case "AVG_COUNT":
			return informationSchemaExactVirtualSpec("FLOAT(12,0)", 0, true, false, "", "", nil, nil, nil, ""), true
		case "COUNT_RESET":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "MAX_COUNT_RESET":
			return informationSchemaExactVirtualSpec("BIGINT", 0, true, false, "", "", nil, nil, nil, ""), true
		case "MIN_COUNT_RESET":
			return informationSchemaExactVirtualSpec("BIGINT", 0, true, false, "", "", nil, nil, nil, ""), true
		case "AVG_COUNT_RESET":
			return informationSchemaExactVirtualSpec("FLOAT(12,0)", 0, true, false, "", "", nil, nil, nil, ""), true
		case "TIME_ENABLED":
			return informationSchemaExactVirtualSpec("DATETIME", 0, true, false, "", "", nil, nil, nil, ""), true
		case "TIME_DISABLED":
			return informationSchemaExactVirtualSpec("DATETIME", 0, true, false, "", "", nil, nil, nil, ""), true
		case "TIME_ELAPSED":
			return informationSchemaExactVirtualSpec("BIGINT", 0, true, false, "", "", nil, nil, nil, ""), true
		case "TIME_RESET":
			return informationSchemaExactVirtualSpec("DATETIME", 0, true, false, "", "", nil, nil, nil, ""), true
		case "STATUS":
			return informationSchemaExactVirtualSpec("VARCHAR", 193, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "TYPE":
			return informationSchemaExactVirtualSpec("VARCHAR", 193, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "COMMENT":
			return informationSchemaExactVirtualSpec("VARCHAR", 193, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		}
	case "innodb_session_temp_tablespaces":
		switch column {
		case "ID":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "SPACE":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "PATH":
			return informationSchemaExactVirtualSpec("VARCHAR", 4001, false, false, "utf8mb3", "utf8mb3_general_ci", int64(1333), nil, nil, ""), true
		case "SIZE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "STATE":
			return informationSchemaExactVirtualSpec("VARCHAR", 192, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "PURPOSE":
			return informationSchemaExactVirtualSpec("VARCHAR", 192, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		}
	case "innodb_tables":
		switch column {
		case "TABLE_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 655, false, false, "utf8mb3", "utf8mb3_general_ci", int64(218), nil, nil, ""), true
		case "FLAG":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "N_COLS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "SPACE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "ROW_FORMAT":
			return informationSchemaExactVirtualSpec("VARCHAR", 12, true, false, "utf8mb3", "utf8mb3_general_ci", int64(4), nil, nil, ""), true
		case "ZIP_PAGE_SIZE":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "SPACE_TYPE":
			return informationSchemaExactVirtualSpec("VARCHAR", 10, true, false, "utf8mb3", "utf8mb3_general_ci", int64(3), nil, nil, ""), true
		case "INSTANT_COLS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "TOTAL_ROW_VERSIONS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		}
	case "innodb_tablespaces":
		switch column {
		case "SPACE":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 655, false, false, "utf8mb3", "utf8mb3_general_ci", int64(218), nil, nil, ""), true
		case "FLAG":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "ROW_FORMAT":
			return informationSchemaExactVirtualSpec("VARCHAR", 22, true, false, "utf8mb3", "utf8mb3_general_ci", int64(7), nil, nil, ""), true
		case "PAGE_SIZE":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "ZIP_PAGE_SIZE":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "SPACE_TYPE":
			return informationSchemaExactVirtualSpec("VARCHAR", 10, true, false, "utf8mb3", "utf8mb3_general_ci", int64(3), nil, nil, ""), true
		case "FS_BLOCK_SIZE":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "FILE_SIZE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "ALLOCATED_SIZE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "AUTOEXTEND_SIZE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "SERVER_VERSION":
			return informationSchemaExactVirtualSpec("VARCHAR", 10, true, false, "utf8mb3", "utf8mb3_general_ci", int64(3), nil, nil, ""), true
		case "SPACE_VERSION":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "ENCRYPTION":
			return informationSchemaExactVirtualSpec("VARCHAR", 1, true, false, "utf8mb3", "utf8mb3_general_ci", int64(0), nil, nil, ""), true
		case "STATE":
			return informationSchemaExactVirtualSpec("VARCHAR", 10, true, false, "utf8mb3", "utf8mb3_general_ci", int64(3), nil, nil, ""), true
		}
	case "innodb_tablespaces_brief":
		switch column {
		case "SPACE":
			return informationSchemaExactVirtualSpec("VARBINARY", 256, true, false, "", "", int64(256), nil, nil, nil), true
		case "NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 268, false, false, "utf8mb3", "utf8mb3_bin", int64(268), nil, nil, nil), true
		case "PATH":
			return informationSchemaExactVirtualSpec("VARCHAR", 512, false, false, "utf8mb3", "utf8mb3_bin", int64(512), nil, nil, nil), true
		case "FLAG":
			return informationSchemaExactVirtualSpec("VARBINARY", 256, true, false, "", "", int64(256), nil, nil, nil), true
		case "SPACE_TYPE":
			return informationSchemaExactVirtualSpec("VARCHAR", 7, false, false, "utf8mb3", "utf8mb3_general_ci", int64(7), nil, nil, ""), true
		}
	case "innodb_tablestats":
		switch column {
		case "TABLE_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 193, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "STATS_INITIALIZED":
			return informationSchemaExactVirtualSpec("VARCHAR", 193, false, false, "utf8mb3", "utf8mb3_general_ci", int64(64), nil, nil, ""), true
		case "NUM_ROWS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "CLUST_INDEX_SIZE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "OTHER_INDEX_SIZE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "MODIFIED_COUNTER":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "AUTOINC":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "REF_COUNT":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		}
	case "innodb_temp_table_info":
		switch column {
		case "TABLE_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "NAME":
			return informationSchemaExactVirtualSpec("VARCHAR", 64, true, false, "utf8mb3", "utf8mb3_general_ci", int64(21), nil, nil, ""), true
		case "N_COLS":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "SPACE":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		}
	case "innodb_trx":
		switch column {
		case "TRX_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TRX_STATE":
			return informationSchemaExactVirtualSpec("VARCHAR", 13, false, false, "utf8mb3", "utf8mb3_general_ci", int64(4), nil, nil, ""), true
		case "TRX_STARTED":
			return informationSchemaExactVirtualSpec("DATETIME", 0, false, false, "", "", nil, nil, nil, ""), true
		case "TRX_REQUESTED_LOCK_ID":
			return informationSchemaExactVirtualSpec("VARCHAR", 126, true, false, "utf8mb3", "utf8mb3_general_ci", int64(42), nil, nil, ""), true
		case "TRX_WAIT_STARTED":
			return informationSchemaExactVirtualSpec("DATETIME", 0, true, false, "", "", nil, nil, nil, ""), true
		case "TRX_WEIGHT":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TRX_MYSQL_THREAD_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TRX_QUERY":
			return informationSchemaExactVirtualSpec("VARCHAR", 1024, true, false, "utf8mb3", "utf8mb3_general_ci", int64(341), nil, nil, ""), true
		case "TRX_OPERATION_STATE":
			return informationSchemaExactVirtualSpec("VARCHAR", 64, true, false, "utf8mb3", "utf8mb3_general_ci", int64(21), nil, nil, ""), true
		case "TRX_TABLES_IN_USE":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TRX_TABLES_LOCKED":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TRX_LOCK_STRUCTS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TRX_LOCK_MEMORY_BYTES":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TRX_ROWS_LOCKED":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TRX_ROWS_MODIFIED":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TRX_CONCURRENCY_TICKETS":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TRX_ISOLATION_LEVEL":
			return informationSchemaExactVirtualSpec("VARCHAR", 16, false, false, "utf8mb3", "utf8mb3_general_ci", int64(5), nil, nil, ""), true
		case "TRX_UNIQUE_CHECKS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "TRX_FOREIGN_KEY_CHECKS":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "TRX_LAST_FOREIGN_KEY_ERROR":
			return informationSchemaExactVirtualSpec("VARCHAR", 256, true, false, "utf8mb3", "utf8mb3_general_ci", int64(85), nil, nil, ""), true
		case "TRX_ADAPTIVE_HASH_LATCHED":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "TRX_ADAPTIVE_HASH_TIMEOUT":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "TRX_IS_READ_ONLY":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "TRX_AUTOCOMMIT_NON_LOCKING":
			return informationSchemaExactVirtualSpec("INT", 0, false, false, "", "", nil, nil, nil, ""), true
		case "TRX_SCHEDULE_WEIGHT":
			return informationSchemaExactVirtualSpec("BIGINT", 0, true, true, "", "", nil, nil, nil, ""), true
		}
	case "innodb_virtual":
		switch column {
		case "TABLE_ID":
			return informationSchemaExactVirtualSpec("BIGINT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "POS":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		case "BASE_POS":
			return informationSchemaExactVirtualSpec("INT", 0, false, true, "", "", nil, nil, nil, ""), true
		}
	}
	return performanceSchemaVirtualColumnMetadataSpec{}, false
}

type performanceSchemaVirtualColumnMetadataSpec struct {
	typeName                  string
	length                    int
	scale                     int
	unsigned                  bool
	nullable                  bool
	charset                   string
	collation                 string
	characterMaximumLength    int
	characterMaximumLengthSet bool
	characterOctetLength      int
	characterOctetLengthSet   bool
	numericPrecision          interface{}
	numericPrecisionSet       bool
	numericScale              interface{}
	numericScaleSet           bool
	datetimePrecision         interface{}
	datetimePrecisionSet      bool
	defaultValue              interface{}
}

func performanceSchemaVirtualColumnSpecFor(typeName string, length int, nullable, unsigned bool) performanceSchemaVirtualColumnMetadataSpec {
	return performanceSchemaVirtualColumnMetadataSpec{
		typeName: typeName,
		length:   length,
		nullable: nullable,
		unsigned: unsigned,
	}
}

func normalizePerformanceSchemaVirtualColumnMetadataSpec(spec performanceSchemaVirtualColumnMetadataSpec) performanceSchemaVirtualColumnMetadataSpec {
	baseType := strings.ToUpper(strings.TrimSpace(strings.SplitN(spec.typeName, "(", 2)[0]))
	if baseType == "ENUM" || baseType == "SET" {
		if spec.length <= 0 {
			spec.length = performanceSchemaVirtualStringCollectionLength(spec.typeName, baseType)
		}
		spec.charset = "utf8mb4"
	}
	if baseType == "TINYTEXT" && spec.length <= 0 {
		spec.length = 255
		if spec.charset == "" {
			spec.charset = "utf8mb4"
		}
	}
	if baseType == "TEXT" && spec.length <= 0 {
		spec.length = 65535
		if spec.charset == "" {
			spec.charset = "utf8mb4"
		}
	}
	if baseType == "MEDIUMTEXT" && spec.length <= 0 {
		spec.length = 16777215
		if spec.charset == "" {
			spec.charset = "utf8mb4"
		}
	}
	if baseType == "LONGTEXT" && spec.length <= 0 {
		spec.length = 4294967295
		if spec.charset == "" {
			spec.charset = "utf8mb4"
		}
	}
	return spec
}

func performanceSchemaVirtualStringCollectionLength(typeName, baseType string) int {
	open := strings.Index(typeName, "(")
	close := strings.LastIndex(typeName, ")")
	if open < 0 || close <= open || !strings.EqualFold(strings.TrimSpace(strings.SplitN(typeName, "(", 2)[0]), baseType) {
		return 0
	}
	parts := splitTopLevelComma(typeName[open+1 : close])
	if len(parts) == 0 {
		return 0
	}
	length := 0
	for _, part := range parts {
		value := strings.TrimSpace(part)
		value = strings.Trim(value, "'\"")
		if baseType == "SET" {
			length += len(value)
		} else if len(value) > length {
			length = len(value)
		}
	}
	if baseType == "SET" {
		length += len(parts) - 1
	}
	return length
}

func performanceSchemaVirtualDecimalSpec(precision, scale int, nullable bool) performanceSchemaVirtualColumnMetadataSpec {
	return performanceSchemaVirtualColumnMetadataSpec{
		typeName: "DECIMAL",
		length:   precision,
		scale:    scale,
		nullable: nullable,
	}
}

func performanceSchemaVirtualCharacterSpec(typeName string, length int, nullable bool) performanceSchemaVirtualColumnMetadataSpec {
	return performanceSchemaVirtualCharacterSpecWithCharset(typeName, length, nullable, "utf8mb4")
}

func performanceSchemaVirtualBinaryCharacterSpec(typeName string, length int, nullable bool) performanceSchemaVirtualColumnMetadataSpec {
	return performanceSchemaVirtualColumnMetadataSpec{
		typeName:  typeName,
		length:    length,
		nullable:  nullable,
		charset:   "utf8mb4",
		collation: "utf8mb4_bin",
	}
}

func performanceSchemaVirtualBinaryTextSpec(typeName string, nullable bool) performanceSchemaVirtualColumnMetadataSpec {
	return performanceSchemaVirtualColumnMetadataSpec{
		typeName:  typeName,
		nullable:  nullable,
		charset:   "utf8mb3",
		collation: "utf8mb3_bin",
	}
}

func performanceSchemaVirtualCharacterSpecWithCharset(typeName string, length int, nullable bool, charset string) performanceSchemaVirtualColumnMetadataSpec {
	return performanceSchemaVirtualColumnMetadataSpec{
		typeName: typeName,
		length:   length,
		nullable: nullable,
		charset:  charset,
	}
}

func performanceSchemaVirtualColumnSpecForColumn(tableName, columnName string) (performanceSchemaVirtualColumnMetadataSpec, bool) {
	table := strings.ToLower(strings.TrimSpace(tableName))
	column := strings.ToUpper(strings.TrimSpace(columnName))
	if table == "events_statements_current" || table == "events_statements_history" || table == "events_statements_history_long" {
		switch column {
		case "THREAD_ID", "EVENT_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "END_EVENT_ID", "TIMER_START", "TIMER_END", "TIMER_WAIT", "OBJECT_INSTANCE_BEGIN", "NESTING_EVENT_ID", "STATEMENT_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "EVENT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "SOURCE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "LOCK_TIME", "ERRORS", "WARNINGS", "ROWS_AFFECTED", "ROWS_SENT", "ROWS_EXAMINED", "CREATED_TMP_DISK_TABLES", "CREATED_TMP_TABLES", "SELECT_FULL_JOIN", "SELECT_FULL_RANGE_JOIN", "SELECT_RANGE", "SELECT_RANGE_CHECK", "SELECT_SCAN", "SORT_MERGE_PASSES", "SORT_RANGE", "SORT_ROWS", "SORT_SCAN", "NO_INDEX_USED", "NO_GOOD_INDEX_USED", "CPU_TIME", "MAX_CONTROLLED_MEMORY", "MAX_TOTAL_MEMORY":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "SQL_TEXT", "DIGEST_TEXT":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		case "DIGEST":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "CURRENT_SCHEMA", "OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "MYSQL_ERRNO", "NESTING_EVENT_LEVEL":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, false), true
		case "RETURNED_SQLSTATE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 5, true), true
		case "MESSAGE_TEXT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, true), true
		case "NESTING_EVENT_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')", 0, true, false), true
		case "EXECUTION_ENGINE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('PRIMARY','SECONDARY')", 0, true, false), true
		}
	}
	if table == "events_transactions_current" || table == "events_transactions_history" || table == "events_transactions_history_long" {
		switch column {
		case "THREAD_ID", "EVENT_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "END_EVENT_ID", "TRX_ID", "TIMER_START", "TIMER_END", "TIMER_WAIT", "NUMBER_OF_SAVEPOINTS", "NUMBER_OF_ROLLBACK_TO_SAVEPOINT", "NUMBER_OF_RELEASE_SAVEPOINT", "OBJECT_INSTANCE_BEGIN", "NESTING_EVENT_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "EVENT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "STATE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('ACTIVE','COMMITTED','ROLLED BACK')", 0, true, false), true
		case "GTID":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 90, true), true
		case "XID_FORMAT_ID":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, false), true
		case "XID_GTRID", "XID_BQUAL":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 130, true), true
		case "XA_STATE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "SOURCE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "ACCESS_MODE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('READ ONLY','READ WRITE')", 0, true, false), true
		case "ISOLATION_LEVEL":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "AUTOCOMMIT":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, false, false), true
		case "NESTING_EVENT_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')", 0, true, false), true
		}
	}
	if table == "events_waits_current" || table == "events_waits_history" || table == "events_waits_history_long" {
		switch column {
		case "THREAD_ID", "EVENT_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "END_EVENT_ID", "TIMER_START", "TIMER_END", "TIMER_WAIT", "NESTING_EVENT_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "SPINS", "FLAGS":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, true), true
		case "NUMBER_OF_BYTES":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, false), true
		case "OBJECT_INSTANCE_BEGIN":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "NESTING_EVENT_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')", 0, true, false), true
		case "EVENT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "SOURCE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "OBJECT_SCHEMA", "INDEX_NAME", "OBJECT_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "OBJECT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 512, true), true
		case "OPERATION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 32, false), true
		}
	}
	if table == "events_stages_current" || table == "events_stages_history" || table == "events_stages_history_long" {
		switch column {
		case "THREAD_ID", "EVENT_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "END_EVENT_ID", "TIMER_START", "TIMER_END", "TIMER_WAIT", "WORK_COMPLETED", "WORK_ESTIMATED", "NESTING_EVENT_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "EVENT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "SOURCE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "NESTING_EVENT_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')", 0, true, false), true
		}
	}
	if table == "binary_log_transaction_compression_stats" {
		switch column {
		case "LOG_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('BINARY','RELAY')", 0, false, false), true
		case "COMPRESSION_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "TRANSACTION_COUNTER", "COMPRESSED_BYTES_COUNTER", "UNCOMPRESSED_BYTES_COUNTER", "FIRST_TRANSACTION_COMPRESSED_BYTES", "FIRST_TRANSACTION_UNCOMPRESSED_BYTES", "LAST_TRANSACTION_COMPRESSED_BYTES", "LAST_TRANSACTION_UNCOMPRESSED_BYTES":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "COMPRESSION_PERCENTAGE":
			return performanceSchemaVirtualColumnSpecFor("SMALLINT", 0, false, false), true
		case "FIRST_TRANSACTION_ID", "LAST_TRANSACTION_ID":
			return performanceSchemaVirtualColumnSpecFor("TEXT", 0, true, false), true
		case "FIRST_TRANSACTION_TIMESTAMP", "LAST_TRANSACTION_TIMESTAMP":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 6, true, false), true
		}
	}
	if table == "log_status" {
		switch column {
		case "SERVER_UUID":
			return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 36, false), true
		case "LOCAL", "REPLICATION", "STORAGE_ENGINES":
			return performanceSchemaVirtualColumnSpecFor("JSON", 0, false, false), true
		}
	}
	if table == "error_log" {
		switch column {
		case "LOGGED":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 6, false, false), true
		case "THREAD_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "PRIO":
			return performanceSchemaVirtualColumnSpecFor("ENUM('System','Error','Warning','Note')", 0, false, false), true
		case "ERROR_CODE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 10, true), true
		case "SUBSYSTEM":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 7, true), true
		case "DATA":
			return performanceSchemaVirtualColumnSpecFor("TEXT", 0, false, false), true
		}
	}
	if table == "host_cache" {
		switch column {
		case "IP":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "HOST":
			return performanceSchemaVirtualCharacterSpecWithCharset("VARCHAR", 255, true, "ascii"), true
		case "HOST_VALIDATED":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, false, false), true
		case "SUM_CONNECT_ERRORS", "COUNT_HOST_BLOCKED_ERRORS", "COUNT_NAMEINFO_TRANSIENT_ERRORS", "COUNT_NAMEINFO_PERMANENT_ERRORS", "COUNT_FORMAT_ERRORS", "COUNT_ADDRINFO_TRANSIENT_ERRORS", "COUNT_ADDRINFO_PERMANENT_ERRORS", "COUNT_FCRDNS_ERRORS", "COUNT_HOST_ACL_ERRORS", "COUNT_NO_AUTH_PLUGIN_ERRORS", "COUNT_AUTH_PLUGIN_ERRORS", "COUNT_HANDSHAKE_ERRORS", "COUNT_PROXY_USER_ERRORS", "COUNT_PROXY_USER_ACL_ERRORS", "COUNT_AUTHENTICATION_ERRORS", "COUNT_SSL_ERRORS", "COUNT_MAX_USER_CONNECTIONS_ERRORS", "COUNT_MAX_USER_CONNECTIONS_PER_HOUR_ERRORS", "COUNT_DEFAULT_DATABASE_ERRORS", "COUNT_INIT_CONNECT_ERRORS", "COUNT_LOCAL_ERRORS", "COUNT_UNKNOWN_ERRORS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, false), true
		case "FIRST_SEEN", "LAST_SEEN":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 0, false, false), true
		case "FIRST_ERROR_SEEN", "LAST_ERROR_SEEN":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 0, true, false), true
		}
	}
	if table == "performance_timers" {
		switch column {
		case "TIMER_NAME":
			return performanceSchemaVirtualColumnSpecFor("ENUM('CYCLE','NANOSECOND','MICROSECOND','MILLISECOND','THREAD_CPU')", 0, false, false), true
		case "TIMER_FREQUENCY", "TIMER_RESOLUTION", "TIMER_OVERHEAD":
			// MySQL exposes unsupported platform timers as NULL and keeps the
			// numeric columns signed BIGINT in the plugin table definition.
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, false), true
		}
	}
	if table == "session_connect_attrs" || table == "session_account_connect_attrs" {
		switch column {
		case "PROCESSLIST_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "ATTR_NAME":
			return performanceSchemaVirtualBinaryCharacterSpec("VARCHAR", 32, false), true
		case "ATTR_VALUE":
			return performanceSchemaVirtualBinaryCharacterSpec("VARCHAR", 1024, true), true
		case "ORDINAL_POSITION":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, false), true
		}
	}
	if table == "user_variables_by_thread" {
		switch column {
		case "THREAD_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "VARIABLE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "VARIABLE_VALUE":
			return performanceSchemaVirtualColumnSpecFor("LONGBLOB", 0, true, false), true
		}
	}
	if table == "variables_by_thread" || table == "status_by_thread" {
		switch column {
		case "THREAD_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "VARIABLE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "VARIABLE_VALUE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, true), true
		}
	}
	if strings.HasPrefix(table, "memory_summary_") && strings.HasSuffix(table, "_by_event_name") {
		switch column {
		case "USER":
			if table == "memory_summary_by_account_by_event_name" || table == "memory_summary_by_user_by_event_name" {
				return performanceSchemaVirtualColumnMetadataSpec{typeName: "CHAR", length: 32, nullable: true, charset: "utf8mb4", collation: "utf8mb4_bin"}, true
			}
		case "HOST":
			if table == "memory_summary_by_account_by_event_name" || table == "memory_summary_by_host_by_event_name" {
				return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, true, "ascii"), true
			}
		case "THREAD_ID":
			if table == "memory_summary_by_thread_by_event_name" {
				return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
			}
		case "EVENT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "COUNT_ALLOC", "COUNT_FREE", "SUM_NUMBER_OF_BYTES_ALLOC", "SUM_NUMBER_OF_BYTES_FREE":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "LOW_COUNT_USED", "CURRENT_COUNT_USED", "HIGH_COUNT_USED", "LOW_NUMBER_OF_BYTES_USED", "CURRENT_NUMBER_OF_BYTES_USED", "HIGH_NUMBER_OF_BYTES_USED":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, false), true
		}
	}
	if strings.HasPrefix(table, "events_errors_summary_") {
		switch column {
		case "USER":
			return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 32, true), true
		case "HOST":
			return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, true, "ascii"), true
		case "ERROR_NUMBER":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, false), true
		case "ERROR_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "SQL_STATE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 5, true), true
		case "SUM_ERROR_RAISED", "SUM_ERROR_HANDLED":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "FIRST_SEEN", "LAST_SEEN":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 0, true, false), true
		}
	}
	if table == "events_statements_summary_by_program" {
		switch column {
		case "OBJECT_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('EVENT','FUNCTION','PROCEDURE','TABLE','TRIGGER')", 0, false, false), true
		case "OBJECT_SCHEMA", "OBJECT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		}
	}
	if table == "data_locks" || table == "data_lock_waits" {
		switch column {
		case "ENGINE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 32, false), true
		case "ENGINE_LOCK_ID", "REQUESTING_ENGINE_LOCK_ID", "BLOCKING_ENGINE_LOCK_ID":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "ENGINE_TRANSACTION_ID", "REQUESTING_ENGINE_TRANSACTION_ID", "BLOCKING_ENGINE_TRANSACTION_ID",
			"THREAD_ID", "REQUESTING_THREAD_ID", "BLOCKING_THREAD_ID", "EVENT_ID", "REQUESTING_EVENT_ID", "BLOCKING_EVENT_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "OBJECT_INSTANCE_BEGIN", "REQUESTING_OBJECT_INSTANCE_BEGIN", "BLOCKING_OBJECT_INSTANCE_BEGIN":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "OBJECT_SCHEMA", "OBJECT_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "INDEX_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "LOCK_TYPE", "LOCK_MODE", "LOCK_STATUS":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 32, false), true
		case "LOCK_DATA":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 8192, true), true
		}
	}
	if table == "metadata_locks" {
		switch column {
		case "OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COLUMN_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, column != "OBJECT_TYPE"), true
		case "OBJECT_INSTANCE_BEGIN":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "LOCK_TYPE", "LOCK_DURATION", "LOCK_STATUS":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 32, false), true
		case "SOURCE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "OWNER_THREAD_ID", "OWNER_EVENT_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		}
	}
	if table == "objects_summary_global_by_type" || table == "table_lock_waits_summary_by_table" {
		if column == "OBJECT_TYPE" {
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		}
	}
	if table == "innodb_redo_log_files" {
		switch column {
		case "FILE_ID", "START_LSN", "END_LSN", "SIZE_IN_BYTES":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, false), true
		case "FILE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 2000, false), true
		case "IS_FULL":
			return performanceSchemaVirtualColumnSpecFor("TINYINT", 0, false, false), true
		case "CONSUMER_LEVEL":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		}
	}
	if table == "component_scheduler_tasks" {
		switch column {
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "STATUS":
			return performanceSchemaVirtualColumnSpecFor("ENUM('RUNNING','WAITING')", 0, false, false), true
		case "COMMENT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, false), true
		case "INTERVAL_SECONDS":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "TIMES_RUN", "TIMES_FAILED":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}

	// Every Performance Schema event/summary table uses this fixed instrument
	// name contract.  It is not the generic 255-character fallback.
	if column == "EVENT_NAME" {
		return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
	}

	if table == "accounts" || table == "hosts" || table == "users" {
		switch column {
		case "USER":
			if table == "hosts" {
				return performanceSchemaVirtualColumnMetadataSpec{}, false
			}
			return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 32, true), true
		case "HOST":
			if table == "users" {
				return performanceSchemaVirtualColumnMetadataSpec{}, false
			}
			return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, true, "ascii"), true
		case "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, false), true
		case "MAX_SESSION_CONTROLLED_MEMORY", "MAX_SESSION_TOTAL_MEMORY":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}
	if table == "status_by_account" || table == "status_by_host" || table == "status_by_user" {
		switch column {
		case "USER":
			if table == "status_by_account" || table == "status_by_user" {
				return performanceSchemaVirtualColumnMetadataSpec{typeName: "CHAR", length: 32, nullable: true, charset: "utf8mb4", collation: "utf8mb4_bin"}, true
			}
		case "HOST":
			if table == "status_by_account" || table == "status_by_host" {
				return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, true, "ascii"), true
			}
		case "VARIABLE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "VARIABLE_VALUE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, true), true
		}
	}
	if table == "session_status" || table == "session_variables" || table == "global_status" || table == "global_variables" || table == "persisted_variables" {
		switch column {
		case "VARIABLE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "VARIABLE_VALUE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, true), true
		case "SET_TIME":
			if table == "persisted_variables" {
				return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 6, true, false), true
			}
		case "SET_USER":
			if table == "persisted_variables" {
				return performanceSchemaVirtualColumnMetadataSpec{typeName: "CHAR", length: 32, nullable: true, charset: "utf8mb4", collation: "utf8mb4_bin"}, true
			}
		case "SET_HOST":
			if table == "persisted_variables" {
				return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, true, "ascii"), true
			}
		}
	}
	if table == "variables_info" {
		switch column {
		case "VARIABLE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "VARIABLE_SOURCE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('COMPILED','GLOBAL','SERVER','EXPLICIT','EXTRA','USER','LOGIN','COMMAND_LINE','PERSISTED','DYNAMIC')", 0, true, false), true
		case "VARIABLE_PATH":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, true), true
		case "MIN_VALUE", "MAX_VALUE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "SET_TIME":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 6, true, false), true
		case "SET_USER":
			return performanceSchemaVirtualColumnMetadataSpec{typeName: "CHAR", length: 32, nullable: true, charset: "utf8mb4", collation: "utf8mb4_bin"}, true
		case "SET_HOST":
			return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, true, "ascii"), true
		}
	}
	if table == "table_io_waits_summary_by_table" || table == "table_io_waits_summary_by_index_usage" {
		switch column {
		case "OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "INDEX_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		}
	}
	if table == "file_summary_by_event_name" || table == "file_summary_by_instance" {
		switch column {
		case "FILE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 512, false), true
		case "EVENT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "OBJECT_INSTANCE_BEGIN":
			if table == "file_summary_by_instance" {
				return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
			}
		case "SUM_NUMBER_OF_BYTES_READ", "SUM_NUMBER_OF_BYTES_WRITE":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, false), true
		}
	}
	if table == "socket_summary_by_event_name" || table == "socket_summary_by_instance" {
		switch column {
		case "EVENT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "OBJECT_INSTANCE_BEGIN":
			if table == "socket_summary_by_instance" {
				return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
			}
		}
	}
	if table == "table_handles" {
		switch column {
		case "OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "OBJECT_INSTANCE_BEGIN":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "OWNER_THREAD_ID", "OWNER_EVENT_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "INTERNAL_LOCK", "EXTERNAL_LOCK":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		}
	}
	if table == "prepared_statements_instances" {
		switch column {
		case "OBJECT_INSTANCE_BEGIN", "STATEMENT_ID", "OWNER_THREAD_ID", "OWNER_EVENT_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "STATEMENT_NAME", "OWNER_OBJECT_SCHEMA", "OWNER_OBJECT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "SQL_TEXT":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, false, false), true
		case "OWNER_OBJECT_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('EVENT','FUNCTION','PROCEDURE','TABLE','TRIGGER')", 0, true, false), true
		case "EXECUTION_ENGINE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('PRIMARY','SECONDARY')", 0, true, false), true
		}
	}

	// The Performance Schema thread-pool tables use the same native contracts
	// as their INFORMATION_SCHEMA compatibility views.
	if table == "tp_thread_group_state" {
		switch column {
		case "TP_GROUP_ID", "CONSUMER_THREADS", "RESERVE_THREADS", "CONNECT_THREAD_COUNT", "CONNECTION_COUNT", "QUEUED_QUERIES", "QUEUED_TRANSACTIONS", "STALL_LIMIT", "PRIO_KICKUP_TIMER", "THREAD_COUNT", "ACTIVE_THREAD_COUNT", "STALLED_THREAD_COUNT", "MAX_THREAD_IDS_IN_GROUP", "NUM_CONNECT_HANDLER_THREAD_IN_SLEEP", "THREADS_BOUND_TO_TRANSACTION", "QUERY_THREADS_COUNT":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		case "ALGORITHM":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 20, false), true
		case "WAITING_THREAD_NUMBER", "EFFECTIVE_MAX_TRANSACTIONS_LIMIT":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, true), true
		case "OLDEST_QUEUED", "TIME_OF_LAST_THREAD_CREATION", "TIME_OF_EARLIEST_CON_EXPIRE":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "NUM_QUERY_THREADS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}
	if table == "tp_thread_group_stats" {
		switch column {
		case "TP_GROUP_ID":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		case "CONNECTIONS_STARTED", "CONNECTIONS_CLOSED", "QUERIES_EXECUTED", "QUERIES_QUEUED", "THREADS_STARTED", "PRIO_KICKUPS", "STALLED_QUERIES_EXECUTED", "BECOME_CONSUMER_THREAD", "BECOME_RESERVE_THREAD", "BECOME_WAITING_THREAD", "WAKE_THREAD_STALL_CHECKER", "SLEEP_WAITS", "DISK_IO_WAITS", "ROW_LOCK_WAITS", "GLOBAL_LOCK_WAITS", "META_DATA_LOCK_WAITS", "TABLE_LOCK_WAITS", "USER_LOCK_WAITS", "BINLOG_WAITS", "GROUP_COMMIT_WAITS", "FSYNC_WAITS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}
	if table == "tp_thread_state" {
		switch column {
		case "TP_GROUP_ID", "TP_THREAD_NUMBER":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		case "PROCESS_COUNT":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "WAIT_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 30, true), true
		case "TP_THREAD_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 32, false), true
		case "THREAD_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		}
	}

	// Keep the thread/process-list views aligned with the native MySQL 8.4
	// Performance Schema plugin-table definitions.  These tables are queried
	// directly by Connector/J and by common monitoring clients, so the generic
	// identifier/counter fallback is not precise enough here.
	if table == "threads" {
		switch column {
		case "THREAD_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 10, false), true
		case "PROCESSLIST_ID", "PARENT_THREAD_ID", "THREAD_OS_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "PROCESSLIST_USER":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 32, true), true
		case "PROCESSLIST_HOST":
			return performanceSchemaVirtualCharacterSpecWithCharset("VARCHAR", 255, true, "ascii"), true
		case "PROCESSLIST_DB":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "PROCESSLIST_COMMAND":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 16, true), true
		case "PROCESSLIST_TIME":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, false), true
		case "PROCESSLIST_STATE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "PROCESSLIST_INFO":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		case "ROLE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "INSTRUMENTED", "HISTORY":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, false, false), true
		case "CONNECTION_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 16, true), true
		case "RESOURCE_GROUP":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "EXECUTION_ENGINE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('PRIMARY','SECONDARY')", 0, true, false), true
		case "CONTROLLED_MEMORY", "MAX_CONTROLLED_MEMORY", "TOTAL_MEMORY", "MAX_TOTAL_MEMORY":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "TELEMETRY_ACTIVE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, false, false), true
		}
	}

	if table == "processlist" {
		switch column {
		case "ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "USER":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 32, true), true
		case "HOST":
			return performanceSchemaVirtualCharacterSpecWithCharset("VARCHAR", 261, true, "ascii"), true
		case "DB":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "COMMAND":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 16, true), true
		case "TIME":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, false), true
		case "TIME_MS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, false), true
		case "STATE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "INFO":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		case "EXECUTION_ENGINE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('PRIMARY','SECONDARY')", 0, true, false), true
		}
	}

	if table == "setup_consumers" {
		switch column {
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "ENABLED":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, false, false), true
		}
	}

	if table == "setup_loggers" {
		switch column {
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "LEVEL":
			return performanceSchemaVirtualColumnSpecFor("ENUM('none','error','warn','info','debug')", 0, false, false), true
		case "DESCRIPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1023, true), true
		}
	}

	if table == "tls_channel_status" {
		switch column {
		case "CHANNEL", "PROPERTY":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "VALUE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 2048, false), true
		}
	}

	if table == "clone_status" {
		switch column {
		case "ID", "PID", "ERROR_NO":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, false), true
		case "STATE":
			return performanceSchemaVirtualCharacterSpec("CHAR", 16, true), true
		case "BEGIN_TIME", "END_TIME":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 3, true, false), true
		case "SOURCE", "DESTINATION", "ERROR_MESSAGE", "BINLOG_FILE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 512, true), true
		case "BINLOG_POSITION":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, false), true
		case "GTID_EXECUTED":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		}
	}

	if table == "clone_progress" {
		switch column {
		case "ID", "THREADS", "DATA_SPEED", "NETWORK_SPEED":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, false), true
		case "STAGE":
			return performanceSchemaVirtualCharacterSpec("CHAR", 32, true), true
		case "STATE":
			return performanceSchemaVirtualCharacterSpec("CHAR", 16, true), true
		case "BEGIN_TIME", "END_TIME":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 6, true, false), true
		case "ESTIMATE", "DATA", "NETWORK":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, false), true
		}
	}

	if table == "user_defined_functions" {
		switch column {
		case "UDF_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "UDF_RETURN_TYPE", "UDF_TYPE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 20, false), true
		case "UDF_LIBRARY":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, true), true
		case "UDF_USAGE_COUNT":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, false), true
		}
	}

	if table == "keyring_component_status" {
		switch column {
		case "STATUS_KEY":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 256, false), true
		case "STATUS_VALUE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, false), true
		}
	}

	if table == "keyring_keys" {
		switch column {
		case "KEY_ID":
			return performanceSchemaVirtualBinaryCharacterSpec("VARCHAR", 255, false), true
		case "KEY_OWNER", "BACKEND_KEY_ID":
			return performanceSchemaVirtualBinaryCharacterSpec("VARCHAR", 255, true), true
		}
	}

	if table == "setup_meters" {
		switch column {
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 63, false), true
		case "FREQUENCY":
			return performanceSchemaVirtualColumnSpecFor("MEDIUMINT", 0, false, true), true
		case "ENABLED":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, false, false), true
		case "DESCRIPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1023, true), true
		}
	}

	if table == "setup_metrics" {
		switch column {
		case "NAME", "METER":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 63, false), true
		case "METRIC_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('ASYNC COUNTER','ASYNC UPDOWN COUNTER','ASYNC GAUGE COUNTER')", 0, false, false), true
		case "NUM_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('INTEGER','DOUBLE')", 0, false, false), true
		case "UNIT":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 63, true), true
		case "DESCRIPTION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1023, true), true
		}
	}

	if table == "setup_instruments" {
		switch column {
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "ENABLED":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, false, false), true
		case "TIMED":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, true, false), true
		case "PROPERTIES":
			return performanceSchemaVirtualColumnSpecFor("SET('singleton','progress','user','global_statistics','mutable','controlled_by_default')", 0, false, false), true
		case "FLAGS":
			return performanceSchemaVirtualColumnSpecFor("SET('controlled')", 0, true, false), true
		case "VOLATILITY":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "DOCUMENTATION":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		}
	}

	if table == "setup_actors" {
		switch column {
		case "HOST":
			return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, false, "ascii"), true
		case "USER", "ROLE":
			return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 32, false), true
		case "ENABLED", "HISTORY":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, false, false), true
		}
	}

	if table == "setup_objects" {
		switch column {
		case "OBJECT_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('EVENT','FUNCTION','PROCEDURE','TABLE','TRIGGER')", 0, false, false), true
		case "OBJECT_SCHEMA":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "OBJECT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "ENABLED", "TIMED":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, false, false), true
		}
	}

	if table == "setup_threads" {
		switch column {
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "ENABLED", "HISTORY":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, false, false), true
		case "PROPERTIES":
			return performanceSchemaVirtualColumnSpecFor("SET('singleton','user')", 0, false, false), true
		case "VOLATILITY":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "DOCUMENTATION":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
		}
	}

	if table == "file_instances" {
		switch column {
		case "FILE_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 512, false), true
		case "EVENT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "OPEN_COUNT":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		}
	}

	if table == "socket_instances" {
		switch column {
		case "EVENT_NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "OBJECT_INSTANCE_BEGIN":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "THREAD_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "SOCKET_ID", "PORT":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "IP":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "STATE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('IDLE','ACTIVE')", 0, false, false), true
		}
	}

	if table == "cond_instances" || table == "mutex_instances" || table == "rwlock_instances" {
		switch column {
		case "NAME":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 128, false), true
		case "OBJECT_INSTANCE_BEGIN":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "LOCKED_BY_THREAD_ID", "WRITE_LOCKED_BY_THREAD_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "READ_LOCKED_BY_COUNT":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		}
	}

	if table == "replication_applier_status" {
		switch column {
		case "CHANNEL_NAME":
			return performanceSchemaVirtualCharacterSpec("CHAR", 64, false), true
		case "SERVICE_STATE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('ON','OFF')", 0, false, false), true
		case "REMAINING_DELAY":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, true), true
		case "COUNT_TRANSACTIONS_RETRIES":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}
	if table == "replication_applier_filters" {
		switch column {
		case "CHANNEL_NAME", "FILTER_NAME":
			return performanceSchemaVirtualCharacterSpec("CHAR", 64, false), true
		case "FILTER_RULE":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, false, false), true
		case "CONFIGURED_BY":
			return performanceSchemaVirtualColumnSpecFor("ENUM('STARTUP_OPTIONS','CHANGE_REPLICATION_FILTER','STARTUP_OPTIONS_FOR_CHANNEL','CHANGE_REPLICATION_FILTER_FOR_CHANNEL')", 0, false, false), true
		case "ACTIVE_SINCE":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 6, false, false), true
		case "COUNTER":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}
	if table == "replication_applier_global_filters" {
		switch column {
		case "FILTER_NAME":
			return performanceSchemaVirtualCharacterSpec("CHAR", 64, false), true
		case "FILTER_RULE":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, false, false), true
		case "CONFIGURED_BY":
			return performanceSchemaVirtualColumnSpecFor("ENUM('STARTUP_OPTIONS','CHANGE_REPLICATION_FILTER')", 0, false, false), true
		case "ACTIVE_SINCE":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 6, false, false), true
		}
	}
	if table == "replication_asynchronous_connection_failover" {
		switch column {
		case "CHANNEL_NAME", "MANAGED_NAME":
			return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 64, false, "utf8mb3"), true
		case "NETWORK_NAMESPACE":
			return performanceSchemaVirtualCharacterSpec("CHAR", 64, true), true
		case "HOST":
			return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, false, "ascii"), true
		case "PORT":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "WEIGHT":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		}
	}
	if table == "replication_asynchronous_connection_failover_managed" {
		switch column {
		case "CHANNEL_NAME", "MANAGED_NAME", "MANAGED_TYPE":
			return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 64, false, "utf8mb3"), true
		case "CONFIGURATION":
			return performanceSchemaVirtualColumnSpecFor("JSON", 0, true, false), true
		}
	}
	if table == "replication_group_member_stats" {
		switch column {
		case "CHANNEL_NAME":
			return performanceSchemaVirtualCharacterSpec("CHAR", 64, false), true
		case "VIEW_ID":
			return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 60, false), true
		case "MEMBER_ID":
			return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 36, false), true
		case "COUNT_TRANSACTIONS_IN_QUEUE", "COUNT_TRANSACTIONS_CHECKED", "COUNT_CONFLICTS_DETECTED", "COUNT_TRANSACTIONS_ROWS_VALIDATING", "COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE", "COUNT_TRANSACTIONS_REMOTE_APPLIED", "COUNT_TRANSACTIONS_LOCAL_PROPOSED", "COUNT_TRANSACTIONS_LOCAL_ROLLBACK":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "TRANSACTIONS_COMMITTED_ALL_MEMBERS":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, false, false), true
		case "LAST_CONFLICT_FREE_TRANSACTION":
			return performanceSchemaVirtualColumnSpecFor("TEXT", 0, false, false), true
		}
	}
	if table == "replication_group_members" {
		switch column {
		case "CHANNEL_NAME":
			return performanceSchemaVirtualCharacterSpec("CHAR", 64, false), true
		case "MEMBER_ID":
			return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 36, false), true
		case "MEMBER_HOST":
			return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, false, "ascii"), true
		case "MEMBER_PORT":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, true, false), true
		case "MEMBER_STATE", "MEMBER_ROLE", "MEMBER_VERSION", "MEMBER_COMMUNICATION_STACK":
			return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 64, false), true
		}
	}
	if table == "replication_group_communication_information" {
		switch column {
		case "WRITE_CONCURRENCY":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "PROTOCOL_VERSION", "WRITE_CONSENSUS_LEADERS_PREFERRED", "WRITE_CONSENSUS_LEADERS_ACTUAL", "MEMBER_FAILURE_SUSPICIONS_COUNT":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, false, false), true
		case "WRITE_CONSENSUS_SINGLE_LEADER_CAPABLE":
			return performanceSchemaVirtualColumnSpecFor("TINYINT(1)", 0, false, false), true
		}
	}
	if table == "replication_group_configuration_version" {
		switch column {
		case "NAME":
			return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, false, "ascii"), true
		case "VERSION":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}
	if table == "replication_group_member_actions" {
		switch column {
		case "NAME":
			return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, false, "ascii"), true
		case "EVENT", "TYPE", "ERROR_HANDLING":
			return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 64, false, "ascii"), true
		case "ENABLED":
			return performanceSchemaVirtualColumnSpecFor("TINYINT(1)", 0, false, false), true
		case "PRIORITY":
			return performanceSchemaVirtualColumnSpecFor("TINYINT", 0, false, true), true
		}
	}

	if table == "replication_applier_configuration" {
		switch column {
		case "CHANNEL_NAME":
			return performanceSchemaVirtualCharacterSpec("CHAR", 64, false), true
		case "DESIRED_DELAY":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "PRIVILEGE_CHECKS_USER":
			return performanceSchemaVirtualBinaryTextSpec("TEXT", true), true
		case "REQUIRE_ROW_FORMAT":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, false, false), true
		case "REQUIRE_TABLE_PRIMARY_KEY_CHECK":
			return performanceSchemaVirtualColumnSpecFor("ENUM('STREAM','ON','OFF','GENERATE')", 0, false, false), true
		case "ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS_TYPE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('OFF','LOCAL','UUID')", 0, false, false), true
		case "ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS_VALUE":
			return performanceSchemaVirtualBinaryTextSpec("TEXT", true), true
		}
	}

	if table == "replication_connection_configuration" {
		switch column {
		case "CHANNEL_NAME":
			return performanceSchemaVirtualCharacterSpec("CHAR", 64, false), true
		case "HOST":
			return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, false, "ascii"), true
		case "PORT", "CONNECTION_RETRY_INTERVAL", "ZSTD_COMPRESSION_LEVEL":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "USER":
			return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 32, false), true
		case "NETWORK_INTERFACE":
			return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 60, false), true
		case "AUTO_POSITION", "SOURCE_CONNECTION_AUTO_FAILOVER", "GTID_ONLY":
			return performanceSchemaVirtualColumnSpecFor("ENUM('1','0')", 0, false, false), true
		case "SSL_ALLOWED":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO','IGNORED')", 0, false, false), true
		case "SSL_VERIFY_SERVER_CERTIFICATE", "GET_PUBLIC_KEY":
			return performanceSchemaVirtualColumnSpecFor("ENUM('YES','NO')", 0, false, false), true
		case "SSL_CA_FILE", "SSL_CA_PATH", "SSL_CERTIFICATE", "SSL_CIPHER", "SSL_KEY", "PUBLIC_KEY_PATH":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 512, false), true
		case "SSL_CRL_FILE", "SSL_CRL_PATH":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 255, false), true
		case "CONNECTION_RETRY_COUNT":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "HEARTBEAT_INTERVAL":
			return performanceSchemaVirtualColumnMetadataSpec{typeName: "DOUBLE", length: 10, scale: 3, nullable: false}, true
		case "TLS_VERSION":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 255, false), true
		case "NETWORK_NAMESPACE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, false), true
		case "COMPRESSION_ALGORITHM":
			return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 64, false), true
		case "TLS_CIPHERSUITES":
			return performanceSchemaVirtualBinaryTextSpec("TEXT", true), true
		}
	}

	if table == "replication_connection_status" {
		switch column {
		case "CHANNEL_NAME":
			return performanceSchemaVirtualCharacterSpec("CHAR", 64, false), true
		case "GROUP_NAME", "SOURCE_UUID":
			return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 36, false), true
		case "THREAD_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "SERVICE_STATE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('ON','OFF','CONNECTING')", 0, false, false), true
		case "COUNT_RECEIVED_HEARTBEATS":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "LAST_HEARTBEAT_TIMESTAMP", "LAST_ERROR_TIMESTAMP", "LAST_QUEUED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP", "LAST_QUEUED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP", "LAST_QUEUED_TRANSACTION_START_QUEUE_TIMESTAMP", "LAST_QUEUED_TRANSACTION_END_QUEUE_TIMESTAMP", "QUEUEING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP", "QUEUEING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP", "QUEUEING_TRANSACTION_START_QUEUE_TIMESTAMP":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 6, false, false), true
		case "RECEIVED_TRANSACTION_SET":
			return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, false, false), true
		case "LAST_ERROR_NUMBER":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "LAST_ERROR_MESSAGE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, false), true
		case "LAST_QUEUED_TRANSACTION", "QUEUEING_TRANSACTION":
			return performanceSchemaVirtualCharacterSpec("CHAR", 90, true), true
		}
	}

	if table == "replication_applier_status_by_coordinator" || table == "replication_applier_status_by_worker" {
		isWorker := table == "replication_applier_status_by_worker"
		switch column {
		case "CHANNEL_NAME":
			return performanceSchemaVirtualCharacterSpec("CHAR", 64, false), true
		case "WORKER_ID":
			if isWorker {
				return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
			}
		case "THREAD_ID":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, true, true), true
		case "SERVICE_STATE":
			return performanceSchemaVirtualColumnSpecFor("ENUM('ON','OFF')", 0, false, false), true
		case "LAST_ERROR_NUMBER", "LAST_APPLIED_TRANSACTION_LAST_TRANSIENT_ERROR_NUMBER", "APPLYING_TRANSACTION_LAST_TRANSIENT_ERROR_NUMBER":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, false), true
		case "LAST_ERROR_MESSAGE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, false), true
		case "LAST_APPLIED_TRANSACTION_LAST_TRANSIENT_ERROR_MESSAGE", "APPLYING_TRANSACTION_LAST_TRANSIENT_ERROR_MESSAGE":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 1024, true), true
		case "LAST_ERROR_TIMESTAMP", "LAST_PROCESSED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP", "LAST_PROCESSED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP", "LAST_PROCESSED_TRANSACTION_START_BUFFER_TIMESTAMP", "LAST_PROCESSED_TRANSACTION_END_BUFFER_TIMESTAMP", "PROCESSING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP", "PROCESSING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP", "PROCESSING_TRANSACTION_START_BUFFER_TIMESTAMP", "LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP", "LAST_APPLIED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP", "LAST_APPLIED_TRANSACTION_START_APPLY_TIMESTAMP", "LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP", "APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP", "APPLYING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP", "APPLYING_TRANSACTION_START_APPLY_TIMESTAMP", "LAST_APPLIED_TRANSACTION_LAST_TRANSIENT_ERROR_TIMESTAMP", "APPLYING_TRANSACTION_LAST_TRANSIENT_ERROR_TIMESTAMP":
			return performanceSchemaVirtualColumnSpecFor("TIMESTAMP", 6, false, false), true
		case "LAST_PROCESSED_TRANSACTION", "PROCESSING_TRANSACTION", "LAST_APPLIED_TRANSACTION", "APPLYING_TRANSACTION":
			return performanceSchemaVirtualCharacterSpec("CHAR", 90, true), true
		case "LAST_APPLIED_TRANSACTION_RETRIES_COUNT", "APPLYING_TRANSACTION_RETRIES_COUNT":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}

	// The statement event and digest tables use fixed digest/text contracts.
	if column == "DIGEST" && (strings.HasPrefix(table, "events_statements_") || table == "prepared_statements_instances") && table != "events_statements_histogram_by_digest" && table != "events_statements_summary_by_digest" {
		return performanceSchemaVirtualCharacterSpec("CHAR", 64, true), true
	}
	if table == "events_statements_summary_by_digest" && column == "DIGEST" {
		return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
	}
	if column == "RETURNED_SQLSTATE" && strings.HasPrefix(table, "events_statements_") {
		return performanceSchemaVirtualCharacterSpec("VARCHAR", 5, true), true
	}
	if (column == "SQL_TEXT" || column == "DIGEST_TEXT" || column == "QUERY_SAMPLE_TEXT") && strings.HasPrefix(table, "events_statements_") {
		return performanceSchemaVirtualColumnSpecFor("LONGTEXT", 0, true, false), true
	}
	if column == "SOURCE" && (strings.HasPrefix(table, "events_statements_") || strings.HasPrefix(table, "events_stages_") || strings.HasPrefix(table, "events_waits_")) {
		return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
	}
	if table == "events_statements_summary_by_digest" {
		switch column {
		case "FIRST_SEEN", "LAST_SEEN":
			return performanceSchemaVirtualColumnMetadataSpec{typeName: "TIMESTAMP", scale: 6, nullable: false}, true
		case "QUERY_SAMPLE_SEEN":
			return performanceSchemaVirtualColumnMetadataSpec{typeName: "TIMESTAMP", scale: 6, nullable: false}, true
		case "QUERY_SAMPLE_TIMER_WAIT":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		case "QUANTILE_95", "QUANTILE_99", "QUANTILE_999":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}
	if (table == "events_statements_histogram_by_digest" || table == "events_statements_histogram_global") && column == "BUCKET_QUANTILE" {
		return performanceSchemaVirtualColumnMetadataSpec{typeName: "DOUBLE", length: 7, scale: 6, nullable: false}, true
	}
	if table == "events_statements_histogram_by_digest" || table == "events_statements_histogram_global" {
		switch column {
		case "DIGEST":
			return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
		case "BUCKET_NUMBER":
			return performanceSchemaVirtualColumnSpecFor("INT", 0, false, true), true
		case "BUCKET_TIMER_LOW", "BUCKET_TIMER_HIGH", "COUNT_BUCKET", "COUNT_BUCKET_AND_LOWER":
			return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
		}
	}

	// Common identifier and name lengths used by Performance Schema metadata.
	switch column {
	case "SCHEMA_NAME", "CURRENT_SCHEMA", "OBJECT_SCHEMA", "OBJECT_NAME", "TABLE_SCHEMA", "TABLE_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "INDEX_NAME":
		return performanceSchemaVirtualCharacterSpec("VARCHAR", 64, true), true
	case "USER":
		return performanceSchemaVirtualBinaryCharacterSpec("CHAR", 32, true), true
	case "HOST":
		return performanceSchemaVirtualCharacterSpecWithCharset("CHAR", 255, true, "ascii"), true
	}

	// Summary counters, event identifiers, timer values, and byte/row counts
	// are non-negative in the native Performance Schema contract.  Keep the
	// table/column guard narrow so unrelated fields such as PORT or an arbitrary
	// component-specific ID do not get an invented BIGINT definition.
	if performanceSchemaVirtualUnsignedNumericColumn(column) {
		return performanceSchemaVirtualColumnSpecFor("BIGINT", 0, false, true), true
	}
	return performanceSchemaVirtualColumnMetadataSpec{}, false
}

func performanceSchemaVirtualUnsignedNumericColumn(column string) bool {
	if column == "THREAD_ID" || column == "EVENT_ID" || column == "END_EVENT_ID" || column == "OBJECT_INSTANCE_BEGIN" || column == "OWNER_THREAD_ID" || column == "OWNER_EVENT_ID" {
		return true
	}
	for _, prefix := range []string{"COUNT_", "SUM_", "MIN_", "MAX_", "AVG_", "TIMER_", "ROWS_", "NUMBER_OF_"} {
		if strings.HasPrefix(column, prefix) {
			return true
		}
	}
	return strings.HasSuffix(column, "_BYTES") || column == "ERRORS" || column == "WARNINGS" || column == "MYSQL_ERRNO"
}
