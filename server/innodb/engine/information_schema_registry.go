package engine

import (
	"fmt"
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
	"collation_character_set_applicability":    {"COLLATION_NAME", "CHARACTER_SET_NAME"},
	"columns_extensions":                       {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE"},
	"connection_control_failed_login_attempts": {"USERHOST", "FAILED_ATTEMPTS"},
	"mysql_firewall_users":                     {"USERHOST", "MODE"},
	"mysql_firewall_whitelist":                 {"USERHOST", "RULE"},
	"innodb_buffer_page":                       {"POOL_ID", "BLOCK_ID", "SPACE", "PAGE_NUMBER", "PAGE_TYPE", "FLUSH_TYPE", "FIX_COUNT", "IS_HASHED", "NEWEST_MODIFICATION", "OLDEST_MODIFICATION", "ACCESS_TIME", "TABLE_NAME", "INDEX_NAME", "NUMBER_RECORDS", "DATA_SIZE", "COMPRESSED_SIZE", "PAGE_STATE", "IO_FIX", "IS_OLD", "FREE_PAGE_CLOCK"},
	"innodb_buffer_page_lru":                   {"POOL_ID", "LRU_POSITION", "SPACE", "PAGE_NUMBER", "PAGE_TYPE", "FLUSH_TYPE", "FIX_COUNT", "IS_HASHED", "NEWEST_MODIFICATION", "OLDEST_MODIFICATION", "ACCESS_TIME", "TABLE_NAME", "INDEX_NAME", "NUMBER_RECORDS", "DATA_SIZE", "COMPRESSED_SIZE", "PAGE_STATE", "IO_FIX", "IS_OLD", "FREE_PAGE_CLOCK"},
	"innodb_buffer_pool_stats":                 {"POOL_ID", "POOL_SIZE", "FREE_BUFFERS", "DATABASE_PAGES", "OLD_DATABASE_PAGES", "MODIFIED_DATABASE_PAGES", "PENDING_DECOMPRESS", "PENDING_READS", "PENDING_FLUSH_LRU", "PENDING_FLUSH_LIST", "PAGES_MADE_YOUNG", "PAGES_NOT_MADE_YOUNG", "PAGES_MADE_YOUNG_RATE", "PAGES_MADE_NOT_YOUNG_RATE", "NUMBER_PAGES_READ", "NUMBER_PAGES_CREATED", "NUMBER_PAGES_WRITTEN", "PAGES_READ_RATE", "PAGES_CREATE_RATE", "PAGES_WRITTEN_RATE", "NUMBER_PAGES_GET", "HIT_RATE", "YOUNG_MAKE_PER_TH", "NOT_YOUNG_MAKE_PER_TH", "NUMBER_PAGES_READ_AHEAD", "NUMBER_READ_AHEAD_EVICTED", "READ_AHEAD_RATE", "READ_AHEAD_EVICTED_RATE", "LRU_IO_TOTAL", "LRU_IO_CURRENT", "UNCOMPRESS_TOTAL", "UNCOMPRESS_CURRENT"},
	"innodb_cached_indexes":                    {"SPACE_ID", "INDEX_ID", "INDEX_NAME", "N_CACHED_PAGES"},
	"innodb_cmp":                               {"PAGE_SIZE", "COMPRESS_OPS", "COMPRESS_OPS_OK", "COMPRESS_TIME", "UNCOMPRESS_OPS", "UNCOMPRESS_TIME"},
	"innodb_cmp_per_index":                     {"DATABASE_NAME", "TABLE_NAME", "INDEX_NAME", "COMPRESS_OPS", "COMPRESS_OPS_OK", "COMPRESS_TIME", "UNCOMPRESS_OPS", "UNCOMPRESS_TIME"},
	"innodb_cmp_per_index_reset":               {"DATABASE_NAME", "TABLE_NAME", "INDEX_NAME", "COMPRESS_OPS", "COMPRESS_OPS_OK", "COMPRESS_TIME", "UNCOMPRESS_OPS", "UNCOMPRESS_TIME"},
	"innodb_cmp_reset":                         {"PAGE_SIZE", "COMPRESS_OPS", "COMPRESS_OPS_OK", "COMPRESS_TIME", "UNCOMPRESS_OPS", "UNCOMPRESS_TIME"},
	"innodb_cmpmem":                            {"PAGE_SIZE", "BUFFER_POOL_INSTANCE", "COMPRESS_OPS", "COMPRESS_TIME", "UNCOMPRESS_OPS", "UNCOMPRESS_TIME"},
	"innodb_cmpmem_reset":                      {"PAGE_SIZE", "BUFFER_POOL_INSTANCE", "COMPRESS_OPS", "COMPRESS_TIME", "UNCOMPRESS_OPS", "UNCOMPRESS_TIME"},
	"innodb_fields":                            {"INDEX_ID", "NAME", "POS"},
	"innodb_session_temp_tablespaces":          {"SPACE", "PATH", "PURPOSE", "STATE", "SIZE", "ALLOCATED_SIZE"},
	"innodb_tables":                            {"TABLE_ID", "NAME", "FLAG", "N_COLS", "SPACE", "ROW_FORMAT", "ZIP_PAGE_SIZE", "SPACE_TYPE", "INSTANT_COLS", "TOTAL_ROW_VERSIONS"},
	"innodb_tablespaces_brief":                 {"SPACE", "NAME", "PATH", "FLAG", "SPACE_TYPE"},
	"innodb_temp_table_info":                   {"TABLE_ID", "NAME", "N_COLS", "SPACE", "PER_TABLE_TABLESPACE"},
	"innodb_virtual":                           {"TABLE_ID", "POS", "BASE_POS", "M_COLS"},
	"keywords":                                 {"WORD", "RESERVED"},
	"optimizer_trace":                          {"QUERY", "TRACE", "MISSING_BYTES_BEYOND_MAX_MEM_SIZE", "INSUFFICIENT_PRIVILEGES"},
	"profiling":                                {"QUERY_ID", "SEQ", "STATE", "DURATION", "CPU_USER", "CPU_SYSTEM", "CONTEXT_VOLUNTARY", "CONTEXT_INVOLUNTARY", "BLOCK_OPS_IN", "BLOCK_OPS_OUT", "MESSAGES_SENT", "MESSAGES_RECEIVED", "PAGE_FAULTS_MAJOR", "PAGE_FAULTS_MINOR", "SWAPS", "SOURCE_FUNCTION", "SOURCE_FILE", "SOURCE_LINE"},
	"resource_groups":                          {"RESOURCE_GROUP_NAME", "RESOURCE_GROUP_TYPE", "RESOURCE_GROUP_ENABLED", "VCPU_IDS", "THREAD_PRIORITY"},
	"schemata_extensions":                      {"CATALOG_NAME", "SCHEMA_NAME", "OPTIONS"},
	"st_geometry_columns":                      {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "SRS_ID", "GEOMETRY_TYPE", "MIN_X", "MAX_X", "MIN_Y", "MAX_Y"},
	"st_spatial_reference_systems":             {"SRS_NAME", "SRS_ID", "ORGANIZATION", "ORGANIZATION_COORDSYS_ID", "DEFINITION", "DESCRIPTION"},
	"st_units_of_measure":                      {"UNIT_NAME", "UNIT_TYPE", "CONVERSION_FACTOR", "DESCRIPTION"},
	"tables_extensions":                        {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE"},
	"tablespaces_extensions":                   {"TABLESPACE_NAME", "ENGINE_ATTRIBUTE", "SE_PRIVATE_DATA"},
	"table_constraints_extensions":             {"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME", "ENGINE_ATTRIBUTE", "SECONDARY_ENGINE_ATTRIBUTE"},
	"tp_thread_group_state":                    {"TP_GROUP_ID", "CONNECTIONS", "THREADS", "ACTIVE_THREAD_COUNT", "STALLED_THREAD_COUNT", "WAITING_THREAD_COUNT"},
	"tp_thread_group_stats":                    {"TP_GROUP_ID", "CONNECTIONS", "THREADS", "ACTIVE_THREAD_COUNT", "STALLED_THREAD_COUNT", "WAITING_THREAD_COUNT", "CONNECTION_COUNT", "CONNECTIONS_KILLED", "CONNECTIONS_CLOSED"},
	"tp_thread_state":                          {"TP_GROUP_ID", "TP_THREAD_ID", "PROCESSING", "EVENT_COUNT", "WAITING"},
	"user_attributes":                          {"USER", "HOST", "ATTRIBUTE"},
	"view_routine_usage":                       {"VIEW_CATALOG", "VIEW_SCHEMA", "VIEW_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"},
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
		"CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "CHECKSUM", "STATUS", "EXTRA", "NODEGROUP_ID", "TABLESPACE_TYPE",
	},
	"innodb_tablespaces": {
		"SPACE", "NAME", "SPACE_TYPE", "FS_BLOCK_SIZE", "FILE_SIZE", "ALLOCATED_SIZE", "SERVER_VERSION",
		"SPACE_VERSION", "ROW_FORMAT", "PAGE_SIZE", "ZIP_PAGE_SIZE", "AUTOEXTEND_SIZE", "STATE", "FLAGS", "FLAG", "SDI_VERSION", "SDI_OFFSET", "SDI_LENGTH", "SDI_SPACE",
		"SPACE_FLAGS", "SPACE_FLAGS2",
	},
	"innodb_datafiles": {"SPACE", "PATH"},
	"innodb_tablestats": {
		"TABLE_ID", "NAME", "STATS_INITIALIZED", "NUM_ROWS", "CLUST_INDEX_SIZE", "OTHER_INDEX_SIZE", "MODIFIED_COUNTER", "AUTOINC", "REF_COUNT",
	},
	"innodb_indexes": {"INDEX_ID", "NAME", "TABLE_ID", "TYPE", "N_FIELDS", "PAGE_NO", "SPACE", "MERGE_THRESHOLD"},
	"innodb_columns": {
		"TABLE_ID", "POS", "NAME", "MTYPE", "PRTYPE", "LEN", "HAS_DEFAULT", "DEFAULT_VALUE", "DEFAULT_VALUE_UTF8", "VERSION", "HAS_NO_DEFAULT",
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
		"TRX_CONCURRENCY_TICKETS", "TRX_ISOLATION_LEVEL", "TRX_UNIQUE_CHECKS", "TRX_FOREIGN_KEY_CHECKS", "TRX_LAST_FOREIGN_KEY_ERROR", "TRX_IS_READ_ONLY", "TRX_AUTOCOMMIT_NON_LOCKING",
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
		return e.executeInformationSchemaSTGeometryColumnsSelect(query)
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
		reserved := "NO"
		if info.Reserved {
			reserved = "YES"
		}
		if !metadataPatternMatches(strings.ToUpper(info.Word), filters["WORD"]) ||
			!metadataPatternMatches(reserved, filters["RESERVED"]) {
			continue
		}
		rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
			"WORD":     strings.ToUpper(info.Word),
			"RESERVED": reserved,
		}))
	}
	return newInformationSchemaSelectResult("information_schema.keywords", columns, rows)
}

var informationSchemaKeywordFilterPattern = regexp.MustCompile(`(?i)\b(word|reserved)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)

func informationSchemaKeywordFilters(query string) map[string]string {
	filters := make(map[string]string)
	for _, match := range informationSchemaKeywordFilterPattern.FindAllStringSubmatch(query, -1) {
		value := match[2]
		if value == "" {
			value = match[3]
		}
		filters[strings.ToUpper(match[1])] = value
	}
	return filters
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
	baseResult := e.executeInformationSchemaViewsSelect("select * from information_schema.views", session)
	filters := informationSchemaViewUsageFilters(query)
	rows := make([][]interface{}, 0)
	for _, record := range baseResult.Records {
		values := make(map[string]interface{}, len(baseResult.Columns))
		for index, column := range baseResult.Columns {
			if index < len(record.GetValues()) {
				values[strings.ToUpper(column)] = record.GetValues()[index].Raw()
			}
		}
		viewSchema := informationSchemaRawString(values["TABLE_SCHEMA"])
		viewName := informationSchemaRawString(values["TABLE_NAME"])
		definition := informationSchemaRawString(values["VIEW_DEFINITION"])
		if values["TABLE_SCHEMA"] == nil {
			viewSchema = ""
		}
		if values["TABLE_NAME"] == nil {
			viewName = ""
		}
		if values["VIEW_DEFINITION"] == nil {
			definition = ""
		}
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

var informationSchemaViewUsageFilterPattern = regexp.MustCompile(`(?i)\b(view_catalog|view_schema|view_name|table_catalog|table_schema|table_name)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)

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
