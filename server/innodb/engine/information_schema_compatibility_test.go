package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestInformationSchemaNativeSelectStarUsesMySQL84ColumnShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table inventory (id int primary key, note varchar(32) default null)")

	cases := []struct {
		name    string
		query   string
		columns []string
	}{
		{
			name:    "schemata",
			query:   "select * from information_schema.schemata",
			columns: []string{"CATALOG_NAME", "SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME", "SQL_PATH", "DEFAULT_ENCRYPTION"},
		},
		{
			name:    "tables",
			query:   "select * from information_schema.tables where table_schema = 'app'",
			columns: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "AUTO_INCREMENT", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "TABLE_COLLATION", "CHECKSUM", "CREATE_OPTIONS", "TABLE_COMMENT"},
		},
		{
			name:    "columns",
			query:   "select * from information_schema.columns where table_schema = 'app' and table_name = 'inventory'",
			columns: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "COLUMN_TYPE", "COLUMN_KEY", "EXTRA", "PRIVILEGES", "COLUMN_COMMENT", "GENERATION_EXPRESSION", "SRS_ID"},
		},
		{
			name:    "routines",
			query:   "select * from information_schema.routines",
			columns: []string{"SPECIFIC_NAME", "ROUTINE_CATALOG", "ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "DTD_IDENTIFIER", "ROUTINE_BODY", "ROUTINE_DEFINITION", "EXTERNAL_NAME", "EXTERNAL_LANGUAGE", "PARAMETER_STYLE", "IS_DETERMINISTIC", "SQL_DATA_ACCESS", "SQL_PATH", "SECURITY_TYPE", "CREATED", "LAST_ALTERED", "SQL_MODE", "ROUTINE_COMMENT", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION"},
		},
		{
			name:    "triggers",
			query:   "select * from information_schema.triggers",
			columns: []string{"TRIGGER_CATALOG", "TRIGGER_SCHEMA", "TRIGGER_NAME", "EVENT_MANIPULATION", "EVENT_OBJECT_CATALOG", "EVENT_OBJECT_SCHEMA", "EVENT_OBJECT_TABLE", "ACTION_ORDER", "ACTION_CONDITION", "ACTION_STATEMENT", "ACTION_ORIENTATION", "ACTION_TIMING", "ACTION_REFERENCE_OLD_TABLE", "ACTION_REFERENCE_NEW_TABLE", "ACTION_REFERENCE_OLD_ROW", "ACTION_REFERENCE_NEW_ROW", "CREATED", "SQL_MODE", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION"},
		},
		{
			name:    "events",
			query:   "select * from information_schema.events",
			columns: []string{"EVENT_CATALOG", "EVENT_SCHEMA", "EVENT_NAME", "DEFINER", "TIME_ZONE", "EVENT_BODY", "EVENT_DEFINITION", "EVENT_TYPE", "EXECUTE_AT", "INTERVAL_VALUE", "INTERVAL_FIELD", "SQL_MODE", "STARTS", "ENDS", "STATUS", "ON_COMPLETION", "CREATED", "LAST_ALTERED", "LAST_EXECUTED", "EVENT_COMMENT", "ORIGINATOR", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := mustSelectResultSQL(t, executor, "app", tc.query)
			require.Equal(t, tc.columns, result.Columns)
		})
	}
}

func TestInformationSchemaNativeProjectionPreservesNullableColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table nullable_columns (id int, note varchar(32) default null)")

	result := mustSelectResultSQL(t, executor, "app", "select column_name, column_default, generation_expression, srs_id from information_schema.columns where table_schema='app' and table_name='nullable_columns' order by ordinal_position")
	require.Equal(t, []string{"COLUMN_NAME", "COLUMN_DEFAULT", "GENERATION_EXPRESSION", "SRS_ID"}, result.Columns)
	require.Len(t, result.Records, 2)
	for _, record := range result.Records {
		values := record.GetValues()
		require.Len(t, values, 4)
		require.Nil(t, values[1].Raw())
		require.Nil(t, values[2].Raw())
		require.Nil(t, values[3].Raw())
	}
}

func TestInformationSchemaSchemataAppliesCatalogAndPropertyFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table schemata_marker (id int primary key)")

	wrongCatalog := mustQuerySQL(t, executor, "", "select schema_name from information_schema.schemata where catalog_name = 'other'")
	require.Empty(t, wrongCatalog)

	wrongCharset := mustQuerySQL(t, executor, "", "select schema_name from information_schema.schemata where default_character_set_name = 'latin1'")
	require.Empty(t, wrongCharset)

	wrongCollation := mustQuerySQL(t, executor, "", "select schema_name from information_schema.schemata where default_collation_name = 'latin1_bin'")
	require.Empty(t, wrongCollation)

	app := mustQuerySQL(t, executor, "", "select schema_name from information_schema.schemata where schema_name = 'app' and default_encryption = 'N'")
	require.Equal(t, [][]interface{}{{"app"}}, app)
}

func TestInformationSchemaTablesAndColumnsApplyNativeFieldFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metadata_filters (id int not null primary key, note varchar(32) null)")

	require.Empty(t, mustQuerySQL(t, executor, "", "select table_name from information_schema.tables where table_schema='app' and table_name='metadata_filters' and engine='MyISAM'"))
	require.Empty(t, mustQuerySQL(t, executor, "", "select table_name from information_schema.tables where table_schema='app' and table_name='metadata_filters' and table_type='VIEW'"))

	require.Empty(t, mustQuerySQL(t, executor, "", "select column_name from information_schema.columns where table_schema='app' and table_name='metadata_filters' and column_name='id' and is_nullable='YES'"))
	require.Empty(t, mustQuerySQL(t, executor, "", "select column_name from information_schema.columns where table_schema='app' and table_name='metadata_filters' and column_name='id' and data_type='VARCHAR'"))
	require.Empty(t, mustQuerySQL(t, executor, "", "select column_name from information_schema.columns where table_schema='app' and table_name='metadata_filters' and column_name='id' and ordinal_position=2"))

	require.Equal(t, [][]interface{}{{"id"}}, mustQuerySQL(t, executor, "", "select column_name from information_schema.columns where table_schema='app' and table_name='metadata_filters' and column_name='id' and is_nullable='NO' and data_type='INT' and ordinal_position=1"))
}

func TestInformationSchemaConstraintAndIndexViewsApplyNativeFieldFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metadata_constraints (id int not null primary key, note varchar(32))")
	mustExecSQL(t, executor, "app", "alter table metadata_constraints add constraint chk_note check (note is not null)")

	require.Empty(t, mustQuerySQL(t, executor, "", "select index_name from information_schema.statistics where table_schema='app' and table_name='metadata_constraints' and index_name='PRIMARY' and index_type='HASH'"))
	require.Equal(t, [][]interface{}{{"PRIMARY"}}, mustQuerySQL(t, executor, "", "select index_name from information_schema.statistics where table_schema='app' and table_name='metadata_constraints' and index_name='PRIMARY' and index_type='BTREE'"))

	require.Empty(t, mustQuerySQL(t, executor, "", "select constraint_name from information_schema.table_constraints where constraint_schema='app' and table_name='metadata_constraints' and constraint_name='PRIMARY' and enforced='NO'"))
	require.Equal(t, [][]interface{}{{"PRIMARY"}}, mustQuerySQL(t, executor, "", "select constraint_name from information_schema.table_constraints where constraint_schema='app' and table_name='metadata_constraints' and constraint_name='PRIMARY' and constraint_type='PRIMARY KEY' and enforced='YES'"))

	require.Empty(t, mustQuerySQL(t, executor, "", "select constraint_name from information_schema.check_constraints where constraint_schema='app' and table_name='metadata_constraints' and constraint_name='chk_note' and check_clause='missing'"))
	require.Equal(t, [][]interface{}{{"chk_note"}}, mustQuerySQL(t, executor, "", "select constraint_name from information_schema.check_constraints where constraint_schema='app' and table_name='metadata_constraints' and constraint_name='chk_note' and enforced='YES'"))

	require.Empty(t, mustQuerySQL(t, executor, "", "select table_name from information_schema.partitions where table_schema='app' and table_name='metadata_constraints' and partition_method='HASH'"))
}

func TestInformationSchemaProfilingAppliesHistoryFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.RecordStatement("app", "select profiling ok", "SELECT", "ok", 2*time.Millisecond)
	executor.QueryExecutor.metricsRecorder.RecordStatement("app", "select profiling error", "SELECT", "error", 3*time.Millisecond)

	wrongState := mustQuerySQL(t, executor, "", "select state from information_schema.profiling where state = 'missing'")
	require.Empty(t, wrongState)

	filtered := mustQuerySQL(t, executor, "", "select state from information_schema.profiling where state = 'error'")
	require.Equal(t, [][]interface{}{{"error"}}, filtered)
}

func TestInformationSchemaCharacterSetsAppliesPropertyFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	wrongCollation := mustQuerySQL(t, executor, "", "select character_set_name from information_schema.character_sets where default_collate_name = 'missing_collation'")
	require.Empty(t, wrongCollation)

	latin1 := mustQuerySQL(t, executor, "", "select character_set_name from information_schema.character_sets where default_collate_name = 'latin1_swedish_ci' and maxlen = 1")
	require.Equal(t, [][]interface{}{{"latin1"}}, latin1)
}

func TestInformationSchemaCatalogColumnsUseDefaultCatalog(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table catalog_metadata (id int primary key, label varchar(16))")

	cases := []string{
		"select table_catalog from information_schema.tables where table_schema = 'app' and table_name = 'catalog_metadata'",
		"select table_catalog from information_schema.columns where table_schema = 'app' and table_name = 'catalog_metadata' and column_name = 'id'",
		"select table_catalog from information_schema.statistics where table_schema = 'app' and table_name = 'catalog_metadata' and index_name = 'PRIMARY'",
		"select table_catalog from information_schema.partitions where table_schema = 'app' and table_name = 'catalog_metadata'",
		"select table_catalog from information_schema.key_column_usage where table_schema = 'app' and table_name = 'catalog_metadata' and constraint_name = 'PRIMARY'",
		"select constraint_catalog from information_schema.table_constraints where table_schema = 'app' and table_name = 'catalog_metadata' and constraint_name = 'PRIMARY'",
	}
	for _, query := range cases {
		rows := mustQuerySQL(t, executor, "app", query)
		require.NotEmpty(t, rows, query)
		require.Equal(t, "def", fmt.Sprint(rows[0][0]), query)
	}
}

func TestInformationSchemaNativeMetadataRefreshesAfterRenameAndDrop(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table metadata_refresh (id int primary key)")
	require.Len(t, mustQuerySQL(t, executor, "app", "select table_name from information_schema.tables where table_schema='app' and table_name='metadata_refresh'"), 1)
	mustExecSQL(t, executor, "app", "rename table metadata_refresh to metadata_refresh_new")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select table_name from information_schema.tables where table_schema='app' and table_name='metadata_refresh'"))
	require.Len(t, mustQuerySQL(t, executor, "app", "select table_name from information_schema.tables where table_schema='app' and table_name='metadata_refresh_new'"), 1)
	mustExecSQL(t, executor, "app", "drop table metadata_refresh_new")
	require.Empty(t, mustQuerySQL(t, executor, "app", "select table_name from information_schema.tables where table_schema='app' and table_name='metadata_refresh_new'"))
}

func TestInformationSchemaRegistryCoversMySQL84NonFulltextTables(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	for _, tableName := range []string{
		"collation_character_set_applicability", "columns_extensions", "keywords", "optimizer_trace",
		"resource_groups", "schemata_extensions", "st_geometry_columns", "st_spatial_reference_systems",
		"st_units_of_measure", "tables_extensions", "tablespaces_extensions", "table_constraints_extensions",
		"user_attributes", "view_routine_usage", "view_table_usage", "innodb_buffer_page",
		"innodb_buffer_page_lru", "innodb_buffer_pool_stats", "innodb_cached_indexes", "innodb_cmp",
		"innodb_fields", "innodb_session_temp_tablespaces", "innodb_tables", "innodb_tablespaces_brief",
		"innodb_temp_table_info", "innodb_virtual", "tp_thread_group_state", "tp_thread_group_stats", "tp_thread_state",
	} {
		result := mustSelectResultSQL(t, executor, "", "select * from information_schema."+tableName)
		require.NotNil(t, result, tableName)
		require.NotEmpty(t, result.Columns, tableName)
	}
}

func TestInformationSchemaRegistryIncludesEnterpriseFirewallTables(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	users := mustSelectResultSQL(t, executor, "", "select userhost, mode from information_schema.mysql_firewall_users")
	require.Equal(t, []string{"USERHOST", "MODE"}, users.Columns)
	require.Empty(t, users.Records)

	whitelist := mustSelectResultSQL(t, executor, "", "select userhost, rule from information_schema.mysql_firewall_whitelist")
	require.Equal(t, []string{"USERHOST", "RULE"}, whitelist.Columns)
	require.Empty(t, whitelist.Records)

	columns := mustSelectResultSQL(t, executor, "", "select table_name, column_name from information_schema.columns where table_schema = 'information_schema' and table_name in ('MYSQL_FIREWALL_USERS', 'MYSQL_FIREWALL_WHITELIST')")
	require.Equal(t, []string{"TABLE_NAME", "COLUMN_NAME"}, columns.Columns)
	require.ElementsMatch(t, [][]interface{}{
		{"mysql_firewall_users", "USERHOST"},
		{"mysql_firewall_users", "MODE"},
		{"mysql_firewall_whitelist", "USERHOST"},
		{"mysql_firewall_whitelist", "RULE"},
	}, selectResultRows(columns))
}

func TestInformationSchemaSpatialAndResourceCatalogsExposeNativeRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	units := mustSelectResultSQL(t, executor, "", "select unit_name, unit_type, conversion_factor, description from information_schema.st_units_of_measure")
	require.Equal(t, []string{"UNIT_NAME", "UNIT_TYPE", "CONVERSION_FACTOR", "DESCRIPTION"}, units.Columns)
	require.Len(t, units.Records, 47)
	unitRows := selectResultRows(units)
	var metre []interface{}
	for _, row := range unitRows {
		if len(row) > 0 && row[0] == "metre" {
			metre = row
			break
		}
	}
	require.Equal(t, []interface{}{"metre", "LINEAR", "1", ""}, metre)
	metreByFactor := mustSelectResultSQL(t, executor, "", "select unit_name from information_schema.st_units_of_measure where conversion_factor = 1")
	require.Equal(t, [][]interface{}{{"metre"}}, selectResultRows(metreByFactor))
	missingFactor := mustSelectResultSQL(t, executor, "", "select unit_name from information_schema.st_units_of_measure where conversion_factor = 999")
	require.Empty(t, missingFactor.Records)

	srs := mustSelectResultSQL(t, executor, "", "select srs_name, srs_id, organization, organization_coordsys_id, definition, description from information_schema.st_spatial_reference_systems where srs_id = 0")
	require.Equal(t, []string{"SRS_NAME", "SRS_ID", "ORGANIZATION", "ORGANIZATION_COORDSYS_ID", "DEFINITION", "DESCRIPTION"}, srs.Columns)
	require.Len(t, srs.Records, 1)
	srsValues := srs.Records[0].GetValues()
	require.Equal(t, int64(0), srsValues[1].Int())
	require.Empty(t, srsValues[4].String())

	epsg := mustSelectResultSQL(t, executor, "", "select srs_name, srs_id from information_schema.st_spatial_reference_systems where organization = 'EPSG' and srs_name like 'WGS 84%'")
	require.Len(t, epsg.Records, 2)
	mercator := mustSelectResultSQL(t, executor, "", "select srs_name, srs_id from information_schema.st_spatial_reference_systems where organization_coordsys_id = 3857")
	require.Equal(t, [][]interface{}{{"WGS 84 / Pseudo-Mercator", "3857"}}, selectResultRows(mercator))
	missingDefinition := mustSelectResultSQL(t, executor, "", "select srs_id from information_schema.st_spatial_reference_systems where definition = 'not-a-real-definition'")
	require.Empty(t, missingDefinition.Records)

	groups := mustSelectResultSQL(t, executor, "", "select resource_group_name, resource_group_type, resource_group_enabled, vcpu_ids, thread_priority from information_schema.resource_groups")
	require.Equal(t, []string{"RESOURCE_GROUP_NAME", "RESOURCE_GROUP_TYPE", "RESOURCE_GROUP_ENABLED", "VCPU_IDS", "THREAD_PRIORITY"}, groups.Columns)
	require.Len(t, groups.Records, 2)
	groupRows := selectResultRows(groups)
	for _, expected := range []struct{ name, groupType string }{{name: "SYS_default", groupType: "SYSTEM"}, {name: "USR_default", groupType: "USER"}} {
		var found []interface{}
		for _, row := range groupRows {
			if len(row) > 1 && row[0] == expected.name {
				found = row
				break
			}
		}
		require.NotNil(t, found)
		require.Equal(t, []interface{}{expected.name, expected.groupType, "1", found[3], "0"}, found)
		require.NotEmpty(t, found[3])
	}
	disabled := mustSelectResultSQL(t, executor, "", "select resource_group_name from information_schema.resource_groups where resource_group_enabled = 0")
	require.Empty(t, disabled.Records)
	wrongPriority := mustSelectResultSQL(t, executor, "", "select resource_group_name from information_schema.resource_groups where thread_priority = 999")
	require.Empty(t, wrongPriority.Records)
	matchingCPU := mustSelectResultSQL(t, executor, "", "select resource_group_name from information_schema.resource_groups where vcpu_ids like '0-%'")
	require.Len(t, matchingCPU.Records, 2)
}

func TestInformationSchemaConnectionControlFailedLoginsReflectRuntime(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.metricsRecorder.RecordAuthenticationFailure("failed_user", "localhost")
	executor.QueryExecutor.metricsRecorder.RecordAuthenticationFailure("failed_user", "localhost")

	result := mustSelectResultSQL(t, executor, "", "select userhost, failed_attempts from information_schema.connection_control_failed_login_attempts")
	require.Equal(t, []string{"USERHOST", "FAILED_ATTEMPTS"}, result.Columns)
	require.Equal(t, [][]interface{}{{"'failed_user'@'localhost'", "2"}}, selectResultRows(result))
	filtered := mustSelectResultSQL(t, executor, "", "select userhost, failed_attempts from information_schema.connection_control_failed_login_attempts where userhost = '''failed_user''@''localhost''' and failed_attempts = 2")
	require.Equal(t, [][]interface{}{{"'failed_user'@'localhost'", "2"}}, selectResultRows(filtered))
	wrongUser := mustSelectResultSQL(t, executor, "", "select userhost from information_schema.connection_control_failed_login_attempts where userhost = '''other_user''@''localhost'''")
	require.Empty(t, wrongUser.Records)

	executor.QueryExecutor.metricsRecorder.RecordAuthenticationSuccess("failed_user", "localhost")
	cleared := mustSelectResultSQL(t, executor, "", "select userhost, failed_attempts from information_schema.connection_control_failed_login_attempts")
	require.Empty(t, cleared.Records)
}

func TestInformationSchemaKeywordsReflectsParserVocabulary(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select word, reserved from information_schema.keywords")
	require.Greater(t, len(result.Records), 80)
	rows := selectResultRows(result)
	find := func(word string) []interface{} {
		for _, row := range rows {
			if len(row) == 2 && row[0] == word {
				return row
			}
		}
		return nil
	}
	require.Equal(t, []interface{}{"SELECT", "YES"}, find("SELECT"))
	require.Equal(t, []interface{}{"TABLE", "YES"}, find("TABLE"))
	require.Equal(t, []interface{}{"FULLTEXT", "NO"}, find("FULLTEXT"))
}

func TestInformationSchemaKeywordsApplyWordAndReservedFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	selected := mustSelectResultSQL(t, executor, "", "select word, reserved from information_schema.keywords where word = 'SELECT' and reserved = 'YES'")
	require.Equal(t, [][]interface{}{{"SELECT", "YES"}}, selectResultRows(selected))

	missing := mustSelectResultSQL(t, executor, "", "select word from information_schema.keywords where word = 'SELECT' and reserved = 'NO'")
	require.Empty(t, missing.Records)
}

func TestInformationSchemaProfilingProjectsStatementHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(915)
	result := <-executor.ExecuteQuery(session, "select 42", "app")
	require.NoError(t, result.Err)

	profilingResult := <-executor.ExecuteQuery(session, "select query_id, state, duration from information_schema.profiling", "app")
	require.NoError(t, profilingResult.Err)
	selectResult, ok := profilingResult.Data.(*SelectResult)
	require.True(t, ok)
	require.NotEmpty(t, selectResult.Records)
	values := selectResult.Records[len(selectResult.Records)-1].GetValues()
	require.Len(t, values, 3)
	require.NotEmpty(t, values[0].String())
	require.NotEmpty(t, values[1].String())
	require.NotNil(t, values[2].Raw())
	require.NotEmpty(t, values[2].String())
}

func TestInformationSchemaOptimizerTraceProjectsPhysicalPlan(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table trace_rows (id int primary key, note varchar(16))")
	mustExecSQL(t, executor, "app", "insert into trace_rows values (1, 'one')")
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(916)
	queryResult := <-executor.ExecuteQuery(session, "select id from trace_rows where id = 1", "app")
	require.NoError(t, queryResult.Err)

	traceResult := <-executor.ExecuteQuery(session, "select query, trace, missing_bytes_beyond_max_mem_size, insufficient_privileges from information_schema.optimizer_trace", "app")
	require.NoError(t, traceResult.Err)
	selectResult, ok := traceResult.Data.(*SelectResult)
	require.True(t, ok)
	require.NotEmpty(t, selectResult.Records)
	values := selectResult.Records[len(selectResult.Records)-1].GetValues()
	require.Contains(t, values[0].String(), "trace_rows")
	require.Contains(t, values[1].String(), "plan_type")
	require.NotNil(t, values[2].Raw())
	require.Equal(t, "NO", values[3].String())
}

func TestInformationSchemaOptimizerTraceAppliesTraceColumnFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table trace_filter_rows (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into trace_filter_rows values (1)")
	session := newTestMySQLSession()
	session.ctx.SetConnectionID(917)
	queryResult := <-executor.ExecuteQuery(session, "select id from trace_filter_rows where id = 1", "app")
	require.NoError(t, queryResult.Err)

	matched := mustSelectResultSQL(t, executor, "app", "select query, missing_bytes_beyond_max_mem_size, insufficient_privileges from information_schema.optimizer_trace where query like '%trace_filter_rows%' and missing_bytes_beyond_max_mem_size = 0 and insufficient_privileges = 'NO'")
	require.NotEmpty(t, matched.Records)

	missing := mustSelectResultSQL(t, executor, "app", "select query from information_schema.optimizer_trace where query like '%does_not_exist%' or insufficient_privileges = 'YES'")
	require.Empty(t, missing.Records)
}

func TestInformationSchemaInnoDBCompressionAppliesMetricFilters(t *testing.T) {
	executor := NewXMySQLEngine(&conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbCompression:    conf.InnodbCompressionConfig{Enabled: true, Method: "zlib", AllSpaces: true},
	})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	pageSize := mustSelectResultSQL(t, executor, "", "select page_size from information_schema.innodb_cmp")
	require.NotEmpty(t, pageSize.Records)

	wrongPageSize := mustSelectResultSQL(t, executor, "", "select page_size from information_schema.innodb_cmp where page_size = 1")
	require.Empty(t, wrongPageSize.Records)
}

func TestInformationSchemaExtensionViewsReuseNativeMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table extension_rows (id int primary key, label varchar(16))")
	mustExecSQL(t, executor, "app", "create view extension_view as select id from extension_rows")

	columns := mustSelectResultSQL(t, executor, "app", "select table_schema, table_name, column_name, engine_attribute from information_schema.columns_extensions where table_schema='app' and table_name='extension_rows' order by ordinal_position")
	require.Equal(t, []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ENGINE_ATTRIBUTE"}, columns.Columns)
	require.Len(t, columns.Records, 2)
	require.Equal(t, "extension_rows", columns.Records[0].GetValues()[1].String())
	require.Nil(t, columns.Records[0].GetValues()[3].Raw())

	tables := mustSelectResultSQL(t, executor, "app", "select table_schema, table_name, engine_attribute from information_schema.tables_extensions where table_schema='app' and table_name='extension_rows'")
	require.Equal(t, []string{"TABLE_SCHEMA", "TABLE_NAME", "ENGINE_ATTRIBUTE"}, tables.Columns)
	require.Len(t, tables.Records, 1)
	require.Nil(t, tables.Records[0].GetValues()[2].Raw())

	schemata := mustSelectResultSQL(t, executor, "app", "select schema_name, options from information_schema.schemata_extensions where schema_name='app'")
	require.Equal(t, []string{"SCHEMA_NAME", "OPTIONS"}, schemata.Columns)
	require.Len(t, schemata.Records, 1)
	require.Nil(t, schemata.Records[0].GetValues()[1].Raw())

	constraints := mustSelectResultSQL(t, executor, "app", "select table_name, constraint_name, engine_attribute from information_schema.table_constraints_extensions where table_schema='app' and table_name='extension_rows'")
	require.Equal(t, []string{"TABLE_NAME", "CONSTRAINT_NAME", "ENGINE_ATTRIBUTE"}, constraints.Columns)
	require.Len(t, constraints.Records, 1)
	require.Equal(t, "PRIMARY", constraints.Records[0].GetValues()[1].String())
	require.Nil(t, constraints.Records[0].GetValues()[2].Raw())

	applicability := mustSelectResultSQL(t, executor, "app", "select collation_name, character_set_name from information_schema.collation_character_set_applicability where character_set_name='utf8mb4'")
	require.NotEmpty(t, applicability.Records)
	require.Equal(t, "utf8mb4", applicability.Records[0].GetValues()[1].String())

	usage := mustSelectResultSQL(t, executor, "app", "select view_schema, view_name, table_schema, table_name from information_schema.view_table_usage where view_schema='app' and view_name='extension_view'")
	require.Equal(t, []string{"VIEW_SCHEMA", "VIEW_NAME", "TABLE_SCHEMA", "TABLE_NAME"}, usage.Columns)
	require.Contains(t, selectResultRows(usage), []interface{}{"app", "extension_view", "app", "extension_rows"})
}

func TestInformationSchemaExtensionViewsApplyNullableAttributePredicates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table extension_filter_rows (id int primary key)")

	schemata := mustQuerySQL(t, executor, "", "select schema_name from information_schema.schemata_extensions where schema_name = 'app' and options is not null")
	require.Empty(t, schemata)

	columns := mustQuerySQL(t, executor, "", "select table_name from information_schema.columns_extensions where table_schema = 'app' and engine_attribute is not null")
	require.Empty(t, columns)

	valid := mustQuerySQL(t, executor, "", "select table_name from information_schema.columns_extensions where table_schema = 'app' and engine_attribute is null")
	require.Equal(t, [][]interface{}{{"extension_filter_rows"}}, valid)
}

func TestInformationSchemaViewTableUsageFiltersSourceTables(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table left_source (id int primary key)")
	mustExecSQL(t, executor, "app", "create table right_source (id int primary key)")
	mustExecSQL(t, executor, "app", "create view joined_usage as select l.id from left_source l join right_source r on l.id = r.id")

	usage := mustSelectResultSQL(t, executor, "app", "select view_name, table_schema, table_name from information_schema.view_table_usage where table_schema = 'app' and table_name = 'right_source'")
	require.Equal(t, [][]interface{}{{"joined_usage", "app", "right_source"}}, selectResultRows(usage))
}

func TestInformationSchemaViewTableUsageExcludesCTENames(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table base_rows (id int primary key)")
	mustExecSQL(t, executor, "app", "create view cte_usage as with source_rows as (select id from base_rows) select id from source_rows")

	usage := mustSelectResultSQL(t, executor, "app", "select table_schema, table_name from information_schema.view_table_usage where view_name = 'cte_usage'")
	require.Equal(t, [][]interface{}{{"app", "base_rows"}}, selectResultRows(usage))
}

func TestInformationSchemaViewRoutineUsageReflectsStoredFunctions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create function add_one(input_value int) returns int begin return input_value + 1; end")
	mustExecSQL(t, executor, "app", "create view function_view as select add_one(1) as value")

	usage := mustSelectResultSQL(t, executor, "app", "select view_schema, view_name, table_schema, table_name from information_schema.view_routine_usage where view_schema='app' and view_name='function_view'")
	require.Equal(t, []string{"VIEW_SCHEMA", "VIEW_NAME", "TABLE_SCHEMA", "TABLE_NAME"}, usage.Columns)
	require.Contains(t, selectResultRows(usage), []interface{}{"app", "function_view", "app", "add_one"})
}

func TestInformationSchemaViewRoutineUsageFiltersSourceFunctions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create function add_one(input_value int) returns int begin return input_value + 1; end")
	mustExecSQL(t, executor, "app", "create function add_two(input_value int) returns int begin return input_value + 2; end")
	mustExecSQL(t, executor, "app", "create view function_filter_view as select add_one(1) + add_two(1) as value")

	usage := mustSelectResultSQL(t, executor, "app", "select view_name, table_schema, table_name from information_schema.view_routine_usage where table_schema = 'app' and table_name = 'add_two'")
	require.Equal(t, [][]interface{}{{"function_filter_view", "app", "add_two"}}, selectResultRows(usage))
}

func TestInformationSchemaSTGeometryColumnsReflectsPersistedGeometryMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table places (id int primary key, location geometry)")

	result := mustSelectResultSQL(t, executor, "app", "select table_schema, table_name, column_name, geometry_type, srs_id from information_schema.st_geometry_columns where table_schema='app' and table_name='places'")
	require.Equal(t, []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "GEOMETRY_TYPE", "SRS_ID"}, result.Columns)
	require.Len(t, result.Records, 1)
	require.Equal(t, "location", result.Records[0].GetValues()[2].String())
	require.Equal(t, "GEOMETRY", result.Records[0].GetValues()[3].String())
	require.Nil(t, result.Records[0].GetValues()[4].Raw())
}

func TestInformationSchemaSTGeometryColumnsApplyColumnAndTypeFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table places (id int primary key, location geometry)")

	matching := mustSelectResultSQL(t, executor, "app", "select table_name, column_name from information_schema.st_geometry_columns where table_schema = 'app' and column_name = 'location' and geometry_type = 'GEOMETRY'")
	require.Equal(t, [][]interface{}{{"places", "location"}}, selectResultRows(matching))

	wrongType := mustSelectResultSQL(t, executor, "app", "select table_name from information_schema.st_geometry_columns where table_schema = 'app' and geometry_type = 'POINT'")
	require.Empty(t, wrongType.Records)
}

func TestInformationSchemaUserAttributesProjectsPersistedAccountAttributes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'attr_user'@'localhost' identified by 'secret'")
	accounts, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	for index := range accounts.Accounts {
		if accounts.Accounts[index].User == "attr_user" && accounts.Accounts[index].Host == "localhost" {
			accounts.Accounts[index].UserAttributes = `{"team":"compatibility"}`
		}
	}
	require.NoError(t, executor.QueryExecutor.savePersistedAccounts(accounts))

	result := mustSelectResultSQL(t, executor, "", "select user, host, attribute from information_schema.user_attributes where user='attr_user' and host='localhost'")
	require.Equal(t, []string{"USER", "HOST", "ATTRIBUTE"}, result.Columns)
	require.Equal(t, [][]interface{}{{"attr_user", "localhost", `{"team":"compatibility"}`}}, selectResultRows(result))
	wrongAttribute := mustSelectResultSQL(t, executor, "", "select user from information_schema.user_attributes where user='attr_user' and attribute='missing'")
	require.Empty(t, wrongAttribute.Records)
}

func TestInformationSchemaUserAttributesHonorsMySQL84Visibility(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	for _, user := range []string{"attr_viewer", "attr_peer", "attr_system"} {
		mustExecSQL(t, executor, "", fmt.Sprintf("create user '%s'@'localhost' identified by 'secret'", user))
	}
	mustExecSQL(t, executor, "", "grant create user on *.* to 'attr_viewer'@'localhost'")
	mustExecSQL(t, executor, "", "grant system_user on *.* to 'attr_system'@'localhost'")

	accounts, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	for index := range accounts.Accounts {
		switch accounts.Accounts[index].User {
		case "attr_viewer", "attr_peer", "attr_system":
			accounts.Accounts[index].UserAttributes = fmt.Sprintf(`{"owner":"%s"}`, accounts.Accounts[index].User)
		}
	}
	require.NoError(t, executor.QueryExecutor.savePersistedAccounts(accounts))

	viewer := newTestMySQLSession()
	viewer.SetParamByName("user", "attr_viewer")
	viewer.SetParamByName("host", "localhost")
	viewerRows := mustQuerySessionSQL(t, executor, viewer, "", "select user, host from information_schema.user_attributes")
	require.Contains(t, viewerRows, []interface{}{"attr_viewer", "localhost"})
	require.Contains(t, viewerRows, []interface{}{"attr_peer", "localhost"})
	require.NotContains(t, viewerRows, []interface{}{"attr_system", "localhost"})

	system := newTestMySQLSession()
	system.SetParamByName("user", "attr_system")
	system.SetParamByName("host", "localhost")
	require.Equal(t, [][]interface{}{{"attr_system", "localhost"}}, mustQuerySessionSQL(t, executor, system, "", "select user, host from information_schema.user_attributes"))

	replica := newTestMySQLSession()
	replica.SetParamByName("replication_replay", true)
	replicaRows := mustQuerySessionSQL(t, executor, replica, "", "select user, host from information_schema.user_attributes")
	require.Contains(t, replicaRows, []interface{}{"attr_system", "localhost"})
}

func TestInformationSchemaMetadataVisibilityIncludesColumnOnlyGrants(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table column_visible (id int primary key, email varchar(64), secret varchar(64))")
	mustExecSQL(t, executor, "", "create user 'column_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select (email) on app.column_visible to 'column_reader'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "column_reader")
	session.SetParamByName("host", "localhost")
	tables := mustQuerySessionSQL(t, executor, session, "", "select table_schema, table_name from information_schema.tables where table_schema = 'app'")
	require.Contains(t, tables, []interface{}{"app", "column_visible"})
	columns := mustQuerySessionSQL(t, executor, session, "", "select table_name, column_name from information_schema.columns where table_schema = 'app' and table_name = 'column_visible'")
	require.Contains(t, columns, []interface{}{"column_visible", "email"})
}

func TestInformationSchemaPrivilegeViewsRespectSessionVisibility(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table reader_table (id int primary key)")
	mustExecSQL(t, executor, "app", "create table other_table (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'priv_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'priv_other'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.reader_table to 'priv_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on app.other_table to 'priv_other'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on app.* to 'priv_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on *.* to 'priv_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant shutdown on *.* to 'priv_other'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "priv_reader")
	session.SetParamByName("host", "localhost")

	tablePrivileges := mustQuerySessionSQL(t, executor, session, "", "select grantee, table_name from information_schema.table_privileges where table_schema = 'app'")
	require.Contains(t, tablePrivileges, []interface{}{"'priv_reader'@'localhost'", "reader_table"})
	require.NotContains(t, tablePrivileges, []interface{}{"'priv_other'@'localhost'", "other_table"})

	schemaPrivileges := mustQuerySessionSQL(t, executor, session, "", "select grantee, table_schema from information_schema.schema_privileges where table_schema = 'app'")
	require.Contains(t, schemaPrivileges, []interface{}{"'priv_reader'@'localhost'", "app"})
	require.NotContains(t, schemaPrivileges, []interface{}{"'priv_other'@'localhost'", "app"})

	userPrivileges := mustQuerySessionSQL(t, executor, session, "", "select grantee, privilege_type from information_schema.user_privileges")
	require.Contains(t, userPrivileges, []interface{}{"'priv_reader'@'localhost'", "SELECT"})
	require.NotContains(t, userPrivileges, []interface{}{"'priv_other'@'localhost'", "SHUTDOWN"})
}

func TestInformationSchemaPrivilegeViewsApplyPrivilegePredicates(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database filtered_privileges")
	mustExecSQL(t, executor, "filtered_privileges", "create table records (id int primary key, secret varchar(32))")
	mustExecSQL(t, executor, "", "create user 'filtered_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select, update on filtered_privileges.records to 'filtered_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant select, insert on filtered_privileges.* to 'filtered_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant select, shutdown on *.* to 'filtered_reader'@'localhost'")

	tableRows := mustQuerySQL(t, executor, "", "select privilege_type from information_schema.table_privileges where grantee = \"'filtered_reader'@'localhost'\" and privilege_type = 'SELECT'")
	require.Equal(t, [][]interface{}{{"SELECT"}}, tableRows)

	schemaRows := mustQuerySQL(t, executor, "", "select privilege_type from information_schema.schema_privileges where grantee = \"'filtered_reader'@'localhost'\" and privilege_type like 'INS%'")
	require.Equal(t, [][]interface{}{{"INSERT"}}, schemaRows)

	userRows := mustQuerySQL(t, executor, "", "select privilege_type from information_schema.user_privileges where grantee = \"'filtered_reader'@'localhost'\" and privilege_type = 'SHUTDOWN'")
	require.Equal(t, [][]interface{}{{"SHUTDOWN"}}, userRows)

	missingRows := mustQuerySQL(t, executor, "", "select privilege_type from information_schema.user_privileges where grantee = \"'filtered_reader'@'localhost'\" and privilege_type = 'DOES_NOT_EXIST'")
	require.Empty(t, missingRows)
}

func TestInformationSchemaUserPrivilegesIncludesPersistedDynamicGlobalGrants(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'dynamic_viewer'@'localhost' identified by 'secret'")
	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	account := findPersistedAccount(file, "dynamic_viewer", "localhost")
	require.NotNil(t, account)
	account.GlobalGrants = []string{"BACKUP_ADMIN"}
	account.Grants = map[string][]string{}
	require.NoError(t, executor.QueryExecutor.savePersistedAccounts(file))

	session := newTestMySQLSession()
	session.SetParamByName("user", "dynamic_viewer")
	session.SetParamByName("host", "localhost")
	rows := mustQuerySessionSQL(t, executor, session, "", "select grantee, privilege_type from information_schema.user_privileges")
	require.Contains(t, rows, []interface{}{"'dynamic_viewer'@'localhost'", "BACKUP_ADMIN"})
}

func TestInformationSchemaDiscoversVirtualPerformanceSchemaTablesAndColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	tables := mustSelectResultSQL(t, executor, "", "select table_schema, table_name, table_type from information_schema.tables where table_schema='performance_schema' and table_name in ('threads','events_statements_summary_by_thread_by_event_name') order by table_name")
	require.Len(t, tables.Records, 2)
	columns := mustSelectResultSQL(t, executor, "", "select table_name, column_name, ordinal_position from information_schema.columns where table_schema='performance_schema' and table_name='threads' order by ordinal_position")
	require.Len(t, columns.Records, 24)
	require.Equal(t, "THREAD_ID", columns.Records[0].GetValues()[1].String())
	require.Equal(t, "TELEMETRY_ACTIVE", columns.Records[23].GetValues()[1].String())
}

func TestInformationSchemaColumnsDescribesNativeInformationSchemaViews(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	cases := []struct {
		tableName string
		count     int
		first     string
		last      string
	}{
		{tableName: "tables", count: 21, first: "TABLE_CATALOG", last: "TABLE_COMMENT"},
		{tableName: "columns", count: 22, first: "TABLE_CATALOG", last: "SRS_ID"},
		{tableName: "routines", count: 31, first: "SPECIFIC_NAME", last: "DATABASE_COLLATION"},
		{tableName: "events", count: 24, first: "EVENT_CATALOG", last: "DATABASE_COLLATION"},
		{tableName: "plugins", count: 11, first: "PLUGIN_NAME", last: "LOAD_OPTION"},
		{tableName: "innodb_trx", count: 22, first: "TRX_ID", last: "TRX_AUTOCOMMIT_NON_LOCKING"},
		{tableName: "innodb_metrics", count: 17, first: "NAME", last: "COMMENT"},
	}
	for _, tc := range cases {
		t.Run(tc.tableName, func(t *testing.T) {
			result := mustSelectResultSQL(t, executor, "", "select table_name, column_name from information_schema.columns where table_schema='information_schema' and table_name='"+tc.tableName+"' order by ordinal_position")
			require.Len(t, result.Records, tc.count)
			require.Equal(t, tc.tableName, result.Records[0].GetValues()[0].String())
			require.Equal(t, tc.first, result.Records[0].GetValues()[1].String())
			require.Equal(t, tc.last, result.Records[len(result.Records)-1].GetValues()[1].String())
		})
	}
}
