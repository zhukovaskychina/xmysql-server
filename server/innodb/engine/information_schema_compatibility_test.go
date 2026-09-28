package engine

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
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
		{
			name:    "st_geometry_columns",
			query:   "select * from information_schema.st_geometry_columns",
			columns: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "SRS_NAME", "SRS_ID", "GEOMETRY_TYPE_NAME"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := mustSelectResultSQL(t, executor, "app", tc.query)
			require.Equal(t, tc.columns, result.Columns)
		})
	}
}

func TestInformationSchemaInnoDBDictionaryColumnsUseNativeMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	cases := []struct {
		name string
		want []struct {
			name       string
			dataType   string
			columnType string
			nullable   string
			charLength interface{}
			numeric    interface{}
		}
	}{
		{
			name: "innodb_fields",
			want: []struct {
				name       string
				dataType   string
				columnType string
				nullable   string
				charLength interface{}
				numeric    interface{}
			}{
				{name: "INDEX_ID", dataType: "VARBINARY", columnType: "VARBINARY(256)", nullable: "YES", charLength: "256"},
				{name: "NAME", dataType: "VARCHAR", columnType: "VARCHAR(64)", nullable: "NO", charLength: "64"},
				{name: "POS", dataType: "BIGINT", columnType: "BIGINT UNSIGNED", nullable: "NO", numeric: "20"},
			},
		},
		{
			name: "innodb_virtual",
			want: []struct {
				name       string
				dataType   string
				columnType string
				nullable   string
				charLength interface{}
				numeric    interface{}
			}{
				{name: "TABLE_ID", dataType: "BIGINT", columnType: "BIGINT UNSIGNED", nullable: "NO"},
				{name: "POS", dataType: "INT", columnType: "INT UNSIGNED", nullable: "NO"},
				{name: "BASE_POS", dataType: "INT", columnType: "INT UNSIGNED", nullable: "NO"},
			},
		},
		{
			name: "innodb_foreign",
			want: []struct {
				name       string
				dataType   string
				columnType string
				nullable   string
				charLength interface{}
				numeric    interface{}
			}{
				{name: "ID", dataType: "VARCHAR", columnType: "VARCHAR(129)", nullable: "YES", charLength: "129"},
				{name: "FOR_NAME", dataType: "VARCHAR", columnType: "VARCHAR(129)", nullable: "YES", charLength: "129"},
				{name: "REF_NAME", dataType: "VARCHAR", columnType: "VARCHAR(129)", nullable: "YES", charLength: "129"},
				{name: "N_COLS", dataType: "BIGINT", columnType: "BIGINT", nullable: "NO", numeric: "19"},
				{name: "TYPE", dataType: "BIGINT", columnType: "BIGINT UNSIGNED", nullable: "NO", numeric: "20"},
			},
		},
		{
			name: "innodb_foreign_cols",
			want: []struct {
				name       string
				dataType   string
				columnType string
				nullable   string
				charLength interface{}
				numeric    interface{}
			}{
				{name: "ID", dataType: "VARCHAR", columnType: "VARCHAR(129)", nullable: "YES", charLength: "129"},
				{name: "FOR_COL_NAME", dataType: "VARCHAR", columnType: "VARCHAR(64)", nullable: "NO", charLength: "64"},
				{name: "REF_COL_NAME", dataType: "VARCHAR", columnType: "VARCHAR(64)", nullable: "NO", charLength: "64"},
				{name: "POS", dataType: "INT", columnType: "INT UNSIGNED", nullable: "NO", numeric: "10"},
			},
		},
		{
			name: "innodb_indexes",
			want: []struct {
				name       string
				dataType   string
				columnType string
				nullable   string
				charLength interface{}
				numeric    interface{}
			}{
				{name: "INDEX_ID", dataType: "BIGINT", columnType: "BIGINT UNSIGNED", nullable: "NO"},
				{name: "NAME", dataType: "VARCHAR", columnType: "VARCHAR(193)", nullable: "NO", charLength: "64"},
				{name: "TABLE_ID", dataType: "BIGINT", columnType: "BIGINT UNSIGNED", nullable: "NO"},
				{name: "TYPE", dataType: "INT", columnType: "INT", nullable: "NO"},
				{name: "N_FIELDS", dataType: "INT", columnType: "INT", nullable: "NO"},
				{name: "PAGE_NO", dataType: "INT", columnType: "INT", nullable: "NO"},
				{name: "SPACE", dataType: "INT", columnType: "INT", nullable: "NO"},
				{name: "MERGE_THRESHOLD", dataType: "INT", columnType: "INT", nullable: "NO"},
			},
		},
		{
			name: "innodb_columns",
			want: []struct {
				name       string
				dataType   string
				columnType string
				nullable   string
				charLength interface{}
				numeric    interface{}
			}{
				{name: "TABLE_ID", dataType: "BIGINT", columnType: "BIGINT UNSIGNED", nullable: "NO"},
				{name: "NAME", dataType: "VARCHAR", columnType: "VARCHAR(193)", nullable: "NO", charLength: "64"},
				{name: "POS", dataType: "BIGINT", columnType: "BIGINT UNSIGNED", nullable: "NO"},
				{name: "MTYPE", dataType: "INT", columnType: "INT", nullable: "NO"},
				{name: "PRTYPE", dataType: "INT", columnType: "INT", nullable: "NO"},
				{name: "LEN", dataType: "INT", columnType: "INT", nullable: "NO"},
				{name: "HAS_DEFAULT", dataType: "INT", columnType: "INT", nullable: "NO"},
				{name: "DEFAULT_VALUE", dataType: "TEXT", columnType: "TEXT", nullable: "YES", charLength: "65535"},
			},
		},
		{
			name: "innodb_cached_indexes",
			want: []struct {
				name       string
				dataType   string
				columnType string
				nullable   string
				charLength interface{}
				numeric    interface{}
			}{
				{name: "SPACE_ID", dataType: "INT", columnType: "INT UNSIGNED", nullable: "NO"},
				{name: "INDEX_ID", dataType: "BIGINT", columnType: "BIGINT UNSIGNED", nullable: "NO"},
				{name: "N_CACHED_PAGES", dataType: "BIGINT", columnType: "BIGINT UNSIGNED", nullable: "NO"},
			},
		},
		{
			name: "innodb_tables",
			want: []struct {
				name       string
				dataType   string
				columnType string
				nullable   string
				charLength interface{}
				numeric    interface{}
			}{
				{name: "TABLE_ID", dataType: "BIGINT", columnType: "BIGINT UNSIGNED", nullable: "NO"},
				{name: "NAME", dataType: "VARCHAR", columnType: "VARCHAR(655)", nullable: "NO", charLength: "218"},
				{name: "FLAG", dataType: "INT", columnType: "INT", nullable: "NO"},
				{name: "N_COLS", dataType: "INT", columnType: "INT", nullable: "NO"},
				{name: "SPACE", dataType: "BIGINT", columnType: "BIGINT", nullable: "NO"},
				{name: "ROW_FORMAT", dataType: "VARCHAR", columnType: "VARCHAR(12)", nullable: "YES", charLength: "4"},
				{name: "ZIP_PAGE_SIZE", dataType: "INT", columnType: "INT UNSIGNED", nullable: "NO"},
				{name: "SPACE_TYPE", dataType: "VARCHAR", columnType: "VARCHAR(10)", nullable: "YES", charLength: "3"},
				{name: "INSTANT_COLS", dataType: "INT", columnType: "INT", nullable: "NO"},
				{name: "TOTAL_ROW_VERSIONS", dataType: "INT", columnType: "INT", nullable: "NO"},
			},
		},
		{
			name: "innodb_datafiles",
			want: []struct {
				name       string
				dataType   string
				columnType string
				nullable   string
				charLength interface{}
				numeric    interface{}
			}{
				{name: "SPACE", dataType: "VARBINARY", columnType: "VARBINARY(256)", nullable: "YES", charLength: "256"},
				{name: "PATH", dataType: "VARCHAR", columnType: "VARCHAR(512)", nullable: "NO", charLength: "512"},
			},
		},
		{
			name: "innodb_tablespaces_brief",
			want: []struct {
				name       string
				dataType   string
				columnType string
				nullable   string
				charLength interface{}
				numeric    interface{}
			}{
				{name: "SPACE", dataType: "VARBINARY", columnType: "VARBINARY(256)", nullable: "YES", charLength: "256"},
				{name: "NAME", dataType: "VARCHAR", columnType: "VARCHAR(268)", nullable: "NO", charLength: "268"},
				{name: "PATH", dataType: "VARCHAR", columnType: "VARCHAR(512)", nullable: "NO", charLength: "512"},
				{name: "FLAG", dataType: "VARBINARY", columnType: "VARBINARY(256)", nullable: "YES", charLength: "256"},
				{name: "SPACE_TYPE", dataType: "VARCHAR", columnType: "VARCHAR(7)", nullable: "NO", charLength: "7"},
			},
		},
		{
			name: "innodb_metrics",
			want: []struct {
				name       string
				dataType   string
				columnType string
				nullable   string
				charLength interface{}
				numeric    interface{}
			}{
				{name: "NAME", dataType: "VARCHAR", columnType: "VARCHAR(193)", nullable: "NO", charLength: "64"},
				{name: "SUBSYSTEM", dataType: "VARCHAR", columnType: "VARCHAR(193)", nullable: "NO", charLength: "64"},
				{name: "COUNT", dataType: "BIGINT", columnType: "BIGINT", nullable: "NO"},
				{name: "MAX_COUNT", dataType: "BIGINT", columnType: "BIGINT", nullable: "YES"},
				{name: "MIN_COUNT", dataType: "BIGINT", columnType: "BIGINT", nullable: "YES"},
				{name: "AVG_COUNT", dataType: "FLOAT", columnType: "FLOAT(12,0)", nullable: "YES"},
				{name: "COUNT_RESET", dataType: "BIGINT", columnType: "BIGINT", nullable: "NO"},
				{name: "MAX_COUNT_RESET", dataType: "BIGINT", columnType: "BIGINT", nullable: "YES"},
				{name: "MIN_COUNT_RESET", dataType: "BIGINT", columnType: "BIGINT", nullable: "YES"},
				{name: "AVG_COUNT_RESET", dataType: "FLOAT", columnType: "FLOAT(12,0)", nullable: "YES"},
				{name: "TIME_ENABLED", dataType: "DATETIME", columnType: "DATETIME", nullable: "YES"},
				{name: "TIME_DISABLED", dataType: "DATETIME", columnType: "DATETIME", nullable: "YES"},
				{name: "TIME_ELAPSED", dataType: "BIGINT", columnType: "BIGINT", nullable: "YES"},
				{name: "TIME_RESET", dataType: "DATETIME", columnType: "DATETIME", nullable: "YES"},
				{name: "STATUS", dataType: "VARCHAR", columnType: "VARCHAR(193)", nullable: "NO", charLength: "64"},
				{name: "TYPE", dataType: "VARCHAR", columnType: "VARCHAR(193)", nullable: "NO", charLength: "64"},
				{name: "COMMENT", dataType: "VARCHAR", columnType: "VARCHAR(193)", nullable: "NO", charLength: "64"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := mustSelectResultSQL(t, executor, "", fmt.Sprintf("select column_name, data_type, column_type, is_nullable, character_maximum_length, numeric_precision from information_schema.columns where table_schema = 'information_schema' and table_name = '%s' order by ordinal_position", tc.name))
			rows := selectResultRows(result)
			require.Len(t, rows, len(tc.want))
			for index, want := range tc.want {
				expectedCharLength := ""
				if want.charLength != nil {
					expectedCharLength = fmt.Sprint(want.charLength)
				}
				expectedNumericPrecision := ""
				if want.numeric != nil {
					expectedNumericPrecision = fmt.Sprint(want.numeric)
				}
				require.Equal(t, want.name, rows[index][0])
				require.Equal(t, want.dataType, rows[index][1])
				require.Equal(t, want.columnType, rows[index][2])
				require.Equal(t, want.nullable, rows[index][3])
				require.Equal(t, expectedCharLength, fmt.Sprint(rows[index][4]))
				require.Equal(t, expectedNumericPrecision, fmt.Sprint(rows[index][5]))
			}
		})
	}
}

func TestInformationSchemaInnoDBColumnsProjectsPreciseTypeFlags(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table typed (id int not null, amount bigint unsigned not null, label varchar(10), payload varbinary(4))")

	result := mustSelectResultSQL(t, executor, "", "select pos, name, prtype from information_schema.innodb_columns where table_schema='app' and table_name='typed' order by pos")
	require.Equal(t, [][]interface{}{
		{"0", "id", "1283"},
		{"1", "amount", "1800"},
		{"2", "label", "2949135"},
		{"3", "payload", "1039"},
	}, selectResultRows(result))
}

func TestInformationSchemaInnoDBTrxUsesMySQL84TailColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='innodb_trx' and column_name in ('TRX_ADAPTIVE_HASH_LATCHED','TRX_ADAPTIVE_HASH_TIMEOUT','TRX_SCHEDULE_WEIGHT') order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"TRX_ADAPTIVE_HASH_LATCHED", "INT", "", "INT", "NO"},
		{"TRX_ADAPTIVE_HASH_TIMEOUT", "BIGINT", "", "BIGINT UNSIGNED", "NO"},
		{"TRX_SCHEDULE_WEIGHT", "BIGINT", "", "BIGINT UNSIGNED", "YES"},
	}, selectResultRows(result))
}

func TestInformationSchemaTablesProjectsPersistedCreateTime(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database create_time_app")
	mustExecSQL(t, executor, "create_time_app", "create table created_table (id int primary key)")

	result := mustSelectResultSQL(t, executor, "", "select table_name, create_time from information_schema.tables where table_schema='create_time_app' and table_name='created_table'")
	rows := make([][]interface{}, 0, len(result.Records))
	for _, record := range result.Records {
		raw := make([]interface{}, 0, len(record.GetValues()))
		for _, value := range record.GetValues() {
			switch value.Type() {
			case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeInt, basic.ValueTypeBigInt:
				raw = append(raw, value.Int())
			default:
				if bytes, ok := value.Raw().([]byte); ok {
					raw = append(raw, string(bytes))
				} else {
					raw = append(raw, value.Raw())
				}
			}
		}
		rows = append(rows, raw)
	}
	require.Len(t, rows, 1)
	require.Equal(t, "created_table", rows[0][0])
	require.NotEmpty(t, rows[0][1])
}

func TestInformationSchemaPartitionsProjectsPersistedCreateTime(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database partition_create_time_app")
	mustExecSQL(t, executor, "partition_create_time_app", "create table created_partition_table (id int primary key)")

	result := mustSelectResultSQL(t, executor, "", "select table_name, create_time from information_schema.partitions where table_schema='partition_create_time_app' and table_name='created_partition_table'")
	rows := selectResultRows(result)
	require.Len(t, rows, 1)
	require.Equal(t, "created_partition_table", rows[0][0])
	require.NotEmpty(t, rows[0][1])
}

func TestInformationSchemaFilesProjectsPersistedCreateTime(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database files_create_time_app")
	mustExecSQL(t, executor, "files_create_time_app", "create table created_file_table (id int primary key)")

	result := mustSelectResultSQL(t, executor, "", "select table_catalog, table_schema, table_name, creation_time, create_time, fulltext_keys, table_rows from information_schema.files where tablespace_name='files_create_time_app/created_file_table'")
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, "", values[0].String())
	for _, value := range values[1:] {
		require.True(t, value.IsNull(), "expected official FILES column to be NULL, got %q", value.String())
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

func TestInformationSchemaColumnsProjectsNumericTemporalAndCharacterPrecision(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database precision_app")
	mustExecSQL(t, executor, "precision_app", "create table typed_values (signed_id int not null, unsigned_id int unsigned, amount decimal(10,2), default_amount decimal, happened_at datetime(6), label varchar(10))")

	result := mustSelectResultSQL(t, executor, "", "select column_name, character_maximum_length, character_octet_length, numeric_precision, numeric_scale, datetime_precision from information_schema.columns where table_schema='precision_app' and table_name='typed_values' order by ordinal_position")
	rows := make([][]interface{}, 0, len(result.Records))
	for _, record := range result.Records {
		raw := make([]interface{}, 0, len(record.GetValues()))
		for _, value := range record.GetValues() {
			switch value.Type() {
			case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeInt, basic.ValueTypeBigInt:
				raw = append(raw, value.Int())
			default:
				if bytes, ok := value.Raw().([]byte); ok {
					raw = append(raw, string(bytes))
				} else {
					raw = append(raw, value.Raw())
				}
			}
		}
		rows = append(rows, raw)
	}
	require.Len(t, rows, 6)
	require.Equal(t, []interface{}{"signed_id", nil, nil, int64(10), int64(0), nil}, rows[0])
	require.Equal(t, []interface{}{"unsigned_id", nil, nil, int64(10), int64(0), nil}, rows[1])
	require.Equal(t, []interface{}{"amount", nil, nil, int64(10), int64(2), nil}, rows[2])
	require.Equal(t, []interface{}{"default_amount", nil, nil, int64(10), int64(0), nil}, rows[3])
	require.Equal(t, []interface{}{"happened_at", nil, nil, nil, nil, int64(6)}, rows[4])
	require.Equal(t, []interface{}{"label", int64(10), int64(40), nil, nil, nil}, rows[5])
}

func TestInformationSchemaColumnsProjectJdbcColumnSizeFromTypePrecision(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database column_size_app")
	mustExecSQL(t, executor, "column_size_app", "create table typed_values (amount decimal(8,2), happened_at datetime(3), label varchar(7))")

	result := mustSelectResultSQL(t, executor, "", "select column_name, column_size, character_maximum_length, numeric_precision, datetime_precision from information_schema.columns where table_schema='column_size_app' and table_name='typed_values' order by ordinal_position")
	rows := selectResultRows(result)
	require.Equal(t, [][]interface{}{
		{"amount", "8", "", "8", ""},
		{"happened_at", "3", "", "", "3"},
		{"label", "7", "7", "", ""},
	}, rows)
}

func TestInformationSchemaPartitionsProjectPhysicalPartitionRowCounts(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database partition_metadata_app")
	mustExecSQL(t, executor, "partition_metadata_app", "create table ordinary (id int primary key)")
	mustExecSQL(t, executor, "partition_metadata_app", "insert into ordinary values (1), (2)")
	mustExecSQL(t, executor, "partition_metadata_app", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))")
	mustExecSQL(t, executor, "partition_metadata_app", "insert into events values (1), (12), (13)")

	result := mustSelectResultSQL(t, executor, "", "select partition_name, table_rows from information_schema.partitions where table_schema='partition_metadata_app' and table_name='events' order by partition_ordinal_position")
	require.Len(t, result.Records, 2)
	require.Equal(t, "p0", result.Records[0].GetValues()[0].ToString())
	require.Equal(t, int64(1), result.Records[0].GetValues()[1].Int())
	require.Equal(t, "p1", result.Records[1].GetValues()[0].ToString())
	require.Equal(t, int64(2), result.Records[1].GetValues()[1].Int())

	ordinary := mustSelectResultSQL(t, executor, "", "select partition_name, table_rows from information_schema.partitions where table_schema='partition_metadata_app' and table_name='ordinary'")
	require.Len(t, ordinary.Records, 1)
	require.Nil(t, ordinary.Records[0].GetValues()[0].Raw())
	require.Equal(t, int64(2), ordinary.Records[0].GetValues()[1].Int())
}

func TestInformationSchemaColumnsInheritTableCharacterSetForOctetLength(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database charset_app")
	mustExecSQL(t, executor, "charset_app", "create table latin1_values (label varchar(7)) default character set latin1")

	result := mustSelectResultSQL(t, executor, "", "select character_set_name, collation_name, character_maximum_length, character_octet_length from information_schema.columns where table_schema='charset_app' and table_name='latin1_values' and column_name='label'")
	require.Len(t, result.Records, 1)
	values := result.Records[0].GetValues()
	require.Equal(t, "latin1", values[0].String())
	require.Equal(t, "latin1_swedish_ci", values[1].String())
	require.Equal(t, int64(7), values[2].Int())
	require.Equal(t, int64(7), values[3].Int())
}

func TestInformationSchemaViewColumnsPreserveSourcePrecisionAndCharacterSet(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database view_metadata_app")
	mustExecSQL(t, executor, "view_metadata_app", "create table source_values (label varchar(7), amount decimal(8,2), happened_at datetime(3)) default character set latin1")
	mustExecSQL(t, executor, "view_metadata_app", "create view projected_values as select label, amount, happened_at from source_values")

	result := mustSelectResultSQL(t, executor, "", "select column_name, character_set_name, collation_name, character_maximum_length, character_octet_length, numeric_precision, numeric_scale, datetime_precision from information_schema.columns where table_schema='view_metadata_app' and table_name='projected_values' order by ordinal_position")
	require.Len(t, result.Records, 3)

	label := result.Records[0].GetValues()
	require.Equal(t, "label", label[0].String())
	require.Equal(t, "latin1", label[1].String())
	require.Equal(t, "latin1_swedish_ci", label[2].String())
	require.Equal(t, int64(7), label[3].Int())
	require.Equal(t, int64(7), label[4].Int())

	amount := result.Records[1].GetValues()
	require.Equal(t, "amount", amount[0].String())
	require.Nil(t, amount[1].Raw())
	require.Nil(t, amount[2].Raw())
	require.Equal(t, int64(8), amount[5].Int())
	require.Equal(t, int64(2), amount[6].Int())

	happenedAt := result.Records[2].GetValues()
	require.Equal(t, "happened_at", happenedAt[0].String())
	require.Equal(t, int64(3), happenedAt[7].Int())
}

func TestInformationSchemaParametersProjectTypePrecision(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database routine_metadata_app")
	mustExecSQL(t, executor, "routine_metadata_app", "create procedure typed_parameters(IN label varchar(7), IN amount decimal(8,2), IN happened_at datetime(3)) begin select label, amount, happened_at; end")

	result := mustSelectResultSQL(t, executor, "", "select parameter_name, character_maximum_length, character_octet_length, numeric_precision, numeric_scale, datetime_precision from information_schema.parameters where specific_schema='routine_metadata_app' and specific_name='typed_parameters' order by ordinal_position")
	require.Len(t, result.Records, 3)

	label := result.Records[0].GetValues()
	require.Equal(t, "label", label[0].String())
	require.Equal(t, int64(7), label[1].Int())
	require.Equal(t, int64(28), label[2].Int())
	require.Nil(t, label[3].Raw())
	require.Nil(t, label[4].Raw())
	require.Nil(t, label[5].Raw())

	amount := result.Records[1].GetValues()
	require.Equal(t, "amount", amount[0].String())
	require.Equal(t, int64(8), amount[3].Int())
	require.Equal(t, int64(2), amount[4].Int())

	happenedAt := result.Records[2].GetValues()
	require.Equal(t, "happened_at", happenedAt[0].String())
	require.Equal(t, int64(3), happenedAt[5].Int())
}

func TestInformationSchemaRoutinesProjectReturnTypePrecision(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database routine_return_app")
	mustExecSQL(t, executor, "routine_return_app", "create function decimal_return(input_value int) returns decimal(8,2) deterministic begin return input_value; end")
	mustExecSQL(t, executor, "routine_return_app", "create function text_return(input_value int) returns varchar(7) deterministic begin return 'value'; end")

	result := mustSelectResultSQL(t, executor, "", "select routine_schema, routine_name, character_maximum_length, character_octet_length, numeric_precision, numeric_scale, datetime_precision from information_schema.routines where routine_schema='routine_return_app' order by routine_name")
	require.Len(t, result.Records, 2)

	decimalReturn := result.Records[0].GetValues()
	require.Equal(t, "decimal_return", decimalReturn[1].String())
	require.Nil(t, decimalReturn[2].Raw())
	require.Equal(t, int64(8), decimalReturn[4].Int())
	require.Equal(t, int64(2), decimalReturn[5].Int())
	require.Nil(t, decimalReturn[6].Raw())

	textReturn := result.Records[1].GetValues()
	require.Equal(t, "text_return", textReturn[1].String())
	require.Equal(t, int64(7), textReturn[2].Int())
	require.Equal(t, int64(28), textReturn[3].Int())
	require.Nil(t, textReturn[4].Raw())
	require.Nil(t, textReturn[5].Raw())
	require.Nil(t, textReturn[6].Raw())
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

func TestInformationSchemaSchemataIncludesEmptyDatabaseForGlobalPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database empty_schemata_db")
	mustExecSQL(t, executor, "", "create user 'schemata_global_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on *.* to 'schemata_global_reader'@'localhost'")

	reader := newTestMySQLSession()
	reader.SetParamByName("user", "schemata_global_reader")
	reader.SetParamByName("host", "localhost")

	require.Equal(t, [][]interface{}{{"empty_schemata_db"}}, mustQuerySessionSQL(t, executor, reader, "", "select schema_name from information_schema.schemata where schema_name = 'empty_schemata_db'"))
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
	require.Equal(t, [][]interface{}{{"chk_note"}}, mustQuerySQL(t, executor, "", "select constraint_name from information_schema.table_constraints where constraint_schema='app' and table_name='metadata_constraints' and constraint_name='chk_note' and constraint_type='CHECK' and enforced='YES'"))

	require.Empty(t, mustQuerySQL(t, executor, "", "select table_name from information_schema.partitions where table_schema='app' and table_name='metadata_constraints' and partition_method='HASH'"))
}

func TestInformationSchemaPartitionsHonorsTableVisibility(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database partitions_visibility")
	mustExecSQL(t, executor, "partitions_visibility", "create table partitioned_rows (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'partitions_metadata_reader'@'localhost' identified by 'secret'")

	reader := newTestMySQLSession()
	reader.SetParamByName("user", "partitions_metadata_reader")
	reader.SetParamByName("host", "localhost")
	query := "select table_schema, table_name from information_schema.partitions where table_schema='partitions_visibility' and table_name='partitioned_rows'"
	require.Empty(t, mustQuerySessionSQL(t, executor, reader, "", query))

	mustExecSQL(t, executor, "", "grant select on partitions_visibility.partitioned_rows to 'partitions_metadata_reader'@'localhost'")
	require.Equal(t, [][]interface{}{{"partitions_visibility", "partitioned_rows"}}, mustQuerySessionSQL(t, executor, reader, "", query))
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

func TestInformationSchemaThreadPoolTablesExposeMySQL84ShapesWithoutRuntimeRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	cases := map[string][]string{
		"tp_thread_group_state": {"TP_GROUP_ID", "CONSUMER_THREADS", "RESERVE_THREADS", "CONNECT_THREAD_COUNT", "CONNECTION_COUNT", "QUEUED_QUERIES", "QUEUED_TRANSACTIONS", "STALL_LIMIT", "PRIO_KICKUP_TIMER", "ALGORITHM", "THREAD_COUNT", "ACTIVE_THREAD_COUNT", "STALLED_THREAD_COUNT", "WAITING_THREAD_NUMBER", "OLDEST_QUEUED", "MAX_THREAD_IDS_IN_GROUP", "EFFECTIVE_MAX_TRANSACTIONS_LIMIT", "NUM_QUERY_THREADS", "TIME_OF_LAST_THREAD_CREATION", "NUM_CONNECT_HANDLER_THREAD_IN_SLEEP", "THREADS_BOUND_TO_TRANSACTION", "QUERY_THREADS_COUNT", "TIME_OF_EARLIEST_CON_EXPIRE"},
		"tp_thread_group_stats": {"TP_GROUP_ID", "CONNECTIONS_STARTED", "CONNECTIONS_CLOSED", "QUERIES_EXECUTED", "QUERIES_QUEUED", "THREADS_STARTED", "PRIO_KICKUPS", "STALLED_QUERIES_EXECUTED", "BECOME_CONSUMER_THREAD", "BECOME_RESERVE_THREAD", "BECOME_WAITING_THREAD", "WAKE_THREAD_STALL_CHECKER", "SLEEP_WAITS", "DISK_IO_WAITS", "ROW_LOCK_WAITS", "GLOBAL_LOCK_WAITS", "META_DATA_LOCK_WAITS", "TABLE_LOCK_WAITS", "USER_LOCK_WAITS", "BINLOG_WAITS", "GROUP_COMMIT_WAITS", "FSYNC_WAITS"},
		"tp_thread_state":       {"TP_GROUP_ID", "TP_THREAD_NUMBER", "PROCESS_COUNT", "WAIT_TYPE", "TP_THREAD_TYPE", "THREAD_ID"},
	}
	for tableName, expectedColumns := range cases {
		informationSchema := mustSelectResultSQL(t, executor, "", "select * from information_schema."+tableName)
		require.Equal(t, expectedColumns, informationSchema.Columns, tableName)
		require.Empty(t, informationSchema.Records, tableName)
		performanceSchema := mustSelectResultSQL(t, executor, "", "select * from performance_schema."+tableName)
		require.Equal(t, expectedColumns, performanceSchema.Columns, tableName)
		require.Empty(t, performanceSchema.Records, tableName)
	}
}

func TestInformationSchemaNDBConnectionMapUsesNativeShapeWhenNDBIsAbsent(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	result := mustSelectResultSQL(t, executor, "", "select * from information_schema.ndb_transid_mysql_connection_map")
	require.Equal(t, []string{"mysql_connection_id", "node_id", "ndb_transid"}, result.Columns)
	require.Empty(t, result.Records)

	metadata := mustSelectResultSQL(t, executor, "", "select column_name, data_type, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='ndb_transid_mysql_connection_map' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"mysql_connection_id", "BIGINT", "20", "BIGINT UNSIGNED", "NO"},
		{"node_id", "INT", "10", "INT UNSIGNED", "NO"},
		{"ndb_transid", "BIGINT", "20", "BIGINT UNSIGNED", "NO"},
	}, selectResultRows(metadata))
}

func TestInformationSchemaTemporaryInnoDBTablesUseMySQL84Metadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	sessionTablespaces := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='innodb_session_temp_tablespaces' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"ID", "INT", "", "", "INT UNSIGNED", "NO"},
		{"SPACE", "INT", "", "", "INT UNSIGNED", "NO"},
		{"PATH", "VARCHAR", "1333", "", "VARCHAR(4001)", "NO"},
		{"SIZE", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"STATE", "VARCHAR", "64", "", "VARCHAR(192)", "NO"},
		{"PURPOSE", "VARCHAR", "64", "", "VARCHAR(192)", "NO"},
	}, selectResultRows(sessionTablespaces))

	tempInfo := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='innodb_temp_table_info' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"TABLE_ID", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"NAME", "VARCHAR", "21", "", "VARCHAR(64)", "YES"},
		{"N_COLS", "INT", "", "", "INT UNSIGNED", "NO"},
		{"SPACE", "INT", "", "", "INT UNSIGNED", "NO"},
	}, selectResultRows(tempInfo))
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
	unitMetadata := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='st_units_of_measure' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"UNIT_NAME", "VARCHAR", "255", "VARCHAR(255)", "YES"},
		{"UNIT_TYPE", "VARCHAR", "7", "VARCHAR(7)", "YES"},
		{"CONVERSION_FACTOR", "DOUBLE", "", "DOUBLE", "YES"},
		{"DESCRIPTION", "VARCHAR", "255", "VARCHAR(255)", "YES"},
	}, selectResultRows(unitMetadata))
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
	srsMetadata := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='st_spatial_reference_systems' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"SRS_NAME", "VARCHAR", "80", "VARCHAR(80)", "NO"},
		{"SRS_ID", "INT", "", "INT UNSIGNED", "NO"},
		{"ORGANIZATION", "VARCHAR", "256", "VARCHAR(256)", "YES"},
		{"ORGANIZATION_COORDSYS_ID", "INT", "", "INT UNSIGNED", "YES"},
		{"DEFINITION", "VARCHAR", "4096", "VARCHAR(4096)", "NO"},
		{"DESCRIPTION", "VARCHAR", "2048", "VARCHAR(2048)", "YES"},
	}, selectResultRows(srsMetadata))
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
	groupMetadata := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='resource_groups' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"RESOURCE_GROUP_NAME", "VARCHAR", "64", "VARCHAR(64)", "NO"},
		{"RESOURCE_GROUP_TYPE", "ENUM", "6", "ENUM('SYSTEM','USER')", "NO"},
		{"RESOURCE_GROUP_ENABLED", "TINYINT", "", "TINYINT(1)", "NO"},
		{"VCPU_IDS", "BLOB", "65535", "BLOB", "YES"},
		{"THREAD_PRIORITY", "INT", "", "INT", "NO"},
	}, selectResultRows(groupMetadata))
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
	metadata := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='keywords' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"WORD", "VARCHAR", "128", "VARCHAR(128)", "YES"},
		{"RESERVED", "INT", "", "INT", "YES"},
	}, selectResultRows(metadata))
	rows := selectResultRows(result)
	find := func(word string) []interface{} {
		for _, row := range rows {
			if len(row) == 2 && row[0] == word {
				return row
			}
		}
		return nil
	}
	require.Equal(t, []interface{}{"SELECT", "1"}, find("SELECT"))
	require.Equal(t, []interface{}{"TABLE", "1"}, find("TABLE"))
	require.Equal(t, []interface{}{"FULLTEXT", "0"}, find("FULLTEXT"))
}

func TestInformationSchemaKeywordsApplyWordAndReservedFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	selected := mustSelectResultSQL(t, executor, "", "select word, reserved from information_schema.keywords where word = 'SELECT' and reserved = 1")
	require.Equal(t, [][]interface{}{{"SELECT", "1"}}, selectResultRows(selected))

	missing := mustSelectResultSQL(t, executor, "", "select word from information_schema.keywords where word = 'SELECT' and reserved = 0")
	require.Empty(t, missing.Records)

	reserved := mustSelectResultSQL(t, executor, "", "select word from information_schema.keywords where reserved")
	require.NotEmpty(t, reserved.Records)
	for _, row := range selectResultRows(reserved) {
		require.NotEqual(t, "FULLTEXT", row[0])
	}

	nonReserved := mustSelectResultSQL(t, executor, "", "select word from information_schema.keywords where not reserved")
	require.Equal(t, [][]interface{}{{"FULLTEXT"}}, filterRowsByFirstColumn(selectResultRows(nonReserved), "FULLTEXT"))
}

func filterRowsByFirstColumn(rows [][]interface{}, value string) [][]interface{} {
	filtered := make([][]interface{}, 0)
	for _, row := range rows {
		if len(row) > 0 && row[0] == value {
			filtered = append(filtered, row)
		}
	}
	return filtered
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
	metadata := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='profiling' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"QUERY_ID", "INT", "", "", "", "INT", "NO"},
		{"SEQ", "INT", "", "", "", "INT", "NO"},
		{"STATE", "VARCHAR", "10", "", "", "VARCHAR(30)", "NO"},
		{"DURATION", "DECIMAL", "", "", "", "DECIMAL(905,0)", "NO"},
		{"CPU_USER", "DECIMAL", "", "", "", "DECIMAL(905,0)", "YES"},
		{"CPU_SYSTEM", "DECIMAL", "", "", "", "DECIMAL(905,0)", "YES"},
		{"CONTEXT_VOLUNTARY", "INT", "", "", "", "INT", "YES"},
		{"CONTEXT_INVOLUNTARY", "INT", "", "", "", "INT", "YES"},
		{"BLOCK_OPS_IN", "INT", "", "", "", "INT", "YES"},
		{"BLOCK_OPS_OUT", "INT", "", "", "", "INT", "YES"},
		{"MESSAGES_SENT", "INT", "", "", "", "INT", "YES"},
		{"MESSAGES_RECEIVED", "INT", "", "", "", "INT", "YES"},
		{"PAGE_FAULTS_MAJOR", "INT", "", "", "", "INT", "YES"},
		{"PAGE_FAULTS_MINOR", "INT", "", "", "", "INT", "YES"},
		{"SWAPS", "INT", "", "", "", "INT", "YES"},
		{"SOURCE_FUNCTION", "VARCHAR", "10", "", "", "VARCHAR(30)", "YES"},
		{"SOURCE_FILE", "VARCHAR", "6", "", "", "VARCHAR(20)", "YES"},
		{"SOURCE_LINE", "INT", "", "", "", "INT", "YES"},
	}, selectResultRows(metadata))
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
	metadata := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='optimizer_trace' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"QUERY", "VARCHAR", "21845", "", "", "VARCHAR(65535)", "NO"},
		{"TRACE", "VARCHAR", "21845", "", "", "VARCHAR(65535)", "NO"},
		{"MISSING_BYTES_BEYOND_MAX_MEM_SIZE", "INT", "", "", "", "INT", "NO"},
		{"INSUFFICIENT_PRIVILEGES", "TINYINT", "", "", "", "TINYINT(1)", "NO"},
	}, selectResultRows(metadata))
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

func TestInformationSchemaExtensionMetadataUsesNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	rowsByName := func(result *SelectResult) map[string][]interface{} {
		rows := selectResultRows(result)
		values := make(map[string][]interface{}, len(rows))
		for _, row := range rows {
			if len(row) > 0 {
				values[fmt.Sprint(row[0])] = row
			}
		}
		return values
	}
	metadata := func(table string) map[string][]interface{} {
		return rowsByName(mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='"+table+"' order by ordinal_position"))
	}

	columnsExtensions := metadata("columns_extensions")
	for _, column := range []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"} {
		require.Equal(t, []interface{}{column, "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, columnsExtensions[column])
	}
	require.Equal(t, []interface{}{"COLUMN_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, columnsExtensions["COLUMN_NAME"])
	require.Equal(t, []interface{}{"ENGINE_ATTRIBUTE", "JSON", "", "", "JSON", "YES"}, columnsExtensions["ENGINE_ATTRIBUTE"])
	require.Equal(t, []interface{}{"SECONDARY_ENGINE_ATTRIBUTE", "JSON", "", "", "JSON", "YES"}, columnsExtensions["SECONDARY_ENGINE_ATTRIBUTE"])

	tablesExtensions := metadata("tables_extensions")
	for _, column := range []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"} {
		require.Equal(t, []interface{}{column, "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, tablesExtensions[column])
	}
	require.Equal(t, []interface{}{"ENGINE_ATTRIBUTE", "JSON", "", "", "JSON", "YES"}, tablesExtensions["ENGINE_ATTRIBUTE"])
	require.Equal(t, []interface{}{"SECONDARY_ENGINE_ATTRIBUTE", "JSON", "", "", "JSON", "YES"}, tablesExtensions["SECONDARY_ENGINE_ATTRIBUTE"])

	schemataExtensions := metadata("schemata_extensions")
	require.Equal(t, []interface{}{"CATALOG_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, schemataExtensions["CATALOG_NAME"])
	require.Equal(t, []interface{}{"SCHEMA_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, schemataExtensions["SCHEMA_NAME"])
	require.Equal(t, []interface{}{"OPTIONS", "VARCHAR", "256", "", "VARCHAR(256)", "YES"}, schemataExtensions["OPTIONS"])

	tableConstraintsExtensions := metadata("table_constraints_extensions")
	for _, column := range []string{"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME"} {
		require.Equal(t, []interface{}{column, "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, tableConstraintsExtensions[column])
	}
	require.Equal(t, []interface{}{"ENGINE_ATTRIBUTE", "JSON", "", "", "JSON", "YES"}, tableConstraintsExtensions["ENGINE_ATTRIBUTE"])
	require.Equal(t, []interface{}{"SECONDARY_ENGINE_ATTRIBUTE", "JSON", "", "", "JSON", "YES"}, tableConstraintsExtensions["SECONDARY_ENGINE_ATTRIBUTE"])

	tablespacesExtensions := metadata("tablespaces_extensions")
	require.Equal(t, []interface{}{"TABLESPACE_NAME", "VARCHAR", "268", "", "VARCHAR(268)", "NO"}, tablespacesExtensions["TABLESPACE_NAME"])
	require.Equal(t, []interface{}{"ENGINE_ATTRIBUTE", "JSON", "", "", "JSON", "YES"}, tablespacesExtensions["ENGINE_ATTRIBUTE"])
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

func TestInformationSchemaViewTableUsageUsesSomePrivilegeInsteadOfShowView(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table usage_source (id int primary key)")
	mustExecSQL(t, executor, "app", "create view usage_view as select id from usage_source")
	mustExecSQL(t, executor, "", "create user 'usage_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.usage_view to 'usage_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on app.usage_source to 'usage_reader'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "usage_reader")
	session.SetParamByName("host", "localhost")
	usage := mustQuerySessionSQL(t, executor, session, "", "select view_name, table_schema, table_name from information_schema.view_table_usage where view_schema = 'app' and view_name = 'usage_view'")
	require.Equal(t, [][]interface{}{{"usage_view", "app", "usage_source"}}, usage)
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

	usage := mustSelectResultSQL(t, executor, "app", "select table_catalog, table_schema, table_name, specific_catalog, specific_schema, specific_name from information_schema.view_routine_usage where table_schema='app' and specific_name='add_one'")
	require.Equal(t, []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME"}, usage.Columns)
	require.Contains(t, selectResultRows(usage), []interface{}{"def", "app", "function_view", "def", "app", "add_one"})
}

func TestInformationSchemaViewRoutineUsageUsesSomePrivilegeInsteadOfShowView(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create function usage_fn(value int) returns int begin return value + 1; end")
	mustExecSQL(t, executor, "app", "create view routine_usage_view as select usage_fn(1) as value")
	mustExecSQL(t, executor, "", "create user 'routine_usage_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.routine_usage_view to 'routine_usage_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant execute on app.usage_fn to 'routine_usage_reader'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "routine_usage_reader")
	session.SetParamByName("host", "localhost")
	usage := mustQuerySessionSQL(t, executor, session, "", "select table_name, specific_name from information_schema.view_routine_usage where table_schema = 'app' and table_name = 'routine_usage_view'")
	require.Equal(t, [][]interface{}{{"routine_usage_view", "usage_fn"}}, usage)
}

func TestInformationSchemaViewRoutineUsageFiltersSourceFunctions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create function add_one(input_value int) returns int begin return input_value + 1; end")
	mustExecSQL(t, executor, "app", "create function add_two(input_value int) returns int begin return input_value + 2; end")
	mustExecSQL(t, executor, "app", "create view function_filter_view as select add_one(1) + add_two(1) as value")

	usage := mustSelectResultSQL(t, executor, "app", "select table_name, specific_schema, specific_name from information_schema.view_routine_usage where specific_schema = 'app' and specific_name = 'add_two'")
	require.Equal(t, [][]interface{}{{"function_filter_view", "app", "add_two"}}, selectResultRows(usage))
}

func TestInformationSchemaViewRoutineUsageUsesNativeCatalogShape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create function add_one(input_value int) returns int begin return input_value + 1; end")
	mustExecSQL(t, executor, "app", "create view function_view as select add_one(1) as value")

	usage := mustSelectResultSQL(t, executor, "app", "select * from information_schema.view_routine_usage where table_schema='app' and specific_name='add_one'")
	require.Equal(t, []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME"}, usage.Columns)
	require.Equal(t, [][]interface{}{{"def", "app", "function_view", "def", "app", "add_one"}}, selectResultRows(usage))
}

func TestInformationSchemaVirtualViewUsageMetadataUsesNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	assertMetadata := func(tableName string, expectedColumns []string) {
		result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='"+tableName+"' order by ordinal_position")
		require.Equal(t, expectedColumns, func() []string {
			columns := make([]string, 0, len(result.Records))
			for _, record := range result.Records {
				columns = append(columns, record.GetValues()[0].String())
			}
			return columns
		}())
		for _, record := range result.Records {
			values := record.GetValues()
			require.Equal(t, "VARCHAR", values[1].String())
			require.Equal(t, int64(64), values[2].Int())
			require.Equal(t, "", values[3].String())
			require.Equal(t, "VARCHAR(64)", values[4].String())
			require.Equal(t, "NO", values[5].String())
		}
	}

	assertMetadata("view_routine_usage", []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME"})
	assertMetadata("view_table_usage", []string{"VIEW_CATALOG", "VIEW_SCHEMA", "VIEW_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"})
}

func TestInformationSchemaSTGeometryColumnsReflectsPersistedGeometryMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table places (id int primary key, location geometry)")

	result := mustSelectResultSQL(t, executor, "app", "select table_schema, table_name, column_name, geometry_type_name, srs_id from information_schema.st_geometry_columns where table_schema='app' and table_name='places'")
	require.Equal(t, []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "GEOMETRY_TYPE_NAME", "SRS_ID"}, result.Columns)
	require.Len(t, result.Records, 1)
	require.Equal(t, "location", result.Records[0].GetValues()[2].String())
	require.Equal(t, "GEOMETRY", result.Records[0].GetValues()[3].String())
	require.Nil(t, result.Records[0].GetValues()[4].Raw())
	metadata := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='st_geometry_columns' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"TABLE_CATALOG", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
		{"TABLE_SCHEMA", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
		{"TABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
		{"COLUMN_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"SRS_NAME", "VARCHAR", "80", "", "VARCHAR(80)", "YES"},
		{"SRS_ID", "INT", "", "10", "INT UNSIGNED", "YES"},
		{"GEOMETRY_TYPE_NAME", "LONGTEXT", "4294967295", "", "LONGTEXT", "YES"},
	}, selectResultRows(metadata))
}

func TestInformationSchemaSTGeometryColumnsApplyColumnAndTypeFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table places (id int primary key, location geometry)")

	matching := mustSelectResultSQL(t, executor, "app", "select table_name, column_name from information_schema.st_geometry_columns where table_schema = 'app' and column_name = 'location' and geometry_type_name = 'GEOMETRY'")
	require.Equal(t, [][]interface{}{{"places", "location"}}, selectResultRows(matching))

	wrongType := mustSelectResultSQL(t, executor, "app", "select table_name from information_schema.st_geometry_columns where table_schema = 'app' and geometry_type_name = 'POINT'")
	require.Empty(t, wrongType.Records)
}

func TestInformationSchemaSTGeometryColumnsHonorsTableVisibility(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database geometry_visibility")
	mustExecSQL(t, executor, "geometry_visibility", "create table places (id int primary key, location geometry)")
	mustExecSQL(t, executor, "", "create user 'geometry_metadata_reader'@'localhost' identified by 'secret'")

	reader := newTestMySQLSession()
	reader.SetParamByName("user", "geometry_metadata_reader")
	reader.SetParamByName("host", "localhost")
	query := "select table_schema, table_name, column_name from information_schema.st_geometry_columns where table_schema='geometry_visibility'"
	require.Empty(t, mustQuerySessionSQL(t, executor, reader, "", query))

	mustExecSQL(t, executor, "", "grant select on geometry_visibility.places to 'geometry_metadata_reader'@'localhost'")
	require.Equal(t, [][]interface{}{{"geometry_visibility", "places", "location"}}, mustQuerySessionSQL(t, executor, reader, "", query))
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

func TestInformationSchemaUserAttributesUsesNativeMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	metadata := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='user_attributes' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"USER", "CHAR", "32", "", "CHAR(32)", "NO"},
		{"HOST", "CHAR", "255", "", "CHAR(255)", "NO"},
		{"ATTRIBUTE", "LONGTEXT", "4294967295", "", "LONGTEXT", "YES"},
	}, selectResultRows(metadata))
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

func TestInformationSchemaColumnsProjectsEffectiveColumnPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database column_privileges_app")
	mustExecSQL(t, executor, "column_privileges_app", "create table scoped_columns (id int primary key, email varchar(64), secret varchar(64))")
	mustExecSQL(t, executor, "", "create user 'column_scope_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select (email) on column_privileges_app.scoped_columns to 'column_scope_reader'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "column_scope_reader")
	session.SetParamByName("host", "localhost")
	rows := mustQuerySessionSQL(t, executor, session, "", "select column_name, privileges from information_schema.columns where table_schema = 'column_privileges_app' and table_name = 'scoped_columns' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"id", ""},
		{"email", "select"},
		{"secret", ""},
	}, rows)
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
	require.Empty(t, userPrivileges)
}

func TestInformationSchemaPrivilegeViewsHonorGlobalMySQLUserVisibility(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database global_visibility_app")
	mustExecSQL(t, executor, "global_visibility_app", "create table visible_table (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'visibility_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'visibility_target'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on *.* to 'visibility_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on global_visibility_app.visible_table to 'visibility_target'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "visibility_reader")
	session.SetParamByName("host", "localhost")
	rows := mustQuerySessionSQL(t, executor, session, "", "select grantee, table_name from information_schema.table_privileges where table_schema='global_visibility_app' and table_name='visible_table'")
	require.Contains(t, rows, []interface{}{"'visibility_target'@'localhost'", "visible_table"})
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

func TestInformationSchemaPrivilegeVisibilityHonorsDynamicSystemUserGrant(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'metadata_admin'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'metadata_target'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant create user on *.* to 'metadata_admin'@'localhost'")
	mustExecSQL(t, executor, "", "grant system_user on *.* to 'metadata_admin'@'localhost'")
	mustExecSQL(t, executor, "", "grant shutdown on *.* to 'metadata_target'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "metadata_admin")
	session.SetParamByName("host", "localhost")
	rows := mustQuerySessionSQL(t, executor, session, "", "select grantee, privilege_type from information_schema.user_privileges where grantee = \"'metadata_target'@'localhost'\"")
	require.Equal(t, [][]interface{}{{"'metadata_target'@'localhost'", "SHUTDOWN"}}, rows)
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

func TestInformationSchemaVirtualPerformanceSchemaColumnsProjectTypePrecision(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, character_set_name, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='events_statements_summary_global_by_event_name'")
	rows := selectResultRows(result)
	var eventName, countStar []interface{}
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		switch row[0] {
		case "EVENT_NAME":
			eventName = row
		case "COUNT_STAR":
			countStar = row
		}
	}
	require.NotNil(t, eventName)
	require.Equal(t, "VARCHAR", eventName[1])
	require.Equal(t, "128", eventName[2])
	require.Equal(t, "utf8mb4", eventName[5])
	require.Equal(t, "VARCHAR(128)", eventName[6])
	require.Equal(t, "NO", eventName[7])
	require.NotNil(t, countStar)
	require.Equal(t, "BIGINT", countStar[1])
	require.Equal(t, "20", countStar[3])
	require.Equal(t, "0", countStar[4])
	require.Equal(t, "BIGINT UNSIGNED", countStar[6])
	require.Equal(t, "NO", countStar[7])
	for _, record := range result.Records {
		values := record.GetValues()
		if len(values) > 0 && values[0].String() == "COUNT_STAR" {
			require.Nil(t, values[2].Raw())
			require.Nil(t, values[5].Raw())
		}
	}
}

func TestInformationSchemaPerformanceSchemaWaitEventsUseNativeMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='events_waits_current' order by ordinal_position")
	rows := selectResultRows(result)
	require.Equal(t, [][]interface{}{
		{"THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"END_EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"EVENT_NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"},
		{"SOURCE", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"TIMER_START", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"TIMER_END", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"TIMER_WAIT", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"SPINS", "INT", "", "10", "INT UNSIGNED", "YES"},
		{"OBJECT_SCHEMA", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"OBJECT_NAME", "VARCHAR", "512", "", "VARCHAR(512)", "YES"},
		{"INDEX_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"OBJECT_TYPE", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"OBJECT_INSTANCE_BEGIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"NESTING_EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"NESTING_EVENT_TYPE", "ENUM", "11", "", "ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')", "YES"},
		{"OPERATION", "VARCHAR", "32", "", "VARCHAR(32)", "NO"},
		{"NUMBER_OF_BYTES", "BIGINT", "", "19", "BIGINT", "YES"},
		{"FLAGS", "INT", "", "10", "INT UNSIGNED", "YES"},
	}, rows)
}

func TestInformationSchemaPerformanceSchemaStageEventsUseNativeMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='events_stages_current' order by ordinal_position")
	rows := selectResultRows(result)
	require.Equal(t, [][]interface{}{
		{"THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"END_EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"EVENT_NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"},
		{"SOURCE", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"TIMER_START", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"TIMER_END", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"TIMER_WAIT", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"WORK_COMPLETED", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"WORK_ESTIMATED", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"NESTING_EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"NESTING_EVENT_TYPE", "ENUM", "11", "", "ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')", "YES"},
	}, rows)
}

func TestInformationSchemaPerformanceSchemaTransactionEventsUseMySQL84Metadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='events_transactions_current' order by ordinal_position")
	rows := selectResultRows(result)
	require.Equal(t, [][]interface{}{
		{"THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"END_EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"EVENT_NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"},
		{"STATE", "ENUM", "11", "", "ENUM('ACTIVE','COMMITTED','ROLLED BACK')", "YES"},
		{"TRX_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"GTID", "VARCHAR", "90", "", "VARCHAR(90)", "YES"},
		{"XID_FORMAT_ID", "INT", "", "10", "INT", "YES"},
		{"XID_GTRID", "VARCHAR", "130", "", "VARCHAR(130)", "YES"},
		{"XID_BQUAL", "VARCHAR", "130", "", "VARCHAR(130)", "YES"},
		{"XA_STATE", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"SOURCE", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"TIMER_START", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"TIMER_END", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"TIMER_WAIT", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"ACCESS_MODE", "ENUM", "10", "", "ENUM('READ ONLY','READ WRITE')", "YES"},
		{"ISOLATION_LEVEL", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"AUTOCOMMIT", "ENUM", "3", "", "ENUM('YES','NO')", "NO"},
		{"NUMBER_OF_SAVEPOINTS", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"NUMBER_OF_ROLLBACK_TO_SAVEPOINT", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"NUMBER_OF_RELEASE_SAVEPOINT", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"OBJECT_INSTANCE_BEGIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"NESTING_EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"NESTING_EVENT_TYPE", "ENUM", "11", "", "ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')", "YES"},
	}, rows)
}

func TestInformationSchemaVirtualPerformanceSchemaConnectionAndSetupMetadataUsesNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	metadata := func(table string) map[string][]interface{} {
		result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable, character_set_name from information_schema.columns where table_schema='performance_schema' and table_name='"+table+"' order by ordinal_position")
		rows := selectResultRows(result)
		values := make(map[string][]interface{}, len(rows))
		for _, row := range rows {
			if len(row) > 0 {
				values[fmt.Sprint(row[0])] = row
			}
		}
		return values
	}
	assertConnectionShape := func(table string, hasUser, hasHost bool) {
		columns := metadata(table)
		if hasUser {
			require.Equal(t, []interface{}{"USER", "CHAR", "32", "", "CHAR(32)", "YES", "utf8mb4"}, columns["USER"])
		}
		if hasHost {
			require.Equal(t, []interface{}{"HOST", "CHAR", "255", "", "CHAR(255)", "YES", "ascii"}, columns["HOST"])
		}
		start := "CURRENT_CONNECTIONS"
		require.Equal(t, []interface{}{start, "BIGINT", "", "19", "BIGINT", "NO", ""}, columns[start])
		require.Equal(t, []interface{}{"TOTAL_CONNECTIONS", "BIGINT", "", "19", "BIGINT", "NO", ""}, columns["TOTAL_CONNECTIONS"])
		require.Equal(t, []interface{}{"MAX_SESSION_CONTROLLED_MEMORY", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, columns["MAX_SESSION_CONTROLLED_MEMORY"])
		require.Equal(t, []interface{}{"MAX_SESSION_TOTAL_MEMORY", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, columns["MAX_SESSION_TOTAL_MEMORY"])
	}
	assertConnectionShape("accounts", true, true)
	assertConnectionShape("hosts", false, true)
	assertConnectionShape("users", true, false)

	consumers := metadata("setup_consumers")
	require.Equal(t, []interface{}{"NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO", "utf8mb4"}, consumers["NAME"])
	require.Equal(t, []interface{}{"ENABLED", "ENUM", "3", "", "ENUM('YES','NO')", "NO", "utf8mb4"}, consumers["ENABLED"])

	instruments := metadata("setup_instruments")
	require.Equal(t, []interface{}{"NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO", "utf8mb4"}, instruments["NAME"])
	require.Equal(t, []interface{}{"ENABLED", "ENUM", "3", "", "ENUM('YES','NO')", "NO", "utf8mb4"}, instruments["ENABLED"])
	require.Equal(t, []interface{}{"TIMED", "ENUM", "3", "", "ENUM('YES','NO')", "YES", "utf8mb4"}, instruments["TIMED"])
	require.Equal(t, []interface{}{"VOLATILITY", "INT", "", "10", "INT", "NO", ""}, instruments["VOLATILITY"])
	require.Equal(t, []interface{}{"DOCUMENTATION", "LONGTEXT", "4294967295", "", "LONGTEXT", "YES", "utf8mb4"}, instruments["DOCUMENTATION"])

	actors := metadata("setup_actors")
	require.Equal(t, []interface{}{"HOST", "CHAR", "255", "", "CHAR(255)", "NO", "ascii"}, actors["HOST"])
	require.Equal(t, []interface{}{"USER", "CHAR", "32", "", "CHAR(32)", "NO", "utf8mb4"}, actors["USER"])
	require.Equal(t, []interface{}{"ROLE", "CHAR", "32", "", "CHAR(32)", "NO", "utf8mb4"}, actors["ROLE"])
	require.Equal(t, []interface{}{"ENABLED", "ENUM", "3", "", "ENUM('YES','NO')", "NO", "utf8mb4"}, actors["ENABLED"])
	require.Equal(t, []interface{}{"HISTORY", "ENUM", "3", "", "ENUM('YES','NO')", "NO", "utf8mb4"}, actors["HISTORY"])

	objects := metadata("setup_objects")
	require.Equal(t, []interface{}{"OBJECT_TYPE", "ENUM", "9", "", "ENUM('EVENT','FUNCTION','PROCEDURE','TABLE','TRIGGER')", "NO", "utf8mb4"}, objects["OBJECT_TYPE"])
	require.Equal(t, []interface{}{"OBJECT_SCHEMA", "VARCHAR", "64", "", "VARCHAR(64)", "YES", "utf8mb4"}, objects["OBJECT_SCHEMA"])
	require.Equal(t, []interface{}{"OBJECT_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO", "utf8mb4"}, objects["OBJECT_NAME"])
	require.Equal(t, []interface{}{"ENABLED", "ENUM", "3", "", "ENUM('YES','NO')", "NO", "utf8mb4"}, objects["ENABLED"])
	require.Equal(t, []interface{}{"TIMED", "ENUM", "3", "", "ENUM('YES','NO')", "NO", "utf8mb4"}, objects["TIMED"])

	meters := metadata("setup_meters")
	require.Equal(t, []interface{}{"NAME", "VARCHAR", "63", "", "VARCHAR(63)", "NO", "utf8mb4"}, meters["NAME"])
	require.Equal(t, []interface{}{"FREQUENCY", "MEDIUMINT", "", "8", "MEDIUMINT UNSIGNED", "NO", ""}, meters["FREQUENCY"])
	require.Equal(t, []interface{}{"ENABLED", "ENUM", "3", "", "ENUM('YES','NO')", "NO", "utf8mb4"}, meters["ENABLED"])
	require.Equal(t, []interface{}{"DESCRIPTION", "VARCHAR", "1023", "", "VARCHAR(1023)", "YES", "utf8mb4"}, meters["DESCRIPTION"])

	metrics := metadata("setup_metrics")
	require.Equal(t, []interface{}{"NAME", "VARCHAR", "63", "", "VARCHAR(63)", "NO", "utf8mb4"}, metrics["NAME"])
	require.Equal(t, []interface{}{"METER", "VARCHAR", "63", "", "VARCHAR(63)", "NO", "utf8mb4"}, metrics["METER"])
	require.Equal(t, []interface{}{"METRIC_TYPE", "ENUM", "20", "", "ENUM('ASYNC COUNTER','ASYNC UPDOWN COUNTER','ASYNC GAUGE COUNTER')", "NO", "utf8mb4"}, metrics["METRIC_TYPE"])
	require.Equal(t, []interface{}{"NUM_TYPE", "ENUM", "7", "", "ENUM('INTEGER','DOUBLE')", "NO", "utf8mb4"}, metrics["NUM_TYPE"])
	require.Equal(t, []interface{}{"UNIT", "VARCHAR", "63", "", "VARCHAR(63)", "YES", "utf8mb4"}, metrics["UNIT"])
	require.Equal(t, []interface{}{"DESCRIPTION", "VARCHAR", "1023", "", "VARCHAR(1023)", "YES", "utf8mb4"}, metrics["DESCRIPTION"])

	loggers := metadata("setup_loggers")
	require.Equal(t, []interface{}{"NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO", "utf8mb4"}, loggers["NAME"])
	require.Equal(t, []interface{}{"LEVEL", "ENUM", "5", "", "ENUM('none','error','warn','info','debug')", "NO", "utf8mb4"}, loggers["LEVEL"])
	require.Equal(t, []interface{}{"DESCRIPTION", "VARCHAR", "1023", "", "VARCHAR(1023)", "YES", "utf8mb4"}, loggers["DESCRIPTION"])

	tls := metadata("tls_channel_status")
	require.Equal(t, []interface{}{"CHANNEL", "VARCHAR", "128", "", "VARCHAR(128)", "NO", "utf8mb4"}, tls["CHANNEL"])
	require.Equal(t, []interface{}{"PROPERTY", "VARCHAR", "128", "", "VARCHAR(128)", "NO", "utf8mb4"}, tls["PROPERTY"])
	require.Equal(t, []interface{}{"VALUE", "VARCHAR", "2048", "", "VARCHAR(2048)", "NO", "utf8mb4"}, tls["VALUE"])

	cloneStatus := metadata("clone_status")
	require.Equal(t, []interface{}{"ID", "INT", "", "10", "INT", "YES", ""}, cloneStatus["ID"])
	require.Equal(t, []interface{}{"PID", "INT", "", "10", "INT", "YES", ""}, cloneStatus["PID"])
	require.Equal(t, []interface{}{"STATE", "CHAR", "16", "", "CHAR(16)", "YES", "utf8mb4"}, cloneStatus["STATE"])
	require.Equal(t, []interface{}{"BEGIN_TIME", "TIMESTAMP", "", "", "TIMESTAMP(3)", "YES", ""}, cloneStatus["BEGIN_TIME"])
	require.Equal(t, []interface{}{"END_TIME", "TIMESTAMP", "", "", "TIMESTAMP(3)", "YES", ""}, cloneStatus["END_TIME"])
	require.Equal(t, []interface{}{"SOURCE", "VARCHAR", "512", "", "VARCHAR(512)", "YES", "utf8mb4"}, cloneStatus["SOURCE"])
	require.Equal(t, []interface{}{"DESTINATION", "VARCHAR", "512", "", "VARCHAR(512)", "YES", "utf8mb4"}, cloneStatus["DESTINATION"])
	require.Equal(t, []interface{}{"ERROR_NO", "INT", "", "10", "INT", "YES", ""}, cloneStatus["ERROR_NO"])
	require.Equal(t, []interface{}{"ERROR_MESSAGE", "VARCHAR", "512", "", "VARCHAR(512)", "YES", "utf8mb4"}, cloneStatus["ERROR_MESSAGE"])
	require.Equal(t, []interface{}{"BINLOG_FILE", "VARCHAR", "512", "", "VARCHAR(512)", "YES", "utf8mb4"}, cloneStatus["BINLOG_FILE"])
	require.Equal(t, []interface{}{"BINLOG_POSITION", "BIGINT", "", "19", "BIGINT", "YES", ""}, cloneStatus["BINLOG_POSITION"])
	require.Equal(t, []interface{}{"GTID_EXECUTED", "LONGTEXT", "4294967295", "", "LONGTEXT", "YES", "utf8mb4"}, cloneStatus["GTID_EXECUTED"])

	cloneProgress := metadata("clone_progress")
	require.Equal(t, []interface{}{"ID", "INT", "", "10", "INT", "YES", ""}, cloneProgress["ID"])
	require.Equal(t, []interface{}{"STAGE", "CHAR", "32", "", "CHAR(32)", "YES", "utf8mb4"}, cloneProgress["STAGE"])
	require.Equal(t, []interface{}{"STATE", "CHAR", "16", "", "CHAR(16)", "YES", "utf8mb4"}, cloneProgress["STATE"])
	require.Equal(t, []interface{}{"BEGIN_TIME", "TIMESTAMP", "", "", "TIMESTAMP(6)", "YES", ""}, cloneProgress["BEGIN_TIME"])
	require.Equal(t, []interface{}{"END_TIME", "TIMESTAMP", "", "", "TIMESTAMP(6)", "YES", ""}, cloneProgress["END_TIME"])
	require.Equal(t, []interface{}{"THREADS", "INT", "", "10", "INT", "YES", ""}, cloneProgress["THREADS"])
	require.Equal(t, []interface{}{"ESTIMATE", "BIGINT", "", "19", "BIGINT", "YES", ""}, cloneProgress["ESTIMATE"])
	require.Equal(t, []interface{}{"DATA", "BIGINT", "", "19", "BIGINT", "YES", ""}, cloneProgress["DATA"])
	require.Equal(t, []interface{}{"NETWORK", "BIGINT", "", "19", "BIGINT", "YES", ""}, cloneProgress["NETWORK"])
	require.Equal(t, []interface{}{"DATA_SPEED", "INT", "", "10", "INT", "YES", ""}, cloneProgress["DATA_SPEED"])
	require.Equal(t, []interface{}{"NETWORK_SPEED", "INT", "", "10", "INT", "YES", ""}, cloneProgress["NETWORK_SPEED"])

	udfs := metadata("user_defined_functions")
	require.Equal(t, []interface{}{"UDF_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO", "utf8mb4"}, udfs["UDF_NAME"])
	require.Equal(t, []interface{}{"UDF_RETURN_TYPE", "VARCHAR", "20", "", "VARCHAR(20)", "NO", "utf8mb4"}, udfs["UDF_RETURN_TYPE"])
	require.Equal(t, []interface{}{"UDF_TYPE", "VARCHAR", "20", "", "VARCHAR(20)", "NO", "utf8mb4"}, udfs["UDF_TYPE"])
	require.Equal(t, []interface{}{"UDF_LIBRARY", "VARCHAR", "1024", "", "VARCHAR(1024)", "YES", "utf8mb4"}, udfs["UDF_LIBRARY"])
	require.Equal(t, []interface{}{"UDF_USAGE_COUNT", "BIGINT", "", "19", "BIGINT", "YES", ""}, udfs["UDF_USAGE_COUNT"])

	keyringStatus := metadata("keyring_component_status")
	require.Equal(t, []interface{}{"STATUS_KEY", "VARCHAR", "256", "", "VARCHAR(256)", "NO", "utf8mb4"}, keyringStatus["STATUS_KEY"])
	require.Equal(t, []interface{}{"STATUS_VALUE", "VARCHAR", "1024", "", "VARCHAR(1024)", "NO", "utf8mb4"}, keyringStatus["STATUS_VALUE"])

	keyringKeys := metadata("keyring_keys")
	require.Equal(t, []interface{}{"KEY_ID", "VARCHAR", "255", "", "VARCHAR(255)", "NO", "utf8mb4"}, keyringKeys["KEY_ID"])
	require.Equal(t, []interface{}{"KEY_OWNER", "VARCHAR", "255", "", "VARCHAR(255)", "YES", "utf8mb4"}, keyringKeys["KEY_OWNER"])
	require.Equal(t, []interface{}{"BACKEND_KEY_ID", "VARCHAR", "255", "", "VARCHAR(255)", "YES", "utf8mb4"}, keyringKeys["BACKEND_KEY_ID"])

	programSummary := metadata("events_statements_summary_by_program")
	require.Equal(t, []interface{}{"OBJECT_TYPE", "ENUM", "9", "", "ENUM('EVENT','FUNCTION','PROCEDURE','TABLE','TRIGGER')", "NO", "utf8mb4"}, programSummary["OBJECT_TYPE"])
	require.Equal(t, []interface{}{"OBJECT_SCHEMA", "VARCHAR", "64", "", "VARCHAR(64)", "NO", "utf8mb4"}, programSummary["OBJECT_SCHEMA"])
	require.Equal(t, []interface{}{"OBJECT_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO", "utf8mb4"}, programSummary["OBJECT_NAME"])

	digestSummary := metadata("events_statements_summary_by_digest")
	for _, column := range []string{"QUANTILE_95", "QUANTILE_99", "QUANTILE_999"} {
		require.Equal(t, []interface{}{column, "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, digestSummary[column])
	}

	objectsSummary := metadata("objects_summary_global_by_type")
	require.Equal(t, []interface{}{"OBJECT_TYPE", "VARCHAR", "64", "", "VARCHAR(64)", "YES", "utf8mb4"}, objectsSummary["OBJECT_TYPE"])
	tableLockSummary := metadata("table_lock_waits_summary_by_table")
	require.Equal(t, []interface{}{"OBJECT_TYPE", "VARCHAR", "64", "", "VARCHAR(64)", "YES", "utf8mb4"}, tableLockSummary["OBJECT_TYPE"])
	require.Equal(t, []interface{}{"COUNT_READ_NORMAL", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, tableLockSummary["COUNT_READ_NORMAL"])
	require.Equal(t, []interface{}{"COUNT_WRITE_EXTERNAL", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, tableLockSummary["COUNT_WRITE_EXTERNAL"])

	metadataLocks := metadata("metadata_locks")
	require.Equal(t, []interface{}{"OBJECT_TYPE", "VARCHAR", "64", "", "VARCHAR(64)", "NO", "utf8mb4"}, metadataLocks["OBJECT_TYPE"])
	require.Equal(t, []interface{}{"OBJECT_SCHEMA", "VARCHAR", "64", "", "VARCHAR(64)", "YES", "utf8mb4"}, metadataLocks["OBJECT_SCHEMA"])
	require.Equal(t, []interface{}{"OBJECT_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES", "utf8mb4"}, metadataLocks["OBJECT_NAME"])
	require.Equal(t, []interface{}{"COLUMN_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES", "utf8mb4"}, metadataLocks["COLUMN_NAME"])
	require.Equal(t, []interface{}{"OBJECT_INSTANCE_BEGIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, metadataLocks["OBJECT_INSTANCE_BEGIN"])
	require.Equal(t, []interface{}{"LOCK_TYPE", "VARCHAR", "32", "", "VARCHAR(32)", "NO", "utf8mb4"}, metadataLocks["LOCK_TYPE"])
	require.Equal(t, []interface{}{"LOCK_DURATION", "VARCHAR", "32", "", "VARCHAR(32)", "NO", "utf8mb4"}, metadataLocks["LOCK_DURATION"])
	require.Equal(t, []interface{}{"LOCK_STATUS", "VARCHAR", "32", "", "VARCHAR(32)", "NO", "utf8mb4"}, metadataLocks["LOCK_STATUS"])
	require.Equal(t, []interface{}{"SOURCE", "VARCHAR", "64", "", "VARCHAR(64)", "YES", "utf8mb4"}, metadataLocks["SOURCE"])
	require.Equal(t, []interface{}{"OWNER_THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES", ""}, metadataLocks["OWNER_THREAD_ID"])
	require.Equal(t, []interface{}{"OWNER_EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES", ""}, metadataLocks["OWNER_EVENT_ID"])

	threads := metadata("setup_threads")
	require.Equal(t, []interface{}{"NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO", "utf8mb4"}, threads["NAME"])
	require.Equal(t, []interface{}{"ENABLED", "ENUM", "3", "", "ENUM('YES','NO')", "NO", "utf8mb4"}, threads["ENABLED"])
	require.Equal(t, []interface{}{"HISTORY", "ENUM", "3", "", "ENUM('YES','NO')", "NO", "utf8mb4"}, threads["HISTORY"])
	require.Equal(t, []interface{}{"PROPERTIES", "SET", "14", "", "SET('singleton','user')", "NO", "utf8mb4"}, threads["PROPERTIES"])
	require.Equal(t, []interface{}{"VOLATILITY", "INT", "", "10", "INT", "NO", ""}, threads["VOLATILITY"])
	require.Equal(t, []interface{}{"DOCUMENTATION", "LONGTEXT", "4294967295", "", "LONGTEXT", "YES", "utf8mb4"}, threads["DOCUMENTATION"])

	applierStatus := metadata("replication_applier_status")
	require.Equal(t, []interface{}{"CHANNEL_NAME", "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb4"}, applierStatus["CHANNEL_NAME"])
	require.Equal(t, []interface{}{"SERVICE_STATE", "ENUM", "3", "", "ENUM('ON','OFF')", "NO", "utf8mb4"}, applierStatus["SERVICE_STATE"])
	require.Equal(t, []interface{}{"REMAINING_DELAY", "INT", "", "10", "INT UNSIGNED", "YES", ""}, applierStatus["REMAINING_DELAY"])
	require.Equal(t, []interface{}{"COUNT_TRANSACTIONS_RETRIES", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, applierStatus["COUNT_TRANSACTIONS_RETRIES"])

	connectionConfig := metadata("replication_connection_configuration")
	require.Equal(t, []interface{}{"CHANNEL_NAME", "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb4"}, connectionConfig["CHANNEL_NAME"])
	require.Equal(t, []interface{}{"HOST", "CHAR", "255", "", "CHAR(255)", "NO", "ascii"}, connectionConfig["HOST"])
	require.Equal(t, []interface{}{"PORT", "INT", "", "10", "INT", "NO", ""}, connectionConfig["PORT"])
	require.Equal(t, []interface{}{"AUTO_POSITION", "ENUM", "1", "", "ENUM('1','0')", "NO", "utf8mb4"}, connectionConfig["AUTO_POSITION"])
	require.Equal(t, []interface{}{"GET_PUBLIC_KEY", "ENUM", "3", "", "ENUM('YES','NO')", "NO", "utf8mb4"}, connectionConfig["GET_PUBLIC_KEY"])

	connectionStatus := metadata("replication_connection_status")
	require.Equal(t, []interface{}{"GROUP_NAME", "CHAR", "36", "", "CHAR(36)", "NO", "utf8mb4"}, connectionStatus["GROUP_NAME"])
	require.Equal(t, []interface{}{"THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES", ""}, connectionStatus["THREAD_ID"])
	require.Equal(t, []interface{}{"SERVICE_STATE", "ENUM", "10", "", "ENUM('ON','OFF','CONNECTING')", "NO", "utf8mb4"}, connectionStatus["SERVICE_STATE"])
	require.Equal(t, []interface{}{"LAST_HEARTBEAT_TIMESTAMP", "TIMESTAMP", "", "", "TIMESTAMP(6)", "NO", ""}, connectionStatus["LAST_HEARTBEAT_TIMESTAMP"])

	coordinator := metadata("replication_applier_status_by_coordinator")
	require.Equal(t, []interface{}{"THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES", ""}, coordinator["THREAD_ID"])
	require.Equal(t, []interface{}{"LAST_PROCESSED_TRANSACTION", "CHAR", "90", "", "CHAR(90)", "YES", "utf8mb4"}, coordinator["LAST_PROCESSED_TRANSACTION"])
	require.Equal(t, []interface{}{"LAST_PROCESSED_TRANSACTION_END_BUFFER_TIMESTAMP", "TIMESTAMP", "", "", "TIMESTAMP(6)", "NO", ""}, coordinator["LAST_PROCESSED_TRANSACTION_END_BUFFER_TIMESTAMP"])

	worker := metadata("replication_applier_status_by_worker")
	require.Equal(t, []interface{}{"WORKER_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, worker["WORKER_ID"])
	require.Equal(t, []interface{}{"LAST_APPLIED_TRANSACTION_RETRIES_COUNT", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, worker["LAST_APPLIED_TRANSACTION_RETRIES_COUNT"])
	require.Equal(t, []interface{}{"APPLYING_TRANSACTION_LAST_TRANSIENT_ERROR_MESSAGE", "VARCHAR", "1024", "", "VARCHAR(1024)", "YES", "utf8mb4"}, worker["APPLYING_TRANSACTION_LAST_TRANSIENT_ERROR_MESSAGE"])

	applierFilters := metadata("replication_applier_filters")
	require.Equal(t, []interface{}{"CHANNEL_NAME", "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb4"}, applierFilters["CHANNEL_NAME"])
	require.Equal(t, []interface{}{"FILTER_NAME", "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb4"}, applierFilters["FILTER_NAME"])
	require.Equal(t, []interface{}{"FILTER_RULE", "LONGTEXT", "4294967295", "", "LONGTEXT", "NO", "utf8mb4"}, applierFilters["FILTER_RULE"])
	require.Equal(t, []interface{}{"CONFIGURED_BY", "ENUM", "37", "", "ENUM('STARTUP_OPTIONS','CHANGE_REPLICATION_FILTER','STARTUP_OPTIONS_FOR_CHANNEL','CHANGE_REPLICATION_FILTER_FOR_CHANNEL')", "NO", "utf8mb4"}, applierFilters["CONFIGURED_BY"])
	require.Equal(t, []interface{}{"ACTIVE_SINCE", "TIMESTAMP", "", "", "TIMESTAMP(6)", "NO", ""}, applierFilters["ACTIVE_SINCE"])
	require.Equal(t, []interface{}{"COUNTER", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, applierFilters["COUNTER"])

	globalFilters := metadata("replication_applier_global_filters")
	require.Equal(t, []interface{}{"FILTER_NAME", "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb4"}, globalFilters["FILTER_NAME"])
	require.Equal(t, []interface{}{"FILTER_RULE", "LONGTEXT", "4294967295", "", "LONGTEXT", "NO", "utf8mb4"}, globalFilters["FILTER_RULE"])
	require.Equal(t, []interface{}{"CONFIGURED_BY", "ENUM", "25", "", "ENUM('STARTUP_OPTIONS','CHANGE_REPLICATION_FILTER')", "NO", "utf8mb4"}, globalFilters["CONFIGURED_BY"])
	require.Equal(t, []interface{}{"ACTIVE_SINCE", "TIMESTAMP", "", "", "TIMESTAMP(6)", "NO", ""}, globalFilters["ACTIVE_SINCE"])

	failover := metadata("replication_asynchronous_connection_failover")
	require.Equal(t, []interface{}{"CHANNEL_NAME", "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb3"}, failover["CHANNEL_NAME"])
	require.Equal(t, []interface{}{"HOST", "CHAR", "255", "", "CHAR(255)", "NO", "ascii"}, failover["HOST"])
	require.Equal(t, []interface{}{"PORT", "INT", "", "10", "INT", "NO", ""}, failover["PORT"])
	require.Equal(t, []interface{}{"NETWORK_NAMESPACE", "CHAR", "64", "", "CHAR(64)", "YES", "utf8mb4"}, failover["NETWORK_NAMESPACE"])
	require.Equal(t, []interface{}{"WEIGHT", "INT", "", "10", "INT UNSIGNED", "NO", ""}, failover["WEIGHT"])
	require.Equal(t, []interface{}{"MANAGED_NAME", "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb3"}, failover["MANAGED_NAME"])

	managedFailover := metadata("replication_asynchronous_connection_failover_managed")
	require.Equal(t, []interface{}{"CHANNEL_NAME", "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb3"}, managedFailover["CHANNEL_NAME"])
	require.Equal(t, []interface{}{"MANAGED_NAME", "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb3"}, managedFailover["MANAGED_NAME"])
	require.Equal(t, []interface{}{"MANAGED_TYPE", "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb3"}, managedFailover["MANAGED_TYPE"])
	require.Equal(t, []interface{}{"CONFIGURATION", "JSON", "", "", "JSON", "YES", ""}, managedFailover["CONFIGURATION"])

	groupStats := metadata("replication_group_member_stats")
	require.Equal(t, []interface{}{"CHANNEL_NAME", "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb4"}, groupStats["CHANNEL_NAME"])
	require.Equal(t, []interface{}{"VIEW_ID", "CHAR", "60", "", "CHAR(60)", "NO", "utf8mb4"}, groupStats["VIEW_ID"])
	require.Equal(t, []interface{}{"MEMBER_ID", "CHAR", "36", "", "CHAR(36)", "NO", "utf8mb4"}, groupStats["MEMBER_ID"])
	for _, name := range []string{
		"COUNT_TRANSACTIONS_IN_QUEUE", "COUNT_TRANSACTIONS_CHECKED",
		"COUNT_CONFLICTS_DETECTED", "COUNT_TRANSACTIONS_ROWS_VALIDATING",
		"COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE",
		"COUNT_TRANSACTIONS_REMOTE_APPLIED", "COUNT_TRANSACTIONS_LOCAL_PROPOSED",
		"COUNT_TRANSACTIONS_LOCAL_ROLLBACK",
	} {
		require.Equal(t, []interface{}{name, "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, groupStats[name])
	}
	require.Equal(t, []interface{}{"TRANSACTIONS_COMMITTED_ALL_MEMBERS", "LONGTEXT", "4294967295", "", "LONGTEXT", "NO", "utf8mb4"}, groupStats["TRANSACTIONS_COMMITTED_ALL_MEMBERS"])
	require.Equal(t, []interface{}{"LAST_CONFLICT_FREE_TRANSACTION", "TEXT", "65535", "", "TEXT", "NO", "utf8mb4"}, groupStats["LAST_CONFLICT_FREE_TRANSACTION"])

	groupMembers := metadata("replication_group_members")
	require.Equal(t, []interface{}{"CHANNEL_NAME", "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb4"}, groupMembers["CHANNEL_NAME"])
	require.Equal(t, []interface{}{"MEMBER_ID", "CHAR", "36", "", "CHAR(36)", "NO", "utf8mb4"}, groupMembers["MEMBER_ID"])
	require.Equal(t, []interface{}{"MEMBER_HOST", "CHAR", "255", "", "CHAR(255)", "NO", "ascii"}, groupMembers["MEMBER_HOST"])
	require.Equal(t, []interface{}{"MEMBER_PORT", "INT", "", "10", "INT", "YES", ""}, groupMembers["MEMBER_PORT"])
	for _, name := range []string{"MEMBER_STATE", "MEMBER_ROLE", "MEMBER_VERSION", "MEMBER_COMMUNICATION_STACK"} {
		require.Equal(t, []interface{}{name, "CHAR", "64", "", "CHAR(64)", "NO", "utf8mb4"}, groupMembers[name])
	}

	groupCommunication := metadata("replication_group_communication_information")
	require.Equal(t, []interface{}{"WRITE_CONCURRENCY", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, groupCommunication["WRITE_CONCURRENCY"])
	for _, name := range []string{"PROTOCOL_VERSION", "WRITE_CONSENSUS_LEADERS_PREFERRED", "WRITE_CONSENSUS_LEADERS_ACTUAL", "MEMBER_FAILURE_SUSPICIONS_COUNT"} {
		require.Equal(t, []interface{}{name, "LONGTEXT", "4294967295", "", "LONGTEXT", "NO", "utf8mb4"}, groupCommunication[name])
	}
	require.Equal(t, []interface{}{"WRITE_CONSENSUS_SINGLE_LEADER_CAPABLE", "TINYINT", "", "3", "TINYINT(1)", "NO", ""}, groupCommunication["WRITE_CONSENSUS_SINGLE_LEADER_CAPABLE"])

	groupConfigurationVersion := metadata("replication_group_configuration_version")
	require.Equal(t, []interface{}{"NAME", "CHAR", "255", "", "CHAR(255)", "NO", "ascii"}, groupConfigurationVersion["NAME"])
	require.Equal(t, []interface{}{"VERSION", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO", ""}, groupConfigurationVersion["VERSION"])

	groupActions := metadata("replication_group_member_actions")
	require.Equal(t, []interface{}{"NAME", "CHAR", "255", "", "CHAR(255)", "NO", "ascii"}, groupActions["NAME"])
	require.Equal(t, []interface{}{"EVENT", "CHAR", "64", "", "CHAR(64)", "NO", "ascii"}, groupActions["EVENT"])
	require.Equal(t, []interface{}{"ENABLED", "TINYINT", "", "3", "TINYINT(1)", "NO", ""}, groupActions["ENABLED"])
	require.Equal(t, []interface{}{"TYPE", "CHAR", "64", "", "CHAR(64)", "NO", "ascii"}, groupActions["TYPE"])
	require.Equal(t, []interface{}{"PRIORITY", "TINYINT", "", "4", "TINYINT UNSIGNED", "NO", ""}, groupActions["PRIORITY"])
	require.Equal(t, []interface{}{"ERROR_HANDLING", "CHAR", "64", "", "CHAR(64)", "NO", "ascii"}, groupActions["ERROR_HANDLING"])
}

func TestInformationSchemaVirtualBinaryLogCompressionStatsUsesMySQL84Shape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='binary_log_transaction_compression_stats' order by ordinal_position")
	rows := selectResultRows(result)
	require.Len(t, rows, 14)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			byName[fmt.Sprint(row[0])] = row
		}
	}
	require.Equal(t, []interface{}{"LOG_TYPE", "ENUM", "6", "", "", "ENUM('BINARY','RELAY')", "NO"}, byName["LOG_TYPE"])
	require.Equal(t, []interface{}{"COMPRESSION_TYPE", "VARCHAR", "64", "", "", "VARCHAR(64)", "NO"}, byName["COMPRESSION_TYPE"])
	require.Equal(t, []interface{}{"TRANSACTION_COUNTER", "BIGINT", "", "20", "0", "BIGINT UNSIGNED", "NO"}, byName["TRANSACTION_COUNTER"])
	require.Equal(t, []interface{}{"COMPRESSION_PERCENTAGE", "SMALLINT", "", "5", "0", "SMALLINT", "NO"}, byName["COMPRESSION_PERCENTAGE"])
	require.Equal(t, []interface{}{"FIRST_TRANSACTION_ID", "TEXT", "65535", "", "", "TEXT", "YES"}, byName["FIRST_TRANSACTION_ID"])
	require.Equal(t, []interface{}{"FIRST_TRANSACTION_TIMESTAMP", "TIMESTAMP", "", "", "", "TIMESTAMP(6)", "YES"}, byName["FIRST_TRANSACTION_TIMESTAMP"])
	require.Equal(t, []interface{}{"LAST_TRANSACTION_ID", "TEXT", "65535", "", "", "TEXT", "YES"}, byName["LAST_TRANSACTION_ID"])
	require.Equal(t, []interface{}{"LAST_TRANSACTION_TIMESTAMP", "TIMESTAMP", "", "", "", "TIMESTAMP(6)", "YES"}, byName["LAST_TRANSACTION_TIMESTAMP"])
}

func TestInformationSchemaVirtualLogStatusUsesMySQL84Shape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, column_type, is_nullable, character_set_name from information_schema.columns where table_schema='performance_schema' and table_name='log_status' order by ordinal_position")
	rows := selectResultRows(result)
	require.Len(t, rows, 4)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			byName[fmt.Sprint(row[0])] = row
		}
	}
	require.Equal(t, []interface{}{"SERVER_UUID", "CHAR", "36", "CHAR(36)", "NO", "utf8mb4"}, byName["SERVER_UUID"])
	for _, name := range []string{"LOCAL", "REPLICATION", "STORAGE_ENGINES"} {
		require.Equal(t, []interface{}{name, "JSON", "", "JSON", "NO", ""}, byName[name])
	}
}

func TestInformationSchemaVirtualErrorLogUsesMySQL84Shape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='error_log' order by ordinal_position")
	rows := selectResultRows(result)
	require.Len(t, rows, 6)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			byName[fmt.Sprint(row[0])] = row
		}
	}
	require.Equal(t, []interface{}{"LOGGED", "TIMESTAMP", "", "", "TIMESTAMP(6)", "NO"}, byName["LOGGED"])
	require.Equal(t, []interface{}{"THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, byName["THREAD_ID"])
	require.Equal(t, []interface{}{"PRIO", "ENUM", "7", "", "ENUM('System','Error','Warning','Note')", "NO"}, byName["PRIO"])
	require.Equal(t, []interface{}{"ERROR_CODE", "VARCHAR", "10", "", "VARCHAR(10)", "YES"}, byName["ERROR_CODE"])
	require.Equal(t, []interface{}{"SUBSYSTEM", "VARCHAR", "7", "", "VARCHAR(7)", "YES"}, byName["SUBSYSTEM"])
	require.Equal(t, []interface{}{"DATA", "TEXT", "65535", "", "TEXT", "NO"}, byName["DATA"])
}

func TestInformationSchemaVirtualRedoLogFilesUsesMySQL84Shape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='innodb_redo_log_files' order by ordinal_position")
	rows := selectResultRows(result)
	require.Len(t, rows, 7)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			byName[fmt.Sprint(row[0])] = row
		}
	}
	for _, name := range []string{"FILE_ID", "START_LSN", "END_LSN", "SIZE_IN_BYTES"} {
		require.Equal(t, []interface{}{name, "BIGINT", "", "19", "BIGINT", "NO"}, byName[name])
	}
	require.Equal(t, []interface{}{"FILE_NAME", "VARCHAR", "2000", "", "VARCHAR(2000)", "NO"}, byName["FILE_NAME"])
	require.Equal(t, []interface{}{"IS_FULL", "TINYINT", "", "3", "TINYINT", "NO"}, byName["IS_FULL"])
	require.Equal(t, []interface{}{"CONSUMER_LEVEL", "INT", "", "10", "INT", "NO"}, byName["CONSUMER_LEVEL"])
}

func TestInformationSchemaVirtualComponentSchedulerTasksUsesMySQL84Shape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='component_scheduler_tasks' order by ordinal_position")
	rows := selectResultRows(result)
	require.Len(t, rows, 6)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			byName[fmt.Sprint(row[0])] = row
		}
	}
	require.Equal(t, []interface{}{"NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, byName["NAME"])
	require.Equal(t, []interface{}{"STATUS", "ENUM", "7", "", "ENUM('RUNNING','WAITING')", "NO"}, byName["STATUS"])
	require.Equal(t, []interface{}{"COMMENT", "VARCHAR", "1024", "", "VARCHAR(1024)", "NO"}, byName["COMMENT"])
	require.Equal(t, []interface{}{"INTERVAL_SECONDS", "INT", "", "10", "INT", "NO"}, byName["INTERVAL_SECONDS"])
	for _, name := range []string{"TIMES_RUN", "TIMES_FAILED"} {
		require.Equal(t, []interface{}{name, "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, byName[name])
	}
}

func TestInformationSchemaVirtualStatementEventColumnsUseNativeTextAndDigestShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, character_set_name, column_type from information_schema.columns where table_schema='performance_schema' and table_name='events_statements_current'")
	rows := selectResultRows(result)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			byName[fmt.Sprint(row[0])] = row
		}
	}
	require.Equal(t, []interface{}{"EVENT_NAME", "VARCHAR", "128", "utf8mb4", "VARCHAR(128)"}, byName["EVENT_NAME"])
	require.Equal(t, []interface{}{"DIGEST", "VARCHAR", "64", "utf8mb4", "VARCHAR(64)"}, byName["DIGEST"])
	sqlText := byName["SQL_TEXT"]
	require.Equal(t, "SQL_TEXT", sqlText[0])
	require.Equal(t, "LONGTEXT", sqlText[1])
	require.Equal(t, "LONGTEXT", sqlText[4])
	require.Equal(t, "4294967295", fmt.Sprint(sqlText[2]))
	require.Equal(t, "utf8mb4", sqlText[3])
}

func TestInformationSchemaPerformanceSchemaStatementEventsUseMySQL84Metadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='events_statements_current' order by ordinal_position")
	rows := selectResultRows(result)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			byName[fmt.Sprint(row[0])] = row
		}
	}
	require.Equal(t, []interface{}{"THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, byName["THREAD_ID"])
	require.Equal(t, []interface{}{"END_EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, byName["END_EVENT_ID"])
	require.Equal(t, []interface{}{"EVENT_NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"}, byName["EVENT_NAME"])
	require.Equal(t, []interface{}{"SOURCE", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, byName["SOURCE"])
	require.Equal(t, []interface{}{"LOCK_TIME", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, byName["LOCK_TIME"])
	require.Equal(t, []interface{}{"SQL_TEXT", "LONGTEXT", "4294967295", "", "LONGTEXT", "YES"}, byName["SQL_TEXT"])
	require.Equal(t, []interface{}{"DIGEST", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, byName["DIGEST"])
	require.Equal(t, []interface{}{"MYSQL_ERRNO", "INT", "", "10", "INT", "YES"}, byName["MYSQL_ERRNO"])
	require.Equal(t, []interface{}{"MESSAGE_TEXT", "VARCHAR", "128", "", "VARCHAR(128)", "YES"}, byName["MESSAGE_TEXT"])
	for _, name := range []string{"ERRORS", "WARNINGS", "ROWS_AFFECTED", "ROWS_SENT", "ROWS_EXAMINED", "CPU_TIME", "MAX_CONTROLLED_MEMORY", "MAX_TOTAL_MEMORY"} {
		require.Equal(t, []interface{}{name, "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, byName[name])
	}
	require.Equal(t, []interface{}{"NESTING_EVENT_TYPE", "ENUM", "11", "", "ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')", "YES"}, byName["NESTING_EVENT_TYPE"])
	require.Equal(t, []interface{}{"NESTING_EVENT_LEVEL", "INT", "", "10", "INT", "YES"}, byName["NESTING_EVENT_LEVEL"])
	require.Equal(t, []interface{}{"EXECUTION_ENGINE", "ENUM", "9", "", "ENUM('PRIMARY','SECONDARY')", "YES"}, byName["EXECUTION_ENGINE"])
}

func TestInformationSchemaVirtualColumnsTableUsesNativeMetadataShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='columns' and column_name in ('TABLE_SCHEMA','COLUMN_NAME','ORDINAL_POSITION','COLUMN_DEFAULT','SRS_ID') order by ordinal_position")
	rows := selectResultRows(result)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			byName[fmt.Sprint(row[0])] = row
		}
	}
	require.Equal(t, []interface{}{"TABLE_SCHEMA", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, byName["TABLE_SCHEMA"])
	require.Equal(t, []interface{}{"COLUMN_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, byName["COLUMN_NAME"])
	require.Equal(t, []interface{}{"ORDINAL_POSITION", "INT", "", "10", "INT UNSIGNED", "NO"}, byName["ORDINAL_POSITION"])
	require.Equal(t, []interface{}{"COLUMN_DEFAULT", "TEXT", "65535", "", "TEXT", "YES"}, byName["COLUMN_DEFAULT"])
	require.Equal(t, []interface{}{"SRS_ID", "INT", "", "10", "INT UNSIGNED", "YES"}, byName["SRS_ID"])
}

func TestInformationSchemaVirtualTablesAndSchemataUseNativeMetadataShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	tables := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='tables' and column_name in ('TABLE_NAME','VERSION','TABLE_ROWS','CREATE_TIME') order by ordinal_position")
	schemata := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='schemata' and column_name='DEFAULT_ENCRYPTION'")
	rowsByName := func(result *SelectResult) map[string][]interface{} {
		rows := selectResultRows(result)
		values := make(map[string][]interface{}, len(rows))
		for _, row := range rows {
			if len(row) > 0 {
				values[fmt.Sprint(row[0])] = row
			}
		}
		return values
	}
	tableRows := rowsByName(tables)
	require.Equal(t, []interface{}{"TABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, tableRows["TABLE_NAME"])
	require.Equal(t, []interface{}{"VERSION", "INT", "", "10", "INT", "YES"}, tableRows["VERSION"])
	require.Equal(t, []interface{}{"TABLE_ROWS", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, tableRows["TABLE_ROWS"])
	require.Equal(t, []interface{}{"CREATE_TIME", "TIMESTAMP", "", "", "TIMESTAMP", "NO"}, tableRows["CREATE_TIME"])
	require.Equal(t, []interface{}{"DEFAULT_ENCRYPTION", "ENUM", "3", "", "ENUM('NO','YES')", "NO"}, rowsByName(schemata)["DEFAULT_ENCRYPTION"])
}

func TestInformationSchemaVirtualStatisticsUsesNativeIndexMetadataShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='statistics' and column_name in ('TABLE_NAME','INDEX_NAME','NON_UNIQUE','SEQ_IN_INDEX','CARDINALITY','COLLATION') order by ordinal_position")
	rows := selectResultRows(result)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			byName[fmt.Sprint(row[0])] = row
		}
	}
	require.Equal(t, []interface{}{"TABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, byName["TABLE_NAME"])
	require.Equal(t, []interface{}{"INDEX_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, byName["INDEX_NAME"])
	require.Equal(t, []interface{}{"NON_UNIQUE", "INT", "", "10", "INT", "NO"}, byName["NON_UNIQUE"])
	require.Equal(t, []interface{}{"SEQ_IN_INDEX", "INT", "", "10", "INT UNSIGNED", "NO"}, byName["SEQ_IN_INDEX"])
	require.Equal(t, []interface{}{"CARDINALITY", "BIGINT", "", "19", "BIGINT", "YES"}, byName["CARDINALITY"])
	require.Equal(t, []interface{}{"COLLATION", "VARCHAR", "1", "", "VARCHAR(1)", "YES"}, byName["COLLATION"])
}

func TestInformationSchemaVirtualRoutineMetadataUsesNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	routines := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='routines' and column_name in ('ROUTINE_NAME','DATA_TYPE','ROUTINE_DEFINITION','CREATED') order by ordinal_position")
	parameters := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='parameters' and column_name in ('ORDINAL_POSITION','PARAMETER_NAME','DATA_TYPE') order by ordinal_position")
	rowsByName := func(result *SelectResult) map[string][]interface{} {
		rows := selectResultRows(result)
		values := make(map[string][]interface{}, len(rows))
		for _, row := range rows {
			if len(row) > 0 {
				values[fmt.Sprint(row[0])] = row
			}
		}
		return values
	}
	routineRows := rowsByName(routines)
	require.Equal(t, []interface{}{"ROUTINE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, routineRows["ROUTINE_NAME"])
	require.Equal(t, []interface{}{"DATA_TYPE", "LONGTEXT", "4294967295", "", "LONGTEXT", "YES"}, routineRows["DATA_TYPE"])
	require.Equal(t, []interface{}{"ROUTINE_DEFINITION", "LONGTEXT", "4294967295", "", "LONGTEXT", "YES"}, routineRows["ROUTINE_DEFINITION"])
	require.Equal(t, []interface{}{"CREATED", "TIMESTAMP", "", "", "TIMESTAMP", "NO"}, routineRows["CREATED"])
	parameterRows := rowsByName(parameters)
	require.Equal(t, []interface{}{"ORDINAL_POSITION", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, parameterRows["ORDINAL_POSITION"])
	require.Equal(t, []interface{}{"PARAMETER_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, parameterRows["PARAMETER_NAME"])
	require.Equal(t, []interface{}{"DATA_TYPE", "LONGTEXT", "4294967295", "", "LONGTEXT", "YES"}, parameterRows["DATA_TYPE"])
}

func TestInformationSchemaPerformanceSchemaErrorSummaryUsesMySQL84Metadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='events_errors_summary_global_by_error' and column_name in ('ERROR_NUMBER','ERROR_NAME','SQL_STATE','SUM_ERROR_RAISED','SUM_ERROR_HANDLED','FIRST_SEEN','LAST_SEEN') order by ordinal_position")
	rows := selectResultRows(result)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			byName[fmt.Sprint(row[0])] = row
		}
	}
	require.Equal(t, []interface{}{"ERROR_NUMBER", "INT", "", "10", "INT", "YES"}, byName["ERROR_NUMBER"])
	require.Equal(t, []interface{}{"ERROR_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, byName["ERROR_NAME"])
	require.Equal(t, []interface{}{"SQL_STATE", "VARCHAR", "5", "", "VARCHAR(5)", "YES"}, byName["SQL_STATE"])
	require.Equal(t, []interface{}{"SUM_ERROR_RAISED", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, byName["SUM_ERROR_RAISED"])
	require.Equal(t, []interface{}{"SUM_ERROR_HANDLED", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, byName["SUM_ERROR_HANDLED"])
	require.Equal(t, []interface{}{"FIRST_SEEN", "TIMESTAMP", "", "", "TIMESTAMP", "YES"}, byName["FIRST_SEEN"])
	require.Equal(t, []interface{}{"LAST_SEEN", "TIMESTAMP", "", "", "TIMESTAMP", "YES"}, byName["LAST_SEEN"])
}

func TestInformationSchemaPerformanceSchemaHostCacheUsesMySQL84Metadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='host_cache' and column_name in ('IP','HOST','HOST_VALIDATED','SUM_CONNECT_ERRORS','FIRST_SEEN','LAST_SEEN','FIRST_ERROR_SEEN','LAST_ERROR_SEEN') order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"IP", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
		{"HOST", "VARCHAR", "255", "", "VARCHAR(255)", "YES"},
		{"HOST_VALIDATED", "ENUM", "3", "", "ENUM('YES','NO')", "NO"},
		{"SUM_CONNECT_ERRORS", "BIGINT", "", "19", "BIGINT", "NO"},
		{"FIRST_SEEN", "TIMESTAMP", "", "", "TIMESTAMP", "NO"},
		{"LAST_SEEN", "TIMESTAMP", "", "", "TIMESTAMP", "NO"},
		{"FIRST_ERROR_SEEN", "TIMESTAMP", "", "", "TIMESTAMP", "YES"},
		{"LAST_ERROR_SEEN", "TIMESTAMP", "", "", "TIMESTAMP", "YES"},
	}, selectResultRows(result))
}

func TestInformationSchemaPerformanceSchemaDigestSamplingUsesMySQL84Metadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='events_statements_summary_by_digest' and column_name in ('QUERY_SAMPLE_SEEN','QUERY_SAMPLE_TIMER_WAIT') order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"QUERY_SAMPLE_SEEN", "TIMESTAMP", "", "", "TIMESTAMP(6)", "NO"},
		{"QUERY_SAMPLE_TIMER_WAIT", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
	}, selectResultRows(result))
}

func TestInformationSchemaPerformanceSchemaDigestSummaryTimestampsUseMySQL84Metadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='events_statements_summary_by_digest' and column_name in ('FIRST_SEEN','LAST_SEEN') order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"FIRST_SEEN", "TIMESTAMP", "", "", "TIMESTAMP(6)", "NO"},
		{"LAST_SEEN", "TIMESTAMP", "", "", "TIMESTAMP(6)", "NO"},
	}, selectResultRows(result))
}

func TestInformationSchemaPerformanceSchemaDigestSummaryUsesMySQL84DigestMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='events_statements_summary_by_digest' and column_name='DIGEST'")
	require.Equal(t, [][]interface{}{
		{"DIGEST", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
	}, selectResultRows(result))
}

func TestInformationSchemaPerformanceSchemaHistogramQuantileUsesMySQL84Metadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select table_name, column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name in ('events_statements_histogram_by_digest','events_statements_histogram_global') and column_name='BUCKET_QUANTILE' order by table_name")
	require.Equal(t, [][]interface{}{
		{"events_statements_histogram_by_digest", "BUCKET_QUANTILE", "DOUBLE", "", "7", "DOUBLE(7,6)", "NO"},
		{"events_statements_histogram_global", "BUCKET_QUANTILE", "DOUBLE", "", "7", "DOUBLE(7,6)", "NO"},
	}, selectResultRows(result))
}

func TestInformationSchemaPerformanceSchemaHistogramTablesUseMySQL84ColumnShape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	byDigest := mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_statements_histogram_by_digest where 1 = 0")
	global := mustSelectResultSQL(t, executor, "", "select * from performance_schema.events_statements_histogram_global where 1 = 0")
	require.Equal(t, []string{
		"SCHEMA_NAME", "DIGEST", "BUCKET_NUMBER", "BUCKET_TIMER_LOW",
		"BUCKET_TIMER_HIGH", "COUNT_BUCKET", "COUNT_BUCKET_AND_LOWER", "BUCKET_QUANTILE",
	}, byDigest.Columns)
	require.Equal(t, []string{
		"BUCKET_NUMBER", "BUCKET_TIMER_LOW", "BUCKET_TIMER_HIGH", "COUNT_BUCKET",
		"COUNT_BUCKET_AND_LOWER", "BUCKET_QUANTILE",
	}, global.Columns)
}

func TestInformationSchemaPerformanceSchemaHistogramColumnsUseMySQL84Metadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select table_name, column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name in ('events_statements_histogram_by_digest','events_statements_histogram_global') and column_name in ('SCHEMA_NAME','DIGEST','DIGEST_TEXT','BUCKET_NUMBER','BUCKET_TIMER_LOW','BUCKET_TIMER_HIGH','COUNT_BUCKET','COUNT_BUCKET_AND_LOWER','BUCKET_QUANTILE') order by table_name, ordinal_position")
	rows := selectResultRows(result)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		byName[fmt.Sprint(row[0])+"."+fmt.Sprint(row[1])] = row
	}
	require.Equal(t, []interface{}{"events_statements_histogram_by_digest", "SCHEMA_NAME", "VARCHAR", "64", "", "", "VARCHAR(64)", "YES"}, byName["events_statements_histogram_by_digest.SCHEMA_NAME"])
	require.Equal(t, []interface{}{"events_statements_histogram_by_digest", "DIGEST", "VARCHAR", "64", "", "", "VARCHAR(64)", "YES"}, byName["events_statements_histogram_by_digest.DIGEST"])
	require.Equal(t, []interface{}{"events_statements_histogram_by_digest", "BUCKET_NUMBER", "INT", "", "10", "0", "INT UNSIGNED", "NO"}, byName["events_statements_histogram_by_digest.BUCKET_NUMBER"])
	for _, column := range []string{"BUCKET_TIMER_LOW", "BUCKET_TIMER_HIGH", "COUNT_BUCKET", "COUNT_BUCKET_AND_LOWER"} {
		require.Equal(t, []interface{}{"events_statements_histogram_by_digest", column, "BIGINT", "", "20", "0", "BIGINT UNSIGNED", "NO"}, byName["events_statements_histogram_by_digest."+column])
	}
	require.Equal(t, []interface{}{"events_statements_histogram_by_digest", "BUCKET_QUANTILE", "DOUBLE", "", "7", "6", "DOUBLE(7,6)", "NO"}, byName["events_statements_histogram_by_digest.BUCKET_QUANTILE"])
	require.Equal(t, []interface{}{"events_statements_histogram_global", "BUCKET_NUMBER", "INT", "", "10", "0", "INT UNSIGNED", "NO"}, byName["events_statements_histogram_global.BUCKET_NUMBER"])
	for _, column := range []string{"BUCKET_TIMER_LOW", "BUCKET_TIMER_HIGH", "COUNT_BUCKET", "COUNT_BUCKET_AND_LOWER"} {
		require.Equal(t, []interface{}{"events_statements_histogram_global", column, "BIGINT", "", "20", "0", "BIGINT UNSIGNED", "NO"}, byName["events_statements_histogram_global."+column])
	}
	require.Equal(t, []interface{}{"events_statements_histogram_global", "BUCKET_QUANTILE", "DOUBLE", "", "7", "6", "DOUBLE(7,6)", "NO"}, byName["events_statements_histogram_global.BUCKET_QUANTILE"])
}

func TestInformationSchemaPerformanceSchemaDataLockMetadataUsesMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select table_name, column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name in ('data_locks','data_lock_waits') and column_name in ('ENGINE','ENGINE_LOCK_ID','REQUESTING_ENGINE_LOCK_ID','ENGINE_TRANSACTION_ID','REQUESTING_ENGINE_TRANSACTION_ID','THREAD_ID','REQUESTING_THREAD_ID','OBJECT_INSTANCE_BEGIN','REQUESTING_OBJECT_INSTANCE_BEGIN','LOCK_TYPE','LOCK_MODE','LOCK_STATUS') order by table_name, ordinal_position")
	rows := selectResultRows(result)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 1 {
			byName[fmt.Sprint(row[0])+"."+fmt.Sprint(row[1])] = row
		}
	}
	require.Equal(t, []interface{}{"data_locks", "ENGINE", "VARCHAR", "32", "", "VARCHAR(32)", "NO"}, byName["data_locks.ENGINE"])
	require.Equal(t, []interface{}{"data_locks", "ENGINE_LOCK_ID", "VARCHAR", "128", "", "VARCHAR(128)", "NO"}, byName["data_locks.ENGINE_LOCK_ID"])
	require.Equal(t, []interface{}{"data_locks", "ENGINE_TRANSACTION_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, byName["data_locks.ENGINE_TRANSACTION_ID"])
	require.Equal(t, []interface{}{"data_locks", "THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, byName["data_locks.THREAD_ID"])
	require.Equal(t, []interface{}{"data_locks", "OBJECT_INSTANCE_BEGIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, byName["data_locks.OBJECT_INSTANCE_BEGIN"])
	require.Equal(t, []interface{}{"data_locks", "LOCK_STATUS", "VARCHAR", "32", "", "VARCHAR(32)", "NO"}, byName["data_locks.LOCK_STATUS"])
	require.Equal(t, []interface{}{"data_lock_waits", "REQUESTING_ENGINE_LOCK_ID", "VARCHAR", "128", "", "VARCHAR(128)", "NO"}, byName["data_lock_waits.REQUESTING_ENGINE_LOCK_ID"])
	require.Equal(t, []interface{}{"data_lock_waits", "REQUESTING_THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, byName["data_lock_waits.REQUESTING_THREAD_ID"])
	require.Equal(t, []interface{}{"data_lock_waits", "REQUESTING_OBJECT_INSTANCE_BEGIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, byName["data_lock_waits.REQUESTING_OBJECT_INSTANCE_BEGIN"])
}

func TestInformationSchemaPerformanceSchemaSyncInstanceMetadataUsesMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select table_name, column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name in ('mutex_instances','rwlock_instances') order by table_name, ordinal_position")
	rows := selectResultRows(result)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 1 {
			byName[fmt.Sprint(row[0])+"."+fmt.Sprint(row[1])] = row
		}
	}
	require.Equal(t, []interface{}{"mutex_instances", "NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"}, byName["mutex_instances.NAME"])
	require.Equal(t, []interface{}{"mutex_instances", "OBJECT_INSTANCE_BEGIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, byName["mutex_instances.OBJECT_INSTANCE_BEGIN"])
	require.Equal(t, []interface{}{"mutex_instances", "LOCKED_BY_THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, byName["mutex_instances.LOCKED_BY_THREAD_ID"])
	require.Equal(t, []interface{}{"rwlock_instances", "NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"}, byName["rwlock_instances.NAME"])
	require.Equal(t, []interface{}{"rwlock_instances", "OBJECT_INSTANCE_BEGIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, byName["rwlock_instances.OBJECT_INSTANCE_BEGIN"])
	require.Equal(t, []interface{}{"rwlock_instances", "WRITE_LOCKED_BY_THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, byName["rwlock_instances.WRITE_LOCKED_BY_THREAD_ID"])
	require.Equal(t, []interface{}{"rwlock_instances", "READ_LOCKED_BY_COUNT", "INT", "", "10", "INT UNSIGNED", "NO"}, byName["rwlock_instances.READ_LOCKED_BY_COUNT"])
}

func TestInformationSchemaPerformanceSchemaFileAndSocketInstanceMetadataUsesMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select table_name, column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name in ('file_instances','socket_instances') order by table_name, ordinal_position")
	rows := selectResultRows(result)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 1 {
			byName[fmt.Sprint(row[0])+"."+fmt.Sprint(row[1])] = row
		}
	}
	require.Equal(t, []interface{}{"file_instances", "FILE_NAME", "VARCHAR", "512", "", "VARCHAR(512)", "NO"}, byName["file_instances.FILE_NAME"])
	require.Equal(t, []interface{}{"file_instances", "EVENT_NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"}, byName["file_instances.EVENT_NAME"])
	require.Equal(t, []interface{}{"file_instances", "OPEN_COUNT", "INT", "", "10", "INT UNSIGNED", "NO"}, byName["file_instances.OPEN_COUNT"])
	require.Equal(t, []interface{}{"socket_instances", "EVENT_NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"}, byName["socket_instances.EVENT_NAME"])
	require.Equal(t, []interface{}{"socket_instances", "OBJECT_INSTANCE_BEGIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, byName["socket_instances.OBJECT_INSTANCE_BEGIN"])
	require.Equal(t, []interface{}{"socket_instances", "THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, byName["socket_instances.THREAD_ID"])
	require.Equal(t, []interface{}{"socket_instances", "SOCKET_ID", "INT", "", "10", "INT", "NO"}, byName["socket_instances.SOCKET_ID"])
	require.Equal(t, []interface{}{"socket_instances", "IP", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, byName["socket_instances.IP"])
	require.Equal(t, []interface{}{"socket_instances", "PORT", "INT", "", "10", "INT", "NO"}, byName["socket_instances.PORT"])
	require.Equal(t, []interface{}{"socket_instances", "STATE", "ENUM", "6", "", "ENUM('IDLE','ACTIVE')", "NO"}, byName["socket_instances.STATE"])
}

func TestInformationSchemaPerformanceSchemaTableHandleAndPreparedStatementMetadataUsesMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	metadata := func(table string) [][]interface{} {
		result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='"+table+"' order by ordinal_position")
		return selectResultRows(result)
	}
	require.Equal(t, [][]interface{}{
		{"OBJECT_TYPE", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
		{"OBJECT_SCHEMA", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
		{"OBJECT_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
		{"OBJECT_INSTANCE_BEGIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"OWNER_THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"OWNER_EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"},
		{"INTERNAL_LOCK", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"EXTERNAL_LOCK", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
	}, metadata("table_handles"))
	require.Equal(t, [][]interface{}{
		{"OBJECT_INSTANCE_BEGIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"STATEMENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"STATEMENT_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"SQL_TEXT", "LONGTEXT", "4294967295", "", "LONGTEXT", "NO"},
		{"OWNER_THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"OWNER_EVENT_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"OWNER_OBJECT_TYPE", "ENUM", "9", "", "ENUM('EVENT','FUNCTION','PROCEDURE','TABLE','TRIGGER')", "YES"},
		{"OWNER_OBJECT_SCHEMA", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"OWNER_OBJECT_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"EXECUTION_ENGINE", "ENUM", "9", "", "ENUM('PRIMARY','SECONDARY')", "YES"},
		{"TIMER_PREPARE", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"COUNT_REPREPARE", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"COUNT_EXECUTE", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_TIMER_EXECUTE", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"MIN_TIMER_EXECUTE", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"AVG_TIMER_EXECUTE", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"MAX_TIMER_EXECUTE", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_LOCK_TIME", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_ERRORS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_WARNINGS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_ROWS_AFFECTED", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_ROWS_SENT", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_ROWS_EXAMINED", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_CREATED_TMP_DISK_TABLES", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_CREATED_TMP_TABLES", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_SELECT_FULL_JOIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_SELECT_FULL_RANGE_JOIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_SELECT_RANGE", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_SELECT_RANGE_CHECK", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_SELECT_SCAN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_SORT_MERGE_PASSES", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_SORT_RANGE", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_SORT_ROWS", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_SORT_SCAN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_NO_INDEX_USED", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_NO_GOOD_INDEX_USED", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_CPU_TIME", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"MAX_CONTROLLED_MEMORY", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"MAX_TOTAL_MEMORY", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"COUNT_SECONDARY", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
	}, metadata("prepared_statements_instances"))
}

func TestInformationSchemaPerformanceSchemaTimersMetadataUsesMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='performance_timers' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"TIMER_NAME", "ENUM", "11", "", "ENUM('CYCLE','NANOSECOND','MICROSECOND','MILLISECOND','THREAD_CPU')", "NO"},
		{"TIMER_FREQUENCY", "BIGINT", "", "19", "BIGINT", "YES"},
		{"TIMER_RESOLUTION", "BIGINT", "", "19", "BIGINT", "YES"},
		{"TIMER_OVERHEAD", "BIGINT", "", "19", "BIGINT", "YES"},
	}, selectResultRows(result))
}

func TestInformationSchemaPerformanceSchemaConnectionAttributesMetadataUsesMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select table_name, column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name in ('session_connect_attrs','session_account_connect_attrs') order by table_name, ordinal_position")
	rows := selectResultRows(result)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 1 {
			byName[fmt.Sprint(row[0])+"."+fmt.Sprint(row[1])] = row
		}
	}
	for _, table := range []string{"session_connect_attrs", "session_account_connect_attrs"} {
		require.Equal(t, []interface{}{table, "PROCESSLIST_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, byName[table+".PROCESSLIST_ID"])
		require.Equal(t, []interface{}{table, "ATTR_NAME", "VARCHAR", "32", "", "VARCHAR(32)", "NO"}, byName[table+".ATTR_NAME"])
		require.Equal(t, []interface{}{table, "ATTR_VALUE", "VARCHAR", "1024", "", "VARCHAR(1024)", "YES"}, byName[table+".ATTR_VALUE"])
		require.Equal(t, []interface{}{table, "ORDINAL_POSITION", "INT", "", "10", "INT", "YES"}, byName[table+".ORDINAL_POSITION"])
	}
}

func TestInformationSchemaPerformanceSchemaUserVariablesMetadataUsesMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='user_variables_by_thread' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"VARIABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
		{"VARIABLE_VALUE", "LONGBLOB", "", "", "LONGBLOB", "YES"},
	}, selectResultRows(result))
}

func TestInformationSchemaPerformanceSchemaThreadVariableMetadataUsesMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	for _, table := range []string{"variables_by_thread", "status_by_thread"} {
		result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='"+table+"' order by ordinal_position")
		require.Equal(t, [][]interface{}{
			{"THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
			{"VARIABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
			{"VARIABLE_VALUE", "VARCHAR", "1024", "", "VARCHAR(1024)", "YES"},
		}, selectResultRows(result), table)
	}
}

func TestInformationSchemaPerformanceSchemaMemorySummaryMetadataUsesMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	metadata := func(table string) [][]interface{} {
		result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='"+table+"' order by ordinal_position")
		return selectResultRows(result)
	}
	statColumns := [][]interface{}{
		{"COUNT_ALLOC", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"COUNT_FREE", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_NUMBER_OF_BYTES_ALLOC", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_NUMBER_OF_BYTES_FREE", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"LOW_COUNT_USED", "BIGINT", "", "19", "BIGINT", "NO"},
		{"CURRENT_COUNT_USED", "BIGINT", "", "19", "BIGINT", "NO"},
		{"HIGH_COUNT_USED", "BIGINT", "", "19", "BIGINT", "NO"},
		{"LOW_NUMBER_OF_BYTES_USED", "BIGINT", "", "19", "BIGINT", "NO"},
		{"CURRENT_NUMBER_OF_BYTES_USED", "BIGINT", "", "19", "BIGINT", "NO"},
		{"HIGH_NUMBER_OF_BYTES_USED", "BIGINT", "", "19", "BIGINT", "NO"},
	}
	for _, table := range []string{"memory_summary_global_by_event_name", "memory_summary_by_thread_by_event_name"} {
		rows := metadata(table)
		prefix := [][]interface{}{{"EVENT_NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"}}
		if table == "memory_summary_by_thread_by_event_name" {
			prefix = [][]interface{}{
				{"THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
				{"EVENT_NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"},
			}
		}
		expected := append([][]interface{}{}, prefix...)
		expected = append(expected, statColumns...)
		require.Equal(t, expected, rows, table)
	}
	for _, table := range []string{"memory_summary_by_account_by_event_name", "memory_summary_by_host_by_event_name", "memory_summary_by_user_by_event_name"} {
		rows := metadata(table)
		prefix := [][]interface{}{}
		switch table {
		case "memory_summary_by_account_by_event_name":
			prefix = [][]interface{}{
				{"USER", "CHAR", "32", "", "CHAR(32)", "YES"},
				{"HOST", "CHAR", "255", "", "CHAR(255)", "YES"},
			}
		case "memory_summary_by_host_by_event_name":
			prefix = [][]interface{}{{"HOST", "CHAR", "255", "", "CHAR(255)", "YES"}}
		case "memory_summary_by_user_by_event_name":
			prefix = [][]interface{}{{"USER", "CHAR", "32", "", "CHAR(32)", "YES"}}
		}
		prefix = append(prefix, []interface{}{"EVENT_NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"})
		expected := append(prefix, statColumns...)
		require.Equal(t, expected, rows, table)
	}
}

func TestInformationSchemaPerformanceSchemaStatusByDimensionMetadataUsesMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	metadata := func(table string) [][]interface{} {
		result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='"+table+"' order by ordinal_position")
		return selectResultRows(result)
	}
	common := [][]interface{}{
		{"VARIABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
		{"VARIABLE_VALUE", "VARCHAR", "1024", "", "VARCHAR(1024)", "YES"},
	}
	expected := map[string][][]interface{}{
		"status_by_account": {
			{"USER", "CHAR", "32", "", "CHAR(32)", "YES"},
			{"HOST", "CHAR", "255", "", "CHAR(255)", "YES"},
		},
		"status_by_host": {{"HOST", "CHAR", "255", "", "CHAR(255)", "YES"}},
		"status_by_user": {{"USER", "CHAR", "32", "", "CHAR(32)", "YES"}},
	}
	for table, prefix := range expected {
		want := append(append([][]interface{}{}, prefix...), common...)
		require.Equal(t, want, metadata(table), table)
	}
}

func TestInformationSchemaPerformanceSchemaVariableMetadataUsesMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	metadata := func(table string) [][]interface{} {
		result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='"+table+"' order by ordinal_position")
		return selectResultRows(result)
	}
	variableRows := [][]interface{}{
		{"VARIABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
		{"VARIABLE_VALUE", "VARCHAR", "1024", "", "VARCHAR(1024)", "YES"},
	}
	for _, table := range []string{"session_status", "session_variables", "global_status", "global_variables"} {
		require.Equal(t, variableRows, metadata(table), table)
	}
	require.Equal(t, variableRows, metadata("persisted_variables"))
	variablesInfo := metadata("variables_info")
	require.Equal(t, [][]interface{}{
		{"VARIABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
		{"VARIABLE_SOURCE", "ENUM", "12", "", "ENUM('COMPILED','GLOBAL','SERVER','EXPLICIT','EXTRA','USER','LOGIN','COMMAND_LINE','PERSISTED','DYNAMIC')", "YES"},
		{"VARIABLE_PATH", "VARCHAR", "1024", "", "VARCHAR(1024)", "YES"},
		{"MIN_VALUE", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"MAX_VALUE", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"SET_TIME", "TIMESTAMP", "", "", "TIMESTAMP(6)", "YES"},
		{"SET_USER", "CHAR", "32", "", "CHAR(32)", "YES"},
		{"SET_HOST", "CHAR", "255", "", "CHAR(255)", "YES"},
	}, variablesInfo)
}

func TestInformationSchemaPerformanceSchemaIOSummaryMetadataUsesMySQL84Shapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	metadata := func(table, columns string) [][]interface{} {
		result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='"+table+"' and column_name in ("+columns+") order by ordinal_position")
		return selectResultRows(result)
	}
	require.Equal(t, [][]interface{}{
		{"OBJECT_TYPE", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"OBJECT_SCHEMA", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"OBJECT_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"COUNT_STAR", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_TIMER_WAIT", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
	}, metadata("table_io_waits_summary_by_table", "'OBJECT_TYPE','OBJECT_SCHEMA','OBJECT_NAME','COUNT_STAR','SUM_TIMER_WAIT'"))
	require.Equal(t, [][]interface{}{
		{"INDEX_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"},
		{"COUNT_STAR", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
	}, metadata("table_io_waits_summary_by_index_usage", "'INDEX_NAME','COUNT_STAR'"))
	require.Equal(t, [][]interface{}{
		{"FILE_NAME", "VARCHAR", "512", "", "VARCHAR(512)", "NO"},
		{"EVENT_NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"},
		{"OBJECT_INSTANCE_BEGIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_NUMBER_OF_BYTES_READ", "BIGINT", "", "19", "BIGINT", "NO"},
	}, metadata("file_summary_by_instance", "'FILE_NAME','EVENT_NAME','OBJECT_INSTANCE_BEGIN','SUM_NUMBER_OF_BYTES_READ'"))
	require.Equal(t, [][]interface{}{
		{"EVENT_NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"},
		{"OBJECT_INSTANCE_BEGIN", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
		{"SUM_NUMBER_OF_BYTES_READ", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"},
	}, metadata("socket_summary_by_instance", "'EVENT_NAME','OBJECT_INSTANCE_BEGIN','SUM_NUMBER_OF_BYTES_READ'"))
}

func TestInformationSchemaVirtualInformationSchemaCharactersUseUtf8mb3(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select character_set_name, character_octet_length from information_schema.columns where table_schema='information_schema' and table_name='tables' and column_name='TABLE_NAME'")
	rows := selectResultRows(result)
	require.Len(t, rows, 1)
	require.Equal(t, "utf8mb3", rows[0][0])
	require.Equal(t, "192", rows[0][1])
}

func TestInformationSchemaVirtualCharacterCatalogsUseNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	collations := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='collations' and column_name in ('COLLATION_NAME','CHARACTER_SET_NAME','ID','IS_DEFAULT','SORTLEN','PAD_ATTRIBUTE') order by ordinal_position")
	characterSets := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='character_sets' and column_name in ('CHARACTER_SET_NAME','DEFAULT_COLLATE_NAME','DESCRIPTION','MAXLEN') order by ordinal_position")
	rowsByName := func(result *SelectResult) map[string][]interface{} {
		rows := selectResultRows(result)
		values := make(map[string][]interface{}, len(rows))
		for _, row := range rows {
			if len(row) > 0 {
				values[fmt.Sprint(row[0])] = row
			}
		}
		return values
	}
	collationRows := rowsByName(collations)
	require.Equal(t, []interface{}{"COLLATION_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, collationRows["COLLATION_NAME"])
	require.Equal(t, []interface{}{"CHARACTER_SET_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, collationRows["CHARACTER_SET_NAME"])
	require.Equal(t, []interface{}{"ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, collationRows["ID"])
	require.Equal(t, []interface{}{"IS_DEFAULT", "VARCHAR", "3", "", "VARCHAR(3)", "NO"}, collationRows["IS_DEFAULT"])
	require.Equal(t, []interface{}{"SORTLEN", "INT", "", "10", "INT UNSIGNED", "NO"}, collationRows["SORTLEN"])
	require.Equal(t, []interface{}{"PAD_ATTRIBUTE", "ENUM", "9", "", "ENUM('PAD SPACE','NO PAD')", "NO"}, collationRows["PAD_ATTRIBUTE"])
	characterSetRows := rowsByName(characterSets)
	require.Equal(t, []interface{}{"CHARACTER_SET_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, characterSetRows["CHARACTER_SET_NAME"])
	require.Equal(t, []interface{}{"DEFAULT_COLLATE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, characterSetRows["DEFAULT_COLLATE_NAME"])
	require.Equal(t, []interface{}{"DESCRIPTION", "VARCHAR", "2048", "", "VARCHAR(2048)", "NO"}, characterSetRows["DESCRIPTION"])
	require.Equal(t, []interface{}{"MAXLEN", "INT", "", "10", "INT UNSIGNED", "NO"}, characterSetRows["MAXLEN"])
}

func TestInformationSchemaVirtualCollationApplicabilityUsesNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='collation_character_set_applicability' order by ordinal_position")
	rows := selectResultRows(result)
	require.Equal(t, [][]interface{}{
		{"COLLATION_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
		{"CHARACTER_SET_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"},
	}, rows)
}

func TestInformationSchemaVirtualProcesslistAndEnginesUseNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	processlist := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='processlist' and column_name in ('ID','USER','HOST','DB','COMMAND','TIME','STATE','INFO') order by ordinal_position")
	engines := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='engines' and column_name in ('ENGINE','SUPPORT','COMMENT','TRANSACTIONS','XA','SAVEPOINTS') order by ordinal_position")
	rowsByName := func(result *SelectResult) map[string][]interface{} {
		rows := selectResultRows(result)
		values := make(map[string][]interface{}, len(rows))
		for _, row := range rows {
			if len(row) > 0 {
				values[fmt.Sprint(row[0])] = row
			}
		}
		return values
	}
	processRows := rowsByName(processlist)
	require.Equal(t, []interface{}{"ID", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"}, processRows["ID"])
	require.Equal(t, []interface{}{"USER", "VARCHAR", "10", "", "VARCHAR(32)", "NO"}, processRows["USER"])
	require.Equal(t, []interface{}{"HOST", "VARCHAR", "87", "", "VARCHAR(261)", "NO"}, processRows["HOST"])
	require.Equal(t, []interface{}{"DB", "VARCHAR", "21", "", "VARCHAR(64)", "YES"}, processRows["DB"])
	require.Equal(t, []interface{}{"COMMAND", "VARCHAR", "5", "", "VARCHAR(16)", "NO"}, processRows["COMMAND"])
	require.Equal(t, []interface{}{"TIME", "INT", "", "", "INT", "NO"}, processRows["TIME"])
	require.Equal(t, []interface{}{"STATE", "VARCHAR", "21", "", "VARCHAR(64)", "YES"}, processRows["STATE"])
	require.Equal(t, []interface{}{"INFO", "VARCHAR", "21845", "", "VARCHAR(65535)", "YES"}, processRows["INFO"])
	engineRows := rowsByName(engines)
	require.Equal(t, []interface{}{"ENGINE", "VARCHAR", "21", "", "VARCHAR(64)", "NO"}, engineRows["ENGINE"])
	require.Equal(t, []interface{}{"SUPPORT", "VARCHAR", "2", "", "VARCHAR(8)", "NO"}, engineRows["SUPPORT"])
	require.Equal(t, []interface{}{"COMMENT", "VARCHAR", "26", "", "VARCHAR(80)", "NO"}, engineRows["COMMENT"])
	require.Equal(t, []interface{}{"TRANSACTIONS", "VARCHAR", "1", "", "VARCHAR(3)", "YES"}, engineRows["TRANSACTIONS"])
	require.Equal(t, []interface{}{"XA", "VARCHAR", "1", "", "VARCHAR(3)", "YES"}, engineRows["XA"])
	require.Equal(t, []interface{}{"SAVEPOINTS", "VARCHAR", "1", "", "VARCHAR(3)", "YES"}, engineRows["SAVEPOINTS"])
}

func TestPerformanceSchemaVirtualThreadsAndProcesslistUseNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	threads := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='threads' order by ordinal_position")
	processlist := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='performance_schema' and table_name='processlist' and column_name in ('ID','USER','HOST','DB','COMMAND','TIME','STATE','INFO','EXECUTION_ENGINE') order by ordinal_position")
	rowsByName := func(result *SelectResult) map[string][]interface{} {
		rows := selectResultRows(result)
		values := make(map[string][]interface{}, len(rows))
		for _, row := range rows {
			if len(row) > 0 {
				values[fmt.Sprint(row[0])] = row
			}
		}
		return values
	}
	threadRows := rowsByName(threads)
	require.Equal(t, []interface{}{"THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, threadRows["THREAD_ID"])
	require.Equal(t, []interface{}{"NAME", "VARCHAR", "128", "", "VARCHAR(128)", "NO"}, threadRows["NAME"])
	require.Equal(t, []interface{}{"TYPE", "VARCHAR", "10", "", "VARCHAR(10)", "NO"}, threadRows["TYPE"])
	require.Equal(t, []interface{}{"PROCESSLIST_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, threadRows["PROCESSLIST_ID"])
	require.Equal(t, []interface{}{"PROCESSLIST_USER", "VARCHAR", "32", "", "VARCHAR(32)", "YES"}, threadRows["PROCESSLIST_USER"])
	require.Equal(t, []interface{}{"PROCESSLIST_HOST", "VARCHAR", "255", "", "VARCHAR(255)", "YES"}, threadRows["PROCESSLIST_HOST"])
	require.Equal(t, []interface{}{"PROCESSLIST_DB", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, threadRows["PROCESSLIST_DB"])
	require.Equal(t, []interface{}{"PROCESSLIST_COMMAND", "VARCHAR", "16", "", "VARCHAR(16)", "YES"}, threadRows["PROCESSLIST_COMMAND"])
	require.Equal(t, []interface{}{"PROCESSLIST_TIME", "BIGINT", "", "19", "BIGINT", "YES"}, threadRows["PROCESSLIST_TIME"])
	require.Equal(t, []interface{}{"PROCESSLIST_STATE", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, threadRows["PROCESSLIST_STATE"])
	require.Equal(t, []interface{}{"PROCESSLIST_INFO", "LONGTEXT", "4294967295", "", "LONGTEXT", "YES"}, threadRows["PROCESSLIST_INFO"])
	require.Equal(t, []interface{}{"PARENT_THREAD_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, threadRows["PARENT_THREAD_ID"])
	require.Equal(t, []interface{}{"ROLE", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, threadRows["ROLE"])
	require.Equal(t, []interface{}{"INSTRUMENTED", "ENUM", "3", "", "ENUM('YES','NO')", "NO"}, threadRows["INSTRUMENTED"])
	require.Equal(t, []interface{}{"HISTORY", "ENUM", "3", "", "ENUM('YES','NO')", "NO"}, threadRows["HISTORY"])
	require.Equal(t, []interface{}{"CONNECTION_TYPE", "VARCHAR", "16", "", "VARCHAR(16)", "YES"}, threadRows["CONNECTION_TYPE"])
	require.Equal(t, []interface{}{"THREAD_OS_ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, threadRows["THREAD_OS_ID"])
	require.Equal(t, []interface{}{"RESOURCE_GROUP", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, threadRows["RESOURCE_GROUP"])
	require.Equal(t, []interface{}{"EXECUTION_ENGINE", "ENUM", "9", "", "ENUM('PRIMARY','SECONDARY')", "YES"}, threadRows["EXECUTION_ENGINE"])
	for _, name := range []string{"CONTROLLED_MEMORY", "MAX_CONTROLLED_MEMORY", "TOTAL_MEMORY", "MAX_TOTAL_MEMORY"} {
		require.Equal(t, []interface{}{name, "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, threadRows[name])
	}
	require.Equal(t, []interface{}{"TELEMETRY_ACTIVE", "ENUM", "3", "", "ENUM('YES','NO')", "NO"}, threadRows["TELEMETRY_ACTIVE"])

	processRows := rowsByName(processlist)
	require.Equal(t, []interface{}{"ID", "BIGINT", "", "20", "BIGINT UNSIGNED", "NO"}, processRows["ID"])
	require.Equal(t, []interface{}{"USER", "VARCHAR", "32", "", "VARCHAR(32)", "YES"}, processRows["USER"])
	require.Equal(t, []interface{}{"HOST", "VARCHAR", "261", "", "VARCHAR(261)", "YES"}, processRows["HOST"])
	require.Equal(t, []interface{}{"DB", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, processRows["DB"])
	require.Equal(t, []interface{}{"COMMAND", "VARCHAR", "16", "", "VARCHAR(16)", "YES"}, processRows["COMMAND"])
	require.Equal(t, []interface{}{"TIME", "BIGINT", "", "19", "BIGINT", "YES"}, processRows["TIME"])
	require.Equal(t, []interface{}{"STATE", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, processRows["STATE"])
	require.Equal(t, []interface{}{"INFO", "LONGTEXT", "4294967295", "", "LONGTEXT", "YES"}, processRows["INFO"])
	require.Equal(t, []interface{}{"EXECUTION_ENGINE", "ENUM", "9", "", "ENUM('PRIMARY','SECONDARY')", "YES"}, processRows["EXECUTION_ENGINE"])
}

func TestInformationSchemaVirtualConstraintMetadataUsesNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	keyUsage := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='key_column_usage' and column_name in ('CONSTRAINT_NAME','TABLE_NAME','ORDINAL_POSITION','POSITION_IN_UNIQUE_CONSTRAINT','REFERENCED_TABLE_NAME') order by ordinal_position")
	tableConstraints := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='table_constraints' and column_name in ('CONSTRAINT_NAME','TABLE_NAME','CONSTRAINT_TYPE','ENFORCED') order by ordinal_position")
	checkConstraints := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='check_constraints' and column_name in ('CONSTRAINT_NAME','CHECK_CLAUSE') order by ordinal_position")
	referential := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='referential_constraints' and column_name in ('CONSTRAINT_NAME','MATCH_OPTION','UPDATE_RULE','DELETE_RULE','TABLE_NAME') order by ordinal_position")
	rowsByName := func(result *SelectResult) map[string][]interface{} {
		rows := selectResultRows(result)
		values := make(map[string][]interface{}, len(rows))
		for _, row := range rows {
			if len(row) > 0 {
				values[fmt.Sprint(row[0])] = row
			}
		}
		return values
	}
	keyRows := rowsByName(keyUsage)
	require.Equal(t, []interface{}{"CONSTRAINT_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, keyRows["CONSTRAINT_NAME"])
	require.Equal(t, []interface{}{"TABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, keyRows["TABLE_NAME"])
	require.Equal(t, []interface{}{"ORDINAL_POSITION", "INT", "", "10", "INT UNSIGNED", "NO"}, keyRows["ORDINAL_POSITION"])
	require.Equal(t, []interface{}{"POSITION_IN_UNIQUE_CONSTRAINT", "INT", "", "10", "INT UNSIGNED", "YES"}, keyRows["POSITION_IN_UNIQUE_CONSTRAINT"])
	require.Equal(t, []interface{}{"REFERENCED_TABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, keyRows["REFERENCED_TABLE_NAME"])

	constraintRows := rowsByName(tableConstraints)
	require.Equal(t, []interface{}{"CONSTRAINT_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, constraintRows["CONSTRAINT_NAME"])
	require.Equal(t, []interface{}{"TABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, constraintRows["TABLE_NAME"])
	require.Equal(t, []interface{}{"CONSTRAINT_TYPE", "VARCHAR", "11", "", "VARCHAR(11)", "NO"}, constraintRows["CONSTRAINT_TYPE"])
	require.Equal(t, []interface{}{"ENFORCED", "VARCHAR", "3", "", "VARCHAR(3)", "NO"}, constraintRows["ENFORCED"])

	checkRows := rowsByName(checkConstraints)
	require.Equal(t, []interface{}{"CONSTRAINT_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, checkRows["CONSTRAINT_NAME"])
	require.Equal(t, []interface{}{"CHECK_CLAUSE", "LONGTEXT", "4294967295", "", "LONGTEXT", "NO"}, checkRows["CHECK_CLAUSE"])

	referentialRows := rowsByName(referential)
	require.Equal(t, []interface{}{"CONSTRAINT_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, referentialRows["CONSTRAINT_NAME"])
	require.Equal(t, []interface{}{"MATCH_OPTION", "ENUM", "7", "", "ENUM('NONE','PARTIAL','FULL')", "NO"}, referentialRows["MATCH_OPTION"])
	require.Equal(t, []interface{}{"UPDATE_RULE", "ENUM", "11", "", "ENUM('NO ACTION','RESTRICT','CASCADE','SET NULL','SET DEFAULT')", "NO"}, referentialRows["UPDATE_RULE"])
	require.Equal(t, []interface{}{"DELETE_RULE", "ENUM", "11", "", "ENUM('NO ACTION','RESTRICT','CASCADE','SET NULL','SET DEFAULT')", "NO"}, referentialRows["DELETE_RULE"])
	require.Equal(t, []interface{}{"TABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, referentialRows["TABLE_NAME"])
}

func TestInformationSchemaVirtualViewAndPartitionMetadataUsesNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	views := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='views' and column_name in ('TABLE_NAME','VIEW_DEFINITION','CHECK_OPTION','IS_UPDATABLE','DEFINER','SECURITY_TYPE') order by ordinal_position")
	partitions := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='partitions' and column_name in ('PARTITION_NAME','PARTITION_ORDINAL_POSITION','PARTITION_METHOD','PARTITION_EXPRESSION','TABLE_ROWS','PARTITION_COMMENT','TABLESPACE_NAME') order by ordinal_position")
	rowsByName := func(result *SelectResult) map[string][]interface{} {
		rows := selectResultRows(result)
		values := make(map[string][]interface{}, len(rows))
		for _, row := range rows {
			if len(row) > 0 {
				values[fmt.Sprint(row[0])] = row
			}
		}
		return values
	}
	viewRows := rowsByName(views)
	require.Equal(t, []interface{}{"TABLE_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "NO"}, viewRows["TABLE_NAME"])
	require.Equal(t, []interface{}{"VIEW_DEFINITION", "LONGTEXT", "4294967295", "", "LONGTEXT", "YES"}, viewRows["VIEW_DEFINITION"])
	require.Equal(t, []interface{}{"CHECK_OPTION", "ENUM", "8", "", "ENUM('NONE','LOCAL','CASCADED')", "YES"}, viewRows["CHECK_OPTION"])
	require.Equal(t, []interface{}{"IS_UPDATABLE", "ENUM", "3", "", "ENUM('NO','YES')", "YES"}, viewRows["IS_UPDATABLE"])
	require.Equal(t, []interface{}{"DEFINER", "VARCHAR", "288", "", "VARCHAR(288)", "YES"}, viewRows["DEFINER"])
	require.Equal(t, []interface{}{"SECURITY_TYPE", "VARCHAR", "7", "", "VARCHAR(7)", "YES"}, viewRows["SECURITY_TYPE"])

	partitionRows := rowsByName(partitions)
	require.Equal(t, []interface{}{"PARTITION_NAME", "VARCHAR", "64", "", "VARCHAR(64)", "YES"}, partitionRows["PARTITION_NAME"])
	require.Equal(t, []interface{}{"PARTITION_ORDINAL_POSITION", "INT", "", "10", "INT UNSIGNED", "YES"}, partitionRows["PARTITION_ORDINAL_POSITION"])
	require.Equal(t, []interface{}{"PARTITION_METHOD", "VARCHAR", "13", "", "VARCHAR(13)", "YES"}, partitionRows["PARTITION_METHOD"])
	require.Equal(t, []interface{}{"PARTITION_EXPRESSION", "VARCHAR", "2048", "", "VARCHAR(2048)", "YES"}, partitionRows["PARTITION_EXPRESSION"])
	require.Equal(t, []interface{}{"TABLE_ROWS", "BIGINT", "", "20", "BIGINT UNSIGNED", "YES"}, partitionRows["TABLE_ROWS"])
	require.Equal(t, []interface{}{"PARTITION_COMMENT", "TEXT", "65535", "", "TEXT", "NO"}, partitionRows["PARTITION_COMMENT"])
	require.Equal(t, []interface{}{"TABLESPACE_NAME", "VARCHAR", "268", "", "VARCHAR(268)", "YES"}, partitionRows["TABLESPACE_NAME"])
}

func TestInformationSchemaVirtualPluginsUseNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='plugins' and column_name in ('PLUGIN_NAME','PLUGIN_VERSION','PLUGIN_STATUS','PLUGIN_TYPE','PLUGIN_TYPE_VERSION','PLUGIN_LIBRARY','PLUGIN_AUTHOR','PLUGIN_DESCRIPTION','PLUGIN_LICENSE','LOAD_OPTION') order by ordinal_position")
	rows := selectResultRows(result)
	byName := make(map[string][]interface{}, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			byName[fmt.Sprint(row[0])] = row
		}
	}
	require.Equal(t, []interface{}{"PLUGIN_NAME", "VARCHAR", "21", "", "VARCHAR(64)", "NO"}, byName["PLUGIN_NAME"])
	require.Equal(t, []interface{}{"PLUGIN_VERSION", "VARCHAR", "6", "", "VARCHAR(20)", "NO"}, byName["PLUGIN_VERSION"])
	require.Equal(t, []interface{}{"PLUGIN_STATUS", "VARCHAR", "3", "", "VARCHAR(10)", "NO"}, byName["PLUGIN_STATUS"])
	require.Equal(t, []interface{}{"PLUGIN_TYPE", "VARCHAR", "26", "", "VARCHAR(80)", "NO"}, byName["PLUGIN_TYPE"])
	require.Equal(t, []interface{}{"PLUGIN_TYPE_VERSION", "VARCHAR", "6", "", "VARCHAR(20)", "NO"}, byName["PLUGIN_TYPE_VERSION"])
	require.Equal(t, []interface{}{"PLUGIN_LIBRARY", "VARCHAR", "21", "", "VARCHAR(64)", "YES"}, byName["PLUGIN_LIBRARY"])
	require.Equal(t, []interface{}{"PLUGIN_AUTHOR", "VARCHAR", "21", "", "VARCHAR(64)", "YES"}, byName["PLUGIN_AUTHOR"])
	require.Equal(t, []interface{}{"PLUGIN_DESCRIPTION", "VARCHAR", "21845", "", "VARCHAR(65535)", "YES"}, byName["PLUGIN_DESCRIPTION"])
	require.Equal(t, []interface{}{"PLUGIN_LICENSE", "VARCHAR", "26", "", "VARCHAR(80)", "YES"}, byName["PLUGIN_LICENSE"])
	require.Equal(t, []interface{}{"LOAD_OPTION", "VARCHAR", "21", "", "VARCHAR(64)", "NO"}, byName["LOAD_OPTION"])
}

func TestInformationSchemaVirtualStoredObjectMetadataUsesNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	triggers := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, datetime_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='triggers' and column_name in ('TRIGGER_NAME','EVENT_MANIPULATION','ACTION_ORDER','ACTION_CONDITION','ACTION_STATEMENT','ACTION_ORIENTATION','ACTION_TIMING','ACTION_REFERENCE_OLD_TABLE','ACTION_REFERENCE_OLD_ROW','CREATED') order by ordinal_position")
	events := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, datetime_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='events' and column_name in ('EVENT_NAME','TIME_ZONE','EVENT_BODY','EVENT_DEFINITION','EVENT_TYPE','EXECUTE_AT','INTERVAL_VALUE','INTERVAL_FIELD','SQL_MODE','STARTS','STATUS','ON_COMPLETION','LAST_EXECUTED','EVENT_COMMENT','ORIGINATOR') order by ordinal_position")
	rowsByName := func(result *SelectResult) map[string][]interface{} {
		rows := selectResultRows(result)
		values := make(map[string][]interface{}, len(rows))
		for _, row := range rows {
			if len(row) > 0 {
				values[fmt.Sprint(row[0])] = row
			}
		}
		return values
	}
	triggerRows := rowsByName(triggers)
	require.Equal(t, []interface{}{"TRIGGER_NAME", "VARCHAR", "64", "", "", "VARCHAR(64)", "NO"}, triggerRows["TRIGGER_NAME"])
	require.Equal(t, []interface{}{"EVENT_MANIPULATION", "ENUM", "6", "", "", "ENUM('INSERT','UPDATE','DELETE')", "NO"}, triggerRows["EVENT_MANIPULATION"])
	require.Equal(t, []interface{}{"ACTION_ORDER", "INT", "", "10", "", "INT UNSIGNED", "NO"}, triggerRows["ACTION_ORDER"])
	require.Equal(t, []interface{}{"ACTION_CONDITION", "VARBINARY", "0", "", "", "VARBINARY(0)", "YES"}, triggerRows["ACTION_CONDITION"])
	require.Equal(t, []interface{}{"ACTION_STATEMENT", "LONGTEXT", "4294967295", "", "", "LONGTEXT", "NO"}, triggerRows["ACTION_STATEMENT"])
	require.Equal(t, []interface{}{"ACTION_ORIENTATION", "VARCHAR", "3", "", "", "VARCHAR(3)", "NO"}, triggerRows["ACTION_ORIENTATION"])
	require.Equal(t, []interface{}{"ACTION_TIMING", "ENUM", "6", "", "", "ENUM('BEFORE','AFTER')", "NO"}, triggerRows["ACTION_TIMING"])
	require.Equal(t, []interface{}{"ACTION_REFERENCE_OLD_TABLE", "VARBINARY", "0", "", "", "VARBINARY(0)", "YES"}, triggerRows["ACTION_REFERENCE_OLD_TABLE"])
	require.Equal(t, []interface{}{"ACTION_REFERENCE_OLD_ROW", "VARCHAR", "3", "", "", "VARCHAR(3)", "NO"}, triggerRows["ACTION_REFERENCE_OLD_ROW"])
	require.Equal(t, []interface{}{"CREATED", "TIMESTAMP", "", "", "2", "TIMESTAMP(2)", "NO"}, triggerRows["CREATED"])

	eventRows := rowsByName(events)
	require.Equal(t, []interface{}{"EVENT_NAME", "VARCHAR", "64", "", "", "VARCHAR(64)", "NO"}, eventRows["EVENT_NAME"])
	require.Equal(t, []interface{}{"TIME_ZONE", "VARCHAR", "64", "", "", "VARCHAR(64)", "NO"}, eventRows["TIME_ZONE"])
	require.Equal(t, []interface{}{"EVENT_BODY", "VARCHAR", "3", "", "", "VARCHAR(3)", "NO"}, eventRows["EVENT_BODY"])
	require.Equal(t, []interface{}{"EVENT_DEFINITION", "LONGTEXT", "4294967295", "", "", "LONGTEXT", "NO"}, eventRows["EVENT_DEFINITION"])
	require.Equal(t, []interface{}{"EVENT_TYPE", "VARCHAR", "9", "", "", "VARCHAR(9)", "NO"}, eventRows["EVENT_TYPE"])
	require.Equal(t, []interface{}{"EXECUTE_AT", "DATETIME", "", "", "0", "DATETIME", "YES"}, eventRows["EXECUTE_AT"])
	require.Equal(t, []interface{}{"INTERVAL_VALUE", "VARCHAR", "256", "", "", "VARCHAR(256)", "YES"}, eventRows["INTERVAL_VALUE"])
	require.Equal(t, []interface{}{"INTERVAL_FIELD", "ENUM", "18", "", "", "ENUM('YEAR','QUARTER','MONTH','DAY','HOUR','MINUTE','WEEK','SECOND','MICROSECOND','YEAR_MONTH','DAY_HOUR','DAY_MINUTE','DAY_SECOND','HOUR_MINUTE','HOUR_SECOND','MINUTE_SECOND','DAY_MICROSECOND','HOUR_MICROSECOND','MINUTE_MICROSECOND','SECOND_MICROSECOND')", "YES"}, eventRows["INTERVAL_FIELD"])
	require.Equal(t, []interface{}{"SQL_MODE", "SET", "520", "", "", "SET('REAL_AS_FLOAT','PIPES_AS_CONCAT','ANSI_QUOTES','IGNORE_SPACE','NOT_USED','ONLY_FULL_GROUP_BY','NO_UNSIGNED_SUBTRACTION','NO_DIR_IN_CREATE','NOT_USED_9','NOT_USED_10','NOT_USED_11','NOT_USED_12','NOT_USED_13','NOT_USED_14','NOT_USED_15','NOT_USED_16','NOT_USED_17','NOT_USED_18','ANSI','NO_AUTO_VALUE_ON_ZERO','NO_BACKSLASH_ESCAPES','STRICT_TRANS_TABLES','STRICT_ALL_TABLES','NO_ZERO_IN_DATE','NO_ZERO_DATE','ALLOW_INVALID_DATES','ERROR_FOR_DIVISION_BY_ZERO','TRADITIONAL','NOT_USED_29','HIGH_NOT_PRECEDENCE','NO_ENGINE_SUBSTITUTION','PAD_CHAR_TO_FULL_LENGTH','TIME_TRUNCATE_FRACTIONAL')", "NO"}, eventRows["SQL_MODE"])
	require.Equal(t, []interface{}{"STARTS", "DATETIME", "", "", "0", "DATETIME", "YES"}, eventRows["STARTS"])
	require.Equal(t, []interface{}{"STATUS", "VARCHAR", "21", "", "", "VARCHAR(21)", "NO"}, eventRows["STATUS"])
	require.Equal(t, []interface{}{"ON_COMPLETION", "VARCHAR", "12", "", "", "VARCHAR(12)", "NO"}, eventRows["ON_COMPLETION"])
	require.Equal(t, []interface{}{"LAST_EXECUTED", "DATETIME", "", "", "0", "DATETIME", "YES"}, eventRows["LAST_EXECUTED"])
	require.Equal(t, []interface{}{"EVENT_COMMENT", "VARCHAR", "2048", "", "", "VARCHAR(2048)", "NO"}, eventRows["EVENT_COMMENT"])
	require.Equal(t, []interface{}{"ORIGINATOR", "INT", "", "10", "", "INT UNSIGNED", "NO"}, eventRows["ORIGINATOR"])
}

func TestInformationSchemaVirtualPrivilegeMetadataUsesNativeShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	tablePrivileges := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='table_privileges' and column_name in ('GRANTEE','TABLE_CATALOG','TABLE_SCHEMA','TABLE_NAME','PRIVILEGE_TYPE','IS_GRANTABLE') order by ordinal_position")
	columnPrivileges := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='column_privileges' and column_name in ('GRANTEE','TABLE_CATALOG','TABLE_SCHEMA','TABLE_NAME','COLUMN_NAME','PRIVILEGE_TYPE','IS_GRANTABLE') order by ordinal_position")
	userPrivileges := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='user_privileges' and column_name in ('GRANTEE','TABLE_CATALOG','PRIVILEGE_TYPE','IS_GRANTABLE') order by ordinal_position")
	applicableRoles := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='applicable_roles' and column_name in ('USER','HOST','GRANTEE','GRANTEE_HOST','ROLE_NAME','ROLE_HOST','IS_GRANTABLE','IS_DEFAULT','IS_MANDATORY') order by ordinal_position")
	rowsByName := func(result *SelectResult) map[string][]interface{} {
		rows := selectResultRows(result)
		values := make(map[string][]interface{}, len(rows))
		for _, row := range rows {
			if len(row) > 0 {
				values[fmt.Sprint(row[0])] = row
			}
		}
		return values
	}
	assertPrivilegeRows := func(rows map[string][]interface{}, includeColumnName bool) {
		require.Equal(t, []interface{}{"GRANTEE", "VARCHAR", "97", "", "VARCHAR(292)", "NO"}, rows["GRANTEE"])
		require.Equal(t, []interface{}{"TABLE_CATALOG", "VARCHAR", "170", "", "VARCHAR(512)", "NO"}, rows["TABLE_CATALOG"])
		if includeColumnName {
			require.Equal(t, []interface{}{"COLUMN_NAME", "VARCHAR", "21", "", "VARCHAR(64)", "NO"}, rows["COLUMN_NAME"])
		}
		require.Equal(t, []interface{}{"PRIVILEGE_TYPE", "VARCHAR", "21", "", "VARCHAR(64)", "NO"}, rows["PRIVILEGE_TYPE"])
		require.Equal(t, []interface{}{"IS_GRANTABLE", "VARCHAR", "1", "", "VARCHAR(3)", "NO"}, rows["IS_GRANTABLE"])
	}
	tableRows := rowsByName(tablePrivileges)
	assertPrivilegeRows(tableRows, false)
	require.Equal(t, []interface{}{"TABLE_SCHEMA", "VARCHAR", "21", "", "VARCHAR(64)", "NO"}, tableRows["TABLE_SCHEMA"])
	require.Equal(t, []interface{}{"TABLE_NAME", "VARCHAR", "21", "", "VARCHAR(64)", "NO"}, tableRows["TABLE_NAME"])
	columnRows := rowsByName(columnPrivileges)
	assertPrivilegeRows(columnRows, true)
	require.Equal(t, []interface{}{"TABLE_SCHEMA", "VARCHAR", "21", "", "VARCHAR(64)", "NO"}, columnRows["TABLE_SCHEMA"])
	require.Equal(t, []interface{}{"TABLE_NAME", "VARCHAR", "21", "", "VARCHAR(64)", "NO"}, columnRows["TABLE_NAME"])
	userRows := rowsByName(userPrivileges)
	assertPrivilegeRows(userRows, false)
	roleRows := rowsByName(applicableRoles)
	require.Equal(t, []interface{}{"USER", "VARCHAR", "97", "", "VARCHAR(97)", "YES"}, roleRows["USER"])
	require.Equal(t, []interface{}{"HOST", "VARCHAR", "256", "", "VARCHAR(256)", "YES"}, roleRows["HOST"])
	require.Equal(t, []interface{}{"GRANTEE", "VARCHAR", "97", "", "VARCHAR(97)", "YES"}, roleRows["GRANTEE"])
	require.Equal(t, []interface{}{"GRANTEE_HOST", "VARCHAR", "256", "", "VARCHAR(256)", "YES"}, roleRows["GRANTEE_HOST"])
	require.Equal(t, []interface{}{"ROLE_NAME", "VARCHAR", "255", "", "VARCHAR(255)", "YES"}, roleRows["ROLE_NAME"])
	require.Equal(t, []interface{}{"ROLE_HOST", "VARCHAR", "256", "", "VARCHAR(256)", "YES"}, roleRows["ROLE_HOST"])
	require.Equal(t, []interface{}{"IS_DEFAULT", "VARCHAR", "3", "", "VARCHAR(3)", "YES"}, roleRows["IS_DEFAULT"])
	require.Equal(t, []interface{}{"IS_MANDATORY", "VARCHAR", "3", "", "VARCHAR(3)", "NO"}, roleRows["IS_MANDATORY"])
}

func TestInformationSchemaCatalogNameIsRegisteredAndReturnsNativeRow(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select catalog_name from information_schema.information_schema_catalog_name")
	require.Equal(t, []string{"CATALOG_NAME"}, result.Columns)
	require.Equal(t, [][]interface{}{{"def"}}, selectResultRows(result))
	metadata := mustSelectResultSQL(t, executor, "", "select table_name, column_name, data_type, character_maximum_length, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='information_schema_catalog_name'")
	require.Len(t, metadata.Records, 1)
	values := metadata.Records[0].GetValues()
	require.Equal(t, "information_schema_catalog_name", values[0].String())
	require.Equal(t, "CATALOG_NAME", values[1].String())
	require.Equal(t, "VARCHAR", values[2].String())
	require.Equal(t, int64(64), values[3].Int())
	require.Equal(t, "VARCHAR(64)", values[4].String())
	require.Equal(t, "NO", values[5].String())
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
		{tableName: "innodb_buffer_page", count: 21, first: "POOL_ID", last: "IS_STALE"},
		{tableName: "innodb_buffer_page_lru", count: 20, first: "POOL_ID", last: "FREE_PAGE_CLOCK"},
		{tableName: "innodb_buffer_pool_stats", count: 32, first: "POOL_ID", last: "UNCOMPRESS_CURRENT"},
		{tableName: "innodb_cmp", count: 6, first: "PAGE_SIZE", last: "UNCOMPRESS_TIME"},
		{tableName: "innodb_cmp_per_index", count: 8, first: "DATABASE_NAME", last: "UNCOMPRESS_TIME"},
		{tableName: "innodb_cmpmem", count: 6, first: "PAGE_SIZE", last: "RELOCATION_TIME"},
		{tableName: "innodb_trx", count: 25, first: "TRX_ID", last: "TRX_SCHEDULE_WEIGHT"},
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

func TestInformationSchemaInnoDBBufferViewsUseMySQL84Metadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	stats := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='innodb_buffer_pool_stats' and column_name in ('HIT_RATE','PAGES_MADE_YOUNG_RATE','YOUNG_MAKE_PER_THOUSAND_GETS','NOT_YOUNG_MAKE_PER_THOUSAND_GETS') order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"PAGES_MADE_YOUNG_RATE", "FLOAT", "", "", "FLOAT(12,0)", "NO"},
		{"HIT_RATE", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"YOUNG_MAKE_PER_THOUSAND_GETS", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"NOT_YOUNG_MAKE_PER_THOUSAND_GETS", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
	}, selectResultRows(stats))

	page := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='innodb_buffer_page' and column_name in ('PAGE_TYPE','TABLE_NAME','IS_OLD','FREE_PAGE_CLOCK','IS_STALE') order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"PAGE_TYPE", "VARCHAR", "21", "", "VARCHAR(64)", "YES"},
		{"TABLE_NAME", "VARCHAR", "341", "", "VARCHAR(1024)", "YES"},
		{"IS_OLD", "VARCHAR", "1", "", "VARCHAR(3)", "YES"},
		{"FREE_PAGE_CLOCK", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"IS_STALE", "VARCHAR", "1", "", "VARCHAR(3)", "YES"},
	}, selectResultRows(page))

	lru := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='innodb_buffer_page_lru' and column_name in ('LRU_POSITION','COMPRESSED','PAGE_STATE') order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"LRU_POSITION", "BIGINT", "", "", "BIGINT UNSIGNED", "NO"},
		{"COMPRESSED", "VARCHAR", "1", "", "VARCHAR(3)", "YES"},
	}, selectResultRows(lru))
}

func TestInformationSchemaInnoDBCompressionViewsUseMySQL84Metadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	cmp := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='innodb_cmp' and column_name in ('PAGE_SIZE','COMPRESS_OPS','UNCOMPRESS_TIME') order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"PAGE_SIZE", "INT", "", "", "INT", "NO"},
		{"COMPRESS_OPS", "INT", "", "", "INT", "NO"},
		{"UNCOMPRESS_TIME", "INT", "", "", "INT", "NO"},
	}, selectResultRows(cmp))

	perIndex := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='innodb_cmp_per_index' and column_name in ('DATABASE_NAME','TABLE_NAME','INDEX_NAME','COMPRESS_OPS') order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"DATABASE_NAME", "VARCHAR", "64", "", "VARCHAR(192)", "NO"},
		{"TABLE_NAME", "VARCHAR", "64", "", "VARCHAR(192)", "NO"},
		{"INDEX_NAME", "VARCHAR", "64", "", "VARCHAR(192)", "NO"},
		{"COMPRESS_OPS", "INT", "", "", "INT", "NO"},
	}, selectResultRows(perIndex))

	cmpmem := mustSelectResultSQL(t, executor, "", "select column_name, data_type, character_maximum_length, numeric_precision, column_type, is_nullable from information_schema.columns where table_schema='information_schema' and table_name='innodb_cmpmem' order by ordinal_position")
	require.Equal(t, [][]interface{}{
		{"PAGE_SIZE", "INT", "", "", "INT", "NO"},
		{"BUFFER_POOL_INSTANCE", "INT", "", "", "INT", "NO"},
		{"PAGES_USED", "INT", "", "", "INT", "NO"},
		{"PAGES_FREE", "INT", "", "", "INT", "NO"},
		{"RELOCATION_OPS", "BIGINT", "", "", "BIGINT", "NO"},
		{"RELOCATION_TIME", "INT", "", "", "INT", "NO"},
	}, selectResultRows(cmpmem))
}

func TestInformationSchemaInnoDBCompressionPerIndexProjectsPrimarySpaceStats(t *testing.T) {
	executor := NewXMySQLEngine(&conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbCompression:    conf.InnodbCompressionConfig{Enabled: true, Method: "zlib", AllSpaces: true},
	})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table compressed_index_rows (id int primary key, payload varchar(64))")

	storage, err := executor.GetStorageManager().GetTableStorageManager().GetTableStorageInfo("app", "compressed_index_rows")
	require.NoError(t, err)
	compression := executor.GetStorageManager().GetCompressionManager()
	require.NotNil(t, compression)
	compression.SetCompressionSettings(storage.SpaceID, &manager.CompressionSettings{
		SpaceID: storage.SpaceID, Method: manager.COMPRESSION_ZLIB, Level: manager.COMPRESSION_LEVEL_DEFAULT, MinSavings: 0,
	})
	disabled := mustSelectResultSQL(t, executor, "", "select database_name, table_name, index_name from information_schema.innodb_cmp_per_index where database_name='app' and table_name='compressed_index_rows'")
	require.Empty(t, selectResultRows(disabled))
	session := newTestMySQLSession()
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_cmp_per_index_enabled", "ON"))
	_, err = compression.CompressPage(storage.SpaceID, 1, bytes.Repeat([]byte("x"), 4096))
	require.NoError(t, err)
	require.Equal(t, uint64(1), compression.GetStatsForSpace(storage.SpaceID).TotalPages)

	result := mustSelectResultSQL(t, executor, "", "select database_name, table_name, index_name, compress_ops, compress_ops_ok from information_schema.innodb_cmp_per_index where database_name='app' and table_name='compressed_index_rows' and index_name='PRIMARY'")
	require.Equal(t, [][]interface{}{{"app", "compressed_index_rows", "PRIMARY", "1", "1"}}, selectResultRows(result))
}

func TestInformationSchemaInnoDBCompressionResetViewsClearTheirCounters(t *testing.T) {
	executor := NewXMySQLEngine(&conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbCompression:    conf.InnodbCompressionConfig{Enabled: true, Method: "zlib", AllSpaces: true},
	})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table compressed_reset_rows (id int primary key, payload varchar(64))")

	storage, err := executor.GetStorageManager().GetTableStorageManager().GetTableStorageInfo("app", "compressed_reset_rows")
	require.NoError(t, err)
	compression := executor.GetStorageManager().GetCompressionManager()
	require.NotNil(t, compression)
	compression.SetCompressionSettings(storage.SpaceID, &manager.CompressionSettings{
		SpaceID: storage.SpaceID, Method: manager.COMPRESSION_ZLIB, Level: manager.COMPRESSION_LEVEL_DEFAULT, MinSavings: 0,
	})
	session := newTestMySQLSession()
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	require.NoError(t, executor.QueryExecutor.setGlobalVariable(session, "innodb_cmp_per_index_enabled", "ON"))
	_, err = compression.CompressPage(storage.SpaceID, 1, bytes.Repeat([]byte("r"), 4096))
	require.NoError(t, err)

	reset := mustSelectResultSQL(t, executor, "", "select database_name, table_name, index_name, compress_ops, compress_ops_ok from information_schema.innodb_cmp_per_index_reset where database_name='app' and table_name='compressed_reset_rows' and index_name='PRIMARY'")
	require.Equal(t, [][]interface{}{{"app", "compressed_reset_rows", "PRIMARY", "1", "1"}}, selectResultRows(reset))
	cleared := mustSelectResultSQL(t, executor, "", "select database_name, table_name, index_name, compress_ops, compress_ops_ok from information_schema.innodb_cmp_per_index where database_name='app' and table_name='compressed_reset_rows' and index_name='PRIMARY'")
	require.Empty(t, selectResultRows(cleared))
}

func TestInformationSchemaInnoDBCompressionViewsExposeDecompressionCount(t *testing.T) {
	executor := NewXMySQLEngine(&conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
		InnodbCompression:    conf.InnodbCompressionConfig{Enabled: true, Method: "zlib", AllSpaces: true},
	})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table compressed_uncompress_rows (id int primary key, payload varchar(64))")

	storage, err := executor.GetStorageManager().GetTableStorageManager().GetTableStorageInfo("app", "compressed_uncompress_rows")
	require.NoError(t, err)
	compression := executor.GetStorageManager().GetCompressionManager()
	require.NotNil(t, compression)
	compression.SetCompressionSettings(storage.SpaceID, &manager.CompressionSettings{
		SpaceID: storage.SpaceID, Method: manager.COMPRESSION_ZLIB, Level: manager.COMPRESSION_LEVEL_DEFAULT, MinSavings: 0,
	})
	compression.GetStatsAndReset()
	compression.GetStatsForSpaceAndReset(storage.SpaceID)
	compressed, err := compression.CompressPage(storage.SpaceID, 1, bytes.Repeat([]byte("u"), 4096))
	require.NoError(t, err)
	_, err = compression.DecompressPage(storage.SpaceID, 1, compressed)
	require.NoError(t, err)

	result := mustSelectResultSQL(t, executor, "", "select page_size, compress_ops, compress_ops_ok, uncompress_ops from information_schema.innodb_cmp")
	require.Equal(t, [][]interface{}{{"16384", "1", "1", "1"}}, selectResultRows(result))
}
