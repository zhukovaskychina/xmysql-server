package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestInformationSchemaAuxiliaryTablesReturnStableMetadataShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table space_catalog (id int primary key, label varchar(16))")

	cases := []struct {
		name  string
		query string
		cols  []string
	}{
		{
			name:  "column_statistics",
			query: "select schema_name, table_name, column_name, histogram from information_schema.column_statistics where schema_name = 'app'",
			cols:  []string{"SCHEMA_NAME", "TABLE_NAME", "COLUMN_NAME", "HISTOGRAM"},
		},
		{
			name:  "tablespaces",
			query: "select tablespace_name, engine, tablespace_type, status from information_schema.tablespaces",
			cols:  []string{"TABLESPACE_NAME", "ENGINE", "TABLESPACE_TYPE", "STATUS"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := <-executor.ExecuteQuery(nil, tc.query, "app")
			require.NoError(t, result.Err)
			selectResult, ok := result.Data.(*SelectResult)
			require.True(t, ok)
			require.Equal(t, tc.cols, selectResult.Columns)
			if tc.name == "tablespaces" {
				require.NotEmpty(t, selectResult.Records)
			} else {
				require.Empty(t, selectResult.Records)
			}
		})
	}

	files := mustSelectResultSQL(t, executor, "app", "select file_id, file_name, file_type, tablespace_name, engine from information_schema.files where tablespace_name like 'app/%'")
	require.Equal(t, []string{"FILE_ID", "FILE_NAME", "FILE_TYPE", "TABLESPACE_NAME", "ENGINE"}, files.Columns)
	require.NotEmpty(t, files.Records)
	require.Equal(t, "TABLESPACE", files.Records[0].GetValues()[2].String())
	wrongFile := mustSelectResultSQL(t, executor, "app", "select file_name from information_schema.files where file_id = 999999999")
	require.Empty(t, wrongFile.Records)

	wrongTablespace := mustSelectResultSQL(t, executor, "app", "select tablespace_name from information_schema.tablespaces where tablespace_name = 'other'")
	require.Empty(t, wrongTablespace.Records)

	extensions := mustSelectResultSQL(t, executor, "app", "select tablespace_name, engine_attribute, se_private_data from information_schema.tablespaces_extensions where tablespace_name like 'app/%'")
	require.Equal(t, []string{"TABLESPACE_NAME", "ENGINE_ATTRIBUTE", "SE_PRIVATE_DATA"}, extensions.Columns)
	require.NotEmpty(t, extensions.Records)
	require.Nil(t, extensions.Records[0].GetValues()[1].Raw())

	innodbTables := mustSelectResultSQL(t, executor, "app", "select table_id, name, n_cols, space from information_schema.innodb_tables")
	require.Equal(t, []string{"TABLE_ID", "NAME", "N_COLS", "SPACE"}, innodbTables.Columns)
	require.NotEmpty(t, innodbTables.Records)

	innodbFields := mustSelectResultSQL(t, executor, "app", "select index_id, name, pos from information_schema.innodb_fields")
	require.Equal(t, []string{"INDEX_ID", "NAME", "POS"}, innodbFields.Columns)
	require.NotEmpty(t, innodbFields.Records)

	brief := mustSelectResultSQL(t, executor, "app", "select space, name, path, space_type from information_schema.innodb_tablespaces_brief where name like 'app/%'")
	require.Equal(t, []string{"SPACE", "NAME", "PATH", "SPACE_TYPE"}, brief.Columns)
	require.NotEmpty(t, brief.Records)

	bufferPool := mustSelectResultSQL(t, executor, "app", "select pool_id, pool_size, free_buffers, database_pages, modified_database_pages, hit_rate from information_schema.innodb_buffer_pool_stats")
	require.Equal(t, []string{"POOL_ID", "POOL_SIZE", "FREE_BUFFERS", "DATABASE_PAGES", "MODIFIED_DATABASE_PAGES", "HIT_RATE"}, bufferPool.Columns)
	require.Len(t, bufferPool.Records, 1)
	wrongPool := mustSelectResultSQL(t, executor, "app", "select pool_id from information_schema.innodb_buffer_pool_stats where pool_id = 99")
	require.Empty(t, wrongPool.Records)

	storage, err := executor.storageMgr.GetTableStorageManager().GetTableStorageInfo("app", "space_catalog")
	require.NoError(t, err)
	_, err = executor.storageMgr.GetBufferPoolManager().GetPage(storage.SpaceID, storage.RootPageNo)
	require.NoError(t, err)
	bufferPage := mustSelectResultSQL(t, executor, "app", "select space, page_number, table_name, page_state, is_old from information_schema.innodb_buffer_page where space = '"+fmt.Sprint(storage.SpaceID)+"'")
	require.Equal(t, []string{"SPACE", "PAGE_NUMBER", "TABLE_NAME", "PAGE_STATE", "IS_OLD"}, bufferPage.Columns)
	require.NotEmpty(t, bufferPage.Records)
	for _, row := range bufferPage.Records {
		require.Equal(t, int64(storage.SpaceID), row.GetValues()[0].Int())
	}
	wrongPage := mustSelectResultSQL(t, executor, "app", "select page_number from information_schema.innodb_buffer_page where space = '"+fmt.Sprint(storage.SpaceID)+"' and page_number = 999999999")
	require.Empty(t, wrongPage.Records)

	bufferPageLRU := mustSelectResultSQL(t, executor, "app", "select lru_position, space, page_number from information_schema.innodb_buffer_page_lru")
	require.Equal(t, []string{"LRU_POSITION", "SPACE", "PAGE_NUMBER"}, bufferPageLRU.Columns)
	require.NotEmpty(t, bufferPageLRU.Records)

	cachedIndexes := mustSelectResultSQL(t, executor, "app", "select space_id, index_name, n_cached_pages from information_schema.innodb_cached_indexes where table_schema = 'app' and table_name = 'space_catalog'")
	require.Equal(t, []string{"SPACE_ID", "INDEX_NAME", "N_CACHED_PAGES"}, cachedIndexes.Columns)
	require.NotEmpty(t, cachedIndexes.Records)
	require.Greater(t, cachedIndexes.Records[0].GetValues()[2].Int(), int64(0))
	wrongIndex := mustSelectResultSQL(t, executor, "app", "select index_name from information_schema.innodb_cached_indexes where table_schema = 'app' and table_name = 'space_catalog' and index_name = 'other'")
	require.Empty(t, wrongIndex.Records)
}

func TestInformationSchemaColumnStatisticsProjectsAnalyzeFacts(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database stats_app")
	mustExecSQL(t, executor, "stats_app", "create table histogram_source (id int, label varchar(16))")
	mustExecSQL(t, executor, "stats_app", "insert into histogram_source values (1, 'a'), (3, 'b'), (9, null)")
	mustExecSQL(t, executor, "stats_app", "analyze table histogram_source")

	result := mustSelectResultSQL(t, executor, "stats_app", "select schema_name, table_name, column_name, histogram from information_schema.column_statistics where schema_name = 'stats_app' and table_name = 'histogram_source'")
	require.Equal(t, []string{"SCHEMA_NAME", "TABLE_NAME", "COLUMN_NAME", "HISTOGRAM"}, result.Columns)
	require.Len(t, result.Records, 2)

	for _, record := range result.Records {
		require.Equal(t, "stats_app", record.GetValues()[0].String())
		require.Equal(t, "histogram_source", record.GetValues()[1].String())
		var histogram map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(record.GetValues()[3].String()), &histogram))
		require.Equal(t, "equi-height", histogram["histogram-type"])
		require.Equal(t, float64(1), histogram["sampling-rate"])
		require.NotEmpty(t, histogram["buckets"])
	}
	wrongHistogram := mustSelectResultSQL(t, executor, "stats_app", "select column_name from information_schema.column_statistics where schema_name='stats_app' and table_name='histogram_source' and histogram='missing'")
	require.Empty(t, wrongHistogram.Records)
}

func TestInformationSchemaTemporaryInnoDBViewsFilterLiveSession(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database temp_app")
	mustExecSessionSQL(t, executor, session, "temp_app", "create temporary table temp_probe (id int)")

	sessionSpaces := mustQuerySessionSQL(t, executor, session, "temp_app", "select purpose, state from information_schema.innodb_session_temp_tablespaces")
	require.NotEmpty(t, sessionSpaces)
	tempInfo := mustQuerySessionSQL(t, executor, session, "temp_app", "select name from information_schema.innodb_temp_table_info")
	require.NotEmpty(t, tempInfo)
	wrongSessionSpace := mustQuerySessionSQL(t, executor, session, "temp_app", "select space from information_schema.innodb_session_temp_tablespaces where purpose = 'PERMANENT'")
	require.Empty(t, wrongSessionSpace)
	wrongTempName := mustQuerySessionSQL(t, executor, session, "temp_app", "select name from information_schema.innodb_temp_table_info where name = 'temp_app/other'")
	require.Empty(t, wrongTempName)
}

func TestAnalyzeTableHistogramUpdateDropAndPersistence(t *testing.T) {
	dataDir := t.TempDir()
	first := NewXMySQLEngine(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, first.Close())
		}
	})
	mustExecSQL(t, first, "", "create database stats_app")
	mustExecSQL(t, first, "stats_app", "create table histogram_source (id int, label varchar(16))")
	mustExecSQL(t, first, "stats_app", "insert into histogram_source values (1, 'a'), (3, 'b'), (5, 'b'), (9, null)")
	mustExecSQL(t, first, "stats_app", "analyze table histogram_source update histogram on id, label with 3 buckets")

	stats, err := first.QueryExecutor.infosSchemaManager.GetTableStats(context.Background(), "stats_app", "histogram_source")
	require.NoError(t, err)
	require.Len(t, stats.ColumnStats["id"].Histogram.Buckets, 3)
	require.Equal(t, 3, stats.ColumnStats["id"].Histogram.NumberOfBucketsSpecified)
	require.InDelta(t, 1.0, stats.ColumnStats["id"].Histogram.Buckets[2].CumulativeFrequency, 0.0001)
	require.InDelta(t, 0.25, stats.ColumnStats["label"].Histogram.NullValues, 0.0001)

	result := mustSelectResultSQL(t, first, "stats_app", "select column_name, histogram from information_schema.column_statistics where schema_name = 'stats_app' and table_name = 'histogram_source' and column_name = 'id'")
	require.Len(t, result.Records, 1)
	var histogram map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(result.Records[0].GetValues()[1].String()), &histogram))
	require.Equal(t, float64(3), histogram["number-of-buckets-specified"])
	require.Len(t, histogram["buckets"], 3)
	// A scalar ANALYZE refresh must not implicitly remove an explicitly
	// managed histogram.
	mustExecSQL(t, first, "stats_app", "analyze table histogram_source")
	refreshed, err := first.QueryExecutor.infosSchemaManager.GetTableStats(context.Background(), "stats_app", "histogram_source")
	require.NoError(t, err)
	require.Len(t, refreshed.ColumnStats["id"].Histogram.Buckets, 3)

	// The statistics sidecar is independently durable, so a fresh executor
	// must see the same explicit histogram rather than the scalar fallback.
	require.NoError(t, first.Close())
	closed = true
	second := NewXMySQLEngine(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	reloaded, err := second.QueryExecutor.infosSchemaManager.GetTableStats(context.Background(), "stats_app", "histogram_source")
	require.NoError(t, err)
	require.Len(t, reloaded.ColumnStats["id"].Histogram.Buckets, 3)

	mustExecSQL(t, second, "stats_app", "analyze table histogram_source drop histogram on id")
	dropped := mustSelectResultSQL(t, second, "stats_app", "select column_name from information_schema.column_statistics where schema_name = 'stats_app' and table_name = 'histogram_source' and column_name = 'id'")
	require.Empty(t, dropped.Records)
	require.True(t, reloaded.ColumnStats["id"].HistogramDropped == false)
	updated, err := second.QueryExecutor.infosSchemaManager.GetTableStats(context.Background(), "stats_app", "histogram_source")
	require.NoError(t, err)
	require.Nil(t, updated.ColumnStats["id"].Histogram)
	require.True(t, updated.ColumnStats["id"].HistogramDropped)
	invalid := <-second.ExecuteQuery(nil, "analyze table histogram_source update histogram on missing_col with 3 buckets", "stats_app")
	require.Error(t, invalid.Err)
}
