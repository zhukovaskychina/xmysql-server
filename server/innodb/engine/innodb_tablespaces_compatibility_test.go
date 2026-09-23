package engine

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInformationSchemaInnoDBTablespacesReflectsDurableCatalog(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create tablespace ledger add datafile 'ledger.ibd'")

	result := <-executor.ExecuteQuery(nil, "select space, name, space_type, state from information_schema.innodb_tablespaces", "")
	require.NoError(t, result.Err)
	rows, ok := result.Data.(*SelectResult)
	require.True(t, ok)

	found := false
	for _, record := range rows.Records {
		values := record.GetValues()
		if len(values) == 4 && values[1].String() == "ledger" {
			found = true
			require.Equal(t, "Single", values[2].String())
			require.Equal(t, "ACTIVE", values[3].String())
		}
	}
	require.True(t, found, "created tablespace was not projected: %#v", rows.Records)

	physical := <-executor.ExecuteQuery(nil, "select flag, page_size, zip_page_size, row_format, space_flags from information_schema.innodb_tablespaces where name = 'ledger'", "")
	require.NoError(t, physical.Err)
	physicalRows := physical.Data.(*SelectResult).Records
	require.Len(t, physicalRows, 1)
	physicalValues := physicalRows[0].GetValues()
	require.Equal(t, int64(0), physicalValues[0].Int())
	require.Equal(t, int64(16384), physicalValues[1].Int())
	require.Equal(t, int64(0), physicalValues[2].Int())
	require.Equal(t, "Compact or Redundant", physicalValues[3].String())
	require.Equal(t, int64(0), physicalValues[4].Int())

	filtered := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_tablespaces where name like 'led%'", "")
	require.NoError(t, filtered.Err)
	require.Equal(t, [][]interface{}{{"ledger"}}, selectResultRows(filtered.Data.(*SelectResult)))
	wrongSpace := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_tablespaces where space = 999999999", "")
	require.NoError(t, wrongSpace.Err)
	require.Empty(t, wrongSpace.Data.(*SelectResult).Records)
	wrongState := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_tablespaces where state = 'DISCARDED'", "")
	require.NoError(t, wrongState.Err)
	require.Empty(t, wrongState.Data.(*SelectResult).Records)
	wrongBriefType := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_tablespaces_brief where space_type = 'General'", "")
	require.NoError(t, wrongBriefType.Err)
	require.Empty(t, wrongBriefType.Data.(*SelectResult).Records)
}

func TestInformationSchemaInnoDBTablespacesReflectsRenameAndDrop(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create tablespace before_name add datafile 'before_name.ibd'")
	mustExecSQL(t, executor, "", "alter tablespace before_name rename to after_name")

	result := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_tablespaces", "")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	seenBefore, seenAfter := false, false
	for _, record := range rows.Records {
		switch record.GetValues()[0].String() {
		case "before_name":
			seenBefore = true
		case "after_name":
			seenAfter = true
		}
	}
	require.False(t, seenBefore)
	require.True(t, seenAfter)

	mustExecSQL(t, executor, "", "drop tablespace after_name engine=innodb")
	result = <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_tablespaces", "")
	require.NoError(t, result.Err)
	for _, record := range result.Data.(*SelectResult).Records {
		require.NotEqual(t, "after_name", record.GetValues()[0].String())
	}
}

func TestInformationSchemaInnoDBDatafilesReflectsPhysicalPath(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create tablespace datafile_probe add datafile 'datafile_probe.ibd'")

	result := <-executor.ExecuteQuery(nil, "select space, path from information_schema.innodb_datafiles", "")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	found := false
	for _, record := range rows.Records {
		values := record.GetValues()
		if len(values) == 2 && values[1].String() != "" && strings.HasSuffix(values[1].String(), "datafile_probe.ibd") {
			found = true
			require.Greater(t, values[0].Int(), int64(0))
		}
	}
	require.True(t, found, "created tablespace file was not projected: %#v", rows.Records)
	wrongSpace := <-executor.ExecuteQuery(nil, "select path from information_schema.innodb_datafiles where space = 999999999", "")
	require.NoError(t, wrongSpace.Err)
	require.Empty(t, wrongSpace.Data.(*SelectResult).Records)
	wrongPath := <-executor.ExecuteQuery(nil, "select path from information_schema.innodb_datafiles where path = 'other.ibd'", "")
	require.NoError(t, wrongPath.Err)
	require.Empty(t, wrongPath.Data.(*SelectResult).Records)
}

func TestInformationSchemaInnoDBTableStatsReflectsMappedTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into ledger values (1), (2)")

	result := <-executor.ExecuteQuery(nil, "select name, stats_initialized, num_rows, clust_index_size from information_schema.innodb_tablestats where table_schema = 'app' and table_name = 'ledger'", "app")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	require.Len(t, rows.Records, 1)
	values := rows.Records[0].GetValues()
	require.Equal(t, "app/ledger", values[0].String())
	require.Equal(t, "Initialized", values[1].String())
	require.Equal(t, int64(2), values[2].Int())
	require.Greater(t, values[3].Int(), int64(0))

	wrongName := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_tablestats where name = 'app/other'", "app")
	require.NoError(t, wrongName.Err)
	require.Empty(t, wrongName.Data.(*SelectResult).Records)
	wrongTableID := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_tablestats where table_id = 999999999", "app")
	require.NoError(t, wrongTableID.Err)
	require.Empty(t, wrongTableID.Data.(*SelectResult).Records)
}

func TestInformationSchemaInnoDBTableStatsProjectsSecondaryIndexPages(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key, label varchar(32), code int, index idx_label (label))")
	mustExecSQL(t, executor, "app", "insert into ledger values (1, 'alpha', 10), (2, 'beta', 20)")

	result := <-executor.ExecuteQuery(nil, "select clust_index_size, other_index_size from information_schema.innodb_tablestats where table_schema = 'app' and table_name = 'ledger'", "app")
	require.NoError(t, result.Err)
	records := result.Data.(*SelectResult).Records
	require.Len(t, records, 1)
	values := records[0].GetValues()
	require.Greater(t, values[0].Int(), int64(0))
	require.Greater(t, values[1].Int(), int64(0))
}

func TestInformationSchemaInnoDBTableStatsProjectsModifiedCounter(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into ledger values (1)")

	result := <-executor.ExecuteQuery(nil, "select modified_counter from information_schema.innodb_tablestats where table_schema = 'app' and table_name = 'ledger'", "app")
	require.NoError(t, result.Err)
	records := result.Data.(*SelectResult).Records
	require.Len(t, records, 1)
	require.Greater(t, records[0].GetValues()[0].Int(), int64(0))
}

func TestInformationSchemaInnoDBTableStatsProjectsPersistedAutoIncrement(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table sequence_rows (id int auto_increment primary key, label varchar(20))")

	initial := <-executor.ExecuteQuery(nil, "select autoinc from information_schema.innodb_tablestats where table_schema = 'app' and table_name = 'sequence_rows'", "app")
	require.NoError(t, initial.Err)
	require.Len(t, initial.Data.(*SelectResult).Records, 1)
	require.Equal(t, int64(1), initial.Data.(*SelectResult).Records[0].GetValues()[0].Int())

	mustExecSQL(t, executor, "app", "insert into sequence_rows (label) values ('first')")
	advanced := <-executor.ExecuteQuery(nil, "select autoinc from information_schema.innodb_tablestats where table_schema = 'app' and table_name = 'sequence_rows'", "app")
	require.NoError(t, advanced.Err)
	require.Equal(t, int64(2), advanced.Data.(*SelectResult).Records[0].GetValues()[0].Int())
}

func TestInformationSchemaInnoDBTablesReportsHiddenAndInstantColumnMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key, label varchar(32))")

	initial := <-executor.ExecuteQuery(nil, "select n_cols, instant_cols, total_row_versions from information_schema.innodb_tables where table_schema = 'app' and table_name = 'ledger'", "app")
	require.NoError(t, initial.Err)
	require.Len(t, initial.Data.(*SelectResult).Records, 1)
	initialValues := initial.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, int64(5), initialValues[0].Int())
	require.Equal(t, int64(0), initialValues[1].Int())
	require.Equal(t, int64(0), initialValues[2].Int())

	mustExecSQL(t, executor, "app", "alter table ledger add column note varchar(20) default 'guest', algorithm=instant")
	instant := <-executor.ExecuteQuery(nil, "select n_cols, instant_cols, total_row_versions from information_schema.innodb_tables where table_schema = 'app' and table_name = 'ledger'", "app")
	require.NoError(t, instant.Err)
	instantValues := instant.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, int64(6), instantValues[0].Int())
	require.Equal(t, int64(2), instantValues[1].Int())
	require.Equal(t, int64(1), instantValues[2].Int())
}

func TestInformationSchemaInnoDBTablesProjectsRowFormatAndTablespaceType(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "", "create tablespace shared add datafile 'shared.ibd'")
	mustExecSQL(t, executor, "app", "create table compressed_rows (id int primary key) row_format=compressed")
	mustExecSQL(t, executor, "app", "create table shared_rows (id int primary key) tablespace shared")

	result := <-executor.ExecuteQuery(nil, "select name, row_format, space_type from information_schema.innodb_tables where name like 'app/%'", "app")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	require.Len(t, rows.Records, 2)
	seenCompressed, seenShared := false, false
	for _, record := range rows.Records {
		values := record.GetValues()
		switch values[0].String() {
		case "app/compressed_rows":
			seenCompressed = true
			require.Equal(t, "Compressed", values[1].String())
			require.Equal(t, "Single", values[2].String())
		case "app/shared_rows":
			seenShared = true
			require.Equal(t, "Dynamic", values[1].String())
			require.Equal(t, "General", values[2].String())
		}
	}
	require.True(t, seenCompressed)
	require.True(t, seenShared)
}

func TestInformationSchemaInnoDBTablesProjectsPersistedTablespaceFlag(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key)")

	storage, err := executor.GetStorageManager().GetTableStorageManager().GetTableStorageInfo("app", "ledger")
	require.NoError(t, err)
	space, err := executor.GetStorageManager().GetSpaceManager().GetSpace(storage.SpaceID)
	require.NoError(t, err)
	data, err := space.LoadPageByPageNumber(0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), 95)
	binary.LittleEndian.PutUint32(data[91:95], 7)
	require.NoError(t, space.FlushToDisk(0, data))

	table := mustSelectResultSQL(t, executor, "app", "select flag from information_schema.innodb_tables where name='app/ledger'")
	tablespace := mustSelectResultSQL(t, executor, "app", "select flag from information_schema.innodb_tablespaces where name='app/ledger'")
	require.Len(t, table.Records, 1)
	require.Len(t, tablespace.Records, 1)
	require.Equal(t, int64(7), table.Records[0].GetValues()[0].Int())
	require.Equal(t, int64(7), tablespace.Records[0].GetValues()[0].Int())
}

func TestInformationSchemaInnoDBIndexesReflectsClusteredMapping(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key)")

	result := <-executor.ExecuteQuery(nil, "select name, type, table_id, page_no, space from information_schema.innodb_indexes where table_schema = 'app' and table_name = 'ledger'", "app")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	require.Len(t, rows.Records, 1)
	values := rows.Records[0].GetValues()
	require.Equal(t, "PRIMARY", values[0].String())
	require.Equal(t, int64(3), values[1].Int())
	require.Greater(t, values[2].Int(), int64(0))
	require.Greater(t, values[3].Int(), int64(0))
	require.Equal(t, values[2].Int(), values[4].Int())

	wrongIndex := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_indexes where table_schema = 'app' and table_name = 'ledger' and name = 'other'", "app")
	require.NoError(t, wrongIndex.Err)
	require.Empty(t, wrongIndex.Data.(*SelectResult).Records)
}

func TestInformationSchemaInnoDBIndexesProjectsSyntheticClusteredIndex(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (label varchar(32))")

	result := <-executor.ExecuteQuery(nil, "select name, type, n_fields, page_no from information_schema.innodb_indexes where table_schema = 'app' and table_name = 'ledger'", "app")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	require.Len(t, rows.Records, 1)
	values := rows.Records[0].GetValues()
	require.Equal(t, "GEN_CLUST_INDEX", values[0].String())
	require.Equal(t, int64(1), values[1].Int())
	require.Equal(t, int64(0), values[2].Int())
	require.Nil(t, values[3].Raw())
}

func TestInformationSchemaInnoDBIndexesProjectsPersistedSecondaryIndexes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key, label varchar(32))")
	mustExecSQL(t, executor, "app", "alter table ledger add index idx_label (label)")

	result := <-executor.ExecuteQuery(nil, "select name, type, n_fields, page_no, space from information_schema.innodb_indexes where table_schema = 'app' and table_name = 'ledger' and name = 'idx_label'", "app")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	require.Len(t, rows.Records, 1)
	values := rows.Records[0].GetValues()
	require.Equal(t, "idx_label", values[0].String())
	require.Equal(t, int64(0), values[1].Int())
	require.Equal(t, int64(1), values[2].Int())
	require.Nil(t, values[3].Raw())
	require.Greater(t, values[4].Int(), int64(0))
}

func TestInformationSchemaInnoDBFieldsProjectsPersistedSecondaryIndexColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key, label varchar(32), code int)")
	mustExecSQL(t, executor, "app", "alter table ledger add unique index uq_label_code (label, code)")

	result := <-executor.ExecuteQuery(nil, "select index_id, name, pos from information_schema.innodb_fields where table_schema = 'app' and table_name = 'ledger' and name in ('label', 'code')", "app")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	require.Len(t, rows.Records, 2)
	first := rows.Records[0].GetValues()
	second := rows.Records[1].GetValues()
	require.Equal(t, "label", first[1].String())
	require.Equal(t, int64(0), first[2].Int())
	require.Equal(t, "code", second[1].String())
	require.Equal(t, int64(1), second[2].Int())
	require.Equal(t, first[0].Int(), second[0].Int())
}

func TestInformationSchemaInnoDBTableDictionaryFiltersRuntimeFields(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key, label varchar(32))")
	mustExecSQL(t, executor, "app", "create table generated_ledger (id int, total int generated always as (id + 1) stored)")

	wrongTableID := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_tables where table_id = 999999999", "app")
	require.NoError(t, wrongTableID.Err)
	require.Empty(t, wrongTableID.Data.(*SelectResult).Records)
	wrongIndexID := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_fields where index_id = 999999999", "app")
	require.NoError(t, wrongIndexID.Err)
	require.Empty(t, wrongIndexID.Data.(*SelectResult).Records)
	wrongPosition := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_fields where pos = 99", "app")
	require.NoError(t, wrongPosition.Err)
	require.Empty(t, wrongPosition.Data.(*SelectResult).Records)
	virtual := <-executor.ExecuteQuery(nil, "select table_id, pos, base_pos, m_cols from information_schema.innodb_virtual", "app")
	require.NoError(t, virtual.Err)
	require.Len(t, virtual.Data.(*SelectResult).Records, 1)
	virtualValues := virtual.Data.(*SelectResult).Records[0].GetValues()
	require.Equal(t, int64(1), virtualValues[1].Int())
	require.Equal(t, int64(0), virtualValues[2].Int())
	require.Equal(t, int64(1), virtualValues[3].Int())
	wrongVirtualCols := <-executor.ExecuteQuery(nil, "select table_id from information_schema.innodb_virtual where m_cols = 999999", "app")
	require.NoError(t, wrongVirtualCols.Err)
	require.Empty(t, wrongVirtualCols.Data.(*SelectResult).Records)
}

func TestInformationSchemaInnoDBColumnsReflectsDurableDefinition(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key, label varchar(32))")

	result := <-executor.ExecuteQuery(nil, "select pos, name, mtype, len from information_schema.innodb_columns where table_schema = 'app' and table_name = 'ledger'", "app")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	require.Len(t, rows.Records, 2)
	require.Equal(t, int64(0), rows.Records[0].GetValues()[0].Int())
	require.Equal(t, "id", rows.Records[0].GetValues()[1].String())
	require.Equal(t, int64(6), rows.Records[0].GetValues()[2].Int())
	require.Equal(t, int64(4), rows.Records[0].GetValues()[3].Int())
	require.Equal(t, int64(1), rows.Records[1].GetValues()[0].Int())
	require.Equal(t, "label", rows.Records[1].GetValues()[1].String())
	require.Equal(t, int64(1), rows.Records[1].GetValues()[2].Int())
	require.Equal(t, int64(128), rows.Records[1].GetValues()[3].Int())
	wrongName := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_columns where table_schema = 'app' and table_name = 'ledger' and name = 'other'", "app")
	require.NoError(t, wrongName.Err)
	require.Empty(t, wrongName.Data.(*SelectResult).Records)
	wrongPosition := <-executor.ExecuteQuery(nil, "select name from information_schema.innodb_columns where table_schema = 'app' and table_name = 'ledger' and pos = 99", "app")
	require.NoError(t, wrongPosition.Err)
	require.Empty(t, wrongPosition.Data.(*SelectResult).Records)
}

func TestInformationSchemaInnoDBColumnsReportsInstantAddIndicator(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key)")
	mustExecSQL(t, executor, "app", "alter table ledger add column note varchar(20) default 'guest', algorithm=instant")

	result := <-executor.ExecuteQuery(nil, "select name, has_default from information_schema.innodb_columns where table_schema = 'app' and table_name = 'ledger' and name = 'note'", "app")
	require.NoError(t, result.Err)
	rows := result.Data.(*SelectResult)
	require.Len(t, rows.Records, 1)
	require.Equal(t, "note", rows.Records[0].GetValues()[0].String())
	require.Equal(t, int64(1), rows.Records[0].GetValues()[1].Int())
}

func TestInformationSchemaInnoDBColumnsProjectsBinaryMainTypes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table binary_columns (id int primary key, fixed_value binary(4), variable_value varbinary(8))")

	result := mustSelectResultSQL(t, executor, "app", "select name, mtype, len from information_schema.innodb_columns where table_schema = 'app' and table_name = 'binary_columns' and name in ('fixed_value', 'variable_value') order by pos")
	require.Len(t, result.Records, 2)
	first := result.Records[0].GetValues()
	require.Equal(t, "fixed_value", first[0].String())
	require.Equal(t, int64(3), first[1].Int())
	require.Equal(t, int64(4), first[2].Int())
	second := result.Records[1].GetValues()
	require.Equal(t, "variable_value", second[0].String())
	require.Equal(t, int64(4), second[1].Int())
	require.Equal(t, int64(8), second[2].Int())
}

func TestInformationSchemaInnoDBForeignProjectsPersistedDictionary(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table child (id int primary key, parent_id int, constraint fk_parent foreign key (parent_id) references parent (id))")

	foreign := mustSelectResultSQL(t, executor, "app", "select id, for_name, ref_name, n_cols, type from information_schema.innodb_foreign where for_name = 'app/child'")
	require.Len(t, foreign.Records, 1)
	foreignValues := foreign.Records[0].GetValues()
	require.Equal(t, "app/fk_parent", foreignValues[0].String())
	require.Equal(t, "app/child", foreignValues[1].String())
	require.Equal(t, "app/parent", foreignValues[2].String())
	require.Equal(t, int64(1), foreignValues[3].Int())
	require.Equal(t, int64(0), foreignValues[4].Int())
	wrongForeignCount := mustSelectResultSQL(t, executor, "app", "select id from information_schema.innodb_foreign where for_name = 'app/child' and n_cols = 99")
	require.Empty(t, wrongForeignCount.Records)
	foreignLike := mustSelectResultSQL(t, executor, "app", "select id from information_schema.innodb_foreign where for_name like 'app/%' and ref_name like 'app/par%' ")
	require.Len(t, foreignLike.Records, 1)
	wrongForeignLike := mustSelectResultSQL(t, executor, "app", "select id from information_schema.innodb_foreign where ref_name like 'other/%'")
	require.Empty(t, wrongForeignLike.Records)
	foreignColumns := mustSelectResultSQL(t, executor, "app", "select id, for_col_name, ref_col_name, pos from information_schema.innodb_foreign_cols where id = 'app/fk_parent'")
	require.Len(t, foreignColumns.Records, 1)
	foreignColumnValues := foreignColumns.Records[0].GetValues()
	require.Equal(t, "app/fk_parent", foreignColumnValues[0].String())
	require.Equal(t, "parent_id", foreignColumnValues[1].String())
	require.Equal(t, "id", foreignColumnValues[2].String())
	require.Equal(t, int64(0), foreignColumnValues[3].Int())
	wrongForeignColumn := mustSelectResultSQL(t, executor, "app", "select id from information_schema.innodb_foreign_cols where id = 'app/fk_parent' and for_col_name = 'other'")
	require.Empty(t, wrongForeignColumn.Records)
	foreignColumnLike := mustSelectResultSQL(t, executor, "app", "select id from information_schema.innodb_foreign_cols where id like 'app/fk_%' and for_col_name like 'parent_%'")
	require.Len(t, foreignColumnLike.Records, 1)
	wrongForeignPosition := mustSelectResultSQL(t, executor, "app", "select id from information_schema.innodb_foreign_cols where id = 'app/fk_parent' and pos = 99")
	require.Empty(t, wrongForeignPosition.Records)
}

func TestInformationSchemaInnoDBForeignProjectsReferentialActionTypeFlags(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table parent (id int primary key)")
	mustExecSQL(t, executor, "app", "create table parent_two (id int primary key)")
	mustExecSQL(t, executor, "app", "create table child_actions (id int primary key, parent_id int, parent_two_id int, constraint fk_cascade foreign key (parent_id) references parent (id) on delete cascade on update set null, constraint fk_no_action foreign key (parent_two_id) references parent_two (id) on delete no action on update cascade)")

	result := mustSelectResultSQL(t, executor, "app", "select id, type from information_schema.innodb_foreign where for_name = 'app/child_actions' order by id")
	require.Len(t, result.Records, 2)
	first := result.Records[0].GetValues()
	second := result.Records[1].GetValues()
	require.Equal(t, "app/fk_cascade", first[0].String())
	require.Equal(t, int64(9), first[1].Int())
	require.Equal(t, "app/fk_no_action", second[0].String())
	require.Equal(t, int64(20), second[1].Int())
}
