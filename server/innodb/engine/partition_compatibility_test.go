package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionedCreatePersistsInformationSchemaMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	query := "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))"
	result := <-executor.ExecuteQuery(nil, query, "app")
	require.NoError(t, result.Err)
	raw, err := os.ReadFile(filepath.Join(executor.GetDataDir(), "app", "events.frm"))
	require.NoError(t, err)
	var persisted map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &persisted))
	require.Contains(t, persisted, "partitioning")
	storageInfo, err := executor.GetStorageManager().GetTableStorageManager().GetTableStorageInfo("app", "events")
	require.NoError(t, err)
	require.NotEmpty(t, storageInfo.Partitioning)

	rows := mustQuerySQL(t, executor, "app", "select table_name, partition_name, partition_method, partition_expression, partition_description from information_schema.partitions where table_schema = 'app' and table_name = 'events'")
	require.Len(t, rows, 2)
	require.Equal(t, "p0", rows[0][1])
	require.Equal(t, "RANGE", rows[0][2])
	require.Equal(t, "id", rows[0][3])
	require.Equal(t, "10", rows[0][4])
	require.Equal(t, "MAXVALUE", rows[1][4])
}

func TestPartitionedTableUsesIndependentPhysicalSpaces(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "insert into events values (3), (17)")

	storage := executor.GetStorageManager().GetTableStorageManager()
	info, err := storage.GetTableStorageInfo("app", "events")
	require.NoError(t, err)
	require.Len(t, info.Partitions, 2)
	require.NotEqual(t, info.Partitions[0].SpaceID, info.Partitions[1].SpaceID)
	require.NotZero(t, info.Partitions[0].RootPageNo)
	require.NotZero(t, info.Partitions[1].RootPageNo)

	spaces, err := executor.GetStorageManager().ListSpaces()
	require.NoError(t, err)
	spaceNames := make(map[string]bool)
	for _, space := range spaces {
		spaceNames[space.Name] = true
	}
	require.True(t, spaceNames["app/events#p0"])
	require.True(t, spaceNames["app/events#p1"])
	require.Equal(t, [][]interface{}{{"3"}, {"17"}}, mustQuerySQL(t, executor, "app", "select id from events order by id"))
}

func TestRangeColumnsPartitionRoutingAndReadback(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table range_columns (id int primary key) partition by range columns (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "insert into range_columns values (3), (17)")
	require.Equal(t, [][]interface{}{{"3"}, {"17"}}, mustQuerySQL(t, executor, "app", "select id from range_columns order by id"))
}

func TestRangeColumnsStringPartitionRoutingAndReadback(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table range_columns_text (region varchar(16) primary key) partition by range columns (region) (partition p0 values less than ('m'), partition p1 values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "insert into range_columns_text values ('alpha'), ('zulu')")
	require.Equal(t, [][]interface{}{{"alpha"}, {"zulu"}}, mustQuerySQL(t, executor, "app", "select region from range_columns_text order by region"))
}

func TestRangeFunctionPartitionRoutingAndReadback(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events_by_year (id int primary key, event_date date) partition by range (year(event_date)) (partition p2024 values less than (2025), partition pmax values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "insert into events_by_year values (1, '2024-06-01'), (2, '2025-06-01')")
	require.Equal(t, [][]interface{}{{"1", "2024-06-01"}, {"2", "2025-06-01"}}, mustQuerySQL(t, executor, "app", "select id, event_date from events_by_year order by id"))
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select id from events_by_year where year(event_date) = 2024"))
}

func TestRangeToDaysFunctionPartitionRoutingAndReadback(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events_by_day (id int primary key, event_date date) partition by range (to_days(event_date)) (partition p2024 values less than (739617), partition pmax values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "insert into events_by_day values (1, '2024-06-01'), (2, '2025-06-01')")
	require.Equal(t, [][]interface{}{{"1", "2024-06-01"}, {"2", "2025-06-01"}}, mustQuerySQL(t, executor, "app", "select id, event_date from events_by_day order by id"))
}

func TestRangeArithmeticPartitionRoutingAndReadback(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table shifted_events (id int primary key) partition by range (id + 10) (partition p0 values less than (20), partition p1 values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "insert into shifted_events values (5), (15)")
	require.Equal(t, [][]interface{}{{"5"}, {"15"}}, mustQuerySQL(t, executor, "app", "select id from shifted_events order by id"))
}

func TestRangeCastPartitionRoutingAndReadback(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table cast_events (code varchar(8) primary key) partition by range (cast(code as signed)) (partition p0 values less than (10), partition p1 values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "insert into cast_events values ('5'), ('15')")
	require.Equal(t, [][]interface{}{{"15"}, {"5"}}, mustQuerySQL(t, executor, "app", "select code from cast_events order by code"))
}

func TestExchangePartitionSwapsRowsWithOrdinaryTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key, label varchar(16)) partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "create table exchange_events (id int primary key, label varchar(16))")
	mustExecSQL(t, executor, "app", "insert into events values (3, 'source'), (17, 'stay')")
	mustExecSQL(t, executor, "app", "insert into exchange_events values (7, 'exchange')")
	mustExecSQL(t, executor, "app", "alter table events exchange partition p0 with table exchange_events")
	require.Equal(t, [][]interface{}{{"7", "exchange"}, {"17", "stay"}}, mustQuerySQL(t, executor, "app", "select id, label from events order by id"))
	require.Equal(t, [][]interface{}{{"3", "source"}}, mustQuerySQL(t, executor, "app", "select id, label from exchange_events order by id"))
}

func TestRemovePartitioningMigratesRowsToOrdinaryTable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table removable (id int primary key, label varchar(16)) partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "insert into removable values (3, 'low'), (17, 'high')")
	mustExecSQL(t, executor, "app", "alter table removable remove partitioning")
	require.Equal(t, [][]interface{}{{"3", "low"}, {"17", "high"}}, mustQuerySQL(t, executor, "app", "select id, label from removable order by id"))
	mustExecSQL(t, executor, "app", "insert into removable values (25, 'ordinary')")
	require.Equal(t, [][]interface{}{{"3", "low"}, {"17", "high"}, {"25", "ordinary"}}, mustQuerySQL(t, executor, "app", "select id, label from removable order by id"))
	rows := mustQuerySQL(t, executor, "app", "select partition_name from information_schema.partitions where table_schema = 'app' and table_name = 'removable'")
	require.Equal(t, [][]interface{}{{nil}}, rows)
}

func TestRemovePartitioningPreservesDuplicateRowsWithoutPrimaryKey(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table duplicate_rows (id int, label varchar(16)) partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "insert into duplicate_rows values (3, 'same'), (3, 'same'), (17, 'high')")
	mustExecSQL(t, executor, "app", "alter table duplicate_rows remove partitioning")
	require.Equal(t, [][]interface{}{{"3", "same"}, {"3", "same"}, {"17", "high"}}, mustQuerySQL(t, executor, "app", "select id, label from duplicate_rows order by id"))
}

func TestRangeColumnsMultiColumnPartitionRoutingAndReadback(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table range_columns_multi (tenant_id int, region varchar(16), primary key (tenant_id, region)) partition by range columns (tenant_id, region) (partition p0 values less than (10, 'm'), partition p1 values less than (maxvalue, maxvalue))")
	mustExecSQL(t, executor, "app", "insert into range_columns_multi values (3, 'zulu'), (10, 'alpha'), (10, 'zulu'), (17, 'alpha')")
	require.Equal(t, [][]interface{}{{"3", "zulu"}, {"10", "alpha"}, {"10", "zulu"}, {"17", "alpha"}}, mustQuerySQL(t, executor, "app", "select tenant_id, region from range_columns_multi order by tenant_id, region"))
}

func TestListColumnsMultiColumnPartitionRoutingAndReadback(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table list_columns_multi (tenant_id int, region varchar(16), primary key (tenant_id, region)) partition by list columns (tenant_id, region) (partition p_east values in ((1, 'east'), (2, 'west')), partition p_south values in ((3, 'south')))")
	mustExecSQL(t, executor, "app", "insert into list_columns_multi values (1, 'east'), (2, 'west'), (3, 'south')")
	require.Equal(t, [][]interface{}{{"1", "east"}, {"2", "west"}, {"3", "south"}}, mustQuerySQL(t, executor, "app", "select tenant_id, region from list_columns_multi order by tenant_id, region"))
}

func TestReorganizeMultiColumnRangePartitionsPreservesRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table range_columns_reorg (tenant_id int, region varchar(16), primary key (tenant_id, region)) partition by range columns (tenant_id, region) (partition p0 values less than (10, 'm'), partition pmax values less than (maxvalue, maxvalue))")
	mustExecSQL(t, executor, "app", "insert into range_columns_reorg values (3, 'zulu'), (10, 'alpha'), (10, 'zulu')")
	mustExecSQL(t, executor, "app", "alter table range_columns_reorg reorganize partition p0 into (partition p0a values less than (5, 'z'), partition p0b values less than (10, 'm'))")
	require.Equal(t, [][]interface{}{{"3", "zulu"}, {"10", "alpha"}, {"10", "zulu"}}, mustQuerySQL(t, executor, "app", "select tenant_id, region from range_columns_reorg order by tenant_id, region"))
	require.Equal(t, [][]interface{}{{"p0a"}, {"p0b"}, {"pmax"}}, mustQuerySQL(t, executor, "app", "select partition_name from information_schema.partitions where table_schema = 'app' and table_name = 'range_columns_reorg'"))
}

func TestReorganizeMultiColumnListPartitionsPreservesRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table list_columns_reorg (tenant_id int, region varchar(16), primary key (tenant_id, region)) partition by list columns (tenant_id, region) (partition p0 values in ((1, 'east'), (2, 'west')), partition p1 values in ((3, 'south')))")
	mustExecSQL(t, executor, "app", "insert into list_columns_reorg values (1, 'east'), (2, 'west'), (3, 'south')")
	mustExecSQL(t, executor, "app", "alter table list_columns_reorg reorganize partition p0 into (partition p0a values in ((1, 'east')), partition p0b values in ((2, 'west')))")
	require.Equal(t, [][]interface{}{{"1", "east"}, {"2", "west"}, {"3", "south"}}, mustQuerySQL(t, executor, "app", "select tenant_id, region from list_columns_reorg order by tenant_id, region"))
	require.Equal(t, [][]interface{}{{"p0a"}, {"p0b"}, {"p1"}}, mustQuerySQL(t, executor, "app", "select partition_name from information_schema.partitions where table_schema = 'app' and table_name = 'list_columns_reorg'"))
}

func TestReorganizeMultiColumnListPartitionsRejectsDuplicateTuple(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table list_columns_reorg_duplicate (tenant_id int, region varchar(16), primary key (tenant_id, region)) partition by list columns (tenant_id, region) (partition p0 values in ((1, 'east')), partition p1 values in ((2, 'west')))")
	result := <-executor.ExecuteQuery(nil, "alter table list_columns_reorg_duplicate reorganize partition p0 into (partition p0a values in ((1, 'east')), partition p0b values in ((2, 'west')))", "app")
	require.Error(t, result.Err)
	require.Equal(t, [][]interface{}{{"p0"}, {"p1"}}, mustQuerySQL(t, executor, "app", "select partition_name from information_schema.partitions where table_schema = 'app' and table_name = 'list_columns_reorg_duplicate'"))
}

func TestPartitionedDMLRejectsOutOfRangeRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (20))")
	mustExecSQL(t, executor, "app", "insert into events values (7)")
	result := <-executor.ExecuteQuery(nil, "insert into events values (21)", "app")
	if result.Err == nil {
		t.Fatal("expected out-of-range partition insert to fail")
	}
}

func TestDropPartitionUpdatesDescriptorAndInformationSchema(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (20))")
	mustExecSQL(t, executor, "app", "insert into events values (7), (17)")
	mustExecSQL(t, executor, "app", "alter table events drop partition p0")
	rowsAfterDrop := mustQuerySQL(t, executor, "app", "select id from events order by id")
	require.Equal(t, [][]interface{}{{"17"}}, rowsAfterDrop)
	rows := mustQuerySQL(t, executor, "app", "select partition_name from information_schema.partitions where table_schema = 'app' and table_name = 'events'")
	require.Equal(t, [][]interface{}{{"p1"}}, rows)
	info, err := executor.GetStorageManager().GetTableStorageManager().GetTableStorageInfo("app", "events")
	require.NoError(t, err)
	partitions, ok := info.Partitioning["partitions"].([]interface{})
	require.True(t, ok)
	require.Len(t, partitions, 1)
}

func TestTruncatePartitionDeletesRowsButKeepsDescriptor(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (20))")
	mustExecSQL(t, executor, "app", "insert into events values (7), (17)")
	mustExecSQL(t, executor, "app", "alter table events truncate partition p0")
	require.Equal(t, [][]interface{}{{"17"}}, mustQuerySQL(t, executor, "app", "select id from events order by id"))
	require.Equal(t, [][]interface{}{{"p0"}, {"p1"}}, mustQuerySQL(t, executor, "app", "select partition_name from information_schema.partitions where table_schema = 'app' and table_name = 'events'"))
}

func TestMultiColumnDropAndTruncatePartitionDeletesRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table range_columns_cleanup (tenant_id int, region varchar(16), primary key (tenant_id, region)) partition by range columns (tenant_id, region) (partition p0 values less than (10, 'm'), partition p1 values less than (maxvalue, maxvalue))")
	mustExecSQL(t, executor, "app", "insert into range_columns_cleanup values (3, 'zulu'), (10, 'alpha'), (10, 'zulu')")
	mustExecSQL(t, executor, "app", "alter table range_columns_cleanup truncate partition p0")
	require.Equal(t, [][]interface{}{{"10", "zulu"}}, mustQuerySQL(t, executor, "app", "select tenant_id, region from range_columns_cleanup order by tenant_id, region"))
	mustExecSQL(t, executor, "app", "insert into range_columns_cleanup values (3, 'zulu')")
	mustExecSQL(t, executor, "app", "alter table range_columns_cleanup drop partition p0")
	require.Equal(t, [][]interface{}{{"10", "zulu"}}, mustQuerySQL(t, executor, "app", "select tenant_id, region from range_columns_cleanup order by tenant_id, region"))
}

func TestAddPartitionExtendsRangeDescriptor(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (20))")
	mustExecSQL(t, executor, "app", "alter table events add partition (partition p2 values less than (30))")
	mustExecSQL(t, executor, "app", "insert into events values (27)")
	require.Equal(t, [][]interface{}{{"27"}}, mustQuerySQL(t, executor, "app", "select id from events"))
	require.Equal(t, [][]interface{}{{"p0"}, {"p1"}, {"p2"}}, mustQuerySQL(t, executor, "app", "select partition_name from information_schema.partitions where table_schema = 'app' and table_name = 'events'"))
}

func TestAddMultiColumnRangePartitionExtendsTupleDescriptor(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table range_columns_add (tenant_id int, region varchar(16), primary key (tenant_id, region)) partition by range columns (tenant_id, region) (partition p0 values less than (10, 'm'))")
	mustExecSQL(t, executor, "app", "alter table range_columns_add add partition (partition p1 values less than (20, 'a'))")
	mustExecSQL(t, executor, "app", "insert into range_columns_add values (10, 'alpha'), (17, 'zulu')")
	require.Equal(t, [][]interface{}{{"10", "alpha"}, {"17", "zulu"}}, mustQuerySQL(t, executor, "app", "select tenant_id, region from range_columns_add order by tenant_id, region"))
	require.Equal(t, [][]interface{}{{"p0"}, {"p1"}}, mustQuerySQL(t, executor, "app", "select partition_name from information_schema.partitions where table_schema = 'app' and table_name = 'range_columns_add'"))
}

func TestReorganizePartitionReplacesRangeDescriptor(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (20))")
	mustExecSQL(t, executor, "app", "insert into events values (3), (7), (17)")
	mustExecSQL(t, executor, "app", "alter table events reorganize partition p0 into (partition p0a values less than (5), partition p0b values less than (10))")
	require.Equal(t, [][]interface{}{{"p0a"}, {"p0b"}, {"p1"}}, mustQuerySQL(t, executor, "app", "select partition_name from information_schema.partitions where table_schema = 'app' and table_name = 'events'"))
	require.Equal(t, [][]interface{}{{"3"}, {"7"}, {"17"}}, mustQuerySQL(t, executor, "app", "select id from events order by id"))
}

func TestReorganizePartitionSupportsMultipleAndListPartitions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than (30))")
	mustExecSQL(t, executor, "app", "alter table events reorganize partition p0, p1 into (partition p01 values less than (20))")
	require.Equal(t, [][]interface{}{{"p01"}, {"p2"}}, mustQuerySQL(t, executor, "app", "select partition_name from information_schema.partitions where table_schema = 'app' and table_name = 'events'"))

	mustExecSQL(t, executor, "app", "create table buckets (id int primary key) partition by list (id) (partition p0 values in (1, 2), partition p1 values in (3, 4))")
	mustExecSQL(t, executor, "app", "alter table buckets reorganize partition p0 into (partition p_even values in (2), partition p_one values in (1))")
	require.Equal(t, [][]interface{}{{"p_even"}, {"p_one"}, {"p1"}}, mustQuerySQL(t, executor, "app", "select partition_name from information_schema.partitions where table_schema = 'app' and table_name = 'buckets'"))
}

func TestReorganizePartitionRejectsExistingRowsOutsideNewRange(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (20))")
	mustExecSQL(t, executor, "app", "insert into events values (7)")
	result := <-executor.ExecuteQuery(nil, "alter table events reorganize partition p0 into (partition p0a values less than (5), partition p0b values less than (6))", "app")
	require.Error(t, result.Err)
	require.Equal(t, [][]interface{}{{"p0"}, {"p1"}}, mustQuerySQL(t, executor, "app", "select partition_name from information_schema.partitions where table_schema = 'app' and table_name = 'events'"))
}

func TestListAndHashPartitionRoutingIsDeterministic(t *testing.T) {
	list := []interface{}{
		map[string]interface{}{"name": "p0", "values": "VALUES IN (1, 3)"},
		map[string]interface{}{"name": "p1", "values": "VALUES IN (2, 4)"},
	}
	partition, err := partitionForValue("LIST", int64(3), list)
	require.NoError(t, err)
	require.Equal(t, 0, partition)

	hash := []interface{}{
		map[string]interface{}{"name": "p0"},
		map[string]interface{}{"name": "p1"},
		map[string]interface{}{"name": "p2"},
	}
	partition, err = partitionForValue("HASH", int64(-1), hash)
	require.NoError(t, err)
	require.Equal(t, 2, partition)
}

func TestStringListPartitionRoutingAndPruning(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table regions (region varchar(16) primary key) partition by list (region) (partition p_east values in ('east', 'west'), partition p_south values in ('south'))")
	mustExecSQL(t, executor, "app", "insert into regions values ('west'), ('south')")
	require.Equal(t, [][]interface{}{{"west"}}, mustQuerySQL(t, executor, "app", "select region from regions where region = 'west'"))
	require.Equal(t, [][]interface{}{{"west"}}, mustQuerySQL(t, executor, "app", "select region from regions where region in ('west')"))
	require.Equal(t, [][]interface{}{{"south"}}, mustQuerySQL(t, executor, "app", "select region from regions where region not in ('east', 'west')"))
	info, err := executor.GetStorageManager().GetTableStorageManager().GetTableStorageInfo("app", "regions")
	require.NoError(t, err)
	require.Len(t, info.Partitions, 2)
}

func TestPartitionConstantPredicatesPreserveResultsAcrossPrunedRanges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "insert into events values (-3), (7), (17), (27)")
	require.Equal(t, [][]interface{}{{"-3"}, {"7"}}, mustQuerySQL(t, executor, "app", "select id from events where id < 10 order by id"))
	require.Equal(t, [][]interface{}{{"17"}, {"27"}}, mustQuerySQL(t, executor, "app", "select id from events where id >= 10 order by id"))
	require.Equal(t, [][]interface{}{{"17"}}, mustQuerySQL(t, executor, "app", "select id from events where id between 10 and 20 order by id"))
	require.Equal(t, [][]interface{}{{"-3"}, {"7"}, {"27"}}, mustQuerySQL(t, executor, "app", "select id from events where id not between 10 and 20 order by id"))
}
