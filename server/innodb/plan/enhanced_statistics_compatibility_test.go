package plan

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

type statisticsRecordAccessor struct {
	records          [][]interface{}
	rate             float64
	dataSize         int64
	indexSize        int64
	spaceID          uint32
	indexID          uint32
	indexCardinality int64
	treeDepth        int
	leafPages        int64
	nonLeafPages     int64
}

func (a *statisticsRecordAccessor) GetTableRowCount(uint32) (int64, error) {
	return int64(len(a.records)), nil
}
func (a *statisticsRecordAccessor) SampleTableRecords(_ uint32, rate float64) ([][]interface{}, error) {
	a.rate = rate
	return a.records, nil
}
func (a *statisticsRecordAccessor) GetIndexCardinality(uint32) (int64, error) {
	if a.indexCardinality == 0 {
		return 0, nil
	}
	return a.indexCardinality, nil
}
func (a *statisticsRecordAccessor) GetTableSpaceSize(uint32) (int64, int64, error) {
	return a.dataSize, a.indexSize, nil
}
func (a *statisticsRecordAccessor) GetBTreeStatistics(uint32) (int, int64, int64, error) {
	if a.treeDepth == 0 && a.leafPages == 0 && a.nonLeafPages == 0 {
		return 0, 0, 0, fmt.Errorf("not implemented in statistics test accessor")
	}
	return a.treeDepth, a.leafPages, a.nonLeafPages, nil
}
func (a *statisticsRecordAccessor) GetTableSpaceID(string, string) (uint32, error) {
	return a.spaceID, nil
}
func (a *statisticsRecordAccessor) GetIndexID(string, string, string) (uint32, error) {
	if a.indexID == 0 {
		return 0, fmt.Errorf("index ID is not configured")
	}
	return a.indexID, nil
}

var _ StorageEngineAccessor = (*statisticsRecordAccessor)(nil)
var _ basic.Space = (*statsTestSpace)(nil)

type unavailableStatsSpaceManager struct{}

func (unavailableStatsSpaceManager) CreateSpace(uint32, string, bool) (basic.Space, error) {
	return nil, fmt.Errorf("space unavailable")
}
func (unavailableStatsSpaceManager) GetSpace(uint32) (basic.Space, error) {
	return nil, fmt.Errorf("space unavailable")
}
func (unavailableStatsSpaceManager) DropSpace(uint32) error { return fmt.Errorf("space unavailable") }
func (unavailableStatsSpaceManager) AllocateExtent(uint32, basic.ExtentPurpose) (basic.Extent, error) {
	return nil, fmt.Errorf("space unavailable")
}
func (unavailableStatsSpaceManager) FreeExtent(uint32, uint32) error {
	return fmt.Errorf("space unavailable")
}
func (unavailableStatsSpaceManager) Begin() (basic.Tx, error) {
	return nil, fmt.Errorf("space unavailable")
}
func (unavailableStatsSpaceManager) CreateNewTablespace(string) uint32 {
	return 0
}
func (unavailableStatsSpaceManager) CreateTableSpace(string) (uint32, error) {
	return 0, fmt.Errorf("space unavailable")
}
func (unavailableStatsSpaceManager) GetTableSpace(uint32) (basic.FileTableSpace, error) {
	return nil, fmt.Errorf("space unavailable")
}
func (unavailableStatsSpaceManager) GetTableSpaceByName(string) (basic.FileTableSpace, error) {
	return nil, fmt.Errorf("space unavailable")
}
func (unavailableStatsSpaceManager) GetTableSpaceInfo(uint32) (*basic.TableSpaceInfo, error) {
	return nil, fmt.Errorf("space unavailable")
}
func (unavailableStatsSpaceManager) DropTableSpace(uint32) error {
	return fmt.Errorf("space unavailable")
}
func (unavailableStatsSpaceManager) Close() error { return nil }

var _ basic.SpaceManager = (*unavailableStatsSpaceManager)(nil)

func TestEnhancedStatisticsCollectorResolvesTableSpaceThroughStorageAccessor(t *testing.T) {
	accessor := &statisticsRecordAccessor{spaceID: 4242}
	collector := NewEnhancedStatisticsCollector(nil, nil, nil)
	collector.SetStorageEngineAccessor(accessor)
	defer collector.Stop()

	schema := metadata.NewSchema("analytics")
	table := metadata.NewTable("events")
	if err := schema.AddTable(table); err != nil {
		t.Fatalf("AddTable() error = %v", err)
	}

	if got := collector.getTableSpaceID(table); got != 4242 {
		t.Fatalf("getTableSpaceID() = %d, want accessor mapping 4242", got)
	}
}

func TestAdaptiveSamplerUsesConfiguredThresholds(t *testing.T) {
	sampler := NewAdaptiveSampler()
	sampler.mu.Lock()
	sampler.sampleRates = map[int64]float64{
		100:  0.8,
		1000: 0.4,
	}
	sampler.mu.Unlock()

	if got := sampler.GetSampleRate(99); got != 0.8 {
		t.Fatalf("GetSampleRate(99) = %v, want configured 0.8", got)
	}
	if got := sampler.GetSampleRate(100); got != 0.4 {
		t.Fatalf("GetSampleRate(100) = %v, want configured 0.4", got)
	}
	if got := sampler.GetSampleRate(1000); got != 0.4 {
		t.Fatalf("GetSampleRate(1000) = %v, want last configured 0.4", got)
	}
}

func TestStatisticsFindMinMaxIgnoresNullSamples(t *testing.T) {
	values := []interface{}{nil, int64(2), int64(1), nil}

	maxValue, minValue := findMinMax(values)
	if maxValue != int64(2) || minValue != int64(1) {
		t.Fatalf("findMinMax() = max=%#v min=%#v, want max=2 min=1", maxValue, minValue)
	}

	collector := &EnhancedStatisticsCollector{}
	maxValue, minValue = collector.findMinMax(values)
	if maxValue != int64(2) || minValue != int64(1) {
		t.Fatalf("EnhancedStatisticsCollector.findMinMax() = max=%#v min=%#v, want max=2 min=1", maxValue, minValue)
	}
}

func TestEnhancedStatisticsCollectorFallsBackAndInvalidatesSafely(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{
		ExpirationTime:     time.Hour,
		SampleRate:         1,
		HistogramBuckets:   8,
		EnableAutoUpdate:   false,
		AutoUpdateInterval: time.Hour,
	}, nil, nil)
	defer collector.Stop()

	table := metadata.NewTable("stats_fallback")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt, IsNullable: false})
	ctx := context.Background()
	if _, err := collector.CollectTableStatistics(ctx, table); err != nil {
		t.Fatalf("CollectTableStatistics() error = %v", err)
	}
	columnStats, err := collector.CollectColumnStatistics(ctx, table, table.Columns[0])
	if err != nil {
		t.Fatalf("first CollectColumnStatistics() error = %v", err)
	}
	if columnStats.DistinctCount <= 0 {
		t.Fatalf("fallback column distinct count = %d, want positive", columnStats.DistinctCount)
	}

	collector.Invalidate("stats_fallback")
	if _, exists := collector.GetTableStatistics("stats_fallback"); exists {
		t.Fatal("Invalidate() left table statistics in cache")
	}
	if _, err := collector.CollectTableStatistics(ctx, table); err != nil {
		t.Fatalf("recollect after Invalidate() error = %v", err)
	}
	refreshed, exists := collector.GetTableStatistics("stats_fallback")
	if !exists || refreshed.ModifyCount != 1 {
		t.Fatalf("recollect should expose one invalidation, got %#v", refreshed)
	}
	collector.Invalidate("stats_fallback")
	collector.Invalidate("stats_fallback")
	refreshed, err = collector.CollectTableStatistics(ctx, table)
	if err != nil || refreshed.ModifyCount != 2 {
		t.Fatalf("batched invalidations should be counted before refresh: stats=%#v err=%v", refreshed, err)
	}
}

func TestStatisticsCollectorUsesCaseInsensitiveCacheKeys(t *testing.T) {
	collector := NewStatisticsCollector(&StatisticsConfig{
		ExpirationTime:   time.Hour,
		EnableAutoUpdate: false,
	})
	defer collector.Stop()

	now := time.Now().Unix()
	tableStats := &TableStats{TableName: "Orders", RowCount: 17, LastAnalyzeTime: now}
	columnStats := &ColumnStats{ColumnName: "CustomerID", DistinctCount: 9}
	indexStats := &IndexStats{IndexName: "PRIMARY", Cardinality: 17}
	collector.tableStats["Orders"] = tableStats
	collector.columnStats["Orders.CustomerID"] = columnStats
	collector.indexStats["Orders.PRIMARY"] = indexStats

	if got, err := collector.CollectTableStatistics(context.Background(), metadata.NewTable("orders")); err != nil || got != tableStats {
		t.Fatalf("CollectTableStatistics() = %#v, %v; want cached mixed-case entry", got, err)
	}
	if got, exists := collector.GetTableStatistics("orders"); !exists || got != tableStats {
		t.Fatalf("GetTableStatistics() = %#v, %v; want cached mixed-case entry", got, exists)
	}
	if got, exists := collector.GetColumnStatistics("ORDERS", "customerid"); !exists || got != columnStats {
		t.Fatalf("GetColumnStatistics() = %#v, %v; want cached mixed-case entry", got, exists)
	}
	if got, exists := collector.GetIndexStatistics("orders", "primary"); !exists || got != indexStats {
		t.Fatalf("GetIndexStatistics() = %#v, %v; want cached mixed-case entry", got, exists)
	}

	uncachedIndex, err := collector.CollectIndexStatistics(context.Background(), metadata.NewTable("orders"), &metadata.Index{
		Name:     "secondary",
		IsUnique: true,
	})
	if err != nil || uncachedIndex.Cardinality != tableStats.RowCount {
		t.Fatalf("CollectIndexStatistics() = %#v, %v; want row count from mixed-case table cache", uncachedIndex, err)
	}

	collector.handleUpdateRequest(&StatisticsUpdateRequest{TableName: "oRdErS", UpdateType: UpdateTypeAll})
	if _, exists := collector.GetTableStatistics("orders"); exists {
		t.Fatal("UpdateTypeAll left mixed-case table statistics in cache")
	}
	if _, exists := collector.GetColumnStatistics("orders", "customerid"); exists {
		t.Fatal("UpdateTypeAll left mixed-case column statistics in cache")
	}
	if _, exists := collector.GetIndexStatistics("orders", "primary"); exists {
		t.Fatal("UpdateTypeAll left mixed-case index statistics in cache")
	}
}

func TestEnhancedStatisticsCollectorCollectUsesCaseInsensitiveCacheKeys(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{
		ExpirationTime:   time.Hour,
		EnableAutoUpdate: false,
	}, nil, nil)
	defer collector.Stop()

	now := time.Now().Unix()
	tableStats := &TableStats{TableName: "Orders", RowCount: 17, LastAnalyzeTime: now}
	columnStats := &ColumnStats{ColumnName: "CustomerID", LastUpdated: now, DistinctCount: 9}
	indexStats := &IndexStats{IndexName: "PRIMARY", LastUpdated: now, Cardinality: 17}
	collector.tableStats["Orders"] = tableStats
	collector.columnStats["Orders.CustomerID"] = columnStats
	collector.indexStats["Orders.PRIMARY"] = indexStats

	table := metadata.NewTable("orders")
	column := &metadata.Column{Name: "customerid"}
	index := &metadata.Index{Name: "primary", IsUnique: true}
	if got, err := collector.CollectTableStatistics(context.Background(), table); err != nil || got != tableStats {
		t.Fatalf("CollectTableStatistics() = %#v, %v; want cached mixed-case entry", got, err)
	}
	if got, err := collector.CollectColumnStatistics(context.Background(), table, column); err != nil || got != columnStats {
		t.Fatalf("CollectColumnStatistics() = %#v, %v; want cached mixed-case entry", got, err)
	}
	if got, err := collector.CollectIndexStatistics(context.Background(), table, index); err != nil || got != indexStats {
		t.Fatalf("CollectIndexStatistics() = %#v, %v; want cached mixed-case entry", got, err)
	}
}

func TestEnhancedStatisticsCollectorReadsPersistedAutoIncrementMetadata(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{EnableAutoUpdate: false}, nil, nil)
	defer collector.Stop()
	table := metadata.NewTable("auto_stats")
	table.Stats.AutoIncrement = 17
	if got := collector.getAutoIncrementValue(table); got != 17 {
		t.Fatalf("getAutoIncrementValue() = %d, want 17", got)
	}
}

func TestEnhancedStatisticsCollectorUsesPersistedIndexStatistics(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{EnableAutoUpdate: false}, nil, nil)
	defer collector.Stop()
	table := metadata.NewTable("persisted_index_stats")
	index := &metadata.Index{
		Name:  "idx_value",
		Table: table,
		Stats: &metadata.IndexStatistics{Cardinality: 42, LeafPages: 7, NonLeafPages: 2},
	}
	table.AddIndex(index)
	stats, err := collector.CollectIndexStatistics(context.Background(), table, index)
	if err != nil {
		t.Fatalf("CollectIndexStatistics() error = %v", err)
	}
	if stats.Cardinality != 42 || stats.LeafPages != 7 || stats.NonLeafPages != 2 {
		t.Fatalf("persisted index stats = %#v, want cardinality/pages from metadata", stats)
	}
}

func TestEnhancedStatisticsCollectorUsesLiveIndexStatistics(t *testing.T) {
	accessor := &statisticsRecordAccessor{
		indexID:          77,
		indexCardinality: 321,
		treeDepth:        3,
		leafPages:        12,
		nonLeafPages:     2,
	}
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{EnableAutoUpdate: false}, nil, nil)
	collector.SetStorageEngineAccessor(accessor)
	defer collector.Stop()

	table := metadata.NewTable("live_index_stats")
	table.Schema = metadata.NewSchema("analytics")
	table.AddColumn(&metadata.Column{Name: "value", DataType: metadata.TypeInt})
	index := &metadata.Index{Name: "idx_value", Columns: []string{"value"}}
	if err := table.AddIndex(index); err != nil {
		t.Fatalf("AddIndex() error = %v", err)
	}
	collector.tableStats[table.Name] = &TableStats{TableName: table.Name, RowCount: 1000}

	stats, err := collector.CollectIndexStatistics(context.Background(), table, index)
	if err != nil {
		t.Fatalf("CollectIndexStatistics() error = %v", err)
	}
	if stats.Cardinality != 321 || stats.TreeDepth != 3 || stats.LeafPages != 12 || stats.NonLeafPages != 2 {
		t.Fatalf("live index stats = %#v, want cardinality/tree/pages from accessor", stats)
	}
}

func TestEnhancedStatisticsCollectorUsesDecodedRowsForIndexCardinality(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{
		SampleRate:       1,
		EnableAutoUpdate: false,
	}, nil, nil)
	defer collector.Stop()
	collector.SetStorageEngineAccessor(&statisticsRecordAccessor{records: [][]interface{}{
		{1, "alice", "active"},
		{2, "alice", "inactive"},
		{3, "bob", "active"},
		{4, "bob", "active"},
	}})

	table := metadata.NewTable("decoded_index_stats")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar})
	table.AddColumn(&metadata.Column{Name: "state", DataType: metadata.TypeVarchar})
	index := &metadata.Index{Name: "idx_name_state", Columns: []string{"name", "state"}}
	table.AddIndex(index)

	cardinality, ok := collector.sampleIndexCardinality(context.Background(), table, index, 4)
	if !ok || cardinality != 3 {
		t.Fatalf("sampleIndexCardinality = %d, %v; want 3, true", cardinality, ok)
	}
}

func TestEnhancedStatisticsCollectorSamplesNullableUniqueIndex(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{
		SampleRate:       1,
		EnableAutoUpdate: false,
	}, nil, nil)
	defer collector.Stop()
	collector.SetStorageEngineAccessor(&statisticsRecordAccessor{records: [][]interface{}{
		{1, "alice"},
		{2, nil},
		{3, nil},
	}})

	table := metadata.NewTable("nullable_unique_stats")
	table.AddColumn(&metadata.Column{Name: "id", OrdinalPosition: 1, DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "name", OrdinalPosition: 2, DataType: metadata.TypeVarchar, IsNullable: true})
	index := &metadata.Index{Name: "uq_name", Columns: []string{"name"}, IsUnique: true}
	table.AddIndex(index)

	stats, err := collector.CollectIndexStatistics(context.Background(), table, index)
	if err != nil {
		t.Fatalf("CollectIndexStatistics() error = %v", err)
	}
	if stats.Cardinality != 2 {
		t.Fatalf("nullable unique cardinality = %d, want 2 distinct sampled keys", stats.Cardinality)
	}
}

func TestEnhancedStatisticsCollectorSamplesDecodedRecordsInsteadOfSyntheticValues(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{
		SampleRate:         1,
		HistogramBuckets:   4,
		EnableAutoUpdate:   false,
		AutoUpdateInterval: time.Hour,
	}, nil, nil)
	defer collector.Stop()
	accessor := &statisticsRecordAccessor{records: [][]interface{}{
		{101, "alice"},
		{102, "bob"},
		{103, "alice"},
	}}
	collector.SetStorageEngineAccessor(accessor)
	values := collector.sampleColumnData(&statsTestSpace{pages: map[uint32][]byte{}}, &metadata.Column{
		Name:            "name",
		OrdinalPosition: 2,
		DataType:        metadata.TypeVarchar,
	}, 3)
	if fmt.Sprint(values) != fmt.Sprint([]interface{}{"alice", "bob", "alice"}) {
		t.Fatalf("sampled values = %v, want decoded records", values)
	}
	if accessor.rate != 1 {
		t.Fatalf("SampleTableRecords rate = %v, want 1", accessor.rate)
	}
}

func TestEnhancedStatisticsCollectorExactModeDoesNotTruncateDecodedRows(t *testing.T) {
	records := make([][]interface{}, 50001)
	for i := range records {
		records[i] = []interface{}{i, fmt.Sprintf("value-%d", i)}
	}
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{
		SampleRate:       1,
		HistogramBuckets: 4,
		EnableAutoUpdate: false,
	}, nil, nil)
	defer collector.Stop()
	collector.SetStorageEngineAccessor(&statisticsRecordAccessor{records: records})

	values := collector.sampleColumnData(&statsTestSpace{pages: map[uint32][]byte{}}, &metadata.Column{
		Name:            "value",
		OrdinalPosition: 2,
		DataType:        metadata.TypeVarchar,
	}, 100000)
	if len(values) != len(records) {
		t.Fatalf("exact decoded sample length = %d, want %d", len(values), len(records))
	}
}

func TestEnhancedStatisticsCollectorUsesAccessorWithoutSpaceManager(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{
		SampleRate:       1,
		HistogramBuckets: 4,
		EnableAutoUpdate: false,
	}, nil, nil)
	defer collector.Stop()
	collector.SetStorageEngineAccessor(&statisticsRecordAccessor{
		spaceID: 808,
		records: [][]interface{}{{1, "alice"}, {2, "alice"}, {3, "bob"}},
	})
	table := metadata.NewTable("accessor_only_stats")
	table.Schema = metadata.NewSchema("test_db")
	table.AddColumn(&metadata.Column{Name: "id", OrdinalPosition: 1, DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "name", OrdinalPosition: 2, DataType: metadata.TypeVarchar})

	stats, err := collector.CollectColumnStatistics(context.Background(), table, table.Columns[1])
	if err != nil {
		t.Fatalf("CollectColumnStatistics() error = %v", err)
	}
	if stats.DistinctCount != 2 || stats.NullCount != 0 || stats.NotNullCount != 3 {
		t.Fatalf("accessor-only stats = %#v, want distinct=2 null=0 notnull=3", stats)
	}
}

func TestEnhancedStatisticsCollectorUsesStorageAccessorSpaceSizes(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{EnableAutoUpdate: false}, nil, nil)
	defer collector.Stop()
	collector.SetStorageEngineAccessor(&statisticsRecordAccessor{dataSize: 1234, indexSize: 567})

	dataSize, indexSize := collector.getSpaceSize(&statsTestSpace{pages: map[uint32][]byte{}})
	if dataSize != 1234 || indexSize != 567 {
		t.Fatalf("storage sizes = %d/%d, want 1234/567", dataSize, indexSize)
	}
}

func TestEnhancedStatisticsCollectorUsesStorageAccessorRowCount(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{EnableAutoUpdate: false}, nil, nil)
	defer collector.Stop()
	collector.SetStorageEngineAccessor(&statisticsRecordAccessor{records: [][]interface{}{{1}, {2}, {3}}})

	got := collector.getRealRowCount(&statsTestSpace{pages: map[uint32][]byte{}})
	if got != 3 {
		t.Fatalf("storage row count = %d, want 3", got)
	}
}

func TestEnhancedStatisticsCollectorUsesAccessorWhenSpaceHandleIsUnavailable(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{EnableAutoUpdate: false}, &unavailableStatsSpaceManager{}, nil)
	defer collector.Stop()
	collector.SetStorageEngineAccessor(&statisticsRecordAccessor{
		spaceID:   808,
		records:   [][]interface{}{{1}, {2}, {3}, {4}},
		dataSize:  1234,
		indexSize: 567,
	})
	table := metadata.NewTable("accessor_fallback_stats")
	table.Schema = metadata.NewSchema("test_db")

	stats, err := collector.CollectTableStatistics(context.Background(), table)
	if err != nil {
		t.Fatalf("CollectTableStatistics() error = %v", err)
	}
	if stats.RowCount != 4 || stats.DataLength != 1234 || stats.IndexLength != 567 || stats.TotalSize != 1801 {
		t.Fatalf("accessor fallback stats = %#v, want row count and sizes from accessor", stats)
	}
}

func TestEnhancedStatisticsHistogramUsesSampleBucketFacts(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{
		HistogramBuckets: 2,
		EnableAutoUpdate: false,
	}, nil, nil)
	defer collector.Stop()

	histogram := collector.buildEnhancedHistogram(
		[]interface{}{int64(1), int64(2), int64(3), int64(10)},
		&metadata.Column{Name: "score", DataType: metadata.TypeBigInt},
		HistogramEquiWidth,
		8,
	)
	if histogram == nil || len(histogram.Buckets) != 2 {
		t.Fatalf("histogram = %#v, want two buckets", histogram)
	}
	if histogram.Buckets[0].Distinct != 3 {
		t.Fatalf("first bucket distinct = %d, want exact sample distinct 3", histogram.Buckets[0].Distinct)
	}
	var total int64
	for _, bucket := range histogram.Buckets {
		total += bucket.Count
	}
	if total != 8 {
		t.Fatalf("scaled bucket count = %d, want 8 without boundary double-counting", total)
	}
}

func TestEnhancedStatisticsHistogramScalesNonNullRowsOnly(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{
		HistogramBuckets:   2,
		EnableAutoUpdate:   false,
	}, nil, nil)
	defer collector.Stop()

	histogram := collector.buildEnhancedHistogram(
		[]interface{}{nil, int64(1), int64(2), int64(3), int64(10)},
		&metadata.Column{Name: "score", DataType: metadata.TypeBigInt},
		HistogramEquiWidth,
		8,
	)
	if histogram == nil || len(histogram.Buckets) != 2 {
		t.Fatalf("histogram = %#v, want two buckets", histogram)
	}
	var bucketTotal int64
	for _, bucket := range histogram.Buckets {
		bucketTotal += bucket.Count
	}
	if histogram.TotalCount != 8 {
		t.Fatalf("histogram total count = %d, want 8 including NULL rows", histogram.TotalCount)
	}
	if bucketTotal != 6 {
		t.Fatalf("non-NULL bucket total = %d, want 6 estimated non-NULL rows", bucketTotal)
	}
}

func TestEnhancedStatisticsHistogramHandlesEmptyAndZeroBucketConfiguration(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{
		HistogramBuckets: 0,
		EnableAutoUpdate: false,
	}, nil, nil)
	defer collector.Stop()

	histogram := collector.buildEnhancedHistogram(
		[]interface{}{int64(7)},
		&metadata.Column{Name: "score", DataType: metadata.TypeBigInt},
		HistogramEquiWidth,
		1,
	)
	if histogram == nil || len(histogram.Buckets) != 1 || histogram.Buckets[0].Count != 1 {
		t.Fatalf("zero-bucket histogram = %#v, want one exact bucket", histogram)
	}
}

func TestEnhancedStatisticsCollectorUsesCaseInsensitiveCacheKeys(t *testing.T) {
	collector := NewEnhancedStatisticsCollector(&StatisticsConfig{EnableAutoUpdate: false}, nil, nil)
	defer collector.Stop()
	collector.tableStats["Orders"] = &TableStats{TableName: "Orders", RowCount: 9}
	collector.columnStats["Orders.CustomerID"] = &ColumnStats{ColumnName: "CustomerID", DistinctCount: 3}
	collector.indexStats["Orders.PRIMARY"] = &IndexStats{IndexName: "PRIMARY", Cardinality: 9}

	if stats, ok := collector.GetTableStatistics("orders"); !ok || stats.RowCount != 9 {
		t.Fatalf("case-insensitive table statistics = %#v, %v", stats, ok)
	}
	if stats, ok := collector.GetColumnStatistics("orders", "customerid"); !ok || stats.DistinctCount != 3 {
		t.Fatalf("case-insensitive column statistics = %#v, %v", stats, ok)
	}
	if stats, ok := collector.GetIndexStatistics("orders", "primary"); !ok || stats.Cardinality != 9 {
		t.Fatalf("case-insensitive index statistics = %#v, %v", stats, ok)
	}

	collector.Invalidate("orders")
	if _, ok := collector.GetTableStatistics("ORDERS"); ok {
		t.Fatal("case-insensitive Invalidate() left table statistics in cache")
	}
	if _, ok := collector.GetColumnStatistics("ORDERS", "CUSTOMERID"); ok {
		t.Fatal("case-insensitive Invalidate() left column statistics in cache")
	}
	if _, ok := collector.GetIndexStatistics("ORDERS", "PRIMARY"); ok {
		t.Fatal("case-insensitive Invalidate() left index statistics in cache")
	}
}
