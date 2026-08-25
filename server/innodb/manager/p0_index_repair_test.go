package manager

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

type indexRepairFixture struct {
	ctx          context.Context
	storage      *StorageManager
	tableStorage *TableStorageManager
	tableMeta    *metadata.TableMeta
	tableInfo    *TableStorageInfo
	indexManager *IndexManager
}

func newIndexRepairFixture(t *testing.T) *indexRepairFixture {
	t.Helper()

	dataDir := t.TempDir()
	storage := NewStorageManager(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        filepath.Join(dataDir, "innodb"),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       PAGE_SIZE,
	})
	t.Cleanup(func() {
		_ = storage.Close()
	})

	ctx := context.Background()
	tableStorage := NewTableStorageManager(storage)
	handle, err := storage.CreateTablespace("app/users")
	if err != nil {
		t.Fatalf("CreateTablespace() error = %v", err)
	}
	tableInfo := &TableStorageInfo{
		SchemaName:    "app",
		TableName:     "users",
		SpaceID:       handle.SpaceID,
		RootPageNo:    3,
		IndexPageNo:   3,
		DataSegmentID: handle.DataSegmentID,
		Type:          TableTypeUser,
	}
	if err := tableStorage.RegisterTable(ctx, tableInfo); err != nil {
		t.Fatalf("RegisterTable() error = %v", err)
	}

	tableMeta := &metadata.TableMeta{
		Name: "users",
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: metadata.TypeInt, IsPrimary: true},
			{Name: "email", Type: metadata.TypeVarchar},
		},
		PrimaryKey: []string{"id"},
		Indices: []metadata.IndexMeta{
			{Name: "idx_email", Columns: []string{"email"}},
		},
	}
	tableManager := &TableManager{
		tableMetaCache:  map[string]*metadata.TableMeta{"app.users": tableMeta},
		tableStatsCache: make(map[string]*metadata.InfoTableStats),
		tableIndexCache: make(map[string][]*Index),
	}
	storage.SetTableStorageManager(tableStorage)
	storage.SetTableManager(tableManager)

	indexManager := NewIndexManagerWithStorage(storage.GetSegmentManager(), storage.GetBufferPoolManager(), storage, nil)
	storage.SetIndexManager(indexManager)

	return &indexRepairFixture{
		ctx:          ctx,
		storage:      storage,
		tableStorage: tableStorage,
		tableMeta:    tableMeta,
		tableInfo:    tableInfo,
		indexManager: indexManager,
	}
}

func (f *indexRepairFixture) createUsersEmailIndex(t *testing.T) uint64 {
	t.Helper()
	if err := f.indexManager.EnsureSecondaryIndexes(f.tableInfo, f.tableMeta); err != nil {
		t.Fatalf("EnsureSecondaryIndexes() error = %v", err)
	}
	index := f.indexManager.GetIndexByName(SecondaryIndexTableID("app", "users"), "idx_email")
	if index == nil {
		t.Fatalf("idx_email was not registered")
	}
	return index.IndexID
}

func (f *indexRepairFixture) insertClusteredUser(t *testing.T, id int64, email string) {
	t.Helper()
	row := map[string]interface{}{"id": id, "email": email}
	btree, err := f.tableStorage.CreateBTreeManagerForTable(f.ctx, "app", "users")
	if err != nil {
		t.Fatalf("CreateBTreeManagerForTable() error = %v", err)
	}
	encoded, err := encodeP0ClusteredRecord(row, f.tableMeta)
	if err != nil {
		t.Fatalf("encode clustered record: %v", err)
	}
	if err := btree.Insert(f.ctx, id, encoded); err != nil {
		t.Fatalf("insert clustered row: %v", err)
	}
	if err := f.indexManager.SyncSecondaryIndexesOnInsert(SecondaryIndexTableID("app", "users"), row, buildP0PrimaryKey(t, row, f.tableMeta)); err != nil {
		t.Fatalf("SyncSecondaryIndexesOnInsert() error = %v", err)
	}
}

func (f *indexRepairFixture) removeSecondaryEntry(t *testing.T, indexID uint64, email string, id int64) {
	t.Helper()
	if err := f.indexManager.DeleteKey(indexID, f.secondaryKey(t, email, id)); err != nil {
		t.Fatalf("DeleteKey() error = %v", err)
	}
}

func (f *indexRepairFixture) insertStaleSecondaryEntry(t *testing.T, indexID uint64, email string, id int64) {
	t.Helper()
	if err := f.indexManager.InsertKey(indexID, f.secondaryKey(t, email, id), EncodeSecondaryIndexValue(buildP0PrimaryKey(t, map[string]interface{}{"id": id}, f.tableMeta))); err != nil {
		t.Fatalf("InsertKey(stale) error = %v", err)
	}
}

func (f *indexRepairFixture) secondaryKey(t *testing.T, email string, id int64) []byte {
	t.Helper()
	key, err := EncodeSecondaryIndexKey(
		SecondaryIndexTableID("app", "users"),
		metadata.IndexMeta{Name: "idx_email", Columns: []string{"email"}},
		map[string]interface{}{"email": email},
		buildP0PrimaryKey(t, map[string]interface{}{"id": id}, f.tableMeta),
	)
	if err != nil {
		t.Fatalf("EncodeSecondaryIndexKey() error = %v", err)
	}
	return key
}

func TestP0ValidateIndexDetectsMissingSecondaryEntry(t *testing.T) {
	fixture := newIndexRepairFixture(t)
	indexID := fixture.createUsersEmailIndex(t)
	fixture.insertClusteredUser(t, 1, "a@example.com")
	fixture.removeSecondaryEntry(t, indexID, "a@example.com", 1)

	if err := fixture.indexManager.ValidateIndex(indexID); err == nil {
		t.Fatalf("ValidateIndex accepted index with missing secondary entry")
	}
}

func TestP0RepairIndexRebuildsMissingSecondaryEntry(t *testing.T) {
	fixture := newIndexRepairFixture(t)
	indexID := fixture.createUsersEmailIndex(t)
	fixture.insertClusteredUser(t, 1, "a@example.com")
	fixture.removeSecondaryEntry(t, indexID, "a@example.com", 1)

	if err := fixture.indexManager.RepairIndex(indexID); err != nil {
		t.Fatalf("RepairIndex failed: %v", err)
	}
	if err := fixture.indexManager.ValidateIndex(indexID); err != nil {
		t.Fatalf("ValidateIndex after repair failed: %v", err)
	}
	if _, _, err := fixture.indexManager.SearchKey(indexID, fixture.secondaryKey(t, "a@example.com", 1)); err != nil {
		t.Fatalf("SearchKey after repair failed: %v", err)
	}
}

func TestP0RepairIndexDropsStaleSecondaryEntryAndFixesCount(t *testing.T) {
	fixture := newIndexRepairFixture(t)
	indexID := fixture.createUsersEmailIndex(t)
	fixture.insertClusteredUser(t, 1, "a@example.com")
	fixture.insertStaleSecondaryEntry(t, indexID, "stale@example.com", 99)
	fixture.indexManager.indexes[indexID].KeyCount = 99

	if err := fixture.indexManager.ValidateIndex(indexID); err == nil {
		t.Fatalf("ValidateIndex accepted stale secondary entry and bad key count")
	}
	if err := fixture.indexManager.RepairIndex(indexID); err != nil {
		t.Fatalf("RepairIndex failed: %v", err)
	}
	if err := fixture.indexManager.ValidateIndex(indexID); err != nil {
		t.Fatalf("ValidateIndex after repair failed: %v", err)
	}
	if _, _, err := fixture.indexManager.SearchKey(indexID, fixture.secondaryKey(t, "stale@example.com", 99)); err == nil {
		t.Fatalf("SearchKey found stale secondary entry after repair")
	}
	if got := fixture.indexManager.indexes[indexID].KeyCount; got != 1 {
		t.Fatalf("KeyCount after repair = %d, want 1", got)
	}
}

func TestP0ValidateIndexRejectsInvalidSecondaryValue(t *testing.T) {
	fixture := newIndexRepairFixture(t)
	indexID := fixture.createUsersEmailIndex(t)
	fixture.insertClusteredUser(t, 1, "a@example.com")

	key := fixture.secondaryKey(t, "a@example.com", 1)
	if err := fixture.indexManager.UpdateKey(indexID, key, key, []byte("bad-secondary-value")); err != nil {
		t.Fatalf("inject invalid secondary value: %v", err)
	}
	if err := fixture.indexManager.ValidateIndex(indexID); err == nil {
		t.Fatalf("ValidateIndex accepted invalid secondary value")
	}
	if err := fixture.indexManager.RepairIndex(indexID); err != nil {
		t.Fatalf("RepairIndex failed: %v", err)
	}
	if err := fixture.indexManager.ValidateIndex(indexID); err != nil {
		t.Fatalf("ValidateIndex after repair failed: %v", err)
	}
}

func buildP0PrimaryKey(t *testing.T, row map[string]interface{}, tableMeta *metadata.TableMeta) []byte {
	t.Helper()
	key, err := buildSecondaryPrimaryKey(row, tableMeta)
	if err != nil {
		t.Fatalf("build primary key: %v", err)
	}
	return key
}

func encodeP0ClusteredRecord(row map[string]interface{}, tableMeta *metadata.TableMeta) ([]byte, error) {
	buffer := make([]byte, 0)
	buffer = append(buffer, []byte("XIR1")...)
	buffer = append(buffer, byte(1))
	buffer = binary.BigEndian.AppendUint16(buffer, uint16(len(tableMeta.Columns)))
	for _, col := range tableMeta.Columns {
		valueType, valueBytes, err := encodeP0ClusteredValue(row[col.Name], col.Type)
		if err != nil {
			return nil, fmt.Errorf("encode column %s: %v", col.Name, err)
		}
		buffer = append(buffer, valueType)
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(valueBytes)))
		buffer = append(buffer, valueBytes...)
	}
	return buffer, nil
}

func encodeP0ClusteredValue(value interface{}, dataType metadata.DataType) (byte, []byte, error) {
	if value == nil {
		return 0, nil, nil
	}
	switch dataType {
	case metadata.TypeInt, metadata.TypeBigInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeTinyInt, metadata.TypeYear:
		var intValue int64
		switch v := value.(type) {
		case int:
			intValue = int64(v)
		case int64:
			intValue = v
		default:
			return 0, nil, fmt.Errorf("unsupported integer value %T", value)
		}
		return 1, binary.BigEndian.AppendUint64(nil, uint64(intValue)), nil
	default:
		return 5, []byte(fmt.Sprintf("%v", value)), nil
	}
}
