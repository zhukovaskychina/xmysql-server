package engine

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TestIndexScanOperator_CoveringIndex 测试覆盖索引（不需要回表）
func TestIndexScanOperator_CoveringIndex(t *testing.T) {
	// 创建测试表
	table := &metadata.Table{
		Name: "users",
		Columns: []*metadata.Column{
			{Name: "id", DataType: metadata.TypeInt},
			{Name: "name", DataType: metadata.TypeVarchar},
			{Name: "email", DataType: metadata.TypeVarchar},
			{Name: "age", DataType: metadata.TypeInt},
		},
		Indices: []*metadata.Index{
			{
				Name:      "PRIMARY",
				Columns:   []string{"id"},
				IsPrimary: true,
				IsUnique:  true,
			},
			{
				Name:      "idx_name_email",
				Columns:   []string{"name", "email"},
				IsPrimary: false,
				IsUnique:  false,
			},
		},
	}

	// 创建模拟的表管理器
	mockTableManager := &mockTableManagerForTest{table: table}

	// 创建IndexScanOperator，只查询name和email列
	op := NewIndexScanOperator(
		"testdb",
		"users",
		"idx_name_email",
		nil, // indexManager
		mockTableManager,
		nil,                       // bufferPoolManager
		nil,                       // startKey
		nil,                       // endKey
		[]string{"name", "email"}, // 只需要索引列
	)

	// 打开算子
	ctx := context.Background()
	if err := op.Open(ctx); err != nil {
		t.Fatalf("Failed to open operator: %v", err)
	}

	// 验证是否识别为覆盖索引
	if !op.isCoveringIndex {
		t.Error("Expected covering index, but got false")
	}

	t.Logf("Successfully identified covering index for query requiring only %v", op.requiredColumns)
}

// TestIndexScanOperator_NonCoveringIndex 测试非覆盖索引（需要回表）
func TestIndexScanOperator_NonCoveringIndex(t *testing.T) {
	// 创建测试表
	table := &metadata.Table{
		Name: "users",
		Columns: []*metadata.Column{
			{Name: "id", DataType: metadata.TypeInt},
			{Name: "name", DataType: metadata.TypeVarchar},
			{Name: "email", DataType: metadata.TypeVarchar},
			{Name: "age", DataType: metadata.TypeInt},
		},
		Indices: []*metadata.Index{
			{
				Name:      "PRIMARY",
				Columns:   []string{"id"},
				IsPrimary: true,
				IsUnique:  true,
			},
			{
				Name:      "idx_name",
				Columns:   []string{"name"},
				IsPrimary: false,
				IsUnique:  false,
			},
		},
	}

	// 创建模拟的表管理器
	mockTableManager := &mockTableManagerForTest{table: table}

	// 创建IndexScanOperator，查询name和age列
	op := NewIndexScanOperator(
		"testdb",
		"users",
		"idx_name",
		nil, // indexManager
		mockTableManager,
		nil,                              // bufferPoolManager
		nil,                              // startKey
		nil,                              // endKey
		[]string{"name", "email", "age"}, // 需要索引外的列
	)

	// 打开算子
	ctx := context.Background()
	if err := op.Open(ctx); err != nil {
		t.Fatalf("Failed to open operator: %v", err)
	}

	// 验证是否识别为非覆盖索引
	if op.isCoveringIndex {
		t.Error("Expected non-covering index, but got true")
	}

	t.Logf("Successfully identified non-covering index for query requiring %v", op.requiredColumns)
}

// TestIndexScanOperator_PrimaryIndex 测试主键索引（总是覆盖索引）
func TestIndexScanOperator_PrimaryIndex(t *testing.T) {
	// 创建测试表
	table := &metadata.Table{
		Name: "users",
		Columns: []*metadata.Column{
			{Name: "id", DataType: metadata.TypeInt},
			{Name: "name", DataType: metadata.TypeVarchar},
			{Name: "age", DataType: metadata.TypeInt},
		},
		Indices: []*metadata.Index{
			{
				Name:      "PRIMARY",
				Columns:   []string{"id"},
				IsPrimary: true,
				IsUnique:  true,
			},
		},
	}

	// 创建模拟的表管理器
	mockTableManager := &mockTableManagerForTest{table: table}

	// 创建IndexScanOperator，使用主键索引
	op := NewIndexScanOperator(
		"testdb",
		"users",
		"PRIMARY",
		nil, // indexManager
		mockTableManager,
		nil,                           // bufferPoolManager
		nil,                           // startKey
		nil,                           // endKey
		[]string{"id", "name", "age"}, // 查询所有列
	)

	// 打开算子
	ctx := context.Background()
	if err := op.Open(ctx); err != nil {
		t.Fatalf("Failed to open operator: %v", err)
	}

	// 验证主键索引总是覆盖索引
	if !op.isCoveringIndex {
		t.Error("Expected primary index to be covering index, but got false")
	}

	t.Logf("Primary index is always covering index")
}

// TestExtractPrimaryKey 测试从二级索引记录提取主键
func TestExtractPrimaryKey(t *testing.T) {
	// 创建模拟的二级索引记录
	mockRow := &mockSecondaryIndexRow{
		indexKeys:   []basic.Value{basic.NewString("Alice")},
		primaryKeys: []basic.Value{basic.NewInt64(1)},
	}

	op := &IndexScanOperator{}

	// 提取主键
	primaryKey, err := op.extractPrimaryKey(mockRow)
	if err != nil {
		t.Fatalf("Failed to extract primary key: %v", err)
	}

	if primaryKey == nil {
		t.Fatal("Expected primary key, got nil")
	}

	// 验证主键值
	if primaryKey.ToInt64() != 1 {
		t.Errorf("Expected primary key 1, got %d", primaryKey.ToInt64())
	}

	t.Logf("Successfully extracted primary key: %v", primaryKey)
}

// mockTableManagerForTest 模拟的表管理器
type mockTableManagerForTest struct {
	table *metadata.Table
}

func (m *mockTableManagerForTest) GetTable(ctx context.Context, schemaName, tableName string) (*metadata.Table, error) {
	return m.table, nil
}

func (m *mockTableManagerForTest) GetTableMetadata(ctx context.Context, schemaName, tableName string) (*metadata.TableMeta, error) {
	return nil, nil
}

func (m *mockTableManagerForTest) GetTableStats(ctx context.Context, schemaName, tableName string) (*metadata.InfoTableStats, error) {
	return nil, nil
}

func (m *mockTableManagerForTest) GetTableIndices(ctx context.Context, schemaName, tableName string) ([]*manager.Index, error) {
	return nil, nil
}

func (m *mockTableManagerForTest) GetTableBTreeManager(ctx context.Context, schemaName, tableName string) (basic.BPlusTreeManager, error) {
	return nil, nil
}

func (m *mockTableManagerForTest) GetTableStorageInfo(schemaName, tableName string) (*manager.TableStorageInfo, error) {
	return nil, nil
}

// mockSecondaryIndexRow 模拟的二级索引行记录
type mockSecondaryIndexRow struct {
	basic.Row
	indexKeys   []basic.Value
	primaryKeys []basic.Value
}

func (m *mockSecondaryIndexRow) GetPrimaryKeys() []basic.Value {
	return m.primaryKeys
}

func (m *mockSecondaryIndexRow) GetIndexKeys() []basic.Value {
	return m.indexKeys
}

func (m *mockSecondaryIndexRow) GetPrimaryKey() basic.Value {
	if len(m.primaryKeys) > 0 {
		return m.primaryKeys[0]
	}
	return nil
}

func (m *mockSecondaryIndexRow) ReadValueByIndex(index int) basic.Value {
	return nil
}

func (m *mockSecondaryIndexRow) Less(than basic.Row) bool {
	return false
}

func (m *mockSecondaryIndexRow) ToByte() []byte {
	return nil
}

func (m *mockSecondaryIndexRow) IsInfimumRow() bool {
	return false
}

func (m *mockSecondaryIndexRow) IsSupremumRow() bool {
	return false
}

func (m *mockSecondaryIndexRow) GetPageNumber() uint32 {
	return 0
}

func (m *mockSecondaryIndexRow) WriteWithNull(content []byte) {
}

func (m *mockSecondaryIndexRow) WriteBytesWithNullWithsPos(content []byte, index byte) {
}

func (m *mockSecondaryIndexRow) GetRowLength() uint16 {
	return 0
}

func (m *mockSecondaryIndexRow) GetHeaderLength() uint16 {
	return 0
}

func (m *mockSecondaryIndexRow) GetFieldLength() int {
	return 0
}

func (m *mockSecondaryIndexRow) SetNOwned(cnt byte) {
}

func (m *mockSecondaryIndexRow) GetNOwned() byte {
	return 0
}

func (m *mockSecondaryIndexRow) GetNextRowOffset() uint16 {
	return 0
}

func (m *mockSecondaryIndexRow) SetNextRowOffset(offset uint16) {
}

func (m *mockSecondaryIndexRow) GetHeapNo() uint16 {
	return 0
}

func (m *mockSecondaryIndexRow) SetHeapNo(heapNo uint16) {
}

func (m *mockSecondaryIndexRow) SetTransactionId(trxId uint64) {
}

func (m *mockSecondaryIndexRow) GetValueByColName(colName string) basic.Value {
	return nil
}

func (m *mockSecondaryIndexRow) ToString() string {
	return "mockSecondaryIndexRow"
}
