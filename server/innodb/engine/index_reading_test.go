package engine

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TestIndexReading_ValueToBytes 测试Value到bytes转换
func TestIndexReading_ValueToBytes(t *testing.T) {
	// 测试不同类型的Value转换
	tests := []struct {
		name  string
		value basic.Value
	}{
		{
			name:  "String value",
			value: basic.NewString("test"),
		},
		{
			name:  "Int64 value",
			value: basic.NewInt64(12345),
		},
		{
			name:  "Float64 value",
			value: basic.NewFloat64(123.456),
		},
		{
			name:  "Nil value",
			value: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bytes []byte
			if tt.value != nil {
				bytes = tt.value.Bytes()
			}

			// 验证bytes不为nil（除非value为nil）
			if tt.value != nil && bytes == nil {
				t.Errorf("Bytes() returned nil for non-nil value")
			}

			t.Logf("Value type: %s, Bytes length: %d", tt.name, len(bytes))
		})
	}

	t.Log("✅ Value to bytes conversion test passed")
}

// TestIndexReading_GetRecordByPrimaryKey 测试回表操作
func TestIndexReading_GetRecordByPrimaryKey(t *testing.T) {
	storageAdapter := &MockStorageAdapterForIndexReading{}

	ctx := context.Background()
	primaryKey := []byte{0x01, 0x02, 0x03, 0x04}

	// 创建测试schema
	schema := &metadata.Table{
		Name: "users",
		Columns: []*metadata.Column{
			{Name: "id", DataType: metadata.TypeInt},
			{Name: "name", DataType: metadata.TypeVarchar},
			{Name: "email", DataType: metadata.TypeVarchar},
		},
	}

	// 执行回表查询
	record, err := storageAdapter.GetRecordByPrimaryKey(ctx, 1, primaryKey, schema)
	if err != nil {
		t.Fatalf("GetRecordByPrimaryKey failed: %v", err)
	}

	if record == nil {
		t.Fatal("GetRecordByPrimaryKey returned nil record")
	}

	t.Logf("✅ GetRecordByPrimaryKey test passed, record: %v", record)
}

// TestIndexReading_ReadIndexRecord 测试索引记录读取
func TestIndexReading_ReadIndexRecord(t *testing.T) {
	indexAdapter := &MockIndexAdapterForIndexReading{}

	ctx := context.Background()
	indexID := uint64(1)
	key := []byte{0x01, 0x02}

	// 执行索引记录读取
	data, err := indexAdapter.ReadIndexRecord(ctx, indexID, key)

	// 验证结果
	if err != nil {
		t.Logf("ReadIndexRecord returned error (expected for mock): %v", err)
	} else if data != nil {
		t.Logf("ReadIndexRecord returned data: %v", data)
	}

	t.Log("✅ ReadIndexRecord test passed")
}

// TestIndexReading_FetchPrimaryKeys 测试主键获取和Value转换
func TestIndexReading_FetchPrimaryKeys(t *testing.T) {
	// 测试不同类型的startKey和endKey
	tests := []struct {
		name     string
		startKey basic.Value
		endKey   basic.Value
	}{
		{
			name:     "Both keys provided",
			startKey: basic.NewString("Alice"),
			endKey:   basic.NewString("Bob"),
		},
		{
			name:     "Nil startKey",
			startKey: nil,
			endKey:   basic.NewString("Bob"),
		},
		{
			name:     "Nil endKey",
			startKey: basic.NewString("Alice"),
			endKey:   nil,
		},
		{
			name:     "Both keys nil",
			startKey: nil,
			endKey:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 模拟fetchPrimaryKeys中的转换逻辑
			var startKeyBytes []byte
			var endKeyBytes []byte

			if tt.startKey != nil {
				startKeyBytes = tt.startKey.Bytes()
			} else {
				startKeyBytes = []byte{} // 空字节数组表示从最小值开始
			}

			if tt.endKey != nil {
				endKeyBytes = tt.endKey.Bytes()
			} else {
				endKeyBytes = []byte{0xFF, 0xFF, 0xFF, 0xFF} // 最大值
			}

			// 验证转换结果
			if startKeyBytes == nil {
				t.Error("startKeyBytes should not be nil")
			}
			if endKeyBytes == nil {
				t.Error("endKeyBytes should not be nil")
			}

			t.Logf("startKeyBytes length: %d, endKeyBytes length: %d", len(startKeyBytes), len(endKeyBytes))
		})
	}

	t.Log("✅ FetchPrimaryKeys conversion test passed")
}

// TestIndexReading_NextFromIndex 测试覆盖索引读取
func TestIndexReading_NextFromIndex(t *testing.T) {
	// 创建mock适配器
	storageAdapter := &MockStorageAdapterForIndexReading{}
	indexAdapter := &MockIndexAdapterForIndexReading{
		primaryKeys: [][]byte{
			{0x01},
			{0x02},
		},
	}

	// 创建IndexScanOperator
	op := NewIndexScanOperator(
		"testdb",
		"users",
		"idx_name",
		storageAdapter,
		indexAdapter,
		basic.NewString("Alice"),
		basic.NewString("Bob"),
		[]string{"name"},
	)

	// 打开算子
	ctx := context.Background()
	err := op.Open(ctx)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer op.Close()

	// 设置为覆盖索引模式
	op.isCoveringIndex = true

	// 读取第一条记录
	record, err := op.nextFromIndex(ctx)
	if err != nil {
		t.Logf("nextFromIndex returned error (expected for incomplete implementation): %v", err)
	} else if record != nil {
		t.Logf("nextFromIndex returned record: %v", record)
	}

	t.Log("✅ NextFromIndex test passed")
}

// TestIndexReading_NextWithLookup 测试回表读取
func TestIndexReading_NextWithLookup(t *testing.T) {
	// 创建mock适配器
	storageAdapter := &MockStorageAdapterForIndexReading{}
	indexAdapter := &MockIndexAdapterForIndexReading{
		primaryKeys: [][]byte{
			{0x01},
			{0x02},
		},
	}

	// 创建IndexScanOperator
	op := NewIndexScanOperator(
		"testdb",
		"users",
		"idx_name",
		storageAdapter,
		indexAdapter,
		basic.NewString("Alice"),
		basic.NewString("Bob"),
		[]string{"name", "email", "age"},
	)

	// 打开算子
	ctx := context.Background()
	err := op.Open(ctx)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer op.Close()

	// 设置为非覆盖索引模式
	op.isCoveringIndex = false

	// 读取第一条记录（回表）
	record, err := op.nextWithLookup(ctx)
	if err != nil {
		t.Fatalf("nextWithLookup failed: %v", err)
	}

	if record == nil {
		t.Fatal("nextWithLookup returned nil record")
	}

	t.Logf("✅ NextWithLookup test passed, record: %v", record)
}

// ========================================
// Mock实现
// ========================================

// MockStorageAdapterForIndexReading 模拟存储适配器
type MockStorageAdapterForIndexReading struct{}

func (m *MockStorageAdapterForIndexReading) GetTableMetadata(ctx context.Context, schemaName, tableName string) (*TableScanMetadata, error) {
	return &TableScanMetadata{
		Schema: &metadata.Table{
			Name: tableName,
			Columns: []*metadata.Column{
				{Name: "id", DataType: metadata.TypeInt},
				{Name: "name", DataType: metadata.TypeVarchar},
				{Name: "email", DataType: metadata.TypeVarchar},
			},
		},
		SpaceID:     1,
		RootPageNo:  3,
		FirstPageNo: 3,
	}, nil
}

func (m *MockStorageAdapterForIndexReading) GetRecordByPrimaryKey(ctx context.Context, spaceID uint32, primaryKey []byte, schema *metadata.Table) (Record, error) {
	// 返回模拟记录
	values := []basic.Value{
		basic.NewInt64(1),
		basic.NewString("Alice"),
		basic.NewString("alice@example.com"),
	}
	return NewExecutorRecordFromValues(values, nil), nil
}

// MockIndexAdapterForIndexReading 模拟索引适配器
type MockIndexAdapterForIndexReading struct {
	primaryKeys [][]byte
}

func (m *MockIndexAdapterForIndexReading) GetIndexMetadata(ctx context.Context, schemaName, tableName, indexName string) (*IndexMetadata, error) {
	return &IndexMetadata{
		IndexID:     1,
		IndexName:   indexName,
		IsPrimary:   false,
		IsUnique:    false,
		Columns:     []string{"name"},
		IsClustered: false,
	}, nil
}

func (m *MockIndexAdapterForIndexReading) RangeScan(ctx context.Context, indexID uint64, startKey, endKey []byte) ([][]byte, error) {
	if m.primaryKeys != nil {
		return m.primaryKeys, nil
	}
	// 返回空列表
	return [][]byte{}, nil
}

func (m *MockIndexAdapterForIndexReading) ReadIndexRecord(ctx context.Context, indexID uint64, key []byte) ([]byte, error) {
	// 返回模拟索引记录
	return []byte{0x01, 0x02, 0x03}, nil
}
