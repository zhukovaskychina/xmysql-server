package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// MockIndexSyncer 用于测试的Mock索引同步器
// 实现 SecondaryIndexSyncer 接口
type MockIndexSyncer struct {
	insertCalls []IndexSyncCall
	updateCalls []IndexSyncCall
	deleteCalls []IndexSyncCall
}

type IndexSyncCall struct {
	TableID uint64
	RowData map[string]interface{}
	OldData map[string]interface{}
	NewData map[string]interface{}
	PKValue []byte
}

func NewMockIndexSyncer() *MockIndexSyncer {
	return &MockIndexSyncer{
		insertCalls: make([]IndexSyncCall, 0),
		updateCalls: make([]IndexSyncCall, 0),
		deleteCalls: make([]IndexSyncCall, 0),
	}
}

func (m *MockIndexSyncer) SyncSecondaryIndexesOnInsert(tableID uint64, rowData map[string]interface{}, pkValue []byte) error {
	m.insertCalls = append(m.insertCalls, IndexSyncCall{
		TableID: tableID,
		RowData: rowData,
		PKValue: pkValue,
	})
	return nil
}

func (m *MockIndexSyncer) SyncSecondaryIndexesOnUpdate(tableID uint64, oldData, newData map[string]interface{}, pkValue []byte) error {
	m.updateCalls = append(m.updateCalls, IndexSyncCall{
		TableID: tableID,
		OldData: oldData,
		NewData: newData,
		PKValue: pkValue,
	})
	return nil
}

func (m *MockIndexSyncer) SyncSecondaryIndexesOnDelete(tableID uint64, rowData map[string]interface{}) error {
	m.deleteCalls = append(m.deleteCalls, IndexSyncCall{
		TableID: tableID,
		RowData: rowData,
	})
	return nil
}

// MockBTreeManager 用于测试的Mock B+树管理器
type MockBTreeManager struct {
	insertedData map[interface{}][]byte
}

func NewMockBTreeManager() *MockBTreeManager {
	return &MockBTreeManager{
		insertedData: make(map[interface{}][]byte),
	}
}

func (m *MockBTreeManager) Init(ctx context.Context, spaceId uint32, rootPage uint32) error {
	return nil
}

func (m *MockBTreeManager) GetAllLeafPages(ctx context.Context) ([]uint32, error) {
	return nil, nil
}

func (m *MockBTreeManager) Search(ctx context.Context, key interface{}) (uint32, int, error) {
	return 0, 0, nil
}

func (m *MockBTreeManager) Insert(ctx context.Context, key interface{}, value []byte) error {
	m.insertedData[key] = value
	return nil
}

func (m *MockBTreeManager) RangeSearch(ctx context.Context, startKey, endKey interface{}) ([]basic.Row, error) {
	return nil, nil
}

func (m *MockBTreeManager) GetFirstLeafPage(ctx context.Context) (uint32, error) {
	return 0, nil
}

// TestDMLExecutor_InsertWithSecondaryIndexSync 测试INSERT时的二级索引同步
func TestDMLExecutor_InsertWithSecondaryIndexSync(t *testing.T) {
	mockIndexSyncer := NewMockIndexSyncer()
	mockBTreeManager := NewMockBTreeManager()

	dmlExecutor := NewDMLExecutor(
		nil, // optimizerManager
		nil, // bufferPoolManager
		mockBTreeManager,
		nil, // tableManager
		nil, // txManager
		mockIndexSyncer,
	)

	// 准备测试数据
	tableMeta := &metadata.TableMeta{
		Name:       "users",
		Columns:    []*metadata.ColumnMeta{},
		PrimaryKey: []string{"id"},
	}

	rowData := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"id":   int64(1),
			"name": "Alice",
			"age":  25,
		},
	}

	// 执行插入
	ctx := context.Background()
	insertID, err := dmlExecutor.insertRow(ctx, nil, rowData, tableMeta)

	// 验证结果
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), insertID)

	// 验证主键索引已插入
	assert.Equal(t, 1, len(mockBTreeManager.insertedData))

	// 验证二级索引同步被调用
	assert.Equal(t, 1, len(mockIndexSyncer.insertCalls))

	call := mockIndexSyncer.insertCalls[0]
	assert.Greater(t, call.TableID, uint64(0), "TableID should be non-zero")
	assert.Equal(t, "Alice", call.RowData["name"])
	assert.Equal(t, 25, call.RowData["age"])

	t.Log("✅ INSERT二级索引同步测试通过")
}

// TestDMLExecutor_UpdateWithSecondaryIndexSync 测试UPDATE时的二级索引同步
func TestDMLExecutor_UpdateWithSecondaryIndexSync(t *testing.T) {
	mockIndexSyncer := NewMockIndexSyncer()
	mockBTreeManager := NewMockBTreeManager()

	dmlExecutor := NewDMLExecutor(
		nil,
		nil,
		mockBTreeManager,
		nil,
		nil,
		mockIndexSyncer,
	)

	tableMeta := &metadata.TableMeta{
		Name:       "users",
		Columns:    []*metadata.ColumnMeta{},
		PrimaryKey: []string{"id"},
	}

	rowInfo := &RowUpdateInfo{
		RowId: 1,
		OldValues: map[string]interface{}{
			"id":   1,
			"name": "Alice",
			"age":  25,
		},
	}

	updateExprs := []*UpdateExpression{
		{
			ColumnName: "name",
			NewValue:   "Bob",
			ColumnType: metadata.TypeVarchar,
		},
	}

	// 执行更新
	ctx := context.Background()
	err := dmlExecutor.updateRow(ctx, nil, rowInfo, updateExprs, tableMeta)

	// 验证结果
	assert.NoError(t, err)

	// 验证二级索引同步被调用
	assert.Equal(t, 1, len(mockIndexSyncer.updateCalls))

	call := mockIndexSyncer.updateCalls[0]
	assert.Greater(t, call.TableID, uint64(0))
	assert.Equal(t, "Alice", call.OldData["name"])
	assert.Equal(t, "Bob", call.NewData["name"])

	t.Log("✅ UPDATE二级索引同步测试通过")
}

// TestDMLExecutor_DeleteWithSecondaryIndexSync 测试DELETE时的二级索引同步
func TestDMLExecutor_DeleteWithSecondaryIndexSync(t *testing.T) {
	mockIndexSyncer := NewMockIndexSyncer()
	mockBTreeManager := NewMockBTreeManager()

	dmlExecutor := NewDMLExecutor(
		nil,
		nil,
		mockBTreeManager,
		nil,
		nil,
		mockIndexSyncer,
	)

	tableMeta := &metadata.TableMeta{
		Name:       "users",
		Columns:    []*metadata.ColumnMeta{},
		PrimaryKey: []string{"id"},
	}

	rowInfo := &RowUpdateInfo{
		RowId: 1,
		OldValues: map[string]interface{}{
			"id":   1,
			"name": "Alice",
			"age":  25,
		},
	}

	// 执行删除
	ctx := context.Background()
	err := dmlExecutor.deleteRow(ctx, nil, rowInfo, tableMeta)

	// 验证结果
	assert.NoError(t, err)

	// 验证二级索引同步被调用
	assert.Equal(t, 1, len(mockIndexSyncer.deleteCalls))

	call := mockIndexSyncer.deleteCalls[0]
	assert.Greater(t, call.TableID, uint64(0))
	assert.Equal(t, "Alice", call.RowData["name"])
	assert.Equal(t, 25, call.RowData["age"])

	t.Log("✅ DELETE二级索引同步测试通过")
}
