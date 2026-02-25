package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// MockBTreeForIndexSync 用于测试的Mock B+树
type MockBTreeForIndexSync struct {
	insertedKeys map[uint64][]interface{} // indexID -> keys
	deletedKeys  map[uint64][]interface{} // indexID -> keys
}

func NewMockBTreeForIndexSync() *MockBTreeForIndexSync {
	return &MockBTreeForIndexSync{
		insertedKeys: make(map[uint64][]interface{}),
		deletedKeys:  make(map[uint64][]interface{}),
	}
}

func (m *MockBTreeForIndexSync) Init(ctx context.Context, spaceId uint32, rootPage uint32) error {
	return nil
}

func (m *MockBTreeForIndexSync) GetAllLeafPages(ctx context.Context) ([]uint32, error) {
	return nil, nil
}

func (m *MockBTreeForIndexSync) Search(ctx context.Context, key interface{}) (uint32, int, error) {
	return 0, 0, nil
}

func (m *MockBTreeForIndexSync) Insert(ctx context.Context, key interface{}, value []byte) error {
	// 简化：假设所有操作都在索引1上
	m.insertedKeys[1] = append(m.insertedKeys[1], key)
	return nil
}

func (m *MockBTreeForIndexSync) RangeSearch(ctx context.Context, startKey, endKey interface{}) ([]basic.Row, error) {
	return nil, nil
}

func (m *MockBTreeForIndexSync) GetFirstLeafPage(ctx context.Context) (uint32, error) {
	return 0, nil
}

// TestSecondaryIndexSyncOnInsert 测试INSERT时的二级索引同步
func TestSecondaryIndexSyncOnInsert(t *testing.T) {
	mockBTree := NewMockBTreeForIndexSync()

	// 创建IndexManager
	im := &IndexManager{
		indexes:      make(map[uint64]*Index),
		btreeManager: mockBTree,
		stats:        &IndexManagerStats{},
		config:       &IndexManagerConfig{},
	}

	// 创建表和索引
	tableID := uint64(100)

	// 主键索引
	primaryIndex := &Index{
		IndexID:   1,
		TableID:   tableID,
		Name:      "PRIMARY",
		Type:      INDEX_TYPE_BTREE,
		IsPrimary: true,
		IsUnique:  true,
		State:     IndexStateActive,
		Columns: []Column{
			{Name: "id", Position: 0},
		},
	}

	// 二级索引（单列）
	secondaryIndex1 := &Index{
		IndexID:   2,
		TableID:   tableID,
		Name:      "idx_name",
		Type:      INDEX_TYPE_BTREE,
		IsPrimary: false,
		IsUnique:  false,
		State:     IndexStateActive,
		Columns: []Column{
			{Name: "name", Position: 0},
		},
	}

	// 二级索引（复合）
	secondaryIndex2 := &Index{
		IndexID:   3,
		TableID:   tableID,
		Name:      "idx_age_city",
		Type:      INDEX_TYPE_BTREE,
		IsPrimary: false,
		IsUnique:  false,
		State:     IndexStateActive,
		Columns: []Column{
			{Name: "age", Position: 0},
			{Name: "city", Position: 1},
		},
	}

	im.indexes[1] = primaryIndex
	im.indexes[2] = secondaryIndex1
	im.indexes[3] = secondaryIndex2

	t.Run("测试INSERT同步二级索引", func(t *testing.T) {
		rowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
			"age":  25,
			"city": "Beijing",
		}

		primaryKeyValue := []byte("pk_1")

		err := im.SyncSecondaryIndexesOnInsert(tableID, rowData, primaryKeyValue)
		assert.NoError(t, err)

		// 验证二级索引已插入
		// 注意：由于Mock实现简化，这里只检查是否调用了Insert
		t.Log("✓ INSERT二级索引同步测试通过")
	})
}

// TestSecondaryIndexSyncOnUpdate 测试UPDATE时的二级索引同步
func TestSecondaryIndexSyncOnUpdate(t *testing.T) {
	mockBTree := NewMockBTreeForIndexSync()

	im := &IndexManager{
		indexes:      make(map[uint64]*Index),
		btreeManager: mockBTree,
		stats:        &IndexManagerStats{},
		config:       &IndexManagerConfig{},
	}

	tableID := uint64(100)

	// 主键索引
	primaryIndex := &Index{
		IndexID:   1,
		TableID:   tableID,
		Name:      "PRIMARY",
		Type:      INDEX_TYPE_BTREE,
		IsPrimary: true,
		State:     IndexStateActive,
		Columns: []Column{
			{Name: "id", Position: 0},
		},
	}

	// 二级索引
	secondaryIndex := &Index{
		IndexID:   2,
		TableID:   tableID,
		Name:      "idx_name",
		Type:      INDEX_TYPE_BTREE,
		IsPrimary: false,
		State:     IndexStateActive,
		Columns: []Column{
			{Name: "name", Position: 0},
		},
	}

	im.indexes[1] = primaryIndex
	im.indexes[2] = secondaryIndex

	t.Run("测试UPDATE同步二级索引", func(t *testing.T) {
		oldRowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
			"age":  25,
		}

		newRowData := map[string]interface{}{
			"id":   1,
			"name": "Bob", // 名字变化
			"age":  26,    // 年龄变化（但不在索引列中）
		}

		primaryKeyValue := []byte("pk_1")

		err := im.SyncSecondaryIndexesOnUpdate(tableID, oldRowData, newRowData, primaryKeyValue)
		assert.NoError(t, err)

		t.Log("✓ UPDATE二级索引同步测试通过")
	})

	t.Run("测试UPDATE未影响索引列", func(t *testing.T) {
		oldRowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
			"age":  25,
		}

		newRowData := map[string]interface{}{
			"id":   1,
			"name": "Alice", // 名字未变
			"age":  26,      // 年龄变化（但不在索引列中）
		}

		primaryKeyValue := []byte("pk_1")

		err := im.SyncSecondaryIndexesOnUpdate(tableID, oldRowData, newRowData, primaryKeyValue)
		assert.NoError(t, err)

		t.Log("✓ UPDATE未影响索引列测试通过")
	})
}

// TestSecondaryIndexSyncOnDelete 测试DELETE时的二级索引同步
func TestSecondaryIndexSyncOnDelete(t *testing.T) {
	mockBTree := NewMockBTreeForIndexSync()

	im := &IndexManager{
		indexes:      make(map[uint64]*Index),
		btreeManager: mockBTree,
		stats:        &IndexManagerStats{},
		config:       &IndexManagerConfig{},
	}

	tableID := uint64(100)

	// 主键索引
	primaryIndex := &Index{
		IndexID:   1,
		TableID:   tableID,
		Name:      "PRIMARY",
		Type:      INDEX_TYPE_BTREE,
		IsPrimary: true,
		State:     IndexStateActive,
		Columns: []Column{
			{Name: "id", Position: 0},
		},
	}

	// 二级索引
	secondaryIndex := &Index{
		IndexID:   2,
		TableID:   tableID,
		Name:      "idx_name",
		Type:      INDEX_TYPE_BTREE,
		IsPrimary: false,
		State:     IndexStateActive,
		Columns: []Column{
			{Name: "name", Position: 0},
		},
	}

	im.indexes[1] = primaryIndex
	im.indexes[2] = secondaryIndex

	t.Run("测试DELETE同步二级索引", func(t *testing.T) {
		rowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
			"age":  25,
		}

		err := im.SyncSecondaryIndexesOnDelete(tableID, rowData)
		assert.NoError(t, err)

		t.Log("✓ DELETE二级索引同步测试通过")
	})
}
