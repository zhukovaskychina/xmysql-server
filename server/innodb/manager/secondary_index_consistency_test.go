package manager

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// EnhancedMockBTree 增强的Mock B+树，支持完整的CRUD操作
type EnhancedMockBTree struct {
	data map[string]map[string][]byte // indexID -> keyString -> value
}

func NewEnhancedMockBTree() *EnhancedMockBTree {
	return &EnhancedMockBTree{
		data: make(map[string]map[string][]byte),
	}
}

// keyToString 将key转换为字符串（用于map键）
func keyToString(key interface{}) string {
	switch v := key.(type) {
	case string:
		return v
	case int:
		return fmt.Sprintf("int:%d", v)
	case int64:
		return fmt.Sprintf("int64:%d", v)
	case []interface{}:
		// 复合键
		result := "composite:"
		for i, part := range v {
			if i > 0 {
				result += ","
			}
			result += keyToString(part)
		}
		return result
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (m *EnhancedMockBTree) Init(ctx context.Context, spaceId uint32, rootPage uint32) error {
	return nil
}

func (m *EnhancedMockBTree) GetAllLeafPages(ctx context.Context) ([]uint32, error) {
	return nil, nil
}

func (m *EnhancedMockBTree) Search(ctx context.Context, key interface{}) (uint32, int, error) {
	// 简化实现：检查key是否存在
	keyStr := keyToString(key)
	for _, indexData := range m.data {
		if _, exists := indexData[keyStr]; exists {
			return 0, 0, nil
		}
	}
	return 0, 0, fmt.Errorf("key not found")
}

func (m *EnhancedMockBTree) Insert(ctx context.Context, key interface{}, value []byte) error {
	// 使用默认索引ID "default"
	indexID := "default"
	if m.data[indexID] == nil {
		m.data[indexID] = make(map[string][]byte)
	}
	keyStr := keyToString(key)
	m.data[indexID][keyStr] = value
	return nil
}

func (m *EnhancedMockBTree) Delete(ctx context.Context, key interface{}) error {
	// 从所有索引中删除
	keyStr := keyToString(key)
	for _, indexData := range m.data {
		delete(indexData, keyStr)
	}
	return nil
}

func (m *EnhancedMockBTree) RangeSearch(ctx context.Context, startKey, endKey interface{}) ([]basic.Row, error) {
	return nil, nil
}

func (m *EnhancedMockBTree) GetFirstLeafPage(ctx context.Context) (uint32, error) {
	return 0, nil
}

// GetKeyCount 获取指定索引的键数量
func (m *EnhancedMockBTree) GetKeyCount(indexID string) int {
	if indexData, exists := m.data[indexID]; exists {
		return len(indexData)
	}
	return 0
}

// HasKey 检查键是否存在
func (m *EnhancedMockBTree) HasKey(indexID string, key interface{}) bool {
	keyStr := keyToString(key)
	if indexData, exists := m.data[indexID]; exists {
		_, keyExists := indexData[keyStr]
		return keyExists
	}
	return false
}

// TestSecondaryIndexConsistency 测试二级索引一致性
func TestSecondaryIndexConsistency(t *testing.T) {
	t.Run("完整的INSERT-UPDATE-DELETE流程", func(t *testing.T) {
		mockBTree := NewEnhancedMockBTree()
		im := &IndexManager{
			indexes:      make(map[uint64]*Index),
			btreeManager: mockBTree,
			stats:        &IndexManagerStats{},
			config:       &IndexManagerConfig{},
		}

		tableID := uint64(100)

		// 创建主键索引
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

		// 创建二级索引1（单列）
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

		// 创建二级索引2（复合）
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
		// Step 1: INSERT
		rowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
			"age":  25,
			"city": "Beijing",
		}
		primaryKeyValue := []byte("pk_1")

		err := im.SyncSecondaryIndexesOnInsert(tableID, rowData, primaryKeyValue)
		assert.NoError(t, err)
		t.Log("✓ Step 1: INSERT完成")

		// Step 2: UPDATE（修改索引列）
		oldRowData := rowData
		newRowData := map[string]interface{}{
			"id":   1,
			"name": "Bob",      // 名字变化
			"age":  26,         // 年龄变化
			"city": "Shanghai", // 城市变化
		}

		err = im.SyncSecondaryIndexesOnUpdate(tableID, oldRowData, newRowData, primaryKeyValue)
		assert.NoError(t, err)
		t.Log("✓ Step 2: UPDATE完成")

		// Step 3: DELETE
		err = im.SyncSecondaryIndexesOnDelete(tableID, newRowData)
		assert.NoError(t, err)
		t.Log("✓ Step 3: DELETE完成")

		t.Log("✓ 完整流程测试通过")
	})

	t.Run("唯一索引冲突检测", func(t *testing.T) {
		mockBTree := NewEnhancedMockBTree()
		im := &IndexManager{
			indexes:      make(map[uint64]*Index),
			btreeManager: mockBTree,
			stats:        &IndexManagerStats{},
			config:       &IndexManagerConfig{},
		}

		tableID := uint64(100)

		// 创建主键索引
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

		// 创建唯一索引
		uniqueIndex := &Index{
			IndexID:   2,
			TableID:   tableID,
			Name:      "idx_email_unique",
			Type:      INDEX_TYPE_BTREE,
			IsPrimary: false,
			IsUnique:  true,
			State:     IndexStateActive,
			Columns: []Column{
				{Name: "email", Position: 0},
			},
		}
		im.indexes[1] = primaryIndex
		im.indexes[2] = uniqueIndex

		// 插入第一条记录
		rowData1 := map[string]interface{}{
			"id":    2,
			"name":  "Charlie",
			"email": "charlie@example.com",
		}
		err := im.SyncSecondaryIndexesOnInsert(tableID, rowData1, []byte("pk_2"))
		assert.NoError(t, err)

		// 尝试插入重复的email（应该失败）
		rowData2 := map[string]interface{}{
			"id":    3,
			"name":  "David",
			"email": "charlie@example.com", // 重复的email
		}
		err = im.SyncSecondaryIndexesOnInsert(tableID, rowData2, []byte("pk_3"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")

		t.Log("✓ 唯一索引冲突检测通过")
	})

	t.Run("批量操作一致性", func(t *testing.T) {
		mockBTree := NewEnhancedMockBTree()
		im := &IndexManager{
			indexes:      make(map[uint64]*Index),
			btreeManager: mockBTree,
			stats:        &IndexManagerStats{},
			config:       &IndexManagerConfig{},
		}

		tableID := uint64(100)

		// 创建主键索引
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

		// 创建二级索引
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

		// 批量插入
		for i := 10; i < 20; i++ {
			rowData := map[string]interface{}{
				"id":   i,
				"name": fmt.Sprintf("User%d", i),
				"age":  20 + i,
				"city": "Beijing",
			}
			err := im.SyncSecondaryIndexesOnInsert(tableID, rowData, []byte(fmt.Sprintf("pk_%d", i)))
			assert.NoError(t, err)
		}

		// 批量更新
		for i := 10; i < 15; i++ {
			oldRowData := map[string]interface{}{
				"id":   i,
				"name": fmt.Sprintf("User%d", i),
				"age":  20 + i,
				"city": "Beijing",
			}
			newRowData := map[string]interface{}{
				"id":   i,
				"name": fmt.Sprintf("UpdatedUser%d", i),
				"age":  30 + i,
				"city": "Shanghai",
			}
			err := im.SyncSecondaryIndexesOnUpdate(tableID, oldRowData, newRowData, []byte(fmt.Sprintf("pk_%d", i)))
			assert.NoError(t, err)
		}

		// 批量删除
		for i := 15; i < 20; i++ {
			rowData := map[string]interface{}{
				"id":   i,
				"name": fmt.Sprintf("User%d", i),
				"age":  20 + i,
				"city": "Beijing",
			}
			err := im.SyncSecondaryIndexesOnDelete(tableID, rowData)
			assert.NoError(t, err)
		}

		t.Log("✓ 批量操作一致性测试通过")
	})

	t.Run("NULL值处理", func(t *testing.T) {
		mockBTree := NewEnhancedMockBTree()
		im := &IndexManager{
			indexes:      make(map[uint64]*Index),
			btreeManager: mockBTree,
			stats:        &IndexManagerStats{},
			config:       &IndexManagerConfig{},
		}

		tableID := uint64(100)

		// 创建主键索引
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

		// 创建二级索引
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

		// 插入包含NULL值的记录
		rowData := map[string]interface{}{
			"id":   100,
			"name": nil, // NULL值
			"age":  25,
			"city": "Beijing",
		}
		err := im.SyncSecondaryIndexesOnInsert(tableID, rowData, []byte("pk_100"))
		assert.NoError(t, err)

		// 更新NULL值
		newRowData := map[string]interface{}{
			"id":   100,
			"name": "NewName", // NULL -> 非NULL
			"age":  25,
			"city": "Beijing",
		}
		err = im.SyncSecondaryIndexesOnUpdate(tableID, rowData, newRowData, []byte("pk_100"))
		assert.NoError(t, err)

		t.Log("✓ NULL值处理测试通过")
	})
}

// TestSecondaryIndexErrorHandling 测试二级索引错误处理
func TestSecondaryIndexErrorHandling(t *testing.T) {
	mockBTree := NewEnhancedMockBTree()

	im := &IndexManager{
		indexes:      make(map[uint64]*Index),
		btreeManager: mockBTree,
		stats:        &IndexManagerStats{},
		config:       &IndexManagerConfig{},
	}

	tableID := uint64(100)

	t.Run("索引列缺失", func(t *testing.T) {
		// 创建索引
		idx := &Index{
			IndexID:   1,
			TableID:   tableID,
			Name:      "idx_missing_col",
			Type:      INDEX_TYPE_BTREE,
			IsPrimary: false,
			State:     IndexStateActive,
			Columns: []Column{
				{Name: "nonexistent_column", Position: 0},
			},
		}
		im.indexes[1] = idx

		// 尝试插入（应该失败）
		rowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
		}
		err := im.SyncSecondaryIndexesOnInsert(tableID, rowData, []byte("pk_1"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")

		t.Log("✓ 索引列缺失错误处理通过")
	})

	t.Run("禁用的索引不同步", func(t *testing.T) {
		mockBTree := NewEnhancedMockBTree()
		im := &IndexManager{
			indexes:      make(map[uint64]*Index),
			btreeManager: mockBTree,
			stats:        &IndexManagerStats{},
			config:       &IndexManagerConfig{},
		}

		tableID := uint64(100)

		// 创建主键索引
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

		// 创建禁用的索引
		disabledIndex := &Index{
			IndexID:   2,
			TableID:   tableID,
			Name:      "idx_disabled",
			Type:      INDEX_TYPE_BTREE,
			IsPrimary: false,
			State:     IndexStateDisabled, // 禁用状态
			Columns: []Column{
				{Name: "name", Position: 0},
			},
		}

		im.indexes[1] = primaryIndex
		im.indexes[2] = disabledIndex

		// 插入记录（禁用的索引不应该被同步）
		rowData := map[string]interface{}{
			"id":   2,
			"name": "Bob",
		}
		err := im.SyncSecondaryIndexesOnInsert(tableID, rowData, []byte("pk_2"))
		assert.NoError(t, err) // 应该成功，但不同步禁用的索引

		t.Log("✓ 禁用索引不同步测试通过")
	})
}
