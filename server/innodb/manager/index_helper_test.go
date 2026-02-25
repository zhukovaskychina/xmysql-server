package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestExtractIndexKey 测试从行数据中提取索引键
func TestExtractIndexKey(t *testing.T) {
	im := &IndexManager{
		indexes: make(map[uint64]*Index),
	}

	t.Run("单列索引", func(t *testing.T) {
		idx := &Index{
			IndexID: 1,
			Columns: []Column{
				{Name: "name", Position: 0},
			},
		}

		rowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
			"age":  25,
		}

		key, err := im.extractIndexKey(idx, rowData)
		assert.NoError(t, err)
		assert.Equal(t, "Alice", key)

		t.Log("✓ 单列索引键提取测试通过")
	})

	t.Run("复合索引", func(t *testing.T) {
		idx := &Index{
			IndexID: 2,
			Columns: []Column{
				{Name: "age", Position: 0},
				{Name: "city", Position: 1},
			},
		}

		rowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
			"age":  25,
			"city": "Beijing",
		}

		key, err := im.extractIndexKey(idx, rowData)
		assert.NoError(t, err)

		// 复合索引返回切片
		keyParts, ok := key.([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 2, len(keyParts))
		assert.Equal(t, 25, keyParts[0])
		assert.Equal(t, "Beijing", keyParts[1])

		t.Log("✓ 复合索引键提取测试通过")
	})

	t.Run("列不存在", func(t *testing.T) {
		idx := &Index{
			IndexID: 3,
			Columns: []Column{
				{Name: "nonexistent", Position: 0},
			},
		}

		rowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
		}

		_, err := im.extractIndexKey(idx, rowData)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")

		t.Log("✓ 列不存在错误处理测试通过")
	})
}

// TestIsIndexAffected 测试检查索引列是否被更新
func TestIsIndexAffected(t *testing.T) {
	im := &IndexManager{
		indexes: make(map[uint64]*Index),
	}

	idx := &Index{
		IndexID: 1,
		Columns: []Column{
			{Name: "name", Position: 0},
			{Name: "age", Position: 1},
		},
	}

	t.Run("索引列被更新", func(t *testing.T) {
		oldRowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
			"age":  25,
			"city": "Beijing",
		}

		newRowData := map[string]interface{}{
			"id":   1,
			"name": "Bob", // 名字变化
			"age":  25,
			"city": "Shanghai", // 城市变化（但不在索引列中）
		}

		affected := im.isIndexAffected(idx, oldRowData, newRowData)
		assert.True(t, affected)

		t.Log("✓ 索引列被更新检测通过")
	})

	t.Run("索引列未被更新", func(t *testing.T) {
		oldRowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
			"age":  25,
			"city": "Beijing",
		}

		newRowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",    // 名字未变
			"age":  25,         // 年龄未变
			"city": "Shanghai", // 城市变化（但不在索引列中）
		}

		affected := im.isIndexAffected(idx, oldRowData, newRowData)
		assert.False(t, affected)

		t.Log("✓ 索引列未被更新检测通过")
	})

	t.Run("列存在性变化", func(t *testing.T) {
		oldRowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
			"age":  25,
		}

		newRowData := map[string]interface{}{
			"id":   1,
			"name": "Alice",
			// age 列被删除
		}

		affected := im.isIndexAffected(idx, oldRowData, newRowData)
		assert.True(t, affected)

		t.Log("✓ 列存在性变化检测通过")
	})
}

// TestGetSecondaryIndexesByTable 测试获取表的二级索引
func TestGetSecondaryIndexesByTable(t *testing.T) {
	im := &IndexManager{
		indexes: make(map[uint64]*Index),
	}

	tableID := uint64(100)

	// 主键索引
	im.indexes[1] = &Index{
		IndexID:   1,
		TableID:   tableID,
		Name:      "PRIMARY",
		IsPrimary: true,
		State:     IndexStateActive,
	}

	// 二级索引1
	im.indexes[2] = &Index{
		IndexID:   2,
		TableID:   tableID,
		Name:      "idx_name",
		IsPrimary: false,
		State:     IndexStateActive,
	}

	// 二级索引2
	im.indexes[3] = &Index{
		IndexID:   3,
		TableID:   tableID,
		Name:      "idx_age",
		IsPrimary: false,
		State:     IndexStateActive,
	}

	// 其他表的索引
	im.indexes[4] = &Index{
		IndexID:   4,
		TableID:   200,
		Name:      "idx_other",
		IsPrimary: false,
		State:     IndexStateActive,
	}

	// 禁用的索引
	im.indexes[5] = &Index{
		IndexID:   5,
		TableID:   tableID,
		Name:      "idx_disabled",
		IsPrimary: false,
		State:     IndexStateDisabled,
	}

	t.Run("获取表的二级索引", func(t *testing.T) {
		secondaryIndexes := im.getSecondaryIndexesByTable(tableID)

		// 应该只返回2个活跃的二级索引
		assert.Equal(t, 2, len(secondaryIndexes))

		// 验证返回的索引
		indexIDs := make(map[uint64]bool)
		for _, idx := range secondaryIndexes {
			indexIDs[idx.IndexID] = true
			assert.Equal(t, tableID, idx.TableID)
			assert.False(t, idx.IsPrimary)
			assert.Equal(t, IndexStateActive, idx.State)
		}

		assert.True(t, indexIDs[2])
		assert.True(t, indexIDs[3])
		assert.False(t, indexIDs[1]) // 主键不应包含
		assert.False(t, indexIDs[4]) // 其他表的索引不应包含
		assert.False(t, indexIDs[5]) // 禁用的索引不应包含

		t.Log("✓ 获取表的二级索引测试通过")
	})
}
