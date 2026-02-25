package mvcc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewReadView(t *testing.T) {
	t.Run("创建空活跃事务列表的ReadView", func(t *testing.T) {
		rv := NewReadView([]uint64{}, 1, 10)

		assert.Equal(t, uint64(1), rv.TxID)
		assert.Equal(t, uint64(10), rv.HighWaterMark)
		assert.Equal(t, uint64(10), rv.LowWaterMark) // 无活跃事务时，lowWaterMark = nextTxID
		assert.Empty(t, rv.ActiveTxIDs)
		assert.Empty(t, rv.ActiveTxMap)
	})

	t.Run("创建有活跃事务的ReadView", func(t *testing.T) {
		activeIDs := []uint64{3, 5, 2, 7}
		rv := NewReadView(activeIDs, 4, 10)

		assert.Equal(t, uint64(4), rv.TxID)
		assert.Equal(t, uint64(10), rv.HighWaterMark)
		assert.Equal(t, uint64(2), rv.LowWaterMark) // 最小活跃事务ID
		assert.Len(t, rv.ActiveTxIDs, 4)
		assert.Len(t, rv.ActiveTxMap, 4)

		// 验证ActiveTxIDs已排序
		assert.Equal(t, []uint64{2, 3, 5, 7}, rv.ActiveTxIDs)

		// 验证ActiveTxMap包含所有活跃事务
		for _, id := range activeIDs {
			assert.True(t, rv.ActiveTxMap[id])
		}
	})

	t.Run("创建重复活跃事务ID的ReadView", func(t *testing.T) {
		activeIDs := []uint64{3, 5, 3, 5, 2}
		rv := NewReadView(activeIDs, 4, 10)

		// 应该保留重复的ID（因为是从slice复制的）
		assert.Len(t, rv.ActiveTxIDs, 5)
		assert.Len(t, rv.ActiveTxMap, 3) // map会去重
	})
}

func TestReadView_IsVisible(t *testing.T) {
	// 创建测试场景：
	// - 当前事务ID: 4
	// - 活跃事务: [2, 3, 5, 7]
	// - 下一个事务ID: 10
	activeIDs := []uint64{2, 3, 5, 7}
	rv := NewReadView(activeIDs, 4, 10)

	t.Run("规则1: 当前事务创建的版本可见", func(t *testing.T) {
		assert.True(t, rv.IsVisible(4))
	})

	t.Run("规则2: 小于lowWaterMark的版本可见", func(t *testing.T) {
		assert.True(t, rv.IsVisible(1))
		assert.False(t, rv.IsVisible(2)) // 2是活跃事务
	})

	t.Run("规则3: 大于等于highWaterMark的版本不可见", func(t *testing.T) {
		assert.False(t, rv.IsVisible(10))
		assert.False(t, rv.IsVisible(11))
		assert.False(t, rv.IsVisible(100))
	})

	t.Run("规则4: 在范围内的活跃事务不可见", func(t *testing.T) {
		assert.False(t, rv.IsVisible(2))
		assert.False(t, rv.IsVisible(3))
		assert.False(t, rv.IsVisible(5))
		assert.False(t, rv.IsVisible(7))
	})

	t.Run("规则4: 在范围内但不活跃的事务可见", func(t *testing.T) {
		// 6, 8, 9 不在活跃列表中，且在[lowWaterMark, highWaterMark)范围内
		assert.True(t, rv.IsVisible(6))
		assert.True(t, rv.IsVisible(8))
		assert.True(t, rv.IsVisible(9))
	})
}

func TestReadView_IsVisibleFast(t *testing.T) {
	// 创建相同的测试场景
	activeIDs := []uint64{2, 3, 5, 7}
	rv := NewReadView(activeIDs, 4, 10)

	t.Run("IsVisibleFast应该与IsVisible结果一致", func(t *testing.T) {
		testCases := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
		for _, txID := range testCases {
			expected := rv.IsVisible(txID)
			actual := rv.IsVisibleFast(txID)
			assert.Equal(t, expected, actual, "txID=%d", txID)
		}
	})
}

func TestReadView_EdgeCases(t *testing.T) {
	t.Run("事务ID为0", func(t *testing.T) {
		rv := NewReadView([]uint64{1, 2, 3}, 4, 10)
		// 0通常是无效的事务ID，应该小于lowWaterMark
		assert.True(t, rv.IsVisible(0))
	})

	t.Run("只有一个活跃事务", func(t *testing.T) {
		rv := NewReadView([]uint64{5}, 4, 10)
		assert.Equal(t, uint64(5), rv.LowWaterMark)
		assert.False(t, rv.IsVisible(5))
		assert.True(t, rv.IsVisible(4))
		assert.True(t, rv.IsVisible(6))
	})

	t.Run("当前事务是最小的活跃事务", func(t *testing.T) {
		rv := NewReadView([]uint64{2, 3, 5}, 2, 10)
		assert.Equal(t, uint64(2), rv.LowWaterMark)
		assert.True(t, rv.IsVisible(2)) // 规则1优先
		assert.True(t, rv.IsVisible(1))
		assert.False(t, rv.IsVisible(3))
	})

	t.Run("所有事务ID都很大", func(t *testing.T) {
		activeIDs := []uint64{1000, 2000, 3000}
		rv := NewReadView(activeIDs, 1500, 5000)
		assert.Equal(t, uint64(1000), rv.LowWaterMark)
		assert.True(t, rv.IsVisible(999))
		assert.False(t, rv.IsVisible(1000))
		assert.True(t, rv.IsVisible(1500))
		assert.False(t, rv.IsVisible(5000))
	})
}

func TestReadView_Concurrency(t *testing.T) {
	t.Run("ReadView应该是不可变的", func(t *testing.T) {
		activeIDs := []uint64{2, 3, 5}
		rv := NewReadView(activeIDs, 4, 10)

		// 修改原始slice不应影响ReadView
		activeIDs[0] = 999
		assert.Equal(t, uint64(2), rv.ActiveTxIDs[0])
		assert.False(t, rv.ActiveTxMap[999])
		assert.True(t, rv.ActiveTxMap[2])
	})
}

func BenchmarkReadView_IsVisible(b *testing.B) {
	// 创建一个有100个活跃事务的ReadView
	activeIDs := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		activeIDs[i] = uint64(i*2 + 1) // 奇数事务ID
	}
	rv := NewReadView(activeIDs, 50, 300)

	b.Run("Map查找", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rv.IsVisible(uint64(i % 300))
		}
	})

	b.Run("二分查找", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rv.IsVisibleFast(uint64(i % 300))
		}
	})
}

func BenchmarkReadView_Creation(b *testing.B) {
	activeIDs := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		activeIDs[i] = uint64(i*2 + 1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewReadView(activeIDs, 50, 300)
	}
}
