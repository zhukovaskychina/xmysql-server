package mvcc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadView(t *testing.T) {
	// 创建测试场景
	activeIDs := []int64{2, 3, 5}
	minTrxID := int64(2)
	maxTrxID := int64(6)
	creatorTrxID := int64(4)

	rv := NewReadView(activeIDs, minTrxID, maxTrxID, creatorTrxID)

	t.Run("基本属性测试", func(t *testing.T) {
		assert.Equal(t, TrxId(minTrxID), rv.GetMinTrxID())
		assert.Equal(t, TrxId(maxTrxID), rv.GetMaxTrxID())
		assert.Equal(t, TrxId(creatorTrxID), rv.GetCreatorTrxID())
		assert.Len(t, rv.GetActiveIDs(), len(activeIDs))
	})

	t.Run("可见性规则测试", func(t *testing.T) {
		// 当前事务创建的版本
		assert.True(t, rv.IsVisible(creatorTrxID))

		// 小于最小活跃事务ID的版本
		assert.True(t, rv.IsVisible(1))

		// 大于等于下一个要分配的事务ID的版本
		assert.False(t, rv.IsVisible(maxTrxID))
		assert.False(t, rv.IsVisible(maxTrxID+1))

		// 活跃事务列表中的版本
		assert.False(t, rv.IsVisible(2))
		assert.False(t, rv.IsVisible(3))
		assert.False(t, rv.IsVisible(5))

		// 不在活跃列表中且在合理范围内的版本
		assert.True(t, rv.IsVisible(1))
	})

	t.Run("边界条件测试", func(t *testing.T) {
		// 空活跃事务列表
		emptyRv := NewReadView([]int64{}, 1, 2, 1)
		assert.True(t, emptyRv.IsVisible(1))
		assert.False(t, emptyRv.IsVisible(2))

		// 最小值等于最大值
		sameRv := NewReadView([]int64{1}, 1, 1, 1)
		assert.True(t, sameRv.IsVisible(1))
		assert.False(t, sameRv.IsVisible(2))
	})

	t.Run("复杂场景测试", func(t *testing.T) {
		// 模拟多个事务的场景
		complexRv := NewReadView(
			[]int64{2, 4, 6, 8}, // 活跃事务
			2,                   // 最小活跃事务ID
			10,                  // 下一个事务ID
			5,                   // 当前事务ID
		)

		// 测试各种版本的可见性
		visibilityTests := []struct {
			version  int64
			expected bool
		}{
			{1, true},   // 小于最小活跃事务ID
			{2, false},  // 在活跃列表中
			{3, true},   // 不在活跃列表中且在范围内
			{4, false},  // 在活跃列表中
			{5, true},   // 当前事务ID
			{6, false},  // 在活跃列表中
			{7, true},   // 不在活跃列表中且在范围内
			{8, false},  // 在活跃列表中
			{9, true},   // 不在活跃列表中且在范围内
			{10, false}, // 等于maxTrxID
			{11, false}, // 大于maxTrxID
		}

		for _, tt := range visibilityTests {
			assert.Equal(t, tt.expected, complexRv.IsVisible(tt.version),
				"version %d should have visibility %v", tt.version, tt.expected)
		}
	})
}
