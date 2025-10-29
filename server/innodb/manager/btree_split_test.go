package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeSplitter_SetSplitRatio 测试设置分裂比例
func TestNodeSplitter_SetSplitRatio(t *testing.T) {
	tests := []struct {
		name     string
		ratio    float64
		expected bool
		want     float64
	}{
		{"默认50/50", 0.5, true, 0.5},
		{"40/60分裂", 0.4, true, 0.4},
		{"60/40分裂", 0.6, true, 0.6},
		{"边界值0.4", 0.4, true, 0.4},
		{"边界值0.6", 0.6, true, 0.6},
		{"非法值0.3", 0.3, false, 0.5}, // 保持原值
		{"非法值0.7", 0.7, false, 0.5}, // 保持原值
		{"非法值0.0", 0.0, false, 0.5},
		{"非法值1.0", 1.0, false, 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpm := setupTestBufferPool(t)
			btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
			splitter := NewNodeSplitter(btm, 3)

			got := splitter.SetSplitRatio(tt.ratio)
			assert.Equal(t, tt.expected, got)

			if tt.expected {
				assert.Equal(t, tt.ratio, splitter.GetSplitRatio())
			} else {
				assert.Equal(t, 0.5, splitter.GetSplitRatio()) // 失败时保持默认值
			}
		})
	}
}

// TestNodeSplitter_SplitLeafNode 测试叶子节点分裂
func TestNodeSplitter_SplitLeafNode(t *testing.T) {
	bpm := setupTestBufferPool(t)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()

	// 初始化B+树
	err := btm.Init(ctx, 1, 1)
	require.NoError(t, err)

	splitter := NewNodeSplitter(btm, 3)

	tests := []struct {
		name           string
		nodeKeys       []interface{}
		nodeRecords    []uint32
		splitRatio     float64
		expectedLeft   int
		expectedRight  int
		expectedMiddle interface{}
	}{
		{
			name:           "50/50分裂5个键",
			nodeKeys:       []interface{}{10, 20, 30, 40, 50},
			nodeRecords:    []uint32{1, 2, 3, 4, 5},
			splitRatio:     0.5,
			expectedLeft:   2,
			expectedRight:  3,
			expectedMiddle: 30,
		},
		{
			name:           "40/60分裂5个键",
			nodeKeys:       []interface{}{10, 20, 30, 40, 50},
			nodeRecords:    []uint32{1, 2, 3, 4, 5},
			splitRatio:     0.4,
			expectedLeft:   2,
			expectedRight:  3,
			expectedMiddle: 30,
		},
		{
			name:           "60/40分裂5个键",
			nodeKeys:       []interface{}{10, 20, 30, 40, 50},
			nodeRecords:    []uint32{1, 2, 3, 4, 5},
			splitRatio:     0.6,
			expectedLeft:   3,
			expectedRight:  2,
			expectedMiddle: 40,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			splitter.SetSplitRatio(tt.splitRatio)

			// 创建满载叶子节点
			fullNode := &BPlusTreeNode{
				PageNum:  1001,
				IsLeaf:   true,
				Keys:     tt.nodeKeys,
				Records:  tt.nodeRecords,
				NextLeaf: 0,
			}

			// 添加到缓存
			btm.nodeCache[fullNode.PageNum] = fullNode

			// 执行分裂
			newPageNo, middleKey, err := splitter.SplitLeafNode(ctx, fullNode)

			// 验证结果
			assert.NoError(t, err)
			assert.Greater(t, newPageNo, uint32(0))
			assert.Equal(t, tt.expectedMiddle, middleKey)
			assert.Equal(t, tt.expectedLeft, len(fullNode.Keys))
			assert.Equal(t, newPageNo, fullNode.NextLeaf)

			// 验证新节点
			newNode, exists := btm.nodeCache[newPageNo]
			assert.True(t, exists)
			assert.Equal(t, tt.expectedRight, len(newNode.Keys))
			assert.True(t, newNode.IsLeaf)
			assert.True(t, newNode.isDirty)
		})
	}
}

// TestNodeSplitter_SplitNonLeafNode 测试非叶子节点分裂
func TestNodeSplitter_SplitNonLeafNode(t *testing.T) {
	bpm := setupTestBufferPool(t)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()

	err := btm.Init(ctx, 1, 1)
	require.NoError(t, err)

	splitter := NewNodeSplitter(btm, 3)

	// 创建满载非叶子节点
	fullNode := &BPlusTreeNode{
		PageNum:  2001,
		IsLeaf:   false,
		Keys:     []interface{}{50, 100, 150, 200, 250},
		Children: []uint32{1001, 1002, 1003, 1004, 1005, 1006},
		isDirty:  false,
	}

	btm.nodeCache[fullNode.PageNum] = fullNode

	// 执行分裂
	newPageNo, middleKey, err := splitter.SplitNonLeafNode(ctx, fullNode)

	// 验证结果
	assert.NoError(t, err)
	assert.Greater(t, newPageNo, uint32(0))
	assert.Equal(t, 150, middleKey) // 中间键

	// 验证左节点（原节点）
	assert.Equal(t, 2, len(fullNode.Keys)) // 50, 100
	assert.Equal(t, 3, len(fullNode.Children))
	assert.True(t, fullNode.isDirty)

	// 验证右节点
	newNode, exists := btm.nodeCache[newPageNo]
	assert.True(t, exists)
	assert.Equal(t, 2, len(newNode.Keys)) // 200, 250
	assert.Equal(t, 3, len(newNode.Children))
	assert.False(t, newNode.IsLeaf)
	assert.True(t, newNode.isDirty)
}

// TestNodeSplitter_CreateNewRoot 测试创建新根节点
func TestNodeSplitter_CreateNewRoot(t *testing.T) {
	bpm := setupTestBufferPool(t)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()

	oldRootPage := uint32(1)
	err := btm.Init(ctx, 1, oldRootPage)
	require.NoError(t, err)

	splitter := NewNodeSplitter(btm, 3)

	// 执行创建新根
	err = splitter.createNewRoot(ctx, oldRootPage, 2, 100)

	assert.NoError(t, err)

	// 验证新根节点
	newRootPage := btm.rootPage
	assert.NotEqual(t, oldRootPage, newRootPage)

	newRoot, exists := btm.nodeCache[newRootPage]
	assert.True(t, exists)
	assert.False(t, newRoot.IsLeaf)
	assert.Equal(t, 1, len(newRoot.Keys))
	assert.Equal(t, 100, newRoot.Keys[0])
	assert.Equal(t, 2, len(newRoot.Children))
	assert.Equal(t, oldRootPage, newRoot.Children[0])
	assert.Equal(t, uint32(2), newRoot.Children[1])
	assert.True(t, newRoot.isDirty)
}

// TestNodeSplitter_RecursiveSplit 测试递归分裂
func TestNodeSplitter_RecursiveSplit(t *testing.T) {
	bpm := setupTestBufferPool(t)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()

	err := btm.Init(ctx, 1, 1)
	require.NoError(t, err)

	splitter := NewNodeSplitter(btm, 3)

	// 创建一个3层的B+树结构
	// 根节点 (2001) -> 子节点 (3001, 3002) -> 叶子节点
	rootNode := &BPlusTreeNode{
		PageNum:  2001,
		IsLeaf:   false,
		Keys:     []interface{}{100},
		Children: []uint32{3001, 3002},
	}

	leftChild := &BPlusTreeNode{
		PageNum:  3001,
		IsLeaf:   false,
		Keys:     []interface{}{30, 50, 70, 90, 110}, // 5个键，超过maxKeys
		Children: []uint32{4001, 4002, 4003, 4004, 4005, 4006},
	}

	rightChild := &BPlusTreeNode{
		PageNum:  3002,
		IsLeaf:   false,
		Keys:     []interface{}{150, 200},
		Children: []uint32{4007, 4008, 4009},
	}

	// 加入缓存
	btm.nodeCache[rootNode.PageNum] = rootNode
	btm.nodeCache[leftChild.PageNum] = leftChild
	btm.nodeCache[rightChild.PageNum] = rightChild
	btm.rootPage = rootNode.PageNum
	btm.treeHeight = 2

	// 执行分裂leftChild，应该触发递归
	newPage, middleKey, err := splitter.SplitNonLeafNode(ctx, leftChild)
	assert.NoError(t, err)
	assert.Greater(t, newPage, uint32(0))

	// 插入到父节点，应该不触发父节点分裂（因为rootNode只有1个键）
	err = splitter.InsertIntoParent(ctx, leftChild.PageNum, newPage, middleKey)
	assert.NoError(t, err)

	// 验证父节点现在有2个键
	assert.Equal(t, 2, len(rootNode.Keys))
	assert.Equal(t, 3, len(rootNode.Children))
	assert.True(t, rootNode.isDirty)
}

// TestNodeSplitter_RecursiveDepthLimit 测试递归深度限制
func TestNodeSplitter_RecursiveDepthLimit(t *testing.T) {
	bpm := setupTestBufferPool(t)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()

	err := btm.Init(ctx, 1, 1)
	require.NoError(t, err)

	splitter := NewNodeSplitter(btm, 3)
	splitter.maxRecursionDepth = 2 // 设置很小的限制

	// 模拟一个会超过深度限制的情况
	// 这里我们直接调用内部方法
	err = splitter.insertIntoParentWithDepth(ctx, 1, 2, 100, 3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "maximum recursion depth")
}

// TestNodeSplitter_TreeHeightTracking 测试树高度跟踪
func TestNodeSplitter_TreeHeightTracking(t *testing.T) {
	bpm := setupTestBufferPool(t)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()

	err := btm.Init(ctx, 1, 1)
	require.NoError(t, err)

	initialHeight := btm.GetTreeHeight()
	assert.Equal(t, uint32(1), initialHeight)

	splitter := NewNodeSplitter(btm, 3)

	// 创建新根节点，应该增加高度
	err = splitter.createNewRoot(ctx, 1, 2, 100)
	assert.NoError(t, err)

	newHeight := btm.GetTreeHeight()
	assert.Equal(t, uint32(2), newHeight)
	assert.Greater(t, newHeight, initialHeight)
}

// setupTestBufferPool 创建测试用的缓冲池
func setupTestBufferPool(t *testing.T) *OptimizedBufferPoolManager {
	config := &BufferPoolConfig{
		PoolSize:         128,
		PageSize:         16384,
		MaxDirtyPages:    64,
		FlushInterval:    60,
		EnablePrefetch:   false,
		PrefetchSize:     4,
		EvictionPolicy:   "LRU",
		EnableMonitoring: false,
	}

	bpm, err := NewOptimizedBufferPoolManager(config)
	require.NoError(t, err)
	return bpm
}
