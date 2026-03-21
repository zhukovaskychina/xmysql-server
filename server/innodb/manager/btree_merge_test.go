package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeMerger_MergeLeafNodes IDX-002：叶子节点合并后键与 NextLeaf 正确
func TestNodeMerger_MergeLeafNodes(t *testing.T) {
	bpm := setupTestBufferPool(t)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()
	err := btm.Init(ctx, 1, 1)
	require.NoError(t, err)

	left := &BPlusTreeNode{
		PageNum:  1001,
		IsLeaf:   true,
		Keys:     []interface{}{10, 20},
		Records:  []uint32{1, 2},
		NextLeaf: 1002,
	}
	right := &BPlusTreeNode{
		PageNum:  1002,
		IsLeaf:   true,
		Keys:     []interface{}{30, 40},
		Records:  []uint32{3, 4},
		NextLeaf: 0,
	}
	btm.nodeCache[left.PageNum] = left
	btm.nodeCache[right.PageNum] = right

	merger := NewNodeMerger(btm, 3)
	mergedPageNo, err := merger.MergeLeafNodes(ctx, left, right)
	require.NoError(t, err)
	assert.Equal(t, uint32(1001), mergedPageNo)
	assert.Len(t, left.Keys, 4)
	assert.Equal(t, []interface{}{10, 20, 30, 40}, left.Keys)
	assert.Len(t, left.Records, 4)
	assert.Equal(t, []uint32{1, 2, 3, 4}, left.Records)
	assert.Equal(t, uint32(0), left.NextLeaf)
}

// TestNodeMerger_MergeNonLeafNodes IDX-002：非叶子节点合并后键与子指针正确
func TestNodeMerger_MergeNonLeafNodes(t *testing.T) {
	bpm := setupTestBufferPool(t)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()
	err := btm.Init(ctx, 1, 1)
	require.NoError(t, err)

	left := &BPlusTreeNode{
		PageNum:  2001,
		IsLeaf:   false,
		Keys:     []interface{}{10, 20},
		Children: []uint32{101, 102, 103},
	}
	right := &BPlusTreeNode{
		PageNum:  2002,
		IsLeaf:   false,
		Keys:     []interface{}{40, 50},
		Children: []uint32{104, 105, 106},
	}
	btm.nodeCache[left.PageNum] = left
	btm.nodeCache[right.PageNum] = right

	merger := NewNodeMerger(btm, 3)
	mergedPageNo, err := merger.MergeNonLeafNodes(ctx, left, right, 30)
	require.NoError(t, err)
	assert.Equal(t, uint32(2001), mergedPageNo)
	assert.Len(t, left.Keys, 5)
	assert.Equal(t, []interface{}{10, 20, 30, 40, 50}, left.Keys)
	assert.Len(t, left.Children, 6)
	assert.Equal(t, []uint32{101, 102, 103, 104, 105, 106}, left.Children)
}
