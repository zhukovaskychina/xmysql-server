package manager

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
)

// NodeMerger B+树节点合并操作
// 当节点的键数少于最小值时，与兄弟节点合并或从兄弟节点借键
type NodeMerger struct {
	manager *DefaultBPlusTreeManager
	minKeys int // 节点最小键数
	maxKeys int // 节点最大键数
}

// NewNodeMerger 创建节点合并器
func NewNodeMerger(manager *DefaultBPlusTreeManager, degree int) *NodeMerger {
	return &NodeMerger{
		manager: manager,
		minKeys: degree - 1,
		maxKeys: 2*degree - 1,
	}
}

// MergeLeafNodes 合并两个叶子节点
// leftNode: 左节点，rightNode: 右节点
// 返回：合并后的节点页号
func (m *NodeMerger) MergeLeafNodes(ctx context.Context, leftNode, rightNode *BPlusTreeNode) (mergedPageNo uint32, err error) {
	if !leftNode.IsLeaf || !rightNode.IsLeaf {
		return 0, fmt.Errorf("both nodes must be leaf nodes")
	}

	logger.Debugf("🔗 Merging leaf nodes: left=%d (%d keys), right=%d (%d keys)",
		leftNode.PageNum, len(leftNode.Keys), rightNode.PageNum, len(rightNode.Keys))

	// 检查合并后是否会超出最大键数
	totalKeys := len(leftNode.Keys) + len(rightNode.Keys)
	if totalKeys > m.maxKeys {
		return 0, fmt.Errorf("cannot merge: total keys %d exceeds max %d", totalKeys, m.maxKeys)
	}

	// 将右节点的所有键和记录合并到左节点
	leftNode.Keys = append(leftNode.Keys, rightNode.Keys...)
	leftNode.Records = append(leftNode.Records, rightNode.Records...)
	leftNode.NextLeaf = rightNode.NextLeaf // 更新链表指针
	leftNode.isDirty = true

	// 标记右节点为待删除
	rightNode.isDirty = true

	logger.Debugf("✅ Leaf merge complete: merged_node=%d (%d keys)", leftNode.PageNum, len(leftNode.Keys))

	return leftNode.PageNum, nil
}

// MergeNonLeafNodes 合并两个非叶子节点
// middleKey: 从父节点下降的中间键
func (m *NodeMerger) MergeNonLeafNodes(ctx context.Context, leftNode, rightNode *BPlusTreeNode, middleKey interface{}) (mergedPageNo uint32, err error) {
	if leftNode.IsLeaf || rightNode.IsLeaf {
		return 0, fmt.Errorf("both nodes must be non-leaf nodes")
	}

	logger.Debugf("🔗 Merging non-leaf nodes: left=%d (%d keys), right=%d (%d keys), middle_key=%v",
		leftNode.PageNum, len(leftNode.Keys), rightNode.PageNum, len(rightNode.Keys), middleKey)

	// 将中间键和右节点的键合并到左节点
	// 格式: [left.keys][middleKey][right.keys]
	leftNode.Keys = append(leftNode.Keys, middleKey)
	leftNode.Keys = append(leftNode.Keys, rightNode.Keys...)

	// 合并子节点指针
	leftNode.Children = append(leftNode.Children, rightNode.Children...)
	leftNode.isDirty = true

	// 标记右节点为待删除
	rightNode.isDirty = true

	logger.Debugf("✅ Non-leaf merge complete: merged_node=%d (%d keys)", leftNode.PageNum, len(leftNode.Keys))

	return leftNode.PageNum, nil
}

// BorrowFromLeftSibling 从左兄弟节点借键
func (m *NodeMerger) BorrowFromLeftSibling(ctx context.Context, node, leftSibling *BPlusTreeNode, parentKey interface{}) (newParentKey interface{}, err error) {
	if len(leftSibling.Keys) <= m.minKeys {
		return nil, fmt.Errorf("left sibling has too few keys to borrow")
	}

	logger.Debugf("⬅️ Borrowing from left sibling: node=%d, left_sibling=%d", node.PageNum, leftSibling.PageNum)

	if node.IsLeaf {
		// 叶子节点：移动最后一个键和记录
		lastKey := leftSibling.Keys[len(leftSibling.Keys)-1]
		lastRecord := leftSibling.Records[len(leftSibling.Records)-1]

		// 从左兄弟删除
		leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
		leftSibling.Records = leftSibling.Records[:len(leftSibling.Records)-1]
		leftSibling.isDirty = true

		// 插入到当前节点开头
		node.Keys = append([]interface{}{lastKey}, node.Keys...)
		node.Records = append([]uint32{lastRecord}, node.Records...)
		node.isDirty = true

		// 新的父键是当前节点的第一个键
		newParentKey = node.Keys[0]
	} else {
		// 非叶子节点：需要通过父节点的键
		lastKey := leftSibling.Keys[len(leftSibling.Keys)-1]
		lastChild := leftSibling.Children[len(leftSibling.Children)-1]

		// 从左兄弟删除
		leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
		leftSibling.Children = leftSibling.Children[:len(leftSibling.Children)-1]
		leftSibling.isDirty = true

		// 将父键插入当前节点，将借来的键提升为新父键
		node.Keys = append([]interface{}{parentKey}, node.Keys...)
		node.Children = append([]uint32{lastChild}, node.Children...)
		node.isDirty = true

		newParentKey = lastKey
	}

	logger.Debugf("✅ Borrowed successfully, new_parent_key=%v", newParentKey)
	return newParentKey, nil
}

// BorrowFromRightSibling 从右兄弟节点借键
func (m *NodeMerger) BorrowFromRightSibling(ctx context.Context, node, rightSibling *BPlusTreeNode, parentKey interface{}) (newParentKey interface{}, err error) {
	if len(rightSibling.Keys) <= m.minKeys {
		return nil, fmt.Errorf("right sibling has too few keys to borrow")
	}

	logger.Debugf("➡️ Borrowing from right sibling: node=%d, right_sibling=%d", node.PageNum, rightSibling.PageNum)

	if node.IsLeaf {
		// 叶子节点：移动第一个键和记录
		firstKey := rightSibling.Keys[0]
		firstRecord := rightSibling.Records[0]

		// 从右兄弟删除
		rightSibling.Keys = rightSibling.Keys[1:]
		rightSibling.Records = rightSibling.Records[1:]
		rightSibling.isDirty = true

		// 追加到当前节点末尾
		node.Keys = append(node.Keys, firstKey)
		node.Records = append(node.Records, firstRecord)
		node.isDirty = true

		// 新的父键是右兄弟的第一个键
		newParentKey = rightSibling.Keys[0]
	} else {
		// 非叶子节点
		firstKey := rightSibling.Keys[0]
		firstChild := rightSibling.Children[0]

		// 从右兄弟删除
		rightSibling.Keys = rightSibling.Keys[1:]
		rightSibling.Children = rightSibling.Children[1:]
		rightSibling.isDirty = true

		// 将父键追加到当前节点
		node.Keys = append(node.Keys, parentKey)
		node.Children = append(node.Children, firstChild)
		node.isDirty = true

		newParentKey = firstKey
	}

	logger.Debugf("✅ Borrowed successfully, new_parent_key=%v", newParentKey)
	return newParentKey, nil
}

// DeleteFromParent 从父节点删除键
func (m *NodeMerger) DeleteFromParent(ctx context.Context, parentNode *BPlusTreeNode, childPage uint32) error {
	logger.Debugf("🗑️ Deleting child %d from parent %d", childPage, parentNode.PageNum)

	// 找到子节点在父节点中的位置
	deletePos := -1
	for i, child := range parentNode.Children {
		if child == childPage {
			deletePos = i
			break
		}
	}

	if deletePos == -1 {
		return fmt.Errorf("child page %d not found in parent %d", childPage, parentNode.PageNum)
	}

	// 删除对应的键和子节点指针
	if deletePos > 0 {
		// 删除前一个键（因为键的数量比子节点少1）
		parentNode.Keys = append(parentNode.Keys[:deletePos-1], parentNode.Keys[deletePos:]...)
	} else if len(parentNode.Keys) > 0 {
		// 如果是第一个子节点，删除第一个键
		parentNode.Keys = parentNode.Keys[1:]
	}

	// 删除子节点指针
	parentNode.Children = append(parentNode.Children[:deletePos], parentNode.Children[deletePos+1:]...)
	parentNode.isDirty = true

	logger.Debugf("✅ Deleted from parent, remaining keys=%d, children=%d",
		len(parentNode.Keys), len(parentNode.Children))

	// 检查父节点是否需要合并
	if len(parentNode.Keys) < m.minKeys && parentNode.PageNum != m.manager.rootPage {
		logger.Debugf("⚠️ Parent node %d has too few keys (%d < %d), needs rebalancing",
			parentNode.PageNum, len(parentNode.Keys), m.minKeys)
		// 这里可以递归调用重平衡逻辑
	}

	// 如果是根节点且只有一个子节点，降低树高
	if parentNode.PageNum == m.manager.rootPage && len(parentNode.Children) == 1 {
		newRoot := parentNode.Children[0]
		m.manager.mutex.Lock()
		m.manager.rootPage = newRoot
		m.manager.mutex.Unlock()
		logger.Debugf("🌲 Tree height reduced, new root=%d", newRoot)
	}

	return nil
}

// CanMerge 检查两个节点是否可以合并
func (m *NodeMerger) CanMerge(node1, node2 *BPlusTreeNode) bool {
	if node1 == nil || node2 == nil {
		return false
	}

	if node1.IsLeaf != node2.IsLeaf {
		return false
	}

	totalKeys := len(node1.Keys) + len(node2.Keys)

	// 非叶子节点合并时需要加上中间键
	if !node1.IsLeaf {
		totalKeys++
	}

	return totalKeys <= m.maxKeys
}

// FindSiblings 查找节点的兄弟节点
func (m *NodeMerger) FindSiblings(ctx context.Context, node *BPlusTreeNode) (leftSibling, rightSibling *BPlusTreeNode, err error) {
	// 需要从父节点中找到当前节点的位置
	// 然后获取其左右兄弟

	// 简化实现：这里需要实现完整的父节点查找逻辑
	// 实际应该在B+树中维护父节点指针或使用栈记录路径

	return nil, nil, fmt.Errorf("not implemented: sibling lookup requires parent tracking")
}

// MergeConfig 合并配置
type MergeConfig struct {
	MinFillFactor   float64 // 最小填充因子
	BorrowThreshold float64 // 借键阈值
	MergeThreshold  float64 // 合并阈值
}

// DefaultMergeConfig 默认合并配置
var DefaultMergeConfig = &MergeConfig{
	MinFillFactor:   0.4,
	BorrowThreshold: 0.5,
	MergeThreshold:  0.3,
}
