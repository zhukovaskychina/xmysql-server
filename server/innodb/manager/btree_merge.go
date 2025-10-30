package manager

import (
	"context"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/logger"
)

// NodeMerger B+树节点合并操作
// 当节点的键数少于最小值时，与兄弟节点合并或从兄弟节点借键
type NodeMerger struct {
	manager           *DefaultBPlusTreeManager
	minKeys           int // 节点最小键数
	maxKeys           int // 节点最大键数
	maxRecursionDepth int // 最大递归深度限制
}

// NewNodeMerger 创建节点合并器
func NewNodeMerger(manager *DefaultBPlusTreeManager, degree int) *NodeMerger {
	return &NodeMerger{
		manager:           manager,
		minKeys:           degree - 1,
		maxKeys:           2*degree - 1,
		maxRecursionDepth: 10, // 最大递归深度10层
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
	return m.deleteFromParentWithDepth(ctx, parentNode, childPage, 0)
}

// deleteFromParentWithDepth 带递归深度的删除父节点方法
func (m *NodeMerger) deleteFromParentWithDepth(ctx context.Context, parentNode *BPlusTreeNode, childPage uint32, depth int) error {
	logger.Debugf("🗑️ [Depth %d] Deleting child %d from parent %d", depth, childPage, parentNode.PageNum)

	// 检查递归深度限制
	if depth >= m.maxRecursionDepth {
		return fmt.Errorf("maximum recursion depth %d exceeded", m.maxRecursionDepth)
	}

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

	logger.Debugf("✅ [Depth %d] Deleted from parent, remaining keys=%d, children=%d",
		depth, len(parentNode.Keys), len(parentNode.Children))

	// 如果是根节点且只有一个子节点，降低树高
	if parentNode.PageNum == m.manager.rootPage {
		if len(parentNode.Children) == 1 {
			newRoot := parentNode.Children[0]
			m.manager.mutex.Lock()
			m.manager.rootPage = newRoot
			m.manager.mutex.Unlock()

			// 降低树高度
			m.manager.decrementTreeHeight()

			logger.Debugf("🌲 [Depth %d] Tree height reduced, new root=%d, tree_height=%d",
				depth, newRoot, m.manager.GetTreeHeight())
		}
		return nil
	}

	// 检查父节点是否需要重平衡
	if len(parentNode.Keys) < m.minKeys {
		logger.Debugf("⚠️ [Depth %d] Parent node %d has too few keys (%d < %d), needs rebalancing",
			depth, parentNode.PageNum, len(parentNode.Keys), m.minKeys)

		// 递归重平衡父节点
		return m.rebalanceAfterMerge(ctx, parentNode, depth+1)
	}

	return nil
}

// rebalanceAfterMerge 合并后的重平衡操作
func (m *NodeMerger) rebalanceAfterMerge(ctx context.Context, node *BPlusTreeNode, depth int) error {
	logger.Debugf("⚖️ [Depth %d] Rebalancing node %d after merge", depth, node.PageNum)

	// 检查递归深度限制
	if depth >= m.maxRecursionDepth {
		return fmt.Errorf("maximum recursion depth %d exceeded", m.maxRecursionDepth)
	}

	// 查找兄弟节点
	leftSibling, rightSibling, err := m.FindSiblings(ctx, node)
	if err != nil {
		return fmt.Errorf("failed to find siblings: %v", err)
	}

	// 获取父节点信息
	parentNode, childIndex, err := m.findParentWithPath(ctx, node.PageNum)
	if err != nil {
		return fmt.Errorf("failed to find parent: %v", err)
	}

	// 先尝试从左兄弟借键
	if leftSibling != nil && len(leftSibling.Keys) > m.minKeys {
		logger.Debugf("⬅️ [Depth %d] Attempting to borrow from left sibling", depth)
		parentKey := parentNode.Keys[childIndex-1]
		newParentKey, err := m.BorrowFromLeftSibling(ctx, node, leftSibling, parentKey)
		if err == nil {
			parentNode.Keys[childIndex-1] = newParentKey
			parentNode.isDirty = true
			logger.Debugf("✅ [Depth %d] Successfully borrowed from left sibling", depth)
			return nil
		}
	}

	// 尝试从右兄弟借键
	if rightSibling != nil && len(rightSibling.Keys) > m.minKeys {
		logger.Debugf("➡️ [Depth %d] Attempting to borrow from right sibling", depth)
		parentKey := parentNode.Keys[childIndex]
		newParentKey, err := m.BorrowFromRightSibling(ctx, node, rightSibling, parentKey)
		if err == nil {
			parentNode.Keys[childIndex] = newParentKey
			parentNode.isDirty = true
			logger.Debugf("✅ [Depth %d] Successfully borrowed from right sibling", depth)
			return nil
		}
	}

	// 无法借键，尝试合并
	if leftSibling != nil && m.CanMerge(node, leftSibling) {
		logger.Debugf("🔗 [Depth %d] Merging with left sibling", depth)
		if node.IsLeaf {
			_, err = m.MergeLeafNodes(ctx, leftSibling, node)
		} else {
			middleKey := parentNode.Keys[childIndex-1]
			_, err = m.MergeNonLeafNodes(ctx, leftSibling, node, middleKey)
		}
		if err == nil {
			// 从父节点删除被合并的节点
			return m.deleteFromParentWithDepth(ctx, parentNode, node.PageNum, depth)
		}
	} else if rightSibling != nil && m.CanMerge(node, rightSibling) {
		logger.Debugf("🔗 [Depth %d] Merging with right sibling", depth)
		if node.IsLeaf {
			_, err = m.MergeLeafNodes(ctx, node, rightSibling)
		} else {
			middleKey := parentNode.Keys[childIndex]
			_, err = m.MergeNonLeafNodes(ctx, node, rightSibling, middleKey)
		}
		if err == nil {
			// 从父节点删除被合并的节点
			return m.deleteFromParentWithDepth(ctx, parentNode, rightSibling.PageNum, depth)
		}
	}

	// 如果无法重平衡，记录警告但不失败
	logger.Warnf("⚠️ [Depth %d] Unable to rebalance node %d, but continuing", depth, node.PageNum)
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
	logger.Debugf("🔍 Finding siblings for node %d", node.PageNum)

	// 如果是根节点，没有兄弟
	if node.PageNum == m.manager.rootPage {
		logger.Debugf("⚠️ Node %d is root, no siblings", node.PageNum)
		return nil, nil, nil
	}

	// 查找父节点和当前节点的位置
	parentNode, childIndex, err := m.findParentWithPath(ctx, node.PageNum)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find parent: %v", err)
	}

	logger.Debugf("🔍 Found parent node %d, child_index=%d", parentNode.PageNum, childIndex)

	// 获取左兄弟
	if childIndex > 0 {
		leftPage := parentNode.Children[childIndex-1]
		leftSibling, err = m.manager.getNode(ctx, leftPage)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get left sibling: %v", err)
		}
		logger.Debugf("⬅️ Found left sibling: page=%d, keys=%d", leftSibling.PageNum, len(leftSibling.Keys))
	}

	// 获取右兄弟
	if childIndex < len(parentNode.Children)-1 {
		rightPage := parentNode.Children[childIndex+1]
		rightSibling, err = m.manager.getNode(ctx, rightPage)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get right sibling: %v", err)
		}
		logger.Debugf("➡️ Found right sibling: page=%d, keys=%d", rightSibling.PageNum, len(rightSibling.Keys))
	}

	return leftSibling, rightSibling, nil
}

// findParentWithPath 查找父节点并返回子节点在父节点中的索引
func (m *NodeMerger) findParentWithPath(ctx context.Context, childPage uint32) (*BPlusTreeNode, int, error) {
	// 从根节点开始搜索
	return m.findParentRecursive(ctx, m.manager.rootPage, childPage, 0)
}

// findParentRecursive 递归查找父节点
func (m *NodeMerger) findParentRecursive(ctx context.Context, currentPage, targetPage uint32, depth int) (*BPlusTreeNode, int, error) {
	// 检查递归深度限制
	if depth >= m.maxRecursionDepth {
		return nil, -1, fmt.Errorf("maximum recursion depth %d exceeded", m.maxRecursionDepth)
	}

	node, err := m.manager.getNode(ctx, currentPage)
	if err != nil {
		return nil, -1, err
	}

	if node.IsLeaf {
		return nil, -1, fmt.Errorf("target page %d not found", targetPage)
	}

	// 在子节点中查找目标
	for i, childPage := range node.Children {
		if childPage == targetPage {
			// 找到目标节点，返回父节点和索引
			return node, i, nil
		}
	}

	// 在子树中递归查找
	for _, childPage := range node.Children {
		result, index, err := m.findParentRecursive(ctx, childPage, targetPage, depth+1)
		if err == nil {
			return result, index, nil
		}
	}

	return nil, -1, fmt.Errorf("parent of page %d not found", targetPage)
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

// shouldBorrow 判断是否应该借键
func (m *NodeMerger) shouldBorrow(node *BPlusTreeNode, config *MergeConfig) bool {
	if config == nil {
		config = DefaultMergeConfig
	}
	fillFactor := float64(len(node.Keys)) / float64(m.maxKeys)
	return fillFactor < config.BorrowThreshold && len(node.Keys) >= m.minKeys
}

// shouldMerge 判断是否应该合并
func (m *NodeMerger) shouldMerge(node *BPlusTreeNode, config *MergeConfig) bool {
	if config == nil {
		config = DefaultMergeConfig
	}
	fillFactor := float64(len(node.Keys)) / float64(m.maxKeys)
	return fillFactor < config.MergeThreshold || len(node.Keys) < m.minKeys
}

// Rebalance 重平衡节点（公开API）
func (m *NodeMerger) Rebalance(ctx context.Context, node *BPlusTreeNode) error {
	return m.rebalanceAfterMerge(ctx, node, 0)
}
