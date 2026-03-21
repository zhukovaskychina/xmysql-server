package manager

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
)

// SplitNode B+树节点分裂操作
// 当节点满时，将节点分裂为两个节点，保持B+树平衡性
type NodeSplitter struct {
	manager           *DefaultBPlusTreeManager
	minKeys           int     // 节点最小键数（通常为度数-1）
	maxKeys           int     // 节点最大键数（通常为2*度数-1）
	splitRatio        float64 // 分裂比例（左节点占比）
	maxRecursionDepth int     // 最大递归深度限制
}

// NewNodeSplitter 创建节点分裂器
func NewNodeSplitter(manager *DefaultBPlusTreeManager, degree int) *NodeSplitter {
	return &NodeSplitter{
		manager:           manager,
		minKeys:           degree - 1,
		maxKeys:           2*degree - 1,
		splitRatio:        0.5, // 默认50/50分裂
		maxRecursionDepth: 10,  // 最大递归深度10层
	}
}

// SetSplitRatio 设置分裂比例
// ratio: 左节点占比，范围[0.4, 0.6]
// 返回: 是否设置成功
func (s *NodeSplitter) SetSplitRatio(ratio float64) bool {
	if ratio < 0.4 || ratio > 0.6 {
		logger.Debugf("⚠️ Invalid split ratio %.2f, must be in [0.4, 0.6]", ratio)
		return false
	}
	s.splitRatio = ratio
	logger.Debugf("✅ Split ratio set to %.2f", ratio)
	return true
}

// GetSplitRatio 获取当前分裂比例
func (s *NodeSplitter) GetSplitRatio() float64 {
	return s.splitRatio
}

// SplitLeafNode 分裂叶子节点
// 返回：新节点的页号和提升到父节点的键
func (s *NodeSplitter) SplitLeafNode(ctx context.Context, node *BPlusTreeNode) (newPageNo uint32, middleKey interface{}, err error) {
	if !node.IsLeaf {
		return 0, nil, fmt.Errorf("node %d is not a leaf node", node.PageNum)
	}

	// 节点已满（keys >= maxKeys）时需分裂，以便插入新键
	if len(node.Keys) < s.maxKeys {
		return 0, nil, fmt.Errorf("node %d does not need splitting (keys=%d, max=%d)",
			node.PageNum, len(node.Keys), s.maxKeys)
	}

	logger.Debugf("🌳 Splitting leaf node %d with %d keys", node.PageNum, len(node.Keys))

	// 计算分裂点
	splitPoint := int(float64(len(node.Keys)) * s.splitRatio)
	if splitPoint < s.minKeys {
		splitPoint = s.minKeys
	}
	if splitPoint > len(node.Keys)-s.minKeys {
		splitPoint = len(node.Keys) - s.minKeys
	}

	// 分配新页面
	newPageNo, err = s.manager.allocateNewPage(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to allocate new page: %v", err)
	}

	// 创建新叶子节点（右节点）
	newNode := &BPlusTreeNode{
		PageNum:  newPageNo,
		IsLeaf:   true,
		Keys:     make([]interface{}, len(node.Keys)-splitPoint),
		Records:  make([]uint32, len(node.Records)-splitPoint),
		NextLeaf: node.NextLeaf, // 新节点指向原节点的下一个节点
		isDirty:  true,
	}

	// 复制右半部分数据到新节点
	copy(newNode.Keys, node.Keys[splitPoint:])
	copy(newNode.Records, node.Records[splitPoint:])

	// 截断原节点（左节点）
	node.Keys = node.Keys[:splitPoint]
	node.Records = node.Records[:splitPoint]
	node.NextLeaf = newPageNo // 原节点指向新节点
	node.isDirty = true

	// 提升中间键（新节点的第一个键）
	middleKey = newNode.Keys[0]

	// 将新节点加入缓存
	s.manager.mutex.Lock()
	s.manager.nodeCache[newPageNo] = newNode
	s.manager.mutex.Unlock()

	// 标记脏页并立即刷新（保证持久化）
	if err := s.flushDirtyPages(ctx, []uint32{node.PageNum, newPageNo}); err != nil {
		logger.Warnf("⚠️ Failed to flush dirty pages after leaf split: %v", err)
	}

	logger.Debugf("✅ Leaf split complete: left_node=%d (%d keys), right_node=%d (%d keys), middle_key=%v",
		node.PageNum, len(node.Keys), newPageNo, len(newNode.Keys), middleKey)

	return newPageNo, middleKey, nil
}

// SplitNonLeafNode 分裂非叶子节点
// 返回：新节点的页号和提升到父节点的键
func (s *NodeSplitter) SplitNonLeafNode(ctx context.Context, node *BPlusTreeNode) (newPageNo uint32, middleKey interface{}, err error) {
	if node.IsLeaf {
		return 0, nil, fmt.Errorf("node %d is a leaf node", node.PageNum)
	}

	// 内部节点已满时需分裂
	if len(node.Keys) < s.maxKeys {
		return 0, nil, fmt.Errorf("node %d does not need splitting (keys=%d, max=%d)",
			node.PageNum, len(node.Keys), s.maxKeys)
	}

	logger.Debugf("🌳 Splitting non-leaf node %d with %d keys", node.PageNum, len(node.Keys))

	// 计算分裂点（中间位置）
	splitPoint := len(node.Keys) / 2

	// 分配新页面
	newPageNo, err = s.manager.allocateNewPage(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to allocate new page: %v", err)
	}

	// 提升的中间键
	middleKey = node.Keys[splitPoint]

	// 创建新非叶子节点（右节点）
	newNode := &BPlusTreeNode{
		PageNum:  newPageNo,
		IsLeaf:   false,
		Keys:     make([]interface{}, len(node.Keys)-splitPoint-1), // 不包括中间键
		Children: make([]uint32, len(node.Children)-splitPoint-1),
		isDirty:  true,
	}

	// 复制右半部分数据到新节点（不包括中间键）
	copy(newNode.Keys, node.Keys[splitPoint+1:])
	copy(newNode.Children, node.Children[splitPoint+1:])

	// 截断原节点（左节点）
	node.Keys = node.Keys[:splitPoint]
	node.Children = node.Children[:splitPoint+1] // 保留splitPoint+1个子节点
	node.isDirty = true

	// 将新节点加入缓存
	s.manager.mutex.Lock()
	s.manager.nodeCache[newPageNo] = newNode
	s.manager.mutex.Unlock()

	// 标记脏页并立即刷新（保证持久化）
	if err := s.flushDirtyPages(ctx, []uint32{node.PageNum, newPageNo}); err != nil {
		logger.Warnf("⚠️ Failed to flush dirty pages after non-leaf split: %v", err)
	}

	logger.Debugf("✅ Non-leaf split complete: left_node=%d (%d keys), right_node=%d (%d keys), middle_key=%v",
		node.PageNum, len(node.Keys), newPageNo, len(newNode.Keys), middleKey)

	return newPageNo, middleKey, nil
}

// InsertIntoParent 将分裂产生的新键插入父节点
func (s *NodeSplitter) InsertIntoParent(ctx context.Context, leftPage, rightPage uint32, middleKey interface{}) error {
	return s.insertIntoParentWithDepth(ctx, leftPage, rightPage, middleKey, 0)
}

// insertIntoParentWithDepth 带递归深度的插入父节点方法
func (s *NodeSplitter) insertIntoParentWithDepth(ctx context.Context, leftPage, rightPage uint32, middleKey interface{}, depth int) error {
	logger.Debugf("📌 [Depth %d] Inserting middle_key=%v into parent (left=%d, right=%d)", depth, middleKey, leftPage, rightPage)

	// 检查递归深度限制
	if depth >= s.maxRecursionDepth {
		return fmt.Errorf("maximum recursion depth %d exceeded", s.maxRecursionDepth)
	}

	// 如果分裂的是根节点，创建新根
	if leftPage == s.manager.rootPage {
		logger.Debugf("🌲 [Depth %d] Splitting root node, creating new root", depth)
		return s.createNewRoot(ctx, leftPage, rightPage, middleKey)
	}

	// 找到父节点
	parentNode, err := s.findParentNode(ctx, leftPage)
	if err != nil {
		return fmt.Errorf("failed to find parent node: %v", err)
	}

	logger.Debugf("🔍 [Depth %d] Found parent node %d with %d keys", depth, parentNode.PageNum, len(parentNode.Keys))

	// 在父节点中插入新键和子节点指针
	insertPos := s.findInsertPosition(parentNode.Keys, middleKey)

	// 插入键
	parentNode.Keys = append(parentNode.Keys[:insertPos], append([]interface{}{middleKey}, parentNode.Keys[insertPos:]...)...)

	// 插入子节点指针
	parentNode.Children = append(parentNode.Children[:insertPos+1], append([]uint32{rightPage}, parentNode.Children[insertPos+1:]...)...)
	parentNode.isDirty = true

	// 检查父节点是否需要分裂
	if len(parentNode.Keys) > s.maxKeys {
		logger.Debugf("⚠️ [Depth %d] Parent node %d is full (%d keys > %d), needs splitting",
			depth, parentNode.PageNum, len(parentNode.Keys), s.maxKeys)

		newParentPage, newMiddleKey, err := s.SplitNonLeafNode(ctx, parentNode)
		if err != nil {
			return fmt.Errorf("failed to split parent node: %v", err)
		}

		logger.Debugf("➡️ [Depth %d] Recursively inserting into grandparent (depth %d)", depth, depth+1)
		// 递归向上插入（增加深度）
		return s.insertIntoParentWithDepth(ctx, parentNode.PageNum, newParentPage, newMiddleKey, depth+1)
	}

	logger.Debugf("✅ [Depth %d] Inserted into parent node %d successfully", depth, parentNode.PageNum)
	return nil
}

// createNewRoot 创建新的根节点
func (s *NodeSplitter) createNewRoot(ctx context.Context, leftPage, rightPage uint32, middleKey interface{}) error {
	logger.Debugf("🌲 Creating new root with middle_key=%v", middleKey)

	// 分配新根页面
	newRootPage, err := s.manager.allocateNewPage(ctx)
	if err != nil {
		return fmt.Errorf("failed to allocate new root page: %v", err)
	}

	// 创建新根节点
	newRoot := &BPlusTreeNode{
		PageNum:  newRootPage,
		IsLeaf:   false,
		Keys:     []interface{}{middleKey},
		Children: []uint32{leftPage, rightPage},
		isDirty:  true,
	}

	// 更新缓存和根页号
	s.manager.mutex.Lock()
	s.manager.nodeCache[newRootPage] = newRoot
	oldRootPage := s.manager.rootPage
	s.manager.rootPage = newRootPage
	s.manager.mutex.Unlock()

	// 增加树高度
	s.manager.incrementTreeHeight()

	// 刷新新根节点
	if err := s.flushDirtyPages(ctx, []uint32{newRootPage}); err != nil {
		logger.Warnf("⚠️ Failed to flush new root page: %v", err)
	}

	logger.Debugf("✅ New root created: page=%d (old_root=%d), left_child=%d, right_child=%d, tree_height=%d",
		newRootPage, oldRootPage, leftPage, rightPage, s.manager.GetTreeHeight())

	return nil
}

// findParentNode 查找节点的父节点
func (s *NodeSplitter) findParentNode(ctx context.Context, childPage uint32) (*BPlusTreeNode, error) {
	// 从根节点开始搜索
	return s.findParentRecursive(ctx, s.manager.rootPage, childPage, nil)
}

// findParentRecursive 递归查找父节点
func (s *NodeSplitter) findParentRecursive(ctx context.Context, currentPage, targetPage uint32, parent *BPlusTreeNode) (*BPlusTreeNode, error) {
	if currentPage == targetPage {
		return parent, nil
	}

	node, err := s.manager.getNode(ctx, currentPage)
	if err != nil {
		return nil, err
	}

	if node.IsLeaf {
		return nil, fmt.Errorf("target page %d not found", targetPage)
	}

	// 在子节点中查找
	for _, childPage := range node.Children {
		if childPage == targetPage {
			return node, nil
		}

		// 递归搜索子树
		result, err := s.findParentRecursive(ctx, childPage, targetPage, node)
		if err == nil {
			return result, nil
		}
	}

	return nil, fmt.Errorf("parent of page %d not found", targetPage)
}

// findInsertPosition 查找插入位置
func (s *NodeSplitter) findInsertPosition(keys []interface{}, key interface{}) int {
	// 二分查找插入位置
	left, right := 0, len(keys)

	for left < right {
		mid := (left + right) / 2
		if s.compareKeys(keys[mid], key) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left
}

// compareKeys 比较两个键
func (s *NodeSplitter) compareKeys(a, b interface{}) int {
	// 简化实现：假设键都是可比较类型
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// SplitConfig 分裂配置
type SplitConfig struct {
	MinKeysRatio    float64 // 最小键数比例（相对于度数）
	SplitRatio      float64 // 分裂比例
	AllowUnbalanced bool    // 是否允许不平衡分裂
}

// DefaultSplitConfig 默认分裂配置
var DefaultSplitConfig = &SplitConfig{
	MinKeysRatio:    0.5,
	SplitRatio:      0.5,
	AllowUnbalanced: false,
}

// flushDirtyPages 批量刷新脏页到磁盘
func (s *NodeSplitter) flushDirtyPages(ctx context.Context, pageNums []uint32) error {
	if s.manager.bufferPoolManager == nil {
		return fmt.Errorf("buffer pool manager is nil")
	}

	logger.Debugf("💾 Flushing %d dirty pages to disk", len(pageNums))

	for _, pageNum := range pageNums {
		if err := s.manager.bufferPoolManager.FlushPage(s.manager.spaceId, pageNum); err != nil {
			logger.Errorf("❌ Failed to flush page %d: %v", pageNum, err)
			return fmt.Errorf("failed to flush page %d: %v", pageNum, err)
		}
		logger.Debugf("✅ Page %d flushed successfully", pageNum)
	}

	logger.Debugf("💾 All %d pages flushed successfully", len(pageNums))
	return nil
}

// FlushAllDirtyPages 刷新所有脏页（用于批量操作后）
func (s *NodeSplitter) FlushAllDirtyPages(ctx context.Context) error {
	s.manager.mutex.RLock()
	var dirtyPages []uint32
	for pageNum, node := range s.manager.nodeCache {
		if node.isDirty {
			dirtyPages = append(dirtyPages, pageNum)
		}
	}
	s.manager.mutex.RUnlock()

	if len(dirtyPages) == 0 {
		logger.Debugf("💾 No dirty pages to flush")
		return nil
	}

	logger.Debugf("💾 Flushing %d dirty pages in batch", len(dirtyPages))
	return s.flushDirtyPages(ctx, dirtyPages)
}
