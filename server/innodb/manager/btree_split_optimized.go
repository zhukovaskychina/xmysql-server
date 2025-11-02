package manager

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"sync"
)

// OptimizedNodeSplitter 优化的B+树节点分裂器
// 主要优化：
// 1. 延迟刷盘 - 批量刷新脏页而非每次分裂都刷盘
// 2. 父节点缓存 - 缓存父子关系避免重复查找
// 3. 预分配空间 - 减少内存分配次数
// 4. 延迟分裂 - 允许节点超过阈值一定比例再分裂
type OptimizedNodeSplitter struct {
	manager           *DefaultBPlusTreeManager
	minKeys           int     // 节点最小键数
	maxKeys           int     // 节点最大键数
	splitRatio        float64 // 分裂比例
	maxRecursionDepth int     // 最大递归深度

	// 优化相关字段
	parentCache      map[uint32]uint32 // 子页号 -> 父页号的缓存
	parentCacheMutex sync.RWMutex      // 父节点缓存锁
	deferredFlush    bool              // 是否延迟刷盘
	dirtyPages       []uint32          // 待刷新的脏页列表
	dirtyPagesMutex  sync.Mutex        // 脏页列表锁
	splitThreshold   float64           // 分裂阈值（相对于maxKeys的比例）
}

// NewOptimizedNodeSplitter 创建优化的节点分裂器
func NewOptimizedNodeSplitter(manager *DefaultBPlusTreeManager, degree int) *OptimizedNodeSplitter {
	return &OptimizedNodeSplitter{
		manager:           manager,
		minKeys:           degree - 1,
		maxKeys:           2*degree - 1,
		splitRatio:        0.5,
		maxRecursionDepth: 10,
		parentCache:       make(map[uint32]uint32),
		deferredFlush:     true, // 默认启用延迟刷盘
		dirtyPages:        make([]uint32, 0, 100),
		splitThreshold:    1.0, // 默认100%满时分裂
	}
}

// SetDeferredFlush 设置是否延迟刷盘
func (s *OptimizedNodeSplitter) SetDeferredFlush(enabled bool) {
	s.deferredFlush = enabled
	logger.Debugf("🔧 Deferred flush %s", map[bool]string{true: "enabled", false: "disabled"}[enabled])
}

// SetSplitThreshold 设置分裂阈值
// threshold: 相对于maxKeys的比例，范围[0.8, 1.2]
// 例如：1.0表示100%满时分裂，1.1表示110%满时分裂（允许超载）
func (s *OptimizedNodeSplitter) SetSplitThreshold(threshold float64) bool {
	if threshold < 0.8 || threshold > 1.2 {
		logger.Debugf("⚠️ Invalid split threshold %.2f, must be in [0.8, 1.2]", threshold)
		return false
	}
	s.splitThreshold = threshold
	logger.Debugf("✅ Split threshold set to %.2f", threshold)
	return true
}

// ShouldSplit 判断节点是否需要分裂
func (s *OptimizedNodeSplitter) ShouldSplit(node *BPlusTreeNode) bool {
	threshold := int(float64(s.maxKeys) * s.splitThreshold)
	return len(node.Keys) > threshold
}

// SplitLeafNode 分裂叶子节点（优化版）
func (s *OptimizedNodeSplitter) SplitLeafNode(ctx context.Context, node *BPlusTreeNode) (newPageNo uint32, middleKey interface{}, err error) {
	if !node.IsLeaf {
		return 0, nil, fmt.Errorf("node %d is not a leaf node", node.PageNum)
	}

	if !s.ShouldSplit(node) {
		return 0, nil, fmt.Errorf("node %d does not need splitting (keys=%d, threshold=%d)",
			node.PageNum, len(node.Keys), int(float64(s.maxKeys)*s.splitThreshold))
	}

	logger.Debugf("🌳 [Optimized] Splitting leaf node %d with %d keys", node.PageNum, len(node.Keys))

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

	// 预分配切片空间（优化：减少内存分配）
	rightKeysLen := len(node.Keys) - splitPoint
	newKeys := make([]interface{}, rightKeysLen)
	newRecords := make([]uint32, rightKeysLen)

	// 复制右半部分数据到新节点
	copy(newKeys, node.Keys[splitPoint:])
	copy(newRecords, node.Records[splitPoint:])

	// 创建新叶子节点（右节点）
	newNode := &BPlusTreeNode{
		PageNum:  newPageNo,
		IsLeaf:   true,
		Keys:     newKeys,
		Records:  newRecords,
		NextLeaf: node.NextLeaf,
		isDirty:  true,
	}

	// 截断原节点（左节点）- 重用原有切片
	node.Keys = node.Keys[:splitPoint]
	node.Records = node.Records[:splitPoint]
	node.NextLeaf = newPageNo
	node.isDirty = true

	// 提升中间键（新节点的第一个键）
	middleKey = newNode.Keys[0]

	// 将新节点加入缓存
	s.manager.mutex.Lock()
	s.manager.nodeCache[newPageNo] = newNode
	s.manager.mutex.Unlock()

	// 记录脏页（延迟刷新）
	s.markDirtyPages([]uint32{node.PageNum, newPageNo})

	logger.Debugf("✅ [Optimized] Leaf split complete: left_node=%d (%d keys), right_node=%d (%d keys), middle_key=%v",
		node.PageNum, len(node.Keys), newPageNo, len(newNode.Keys), middleKey)

	return newPageNo, middleKey, nil
}

// SplitNonLeafNode 分裂非叶子节点（优化版）
func (s *OptimizedNodeSplitter) SplitNonLeafNode(ctx context.Context, node *BPlusTreeNode) (newPageNo uint32, middleKey interface{}, err error) {
	if node.IsLeaf {
		return 0, nil, fmt.Errorf("node %d is a leaf node", node.PageNum)
	}

	if !s.ShouldSplit(node) {
		return 0, nil, fmt.Errorf("node %d does not need splitting (keys=%d, threshold=%d)",
			node.PageNum, len(node.Keys), int(float64(s.maxKeys)*s.splitThreshold))
	}

	logger.Debugf("🌳 [Optimized] Splitting non-leaf node %d with %d keys", node.PageNum, len(node.Keys))

	// 计算分裂点（中间位置）
	splitPoint := len(node.Keys) / 2

	// 分配新页面
	newPageNo, err = s.manager.allocateNewPage(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to allocate new page: %v", err)
	}

	// 提升的中间键
	middleKey = node.Keys[splitPoint]

	// 预分配切片空间（优化：减少内存分配）
	rightKeysLen := len(node.Keys) - splitPoint - 1
	newKeys := make([]interface{}, rightKeysLen)
	newChildren := make([]uint32, rightKeysLen+1)

	// 复制右半部分数据到新节点（不包括中间键）
	copy(newKeys, node.Keys[splitPoint+1:])
	copy(newChildren, node.Children[splitPoint+1:])

	// 创建新非叶子节点（右节点）
	newNode := &BPlusTreeNode{
		PageNum:  newPageNo,
		IsLeaf:   false,
		Keys:     newKeys,
		Children: newChildren,
		isDirty:  true,
	}

	// 截断原节点（左节点）
	node.Keys = node.Keys[:splitPoint]
	node.Children = node.Children[:splitPoint+1]
	node.isDirty = true

	// 将新节点加入缓存
	s.manager.mutex.Lock()
	s.manager.nodeCache[newPageNo] = newNode
	s.manager.mutex.Unlock()

	// 更新父节点缓存（优化：缓存新节点的子节点关系）
	s.updateParentCache(newPageNo, newNode.Children)

	// 记录脏页（延迟刷新）
	s.markDirtyPages([]uint32{node.PageNum, newPageNo})

	logger.Debugf("✅ [Optimized] Non-leaf split complete: left_node=%d (%d keys), right_node=%d (%d keys), middle_key=%v",
		node.PageNum, len(node.Keys), newPageNo, len(newNode.Keys), middleKey)

	return newPageNo, middleKey, nil
}

// InsertIntoParent 将分裂产生的新键插入父节点（优化版）
func (s *OptimizedNodeSplitter) InsertIntoParent(ctx context.Context, leftPage, rightPage uint32, middleKey interface{}) error {
	return s.insertIntoParentWithDepth(ctx, leftPage, rightPage, middleKey, 0)
}

// insertIntoParentWithDepth 带递归深度的插入父节点方法（优化版）
func (s *OptimizedNodeSplitter) insertIntoParentWithDepth(ctx context.Context, leftPage, rightPage uint32, middleKey interface{}, depth int) error {
	logger.Debugf("📌 [Optimized Depth %d] Inserting middle_key=%v into parent (left=%d, right=%d)", depth, middleKey, leftPage, rightPage)

	// 检查递归深度限制
	if depth >= s.maxRecursionDepth {
		return fmt.Errorf("maximum recursion depth %d exceeded", s.maxRecursionDepth)
	}

	// 如果分裂的是根节点，创建新根
	if leftPage == s.manager.rootPage {
		logger.Debugf("🌲 [Optimized Depth %d] Splitting root node, creating new root", depth)
		return s.createNewRoot(ctx, leftPage, rightPage, middleKey)
	}

	// 优化：先尝试从缓存查找父节点
	parentNode, err := s.findParentNodeOptimized(ctx, leftPage)
	if err != nil {
		return fmt.Errorf("failed to find parent node: %v", err)
	}

	logger.Debugf("🔍 [Optimized Depth %d] Found parent node %d with %d keys", depth, parentNode.PageNum, len(parentNode.Keys))

	// 在父节点中插入新键和子节点指针（优化：减少内存分配）
	insertPos := s.findInsertPosition(parentNode.Keys, middleKey)

	// 优化：使用预分配的切片避免多次分配
	newKeys := make([]interface{}, len(parentNode.Keys)+1)
	copy(newKeys[:insertPos], parentNode.Keys[:insertPos])
	newKeys[insertPos] = middleKey
	copy(newKeys[insertPos+1:], parentNode.Keys[insertPos:])
	parentNode.Keys = newKeys

	newChildren := make([]uint32, len(parentNode.Children)+1)
	copy(newChildren[:insertPos+1], parentNode.Children[:insertPos+1])
	newChildren[insertPos+1] = rightPage
	copy(newChildren[insertPos+2:], parentNode.Children[insertPos+1:])
	parentNode.Children = newChildren

	parentNode.isDirty = true

	// 更新父节点缓存
	s.updateParentCache(parentNode.PageNum, parentNode.Children)

	// 检查父节点是否需要分裂
	if s.ShouldSplit(parentNode) {
		logger.Debugf("⚠️ [Optimized Depth %d] Parent node %d is full (%d keys > threshold), needs splitting",
			depth, parentNode.PageNum, len(parentNode.Keys))

		newParentPage, newMiddleKey, err := s.SplitNonLeafNode(ctx, parentNode)
		if err != nil {
			return fmt.Errorf("failed to split parent node: %v", err)
		}

		logger.Debugf("➡️ [Optimized Depth %d] Recursively inserting into grandparent (depth %d)", depth, depth+1)
		// 递归向上插入（增加深度）
		return s.insertIntoParentWithDepth(ctx, parentNode.PageNum, newParentPage, newMiddleKey, depth+1)
	}

	logger.Debugf("✅ [Optimized Depth %d] Inserted into parent node %d successfully", depth, parentNode.PageNum)
	return nil
}

// createNewRoot 创建新的根节点（优化版）
func (s *OptimizedNodeSplitter) createNewRoot(ctx context.Context, leftPage, rightPage uint32, middleKey interface{}) error {
	logger.Debugf("🌲 [Optimized] Creating new root with middle_key=%v", middleKey)

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

	// 更新父节点缓存
	s.updateParentCache(newRootPage, newRoot.Children)

	// 记录脏页（延迟刷新）
	s.markDirtyPages([]uint32{newRootPage})

	logger.Debugf("✅ [Optimized] New root created: page=%d (old_root=%d), left_child=%d, right_child=%d, tree_height=%d",
		newRootPage, oldRootPage, leftPage, rightPage, s.manager.GetTreeHeight())

	return nil
}

// findParentNodeOptimized 优化的查找父节点方法
// 优先使用缓存，缓存未命中时才进行树遍历
func (s *OptimizedNodeSplitter) findParentNodeOptimized(ctx context.Context, childPage uint32) (*BPlusTreeNode, error) {
	// 优化1：先尝试从缓存获取父节点
	s.parentCacheMutex.RLock()
	parentPage, found := s.parentCache[childPage]
	s.parentCacheMutex.RUnlock()

	if found {
		logger.Debugf("🎯 [Cache Hit] Found parent %d for child %d in cache", parentPage, childPage)
		return s.manager.getNode(ctx, parentPage)
	}

	// 缓存未命中，从根节点开始搜索
	logger.Debugf("🔍 [Cache Miss] Searching parent for child %d from root", childPage)
	parentNode, err := s.findParentRecursive(ctx, s.manager.rootPage, childPage, nil)
	if err != nil {
		return nil, err
	}

	// 更新缓存
	s.parentCacheMutex.Lock()
	s.parentCache[childPage] = parentNode.PageNum
	s.parentCacheMutex.Unlock()

	return parentNode, nil
}

// findParentRecursive 递归查找父节点
func (s *OptimizedNodeSplitter) findParentRecursive(ctx context.Context, currentPage, targetPage uint32, parent *BPlusTreeNode) (*BPlusTreeNode, error) {
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

// findInsertPosition 查找插入位置（二分查找）
func (s *OptimizedNodeSplitter) findInsertPosition(keys []interface{}, key interface{}) int {
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
func (s *OptimizedNodeSplitter) compareKeys(a, b interface{}) int {
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// updateParentCache 更新父节点缓存
func (s *OptimizedNodeSplitter) updateParentCache(parentPage uint32, children []uint32) {
	s.parentCacheMutex.Lock()
	defer s.parentCacheMutex.Unlock()

	for _, childPage := range children {
		s.parentCache[childPage] = parentPage
	}
}

// ClearParentCache 清空父节点缓存
func (s *OptimizedNodeSplitter) ClearParentCache() {
	s.parentCacheMutex.Lock()
	defer s.parentCacheMutex.Unlock()

	s.parentCache = make(map[uint32]uint32)
	logger.Debugf("🧹 Parent cache cleared")
}

// markDirtyPages 标记脏页（延迟刷新）
func (s *OptimizedNodeSplitter) markDirtyPages(pages []uint32) {
	if !s.deferredFlush {
		// 如果未启用延迟刷盘，立即刷新
		ctx := context.Background()
		if err := s.flushDirtyPages(ctx, pages); err != nil {
			logger.Warnf("⚠️ Failed to flush dirty pages: %v", err)
		}
		return
	}

	// 延迟刷盘：添加到待刷新列表
	s.dirtyPagesMutex.Lock()
	s.dirtyPages = append(s.dirtyPages, pages...)
	s.dirtyPagesMutex.Unlock()

	logger.Debugf("📝 Marked %d pages as dirty (total: %d)", len(pages), len(s.dirtyPages))
}

// FlushDirtyPages 刷新所有待刷新的脏页
func (s *OptimizedNodeSplitter) FlushDirtyPages(ctx context.Context) error {
	s.dirtyPagesMutex.Lock()
	pages := make([]uint32, len(s.dirtyPages))
	copy(pages, s.dirtyPages)
	s.dirtyPages = s.dirtyPages[:0] // 清空列表
	s.dirtyPagesMutex.Unlock()

	if len(pages) == 0 {
		logger.Debugf("💾 No dirty pages to flush")
		return nil
	}

	logger.Debugf("💾 Flushing %d dirty pages in batch", len(pages))
	return s.flushDirtyPages(ctx, pages)
}

// flushDirtyPages 批量刷新脏页到磁盘
func (s *OptimizedNodeSplitter) flushDirtyPages(ctx context.Context, pageNums []uint32) error {
	if s.manager.bufferPoolManager == nil {
		return fmt.Errorf("buffer pool manager is nil")
	}

	logger.Debugf("💾 Flushing %d dirty pages to disk", len(pageNums))

	for _, pageNum := range pageNums {
		if err := s.manager.bufferPoolManager.FlushPage(s.manager.spaceId, pageNum); err != nil {
			logger.Errorf("❌ Failed to flush page %d: %v", pageNum, err)
			return fmt.Errorf("failed to flush page %d: %v", pageNum, err)
		}
	}

	logger.Debugf("💾 All %d pages flushed successfully", len(pageNums))
	return nil
}

// GetStatistics 获取分裂器统计信息
func (s *OptimizedNodeSplitter) GetStatistics() map[string]interface{} {
	s.parentCacheMutex.RLock()
	cacheSize := len(s.parentCache)
	s.parentCacheMutex.RUnlock()

	s.dirtyPagesMutex.Lock()
	dirtyCount := len(s.dirtyPages)
	s.dirtyPagesMutex.Unlock()

	return map[string]interface{}{
		"parent_cache_size":   cacheSize,
		"dirty_pages_count":   dirtyCount,
		"deferred_flush":      s.deferredFlush,
		"split_threshold":     s.splitThreshold,
		"split_ratio":         s.splitRatio,
		"max_recursion_depth": s.maxRecursionDepth,
	}
}
