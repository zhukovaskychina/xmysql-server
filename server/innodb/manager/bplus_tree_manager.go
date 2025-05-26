package manager

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/page"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// BPlusTreeNode B+树节点
type BPlusTreeNode struct {
	PageNum  uint32
	IsLeaf   bool
	Keys     []interface{}
	Children []uint32 // 非叶子节点的子节点页号
	Records  []uint32 // 叶子节点的记录位置
	NextLeaf uint32   // 叶子节点链表
	isDirty  bool     // 是否为脏节点
}

// DefaultBPlusTreeManager B+树管理器默认实现
type DefaultBPlusTreeManager struct {
	spaceId           uint32
	rootPage          uint32
	bufferPoolManager *OptimizedBufferPoolManager
	mutex             sync.RWMutex
	config            BPlusTreeConfig
	stats             *BPlusTreeStats

	// 缓存当前打开的节点
	nodeCache map[uint32]*BPlusTreeNode

	// 记录节点访问时间，用于LRU
	lastAccess map[uint32]time.Time
}

// BPlusTreeConfig B+树配置
type BPlusTreeConfig struct {
	// 缓存大小限制
	MaxCacheSize uint32
	// 脏节点刷新阈值
	DirtyThreshold float64
	// 缓存淘汰策略
	EvictionPolicy string
}

// DefaultBPlusTreeConfig 默认配置
var DefaultBPlusTreeConfig = BPlusTreeConfig{
	MaxCacheSize:   1000,
	DirtyThreshold: 0.7,
	EvictionPolicy: "LRU",
}

// NewBPlusTreeManager 创建B+树管理器
func NewBPlusTreeManager(bpm *OptimizedBufferPoolManager, config *BPlusTreeConfig) *DefaultBPlusTreeManager {
	if config == nil {
		config = &DefaultBPlusTreeConfig
	}

	btm := &DefaultBPlusTreeManager{
		bufferPoolManager: bpm,
		nodeCache:         make(map[uint32]*BPlusTreeNode),
		config:            *config,
		stats: &BPlusTreeStats{
			cacheHits:   0,
			cacheMisses: 0,
			dirtyNodes:  0,
		},
		lastAccess: make(map[uint32]time.Time),
	}

	// 启动后台清理任务
	go btm.backgroundCleaner()

	return btm
}

// BPlusTreeStats B+树统计信息
type BPlusTreeStats struct {
	cacheHits   uint64
	cacheMisses uint64
	dirtyNodes  uint32
}

// backgroundCleaner 后台清理任务
func (m *DefaultBPlusTreeManager) backgroundCleaner() {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanCache()
		}
	}
}

// cleanCache 清理缓存
func (m *DefaultBPlusTreeManager) cleanCache() {
	// 先收集需要处理的信息，避免长时间持锁
	var needFlush bool
	var needEvict bool
	var dirtyNodes []*BPlusTreeNode

	m.mutex.RLock()
	dirtyRatio := float64(m.stats.dirtyNodes) / float64(len(m.nodeCache))
	needFlush = dirtyRatio > m.config.DirtyThreshold
	needEvict = uint32(len(m.nodeCache)) > m.config.MaxCacheSize

	// 收集脏节点信息
	if needFlush {
		for _, node := range m.nodeCache {
			if node.isDirty {
				dirtyNodes = append(dirtyNodes, node)
			}
		}
	}
	m.mutex.RUnlock()

	// 在释放锁后执行耗时操作
	if needFlush {
		m.flushDirtyNodesAsync(dirtyNodes)
	}

	if needEvict {
		m.evictNodesAsync()
	}
}

// flushDirtyNodes 刷新脏节点（保留原方法供内部使用）
func (m *DefaultBPlusTreeManager) flushDirtyNodes() {
	for _, node := range m.nodeCache {
		if node.isDirty {
			if err := m.flushNode(node); err != nil {
				// 记录错误但继续处理其他节点
				fmt.Printf("Error flushing node %d: %v\n", node.PageNum, err)
			}
		}
	}
}

// flushDirtyNodesAsync 异步刷新脏节点（不持锁）
func (m *DefaultBPlusTreeManager) flushDirtyNodesAsync(dirtyNodes []*BPlusTreeNode) {
	for _, node := range dirtyNodes {
		if err := m.flushNode(node); err != nil {
			// 记录错误但继续处理其他节点
			fmt.Printf("Error flushing node %d: %v\n", node.PageNum, err)
		} else {
			// 刷新成功后，在锁内更新节点状态
			m.mutex.Lock()
			if cachedNode, ok := m.nodeCache[node.PageNum]; ok && cachedNode == node {
				cachedNode.isDirty = false
			}
			m.mutex.Unlock()
		}
	}
}

// evictNodes 淘汰节点
func (m *DefaultBPlusTreeManager) evictNodes() {
	m.evictLRU()
}

// evictNodesAsync 异步淘汰节点（不持锁）
func (m *DefaultBPlusTreeManager) evictNodesAsync() {
	// 收集淘汰候选节点
	var candidates []struct {
		pageNum    uint32
		lastAccess time.Time
		node       *BPlusTreeNode
	}

	m.mutex.RLock()
	targetSize := uint32(float64(m.config.MaxCacheSize) * 0.8)
	if uint32(len(m.nodeCache)) <= targetSize {
		m.mutex.RUnlock()
		return
	}

	for pageNum, lastAccess := range m.lastAccess {
		if node, ok := m.nodeCache[pageNum]; ok {
			candidates = append(candidates, struct {
				pageNum    uint32
				lastAccess time.Time
				node       *BPlusTreeNode
			}{pageNum, lastAccess, node})
		}
	}
	m.mutex.RUnlock()

	// 按访问时间排序
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastAccess.Before(candidates[j].lastAccess)
	})

	// 异步处理淘汰
	for _, candidate := range candidates {
		m.mutex.RLock()
		currentSize := uint32(len(m.nodeCache))
		m.mutex.RUnlock()

		if currentSize <= targetSize {
			break
		}

		// 如果是脏节点，先刷新
		if candidate.node.isDirty {
			if err := m.flushNode(candidate.node); err != nil {
				continue // 刷新失败，跳过
			}
		}

		// 从缓存中删除
		m.mutex.Lock()
		delete(m.nodeCache, candidate.pageNum)
		delete(m.lastAccess, candidate.pageNum)
		m.mutex.Unlock()
	}
}

// evictLRU 执行 LRU 淘汰
func (m *DefaultBPlusTreeManager) evictLRU() {
	// 计算目标缓存大小
	targetSize := uint32(float64(m.config.MaxCacheSize) * 0.8)

	// 收集所有节点的访问时间
	type nodeAccess struct {
		pageNum    uint32
		lastAccess time.Time
	}
	nodeAccesses := make([]nodeAccess, 0, len(m.nodeCache))

	for pageNum, lastAccess := range m.lastAccess {
		nodeAccesses = append(nodeAccesses, nodeAccess{pageNum, lastAccess})
	}

	// 按访问时间排序
	sort.Slice(nodeAccesses, func(i, j int) bool {
		return nodeAccesses[i].lastAccess.Before(nodeAccesses[j].lastAccess)
	})

	// 淘汰最早访问的节点，直到达到目标大小
	for _, na := range nodeAccesses {
		if uint32(len(m.nodeCache)) <= targetSize {
			break
		}

		// 检查节点是否脏
		node := m.nodeCache[na.pageNum]
		if node.isDirty {
			// 如果是脏节点，需要先刷新到磁盘
			if err := m.flushNode(node); err != nil {
				// 如果刷新失败，跳过该节点
				continue
			}
		}

		// 从缓存中删除
		delete(m.nodeCache, na.pageNum)
		delete(m.lastAccess, na.pageNum)
	}
}

// flushNode 刷新节点到磁盘
func (m *DefaultBPlusTreeManager) flushNode(node *BPlusTreeNode) error {
	// 获取缓冲池中的页面
	bufferPage, err := m.bufferPoolManager.GetPage(m.spaceId, node.PageNum)
	if err != nil {
		return fmt.Errorf("get page from buffer pool failed: %v", err)
	}

	// 将节点写回页面
	if err := m.writeNodeToPage(node, bufferPage); err != nil {
		return fmt.Errorf("write node to page failed: %v", err)
	}

	// 标记页面为脏并请求刷新
	bufferPage.MarkDirty()
	if err := m.bufferPoolManager.FlushPage(m.spaceId, node.PageNum); err != nil {
		return fmt.Errorf("flush page failed: %v", err)
	}

	// 重置节点的脏标记
	node.isDirty = false
	return nil
}

// writeNodeToPage 将节点写入页面
func (m *DefaultBPlusTreeManager) writeNodeToPage(node *BPlusTreeNode, bufferPage *buffer_pool.BufferPage) error {
	// 创建数据页面实现来操作页面内容
	p := page.NewDataPageImpl(node.PageNum, 0)
	p.SetLeafPage(node.IsLeaf)
	p.SetNextPage(node.NextLeaf)

	if node.IsLeaf {
		// 写入叶子节点记录
		records := make([]page.Record, len(node.Keys))
		for i := range node.Keys {
			// 将key转换为字节数组
			keyBytes := []byte(fmt.Sprintf("%v", node.Keys[i]))
			records[i] = page.Record{
				Data: keyBytes,
			}
		}
		if err := p.WriteRecords(records); err != nil {
			return err
		}
	} else {
		// 写入非叶子节点索引项
		entries := make([]page.IndexEntry, len(node.Keys))
		for i := range node.Keys {
			keyBytes := []byte(fmt.Sprintf("%v", node.Keys[i]))
			childPage := uint32(0)
			if i < len(node.Children) {
				childPage = node.Children[i]
			}
			entries[i] = page.IndexEntry{
				Key:  keyBytes,
				Page: childPage,
			}
		}
		if err := p.WriteIndexEntries(entries); err != nil {
			return err
		}
	}

	// 序列化页面数据并写入缓冲页面
	serializedData := p.Serialize()
	bufferPage.SetContent(serializedData)

	return nil
}

func (m *DefaultBPlusTreeManager) Init(ctx context.Context, spaceId uint32, rootPage uint32) error {
	// 只在设置基本参数时使用锁，避免在持有锁时调用其他方法
	m.mutex.Lock()
	m.spaceId = spaceId
	m.rootPage = rootPage
	m.mutex.Unlock()

	// 在释放锁后加载根节点，避免死锁
	_, err := m.getNode(ctx, rootPage)
	return err
}

func (m *DefaultBPlusTreeManager) GetAllLeafPages(ctx context.Context) ([]uint32, error) {
	// 这个方法不需要持有锁，因为getNode内部会处理锁
	leafPages := make([]uint32, 0)
	firstLeaf, err := m.findFirstLeafPageLockFree(ctx)
	if err != nil {
		return nil, err
	}

	// 遍历叶子节点链表
	currentPage := firstLeaf
	for currentPage != 0 {
		node, err := m.getNode(ctx, currentPage)
		if err != nil {
			return nil, err
		}
		leafPages = append(leafPages, currentPage)
		currentPage = node.NextLeaf
	}

	return leafPages, nil
}

func (m *DefaultBPlusTreeManager) Search(ctx context.Context, key interface{}) (uint32, int, error) {
	// 这个方法不需要持有锁，因为getNode内部会处理锁
	node, err := m.getNode(ctx, m.rootPage)
	if err != nil {
		return 0, 0, err
	}

	// 从根节点开始查找
	for !node.IsLeaf {
		// 检查是否有子节点
		if len(node.Children) == 0 {
			return 0, 0, fmt.Errorf("non-leaf node has no children")
		}

		childIndex := m.findChildIndex(node, key)
		if childIndex >= len(node.Children) {
			childIndex = len(node.Children) - 1
		}

		// 确保不为负数
		if childIndex < 0 {
			childIndex = 0
		}

		node, err = m.getNode(ctx, node.Children[childIndex])
		if err != nil {
			return 0, 0, err
		}
	}

	// 在叶子节点中查找记录位置
	recordIndex := m.findRecordIndex(node, key)

	// 检查索引是否有效
	if recordIndex >= len(node.Records) || recordIndex >= len(node.Keys) {
		return 0, 0, fmt.Errorf("key not found: record index %d out of range (records: %d, keys: %d)",
			recordIndex, len(node.Records), len(node.Keys))
	}

	// 检查键是否匹配
	if len(node.Keys) == 0 || m.compareKeys(node.Keys[recordIndex], key) != 0 {
		return 0, 0, fmt.Errorf("key not found: key mismatch")
	}

	return node.PageNum, int(node.Records[recordIndex]), nil
}

func (m *DefaultBPlusTreeManager) Insert(ctx context.Context, key interface{}, value []byte) error {
	// 简化的插入实现：直接添加到根节点
	rootNode, err := m.getNode(ctx, m.rootPage)
	if err != nil {
		return fmt.Errorf("failed to get root node: %v", err)
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 如果是叶子节点，直接添加键值对
	if rootNode.IsLeaf {
		// 添加新的键和记录
		rootNode.Keys = append(rootNode.Keys, key)
		rootNode.Records = append(rootNode.Records, uint32(len(rootNode.Records))) // 简化的记录位置
		rootNode.isDirty = true

		// 将记录存储到页面中（简化实现）
		err = m.storeRecordInPage(ctx, rootNode.PageNum, key, value)
		if err != nil {
			return fmt.Errorf("failed to store record: %v", err)
		}

		fmt.Printf("    ✓ Inserted key '%v' into leaf node (page %d)\n", key, rootNode.PageNum)
		return nil
	}

	// 非叶子节点的处理（简化版本）
	// 找到合适的子节点并递归插入
	childIndex := m.findChildIndex(rootNode, key)
	if childIndex < len(rootNode.Children) {
		// 递归插入到子节点（这里简化为直接插入到第一个子节点）
		childPageNo := rootNode.Children[0]
		if childPageNo != 0 {
			childNode, err := m.getNode(ctx, childPageNo)
			if err == nil && childNode.IsLeaf {
				childNode.Keys = append(childNode.Keys, key)
				childNode.Records = append(childNode.Records, uint32(len(childNode.Records)))
				childNode.isDirty = true

				err = m.storeRecordInPage(ctx, childNode.PageNum, key, value)
				if err == nil {
					fmt.Printf("    ✓ Inserted key '%v' into child leaf node (page %d)\n", key, childNode.PageNum)
					return nil
				}
			}
		}
	}

	// 如果上述方法都失败，创建新的叶子节点
	return m.insertIntoNewLeafNode(ctx, key, value)
}

func (m *DefaultBPlusTreeManager) RangeSearch(ctx context.Context, startKey, endKey interface{}) ([]basic.Row, error) {
	// 这个方法不需要持有锁，因为内部方法会处理锁

	// 找到起始叶子节点
	startNode, err := m.findLeafNode(ctx, startKey)
	if err != nil {
		return nil, err
	}

	results := make([]basic.Row, 0)
	currentNode := startNode

	// 遍历叶子节点链表
	for currentNode != nil {
		// 遍历当前节点的记录
		for i, key := range currentNode.Keys {
			// 如果超过结束键，返回结果
			if m.compareKeys(key, endKey) > 0 {
				return results, nil
			}

			// 检查是否小于起始键
			if m.compareKeys(key, startKey) < 0 {
				continue
			}

			// 获取记录并转换为Row
			record, err := m.getRecord(ctx, currentNode.PageNum, int(currentNode.Records[i]))
			if err != nil {
				return nil, err
			}

			// 将page.Record转换为basic.Row
			// 这里需要创建一个适配器来转换类型
			row := &RecordRowAdapter{record: record}
			results = append(results, row)
		}

		// 移动到下一个叶子节点
		if currentNode.NextLeaf == 0 {
			break
		}
		currentNode, err = m.getNode(ctx, currentNode.NextLeaf)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

func (m *DefaultBPlusTreeManager) GetFirstLeafPage(ctx context.Context) (uint32, error) {
	// 这个方法不需要持有锁，因为内部方法会处理锁
	return m.findFirstLeafPageLockFree(ctx)
}

// 内部辅助方法
func (m *DefaultBPlusTreeManager) getNode(ctx context.Context, pageNum uint32) (*BPlusTreeNode, error) {
	// 1. 先读缓存（读锁）
	m.mutex.RLock()
	node, ok := m.nodeCache[pageNum]
	if ok {
		m.lastAccess[pageNum] = time.Now()
		atomic.AddUint64(&m.stats.cacheHits, 1)
		m.mutex.RUnlock()
		return node, nil
	}
	m.mutex.RUnlock()

	atomic.AddUint64(&m.stats.cacheMisses, 1)

	// 2. 缓存未命中，加载页面（不持锁）
	bufferPage, err := m.bufferPoolManager.GetPage(m.spaceId, pageNum)
	if err != nil {
		return nil, fmt.Errorf("get page from buffer pool failed: %v", err)
	}

	node, err = m.parseBufferPage(bufferPage)
	if err != nil {
		return nil, fmt.Errorf("parse page failed: %v", err)
	}

	// 3. 写缓存（写锁）
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 重新检查缓存，避免重复加载
	if existingNode, ok := m.nodeCache[pageNum]; ok {
		return existingNode, nil
	}

	// 如果缓存满了，先释放锁，异步执行LRU淘汰，然后重新获取锁
	if uint32(len(m.nodeCache)) >= m.config.MaxCacheSize {
		m.mutex.Unlock()
		// 异步淘汰一些节点（不会引起死锁）
		go m.evictNodesAsync()
		m.mutex.Lock()
		// 重新检查缓存，可能在淘汰过程中已经被其他goroutine添加了
		if existingNode, ok := m.nodeCache[pageNum]; ok {
			return existingNode, nil
		}
	}

	m.nodeCache[pageNum] = node
	m.lastAccess[pageNum] = time.Now()

	return node, nil
}

func (m *DefaultBPlusTreeManager) parseBufferPage(bufferPage *buffer_pool.BufferPage) (*BPlusTreeNode, error) {
	node := &BPlusTreeNode{
		PageNum: bufferPage.GetPageNo(),
		IsLeaf:  false, // 将在解析后更新
		isDirty: false,
	}

	// 创建数据页面实现来解析内容
	p := page.NewDataPageImpl(bufferPage.GetPageNo(), 0)

	// 从缓冲页面内容反序列化
	if err := p.Deserialize(bufferPage.GetContent()); err != nil {
		return nil, err
	}

	node.IsLeaf = p.IsLeafPage()
	node.NextLeaf = p.GetNextPage()

	// 解析页面内容
	if node.IsLeaf {
		// 解析叶子节点
		records := p.GetRecords()
		node.Keys = make([]interface{}, len(records))
		node.Records = make([]uint32, len(records))
		for i, record := range records {
			// 将字节数组转换回key
			node.Keys[i] = string(record.Data)
			node.Records[i] = uint32(i) // 简化处理，使用索引作为记录位置
		}
	} else {
		// 解析非叶子节点
		entries := p.GetIndexEntries()
		node.Keys = make([]interface{}, len(entries))
		node.Children = make([]uint32, len(entries))
		for i, entry := range entries {
			node.Keys[i] = string(entry.Key)
			node.Children[i] = entry.Page
		}
	}

	return node, nil
}

func (m *DefaultBPlusTreeManager) findChildIndex(node *BPlusTreeNode, key interface{}) int {
	// 二分查找合适的子节点
	low, high := 0, len(node.Keys)
	for low < high {
		mid := (low + high) / 2
		if m.compareKeys(node.Keys[mid], key) <= 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

func (m *DefaultBPlusTreeManager) findRecordIndex(node *BPlusTreeNode, key interface{}) int {
	// 二分查找记录位置
	low, high := 0, len(node.Keys)
	for low < high {
		mid := (low + high) / 2
		if m.compareKeys(node.Keys[mid], key) < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

func (m *DefaultBPlusTreeManager) findFirstLeafPage(ctx context.Context) (uint32, error) {
	node, err := m.getNode(ctx, m.rootPage)
	if err != nil {
		return 0, err
	}

	// 一直往左子节点走，直到叶子节点
	for !node.IsLeaf {
		if len(node.Children) == 0 {
			return 0, fmt.Errorf("non-leaf node has no children")
		}
		node, err = m.getNode(ctx, node.Children[0])
		if err != nil {
			return 0, err
		}
	}

	return node.PageNum, nil
}

// findFirstLeafPageLockFree 不持有锁的版本，委托给getNode处理锁
func (m *DefaultBPlusTreeManager) findFirstLeafPageLockFree(ctx context.Context) (uint32, error) {
	node, err := m.getNode(ctx, m.rootPage)
	if err != nil {
		return 0, err
	}

	// 一直往左子节点走，直到叶子节点
	for !node.IsLeaf {
		if len(node.Children) == 0 {
			return 0, fmt.Errorf("non-leaf node has no children")
		}
		node, err = m.getNode(ctx, node.Children[0])
		if err != nil {
			return 0, err
		}
	}

	return node.PageNum, nil
}

func (m *DefaultBPlusTreeManager) findLeafNode(ctx context.Context, key interface{}) (*BPlusTreeNode, error) {
	node, err := m.getNode(ctx, m.rootPage)
	if err != nil {
		return nil, err
	}

	// 从根节点开始查找到叶子节点
	for !node.IsLeaf {
		childIndex := m.findChildIndex(node, key)
		if childIndex >= len(node.Children) {
			childIndex = len(node.Children) - 1
		}
		node, err = m.getNode(ctx, node.Children[childIndex])
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

func (m *DefaultBPlusTreeManager) getRecord(ctx context.Context, pageNum uint32, slot int) (*page.Record, error) {
	bufferPage, err := m.bufferPoolManager.GetPage(m.spaceId, pageNum)
	if err != nil {
		return nil, err
	}

	// 创建数据页面实现
	p := page.NewDataPageImpl(pageNum, 0)

	if err := p.Deserialize(bufferPage.GetContent()); err != nil {
		return nil, err
	}

	records := p.GetRecords()
	if slot >= len(records) || slot < 0 {
		return nil, fmt.Errorf("invalid slot %d", slot)
	}

	return &records[slot], nil
}

func (m *DefaultBPlusTreeManager) compareKeys(a, b interface{}) int {
	// 根据实际类型实现比较逻辑
	switch v1 := a.(type) {
	case int:
		v2 := b.(int)
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		}
		return 0
	case string:
		v2 := b.(string)
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		}
		return 0
	default:
		panic("unsupported key type")
	}
}

// RecordRowAdapter 将page.Record适配为basic.Row接口
type RecordRowAdapter struct {
	record *page.Record
}

func (r *RecordRowAdapter) Less(than basic.Row) bool {
	// 简化实现，实际应该根据具体的比较逻辑
	return false
}

func (r *RecordRowAdapter) ToByte() []byte {
	if r.record != nil {
		return r.record.Data
	}
	return nil
}

func (r *RecordRowAdapter) IsInfimumRow() bool {
	return false
}

func (r *RecordRowAdapter) IsSupremumRow() bool {
	return false
}

func (r *RecordRowAdapter) GetPageNumber() uint32 {
	return 0 // 需要从上下文获取
}

func (r *RecordRowAdapter) WriteWithNull(content []byte) {
	// TODO: 实现
}

func (r *RecordRowAdapter) WriteBytesWithNullWithsPos(content []byte, index byte) {
	// TODO: 实现
}

func (r *RecordRowAdapter) GetRowLength() uint16 {
	if r.record != nil {
		return uint16(len(r.record.Data))
	}
	return 0
}

func (r *RecordRowAdapter) GetHeaderLength() uint16 {
	return 0 // TODO: 实现
}

func (r *RecordRowAdapter) GetPrimaryKey() basic.Value {
	return nil // TODO: 实现
}

func (r *RecordRowAdapter) GetFieldLength() int {
	return 1 // 简化实现
}

func (r *RecordRowAdapter) ReadValueByIndex(index int) basic.Value {
	return nil // TODO: 实现
}

func (r *RecordRowAdapter) SetNOwned(cnt byte) {
	// TODO: 实现
}

func (r *RecordRowAdapter) GetNOwned() byte {
	return 0
}

func (r *RecordRowAdapter) GetNextRowOffset() uint16 {
	return 0
}

func (r *RecordRowAdapter) SetNextRowOffset(offset uint16) {
	// TODO: 实现
}

func (r *RecordRowAdapter) GetHeapNo() uint16 {
	return 0
}

func (r *RecordRowAdapter) SetHeapNo(heapNo uint16) {
	// TODO: 实现
}

func (r *RecordRowAdapter) SetTransactionId(trxId uint64) {
	// TODO: 实现
}

func (r *RecordRowAdapter) GetValueByColName(colName string) basic.Value {
	return nil // TODO: 实现
}

func (r *RecordRowAdapter) ToString() string {
	if r.record != nil {
		return string(r.record.Data)
	}
	return ""
}

// storeRecordInPage 将记录存储到页面中
func (m *DefaultBPlusTreeManager) storeRecordInPage(ctx context.Context, pageNum uint32, key interface{}, value []byte) error {
	// 获取页面
	bufferPage, err := m.bufferPoolManager.GetPage(m.spaceId, pageNum)
	if err != nil {
		return fmt.Errorf("failed to get page: %v", err)
	}

	// 创建记录数据
	keyStr := fmt.Sprintf("%v", key)
	recordData := make([]byte, len(keyStr)+4+len(value))

	// 写入键长度
	keyLen := uint32(len(keyStr))
	recordData[0] = byte(keyLen)
	recordData[1] = byte(keyLen >> 8)
	recordData[2] = byte(keyLen >> 16)
	recordData[3] = byte(keyLen >> 24)

	// 写入键
	copy(recordData[4:4+len(keyStr)], []byte(keyStr))

	// 写入值
	copy(recordData[4+len(keyStr):], value)

	// 更新页面内容（简化实现：追加到现有内容之后）
	existingContent := bufferPage.GetContent()
	newContent := make([]byte, len(existingContent)+len(recordData))
	copy(newContent, existingContent)
	copy(newContent[len(existingContent):], recordData)

	bufferPage.SetContent(newContent)
	bufferPage.MarkDirty()

	return nil
}

// insertIntoNewLeafNode 插入到新的叶子节点
func (m *DefaultBPlusTreeManager) insertIntoNewLeafNode(ctx context.Context, key interface{}, value []byte) error {
	// 分配新页面
	newPageNum := uint32(100) // 简化实现：使用固定页号

	// 创建新的叶子节点
	newNode := &BPlusTreeNode{
		PageNum:  newPageNum,
		IsLeaf:   true,
		Keys:     []interface{}{key},
		Records:  []uint32{0},
		NextLeaf: 0,
		isDirty:  true,
	}

	// 将节点添加到缓存
	m.nodeCache[newPageNum] = newNode
	m.lastAccess[newPageNum] = time.Now()

	// 存储记录到页面
	err := m.storeRecordInPage(ctx, newPageNum, key, value)
	if err != nil {
		return fmt.Errorf("failed to store record in new leaf: %v", err)
	}

	fmt.Printf("    ✓ Created new leaf node (page %d) and inserted key '%v'\n", newPageNum, key)
	return nil
}
