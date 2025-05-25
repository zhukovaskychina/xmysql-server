package manager

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/buffer_pool"
	"xmysql-server/server/innodb/storage/wrapper/page"
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
	bufferPoolManager *BufferPoolManager
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
func NewBPlusTreeManager(bpm *BufferPoolManager, config *BPlusTreeConfig) *DefaultBPlusTreeManager {
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
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 如果脏节点比例超过阈值，执行刷新
	dirtyRatio := float64(m.stats.dirtyNodes) / float64(len(m.nodeCache))
	if dirtyRatio > m.config.DirtyThreshold {
		m.flushDirtyNodes()
	}

	// 如果缓存大小超过限制，执行淘汰
	if uint32(len(m.nodeCache)) > m.config.MaxCacheSize {
		m.evictNodes()
	}
}

// flushDirtyNodes 刷新脏节点
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

// evictNodes 淘汰节点
func (m *DefaultBPlusTreeManager) evictNodes() {
	m.evictLRU()
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
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.spaceId = spaceId
	m.rootPage = rootPage

	// 加载根节点
	_, err := m.getNode(ctx, rootPage)
	return err
}

func (m *DefaultBPlusTreeManager) GetAllLeafPages(ctx context.Context) ([]uint32, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	leafPages := make([]uint32, 0)
	firstLeaf, err := m.findFirstLeafPage(ctx)
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
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	node, err := m.getNode(ctx, m.rootPage)
	if err != nil {
		return 0, 0, err
	}

	// 从根节点开始查找
	for !node.IsLeaf {
		childIndex := m.findChildIndex(node, key)
		if childIndex >= len(node.Children) {
			childIndex = len(node.Children) - 1
		}
		node, err = m.getNode(ctx, node.Children[childIndex])
		if err != nil {
			return 0, 0, err
		}
	}

	// 在叶子节点中查找记录位置
	recordIndex := m.findRecordIndex(node, key)
	if recordIndex >= len(node.Records) || m.compareKeys(node.Keys[recordIndex], key) != 0 {
		return 0, 0, fmt.Errorf("key not found")
	}

	return node.PageNum, int(node.Records[recordIndex]), nil
}

func (m *DefaultBPlusTreeManager) Insert(ctx context.Context, key interface{}, value []byte) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// TODO: 实现插入逻辑
	return fmt.Errorf("insert not implemented")
}

func (m *DefaultBPlusTreeManager) RangeSearch(ctx context.Context, startKey, endKey interface{}) ([]basic.Row, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

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
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.findFirstLeafPage(ctx)
}

// 内部辅助方法

func (m *DefaultBPlusTreeManager) getNode(ctx context.Context, pageNum uint32) (*BPlusTreeNode, error) {
	m.mutex.RLock()
	// 检查缓存
	if node, ok := m.nodeCache[pageNum]; ok {
		// 更新访问时间
		m.lastAccess[pageNum] = time.Now()
		atomic.AddUint64(&m.stats.cacheHits, 1)
		m.mutex.RUnlock()
		return node, nil
	}
	m.mutex.RUnlock()

	// 缓存未命中，需要从 BufferPool 获取
	atomic.AddUint64(&m.stats.cacheMisses, 1)

	// 从 BufferPool 获取页
	bufferPage, err := m.bufferPoolManager.GetPage(m.spaceId, pageNum)
	if err != nil {
		return nil, fmt.Errorf("get page from buffer pool failed: %v", err)
	}

	// 解析页面内容为 B+树节点
	node, err := m.parseBufferPage(bufferPage)
	if err != nil {
		return nil, fmt.Errorf("parse page failed: %v", err)
	}

	// 更新缓存
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 再次检查缓存，防止并发加载
	if existingNode, ok := m.nodeCache[pageNum]; ok {
		return existingNode, nil
	}

	// 检查缓存大小
	if uint32(len(m.nodeCache)) >= m.config.MaxCacheSize {
		// 执行 LRU 淘汰
		m.evictLRU()
	}

	// 添加到缓存
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
