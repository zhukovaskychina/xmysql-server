package manager

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/space"
	"sync"
	"sync/atomic"
)

// TablespaceHandle represents a handle to a tablespace
type TablespaceHandle struct {
	SpaceID       uint32
	DataSegmentID uint64
	Name          string
}

// StorageManager implements the storage management interface
type StorageManager struct {
	spaceMgr    basic.SpaceManager
	segmentMgr  *SegmentManager
	bufferPool  *buffer_pool.BufferPool
	pageMgr     *DefaultPageManager
	tablespaces map[string]*TablespaceHandle
	nextTxID    uint64
	mu          sync.RWMutex
}

func (sm *StorageManager) Init() {
	// 初始化存储管理器
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 确保所有组件都已初始化
	if sm.spaceMgr == nil || sm.bufferPool == nil || sm.pageMgr == nil || sm.segmentMgr == nil {
		panic("storage manager components not properly initialized")
	}
}

func (sm *StorageManager) GetBufferPoolManager() *BufferPoolManager {
	return nil
}

func (sm *StorageManager) OpenSpace(spaceID uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 委托给SpaceManager处理
	space, err := sm.spaceMgr.GetSpace(spaceID)
	if err != nil {
		return fmt.Errorf("failed to open space %d: %v", spaceID, err)
	}

	// 激活空间
	space.SetActive(true)
	return nil
}

func (sm *StorageManager) CloseSpace(spaceID uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 获取空间
	space, err := sm.spaceMgr.GetSpace(spaceID)
	if err != nil {
		return fmt.Errorf("failed to get space %d: %v", spaceID, err)
	}

	// 先刷新所有脏页
	if err := sm.Flush(); err != nil {
		return fmt.Errorf("failed to flush space %d: %v", spaceID, err)
	}

	// 停用空间
	space.SetActive(false)
	return nil
}

func (sm *StorageManager) DeleteSpace(spaceID uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 先关闭空间
	if err := sm.CloseSpace(spaceID); err != nil {
		return err
	}

	// 从tablespaces中删除
	for name, handle := range sm.tablespaces {
		if handle.SpaceID == spaceID {
			delete(sm.tablespaces, name)
			break
		}
	}

	// 委托给SpaceManager删除
	return sm.spaceMgr.DropSpace(spaceID)
}

func (sm *StorageManager) GetSpaceInfo(spaceID uint32) (*basic.SpaceInfo, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// 获取空间
	space, err := sm.spaceMgr.GetSpace(spaceID)
	if err != nil {
		return nil, fmt.Errorf("space %d not found: %v", spaceID, err)
	}

	// 构建SpaceInfo
	info := &basic.SpaceInfo{
		SpaceID:      space.ID(),
		Name:         space.Name(),
		PageSize:     16384, // 固定16KB页面大小
		TotalPages:   uint64(space.GetPageCount()),
		ExtentSize:   64,    // 标准64页一个区
		IsCompressed: false, // 暂不支持压缩
		State:        "active",
	}

	if space.IsActive() {
		info.State = "active"
	} else {
		info.State = "inactive"
	}

	return info, nil
}

func (sm *StorageManager) ListSpaces() ([]basic.SpaceInfo, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var spaces []basic.SpaceInfo

	// 遍历所有tablespace handles
	for _, handle := range sm.tablespaces {
		info, err := sm.GetSpaceInfo(handle.SpaceID)
		if err != nil {
			continue // 跳过错误的空间
		}
		spaces = append(spaces, *info)
	}

	return spaces, nil
}

func (sm *StorageManager) BeginTransaction() (uint64, error) {
	txID := atomic.AddUint64(&sm.nextTxID, 1)
	return txID, nil
}

func (sm *StorageManager) CommitTransaction(txID uint64) error {
	// 实现事务提交逻辑
	// 1. 刷新所有脏页
	if err := sm.Flush(); err != nil {
		return fmt.Errorf("failed to flush during commit: %v", err)
	}

	// 2. TODO: 写入事务日志

	return nil
}

func (sm *StorageManager) RollbackTransaction(txID uint64) error {
	// 实现事务回滚逻辑
	// TODO: 恢复到事务开始前的状态
	return nil
}

func (sm *StorageManager) Sync(spaceID uint32) error {
	// 同步指定空间的所有数据到磁盘
	// 使用Flush方法来刷新所有数据
	return sm.Flush()
}

// NewStorageManager creates a new StorageManager instance with conf
func NewStorageManager(conf *conf.Cfg) *StorageManager {
	dataDir := conf.GetString("innodb.data_dir")
	bufferPoolSize := conf.GetInt("innodb.buffer_pool_size")

	// Create storage manager instance
	sm := &StorageManager{
		tablespaces: make(map[string]*TablespaceHandle),
		nextTxID:    1,
	}

	// Initialize space manager
	sm.spaceMgr = space.NewSpaceManager(dataDir)

	// Initialize buffer pool
	bufferPoolConfig := &buffer_pool.BufferPoolConfig{
		TotalPages:     uint32(bufferPoolSize / 16384), // 16KB per page
		PageSize:       16384,
		BufferPoolSize: uint64(bufferPoolSize),
		StorageManager: sm.spaceMgr,
	}
	sm.bufferPool = buffer_pool.NewBufferPool(bufferPoolConfig)

	// Initialize page manager
	pageConfig := &PageConfig{
		CacheSize:      1000,
		DirtyThreshold: 0.7,
		EvictionPolicy: "LRU",
	}
	sm.pageMgr = NewPageManager(sm.bufferPool, pageConfig)

	// Initialize segment manager
	sm.segmentMgr = NewSegmentManager(sm.bufferPool)

	return sm
}

// CreateSegment creates a new segment
func (sm *StorageManager) CreateSegment(spaceID uint32, purpose basic.SegmentPurpose) (basic.Segment, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 根据purpose选择合适的segment类型
	segType := SEGMENT_TYPE_DATA
	if purpose == basic.SegmentPurposeNonLeaf {
		segType = SEGMENT_TYPE_INDEX
	}

	return sm.segmentMgr.CreateSegment(spaceID, segType, false)
}

// GetSegment retrieves an existing segment
func (sm *StorageManager) GetSegment(segmentID uint64) (basic.Segment, error) {
	segment := sm.segmentMgr.GetSegment(uint32(segmentID))
	if segment == nil {
		return nil, fmt.Errorf("segment %d not found", segmentID)
	}
	return segment, nil
}

// FreeSegment frees a segment
func (sm *StorageManager) FreeSegment(segmentID uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// TODO: 实现segment释放逻辑
	// 1. 释放segment的所有页面
	// 2. 从segment管理器中删除
	// 暂时返回nil，等待SegmentManager实现FreeSegment方法
	return nil
}

// AllocateExtent allocates a new extent
func (sm *StorageManager) AllocateExtent(spaceID uint32, purpose basic.ExtentPurpose) (basic.Extent, error) {
	return sm.spaceMgr.AllocateExtent(spaceID, purpose)
}

// FreeExtent frees an extent
func (sm *StorageManager) FreeExtent(spaceID, extentID uint32) error {
	return sm.spaceMgr.FreeExtent(spaceID, extentID)
}

// GetPage retrieves a page using DefaultPageManager
func (sm *StorageManager) GetPage(spaceID, pageNo uint32) (basic.IPage, error) {
	// 直接使用DefaultPageManager获取页面
	return sm.pageMgr.GetPage(spaceID, pageNo)
}

// AllocPage allocates a new page using DefaultPageManager
func (sm *StorageManager) AllocPage(spaceID uint32, pageType basic.PageType) (basic.IPage, error) {
	// Convert basic.PageType to common.PageType
	commonPageType := common.PageType(pageType)

	// 直接使用DefaultPageManager创建页面
	page, err := sm.pageMgr.CreatePage(commonPageType)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate page: %v", err)
	}

	return page, nil
}

// FreePage frees a page
func (sm *StorageManager) FreePage(spaceID, pageNo uint32) error {
	// Use page manager to flush the page before freeing
	return sm.pageMgr.FlushPage(spaceID, pageNo)
}

// Begin starts a new transaction
func (sm *StorageManager) Begin() (basic.Transaction, error) {
	txID := atomic.AddUint64(&sm.nextTxID, 1)
	return newTransaction(txID, sm), nil
}

// Commit commits a transaction
func (sm *StorageManager) Commit(tx basic.Transaction) error {
	return tx.Commit()
}

// Rollback rolls back a transaction
func (sm *StorageManager) Rollback(tx basic.Transaction) error {
	return tx.Rollback()
}

// Flush flushes all changes to disk
func (sm *StorageManager) Flush() error {
	// Use page manager to flush all pages
	return sm.pageMgr.FlushAll()
}

// Close releases all resources
func (sm *StorageManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Flush all changes
	if err := sm.Flush(); err != nil {
		return fmt.Errorf("failed to flush during close: %v", err)
	}

	// TODO: Close buffer pool when method is available
	// if err := sm.bufferPool.Close(); err != nil {
	//     return fmt.Errorf("failed to close buffer pool: %v", err)
	// }

	// Close space manager
	if err := sm.spaceMgr.Close(); err != nil {
		return fmt.Errorf("failed to close space manager: %v", err)
	}

	return nil
}

// CreateTablespace creates a new tablespace
func (sm *StorageManager) CreateTablespace(name string) (*TablespaceHandle, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 检查是否已存在
	if _, exists := sm.tablespaces[name]; exists {
		return nil, fmt.Errorf("tablespace %s already exists", name)
	}

	// 创建新的表空间
	spaceID, err := sm.spaceMgr.CreateTableSpace(name)
	if err != nil {
		return nil, fmt.Errorf("failed to create tablespace: %v", err)
	}

	// 创建数据段
	_, err = sm.CreateSegment(spaceID, basic.SegmentPurposeLeaf)
	if err != nil {
		return nil, fmt.Errorf("failed to create data segment: %v", err)
	}

	// 创建handle
	handle := &TablespaceHandle{
		SpaceID:       spaceID,
		DataSegmentID: uint64(spaceID), // 暂时使用spaceID作为segmentID
		Name:          name,
	}

	sm.tablespaces[name] = handle
	return handle, nil
}

// GetTablespace gets a tablespace handle
func (sm *StorageManager) GetTablespace(name string) (*TablespaceHandle, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	handle, exists := sm.tablespaces[name]
	if !exists {
		return nil, fmt.Errorf("tablespace %s not found", name)
	}

	return handle, nil
}

func (sm *StorageManager) GetSegmentManager() *SegmentManager {
	return sm.segmentMgr
}

func (sm *StorageManager) GetSpaceManager() basic.SpaceManager {
	return sm.spaceMgr
}

func (sm *StorageManager) GetPageManager() basic.PageManager {
	return nil
}

// Transaction implementation
type txImpl struct {
	id        uint64
	sm        *StorageManager
	writes    []func()
	committed bool
	mu        sync.Mutex
}

// newTransaction creates a new transaction
func newTransaction(id uint64, sm *StorageManager) *txImpl {
	return &txImpl{
		id:     id,
		sm:     sm,
		writes: make([]func(), 0),
	}
}

// ID returns the transaction ID
func (t *txImpl) ID() uint64 {
	return t.id
}

// Commit commits the transaction
func (t *txImpl) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.committed {
		return nil
	}

	// 执行所有写操作
	for _, write := range t.writes {
		write()
	}

	// 提交事务
	if err := t.sm.CommitTransaction(t.id); err != nil {
		return fmt.Errorf("failed to commit transaction %d: %v", t.id, err)
	}

	t.committed = true
	return nil
}

// Rollback rolls back the transaction
func (t *txImpl) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.committed {
		return fmt.Errorf("transaction %d already committed", t.id)
	}

	// 回滚事务
	if err := t.sm.RollbackTransaction(t.id); err != nil {
		return fmt.Errorf("failed to rollback transaction %d: %v", t.id, err)
	}

	t.writes = nil
	return nil
}

// AddWrite adds a write operation to the transaction
func (t *txImpl) AddWrite(writeFn func()) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.committed {
		t.writes = append(t.writes, writeFn)
	}
}
