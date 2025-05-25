package manager

import (
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"sync"
)

var (
	ErrTablespaceNotFound = errors.New("tablespace not found")
)

// SpaceManager 管理表空间
type SpaceManagerImpl struct {
	basic.SpaceManager
	mu sync.RWMutex

	// 表空间映射: space_id -> tablespace
	spaces map[uint32]*Tablespace

	// 页面管理器
	pageManager basic.PageManager

	// 区管理器
	extentManager *ExtentManager

	// 缓冲池
	bufferPool *buffer_pool.BufferPool
}

// Tablespace 表示一个表空间
type Tablespace struct {
	SpaceID     uint32            // 表空间ID
	Name        string            // 表空间名称
	Path        string            // 文件路径
	PageSize    uint32            // 页大小
	FileSize    uint64            // 文件大小
	FreeSpace   uint64            // 空闲空间
	Extents     []uint32          // 区列表
	Pages       map[uint32]uint32 // 页面映射
	IsTemporary bool              // 是否临时表空间
}

// NewSpaceManager 创建空间管理器
func NewSpaceManager(pageManager basic.PageManager, bufferPool *buffer_pool.BufferPool) *SpaceManagerImpl {
	return &SpaceManagerImpl{
		spaces:        make(map[uint32]*Tablespace),
		pageManager:   pageManager,
		extentManager: NewExtentManager(bufferPool),
		bufferPool:    bufferPool,
	}
}

// CreateTablespace 创建新表空间
func (sm *SpaceManagerImpl) CreateTablespace(name string, pageSize uint32, isTemp bool) (*Tablespace, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 生成新的表空间ID
	spaceID := uint32(len(sm.spaces) + 1)

	// 创建表空间对象
	ts := &Tablespace{
		SpaceID:     spaceID,
		Name:        name,
		PageSize:    pageSize,
		Pages:       make(map[uint32]uint32),
		IsTemporary: isTemp,
	}

	// 保存表空间
	sm.spaces[spaceID] = ts
	return ts, nil
}

// GetTablespace 获取表空间
func (sm *SpaceManagerImpl) GetTablespace(spaceID uint32) *Tablespace {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.spaces[spaceID]
}

// AllocatePage 在表空间中分配新页面
func (sm *SpaceManagerImpl) AllocatePage(spaceID uint32) (uint32, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ts := sm.spaces[spaceID]
	if ts == nil {
		return 0, ErrTablespaceNotFound
	}

	// 简化实现，直接分配页面ID
	pageNo := uint32(len(ts.Pages) + 1)
	ts.Pages[pageNo] = 1 // 假设在区1中
	return pageNo, nil
}

// FreePage 释放页面
func (sm *SpaceManagerImpl) FreePage(spaceID uint32, pageNo uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ts := sm.spaces[spaceID]
	if ts == nil {
		return ErrTablespaceNotFound
	}

	delete(ts.Pages, pageNo)
	return nil
}

// DropTablespace 删除表空间
func (sm *SpaceManagerImpl) DropTablespace(spaceID uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ts := sm.spaces[spaceID]
	if ts == nil {
		return ErrTablespaceNotFound
	}

	delete(sm.spaces, spaceID)
	return nil
}

// GetFreeSpace 获取表空间剩余空间
func (sm *SpaceManagerImpl) GetFreeSpace(spaceID uint32) uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	ts := sm.spaces[spaceID]
	if ts == nil {
		return 0
	}
	return ts.FreeSpace
}

// Close 关闭空间管理器
func (sm *SpaceManagerImpl) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 清理资源
	sm.spaces = nil
	return nil
}
