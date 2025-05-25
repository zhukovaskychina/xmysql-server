package extent

import (
	_ "encoding/binary"
	"errors"
	"sync"
	"time"
	"xmysql-server/server/innodb/basic"
)

var (
	ErrExtentFull    = errors.New("extent is full")
	ErrExtentEmpty   = errors.New("extent is empty")
	ErrPageNotFound  = errors.New("page not found in extent")
	ErrInvalidExtent = errors.New("invalid extent")
)

// BaseExtent 基础区实现
type BaseExtent struct {
	basic.Extent
	mu       sync.RWMutex
	header   basic.ExtentHeader
	stats    basic.ExtentStats
	pages    map[uint32]bool // 页面映射表
	pageList []uint32        // 有序页面列表
}

// NewBaseExtent 创建基础区
func NewBaseExtent(spaceID, extentID uint32, extType basic.ExtentType) *BaseExtent {
	be := &BaseExtent{
		pages: make(map[uint32]bool),
	}

	// 初始化区头
	be.header = basic.ExtentHeader{
		ExtentID:   extentID,
		SpaceID:    spaceID,
		State:      basic.ExtentStateFree,
		Type:       extType,
		FirstPage:  extentID * 64, // 假设每个区64页
		PageCount:  0,
		FreeSpace:  64 * 16 * 1024, // 64页 * 16KB
		CreateTime: time.Now().UnixNano(),
	}

	return be
}

// GetID 获取区ID
func (be *BaseExtent) GetID() uint32 {
	return be.header.ExtentID
}

// GetState 获取区状态
func (be *BaseExtent) GetState() basic.ExtentState {
	be.mu.RLock()
	defer be.mu.RUnlock()
	return be.header.State
}

// GetType 获取区类型
func (be *BaseExtent) GetType() basic.ExtentType {
	return be.header.Type
}

// GetSpaceID 获取表空间ID
func (be *BaseExtent) GetSpaceID() uint32 {
	return be.header.SpaceID
}

// GetSegmentID 获取段ID
func (be *BaseExtent) GetSegmentID() uint64 {
	return be.header.SegmentID
}

// AllocatePage 分配页面
func (be *BaseExtent) AllocatePage() (uint32, error) {
	be.mu.Lock()
	defer be.mu.Unlock()

	// 检查区是否已满
	if be.header.PageCount >= 64 {
		return 0, ErrExtentFull
	}

	// 查找第一个空闲页面
	pageNo := be.header.FirstPage
	for i := uint32(0); i < 64; i++ {
		if !be.pages[pageNo+i] {
			// 分配页面
			be.pages[pageNo+i] = true
			be.pageList = append(be.pageList, pageNo+i)
			be.header.PageCount++
			be.header.FreeSpace -= 16 * 1024 // 减少16KB空闲空间

			// 更新状态
			if be.header.PageCount == 64 {
				be.header.State = basic.ExtentStateFull
			} else if be.header.State == basic.ExtentStateFree {
				be.header.State = basic.ExtentStatePartial
			}

			// 更新统计信息
			be.stats.LastAllocated = time.Now().UnixNano()
			be.stats.TotalPages.Add(1)
			be.stats.FreePages.Add(^uint32(0))

			return pageNo + i, nil
		}
	}

	return 0, ErrExtentFull
}

// FreePage 释放页面
func (be *BaseExtent) FreePage(pageNo uint32) error {
	be.mu.Lock()
	defer be.mu.Unlock()

	// 检查页面是否属于该区
	if pageNo < be.header.FirstPage || pageNo >= be.header.FirstPage+64 {
		return ErrPageNotFound
	}

	// 检查页面是否已分配
	if !be.pages[pageNo] {
		return ErrPageNotFound
	}

	// 释放页面
	delete(be.pages, pageNo)
	for i, p := range be.pageList {
		if p == pageNo {
			be.pageList = append(be.pageList[:i], be.pageList[i+1:]...)
			break
		}
	}

	be.header.PageCount--
	be.header.FreeSpace += 16 * 1024 // 增加16KB空闲空间

	// 更新状态
	if be.header.PageCount == 0 {
		be.header.State = basic.ExtentStateFree
	} else if be.header.State == basic.ExtentStateFull {
		be.header.State = basic.ExtentStatePartial
	}

	// 更新统计信息
	be.stats.LastFreed = time.Now().UnixNano()
	be.stats.TotalPages.Add(^uint32(0))
	be.stats.FreePages.Add(1)

	return nil
}

// GetPageCount 获取页面数量
func (be *BaseExtent) GetPageCount() uint32 {
	be.mu.RLock()
	defer be.mu.RUnlock()
	return be.header.PageCount
}

// GetFreePages 获取空闲页面列表
func (be *BaseExtent) GetFreePages() []uint32 {
	be.mu.RLock()
	defer be.mu.RUnlock()

	freePages := make([]uint32, 0)
	for i := uint32(0); i < 64; i++ {
		pageNo := be.header.FirstPage + i
		if !be.pages[pageNo] {
			freePages = append(freePages, pageNo)
		}
	}
	return freePages
}

// GetFreeSpace 获取空闲空间
func (be *BaseExtent) GetFreeSpace() uint64 {
	be.mu.RLock()
	defer be.mu.RUnlock()
	return be.header.FreeSpace
}

// IsFull 是否已满
func (be *BaseExtent) IsFull() bool {
	be.mu.RLock()
	defer be.mu.RUnlock()
	return be.header.State == basic.ExtentStateFull
}

// IsEmpty 是否为空
func (be *BaseExtent) IsEmpty() bool {
	be.mu.RLock()
	defer be.mu.RUnlock()
	return be.header.State == basic.ExtentStateFree
}

// GetStats 获取统计信息
func (be *BaseExtent) GetStats() *basic.ExtentStats {
	be.mu.RLock()
	defer be.mu.RUnlock()
	return &be.stats
}

// Defragment 碎片整理
func (be *BaseExtent) Defragment() error {
	be.mu.Lock()
	defer be.mu.Unlock()

	// TODO: 实现碎片整理
	be.stats.LastDefragged = time.Now().UnixNano()
	return nil
}

// Reset 重置区
func (be *BaseExtent) Reset() error {
	be.mu.Lock()
	defer be.mu.Unlock()

	// 清空页面映射
	be.pages = make(map[uint32]bool)
	be.pageList = nil

	// 重置区头
	be.header.State = basic.ExtentStateFree
	be.header.PageCount = 0
	be.header.FreeSpace = 64 * 16 * 1024

	// 重置统计信息
	be.stats = basic.ExtentStats{}

	return nil
}

// Lock 加锁
func (be *BaseExtent) Lock() {
	be.mu.Lock()
}

// Unlock 解锁
func (be *BaseExtent) Unlock() {
	be.mu.Unlock()
}

// RLock 读锁
func (be *BaseExtent) RLock() {
	be.mu.RLock()
}

// RUnlock 读解锁
func (be *BaseExtent) RUnlock() {
	be.mu.RUnlock()
}
