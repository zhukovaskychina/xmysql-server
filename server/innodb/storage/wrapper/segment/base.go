package segment

import (
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	extent3 "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/extent"
	"sync"
	"time"
)

var (
	ErrSegmentFull   = errors.New("segment is full")
	ErrNoFreeExtents = errors.New("no free extents available")
	ErrPageNotFound  = errors.New("page not found in segment")
)

// BaseSegment 基础段实现
type BaseSegment struct {
	Segment
	mu         sync.RWMutex
	header     SegmentHeader
	stats      SegmentStats
	extents    *extent3.BaseExtentList
	freePages  map[uint32]bool
	dirtyPages map[uint32]bool
}

// NewBaseSegment 创建基础段
func NewBaseSegment(segID uint64, spaceID uint32, segType SegmentType) *BaseSegment {
	bs := &BaseSegment{
		extents:    extent3.NewBaseExtentList(),
		freePages:  make(map[uint32]bool),
		dirtyPages: make(map[uint32]bool),
	}

	// 初始化段头
	bs.header = SegmentHeader{
		SegmentID:   segID,
		SegmentType: segType,
		State:       SegmentStateActive,
		SpaceID:     spaceID,
		CreateTime:  time.Now().UnixNano(),
	}

	return bs
}

// GetID 获取段ID
func (bs *BaseSegment) GetID() uint64 {
	return bs.header.SegmentID
}

// GetType 获取段类型
func (bs *BaseSegment) GetType() SegmentType {
	return bs.header.SegmentType
}

// GetState 获取段状态
func (bs *BaseSegment) GetState() SegmentState {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return bs.header.State
}

// GetSpaceID 获取表空间ID
func (bs *BaseSegment) GetSpaceID() uint32 {
	return bs.header.SpaceID
}

// AllocatePage 分配页面
func (bs *BaseSegment) AllocatePage() (uint32, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// 先从空闲页面列表中分配
	for pageNo := range bs.freePages {
		delete(bs.freePages, pageNo)
		bs.stats.FreePages.Add(^uint32(0))
		bs.stats.TotalPages.Add(1)
		return pageNo, nil
	}

	// 查找有空闲页面的区
	iter := bs.extents.Iterator()
	for iter.Next() {
		ext := iter.Current()
		if !ext.IsFull() {
			pageNo, err := ext.AllocatePage()
			if err == nil {
				bs.stats.TotalPages.Add(1)
				return pageNo, nil
			}
		}
	}

	// 分配新的区
	ext, err := bs.AllocateExtent()
	if err != nil {
		return 0, err
	}

	// 从新区中分配页面
	pageNo, err := ext.AllocatePage()
	if err != nil {
		return 0, err
	}

	bs.stats.TotalPages.Add(1)
	return pageNo, nil
}

// FreePage 释放页面
func (bs *BaseSegment) FreePage(pageNo uint32) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// 查找页面所在的区
	iter := bs.extents.Iterator()
	for iter.Next() {
		ext := iter.Current()
		if pageNo >= ext.GetID()*64 && pageNo < (ext.GetID()+1)*64 {
			if err := ext.FreePage(pageNo); err != nil {
				return err
			}
			bs.freePages[pageNo] = true
			bs.stats.FreePages.Add(1)
			bs.stats.TotalPages.Add(^uint32(0))

			// 如果区变空了，考虑释放它
			if ext.IsEmpty() {
				bs.FreeExtent(ext.GetID())
			}

			return nil
		}
	}

	return ErrPageNotFound
}

// GetPageCount 获取页面数量
func (bs *BaseSegment) GetPageCount() uint32 {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return bs.stats.TotalPages.Load()
}

// GetFreePages 获取空闲页面列表
func (bs *BaseSegment) GetFreePages() []uint32 {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	freePages := make([]uint32, 0, len(bs.freePages))
	for pageNo := range bs.freePages {
		freePages = append(freePages, pageNo)
	}
	return freePages
}

// AllocateExtent 分配新区
func (bs *BaseSegment) AllocateExtent() (basic.Extent, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// 创建新区
	ext := extent3.NewBaseExtent(bs.header.SpaceID, bs.header.ExtentCount, basic.ExtentType(bs.header.SegmentType))

	// 添加到区列表
	if err := bs.extents.Add(ext); err != nil {
		return nil, err
	}

	bs.header.ExtentCount++
	bs.stats.ExtentCount.Add(1)

	return ext, nil
}

// FreeExtent 释放区
func (bs *BaseSegment) FreeExtent(extentID uint32) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// 从区列表中移除
	if err := bs.extents.Remove(extentID); err != nil {
		return err
	}

	bs.stats.ExtentCount.Add(^uint32(0))

	return nil
}

// GetExtentCount 获取区数量
func (bs *BaseSegment) GetExtentCount() uint32 {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return bs.stats.ExtentCount.Load()
}

// GetExtents 获取所有区
func (bs *BaseSegment) GetExtents() []uint32 {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	extents := bs.extents.GetAll()
	result := make([]uint32, len(extents))
	for i, ext := range extents {
		result[i] = ext.GetID()
	}
	return result
}

// GetStats 获取统计信息
func (bs *BaseSegment) GetStats() *SegmentStats {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return &bs.stats
}

// Defragment 碎片整理
func (bs *BaseSegment) Defragment() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// 遍历所有区进行碎片整理
	iter := bs.extents.Iterator()
	for iter.Next() {
		ext := iter.Current()
		if err := ext.Defragment(); err != nil {
			return err
		}
	}

	bs.stats.LastDefragged = time.Now().UnixNano()
	return nil
}

// Extend 扩展段
func (bs *BaseSegment) Extend(pages uint32) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// 计算需要的区数量
	extentsNeeded := (pages + 63) / 64 // 向上取整

	// 分配新区
	for i := uint32(0); i < extentsNeeded; i++ {
		if _, err := bs.AllocateExtent(); err != nil {
			return err
		}
	}

	bs.stats.LastExtended = time.Now().UnixNano()
	return nil
}

// Truncate 截断段
func (bs *BaseSegment) Truncate(pages uint32) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	currentPages := bs.stats.TotalPages.Load()
	if pages >= currentPages {
		return nil
	}

	// 释放多余的页面
	pagesToFree := currentPages - pages
	for pageNo := range bs.freePages {
		if pagesToFree == 0 {
			break
		}
		delete(bs.freePages, pageNo)
		pagesToFree--
	}

	// 如果还需要释放更多页面，从区中释放
	if pagesToFree > 0 {
		iter := bs.extents.Iterator()
		for iter.Next() && pagesToFree > 0 {
			ext := iter.Current()
			pageCount := ext.GetPageCount()
			if pageCount <= pagesToFree {
				bs.FreeExtent(ext.GetID())
				pagesToFree -= pageCount
			}
		}
	}

	return nil
}

// Lock 加锁
func (bs *BaseSegment) Lock() {
	bs.mu.Lock()
}

// Unlock 解锁
func (bs *BaseSegment) Unlock() {
	bs.mu.Unlock()
}

// RLock 读锁
func (bs *BaseSegment) RLock() {
	bs.mu.RLock()
}

// RUnlock 读解锁
func (bs *BaseSegment) RUnlock() {
	bs.mu.RUnlock()
}
