package manager

import (
	"fmt"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/extent"
)

// SegmentManager 管理表空间中的段
type SegmentManager struct {
	mu sync.RWMutex

	// 段映射: segment_id -> segment
	segments map[uint32]*SegmentImpl

	// 缓冲池
	bufferPool *buffer_pool.BufferPool

	// 区管理器
	extentManager *ExtentManager

	// 统计信息
	stats *SegmentStats
}

// SegmentImpl 表示一个段，增强Fragment和Extent链表管理
type SegmentImpl struct {
	basic.Segment
	SegmentID uint32 // 段ID
	SpaceID   uint32 // 所属表空间ID
	Type      uint8  // 段类型(数据段/索引段/Undo段/BLOB段)

	// Fragment管理（前32个页面）
	FragmentPages [32]bool // Fragment页面使用情况
	FragmentUsed  uint32   // 已使用的Fragment页面数
	FragmentFull  bool     // Fragment是否已满

	// Extent链表管理
	FreeExtents    []*extent.BaseExtent // 完全空闲的Extent链表
	NotFullExtents []*extent.BaseExtent // 部分使用的Extent链表
	FullExtents    []*extent.BaseExtent // 完全使用的Extent链表

	// 统计信息
	FreeSpace   uint64 // 空闲空间
	PageCount   uint32 // 页面数量
	ExtentCount uint32 // Extent数量

	IsTemporary bool               // 是否临时段
	LastExtent  *extent.BaseExtent // 最后一个Extent（优化分配）
}

// SegmentStats 段统计信息
type SegmentStats struct {
	TotalSegments     uint32  // 总段数
	TotalPages        uint32  // 总页面数
	TotalExtents      uint32  // 总区数
	FreeSpace         uint64  // 总空闲空间
	FragmentationRate float64 // 碎片率
}

// 段类型常量
const (
	SEGMENT_TYPE_DATA  uint8 = iota // 数据段
	SEGMENT_TYPE_INDEX              // 索引段
	SEGMENT_TYPE_UNDO               // Undo段
	SEGMENT_TYPE_BLOB               // BLOB段
)

// Fragment分配常量
const (
	FragmentPageCount = 32    // Fragment页面数量
	PageSize          = 16384 // 页面大小16KB
)

// NewSegmentManager 创建段管理器
func NewSegmentManager(bp *buffer_pool.BufferPool) *SegmentManager {
	return &SegmentManager{
		segments:      make(map[uint32]*SegmentImpl),
		bufferPool:    bp,
		extentManager: NewExtentManager(bp),
		stats:         &SegmentStats{},
	}
}

// CreateSegment 创建新段，根据段类型选择分配策略
func (sm *SegmentManager) CreateSegment(spaceID uint32, segType uint8, isTemp bool) (basic.Segment, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 生成段ID
	segID := uint32(len(sm.segments) + 1)

	// 创建段
	seg := &SegmentImpl{
		SegmentID:      segID,
		SpaceID:        spaceID,
		Type:           segType,
		FragmentPages:  [32]bool{},
		FragmentUsed:   0,
		FragmentFull:   false,
		FreeExtents:    make([]*extent.BaseExtent, 0),
		NotFullExtents: make([]*extent.BaseExtent, 0),
		FullExtents:    make([]*extent.BaseExtent, 0),
		IsTemporary:    isTemp,
	}

	// 根据段类型分配初始空间
	switch segType {
	case SEGMENT_TYPE_DATA:
		// 数据段：使用Fragment页面，不立即分配Extent
		seg.PageCount = 0
		seg.FreeSpace = FragmentPageCount * PageSize

	case SEGMENT_TYPE_INDEX:
		// 索引段：优先分配完整Extent
		ext, err := sm.extentManager.AllocateExtent(spaceID, basic.ExtentTypeIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to allocate extent for index segment: %v", err)
		}
		seg.NotFullExtents = append(seg.NotFullExtents, ext)
		seg.LastExtent = ext
		seg.ExtentCount = 1
		seg.PageCount = PagesPerExtent
		seg.FreeSpace = uint64(PagesPerExtent * PageSize)

	case SEGMENT_TYPE_UNDO:
		// Undo段：分配固定大小的Extent
		ext, err := sm.extentManager.AllocateExtent(spaceID, basic.ExtentTypeSystem)
		if err != nil {
			return nil, fmt.Errorf("failed to allocate extent for undo segment: %v", err)
		}
		seg.FreeExtents = append(seg.FreeExtents, ext)
		seg.LastExtent = ext
		seg.ExtentCount = 1
		seg.PageCount = PagesPerExtent
		seg.FreeSpace = uint64(PagesPerExtent * PageSize)

	case SEGMENT_TYPE_BLOB:
		// BLOB段：按需分配
		seg.PageCount = 0
		seg.FreeSpace = 0

	default:
		return nil, fmt.Errorf("unknown segment type: %d", segType)
	}

	// 保存段
	sm.segments[segID] = seg

	// 更新统计信息
	sm.stats.TotalSegments++
	sm.stats.TotalExtents += seg.ExtentCount
	sm.stats.TotalPages += seg.PageCount
	sm.stats.FreeSpace += seg.FreeSpace

	return seg, nil
}

// GetSegment 获取段
func (sm *SegmentManager) GetSegment(segID uint32) basic.Segment {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.segments[segID]
}

// AllocatePage 在段中分配新页面，根据段类型使用不同策略
func (sm *SegmentManager) AllocatePage(segID uint32) (uint32, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 获取段
	seg := sm.segments[segID]
	if seg == nil {
		return 0, ErrSegmentNotFound
	}

	// 根据段类型选择分配策略
	switch seg.Type {
	case SEGMENT_TYPE_DATA:
		return sm.allocatePageForDataSegment(seg)
	case SEGMENT_TYPE_INDEX:
		return sm.allocatePageForIndexSegment(seg)
	case SEGMENT_TYPE_UNDO:
		return sm.allocatePageForUndoSegment(seg)
	case SEGMENT_TYPE_BLOB:
		return sm.allocatePageForBlobSegment(seg)
	default:
		return 0, fmt.Errorf("unknown segment type: %d", seg.Type)
	}
}

// allocatePageForDataSegment 从数据段分配页面（优先使用Fragment）
func (sm *SegmentManager) allocatePageForDataSegment(seg *SegmentImpl) (uint32, error) {
	// 首先尝试从Fragment分配
	if !seg.FragmentFull {
		for i := 0; i < FragmentPageCount; i++ {
			if !seg.FragmentPages[i] {
				seg.FragmentPages[i] = true
				seg.FragmentUsed++
				seg.PageCount++
				seg.FreeSpace -= PageSize

				if seg.FragmentUsed >= FragmentPageCount {
					seg.FragmentFull = true
				}

				// 更新统计
				sm.stats.TotalPages++
				sm.stats.FreeSpace -= PageSize

				return uint32(i), nil
			}
		}
	}

	// Fragment已满，从Extent分配
	return sm.allocatePageFromExtent(seg)
}

// allocatePageForIndexSegment 从索引段分配页面（优先Extent）
func (sm *SegmentManager) allocatePageForIndexSegment(seg *SegmentImpl) (uint32, error) {
	return sm.allocatePageFromExtent(seg)
}

// allocatePageForUndoSegment 从Undo段分配页面（循环复用）
func (sm *SegmentManager) allocatePageForUndoSegment(seg *SegmentImpl) (uint32, error) {
	// 优先从空闲Extent分配
	if len(seg.FreeExtents) > 0 {
		ext := seg.FreeExtents[0]
		pageNo, err := ext.AllocatePage()
		if err != nil {
			return 0, err
		}

		// 如果Extent不再空闲，移动到NotFull链表
		if !ext.IsEmpty() {
			seg.FreeExtents = seg.FreeExtents[1:]
			seg.NotFullExtents = append(seg.NotFullExtents, ext)
		}

		seg.PageCount++
		seg.FreeSpace -= PageSize
		sm.stats.TotalPages++
		sm.stats.FreeSpace -= PageSize

		return pageNo, nil
	}

	// 从NotFull分配
	return sm.allocatePageFromExtent(seg)
}

// allocatePageForBlobSegment 从BLOB段分配页面（按需分配）
func (sm *SegmentManager) allocatePageForBlobSegment(seg *SegmentImpl) (uint32, error) {
	return sm.allocatePageFromExtent(seg)
}

// allocatePageFromExtent 从Extent分配页面
func (sm *SegmentManager) allocatePageFromExtent(seg *SegmentImpl) (uint32, error) {
	// 优先从 NotFull Extent 分配
	if len(seg.NotFullExtents) > 0 {
		ext := seg.NotFullExtents[0]
		pageNo, err := ext.AllocatePage()
		if err == nil {
			// 检查Extent是否已满
			if ext.IsFull() {
				// 从 NotFull 移动到 Full
				seg.NotFullExtents = seg.NotFullExtents[1:]
				seg.FullExtents = append(seg.FullExtents, ext)
			}

			seg.PageCount++
			seg.FreeSpace -= PageSize
			sm.stats.TotalPages++
			sm.stats.FreeSpace -= PageSize

			return pageNo, nil
		}
	}

	// 从 Free Extent 分配
	if len(seg.FreeExtents) > 0 {
		ext := seg.FreeExtents[0]
		pageNo, err := ext.AllocatePage()
		if err == nil {
			// 从 Free 移动到 NotFull
			seg.FreeExtents = seg.FreeExtents[1:]
			seg.NotFullExtents = append(seg.NotFullExtents, ext)
			seg.LastExtent = ext

			seg.PageCount++
			seg.FreeSpace -= PageSize
			sm.stats.TotalPages++
			sm.stats.FreeSpace -= PageSize

			return pageNo, nil
		}
	}

	// 所有Extent已满，分配新Extent
	extType := sm.getExtentType(seg.Type)
	ext, err := sm.extentManager.AllocateExtent(seg.SpaceID, extType)
	if err != nil {
		return 0, fmt.Errorf("failed to allocate new extent: %v", err)
	}

	// 添加到 NotFull 链表
	seg.NotFullExtents = append(seg.NotFullExtents, ext)
	seg.LastExtent = ext
	seg.ExtentCount++
	sm.stats.TotalExtents++

	// 分配页面
	return ext.AllocatePage()
}

// getExtentType 根据段类型获取Extent类型
func (sm *SegmentManager) getExtentType(segType uint8) basic.ExtentType {
	switch segType {
	case SEGMENT_TYPE_DATA:
		return basic.ExtentTypeData
	case SEGMENT_TYPE_INDEX:
		return basic.ExtentTypeIndex
	case SEGMENT_TYPE_UNDO:
		return basic.ExtentTypeSystem // Undo uses system extent type
	case SEGMENT_TYPE_BLOB:
		return basic.ExtentTypeData // BLOB uses data extent type
	default:
		return basic.ExtentTypeData
	}
}

// FreePage 释放页面，维护Extent链表
func (sm *SegmentManager) FreePage(segID uint32, pageNo uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 获取段
	seg := sm.segments[segID]
	if seg == nil {
		return ErrSegmentNotFound
	}

	// 检查是否是Fragment页面
	if pageNo < FragmentPageCount && seg.Type == SEGMENT_TYPE_DATA {
		if seg.FragmentPages[pageNo] {
			seg.FragmentPages[pageNo] = false
			seg.FragmentUsed--
			seg.FragmentFull = false
			seg.PageCount--
			seg.FreeSpace += PageSize
			sm.stats.TotalPages--
			sm.stats.FreeSpace += PageSize
			return nil
		}
		return fmt.Errorf("fragment page %d is not allocated", pageNo)
	}

	// 找到页面所在的Extent
	extentID := pageNo / PagesPerExtent

	// 搜索所有Extent链表
	extent := sm.findExtentInLists(seg, extentID)
	if extent == nil {
		return fmt.Errorf("extent %d not found for page %d", extentID, pageNo)
	}

	// 记录Extent原来的状态
	wasFull := extent.IsFull()
	wasNotFull := !extent.IsEmpty() && !extent.IsFull()

	// 释放页面
	if err := extent.FreePage(pageNo); err != nil {
		return err
	}

	// 更新统计
	seg.PageCount--
	seg.FreeSpace += PageSize
	sm.stats.TotalPages--
	sm.stats.FreeSpace += PageSize

	// 更新Extent链表
	isEmpty := extent.IsEmpty()
	isFull := extent.IsFull()

	if wasFull && !isFull {
		// 从Full移动到NotFull
		sm.moveExtent(seg, extent, &seg.FullExtents, &seg.NotFullExtents)
	} else if wasNotFull && isEmpty {
		// 从NotFull移动到Free
		sm.moveExtent(seg, extent, &seg.NotFullExtents, &seg.FreeExtents)

		// 如果不是临时段且Free Extent过多，考虑释放
		if !seg.IsTemporary && len(seg.FreeExtents) > 2 {
			sm.tryFreeExtent(seg, extent)
		}
	}

	return nil
}

// findExtentInLists 在所有Extent链表中查找Extent
func (sm *SegmentManager) findExtentInLists(seg *SegmentImpl, extentID uint32) *extent.BaseExtent {
	// 搜索Free链表
	for _, ext := range seg.FreeExtents {
		if ext.GetID() == extentID {
			return ext
		}
	}

	// 搜索NotFull链表
	for _, ext := range seg.NotFullExtents {
		if ext.GetID() == extentID {
			return ext
		}
	}

	// 搜索Full链表
	for _, ext := range seg.FullExtents {
		if ext.GetID() == extentID {
			return ext
		}
	}

	return nil
}

// moveExtent 在Extent链表之间移动Extent
func (sm *SegmentManager) moveExtent(seg *SegmentImpl, ext *extent.BaseExtent,
	from *[]*extent.BaseExtent, to *[]*extent.BaseExtent) {
	// 从from移除
	for i, e := range *from {
		if e.GetID() == ext.GetID() {
			*from = append((*from)[:i], (*from)[i+1:]...)
			break
		}
	}

	// 添加到to
	*to = append(*to, ext)
}

// DropSegment 删除段
func (sm *SegmentManager) DropSegment(segID uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	seg := sm.segments[segID]
	if seg == nil {
		return ErrSegmentNotFound
	}

	// 释放所有区（合并所有extent列表）
	allExtents := append([]*extent.BaseExtent{}, seg.FreeExtents...)
	allExtents = append(allExtents, seg.NotFullExtents...)
	allExtents = append(allExtents, seg.FullExtents...)

	for _, ext := range allExtents {
		if err := sm.extentManager.FreeExtent(ext.GetID()); err != nil {
			return err
		}
	}

	delete(sm.segments, segID)
	return nil
}

// GetFreeSpace 获取段剩余空间
func (sm *SegmentManager) GetFreeSpace(segID uint32) uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	seg := sm.segments[segID]
	if seg == nil {
		return 0
	}
	return seg.FreeSpace
}

// Close 关闭段管理器
func (sm *SegmentManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 释放所有段
	for _, seg := range sm.segments {
		// 合并所有extent列表
		allExtents := append([]*extent.BaseExtent{}, seg.FreeExtents...)
		allExtents = append(allExtents, seg.NotFullExtents...)
		allExtents = append(allExtents, seg.FullExtents...)

		for _, ext := range allExtents {
			if err := sm.extentManager.FreeExtent(ext.GetID()); err != nil {
				return err
			}
		}
	}

	// 清空映射
	sm.segments = make(map[uint32]*SegmentImpl)
	sm.stats = &SegmentStats{}

	return nil
}

func (sm *SegmentManager) tryFreeExtent(seg *SegmentImpl, ext basic.Extent) {
	// 仅当Free Extent超过2个时才释放（保留一些空闲Extent以便快速分配）
	if len(seg.FreeExtents) > 2 {
		// 从Free链表移除
		for i, e := range seg.FreeExtents {
			if e.GetID() == ext.GetID() {
				seg.FreeExtents = append(seg.FreeExtents[:i], seg.FreeExtents[i+1:]...)
				break
			}
		}

		// 释放Extent
		sm.extentManager.FreeExtent(ext.GetID())
		seg.ExtentCount--
		sm.stats.TotalExtents--
	}
}
