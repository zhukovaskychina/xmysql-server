package manager

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/extent"
	"sync"
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

// Segment 表示一个段
type SegmentImpl struct {
	basic.Segment
	SegmentID   uint32               // 段ID
	SpaceID     uint32               // 所属表空间ID
	Type        uint8                // 段类型(数据段/索引段)
	Extents     []*extent.BaseExtent // 区列表
	FreeSpace   uint64               // 空闲空间
	PageCount   uint32               // 页面数量
	IsTemporary bool                 // 是否临时段
	LastExtent  *extent.BaseExtent   // 最后一个区
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

// CreateSegment 创建新段
func (sm *SegmentManager) CreateSegment(spaceID uint32, segType uint8, isTemp bool) (basic.Segment, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 生成段ID
	segID := uint32(len(sm.segments) + 1)

	// 创建段
	seg := &SegmentImpl{
		SegmentID:   segID,
		SpaceID:     spaceID,
		Type:        segType,
		Extents:     make([]*extent.BaseExtent, 0),
		IsTemporary: isTemp,
	}

	// 分配初始区
	extType := basic.ExtentTypeData
	if segType == SEGMENT_TYPE_INDEX {
		extType = basic.ExtentTypeIndex
	}

	ext, err := sm.extentManager.AllocateExtent(spaceID, extType)
	if err != nil {
		return nil, err
	}

	seg.Extents = append(seg.Extents, ext)
	seg.LastExtent = ext
	seg.FreeSpace = ext.GetFreeSpace()
	seg.PageCount = ext.GetPageCount()

	// 保存段
	sm.segments[segID] = seg

	// 更新统计信息
	sm.stats.TotalSegments++
	sm.stats.TotalExtents++
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

// AllocatePage 在段中分配新页面
func (sm *SegmentManager) AllocatePage(segID uint32) (uint32, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 获取段
	seg := sm.segments[segID]
	if seg == nil {
		return 0, ErrSegmentNotFound
	}

	// 尝试在最后一个区分配页面
	if seg.LastExtent != nil {
		pageNo, err := seg.LastExtent.AllocatePage()
		if err == nil {
			// 更新统计
			seg.PageCount++
			seg.FreeSpace -= 16 * 1024 // 16KB per page
			sm.stats.TotalPages++
			sm.stats.FreeSpace -= 16 * 1024
			return pageNo, nil
		}
	}

	// 最后一个区已满，分配新区
	extType := basic.ExtentTypeData
	if seg.Type == SEGMENT_TYPE_INDEX {
		extType = basic.ExtentTypeIndex
	}

	ext, err := sm.extentManager.AllocateExtent(seg.SpaceID, extType)
	if err != nil {
		return 0, err
	}

	// 更新段信息
	seg.Extents = append(seg.Extents, ext)
	seg.LastExtent = ext
	sm.stats.TotalExtents++

	// 在新区分配页面
	return ext.AllocatePage()
}

// FreePage 释放页面
func (sm *SegmentManager) FreePage(segID uint32, pageNo uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 获取段
	seg := sm.segments[segID]
	if seg == nil {
		return ErrSegmentNotFound
	}

	// 找到页面所在的区
	extentID := pageNo / 64 // 每个区64页
	for _, ext := range seg.Extents {
		if ext.GetID() == extentID {
			if err := ext.FreePage(pageNo); err != nil {
				return err
			}

			// 更新统计
			seg.PageCount--
			seg.FreeSpace += 16 * 1024
			sm.stats.TotalPages--
			sm.stats.FreeSpace += 16 * 1024

			// 如果区为空，考虑释放
			if ext.IsEmpty() && !seg.IsTemporary {
				sm.tryFreeExtent(seg, ext)
			}

			return nil
		}
	}

	return ErrPageNotFound
}

// DropSegment 删除段
func (sm *SegmentManager) DropSegment(segID uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	seg := sm.segments[segID]
	if seg == nil {
		return ErrSegmentNotFound
	}

	// 释放所有区
	for _, ext := range seg.Extents {
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
		for _, ext := range seg.Extents {
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
	// TODO: 实现区释放逻辑
}
