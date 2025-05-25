/*
Segment（段）是InnoDB存储引擎中的逻辑空间管理单位

主要类型：
1. 数据段 - 存储表的聚簇索引数据
2. 索引段 - 存储辅助索引的B+Tree结构
3. Undo段 - 存储事务的Undo日志
4. BLOB段 - 存储超大字段数据

物理结构：
- 段的元数据保存在INODE页面中
- 通过Extent（区）管理实际的物理空间
- 维护三个Extent链表：
  * 空闲Extent链表
  * 部分使用的Extent链表
  * 已满的Extent链表
*/

package segs

import (
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/extents"
	"github.com/zhukovaskychina/xmysql-server/util"
)

// Segment类型常量
const (
	SEG_TYPE_DATA  = 1 // 数据段
	SEG_TYPE_INDEX = 2 // 索引段
	SEG_TYPE_UNDO  = 3 // Undo段
	SEG_TYPE_BLOB  = 4 // BLOB段
)

// Segment头部结构
type SegmentHeader struct {
	INodeEntrySpaceId    []byte // 4字节 INode Entry所在的表空间ID
	PageNumberINodeEntry []byte // 4字节 INode Entry所在的页面编号
	ByteOffsetINodeEntry []byte // 2字节 INode Entry在页面中的偏移量
}

// Segment完整结构
type Segment struct {
	Header      *SegmentHeader         // 段头信息
	ID          uint64                 // 段ID
	Type        uint8                  // 段类型
	SpaceID     uint32                 // 所属表空间ID
	FreeExtents []*extents.ExtentEntry // 空闲Extent链表
	FragExtents []*extents.ExtentEntry // 部分使用的Extent链表
	FullExtents []*extents.ExtentEntry // 已满的Extent链表
	TotalPages  uint32                 // 总页面数
	FreePages   uint32                 // 空闲页面数
}

// 创建新的段头
func NewSegmentHeader(spaceId uint32, pageNumber uint32, offset uint16) *SegmentHeader {
	return &SegmentHeader{
		INodeEntrySpaceId:    util.ConvertUInt4Bytes(spaceId),
		PageNumberINodeEntry: util.ConvertUInt4Bytes(pageNumber),
		ByteOffsetINodeEntry: util.ConvertUInt2Bytes(offset*192 + 50),
	}
}

// 创建新的段
func NewSegment(id uint64, segType uint8, spaceID uint32) *Segment {
	return &Segment{
		ID:          id,
		Type:        segType,
		SpaceID:     spaceID,
		FreeExtents: make([]*extents.ExtentEntry, 0),
		FragExtents: make([]*extents.ExtentEntry, 0),
		FullExtents: make([]*extents.ExtentEntry, 0),
	}
}

// 获取段头的序列化字节
func (s *SegmentHeader) GetBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, s.INodeEntrySpaceId...)
	buff = append(buff, s.PageNumberINodeEntry...)
	buff = append(buff, s.ByteOffsetINodeEntry...)
	return buff
}

// 分配新的Extent
func (s *Segment) AllocateExtent(firstPageNo uint32) error {
	extent := extents.NewExtentEntry(firstPageNo)
	extent.SetSegmentID(s.ID)
	s.FreeExtents = append(s.FreeExtents, extent)
	s.TotalPages += extents.PAGES_PER_EXTENT
	s.FreePages += extents.PAGES_PER_EXTENT
	return nil
}

// 分配页面
func (s *Segment) AllocatePage() (uint32, error) {
	// 先从部分使用的Extent中分配
	for _, extent := range s.FragExtents {
		for i := uint8(0); i < extents.PAGES_PER_EXTENT; i++ {
			if extent.IsPageFree(i) {
				if err := extent.AllocatePage(i); err != nil {
					continue
				}

				// 如果Extent已满，移到已满链表
				if extent.GetState() == extents.EXTENT_FULL {
					s.moveExtentToFull(extent)
				}

				s.FreePages--
				return extent.FirstPageNo + uint32(i), nil
			}
		}
	}

	// 如果没有合适的部分使用的Extent，使用空闲Extent
	if len(s.FreeExtents) > 0 {
		extent := s.FreeExtents[0]
		s.FreeExtents = s.FreeExtents[1:]
		s.FragExtents = append(s.FragExtents, extent)

		// 分配第一个页面
		if err := extent.AllocatePage(0); err != nil {
			return 0, err
		}

		s.FreePages--
		return extent.FirstPageNo, nil
	}

	return 0, errors.New("没有可用的页面")
}

// 释放页面
func (s *Segment) FreePage(pageNo uint32) error {
	// 查找页面所在的Extent
	extent := s.findExtentByPageNo(pageNo)
	if extent == nil {
		return errors.New("页面不属于该段")
	}

	// 计算页面在Extent中的偏移
	pageOffset := uint8((pageNo - extent.FirstPageNo) % extents.PAGES_PER_EXTENT)

	// 释放页面
	if err := extent.FreePage(pageOffset); err != nil {
		return err
	}

	// 更新Extent状态
	switch extent.GetState() {
	case extents.EXTENT_FREE:
		s.moveExtentToFree(extent)
	case extents.EXTENT_PARTIAL:
		s.moveExtentToFrag(extent)
	}

	s.FreePages++
	return nil
}

// 在所有链表中查找指定页面所在的Extent
func (s *Segment) findExtentByPageNo(pageNo uint32) *extents.ExtentEntry {
	// 检查部分使用的Extent
	for _, extent := range s.FragExtents {
		if pageNo >= extent.FirstPageNo && pageNo < extent.FirstPageNo+extents.PAGES_PER_EXTENT {
			return extent
		}
	}

	// 检查已满的Extent
	for _, extent := range s.FullExtents {
		if pageNo >= extent.FirstPageNo && pageNo < extent.FirstPageNo+extents.PAGES_PER_EXTENT {
			return extent
		}
	}

	// 检查空闲的Extent
	for _, extent := range s.FreeExtents {
		if pageNo >= extent.FirstPageNo && pageNo < extent.FirstPageNo+extents.PAGES_PER_EXTENT {
			return extent
		}
	}

	return nil
}

// 将Extent移动到空闲链表
func (s *Segment) moveExtentToFree(extent *extents.ExtentEntry) {
	s.removeExtentFromLists(extent)
	s.FreeExtents = append(s.FreeExtents, extent)
}

// 将Extent移动到部分使用链表
func (s *Segment) moveExtentToFrag(extent *extents.ExtentEntry) {
	s.removeExtentFromLists(extent)
	s.FragExtents = append(s.FragExtents, extent)
}

// 将Extent移动到已满链表
func (s *Segment) moveExtentToFull(extent *extents.ExtentEntry) {
	s.removeExtentFromLists(extent)
	s.FullExtents = append(s.FullExtents, extent)
}

// 从所有链表中移除指定的Extent
func (s *Segment) removeExtentFromLists(target *extents.ExtentEntry) {
	// 从空闲链表移除
	newFree := make([]*extents.ExtentEntry, 0)
	for _, extent := range s.FreeExtents {
		if extent != target {
			newFree = append(newFree, extent)
		}
	}
	s.FreeExtents = newFree

	// 从部分使用链表移除
	newFrag := make([]*extents.ExtentEntry, 0)
	for _, extent := range s.FragExtents {
		if extent != target {
			newFrag = append(newFrag, extent)
		}
	}
	s.FragExtents = newFrag

	// 从已满链表移除
	newFull := make([]*extents.ExtentEntry, 0)
	for _, extent := range s.FullExtents {
		if extent != target {
			newFull = append(newFull, extent)
		}
	}
	s.FullExtents = newFull
}
