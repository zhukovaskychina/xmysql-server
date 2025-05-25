package segs

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"xmysql-server/server/innodb/storage/store/extents"
)

func TestNewSegment(t *testing.T) {
	segment := NewSegment(1, SEG_TYPE_DATA, 1)

	assert.Equal(t, uint64(1), segment.ID)
	assert.Equal(t, uint8(SEG_TYPE_DATA), segment.Type)
	assert.Equal(t, uint32(1), segment.SpaceID)
	assert.Empty(t, segment.FreeExtents)
	assert.Empty(t, segment.FragExtents)
	assert.Empty(t, segment.FullExtents)
}

func TestSegment_AllocateExtent(t *testing.T) {
	segment := NewSegment(1, SEG_TYPE_DATA, 1)

	// 分配第一个Extent
	err := segment.AllocateExtent(0)
	assert.NoError(t, err)
	assert.Len(t, segment.FreeExtents, 1)
	assert.Equal(t, uint32(extents.PAGES_PER_EXTENT), segment.TotalPages)
	assert.Equal(t, uint32(extents.PAGES_PER_EXTENT), segment.FreePages)

	// 验证Extent属性
	extent := segment.FreeExtents[0]
	assert.Equal(t, uint64(1), extent.GetSegmentID())
	assert.Equal(t, uint32(0), extent.FirstPageNo)
	assert.Equal(t, uint8(extents.EXTENT_FREE), extent.GetState())
}

func TestSegment_AllocatePage(t *testing.T) {
	segment := NewSegment(1, SEG_TYPE_DATA, 1)

	// 没有Extent时分配页面应该失败
	_, err := segment.AllocatePage()
	assert.Error(t, err)

	// 分配一个Extent
	segment.AllocateExtent(0)

	// 分配第一个页面
	pageNo, err := segment.AllocatePage()
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), pageNo)
	assert.Empty(t, segment.FreeExtents)
	assert.Len(t, segment.FragExtents, 1)
	assert.Equal(t, uint32(extents.PAGES_PER_EXTENT-1), segment.FreePages)

	// 分配所有剩余页面
	for i := uint32(1); i < extents.PAGES_PER_EXTENT; i++ {
		pageNo, err = segment.AllocatePage()
		assert.NoError(t, err)
		assert.Equal(t, i, pageNo)
	}

	// Extent应该已满
	assert.Empty(t, segment.FragExtents)
	assert.Len(t, segment.FullExtents, 1)
	assert.Equal(t, uint32(0), segment.FreePages)

	// 再次分配应该失败
	_, err = segment.AllocatePage()
	assert.Error(t, err)
}

func TestSegment_FreePage(t *testing.T) {
	segment := NewSegment(1, SEG_TYPE_DATA, 1)
	segment.AllocateExtent(0)

	// 分配一些页面
	pageNo, _ := segment.AllocatePage()
	segment.AllocatePage()

	// 释放一个页面
	err := segment.FreePage(pageNo)
	assert.NoError(t, err)
	assert.Len(t, segment.FragExtents, 1)
	assert.Equal(t, uint32(extents.PAGES_PER_EXTENT-1), segment.FreePages)

	// 释放不存在的页面
	err = segment.FreePage(1000)
	assert.Error(t, err)

	// 释放所有已分配页面
	segment.FreePage(1)
	assert.Len(t, segment.FreeExtents, 1)
	assert.Empty(t, segment.FragExtents)
	assert.Equal(t, uint32(extents.PAGES_PER_EXTENT), segment.FreePages)
}

func TestSegmentHeader(t *testing.T) {
	header := NewSegmentHeader(1, 2, 3)

	// 验证序列化
	bytes := header.GetBytes()
	assert.Len(t, bytes, 10) // 4 + 4 + 2 字节

	// 验证数据
	assert.Equal(t, uint32(1), header.GetSpaceID())
	assert.Equal(t, uint32(2), header.GetPageNumber())
	assert.Equal(t, uint16(3*192+50), header.GetByteOffset())
}

func TestSegment_ExtentManagement(t *testing.T) {
	segment := NewSegment(1, SEG_TYPE_DATA, 1)

	// 分配多个Extent
	segment.AllocateExtent(0)
	segment.AllocateExtent(64)

	// 验证Extent链表管理
	assert.Len(t, segment.FreeExtents, 2)

	// 分配一些页面使Extent部分使用
	segment.AllocatePage()
	assert.Len(t, segment.FreeExtents, 1)
	assert.Len(t, segment.FragExtents, 1)

	// 填满第一个Extent
	for i := 0; i < 63; i++ {
		segment.AllocatePage()
	}

	assert.Len(t, segment.FreeExtents, 1)
	assert.Empty(t, segment.FragExtents)
	assert.Len(t, segment.FullExtents, 1)
}

func TestSegment_PageAllocationStrategy(t *testing.T) {
	segment := NewSegment(1, SEG_TYPE_DATA, 1)
	segment.AllocateExtent(0)
	segment.AllocateExtent(64)

	// 分配一些页面后释放
	page1, _ := segment.AllocatePage()
	page2, _ := segment.AllocatePage()
	segment.FreePage(page1)

	// 新的分配应该优先使用已释放的页面
	newPage, _ := segment.AllocatePage()
	assert.Equal(t, page1, newPage)

	// 继续使用同一个Extent中的空闲页面
	segment.FreePage(page2)
	newPage, _ = segment.AllocatePage()
	assert.Equal(t, page2, newPage)
}
