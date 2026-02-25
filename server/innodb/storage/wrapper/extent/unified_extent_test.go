package extent

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/extents"
)

func TestNewUnifiedExtent(t *testing.T) {
	id := uint32(1)
	spaceID := uint32(0)
	startPage := uint32(64)
	extType := basic.ExtentTypeData
	purpose := basic.ExtentPurposeData

	ue := NewUnifiedExtent(id, spaceID, startPage, extType, purpose)

	assert.NotNil(t, ue)
	assert.Equal(t, id, ue.GetID())
	assert.Equal(t, spaceID, ue.GetSpaceID())
	assert.Equal(t, startPage, ue.GetStartPage())
	assert.Equal(t, extType, ue.GetType())
	assert.Equal(t, purpose, ue.GetPurpose())
	assert.Equal(t, basic.ExtentStateFree, ue.GetState())
	assert.Equal(t, uint32(0), ue.GetPageCount())
	assert.Equal(t, uint64(PagesPerExtent*PageSize), ue.GetFreeSpace())
	assert.True(t, ue.IsEmpty())
	assert.False(t, ue.IsFull())
}

func TestUnifiedExtent_AllocatePage(t *testing.T) {
	ue := NewUnifiedExtent(1, 0, 64, basic.ExtentTypeData, basic.ExtentPurposeData)

	// Allocate first page
	pageNo, err := ue.AllocatePage()
	assert.NoError(t, err)
	assert.Equal(t, uint32(64), pageNo)
	assert.Equal(t, basic.ExtentStatePartial, ue.GetState())
	assert.Equal(t, uint32(1), ue.GetPageCount())
	assert.False(t, ue.IsEmpty())
	assert.False(t, ue.IsFull())

	// Allocate second page
	pageNo, err = ue.AllocatePage()
	assert.NoError(t, err)
	assert.Equal(t, uint32(65), pageNo)
	assert.Equal(t, uint32(2), ue.GetPageCount())

	// Verify page is allocated
	assert.True(t, ue.IsPageAllocated(64))
	assert.True(t, ue.IsPageAllocated(65))
	assert.False(t, ue.IsPageAllocated(66))
}

func TestUnifiedExtent_AllocateAllPages(t *testing.T) {
	ue := NewUnifiedExtent(1, 0, 0, basic.ExtentTypeData, basic.ExtentPurposeData)

	// Allocate all pages
	for i := 0; i < PagesPerExtent; i++ {
		pageNo, err := ue.AllocatePage()
		assert.NoError(t, err)
		assert.Equal(t, uint32(i), pageNo)
	}

	// Verify extent is full
	assert.True(t, ue.IsFull())
	assert.Equal(t, basic.ExtentStateFull, ue.GetState())
	assert.Equal(t, uint32(PagesPerExtent), ue.GetPageCount())
	assert.Equal(t, uint64(0), ue.GetFreeSpace())

	// Try to allocate one more page (should fail)
	_, err := ue.AllocatePage()
	assert.Error(t, err)
	assert.Equal(t, ErrExtentFull, err)
}

func TestUnifiedExtent_FreePage(t *testing.T) {
	ue := NewUnifiedExtent(1, 0, 64, basic.ExtentTypeData, basic.ExtentPurposeData)

	// Allocate some pages
	page1, _ := ue.AllocatePage()
	page2, _ := ue.AllocatePage()
	page3, _ := ue.AllocatePage()

	assert.Equal(t, uint32(3), ue.GetPageCount())

	// Free middle page
	err := ue.FreePage(page2)
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), ue.GetPageCount())
	assert.False(t, ue.IsPageAllocated(page2))
	assert.True(t, ue.IsPageAllocated(page1))
	assert.True(t, ue.IsPageAllocated(page3))

	// Free remaining pages
	err = ue.FreePage(page1)
	assert.NoError(t, err)
	err = ue.FreePage(page3)
	assert.NoError(t, err)

	// Verify extent is empty
	assert.True(t, ue.IsEmpty())
	assert.Equal(t, basic.ExtentStateFree, ue.GetState())
	assert.Equal(t, uint32(0), ue.GetPageCount())
}

func TestUnifiedExtent_FreePageErrors(t *testing.T) {
	ue := NewUnifiedExtent(1, 0, 64, basic.ExtentTypeData, basic.ExtentPurposeData)

	// Try to free unallocated page
	err := ue.FreePage(64)
	assert.Error(t, err)
	assert.Equal(t, ErrPageNotFound, err)

	// Try to free page outside extent range
	err = ue.FreePage(0)
	assert.Error(t, err)
	assert.Equal(t, ErrPageNotFound, err)

	err = ue.FreePage(128)
	assert.Error(t, err)
	assert.Equal(t, ErrPageNotFound, err)

	// Allocate and free a page
	pageNo, _ := ue.AllocatePage()
	err = ue.FreePage(pageNo)
	assert.NoError(t, err)

	// Try to free the same page again
	err = ue.FreePage(pageNo)
	assert.Error(t, err)
}

func TestUnifiedExtent_GetFreePages(t *testing.T) {
	ue := NewUnifiedExtent(1, 0, 0, basic.ExtentTypeData, basic.ExtentPurposeData)

	// Initially all pages are free
	freePages := ue.GetFreePages()
	assert.Equal(t, PagesPerExtent, len(freePages))

	// Allocate some pages
	ue.AllocatePage() // page 0
	ue.AllocatePage() // page 1
	ue.AllocatePage() // page 2

	freePages = ue.GetFreePages()
	assert.Equal(t, PagesPerExtent-3, len(freePages))
	assert.Equal(t, uint32(3), freePages[0]) // First free page should be 3
}

func TestUnifiedExtent_GetAllocatedPages(t *testing.T) {
	ue := NewUnifiedExtent(1, 0, 0, basic.ExtentTypeData, basic.ExtentPurposeData)

	// Initially no pages are allocated
	allocatedPages := ue.GetAllocatedPages()
	assert.Equal(t, 0, len(allocatedPages))

	// Allocate some pages
	page1, _ := ue.AllocatePage()
	page2, _ := ue.AllocatePage()
	page3, _ := ue.AllocatePage()

	allocatedPages = ue.GetAllocatedPages()
	assert.Equal(t, 3, len(allocatedPages))
	assert.Contains(t, allocatedPages, page1)
	assert.Contains(t, allocatedPages, page2)
	assert.Contains(t, allocatedPages, page3)
}

func TestUnifiedExtent_SegmentID(t *testing.T) {
	ue := NewUnifiedExtent(1, 0, 64, basic.ExtentTypeData, basic.ExtentPurposeData)

	// Initially segment ID is 0
	assert.Equal(t, uint64(0), ue.GetSegmentID())

	// Set segment ID
	ue.SetSegmentID(12345)
	assert.Equal(t, uint64(12345), ue.GetSegmentID())

	// Verify it's also set in the entry
	assert.Equal(t, uint64(12345), ue.entry.GetSegmentID())
}

func TestUnifiedExtent_Reset(t *testing.T) {
	ue := NewUnifiedExtent(1, 0, 64, basic.ExtentTypeData, basic.ExtentPurposeData)

	// Allocate some pages and set segment ID
	ue.SetSegmentID(999)
	ue.AllocatePage()
	ue.AllocatePage()
	ue.AllocatePage()

	assert.Equal(t, uint32(3), ue.GetPageCount())
	assert.Equal(t, basic.ExtentStatePartial, ue.GetState())

	// Reset the extent
	err := ue.Reset()
	assert.NoError(t, err)

	// Verify extent is reset
	assert.Equal(t, uint32(0), ue.GetPageCount())
	assert.Equal(t, basic.ExtentStateFree, ue.GetState())
	assert.True(t, ue.IsEmpty())
	assert.Equal(t, uint64(PagesPerExtent*PageSize), ue.GetFreeSpace())

	// Segment ID should be preserved
	assert.Equal(t, uint64(999), ue.GetSegmentID())
}

func TestUnifiedExtent_Serialization(t *testing.T) {
	ue := NewUnifiedExtent(1, 0, 128, basic.ExtentTypeData, basic.ExtentPurposeData)

	// Set up extent
	ue.SetSegmentID(12345)
	ue.AllocatePage() // page 128
	ue.AllocatePage() // page 129
	ue.AllocatePage() // page 130

	// Serialize
	data := ue.Serialize()
	assert.Equal(t, 32, len(data))

	// Deserialize into new entry
	newEntry, err := extents.DeserializeExtentEntry(data)
	assert.NoError(t, err)

	// Create new unified extent from entry
	newUE := NewUnifiedExtentFromEntry(newEntry, 1, 0, basic.ExtentTypeData, basic.ExtentPurposeData)

	// Verify data consistency
	assert.Equal(t, ue.GetSegmentID(), newUE.GetSegmentID())
	assert.Equal(t, ue.GetState(), newUE.GetState())
	assert.Equal(t, ue.GetPageCount(), newUE.GetPageCount())
	assert.Equal(t, ue.GetStartPage(), newUE.GetStartPage())
	assert.Equal(t, ue.IsPageAllocated(128), newUE.IsPageAllocated(128))
	assert.Equal(t, ue.IsPageAllocated(129), newUE.IsPageAllocated(129))
	assert.Equal(t, ue.IsPageAllocated(130), newUE.IsPageAllocated(130))
	assert.Equal(t, ue.IsPageAllocated(131), newUE.IsPageAllocated(131))
}

func TestUnifiedExtent_GetBitmap(t *testing.T) {
	ue := NewUnifiedExtent(1, 0, 0, basic.ExtentTypeData, basic.ExtentPurposeData)

	// Allocate some pages
	ue.AllocatePage() // page 0
	ue.AllocatePage() // page 1

	bitmap := ue.GetBitmap()
	assert.Equal(t, 16, len(bitmap))

	// Verify bitmap is not empty
	hasNonZero := false
	for _, b := range bitmap {
		if b != 0 {
			hasNonZero = true
			break
		}
	}
	assert.True(t, hasNonZero, "Bitmap should have non-zero bytes after allocation")
}

func TestUnifiedExtent_Stats(t *testing.T) {
	ue := NewUnifiedExtent(1, 0, 64, basic.ExtentTypeData, basic.ExtentPurposeData)

	stats := ue.GetStats()
	assert.NotNil(t, stats)
	assert.Equal(t, uint32(PagesPerExtent), stats.TotalPages)
	assert.Equal(t, uint32(PagesPerExtent), stats.FreePages)
	assert.Equal(t, int64(0), stats.LastAllocated)

	// Allocate a page
	ue.AllocatePage()

	stats = ue.GetStats()
	assert.Equal(t, uint32(PagesPerExtent-1), stats.FreePages)
	assert.NotEqual(t, int64(0), stats.LastAllocated)
}

func TestUnifiedExtent_Concurrency(t *testing.T) {
	ue := NewUnifiedExtent(1, 0, 0, basic.ExtentTypeData, basic.ExtentPurposeData)

	// Test that locking methods don't panic
	ue.Lock()
	ue.Unlock()

	ue.RLock()
	ue.RUnlock()

	// This test mainly ensures the methods exist and work
	assert.NotNil(t, ue)
}
