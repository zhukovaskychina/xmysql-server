package pages

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewAllocatedPage(t *testing.T) {
	spaceID := uint32(2)
	pageNo := uint32(1)
	page := NewAllocatedPage(spaceID, pageNo)

	assert.NotNil(t, page)
	assert.False(t, page.IsInitialized())
	assert.Equal(t, spaceID, page.GetSpaceID())
	assert.Equal(t, pageNo, page.GetPageNo())
	assert.Equal(t, common.FIL_PAGE_TYPE_ALLOCATED, page.GetPageType())
}

func TestAllocatedPage_Initialize(t *testing.T) {
	page := NewAllocatedPage(1, 1)

	// First initialization should succeed
	err := page.Initialize()
	assert.NoError(t, err)
	assert.True(t, page.IsInitialized())

	// Check if page body is zeroed
	body := page.GetPageBody()
	for _, b := range body {
		assert.Equal(t, byte(0), b)
	}

	// Second initialization should fail
	err = page.Initialize()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already initialized")
}

func TestAllocatedPage_LoadPageBody(t *testing.T) {
	page := NewAllocatedPage(1, 1)

	// Test with invalid size
	err := page.LoadPageBody(make([]byte, 100))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page data size")

	// Test with correct size
	content := make([]byte, common.PageSize)
	for i := range content {
		content[i] = byte(i % 256)
	}
	err = page.LoadPageBody(content)
	assert.NoError(t, err)

	// Verify content
	body := page.GetPageBody()
	assert.Equal(t, content, body)
}

func TestAllocatedPage_GetSerializeBytes(t *testing.T) {
	page := NewAllocatedPage(1, 1)
	page.Initialize()

	bytes := page.GetSerializeBytes()
	assert.Equal(t, common.PageSize, len(bytes))
}

func TestAllocatedPage_ValidatePageContent(t *testing.T) {
	page := NewAllocatedPage(1, 1)

	// Valid page should pass validation
	err := page.ValidatePageContent()
	assert.NoError(t, err)

	// Test with invalid page size
	page.data = make([]byte, 100)
	err = page.ValidatePageContent()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page size")
}

func TestAllocatedPage_SetChecksum(t *testing.T) {
	page := NewAllocatedPage(1, 1)

	// For now, just verify that checksum can be set without error
	// TODO: Add proper checksum verification once implemented
	assert.NotPanics(t, func() {
		page.SetChecksum()
	})
}
