package pages

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewAllocatedPage(t *testing.T) {
	pageNo := uint32(1)
	spaceId := uint32(2)
	page := NewAllocatedPage(pageNo, spaceId)

	assert.NotNil(t, page)
	assert.False(t, page.IsInitialized())
	assert.Equal(t, pageNo, page.FileHeader.GetCurrentPageOffset())
	assert.Equal(t, spaceId, page.FileHeader.GetFilePageArch())
	assert.Equal(t, int16(FIL_PAGE_TYPE_ALLOCATED), page.FileHeader.GetPageType())
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
	assert.Equal(t, ErrPageAlreadyInited, err)
}

func TestAllocatedPage_LoadPageBody(t *testing.T) {
	page := NewAllocatedPage(1, 1)

	// Test with invalid size
	err := page.LoadPageBody(make([]byte, 100))
	assert.Equal(t, ErrInvalidPageSize, err)

	// Test with correct size
	content := make([]byte, 16384)
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
	expectedSize := 38 + 16384 + 8 // FileHeader + PageBody + FileTrailer
	assert.Equal(t, expectedSize, len(bytes))
}

func TestAllocatedPage_ValidatePageContent(t *testing.T) {
	page := NewAllocatedPage(1, 1)

	// Valid page should pass validation
	err := page.ValidatePageContent()
	assert.NoError(t, err)

	// Change page type to invalid value
	page.FileHeader.WritePageFileType(999)
	err = page.ValidatePageContent()
	assert.Equal(t, ErrInvalidPageContent, err)
}

func TestAllocatedPage_SetChecksum(t *testing.T) {
	page := NewAllocatedPage(1, 1)
	page.SetChecksum()

	// For now, just verify that checksum can be set without error
	// TODO: Add proper checksum verification once implemented
	checksum := make([]byte, 4)
	assert.NotPanics(t, func() {
		page.SetChecksum()
	})
}
