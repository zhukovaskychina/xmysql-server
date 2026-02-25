package types

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

func TestNewUnifiedPage(t *testing.T) {
	spaceID := uint32(1)
	pageNo := uint32(100)
	pageType := common.FIL_PAGE_INDEX

	page := NewUnifiedPage(spaceID, pageNo, pageType)

	if page == nil {
		t.Fatal("NewUnifiedPage returned nil")
	}

	if page.GetSpaceID() != spaceID {
		t.Errorf("Expected spaceID %d, got %d", spaceID, page.GetSpaceID())
	}

	if page.GetPageNo() != pageNo {
		t.Errorf("Expected pageNo %d, got %d", pageNo, page.GetPageNo())
	}

	if page.GetPageType() != pageType {
		t.Errorf("Expected pageType %d, got %d", pageType, page.GetPageType())
	}

	if page.GetSize() != DefaultPageSize {
		t.Errorf("Expected size %d, got %d", DefaultPageSize, page.GetSize())
	}
}

func TestUnifiedPageInit(t *testing.T) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)

	err := page.Init()
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Check that body is cleared
	body := page.GetBody()
	allZero := true
	for _, b := range body {
		if b != 0 {
			allZero = false
			break
		}
	}

	if !allZero {
		t.Error("Expected body to be all zeros after Init")
	}
}

func TestUnifiedPageDirtyFlag(t *testing.T) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)

	// Initially should not be dirty
	if page.IsDirty() {
		t.Error("New page should not be dirty")
	}

	// Mark as dirty
	page.MarkDirty()
	if !page.IsDirty() {
		t.Error("Page should be dirty after MarkDirty")
	}

	// Clear dirty flag
	page.ClearDirty()
	if page.IsDirty() {
		t.Error("Page should not be dirty after ClearDirty")
	}
}

func TestUnifiedPageState(t *testing.T) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)

	// Check initial state
	if page.GetState() != basic.PageStateActive {
		t.Errorf("Expected initial state %d, got %d", basic.PageStateActive, page.GetState())
	}

	// Set state to dirty
	page.SetState(basic.PageStateDirty)
	if page.GetState() != basic.PageStateDirty {
		t.Errorf("Expected state %d, got %d", basic.PageStateDirty, page.GetState())
	}
}

func TestUnifiedPageLSN(t *testing.T) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)

	// Initial LSN should be 0
	if page.GetLSN() != 0 {
		t.Errorf("Expected initial LSN 0, got %d", page.GetLSN())
	}

	// Set LSN
	lsn := uint64(12345)
	page.SetLSN(lsn)
	if page.GetLSN() != lsn {
		t.Errorf("Expected LSN %d, got %d", lsn, page.GetLSN())
	}

	// Setting LSN should mark page as dirty
	if !page.IsDirty() {
		t.Error("Page should be dirty after SetLSN")
	}
}

func TestUnifiedPagePinUnpin(t *testing.T) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)

	// Initial pin count should be 0
	if page.GetPinCount() != 0 {
		t.Errorf("Expected initial pin count 0, got %d", page.GetPinCount())
	}

	// Pin the page
	page.Pin()
	if page.GetPinCount() != 1 {
		t.Errorf("Expected pin count 1, got %d", page.GetPinCount())
	}

	// Pin again
	page.Pin()
	if page.GetPinCount() != 2 {
		t.Errorf("Expected pin count 2, got %d", page.GetPinCount())
	}

	// Unpin
	page.Unpin()
	if page.GetPinCount() != 1 {
		t.Errorf("Expected pin count 1, got %d", page.GetPinCount())
	}

	// Unpin again
	page.Unpin()
	if page.GetPinCount() != 0 {
		t.Errorf("Expected pin count 0, got %d", page.GetPinCount())
	}

	// Unpin when already 0 should not go negative
	page.Unpin()
	if page.GetPinCount() != 0 {
		t.Errorf("Expected pin count 0, got %d", page.GetPinCount())
	}
}

func TestUnifiedPageSerialization(t *testing.T) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)
	page.SetLSN(12345)

	// Serialize
	data, err := page.ToBytes()
	if err != nil {
		t.Fatalf("ToBytes failed: %v", err)
	}

	if len(data) != int(DefaultPageSize) {
		t.Errorf("Expected data length %d, got %d", DefaultPageSize, len(data))
	}

	// Create new page and deserialize
	newPage := NewUnifiedPage(0, 0, common.FIL_PAGE_TYPE_ALLOCATED)
	err = newPage.ParseFromBytes(data)
	if err != nil {
		t.Fatalf("ParseFromBytes failed: %v", err)
	}

	// Verify fields
	if newPage.GetSpaceID() != page.GetSpaceID() {
		t.Errorf("SpaceID mismatch: expected %d, got %d", page.GetSpaceID(), newPage.GetSpaceID())
	}

	if newPage.GetPageNo() != page.GetPageNo() {
		t.Errorf("PageNo mismatch: expected %d, got %d", page.GetPageNo(), newPage.GetPageNo())
	}

	if newPage.GetPageType() != page.GetPageType() {
		t.Errorf("PageType mismatch: expected %d, got %d", page.GetPageType(), newPage.GetPageType())
	}

	if newPage.GetLSN() != page.GetLSN() {
		t.Errorf("LSN mismatch: expected %d, got %d", page.GetLSN(), newPage.GetLSN())
	}
}

func TestUnifiedPageChecksum(t *testing.T) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)

	// Update checksum
	page.UpdateChecksum()

	// Validate checksum
	err := page.ValidateChecksum()
	if err != nil {
		t.Errorf("ValidateChecksum failed: %v", err)
	}

	// Check IsCorrupted
	if page.IsCorrupted() {
		t.Error("Page should not be corrupted")
	}
}

func TestUnifiedPageDataAccess(t *testing.T) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)

	// Get initial data
	data := page.GetData()
	if len(data) != int(DefaultPageSize) {
		t.Errorf("Expected data length %d, got %d", DefaultPageSize, len(data))
	}

	// Set new data
	newData := make([]byte, DefaultPageSize)
	for i := range newData {
		newData[i] = byte(i % 256)
	}

	err := page.SetData(newData)
	if err != nil {
		t.Fatalf("SetData failed: %v", err)
	}

	// Verify data was set
	retrievedData := page.GetData()
	for i := range retrievedData {
		if retrievedData[i] != newData[i] {
			t.Errorf("Data mismatch at index %d: expected %d, got %d", i, newData[i], retrievedData[i])
			break
		}
	}

	// Setting data should mark page as dirty
	if !page.IsDirty() {
		t.Error("Page should be dirty after SetData")
	}
}

func TestUnifiedPageBodyAccess(t *testing.T) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)

	// Get initial body
	body := page.GetBody()
	if len(body) != PageBodySize {
		t.Errorf("Expected body length %d, got %d", PageBodySize, len(body))
	}

	// Set new body
	newBody := make([]byte, PageBodySize)
	for i := range newBody {
		newBody[i] = byte(i % 256)
	}

	page.SetBody(newBody)

	// Verify body was set
	retrievedBody := page.GetBody()
	for i := range retrievedBody {
		if retrievedBody[i] != newBody[i] {
			t.Errorf("Body mismatch at index %d: expected %d, got %d", i, newBody[i], retrievedBody[i])
			break
		}
	}
}

func TestUnifiedPageStats(t *testing.T) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)

	stats := page.GetStats()
	if stats == nil {
		t.Fatal("GetStats returned nil")
	}

	// Initial stats
	if stats.ReadCount != 0 {
		t.Errorf("Expected initial ReadCount 0, got %d", stats.ReadCount)
	}

	if stats.WriteCount != 0 {
		t.Errorf("Expected initial WriteCount 0, got %d", stats.WriteCount)
	}

	// Read should update stats
	_ = page.Read()
	stats = page.GetStats()
	if stats.ReadCount != 1 {
		t.Errorf("Expected ReadCount 1, got %d", stats.ReadCount)
	}

	// Write should update stats
	_ = page.Write()
	stats = page.GetStats()
	if stats.WriteCount != 1 {
		t.Errorf("Expected WriteCount 1, got %d", stats.WriteCount)
	}
}

func TestUnifiedPageRelease(t *testing.T) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)

	// Release the page
	page.Release()

	// Check state (should be Clean after release)
	if page.GetState() != basic.PageStateClean {
		t.Errorf("Expected state %d after Release, got %d", basic.PageStateClean, page.GetState())
	}

	// Check pin count
	if page.GetPinCount() != 0 {
		t.Errorf("Expected pin count 0 after Release, got %d", page.GetPinCount())
	}
}

// Benchmark tests
func BenchmarkUnifiedPageCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewUnifiedPage(1, uint32(i), common.FIL_PAGE_INDEX)
	}
}

func BenchmarkUnifiedPageSerialization(b *testing.B) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = page.ToBytes()
	}
}

func BenchmarkUnifiedPageDeserialization(b *testing.B) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)
	data, _ := page.ToBytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newPage := NewUnifiedPage(0, 0, common.FIL_PAGE_TYPE_ALLOCATED)
		_ = newPage.ParseFromBytes(data)
	}
}

func BenchmarkUnifiedPageChecksum(b *testing.B) {
	page := NewUnifiedPage(1, 100, common.FIL_PAGE_INDEX)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page.UpdateChecksum()
	}
}
