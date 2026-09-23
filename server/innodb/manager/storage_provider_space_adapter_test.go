package manager

import "testing"

func TestProviderSpaceReportsProviderCapacity(t *testing.T) {
	provider := newMockOptimizedStorageProvider()
	for pageNo := uint32(0); pageNo < 128; pageNo++ {
		provider.pages[makePageID(9, pageNo)] = make([]byte, PAGE_SIZE)
	}

	space := newProviderSpace(9, "test", false, provider)
	if got := space.GetPageCount(); got != 128 {
		t.Fatalf("GetPageCount() = %d, want 128", got)
	}
	if got := space.GetExtentCount(); got != 2 {
		t.Fatalf("GetExtentCount() = %d, want 2", got)
	}
	if got := space.GetUsedSpace(); got != 128*PAGE_SIZE {
		t.Fatalf("GetUsedSpace() = %d, want %d", got, 128*PAGE_SIZE)
	}
}
