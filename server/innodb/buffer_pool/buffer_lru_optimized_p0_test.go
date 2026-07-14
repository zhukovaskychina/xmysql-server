package buffer_pool

import "testing"

func TestOptimizedLRUCacheEvictReturnsOriginalPageIdentity(t *testing.T) {
	cache := NewOptimizedLRUCache(2, 0.5, 0.5, 0)
	page := NewBufferPage(7, 42)
	page.SetContent([]byte("payload"))

	if err := cache.Set(7, 42, NewBufferBlock(page)); err != nil {
		t.Fatalf("set page: %v", err)
	}

	evicted := cache.Evict()
	if evicted == nil {
		t.Fatal("expected evicted page")
	}
	if evicted.GetSpaceID() != 7 || evicted.GetPageNo() != 42 {
		t.Fatalf("evicted wrong page identity: space=%d page=%d", evicted.GetSpaceID(), evicted.GetPageNo())
	}
	if string(evicted.GetContent()) != "payload" {
		t.Fatalf("evicted wrong page content: %q", string(evicted.GetContent()))
	}
}

func TestOptimizedLRUCacheEvictFallsBackToYoungList(t *testing.T) {
	cache := NewOptimizedLRUCache(4, 0.5, 0.5, 0)
	page := NewBufferPage(3, 9)

	cache.SetYoung(3, 9, NewBufferBlock(page))

	evicted := cache.Evict()
	if evicted == nil {
		t.Fatal("expected evicted page from young list")
	}
	if evicted.GetSpaceID() != 3 || evicted.GetPageNo() != 9 {
		t.Fatalf("evicted wrong young page: space=%d page=%d", evicted.GetSpaceID(), evicted.GetPageNo())
	}
}

func TestBufferPagePinCount(t *testing.T) {
	page := NewBufferPage(1, 1)
	if page.GetPinCount() != 0 {
		t.Fatalf("new page pin count = %d", page.GetPinCount())
	}
	page.Pin()
	page.Pin()
	if page.GetPinCount() != 2 {
		t.Fatalf("expected pin count 2, got %d", page.GetPinCount())
	}
	if !page.IsPinned() {
		t.Fatal("expected page to be pinned")
	}
	page.Unpin()
	page.Unpin()
	page.Unpin()
	if page.GetPinCount() != 0 {
		t.Fatalf("pin count should not go below zero, got %d", page.GetPinCount())
	}
	if page.IsPinned() {
		t.Fatal("expected page to be unpinned")
	}
}

func TestOptimizedLRUCacheEvictSkipsPinnedPages(t *testing.T) {
	cache := NewOptimizedLRUCache(2, 0.5, 0.5, 0)
	pinned := NewBufferPage(1, 1)
	pinned.Pin()
	unpinned := NewBufferPage(1, 2)

	if err := cache.Set(1, 1, NewBufferBlock(pinned)); err != nil {
		t.Fatalf("set pinned page: %v", err)
	}
	if err := cache.Set(1, 2, NewBufferBlock(unpinned)); err != nil {
		t.Fatalf("set unpinned page: %v", err)
	}

	evicted := cache.Evict()
	if evicted == nil {
		t.Fatal("expected an unpinned page to be evicted")
	}
	if evicted.GetPageNo() == pinned.GetPageNo() {
		t.Fatal("pinned page must not be evicted")
	}
}
