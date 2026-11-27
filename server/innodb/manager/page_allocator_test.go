package manager

import "testing"

func TestPageAllocatorFindFreePageInExtent(t *testing.T) {
	pa := NewPageAllocator(nil, 0, nil)

	offset := pa.findFreePageInExtent(0)
	if offset != 0 {
		t.Fatalf("expect first free page offset=0, got %d", offset)
	}

	for i := uint32(0); i < PagesPerExtent; i++ {
		pa.setExtentPageUsed(0, i)
	}

	offset = pa.findFreePageInExtent(0)
	if offset != ^uint32(0) {
		t.Fatalf("expect extent 0 to be full, got offset=%d", offset)
	}

	pa.freePageInExtent(0, 31)
	offset = pa.findFreePageInExtent(0)
	if offset != 31 {
		t.Fatalf("expect freed page offset=31, got %d", offset)
	}
}

func TestPageAllocatorListHelpers(t *testing.T) {
	list := appendDistinct([]uint32{1, 3}, 1)
	if len(list) != 2 || list[0] != 1 || list[1] != 3 {
		t.Fatalf("appendDistinct should keep existing ids, got %#v", list)
	}

	list = appendDistinct(list, 2)
	if len(list) != 3 || list[2] != 2 {
		t.Fatalf("appendDistinct should append new id, got %#v", list)
	}

	list = removeExtent(list, 3)
	if len(list) != 2 || list[0] != 1 || list[1] != 2 {
		t.Fatalf("removeExtent failed, got %#v", list)
	}
}

func TestPageAllocatorRejectsDoubleFreeAndPreservesStats(t *testing.T) {
	pa := NewPageAllocator(nil, 0, nil)
	pageNo, err := pa.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage() error = %v", err)
	}

	if err := pa.FreePage(pageNo); err != nil {
		t.Fatalf("first FreePage(%d) error = %v", pageNo, err)
	}
	stats := pa.GetStats()
	if stats.AllocatedPages != 0 || stats.FragmentPages != 0 {
		t.Fatalf("stats after free = %#v, want no allocated fragment pages", stats)
	}

	if err := pa.FreePage(pageNo); err == nil {
		t.Fatalf("second FreePage(%d) unexpectedly succeeded", pageNo)
	}
	stats = pa.GetStats()
	if stats.AllocatedPages != 0 || stats.FragmentPages != 0 {
		t.Fatalf("stats after rejected double free = %#v, want unchanged zero counts", stats)
	}
}

func TestPageAllocatorBatchStatsUseActualPageCount(t *testing.T) {
	spaceManager := NewMockSpaceManager()
	if _, err := spaceManager.GetSpace(0); err != nil {
		t.Fatalf("GetSpace() error = %v", err)
	}
	pa := NewPageAllocator(spaceManager, 0, nil)
	for i := uint32(0); i < FragmentPages; i++ {
		if _, err := pa.AllocatePage(); err != nil {
			t.Fatalf("seed AllocatePage(%d) error = %v", i, err)
		}
	}
	pages, err := pa.AllocatePages(12)
	if err != nil {
		t.Fatalf("AllocatePages() error = %v", err)
	}
	if len(pages) != 12 {
		t.Fatalf("AllocatePages() returned %d pages, want 12", len(pages))
	}
	stats := pa.GetStats()
	if stats.AllocatedPages != FragmentPages+uint32(len(pages)) {
		t.Fatalf("AllocatedPages = %d, want %d", stats.AllocatedPages, FragmentPages+uint32(len(pages)))
	}
	if stats.FragmentPages != FragmentPages {
		t.Fatalf("FragmentPages = %d, want %d", stats.FragmentPages, FragmentPages)
	}
	if stats.ExtentPages != 12 {
		t.Fatalf("ExtentPages = %d, want 12", stats.ExtentPages)
	}
}

func TestPageAllocatorBatchFreePrevalidatesWithoutPartialRelease(t *testing.T) {
	pa := NewPageAllocator(nil, 0, nil)
	first, err := pa.AllocatePage()
	if err != nil {
		t.Fatalf("first AllocatePage() error = %v", err)
	}
	second, err := pa.AllocatePage()
	if err != nil {
		t.Fatalf("second AllocatePage() error = %v", err)
	}

	if err := pa.FreePages([]uint32{first, first}); err == nil {
		t.Fatal("FreePages() accepted duplicate page")
	}
	stats := pa.GetStats()
	if stats.AllocatedPages != 2 || stats.FragmentPages != 2 {
		t.Fatalf("stats after duplicate rejection = %#v, want both pages allocated", stats)
	}

	if err := pa.FreePages([]uint32{first, FragmentPages + 1}); err == nil {
		t.Fatal("FreePages() accepted an unallocated page")
	}
	stats = pa.GetStats()
	if stats.AllocatedPages != 2 || stats.FragmentPages != 2 {
		t.Fatalf("stats after mixed rejection = %#v, want both pages allocated", stats)
	}

	if err := pa.FreePages([]uint32{first, second}); err != nil {
		t.Fatalf("valid FreePages() error = %v", err)
	}
	if stats = pa.GetStats(); stats.AllocatedPages != 0 || stats.FragmentPages != 0 {
		t.Fatalf("stats after valid batch free = %#v, want zero counts", stats)
	}
}
