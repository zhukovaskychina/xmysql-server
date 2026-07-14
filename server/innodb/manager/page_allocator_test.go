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
