package space

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/ibd"
)

func TestIBDSpaceFreePageUpdatesExtentAllocation(t *testing.T) {
	file := ibd.NewIBDFile(t.TempDir(), "page-reclaim", 11)
	if err := file.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer file.Close()

	space := NewIBDSpace(file, false)
	ext, err := space.AllocateExtent(basic.ExtentPurposeData)
	if err != nil {
		t.Fatalf("AllocateExtent() error = %v", err)
	}
	pageNo, err := ext.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage() error = %v", err)
	}

	if err := space.FreePage(pageNo); err != nil {
		t.Fatalf("FreePage() error = %v", err)
	}
	if err := space.FreePage(pageNo); err == nil {
		t.Fatal("second FreePage() unexpectedly succeeded")
	}
}

func TestIBDSpaceAllocatesPagesFromExistingExtent(t *testing.T) {
	file := ibd.NewIBDFile(t.TempDir(), "page-allocate", 12)
	if err := file.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer file.Close()

	space := NewIBDSpace(file, false)
	first, err := space.AllocatePage()
	if err != nil {
		t.Fatalf("first AllocatePage() error = %v", err)
	}
	second, err := space.AllocatePage()
	if err != nil {
		t.Fatalf("second AllocatePage() error = %v", err)
	}
	if second != first+1 {
		t.Fatalf("second page = %d, first = %d; allocation did not reuse extent", second, first)
	}
}
