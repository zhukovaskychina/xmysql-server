package space

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/ibd"
)

func TestShrinkToFitTruncatesOnlyTrailingFreePages(t *testing.T) {
	file := ibd.NewIBDFile(t.TempDir(), "shrink-to-fit", 13)
	if err := file.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer file.Close()

	space := NewIBDSpace(file, false)
	_, err := space.AllocateExtent(basic.ExtentPurposeData)
	if err != nil {
		t.Fatalf("first AllocateExtent() error = %v", err)
	}
	second, err := space.AllocateExtent(basic.ExtentPurposeData)
	if err != nil {
		t.Fatalf("second AllocateExtent() error = %v", err)
	}
	if err := file.WritePage(127, make([]byte, PageSize)); err != nil {
		t.Fatalf("WritePage() error = %v", err)
	}
	if err := space.FreeExtent(second.GetID()); err != nil {
		t.Fatalf("FreeExtent() error = %v", err)
	}

	if err := space.ShrinkToFit(); err != nil {
		t.Fatalf("ShrinkToFit() error = %v", err)
	}
	size, err := file.Size()
	if err != nil {
		t.Fatalf("Size() error = %v", err)
	}
	wantSize := int64(PagesPerExtent * PageSize)
	if size != wantSize {
		t.Fatalf("file size = %d, want %d", size, wantSize)
	}
}

func TestShrinkToFitDoesNotTruncateLiveTail(t *testing.T) {
	file := ibd.NewIBDFile(t.TempDir(), "shrink-live-tail", 14)
	if err := file.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer file.Close()

	space := NewIBDSpace(file, false)
	_, err := space.AllocateExtent(basic.ExtentPurposeData)
	if err != nil {
		t.Fatalf("AllocateExtent() error = %v", err)
	}
	if err := file.WritePage(63, make([]byte, PageSize)); err != nil {
		t.Fatalf("WritePage() error = %v", err)
	}
	before, err := file.Size()
	if err != nil {
		t.Fatalf("Size() before error = %v", err)
	}
	if err := space.ShrinkToFit(); err != nil {
		t.Fatalf("ShrinkToFit() error = %v", err)
	}
	after, err := file.Size()
	if err != nil {
		t.Fatalf("Size() after error = %v", err)
	}
	if after != before {
		t.Fatalf("file size changed from %d to %d with live tail", before, after)
	}
}

func TestShrinkToFitPreservesNextExtentAlignment(t *testing.T) {
	file := ibd.NewIBDFile(t.TempDir(), "shrink-alignment", 15)
	if err := file.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer file.Close()

	space := NewIBDSpace(file, false)
	ext, err := space.AllocateExtent(basic.ExtentPurposeData)
	if err != nil {
		t.Fatalf("AllocateExtent() error = %v", err)
	}
	if err := file.WritePage(63, make([]byte, PageSize)); err != nil {
		t.Fatalf("WritePage() error = %v", err)
	}
	if err := space.FreeExtent(ext.GetID()); err != nil {
		t.Fatalf("FreeExtent() error = %v", err)
	}
	if err := space.ShrinkToFit(); err != nil {
		t.Fatalf("ShrinkToFit() error = %v", err)
	}
	next, err := space.AllocateExtent(basic.ExtentPurposeData)
	if err != nil {
		t.Fatalf("AllocateExtent() after shrink error = %v", err)
	}
	if got := next.StartPage(); got != PagesPerExtent {
		t.Fatalf("next extent start page = %d, want %d", got, PagesPerExtent)
	}
}
