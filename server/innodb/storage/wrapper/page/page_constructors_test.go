package page

import (
	"testing"
)

func TestNewInodePageWrapper(t *testing.T) {
	id := uint32(3)
	spaceID := uint32(0)

	// Test creating a new INode page wrapper
	inode := NewInodePageWrapper(id, spaceID)

	if inode == nil {
		t.Fatal("NewInodePageWrapper returned nil")
	}

	if inode.SpaceID != spaceID {
		t.Errorf("Expected SpaceID %d, got %d", spaceID, inode.SpaceID)
	}

	if inode.PageNo != id {
		t.Errorf("Expected PageNo %d, got %d", id, inode.PageNo)
	}

	// Test that the underlying inode page is created
	inodePage := inode.GetInodePage()
	if inodePage == nil {
		t.Error("GetInodePage() returned nil")
	}
}

func TestNewIBufPageWrapper(t *testing.T) {
	id := uint32(4)
	spaceID := uint32(0)

	// Test creating IBuf page wrapper
	ibuf := NewIBufPageWrapper(id, spaceID)

	if ibuf == nil {
		t.Fatal("NewIBufPageWrapper returned nil")
	}

	if ibuf.BasePageWrapper == nil {
		t.Error("BasePageWrapper should not be nil")
	}
}

func TestNewAllocatePageWrapper(t *testing.T) {
	id := uint32(5)
	spaceID := uint32(0)

	// Test creating allocated page wrapper
	allocated := NewAllocatePageWrapper(id, spaceID)

	if allocated == nil {
		t.Fatal("NewAllocatePageWrapper returned nil")
	}

	if allocated.BasePageWrapper == nil {
		t.Error("BasePageWrapper should not be nil")
	}
}
