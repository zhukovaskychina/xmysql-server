package system

import (
	"testing"
)

func TestNewINode(t *testing.T) {
	spaceID := uint32(0)
	pageNo := uint32(3)

	// Test creating a new INode
	inode := NewINode(spaceID, pageNo)

	if inode == nil {
		t.Fatal("NewINode returned nil")
	}

	if inode.SpaceID != spaceID {
		t.Errorf("Expected SpaceID %d, got %d", spaceID, inode.SpaceID)
	}

	if inode.PageNo != pageNo {
		t.Errorf("Expected PageNo %d, got %d", pageNo, inode.PageNo)
	}

	if inode.FSPHeader == nil {
		t.Error("FSPHeader should not be nil")
	}

	if inode.FSPHeader.SpaceID != spaceID {
		t.Errorf("Expected FSPHeader.SpaceID %d, got %d", spaceID, inode.FSPHeader.SpaceID)
	}
}

func TestINodeBytes(t *testing.T) {
	spaceID := uint32(0)
	pageNo := uint32(3)

	inode := NewINode(spaceID, pageNo)

	// Test ToBytes
	data := inode.ToBytes()
	if data == nil {
		t.Error("ToBytes returned nil")
	}

	// Test ParseFromBytes
	newInode := &INode{}
	err := newInode.ParseFromBytes(data)
	if err != nil {
		t.Errorf("ParseFromBytes failed: %v", err)
	}

	if newInode.inodePage == nil {
		t.Error("ParseFromBytes should set inodePage")
	}
}
