package pages

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"testing"
)

func TestFspHdrPage_DeserializeFields(t *testing.T) {
	// Arrange
	spaceID := uint32(123)
	fp := NewFspHrdPage(spaceID)

	s := NewDefaultPageSerializer()
	data, err := s.Serialize(fp)
	if err != nil {
		t.Fatalf("Serialize FSP_HDR error: %v", err)
	}

	// Act
	got, err := s.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize FSP_HDR error: %v", err)
	}

	// Assert
	fsp, ok := got.(*FspHrdBinaryPage)
	if !ok {
		t.Fatalf("expected *FspHrdBinaryPage, got %T", got)
	}

	if uint16(fsp.FileHeader.GetPageType()) != uint16(common.FILE_PAGE_TYPE_FSP_HDR) {
		t.Fatalf("page type mismatch: got %d", fsp.FileHeader.GetPageType())
	}

	if fsp.FileSpaceHeader == nil {
		t.Fatalf("FileSpaceHeader is nil")
	}

	if gotSpace := fsp.FileSpaceHeader.SpaceId(); len(gotSpace) != 4 {
		t.Fatalf("SpaceId length invalid: %d", len(gotSpace))
	}

	// Verify space id matches
	if want := spaceID; want != fsp.FileSpaceHeader.GetSpaceID() {
		t.Fatalf("SpaceId mismatch: want %d got %d", want, fsp.FileSpaceHeader.GetSpaceID())
	}

	if len(fsp.XDESEntrys) != 256 {
		t.Fatalf("XDESEntrys length: want 256 got %d", len(fsp.XDESEntrys))
	}

	// Validate empty space size matches expected layout
	const xdesEntrySize = 40
	const xdesEntryCount = 256
	const fspHdrStructSize = 4 + 4 + 4 + 4 + 4 + 4 + 16 + 16 + 16 + 8 + 16 + 16 // 112 bytes
	expectedEmpty := int(common.PageSize) - int(common.FileHeaderSize) - int(common.FileTrailerSize) - xdesEntrySize*xdesEntryCount - fspHdrStructSize
	if len(fsp.EmptySpace) != expectedEmpty {
		t.Fatalf("empty space size mismatch: want %d got %d", expectedEmpty, len(fsp.EmptySpace))
	}
}

func TestINodePage_DeserializeFields(t *testing.T) {
	// Arrange
	page := NewINodePage(7, 2)
	s := NewDefaultPageSerializer()

	data, err := s.Serialize(page)
	if err != nil {
		t.Fatalf("Serialize INODE error: %v", err)
	}

	// Act
	got, err := s.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize INODE error: %v", err)
	}

	// Assert
	ip, ok := got.(*INodePage)
	if !ok {
		t.Fatalf("expected *INodePage, got %T", got)
	}

	if uint16(ip.FileHeader.GetPageType()) != uint16(common.FIL_PAGE_INODE) {
		t.Fatalf("page type mismatch: got %d", ip.FileHeader.GetPageType())
	}

	if len(ip.INodeEntries) != 85 {
		t.Fatalf("INodeEntries length: want 85 got %d", len(ip.INodeEntries))
	}

	if len(ip.INodePageList.PreNodePageNumber) != 4 || len(ip.INodePageList.NextNodePageNumber) != 4 {
		t.Fatalf("INodePageList pointers length invalid")
	}

	if len(ip.EmptySpace) != 6 {
		t.Fatalf("EmptySpace length: want 6 got %d", len(ip.EmptySpace))
	}
}
