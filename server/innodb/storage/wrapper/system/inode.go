package system

import (
	"xmysql-server/server/innodb/storage/store/pages"
)

// INode represents an InnoDB inode wrapper
type INode struct {
	SpaceID   uint32
	PageNo    uint32
	Offset    uint16
	FSPHeader *FSPHeader

	// Underlying INode page from store/pages
	inodePage *pages.INodePage
}

// NewINode creates a new INode wrapper based on store/pages INodePage
func NewINode(spaceID, pageNo uint32) *INode {
	// Create underlying INode page from store/pages
	inodePage := pages.NewINodePage(spaceID, pageNo)

	return &INode{
		SpaceID: spaceID,
		PageNo:  pageNo,
		Offset:  0,
		FSPHeader: &FSPHeader{
			SpaceID: spaceID,
		},
		inodePage: inodePage,
	}
}

// GetInodePage returns the underlying INode page
func (inode *INode) GetInodePage() *pages.INodePage {
	return inode.inodePage
}

// ToBytes serializes the INode to bytes
func (inode *INode) ToBytes() []byte {
	if inode.inodePage != nil {
		return inode.inodePage.GetSerializeBytes()
	}
	return nil
}

// ParseFromBytes parses INode from bytes
func (inode *INode) ParseFromBytes(data []byte) error {
	inode.inodePage = pages.NewINodeByParseBytes(data)
	return nil
}

// FSPHeader represents the file space header
type FSPHeader struct {
	SpaceID      uint32
	Size         uint32
	FreeLimit    uint32
	Flags        uint32
	FreeListBase struct {
		Length uint32
		First  uint32
		Last   uint32
	}
	FragArrayBase struct {
		Length uint32
		First  uint32
		Last   uint32
	}
	NextSegmentID uint64
}
