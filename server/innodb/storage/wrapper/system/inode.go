package system

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
)

// INode represents an InnoDB inode wrapper
type INode struct {
	SpaceID   uint32
	PageNo    uint32
	Offset    uint16
	FSPHeader *FSPHeader

	// Underlying INode page from store/pages
	inodePage *pages.INodePage
	storage   basic.StorageProvider
}

// NewINode creates a new INode wrapper based on store/pages INodePage
func NewINode(spaceID, pageNo uint32) *INode {
	return NewINodeWithStorage(spaceID, pageNo, nil)
}

// NewINodeWithStorage creates the legacy system inode wrapper with an
// optional durable page provider.
func NewINodeWithStorage(spaceID, pageNo uint32, storage basic.StorageProvider) *INode {
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
		storage:   storage,
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
	if len(data) < common.PageSize {
		return fmt.Errorf("inode page has %d bytes, want at least %d", len(data), common.PageSize)
	}
	inode.inodePage = pages.NewINodeByParseBytes(data)
	return nil
}

// Read loads the inode page from the configured durable provider.
func (inode *INode) Read() error {
	if inode.storage == nil {
		return fmt.Errorf("inode page storage provider is unavailable")
	}
	data, err := inode.storage.ReadPage(inode.SpaceID, inode.PageNo)
	if err != nil {
		return err
	}
	return inode.ParseFromBytes(data)
}

// Write persists the current inode page through the configured provider.
func (inode *INode) Write() error {
	if inode.storage == nil {
		return fmt.Errorf("inode page storage provider is unavailable")
	}
	data := inode.ToBytes()
	if len(data) < common.PageSize {
		return fmt.Errorf("inode page serializes to %d bytes, want at least %d", len(data), common.PageSize)
	}
	return inode.storage.WritePage(inode.SpaceID, inode.PageNo, data[:common.PageSize])
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
