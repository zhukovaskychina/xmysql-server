package basic

// IIndexWrapper defines the interface for index wrappers
type IIndexWrapper interface {
	GetSegLeaf() []byte
	GetSegTop() []byte
	GetPageNo() uint32
	GetSpaceID() uint32
	GetIndexID() uint64
}

// ISysTableSpace defines the interface for system tablespace
type ISysTableSpace interface {
	GetBlockFile() IBlockFile
}

// IBlockFile defines the interface for block files
type IBlockFile interface {
	Read(pageNo uint32) ([]byte, error)
	Write(pageNo uint32, content []byte) error
	Sync() error
	Close() error
}
