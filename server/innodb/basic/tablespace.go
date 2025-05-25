package basic

// BlockFile represents a file that can be read and written in blocks/pages
type BlockFile interface {
	ReadPage(pageNo uint32) ([]byte, error)
	WritePage(pageNo uint32, content []byte) error
}

// FileTableSpace represents a tablespace file
type FileTableSpace interface {
	FlushToDisk(pageNo uint32, content []byte)
	LoadPageByPageNumber(pageNo uint32) ([]byte, error)
	GetSpaceId() uint32
}

// SysTableSpace represents a system tablespace
type SysTableSpace interface {
	FileTableSpace
	GetBlockFile() BlockFile

	GetSysTables() IIndexWrapper
	GetSysColumns() IIndexWrapper
	GetSysIndexes() IIndexWrapper
	GetSysFields() IIndexWrapper
}
