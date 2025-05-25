package basic

// PageManager handles page-level operations
type PageManager interface {
	// AllocPage allocates a new page in the given tablespace
	AllocPage(spaceID uint32) (uint32, error)

	// FreePage frees a page in the given tablespace
	FreePage(spaceID uint32, pageNo uint32) error

	// GetPage retrieves a page from buffer pool or disk
	GetPage(spaceID uint32, pageNo uint32) ([]byte, error)

	// WritePage writes a page to buffer pool
	WritePage(spaceID uint32, pageNo uint32, content []byte) error

	// FlushPage flushes a page from buffer pool to disk
	FlushPage(spaceID uint32, pageNo uint32) error
	ScanLeaves(no uint32) (interface{}, interface{})
	InsertKey(no uint32, key []byte, record Record) interface{}
	AllocatePage(id interface{}) (interface{}, interface{})
}
