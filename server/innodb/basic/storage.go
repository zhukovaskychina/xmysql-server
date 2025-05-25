package basic

// StorageProvider defines the interface for storage operations
type StorageProvider interface {
	// Page operations
	ReadPage(spaceID uint32, pageNo uint32) ([]byte, error)
	WritePage(spaceID uint32, pageNo uint32, data []byte) error
	AllocatePage(spaceID uint32) (uint32, error)
	FreePage(spaceID uint32, pageNo uint32) error

	// Space operations
	CreateSpace(name string, pageSize uint32) (uint32, error)
	OpenSpace(spaceID uint32) error
	CloseSpace(spaceID uint32) error
	DeleteSpace(spaceID uint32) error

	// Space info
	GetSpaceInfo(spaceID uint32) (*SpaceInfo, error)
	ListSpaces() ([]SpaceInfo, error)

	// Transaction support
	BeginTransaction() (uint64, error)
	CommitTransaction(txID uint64) error
	RollbackTransaction(txID uint64) error

	// I/O operations
	Sync(spaceID uint32) error
	Close() error
}

// SpaceInfo contains information about a tablespace
type SpaceInfo struct {
	SpaceID      uint32 // Space ID
	Name         string // Space name
	Path         string // File path
	PageSize     uint32 // Page size in bytes
	TotalPages   uint64 // Total number of pages
	FreePages    uint64 // Number of free pages
	ExtentSize   uint32 // Size of extent in pages
	IsCompressed bool   // Whether the space is compressed
	State        string // Space state (e.g., "active", "inactive")
}

// PageInfo contains information about a page
type PageInfo struct {
	SpaceID      uint32 // Space ID
	PageNo       uint32 // Page number
	LSN          uint64 // Log sequence number
	Type         uint16 // Page type
	FileOffset   uint64 // Offset in file
	IsDirty      bool   // Whether page is dirty
	IsCompressed bool   // Whether page is compressed
}
