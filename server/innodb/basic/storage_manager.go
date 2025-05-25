package basic

// StorageManager is the central manager for all storage-related operations
type StorageManager interface {
	// Segment management
	CreateSegment(spaceID uint32, purpose SegmentPurpose) (Segment, error)
	GetSegment(segmentID uint64) (Segment, error)
	FreeSegment(segmentID uint64) error

	// Space management
	AllocateExtent(spaceID uint32, purpose ExtentPurpose) (Extent, error)
	FreeExtent(spaceID, extentID uint32) error

	// Page management
	AllocPage(spaceID uint32, pageType PageType) (IPage, error)
	GetPage(spaceID, pageNo uint32) (IPage, error)
	FreePage(spaceID, pageNo uint32) error

	// Transaction support
	Begin() (Transaction, error)
	Commit(tx Transaction) error
	Rollback(tx Transaction) error

	// Maintenance
	Flush() error
	Close() error
}

// Transaction represents a storage transaction
type Transaction interface {
	ID() uint64
	Commit() error
	Rollback() error
}
