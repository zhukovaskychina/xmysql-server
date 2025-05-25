package basic

// ExtentPurpose defines the purpose of an extent
type ExtentPurpose int

// Tx represents a space transaction
type Tx interface {
	Commit() error
	Rollback() error
}

// TableSpaceInfo contains information about a tablespace
type TableSpaceInfo struct {
	SpaceID      uint32
	Name         string
	FilePath     string
	Size         uint64
	FreeSpace    uint64
	SegmentCount uint32
}
