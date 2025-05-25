package basic

const (
	ExtentPurposeData   ExtentPurpose = iota // Data extent
	ExtentPurposeIndex                       // Index extent
	ExtentPurposeUndo                        // Undo log extent
	ExtentPurposeSystem                      // System extent
)

// Space represents a tablespace in the storage engine
type Space interface {
	// Basic information
	ID() uint32
	Name() string
	IsSystem() bool

	// Space management
	AllocateExtent(purpose ExtentPurpose) (Extent, error)
	FreeExtent(extentID uint32) error

	// Statistics
	GetPageCount() uint32
	GetExtentCount() uint32
	GetUsedSpace() uint64

	// State management
	IsActive() bool
	SetActive(active bool)
	LoadPageByPageNumber(no uint32) ([]byte, error)
	FlushToDisk(no uint32, content []byte) error
}

// SpaceManager manages all tablespaces in the storage engine
type SpaceManager interface {
	// Space operations
	CreateSpace(spaceID uint32, name string, isSystem bool) (Space, error)
	GetSpace(spaceID uint32) (Space, error)
	DropSpace(spaceID uint32) error

	// Extent management
	AllocateExtent(spaceID uint32, purpose ExtentPurpose) (Extent, error)
	FreeExtent(spaceID, extentID uint32) error

	// Transaction support
	Begin() (Tx, error)

	// TableSpace operations (compatibility with StorageManager)
	CreateNewTablespace(name string) uint32
	CreateTableSpace(name string) (uint32, error)
	GetTableSpace(spaceID uint32) (FileTableSpace, error)
	GetTableSpaceByName(name string) (FileTableSpace, error)
	GetTableSpaceInfo(spaceID uint32) (*TableSpaceInfo, error)
	DropTableSpace(spaceID uint32) error
	Close() error
}
