package basic

// IBTreeManager defines the interface for managing multiple B+ trees
type IBTreeManager interface {
	// CreateTree creates a new B+ tree
	CreateTree(spaceID uint32, indexID uint64) (IBTree, error)

	// GetTree retrieves an existing B+ tree
	GetTree(spaceID uint32, indexID uint64) (IBTree, error)

	// DeleteTree removes a B+ tree
	DeleteTree(spaceID uint32, indexID uint64) error

	// Sync flushes all B+ trees to disk
	Sync() error
}
