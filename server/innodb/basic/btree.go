package basic

// BTreeConfig holds configuration for a B+Tree
type BTreeConfig struct {
	SpaceID    uint32
	RootPageNo uint32
	IndexName  string
}

// BTreeStats holds statistics for a B+Tree
type BTreeStats struct {
	NodeCount int
	LeafCount int
	Height    int
	Size      int64
}

// IBTree defines the interface for B+Tree operations
type IBTree interface {
	// Core operations
	Insert(key []byte, value Row) error
	Delete(key []byte) error
	Find(key []byte) (Row, error)

	// Iterator operations
	Keys() (RowItemsIterator, error)
	Backward() (Iterator, error)

	// Segment operations
	GetDataSegment() XMySQLSegment
	GetInternalSegment() XMySQLSegment

	// Stats
	TREESize() int
}
