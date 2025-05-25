package basic

type Segment interface {
}

// SegmentManager handles segment-level operations
type SegmentManager interface {
	// CreateSegment creates a new segment in the given tablespace
	CreateSegment(spaceID uint32) (uint32, error)

	// DropSegment drops a segment
	DropSegment(spaceID uint32, segmentID uint32) error

	// AllocatePages allocates pages in a segment
	AllocatePages(spaceID uint32, segmentID uint32, numPages uint32) ([]uint32, error)

	// FreePages frees pages in a segment
	FreePages(spaceID uint32, segmentID uint32, pageNos []uint32) error

	// GetSegmentInfo gets information about a segment
	GetSegmentInfo(spaceID uint32, segmentID uint32) (*SegmentInfo, error)
}

// SegmentInfo contains information about a segment
type SegmentInfo struct {
	SegmentID  uint32
	SpaceID    uint32
	PageCount  uint32
	FreePages  uint32
	TotalPages uint32
}
