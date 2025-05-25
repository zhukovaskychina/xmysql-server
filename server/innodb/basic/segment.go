package basic

// ISegment represents a segment in the storage engine
type ISegment interface {
	// GetID returns the segment ID
	GetID() uint64

	// GetSpaceID returns the tablespace ID
	GetSpaceID() uint32

	// GetRootPageNo returns the root page number
	GetRootPageNo() uint32

	// SetRootPageNo sets the root page number
	SetRootPageNo(pageNo uint32)

	// GetType returns the segment type
	GetType() SegmentType
}

// SegmentType represents the type of a segment
type SegmentType uint8

const (
	// SegmentTypeSys represents a system segment
	SegmentTypeSys SegmentType = iota
	// SegmentTypeUndoLog represents an undo log segment
	SegmentTypeUndoLog
	// SegmentTypeIndex represents an index segment
	SegmentTypeIndex
	// SegmentTypeTablespace represents a tablespace segment
	SegmentTypeTablespace
)

// ISegmentManager manages segments
type ISegmentManager interface {
	// CreateSegment creates a new segment
	CreateSegment(spaceID uint32, segType SegmentType) (ISegment, error)

	// GetSegment returns an existing segment
	GetSegment(segmentID uint64) (ISegment, error)

	// FreeSegment frees a segment
	FreeSegment(segmentID uint64) error
}
