package segment

// SegmentType 段类型
type SegmentType uint8

const (
	SegmentTypeData SegmentType = iota
	SegmentTypeIndex
	SegmentTypeRollback
	SegmentTypeSystem
)

// SegmentState 段状态
type SegmentState uint8

const (
	SegmentStateFree SegmentState = iota
	SegmentStateActive
	SegmentStateFull
	SegmentStateDeleted
)

// SegmentStats 段统计信息
type SegmentStats struct {
	TotalPages    uint32 // 使用atomic操作
	FreePages     uint32 // 使用atomic操作
	FragPages     uint32 // 使用atomic操作
	FullPages     uint32 // 使用atomic操作
	ExtentCount   uint32 // 使用atomic操作
	LastModified  int64
	LastExtended  int64
	LastDefragged int64
}

// SegmentHeader 段头部信息
type SegmentHeader struct {
	SegmentID    uint64
	SegmentType  SegmentType
	State        SegmentState
	SpaceID      uint32
	RootPage     uint32
	FreeListBase uint32
	FullListBase uint32
	FragListBase uint32
	ExtentCount  uint32
	PageCount    uint32
	CreateTime   int64
}

// Segment 段接口
type Segment interface {
	// 基本信息
	GetID() uint64
	GetType() SegmentType
	GetState() SegmentState
	GetSpaceID() uint32

	// 页面管理
	AllocatePage() (uint32, error)
	FreePage(pageNo uint32) error
	GetPageCount() uint32
	GetFreePages() []uint32

	// 区管理
	AllocateExtent() (uint32, error)
	FreeExtent(extentID uint32) error
	GetExtentCount() uint32
	GetExtents() []uint32

	// 统计信息
	GetStats() *SegmentStats

	// 维护操作
	Defragment() error
	Extend(pages uint32) error
	Truncate(pages uint32) error

	// 并发控制
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

// SegmentManager 段管理器接口
type SegmentManager interface {
	// 段操作
	CreateSegment(spaceID uint32, segType SegmentType) (Segment, error)
	GetSegment(segID uint64) (Segment, error)
	DropSegment(segID uint64) error

	// 空间管理
	AllocatePage(segID uint64) (uint32, error)
	FreePage(segID uint64, pageNo uint32) error

	// 统计信息
	GetStats() *SegmentManagerStats

	// 维护操作
	DefragmentSegment(segID uint64) error
	ExtendSegment(segID uint64, pages uint32) error
	TruncateSegment(segID uint64, pages uint32) error
}

// SegmentManagerStats 段管理器统计信息
type SegmentManagerStats struct {
	TotalSegments   uint32 // 使用atomic操作
	ActiveSegments  uint32 // 使用atomic操作
	DeletedSegments uint32 // 使用atomic操作
	TotalPages      uint64 // 使用atomic操作
	FreePages       uint64 // 使用atomic操作
	AllocOperations uint64 // 使用atomic操作
	FreeOperations  uint64 // 使用atomic操作
	LastDefragTime  int64
}
