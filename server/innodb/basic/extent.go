package basic

import "sync/atomic"

// ExtentState represents the state of an extent
type ExtentState uint8

const (
	ExtentStateFree ExtentState = iota
	ExtentStatePartial
	ExtentStateFull
	ExtentStateSystem
)

// ExtentType 区类型
type ExtentType uint8

const (
	ExtentTypeData ExtentType = iota
	ExtentTypeIndex
	ExtentTypeSystem
)

// ExtentStats 区统计信息
type ExtentStats struct {
	TotalPages    atomic.Uint32
	FreePages     atomic.Uint32
	FragPages     atomic.Uint32
	LastAllocated int64
	LastFreed     int64
	LastDefragged int64
}

// ExtentHeader 区头部信息
type ExtentHeader struct {
	ExtentID   uint32
	SpaceID    uint32
	State      ExtentState
	Type       ExtentType
	SegmentID  uint64
	FirstPage  uint32
	PageCount  uint32
	FreeSpace  uint64
	CreateTime int64
}

// Extent 区接口
type Extent interface {
	// 基本信息
	GetID() uint32
	GetState() ExtentState
	GetType() ExtentType
	GetSpaceID() uint32
	GetSegmentID() uint64

	// 页面管理
	AllocatePage() (uint32, error)
	FreePage(pageNo uint32) error
	GetPageCount() uint32
	GetFreePages() []uint32

	// 空间管理
	GetFreeSpace() uint64
	IsFull() bool
	IsEmpty() bool

	// 统计信息
	GetStats() *ExtentStats

	// 维护操作
	Defragment() error
	Reset() error

	// 并发控制
	Lock()
	Unlock()
	RLock()
	RUnlock()

	StartPage() int

	GetStartPage() uint32

	GetBitmap() []byte
}

// ExtentManager 区管理器接口
type ExtentManager interface {
	// 区操作
	CreateExtent(spaceID uint32, extType ExtentType) (Extent, error)
	GetExtent(extentID uint32) (Extent, error)
	FreeExtent(extentID uint32) error

	// 页面管理
	AllocatePage(extentID uint32) (uint32, error)
	FreePage(extentID uint32, pageNo uint32) error

	// 空间管理
	GetFreeExtents() []uint32
	GetFullExtents() []uint32
	GetPartialExtents() []uint32

	// 统计信息
	GetStats() *ExtentManagerStats
}

// ExtentManagerStats 区管理器统计信息
type ExtentManagerStats struct {
	TotalExtents    atomic.Uint32
	FreeExtents     atomic.Uint32
	FullExtents     atomic.Uint32
	PartialExtents  atomic.Uint32
	TotalPages      atomic.Uint64
	FreePages       atomic.Uint64
	AllocOperations atomic.Uint64
	FreeOperations  atomic.Uint64
	LastDefragTime  int64
}

// ExtentList 区链表
type ExtentList interface {
	// 基本操作
	Add(extent Extent) error
	Remove(extentID uint32) error
	Get(extentID uint32) (Extent, error)
	GetAll() []Extent

	// 状态查询
	IsEmpty() bool
	GetCount() uint32
	GetTotalPages() uint32
	GetFreePages() uint32

	// 迭代器
	Iterator() ExtentIterator
}

// ExtentIterator 区迭代器
type ExtentIterator interface {
	// 迭代器操作
	Next() bool
	Current() Extent
	Reset()
	Close()
}

// ExtentInfo 区段信息
type ExtentInfo struct {
	ID        uint32
	StartPage uint32
	PageCount uint32
	FreePages uint32
	State     ExtentState
	Bitmap    []byte // 页面使用位图
}
