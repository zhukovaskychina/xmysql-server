/*
Extent（区）是InnoDB存储引擎中的物理空间管理单位。

特点:
1. 固定大小：1MB（64个页，每页16KB）
2. 页面分配：支持连续的页面分配
3. 状态管理：跟踪每个页面的使用状态
4. 并发控制：支持并发访问

状态:
- Free: 完全空闲
- Partial: 部分使用
- Full: 完全使用
- System: 系统使用
*/

package space

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"sync"
	"time"
)

// Constants for extent management
const (
	PagesPerExtent = 64    // 每个区64页
	PageSize       = 16384 // 16KB per page
	ExtentSize     = PagesPerExtent * PageSize
)

// ExtentStats holds statistics about an extent
type ExtentStats struct {
	AllocatedPages uint32    // Number of allocated pages
	FreePages      uint32    // Number of free pages
	FragmentCount  uint32    // Number of fragments
	LastAllocTime  time.Time // Last allocation time
	LastFreeTime   time.Time // Last free time
}

// ExtentImpl implements the Extent interface
type ExtentImpl struct {
	mu sync.RWMutex
	basic.Extent
	id        uint32              // Extent ID
	spaceID   uint32              // Tablespace ID
	startPage uint32              // First page number
	purpose   basic.ExtentPurpose // Purpose of this extent
	state     basic.ExtentState   // Current state
	pages     map[uint32]bool     // Page allocation map
	stats     ExtentStats         // Statistics
}

// NewExtent creates a new extent
func NewExtent(id, spaceID, startPage uint32, purpose basic.ExtentPurpose) *ExtentImpl {
	return &ExtentImpl{
		id:        id,
		spaceID:   spaceID,
		startPage: startPage,
		purpose:   purpose,
		state:     basic.ExtentStateFree,
		pages:     make(map[uint32]bool, PagesPerExtent),
		stats: ExtentStats{
			AllocatedPages: 0,
			FreePages:      PagesPerExtent,
			FragmentCount:  0,
			LastAllocTime:  time.Time{},
			LastFreeTime:   time.Time{},
		},
	}
}

// ID returns the extent ID
func (e *ExtentImpl) ID() uint32 {
	return e.id
}

// SpaceID returns the tablespace ID
func (e *ExtentImpl) SpaceID() uint32 {
	return e.spaceID
}

// StartPage returns the first page number in this extent
func (e *ExtentImpl) StartPage() int {
	return int(e.startPage)
}

// Purpose returns the extent's purpose
func (e *ExtentImpl) Purpose() basic.ExtentPurpose {
	return e.purpose
}

// IsFree returns whether the extent is free
func (e *ExtentImpl) IsFree() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == basic.ExtentStateFree
}

// SetFree sets the extent's free state
func (e *ExtentImpl) SetFree(free bool) {
	e.Lock()
	defer e.Unlock()
	if free {
		e.state = basic.ExtentStateFree
	} else {
		e.state = basic.ExtentStatePartial
	}
}

// AllocatePage allocates a new page from this extent
func (e *ExtentImpl) AllocatePage() (uint32, error) {
	e.Lock()
	defer e.Unlock()

	if e.stats.FreePages == 0 {
		return 0, fmt.Errorf("extent %d is full", e.id)
	}

	// Find first free page
	for offset := uint32(0); offset < PagesPerExtent; offset++ {
		pageNo := e.startPage + offset
		if !e.pages[pageNo] {
			// Allocate the page
			e.pages[pageNo] = true
			e.stats.AllocatedPages++
			e.stats.FreePages--
			e.stats.LastAllocTime = time.Now()

			// Update state
			if e.stats.FreePages == 0 {
				e.state = basic.ExtentStateFull
			} else if e.state == basic.ExtentStateFree {
				e.state = basic.ExtentStatePartial
			}

			return pageNo, nil
		}
	}

	return 0, fmt.Errorf("no free pages found in extent %d", e.id)
}

// FreePage frees a previously allocated page
func (e *ExtentImpl) FreePage(pageNo uint32) error {
	e.Lock()
	defer e.Unlock()

	// Validate page number
	if pageNo < e.startPage || pageNo >= e.startPage+PagesPerExtent {
		return fmt.Errorf("page %d is not in extent %d", pageNo, e.id)
	}

	// Check if page is allocated
	if !e.pages[pageNo] {
		return fmt.Errorf("page %d is already free", pageNo)
	}

	// Free the page
	delete(e.pages, pageNo)
	e.stats.AllocatedPages--
	e.stats.FreePages++
	e.stats.LastFreeTime = time.Now()

	// Update state
	if e.stats.AllocatedPages == 0 {
		e.state = basic.ExtentStateFree
	} else {
		e.state = basic.ExtentStatePartial
	}

	return nil
}

// IsPageAllocated checks if a page is allocated
func (e *ExtentImpl) IsPageAllocated(pageNo uint32) bool {
	e.RLock()
	defer e.RUnlock()
	return e.pages[pageNo]
}

// Stats returns current statistics
func (e *ExtentImpl) Stats() ExtentStats {
	e.RLock()
	defer e.RUnlock()
	return e.stats
}

// Reset resets the extent to its initial state
func (e *ExtentImpl) Reset() error {
	e.Lock()
	defer e.Unlock()

	e.state = basic.ExtentStateFree
	e.pages = make(map[uint32]bool, PagesPerExtent)
	e.stats = ExtentStats{
		AllocatedPages: 0,
		FreePages:      PagesPerExtent,
		FragmentCount:  0,
		LastAllocTime:  time.Time{},
		LastFreeTime:   time.Time{},
	}
	return nil
}

// 实现basic.Extent接口的其他方法

func (e *ExtentImpl) GetID() uint32 {
	return e.id
}

func (e *ExtentImpl) GetState() basic.ExtentState {
	e.RLock()
	defer e.RUnlock()
	return e.state
}

func (e *ExtentImpl) GetType() basic.ExtentType {
	// 根据purpose转换为type
	switch e.purpose {
	case basic.ExtentPurposeSystem:
		return basic.ExtentTypeSystem
	case basic.ExtentPurposeIndex:
		return basic.ExtentTypeIndex
	default:
		return basic.ExtentTypeData
	}
}

func (e *ExtentImpl) GetSpaceID() uint32 {
	return e.spaceID
}

func (e *ExtentImpl) GetSegmentID() uint64 {
	return uint64(e.id) // 简化实现
}

func (e *ExtentImpl) GetPageCount() uint32 {
	e.RLock()
	defer e.RUnlock()
	return e.stats.AllocatedPages
}

func (e *ExtentImpl) GetFreePages() []uint32 {
	e.RLock()
	defer e.RUnlock()

	freePages := make([]uint32, 0, e.stats.FreePages)
	for offset := uint32(0); offset < PagesPerExtent; offset++ {
		pageNo := e.startPage + offset
		if !e.pages[pageNo] {
			freePages = append(freePages, pageNo)
		}
	}
	return freePages
}

func (e *ExtentImpl) GetFreeSpace() uint64 {
	e.RLock()
	defer e.RUnlock()
	return uint64(e.stats.FreePages) * PageSize
}

func (e *ExtentImpl) IsFull() bool {
	e.RLock()
	defer e.RUnlock()
	return e.stats.FreePages == 0
}

func (e *ExtentImpl) IsEmpty() bool {
	e.RLock()
	defer e.RUnlock()
	return e.stats.AllocatedPages == 0
}

func (e *ExtentImpl) GetStats() *basic.ExtentStats {
	e.RLock()
	defer e.RUnlock()

	// 转换为basic.ExtentStats
	basicStats := &basic.ExtentStats{
		LastAllocated: e.stats.LastAllocTime.Unix(),
		LastFreed:     e.stats.LastFreeTime.Unix(),
		LastDefragged: 0,
	}
	basicStats.TotalPages.Store(PagesPerExtent)
	basicStats.FreePages.Store(e.stats.FreePages)
	basicStats.FragPages.Store(e.stats.FragmentCount)

	return basicStats
}

func (e *ExtentImpl) Defragment() error {
	// TODO: 实现碎片整理逻辑
	return nil
}

func (e *ExtentImpl) Lock() {
	e.mu.Lock()
}

func (e *ExtentImpl) Unlock() {
	e.mu.Unlock()
}

func (e *ExtentImpl) RLock() {
	e.mu.RLock()
}

func (e *ExtentImpl) RUnlock() {
	e.mu.RUnlock()
}

func (e *ExtentImpl) GetStartPage() uint32 {
	return e.startPage
}

func (e *ExtentImpl) GetBitmap() []byte {
	e.RLock()
	defer e.RUnlock()

	// 创建位图，每个位表示一个页面的分配状态
	bitmap := make([]byte, (PagesPerExtent+7)/8) // 向上取整到字节
	for offset := uint32(0); offset < PagesPerExtent; offset++ {
		pageNo := e.startPage + offset
		if e.pages[pageNo] {
			byteIndex := offset / 8
			bitIndex := offset % 8
			bitmap[byteIndex] |= 1 << bitIndex
		}
	}
	return bitmap
}
