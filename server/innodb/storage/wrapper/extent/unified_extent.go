/*
Unified Extent Implementation

This file provides a unified extent implementation that combines the best features
of all previous extent implementations:

1. Serialization support (from ExtentEntry)
2. Concurrency control (from BaseExtent/ExtentImpl)
3. Statistics tracking (from BaseExtent/ExtentImpl)
4. Purpose tracking (from ExtentImpl)
5. Hybrid bitmap/map page tracking for optimal performance

Architecture:
- Uses extents.ExtentEntry for persistent storage format
- Adds runtime structures for performance (map, list)
- Implements full basic.Extent interface
- Thread-safe with RWMutex
*/

package extent

import (
	"fmt"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/extents"
)

// Error variables are already defined in extent.go, so we don't redeclare them here

const (
	PagesPerExtent = 64    // 每个区64页
	PageSize       = 16384 // 16KB per page
	ExtentSize     = PagesPerExtent * PageSize
)

// UnifiedExtent is the unified extent implementation that combines all features
type UnifiedExtent struct {
	mu sync.RWMutex

	// Core identification
	id        uint32
	spaceID   uint32
	segmentID uint64
	startPage uint32

	// Type and purpose
	extType basic.ExtentType
	purpose basic.ExtentPurpose
	state   basic.ExtentState

	// Page tracking (hybrid approach)
	bitmap   [16]byte        // Persistent bitmap (2 bits per page, 64 pages = 128 bits = 16 bytes)
	pages    map[uint32]bool // Runtime cache for O(1) lookup
	pageList []uint32        // Ordered list for iteration

	// Statistics
	stats basic.ExtentStats

	// Persistence layer
	entry *extents.ExtentEntry // Underlying storage format
}

// NewUnifiedExtent creates a new unified extent
func NewUnifiedExtent(id, spaceID, startPage uint32, extType basic.ExtentType, purpose basic.ExtentPurpose) *UnifiedExtent {
	ue := &UnifiedExtent{
		id:        id,
		spaceID:   spaceID,
		segmentID: 0,
		startPage: startPage,
		extType:   extType,
		purpose:   purpose,
		state:     basic.ExtentStateFree,
		pages:     make(map[uint32]bool, PagesPerExtent),
		pageList:  make([]uint32, 0, PagesPerExtent),
		stats: basic.ExtentStats{
			TotalPages:    PagesPerExtent,
			FreePages:     PagesPerExtent,
			FragPages:     0,
			LastAllocated: 0,
			LastFreed:     0,
			LastDefragged: 0,
		},
	}

	// Create underlying ExtentEntry
	ue.entry = extents.NewExtentEntry(startPage)

	return ue
}

// NewUnifiedExtentFromEntry creates a unified extent from an ExtentEntry
func NewUnifiedExtentFromEntry(entry *extents.ExtentEntry, id, spaceID uint32, extType basic.ExtentType, purpose basic.ExtentPurpose) *UnifiedExtent {
	ue := &UnifiedExtent{
		id:        id,
		spaceID:   spaceID,
		segmentID: entry.GetSegmentID(),
		startPage: entry.FirstPageNo,
		extType:   extType,
		purpose:   purpose,
		state:     mapExtentState(entry.GetState()),
		bitmap:    entry.PageBitmap,
		pages:     make(map[uint32]bool, PagesPerExtent),
		pageList:  make([]uint32, 0, entry.GetUsedPages()),
		entry:     entry,
	}

	// Build runtime structures from bitmap
	usedPages := uint32(0)
	for offset := uint8(0); offset < PagesPerExtent; offset++ {
		if !entry.IsPageFree(offset) {
			pageNo := entry.FirstPageNo + uint32(offset)
			ue.pages[pageNo] = true
			ue.pageList = append(ue.pageList, pageNo)
			usedPages++
		}
	}

	// Initialize statistics
	ue.stats = basic.ExtentStats{
		TotalPages:    PagesPerExtent,
		FreePages:     PagesPerExtent - usedPages,
		FragPages:     0,
		LastAllocated: 0,
		LastFreed:     0,
		LastDefragged: 0,
	}

	return ue
}

// mapExtentState converts store layer state to basic layer state
func mapExtentState(storeState uint8) basic.ExtentState {
	switch storeState {
	case extents.EXTENT_FREE:
		return basic.ExtentStateFree
	case extents.EXTENT_PARTIAL:
		return basic.ExtentStatePartial
	case extents.EXTENT_FULL:
		return basic.ExtentStateFull
	case extents.EXTENT_SYSTEM:
		return basic.ExtentStateSystem
	default:
		return basic.ExtentStateFree
	}
}

// mapToStoreState converts basic layer state to store layer state
func mapToStoreState(basicState basic.ExtentState) uint8 {
	switch basicState {
	case basic.ExtentStateFree:
		return extents.EXTENT_FREE
	case basic.ExtentStatePartial:
		return extents.EXTENT_PARTIAL
	case basic.ExtentStateFull:
		return extents.EXTENT_FULL
	case basic.ExtentStateSystem:
		return extents.EXTENT_SYSTEM
	default:
		return extents.EXTENT_FREE
	}
}

// ============================================================================
// basic.Extent Interface Implementation
// ============================================================================

// GetID returns the extent ID
func (ue *UnifiedExtent) GetID() uint32 {
	return ue.id
}

// GetState returns the extent state
func (ue *UnifiedExtent) GetState() basic.ExtentState {
	ue.mu.RLock()
	defer ue.mu.RUnlock()
	return ue.state
}

// GetType returns the extent type
func (ue *UnifiedExtent) GetType() basic.ExtentType {
	return ue.extType
}

// GetSpaceID returns the tablespace ID
func (ue *UnifiedExtent) GetSpaceID() uint32 {
	return ue.spaceID
}

// GetSegmentID returns the segment ID
func (ue *UnifiedExtent) GetSegmentID() uint64 {
	ue.mu.RLock()
	defer ue.mu.RUnlock()
	return ue.segmentID
}

// SetSegmentID sets the segment ID
func (ue *UnifiedExtent) SetSegmentID(segID uint64) {
	ue.mu.Lock()
	defer ue.mu.Unlock()
	ue.segmentID = segID
	ue.entry.SetSegmentID(segID)
}

// AllocatePage allocates a page from this extent
func (ue *UnifiedExtent) AllocatePage() (uint32, error) {
	ue.mu.Lock()
	defer ue.mu.Unlock()

	if ue.stats.FreePages == 0 {
		return 0, ErrExtentFull
	}

	// Find first free page using bitmap
	for offset := uint8(0); offset < PagesPerExtent; offset++ {
		if ue.entry.IsPageFree(offset) {
			pageNo := ue.startPage + uint32(offset)

			// Allocate in entry (updates bitmap)
			if err := ue.entry.AllocatePage(offset); err != nil {
				return 0, fmt.Errorf("failed to allocate page in entry: %w", err)
			}

			// Update runtime structures
			ue.pages[pageNo] = true
			ue.pageList = append(ue.pageList, pageNo)

			// Update statistics
			ue.stats.FreePages--
			ue.stats.LastAllocated = time.Now().UnixNano()

			// Update state
			ue.state = mapExtentState(ue.entry.GetState())

			return pageNo, nil
		}
	}

	return 0, ErrExtentFull
}

// FreePage frees a previously allocated page
func (ue *UnifiedExtent) FreePage(pageNo uint32) error {
	ue.mu.Lock()
	defer ue.mu.Unlock()

	// Validate page number
	if pageNo < ue.startPage || pageNo >= ue.startPage+PagesPerExtent {
		return ErrPageNotFound
	}

	// Check if page is allocated
	if !ue.pages[pageNo] {
		return ErrPageNotFound
	}

	offset := uint8(pageNo - ue.startPage)

	// Free in entry (updates bitmap)
	if err := ue.entry.FreePage(offset); err != nil {
		return fmt.Errorf("failed to free page in entry: %w", err)
	}

	// Update runtime structures
	delete(ue.pages, pageNo)
	for i, p := range ue.pageList {
		if p == pageNo {
			ue.pageList = append(ue.pageList[:i], ue.pageList[i+1:]...)
			break
		}
	}

	// Update statistics
	ue.stats.FreePages++
	ue.stats.LastFreed = time.Now().UnixNano()

	// Update state
	ue.state = mapExtentState(ue.entry.GetState())

	return nil
}

// GetPageCount returns the number of allocated pages
func (ue *UnifiedExtent) GetPageCount() uint32 {
	ue.mu.RLock()
	defer ue.mu.RUnlock()
	return uint32(ue.entry.GetUsedPages())
}

// GetFreePages returns a list of free page numbers
func (ue *UnifiedExtent) GetFreePages() []uint32 {
	ue.mu.RLock()
	defer ue.mu.RUnlock()

	freePages := make([]uint32, 0, ue.stats.FreePages)
	for offset := uint8(0); offset < PagesPerExtent; offset++ {
		if ue.entry.IsPageFree(offset) {
			freePages = append(freePages, ue.startPage+uint32(offset))
		}
	}
	return freePages
}

// GetFreeSpace returns the amount of free space in bytes
func (ue *UnifiedExtent) GetFreeSpace() uint64 {
	ue.mu.RLock()
	defer ue.mu.RUnlock()
	return uint64(ue.stats.FreePages) * PageSize
}

// IsFull returns true if the extent is full
func (ue *UnifiedExtent) IsFull() bool {
	ue.mu.RLock()
	defer ue.mu.RUnlock()
	return ue.state == basic.ExtentStateFull
}

// IsEmpty returns true if the extent is empty
func (ue *UnifiedExtent) IsEmpty() bool {
	ue.mu.RLock()
	defer ue.mu.RUnlock()
	return ue.state == basic.ExtentStateFree
}

// GetStats returns extent statistics
func (ue *UnifiedExtent) GetStats() *basic.ExtentStats {
	ue.mu.RLock()
	defer ue.mu.RUnlock()
	statsCopy := ue.stats
	return &statsCopy
}

// Defragment performs defragmentation on the extent
func (ue *UnifiedExtent) Defragment() error {
	ue.mu.Lock()
	defer ue.mu.Unlock()

	// TODO: Implement actual defragmentation logic
	// For now, just update the timestamp
	ue.stats.LastDefragged = time.Now().UnixNano()
	return nil
}

// Reset resets the extent to its initial state
func (ue *UnifiedExtent) Reset() error {
	ue.mu.Lock()
	defer ue.mu.Unlock()

	// Reset entry
	ue.entry = extents.NewExtentEntry(ue.startPage)
	ue.entry.SetSegmentID(ue.segmentID)

	// Reset runtime structures
	ue.pages = make(map[uint32]bool, PagesPerExtent)
	ue.pageList = make([]uint32, 0, PagesPerExtent)
	ue.bitmap = [16]byte{}

	// Reset state
	ue.state = basic.ExtentStateFree

	// Reset statistics
	ue.stats = basic.ExtentStats{
		TotalPages:    PagesPerExtent,
		FreePages:     PagesPerExtent,
		FragPages:     0,
		LastAllocated: 0,
		LastFreed:     0,
		LastDefragged: 0,
	}

	return nil
}

// Lock acquires a write lock
func (ue *UnifiedExtent) Lock() {
	ue.mu.Lock()
}

// Unlock releases a write lock
func (ue *UnifiedExtent) Unlock() {
	ue.mu.Unlock()
}

// RLock acquires a read lock
func (ue *UnifiedExtent) RLock() {
	ue.mu.RLock()
}

// RUnlock releases a read lock
func (ue *UnifiedExtent) RUnlock() {
	ue.mu.RUnlock()
}

// StartPage returns the first page number (as int for compatibility)
func (ue *UnifiedExtent) StartPage() int {
	return int(ue.startPage)
}

// GetStartPage returns the first page number
func (ue *UnifiedExtent) GetStartPage() uint32 {
	return ue.startPage
}

// GetBitmap returns the page allocation bitmap
func (ue *UnifiedExtent) GetBitmap() []byte {
	ue.mu.RLock()
	defer ue.mu.RUnlock()

	// Return a copy of the bitmap
	bitmap := make([]byte, 16)
	copy(bitmap, ue.entry.PageBitmap[:])
	return bitmap
}

// ============================================================================
// Additional Methods (Not in basic.Extent interface)
// ============================================================================

// GetPurpose returns the extent purpose
func (ue *UnifiedExtent) GetPurpose() basic.ExtentPurpose {
	return ue.purpose
}

// SetPurpose sets the extent purpose
func (ue *UnifiedExtent) SetPurpose(purpose basic.ExtentPurpose) {
	ue.mu.Lock()
	defer ue.mu.Unlock()
	ue.purpose = purpose
}

// IsPageAllocated checks if a specific page is allocated
func (ue *UnifiedExtent) IsPageAllocated(pageNo uint32) bool {
	ue.mu.RLock()
	defer ue.mu.RUnlock()

	if pageNo < ue.startPage || pageNo >= ue.startPage+PagesPerExtent {
		return false
	}

	return ue.pages[pageNo]
}

// GetAllocatedPages returns a list of allocated page numbers
func (ue *UnifiedExtent) GetAllocatedPages() []uint32 {
	ue.mu.RLock()
	defer ue.mu.RUnlock()

	// Return a copy of the page list
	pages := make([]uint32, len(ue.pageList))
	copy(pages, ue.pageList)
	return pages
}

// Serialize serializes the extent to bytes
func (ue *UnifiedExtent) Serialize() []byte {
	ue.mu.RLock()
	defer ue.mu.RUnlock()
	return ue.entry.Serialize()
}

// GetEntry returns the underlying ExtentEntry (for persistence)
func (ue *UnifiedExtent) GetEntry() *extents.ExtentEntry {
	ue.mu.RLock()
	defer ue.mu.RUnlock()
	return ue.entry
}

// String returns a string representation of the extent
func (ue *UnifiedExtent) String() string {
	ue.mu.RLock()
	defer ue.mu.RUnlock()

	return fmt.Sprintf("Extent{ID:%d, Space:%d, Segment:%d, Start:%d, Type:%v, Purpose:%v, State:%v, Used:%d/%d}",
		ue.id, ue.spaceID, ue.segmentID, ue.startPage, ue.extType, ue.purpose, ue.state,
		ue.entry.GetUsedPages(), PagesPerExtent)
}
