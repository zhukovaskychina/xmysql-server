package types

import (
	"encoding/binary"
	"errors"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

// Index page specific errors
var (
	ErrRecordNotFound    = errors.New("record not found")
	ErrPageFull          = errors.New("page is full")
	ErrInvalidRecordSize = errors.New("invalid record size")
)

// Index page header offsets (56 bytes total)
const (
	PAGE_N_DIR_SLOTS  = 0  // Number of directory slots (2 bytes)
	PAGE_HEAP_TOP     = 2  // Top of heap (2 bytes)
	PAGE_N_HEAP       = 4  // Number of records in heap (2 bytes)
	PAGE_FREE         = 6  // Pointer to free list (2 bytes)
	PAGE_GARBAGE      = 8  // Garbage bytes (2 bytes)
	PAGE_LAST_INSERT  = 10 // Last insert position (2 bytes)
	PAGE_DIRECTION    = 12 // Insert direction (2 bytes)
	PAGE_N_DIRECTION  = 14 // Consecutive inserts in direction (2 bytes)
	PAGE_N_RECS       = 16 // Number of user records (2 bytes)
	PAGE_MAX_TRX_ID   = 18 // Max transaction ID (8 bytes)
	PAGE_LEVEL        = 26 // B+tree level (2 bytes)
	PAGE_INDEX_ID     = 28 // Index ID (8 bytes)
	PAGE_BTR_SEG_LEAF = 36 // Leaf segment header (10 bytes)
	PAGE_BTR_SEG_TOP  = 46 // Non-leaf segment header (10 bytes)
	INDEX_HEADER_SIZE = 56 // Total index header size
)

// Infimum and supremum record constants
const (
	INFIMUM_SUPREMUM_SIZE = 26 // Size of infimum + supremum records
	INFIMUM_OFFSET        = 0  // Offset of infimum record
	SUPREMUM_OFFSET       = 13 // Offset of supremum record
)

// Insert direction constants
const (
	PAGE_LEFT         = 1 // Insert to the left
	PAGE_RIGHT        = 2 // Insert to the right
	PAGE_SAME         = 3 // Insert at same position
	PAGE_NO_DIRECTION = 0 // No specific direction
)

// IndexHeader represents the index page header structure
type IndexHeader struct {
	NDirSlots   uint16   // Number of directory slots
	HeapTop     uint16   // Top of heap
	NHeap       uint16   // Number of records in heap
	Free        uint16   // Pointer to free list
	Garbage     uint16   // Garbage bytes
	LastInsert  uint16   // Last insert position
	Direction   uint16   // Insert direction
	NDirections uint16   // Consecutive inserts in direction
	NRecs       uint16   // Number of user records
	MaxTrxID    uint64   // Max transaction ID
	Level       uint16   // B+tree level (0 for leaf)
	IndexID     uint64   // Index ID
	BtrSegLeaf  [10]byte // Leaf segment header
	BtrSegTop   [10]byte // Non-leaf segment header
}

// IndexPage represents a B+Tree index page
type IndexPage struct {
	*UnifiedPage // Embedded base page

	// Index-specific header (56 bytes)
	indexHeader IndexHeader

	// Page content sections
	infimumSupremum []byte // Min/max records (26 bytes)
	userRecords     []byte // Actual user records
	freeSpace       []byte // Free space
	pageDirectory   []byte // Slot directory
}

// NewIndexPage creates a new index page
func NewIndexPage(spaceID, pageNo uint32) *IndexPage {
	ip := &IndexPage{
		UnifiedPage:     NewUnifiedPage(spaceID, pageNo, common.FIL_PAGE_INDEX),
		infimumSupremum: make([]byte, INFIMUM_SUPREMUM_SIZE),
		userRecords:     make([]byte, 0),
		freeSpace:       make([]byte, 0),
		pageDirectory:   make([]byte, 0),
	}

	// Initialize index header with defaults
	ip.indexHeader = IndexHeader{
		NDirSlots:   2, // Minimum: infimum and supremum
		HeapTop:     FileHeaderSize + INDEX_HEADER_SIZE + INFIMUM_SUPREMUM_SIZE,
		NHeap:       2, // Infimum and supremum
		Free:        0, // No free list initially
		Garbage:     0,
		LastInsert:  0,
		Direction:   PAGE_NO_DIRECTION,
		NDirections: 0,
		NRecs:       0, // No user records initially
		MaxTrxID:    0,
		Level:       0, // Leaf page by default
		IndexID:     0,
	}

	// Initialize infimum and supremum records
	ip.initInfimumSupremum()

	// Serialize index header to page body
	ip.serializeIndexHeader()

	return ip
}

// initInfimumSupremum initializes the infimum and supremum records
func (ip *IndexPage) initInfimumSupremum() {
	// Infimum record (13 bytes)
	// Format: record header (5 bytes) + "infimum" (8 bytes)
	infimum := []byte{
		0x01, 0x00, 0x02, 0x00, 0x0D, // Record header
		'i', 'n', 'f', 'i', 'm', 'u', 'm', 0x00, // "infimum"
	}
	copy(ip.infimumSupremum[INFIMUM_OFFSET:], infimum)

	// Supremum record (13 bytes)
	// Format: record header (5 bytes) + "supremum" (8 bytes)
	supremum := []byte{
		0x01, 0x00, 0x02, 0x00, 0x00, // Record header
		's', 'u', 'p', 'r', 'e', 'm', 'u', 'm', // "supremum"
	}
	copy(ip.infimumSupremum[SUPREMUM_OFFSET:], supremum)
}

// serializeIndexHeader serializes the index header to the page body
func (ip *IndexPage) serializeIndexHeader() {
	body := ip.GetBody()
	if len(body) < INDEX_HEADER_SIZE {
		return
	}

	binary.BigEndian.PutUint16(body[PAGE_N_DIR_SLOTS:], ip.indexHeader.NDirSlots)
	binary.BigEndian.PutUint16(body[PAGE_HEAP_TOP:], ip.indexHeader.HeapTop)
	binary.BigEndian.PutUint16(body[PAGE_N_HEAP:], ip.indexHeader.NHeap)
	binary.BigEndian.PutUint16(body[PAGE_FREE:], ip.indexHeader.Free)
	binary.BigEndian.PutUint16(body[PAGE_GARBAGE:], ip.indexHeader.Garbage)
	binary.BigEndian.PutUint16(body[PAGE_LAST_INSERT:], ip.indexHeader.LastInsert)
	binary.BigEndian.PutUint16(body[PAGE_DIRECTION:], ip.indexHeader.Direction)
	binary.BigEndian.PutUint16(body[PAGE_N_DIRECTION:], ip.indexHeader.NDirections)
	binary.BigEndian.PutUint16(body[PAGE_N_RECS:], ip.indexHeader.NRecs)
	binary.BigEndian.PutUint64(body[PAGE_MAX_TRX_ID:], ip.indexHeader.MaxTrxID)
	binary.BigEndian.PutUint16(body[PAGE_LEVEL:], ip.indexHeader.Level)
	binary.BigEndian.PutUint64(body[PAGE_INDEX_ID:], ip.indexHeader.IndexID)
	copy(body[PAGE_BTR_SEG_LEAF:], ip.indexHeader.BtrSegLeaf[:])
	copy(body[PAGE_BTR_SEG_TOP:], ip.indexHeader.BtrSegTop[:])

	// Copy infimum and supremum
	copy(body[INDEX_HEADER_SIZE:], ip.infimumSupremum)

	ip.SetBody(body)
}

// deserializeIndexHeader deserializes the index header from the page body
func (ip *IndexPage) deserializeIndexHeader() {
	body := ip.GetBody()
	if len(body) < INDEX_HEADER_SIZE {
		return
	}

	ip.indexHeader.NDirSlots = binary.BigEndian.Uint16(body[PAGE_N_DIR_SLOTS:])
	ip.indexHeader.HeapTop = binary.BigEndian.Uint16(body[PAGE_HEAP_TOP:])
	ip.indexHeader.NHeap = binary.BigEndian.Uint16(body[PAGE_N_HEAP:])
	ip.indexHeader.Free = binary.BigEndian.Uint16(body[PAGE_FREE:])
	ip.indexHeader.Garbage = binary.BigEndian.Uint16(body[PAGE_GARBAGE:])
	ip.indexHeader.LastInsert = binary.BigEndian.Uint16(body[PAGE_LAST_INSERT:])
	ip.indexHeader.Direction = binary.BigEndian.Uint16(body[PAGE_DIRECTION:])
	ip.indexHeader.NDirections = binary.BigEndian.Uint16(body[PAGE_N_DIRECTION:])
	ip.indexHeader.NRecs = binary.BigEndian.Uint16(body[PAGE_N_RECS:])
	ip.indexHeader.MaxTrxID = binary.BigEndian.Uint64(body[PAGE_MAX_TRX_ID:])
	ip.indexHeader.Level = binary.BigEndian.Uint16(body[PAGE_LEVEL:])
	ip.indexHeader.IndexID = binary.BigEndian.Uint64(body[PAGE_INDEX_ID:])
	copy(ip.indexHeader.BtrSegLeaf[:], body[PAGE_BTR_SEG_LEAF:])
	copy(ip.indexHeader.BtrSegTop[:], body[PAGE_BTR_SEG_TOP:])

	// Copy infimum and supremum
	copy(ip.infimumSupremum, body[INDEX_HEADER_SIZE:INDEX_HEADER_SIZE+INFIMUM_SUPREMUM_SIZE])
}

// GetIndexHeader returns the index header
func (ip *IndexPage) GetIndexHeader() *IndexHeader {
	return &ip.indexHeader
}

// GetUserRecords returns the user records
func (ip *IndexPage) GetUserRecords() []byte {
	result := make([]byte, len(ip.userRecords))
	copy(result, ip.userRecords)
	return result
}

// AddRecord adds a record to the page
func (ip *IndexPage) AddRecord(record []byte) error {
	if len(record) == 0 {
		return ErrInvalidRecordSize
	}

	// Check if there's enough space
	freeSpace := ip.GetFreeSpace()
	if uint16(len(record)) > freeSpace {
		return ErrPageFull
	}

	// Add record to user records
	ip.userRecords = append(ip.userRecords, record...)

	// Update index header
	ip.indexHeader.NRecs++
	ip.indexHeader.NHeap++
	ip.indexHeader.HeapTop += uint16(len(record))
	ip.indexHeader.LastInsert = ip.indexHeader.HeapTop - uint16(len(record))

	// Update direction tracking
	// This is a simplified version - real implementation would track insert position
	ip.indexHeader.NDirections++

	// Serialize changes
	ip.serializeIndexHeader()

	// Mark page as dirty
	ip.MarkDirty()

	return nil
}

// DeleteRecord deletes a record at the given offset
func (ip *IndexPage) DeleteRecord(offset uint16) error {
	if offset >= ip.indexHeader.HeapTop {
		return ErrRecordNotFound
	}

	// This is a simplified implementation
	// Real implementation would:
	// 1. Mark record as deleted
	// 2. Add to free list
	// 3. Update garbage count
	// 4. Potentially compact page

	ip.indexHeader.NRecs--
	ip.indexHeader.Garbage += 10 // Approximate record size

	// Serialize changes
	ip.serializeIndexHeader()

	// Mark page as dirty
	ip.MarkDirty()

	return nil
}

// GetPageDirectory returns the page directory
func (ip *IndexPage) GetPageDirectory() []byte {
	result := make([]byte, len(ip.pageDirectory))
	copy(result, ip.pageDirectory)
	return result
}

// IsLeafPage returns true if this is a leaf page
func (ip *IndexPage) IsLeafPage() bool {
	return ip.indexHeader.Level == 0
}

// GetLevel returns the B+tree level
func (ip *IndexPage) GetLevel() uint16 {
	return ip.indexHeader.Level
}

// SetLevel sets the B+tree level
func (ip *IndexPage) SetLevel(level uint16) {
	ip.indexHeader.Level = level
	ip.serializeIndexHeader()
	ip.MarkDirty()
}

// GetIndexID returns the index ID
func (ip *IndexPage) GetIndexID() uint64 {
	return ip.indexHeader.IndexID
}

// SetIndexID sets the index ID
func (ip *IndexPage) SetIndexID(indexID uint64) {
	ip.indexHeader.IndexID = indexID
	ip.serializeIndexHeader()
	ip.MarkDirty()
}

// GetRecordCount returns the number of user records
func (ip *IndexPage) GetRecordCount() uint16 {
	return ip.indexHeader.NRecs
}

// GetFreeSpace returns the amount of free space in the page
func (ip *IndexPage) GetFreeSpace() uint16 {
	// Calculate free space
	// Total page size - header - index header - infimum/supremum - used records - directory - trailer
	usedSpace := FileHeaderSize + INDEX_HEADER_SIZE + INFIMUM_SUPREMUM_SIZE +
		uint16(len(ip.userRecords)) + uint16(len(ip.pageDirectory)) + FileTrailerSize

	if usedSpace >= uint16(ip.GetSize()) {
		return 0
	}

	return uint16(ip.GetSize()) - usedSpace
}

// GetMaxTrxID returns the maximum transaction ID
func (ip *IndexPage) GetMaxTrxID() uint64 {
	return ip.indexHeader.MaxTrxID
}

// SetMaxTrxID sets the maximum transaction ID
func (ip *IndexPage) SetMaxTrxID(trxID uint64) {
	if trxID > ip.indexHeader.MaxTrxID {
		ip.indexHeader.MaxTrxID = trxID
		ip.serializeIndexHeader()
		ip.MarkDirty()
	}
}

// Compact compacts the page by removing garbage
func (ip *IndexPage) Compact() error {
	// This is a simplified implementation
	// Real implementation would:
	// 1. Collect all valid records
	// 2. Rebuild page from scratch
	// 3. Update all pointers and directory

	if ip.indexHeader.Garbage == 0 {
		return nil // Nothing to compact
	}

	// Reset garbage counter
	ip.indexHeader.Garbage = 0
	ip.serializeIndexHeader()
	ip.MarkDirty()

	return nil
}

// ParseFromBytes overrides UnifiedPage to also parse index-specific data
func (ip *IndexPage) ParseFromBytes(data []byte) error {
	// First parse the base page
	if err := ip.UnifiedPage.ParseFromBytes(data); err != nil {
		return err
	}

	// Then parse index-specific data
	ip.deserializeIndexHeader()

	return nil
}

// ToBytes overrides UnifiedPage to include index-specific data
func (ip *IndexPage) ToBytes() ([]byte, error) {
	// Serialize index header first
	ip.serializeIndexHeader()

	// Then use base page serialization
	return ip.UnifiedPage.ToBytes()
}
