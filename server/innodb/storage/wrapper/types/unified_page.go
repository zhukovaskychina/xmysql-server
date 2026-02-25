package types

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

// UnifiedPage errors
var (
	ErrUnifiedInvalidPageSize   = errors.New("invalid page size")
	ErrUnifiedPageCorrupted     = errors.New("page is corrupted")
	ErrUnifiedPageNotLoaded     = errors.New("page not loaded")
	ErrUnifiedInvalidData       = errors.New("invalid page data")
	ErrUnifiedDataTooLarge      = errors.New("data too large for page")
	ErrUnifiedInsufficientSpace = errors.New("insufficient space in page")
)

// File header field offsets (InnoDB format)
const (
	FIL_PAGE_SPACE_OR_CHKSUM = 0  // Checksum (4 bytes)
	FIL_PAGE_OFFSET          = 4  // Page number (4 bytes)
	FIL_PAGE_PREV            = 8  // Previous page (4 bytes)
	FIL_PAGE_NEXT            = 12 // Next page (4 bytes)
	FIL_PAGE_LSN             = 16 // LSN (8 bytes)
	FIL_PAGE_TYPE            = 24 // Page type (2 bytes)
	FIL_PAGE_FILE_FLUSH_LSN  = 26 // Flush LSN (8 bytes)
	FIL_PAGE_SPACE_ID        = 34 // Space ID (4 bytes)
	FIL_PAGE_DATA            = 38 // Start of page body
)

// File trailer field offsets
const (
	FIL_PAGE_END_LSN_OLD_CHKSUM = 0 // Old checksum (4 bytes)
	FIL_PAGE_LSN_LOW            = 4 // Low 32 bits of LSN (4 bytes)
)

// Page size constants
const (
	FileHeaderSize  = 38    // File header size
	FileTrailerSize = 8     // File trailer size
	DefaultPageSize = 16384 // 16KB default page size
	PageBodySize    = DefaultPageSize - FileHeaderSize - FileTrailerSize
)

// FileHeader represents the InnoDB file header structure
type FileHeader struct {
	Checksum uint32 // Page checksum
	PageNo   uint32 // Page number
	PrevPage uint32 // Previous page number
	NextPage uint32 // Next page number
	LSN      uint64 // Log sequence number
	PageType uint16 // Page type
	FlushLSN uint64 // Flush LSN
	SpaceID  uint32 // Tablespace ID
}

// FileTrailer represents the InnoDB file trailer structure
type FileTrailer struct {
	OldChecksum uint32 // Old-style checksum
	LSNLow      uint32 // Low 32 bits of LSN
}

// PageStats tracks page access statistics
type PageStats struct {
	ReadCount    uint64 // Number of reads
	WriteCount   uint64 // Number of writes
	LastAccessed uint64 // Last access timestamp (nanoseconds)
	LastModified uint64 // Last modification timestamp (nanoseconds)
	PinCount     uint64 // Current pin count
	DirtyCount   uint64 // Number of times marked dirty
}

// UnifiedPage is the base class for all page types
// It consolidates functionality from AbstractPage, BasePage, and BasePageWrapper
type UnifiedPage struct {
	// Concurrency control
	mu sync.RWMutex

	// Core page structure (InnoDB format)
	header  FileHeader  // 38 bytes - File header
	body    []byte      // Variable - Page body content
	trailer FileTrailer // 8 bytes - File trailer

	// Page metadata
	spaceID  uint32          // Tablespace ID
	pageNo   uint32          // Page number within tablespace
	pageType common.PageType // Page type (INDEX, BLOB, UNDO, etc.)
	size     uint32          // Page size (typically 16KB)

	// State management (atomic for thread-safety)
	state    uint32 // Page state (Clean, Dirty, Loading, etc.) - use atomic operations
	lsn      uint64 // Log Sequence Number - use atomic operations
	dirty    uint32 // Dirty flag (0=false, 1=true) - use atomic operations
	pinCount int32  // Pin count for buffer pool - use atomic operations

	// Statistics
	stats PageStats // Access statistics

	// Persistence
	rawData []byte // Raw page data for serialization

	// Buffer pool integration
	bufferPage *buffer_pool.BufferPage // Associated buffer page
}

// NewUnifiedPage creates a new unified page
func NewUnifiedPage(spaceID, pageNo uint32, pageType common.PageType) *UnifiedPage {
	page := &UnifiedPage{
		spaceID:  spaceID,
		pageNo:   pageNo,
		pageType: pageType,
		size:     DefaultPageSize,
		body:     make([]byte, PageBodySize),
		rawData:  make([]byte, DefaultPageSize),
	}

	// Initialize file header
	page.header = FileHeader{
		Checksum: 0,
		PageNo:   pageNo,
		PrevPage: 0xFFFFFFFF, // No previous page
		NextPage: 0xFFFFFFFF, // No next page
		LSN:      0,
		PageType: uint16(pageType),
		FlushLSN: 0,
		SpaceID:  spaceID,
	}

	// Initialize file trailer
	page.trailer = FileTrailer{
		OldChecksum: 0,
		LSNLow:      0,
	}

	// Initialize state
	atomic.StoreUint32(&page.state, uint32(basic.PageStateActive))
	atomic.StoreUint32(&page.dirty, 0)
	atomic.StoreUint64(&page.lsn, 0)
	atomic.StoreInt32(&page.pinCount, 0)

	// Initialize statistics
	page.stats.LastAccessed = uint64(time.Now().UnixNano())
	page.stats.LastModified = uint64(time.Now().UnixNano())

	return page
}

// Init initializes the page structure
func (p *UnifiedPage) Init() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Clear body
	for i := range p.body {
		p.body[i] = 0
	}

	// Update checksum
	p.updateChecksumInternal()

	// Serialize to rawData
	return p.serializeInternal()
}

// GetPageID returns the page ID (same as page number)
func (p *UnifiedPage) GetPageID() uint32 {
	return p.pageNo
}

// GetPageNo returns the page number
func (p *UnifiedPage) GetPageNo() uint32 {
	return p.pageNo
}

// GetSpaceID returns the tablespace ID
func (p *UnifiedPage) GetSpaceID() uint32 {
	return p.spaceID
}

// GetPageType returns the page type
func (p *UnifiedPage) GetPageType() common.PageType {
	return p.pageType
}

// GetSize returns the page size
func (p *UnifiedPage) GetSize() uint32 {
	return p.size
}

// GetData returns the complete page data
func (p *UnifiedPage) GetData() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Update statistics
	p.updateAccessStats()

	result := make([]byte, p.size)
	copy(result, p.rawData)
	return result
}

// SetData sets the complete page data
func (p *UnifiedPage) SetData(data []byte) error {
	if len(data) != int(p.size) {
		return ErrUnifiedInvalidPageSize
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	copy(p.rawData, data)
	p.MarkDirty()

	// Parse header and trailer
	return p.deserializeInternal(data)
}

// GetBody returns the page body content
func (p *UnifiedPage) GetBody() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]byte, len(p.body))
	copy(result, p.body)
	return result
}

// SetBody sets the page body content
func (p *UnifiedPage) SetBody(body []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(body) > len(p.body) {
		body = body[:len(p.body)]
	}

	copy(p.body, body)
	p.MarkDirty()
}

// GetContent returns the page content (alias for GetData)
func (p *UnifiedPage) GetContent() []byte {
	return p.GetData()
}

// SetContent sets the page content (alias for SetData)
func (p *UnifiedPage) SetContent(content []byte) {
	_ = p.SetData(content)
}

// IsDirty returns whether the page is dirty
func (p *UnifiedPage) IsDirty() bool {
	return atomic.LoadUint32(&p.dirty) == 1
}

// MarkDirty marks the page as dirty
func (p *UnifiedPage) MarkDirty() {
	if atomic.LoadUint32(&p.dirty) == 0 {
		atomic.StoreUint32(&p.dirty, 1)
		p.stats.DirtyCount++
		p.stats.LastModified = uint64(time.Now().UnixNano())
		atomic.StoreUint32(&p.state, uint32(basic.PageStateDirty))
	}
}

// ClearDirty clears the dirty flag
func (p *UnifiedPage) ClearDirty() {
	atomic.StoreUint32(&p.dirty, 0)
	if atomic.LoadInt32(&p.pinCount) == 0 {
		atomic.StoreUint32(&p.state, uint32(basic.PageStateClean))
	}
}

// SetDirty sets the dirty flag
func (p *UnifiedPage) SetDirty(dirty bool) {
	if dirty {
		p.MarkDirty()
	} else {
		p.ClearDirty()
	}
}

// GetState returns the current page state
func (p *UnifiedPage) GetState() basic.PageState {
	return basic.PageState(atomic.LoadUint32(&p.state))
}

// SetState sets the page state
func (p *UnifiedPage) SetState(state basic.PageState) {
	atomic.StoreUint32(&p.state, uint32(state))
}

// GetLSN returns the log sequence number
func (p *UnifiedPage) GetLSN() uint64 {
	return atomic.LoadUint64(&p.lsn)
}

// SetLSN sets the log sequence number
func (p *UnifiedPage) SetLSN(lsn uint64) {
	atomic.StoreUint64(&p.lsn, lsn)
	p.header.LSN = lsn
	p.trailer.LSNLow = uint32(lsn & 0xFFFFFFFF)
	p.MarkDirty()
}

// Pin increments the pin count
func (p *UnifiedPage) Pin() {
	count := atomic.AddInt32(&p.pinCount, 1)
	p.stats.PinCount = uint64(count)
	atomic.StoreUint32(&p.state, uint32(basic.PageStatePinned))
}

// Unpin decrements the pin count
func (p *UnifiedPage) Unpin() {
	if atomic.LoadInt32(&p.pinCount) > 0 {
		count := atomic.AddInt32(&p.pinCount, -1)
		p.stats.PinCount = uint64(count)

		if count == 0 {
			if p.IsDirty() {
				atomic.StoreUint32(&p.state, uint32(basic.PageStateDirty))
			} else {
				atomic.StoreUint32(&p.state, uint32(basic.PageStateClean))
			}
		}
	}
}

// GetPinCount returns the current pin count
func (p *UnifiedPage) GetPinCount() int32 {
	return atomic.LoadInt32(&p.pinCount)
}

// Serialize converts the page to bytes
func (p *UnifiedPage) Serialize() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.serializeInternal(); err != nil {
		return nil, err
	}

	result := make([]byte, p.size)
	copy(result, p.rawData)
	return result, nil
}

// serializeInternal performs serialization without locking (internal use)
func (p *UnifiedPage) serializeInternal() error {
	// Write file header
	binary.BigEndian.PutUint32(p.rawData[FIL_PAGE_SPACE_OR_CHKSUM:], p.header.Checksum)
	binary.BigEndian.PutUint32(p.rawData[FIL_PAGE_OFFSET:], p.header.PageNo)
	binary.BigEndian.PutUint32(p.rawData[FIL_PAGE_PREV:], p.header.PrevPage)
	binary.BigEndian.PutUint32(p.rawData[FIL_PAGE_NEXT:], p.header.NextPage)
	binary.BigEndian.PutUint64(p.rawData[FIL_PAGE_LSN:], p.header.LSN)
	binary.BigEndian.PutUint16(p.rawData[FIL_PAGE_TYPE:], p.header.PageType)
	binary.BigEndian.PutUint64(p.rawData[FIL_PAGE_FILE_FLUSH_LSN:], p.header.FlushLSN)
	binary.BigEndian.PutUint32(p.rawData[FIL_PAGE_SPACE_ID:], p.header.SpaceID)

	// Write page body
	copy(p.rawData[FileHeaderSize:FileHeaderSize+len(p.body)], p.body)

	// Write file trailer
	trailerOffset := int(p.size) - FileTrailerSize
	binary.BigEndian.PutUint32(p.rawData[trailerOffset+FIL_PAGE_END_LSN_OLD_CHKSUM:], p.trailer.OldChecksum)
	binary.BigEndian.PutUint32(p.rawData[trailerOffset+FIL_PAGE_LSN_LOW:], p.trailer.LSNLow)

	return nil
}

// Deserialize parses the page from bytes
func (p *UnifiedPage) Deserialize(data []byte) error {
	if len(data) != int(p.size) {
		return ErrUnifiedInvalidPageSize
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.deserializeInternal(data)
}

// deserializeInternal performs deserialization without locking (internal use)
func (p *UnifiedPage) deserializeInternal(data []byte) error {
	// Parse file header
	p.header.Checksum = binary.BigEndian.Uint32(data[FIL_PAGE_SPACE_OR_CHKSUM:])
	p.header.PageNo = binary.BigEndian.Uint32(data[FIL_PAGE_OFFSET:])
	p.header.PrevPage = binary.BigEndian.Uint32(data[FIL_PAGE_PREV:])
	p.header.NextPage = binary.BigEndian.Uint32(data[FIL_PAGE_NEXT:])
	p.header.LSN = binary.BigEndian.Uint64(data[FIL_PAGE_LSN:])
	p.header.PageType = binary.BigEndian.Uint16(data[FIL_PAGE_TYPE:])
	p.header.FlushLSN = binary.BigEndian.Uint64(data[FIL_PAGE_FILE_FLUSH_LSN:])
	p.header.SpaceID = binary.BigEndian.Uint32(data[FIL_PAGE_SPACE_ID:])

	// Parse page body
	copy(p.body, data[FileHeaderSize:len(data)-FileTrailerSize])

	// Parse file trailer
	trailerOffset := len(data) - FileTrailerSize
	p.trailer.OldChecksum = binary.BigEndian.Uint32(data[trailerOffset+FIL_PAGE_END_LSN_OLD_CHKSUM:])
	p.trailer.LSNLow = binary.BigEndian.Uint32(data[trailerOffset+FIL_PAGE_LSN_LOW:])

	// Update metadata
	p.pageNo = p.header.PageNo
	p.spaceID = p.header.SpaceID
	p.pageType = common.PageType(p.header.PageType)
	atomic.StoreUint64(&p.lsn, p.header.LSN)

	return nil
}

// ToBytes converts the page to bytes (alias for Serialize)
func (p *UnifiedPage) ToBytes() ([]byte, error) {
	return p.Serialize()
}

// ParseFromBytes parses the page from bytes (alias for Deserialize)
func (p *UnifiedPage) ParseFromBytes(data []byte) error {
	return p.Deserialize(data)
}

// CalculateChecksum calculates the CRC32 checksum of the page
func (p *UnifiedPage) CalculateChecksum() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Calculate checksum over entire page except checksum field itself
	data := make([]byte, p.size-4) // Exclude checksum field
	copy(data, p.rawData[4:])

	return crc32.ChecksumIEEE(data)
}

// ValidateChecksum verifies the page checksum
func (p *UnifiedPage) ValidateChecksum() error {
	calculated := p.CalculateChecksum()
	if calculated != p.header.Checksum {
		return ErrUnifiedPageCorrupted
	}
	return nil
}

// UpdateChecksum recalculates and updates the checksum
func (p *UnifiedPage) UpdateChecksum() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.updateChecksumInternal()
}

// updateChecksumInternal updates checksum without locking (internal use)
func (p *UnifiedPage) updateChecksumInternal() {
	// Serialize first to ensure rawData is up-to-date
	_ = p.serializeInternal()

	// Calculate checksum over entire page except checksum field
	data := make([]byte, p.size-4)
	copy(data, p.rawData[4:])

	checksum := crc32.ChecksumIEEE(data)
	p.header.Checksum = checksum
	binary.BigEndian.PutUint32(p.rawData[FIL_PAGE_SPACE_OR_CHKSUM:], checksum)
}

// IsCorrupted checks if the page is corrupted
func (p *UnifiedPage) IsCorrupted() bool {
	return p.ValidateChecksum() != nil
}

// GetFileHeader returns the file header bytes
func (p *UnifiedPage) GetFileHeader() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()

	header := make([]byte, FileHeaderSize)
	copy(header, p.rawData[:FileHeaderSize])
	return header
}

// GetFileTrailer returns the file trailer bytes
func (p *UnifiedPage) GetFileTrailer() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()

	trailer := make([]byte, FileTrailerSize)
	trailerOffset := int(p.size) - FileTrailerSize
	copy(trailer, p.rawData[trailerOffset:])
	return trailer
}

// GetFileHeaderBytes returns the file header bytes (alias)
func (p *UnifiedPage) GetFileHeaderBytes() []byte {
	return p.GetFileHeader()
}

// GetFileTrailerBytes returns the file trailer bytes (alias)
func (p *UnifiedPage) GetFileTrailerBytes() []byte {
	return p.GetFileTrailer()
}

// GetStats returns the page statistics
func (p *UnifiedPage) GetStats() *PageStats {
	return &p.stats
}

// UpdateAccessStats updates access statistics
func (p *UnifiedPage) UpdateAccessStats() {
	p.stats.LastAccessed = uint64(time.Now().UnixNano())
}

// updateAccessStats updates access statistics (internal use)
func (p *UnifiedPage) updateAccessStats() {
	p.stats.ReadCount++
	p.stats.LastAccessed = uint64(time.Now().UnixNano())
}

// Read reads the page from disk/buffer pool
func (p *UnifiedPage) Read() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats.ReadCount++
	p.stats.LastAccessed = uint64(time.Now().UnixNano())

	// If we have a buffer page, read from it
	if p.bufferPage != nil {
		// Buffer pool integration would go here
		// For now, just update stats
	}

	return nil
}

// Write writes the page to disk/buffer pool
func (p *UnifiedPage) Write() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats.WriteCount++
	p.stats.LastModified = uint64(time.Now().UnixNano())

	// Update checksum before writing
	p.updateChecksumInternal()

	// If we have a buffer page, write to it
	if p.bufferPage != nil {
		// Buffer pool integration would go here
		// For now, just update stats
	}

	return nil
}

// Flush flushes the page to disk
func (p *UnifiedPage) Flush() error {
	if err := p.Write(); err != nil {
		return err
	}

	p.ClearDirty()
	atomic.StoreUint32(&p.state, uint32(basic.PageStateFlushed))

	return nil
}

// IsLeafPage checks if this is a leaf page (for index pages)
func (p *UnifiedPage) IsLeafPage() bool {
	// This is a default implementation
	// Specific page types (like IndexPage) should override this
	return false
}

// Release releases the page resources
func (p *UnifiedPage) Release() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Clear all data
	p.body = nil
	p.rawData = nil
	p.bufferPage = nil

	// Reset state
	atomic.StoreUint32(&p.state, uint32(basic.PageStateClean))
	atomic.StoreInt32(&p.pinCount, 0)
	atomic.StoreUint32(&p.dirty, 0)
}

// SetBufferPage sets the associated buffer page
func (p *UnifiedPage) SetBufferPage(bufferPage *buffer_pool.BufferPage) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.bufferPage = bufferPage
}

// GetBufferPage returns the associated buffer page
func (p *UnifiedPage) GetBufferPage() *buffer_pool.BufferPage {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.bufferPage
}

// LockPage acquires the write lock (for external access)
func (p *UnifiedPage) LockPage() {
	p.mu.Lock()
}

// UnlockPage releases the write lock (for external access)
func (p *UnifiedPage) UnlockPage() {
	p.mu.Unlock()
}

// RLockPage acquires the read lock (for external access)
func (p *UnifiedPage) RLockPage() {
	p.mu.RLock()
}

// RUnlockPage releases the read lock (for external access)
func (p *UnifiedPage) RUnlockPage() {
	p.mu.RUnlock()
}
