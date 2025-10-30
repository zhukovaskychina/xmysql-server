package types

import (
	"encoding/binary"
	"errors"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

// Undo page specific errors
var (
	ErrUndoPageFull       = errors.New("undo page is full")
	ErrInvalidUndoRecord  = errors.New("invalid undo record")
	ErrUndoRecordNotFound = errors.New("undo record not found")
)

// Undo page header offsets (16 bytes total)
const (
	UNDO_HDR_TYPE          = 0  // Undo log type (2 bytes)
	UNDO_HDR_STATE         = 2  // Undo log state (2 bytes)
	UNDO_HDR_LAST_LOG      = 4  // Last log record offset (2 bytes)
	UNDO_HDR_FREE_LIST_LEN = 6  // Free list length (2 bytes)
	UNDO_HDR_FREE_LIST     = 8  // Free list start (2 bytes)
	UNDO_HDR_RESERVED      = 10 // Reserved (6 bytes)
	UNDO_HEADER_SIZE       = 16 // Total undo header size
)

// Undo log types
const (
	TRX_UNDO_INSERT = 1 // Insert undo log
	TRX_UNDO_UPDATE = 2 // Update undo log
)

// Undo log states
const (
	TRX_UNDO_ACTIVE   = 1 // Active undo log
	TRX_UNDO_CACHED   = 2 // Cached undo log
	TRX_UNDO_TO_FREE  = 3 // To be freed
	TRX_UNDO_TO_PURGE = 4 // To be purged
	TRX_UNDO_PREPARED = 5 // Prepared for commit
)

// Undo record types
const (
	TRX_UNDO_INSERT_REC    = 11 // Insert undo record
	TRX_UNDO_UPD_EXIST_REC = 12 // Update existing record
	TRX_UNDO_UPD_DEL_REC   = 13 // Update deleted record
	TRX_UNDO_DEL_MARK_REC  = 14 // Delete mark record
)

// Maximum undo data per page
const (
	MAX_UNDO_DATA_SIZE = DefaultPageSize - FileHeaderSize - UNDO_HEADER_SIZE - FileTrailerSize
)

// UndoHeader represents the undo page header structure
type UndoHeader struct {
	Type          uint16 // Undo log type
	State         uint16 // Undo log state
	LastLogOffset uint16 // Last log record offset
	FreeListLen   uint16 // Free list length
	FreeListStart uint16 // Free list start
}

// UndoRecord represents a single undo record
type UndoRecord struct {
	Type    uint8  // Record type
	TrxID   uint64 // Transaction ID
	RollPtr uint64 // Roll pointer
	Offset  uint16 // Offset in page
	Length  uint16 // Record length
	Data    []byte // Undo data
}

// UndoPage represents an undo log page
type UndoPage struct {
	*UnifiedPage // Embedded base page

	// Undo-specific header (16 bytes)
	undoHeader UndoHeader

	// Undo records
	undoRecords []UndoRecord

	// Free space tracking
	usedSpace uint16
}

// NewUndoPage creates a new undo page
func NewUndoPage(spaceID, pageNo uint32) *UndoPage {
	up := &UndoPage{
		UnifiedPage: NewUnifiedPage(spaceID, pageNo, common.FIL_PAGE_UNDO_LOG),
		undoRecords: make([]UndoRecord, 0),
		usedSpace:   UNDO_HEADER_SIZE,
	}

	// Initialize undo header
	up.undoHeader = UndoHeader{
		Type:          TRX_UNDO_INSERT,
		State:         TRX_UNDO_ACTIVE,
		LastLogOffset: UNDO_HEADER_SIZE,
		FreeListLen:   0,
		FreeListStart: 0,
	}

	// Serialize undo header to page body
	up.serializeUndoHeader()

	return up
}

// serializeUndoHeader serializes the undo header to the page body
func (up *UndoPage) serializeUndoHeader() {
	body := up.GetBody()
	if len(body) < UNDO_HEADER_SIZE {
		return
	}

	binary.BigEndian.PutUint16(body[UNDO_HDR_TYPE:], up.undoHeader.Type)
	binary.BigEndian.PutUint16(body[UNDO_HDR_STATE:], up.undoHeader.State)
	binary.BigEndian.PutUint16(body[UNDO_HDR_LAST_LOG:], up.undoHeader.LastLogOffset)
	binary.BigEndian.PutUint16(body[UNDO_HDR_FREE_LIST_LEN:], up.undoHeader.FreeListLen)
	binary.BigEndian.PutUint16(body[UNDO_HDR_FREE_LIST:], up.undoHeader.FreeListStart)

	// Serialize undo records
	offset := UNDO_HEADER_SIZE
	for _, record := range up.undoRecords {
		if offset+int(record.Length) > len(body) {
			break
		}

		// Write record header
		body[offset] = record.Type
		binary.BigEndian.PutUint64(body[offset+1:], record.TrxID)
		binary.BigEndian.PutUint64(body[offset+9:], record.RollPtr)
		binary.BigEndian.PutUint16(body[offset+17:], record.Length)

		// Write record data
		copy(body[offset+19:], record.Data)

		offset += int(record.Length)
	}

	up.SetBody(body)
}

// deserializeUndoHeader deserializes the undo header from the page body
func (up *UndoPage) deserializeUndoHeader() {
	body := up.GetBody()
	if len(body) < UNDO_HEADER_SIZE {
		return
	}

	up.undoHeader.Type = binary.BigEndian.Uint16(body[UNDO_HDR_TYPE:])
	up.undoHeader.State = binary.BigEndian.Uint16(body[UNDO_HDR_STATE:])
	up.undoHeader.LastLogOffset = binary.BigEndian.Uint16(body[UNDO_HDR_LAST_LOG:])
	up.undoHeader.FreeListLen = binary.BigEndian.Uint16(body[UNDO_HDR_FREE_LIST_LEN:])
	up.undoHeader.FreeListStart = binary.BigEndian.Uint16(body[UNDO_HDR_FREE_LIST:])

	// Deserialize undo records
	up.undoRecords = make([]UndoRecord, 0)
	offset := UNDO_HEADER_SIZE

	for offset < int(up.undoHeader.LastLogOffset) && offset+19 < len(body) {
		record := UndoRecord{
			Type:    body[offset],
			TrxID:   binary.BigEndian.Uint64(body[offset+1:]),
			RollPtr: binary.BigEndian.Uint64(body[offset+9:]),
			Length:  binary.BigEndian.Uint16(body[offset+17:]),
			Offset:  uint16(offset),
		}

		if offset+int(record.Length) > len(body) {
			break
		}

		// Read record data
		dataLen := int(record.Length) - 19 // Subtract header size
		if dataLen > 0 {
			record.Data = make([]byte, dataLen)
			copy(record.Data, body[offset+19:offset+19+dataLen])
		}

		up.undoRecords = append(up.undoRecords, record)
		offset += int(record.Length)
	}

	up.usedSpace = uint16(offset)
}

// AddUndoRecord adds an undo record to the page
func (up *UndoPage) AddUndoRecord(record UndoRecord) error {
	// Calculate record size (header + data)
	recordSize := uint16(19 + len(record.Data)) // 1 + 8 + 8 + 2 + data
	record.Length = recordSize

	// Check if there's enough space
	if up.usedSpace+recordSize > MAX_UNDO_DATA_SIZE {
		return ErrUndoPageFull
	}

	// Set record offset
	record.Offset = up.usedSpace

	// Add record
	up.undoRecords = append(up.undoRecords, record)

	// Update header
	up.usedSpace += recordSize
	up.undoHeader.LastLogOffset = up.usedSpace

	// Serialize changes
	up.serializeUndoHeader()

	// Mark page as dirty
	up.MarkDirty()

	return nil
}

// GetUndoRecords returns all undo records
func (up *UndoPage) GetUndoRecords() []UndoRecord {
	result := make([]UndoRecord, len(up.undoRecords))
	copy(result, up.undoRecords)
	return result
}

// GetUndoRecord returns an undo record by index
func (up *UndoPage) GetUndoRecord(index int) (*UndoRecord, error) {
	if index < 0 || index >= len(up.undoRecords) {
		return nil, ErrUndoRecordNotFound
	}

	record := up.undoRecords[index]
	return &record, nil
}

// GetRecordCount returns the number of undo records
func (up *UndoPage) GetRecordCount() int {
	return len(up.undoRecords)
}

// GetFreeSpace returns the amount of free space in the page
func (up *UndoPage) GetFreeSpace() uint16 {
	if up.usedSpace >= MAX_UNDO_DATA_SIZE {
		return 0
	}
	return MAX_UNDO_DATA_SIZE - up.usedSpace
}

// GetUndoHeader returns the undo header
func (up *UndoPage) GetUndoHeader() *UndoHeader {
	return &up.undoHeader
}

// GetType returns the undo log type
func (up *UndoPage) GetType() uint16 {
	return up.undoHeader.Type
}

// SetType sets the undo log type
func (up *UndoPage) SetType(undoType uint16) {
	up.undoHeader.Type = undoType
	up.serializeUndoHeader()
	up.MarkDirty()
}

// GetState returns the undo log state
func (up *UndoPage) GetState() uint16 {
	return up.undoHeader.State
}

// SetState sets the undo log state
func (up *UndoPage) SetState(state uint16) {
	up.undoHeader.State = state
	up.serializeUndoHeader()
	up.MarkDirty()
}

// IsActive returns true if the undo log is active
func (up *UndoPage) IsActive() bool {
	return up.undoHeader.State == TRX_UNDO_ACTIVE
}

// IsCached returns true if the undo log is cached
func (up *UndoPage) IsCached() bool {
	return up.undoHeader.State == TRX_UNDO_CACHED
}

// MarkForPurge marks the undo log for purge
func (up *UndoPage) MarkForPurge() {
	up.SetState(TRX_UNDO_TO_PURGE)
}

// ClearRecords clears all undo records
func (up *UndoPage) ClearRecords() {
	up.undoRecords = make([]UndoRecord, 0)
	up.usedSpace = UNDO_HEADER_SIZE
	up.undoHeader.LastLogOffset = UNDO_HEADER_SIZE
	up.serializeUndoHeader()
	up.MarkDirty()
}

// ParseFromBytes overrides UnifiedPage to also parse undo-specific data
func (up *UndoPage) ParseFromBytes(data []byte) error {
	// First parse the base page
	if err := up.UnifiedPage.ParseFromBytes(data); err != nil {
		return err
	}

	// Then parse undo-specific data
	up.deserializeUndoHeader()

	return nil
}

// ToBytes overrides UnifiedPage to include undo-specific data
func (up *UndoPage) ToBytes() ([]byte, error) {
	// Serialize undo header first
	up.serializeUndoHeader()

	// Then use base page serialization
	return up.UnifiedPage.ToBytes()
}
