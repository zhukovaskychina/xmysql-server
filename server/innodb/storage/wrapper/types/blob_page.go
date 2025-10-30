package types

import (
	"encoding/binary"
	"errors"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

// BLOB page specific errors
var (
	ErrBlobTooLarge     = errors.New("BLOB data too large for page")
	ErrInvalidBlobChain = errors.New("invalid BLOB chain")
	ErrBlobNotComplete  = errors.New("BLOB data not complete")
)

// BLOB page header offsets (20 bytes total)
const (
	BLOB_HDR_LENGTH     = 0  // Total BLOB length (4 bytes)
	BLOB_HDR_NEXT_PAGE  = 4  // Next BLOB page number (4 bytes)
	BLOB_HDR_OFFSET     = 8  // Offset in total BLOB (4 bytes)
	BLOB_HDR_SEGMENT_ID = 12 // Segment ID (8 bytes)
	BLOB_HEADER_SIZE    = 20 // Total BLOB header size
)

// BLOB data constants
const (
	// Maximum BLOB data per page = PageSize - FileHeader - BlobHeader - FileTrailer
	MAX_BLOB_DATA_SIZE = DefaultPageSize - FileHeaderSize - BLOB_HEADER_SIZE - FileTrailerSize // 16318 bytes
)

// BlobHeader represents the BLOB page header structure
type BlobHeader struct {
	Length    uint32 // Total BLOB length
	NextPage  uint32 // Next BLOB page (0 if last)
	Offset    uint32 // Offset in total BLOB
	SegmentID uint64 // Segment ID
}

// BlobPage represents a BLOB data page
type BlobPage struct {
	*UnifiedPage // Embedded base page

	// BLOB-specific header (20 bytes)
	blobHeader BlobHeader

	// BLOB data (16318 bytes max)
	data []byte
}

// NewBlobPage creates a new BLOB page
func NewBlobPage(spaceID, pageNo uint32, segmentID uint64) *BlobPage {
	bp := &BlobPage{
		UnifiedPage: NewUnifiedPage(spaceID, pageNo, common.FIL_PAGE_TYPE_BLOB),
		data:        make([]byte, 0, MAX_BLOB_DATA_SIZE),
	}

	// Initialize BLOB header
	bp.blobHeader = BlobHeader{
		Length:    0,
		NextPage:  0, // No next page initially
		Offset:    0,
		SegmentID: segmentID,
	}

	// Serialize BLOB header to page body
	bp.serializeBlobHeader()

	return bp
}

// serializeBlobHeader serializes the BLOB header to the page body
func (bp *BlobPage) serializeBlobHeader() {
	body := bp.GetBody()
	if len(body) < BLOB_HEADER_SIZE {
		return
	}

	binary.BigEndian.PutUint32(body[BLOB_HDR_LENGTH:], bp.blobHeader.Length)
	binary.BigEndian.PutUint32(body[BLOB_HDR_NEXT_PAGE:], bp.blobHeader.NextPage)
	binary.BigEndian.PutUint32(body[BLOB_HDR_OFFSET:], bp.blobHeader.Offset)
	binary.BigEndian.PutUint64(body[BLOB_HDR_SEGMENT_ID:], bp.blobHeader.SegmentID)

	// Copy BLOB data after header
	if len(bp.data) > 0 {
		copy(body[BLOB_HEADER_SIZE:], bp.data)
	}

	bp.SetBody(body)
}

// deserializeBlobHeader deserializes the BLOB header from the page body
func (bp *BlobPage) deserializeBlobHeader() {
	body := bp.GetBody()
	if len(body) < BLOB_HEADER_SIZE {
		return
	}

	bp.blobHeader.Length = binary.BigEndian.Uint32(body[BLOB_HDR_LENGTH:])
	bp.blobHeader.NextPage = binary.BigEndian.Uint32(body[BLOB_HDR_NEXT_PAGE:])
	bp.blobHeader.Offset = binary.BigEndian.Uint32(body[BLOB_HDR_OFFSET:])
	bp.blobHeader.SegmentID = binary.BigEndian.Uint64(body[BLOB_HDR_SEGMENT_ID:])

	// Extract BLOB data
	dataSize := len(body) - BLOB_HEADER_SIZE
	if dataSize > 0 && dataSize <= MAX_BLOB_DATA_SIZE {
		bp.data = make([]byte, dataSize)
		copy(bp.data, body[BLOB_HEADER_SIZE:])
	}
}

// SetBlobData sets the BLOB data for this page
func (bp *BlobPage) SetBlobData(data []byte, totalLength, offset, nextPage uint32) error {
	if len(data) > MAX_BLOB_DATA_SIZE {
		return ErrBlobTooLarge
	}

	// Update BLOB header
	bp.blobHeader.Length = totalLength
	bp.blobHeader.Offset = offset
	bp.blobHeader.NextPage = nextPage

	// Copy data
	bp.data = make([]byte, len(data))
	copy(bp.data, data)

	// Serialize changes
	bp.serializeBlobHeader()

	// Mark page as dirty
	bp.MarkDirty()

	return nil
}

// GetBlobData returns the BLOB data
func (bp *BlobPage) GetBlobData() []byte {
	result := make([]byte, len(bp.data))
	copy(result, bp.data)
	return result
}

// GetBlobHeader returns the BLOB header
func (bp *BlobPage) GetBlobHeader() *BlobHeader {
	return &bp.blobHeader
}

// GetNextPageNo returns the next BLOB page number
func (bp *BlobPage) GetNextPageNo() uint32 {
	return bp.blobHeader.NextPage
}

// SetNextPageNo sets the next BLOB page number
func (bp *BlobPage) SetNextPageNo(nextPage uint32) {
	bp.blobHeader.NextPage = nextPage
	bp.serializeBlobHeader()
	bp.MarkDirty()
}

// IsLastPage returns true if this is the last page in the BLOB chain
func (bp *BlobPage) IsLastPage() bool {
	return bp.blobHeader.NextPage == 0
}

// GetSegmentID returns the segment ID
func (bp *BlobPage) GetSegmentID() uint64 {
	return bp.blobHeader.SegmentID
}

// GetTotalLength returns the total BLOB length
func (bp *BlobPage) GetTotalLength() uint32 {
	return bp.blobHeader.Length
}

// GetOffset returns the offset in the total BLOB
func (bp *BlobPage) GetOffset() uint32 {
	return bp.blobHeader.Offset
}

// GetDataSize returns the size of BLOB data in this page
func (bp *BlobPage) GetDataSize() int {
	return len(bp.data)
}

// GetAvailableSpace returns the available space for BLOB data
func (bp *BlobPage) GetAvailableSpace() int {
	return MAX_BLOB_DATA_SIZE - len(bp.data)
}

// AppendData appends data to the BLOB page
func (bp *BlobPage) AppendData(data []byte) error {
	if len(bp.data)+len(data) > MAX_BLOB_DATA_SIZE {
		return ErrBlobTooLarge
	}

	bp.data = append(bp.data, data...)
	bp.serializeBlobHeader()
	bp.MarkDirty()

	return nil
}

// ClearData clears the BLOB data
func (bp *BlobPage) ClearData() {
	bp.data = make([]byte, 0, MAX_BLOB_DATA_SIZE)
	bp.blobHeader.Length = 0
	bp.blobHeader.Offset = 0
	bp.blobHeader.NextPage = 0
	bp.serializeBlobHeader()
	bp.MarkDirty()
}

// ParseFromBytes overrides UnifiedPage to also parse BLOB-specific data
func (bp *BlobPage) ParseFromBytes(data []byte) error {
	// First parse the base page
	if err := bp.UnifiedPage.ParseFromBytes(data); err != nil {
		return err
	}

	// Then parse BLOB-specific data
	bp.deserializeBlobHeader()

	return nil
}

// ToBytes overrides UnifiedPage to include BLOB-specific data
func (bp *BlobPage) ToBytes() ([]byte, error) {
	// Serialize BLOB header first
	bp.serializeBlobHeader()

	// Then use base page serialization
	return bp.UnifiedPage.ToBytes()
}

// ValidateBlobChain validates the BLOB chain integrity
func (bp *BlobPage) ValidateBlobChain() error {
	// Check if offset + data size <= total length
	if bp.blobHeader.Offset+uint32(len(bp.data)) > bp.blobHeader.Length {
		return ErrInvalidBlobChain
	}

	// If this is the last page, check if we have all data
	if bp.IsLastPage() {
		if bp.blobHeader.Offset+uint32(len(bp.data)) != bp.blobHeader.Length {
			return ErrBlobNotComplete
		}
	}

	return nil
}
