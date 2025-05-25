package types

// PageHeader represents the header of a page
type PageHeader struct {
	FileHeader  []byte
	FileBody    []byte
	FileTrailer []byte
}

// File header offsets
const (
	FHeaderSpaceID      = 0
	FHeaderPageNo       = 4
	FHeaderPreviousPage = 8
	FHeaderNextPage     = 12
	FHeaderLSN          = 16
	FHeaderPageType     = 24
	FHeaderFileSequence = 26
	FHeaderPageVersion  = 28
	FHeaderCompressType = 30
)

// File trailer offsets
const (
	FTrailerChecksum = 0
	FTrailerLSN      = 4
)

// NewPageHeader creates a new page header with the given size
func NewPageHeader(size uint32) *PageHeader {
	return &PageHeader{
		FileHeader:  make([]byte, 38),      // 38 bytes header
		FileBody:    make([]byte, size-46), // size - (header + trailer)
		FileTrailer: make([]byte, 8),       // 8 bytes trailer
	}
}
