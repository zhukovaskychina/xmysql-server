package types

import "github.com/zhukovaskychina/xmysql-server/server/common"

// PageHeader represents the header of a page
type PageHeader struct {
	FileHeader  []byte
	FileBody    []byte
	FileTrailer []byte
}

// File header offsets
const (
	FHeaderSpaceID      = common.LegacyBasePageHeaderSpaceIDOffset
	FHeaderPageNo       = common.LegacyBasePageHeaderPageNoOffset
	FHeaderPreviousPage = common.LegacyBasePageHeaderPrevOffset
	FHeaderNextPage     = common.LegacyBasePageHeaderNextOffset
	FHeaderLSN          = common.LegacyBasePageHeaderLSNOffset
	FHeaderPageType     = common.LegacyBasePageHeaderPageTypeOffset
	FHeaderFileSequence = common.LegacyBasePageHeaderFileSequenceOffset
	FHeaderPageVersion  = common.LegacyBasePageHeaderPageVersionOffset
	FHeaderCompressType = common.LegacyBasePageHeaderCompressTypeOffset
)

// File trailer offsets
const (
	FTrailerChecksum = common.LegacyBasePageTrailerChecksumOffset
	FTrailerLSN      = common.LegacyBasePageTrailerLSNOffset
)

// NewPageHeader creates a new page header with the given size
func NewPageHeader(size uint32) *PageHeader {
	return &PageHeader{
		FileHeader:  make([]byte, common.FileHeaderSize),                             // 38 bytes header
		FileBody:    make([]byte, size-common.FileHeaderSize-common.FileTrailerSize), // size - (header + trailer)
		FileTrailer: make([]byte, common.FileTrailerSize),                            // 8 bytes trailer
	}
}
