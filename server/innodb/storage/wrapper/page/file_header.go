package page

// FileHeader represents the header of a page file
type FileHeader struct {
	FilePageOffset        []byte // 4 bytes
	FilePagePrev          []byte // 4 bytes
	FilePageNext          []byte // 4 bytes
	FilePageLSN           []byte // 8 bytes
	FilePageType          []byte // 2 bytes
	FilePageFileFlushLSN  []byte // 8 bytes
	FilePageSpaceOrChksum []byte // 4 bytes
	FilePageNumber        []byte // 4 bytes
}

// NewFileHeader creates a new FileHeader instance
func NewFileHeader() *FileHeader {
	return &FileHeader{
		FilePageOffset:        make([]byte, 4),
		FilePagePrev:          make([]byte, 4),
		FilePageNext:          make([]byte, 4),
		FilePageLSN:           make([]byte, 8),
		FilePageType:          make([]byte, 2),
		FilePageFileFlushLSN:  make([]byte, 8),
		FilePageSpaceOrChksum: make([]byte, 4),
		FilePageNumber:        make([]byte, 4),
	}
}

// GetBytes returns the serialized bytes of the FileHeader
func (fh *FileHeader) GetBytes() []byte {
	bytes := make([]byte, 0, 38) // 38 = total size of all fields
	bytes = append(bytes, fh.FilePageOffset...)
	bytes = append(bytes, fh.FilePagePrev...)
	bytes = append(bytes, fh.FilePageNext...)
	bytes = append(bytes, fh.FilePageLSN...)
	bytes = append(bytes, fh.FilePageType...)
	bytes = append(bytes, fh.FilePageFileFlushLSN...)
	bytes = append(bytes, fh.FilePageSpaceOrChksum...)
	bytes = append(bytes, fh.FilePageNumber...)
	return bytes
}

// ParseBytes parses the given bytes into FileHeader fields
func (fh *FileHeader) ParseBytes(bytes []byte) {
	if len(bytes) < 38 {
		return
	}
	copy(fh.FilePageOffset, bytes[0:4])
	copy(fh.FilePagePrev, bytes[4:8])
	copy(fh.FilePageNext, bytes[8:12])
	copy(fh.FilePageLSN, bytes[12:20])
	copy(fh.FilePageType, bytes[20:22])
	copy(fh.FilePageFileFlushLSN, bytes[22:30])
	copy(fh.FilePageSpaceOrChksum, bytes[30:34])
	copy(fh.FilePageNumber, bytes[34:38])
}
