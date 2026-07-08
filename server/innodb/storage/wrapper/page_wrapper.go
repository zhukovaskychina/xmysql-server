package wrapper

import (
	"encoding/binary"
)

// FileHeader 文件头部
type FileHeader struct {
	SpaceID  uint32
	PageNo   uint32
	PageType uint16
	LSN      uint64
}

// FileTrailer 文件尾部
type FileTrailer struct {
	LSN      uint64
	Checksum uint32
}

// NewFileHeader 创建一个新的文件头部
func NewFileHeader(spaceID uint32, pageNo uint32, pageType uint16, lsn uint64) *FileHeader {
	return &FileHeader{
		SpaceID:  spaceID,
		PageNo:   pageNo,
		PageType: pageType,
		LSN:      lsn,
	}
}

// NewFileTrailer 创建一个新的文件尾部
func NewFileTrailer(lsn uint64, checksum uint32) *FileTrailer {
	return &FileTrailer{
		LSN:      lsn,
		Checksum: checksum,
	}
}

// GetBytes 获取文件头部的字节数组
func (fh *FileHeader) GetBytes() []byte {
	bytes := make([]byte, 18)
	binary.LittleEndian.PutUint32(bytes[0:4], fh.SpaceID)
	binary.LittleEndian.PutUint32(bytes[4:8], fh.PageNo)
	binary.LittleEndian.PutUint16(bytes[8:10], fh.PageType)
	binary.LittleEndian.PutUint64(bytes[10:18], fh.LSN)
	return bytes
}

// GetBytes 获取文件尾部的字节数组
func (ft *FileTrailer) GetBytes() []byte {
	bytes := make([]byte, 12)
	binary.LittleEndian.PutUint64(bytes[0:8], ft.LSN)
	binary.LittleEndian.PutUint32(bytes[8:12], ft.Checksum)
	return bytes
}
