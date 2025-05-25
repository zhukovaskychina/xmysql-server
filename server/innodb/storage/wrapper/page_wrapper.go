package wrapper

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
	// TODO: 实现文件头部序列化
	return nil
}

// GetBytes 获取文件尾部的字节数组
func (ft *FileTrailer) GetBytes() []byte {
	// TODO: 实现文件尾部序列化
	return nil
}
