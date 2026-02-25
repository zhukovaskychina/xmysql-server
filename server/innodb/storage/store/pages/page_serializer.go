package pages

import (
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
)

var (
	ErrUnsupportedPageType = errors.New("unsupported page type")
	ErrSerializationFailed = errors.New("serialization failed")
)

// PageSerializer 页面序列化接口
type PageSerializer interface {
	Serialize(page IPage) ([]byte, error)
	Deserialize(data []byte) (IPage, error)
	GetSerializedSize(page IPage) int
}

// DefaultPageSerializer 默认页面序列化器
type DefaultPageSerializer struct {
	integrityChecker *PageIntegrityChecker
}

// NewDefaultPageSerializer 创建默认页面序列化器
func NewDefaultPageSerializer() *DefaultPageSerializer {
	return &DefaultPageSerializer{
		integrityChecker: NewPageIntegrityChecker(ChecksumCRC32),
	}
}

// Serialize 序列化页面
func (s *DefaultPageSerializer) Serialize(page IPage) ([]byte, error) {
	if page == nil {
		return nil, errors.New("page is nil")
	}

	// 获取页面序列化数据
	data := page.GetSerializeBytes()
	if data == nil {
		return nil, ErrSerializationFailed
	}

	// 计算并设置校验和
	if len(data) >= FileHeaderSize+8 {
		checksum := s.integrityChecker.CalculateChecksum(data)
		data[0] = byte(checksum)
		data[1] = byte(checksum >> 8)
		data[2] = byte(checksum >> 16)
		data[3] = byte(checksum >> 24)

		// 设置文件尾部校验和
		trailerOffset := len(data) - 8
		data[trailerOffset] = byte(checksum)
		data[trailerOffset+1] = byte(checksum >> 8)
		data[trailerOffset+2] = byte(checksum >> 16)
		data[trailerOffset+3] = byte(checksum >> 24)
	}

	return data, nil
}

// Deserialize 反序列化页面
func (s *DefaultPageSerializer) Deserialize(data []byte) (IPage, error) {
	if len(data) < FileHeaderSize {
		return nil, errors.New("invalid page data size")
	}

	// 验证页面完整性
	if err := s.integrityChecker.ValidatePage(data); err != nil {
		return nil, err
	}

	// 解析页面头部获取页面类型
	pageType := int16(data[24]) | int16(data[25])<<8

	// 根据页面类型创建相应的页面对象
	switch pageType {
	case int16(common.FILE_PAGE_INDEX):
		return s.deserializeIndexPage(data)
	case int16(common.FIL_PAGE_TYPE_FSP_HDR):
		return s.deserializeFSPPage(data)
	case int16(common.FIL_PAGE_INODE):
		return s.deserializeInodePage(data)
	case int16(common.FIL_PAGE_TYPE_SYS):
		return s.deserializeSystemPage(data)
	case int16(common.FIL_PAGE_TYPE_BLOB):
		return s.deserializeBlobConcrete(data)
	case int16(common.FIL_PAGE_TYPE_COMPRESSED):
		return s.deserializeCompressedPage(data)
	case int16(common.FIL_PAGE_TYPE_ENCRYPTED):
		return s.deserializeEncryptedPage(data)
	default:
		// 对于未知类型，创建基础页面
		return s.deserializeBasePage(data)
	}
}

// deserializeIndexPage 反序列化索引页面
func (s *DefaultPageSerializer) deserializeIndexPage(data []byte) (IPage, error) {
	if len(data) < FileHeaderSize+56 { // 文件头+页面头
		return nil, errors.New("invalid index page size")
	}

	page := &IndexPage{}

	// 加载文件头部
	page.LoadFileHeader(data[:FileHeaderSize])

	// 解析页面头部
	page.ParsePageHeader(data[FileHeaderSize : FileHeaderSize+56])

	// 解析infimum/supremum记录
	page.ParseInfimumSupermum(data[FileHeaderSize+56 : FileHeaderSize+56+26])

	// 解析页面目录
	page.ParsePageSlots(data)

	// 解析用户记录和空闲空间
	page.ParseUserRecordsAndFreeSpace(data)

	// 加载文件尾部
	page.LoadFileTrailer(data[len(data)-8:])

	return page, nil
}

// deserializeFSPPage 反序列化文件空间头页面（返回具体类型 FspHrdBinaryPage）
func (s *DefaultPageSerializer) deserializeFSPPage(data []byte) (IPage, error) {
	if len(data) != int(common.PageSize) {
		return nil, errors.New("invalid FSP_HDR page size")
	}

	const xdesEntrySize = 40
	const xdesEntryCount = 256
	const fspHdrStructSize = 4 + 4 + 4 + 4 + 4 + 4 + 16 + 16 + 16 + 8 + 16 + 16 // 112 bytes

	hdrOff := int(common.FileHeaderSize)
	trailerLen := int(common.FileTrailerSize)

	fsp := &FspHrdBinaryPage{FileSpaceHeader: &FileSpaceHeader{}}
	// 文件头
	fsp.LoadFileHeader(data[:hdrOff])

	// FileSpaceHeader - 使用新的LoadFromBytes方法（单次复制，零切片创建）
	fsp.FileSpaceHeader.LoadFromBytes(data[hdrOff : hdrOff+fspHdrStructSize])

	// XDES entries
	xdesTotal := xdesEntrySize * xdesEntryCount
	startXdes := hdrOff + fspHdrStructSize
	endXdes := startXdes + xdesTotal
	if endXdes > len(data) {
		return nil, errors.New("invalid XDES area")
	}
	entries := make([]XDESEntry, xdesEntryCount)
	for i := 0; i < xdesEntryCount; i++ {
		o := startXdes + i*xdesEntrySize
		entries[i] = ParseXDesEntry(data[o : o+xdesEntrySize])
	}
	fsp.XDESEntrys = entries

	// Empty space
	emptyStart := endXdes
	emptyEnd := len(data) - trailerLen
	if emptyStart > emptyEnd {
		return nil, errors.New("invalid empty space range")
	}
	fsp.EmptySpace = data[emptyStart:emptyEnd]

	// 文件尾
	fsp.LoadFileTrailer(data[len(data)-trailerLen:])

	return fsp, nil
}

// deserializeInodePage 反序列化inode页面（返回具体类型 INodePage）
func (s *DefaultPageSerializer) deserializeInodePage(data []byte) (IPage, error) {
	if len(data) != int(common.PageSize) {
		return nil, errors.New("invalid INODE page size")
	}
	return NewINodeByParseBytes(data), nil
}

// deserializeBlobPage 反序列化BLOB页面

// deserializeSystemPage 反序列化系统页面
func (s *DefaultPageSerializer) deserializeSystemPage(data []byte) (IPage, error) {
	sp := &SystemPage{}
	// 确保内部缓冲分配
	sp.SystemData = make([]byte, SystemDataSize)
	if err := sp.Deserialize(data); err != nil {
		return nil, err
	}
	return sp, nil
}

// deserializeBlobConcrete 反序列化BLOB页面（具体类型）
func (s *DefaultPageSerializer) deserializeBlobConcrete(data []byte) (IPage, error) {
	bp := &BlobPage{Data: make([]byte, BlobDataSize)}
	if err := bp.Deserialize(data); err != nil {
		return nil, err
	}
	return bp, nil
}

// deserializeCompressedPage 反序列化压缩页面
func (s *DefaultPageSerializer) deserializeCompressedPage(data []byte) (IPage, error) {
	cp := &CompressedPage{}
	if err := cp.Deserialize(data); err != nil {
		return nil, err
	}
	return cp, nil
}

// deserializeEncryptedPage 反序列化加密页面
func (s *DefaultPageSerializer) deserializeEncryptedPage(data []byte) (IPage, error) {
	ep := &EncryptedPage{EncryptedData: make([]byte, MaxEncryptedDataSize)}
	if err := ep.Deserialize(data); err != nil {
		return nil, err
	}
	return ep, nil
}

func (s *DefaultPageSerializer) deserializeBlobPage(data []byte) (IPage, error) {
	page := &AbstractPage{}
	page.LoadFileHeader(data[:FileHeaderSize])
	page.LoadFileTrailer(data[len(data)-8:])
	return page, nil
}

// deserializeBasePage 反序列化基础页面
func (s *DefaultPageSerializer) deserializeBasePage(data []byte) (IPage, error) {
	page := &AbstractPage{}
	page.LoadFileHeader(data[:FileHeaderSize])
	page.LoadFileTrailer(data[len(data)-8:])
	return page, nil
}

// GetSerializedSize 获取序列化后的大小
func (s *DefaultPageSerializer) GetSerializedSize(page IPage) int {
	if page == nil {
		return 0
	}

	data := page.GetSerializeBytes()
	if data == nil {
		return 0
	}

	return len(data)
}
