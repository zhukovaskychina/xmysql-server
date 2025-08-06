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
	case int16(common.FIL_PAGE_TYPE_BLOB):
		return s.deserializeBlobPage(data)
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

// deserializeFSPPage 反序列化文件空间头页面
func (s *DefaultPageSerializer) deserializeFSPPage(data []byte) (IPage, error) {
	page := &AbstractPage{}
	page.LoadFileHeader(data[:FileHeaderSize])
	page.LoadFileTrailer(data[len(data)-8:])
	return page, nil
}

// deserializeInodePage 反序列化inode页面
func (s *DefaultPageSerializer) deserializeInodePage(data []byte) (IPage, error) {
	page := &AbstractPage{}
	page.LoadFileHeader(data[:FileHeaderSize])
	page.LoadFileTrailer(data[len(data)-8:])
	return page, nil
}

// deserializeBlobPage 反序列化BLOB页面
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
