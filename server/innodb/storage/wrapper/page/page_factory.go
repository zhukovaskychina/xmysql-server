package page

import (
	"encoding/binary"
	"errors"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/buffer_pool"
	"xmysql-server/server/innodb/storage/store/pages"
)

var (
	ErrInvalidPageType = errors.New("invalid page type")
	ErrInvalidPageData = errors.New("invalid page data")
)

// PageFactory 页面工厂，负责创建和解析各种类型的页面
type PageFactory struct{}

// NewPageFactory 创建页面工厂实例
func NewPageFactory() *PageFactory {
	return &PageFactory{}
}

// IPageWrapper 页面包装器接口
type IPageWrapper interface {
	// 基本信息
	GetPageID() uint32
	GetSpaceID() uint32
	GetPageType() common.PageType

	// 序列化
	ParseFromBytes(data []byte) error
	ToBytes() ([]byte, error)

	// 文件头尾访问
	GetFileHeader() *pages.FileHeader
	GetFileTrailer() *pages.FileTrailer
}

// CreatePage 根据页面类型创建对应的页面wrapper
func (f *PageFactory) CreatePage(pageType common.PageType, id, spaceID uint32, bufferPool *buffer_pool.BufferPool) IPageWrapper {
	switch pageType {
	case common.FIL_PAGE_INDEX:
		return NewPageIndexWithSpaceId(spaceID, id).(IPageWrapper)
	case common.FIL_PAGE_FSP_HDR:
		return CreateFSPPageWrapper(id, spaceID, bufferPool)
	case common.FIL_PAGE_INODE:
		// 注意：这里返回的是system.INode，需要适配器
		return NewBasePageWrapper(id, spaceID, pageType) // 使用基础wrapper代替
	case common.FIL_PAGE_IBUF_FREE_LIST:
		return NewIBufFreeListPageWrapper(id, spaceID)
	case common.FIL_PAGE_TYPE_SYS:
		return CreateDataDictionaryPageWrapper(id, spaceID, bufferPool)
	case common.FIL_PAGE_TYPE_XDES:
		return CreateXDESPageWrapper(id, spaceID, bufferPool)
	case common.FIL_PAGE_UNDO_LOG:
		return NewUndoLogPageWrapper(id, spaceID, id, nil)
	case common.FIL_PAGE_TYPE_ALLOCATED:
		return NewAllocatePageWrapper(id, spaceID)
	case common.FIL_PAGE_TYPE_BLOB:
		return NewBlobPageWrapper(id, spaceID, 0)
	case common.FIL_PAGE_TYPE_COMPRESSED:
		return NewCompressedPageWrapper(id, spaceID)
	case common.FIL_PAGE_TYPE_ENCRYPTED:
		return NewEncryptedPageWrapper(id, spaceID, id, nil)
	case common.FIL_PAGE_IBUF_BITMAP:
		return CreateIBufBitmapPageWrapper(id, spaceID, bufferPool)
	case common.FIL_PAGE_TYPE_TRX_SYS:
		return CreateTrxSysPageWrapper(id, spaceID, bufferPool)
	default:
		// 返回一个基础wrapper
		return NewBasePageWrapper(id, spaceID, pageType)
	}
}

// CreateBlobPage 创建BLOB页面（提供段ID参数）
func (f *PageFactory) CreateBlobPage(id, spaceID uint32, segmentID uint64) *BlobPageWrapper {
	return NewBlobPageWrapper(id, spaceID, segmentID)
}

// CreateRollbackPage 创建回滚页面
func (f *PageFactory) CreateRollbackPage(id, spaceID uint32) *RollbackPageWrapper {
	return NewRollbackPageWrapper(id, spaceID)
}

// ParsePage 从字节数据解析页面
func (f *PageFactory) ParsePage(data []byte) (IPageWrapper, error) {
	if len(data) < pages.FileHeaderSize {
		return nil, ErrInvalidPageData
	}

	// 解析页面头部
	spaceID := binary.LittleEndian.Uint32(data[34:38])
	pageID := binary.LittleEndian.Uint32(data[4:8])
	pageType := common.PageType(binary.LittleEndian.Uint16(data[24:26]))

	// 创建对应类型的页面
	page := f.CreatePage(pageType, pageID, spaceID, nil)
	if page == nil {
		return nil, ErrInvalidPageType
	}

	// 解析页面数据
	err := page.ParseFromBytes(data)
	if err != nil {
		return nil, err
	}

	return page, nil
}
