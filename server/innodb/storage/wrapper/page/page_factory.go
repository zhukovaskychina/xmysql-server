package page

import (
	"encoding/binary"
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/types"
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

func newFallbackPageWrapper(pageType common.PageType, id, spaceID uint32) IPageWrapper {
	// 统一入口：除专用页面包装器外，默认走 types.UnifiedPage。
	// 任何新增页面路径都应优先使用该统一入口。
	return types.NewUnifiedPage(spaceID, id, pageType)
}

// IPageWrapper 使用统一的页面包装器接口
// 此类型别名用于向后兼容，新代码应直接使用 types.IPageWrapper
type IPageWrapper = types.IPageWrapper

// CreatePage 根据页面类型创建对应的页面wrapper
func (f *PageFactory) CreatePage(pageType common.PageType, id, spaceID uint32, bufferPool *buffer_pool.BufferPool) IPageWrapper {
	return f.CreatePageWithStorage(pageType, id, spaceID, bufferPool, nil)
}

// CreatePageWithStorage creates a page through the same factory dispatch while
// preserving the supplied durable provider across every provider-aware wrapper.
// The provider is intentionally optional so the legacy CreatePage behavior
// remains source-compatible for callers that only parse or construct pages.
func (f *PageFactory) CreatePageWithStorage(pageType common.PageType, id, spaceID uint32, bufferPool *buffer_pool.BufferPool, storage basic.StorageProvider) IPageWrapper {
	switch pageType {
	case common.FIL_PAGE_INDEX:
		// The legacy IndexPage exposes the index-specific API but does not
		// implement the complete IPageWrapper contract (notably Flush). The
		// factory contract is the unified page interface, so use the provider-
		// backed unified wrapper here instead of returning a value that would
		// fail the old runtime type assertion.
		return types.NewUnifiedPageWithStorage(spaceID, id, pageType, storage)
	case common.FIL_PAGE_FSP_HDR:
		return NewFSPPageWrapperWithStorage(id, spaceID, bufferPool, storage)
	case common.FIL_PAGE_INODE:
		return types.NewUnifiedPageWithStorage(spaceID, id, pageType, storage)
	case common.FIL_PAGE_IBUF_FREE_LIST:
		return NewIBufFreeListPageWrapperWithStorage(id, spaceID, storage)
	case common.FIL_PAGE_TYPE_SYS:
		return NewDataDictionaryPageWrapperWithStorage(id, spaceID, bufferPool, storage)
	case common.FIL_PAGE_TYPE_XDES:
		return NewXDESPageWrapperWithStorage(id, spaceID, bufferPool, storage)
	case common.FIL_PAGE_UNDO_LOG:
		return NewUndoLogPageWrapperWithStorage(id, spaceID, id, bufferPool, storage)
	case common.FIL_PAGE_TYPE_ALLOCATED:
		return NewAllocatePageWrapperWithStorage(id, spaceID, storage)
	case common.FIL_PAGE_TYPE_BLOB:
		return NewBlobPageWrapperWithStorage(id, spaceID, 0, storage)
	case common.FIL_PAGE_TYPE_COMPRESSED:
		return NewCompressedPageWrapperWithStorage(id, spaceID, storage)
	case common.FIL_PAGE_TYPE_ENCRYPTED:
		return NewEncryptedPageWrapperWithStorage(id, spaceID, id, bufferPool, storage)
	case common.FIL_PAGE_IBUF_BITMAP:
		return NewIBufBitmapPageWrapperWithStorage(id, spaceID, bufferPool, storage)
	case common.FIL_PAGE_TYPE_TRX_SYS:
		return NewTrxSysPageWrapperWithStorage(id, spaceID, bufferPool, storage)
	default:
		return types.NewUnifiedPageWithStorage(spaceID, id, pageType, storage)
	}
}

// CreateBlobPage 创建BLOB页面（提供段ID参数）
func (f *PageFactory) CreateBlobPage(id, spaceID uint32, segmentID uint64) *BlobPageWrapper {
	return f.CreateBlobPageWithStorage(id, spaceID, segmentID, nil)
}

// CreateBlobPageWithStorage creates a BLOB page with an optional durable provider.
func (f *PageFactory) CreateBlobPageWithStorage(id, spaceID uint32, segmentID uint64, storage basic.StorageProvider) *BlobPageWrapper {
	return NewBlobPageWrapperWithStorage(id, spaceID, segmentID, storage)
}

// CreateRollbackPage 创建回滚页面
func (f *PageFactory) CreateRollbackPage(id, spaceID uint32) *RollbackPageWrapper {
	return f.CreateRollbackPageWithStorage(id, spaceID, nil)
}

// CreateRollbackPageWithStorage creates a rollback page with an optional durable provider.
func (f *PageFactory) CreateRollbackPageWithStorage(id, spaceID uint32, storage basic.StorageProvider) *RollbackPageWrapper {
	return NewRollbackPageWrapperWithStorage(id, spaceID, storage)
}

// ParsePage 从字节数据解析页面
func (f *PageFactory) ParsePage(data []byte) (IPageWrapper, error) {
	return f.ParsePageWithStorage(data, nil)
}

// ParsePageWithStorage parses a page and retains the supplied provider for
// subsequent Read/Write/Flush operations.
func (f *PageFactory) ParsePageWithStorage(data []byte, storage basic.StorageProvider) (IPageWrapper, error) {
	if len(data) < pages.FileHeaderSize {
		return nil, ErrInvalidPageData
	}

	// 解析页面头部
	spaceID := binary.LittleEndian.Uint32(data[34:38])
	pageID := binary.LittleEndian.Uint32(data[4:8])
	pageType := common.PageType(binary.LittleEndian.Uint16(data[24:26]))

	// 创建对应类型的页面
	page := f.CreatePageWithStorage(pageType, pageID, spaceID, nil, storage)
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
