package types

import (
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"time"
)

// IPageWrapper 统一的页面包装器接口
// 所有页面实现都应该实现此接口
// 这是 Storage 模块的核心接口，用于抽象不同类型的页面
type IPageWrapper interface {
	// ========================================
	// 基本信息
	// ========================================

	// GetPageID 获取页面ID（与 GetPageNo 相同）
	GetPageID() uint32

	// GetSpaceID 获取表空间ID
	GetSpaceID() uint32

	// GetPageNo 获取页面号
	GetPageNo() uint32

	// GetPageType 获取页面类型
	// 返回 common.PageType 以保持与项目其他部分的一致性
	GetPageType() common.PageType

	// ========================================
	// LSN (Log Sequence Number) 管理
	// ========================================

	// GetLSN 获取页面的 LSN
	GetLSN() uint64

	// SetLSN 设置页面的 LSN
	SetLSN(lsn uint64)

	// ========================================
	// 状态管理
	// ========================================

	// GetState 获取页面状态
	GetState() basic.PageState

	// SetState 设置页面状态
	SetState(state basic.PageState)

	// IsDirty 检查页面是否为脏页
	IsDirty() bool

	// MarkDirty 标记页面为脏页
	MarkDirty()

	// ========================================
	// 缓冲池管理
	// ========================================

	// Pin 固定页面（增加引用计数）
	Pin()

	// Unpin 取消固定页面（减少引用计数）
	Unpin()

	// GetPinCount 获取引用计数
	GetPinCount() int32

	// GetStats 获取页面统计信息
	GetStats() *basic.PageStats

	// ========================================
	// 序列化
	// ========================================

	// GetFileHeader 获取文件头（38 字节）
	// 返回字节数组以避免不必要的内存分配
	GetFileHeader() []byte

	// GetFileTrailer 获取文件尾（8 字节）
	// 返回字节数组以避免不必要的内存分配
	GetFileTrailer() []byte

	// GetFileHeaderStruct 获取文件头结构体
	// 用于需要访问结构化数据的场景
	GetFileHeaderStruct() *pages.FileHeader

	// GetFileTrailerStruct 获取文件尾结构体
	// 用于需要访问结构化数据的场景
	GetFileTrailerStruct() *pages.FileTrailer

	// ToBytes 序列化页面为字节数组
	ToBytes() ([]byte, error)

	// ToByte 序列化页面为字节数组（兼容旧接口）
	// Deprecated: 使用 ToBytes() 代替
	ToByte() []byte

	// ParseFromBytes 从字节数组反序列化页面
	ParseFromBytes(data []byte) error

	// ========================================
	// I/O 操作
	// ========================================

	// Read 从磁盘或缓冲池读取页面
	Read() error

	// Write 将页面写入缓冲池和磁盘
	Write() error

	// Flush 强制刷新页面到磁盘
	Flush() error
}

// IPageFactory 页面工厂接口
type IPageFactory interface {
	// CreatePage 创建指定类型的页面
	CreatePage(pageType common.PageType, pageID, spaceID uint32) (IPageWrapper, error)

	// LoadPage 从字节数组加载页面
	LoadPage(data []byte) (IPageWrapper, error)

	// ParsePage 解析字节数组为页面（与 LoadPage 相同）
	ParsePage(data []byte) (IPageWrapper, error)
}

// BasePageWrapper provides a base implementation of IPageWrapper
//
// Deprecated: BasePageWrapper is deprecated and will be removed in a future version.
// Use UnifiedPage instead, which provides:
//   - Better concurrency control with atomic operations
//   - Complete IPageWrapper interface implementation
//   - Integrated statistics and buffer pool support
//   - Full serialization/deserialization support
//
// Migration example:
//
//	// Old code:
//	page := types.NewBasePageWrapper(id, spaceID, pageNo, pageType)
//
//	// New code:
//	page := types.NewUnifiedPage(spaceID, pageNo, pageType)
type BasePageWrapper struct {
	ID          uint32
	SpaceID     uint32
	PageNo      uint32
	PageType    common.PageType
	LSN         uint64
	State       basic.PageState
	Stats       basic.PageStats
	Content     []byte
	isDirtyFlag bool
	PinCount    int32
	fileHeader  *pages.FileHeader
	fileTrailer *pages.FileTrailer
}

// NewBasePageWrapper creates a new base page wrapper
//
// Deprecated: Use NewUnifiedPage instead
func NewBasePageWrapper(id, spaceID, pageNo uint32, pageType common.PageType) *BasePageWrapper {
	return &BasePageWrapper{
		ID:       id,
		SpaceID:  spaceID,
		PageNo:   pageNo,
		PageType: pageType,
		Content:  make([]byte, 16384), // Default InnoDB page size
	}
}

// ========================================
// 基本信息
// ========================================

// GetPageID implements IPageWrapper
func (b *BasePageWrapper) GetPageID() uint32 {
	return b.PageNo
}

// GetSpaceID implements IPageWrapper
func (b *BasePageWrapper) GetSpaceID() uint32 {
	return b.SpaceID
}

// GetPageNo implements IPageWrapper
func (b *BasePageWrapper) GetPageNo() uint32 {
	return b.PageNo
}

// GetPageType implements IPageWrapper
func (b *BasePageWrapper) GetPageType() common.PageType {
	return b.PageType
}

// ========================================
// LSN 管理
// ========================================

// GetLSN implements IPageWrapper
func (b *BasePageWrapper) GetLSN() uint64 {
	return b.LSN
}

// SetLSN implements IPageWrapper
func (b *BasePageWrapper) SetLSN(lsn uint64) {
	b.LSN = lsn
}

// ========================================
// 状态管理
// ========================================

// GetState implements IPageWrapper
func (b *BasePageWrapper) GetState() basic.PageState {
	return b.State
}

// SetState implements IPageWrapper
func (b *BasePageWrapper) SetState(state basic.PageState) {
	b.State = state
}

// IsDirty implements IPageWrapper
func (b *BasePageWrapper) IsDirty() bool {
	return b.isDirtyFlag
}

// MarkDirty implements IPageWrapper
func (b *BasePageWrapper) MarkDirty() {
	b.isDirtyFlag = true
	b.Stats.DirtyCount++
}

// ========================================
// 缓冲池管理
// ========================================

// Pin implements IPageWrapper
func (b *BasePageWrapper) Pin() {
	b.PinCount++
}

// Unpin implements IPageWrapper
func (b *BasePageWrapper) Unpin() {
	if b.PinCount > 0 {
		b.PinCount--
	}
}

// GetPinCount implements IPageWrapper
func (b *BasePageWrapper) GetPinCount() int32 {
	return b.PinCount
}

// GetStats implements IPageWrapper
func (b *BasePageWrapper) GetStats() *basic.PageStats {
	return &b.Stats
}

// ========================================
// 序列化
// ========================================

// GetFileHeader implements IPageWrapper
func (b *BasePageWrapper) GetFileHeader() []byte {
	if len(b.Content) >= 38 {
		return b.Content[:38]
	}
	return make([]byte, 38)
}

// GetFileTrailer implements IPageWrapper
func (b *BasePageWrapper) GetFileTrailer() []byte {
	if len(b.Content) >= 8 {
		return b.Content[len(b.Content)-8:]
	}
	return make([]byte, 8)
}

// GetFileHeaderStruct implements IPageWrapper
func (b *BasePageWrapper) GetFileHeaderStruct() *pages.FileHeader {
	if b.fileHeader == nil && len(b.Content) >= 38 {
		b.fileHeader = &pages.FileHeader{}
		// 手动复制文件头数据
		copy(b.fileHeader.FilePageSpaceOrCheckSum[:], b.Content[0:4])
		copy(b.fileHeader.FilePageOffset[:], b.Content[4:8])
		copy(b.fileHeader.FilePagePrev[:], b.Content[8:12])
		copy(b.fileHeader.FilePageNext[:], b.Content[12:16])
		copy(b.fileHeader.FilePageLSN[:], b.Content[16:24])
		copy(b.fileHeader.FilePageType[:], b.Content[24:26])
		copy(b.fileHeader.FilePageFileFlushLSN[:], b.Content[26:34])
		copy(b.fileHeader.FilePageArch[:], b.Content[34:38])
	}
	return b.fileHeader
}

// GetFileTrailerStruct implements IPageWrapper
func (b *BasePageWrapper) GetFileTrailerStruct() *pages.FileTrailer {
	if b.fileTrailer == nil && len(b.Content) >= 8 {
		b.fileTrailer = &pages.FileTrailer{}
		// 手动复制文件尾数据
		copy(b.fileTrailer.FileTrailer[:], b.Content[len(b.Content)-8:])
	}
	return b.fileTrailer
}

// ToBytes implements IPageWrapper (返回副本，安全)
func (b *BasePageWrapper) ToBytes() ([]byte, error) {
	result := make([]byte, len(b.Content))
	copy(result, b.Content)
	return result, nil
}

// ToByte implements IPageWrapper (兼容旧接口，返回副本，安全)
// Deprecated: 使用 ToBytes() 代替
func (b *BasePageWrapper) ToByte() []byte {
	result := make([]byte, len(b.Content))
	copy(result, b.Content)
	return result
}

// ReadContent 高性能只读访问（回调模式，零拷贝）
// 使用场景：只需要读取内容，不需要修改
func (b *BasePageWrapper) ReadContent(fn func([]byte)) {
	fn(b.Content)
}

// ParseFromBytes implements IPageWrapper
func (b *BasePageWrapper) ParseFromBytes(data []byte) error {
	if len(data) != 16384 {
		return ErrInvalidPageSize
	}
	b.Content = make([]byte, len(data))
	copy(b.Content, data)
	b.fileHeader = nil
	b.fileTrailer = nil
	return nil
}

// ========================================
// I/O 操作
// ========================================

// Read implements IPageWrapper
func (b *BasePageWrapper) Read() error {
	b.Stats.ReadCount++
	b.Stats.LastAccessAt = uint64(time.Now().UnixNano())
	return nil
}

// Write implements IPageWrapper
func (b *BasePageWrapper) Write() error {
	b.Stats.WriteCount++
	b.Stats.LastAccessAt = uint64(time.Now().UnixNano())
	b.MarkDirty()
	return nil
}

// Flush implements IPageWrapper
func (b *BasePageWrapper) Flush() error {
	// 基础实现不执行实际的刷新操作
	// 子类应该重写此方法以实现实际的刷新逻辑
	b.isDirtyFlag = false
	return nil
}

// ========================================
// 错误定义
// ========================================

var (
	// ErrInvalidPageSize 无效的页面大小
	ErrInvalidPageSize = errors.New("invalid page size, expected 16384 bytes")
)
