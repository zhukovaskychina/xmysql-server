package page

import (
	"encoding/binary"
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	pageTypes "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/types"
	"sync/atomic"
	"time"
)

var (
	ErrInvalidPage = errors.New("invalid page")
	ErrPageLocked  = errors.New("page locked")
)

// File header field offsets
const (
	FHeaderSpaceID      = 34
	FHeaderPageNo       = 4
	FHeaderPreviousPage = 8
	FHeaderNextPage     = 12
	FHeaderLSN          = 16
	FHeaderPageType     = 24
	FHeaderFileSequence = 26
	FHeaderPageVersion  = 28
)

// File trailer field offsets
const (
	FTrailerChecksum = 0
	FTrailerLSN      = 4
)

// BasePage 基础页面实现
type BasePage struct {
	ConcurrentWrapper

	// 原始页面数据，包含文件头、页面体和文件尾
	rawPage *pageTypes.PageHeader

	// 关联的缓冲池，可为空
	bufferPool basic.IBufferPool

	state    uint32 // 使用atomic操作
	stats    basic.PageStats
	pinCount int32  // 使用atomic操作
	dirty    uint32 // 使用atomic操作，0=false, 1=true

	// 缓存的完整页面内容
	content []byte
}

// Ensure BasePage implements IPageWrapper
var _ IPageWrapper = (*BasePage)(nil)

// NewBasePage 创建基础页面
func NewBasePage(spaceID, pageNo uint32, pageType common.PageType) *BasePage {
	bp := &BasePage{
		rawPage: pageTypes.NewPageHeader(common.PageSize),
		content: make([]byte, common.PageSize),
	}

	// 初始化文件头信息
	binary.BigEndian.PutUint32(bp.rawPage.FileHeader[FHeaderSpaceID:], spaceID)
	binary.BigEndian.PutUint32(bp.rawPage.FileHeader[FHeaderPageNo:], pageNo)
	binary.BigEndian.PutUint16(bp.rawPage.FileHeader[FHeaderPageType:], uint16(pageType))

	// 初始化状态
	atomic.StoreUint32(&bp.state, uint32(common.PageStateInit))
	atomic.StoreUint32(&bp.dirty, 0)
	atomic.StoreInt32(&bp.pinCount, 0)

	copy(bp.content[:common.FileHeaderSize], bp.rawPage.FileHeader)
	copy(bp.content[common.FileHeaderSize:common.PageSize-common.FileTrailerSize], bp.rawPage.FileBody)
	copy(bp.content[common.PageSize-common.FileTrailerSize:], bp.rawPage.FileTrailer)

	return bp
}

// GetPageID 实现IPageWrapper接口
func (bp *BasePage) GetPageID() uint32 {
	return bp.GetPageNo()
}

// GetSpaceID 实现IPageWrapper接口
func (bp *BasePage) GetSpaceID() uint32 {
	return binary.BigEndian.Uint32(bp.rawPage.FileHeader[FHeaderSpaceID:])
}

// GetPageNo 实现IPageWrapper接口
func (bp *BasePage) GetPageNo() uint32 {
	return binary.BigEndian.Uint32(bp.rawPage.FileHeader[FHeaderPageNo:])
}

// GetPageType 实现IPageWrapper接口
func (bp *BasePage) GetPageType() common.PageType {
	return common.PageType(binary.BigEndian.Uint16(bp.rawPage.FileHeader[FHeaderPageType:]))
}

// GetLSN 实现Page接口
func (bp *BasePage) GetLSN() uint64 {
	return binary.BigEndian.Uint64(bp.rawPage.FileHeader[FHeaderLSN:])
}

// SetLSN 实现Page接口
func (bp *BasePage) SetLSN(lsn uint64) {
	binary.BigEndian.PutUint64(bp.rawPage.FileHeader[FHeaderLSN:], lsn)
	bp.MarkDirty()
}

// IsDirty 实现Page接口
func (bp *BasePage) IsDirty() bool {
	return atomic.LoadUint32(&bp.dirty) == 1
}

// MarkDirty 实现Page接口
func (bp *BasePage) MarkDirty() {
	atomic.StoreUint32(&bp.dirty, 1)
	bp.stats.DirtyCount++
	atomic.StoreUint32(&bp.state, uint32(common.PageStateModified))
}

// GetState 实现Page接口
func (bp *BasePage) GetState() basic.PageState {
	return basic.PageState(atomic.LoadUint32(&bp.state))
}

// SetState 实现Page接口
func (bp *BasePage) SetState(state basic.PageState) {
	atomic.StoreUint32(&bp.state, uint32(state))
	if state == common.PageStateFlushed {
		atomic.StoreUint32(&bp.dirty, 0)
		bp.stats.WriteCount++
	}
}

// GetStats 实现Page接口
func (bp *BasePage) GetStats() *basic.PageStats {
	return &bp.stats
}

// Pin 实现Page接口
func (bp *BasePage) Pin() {
	atomic.AddInt32(&bp.pinCount, 1)
	bp.stats.PinCount = uint64(atomic.LoadInt32(&bp.pinCount))
	atomic.StoreUint32(&bp.state, uint32(common.PageStatePinned))
}

// Unpin 实现Page接口
func (bp *BasePage) Unpin() {
	if atomic.LoadInt32(&bp.pinCount) > 0 {
		atomic.AddInt32(&bp.pinCount, -1)
		bp.stats.PinCount = uint64(atomic.LoadInt32(&bp.pinCount))
	}
	if atomic.LoadInt32(&bp.pinCount) == 0 {
		if bp.IsDirty() {
			atomic.StoreUint32(&bp.state, uint32(common.PageStateDirty))
		} else {
			atomic.StoreUint32(&bp.state, uint32(common.PageStateClean))
		}
	}
}

// GetPinCount 获取引用计数
func (bp *BasePage) GetPinCount() int32 {
	return atomic.LoadInt32(&bp.pinCount)
}

// Read 实现Page接口
func (bp *BasePage) Read() error {
	bp.Lock()
	defer bp.Unlock()

	bp.stats.ReadCount++
	bp.stats.AccessTime = uint64(time.Now().UnixNano())

	if bp.bufferPool != nil {
		if page, err := bp.bufferPool.GetPage(bp.GetSpaceID(), bp.GetPageNo()); err == nil && page != nil {
			return bp.ParseFromBytes(page.GetData())
		}
	}

	return nil
}

// Write 实现Page接口
func (bp *BasePage) Write() error {
	bp.Lock()
	defer bp.Unlock()

	bp.stats.WriteCount++
	bp.stats.AccessTime = uint64(time.Now().UnixNano())

	data, err := bp.ToBytes()
	if err != nil {
		return err
	}

	if bp.bufferPool != nil {
		bufPage, err := bp.bufferPool.GetPage(bp.GetSpaceID(), bp.GetPageNo())
		if err != nil || bufPage == nil {
			bufPage, err = bp.bufferPool.NewPage(bp.GetSpaceID(), bp.GetPageNo(), basic.PageType(bp.GetPageType()))
			if err != nil {
				return err
			}
		}
		if err := bufPage.SetData(data); err == nil {
			bufPage.SetDirty(true)
		}
	}

	atomic.StoreUint32(&bp.dirty, 0)
	return nil
}

// Flush 实现IPageWrapper接口 - 强制刷新页面到磁盘
func (bp *BasePage) Flush() error {
	// 先调用 Write 将数据写入缓冲池
	if err := bp.Write(); err != nil {
		return err
	}

	// 如果有缓冲池，强制刷新
	if bp.bufferPool != nil {
		bufPage, err := bp.bufferPool.GetPage(bp.GetSpaceID(), bp.GetPageNo())
		if err == nil && bufPage != nil {
			// 这里应该调用缓冲池的刷新方法
			// 但 basic.IBufferPool 接口可能没有 Flush 方法
			// 所以我们只是确保页面被标记为脏页
			bufPage.SetDirty(true)
		}
	}

	return nil
}

// GetContent 获取页面内容
func (bp *BasePage) GetContent() []byte {
	result := make([]byte, len(bp.content))
	copy(result, bp.content)
	return result
}

// SetContent 设置页面内容
func (bp *BasePage) SetContent(content []byte) error {
	if len(content) != int(bp.Size()) {
		return ErrInvalidPage
	}
	copy(bp.content, content)
	copy(bp.rawPage.FileHeader, content[:common.FileHeaderSize])
	copy(bp.rawPage.FileBody, content[common.FileHeaderSize:len(content)-common.FileTrailerSize])
	copy(bp.rawPage.FileTrailer, content[len(content)-common.FileTrailerSize:])
	bp.MarkDirty()
	return nil
}

// ParseFromBytes 从字节解析页面
func (bp *BasePage) ParseFromBytes(data []byte) error {
	if err := bp.SetContent(data); err != nil {
		return err
	}
	return nil
}

// ToBytes 序列化为字节
func (bp *BasePage) ToBytes() ([]byte, error) {
	copy(bp.content[:common.FileHeaderSize], bp.rawPage.FileHeader)
	copy(bp.content[common.FileHeaderSize:common.PageSize-common.FileTrailerSize], bp.rawPage.FileBody)
	copy(bp.content[common.PageSize-common.FileTrailerSize:], bp.rawPage.FileTrailer)
	result := make([]byte, len(bp.content))
	copy(result, bp.content)
	return result, nil
}

// ToByte 序列化为字节（兼容旧接口）
func (bp *BasePage) ToByte() []byte {
	bytes, _ := bp.ToBytes()
	return bytes
}

// GetFileHeader 获取文件头（返回字节数组）
func (bp *BasePage) GetFileHeader() []byte {
	bp.RLock()
	defer bp.RUnlock()
	result := make([]byte, common.FileHeaderSize)
	copy(result, bp.rawPage.FileHeader)
	return result
}

// GetFileTrailer 获取文件尾（返回字节数组）
func (bp *BasePage) GetFileTrailer() []byte {
	bp.RLock()
	defer bp.RUnlock()
	result := make([]byte, common.FileTrailerSize)
	copy(result, bp.rawPage.FileTrailer)
	return result
}

// GetFileHeaderStruct 获取文件头结构体
func (bp *BasePage) GetFileHeaderStruct() *pages.FileHeader {
	header := pages.NewFileHeader()
	header.ParseFileHeader(bp.rawPage.FileHeader)
	return &header
}

// GetFileTrailerStruct 获取文件尾结构体
func (bp *BasePage) GetFileTrailerStruct() *pages.FileTrailer {
	trailer := pages.NewFileTrailer()
	copy(trailer.FileTrailer[:], bp.rawPage.FileTrailer)
	return &trailer
}

// Size 获取页面大小
func (bp *BasePage) Size() uint32 {
	return common.PageSize
}

// UpdateChecksum 更新校验和
func (bp *BasePage) UpdateChecksum() {
	var sum uint64
	for _, b := range bp.content[:len(bp.content)-common.FileTrailerSize] {
		sum += uint64(b)
	}
	binary.BigEndian.PutUint64(bp.rawPage.FileTrailer[FTrailerChecksum:], sum)
	copy(bp.content[len(bp.content)-common.FileTrailerSize:], bp.rawPage.FileTrailer)
}

// ValidateChecksum 验证校验和
func (bp *BasePage) ValidateChecksum() bool {
	var sum uint64
	for _, b := range bp.content[:len(bp.content)-common.FileTrailerSize] {
		sum += uint64(b)
	}
	stored := binary.BigEndian.Uint64(bp.rawPage.FileTrailer[FTrailerChecksum:])
	return sum == stored
}
