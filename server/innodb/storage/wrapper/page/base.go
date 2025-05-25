package page

import (
	"encoding/binary"
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"
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
	rawPage  *PageHeader // 使用原始页面结构
	state    atomic.Uint32
	stats    basic.PageStats
	pinCount atomic.Int32
	dirty    atomic.Bool
}

// Ensure BasePage implements IPageWrapper
var _ IPageWrapper = (*BasePage)(nil)

// NewBasePage 创建基础页面
func NewBasePage(spaceID, pageNo uint32, pageType common.PageType) *BasePage {
	bp := &BasePage{
		rawPage: NewPageHeader(),
	}

	// 初始化页面头
	header := make([]byte, 38)
	binary.LittleEndian.PutUint32(header[FHeaderSpaceID:], spaceID)
	binary.LittleEndian.PutUint32(header[FHeaderPageNo:], pageNo)
	binary.LittleEndian.PutUint16(header[FHeaderPageType:], uint16(pageType))

	// 初始化状态
	bp.state.Store(uint32(common.PageStateInit))
	bp.dirty.Store(false)
	bp.pinCount.Store(0)

	return bp
}

// GetPageID 实现IPageWrapper接口
func (bp *BasePage) GetPageID() uint32 {
	return bp.GetPageNo()
}

// GetSpaceID 实现IPageWrapper接口
func (bp *BasePage) GetSpaceID() uint32 {
	// TODO: 从rawPage中读取
	return 0
}

// GetPageNo 实现IPageWrapper接口
func (bp *BasePage) GetPageNo() uint32 {
	// TODO: 从rawPage中读取
	return 0
}

// GetPageType 实现IPageWrapper接口
func (bp *BasePage) GetPageType() common.PageType {
	// TODO: 从rawPage中读取
	return common.FIL_PAGE_TYPE_ALLOCATED
}

// GetLSN 实现Page接口
func (bp *BasePage) GetLSN() uint64 {
	// TODO: 从rawPage中读取
	return 0
}

// SetLSN 实现Page接口
func (bp *BasePage) SetLSN(lsn uint64) {
	// TODO: 写入rawPage
	bp.MarkDirty()
}

// IsDirty 实现Page接口
func (bp *BasePage) IsDirty() bool {
	return bp.dirty.Load()
}

// MarkDirty 实现Page接口
func (bp *BasePage) MarkDirty() {
	bp.dirty.Store(true)
	bp.stats.DirtyCount++
	bp.state.Store(uint32(common.PageStateModified))
}

// GetState 实现Page接口
func (bp *BasePage) GetState() basic.PageState {
	return basic.PageState(bp.state.Load())
}

// SetState 实现Page接口
func (bp *BasePage) SetState(state basic.PageState) {
	bp.state.Store(uint32(state))
	if state == common.PageStateFlushed {
		bp.dirty.Store(false)
		bp.stats.WriteCount++
	}
}

// GetStats 实现Page接口
func (bp *BasePage) GetStats() *basic.PageStats {
	return &bp.stats
}

// Pin 实现Page接口
func (bp *BasePage) Pin() {
	bp.pinCount.Add(1)
	bp.stats.PinCount = uint64(bp.pinCount.Load())
	bp.state.Store(uint32(common.PageStatePinned))
}

// Unpin 实现Page接口
func (bp *BasePage) Unpin() {
	if bp.pinCount.Load() > 0 {
		bp.pinCount.Add(-1)
		bp.stats.PinCount = uint64(bp.pinCount.Load())
	}
	if bp.pinCount.Load() == 0 {
		if bp.IsDirty() {
			bp.state.Store(uint32(common.PageStateDirty))
		} else {
			bp.state.Store(uint32(common.PageStateClean))
		}
	}
}

// Read 实现Page接口
func (bp *BasePage) Read() error {
	bp.Lock()
	defer bp.Unlock()

	bp.stats.ReadCount++
	bp.stats.AccessTime = uint64(time.Now().UnixNano())

	// TODO: 实现从磁盘读取页面
	return nil
}

// Write 实现Page接口
func (bp *BasePage) Write() error {
	bp.Lock()
	defer bp.Unlock()

	bp.stats.WriteCount++
	bp.stats.AccessTime = uint64(time.Now().UnixNano())

	// TODO: 实现写入磁盘
	return nil
}

// GetContent 获取页面内容
func (bp *BasePage) GetContent() []byte {
	// TODO: 返回rawPage的内容
	return make([]byte, 16384)
}

// SetContent 设置页面内容
func (bp *BasePage) SetContent(content []byte) error {
	// TODO: 设置rawPage的内容
	bp.MarkDirty()
	return nil
}

// ParseFromBytes 从字节解析页面
func (bp *BasePage) ParseFromBytes(data []byte) error {
	// TODO: 解析页面数据到rawPage
	return nil
}

// ToBytes 序列化为字节
func (bp *BasePage) ToBytes() ([]byte, error) {
	// TODO: 序列化rawPage为字节
	return make([]byte, 16384), nil
}

// GetFileHeader 获取文件头
func (bp *BasePage) GetFileHeader() *pages.FileHeader {
	// TODO: 返回rawPage的FileHeader
	header := pages.NewFileHeader()
	return &header
}

// GetFileTrailer 获取文件尾
func (bp *BasePage) GetFileTrailer() *pages.FileTrailer {
	// TODO: 返回rawPage的FileTrailer
	trailer := pages.NewFileTrailer()
	return &trailer
}

// Size 获取页面大小
func (bp *BasePage) Size() uint32 {
	return 16384
}

// UpdateChecksum 更新校验和
func (bp *BasePage) UpdateChecksum() {
	// TODO: 实现校验和更新
}

// ValidateChecksum 验证校验和
func (bp *BasePage) ValidateChecksum() bool {
	// TODO: 实现校验和验证
	return true
}
