package manager

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"sync/atomic"
)

// defaultPage 默认页面实现，实现basic.IPage接口
type defaultPage struct {
	// 基本信息
	pageID   uint32
	spaceID  uint32
	pageType basic.PageType
	size     uint32

	// 数据
	data []byte

	// 状态
	dirty uint32
	lsn   uint64
	state basic.PageState

	pinCount int32
}

// newPage 创建新页面
func newPage(typ basic.PageType, pageNo uint32) basic.IPage {
	return &defaultPage{
		pageID:   pageNo,
		spaceID:  0,
		pageType: typ,
		size:     16384, // 16KB
		data:     make([]byte, 16384),
		state:    basic.PageStateNew,
	}
}

// ========================================
// 基本信息获取
// ========================================
func (p *defaultPage) GetPageID() uint32 {
	return p.pageID
}

func (p *defaultPage) GetPageNo() uint32 {
	return p.pageID
}

func (p *defaultPage) GetSpaceID() uint32 {
	return p.spaceID
}

func (p *defaultPage) GetPageType() basic.PageType {
	return p.pageType
}

func (p *defaultPage) GetSize() uint32 {
	return p.size
}

// ========================================
// 数据访问
// ========================================
func (p *defaultPage) GetData() []byte {
	return p.data
}

func (p *defaultPage) GetContent() []byte {
	return p.data
}

func (p *defaultPage) SetData(data []byte) error {
	if len(data) > int(p.size) {
		return ErrPageDataTooLarge
	}
	copy(p.data, data)
	p.SetDirty(true)
	return nil
}

func (p *defaultPage) SetContent(content []byte) {
	copy(p.data, content)
	p.SetDirty(true)
}

// ========================================
// 状态管理
// ========================================
func (p *defaultPage) IsDirty() bool {
	return atomic.LoadUint32(&p.dirty) == 1
}

func (p *defaultPage) SetDirty(dirty bool) {
	if dirty {
		atomic.StoreUint32(&p.dirty, 1)
	} else {
		atomic.StoreUint32(&p.dirty, 0)
	}
}

func (p *defaultPage) MarkDirty() {
	atomic.StoreUint32(&p.dirty, 1)
}

func (p *defaultPage) ClearDirty() {
	atomic.StoreUint32(&p.dirty, 0)
}

func (p *defaultPage) GetState() basic.PageState {
	return p.state
}

func (p *defaultPage) SetState(state basic.PageState) {
	p.state = state
}

// ========================================
// LSN (Log Sequence Number) 管理
// ========================================
func (p *defaultPage) GetLSN() uint64 {
	return atomic.LoadUint64(&p.lsn)
}

func (p *defaultPage) SetLSN(lsn uint64) {
	atomic.StoreUint64(&p.lsn, lsn)
}

// ========================================
// 缓冲池管理
// ========================================
func (p *defaultPage) Pin() {
	atomic.AddInt32(&p.pinCount, 1)
	p.SetState(basic.PageStateLoaded)
}

func (p *defaultPage) Unpin() {
	if atomic.AddInt32(&p.pinCount, -1) < 0 {
		atomic.StoreInt32(&p.pinCount, 0)
	}

	if atomic.LoadInt32(&p.pinCount) == 0 && p.state == basic.PageStateLoaded && !p.IsDirty() {
		p.SetState(basic.PageStateClean)
	}
}

// ========================================
// IO 操作
// ========================================
func (p *defaultPage) Read() error {
	// 当前实现使用内存缓存承载页面内容，不依赖外部存储
	// 调用方应在上层提供真实的磁盘/IO读取能力
	return nil
}

func (p *defaultPage) Write() error {
	// 当前实现仅在内存中标记脏页状态，具体持久化由上层缓冲池管理器负责
	if p.IsDirty() {
		p.ClearDirty()
		p.SetState(basic.PageStateClean)
	}
	return nil
}

// ========================================
// 页面类型检查
// ========================================
func (p *defaultPage) IsLeafPage() bool {
	return p.pageType == basic.PageTypeIndex
}

// ========================================
// 生命周期管理
// ========================================
func (p *defaultPage) Init() error {
	// 初始化页面头部
	if len(p.data) >= 2 {
		p.data[0] = byte(p.pageType)
		p.data[1] = byte(p.pageType >> 8)
	}
	return nil
}

func (p *defaultPage) Release() {
	p.data = nil
}

// parsePage 从数据解析页面
func parsePage(data []byte) (basic.IPage, error) {
	if len(data) < 16 {
		return nil, ErrInvalidPageData
	}

	// 解析页面头部获取类型
	typ := basic.PageType(data[0])

	// 创建页面实例
	p := &defaultPage{
		pageID:   0, // 页面号需要从数据中解析
		spaceID:  0, // 表空间ID需要从数据中解析
		pageType: typ,
		size:     uint32(len(data)),
		data:     data,
		state:    basic.PageStateActive,
	}

	return p, nil
}
