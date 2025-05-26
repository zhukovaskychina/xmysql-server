package buffer_pool

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"sync"
	"time"
)

//TODO 用来实现bufferpool
/**
这个可以理解为另外一个数据页的控制体，大部分的数据页信息存在其中，例如space_id, page_no, page state, newest_modification，
oldest_modification，access_time以及压缩页的所有信息等。压缩页的信息包括压缩页的大小，压缩页的数据指针(真正的压缩页数据是存储在由伙伴
系统分配的数据页上)。这里需要注意一点，如果某个压缩页被解压了，解压页的数据指针是存储在buf_block_t的frame字段里。

**/
type BufferPage struct {
	// 基本信息
	spaceId   uint32
	pageNo    uint32
	pageState BufferPageState
	flushType BufferFlushType
	iofix     buffer_io_fix

	// 版本控制
	newestModification common.LSNT
	oldestModification common.LSNT
	accessTime         uint64

	// 页面内容
	content []byte

	// 状态标记
	dirty           bool
	mu              sync.RWMutex
	isInYoungRegion bool
}

// GetContent 获取页面内容
func (bp *BufferPage) GetContent() []byte {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.content
}

// SetContent 设置页面内容
func (bp *BufferPage) SetContent(content []byte) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.content = content
}

// GetSpaceID 获取表空间ID
func (bp *BufferPage) GetSpaceID() uint32 {
	return bp.spaceId
}

// GetPageNo 获取页面号
func (bp *BufferPage) GetPageNo() uint32 {
	return bp.pageNo
}

// GetLSN 获取LSN
func (bp *BufferPage) GetLSN() uint64 {
	return uint64(bp.newestModification)
}

// IsDirty 检查是否为脏页
func (bp *BufferPage) IsDirty() bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.dirty
}

// MarkDirty 标记为脏页
func (bp *BufferPage) MarkDirty() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.dirty = true
}

// ClearDirty 清除脏页标记
func (bp *BufferPage) ClearDirty() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.dirty = false
}

// Reset resets the buffer page to initial state
func (bp *BufferPage) Reset() {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.spaceId = 0
	bp.pageNo = 0
	bp.pageState = BUF_BLOCK_NOT_USED
	bp.flushType = BUF_FLUSH_NONE
	bp.iofix = BUF_IO_NONE
	bp.newestModification = 0
	bp.oldestModification = 0
	bp.accessTime = 0
	bp.dirty = false
	bp.content = make([]byte, common.UNIV_PAGE_SIZE)
}

// IsFree returns true if the page is free
func (bp *BufferPage) IsFree() bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.pageState == BUF_BLOCK_NOT_USED
}

// Init initializes the buffer page
func (bp *BufferPage) Init(spaceID uint32, pageNo uint32, content []byte) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.spaceId = spaceID
	bp.pageNo = pageNo
	bp.pageState = BUF_BLOCK_READY_FOR_USE
	bp.flushType = BUF_FLUSH_NONE
	bp.iofix = BUF_IO_NONE
	bp.newestModification = 0
	bp.oldestModification = 0
	bp.accessTime = uint64(time.Now().UnixNano())
	bp.dirty = false

	// Copy content
	if len(content) > 0 {
		if bp.content == nil {
			bp.content = make([]byte, common.UNIV_PAGE_SIZE)
		}
		copy(bp.content, content)
	}
}

// SetDirty sets the dirty flag
func (bp *BufferPage) SetDirty(dirty bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.dirty = dirty
}

// GetData returns the page data
func (bp *BufferPage) GetData() []byte {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.content
}

// IsInYoungRegion returns whether the page is in young region
func (bp *BufferPage) IsInYoungRegion() bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.isInYoungRegion
}

// SetInYoungRegion sets whether the page is in young region
func (bp *BufferPage) SetInYoungRegion(young bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.isInYoungRegion = young
}

func (bp *BufferPage) Unpin() {

}

// NewBufferPage creates a new buffer page
func NewBufferPage(spaceID uint32, pageNo uint32) *BufferPage {
	bp := &BufferPage{
		spaceId:   spaceID,
		pageNo:    pageNo,
		pageState: BUF_BLOCK_NOT_USED,
		flushType: BUF_FLUSH_NONE,
		iofix:     BUF_IO_NONE,
		content:   make([]byte, common.UNIV_PAGE_SIZE),
	}
	return bp
}
