package page

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"sync/atomic"
)

// PageImpl implements the IPageWrapper interface
type PageImpl struct {
	spaceID  uint32
	pageNo   uint32
	pageType common.PageType
	lsn      uint64
	isDirty  bool
	pinCount int32
	state    basic.PageState
	stats    *basic.PageStats
	pageData []byte
}

// NewPage creates a new page instance
func NewPage(spaceID, pageNo uint32, pageType common.PageType) *PageImpl {
	return &PageImpl{
		spaceID:  spaceID,
		pageNo:   pageNo,
		pageType: pageType,
		state:    basic.PageStateNew,
		stats:    &basic.PageStats{},
		pageData: make([]byte, 0), // Initialize with appropriate size
	}
}

// GetSpaceID returns the space ID of the page
func (p *PageImpl) GetSpaceID() uint32 {
	return p.spaceID
}

// GetPageNo returns the page number
func (p *PageImpl) GetPageNo() uint32 {
	return p.pageNo
}

// GetPageType returns the page type
func (p *PageImpl) GetPageType() uint16 {
	return uint16(p.pageType)
}

// GetLSN returns the log sequence number
func (p *PageImpl) GetLSN() uint64 {
	return atomic.LoadUint64(&p.lsn)
}

// SetLSN sets the log sequence number
func (p *PageImpl) SetLSN(lsn uint64) {
	atomic.StoreUint64(&p.lsn, lsn)
}

// IsDirty returns whether the page has been modified
func (p *PageImpl) IsDirty() bool {
	return p.isDirty
}

// MarkDirty marks the page as modified
func (p *PageImpl) MarkDirty() {
	p.isDirty = true
}

// GetState returns the current state of the page
func (p *PageImpl) GetState() basic.PageState {
	return p.state
}

// SetState sets the state of the page
func (p *PageImpl) SetState(state basic.PageState) {
	p.state = state
}

// GetStats returns page statistics
func (p *PageImpl) GetStats() *basic.PageStats {
	return p.stats
}

// Pin increments the pin count
func (p *PageImpl) Pin() {
	atomic.AddInt32(&p.pinCount, 1)
}

// Unpin decrements the pin count
func (p *PageImpl) Unpin() {
	if atomic.AddInt32(&p.pinCount, -1) < 0 {
		panic("unpin called too many times")
	}
}

// Read loads the page data from disk
func (p *PageImpl) Read() error {
	// TODO: Implement actual disk reading logic
	// 1. Check if page is already in memory
	// 2. If not, read from disk
	// 3. Update page state and statistics
	return nil
}

// Write persists the page data to disk
func (p *PageImpl) Write() error {
	// TODO: Implement actual disk writing logic
	// 1. Check if page is dirty
	// 2. If dirty, write to disk
	// 3. Update page state and statistics
	p.isDirty = false
	return nil
}
