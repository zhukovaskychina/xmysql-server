package page

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"sync/atomic"
	"time"
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
	storage  basic.StorageProvider
}

var ErrPageStorageUnavailable = fmt.Errorf("page storage provider is unavailable")

// NewPage creates a new page instance
func NewPage(spaceID, pageNo uint32, pageType common.PageType) *PageImpl {
	return NewPageWithStorage(spaceID, pageNo, pageType, nil)
}

// NewPageWithStorage creates a legacy page wrapper backed by a real provider.
// NewPage remains available for source compatibility but deliberately returns
// an explicit error from Read/Write when no provider is supplied.
func NewPageWithStorage(spaceID, pageNo uint32, pageType common.PageType, storage basic.StorageProvider) *PageImpl {
	return &PageImpl{
		spaceID:  spaceID,
		pageNo:   pageNo,
		pageType: pageType,
		state:    basic.PageStateNew,
		stats:    &basic.PageStats{},
		pageData: make([]byte, 0), // Initialize with appropriate size
		storage:  storage,
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
		logger.Warnf("page unpin underflow: space=%d page=%d, pin count reset to 0", p.spaceID, p.pageNo)
		atomic.StoreInt32(&p.pinCount, 0)
	}
}

// Read loads the page data from disk
func (p *PageImpl) Read() error {
	if p.storage == nil {
		return ErrPageStorageUnavailable
	}
	data, err := p.storage.ReadPage(p.spaceID, p.pageNo)
	if err != nil {
		return err
	}
	if len(data) < common.PageSize {
		return fmt.Errorf("page %d/%d has %d bytes, want at least %d", p.spaceID, p.pageNo, len(data), common.PageSize)
	}
	p.pageData = append(p.pageData[:0], data...)

	p.state = basic.PageStateLoaded
	p.stats.ReadCount++
	p.stats.LastAccessAt = uint64(time.Now().UnixNano())
	p.stats.AccessTime = p.stats.LastAccessAt
	return nil
}

// Write persists the page data to disk
func (p *PageImpl) Write() error {
	if !p.isDirty {
		return nil
	}
	if p.storage == nil {
		return ErrPageStorageUnavailable
	}
	if len(p.pageData) < common.PageSize {
		return fmt.Errorf("page %d/%d has %d bytes, want at least %d", p.spaceID, p.pageNo, len(p.pageData), common.PageSize)
	}
	if err := p.storage.WritePage(p.spaceID, p.pageNo, p.pageData); err != nil {
		return err
	}

	p.isDirty = false
	p.stats.WriteCount++
	p.stats.LastModified = uint64(time.Now().UnixNano())
	p.stats.AccessTime = p.stats.LastModified
	p.state = common.PageStateClean
	return nil
}
