package basic

import (
	"errors"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

var (
	ErrBasePageStorageUnavailable = errors.New("base page storage provider is unavailable")
	ErrBasePageInvalidSize        = errors.New("base page content has an invalid size")
)

// BasePageWrapper provides a base implementation of IPageWrapper
type BasePageWrapper struct {
	ID       uint32
	SpaceID  uint32
	PageNo   uint32
	PageType uint16
	LSN      uint64
	State    PageState
	Stats    PageStats
	Content  []byte
	IsDirty  bool
	PinCount int32
	storage  StorageProvider
}

// NewBasePageWrapper creates a new base page wrapper
func NewBasePageWrapper(id, spaceID, pageNo uint32, pageType uint16) *BasePageWrapper {
	return NewBasePageWrapperWithStorage(id, spaceID, pageNo, pageType, nil)
}

// NewBasePageWrapperWithStorage creates a basic page wrapper backed by an
// optional durable provider. The historical constructor remains compatible
// while provider-backed callers get real page I/O.
func NewBasePageWrapperWithStorage(id, spaceID, pageNo uint32, pageType uint16, storage StorageProvider) *BasePageWrapper {
	return &BasePageWrapper{
		ID:       id,
		SpaceID:  spaceID,
		PageNo:   pageNo,
		PageType: pageType,
		Content:  make([]byte, 16384), // Default InnoDB page size
		storage:  storage,
	}
}

// GetFileHeader implements IPageWrapper
func (bp *BasePageWrapper) GetFileHeader() []byte {
	return bp.Content[:38]
}

// GetFileTrailer implements IPageWrapper
func (bp *BasePageWrapper) GetFileTrailer() []byte {
	return bp.Content[16376:]
}

// GetSpaceID implements IPageWrapper
func (bp *BasePageWrapper) GetSpaceID() uint32 {
	return bp.SpaceID
}

// GetPageNo implements IPageWrapper
func (bp *BasePageWrapper) GetPageNo() uint32 {
	return bp.PageNo
}

// GetPageType implements IPageWrapper
func (bp *BasePageWrapper) GetPageType() uint16 {
	return bp.PageType
}

// GetLSN implements IPageWrapper
func (bp *BasePageWrapper) GetLSN() uint64 {
	return bp.LSN
}

// SetLSN implements IPageWrapper
func (bp *BasePageWrapper) SetLSN(lsn uint64) {
	bp.LSN = lsn
}

// MarkDirty implements IPageWrapper
func (bp *BasePageWrapper) MarkDirty() {
	bp.IsDirty = true
}

// GetState implements IPageWrapper
func (bp *BasePageWrapper) GetState() PageState {
	return bp.State
}

// SetState implements IPageWrapper
func (bp *BasePageWrapper) SetState(state PageState) {
	bp.State = state
}

// GetStats implements IPageWrapper
func (bp *BasePageWrapper) GetStats() *PageStats {
	return &bp.Stats
}

// Pin implements IPageWrapper
func (bp *BasePageWrapper) Pin() {
	bp.PinCount++
}

// Unpin implements IPageWrapper
func (bp *BasePageWrapper) Unpin() {
	if bp.PinCount > 0 {
		bp.PinCount--
	}
}

// Read implements IPageWrapper
func (bp *BasePageWrapper) Read() error {
	if bp.storage == nil {
		return ErrBasePageStorageUnavailable
	}
	data, err := bp.storage.ReadPage(bp.SpaceID, bp.PageNo)
	if err != nil {
		return err
	}
	if len(data) < common.PageSize {
		return ErrBasePageInvalidSize
	}
	if len(bp.Content) < common.PageSize {
		bp.Content = make([]byte, common.PageSize)
	}
	copy(bp.Content, data[:common.PageSize])
	now := uint64(time.Now().UnixNano())
	bp.Stats.ReadCount++
	bp.Stats.AccessTime = now
	bp.Stats.LastAccessAt = now
	bp.Stats.LastAccessed = now
	bp.State = common.PageStateLoaded
	return nil
}

// Write implements IPageWrapper
func (bp *BasePageWrapper) Write() error {
	if bp.storage == nil {
		return ErrBasePageStorageUnavailable
	}
	if len(bp.Content) < common.PageSize {
		return ErrBasePageInvalidSize
	}
	if err := bp.storage.WritePage(bp.SpaceID, bp.PageNo, bp.Content[:common.PageSize]); err != nil {
		return err
	}
	now := uint64(time.Now().UnixNano())
	bp.Stats.WriteCount++
	bp.Stats.AccessTime = now
	bp.Stats.LastAccessAt = now
	bp.Stats.LastModified = now
	bp.IsDirty = false
	bp.State = common.PageStateFlushed
	return nil
}
