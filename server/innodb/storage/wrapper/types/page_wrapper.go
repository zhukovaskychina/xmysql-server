package types

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"time"
)

// IPageWrapper represents a page wrapper interface
type IPageWrapper interface {
	GetFileHeader() []byte
	GetFileTrailer() []byte
	GetSpaceID() uint32
	GetPageNo() uint32
	GetPageType() uint16
	GetLSN() uint64
	SetLSN(lsn uint64)
	GetState() basic.PageState
	SetState(state basic.PageState)
	GetStats() *basic.PageStats
	Pin()
	Unpin()
	Read() error
	Write() error
	IsDirty() bool
	MarkDirty()
}

// BasePageWrapper provides a base implementation of IPageWrapper
type BasePageWrapper struct {
	ID          uint32
	SpaceID     uint32
	PageNo      uint32
	PageType    uint16
	LSN         uint64
	State       basic.PageState
	Stats       basic.PageStats
	Content     []byte
	isDirtyFlag bool
	PinCount    int32
}

// NewBasePageWrapper creates a new base page wrapper
func NewBasePageWrapper(id, spaceID, pageNo uint32, pageType uint16) *BasePageWrapper {
	return &BasePageWrapper{
		ID:       id,
		SpaceID:  spaceID,
		PageNo:   pageNo,
		PageType: pageType,
		Content:  make([]byte, 16384), // Default InnoDB page size
	}
}

// GetFileHeader implements IPageWrapper
func (b *BasePageWrapper) GetFileHeader() []byte {
	return b.Content[:38]
}

// GetFileTrailer implements IPageWrapper
func (b *BasePageWrapper) GetFileTrailer() []byte {
	return b.Content[len(b.Content)-8:]
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
func (b *BasePageWrapper) GetPageType() uint16 {
	return b.PageType
}

// GetLSN implements IPageWrapper
func (b *BasePageWrapper) GetLSN() uint64 {
	return b.LSN
}

// SetLSN implements IPageWrapper
func (b *BasePageWrapper) SetLSN(lsn uint64) {
	b.LSN = lsn
}

// GetState implements IPageWrapper
func (b *BasePageWrapper) GetState() basic.PageState {
	return b.State
}

// SetState implements IPageWrapper
func (b *BasePageWrapper) SetState(state basic.PageState) {
	b.State = state
}

// GetStats implements IPageWrapper
func (b *BasePageWrapper) GetStats() *basic.PageStats {
	return &b.Stats
}

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
	return nil
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
