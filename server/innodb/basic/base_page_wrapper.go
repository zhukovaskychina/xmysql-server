package basic

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
	// To be implemented by derived classes
	return nil
}

// Write implements IPageWrapper
func (bp *BasePageWrapper) Write() error {
	// To be implemented by derived classes
	return nil
}
