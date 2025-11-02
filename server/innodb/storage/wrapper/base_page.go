package wrapper

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/latch"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/types"
)

// BasePage 基础页面结构
//
// Deprecated: BasePage is deprecated and will be removed in a future version.
// Use types.UnifiedPage instead, which provides:
//   - Better concurrency control with atomic operations
//   - Complete IPageWrapper interface implementation
//   - Integrated statistics and buffer pool support
//   - Full serialization/deserialization support
//
// Migration example:
//
//	// Old code:
//	page := wrapper.NewBasePage(spaceID, pageNo, pageType)
//
//	// New code:
//	page := types.NewUnifiedPage(spaceID, pageNo, pageType)
type BasePage struct {
	*types.UnifiedPage

	// Legacy fields for backward compatibility
	Latch *latch.Latch
}

// NewBasePage 创建一个新的基础页面
//
// Deprecated: Use types.NewUnifiedPage instead
func NewBasePage(spaceID uint32, pageNo uint32, pageType common.PageType) *BasePage {
	return &BasePage{
		UnifiedPage: types.NewUnifiedPage(spaceID, pageNo, pageType),
		Latch:       latch.NewLatch(),
	}
}

// Note: Most methods are now inherited from types.UnifiedPage
// The following methods are available through embedding:
// - GetContent/SetContent
// - GetID/GetSpaceID/GetPageNo/GetPageType
// - GetLSN/SetLSN
// - IsDirty/MarkDirty
// - GetState/SetState
// - GetStats
// - Pin/Unpin
// - Read/Write (through UnifiedPage's implementation)
//
// Legacy Latch field is kept for backward compatibility

// Lock provides access to UnifiedPage's internal lock
func (bp *BasePage) Lock() {
	bp.UnifiedPage.LockPage()
}

// Unlock provides access to UnifiedPage's internal unlock
func (bp *BasePage) Unlock() {
	bp.UnifiedPage.UnlockPage()
}

// RLock provides access to UnifiedPage's internal read lock
func (bp *BasePage) RLock() {
	bp.UnifiedPage.RLockPage()
}

// RUnlock provides access to UnifiedPage's internal read unlock
func (bp *BasePage) RUnlock() {
	bp.UnifiedPage.RUnlockPage()
}
