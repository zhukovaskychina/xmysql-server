package mvcc

// Deprecated: This file is deprecated and will be removed in a future version.
// All methods in this file are empty stubs with no implementation.
// MVCC functionality has been migrated to:
// - format/mvcc for data structures (ReadView, RecordVersion, VersionChain)
// - wrapper/mvcc for business logic (IMVCCPage, PageSnapshot)
// - manager for transaction management (MVCCManager, TransactionManager)
//
// Migration guide:
// - For ReadView: use format/mvcc.ReadView
// - For transaction management: use manager.MVCCManager or manager.TransactionManager
//
// This file will be removed after all references are updated.

// Mvcc MVCC控制结构
// Deprecated: 所有方法都是空实现，请使用manager.MVCCManager代替
type Mvcc struct {
	ActiveViews []ReadView
	FreeViews   []ReadView
}

// CreateView 创建一个readview
// Deprecated: 空实现，请使用manager.MVCCManager.BeginTransaction()代替
func (m Mvcc) CreateView() (*ReadView, *TrxT) {
	return nil, nil
}

// CloseView 关闭一个readview
// Deprecated: 空实现，format/mvcc.ReadView是不可变的，不需要显式关闭
func (m Mvcc) CloseView(view *ReadView, ownMutex bool) {
}

// IsViewRelease 是否关闭一个View
// Deprecated: 空实现
func (m Mvcc) IsViewRelease(view *ReadView) bool {
	return false
}

// CloneOldestView 克隆最老的View
// Deprecated: 空实现
func (m Mvcc) CloneOldestView() {
}

// GetActiveReadViewSize 获取活跃ReadView数量
// Deprecated: 空实现
func (m Mvcc) GetActiveReadViewSize() int {
	return 0
}

// IsReadViewActive 判断ReadView是否活跃
// Deprecated: 空实现
func (m Mvcc) IsReadViewActive(view ReadView) bool {
	return false
}
