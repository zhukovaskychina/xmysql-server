package mvcc

import (
	"sort"
	"sync"
)

// Deprecated: use manager.TransactionManager and format/mvcc for new code.
// This compatibility wrapper remains functional for older package consumers
// while the migration to the current MVCC implementation is completed.
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
// Deprecated: use manager.MVCCManager for new code.
type Mvcc struct {
	ActiveViews []ReadView
	FreeViews   []ReadView
	mu          *sync.RWMutex
	nextTrxID   int64
}

// CreateView 创建一个readview
// Deprecated: use manager.MVCCManager.BeginTransaction() for new code.
func (m *Mvcc) CreateView() (*ReadView, *TrxT) {
	if m == nil {
		return nil, nil
	}
	m.ensureState()
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextTrxID++
	creator := m.nextTrxID
	activeIDs := make([]int64, 0, len(m.ActiveViews))
	for _, active := range m.ActiveViews {
		activeIDs = append(activeIDs, int64(active.creatorTrxID))
	}
	sort.Slice(activeIDs, func(i, j int) bool { return activeIDs[i] < activeIDs[j] })
	minTrxID := creator
	if len(activeIDs) > 0 {
		minTrxID = activeIDs[0]
	}
	view := NewReadView(activeIDs, minTrxID, creator+1, creator)
	m.ActiveViews = append(m.ActiveViews, *view)
	readViews := []ReadView{*view}
	return view, &TrxT{ReadViews: &readViews, IsolationLevel: RepeatableRead, trxStateT: Active}
}

// CloseView 关闭一个readview
// Deprecated: format/mvcc.ReadView is immutable; new code does not need close.
func (m *Mvcc) CloseView(view *ReadView, ownMutex bool) {
	if m == nil || view == nil {
		return
	}
	m.ensureState()
	if ownMutex {
		m.mu.Lock()
		defer m.mu.Unlock()
	}
	for index := range m.ActiveViews {
		candidate := &m.ActiveViews[index]
		if candidate.creatorTrxID == view.creatorTrxID && candidate.createTime.Equal(view.createTime) {
			m.FreeViews = append(m.FreeViews, *candidate)
			m.ActiveViews = append(m.ActiveViews[:index], m.ActiveViews[index+1:]...)
			return
		}
	}
}

// IsViewRelease 是否关闭一个View
// Deprecated: use the current manager MVCC lifecycle for new code.
func (m *Mvcc) IsViewRelease(view *ReadView) bool {
	if m == nil || view == nil {
		return true
	}
	return !m.IsReadViewActive(*view)
}

// CloneOldestView 克隆最老的View
// Deprecated: use the current manager MVCC lifecycle for new code.

func (m *Mvcc) CloneOldestView() {
	if m == nil {
		return
	}
	m.ensureState()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.ActiveViews) == 0 {
		return
	}
	oldest := m.ActiveViews[0]
	for _, candidate := range m.ActiveViews[1:] {
		if candidate.createTime.Before(oldest.createTime) {
			oldest = candidate
		}
	}
	clone := oldest.Clone()
	m.ActiveViews = append(m.ActiveViews, *clone)
}

// GetActiveReadViewSize 获取活跃ReadView数量
// Deprecated: use the current manager MVCC lifecycle for new code.
func (m *Mvcc) GetActiveReadViewSize() int {
	if m == nil {
		return 0
	}
	m.ensureState()
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.ActiveViews)
}

// IsReadViewActive 判断ReadView是否活跃
// Deprecated: 空实现
func (m *Mvcc) IsReadViewActive(view ReadView) bool {
	if m == nil {
		return false
	}
	m.ensureState()
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, active := range m.ActiveViews {
		if active.creatorTrxID == view.creatorTrxID && active.createTime.Equal(view.createTime) {
			return true
		}
	}
	return false
}

func (m *Mvcc) ensureState() {
	if m.mu == nil {
		m.mu = &sync.RWMutex{}
	}
}
