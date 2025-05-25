package mvcc

// TrxId 事务ID类型
type TrxId int64

// ReadView MVCC读视图
type ReadView struct {
	activeIDs    []TrxId // 创建ReadView时的活跃事务ID列表
	minTrxID     TrxId   // 活跃事务中最小的事务ID
	maxTrxID     TrxId   // 系统将分配给下一个事务的ID
	creatorTrxID TrxId   // 创建该ReadView的事务ID
}

// NewReadView 创建新的ReadView
func NewReadView(activeIDs []int64, minTrxID, maxTrxID, creatorTrxID int64) *ReadView {
	// 转换活跃事务ID列表
	ids := make([]TrxId, len(activeIDs))
	for i, id := range activeIDs {
		ids[i] = TrxId(id)
	}

	return &ReadView{
		activeIDs:    ids,
		minTrxID:     TrxId(minTrxID),
		maxTrxID:     TrxId(maxTrxID),
		creatorTrxID: TrxId(creatorTrxID),
	}
}

// IsVisible 判断给定版本是否对当前事务可见
func (rv *ReadView) IsVisible(version int64) bool {
	trxID := TrxId(version)

	// 如果版本是由当前事务创建的，则可见
	if trxID == rv.creatorTrxID {
		return true
	}

	// 如果版本大于等于下一个要分配的事务ID，则不可见
	if trxID >= rv.maxTrxID {
		return false
	}

	// 如果版本小于最小活跃事务ID，则可见
	if trxID < rv.minTrxID {
		return true
	}

	// 如果版本ID在活跃事务列表中，则不可见
	for _, activeID := range rv.activeIDs {
		if trxID == activeID {
			return false
		}
	}

	return true
}

// GetActiveIDs 获取活跃事务ID列表
func (rv *ReadView) GetActiveIDs() []TrxId {
	return rv.activeIDs
}

// GetMinTrxID 获取最小活跃事务ID
func (rv *ReadView) GetMinTrxID() TrxId {
	return rv.minTrxID
}

// GetMaxTrxID 获取下一个要分配的事务ID
func (rv *ReadView) GetMaxTrxID() TrxId {
	return rv.maxTrxID
}

// GetCreatorTrxID 获取创建该ReadView的事务ID
func (rv *ReadView) GetCreatorTrxID() TrxId {
	return rv.creatorTrxID
}
