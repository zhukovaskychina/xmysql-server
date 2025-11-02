package mvcc

// Deprecated: This file is deprecated and will be removed in a future version.
// ReadView functionality has been migrated to format/mvcc/read_view.go
//
// Migration guide:
// - Old: import "github.com/.../storage/store/mvcc"
//        rv := mvcc.NewReadView(activeIDs, minTrxID, maxTrxID, creatorTrxID)
//
// - New: import formatmvcc "github.com/.../storage/format/mvcc"
//        rv := formatmvcc.NewReadView(activeIDs, txID, nextTxID)
//
// Key differences:
// 1. format/mvcc uses uint64 instead of int64 for transaction IDs
// 2. format/mvcc automatically calculates lowWaterMark from activeIDs
// 3. format/mvcc provides both map-based (O(1)) and binary-search-based visibility checks
//
// This file will be removed after all references are updated.

import (
	"sort"
	"time"
)

// TrxId 事务ID类型
// Deprecated: 使用uint64代替
type TrxId int64

// ReadView MVCC读视图
// Deprecated: 使用format/mvcc.ReadView代替
type ReadView struct {
	activeIDs    []TrxId        // 创建ReadView时的活跃事务ID列表
	minTrxID     TrxId          // 活跃事务中最小的事务ID
	maxTrxID     TrxId          // 系统将分配给下一个事务的ID
	creatorTrxID TrxId          // 创建该ReadView的事务ID
	createTime   time.Time      // ReadView创建时间
	activeTxMap  map[TrxId]bool // 活跃事务的map，加速查找
}

// NewReadView 创建新的ReadView
// Deprecated: 使用format/mvcc.NewReadView(activeIDs, txID, nextTxID)代替
func NewReadView(activeIDs []int64, minTrxID, maxTrxID, creatorTrxID int64) *ReadView {
	// 转换活跃事务ID列表
	ids := make([]TrxId, len(activeIDs))
	activeTxMap := make(map[TrxId]bool)
	for i, id := range activeIDs {
		ids[i] = TrxId(id)
		activeTxMap[TrxId(id)] = true
	}

	// 对活跃事务ID排序，便于二分查找
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	return &ReadView{
		activeIDs:    ids,
		minTrxID:     TrxId(minTrxID),
		maxTrxID:     TrxId(maxTrxID),
		creatorTrxID: TrxId(creatorTrxID),
		createTime:   time.Now(),
		activeTxMap:  activeTxMap,
	}
}

// IsVisible 判断给定版本是否对当前事务可见
// 根据InnoDB的MVCC可见性规则：
// 1. 如果版本是由当前事务创建的，则可见
// 2. 如果版本的trx_id < min_trx_id，说明生成该版本的事务在ReadView创建前已提交，可见
// 3. 如果版本的trx_id >= max_trx_id，说明生成该版本的事务在ReadView创建后才开始，不可见
// 4. 如果 min_trx_id <= trx_id < max_trx_id，需要判断是否在活跃列表中
//   - 在活跃列表中，不可见（未提交）
//   - 不在活跃列表中，可见（已提交）
func (rv *ReadView) IsVisible(version int64) bool {
	trxID := TrxId(version)

	// 规刱1：如果版本是由当前事务创建的，则可见
	if trxID == rv.creatorTrxID {
		return true
	}

	// 规刱2：如果版本的trx_id < min_trx_id，说明生成该版本的事务在ReadView创建前已提交
	if trxID < rv.minTrxID {
		return true
	}

	// 规刱3：如果版本的trx_id >= max_trx_id，说明生成该版本的事务在ReadView创建后才开始
	if trxID >= rv.maxTrxID {
		return false
	}

	// 规刱4：如果 min_trx_id <= trx_id < max_trx_id，判断是否在活跃列表中
	// 使用map进行O(1)查找
	if rv.activeTxMap[trxID] {
		return false // 在活跃列表中，不可见
	}

	return true // 不在活跃列表中，可见
}

// IsVisibleFast 快速可见性判断（使用二分查找）
func (rv *ReadView) IsVisibleFast(version int64) bool {
	trxID := TrxId(version)

	if trxID == rv.creatorTrxID {
		return true
	}

	if trxID < rv.minTrxID {
		return true
	}

	if trxID >= rv.maxTrxID {
		return false
	}

	// 使用二分查找在排序的活跃列表中查找
	idx := sort.Search(len(rv.activeIDs), func(i int) bool {
		return rv.activeIDs[i] >= trxID
	})

	// 如果找到且匹配，说明在活跃列表中
	if idx < len(rv.activeIDs) && rv.activeIDs[idx] == trxID {
		return false
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

// GetCreateTime 获取ReadView创建时间
func (rv *ReadView) GetCreateTime() time.Time {
	return rv.createTime
}

// GetActiveCount 获取活跃事务数量
func (rv *ReadView) GetActiveCount() int {
	return len(rv.activeIDs)
}

// Clone 克隆ReadView
func (rv *ReadView) Clone() *ReadView {
	ids := make([]TrxId, len(rv.activeIDs))
	copy(ids, rv.activeIDs)

	activeTxMap := make(map[TrxId]bool)
	for k, v := range rv.activeTxMap {
		activeTxMap[k] = v
	}

	return &ReadView{
		activeIDs:    ids,
		minTrxID:     rv.minTrxID,
		maxTrxID:     rv.maxTrxID,
		creatorTrxID: rv.creatorTrxID,
		createTime:   rv.createTime,
		activeTxMap:  activeTxMap,
	}
}
