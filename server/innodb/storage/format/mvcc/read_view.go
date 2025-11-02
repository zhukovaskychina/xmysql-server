package mvcc

import (
	"fmt"
	"sort"
	"time"
)

// ReadView MVCC读视图
// 用于实现多版本并发控制的快照隔离
// 统一了store/mvcc和wrapper/mvcc中的ReadView定义
type ReadView struct {
	// 当前事务ID
	TxID uint64

	// 创建时间
	CreateTS time.Time

	// 最小活跃事务ID（低水位线）
	LowWaterMark uint64

	// 下一个要分配的事务ID（高水位线）
	HighWaterMark uint64

	// 活跃事务ID列表（排序，用于二分查找）
	ActiveTxIDs []uint64

	// 活跃事务map（用于O(1)查找）
	ActiveTxMap map[uint64]bool
}

// NewReadView 创建新的ReadView
// activeTxIDs: 创建ReadView时的活跃事务ID列表
// txID: 创建该ReadView的事务ID
// nextTxID: 系统将分配给下一个事务的ID
func NewReadView(activeTxIDs []uint64, txID, nextTxID uint64) *ReadView {
	// 复制活跃事务ID列表
	ids := make([]uint64, len(activeTxIDs))
	copy(ids, activeTxIDs)

	// 对活跃事务ID排序，便于二分查找
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	// 创建活跃事务map，用于O(1)查找
	activeTxMap := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		activeTxMap[id] = true
	}

	// 计算水位线
	var lowWaterMark uint64
	if len(ids) > 0 {
		lowWaterMark = ids[0]
		for _, id := range ids {
			if id < lowWaterMark {
				lowWaterMark = id
			}
		}
	} else {
		lowWaterMark = nextTxID
	}

	return &ReadView{
		TxID:          txID,
		CreateTS:      time.Now(),
		LowWaterMark:  lowWaterMark,
		HighWaterMark: nextTxID,
		ActiveTxIDs:   ids,
		ActiveTxMap:   activeTxMap,
	}
}

// IsVisible 判断给定事务ID的版本是否对当前ReadView可见
// 根据InnoDB的MVCC可见性规则：
// 1. 如果版本是由当前事务创建的，则可见
// 2. 如果版本的txID < lowWaterMark，说明生成该版本的事务在ReadView创建前已提交，可见
// 3. 如果版本的txID >= highWaterMark，说明生成该版本的事务在ReadView创建后才开始，不可见
// 4. 如果 lowWaterMark <= txID < highWaterMark，需要判断是否在活跃列表中
//   - 在活跃列表中，不可见（未提交）
//   - 不在活跃列表中，可见（已提交）
func (rv *ReadView) IsVisible(txID uint64) bool {
	// 规则1：如果版本是由当前事务创建的，则可见
	if txID == rv.TxID {
		return true
	}

	// 规则2：如果版本的txID < lowWaterMark，说明生成该版本的事务在ReadView创建前已提交
	if txID < rv.LowWaterMark {
		return true
	}

	// 规则3：如果版本的txID >= highWaterMark，说明生成该版本的事务在ReadView创建后才开始
	if txID >= rv.HighWaterMark {
		return false
	}

	// 规则4：如果 lowWaterMark <= txID < highWaterMark，判断是否在活跃列表中
	// 使用map进行O(1)查找
	if rv.ActiveTxMap[txID] {
		return false // 在活跃列表中，不可见
	}

	return true // 不在活跃列表中，可见
}

// IsVisibleFast 快速可见性判断（使用二分查找）
// 当ActiveTxMap较大时，二分查找可能更快
func (rv *ReadView) IsVisibleFast(txID uint64) bool {
	// 规则1：如果版本是由当前事务创建的，则可见
	if txID == rv.TxID {
		return true
	}

	// 规则2：如果版本的txID < lowWaterMark，说明生成该版本的事务在ReadView创建前已提交
	if txID < rv.LowWaterMark {
		return true
	}

	// 规则3：如果版本的txID >= highWaterMark，说明生成该版本的事务在ReadView创建后才开始
	if txID >= rv.HighWaterMark {
		return false
	}

	// 规则4：使用二分查找在排序的活跃列表中查找
	idx := sort.Search(len(rv.ActiveTxIDs), func(i int) bool {
		return rv.ActiveTxIDs[i] >= txID
	})

	// 如果找到且匹配，说明在活跃列表中
	if idx < len(rv.ActiveTxIDs) && rv.ActiveTxIDs[idx] == txID {
		return false
	}

	return true
}

// GetTxID 获取创建该ReadView的事务ID
func (rv *ReadView) GetTxID() uint64 {
	return rv.TxID
}

// GetCreateTime 获取ReadView创建时间
func (rv *ReadView) GetCreateTime() time.Time {
	return rv.CreateTS
}

// GetLowWaterMark 获取最小活跃事务ID
func (rv *ReadView) GetLowWaterMark() uint64 {
	return rv.LowWaterMark
}

// GetHighWaterMark 获取下一个要分配的事务ID
func (rv *ReadView) GetHighWaterMark() uint64 {
	return rv.HighWaterMark
}

// GetActiveIDs 获取活跃事务ID列表
func (rv *ReadView) GetActiveIDs() []uint64 {
	return rv.ActiveTxIDs
}

// GetActiveCount 获取活跃事务数量
func (rv *ReadView) GetActiveCount() int {
	return len(rv.ActiveTxIDs)
}

// IsActive 检查指定事务ID是否在活跃列表中
func (rv *ReadView) IsActive(txID uint64) bool {
	return rv.ActiveTxMap[txID]
}

// Clone 克隆ReadView
func (rv *ReadView) Clone() *ReadView {
	ids := make([]uint64, len(rv.ActiveTxIDs))
	copy(ids, rv.ActiveTxIDs)

	activeTxMap := make(map[uint64]bool, len(rv.ActiveTxMap))
	for k, v := range rv.ActiveTxMap {
		activeTxMap[k] = v
	}

	return &ReadView{
		TxID:          rv.TxID,
		CreateTS:      rv.CreateTS,
		LowWaterMark:  rv.LowWaterMark,
		HighWaterMark: rv.HighWaterMark,
		ActiveTxIDs:   ids,
		ActiveTxMap:   activeTxMap,
	}
}

// String 返回ReadView的字符串表示（用于调试）
func (rv *ReadView) String() string {
	return fmt.Sprintf("ReadView{TxID:%d, LowWaterMark:%d, HighWaterMark:%d, ActiveCount:%d}",
		rv.TxID, rv.LowWaterMark, rv.HighWaterMark, len(rv.ActiveTxIDs))
}
