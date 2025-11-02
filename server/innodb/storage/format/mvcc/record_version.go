package mvcc

import (
	"fmt"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// RecordVersion 记录版本
// 用于MVCC实现，维护记录的版本链
// 统一了store/mvcc和wrapper/mvcc中的RecordVersion定义
type RecordVersion struct {
	// 版本号（递增）
	Version uint64

	// 创建该版本的事务ID
	TxID uint64

	// 回滚指针，指向undo log中的记录
	RollPtr uint64

	// 创建时间
	CreateTS time.Time

	// 记录键值
	Key basic.Value

	// 记录数据
	Value basic.Row

	// 删除标记
	Deleted bool

	// 下一个版本（旧版本）
	Next *RecordVersion
}

// NewRecordVersion 创建新的记录版本
func NewRecordVersion(version, txID, rollPtr uint64, key basic.Value, value basic.Row) *RecordVersion {
	return &RecordVersion{
		Version:  version,
		TxID:     txID,
		RollPtr:  rollPtr,
		CreateTS: time.Now(),
		Key:      key,
		Value:    value,
		Deleted:  false,
		Next:     nil,
	}
}

// GetVersion 获取版本号
func (rv *RecordVersion) GetVersion() uint64 {
	return rv.Version
}

// GetTxID 获取事务ID
func (rv *RecordVersion) GetTxID() uint64 {
	return rv.TxID
}

// GetRollPtr 获取回滚指针
func (rv *RecordVersion) GetRollPtr() uint64 {
	return rv.RollPtr
}

// GetCreateTime 获取创建时间
func (rv *RecordVersion) GetCreateTime() time.Time {
	return rv.CreateTS
}

// GetKey 获取键值
func (rv *RecordVersion) GetKey() basic.Value {
	return rv.Key
}

// GetValue 获取记录数据
func (rv *RecordVersion) GetValue() basic.Row {
	return rv.Value
}

// GetNext 获取下一个版本
func (rv *RecordVersion) GetNext() *RecordVersion {
	return rv.Next
}

// SetNext 设置下一个版本
func (rv *RecordVersion) SetNext(next *RecordVersion) {
	rv.Next = next
}

// IsDeleted 检查是否已删除
func (rv *RecordVersion) IsDeleted() bool {
	return rv.Deleted
}

// MarkDeleted 标记为删除
func (rv *RecordVersion) MarkDeleted() {
	rv.Deleted = true
}

// IsVisible 检查对指定ReadView是否可见
func (rv *RecordVersion) IsVisible(readView *ReadView) bool {
	// 如果记录被删除，不可见
	if rv.Deleted {
		return false
	}

	// 使用ReadView的可见性判断
	return readView.IsVisible(rv.TxID)
}

// IsVisibleToTx 检查对指定事务是否可见（简化版本）
// 用于不需要完整ReadView的场景
func (rv *RecordVersion) IsVisibleToTx(txID uint64, readTS time.Time) bool {
	// 如果记录被删除，不可见
	if rv.Deleted {
		return false
	}

	// 如果是同一个事务创建的，可见
	if rv.TxID == txID {
		return true
	}

	// 如果记录创建时间晚于读取时间，不可见
	if rv.CreateTS.After(readTS) {
		return false
	}

	return true
}

// Clone 克隆记录版本（浅拷贝）
func (rv *RecordVersion) Clone() *RecordVersion {
	return &RecordVersion{
		Version:  rv.Version,
		TxID:     rv.TxID,
		RollPtr:  rv.RollPtr,
		CreateTS: rv.CreateTS,
		Key:      rv.Key,
		Value:    rv.Value,
		Deleted:  rv.Deleted,
		Next:     rv.Next, // 注意：这里是浅拷贝
	}
}

// GetVersionChainLength 获取版本链长度
func (rv *RecordVersion) GetVersionChainLength() int {
	count := 1
	current := rv.Next
	for current != nil {
		count++
		current = current.Next
	}
	return count
}

// FindVersionByTxID 在版本链中查找指定事务的版本
func (rv *RecordVersion) FindVersionByTxID(txID uint64) *RecordVersion {
	current := rv
	for current != nil {
		if current.TxID == txID {
			return current
		}
		current = current.Next
	}
	return nil
}

// GetLatestVisibleVersion 获取对指定ReadView可见的最新版本
func (rv *RecordVersion) GetLatestVisibleVersion(readView *ReadView) *RecordVersion {
	current := rv
	for current != nil {
		if current.IsVisible(readView) {
			return current
		}
		current = current.Next
	}
	return nil
}

// GetAllVersions 获取所有版本（用于调试）
func (rv *RecordVersion) GetAllVersions() []*RecordVersion {
	versions := make([]*RecordVersion, 0)
	current := rv
	for current != nil {
		versions = append(versions, current)
		current = current.Next
	}
	return versions
}

// String 返回记录版本的字符串表示（用于调试）
func (rv *RecordVersion) String() string {
	return fmt.Sprintf("RecordVersion{Version:%d, TxID:%d, Deleted:%v, ChainLength:%d}",
		rv.Version, rv.TxID, rv.Deleted, rv.GetVersionChainLength())
}
