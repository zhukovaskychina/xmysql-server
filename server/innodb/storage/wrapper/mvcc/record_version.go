package mvcc

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"time"
)

// RecordVersion 记录版本，用于MVCC实现
type RecordVersion struct {
	Version  uint64         // 版本号
	TxID     uint64         // 事务ID
	CreateTS time.Time      // 创建时间戳
	Key      basic.Value    // 记录键值
	Value    basic.Row      // 记录数据
	Next     *RecordVersion // 指向下一个版本(形成版本链)
	Deleted  bool           // 删除标记
}

// NewRecordVersion 创建新的记录版本
func NewRecordVersion(version, txID uint64, key basic.Value, value basic.Row) *RecordVersion {
	return &RecordVersion{
		Version:  version,
		TxID:     txID,
		CreateTS: time.Now(),
		Key:      key,
		Value:    value,
		Next:     nil,
		Deleted:  false,
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

// IsVisible 检查对指定事务是否可见
func (rv *RecordVersion) IsVisible(txID uint64, readTS time.Time) bool {
	// 简化的可见性检查逻辑
	// 在实际实现中需要考虑事务隔离级别、快照读等复杂情况

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

// Clone 克隆记录版本
func (rv *RecordVersion) Clone() *RecordVersion {
	return &RecordVersion{
		Version:  rv.Version,
		TxID:     rv.TxID,
		CreateTS: rv.CreateTS,
		Key:      rv.Key,
		Value:    rv.Value,
		Next:     rv.Next, // 注意：这里是浅拷贝
		Deleted:  rv.Deleted,
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

// GetLatestVisibleVersion 获取对指定事务可见的最新版本
func (rv *RecordVersion) GetLatestVisibleVersion(txID uint64, readTS time.Time) *RecordVersion {
	current := rv
	for current != nil {
		if current.IsVisible(txID, readTS) {
			return current
		}
		current = current.Next
	}
	return nil
}
