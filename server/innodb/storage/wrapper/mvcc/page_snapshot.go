package mvcc

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"time"
)

// PageSnapshot 页面快照，用于MVCC读取和事务隔离
type PageSnapshot struct {
	PageID    uint32                    // 页面ID
	SpaceID   uint32                    // 表空间ID
	Version   uint64                    // 快照版本
	TxID      uint64                    // 创建快照的事务ID
	CreateTS  time.Time                 // 快照创建时间
	Records   map[uint16]*RecordVersion // 记录快照
	PageState PageSnapshotState         // 页面状态
	Metadata  map[string]interface{}    // 元数据信息
}

// PageSnapshotState 页面快照状态
type PageSnapshotState int

const (
	// SnapshotStateActive 活跃状态
	SnapshotStateActive PageSnapshotState = iota

	// SnapshotStateCommitted 已提交状态
	SnapshotStateCommitted

	// SnapshotStateAborted 已中止状态
	SnapshotStateAborted

	// SnapshotStateExpired 已过期状态
	SnapshotStateExpired
)

// String 返回快照状态的字符串表示
func (pss PageSnapshotState) String() string {
	switch pss {
	case SnapshotStateActive:
		return "ACTIVE"
	case SnapshotStateCommitted:
		return "COMMITTED"
	case SnapshotStateAborted:
		return "ABORTED"
	case SnapshotStateExpired:
		return "EXPIRED"
	default:
		return "UNKNOWN"
	}
}

// NewPageSnapshot 创建新的页面快照
func NewPageSnapshot(pageID, spaceID uint32, version, txID uint64) *PageSnapshot {
	return &PageSnapshot{
		PageID:    pageID,
		SpaceID:   spaceID,
		Version:   version,
		TxID:      txID,
		CreateTS:  time.Now(),
		Records:   make(map[uint16]*RecordVersion),
		PageState: SnapshotStateActive,
		Metadata:  make(map[string]interface{}),
	}
}

// GetPageID 获取页面ID
func (ps *PageSnapshot) GetPageID() uint32 {
	return ps.PageID
}

// GetSpaceID 获取表空间ID
func (ps *PageSnapshot) GetSpaceID() uint32 {
	return ps.SpaceID
}

// GetVersion 获取快照版本
func (ps *PageSnapshot) GetVersion() uint64 {
	return ps.Version
}

// GetTxID 获取事务ID
func (ps *PageSnapshot) GetTxID() uint64 {
	return ps.TxID
}

// GetCreateTime 获取创建时间
func (ps *PageSnapshot) GetCreateTime() time.Time {
	return ps.CreateTS
}

// GetState 获取快照状态
func (ps *PageSnapshot) GetState() PageSnapshotState {
	return ps.PageState
}

// SetState 设置快照状态
func (ps *PageSnapshot) SetState(state PageSnapshotState) {
	ps.PageState = state
}

// AddRecord 添加记录到快照
func (ps *PageSnapshot) AddRecord(slot uint16, record *RecordVersion) {
	if ps.Records == nil {
		ps.Records = make(map[uint16]*RecordVersion)
	}

	// 创建记录的深拷贝
	ps.Records[slot] = record.Clone()
}

// GetRecord 从快照中获取记录
func (ps *PageSnapshot) GetRecord(slot uint16) (*RecordVersion, bool) {
	record, exists := ps.Records[slot]
	return record, exists
}

// GetAllRecords 获取快照中的所有记录
func (ps *PageSnapshot) GetAllRecords() map[uint16]*RecordVersion {
	result := make(map[uint16]*RecordVersion)
	for slot, record := range ps.Records {
		result[slot] = record.Clone()
	}
	return result
}

// GetRecordCount 获取记录数量
func (ps *PageSnapshot) GetRecordCount() int {
	return len(ps.Records)
}

// GetVisibleRecords 获取对指定事务可见的记录
func (ps *PageSnapshot) GetVisibleRecords(txID uint64, readTS time.Time) map[uint16]*RecordVersion {
	result := make(map[uint16]*RecordVersion)

	for slot, record := range ps.Records {
		visibleRecord := record.GetLatestVisibleVersion(txID, readTS)
		if visibleRecord != nil {
			result[slot] = visibleRecord
		}
	}

	return result
}

// IsExpired 检查快照是否过期
func (ps *PageSnapshot) IsExpired(maxAge time.Duration) bool {
	return time.Since(ps.CreateTS) > maxAge
}

// SetMetadata 设置元数据
func (ps *PageSnapshot) SetMetadata(key string, value interface{}) {
	if ps.Metadata == nil {
		ps.Metadata = make(map[string]interface{})
	}
	ps.Metadata[key] = value
}

// GetMetadata 获取元数据
func (ps *PageSnapshot) GetMetadata(key string) (interface{}, bool) {
	if ps.Metadata == nil {
		return nil, false
	}
	value, exists := ps.Metadata[key]
	return value, exists
}

// Clone 创建快照的深拷贝
func (ps *PageSnapshot) Clone() *PageSnapshot {
	newSnapshot := &PageSnapshot{
		PageID:    ps.PageID,
		SpaceID:   ps.SpaceID,
		Version:   ps.Version,
		TxID:      ps.TxID,
		CreateTS:  ps.CreateTS,
		PageState: ps.PageState,
		Records:   make(map[uint16]*RecordVersion),
		Metadata:  make(map[string]interface{}),
	}

	// 深拷贝记录
	for slot, record := range ps.Records {
		newSnapshot.Records[slot] = record.Clone()
	}

	// 深拷贝元数据
	for key, value := range ps.Metadata {
		newSnapshot.Metadata[key] = value
	}

	return newSnapshot
}

// Validate 验证快照的有效性
func (ps *PageSnapshot) Validate() error {
	if ps.PageID == 0 {
		return basic.ErrInvalidPageID
	}

	if ps.Version == 0 {
		return basic.ErrInvalidVersion
	}

	if ps.Records == nil {
		return basic.ErrInvalidSnapshot
	}

	return nil
}

// GetSize 估算快照占用的内存大小（字节）
func (ps *PageSnapshot) GetSize() int {
	size := 64 // 基础结构大小估算

	// 记录大小
	for _, record := range ps.Records {
		size += 16                                   // 槽位开销
		size += record.GetVersionChainLength() * 128 // 每个版本估算128字节
	}

	// 元数据大小（简化估算）
	size += len(ps.Metadata) * 32

	return size
}

// GetAge 获取快照的年龄
func (ps *PageSnapshot) GetAge() time.Duration {
	return time.Since(ps.CreateTS)
}

// Merge 合并另一个快照的记录（用于快照合并操作）
func (ps *PageSnapshot) Merge(other *PageSnapshot) error {
	if other == nil {
		return basic.ErrInvalidSnapshot
	}

	// 只能合并同一页面的快照
	if ps.PageID != other.PageID || ps.SpaceID != other.SpaceID {
		return basic.ErrIncompatibleSnapshot
	}

	// 合并记录（以较新版本为准）
	for slot, record := range other.Records {
		if existingRecord, exists := ps.Records[slot]; !exists || record.Version > existingRecord.Version {
			ps.Records[slot] = record.Clone()
		}
	}

	// 合并元数据
	for key, value := range other.Metadata {
		ps.Metadata[key] = value
	}

	return nil
}
