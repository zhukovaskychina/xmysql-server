package types

import (
	"encoding/binary"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// IRecord 记录接口
type IRecord interface {
	basic.Row // 继承Row接口

	// 记录标识
	GetID() uint64   // 获取记录ID
	SetID(id uint64) // 设置记录ID

	// 记录数据
	GetData() []byte     // 获取记录数据
	SetData(data []byte) // 设置记录数据

	// 记录属性
	GetLength() uint32 // 获取记录长度
	GetType() uint8    // 获取记录类型
	SetType(typ uint8) // 设置记录类型

	// 记录状态
	IsDeleted() bool         // 是否已删除
	SetDeleted(deleted bool) // 设置删除标记

	// MVCC相关
	GetVersion() uint64        // 获取记录版本
	SetVersion(version uint64) // 设置记录版本
	GetTxID() uint64           // 获取事务ID
	SetTxID(txID uint64)       // 设置事务ID

	// 锁相关
	IsLocked() bool         // 是否已锁定
	SetLocked(locked bool)  // 设置锁定标记
	GetLockMode() uint8     // 获取锁模式
	SetLockMode(mode uint8) // 设置锁模式
}

// RecordHeader 记录头部结构
type RecordHeader struct {
	ID       uint64 // 记录ID
	Type     uint8  // 记录类型
	Flags    uint8  // 标记位
	Length   uint32 // 记录长度
	Version  uint64 // 版本号
	TxID     uint64 // 事务ID
	LockMode uint8  // 锁模式
	nullMap  []byte
	varLens  map[byte]uint16
}

// GetDeleteFlag implements basic.FieldDataHeader
func (h RecordHeader) GetDeleteFlag() bool {
	return h.Flags&0x01 == 0x01
}

// SetDeleteFlag implements basic.FieldDataHeader
func (h RecordHeader) SetDeleteFlag(delete bool) {
	if delete {
		h.Flags |= 0x01
	} else {
		h.Flags &^= 0x01
	}
}

// GetRecMinFlag implements basic.FieldDataHeader
func (h RecordHeader) GetRecMinFlag() bool {
	return h.Flags&0x02 == 0x02
}

// SetRecMinFlag implements basic.FieldDataHeader
func (h RecordHeader) SetRecMinFlag(flag bool) {
	if flag {
		h.Flags |= 0x02
	} else {
		h.Flags &^= 0x02
	}
}

// GetNOwned implements basic.FieldDataHeader
func (h RecordHeader) GetNOwned() byte {
	return byte(h.Flags >> 2)
}

// SetNOwned implements basic.FieldDataHeader
func (h RecordHeader) SetNOwned(size byte) {
	h.Flags = (h.Flags & 0x03) | (uint8(size) << 2)
}

// GetHeapNo implements basic.FieldDataHeader
func (h RecordHeader) GetHeapNo() uint16 {
	return uint16(h.Type >> 4)
}

// SetHeapNo implements basic.FieldDataHeader
func (h RecordHeader) SetHeapNo(heapNo uint16) {
	h.Type = (h.Type & 0x0F) | uint8(heapNo<<4)
}

// GetRecordType implements basic.FieldDataHeader
func (h RecordHeader) GetRecordType() uint8 {
	return h.Type & 0x0F
}

// SetRecordType implements basic.FieldDataHeader
func (h RecordHeader) SetRecordType(recordType uint8) {
	h.Type = (h.Type & 0xF0) | (recordType & 0x0F)
}

// GetNextRecord implements basic.FieldDataHeader
func (h RecordHeader) GetNextRecord() uint16 {
	return uint16(h.Length >> 16)
}

// SetNextRecord implements basic.FieldDataHeader
func (h RecordHeader) SetNextRecord(nextRecord uint16) {
	h.Length = (h.Length & 0xFFFF) | (uint32(nextRecord) << 16)
}

// SetValueNull implements basic.FieldDataHeader
func (h *RecordHeader) SetValueNull(nullValue byte, index byte) {
	if h == nil {
		return
	}
	byteIndex := index >> 3
	bitIndex := index & 7
	for uint(len(h.nullMap)) <= uint(byteIndex) {
		h.nullMap = append(h.nullMap, 0)
	}
	mask := byte(1 << bitIndex)
	if nullValue != 0 {
		h.nullMap[byteIndex] |= mask
	} else {
		h.nullMap[byteIndex] &^= mask
	}
}

// GetRowHeaderLength implements basic.FieldDataHeader
func (h RecordHeader) GetRowHeaderLength() uint16 {
	return uint16(len(h.ToByte()))
}

// ToByte implements basic.FieldDataHeader
func (h RecordHeader) ToByte() []byte {
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint64(buf[0:8], h.ID)
	buf[8] = h.Type
	buf[9] = h.Flags
	binary.LittleEndian.PutUint32(buf[10:14], h.Length)
	binary.LittleEndian.PutUint64(buf[14:22], h.Version)
	buf[22] = byte(h.TxID & 0xFF) // Keep low byte for compatibility with fixed header size
	buf[23] = h.LockMode
	return buf
}

// SetValueLengthByIndex implements basic.FieldDataHeader
func (h *RecordHeader) SetValueLengthByIndex(realLength int, index byte) {
	if h == nil {
		return
	}
	if realLength < 0 {
		return
	}
	if h.varLens == nil {
		h.varLens = make(map[byte]uint16, 4)
	}
	if realLength > 0xFFFF {
		realLength = 0xFFFF
	}
	h.varLens[index] = uint16(realLength)
}

// GetVarValueLengthByIndex implements basic.FieldDataHeader
func (h *RecordHeader) GetVarValueLengthByIndex(index byte) int {
	if h == nil || h.varLens == nil {
		return 0
	}
	return int(h.varLens[index])
}

// GetRecordBytesRealLength implements basic.FieldDataHeader
func (h RecordHeader) GetRecordBytesRealLength() int {
	return int(h.Length & 0xFFFF)
}

// IsValueNullByIdx implements basic.FieldDataHeader
func (h RecordHeader) IsValueNullByIdx(index byte) bool {
	byteIndex := index >> 3
	bitIndex := index & 7
	if int(byteIndex) >= len(h.nullMap) {
		return false
	}
	mask := byte(1 << bitIndex)
	return h.nullMap[byteIndex]&mask != 0
}

// GetVarRealLength implements basic.FieldDataHeader
func (h *RecordHeader) GetVarRealLength(currentIndex byte) uint16 {
	if h == nil || h.varLens == nil {
		return 0
	}
	return h.varLens[currentIndex]
}

// IRecordPage 记录页面接口
type IRecordPage interface {
	IPageWrapper

	// 记录操作
	InsertRecord(record IRecord) error    // 插入记录
	DeleteRecord(id uint64) error         // 删除记录
	UpdateRecord(record IRecord) error    // 更新记录
	GetRecord(id uint64) (IRecord, error) // 获取记录
	GetRecords() []IRecord                // 获取所有记录

	// 页面属性
	GetFreeSpace() uint32   // 获取空闲空间大小
	GetRecordCount() uint32 // 获取记录数量
}
