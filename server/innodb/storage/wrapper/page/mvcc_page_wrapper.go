package page

import (
	"encoding/binary"
	"sync"
)

// MVCCPage 定义MVCC页面的接口
type MVCCPage interface {
	// 版本控制
	GetVersion() uint64
	SetVersion(version uint64)

	// 事务ID管理
	GetTrxID() uint64
	SetTrxID(trxID uint64)

	// 回滚指针管理
	GetRollPtr() []byte
	SetRollPtr(ptr []byte)

	// 序列化支持
	ParseFromBytes(data []byte) error
	ToBytes() ([]byte, error)
}

// MVCCPageWrapper 提供多版本并发控制支持
type MVCCPageWrapper struct {
	mu      sync.RWMutex
	version uint64 // 当前版本号
	trxID   uint64 // 创建此版本的事务ID
	rollPtr []byte // 回滚指针，指向undo log
}

// Ensure MVCCPageWrapper implements MVCCPage
var _ MVCCPage = (*MVCCPageWrapper)(nil)

// NewMVCCPageWrapper 创建新的MVCC页面包装器
func NewMVCCPageWrapper() *MVCCPageWrapper {
	return &MVCCPageWrapper{
		version: 1,
		trxID:   0,
		rollPtr: nil,
	}
}

// GetVersion 获取当前版本号
func (m *MVCCPageWrapper) GetVersion() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.version
}

// SetVersion 设置版本号
func (m *MVCCPageWrapper) SetVersion(version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.version = version
}

// GetTrxID 获取事务ID
func (m *MVCCPageWrapper) GetTrxID() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.trxID
}

// SetTrxID 设置事务ID
func (m *MVCCPageWrapper) SetTrxID(trxID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trxID = trxID
}

// GetRollPtr 获取回滚指针
func (m *MVCCPageWrapper) GetRollPtr() []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rollPtr
}

// SetRollPtr 设置回滚指针
func (m *MVCCPageWrapper) SetRollPtr(ptr []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rollPtr = make([]byte, len(ptr))
	copy(m.rollPtr, ptr)
}

// ParseFromBytes 从字节解析MVCC信息
func (m *MVCCPageWrapper) ParseFromBytes(data []byte) error {
	if len(data) < 16 { // version(8) + trxID(8)
		return nil // 数据不足，使用默认值
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.version = binary.BigEndian.Uint64(data[0:8])
	m.trxID = binary.BigEndian.Uint64(data[8:16])

	// 解析rollback pointer
	if len(data) > 16 {
		rollPtrSize := binary.BigEndian.Uint16(data[16:18])
		if rollPtrSize > 0 && len(data) >= 18+int(rollPtrSize) {
			m.rollPtr = make([]byte, rollPtrSize)
			copy(m.rollPtr, data[18:18+rollPtrSize])
		}
	}

	return nil
}

// ToBytes 将MVCC信息转换为字节
func (m *MVCCPageWrapper) ToBytes() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	size := 16 // version(8) + trxID(8)
	rollPtrSize := 0
	if m.rollPtr != nil {
		rollPtrSize = len(m.rollPtr)
		size += 2 + rollPtrSize // 2 bytes for length
	}

	data := make([]byte, size)
	binary.BigEndian.PutUint64(data[0:8], m.version)
	binary.BigEndian.PutUint64(data[8:16], m.trxID)

	if rollPtrSize > 0 {
		binary.BigEndian.PutUint16(data[16:18], uint16(rollPtrSize))
		copy(data[18:], m.rollPtr)
	}

	return data, nil
}
