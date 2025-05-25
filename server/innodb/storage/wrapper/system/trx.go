package system

import (
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrTrxFull = errors.New("transaction page is full")
)

const (
	MaxTrxEntriesPerPage = 100 // 每页最多100个事务条目
)

// TrxState 事务状态
type TrxState uint8

const (
	TrxStateActive TrxState = iota
	TrxStateCommitted
	TrxStateAborted
	TrxStatePrepared
)

// TrxEntry 事务条目
type TrxEntry struct {
	ID         uint64
	State      TrxState
	StartLSN   uint64
	EndLSN     uint64
	StartTime  int64
	EndTime    int64
	ThreadID   uint32
	Properties map[string]string
}

// TrxPage 事务页面
type TrxPage struct {
	*BaseSystemPage
	entries    [MaxTrxEntriesPerPage]*TrxEntry
	entryCount uint16
	freeList   []uint16
	dirtyList  []uint16
}

// NewTrxPage 创建事务页面
func NewTrxPage(spaceID, pageNo uint32) *TrxPage {
	tp := &TrxPage{
		BaseSystemPage: NewBaseSystemPage(spaceID, pageNo, SystemPageTypeTrx),
		freeList:       make([]uint16, 0),
		dirtyList:      make([]uint16, 0),
	}
	return tp
}

// AddEntry 添加条目
func (tp *TrxPage) AddEntry(entry *TrxEntry) (uint16, error) {
	tp.Lock()
	defer tp.Unlock()

	// 检查是否已满
	if tp.entryCount >= MaxTrxEntriesPerPage {
		return 0, ErrTrxFull
	}

	// 查找空闲位置
	var slot uint16
	if len(tp.freeList) > 0 {
		slot = tp.freeList[len(tp.freeList)-1]
		tp.freeList = tp.freeList[:len(tp.freeList)-1]
	} else {
		slot = tp.entryCount
	}

	// 添加条目
	tp.entries[slot] = entry
	tp.entryCount++
	tp.dirtyList = append(tp.dirtyList, slot)

	// 标记页面为脏
	tp.MarkDirty()

	return slot, nil
}

// RemoveEntry 移除条目
func (tp *TrxPage) RemoveEntry(slot uint16) error {
	tp.Lock()
	defer tp.Unlock()

	// 检查slot是否有效
	if slot >= MaxTrxEntriesPerPage || tp.entries[slot] == nil {
		return ErrInvalidSystemPage
	}

	// 清除条目
	tp.entries[slot] = nil
	tp.entryCount--
	tp.freeList = append(tp.freeList, slot)

	// 标记页面为脏
	tp.MarkDirty()

	return nil
}

// GetEntry 获取条目
func (tp *TrxPage) GetEntry(slot uint16) (*TrxEntry, error) {
	tp.RLock()
	defer tp.RUnlock()

	// 检查slot是否有效
	if slot >= MaxTrxEntriesPerPage || tp.entries[slot] == nil {
		return nil, ErrInvalidSystemPage
	}

	return tp.entries[slot], nil
}

// FindEntryByID 根据ID查找条目
func (tp *TrxPage) FindEntryByID(id uint64) (*TrxEntry, uint16, error) {
	tp.RLock()
	defer tp.RUnlock()

	for i, entry := range tp.entries {
		if entry != nil && entry.ID == id {
			return entry, uint16(i), nil
		}
	}

	return nil, 0, ErrInvalidSystemPage
}

// GetActiveTransactions 获取活跃事务
func (tp *TrxPage) GetActiveTransactions() []*TrxEntry {
	tp.RLock()
	defer tp.RUnlock()

	result := make([]*TrxEntry, 0)
	for _, entry := range tp.entries {
		if entry != nil && entry.State == TrxStateActive {
			result = append(result, entry)
		}
	}
	return result
}

// CommitTransaction 提交事务
func (tp *TrxPage) CommitTransaction(id uint64, endLSN uint64) error {
	tp.Lock()
	defer tp.Unlock()

	entry, _, err := tp.FindEntryByID(id)
	if err != nil {
		return err
	}

	entry.State = TrxStateCommitted
	entry.EndLSN = endLSN
	entry.EndTime = time.Now().UnixNano()

	// 标记页面为脏
	tp.MarkDirty()

	return nil
}

// AbortTransaction 回滚事务
func (tp *TrxPage) AbortTransaction(id uint64, endLSN uint64) error {
	tp.Lock()
	defer tp.Unlock()

	entry, _, err := tp.FindEntryByID(id)
	if err != nil {
		return err
	}

	entry.State = TrxStateAborted
	entry.EndLSN = endLSN
	entry.EndTime = time.Now().UnixNano()

	// 标记页面为脏
	tp.MarkDirty()

	return nil
}

// Read 实现Page接口
func (tp *TrxPage) Read() error {
	if err := tp.BaseSystemPage.Read(); err != nil {
		return err
	}

	// 读取Trx页面头
	offset := uint32(87)
	tp.entryCount = binary.LittleEndian.Uint16(tp.Content[offset:])
	offset += 2

	// 读取事务条目
	for i := uint16(0); i < tp.entryCount; i++ {
		entry := &TrxEntry{
			ID:         binary.LittleEndian.Uint64(tp.Content[offset:]),
			State:      TrxState(tp.Content[offset+8]),
			StartLSN:   binary.LittleEndian.Uint64(tp.Content[offset+9:]),
			EndLSN:     binary.LittleEndian.Uint64(tp.Content[offset+17:]),
			StartTime:  int64(binary.LittleEndian.Uint64(tp.Content[offset+25:])),
			EndTime:    int64(binary.LittleEndian.Uint64(tp.Content[offset+33:])),
			ThreadID:   binary.LittleEndian.Uint32(tp.Content[offset+41:]),
			Properties: make(map[string]string),
		}

		tp.entries[i] = entry
		offset += 45
	}

	// 更新统计信息
	tp.stats.LastModified = time.Now().UnixNano()
	tp.stats.Reads.Add(1)

	return nil
}

// Write 实现Page接口
func (tp *TrxPage) Write() error {
	// 写入Trx页面头
	offset := uint32(87)
	binary.LittleEndian.PutUint16(tp.Content[offset:], tp.entryCount)
	offset += 2

	// 写入事务条目
	for i := uint16(0); i < tp.entryCount; i++ {
		if tp.entries[i] != nil {
			entry := tp.entries[i]
			binary.LittleEndian.PutUint64(tp.Content[offset:], entry.ID)
			tp.Content[offset+8] = byte(entry.State)
			binary.LittleEndian.PutUint64(tp.Content[offset+9:], entry.StartLSN)
			binary.LittleEndian.PutUint64(tp.Content[offset+17:], entry.EndLSN)
			binary.LittleEndian.PutUint64(tp.Content[offset+25:], uint64(entry.StartTime))
			binary.LittleEndian.PutUint64(tp.Content[offset+33:], uint64(entry.EndTime))
			binary.LittleEndian.PutUint32(tp.Content[offset+41:], entry.ThreadID)

			offset += 45
		}
	}

	// 更新统计信息
	tp.stats.LastModified = time.Now().UnixNano()
	tp.stats.Writes.Add(1)

	return tp.BaseSystemPage.Write()
}

// Validate 实现SystemPage接口
func (tp *TrxPage) Validate() error {
	if err := tp.BaseSystemPage.Validate(); err != nil {
		return err
	}

	// 验证条目数量
	if tp.entryCount > MaxTrxEntriesPerPage {
		return ErrInvalidSystemPage
	}

	// 验证所有条目
	for i := uint16(0); i < tp.entryCount; i++ {
		if tp.entries[i] != nil {
			entry := tp.entries[i]
			if entry.StartTime > entry.EndTime && entry.EndTime != 0 {
				return ErrInvalidSystemPage
			}
			if entry.StartLSN > entry.EndLSN && entry.EndLSN != 0 {
				return ErrInvalidSystemPage
			}
		}
	}

	return nil
}

// Recover 实现SystemPage接口
func (tp *TrxPage) Recover() error {
	if err := tp.BaseSystemPage.Recover(); err != nil {
		return err
	}

	// 重建空闲列表
	tp.freeList = make([]uint16, 0)
	for i := uint16(0); i < MaxTrxEntriesPerPage; i++ {
		if tp.entries[i] == nil {
			tp.freeList = append(tp.freeList, i)
		}
	}

	// 清空脏列表
	tp.dirtyList = make([]uint16, 0)

	return nil
}
