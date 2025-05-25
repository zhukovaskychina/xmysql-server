package system

import (
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrIBufFull = errors.New("insert buffer is full")
)

const (
	MaxEntriesPerIBufPage = 100 // 每页最多100个条目
)

// IBufEntryType Insert Buffer条目类型
type IBufEntryType uint8

const (
	IBufEntryInsert IBufEntryType = iota
	IBufEntryDelete
	IBufEntryUpdate
)

// IBufEntry Insert Buffer条目
type IBufEntry struct {
	Type      IBufEntryType
	SpaceID   uint32
	PageNo    uint32
	IndexID   uint64
	Data      []byte
	LSN       uint64
	Timestamp int64
}

// IBufPage Insert Buffer页面
type IBufPage struct {
	*BaseSystemPage
	entries    [MaxEntriesPerIBufPage]*IBufEntry
	entryCount uint16
	freeList   []uint16
	dirtyList  []uint16
}

// NewIBufPage 创建IBuf页面
func NewIBufPage(spaceID, pageNo uint32) *IBufPage {
	ip := &IBufPage{
		BaseSystemPage: NewBaseSystemPage(spaceID, pageNo, SystemPageTypeIBuf),
		freeList:       make([]uint16, 0),
		dirtyList:      make([]uint16, 0),
	}
	return ip
}

// AddEntry 添加条目
func (ip *IBufPage) AddEntry(entry *IBufEntry) (uint16, error) {
	ip.Lock()
	defer ip.Unlock()

	// 检查是否已满
	if ip.entryCount >= MaxEntriesPerIBufPage {
		return 0, ErrIBufFull
	}

	// 查找空闲位置
	var slot uint16
	if len(ip.freeList) > 0 {
		slot = ip.freeList[len(ip.freeList)-1]
		ip.freeList = ip.freeList[:len(ip.freeList)-1]
	} else {
		slot = ip.entryCount
	}

	// 添加条目
	ip.entries[slot] = entry
	ip.entryCount++
	ip.dirtyList = append(ip.dirtyList, slot)

	// 标记页面为脏
	ip.MarkDirty()

	return slot, nil
}

// RemoveEntry 移除条目
func (ip *IBufPage) RemoveEntry(slot uint16) error {
	ip.Lock()
	defer ip.Unlock()

	// 检查slot是否有效
	if slot >= MaxEntriesPerIBufPage || ip.entries[slot] == nil {
		return ErrInvalidSystemPage
	}

	// 清除条目
	ip.entries[slot] = nil
	ip.entryCount--
	ip.freeList = append(ip.freeList, slot)

	// 标记页面为脏
	ip.MarkDirty()

	return nil
}

// GetEntry 获取条目
func (ip *IBufPage) GetEntry(slot uint16) (*IBufEntry, error) {
	ip.RLock()
	defer ip.RUnlock()

	// 检查slot是否有效
	if slot >= MaxEntriesPerIBufPage || ip.entries[slot] == nil {
		return nil, ErrInvalidSystemPage
	}

	return ip.entries[slot], nil
}

// GetAllEntries 获取所有条目
func (ip *IBufPage) GetAllEntries() []*IBufEntry {
	ip.RLock()
	defer ip.RUnlock()

	result := make([]*IBufEntry, 0, ip.entryCount)
	for _, entry := range ip.entries {
		if entry != nil {
			result = append(result, entry)
		}
	}
	return result
}

// MergePage 合并页面内容到目标页面
func (ip *IBufPage) MergePage(targetPage []byte) error {
	ip.RLock()
	defer ip.RUnlock()

	// 按LSN顺序应用所有条目
	entries := ip.GetAllEntries()
	for _, entry := range entries {
		switch entry.Type {
		case IBufEntryInsert:
			// 插入记录
			copy(targetPage[entry.PageNo:], entry.Data)
		case IBufEntryDelete:
			// 标记记录为已删除
			targetPage[entry.PageNo] |= 0x80 // 设置删除标记位
		case IBufEntryUpdate:
			// 更新记录
			copy(targetPage[entry.PageNo:], entry.Data)
		}
	}

	return nil
}

// Read 实现Page接口
func (ip *IBufPage) Read() error {
	if err := ip.BaseSystemPage.Read(); err != nil {
		return err
	}

	// 读取IBuf页面头
	offset := uint32(87)
	ip.entryCount = binary.LittleEndian.Uint16(ip.Content[offset:])
	offset += 2

	// 读取条目
	for i := uint16(0); i < ip.entryCount; i++ {
		entry := &IBufEntry{
			Type:      IBufEntryType(ip.Content[offset]),
			SpaceID:   binary.LittleEndian.Uint32(ip.Content[offset+1:]),
			PageNo:    binary.LittleEndian.Uint32(ip.Content[offset+5:]),
			IndexID:   binary.LittleEndian.Uint64(ip.Content[offset+9:]),
			LSN:       binary.LittleEndian.Uint64(ip.Content[offset+17:]),
			Timestamp: int64(binary.LittleEndian.Uint64(ip.Content[offset+25:])),
		}

		// 读取数据长度和数据
		dataLen := binary.LittleEndian.Uint16(ip.Content[offset+33:])
		entry.Data = make([]byte, dataLen)
		copy(entry.Data, ip.Content[offset+35:offset+35+uint32(dataLen)])

		ip.entries[i] = entry
		offset += 35 + uint32(dataLen)
	}

	// 更新统计信息
	ip.stats.LastModified = time.Now().UnixNano()
	ip.stats.Reads.Add(1)

	return nil
}

// Write 实现Page接口
func (ip *IBufPage) Write() error {
	// 写入IBuf页面头
	offset := uint32(87)
	binary.LittleEndian.PutUint16(ip.Content[offset:], ip.entryCount)
	offset += 2

	// 写入条目
	for i := uint16(0); i < ip.entryCount; i++ {
		if ip.entries[i] != nil {
			entry := ip.entries[i]
			ip.Content[offset] = byte(entry.Type)
			binary.LittleEndian.PutUint32(ip.Content[offset+1:], entry.SpaceID)
			binary.LittleEndian.PutUint32(ip.Content[offset+5:], entry.PageNo)
			binary.LittleEndian.PutUint64(ip.Content[offset+9:], entry.IndexID)
			binary.LittleEndian.PutUint64(ip.Content[offset+17:], entry.LSN)
			binary.LittleEndian.PutUint64(ip.Content[offset+25:], uint64(entry.Timestamp))

			// 写入数据长度和数据
			binary.LittleEndian.PutUint16(ip.Content[offset+33:], uint16(len(entry.Data)))
			copy(ip.Content[offset+35:], entry.Data)

			offset += 35 + uint32(len(entry.Data))
		}
	}

	// 更新统计信息
	ip.stats.LastModified = time.Now().UnixNano()
	ip.stats.Writes.Add(1)

	return ip.BaseSystemPage.Write()
}

// Validate 实现SystemPage接口
func (ip *IBufPage) Validate() error {
	if err := ip.BaseSystemPage.Validate(); err != nil {
		return err
	}

	// 验证条目数量
	if ip.entryCount > MaxEntriesPerIBufPage {
		return ErrInvalidSystemPage
	}

	// 验证所有条目
	for i := uint16(0); i < ip.entryCount; i++ {
		if ip.entries[i] != nil {
			if ip.entries[i].SpaceID != ip.GetSpaceID() {
				return ErrInvalidSystemPage
			}
		}
	}

	return nil
}

// Recover 实现SystemPage接口
func (ip *IBufPage) Recover() error {
	if err := ip.BaseSystemPage.Recover(); err != nil {
		return err
	}

	// 重建空闲列表
	ip.freeList = make([]uint16, 0)
	for i := uint16(0); i < MaxEntriesPerIBufPage; i++ {
		if ip.entries[i] == nil {
			ip.freeList = append(ip.freeList, i)
		}
	}

	// 清空脏列表
	ip.dirtyList = make([]uint16, 0)

	return nil
}
