package system

import (
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrDictFull = errors.New("dictionary page is full")
)

const (
	MaxDictEntriesPerPage = 50 // 每页最多50个字典条目
)

// DictEntryType 字典条目类型
type DictEntryType uint8

const (
	DictEntryTable DictEntryType = iota
	DictEntryIndex
	DictEntryColumn
	DictEntryForeign
)

// DictEntry 字典条目
type DictEntry struct {
	Type       DictEntryType
	ID         uint64
	Name       string
	SpaceID    uint32
	PageNo     uint32
	LSN        uint64
	Version    uint32
	Properties map[string]string
}

// DictPage 字典页面
type DictPage struct {
	*BaseSystemPage
	entries    [MaxDictEntriesPerPage]*DictEntry
	entryCount uint16
	freeList   []uint16
	dirtyList  []uint16
}

// NewDictPage 创建字典页面
func NewDictPage(spaceID, pageNo uint32) *DictPage {
	dp := &DictPage{
		BaseSystemPage: NewBaseSystemPage(spaceID, pageNo, SystemPageTypeDict),
		freeList:       make([]uint16, 0),
		dirtyList:      make([]uint16, 0),
	}
	return dp
}

// AddEntry 添加条目
func (dp *DictPage) AddEntry(entry *DictEntry) (uint16, error) {
	dp.Lock()
	defer dp.Unlock()

	// 检查是否已满
	if dp.entryCount >= MaxDictEntriesPerPage {
		return 0, ErrDictFull
	}

	// 查找空闲位置
	var slot uint16
	if len(dp.freeList) > 0 {
		slot = dp.freeList[len(dp.freeList)-1]
		dp.freeList = dp.freeList[:len(dp.freeList)-1]
	} else {
		slot = dp.entryCount
	}

	// 添加条目
	dp.entries[slot] = entry
	dp.entryCount++
	dp.dirtyList = append(dp.dirtyList, slot)

	// 标记页面为脏
	dp.MarkDirty()

	return slot, nil
}

// RemoveEntry 移除条目
func (dp *DictPage) RemoveEntry(slot uint16) error {
	dp.Lock()
	defer dp.Unlock()

	// 检查slot是否有效
	if slot >= MaxDictEntriesPerPage || dp.entries[slot] == nil {
		return ErrInvalidSystemPage
	}

	// 清除条目
	dp.entries[slot] = nil
	dp.entryCount--
	dp.freeList = append(dp.freeList, slot)

	// 标记页面为脏
	dp.MarkDirty()

	return nil
}

// GetEntry 获取条目
func (dp *DictPage) GetEntry(slot uint16) (*DictEntry, error) {
	dp.RLock()
	defer dp.RUnlock()

	// 检查slot是否有效
	if slot >= MaxDictEntriesPerPage || dp.entries[slot] == nil {
		return nil, ErrInvalidSystemPage
	}

	return dp.entries[slot], nil
}

// FindEntryByID 根据ID查找条目
func (dp *DictPage) FindEntryByID(id uint64) (*DictEntry, uint16, error) {
	dp.RLock()
	defer dp.RUnlock()

	for i, entry := range dp.entries {
		if entry != nil && entry.ID == id {
			return entry, uint16(i), nil
		}
	}

	return nil, 0, ErrInvalidSystemPage
}

// FindEntryByName 根据名称查找条目
func (dp *DictPage) FindEntryByName(name string) (*DictEntry, uint16, error) {
	dp.RLock()
	defer dp.RUnlock()

	for i, entry := range dp.entries {
		if entry != nil && entry.Name == name {
			return entry, uint16(i), nil
		}
	}

	return nil, 0, ErrInvalidSystemPage
}

// GetAllEntries 获取所有条目
func (dp *DictPage) GetAllEntries() []*DictEntry {
	dp.RLock()
	defer dp.RUnlock()

	result := make([]*DictEntry, 0, dp.entryCount)
	for _, entry := range dp.entries {
		if entry != nil {
			result = append(result, entry)
		}
	}
	return result
}

// Read 实现Page接口
func (dp *DictPage) Read() error {
	if err := dp.BaseSystemPage.Read(); err != nil {
		return err
	}

	// 读取Dict页面头
	offset := uint32(87)
	dp.entryCount = binary.LittleEndian.Uint16(dp.Content[offset:])
	offset += 2

	// 读取条目
	for i := uint16(0); i < dp.entryCount; i++ {
		entry := &DictEntry{
			Type:       DictEntryType(dp.Content[offset]),
			ID:         binary.LittleEndian.Uint64(dp.Content[offset+1:]),
			SpaceID:    binary.LittleEndian.Uint32(dp.Content[offset+9:]),
			PageNo:     binary.LittleEndian.Uint32(dp.Content[offset+13:]),
			LSN:        binary.LittleEndian.Uint64(dp.Content[offset+17:]),
			Version:    binary.LittleEndian.Uint32(dp.Content[offset+25:]),
			Properties: make(map[string]string),
		}

		// 读取名称长度和名称
		nameLen := binary.LittleEndian.Uint16(dp.Content[offset+29:])
		entry.Name = string(dp.Content[offset+31 : offset+31+uint32(nameLen)])

		dp.entries[i] = entry
		offset += 31 + uint32(nameLen)
	}

	// 更新统计信息
	dp.stats.LastModified = time.Now().UnixNano()
	dp.stats.Reads.Add(1)

	return nil
}

// Write 实现Page接口
func (dp *DictPage) Write() error {
	// 写入Dict页面头
	offset := uint32(87)
	binary.LittleEndian.PutUint16(dp.Content[offset:], dp.entryCount)
	offset += 2

	// 写入条目
	for i := uint16(0); i < dp.entryCount; i++ {
		if dp.entries[i] != nil {
			entry := dp.entries[i]
			dp.Content[offset] = byte(entry.Type)
			binary.LittleEndian.PutUint64(dp.Content[offset+1:], entry.ID)
			binary.LittleEndian.PutUint32(dp.Content[offset+9:], entry.SpaceID)
			binary.LittleEndian.PutUint32(dp.Content[offset+13:], entry.PageNo)
			binary.LittleEndian.PutUint64(dp.Content[offset+17:], entry.LSN)
			binary.LittleEndian.PutUint32(dp.Content[offset+25:], entry.Version)

			// 写入名称长度和名称
			binary.LittleEndian.PutUint16(dp.Content[offset+29:], uint16(len(entry.Name)))
			copy(dp.Content[offset+31:], entry.Name)

			offset += 31 + uint32(len(entry.Name))
		}
	}

	// 更新统计信息
	dp.stats.LastModified = time.Now().UnixNano()
	dp.stats.Writes.Add(1)

	return dp.BaseSystemPage.Write()
}

// Validate 实现SystemPage接口
func (dp *DictPage) Validate() error {
	if err := dp.BaseSystemPage.Validate(); err != nil {
		return err
	}

	// 验证条目数量
	if dp.entryCount > MaxDictEntriesPerPage {
		return ErrInvalidSystemPage
	}

	// 验证所有条目
	for i := uint16(0); i < dp.entryCount; i++ {
		if dp.entries[i] != nil {
			if dp.entries[i].SpaceID != dp.GetSpaceID() {
				return ErrInvalidSystemPage
			}
		}
	}

	return nil
}

// Recover 实现SystemPage接口
func (dp *DictPage) Recover() error {
	if err := dp.BaseSystemPage.Recover(); err != nil {
		return err
	}

	// 重建空闲列表
	dp.freeList = make([]uint16, 0)
	for i := uint16(0); i < MaxDictEntriesPerPage; i++ {
		if dp.entries[i] == nil {
			dp.freeList = append(dp.freeList, i)
		}
	}

	// 清空脏列表
	dp.dirtyList = make([]uint16, 0)

	return nil
}
