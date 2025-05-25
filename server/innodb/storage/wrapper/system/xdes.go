package system

import (
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrXDESFull = errors.New("XDES page is full")
)

const (
	MaxDescriptorsPerPage = 256   // 每页最多256个区描述符
	ExtentSize            = 64    // 每个区64页
	PageSize              = 16384 // 16KB页面大小
)

// ExtentState 区状态
type ExtentState uint8

const (
	ExtentStateFree ExtentState = iota
	ExtentStatePartial
	ExtentStateFull
	ExtentStateFragmented
)

// XDESPage XDES页面
type XDESPage struct {
	*BaseSystemPage
	descriptors [MaxDescriptorsPerPage]ExtentDescriptor
	freeList    []uint16
	fullList    []uint16
	properties  map[string]string
}

// NewXDESPage 创建XDES页面
func NewXDESPage(spaceID, pageNo uint32) *XDESPage {
	xp := &XDESPage{
		BaseSystemPage: NewBaseSystemPage(spaceID, pageNo, SystemPageTypeXDES),
		freeList:       make([]uint16, 0),
		fullList:       make([]uint16, 0),
		properties:     make(map[string]string),
	}
	return xp
}

// AllocateExtent 分配区
func (xp *XDESPage) AllocateExtent() (*ExtentDescriptor, error) {
	xp.Lock()
	defer xp.Unlock()

	// 查找空闲区描述符
	if len(xp.freeList) == 0 {
		return nil, ErrXDESFull
	}

	// 获取空闲描述符
	slot := xp.freeList[len(xp.freeList)-1]
	xp.freeList = xp.freeList[:len(xp.freeList)-1]

	// 初始化区描述符
	desc := &xp.descriptors[slot]
	desc.ID = uint32(slot)
	desc.State = byte(ExtentStateFree)
	desc.PageBits = make([]byte, ExtentSize/8) // 位图，每位表示一个页面

	// 标记页面为脏
	xp.MarkDirty()

	return desc, nil
}

// FreeExtent 释放区
func (xp *XDESPage) FreeExtent(extentID uint32) error {
	xp.Lock()
	defer xp.Unlock()

	// 检查区ID是否有效
	if extentID >= MaxDescriptorsPerPage {
		return ErrInvalidSystemPage
	}

	// 重置区描述符
	desc := &xp.descriptors[extentID]
	desc.State = byte(ExtentStateFree)
	desc.PageBits = make([]byte, ExtentSize/8)

	// 添加到空闲列表
	xp.freeList = append(xp.freeList, uint16(extentID))

	// 从满列表中移除
	for i, id := range xp.fullList {
		if uint32(id) == extentID {
			xp.fullList = append(xp.fullList[:i], xp.fullList[i+1:]...)
			break
		}
	}

	// 标记页面为脏
	xp.MarkDirty()

	return nil
}

// GetExtent 获取区描述符
func (xp *XDESPage) GetExtent(extentID uint32) (*ExtentDescriptor, error) {
	xp.RLock()
	defer xp.RUnlock()

	// 检查区ID是否有效
	if extentID >= MaxDescriptorsPerPage {
		return nil, ErrInvalidSystemPage
	}

	return &xp.descriptors[extentID], nil
}

// AllocatePage 在区中分配页面
func (xp *XDESPage) AllocatePage(extentID uint32) (uint32, error) {
	xp.Lock()
	defer xp.Unlock()

	// 获取区描述符
	desc := &xp.descriptors[extentID]

	// 查找空闲页面
	for i := 0; i < ExtentSize; i++ {
		byteIndex := i / 8
		bitIndex := uint(i % 8)
		if desc.PageBits[byteIndex]&(1<<bitIndex) == 0 {
			// 标记页面为已使用
			desc.PageBits[byteIndex] |= 1 << bitIndex

			// 检查区是否已满
			full := true
			for _, b := range desc.PageBits {
				if b != 0xFF {
					full = false
					break
				}
			}

			if full {
				desc.State = byte(ExtentStateFull)
				// 添加到满列表
				xp.fullList = append(xp.fullList, uint16(extentID))
			} else {
				desc.State = byte(ExtentStatePartial)
			}

			// 标记页面为脏
			xp.MarkDirty()

			// 计算页面号
			return extentID*ExtentSize + uint32(i), nil
		}
	}

	return 0, ErrXDESFull
}

// FreePage 释放页面
func (xp *XDESPage) FreePage(extentID uint32, pageNo uint32) error {
	xp.Lock()
	defer xp.Unlock()

	// 检查页面号是否在区内
	if pageNo/ExtentSize != extentID {
		return ErrInvalidSystemPage
	}

	// 获取区描述符
	desc := &xp.descriptors[extentID]

	// 计算位图索引
	pageOffset := pageNo % ExtentSize
	byteIndex := pageOffset / 8
	bitIndex := uint(pageOffset % 8)

	// 标记页面为空闲
	desc.PageBits[byteIndex] &^= 1 << bitIndex

	// 更新区状态
	empty := true
	partial := false
	for _, b := range desc.PageBits {
		if b == 0xFF {
			empty = false
		} else if b != 0x00 {
			empty = false
			partial = true
			break
		}
	}

	if empty {
		desc.State = byte(ExtentStateFree)
	} else if partial {
		desc.State = byte(ExtentStatePartial)
	}

	// 标记页面为脏
	xp.MarkDirty()

	return nil
}

// GetFreeExtents 获取空闲区列表
func (xp *XDESPage) GetFreeExtents() []uint32 {
	xp.RLock()
	defer xp.RUnlock()

	result := make([]uint32, len(xp.freeList))
	for i, id := range xp.freeList {
		result[i] = uint32(id)
	}
	return result
}

// GetFullExtents 获取已满区列表
func (xp *XDESPage) GetFullExtents() []uint32 {
	xp.RLock()
	defer xp.RUnlock()

	result := make([]uint32, len(xp.fullList))
	for i, id := range xp.fullList {
		result[i] = uint32(id)
	}
	return result
}

// Read 实现Page接口
func (xp *XDESPage) Read() error {
	if err := xp.BaseSystemPage.Read(); err != nil {
		return err
	}

	// 读取XDES页面头
	offset := uint32(87)

	// 读取区描述符
	for i := uint32(0); i < MaxDescriptorsPerPage; i++ {
		if offset+13 > uint32(len(xp.Content)) {
			break
		}

		desc := &xp.descriptors[i]
		desc.ID = binary.LittleEndian.Uint32(xp.Content[offset:])
		desc.State = xp.Content[offset+4]
		desc.PageBits = make([]byte, 8)
		copy(desc.PageBits, xp.Content[offset+5:offset+13]) // 8字节位图
		offset += 13
	}

	// 更新统计信息
	xp.stats.LastModified = time.Now().UnixNano()
	xp.stats.Reads.Add(1)

	return nil
}

// Write 实现Page接口
func (xp *XDESPage) Write() error {
	// 写入XDES页面头
	offset := uint32(87)

	// 写入区描述符
	for i := uint32(0); i < MaxDescriptorsPerPage; i++ {
		desc := &xp.descriptors[i]
		if desc.PageBits == nil {
			break
		}
		binary.LittleEndian.PutUint32(xp.Content[offset:], desc.ID)
		xp.Content[offset+4] = desc.State
		copy(xp.Content[offset+5:offset+13], desc.PageBits)
		offset += 13
	}

	// 更新统计信息
	xp.stats.LastModified = time.Now().UnixNano()
	xp.stats.Writes.Add(1)

	return xp.BaseSystemPage.Write()
}

// Validate 实现SystemPage接口
func (xp *XDESPage) Validate() error {
	if err := xp.BaseSystemPage.Validate(); err != nil {
		return err
	}

	// 验证区描述符
	for i := 0; i < MaxDescriptorsPerPage; i++ {
		desc := &xp.descriptors[i]
		if desc.ID != uint32(i) {
			return ErrInvalidSystemPage
		}
	}

	return nil
}

// Recover 实现SystemPage接口
func (xp *XDESPage) Recover() error {
	if err := xp.BaseSystemPage.Recover(); err != nil {
		return err
	}

	// 重建空闲列表和满列表
	xp.freeList = make([]uint16, 0)
	xp.fullList = make([]uint16, 0)

	for i := 0; i < MaxDescriptorsPerPage; i++ {
		desc := &xp.descriptors[i]
		if desc.State == byte(ExtentStateFree) {
			xp.freeList = append(xp.freeList, uint16(i))
		} else if desc.State == byte(ExtentStateFull) {
			xp.fullList = append(xp.fullList, uint16(i))
		}
	}

	return nil
}
