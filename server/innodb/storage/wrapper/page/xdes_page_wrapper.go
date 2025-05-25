package page

import (
	"encoding/binary"
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"sort"
	"sync"
)

var (
	ErrInvalidXDESHeader = errors.New("invalid extent descriptor header")
	ErrNoFreeExtent      = errors.New("no free extent")
	ErrExtentNotFound    = errors.New("extent not found")
)

// XDESExtentState 扩展区状态
type XDESExtentState uint8

const (
	XDESExtentFree XDESExtentState = iota
	XDESExtentFull
	XDESExtentPartial
	XDESExtentInvalid
)

// XDESExtentDesc 扩展区描述符
type XDESExtentDesc struct {
	ID       uint32
	State    XDESExtentState
	PageBits []byte // 页面位图，每个页面2位
}

// XDESPageWrapper 扩展描述符页面包装器
type XDESPageWrapper struct {
	*BasePageWrapper

	// Buffer Pool支持
	bufferPool *buffer_pool.BufferPool

	// 并发控制
	mu sync.RWMutex

	// 扩展描述符信息
	pageCount   uint32
	extentSize  uint32
	freeList    []uint32         // 空闲扩展区列表
	descriptors []XDESExtentDesc // 扩展区描述符列表
}

// NewXDESPageWrapper 创建扩展描述符页面
func NewXDESPageWrapper(id, spaceID uint32, bp *buffer_pool.BufferPool) *XDESPageWrapper {
	base := NewBasePageWrapper(id, spaceID, common.FIL_PAGE_TYPE_XDES)

	return &XDESPageWrapper{
		BasePageWrapper: base,
		bufferPool:      bp,
		pageCount:       0,
		extentSize:      64, // 默认每个extent 64个页面
		freeList:        make([]uint32, 0),
		descriptors:     make([]XDESExtentDesc, 0),
	}
}

// 实现IPageWrapper接口

// ParseFromBytes 从字节数据解析XDES页面
func (xw *XDESPageWrapper) ParseFromBytes(data []byte) error {
	xw.Lock()
	defer xw.Unlock()

	if err := xw.BasePageWrapper.ParseFromBytes(data); err != nil {
		return err
	}

	if len(data) < common.FileHeaderSize+16 { // 16字节最小头部
		return ErrInvalidXDESHeader
	}

	// 解析扩展描述符信息
	offset := common.FileHeaderSize

	// 解析页面数量
	xw.pageCount = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// 解析扩展区大小
	xw.extentSize = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// 解析空闲列表
	freeCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	xw.freeList = make([]uint32, 0, freeCount)
	for i := uint32(0); i < freeCount && offset+4 <= len(data); i++ {
		xw.freeList = append(xw.freeList, binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
	}

	// 解析描述符数量
	descCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// 解析描述符列表
	xw.descriptors = make([]XDESExtentDesc, 0, descCount)
	for i := uint32(0); i < descCount && offset+5 <= len(data); i++ {
		desc := XDESExtentDesc{}

		// 解析扩展区ID
		desc.ID = binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		// 解析状态
		desc.State = XDESExtentState(data[offset])
		offset++

		// 解析页面位图
		bitMapSize := (xw.extentSize + 3) / 4 // 每个页面2位
		if offset+int(bitMapSize) <= len(data) {
			desc.PageBits = make([]byte, bitMapSize)
			copy(desc.PageBits, data[offset:offset+int(bitMapSize)])
			offset += int(bitMapSize)
		}

		xw.descriptors = append(xw.descriptors, desc)
	}

	return nil
}

// ToBytes 序列化XDES页面为字节数组
func (xw *XDESPageWrapper) ToBytes() ([]byte, error) {
	xw.RLock()
	defer xw.RUnlock()

	// 计算需要的缓冲区大小
	bufSize := common.PageSize

	buf := make([]byte, bufSize)

	// 写入基础页面头
	base, err := xw.BasePageWrapper.ToBytes()
	if err != nil {
		return nil, err
	}
	copy(buf, base[:common.FileHeaderSize])

	// 写入扩展描述符信息
	offset := common.FileHeaderSize

	// 写入页面数量
	binary.LittleEndian.PutUint32(buf[offset:], xw.pageCount)
	offset += 4

	// 写入扩展区大小
	binary.LittleEndian.PutUint32(buf[offset:], xw.extentSize)
	offset += 4

	// 写入空闲列表
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(xw.freeList)))
	offset += 4
	for _, id := range xw.freeList {
		if offset+4 <= len(buf) {
			binary.LittleEndian.PutUint32(buf[offset:], id)
			offset += 4
		}
	}

	// 写入描述符数量
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(xw.descriptors)))
	offset += 4

	// 写入描述符列表
	for _, desc := range xw.descriptors {
		if offset+5+len(desc.PageBits) <= len(buf) {
			// 写入扩展区ID
			binary.LittleEndian.PutUint32(buf[offset:], desc.ID)
			offset += 4

			// 写入状态
			buf[offset] = byte(desc.State)
			offset++

			// 写入页面位图
			copy(buf[offset:], desc.PageBits)
			offset += len(desc.PageBits)
		}
	}

	// 更新基础包装器的内容
	if len(xw.content) != len(buf) {
		xw.content = make([]byte, len(buf))
	}
	copy(xw.content, buf)

	return buf, nil
}

// Read 实现PageWrapper接口
func (xw *XDESPageWrapper) Read() error {
	// 1. 尝试从buffer pool读取
	if xw.bufferPool != nil {
		if page, err := xw.bufferPool.GetPage(xw.GetSpaceID(), xw.GetPageID()); err == nil {
			if page != nil {
				xw.content = page.GetContent()
				return xw.ParseFromBytes(xw.content)
			}
		}
	}

	// 2. 从磁盘读取
	content, err := xw.readFromDisk()
	if err != nil {
		return err
	}

	// 3. 加入buffer pool
	if xw.bufferPool != nil {
		bufferPage := buffer_pool.NewBufferPage(xw.GetSpaceID(), xw.GetPageID())
		bufferPage.SetContent(content)
		xw.bufferPool.PutPage(bufferPage)
	}

	// 4. 解析内容
	xw.content = content
	return xw.ParseFromBytes(content)
}

// Write 实现PageWrapper接口
func (xw *XDESPageWrapper) Write() error {
	// 1. 序列化页面内容
	content, err := xw.ToBytes()
	if err != nil {
		return err
	}

	// 2. 写入buffer pool
	if xw.bufferPool != nil {
		if page, err := xw.bufferPool.GetPage(xw.GetSpaceID(), xw.GetPageID()); err == nil {
			if page != nil {
				page.SetContent(content)
				page.MarkDirty()
			}
		}
	}

	// 3. 写入磁盘
	return xw.writeToDisk(content)
}

// XDES页面特有的方法

// AllocateExtent 分配一个扩展区
func (xw *XDESPageWrapper) AllocateExtent() (*XDESExtentDesc, error) {
	xw.mu.Lock()
	defer xw.mu.Unlock()

	// 检查是否有空闲扩展区
	if len(xw.freeList) == 0 {
		return nil, ErrNoFreeExtent
	}

	// 获取一个空闲扩展区
	extentID := xw.freeList[len(xw.freeList)-1]
	xw.freeList = xw.freeList[:len(xw.freeList)-1]

	// 创建新的扩展区描述符
	desc := XDESExtentDesc{
		ID:       extentID,
		State:    XDESExtentFree,
		PageBits: make([]byte, (xw.extentSize+3)/4),
	}

	// 添加到描述符列表
	xw.descriptors = append(xw.descriptors, desc)

	xw.MarkDirty()
	return &xw.descriptors[len(xw.descriptors)-1], nil
}

// FreeExtent 释放一个扩展区
func (xw *XDESPageWrapper) FreeExtent(extentID uint32) error {
	xw.mu.Lock()
	defer xw.mu.Unlock()

	// 查找扩展区描述符
	var index int = -1
	for i, desc := range xw.descriptors {
		if desc.ID == extentID {
			index = i
			break
		}
	}

	if index == -1 {
		return ErrExtentNotFound
	}

	// 从描述符列表中移除
	xw.descriptors = append(xw.descriptors[:index], xw.descriptors[index+1:]...)

	// 添加到空闲列表
	xw.addToFreeList(extentID)
	xw.MarkDirty()

	return nil
}

// GetExtentDesc 获取扩展区描述符
func (xw *XDESPageWrapper) GetExtentDesc(extentID uint32) *XDESExtentDesc {
	xw.mu.RLock()
	defer xw.mu.RUnlock()

	// 遍历描述符列表
	for i := range xw.descriptors {
		if xw.descriptors[i].ID == extentID {
			return &xw.descriptors[i]
		}
	}

	return nil
}

// SetPageState 设置页面状态
func (xw *XDESPageWrapper) SetPageState(extentID uint32, pageNo uint32, state uint8) error {
	xw.mu.Lock()
	defer xw.mu.Unlock()

	// 查找扩展区描述符
	desc := xw.GetExtentDesc(extentID)
	if desc == nil {
		return ErrExtentNotFound
	}

	// 检查页面号是否有效
	if pageNo >= xw.pageCount {
		return errors.New("invalid page number")
	}

	// 计算页面在位图中的位置
	pageOffset := pageNo % xw.extentSize
	byteOffset := pageOffset / 4
	bitOffset := (pageOffset % 4) * 2

	// 检查状态值是否有效
	if state > 3 { // 2位能表示的最大值
		return errors.New("invalid page state")
	}

	// 更新页面状态
	if int(byteOffset) < len(desc.PageBits) {
		mask := byte(0x03 << bitOffset)
		desc.PageBits[byteOffset] &= ^mask
		desc.PageBits[byteOffset] |= (state << bitOffset)

		// 更新扩展区状态
		oldState := desc.State
		desc.State = xw.calculateExtentState(desc)

		// 如果状态发生变化，更新空闲列表
		if oldState != desc.State {
			xw.updateFreeList(desc, oldState)
		}

		xw.MarkDirty()
	}

	return nil
}

// GetPageState 获取页面状态
func (xw *XDESPageWrapper) GetPageState(extentID uint32, pageNo uint32) (uint8, error) {
	xw.mu.RLock()
	defer xw.mu.RUnlock()

	// 查找扩展区描述符
	desc := xw.GetExtentDesc(extentID)
	if desc == nil {
		return 0, ErrExtentNotFound
	}

	// 计算页面在位图中的位置
	pageOffset := pageNo % xw.extentSize
	byteOffset := pageOffset / 4
	bitOffset := (pageOffset % 4) * 2

	if int(byteOffset) >= len(desc.PageBits) {
		return 0, nil
	}

	return (desc.PageBits[byteOffset] >> bitOffset) & 3, nil
}

// GetFreeExtentCount 获取空闲扩展区数量
func (xw *XDESPageWrapper) GetFreeExtentCount() int {
	xw.mu.RLock()
	defer xw.mu.RUnlock()
	return len(xw.freeList)
}

// GetFirstFreeExtent 获取第一个空闲扩展区
func (xw *XDESPageWrapper) GetFirstFreeExtent() *XDESExtentDesc {
	xw.mu.RLock()
	defer xw.mu.RUnlock()

	if len(xw.freeList) == 0 {
		return nil
	}

	return xw.GetExtentDesc(xw.freeList[0])
}

// Validate 验证XDES页面数据完整性
func (xw *XDESPageWrapper) Validate() error {
	xw.RLock()
	defer xw.RUnlock()

	if xw.extentSize == 0 {
		return errors.New("invalid extent size: zero")
	}

	// 验证描述符的位图大小
	expectedBitmapSize := (xw.extentSize + 3) / 4
	for _, desc := range xw.descriptors {
		if len(desc.PageBits) != int(expectedBitmapSize) {
			return errors.New("bitmap size mismatch")
		}
	}

	return nil
}

// 辅助方法

// calculateExtentState 计算扩展区状态
func (xw *XDESPageWrapper) calculateExtentState(desc *XDESExtentDesc) XDESExtentState {
	if desc == nil {
		return XDESExtentFree
	}

	freeCount := uint32(0)
	totalPages := xw.extentSize

	// 统计空闲页面数
	for i := uint32(0); i < totalPages; i++ {
		byteOffset := i / 4
		bitOffset := (i % 4) * 2

		if int(byteOffset) >= len(desc.PageBits) {
			break
		}

		// 获取页面状态(2位)
		state := (desc.PageBits[byteOffset] >> bitOffset) & 0x03

		// 状态0表示空闲
		if state == 0 {
			freeCount++
		}
	}

	// 根据空闲页面比例确定状态
	if freeCount == totalPages {
		return XDESExtentFree
	} else if freeCount == 0 {
		return XDESExtentFull
	} else {
		return XDESExtentPartial
	}
}

// updateFreeList 更新空闲列表
func (xw *XDESPageWrapper) updateFreeList(desc *XDESExtentDesc, oldState XDESExtentState) {
	// 从旧状态对应的列表中移除
	switch oldState {
	case XDESExtentFree:
		xw.removeFromFreeList(desc.ID)
	}

	// 添加到新状态对应的列表中
	switch desc.State {
	case XDESExtentFree:
		xw.addToFreeList(desc.ID)
	}
}

// addToFreeList 添加到空闲列表
func (xw *XDESPageWrapper) addToFreeList(extentID uint32) {
	// 检查是否已在列表中
	for _, id := range xw.freeList {
		if id == extentID {
			return
		}
	}

	// 添加到列表
	xw.freeList = append(xw.freeList, extentID)

	// 按ID排序
	sort.Slice(xw.freeList, func(i, j int) bool {
		return xw.freeList[i] < xw.freeList[j]
	})
}

// removeFromFreeList 从空闲列表中移除
func (xw *XDESPageWrapper) removeFromFreeList(extentID uint32) {
	for i, id := range xw.freeList {
		if id == extentID {
			// 移除元素
			xw.freeList = append(xw.freeList[:i], xw.freeList[i+1:]...)
			break
		}
	}
}

// 内部方法：从磁盘读取
func (xw *XDESPageWrapper) readFromDisk() ([]byte, error) {
	// TODO: 实现从磁盘读取页面的逻辑
	// 这里需要根据实际的磁盘访问层来实现
	return make([]byte, common.PageSize), nil
}

// 内部方法：写入磁盘
func (xw *XDESPageWrapper) writeToDisk(content []byte) error {
	// TODO: 实现写入磁盘的逻辑
	// 这里需要根据实际的磁盘访问层来实现
	return nil
}
