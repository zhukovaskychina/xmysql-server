package page

import (
	"encoding/binary"
	"errors"
	"sync"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/buffer_pool"
)

var (
	ErrInvalidBitmapHeader = errors.New("invalid insert buffer bitmap header")
)

// IBufBitmapPageWrapper Insert Buffer位图页面包装器
type IBufBitmapPageWrapper struct {
	*BasePageWrapper

	// Buffer Pool支持
	bufferPool *buffer_pool.BufferPool

	// 并发控制
	mu sync.RWMutex

	// 位图信息
	spaceID   uint32 // 表空间ID
	pageCount uint32 // 页面数量
	bitmap    []byte // 位图数据
	freeSpace []byte // 空闲空间位图
	changeMap []byte // 变更位图
}

// NewIBufBitmapPageWrapper 创建Insert Buffer位图页面
func NewIBufBitmapPageWrapper(id, spaceID uint32, bp *buffer_pool.BufferPool) *IBufBitmapPageWrapper {
	base := NewBasePageWrapper(id, spaceID, common.FIL_PAGE_IBUF_BITMAP)

	return &IBufBitmapPageWrapper{
		BasePageWrapper: base,
		bufferPool:      bp,
		spaceID:         spaceID,
		pageCount:       0,
		bitmap:          make([]byte, 0),
		freeSpace:       make([]byte, 0),
		changeMap:       make([]byte, 0),
	}
}

// 实现IPageWrapper接口

// ParseFromBytes 从字节数据解析位图页面
func (bw *IBufBitmapPageWrapper) ParseFromBytes(data []byte) error {
	bw.Lock()
	defer bw.Unlock()

	if err := bw.BasePageWrapper.ParseFromBytes(data); err != nil {
		return err
	}

	if len(data) < common.FileHeaderSize+IBUF_BITMAP_HEADER_SIZE {
		return ErrInvalidBitmapHeader
	}

	// 解析位图头
	offset := common.FileHeaderSize
	bw.spaceID = binary.LittleEndian.Uint32(data[offset:])
	bw.pageCount = binary.LittleEndian.Uint32(data[offset+4:])

	// 计算位图大小
	bitmapSize := int((bw.pageCount + 1) / 2) // 每个字节存储2个页面的位图
	if bitmapSize*2 < int(bw.pageCount) {
		bitmapSize++
	}

	// 解析位图数据
	dataOffset := offset + IBUF_BITMAP_HEADER_SIZE
	if dataOffset+bitmapSize*3 <= len(data) {
		bw.bitmap = make([]byte, bitmapSize)
		copy(bw.bitmap, data[dataOffset:dataOffset+bitmapSize])

		// 解析空闲空间位图
		freeOffset := dataOffset + bitmapSize
		bw.freeSpace = make([]byte, bitmapSize)
		copy(bw.freeSpace, data[freeOffset:freeOffset+bitmapSize])

		// 解析变更位图
		changeOffset := freeOffset + bitmapSize
		bw.changeMap = make([]byte, bitmapSize)
		copy(bw.changeMap, data[changeOffset:changeOffset+bitmapSize])
	}

	return nil
}

// ToBytes 序列化位图页面为字节数组
func (bw *IBufBitmapPageWrapper) ToBytes() ([]byte, error) {
	bw.RLock()
	defer bw.RUnlock()

	bitmapSize := len(bw.bitmap)
	totalSize := common.PageSize

	// 分配缓冲区
	content := make([]byte, totalSize)

	// 写入基础页面头
	base, err := bw.BasePageWrapper.ToBytes()
	if err != nil {
		return nil, err
	}
	copy(content, base[:common.FileHeaderSize])

	// 写入位图头
	offset := common.FileHeaderSize
	binary.LittleEndian.PutUint32(content[offset:], bw.spaceID)
	binary.LittleEndian.PutUint32(content[offset+4:], bw.pageCount)

	// 写入位图数据
	dataOffset := offset + IBUF_BITMAP_HEADER_SIZE
	if len(bw.bitmap) > 0 {
		copy(content[dataOffset:], bw.bitmap)
	}

	// 写入空闲空间位图
	if len(bw.freeSpace) > 0 {
		freeOffset := dataOffset + bitmapSize
		copy(content[freeOffset:], bw.freeSpace)
	}

	// 写入变更位图
	if len(bw.changeMap) > 0 {
		changeOffset := dataOffset + bitmapSize*2
		copy(content[changeOffset:], bw.changeMap)
	}

	// 更新基础包装器的内容
	if len(bw.content) != len(content) {
		bw.content = make([]byte, len(content))
	}
	copy(bw.content, content)

	return content, nil
}

// Read 实现PageWrapper接口
func (bw *IBufBitmapPageWrapper) Read() error {
	// 1. 尝试从buffer pool读取
	if bw.bufferPool != nil {
		if page, err := bw.bufferPool.GetPage(bw.GetSpaceID(), bw.GetPageID()); err == nil {
			if page != nil {
				bw.content = page.GetContent()
				return bw.ParseFromBytes(bw.content)
			}
		}
	}

	// 2. 从磁盘读取
	content, err := bw.readFromDisk()
	if err != nil {
		return err
	}

	// 3. 加入buffer pool
	if bw.bufferPool != nil {
		bufferPage := buffer_pool.NewBufferPage(bw.GetSpaceID(), bw.GetPageID())
		bufferPage.SetContent(content)
		bw.bufferPool.PutPage(bufferPage)
	}

	// 4. 解析内容
	bw.content = content
	return bw.ParseFromBytes(content)
}

// Write 实现PageWrapper接口
func (bw *IBufBitmapPageWrapper) Write() error {
	// 1. 序列化页面内容
	content, err := bw.ToBytes()
	if err != nil {
		return err
	}

	// 2. 写入buffer pool
	if bw.bufferPool != nil {
		if page, err := bw.bufferPool.GetPage(bw.GetSpaceID(), bw.GetPageID()); err == nil {
			if page != nil {
				page.SetContent(content)
				page.MarkDirty()
			}
		}
	}

	// 3. 写入磁盘
	return bw.writeToDisk(content)
}

// Insert Buffer位图页面特有的方法

// IsPageInBuffer 检查页面是否在Insert Buffer中
func (bw *IBufBitmapPageWrapper) IsPageInBuffer(pageNo uint32) bool {
	bw.mu.RLock()
	defer bw.mu.RUnlock()

	if pageNo >= bw.pageCount || len(bw.bitmap) == 0 {
		return false
	}

	bytePos := pageNo / 2
	bitPos := (pageNo % 2) * 4

	if int(bytePos) >= len(bw.bitmap) {
		return false
	}

	return (bw.bitmap[bytePos] & (0x0F << bitPos)) != 0
}

// SetPageInBuffer 设置页面在Insert Buffer中的状态
func (bw *IBufBitmapPageWrapper) SetPageInBuffer(pageNo uint32, inBuffer bool) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	if pageNo >= bw.pageCount || len(bw.bitmap) == 0 {
		return
	}

	bytePos := pageNo / 2
	bitPos := (pageNo % 2) * 4

	if int(bytePos) >= len(bw.bitmap) {
		return
	}

	if inBuffer {
		bw.bitmap[bytePos] |= (0x0F << bitPos)
	} else {
		bw.bitmap[bytePos] &= ^(0x0F << bitPos)
	}

	bw.MarkDirty()
}

// GetPageFreeSpace 获取页面空闲空间
func (bw *IBufBitmapPageWrapper) GetPageFreeSpace(pageNo uint32) byte {
	bw.mu.RLock()
	defer bw.mu.RUnlock()

	if pageNo >= bw.pageCount || len(bw.freeSpace) == 0 {
		return 0
	}

	bytePos := pageNo / 2
	bitPos := (pageNo % 2) * 4

	if int(bytePos) >= len(bw.freeSpace) {
		return 0
	}

	return (bw.freeSpace[bytePos] >> bitPos) & 0x0F
}

// SetPageFreeSpace 设置页面空闲空间
func (bw *IBufBitmapPageWrapper) SetPageFreeSpace(pageNo uint32, freeSpace byte) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	if pageNo >= bw.pageCount || len(bw.freeSpace) == 0 {
		return
	}

	bytePos := pageNo / 2
	bitPos := (pageNo % 2) * 4

	if int(bytePos) >= len(bw.freeSpace) {
		return
	}

	bw.freeSpace[bytePos] &= ^(0x0F << bitPos)
	bw.freeSpace[bytePos] |= ((freeSpace & 0x0F) << bitPos)

	bw.MarkDirty()
}

// IsPageChanged 检查页面是否已变更
func (bw *IBufBitmapPageWrapper) IsPageChanged(pageNo uint32) bool {
	bw.mu.RLock()
	defer bw.mu.RUnlock()

	if pageNo >= bw.pageCount || len(bw.changeMap) == 0 {
		return false
	}

	bytePos := pageNo / 2
	bitPos := (pageNo % 2) * 4

	if int(bytePos) >= len(bw.changeMap) {
		return false
	}

	return (bw.changeMap[bytePos] & (0x0F << bitPos)) != 0
}

// SetPageChanged 设置页面变更状态
func (bw *IBufBitmapPageWrapper) SetPageChanged(pageNo uint32, changed bool) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	if pageNo >= bw.pageCount || len(bw.changeMap) == 0 {
		return
	}

	bytePos := pageNo / 2
	bitPos := (pageNo % 2) * 4

	if int(bytePos) >= len(bw.changeMap) {
		return
	}

	if changed {
		bw.changeMap[bytePos] |= (0x0F << bitPos)
	} else {
		bw.changeMap[bytePos] &= ^(0x0F << bitPos)
	}

	bw.MarkDirty()
}

// InitializeBitmap 初始化位图
func (bw *IBufBitmapPageWrapper) InitializeBitmap(pageCount uint32) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	bw.pageCount = pageCount
	bitmapSize := int((pageCount + 1) / 2)
	if bitmapSize*2 < int(pageCount) {
		bitmapSize++
	}

	bw.bitmap = make([]byte, bitmapSize)
	bw.freeSpace = make([]byte, bitmapSize)
	bw.changeMap = make([]byte, bitmapSize)

	bw.MarkDirty()
}

// GetSpaceID 获取表空间ID
func (bw *IBufBitmapPageWrapper) GetSpaceID() uint32 {
	bw.mu.RLock()
	defer bw.mu.RUnlock()
	return bw.spaceID
}

// GetPageCount 获取页面数量
func (bw *IBufBitmapPageWrapper) GetPageCount() uint32 {
	bw.mu.RLock()
	defer bw.mu.RUnlock()
	return bw.pageCount
}

// Validate 验证位图页面数据完整性
func (bw *IBufBitmapPageWrapper) Validate() error {
	bw.RLock()
	defer bw.RUnlock()

	if bw.pageCount == 0 {
		return errors.New("invalid page count: zero")
	}

	expectedSize := int((bw.pageCount + 1) / 2)
	if expectedSize*2 < int(bw.pageCount) {
		expectedSize++
	}

	if len(bw.bitmap) != expectedSize {
		return errors.New("bitmap size mismatch")
	}

	return nil
}

// 内部方法：从磁盘读取
func (bw *IBufBitmapPageWrapper) readFromDisk() ([]byte, error) {
	// TODO: 实现从磁盘读取页面的逻辑
	// 这里需要根据实际的磁盘访问层来实现
	return make([]byte, common.PageSize), nil
}

// 内部方法：写入磁盘
func (bw *IBufBitmapPageWrapper) writeToDisk(content []byte) error {
	// TODO: 实现写入磁盘的逻辑
	// 这里需要根据实际的磁盘访问层来实现
	return nil
}
