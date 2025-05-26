package page

import (
	"encoding/binary"
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/segment"
	"sync"
)

var (
	ErrInodePageFull    = errors.New("inode page is full")
	ErrSegmentNotFound  = errors.New("segment not found")
	ErrInvalidSegmentID = errors.New("invalid segment ID")
)

// TODO: This file has been temporarily disabled due to missing dependencies
// The original implementation referenced packages that don't exist:
// - xmysql-server/server/innodb/storage/store/segment
// - Various undefined types like XDESEntryWrapper, Fsp, etc.
//
// This file needs to be rewritten once the proper dependencies are available

// InodePageWrapper INode页面包装器实现
type InodePageWrapper struct {
	*BasePageWrapper

	mu         sync.RWMutex
	segmentIDs map[uint64]bool // 简化为只存储段ID
	nextPage   uint32          // 指向下一个INode页面
	prevPage   uint32          // 指向前一个INode页面

	// INode页面特有的数据结构
	freePages []uint32 // 空闲页面列表
	fullPages []uint32 // 满页面列表
}

// NewInodeWrapper 创建新的INode页面包装器（重命名避免冲突）
func NewInodeWrapper(id, spaceID uint32) *InodePageWrapper {
	base := NewBasePageWrapper(id, spaceID, common.FIL_PAGE_INODE)

	return &InodePageWrapper{
		BasePageWrapper: base,
		segmentIDs:      make(map[uint64]bool),
		freePages:       make([]uint32, 0),
		fullPages:       make([]uint32, 0),
	}
}

// 实现PageWrapper接口的方法

// GetID 获取页面ID
func (ip *InodePageWrapper) GetID() uint32 {
	return ip.BasePageWrapper.GetPageID()
}

// GetPageNo 获取页面号
func (ip *InodePageWrapper) GetPageNo() uint32 {
	return ip.BasePageWrapper.GetPageID()
}

// GetContent 获取页面内容
func (ip *InodePageWrapper) GetContent() []byte {
	return ip.BasePageWrapper.content
}

// SetContent 设置页面内容
func (ip *InodePageWrapper) SetContent(data []byte) error {
	return ip.ParseFromBytes(data)
}

// GetBufferPage 获取buffer pool页面
func (ip *InodePageWrapper) GetBufferPage() *buffer_pool.BufferPage {
	return ip.BasePageWrapper.bufferPage
}

// SetBufferPage 设置buffer pool页面
func (ip *InodePageWrapper) SetBufferPage(bp *buffer_pool.BufferPage) {
	ip.BasePageWrapper.bufferPage = bp
}

// Read 从磁盘或buffer pool读取
func (ip *InodePageWrapper) Read() error {
	// TODO: 实现从磁盘或buffer pool读取逻辑
	return nil
}

// Write 写入buffer pool和磁盘
func (ip *InodePageWrapper) Write() error {
	// TODO: 实现写入buffer pool和磁盘逻辑
	ip.markDirty()
	return nil
}

// Init 初始化INode页面
func (ip *InodePageWrapper) Init() error {
	ip.mu.Lock()
	defer ip.mu.Unlock()

	// 初始化INode页面特有的数据结构
	ip.segmentIDs = make(map[uint64]bool)
	ip.freePages = make([]uint32, 0)
	ip.fullPages = make([]uint32, 0)
	ip.nextPage = 0
	ip.prevPage = 0

	return nil
}

// Release 释放INode页面资源
func (ip *InodePageWrapper) Release() error {
	ip.mu.Lock()
	defer ip.mu.Unlock()

	// 清理资源
	ip.segmentIDs = nil
	ip.freePages = nil
	ip.fullPages = nil

	return nil
}

// 实现INodePage接口的方法

// GetSegment 获取指定ID的段
func (ip *InodePageWrapper) GetSegment(id uint64) (*segment.Segment, error) {
	ip.mu.RLock()
	defer ip.mu.RUnlock()

	if !ip.segmentIDs[id] {
		return nil, ErrSegmentNotFound
	}

	// TODO: 由于segment接口不匹配问题，暂时返回nil
	// 在实际使用中，这应该从段管理器中获取正确的段实现
	return nil, errors.New("segment interface not implemented yet")
}

// AddSegment 添加段到页面
func (ip *InodePageWrapper) AddSegment(seg *segment.Segment) error {
	if seg == nil {
		return ErrInvalidSegmentID
	}

	ip.mu.Lock()
	defer ip.mu.Unlock()

	// 使用一个固定的ID作为示例，实际应该从段中获取
	segID := uint64(len(ip.segmentIDs) + 1)

	// 检查是否已存在
	if ip.segmentIDs[segID] {
		return errors.New("segment already exists")
	}

	// 检查页面是否已满（假设最多支持64个段）
	if len(ip.segmentIDs) >= 64 {
		return ErrInodePageFull
	}

	ip.segmentIDs[segID] = true
	ip.markDirty()

	return nil
}

// RemoveSegment 从页面中移除段
func (ip *InodePageWrapper) RemoveSegment(id uint64) error {
	ip.mu.Lock()
	defer ip.mu.Unlock()

	if !ip.segmentIDs[id] {
		return ErrSegmentNotFound
	}

	delete(ip.segmentIDs, id)
	ip.markDirty()

	return nil
}

// GetFreePages 获取空闲页面列表
func (ip *InodePageWrapper) GetFreePages() []uint32 {
	ip.mu.RLock()
	defer ip.mu.RUnlock()

	// 复制切片避免并发问题
	result := make([]uint32, len(ip.freePages))
	copy(result, ip.freePages)
	return result
}

// GetFullPages 获取满页面列表
func (ip *InodePageWrapper) GetFullPages() []uint32 {
	ip.mu.RLock()
	defer ip.mu.RUnlock()

	// 复制切片避免并发问题
	result := make([]uint32, len(ip.fullPages))
	copy(result, ip.fullPages)
	return result
}

// GetNext 获取下一个INode页面的页号
func (ip *InodePageWrapper) GetNext() uint32 {
	ip.mu.RLock()
	defer ip.mu.RUnlock()
	return ip.nextPage
}

// GetPrev 获取前一个INode页面的页号
func (ip *InodePageWrapper) GetPrev() uint32 {
	ip.mu.RLock()
	defer ip.mu.RUnlock()
	return ip.prevPage
}

// SetNext 设置下一个INode页面的页号
func (ip *InodePageWrapper) SetNext(pageNo uint32) {
	ip.mu.Lock()
	defer ip.mu.Unlock()
	ip.nextPage = pageNo
	ip.markDirty()
}

// SetPrev 设置前一个INode页面的页号
func (ip *InodePageWrapper) SetPrev(pageNo uint32) {
	ip.mu.Lock()
	defer ip.mu.Unlock()
	ip.prevPage = pageNo
	ip.markDirty()
}

// 其他辅助方法

// AddFreePage 添加空闲页面
func (ip *InodePageWrapper) AddFreePage(pageNo uint32) error {
	ip.mu.Lock()
	defer ip.mu.Unlock()

	// 检查是否已经存在
	for _, p := range ip.freePages {
		if p == pageNo {
			return nil // 已存在，不重复添加
		}
	}

	ip.freePages = append(ip.freePages, pageNo)
	ip.markDirty()
	return nil
}

// RemoveFreePage 移除空闲页面
func (ip *InodePageWrapper) RemoveFreePage(pageNo uint32) error {
	ip.mu.Lock()
	defer ip.mu.Unlock()

	for i, p := range ip.freePages {
		if p == pageNo {
			// 移除该页面
			ip.freePages = append(ip.freePages[:i], ip.freePages[i+1:]...)
			ip.markDirty()
			return nil
		}
	}

	return errors.New("page not found in free pages list")
}

// AddFullPage 添加满页面
func (ip *InodePageWrapper) AddFullPage(pageNo uint32) error {
	ip.mu.Lock()
	defer ip.mu.Unlock()

	// 检查是否已经存在
	for _, p := range ip.fullPages {
		if p == pageNo {
			return nil // 已存在，不重复添加
		}
	}

	ip.fullPages = append(ip.fullPages, pageNo)
	ip.markDirty()
	return nil
}

// ParseFromBytes 从字节数据解析INode页面
func (ip *InodePageWrapper) ParseFromBytes(data []byte) error {
	if err := ip.BasePageWrapper.ParseFromBytes(data); err != nil {
		return err
	}

	ip.mu.Lock()
	defer ip.mu.Unlock()

	if len(data) < pages.FileHeaderSize+16 { // 至少需要基本头信息
		return errors.New("insufficient data for inode page")
	}

	offset := pages.FileHeaderSize

	// 解析链表指针
	ip.nextPage = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	ip.prevPage = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// 解析空闲页面数量
	freePageCount := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// 解析满页面数量
	fullPageCount := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// 解析空闲页面列表
	ip.freePages = make([]uint32, freePageCount)
	for i := uint32(0); i < freePageCount && offset+4 <= len(data); i++ {
		ip.freePages[i] = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
	}

	// 解析满页面列表
	ip.fullPages = make([]uint32, fullPageCount)
	for i := uint32(0); i < fullPageCount && offset+4 <= len(data); i++ {
		ip.fullPages[i] = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
	}

	return nil
}

// ToBytes 将INode页面转换为字节数据
func (ip *InodePageWrapper) ToBytes() ([]byte, error) {
	ip.mu.RLock()
	defer ip.mu.RUnlock()

	// 计算需要的总大小
	size := 16384 // 标准页面大小

	data := make([]byte, size)

	// 写入基础页面头
	baseData, err := ip.BasePageWrapper.ToBytes()
	if err != nil {
		return nil, err
	}
	copy(data, baseData)

	offset := pages.FileHeaderSize

	// 写入链表指针
	binary.BigEndian.PutUint32(data[offset:offset+4], ip.nextPage)
	offset += 4
	binary.BigEndian.PutUint32(data[offset:offset+4], ip.prevPage)
	offset += 4

	// 写入页面列表信息
	binary.BigEndian.PutUint32(data[offset:offset+4], uint32(len(ip.freePages)))
	offset += 4
	binary.BigEndian.PutUint32(data[offset:offset+4], uint32(len(ip.fullPages)))
	offset += 4

	// 写入空闲页面列表
	for _, pageNo := range ip.freePages {
		if offset+4 <= len(data) {
			binary.BigEndian.PutUint32(data[offset:offset+4], pageNo)
			offset += 4
		}
	}

	// 写入满页面列表
	for _, pageNo := range ip.fullPages {
		if offset+4 <= len(data) {
			binary.BigEndian.PutUint32(data[offset:offset+4], pageNo)
			offset += 4
		}
	}

	return data, nil
}

// markDirty 标记页面为脏页
func (ip *InodePageWrapper) markDirty() {
	if ip.BasePageWrapper != nil {
		ip.BasePageWrapper.MarkDirty()
	}
}

// GetSegmentCount 获取当前页面中的段数量
func (ip *InodePageWrapper) GetSegmentCount() int {
	ip.mu.RLock()
	defer ip.mu.RUnlock()
	return len(ip.segmentIDs)
}

// GetSegmentIDs 获取所有段的ID列表
func (ip *InodePageWrapper) GetSegmentIDs() []uint64 {
	ip.mu.RLock()
	defer ip.mu.RUnlock()

	ids := make([]uint64, 0, len(ip.segmentIDs))
	for id := range ip.segmentIDs {
		ids = append(ids, id)
	}
	return ids
}
