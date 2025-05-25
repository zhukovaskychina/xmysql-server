package page

import (
	"encoding/binary"
	"errors"
	"sync"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/buffer_pool"
	"xmysql-server/server/innodb/storage/store/pages"
)

var (
	ErrNoFreeSpace   = errors.New("no free space available")
	ErrInvalidExtent = errors.New("invalid extent")
)

// ExtentImpl 区段实现
type ExtentImpl struct {
	StartPage uint32
	PageCount uint32
	FreePages uint32
	Bitmap    []byte
}

// FSPPageWrapper 表空间页面包装器
type FSPPageWrapper struct {
	*BasePageWrapper

	// Buffer Pool支持
	bufferPool *buffer_pool.BufferPool

	// 并发控制
	extentLock sync.RWMutex

	// 底层FSP页面实现
	fspPage *pages.FspHrdBinaryPage

	// 空间管理
	size       uint64
	freeSpace  uint64
	pageSize   uint32
	extentSize uint32

	// Extent管理
	freeExtents []*ExtentImpl
	fullExtents []*ExtentImpl
	fragExtents []*ExtentImpl

	// 位图管理
	spaceMap []byte
}

// NewFSPPageWrapper 创建表空间页面包装器
func NewFSPPageWrapper(id uint32, spaceID uint32, bp *buffer_pool.BufferPool) *FSPPageWrapper {
	base := NewBasePageWrapper(id, spaceID, common.FIL_PAGE_FSP_HDR)
	fspPage := pages.NewFspHrdPage(spaceID)

	p := &FSPPageWrapper{
		BasePageWrapper: base,
		bufferPool:      bp,
		fspPage:         fspPage,
		freeExtents:     make([]*ExtentImpl, 0),
		fullExtents:     make([]*ExtentImpl, 0),
		fragExtents:     make([]*ExtentImpl, 0),
		spaceMap:        make([]byte, 16320),
	}

	// 初始化默认值
	p.size = uint64(16384)
	p.pageSize = uint32(16384)
	p.extentSize = 1048576

	return p
}

// 实现IPageWrapper接口

// ParseFromBytes 从字节数据解析FSP页面
func (p *FSPPageWrapper) ParseFromBytes(data []byte) error {
	p.Lock()
	defer p.Unlock()

	if err := p.BasePageWrapper.ParseFromBytes(data); err != nil {
		return err
	}

	// 解析FSP页面特有的数据
	// 简化解析，直接使用数据
	if len(data) >= common.PageSize {
		// 基于现有的fspPage结构更新数据
		// 这里可以添加更详细的解析逻辑
		p.fspPage.LoadFileHeader(data[:pages.FileHeaderSize])
	}

	// 解析FSP页面特定字段
	offset := pages.FileHeaderSize

	// 跳过各种字段，这里简化处理
	if len(data) > offset+32 {
		p.size = uint64(binary.LittleEndian.Uint32(data[offset:]))
		offset += 32 // 跳过其他字段
	}

	// 解析位图
	if len(data) > offset+16320 {
		copy(p.spaceMap, data[offset:offset+16320])
	}

	return nil
}

// ToBytes 序列化FSP页面为字节数组
func (p *FSPPageWrapper) ToBytes() ([]byte, error) {
	p.RLock()
	defer p.RUnlock()

	// 序列化FSP页面
	data := p.fspPage.GetSerializeBytes()

	// 更新基础包装器的内容
	if len(p.content) != len(data) {
		p.content = make([]byte, len(data))
	}
	copy(p.content, data)

	return data, nil
}

// Read 实现PageWrapper接口
func (p *FSPPageWrapper) Read() error {
	// 1. 尝试从buffer pool读取
	if p.bufferPool != nil {
		if page, err := p.bufferPool.GetPage(p.GetSpaceID(), p.GetPageID()); err == nil {
			if page != nil {
				p.content = page.GetContent()
				return p.ParseFromBytes(p.content)
			}
		}
	}

	// 2. 从磁盘读取
	content, err := p.readFromDisk()
	if err != nil {
		return err
	}

	// 3. 加入buffer pool
	if p.bufferPool != nil {
		bufferPage := buffer_pool.NewBufferPage(p.GetSpaceID(), p.GetPageID())
		bufferPage.SetContent(content)
		p.bufferPool.PutPage(bufferPage)
	}

	// 4. 解析内容
	p.content = content
	return p.ParseFromBytes(content)
}

// Write 实现PageWrapper接口
func (p *FSPPageWrapper) Write() error {
	// 1. 序列化页面内容
	content, err := p.ToBytes()
	if err != nil {
		return err
	}

	// 2. 写入buffer pool
	if p.bufferPool != nil {
		if page, err := p.bufferPool.GetPage(p.GetSpaceID(), p.GetPageID()); err == nil {
			if page != nil {
				page.SetContent(content)
				page.MarkDirty()
			}
		}
	}

	// 3. 写入磁盘
	return p.writeToDisk(content)
}

// FSP页面特有的方法

// GetFreeSpace 获取空闲空间
func (p *FSPPageWrapper) GetFreeSpace() uint64 {
	p.extentLock.RLock()
	defer p.extentLock.RUnlock()
	return p.freeSpace
}

// AllocatePages 分配页面
func (p *FSPPageWrapper) AllocatePages(n uint32) ([]uint32, error) {
	p.extentLock.Lock()
	defer p.extentLock.Unlock()

	var pages []uint32

	// 先从碎片区段分配
	for _, ext := range p.fragExtents {
		if ext.FreePages > 0 {
			allocated := p.allocatePagesFromExtent(ext, n)
			pages = append(pages, allocated...)
			n -= uint32(len(allocated))
			if n == 0 {
				return pages, nil
			}
		}
	}

	// 需要分配新的区段
	for n > 0 {
		if len(p.freeExtents) == 0 {
			return nil, ErrNoFreeSpace
		}

		// 获取空闲区段
		ext := p.freeExtents[0]
		p.freeExtents = p.freeExtents[1:]

		// 分配页面
		allocated := p.allocatePagesFromExtent(ext, n)
		pages = append(pages, allocated...)
		n -= uint32(len(allocated))

		// 更新区段状态
		if ext.FreePages == 0 {
			p.fullExtents = append(p.fullExtents, ext)
		} else {
			p.fragExtents = append(p.fragExtents, ext)
		}
	}

	p.MarkDirty()
	return pages, nil
}

// GetFreeExtent 获取空闲区段
func (p *FSPPageWrapper) GetFreeExtent() (*ExtentDesc, error) {
	p.extentLock.RLock()
	defer p.extentLock.RUnlock()

	if len(p.freeExtents) == 0 {
		return nil, ErrNoFreeSpace
	}

	ext := p.freeExtents[0]
	desc := &ExtentDesc{
		ID:        0, // 简化处理
		State:     PageStatusFree,
		SegmentID: 0,
		PageCount: ext.PageCount,
		FreePages: ext.FreePages,
		Bitmap:    ext.Bitmap,
	}

	return desc, nil
}

// AllocateExtent 分配区段
func (p *FSPPageWrapper) AllocateExtent() (*ExtentDesc, error) {
	p.extentLock.Lock()
	defer p.extentLock.Unlock()

	if len(p.freeExtents) == 0 {
		return nil, ErrNoFreeSpace
	}

	// 获取空闲区段
	ext := p.freeExtents[0]
	p.freeExtents = p.freeExtents[1:]

	// 创建区段描述符
	desc := &ExtentDesc{
		ID:        0, // 简化处理
		State:     PageStatusAllocated,
		SegmentID: 0,
		PageCount: ext.PageCount,
		FreePages: ext.FreePages,
		Bitmap:    ext.Bitmap,
	}

	// 加入已分配列表
	p.fragExtents = append(p.fragExtents, ext)

	p.MarkDirty()
	return desc, nil
}

// DeallocateExtent 释放区段
func (p *FSPPageWrapper) DeallocateExtent(desc *ExtentDesc) error {
	p.extentLock.Lock()
	defer p.extentLock.Unlock()

	// 简化实现：创建空闲区段
	ext := &ExtentImpl{
		StartPage: 0, // 需要根据实际情况设置
		PageCount: desc.PageCount,
		FreePages: desc.PageCount,
		Bitmap:    make([]byte, (desc.PageCount+7)/8),
	}

	// 加入空闲列表
	p.freeExtents = append(p.freeExtents, ext)

	p.MarkDirty()
	return nil
}

// Validate 验证FSP页面数据完整性
func (p *FSPPageWrapper) Validate() error {
	p.RLock()
	defer p.RUnlock()

	if p.fspPage != nil {
		// 验证基本数据
		if p.size == 0 {
			return errors.New("invalid FSP page: zero size")
		}
	}

	return nil
}

// GetFSPPage 获取底层的FSP页面实现
func (p *FSPPageWrapper) GetFSPPage() *pages.FspHrdBinaryPage {
	return p.fspPage
}

// 辅助方法
func (p *FSPPageWrapper) allocatePagesFromExtent(ext *ExtentImpl, n uint32) []uint32 {
	var pages []uint32
	allocated := uint32(0)

	for i := uint32(0); i < ext.PageCount && allocated < n; i++ {
		byteOffset := i / 8
		bitOffset := i % 8

		// 检查页面是否空闲
		if ext.Bitmap[byteOffset]&(1<<bitOffset) == 0 {
			// 标记为已使用
			ext.Bitmap[byteOffset] |= (1 << bitOffset)
			pages = append(pages, ext.StartPage+i)
			allocated++
			ext.FreePages--
		}
	}

	return pages
}

// 内部方法：从磁盘读取
func (p *FSPPageWrapper) readFromDisk() ([]byte, error) {
	// TODO: 实现从磁盘读取页面的逻辑
	// 这里需要根据实际的磁盘访问层来实现
	return make([]byte, common.PageSize), nil
}

// 内部方法：写入磁盘
func (p *FSPPageWrapper) writeToDisk(content []byte) error {
	// TODO: 实现写入磁盘的逻辑
	// 这里需要根据实际的磁盘访问层来实现
	return nil
}
