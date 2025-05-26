package page

import (
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"sync"
)

var (
	ErrInvalidPageSize = errors.New("invalid page size")
	ErrPageNotLoaded   = errors.New("page not loaded")
)

// BasePageWrapper 基础页面包装器
type BasePageWrapper struct {
	sync.RWMutex

	// 页面信息
	id       uint32
	spaceID  uint32
	pageType common.PageType
	size     uint32
	lsn      uint64

	// 文件头尾
	header  *pages.FileHeader
	trailer *pages.FileTrailer

	// 脏页标记
	dirty bool

	content    []byte
	bufferPage *buffer_pool.BufferPage
}

// NewBasePageWrapper 创建基础页面包装器
func NewBasePageWrapper(id, spaceID uint32, typ common.PageType) *BasePageWrapper {
	header := pages.NewFileHeader()
	header.WritePageOffset(id)
	header.WritePageArch(spaceID)
	header.WritePageFileType(int16(typ))

	trailer := pages.NewFileTrailer()

	return &BasePageWrapper{
		id:       id,
		spaceID:  spaceID,
		pageType: typ,
		size:     16384, // 标准InnoDB页面大小
		header:   &header,
		trailer:  &trailer,
		content:  make([]byte, 16384),
	}
}

// 实现IPageWrapper接口

// GetPageID 获取页面ID
func (p *BasePageWrapper) GetPageID() uint32 {
	return p.id
}

// GetSpaceID 获取空间ID
func (p *BasePageWrapper) GetSpaceID() uint32 {
	return p.spaceID
}

// GetPageType 获取页面类型
func (p *BasePageWrapper) GetPageType() common.PageType {
	p.RLock()
	defer p.RUnlock()
	return p.pageType
}

// GetFileHeader 获取文件头
func (p *BasePageWrapper) GetFileHeader() *pages.FileHeader {
	return p.header
}

// GetFileTrailer 获取文件尾
func (p *BasePageWrapper) GetFileTrailer() *pages.FileTrailer {
	return p.trailer
}

// ParseFromBytes 从字节数据解析页面
func (p *BasePageWrapper) ParseFromBytes(content []byte) error {
	p.Lock()
	defer p.Unlock()

	if len(content) < int(p.size) {
		return ErrInvalidPageSize
	}

	// 解析文件头
	if err := p.header.ParseFileHeader(content[:pages.FileHeaderSize]); err != nil {
		return err
	}

	// 解析文件尾
	trailerOffset := len(content) - 8
	copy(p.trailer.FileTrailer[:], content[trailerOffset:])

	// 更新页面信息
	p.pageType = common.PageType(p.header.GetPageType())
	p.spaceID = p.header.GetFilePageArch()
	p.id = p.header.GetCurrentPageOffset()
	p.lsn = uint64(p.header.GetPageLSN())

	// 保存内容
	p.content = make([]byte, len(content))
	copy(p.content, content)

	return nil
}

// ToBytes 序列化为字节数组
func (p *BasePageWrapper) ToBytes() ([]byte, error) {
	p.RLock()
	defer p.RUnlock()

	if len(p.content) == 0 {
		return nil, ErrPageNotLoaded
	}

	// 更新文件头
	headerBytes := p.header.GetSerialBytes()
	copy(p.content[:pages.FileHeaderSize], headerBytes)

	// 更新文件尾
	trailerOffset := len(p.content) - 8
	copy(p.content[trailerOffset:], p.trailer.FileTrailer[:])

	// 返回副本
	result := make([]byte, len(p.content))
	copy(result, p.content)

	return result, nil
}

// Size 获取页面大小
func (p *BasePageWrapper) Size() uint32 {
	return p.size
}

// LSN 获取LSN
func (p *BasePageWrapper) LSN() uint64 {
	return p.lsn
}

// IsDirty 判断是否脏页
func (p *BasePageWrapper) IsDirty() bool {
	return p.dirty
}

// MarkDirty 标记为脏页
func (p *BasePageWrapper) MarkDirty() {
	p.dirty = true
}

// ClearDirty 清除脏页标记
func (p *BasePageWrapper) ClearDirty() {
	p.dirty = false
}

// UpdateChecksum 更新校验和
func (p *BasePageWrapper) UpdateChecksum() {
	checksum := p.calculateChecksum()
	p.trailer.SetChecksum(checksum)
}

// ValidateChecksum 验证校验和
func (p *BasePageWrapper) ValidateChecksum() bool {
	calculated := p.calculateChecksum()
	stored := p.trailer.GetChecksum()
	return calculated == stored
}

// calculateChecksum 计算校验和
func (p *BasePageWrapper) calculateChecksum() uint64 {
	// TODO: 实现CRC32或其他校验和算法
	// 这里使用简单的累加作为占位符
	var sum uint64
	for _, b := range p.content[:len(p.content)-8] { // 排除trailer
		sum += uint64(b)
	}
	return sum
}
