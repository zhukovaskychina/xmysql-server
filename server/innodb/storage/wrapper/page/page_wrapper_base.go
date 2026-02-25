package page

import (
	"encoding/binary"
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"sync"
)

var (
	ErrInvalidPageSize = errors.New("invalid page size")
	ErrPageNotLoaded   = errors.New("page not loaded")
)

// BasePageWrapper 基础页面包装器
//
// Deprecated: BasePageWrapper is deprecated and will be removed in a future version.
// Use types.UnifiedPage instead, which provides:
//   - Better concurrency control with atomic operations
//   - Complete IPageWrapper interface implementation
//   - Integrated statistics and buffer pool support
//   - Full serialization/deserialization support
//
// Migration example:
//
//	// Old code:
//	page := page.NewBasePageWrapper(id, spaceID, pageType)
//
//	// New code:
//	page := types.NewUnifiedPage(spaceID, id, pageType)
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
//
// Deprecated: Use types.NewUnifiedPage instead
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

// GetFileHeader 获取文件头（返回副本，安全）
func (p *BasePageWrapper) GetFileHeader() []byte {
	p.RLock()
	defer p.RUnlock()
	result := make([]byte, pages.FileHeaderSize)
	if len(p.content) >= pages.FileHeaderSize {
		copy(result, p.content[:pages.FileHeaderSize])
	}
	return result
}

// GetFileTrailer 获取文件尾（返回副本，安全）
func (p *BasePageWrapper) GetFileTrailer() []byte {
	p.RLock()
	defer p.RUnlock()
	result := make([]byte, 8)
	if len(p.content) >= 8 {
		copy(result, p.content[len(p.content)-8:])
	}
	return result
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

// UpdateChecksum 更新页面校验和
// 计算并更新页面头部和尾部的校验和字段
func (p *BasePageWrapper) UpdateChecksum() {
	p.Lock()
	defer p.Unlock()

	if len(p.content) < pages.FileHeaderSize+8 {
		return
	}

	// 计算校验和
	checker := pages.NewPageIntegrityChecker(pages.ChecksumCRC32)
	checksum32 := checker.CalculateChecksum(p.content)

	// 更新页面头部的校验和字段（前4字节）
	binary.LittleEndian.PutUint32(p.content[0:4], checksum32)

	// 更新FileTrailer中的校验和
	p.trailer.SetChecksum(uint64(checksum32))

	// 更新content中的trailer部分（最后8字节）
	trailerBytes := p.trailer.FileTrailer[:]
	copy(p.content[len(p.content)-8:], trailerBytes)

	// 标记为脏页
	p.dirty = true
}

// ValidateChecksum 验证校验和
func (p *BasePageWrapper) ValidateChecksum() bool {
	calculated := p.calculateChecksum()
	stored := p.trailer.GetChecksum()
	return calculated == stored
}

// calculateChecksum 计算校验和
// 使用CRC32算法计算页面校验和，符合InnoDB标准
func (p *BasePageWrapper) calculateChecksum() uint64 {
	if len(p.content) < pages.FileHeaderSize+8 {
		return 0
	}

	// 使用PageIntegrityChecker计算CRC32校验和
	checker := pages.NewPageIntegrityChecker(pages.ChecksumCRC32)
	checksum32 := checker.CalculateChecksum(p.content)

	// 转换为uint64以匹配FileTrailer的接口
	return uint64(checksum32)
}

// ========================================
// 实现 types.IPageWrapper 接口的缺失方法
// ========================================

// GetPageNo 获取页面号（与 GetPageID 相同）
func (p *BasePageWrapper) GetPageNo() uint32 {
	return p.id
}

// GetLSN 获取LSN
func (p *BasePageWrapper) GetLSN() uint64 {
	p.RLock()
	defer p.RUnlock()
	return p.lsn
}

// SetLSN 设置LSN
func (p *BasePageWrapper) SetLSN(lsn uint64) {
	p.Lock()
	defer p.Unlock()
	p.lsn = lsn
	p.dirty = true
}

// GetState 获取页面状态（暂时返回默认值）
func (p *BasePageWrapper) GetState() basic.PageState {
	// TODO: 添加状态字段
	if p.dirty {
		return basic.PageStateDirty
	}
	return basic.PageStateClean
}

// SetState 设置页面状态
func (p *BasePageWrapper) SetState(state basic.PageState) {
	// TODO: 添加状态字段
	p.dirty = true
}

// Pin 固定页面
func (p *BasePageWrapper) Pin() {
	// TODO: 添加引用计数
}

// Unpin 取消固定页面
func (p *BasePageWrapper) Unpin() {
	// TODO: 添加引用计数
}

// GetPinCount 获取引用计数
func (p *BasePageWrapper) GetPinCount() int32 {
	// TODO: 添加引用计数字段
	return 0
}

// GetStats 获取页面统计信息
func (p *BasePageWrapper) GetStats() *basic.PageStats {
	// TODO: 添加统计信息字段
	return &basic.PageStats{}
}

// GetFileHeaderStruct 获取文件头结构体
func (p *BasePageWrapper) GetFileHeaderStruct() *pages.FileHeader {
	return p.header
}

// GetFileTrailerStruct 获取文件尾结构体
func (p *BasePageWrapper) GetFileTrailerStruct() *pages.FileTrailer {
	return p.trailer
}

// ToByte 序列化为字节数组（兼容旧接口）
func (p *BasePageWrapper) ToByte() []byte {
	bytes, _ := p.ToBytes()
	return bytes
}

// Read 从磁盘或缓冲池读取页面
func (p *BasePageWrapper) Read() error {
	// TODO: 实现读取逻辑
	return nil
}

// Write 将页面写入缓冲池和磁盘
func (p *BasePageWrapper) Write() error {
	p.Lock()
	defer p.Unlock()

	if p.bufferPage != nil {
		data, err := p.ToBytes()
		if err != nil {
			return err
		}
		p.bufferPage.SetContent(data)
		p.bufferPage.SetDirty(true)
	}

	p.dirty = false
	return nil
}

// Flush 强制刷新页面到磁盘
func (p *BasePageWrapper) Flush() error {
	// 先调用 Write
	if err := p.Write(); err != nil {
		return err
	}

	// TODO: 实际的刷新逻辑
	return nil
}
