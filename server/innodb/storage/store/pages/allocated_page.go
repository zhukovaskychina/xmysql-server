/*
FIL_PAGE_TYPE_ALLOCATED页面详细说明

基本属性：
- 页面类型：FIL_PAGE_TYPE_ALLOCATED
- 类型编码：0x0000（十进制0）
- 所属模块：InnoDB表空间（tablespace）页管理
- 页面状态：表示页面已分配但尚未初始化为具体用途

使用场景：
1. 当InnoDB为Segment分配新页面时（从空闲extent中获取），这些页面的初始状态为：
   - 已分配状态
   - 等待被格式化
   - 只有页面头部FIL_PAGE_HEADER包含基本信息（如校验和、页号等）
   - 数据区域尚未初始化为任何具体结构

2. 页面的生命周期：
   空闲页（未分配）
     ↓ 被segment分配
   FIL_PAGE_TYPE_ALLOCATED（已分配未初始化）
     ↓ 格式化为具体用途
   具体页面类型（如INDEX、UNDO_LOG、BLOB等）

3. 常见转换场景：
   - 插入大数据时：ALLOCATED → FIL_PAGE_BLOB
   - 创建索引时：ALLOCATED → FIL_PAGE_INDEX
   - 增长聚簇索引：ALLOCATED → FIL_PAGE_INDEX

4. 应用示例：
   - 表空间增长
   - 索引扩展
   - BLOB数据存储
   - 临时表空间分配
*/

package pages

import (
	"errors"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/basic"
)

// Use basic.FIL_PAGE_TYPE_ALLOCATED for allocated page type

// 通用错误定义
var (
	ErrInvalidPageSize    = errors.New("页面大小无效")
	ErrPageAlreadyInited  = errors.New("页面已经初始化")
	ErrInvalidPageContent = errors.New("页面内容无效")
)

// AllocatedPage represents a page that has been allocated but not yet initialized
type AllocatedPage struct {
	spaceID    uint32
	pageNo     uint32
	data       []byte
	dirty      bool
	initalized bool
}

// NewAllocatedPage creates a new allocated but uninitialized page
func NewAllocatedPage(spaceID, pageNo uint32) *AllocatedPage {
	return &AllocatedPage{
		spaceID: spaceID,
		pageNo:  pageNo,
		data:    make([]byte, common.PageSize),
	}
}

// Initialize prepares the allocated page for use
func (p *AllocatedPage) Initialize() error {
	if p.initalized {
		return errors.New("page already initialized")
	}

	// Initialize page data
	for i := range p.data {
		p.data[i] = 0
	}

	p.initalized = true
	return nil
}

// GetSpaceID implements basic.IPage interface
func (p *AllocatedPage) GetSpaceID() uint32 {
	return p.spaceID
}

// GetPageNo implements basic.IPage interface
func (p *AllocatedPage) GetPageNo() uint32 {
	return p.pageNo
}

// GetPageType implements basic.IPage interface
func (p *AllocatedPage) GetPageType() basic.PageType {
	return common.FIL_PAGE_TYPE_ALLOCATED
}

// GetData implements basic.IPage interface
func (p *AllocatedPage) GetData() []byte {
	return p.data
}

// SetData implements basic.IPage interface
func (p *AllocatedPage) SetData(data []byte) error {
	if len(data) != common.PageSize {
		return errors.New("invalid page data size")
	}
	copy(p.data, data)
	return nil
}

// IsDirty implements basic.IPage interface
func (p *AllocatedPage) IsDirty() bool {
	return p.dirty
}

// SetDirty implements basic.IPage interface
func (p *AllocatedPage) SetDirty(dirty bool) {
	p.dirty = dirty
}

// IsInitialized returns whether the page has been initialized
func (p *AllocatedPage) IsInitialized() bool {
	return p.initalized
}

// GetSerializeBytes returns the serialized page data
func (p *AllocatedPage) GetSerializeBytes() []byte {
	return p.data
}

// LoadPageBody loads the page body content
func (p *AllocatedPage) LoadPageBody(content []byte) error {
	return p.SetData(content)
}

// ValidatePageContent checks if the page content is valid
func (p *AllocatedPage) ValidatePageContent() error {
	if len(p.data) != common.PageSize {
		return errors.New("invalid page size")
	}
	return nil
}

// GetPageBody returns the page body content
func (p *AllocatedPage) GetPageBody() []byte {
	return p.data
}

// SetChecksum calculates and sets the page checksum
func (p *AllocatedPage) SetChecksum() {
	// TODO: implement checksum calculation
	_ = []byte{0x00, 0x00, 0x00, 0x00} // Placeholder for checksum calculation
	// ap.FileHeader.WritePageSpaceCheckSum(checksum)
}
