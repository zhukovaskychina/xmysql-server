/*
Package page 提供了InnoDB存储引擎中各种页面类型的接口定义和数据结构。

该包定义了以下主要页面类型：
- DictionaryPage: 数据字典页面，用于存储表和索引的元数据
- FSPHeaderPage: 表空间头页面，管理表空间的基本信息和空闲空间
- IndexPage: 索引页面，实现B+树的索引节点和叶子节点
- INodePage: INode页面，管理段（Segment）信息
- XDESPage: 扩展描述符页面，管理区（Extent）的状态信息

每种页面类型都实现了PageWrapper基础接口，提供统一的页面操作方法。
部分页面还支持MVCC功能，用于实现事务隔离和并发控制。
*/
package page

import (
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/storage/wrapper/segment"
)

// ========================================
// 页面状态和常量定义
// ========================================

// PageStatus 页面状态枚举
type PageStatus int

const (
	PageStatusFree      PageStatus = iota // 空闲页面
	PageStatusAllocated                   // 已分配页面
	PageStatusFull                        // 已满页面
	PageStatusCorrupted                   // 损坏页面
)

// String 返回页面状态的字符串表示
func (ps PageStatus) String() string {
	switch ps {
	case PageStatusFree:
		return "FREE"
	case PageStatusAllocated:
		return "ALLOCATED"
	case PageStatusFull:
		return "FULL"
	case PageStatusCorrupted:
		return "CORRUPTED"
	default:
		return "UNKNOWN"
	}
}

// ========================================
// 核心数据结构定义
// ========================================

// ExtentDesc 区段描述符
// 描述一个区的基本信息，包括状态、所属段、页面数量等
type ExtentDesc struct {
	ID        uint32     `json:"id"`         // 区ID
	State     PageStatus `json:"state"`      // 区状态
	SegmentID uint64     `json:"segment_id"` // 所属段ID
	PageCount uint32     `json:"page_count"` // 总页面数
	FreePages uint32     `json:"free_pages"` // 空闲页面数
	Bitmap    []byte     `json:"bitmap"`     // 页面使用位图
}

// IsFull 检查区是否已满
func (ed *ExtentDesc) IsFull() bool {
	return ed.FreePages == 0
}

// IsEmpty 检查区是否为空
func (ed *ExtentDesc) IsEmpty() bool {
	return ed.FreePages == ed.PageCount
}

// GetUsageRatio 获取使用率
func (ed *ExtentDesc) GetUsageRatio() float64 {
	if ed.PageCount == 0 {
		return 0
	}
	return float64(ed.PageCount-ed.FreePages) / float64(ed.PageCount)
}

// TableDef 表定义
// 包含表的完整元数据信息
type TableDef struct {
	ID         uint64            `json:"id"`         // 表ID
	Name       string            `json:"name"`       // 表名
	Columns    []*ColumnDef      `json:"columns"`    // 列定义列表
	Indexes    []*IndexDef       `json:"indexes"`    // 索引定义列表
	Properties map[string]string `json:"properties"` // 表属性
}

// GetColumnByName 根据列名获取列定义
func (td *TableDef) GetColumnByName(name string) *ColumnDef {
	for _, col := range td.Columns {
		if col.Name == name {
			return col
		}
	}
	return nil
}

// GetIndexByName 根据索引名获取索引定义
func (td *TableDef) GetIndexByName(name string) *IndexDef {
	for _, idx := range td.Indexes {
		if idx.Name == name {
			return idx
		}
	}
	return nil
}

// GetPrimaryIndex 获取主索引
func (td *TableDef) GetPrimaryIndex() *IndexDef {
	for _, idx := range td.Indexes {
		if idx.Primary {
			return idx
		}
	}
	return nil
}

// ColumnDef 列定义
// 描述表中一个列的完整信息
type ColumnDef struct {
	ID       uint64          `json:"id"`       // 列ID
	Name     string          `json:"name"`     // 列名
	Type     basic.ValueType `json:"type"`     // 数据类型
	Nullable bool            `json:"nullable"` // 是否允许NULL
	Default  basic.Value     `json:"default"`  // 默认值
	MaxLen   uint32          `json:"max_len"`  // 最大长度（字符串类型）
	Comment  string          `json:"comment"`  // 列注释
}

// IndexDef 索引定义
// 描述表中一个索引的完整信息
type IndexDef struct {
	ID       uint64   `json:"id"`        // 索引ID
	Name     string   `json:"name"`      // 索引名
	Columns  []string `json:"columns"`   // 索引列名列表
	Unique   bool     `json:"unique"`    // 是否唯一索引
	Primary  bool     `json:"primary"`   // 是否主键索引
	RootPage uint32   `json:"root_page"` // 根页面号
	Comment  string   `json:"comment"`   // 索引注释
}

// IsComposite 检查是否为复合索引
func (id *IndexDef) IsComposite() bool {
	return len(id.Columns) > 1
}

// ========================================
// 页面接口定义
// ========================================

// DictionaryPage 数据字典页面接口
// 负责管理数据库的元数据信息，包括表定义、列定义、索引定义等
type DictionaryPage interface {
	PageWrapper

	// 表定义管理
	GetTableDef(id uint64) (*TableDef, error)
	AddTableDef(def *TableDef) error
	RemoveTableDef(id uint64) error
	ListTableDefs() ([]*TableDef, error)

	// 索引定义管理
	GetIndexDef(id uint64) (*IndexDef, error)
	AddIndexDef(def *IndexDef) error
	RemoveIndexDef(id uint64) error
	ListIndexDefs() ([]*IndexDef, error)

	// 统计信息
	GetTableCount() uint32
	GetIndexCount() uint32
}

// FSPHeaderPage 表空间头页面接口
// 管理表空间的基本信息和空闲空间分配
type FSPHeaderPage interface {
	PageWrapper

	// 空间基本信息 (GetSpaceID已在PageWrapper中定义)
	GetPageSize() uint32
	GetSpaceSize() uint64

	// 空间管理
	GetFreeSpace() uint64
	GetUsedSpace() uint64
	AllocatePages(n uint32) ([]uint32, error)
	DeallocatePages(pages []uint32) error

	// Extent管理
	GetFreeExtentCount() uint32
	GetFreeExtent() (*ExtentDesc, error)
	AllocateExtent() (*ExtentDesc, error)
	DeallocateExtent(ext *ExtentDesc) error

	// 统计信息
	GetSpaceStats() *SpaceStats
}

// SpaceStats 表空间统计信息
type SpaceStats struct {
	TotalPages    uint32  `json:"total_pages"`   // 总页面数
	FreePages     uint32  `json:"free_pages"`    // 空闲页面数
	UsedPages     uint32  `json:"used_pages"`    // 已使用页面数
	TotalExtents  uint32  `json:"total_extents"` // 总区数
	FreeExtents   uint32  `json:"free_extents"`  // 空闲区数
	UsageRatio    float64 `json:"usage_ratio"`   // 空间使用率
	Fragmentation float64 `json:"fragmentation"` // 碎片率
}

// IIndexPage 索引页面接口
// 实现B+树的索引节点和叶子节点功能，支持MVCC
type IIndexPage interface {
	PageWrapper
	MVCCPage // 支持多版本并发控制

	// 记录管理
	GetRecord(slot uint16) (basic.Row, error)
	InsertRecord(key basic.Value, value basic.Row) error
	DeleteRecord(slot uint16) error
	UpdateRecord(slot uint16, value basic.Row) error
	GetRecordCount() uint16

	// 页面导航
	GetParent() uint32
	GetLeftSibling() uint32
	GetRightSibling() uint32
	SetParent(pageNo uint32) error
	SetLeftSibling(pageNo uint32) error
	SetRightSibling(pageNo uint32) error

	// 页面分裂与合并
	Split() (IIndexPage, error)
	Merge(other IIndexPage) error
	CanMerge(other IIndexPage) bool

	// 记录遍历
	First() uint16
	Last() uint16
	Next(slot uint16) uint16
	Previous(slot uint16) uint16

	// 页面类型检查
	IsLeaf() bool
	IsRoot() bool
	GetLevel() uint16
}

// INodePage INode页面接口
// 管理段（Segment）的信息和页面分配
type INodePage interface {
	PageWrapper

	// Segment管理
	GetSegment(id uint64) (*segment.Segment, error)
	AddSegment(seg *segment.Segment) error
	RemoveSegment(id uint64) error
	ListSegments() ([]*segment.Segment, error)

	// 页面空间管理
	GetFreePages() []uint32
	GetFullPages() []uint32
	GetPartialPages() []uint32

	// 页面分配统计
	GetTotalPages() uint32
	GetUsedPages() uint32
	GetFreePageCount() uint32

	// INode链表操作
	GetNext() uint32
	GetPrev() uint32
	SetNext(pageNo uint32) error
	SetPrev(pageNo uint32) error
}

// XDESPage 扩展描述符页面接口
// 管理区（Extent）的状态信息和页面使用位图
type XDESPage interface {
	PageWrapper

	// Extent描述符管理
	GetExtentDesc(id uint32) (*ExtentDesc, error)
	UpdateExtentDesc(desc *ExtentDesc) error
	GetExtentCount() uint32

	// 页面状态管理
	GetPageStatus(pageNo uint32) (PageStatus, error)
	SetPageStatus(pageNo uint32, status PageStatus) error
	BatchSetPageStatus(pages []uint32, status PageStatus) error

	// 位图操作
	GetBitmap() []byte
	SetBitmap(bitmap []byte) error
	GetPageBit(pageNo uint32) (bool, error)
	SetPageBit(pageNo uint32, used bool) error

	// 统计信息
	GetFreePageCount() uint32
	GetUsedPageCount() uint32
	GetExtentStats() map[uint32]*ExtentDesc
}
