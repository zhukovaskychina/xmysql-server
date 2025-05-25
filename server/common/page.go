package common

// Page size constants
const (
	PageSize            = 16384 // Default page size
	FileHeaderSize      = 38    // File header size
	PageHeaderSize      = 56    // Page header size
	InfimumSupremumSize = 26    // Size of infimum/supremum records
	FileTrailerSize     = 8     // File trailer size
)

type PageState int

type PageType uint16

// Page states
const (
	PageStateInit PageState = iota
	PageStateLoaded
	PageStateModified
	PageStateDirty
	PageStateClean
	PageStatePinned
	PageStateFlushed
)

// Page types defines the different types of pages in InnoDB
const (
	// FIL_PAGE_INDEX (0x0000) - B+Tree 索引页，用于存储记录和索引结构的主要页面
	// 可以是聚簇索引或二级索引的一部分
	FIL_PAGE_INDEX PageType = 0x0000

	// FIL_PAGE_UNDO_LOG (0x0002) - Undo日志页，存储回滚段的undo记录
	// 用于事务回滚和MVCC实现
	FIL_PAGE_UNDO_LOG PageType = 0x0002

	// FIL_PAGE_INODE (0x0003) - 段inode页面，管理段(extent)的元数据
	// 包含段的空间管理信息
	FIL_PAGE_INODE PageType = 0x0003

	// FIL_PAGE_IBUF_FREE_LIST (0x0004) - 插入缓冲空闲列表页
	// 已废弃，保留用于历史兼容
	FIL_PAGE_IBUF_FREE_LIST PageType = 0x0004

	// FIL_PAGE_IBUF_BITMAP (0x0005) - 插入缓冲位图页
	// 记录插入缓冲的状态信息
	FIL_PAGE_IBUF_BITMAP PageType = 0x0005

	// FIL_PAGE_TYPE_SYS (0x0006) - 系统页，存储系统信息
	// 如表空间ID等系统级元数据
	FIL_PAGE_TYPE_SYS PageType = 0x0006

	// FIL_PAGE_TYPE_TRX_SYS (0x0007) - 事务系统页
	// 管理事务信息、回滚段等事务相关数据
	FIL_PAGE_TYPE_TRX_SYS PageType = 0x0007

	// FIL_PAGE_TYPE_FSP_HDR (0x0008) - 表空间头页
	// 存储表空间元信息，如空闲页列表等
	FIL_PAGE_TYPE_FSP_HDR PageType = 0x0008

	// FIL_PAGE_TYPE_XDES (0x0009) - 扩展描述符页
	// 管理extent的使用状态和分配信息
	FIL_PAGE_TYPE_XDES PageType = 0x0009

	// FIL_PAGE_TYPE_BLOB (0x000A) - 外部存储页
	// 存储BLOB/TEXT等大字段的实际内容
	FIL_PAGE_TYPE_BLOB PageType = 0x000A

	// FIL_PAGE_TYPE_COMPRESSED (0x000B) - 压缩页
	// 支持页面压缩功能
	FIL_PAGE_TYPE_COMPRESSED PageType = 0x000B

	// FIL_PAGE_TYPE_ENCRYPTED (0x000C) - 加密页
	// 支持数据加密功能
	FIL_PAGE_TYPE_ENCRYPTED PageType = 0x000C

	// FIL_PAGE_TYPE_COMPRESSED_AND_ENCRYPTED (0x000D) - 压缩且加密的页
	// 同时启用压缩和加密的页面
	FIL_PAGE_TYPE_COMPRESSED_AND_ENCRYPTED PageType = 0x000D

	// FIL_PAGE_TYPE_ENCRYPTED_RTREE (0x000E) - R-Tree加密页
	// 用于空间索引的加密页面
	FIL_PAGE_TYPE_ENCRYPTED_RTREE PageType = 0x000E

	// FIL_PAGE_TYPE_ALLOCATED (0x000F) - 已分配页
	// 标记已分配但未使用的页面
	FIL_PAGE_TYPE_ALLOCATED PageType = 0x000F
)

// Aliases for backward compatibility
const (
	FIL_PAGE_FSP_HDR         = FIL_PAGE_TYPE_FSP_HDR
	FIL_PAGE_COMPRESSED      = FIL_PAGE_TYPE_COMPRESSED
	FIL_PAGE_ENCRYPTED       = FIL_PAGE_TYPE_ENCRYPTED
	FILE_PAGE_TYPE_SYS       = FIL_PAGE_TYPE_SYS
	FILE_PAGE_TYPE_FSP_HDR   = FIL_PAGE_TYPE_FSP_HDR
	FILE_PAGE_TYPE_XDES      = FIL_PAGE_TYPE_XDES
	FILE_PAGE_TYPE_ALLOCATED = FIL_PAGE_TYPE_ALLOCATED
	FILE_PAGE_INDEX          = FIL_PAGE_INDEX
)

// Record type flags
const (
	LEAF_RECORD_TYPE    = 1 // Leaf node record
	NO_LEAF_RECORD_TYPE = 0 // Non-leaf node record
	COMMON_TRUE         = 1
)

// IPage represents a page interface
type IPage interface {
	GetID() uint32
	GetSpaceID() uint32
	GetPageNo() uint32
	GetPageType() PageType
	GetLSN() uint64
	SetLSN(lsn uint64)
	IsDirty() bool
	MarkDirty()
	GetState() PageState
	SetState(state PageState)
	Pin()
	Unpin()
	Read() error
	Write() error
	IsLeafPage() bool
}

// IndexEntry represents an index entry in a page
type IndexEntry struct {
	Key   []byte
	Value []byte
}
