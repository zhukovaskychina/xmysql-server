package system

import (
	"sync/atomic"
)

// SystemPageType 系统页面类型
type SystemPageType uint16

const (
	SystemPageTypeInode SystemPageType = iota
	SystemPageTypeFSP
	SystemPageTypeXDES
	SystemPageTypeIBuf
	SystemPageTypeDict
	SystemPageTypeTrx
)

// SystemPageState 系统页面状态
type SystemPageState uint8

const (
	SystemPageStateNormal SystemPageState = iota
	SystemPageStateCorrupted
	SystemPageStateRecovering
)

// SystemPageStats 系统页面统计信息
type SystemPageStats struct {
	Reads         atomic.Uint64
	Writes        atomic.Uint64
	Corruptions   atomic.Uint32
	Recoveries    atomic.Uint32
	LastModified  int64
	LastRecovered int64
}

// SystemPage 系统页面接口
type SystemPage interface {

	// 系统页面特有方法
	GetSystemType() SystemPageType
	GetSystemState() SystemPageState
	SetSystemState(state SystemPageState)
	GetSystemStats() *SystemPageStats

	// 恢复相关
	Recover() error
	Validate() error
	Backup() error
	Restore() error
}

// SystemPageManager 系统页面管理器接口
type SystemPageManager interface {
	// 页面操作
	GetSystemPage(spaceID uint32, pageNo uint32) (SystemPage, error)
	FlushSystemPage(page SystemPage) error
	NewSystemPage(spaceID uint32, pageType SystemPageType) (SystemPage, error)

	// 恢复操作
	RecoverPage(spaceID uint32, pageNo uint32) error
	ValidatePages() error
	BackupPages() error
	RestorePages() error

	// 统计信息
	GetStats() *SystemPageManagerStats
}

// SystemPageManagerStats 系统页面管理器统计信息
type SystemPageManagerStats struct {
	TotalPages     atomic.Uint32
	CorruptedPages atomic.Uint32
	RecoveredPages atomic.Uint32
	BackupPages    atomic.Uint32
	ValidationOps  atomic.Uint64
	RecoveryOps    atomic.Uint64
	LastValidation int64
	LastRecovery   int64
}

// InodePageData INode页面数据
type InodePageData struct {
	Segments map[uint64]*SegmentData
	FreeList []uint32
	FullList []uint32
	FragList []uint32
}

// SegmentData 段数据
type SegmentData struct {
	ID         uint64
	Type       uint8
	State      uint8
	SpaceID    uint32
	PageCount  uint32
	FreePages  uint32
	Properties map[string]string
}

// FSPPageData FSP页面数据
type FSPPageData struct {
	SpaceID    uint32
	SpaceFlags uint32
	Size       uint64
	FreeLimit  uint32
	FreePages  uint32
	FragPages  uint32
	Properties map[string]string
}

// XDESPageData XDES页面数据
type XDESPageData struct {
	Descriptors []ExtentDescriptor
	FreeList    []uint32
	FullList    []uint32
	Properties  map[string]string
}

// ExtentDescriptor 区描述符
type ExtentDescriptor struct {
	ID       uint32
	State    uint8
	PageBits []byte
}

// IBufPageData Insert Buffer页面数据
type IBufPageData struct {
	SpaceID    uint32
	PageCount  uint32
	FreeSpace  uint32
	Records    []IBufRecord
	Properties map[string]string
}

// IBufRecord Insert Buffer记录
type IBufRecord struct {
	Type    uint8
	SpaceID uint32
	PageNo  uint32
	Data    []byte
}

// DictPageData 数据字典页面数据
type DictPageData struct {
	Tables     []TableData
	Indexes    []IndexData
	Properties map[string]string
}

// TableData 表数据
type TableData struct {
	ID         uint64
	Name       string
	SpaceID    uint32
	RootPage   uint32
	Columns    []ColumnData
	Properties map[string]string
}

// ColumnData 列数据
type ColumnData struct {
	ID         uint16
	Name       string
	Type       uint16
	Length     uint32
	Properties map[string]string
}

// IndexData 索引数据
type IndexData struct {
	ID         uint64
	Name       string
	TableID    uint64
	Type       uint16
	RootPage   uint32
	Properties map[string]string
}

// TrxPageData 事务系统页面数据
type TrxPageData struct {
	MaxTrxID   uint64
	MaxRollPtr uint64
	Properties map[string]string
	TrxList    []TrxData
}

// TrxData 事务数据
type TrxData struct {
	TrxID      uint64
	State      uint8
	StartTime  int64
	Properties map[string]string
}
