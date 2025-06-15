package manager

import (
	"context"
	"time"
)

// BTreeManager B+Tree管理器接口
type BTreeManager interface {
	// 索引管理
	CreateIndex(ctx context.Context, metadata *IndexMetadata) (BTreeIndex, error)
	GetIndex(indexID uint64) (BTreeIndex, error)
	GetIndexByName(tableID uint64, indexName string) (BTreeIndex, error)
	DropIndex(ctx context.Context, indexID uint64) error

	// 索引操作
	Insert(ctx context.Context, indexID uint64, key []byte, value []byte) error
	Delete(ctx context.Context, indexID uint64, key []byte) error
	Search(ctx context.Context, indexID uint64, key []byte) (*IndexRecord, error)
	RangeSearch(ctx context.Context, indexID uint64, startKey, endKey []byte) ([]IndexRecord, error)

	// 管理操作
	FlushIndex(ctx context.Context, indexID uint64) error
	AnalyzeIndex(ctx context.Context, indexID uint64) (*EnhancedIndexStatistics, error)
	RebuildIndex(ctx context.Context, indexID uint64) error

	// 资源管理
	LoadIndex(ctx context.Context, indexID uint64) error
	UnloadIndex(indexID uint64) error
	Close() error
}

// BTreeIndex B+树索引实例
type BTreeIndex interface {
	// 基本信息
	GetIndexID() uint64
	GetTableID() uint64
	GetSpaceID() uint32
	GetRootPageNo() uint32
	GetMetadata() *IndexMetadata

	// 索引操作
	Insert(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Search(ctx context.Context, key []byte) (*IndexRecord, error)
	RangeSearch(ctx context.Context, startKey, endKey []byte) ([]IndexRecord, error)

	// 遍历操作
	GetFirstLeafPage(ctx context.Context) (uint32, error)
	GetAllLeafPages(ctx context.Context) ([]uint32, error)
	Iterator(ctx context.Context) (IndexIterator, error)

	// 页面管理
	GetPage(ctx context.Context, pageNo uint32) (*BTreePage, error)
	AllocatePage(ctx context.Context) (uint32, error)
	DeallocatePage(ctx context.Context, pageNo uint32) error

	// 统计和维护
	GetStatistics() *EnhancedIndexStatistics
	UpdateStatistics(ctx context.Context) error
	CheckConsistency(ctx context.Context) error

	// 资源管理
	Flush(ctx context.Context) error
	IsLoaded() bool
	GetRefCount() int32
	AddRef() int32
	Release() int32
}

// IndexRecord 索引记录
type IndexRecord struct {
	Key        []byte // 索引键
	Value      []byte // 记录值或行ID
	PageNo     uint32 // 所在页号
	SlotNo     uint16 // 槽位号
	TxnID      uint64 // 事务ID
	DeleteMark bool   // 删除标记
}

// IndexIterator 索引迭代器
type IndexIterator interface {
	// 迭代操作
	HasNext() bool
	Next() (*IndexRecord, error)
	HasPrev() bool
	Prev() (*IndexRecord, error)

	// 定位操作
	SeekFirst() error
	SeekLast() error
	SeekTo(key []byte) error

	// 当前位置
	Current() (*IndexRecord, error)
	GetPosition() (uint32, uint16) // pageNo, slotNo

	// 资源管理
	Close() error
}

// BTreePage B+树页面
type BTreePage struct {
	PageNo      uint32        // 页号
	PageType    BTreePageType // 页面类型
	Level       uint8         // 层级（0为叶子层）
	RecordCount uint16        // 记录数
	FreeSpace   uint16        // 剩余空间
	NextPage    uint32        // 下一页（叶子页链表）
	PrevPage    uint32        // 上一页（叶子页链表）

	// 页面内容
	Records    []IndexRecord // 记录列表
	IsLoaded   bool          // 是否已加载
	IsDirty    bool          // 是否脏页
	LastAccess time.Time     // 最后访问时间
	PinCount   int32         // 固定计数
}

// BTreePageType B+树页面类型
type BTreePageType uint8

const (
	BTreePageTypeRoot     BTreePageType = iota // 根页面
	BTreePageTypeInternal                      // 内部页面
	BTreePageTypeLeaf                          // 叶子页面
	BTreePageTypeOverflow                      // 溢出页面
)

// BTreeConfig B+树配置
type BTreeConfig struct {
	// 缓存配置
	MaxCacheSize uint32 // 最大缓存页数
	CachePolicy  string // 缓存策略（LRU/LFU/CLOCK）
	PrefetchSize uint32 // 预读页数

	// 页面配置
	PageSize      uint32  // 页面大小
	FillFactor    float64 // 填充因子
	MinFillFactor float64 // 最小填充因子

	// 性能配置
	SplitThreshold float64 // 分裂阈值
	MergeThreshold float64 // 合并阈值
	AsyncIO        bool    // 异步IO

	// 统计配置
	EnableStats   bool          // 启用统计
	StatsInterval time.Duration // 统计间隔

	// 日志配置
	EnableLogging bool   // 启用日志
	LogLevel      string // 日志级别
}

// DefaultBTreeConfig 默认B+树配置
var DefaultBTreeConfig = &BTreeConfig{
	MaxCacheSize:   1000,
	CachePolicy:    "LRU",
	PrefetchSize:   4,
	PageSize:       16384,
	FillFactor:     0.8,
	MinFillFactor:  0.4,
	SplitThreshold: 0.9,
	MergeThreshold: 0.3,
	AsyncIO:        true,
	EnableStats:    true,
	StatsInterval:  time.Minute * 5,
	EnableLogging:  true,
	LogLevel:       "INFO",
}

// CompareFunc 键比较函数
type CompareFunc func(a, b []byte) int

// KeyExtractFunc 键提取函数
type KeyExtractFunc func(record []byte) []byte

// IndexOptions 索引选项
type IndexOptions struct {
	CompareFunc     CompareFunc    // 键比较函数
	KeyExtractFunc  KeyExtractFunc // 键提取函数
	AllowDuplicates bool           // 是否允许重复键
	PageSplitType   string         // 页分裂类型
	Compression     bool           // 是否启用压缩
}
