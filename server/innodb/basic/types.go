package basic

import "xmysql-server/server/common"

// Key represents a key in an index
type Key interface{}

// ValueType represents the type of a value in the database
type ValueType int

// Value type constants
const (
	ValueTypeNull ValueType = iota
	ValueTypeTinyInt
	ValueTypeSmallInt
	ValueTypeMediumInt
	ValueTypeInt
	ValueTypeBigInt
	ValueTypeFloat
	ValueTypeDouble
	ValueTypeDecimal
	ValueTypeDate
	ValueTypeTime
	ValueTypeDateTime
	ValueTypeTimestamp
	ValueTypeYear
	ValueTypeChar
	ValueTypeVarchar
	ValueTypeBinary
	ValueTypeVarBinary
	ValueTypeTinyBlob
	ValueTypeBlob
	ValueTypeMediumBlob
	ValueTypeLongBlob
	ValueTypeTinyText
	ValueTypeText
	ValueTypeMediumText
	ValueTypeLongText
	ValueTypeEnum
	ValueTypeSet
	ValueTypeJSON
	ValueTypeBool
	ValueTypeBoolean
)

// String returns the string representation of ValueType
func (vt ValueType) String() string {
	switch vt {
	case ValueTypeNull:
		return "NULL"
	case ValueTypeTinyInt:
		return "TINYINT"
	case ValueTypeSmallInt:
		return "SMALLINT"
	case ValueTypeMediumInt:
		return "MEDIUMINT"
	case ValueTypeInt:
		return "INT"
	case ValueTypeBigInt:
		return "BIGINT"
	case ValueTypeFloat:
		return "FLOAT"
	case ValueTypeDouble:
		return "DOUBLE"
	case ValueTypeDecimal:
		return "DECIMAL"
	case ValueTypeDate:
		return "DATE"
	case ValueTypeTime:
		return "TIME"
	case ValueTypeDateTime:
		return "DATETIME"
	case ValueTypeTimestamp:
		return "TIMESTAMP"
	case ValueTypeYear:
		return "YEAR"
	case ValueTypeChar:
		return "CHAR"
	case ValueTypeVarchar:
		return "VARCHAR"
	case ValueTypeBinary:
		return "BINARY"
	case ValueTypeVarBinary:
		return "VARBINARY"
	case ValueTypeTinyBlob:
		return "TINYBLOB"
	case ValueTypeBlob:
		return "BLOB"
	case ValueTypeMediumBlob:
		return "MEDIUMBLOB"
	case ValueTypeLongBlob:
		return "LONGBLOB"
	case ValueTypeTinyText:
		return "TINYTEXT"
	case ValueTypeText:
		return "TEXT"
	case ValueTypeMediumText:
		return "MEDIUMTEXT"
	case ValueTypeLongText:
		return "LONGTEXT"
	case ValueTypeEnum:
		return "ENUM"
	case ValueTypeSet:
		return "SET"
	case ValueTypeJSON:
		return "JSON"
	case ValueTypeBool, ValueTypeBoolean:
		return "BOOLEAN"
	default:
		return "UNKNOWN"
	}
}

// IsNumeric returns true if the value type is numeric
func (vt ValueType) IsNumeric() bool {
	switch vt {
	case ValueTypeTinyInt, ValueTypeSmallInt, ValueTypeMediumInt,
		ValueTypeInt, ValueTypeBigInt, ValueTypeFloat, ValueTypeDouble, ValueTypeDecimal:
		return true
	default:
		return false
	}
}

// IsString returns true if the value type is a string type
func (vt ValueType) IsString() bool {
	switch vt {
	case ValueTypeChar, ValueTypeVarchar, ValueTypeTinyText, ValueTypeText,
		ValueTypeMediumText, ValueTypeLongText, ValueTypeEnum, ValueTypeSet:
		return true
	default:
		return false
	}
}

// IsBinary returns true if the value type is a binary type
func (vt ValueType) IsBinary() bool {
	switch vt {
	case ValueTypeBinary, ValueTypeVarBinary, ValueTypeTinyBlob, ValueTypeBlob,
		ValueTypeMediumBlob, ValueTypeLongBlob:
		return true
	default:
		return false
	}
}

// IsTemporal returns true if the value type is a temporal type
func (vt ValueType) IsTemporal() bool {
	switch vt {
	case ValueTypeDate, ValueTypeTime, ValueTypeDateTime, ValueTypeTimestamp, ValueTypeYear:
		return true
	default:
		return false
	}
}

// IsJSON returns true if the value type is JSON
func (vt ValueType) IsJSON() bool {
	return vt == ValueTypeJSON
}

// GetSize returns the default size for the value type
func (vt ValueType) GetSize() int {
	switch vt {
	case ValueTypeTinyInt:
		return 1
	case ValueTypeSmallInt:
		return 2
	case ValueTypeMediumInt:
		return 3
	case ValueTypeInt:
		return 4
	case ValueTypeBigInt:
		return 8
	case ValueTypeFloat:
		return 4
	case ValueTypeDouble:
		return 8
	case ValueTypeDate:
		return 3
	case ValueTypeTime:
		return 3
	case ValueTypeDateTime:
		return 8
	case ValueTypeTimestamp:
		return 4
	case ValueTypeYear:
		return 1
	case ValueTypeBool, ValueTypeBoolean:
		return 1
	default:
		return -1 // Variable length
	}
}

// Value represents a value in an index
type Value interface {
	Compare(key Value) int
	Raw() interface{}
	ToString() string
	Bytes() []byte
	IsNull() bool
	Int() int64
	Float64() float64
	String() string
	Time() interface{}
	Bool() bool
	// Add type information
	Type() ValueType
	LessOrEqual() (interface{}, interface{})
}

// PageState 页面状态类型别名
type PageState = common.PageState

// PageType 页面类型别名
type PageType = common.PageType

// 页面状态常量
const (
	PageStateNew      = common.PageState(100) // 新创建的页面状态
	PageStateInit     = common.PageStateInit
	PageStateLoaded   = common.PageStateLoaded
	PageStateModified = common.PageStateModified
	PageStateDirty    = common.PageStateDirty
	PageStateClean    = common.PageStateClean
	PageStatePinned   = common.PageStatePinned
	PageStateFlushed  = common.PageStateFlushed
	PageStateActive   = common.PageState(101) // 活跃页面状态
)

// 页面类型常量
const (
	PageTypeIndex                  = common.FIL_PAGE_INDEX
	PageTypeUndoLog                = common.FIL_PAGE_UNDO_LOG
	PageTypeINode                  = common.FIL_PAGE_INODE
	PageTypeIBufFreeList           = common.FIL_PAGE_IBUF_FREE_LIST
	PageTypeIBufBitmap             = common.FIL_PAGE_IBUF_BITMAP
	PageTypeSys                    = common.FIL_PAGE_TYPE_SYS
	PageTypeTrxSys                 = common.FIL_PAGE_TYPE_TRX_SYS
	PageTypeFSPHdr                 = common.FIL_PAGE_TYPE_FSP_HDR
	PageTypeXDES                   = common.FIL_PAGE_TYPE_XDES
	PageTypeBlob                   = common.FIL_PAGE_TYPE_BLOB
	PageTypeCompressed             = common.FIL_PAGE_TYPE_COMPRESSED
	PageTypeEncrypted              = common.FIL_PAGE_TYPE_ENCRYPTED
	PageTypeCompressedAndEncrypted = common.FIL_PAGE_TYPE_COMPRESSED_AND_ENCRYPTED
	PageTypeEncryptedRTree         = common.FIL_PAGE_TYPE_ENCRYPTED_RTREE
	PageTypeAllocated              = common.FIL_PAGE_TYPE_ALLOCATED
)

// PageStats 页面统计信息
type PageStats struct {
	ReadCount    uint64 // 读取次数
	WriteCount   uint64 // 写入次数
	PinCount     uint64 // Pin操作次数
	DirtyCount   uint64 // 标记脏页次数
	AccessTime   uint64 // 最后访问时间
	LastAccessed uint64 // 最后访问时间戳
	LastModified uint64 // 最后修改时间戳
	LastAccessAt uint64 // 最后访问时间戳（别名）
}

// IncReadCount increments the read count
func (ps *PageStats) IncReadCount() {
	ps.ReadCount++
}

// IncWriteCount increments the write count
func (ps *PageStats) IncWriteCount() {
	ps.WriteCount++
}

// GetCurrentTimestamp returns the current timestamp
func GetCurrentTimestamp() uint64 {
	// 简单实现，返回当前时间戳
	return uint64(1000000) // 占位符实现
}

// SegmentPurpose represents the purpose of a segment
type SegmentPurpose int

const (
	SegmentPurposeLeaf SegmentPurpose = iota
	SegmentPurposeNonLeaf
	SegmentPurposeRollback
)
