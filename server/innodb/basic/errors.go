package basic

import "errors"

// 事务相关错误
var (
	ErrInvalidTransactionState  = errors.New("invalid transaction state")
	ErrTransactionNotFound      = errors.New("transaction not found")
	ErrTransactionAlreadyExists = errors.New("transaction already exists")
	ErrTransactionTimeout       = errors.New("transaction timeout")
	ErrTransactionAborted       = errors.New("transaction aborted")
)

// 锁相关错误
var (
	ErrDeadlockDetected = errors.New("deadlock detected")
	ErrLockConflict     = errors.New("lock conflict")
	ErrLockTimeout      = errors.New("lock timeout")
	ErrLockNotFound     = errors.New("lock not found")
)

// 页面相关错误
var (
	ErrPageNotFound      = errors.New("page not found")
	ErrPageCorrupted     = errors.New("page corrupted")
	ErrInvalidPageType   = errors.New("invalid page type")
	ErrInvalidPageSize   = errors.New("invalid page size")
	ErrInvalidPageID     = errors.New("invalid page ID")
	ErrPageAlreadyExists = errors.New("page already exists")
)

// 存储相关错误
var (
	ErrSpaceNotFound   = errors.New("space not found")
	ErrSegmentNotFound = errors.New("segment not found")
	ErrExtentNotFound  = errors.New("extent not found")
	ErrInvalidExtent   = errors.New("invalid extent")
	ErrNoFreePages     = errors.New("no free pages")
	ErrNoFreeSpace     = errors.New("no free space")
)

// 索引相关错误
var (
	ErrIndexNotFound = errors.New("index not found")
	ErrDuplicateKey  = errors.New("duplicate key")
	ErrKeyNotFound   = errors.New("key not found")
	ErrInvalidKey    = errors.New("invalid key")
	ErrTreeCorrupted = errors.New("tree corrupted")
)

// 数据类型相关错误
var (
	ErrUnsupportedType = errors.New("unsupported data type")
	ErrInvalidValue    = errors.New("invalid value")
	ErrInvalidVersion  = errors.New("invalid version")
	ErrValueTooLarge   = errors.New("value too large")
	ErrDataTruncated   = errors.New("data truncated")
)

// MVCC相关错误
var (
	ErrInvalidSnapshot      = errors.New("invalid snapshot")
	ErrIncompatibleSnapshot = errors.New("incompatible snapshot")
	ErrSnapshotExpired      = errors.New("snapshot expired")
	ErrRecordNotFound       = errors.New("record not found")
	ErrPageFull             = errors.New("page full")
)

// 系统错误
var (
	ErrNotImplemented   = errors.New("not implemented")
	ErrInvalidParameter = errors.New("invalid parameter")
	ErrInternalError    = errors.New("internal error")
	ErrOutOfMemory      = errors.New("out of memory")
	ErrIOError          = errors.New("I/O error")
)
