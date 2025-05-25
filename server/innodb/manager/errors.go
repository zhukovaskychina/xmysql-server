package manager

import "errors"

// 页面管理器错误
var (
	ErrPageNotFound      = errors.New("page not found")
	ErrPageDataTooLarge  = errors.New("page data too large")
	ErrInvalidPageData   = errors.New("invalid page data")
	ErrPageAlreadyExists = errors.New("page already exists")
	ErrTxFinished        = errors.New("transaction already finished")
	ErrNoFreePages       = errors.New("no free pages available")
)

// Common errors
var (
	ErrNotImplemented = errors.New("not implemented")
	ErrInvalidParam   = errors.New("invalid parameter")
)

// Buffer pool manager errors
var (
	ErrBufferPoolFull = errors.New("buffer pool full")
	ErrFrameNotFound  = errors.New("frame not found")
	ErrFrameLocked    = errors.New("frame locked")
)

// Segment manager errors
var (
	ErrSegmentNotFound = errors.New("segment not found")
	ErrSegmentFull     = errors.New("segment full")
	ErrInvalidSegment  = errors.New("invalid segment")
)

// Extent manager errors
var (
	ErrExtentNotFound = errors.New("extent not found")
	ErrExtentFull     = errors.New("extent full")
	ErrInvalidExtent  = errors.New("invalid extent")
)

// Transaction manager errors
var (
	ErrTxNotFound      = errors.New("transaction not found")
	ErrTxAlreadyExists = errors.New("transaction already exists")
	ErrTxTimeout       = errors.New("transaction timeout")
	ErrTxAborted       = errors.New("transaction aborted")
)

// Lock manager errors
var (
	ErrLockTimeout      = errors.New("lock timeout")
	ErrDeadlockDetected = errors.New("deadlock detected")
	ErrLockNotFound     = errors.New("lock not found")
)

// MVCC manager errors
var (
	ErrVersionNotFound = errors.New("version not found")
	ErrVersionConflict = errors.New("version conflict")
)

// Schema manager errors
var (
	ErrSchemaNotFound   = errors.New("schema not found")
	ErrTableNotFound    = errors.New("table not found")
	ErrColumnNotFound   = errors.New("column not found")
	ErrIndexNotFound    = errors.New("index not found")
	ErrDuplicateSchema  = errors.New("duplicate schema")
	ErrDuplicateTable   = errors.New("duplicate table")
	ErrDuplicateColumn  = errors.New("duplicate column")
	ErrDuplicateIndex   = errors.New("duplicate index")
	ErrIndexExists      = errors.New("index already exists")
	ErrForeignKeyExists = errors.New("foreign key already exists")
	ErrRefTableNotFound = errors.New("referenced table not found")
)
