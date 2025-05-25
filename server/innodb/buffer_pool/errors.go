package buffer_pool

import "errors"

var (
	// 页面错误
	ErrPageNotFound    = errors.New("page not found in buffer pool")
	ErrPageLocked      = errors.New("page is locked by another transaction")
	ErrPageCorrupted   = errors.New("page content is corrupted")
	ErrInvalidPageType = errors.New("invalid page type")
	ErrInvalidPageSize = errors.New("invalid page size")

	// 缓冲池错误
	ErrBufferPoolFull = errors.New("buffer pool is full")
	ErrInvalidConfig  = errors.New("invalid buffer pool configuration")
	ErrIOError        = errors.New("IO error occurred")

	// 刷新错误
	ErrFlushFailed      = errors.New("failed to flush dirty page")
	ErrCheckpointFailed = errors.New("failed to create checkpoint")

	// 并发错误
	ErrDeadlock = errors.New("deadlock detected")
	ErrTimeout  = errors.New("operation timeout")
)

// BufferPoolError 缓冲池错误结构
type BufferPoolError struct {
	Op  string // 操作名称
	Err error  // 原始错误
}

func (e *BufferPoolError) Error() string {
	if e.Err == nil {
		return "<nil>"
	}
	return e.Op + ": " + e.Err.Error()
}

func (e *BufferPoolError) Unwrap() error {
	return e.Err
}

// NewError 创建新的缓冲池错误
func NewError(op string, err error) error {
	return &BufferPoolError{
		Op:  op,
		Err: err,
	}
}

// IsNotFound 检查是否为页面未找到错误
func IsNotFound(err error) bool {
	return errors.Is(err, ErrPageNotFound)
}

// IsLocked 检查是否为页面锁定错误
func IsLocked(err error) bool {
	return errors.Is(err, ErrPageLocked)
}

// IsCorrupted 检查是否为页面损坏错误
func IsCorrupted(err error) bool {
	return errors.Is(err, ErrPageCorrupted)
}

// IsBufferPoolFull 检查是否为缓冲池已满错误
func IsBufferPoolFull(err error) bool {
	return errors.Is(err, ErrBufferPoolFull)
}

// IsIOError 检查是否为IO错误
func IsIOError(err error) bool {
	return errors.Is(err, ErrIOError)
}

// IsDeadlock 检查是否为死锁错误
func IsDeadlock(err error) bool {
	return errors.Is(err, ErrDeadlock)
}

// IsTimeout 检查是否为超时错误
func IsTimeout(err error) bool {
	return errors.Is(err, ErrTimeout)
}
