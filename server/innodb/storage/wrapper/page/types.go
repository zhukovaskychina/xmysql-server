package page

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/latch"
	"sync"
	"sync/atomic"
)

const (
	PageStateInit     = common.PageStateInit
	PageStateLoaded   = common.PageStateLoaded
	PageStateModified = common.PageStateModified
	PageStateFlushed  = common.PageStateFlushed
)

// ConcurrencyControl 并发控制接口
type ConcurrencyControl interface {
	GetLock() *sync.RWMutex
	GetLatch() *latch.Latch
}

// ConcurrentWrapper 并发控制基类
type ConcurrentWrapper struct {
	mu sync.RWMutex
}

func (cw *ConcurrentWrapper) Lock()          { cw.mu.Lock() }
func (cw *ConcurrentWrapper) Unlock()        { cw.mu.Unlock() }
func (cw *ConcurrentWrapper) RLock()         { cw.mu.RLock() }
func (cw *ConcurrentWrapper) RUnlock()       { cw.mu.RUnlock() }
func (cw *ConcurrentWrapper) TryLock() bool  { return cw.mu.TryLock() }
func (cw *ConcurrentWrapper) TryRLock() bool { return cw.mu.TryRLock() }

// PageTrailer 页面尾部结构
type PageTrailer struct {
	Checksum uint32
	LSN      uint64
}

// PageCache 页面缓存接口
type PageCache interface {
	Get(key interface{}) (interface{}, bool)
	Put(key, value interface{}) bool
	Remove(key interface{}) bool
	Clear()
	GetStats() *CacheStats
}

// CacheStats 缓存统计信息
type CacheStats struct {
	Hits   atomic.Uint64
	Misses atomic.Uint64
	Size   atomic.Uint32
}

// PageManagerStats 页面管理器统计信息
type PageManagerStats struct {
	TotalPages      atomic.Uint32
	DirtyPages      atomic.Uint32
	FreePages       atomic.Uint32
	FlushOperations atomic.Uint64
	ReadOperations  atomic.Uint64
	WriteOperations atomic.Uint64
}
