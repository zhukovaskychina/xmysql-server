package basic

import (
	"time"
)

// PageCache 页面缓存接口
// 使用统一的IPage接口
type PageCache interface {
	// 基本操作
	Get(spaceID, pageNo uint32) (IPage, bool)
	Put(p IPage) error
	Remove(spaceID, pageNo uint32)
	Clear()

	// 遍历
	Range(fn func(IPage) bool)

	// 统计
	Size() int
	Capacity() uint32
	GetStats() *PageCacheStats
}

// PageTx 页面事务接口
// 使用统一的IPage接口
type PageTx interface {
	// 事务操作
	GetPage(spaceID, pageNo uint32) (IPage, error)
	CreatePage(typ PageType) (IPage, error)
	DeletePage(spaceID, pageNo uint32) error

	// 事务控制
	Commit() error
	Rollback() error
}

// PageCacheStats 缓存统计信息
type PageCacheStats struct {
	Hits          uint64        `json:"hits"`            // 命中次数
	Misses        uint64        `json:"misses"`          // 未命中次数
	Evictions     uint64        `json:"evictions"`       // 驱逐次数
	AvgAccessTime time.Duration `json:"avg_access_time"` // 平均访问时间
}
