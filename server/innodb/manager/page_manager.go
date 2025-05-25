package manager

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"sync"
	"time"
)

// DefaultPageManager 默认页面管理器实现
type DefaultPageManager struct {
	sync.RWMutex
	basic.PageManager
	// 底层存储
	bufferPool *buffer_pool.BufferPool

	// 页面缓存
	cache basic.PageCache

	// 统计信息
	stats *PageStats

	// 配置信息
	config *PageConfig
}

// PageStats 统计信息
type PageStats struct {
	// 基础计数
	PageReads  uint64
	PageWrites uint64

	// 缓存统计
	CacheHits   uint64
	CacheMisses uint64

	// 性能统计
	AvgReadTime  time.Duration
	AvgWriteTime time.Duration
}

// PageConfig 配置信息
type PageConfig struct {
	// 缓存配置
	CacheSize      uint32
	DirtyThreshold float64
	EvictionPolicy string

	// IO配置
	ReadAheadSize   uint32
	WriteBufferSize uint32

	// 并发配置
	MaxConcurrency int
}

// NewPageManager 创建页面管理器
func NewPageManager(bp *buffer_pool.BufferPool, config *PageConfig) *DefaultPageManager {
	if config == nil {
		config = &PageConfig{
			CacheSize:      1000,
			DirtyThreshold: 0.7,
			EvictionPolicy: "LRU",
		}
	}

	return &DefaultPageManager{
		bufferPool: bp,
		cache:      NewLRUCache(config.CacheSize),
		stats:      &PageStats{},
		config:     config,
	}
}

// CreatePage 创建新页面
func (pm *DefaultPageManager) CreatePage(typ common.PageType) (basic.IPage, error) {
	pm.Lock()
	defer pm.Unlock()

	// 获取空闲页面
	block, err := pm.bufferPool.GetPageBlock(0, 0) // 使用GetPageBlock获取新页面
	if err != nil || block == nil {
		return nil, ErrNoFreePages
	}

	// 创建页面
	p := newPage(basic.PageType(typ), block.GetPageNo())
	if err := p.Init(); err != nil {
		return nil, err
	}

	// 更新缓冲块
	copy(block.GetContent(), p.GetData())
	pm.bufferPool.UpdateBlock(0, block.GetPageNo(), block) // 更新块状态

	// 加入缓存
	if err := pm.cache.Put(p); err != nil {
		return nil, err
	}

	return p, nil
}

// GetPage 获取页面
func (pm *DefaultPageManager) GetPage(spaceID, pageNo uint32) (basic.IPage, error) {
	pm.RLock()
	defer pm.RUnlock()

	// 先查缓存
	if p, ok := pm.cache.Get(spaceID, pageNo); ok {
		pm.stats.CacheHits++
		return p, nil
	}
	pm.stats.CacheMisses++

	// 从缓冲池加载
	start := time.Now()
	block, err := pm.bufferPool.GetPageBlock(spaceID, pageNo)
	if err != nil || block == nil {
		return nil, ErrPageNotFound
	}
	pm.stats.AvgReadTime = time.Since(start)

	// 解析页面
	p, err := parsePage(block.GetContent())
	if err != nil {
		return nil, err
	}

	// 加入缓存
	if err := pm.cache.Put(p); err != nil {
		return nil, err
	}

	return p, nil
}

// FlushPage 刷新页面
func (pm *DefaultPageManager) FlushPage(spaceID, pageNo uint32) error {
	pm.Lock()
	defer pm.Unlock()

	// 获取页面
	p, ok := pm.cache.Get(spaceID, pageNo)
	if !ok {
		return nil
	}

	// 如果是脏页则刷新
	if p.IsDirty() {
		// 获取页面块
		block, err := pm.bufferPool.GetPageBlock(spaceID, pageNo)
		if err != nil || block == nil {
			return ErrPageNotFound
		}

		// 更新数据
		copy(block.GetContent(), p.GetData())

		// 更新块状态并加入刷新列表
		pm.bufferPool.UpdateBlock(spaceID, pageNo, block)
		pm.bufferPool.GetFlushDiskList().AddBlock(block)

		start := time.Now()
		pm.stats.AvgWriteTime = time.Since(start)
		p.ClearDirty()
	}

	return nil
}

// FlushAll 刷新所有页面
func (pm *DefaultPageManager) FlushAll() error {
	pm.Lock()
	defer pm.Unlock()

	// 获取所有脏页
	dirtyPages := pm.getDirtyPages()

	// 批量刷新
	for _, p := range dirtyPages {
		if err := pm.FlushPage(p.GetSpaceID(), p.GetPageNo()); err != nil {
			return err
		}
	}

	return nil
}

// BeginTx 开始事务
func (pm *DefaultPageManager) BeginTx() (basic.PageTx, error) {
	return NewPageTx(pm), nil
}

// getDirtyPages 获取所有脏页
func (pm *DefaultPageManager) getDirtyPages() []basic.IPage {
	var dirtyPages []basic.IPage

	// 遍历缓存中的脏页
	pm.cache.Range(func(p basic.IPage) bool {
		if p.IsDirty() {
			dirtyPages = append(dirtyPages, p)
		}
		return true
	})

	return dirtyPages
}
