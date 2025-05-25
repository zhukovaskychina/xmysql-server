package manager

import (
	"container/list"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"sync"
	"time"
)

// PageCache 页面缓存接口的本地别名
type PageCache = basic.PageCache

// LRUCache LRU缓存实现，实现了basic.PageCache接口
type LRUCache struct {
	sync.RWMutex

	// 基本属性
	capacity uint32
	size     uint32

	// 存储结构
	items map[uint64]*list.Element
	order *list.List

	// 统计信息
	stats *basic.PageCacheStats
}

// CacheItem 缓存项
type CacheItem struct {
	key      uint64
	page     basic.IPage
	accessed time.Time
}

// NewLRUCache 创建LRU缓存
func NewLRUCache(capacity uint32) basic.PageCache {
	return &LRUCache{
		capacity: capacity,
		items:    make(map[uint64]*list.Element),
		order:    list.New(),
		stats:    &basic.PageCacheStats{},
	}
}

// makeKey 生成缓存键
func makeKey(spaceID, pageNo uint32) uint64 {
	return uint64(spaceID)<<32 | uint64(pageNo)
}

// Get 获取页面
func (c *LRUCache) Get(spaceID, pageNo uint32) (basic.IPage, bool) {
	c.Lock()
	defer c.Unlock()

	// 查找缓存项
	key := makeKey(spaceID, pageNo)
	if elem, ok := c.items[key]; ok {
		// 移到队首并更新访问时间
		c.order.MoveToFront(elem)
		item := elem.Value.(*CacheItem)
		item.accessed = time.Now()

		// 更新统计
		c.stats.Hits++
		return item.page, true
	}

	// 更新统计
	c.stats.Misses++
	return nil, false
}

// Put 放入页面
func (c *LRUCache) Put(p basic.IPage) error {
	if p == nil {
		return fmt.Errorf("page is nil")
	}

	c.Lock()
	defer c.Unlock()

	// 生成键
	key := makeKey(p.GetSpaceID(), p.GetPageNo())

	// 如果已存在则更新
	if elem, ok := c.items[key]; ok {
		item := elem.Value.(*CacheItem)
		item.page = p
		item.accessed = time.Now()
		c.order.MoveToFront(elem)
		return nil
	}

	// 如果满了则淘汰
	if c.size >= c.capacity {
		c.evict()
	}

	// 插入新项
	item := &CacheItem{
		key:      key,
		page:     p,
		accessed: time.Now(),
	}
	elem := c.order.PushFront(item)
	c.items[key] = elem
	c.size++

	return nil
}

// Remove 移除页面
func (c *LRUCache) Remove(spaceID, pageNo uint32) {
	c.Lock()
	defer c.Unlock()

	// 查找并删除
	key := makeKey(spaceID, pageNo)
	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
	}
}

// Clear 清空缓存
func (c *LRUCache) Clear() {
	c.Lock()
	defer c.Unlock()

	// 释放所有页面
	for _, elem := range c.items {
		elem.Value.(*CacheItem).page.Release()
	}

	// 重置存储
	c.items = make(map[uint64]*list.Element)
	c.order.Init()
	c.size = 0

	// 重置统计
	c.stats = &basic.PageCacheStats{}
}

// Range 遍历缓存
func (c *LRUCache) Range(fn func(basic.IPage) bool) {
	c.RLock()
	defer c.RUnlock()

	// 遍历所有页面
	for elem := c.order.Front(); elem != nil; elem = elem.Next() {
		if !fn(elem.Value.(*CacheItem).page) {
			break
		}
	}
}

// Size 获取大小
func (c *LRUCache) Size() int {
	c.RLock()
	defer c.RUnlock()
	return int(c.size)
}

// Capacity 获取容量
func (c *LRUCache) Capacity() uint32 {
	c.RLock()
	defer c.RUnlock()
	return c.capacity
}

// GetStats 获取统计信息
func (c *LRUCache) GetStats() *basic.PageCacheStats {
	c.RLock()
	defer c.RUnlock()

	// 返回统计信息的拷贝
	statsCopy := *c.stats
	return &statsCopy
}

// evict 淘汰最久未用页面
func (c *LRUCache) evict() {
	// 获取最后一项
	elem := c.order.Back()
	if elem == nil {
		return
	}

	// 移除该项
	c.removeElement(elem)
	c.stats.Evictions++
}

// removeElement 移除缓存项
func (c *LRUCache) removeElement(elem *list.Element) {
	// 释放页面
	item := elem.Value.(*CacheItem)
	item.page.Release()

	// 从存储中删除
	delete(c.items, item.key)
	c.order.Remove(elem)
	c.size--
}
