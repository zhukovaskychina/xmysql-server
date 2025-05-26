package buffer_pool

import (
	"container/list"
	"github.com/zhukovaskychina/xmysql-server/util"
	"sync"
	"sync/atomic"
	"time"
)

// OptimizedLRUCache 优化的LRU缓存实现
// 主要优化点：
// 1. 读写锁分离，提高并发性能
// 2. 减少锁持有时间
// 3. 统计信息使用原子操作
// 4. 分段锁设计（可选）
type OptimizedLRUCache struct {
	size                int
	innodbOldBlocksTime int
	evictedFunc         EvictedFunc
	purgeVisitorFunc    PurgeVisitorFunc

	// 使用读写锁，读操作可以并发
	mu sync.RWMutex

	// 统计信息使用原子操作
	hitCount  uint64
	missCount uint64

	// 三个不同的缓存层级
	items      map[uint64]*list.Element // 普通缓存
	youngItems map[uint64]*list.Element // 热点数据
	oldItems   map[uint64]*list.Element // 冷数据

	// 对应的LRU链表
	evictList      *list.List
	evictYoungList *list.List
	evictOldList   *list.List

	// 配置参数
	oldPercent   float64
	youngPercent float64
}

// lruItemOptimized 优化的LRU项目
type lruItemOptimized struct {
	key            uint64
	value          *BufferBlock
	firstVisitTime uint64
	lastVisitTime  uint64
	accessCount    uint32 // 访问次数，用于热度判断
}

// NewOptimizedLRUCache 创建优化的LRU缓存
func NewOptimizedLRUCache(size int, youngPercent, oldPercent float64, innodbOldBlocksTime int) *OptimizedLRUCache {
	cache := &OptimizedLRUCache{
		size:                size,
		innodbOldBlocksTime: innodbOldBlocksTime,
		youngPercent:        youngPercent,
		oldPercent:          oldPercent,

		items:      make(map[uint64]*list.Element),
		youngItems: make(map[uint64]*list.Element),
		oldItems:   make(map[uint64]*list.Element),

		evictList:      list.New(),
		evictYoungList: list.New(),
		evictOldList:   list.New(),
	}

	return cache
}

// Get 获取缓存项（优化版本）
func (c *OptimizedLRUCache) Get(spaceId uint32, pageNo uint32) (*BufferBlock, error) {
	key := c.generateKey(spaceId, pageNo)

	// 首先尝试从年轻区域获取
	if result := c.getFromYoung(key); result != nil {
		atomic.AddUint64(&c.hitCount, 1)
		return result, nil
	}

	// 然后尝试从老年区域获取
	if result := c.getFromOld(key); result != nil {
		atomic.AddUint64(&c.hitCount, 1)
		// 从老年区域获取到的数据，根据访问频率决定是否提升到年轻区域
		c.promoteToYoungIfNeeded(key, result)
		return result, nil
	}

	// 最后尝试从普通区域获取
	if result := c.getFromOrdinary(key); result != nil {
		atomic.AddUint64(&c.hitCount, 1)
		return result, nil
	}

	atomic.AddUint64(&c.missCount, 1)
	return nil, KeyNotFoundError
}

// getFromYoung 从年轻区域获取（优化锁使用）
func (c *OptimizedLRUCache) getFromYoung(key uint64) *BufferBlock {
	c.mu.RLock()
	element, exists := c.youngItems[key]
	if !exists {
		c.mu.RUnlock()
		return nil
	}

	// 快速获取值，然后释放读锁
	item := element.Value.(*lruItemOptimized)
	value := item.value

	// 更新访问时间和计数
	now := uint64(time.Now().Unix())
	atomic.StoreUint64(&item.lastVisitTime, now)
	atomic.AddUint32(&item.accessCount, 1)

	c.mu.RUnlock()

	// 更新LRU顺序需要写锁，但时间很短
	c.mu.Lock()
	if elem, ok := c.youngItems[key]; ok { // 再次检查，防止在锁切换期间被删除
		c.evictYoungList.MoveToFront(elem)
	}
	c.mu.Unlock()

	return value
}

// getFromOld 从老年区域获取
func (c *OptimizedLRUCache) getFromOld(key uint64) *BufferBlock {
	c.mu.RLock()
	element, exists := c.oldItems[key]
	if !exists {
		c.mu.RUnlock()
		return nil
	}

	item := element.Value.(*lruItemOptimized)
	value := item.value

	// 更新访问信息
	now := uint64(time.Now().Unix())
	atomic.StoreUint64(&item.lastVisitTime, now)
	atomic.AddUint32(&item.accessCount, 1)

	c.mu.RUnlock()

	// 更新LRU顺序
	c.mu.Lock()
	if elem, ok := c.oldItems[key]; ok {
		c.evictOldList.MoveToFront(elem)
	}
	c.mu.Unlock()

	return value
}

// getFromOrdinary 从普通区域获取
func (c *OptimizedLRUCache) getFromOrdinary(key uint64) *BufferBlock {
	c.mu.RLock()
	element, exists := c.items[key]
	if !exists {
		c.mu.RUnlock()
		return nil
	}

	item := element.Value.(*lruItemOptimized)
	value := item.value

	now := uint64(time.Now().Unix())
	atomic.StoreUint64(&item.lastVisitTime, now)
	atomic.AddUint32(&item.accessCount, 1)

	c.mu.RUnlock()

	c.mu.Lock()
	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
	}
	c.mu.Unlock()

	return value
}

// promoteToYoungIfNeeded 根据访问频率决定是否提升到年轻区域
func (c *OptimizedLRUCache) promoteToYoungIfNeeded(key uint64, value *BufferBlock) {
	c.mu.RLock()
	element, exists := c.oldItems[key]
	if !exists {
		c.mu.RUnlock()
		return
	}

	item := element.Value.(*lruItemOptimized)
	accessCount := atomic.LoadUint32(&item.accessCount)

	// 如果访问次数超过阈值，提升到年轻区域
	shouldPromote := accessCount > 3 // 阈值可配置
	c.mu.RUnlock()

	if shouldPromote {
		c.mu.Lock()
		// 再次检查项目是否还在老年区域
		if elem, ok := c.oldItems[key]; ok {
			// 从老年区域移除
			c.evictOldList.Remove(elem)
			delete(c.oldItems, key)

			// 添加到年轻区域
			if c.evictYoungList.Len() >= int(float64(c.size)*c.youngPercent) {
				c.evictFromYoung(1)
			}

			newItem := &lruItemOptimized{
				key:            key,
				value:          value,
				firstVisitTime: item.firstVisitTime,
				lastVisitTime:  item.lastVisitTime,
				accessCount:    item.accessCount,
			}
			c.youngItems[key] = c.evictYoungList.PushFront(newItem)
		}
		c.mu.Unlock()
	}
}

// Set 设置缓存项（优化版本）
func (c *OptimizedLRUCache) Set(spaceId uint32, pageNo uint32, value *BufferBlock) error {
	key := c.generateKey(spaceId, pageNo)

	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查当前缓存大小，决定存储策略
	totalLen := len(c.items) + len(c.youngItems) + len(c.oldItems)

	if totalLen < 512 {
		// 小于512时，使用普通缓存
		c.setOrdinaryLocked(key, value)
	} else if totalLen == 512 {
		// 达到512时，进行分层
		c.reorganizeCache()
		c.setOldLocked(key, value)
	} else {
		// 大于512时，默认放入老年区域
		c.setOldLocked(key, value)
	}

	return nil
}

// setOldLocked 在老年区域设置项目（需要持有锁）
func (c *OptimizedLRUCache) setOldLocked(key uint64, value *BufferBlock) {
	if element, exists := c.oldItems[key]; exists {
		// 更新existing item
		item := element.Value.(*lruItemOptimized)
		item.value = value
		atomic.StoreUint64(&item.lastVisitTime, uint64(time.Now().Unix()))
		c.evictOldList.MoveToFront(element)
		return
	}

	// 检查是否需要淘汰
	if c.evictOldList.Len() >= int(float64(c.size)*c.oldPercent) {
		c.evictFromOld(1)
	}

	// 创建新项目
	now := uint64(time.Now().Unix())
	item := &lruItemOptimized{
		key:            key,
		value:          value,
		firstVisitTime: now,
		lastVisitTime:  now,
		accessCount:    1,
	}

	c.oldItems[key] = c.evictOldList.PushFront(item)
}

// setOrdinaryLocked 在普通区域设置项目（需要持有锁）
func (c *OptimizedLRUCache) setOrdinaryLocked(key uint64, value *BufferBlock) {
	if element, exists := c.items[key]; exists {
		item := element.Value.(*lruItemOptimized)
		item.value = value
		atomic.StoreUint64(&item.lastVisitTime, uint64(time.Now().Unix()))
		c.evictList.MoveToFront(element)
		return
	}

	if c.evictList.Len() >= c.size {
		c.evictFromOrdinary(1)
	}

	now := uint64(time.Now().Unix())
	item := &lruItemOptimized{
		key:            key,
		value:          value,
		firstVisitTime: now,
		lastVisitTime:  now,
		accessCount:    1,
	}

	c.items[key] = c.evictList.PushFront(item)
}

// reorganizeCache 重新组织缓存，将普通缓存分配到年轻和老年区域
func (c *OptimizedLRUCache) reorganizeCache() {
	youngCount := int(float64(c.size) * c.youngPercent)

	i := 0
	for element := c.evictList.Front(); element != nil; element = element.Next() {
		item := element.Value.(*lruItemOptimized)

		if i < youngCount {
			c.youngItems[item.key] = c.evictYoungList.PushBack(item)
		} else {
			c.oldItems[item.key] = c.evictOldList.PushBack(item)
		}
		i++
	}

	// 清空普通缓存
	c.evictList = list.New()
	c.items = make(map[uint64]*list.Element)
}

// evictFromYoung 从年轻区域淘汰项目
func (c *OptimizedLRUCache) evictFromYoung(count int) {
	for i := 0; i < count && c.evictYoungList.Len() > 0; i++ {
		element := c.evictYoungList.Back()
		if element == nil {
			break
		}

		item := element.Value.(*lruItemOptimized)
		c.evictYoungList.Remove(element)
		delete(c.youngItems, item.key)

		// 调用淘汰回调
		if c.evictedFunc != nil {
			c.evictedFunc(item.key, item.value)
		}
	}
}

// evictFromOld 从老年区域淘汰项目
func (c *OptimizedLRUCache) evictFromOld(count int) {
	for i := 0; i < count && c.evictOldList.Len() > 0; i++ {
		element := c.evictOldList.Back()
		if element == nil {
			break
		}

		item := element.Value.(*lruItemOptimized)
		c.evictOldList.Remove(element)
		delete(c.oldItems, item.key)

		if c.evictedFunc != nil {
			c.evictedFunc(item.key, item.value)
		}
	}
}

// evictFromOrdinary 从普通区域淘汰项目
func (c *OptimizedLRUCache) evictFromOrdinary(count int) {
	for i := 0; i < count && c.evictList.Len() > 0; i++ {
		element := c.evictList.Back()
		if element == nil {
			break
		}

		item := element.Value.(*lruItemOptimized)
		c.evictList.Remove(element)
		delete(c.items, item.key)

		if c.evictedFunc != nil {
			c.evictedFunc(item.key, item.value)
		}
	}
}

// Remove 移除缓存项
func (c *OptimizedLRUCache) Remove(spaceId uint32, pageNo uint32) bool {
	key := c.generateKey(spaceId, pageNo)

	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查年轻区域
	if element, exists := c.youngItems[key]; exists {
		c.evictYoungList.Remove(element)
		delete(c.youngItems, key)
		return true
	}

	// 检查老年区域
	if element, exists := c.oldItems[key]; exists {
		c.evictOldList.Remove(element)
		delete(c.oldItems, key)
		return true
	}

	// 检查普通区域
	if element, exists := c.items[key]; exists {
		c.evictList.Remove(element)
		delete(c.items, key)
		return true
	}

	return false
}

// Has 检查缓存中是否存在指定键
func (c *OptimizedLRUCache) Has(spaceId uint32, pageNo uint32) bool {
	key := c.generateKey(spaceId, pageNo)

	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.youngItems[key]
	if exists {
		return true
	}

	_, exists = c.oldItems[key]
	if exists {
		return true
	}

	_, exists = c.items[key]
	return exists
}

// Len 返回缓存中的项目总数
func (c *OptimizedLRUCache) Len() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return uint32(len(c.items) + len(c.youngItems) + len(c.oldItems))
}

// Purge 清空所有缓存
func (c *OptimizedLRUCache) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 调用访问回调
	if c.purgeVisitorFunc != nil {
		for _, element := range c.items {
			item := element.Value.(*lruItemOptimized)
			c.purgeVisitorFunc(item.key, item.value)
		}
		for _, element := range c.youngItems {
			item := element.Value.(*lruItemOptimized)
			c.purgeVisitorFunc(item.key, item.value)
		}
		for _, element := range c.oldItems {
			item := element.Value.(*lruItemOptimized)
			c.purgeVisitorFunc(item.key, item.value)
		}
	}

	// 清空所有数据结构
	c.items = make(map[uint64]*list.Element)
	c.youngItems = make(map[uint64]*list.Element)
	c.oldItems = make(map[uint64]*list.Element)
	c.evictList = list.New()
	c.evictYoungList = list.New()
	c.evictOldList = list.New()

	// 重置统计
	atomic.StoreUint64(&c.hitCount, 0)
	atomic.StoreUint64(&c.missCount, 0)
}

// generateKey 生成缓存键
func (c *OptimizedLRUCache) generateKey(spaceId uint32, pageNo uint32) uint64 {
	buff := append(util.ConvertUInt4Bytes(spaceId), util.ConvertUInt4Bytes(pageNo)...)
	return util.HashCode(buff)
}

// 统计相关方法
func (c *OptimizedLRUCache) HitCount() uint64 {
	return atomic.LoadUint64(&c.hitCount)
}

func (c *OptimizedLRUCache) MissCount() uint64 {
	return atomic.LoadUint64(&c.missCount)
}

func (c *OptimizedLRUCache) LookupCount() uint64 {
	return c.HitCount() + c.MissCount()
}

func (c *OptimizedLRUCache) HitRate() float64 {
	hc, mc := c.HitCount(), c.MissCount()
	total := hc + mc
	if total == 0 {
		return 0.0
	}
	return float64(hc) / float64(total)
}

// 以下方法用于兼容原有接口

// GetYoung 从年轻区域获取
func (c *OptimizedLRUCache) GetYoung(spaceId uint32, pageNo uint32) (*BufferBlock, error) {
	key := c.generateKey(spaceId, pageNo)
	if result := c.getFromYoung(key); result != nil {
		return result, nil
	}
	return nil, KeyNotFoundError
}

// SetYoung 设置到年轻区域
func (c *OptimizedLRUCache) SetYoung(spaceId uint32, pageNo uint32, value *BufferBlock) {
	key := c.generateKey(spaceId, pageNo)

	c.mu.Lock()
	defer c.mu.Unlock()

	if element, exists := c.youngItems[key]; exists {
		item := element.Value.(*lruItemOptimized)
		item.value = value
		atomic.StoreUint64(&item.lastVisitTime, uint64(time.Now().Unix()))
		c.evictYoungList.MoveToFront(element)
		return
	}

	if c.evictYoungList.Len() >= int(float64(c.size)*c.youngPercent) {
		c.evictFromYoung(1)
	}

	now := uint64(time.Now().Unix())
	item := &lruItemOptimized{
		key:            key,
		value:          value,
		firstVisitTime: now,
		lastVisitTime:  now,
		accessCount:    1,
	}

	c.youngItems[key] = c.evictYoungList.PushFront(item)
}

// GetOld 从老年区域获取
func (c *OptimizedLRUCache) GetOld(spaceId uint32, pageNo uint32) (*BufferBlock, error) {
	key := c.generateKey(spaceId, pageNo)
	if result := c.getFromOld(key); result != nil {
		return result, nil
	}
	return nil, KeyNotFoundError
}

// SetOld 设置到老年区域
func (c *OptimizedLRUCache) SetOld(spaceId uint32, pageNo uint32, value *BufferBlock) {
	key := c.generateKey(spaceId, pageNo)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.setOldLocked(key, value)
}

// Evict 淘汰一个页面
func (c *OptimizedLRUCache) Evict() *BufferPage {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 优先从老年区域淘汰
	if c.evictOldList.Len() > 0 {
		element := c.evictOldList.Back()
		if element != nil {
			item := element.Value.(*lruItemOptimized)
			c.evictOldList.Remove(element)
			delete(c.oldItems, item.key)

			// 转换为BufferPage返回
			page := NewBufferPage(uint32(item.key>>32), uint32(item.key&0xFFFFFFFF))
			page.SetContent(item.value.GetContent())
			return page
		}
	}

	// 然后从普通区域淘汰
	if c.evictList.Len() > 0 {
		element := c.evictList.Back()
		if element != nil {
			item := element.Value.(*lruItemOptimized)
			c.evictList.Remove(element)
			delete(c.items, item.key)

			page := NewBufferPage(uint32(item.key>>32), uint32(item.key&0xFFFFFFFF))
			page.SetContent(item.value.GetContent())
			return page
		}
	}

	return nil
}

// Range 遍历缓存中的所有页面
func (c *OptimizedLRUCache) Range(f func(page *BufferPage) bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 遍历年轻区域
	for _, element := range c.youngItems {
		item := element.Value.(*lruItemOptimized)
		page := NewBufferPage(uint32(item.key>>32), uint32(item.key&0xFFFFFFFF))
		page.SetContent(item.value.GetContent())
		if !f(page) {
			return
		}
	}

	// 遍历老年区域
	for _, element := range c.oldItems {
		item := element.Value.(*lruItemOptimized)
		page := NewBufferPage(uint32(item.key>>32), uint32(item.key&0xFFFFFFFF))
		page.SetContent(item.value.GetContent())
		if !f(page) {
			return
		}
	}

	// 遍历普通区域
	for _, element := range c.items {
		item := element.Value.(*lruItemOptimized)
		page := NewBufferPage(uint32(item.key>>32), uint32(item.key&0xFFFFFFFF))
		page.SetContent(item.value.GetContent())
		if !f(page) {
			return
		}
	}
}
