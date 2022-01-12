package buffer_pool

import (
	"container/list"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"xmysql-server/util"
)

var KeyNotFoundError = errors.New("Key not found.")

type LRUCache interface {

	//lru 中设置spaceId,pageNo
	Set(spaceId uint32, pageNo uint32, value *BufferBlock) error

	Get(spaceId uint32, pageNo uint32) (*BufferBlock, error)

	Remove(spaceId uint32, pageNo uint32) bool

	// Purge removes all key-value pairs from the cache.
	Purge()

	// Has returns true if the key exists in the cache.
	Has(spaceId uint32, pageNo uint32) bool

	SetYoung(spaceId uint32, pageNo uint32, value *BufferBlock)

	GetYoung(spaceId uint32, pageNo uint32) (*BufferBlock, error)

	SetOld(spaceId uint32, pageNo uint32, value *BufferBlock)

	GetOld(spaceId uint32, pageNo uint32) (*BufferBlock, error)

	Len() uint32
}

type (
	EvictedFunc      func(interface{}, interface{})
	PurgeVisitorFunc func(interface{}, interface{})
	AddedFunc        func(interface{}, interface{})
	DeserializeFunc  func(interface{}, interface{}) (interface{}, error)
	SerializeFunc    func(interface{}, interface{}) (interface{}, error)

	PurgeOldMoveYoungFunc func(interface{}, interface{}) (interface{}, error)
)

type statsAccessor interface {
	HitCount() uint64
	MissCount() uint64
	LookupCount() uint64
	HitRate() float64
}

// statistics
type stats struct {
	hitCount  uint64
	missCount uint64
}

// increment hit count
func (st *stats) IncrHitCount() uint64 {
	return atomic.AddUint64(&st.hitCount, 1)
}

// increment miss count
func (st *stats) IncrMissCount() uint64 {
	return atomic.AddUint64(&st.missCount, 1)
}

// HitCount returns hit count
func (st *stats) HitCount() uint64 {
	return atomic.LoadUint64(&st.hitCount)
}

// MissCount returns miss count
func (st *stats) MissCount() uint64 {
	return atomic.LoadUint64(&st.missCount)
}

// LookupCount returns lookup count
func (st *stats) LookupCount() uint64 {
	return st.HitCount() + st.MissCount()
}

// HitRate returns rate for cache hitting
func (st *stats) HitRate() float64 {
	hc, mc := st.HitCount(), st.MissCount()
	total := hc + mc
	if total == 0 {
		return 0.0
	}
	return float64(hc) / float64(total)
}

// Discards the least recently used items first.
type LRUCacheImpl struct {
	size                int
	innodbOldBlocksTime int
	evictedFunc         EvictedFunc
	purgeVisitorFunc    PurgeVisitorFunc
	mu                  sync.RWMutex

	*stats
	items map[uint64]*list.Element

	youngItems map[uint64]*list.Element

	oldItems map[uint64]*list.Element

	evictYoungList *list.List

	evictOldList *list.List

	evictList *list.List

	oldPercent   float64
	youngPercent float64
}

func (L *LRUCacheImpl) Set(spaceId uint32, pageNo uint32, value *BufferBlock) error {

	if L.Len() < 512 {
		L.setOrdinary(spaceId, pageNo, value)
		return nil
	}
	if L.Len() == 512 {
		L.mu.Lock()
		i := 0
		for e := L.evictList.Front(); e != nil; e = e.Next() {
			if i < int(512*L.youngPercent) {
				currentLruItem := e.Value.(*lruItem)
				L.youngItems[currentLruItem.key] = L.evictYoungList.PushBack(currentLruItem)
			} else {
				currentLruItem := e.Value.(*lruItem)
				L.oldItems[currentLruItem.key] = L.evictOldList.PushBack(currentLruItem)
			}
			i++
		}
		L.evictList = list.New()
		L.items = make(map[uint64]*list.Element)
		L.mu.Unlock()

	}
	L.SetOld(spaceId, pageNo, value)
	return nil
}

func (L *LRUCacheImpl) Get(spaceId uint32, pageNo uint32) (*BufferBlock, error) {
	if result, keyNotFoundError := L.GetYoung(spaceId, pageNo); keyNotFoundError == nil {
		return result, nil
	}
	if result, keyNotFoundError := L.GetOld(spaceId, pageNo); keyNotFoundError == nil {
		return result, nil
	}
	if result, keyNotFoundError := L.getOrdinary(spaceId, pageNo); keyNotFoundError == nil {
		return result, nil
	}
	return nil, KeyNotFoundError
}

func (L *LRUCacheImpl) Remove(spaceId uint32, pageNo uint32) bool {
	L.mu.Lock()
	defer L.mu.Unlock()
	var buff = append(util.ConvertUInt4Bytes(spaceId), util.ConvertUInt4Bytes(pageNo)...)
	hashCode := util.HashCode(buff)
	return L.remove(hashCode)
}
func (L *LRUCacheImpl) remove(key uint64) bool {
	if ent, ok := L.youngItems[key]; ok {
		L.removeYoungElement(ent)
		return true
	}
	if ent, ok := L.oldItems[key]; ok {
		L.removeOldElement(ent)
		return true
	}
	return false
}

func (L *LRUCacheImpl) Purge() {
	L.mu.Lock()
	defer L.mu.Unlock()

	if L.purgeVisitorFunc != nil {
		for key, item := range L.youngItems {
			it := item.Value.(*lruItem)
			v := it.value
			L.purgeVisitorFunc(key, v)
		}
		for key, item := range L.oldItems {
			it := item.Value.(*lruItem)
			v := it.value
			L.purgeVisitorFunc(key, v)
		}
	}

}

func (L *LRUCacheImpl) Has(spaceId uint32, pageNo uint32) bool {
	panic("implement me")
}

//TODO 校验这里的hashcode的安全性
func (L LRUCacheImpl) SetYoung(spaceId uint32, pageNo uint32, value *BufferBlock) {
	L.mu.Lock()
	defer L.mu.Unlock()
	var buff = append(util.ConvertUInt4Bytes(spaceId), util.ConvertUInt4Bytes(pageNo)...)
	hashCode := util.HashCode(buff)

	var item *lruItem
	if it, ok := L.youngItems[hashCode]; ok {
		L.evictYoungList.MoveToFront(it)
		item = it.Value.(*lruItem)
		item.value = value
	} else {
		if L.evictYoungList.Len() >= int(math.Pow10(L.size)*L.youngPercent) {
			L.evictYoung(1)
		}
		item = &lruItem{
			key:   hashCode,
			value: value,
		}
		L.youngItems[hashCode] = L.evictYoungList.PushFront(item)
	}

}

// evict removes the oldest item from the cache.
func (L *LRUCacheImpl) evictYoung(count int) {
	for i := 0; i < count; i++ {
		ent := L.evictYoungList.Back()
		if ent == nil {
			return
		} else {
			L.removeYoungElement(ent)
		}
	}
}
func (c *LRUCacheImpl) removeYoungElement(e *list.Element) {
	c.evictYoungList.Remove(e)
	entry := e.Value.(*lruItem)
	delete(c.items, entry.key)
	if c.evictedFunc != nil {
		entry := e.Value.(*lruItem)
		c.evictedFunc(entry.key, entry.value)
	}
}

// evict removes the oldest item from the cache.
func (L *LRUCacheImpl) evict(count int) {
	for i := 0; i < count; i++ {
		ent := L.evictList.Back()
		if ent == nil {
			return
		} else {
			L.removeElement(ent)
		}
	}
}
func (c *LRUCacheImpl) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	entry := e.Value.(*lruItem)
	delete(c.items, entry.key)
	if c.evictedFunc != nil {
		entry := e.Value.(*lruItem)
		c.evictedFunc(entry.key, entry.value)
	}
}

// evict removes the oldest item from the cache.
func (L *LRUCacheImpl) evictOld(count int) {
	for i := 0; i < count; i++ {
		ent := L.evictOldList.Back()
		if ent == nil {
			return
		} else {
			L.removeOldElement(ent)
		}
	}
}
func (c *LRUCacheImpl) removeOldElement(e *list.Element) {
	c.evictOldList.Remove(e)
	entry := e.Value.(*lruItem)
	delete(c.items, entry.key)
	if c.evictedFunc != nil {
		entry := e.Value.(*lruItem)
		c.evictedFunc(entry.key, entry.value)
	}
}

func (L *LRUCacheImpl) GetYoung(spaceId uint32, pageNo uint32) (*BufferBlock, error) {
	L.mu.Lock()
	defer L.mu.Unlock()
	var buff = append(util.ConvertUInt4Bytes(spaceId), util.ConvertUInt4Bytes(pageNo)...)
	hashCode := util.HashCode(buff)
	return L.getYoungValue(hashCode, false)
}

func (L *LRUCacheImpl) getYoungValue(key uint64, onLoad bool) (*BufferBlock, error) {
	L.mu.Lock()
	item, ok := L.youngItems[key]
	if ok {
		it := item.Value.(*lruItem)
		L.evictYoungList.MoveToFront(item)
		v := it.value
		L.mu.Unlock()
		if !onLoad {
			L.stats.IncrHitCount()
		}
		return v, nil
	}
	L.mu.Unlock()
	if !onLoad {
		L.stats.IncrMissCount()
	}
	return nil, KeyNotFoundError
}

func (L *LRUCacheImpl) SetOld(spaceId uint32, pageNo uint32, value *BufferBlock) {
	L.mu.Lock()
	defer L.mu.Unlock()
	var buff = append(util.ConvertUInt4Bytes(spaceId), util.ConvertUInt4Bytes(pageNo)...)
	hashCode := util.HashCode(buff)

	var item *lruItem
	if it, ok := L.oldItems[hashCode]; ok {
		L.evictOldList.MoveToFront(it)
		item = it.Value.(*lruItem)
		item.value = value
	} else {
		if L.evictOldList.Len() >= int(math.Pow10(L.size)*L.oldPercent) {
			L.evictYoung(1)
		}
		item = &lruItem{
			key:   hashCode,
			value: value,
		}
		L.oldItems[hashCode] = L.evictOldList.PushFront(item)
	}

}
func (L *LRUCacheImpl) setOrdinary(spaceId uint32, pageNo uint32, value *BufferBlock) {
	L.mu.Lock()
	defer L.mu.Unlock()
	var buff = append(util.ConvertUInt4Bytes(spaceId), util.ConvertUInt4Bytes(pageNo)...)
	hashCode := util.HashCode(buff)

	var item *lruItem
	if it, ok := L.items[hashCode]; ok {
		L.evictList.MoveToFront(it)
		item = it.Value.(*lruItem)
		item.value = value
	} else {
		if L.evictList.Len() >= L.size {
			L.evict(1)
		}
		item = &lruItem{
			key:   hashCode,
			value: value,
		}
		L.items[hashCode] = L.evictList.PushFront(item)
	}

}
func (L *LRUCacheImpl) getOrdinary(spaceId uint32, pageNo uint32) (*BufferBlock, error) {
	L.mu.Lock()
	defer L.mu.Unlock()
	var buff = append(util.ConvertUInt4Bytes(spaceId), util.ConvertUInt4Bytes(pageNo)...)
	hashCode := util.HashCode(buff)
	return L.getOrdinaryValue(hashCode, false)
}

func (L *LRUCacheImpl) getOrdinaryValue(key uint64, onLoad bool) (*BufferBlock, error) {
	L.mu.Lock()
	item, ok := L.items[key]
	if ok {
		it := item.Value.(*lruItem)
		L.evictList.MoveToFront(item)
		v := it.value
		L.mu.Unlock()
		if !onLoad {
			L.stats.IncrHitCount()
		}
		return v, nil
	}
	L.mu.Unlock()
	if !onLoad {
		L.stats.IncrMissCount()
	}
	return nil, KeyNotFoundError
}

func (L *LRUCacheImpl) GetOld(spaceId uint32, pageNo uint32) (*BufferBlock, error) {
	L.mu.Lock()
	defer L.mu.Unlock()
	var buff = append(util.ConvertUInt4Bytes(spaceId), util.ConvertUInt4Bytes(pageNo)...)
	hashCode := util.HashCode(buff)
	return L.getOldValue(hashCode, false)
}
func (L *LRUCacheImpl) Len() uint32 {
	if L.evictList.Len() > 0 {
		return uint32(L.evictList.Len())
	}
	return uint32(L.evictOldList.Len() + L.evictYoungList.Len())
}
func (L *LRUCacheImpl) getOldValue(key uint64, onLoad bool) (*BufferBlock, error) {
	L.mu.Lock()
	item, ok := L.oldItems[key]
	if ok {
		it := item.Value.(*lruItem)
		L.evictOldList.MoveToFront(item)
		v := it.value
		L.mu.Unlock()
		if !onLoad {
			L.stats.IncrHitCount()
		}
		return v, nil
	}
	L.mu.Unlock()
	if !onLoad {
		L.stats.IncrMissCount()
	}
	return nil, KeyNotFoundError
}
func NewLRUCacheImpl(size int, youngPercent float64, oldPercent float64, innodbOldBlocksTime int) LRUCache {
	var lrucache = new(LRUCacheImpl)
	lrucache.evictYoungList = list.New()
	lrucache.evictOldList = list.New()
	lrucache.evictList = list.New()
	lrucache.size = size
	lrucache.items = make(map[uint64]*list.Element, 0)
	lrucache.youngItems = make(map[uint64]*list.Element, 0)
	lrucache.oldItems = make(map[uint64]*list.Element, 0)
	lrucache.youngPercent = youngPercent
	lrucache.oldPercent = oldPercent
	lrucache.innodbOldBlocksTime = innodbOldBlocksTime
	return lrucache
}

type lruItem struct {
	key   uint64
	value *BufferBlock

	firstVisitTime uint64
	lastVisitTime  uint64
}
