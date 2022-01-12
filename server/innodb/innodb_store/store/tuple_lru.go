package store

import (
	"container/list"
	"sync"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/schemas"
	"xmysql-server/util"
)

type TupleLRUCacheImpl struct {
	size int

	mu        sync.RWMutex
	items     map[uint64]*list.Element
	evictList *list.List
}

func NewTupleLRUCache() schemas.TupleLRUCache {
	var tupleLRUCache = new(TupleLRUCacheImpl)
	tupleLRUCache.evictList = list.New()
	tupleLRUCache.size = 1024
	tupleLRUCache.items = make(map[uint64]*list.Element, 0)
	return tupleLRUCache
}

func (t TupleLRUCacheImpl) Set(databaseName string, tableName string, table schemas.Table) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var buff = []byte(databaseName + tableName)
	hashCode := util.HashCode(buff)
	var item *tuplelruItem
	if it, ok := t.items[hashCode]; ok {
		t.evictList.MoveToFront(it)
		item = it.Value.(*tuplelruItem)
		item.value = table
	} else {
		if t.evictList.Len() >= t.size {
			t.evict(1)
		}
		item = &tuplelruItem{
			key:   hashCode,
			value: table,
		}
		t.items[hashCode] = t.evictList.PushFront(item)
	}
	return nil
}

func (t *TupleLRUCacheImpl) Get(databaseName string, tableName string) (schemas.Table, error) {
	//t.mu.Lock()
	//defer t.mu.Unlock()
	var buff = []byte(string(databaseName + tableName))
	hashCode := util.HashCode(buff)
	return t.get(hashCode)
}

func (t *TupleLRUCacheImpl) get(key uint64) (schemas.Table, error) {
	t.mu.Lock()
	item, ok := t.items[key]
	if ok {
		it := item.Value.(*tuplelruItem)
		t.evictList.MoveToFront(item)
		v := it.value
		t.mu.Unlock()
		return v, nil
	}
	t.mu.Unlock()
	return nil, common.NewErr(common.ErrNoSuchTable)
}

func (t TupleLRUCacheImpl) Remove(databaseName string, tableName string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	var buff = []byte(databaseName + "/" + tableName)
	hashCode := util.HashCode(buff)
	return t.remove(hashCode)
}

func (t TupleLRUCacheImpl) Has(databaseName string, tableName string) bool {
	panic("implement me")
}
func (t *TupleLRUCacheImpl) remove(key uint64) bool {
	if ent, ok := t.items[key]; ok {
		t.removeElement(ent)
		return true
	}
	return false
}
func (t TupleLRUCacheImpl) Len() uint32 {
	panic("implement me")
}

// evict removes the oldest item from the cache.
func (t *TupleLRUCacheImpl) evict(count int) {
	for i := 0; i < count; i++ {
		ent := t.evictList.Back()
		if ent == nil {
			return
		} else {
			t.removeElement(ent)
		}
	}
}
func (t *TupleLRUCacheImpl) removeElement(e *list.Element) {
	t.evictList.Remove(e)
	entry := e.Value.(*tuplelruItem)
	delete(t.items, entry.key)
}

type tuplelruItem struct {
	key   uint64
	value schemas.Table

	firstVisitTime uint64
	lastVisitTime  uint64
}
