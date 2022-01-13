package buffer_pool

import (
	"container/list"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/util"
)

type BufferPool struct {
	innodbBufferPoolSize uint64 //字节数量

	lruCache LRUCache

	freeBlockList *FreeBlockList //

	flushBlockList *FlushBlockList

	FileSystem basic.FileSystem
}
type FlushToDisk func(system basic.FileSystem, spaceId uint32, pageNo uint32, block BufferBlock)

//TODO 暂时实现一个，后面再有接着实现多个buffer instance
func NewBufferPool(innodbBufferPoolSize uint64, youngPercent float64, oldPercent float64, innodbOldBlocksTime int, system basic.FileSystem) *BufferPool {
	var bufferPool = new(BufferPool)
	bufferPool.innodbBufferPoolSize = innodbBufferPoolSize
	bufferPool.lruCache = NewLRUCacheImpl(int(innodbBufferPoolSize/16384), youngPercent, oldPercent, innodbOldBlocksTime)
	bufferPool.flushBlockList = NewFlushBlockList()
	bufferPool.freeBlockList = NewFreeBlockList(system)
	bufferPool.FileSystem = system
	return bufferPool
}

func (bufferPool *BufferPool) GetPageBlock(space uint32, pageNumber uint32) *BufferBlock {
	bufferBlock := bufferPool.freeBlockList.GetPage(space, pageNumber)
	bufferBlock.BufferPage.pageState = BUF_BLOCK_READY_FOR_USE
	bufferPool.lruCache.Set(space, pageNumber, bufferBlock)
	return bufferBlock
}
func (bufferPool *BufferPool) RangePageLoad(space uint32, pageNumberStart, pageNumberEnd uint32) {
	for i := pageNumberStart; i < pageNumberEnd; i++ {
		bufferPool.GetPageBlock(space, i)
	}
}

func (bufferPool *BufferPool) GetDirtyPageBlock(space uint32, pageNumber uint32) *BufferBlock {

	return nil
}

func (bufferPool *BufferPool) GetFlushDiskList() *FlushBlockList {
	return bufferPool.flushBlockList
}

//更新脏页面
func (bufferPool *BufferPool) UpdateBlock(space uint32, pageNumber uint32, block *BufferBlock) {
	bufferPool.lruCache.Remove(space, pageNumber)
	bufferPool.flushBlockList.AddBlock(block)
}

type FreeBlockList struct {
	FileSystem    basic.FileSystem
	list          *list.List
	mu            sync.RWMutex
	freePageItems map[uint64]*list.Element
}

func NewFreeBlockList(FileSystem basic.FileSystem) *FreeBlockList {
	var freeBlocklist = new(FreeBlockList)
	freeBlocklist.freePageItems = make(map[uint64]*list.Element)
	freeBlocklist.list = list.New()
	freeBlocklist.FileSystem = FileSystem
	return freeBlocklist
}

func (flb *FreeBlockList) AddBlock(spaceId uint32, pageNo uint32) {
	flb.mu.Lock()
	defer flb.mu.Unlock()
	var buff = append(util.ConvertUInt4Bytes(spaceId), util.ConvertUInt4Bytes(pageNo)...)
	hashCode := util.HashCode(buff)
	//没有就去加载
	if _, ok := flb.freePageItems[hashCode]; !ok {
		//需要fileSystem
		content, _ := flb.FileSystem.GetTableSpaceById(spaceId).LoadPageByPageNumber(pageNo)

		bufferBlock := NewBufferBlock(&content, spaceId, pageNo)

		flb.freePageItems[hashCode] = flb.list.PushBack(bufferBlock)
	}
}

func (flb *FreeBlockList) GetPage(spaceId uint32, pageNo uint32) *BufferBlock {
	var buff = append(util.ConvertUInt4Bytes(spaceId), util.ConvertUInt4Bytes(pageNo)...)
	hashCode := util.HashCode(buff)
	var element = flb.freePageItems[hashCode]
	//没有就去加载
	if _, ok := flb.freePageItems[hashCode]; !ok {
		//需要fileSystem
		content, _ := flb.FileSystem.GetTableSpaceById(spaceId).LoadPageByPageNumber(pageNo)
		bufferBlock := NewBufferBlock(&content, spaceId, pageNo)
		bufferBlock.BufferPage.pageState = BUF_BLOCK_NOT_USED
		element = flb.list.PushBack(bufferBlock)
		flb.freePageItems[hashCode] = element
	}
	var result = element.Value.(*BufferBlock)
	flb.list.Remove(element)
	delete(flb.freePageItems, hashCode)
	result.BufferPage.pageState = BUF_BLOCK_READY_FOR_USE
	return result
}

//脏页
type FlushBlockList struct {
	list list.List

	mu sync.RWMutex
}

func NewFlushBlockList() *FlushBlockList {
	var flushBlockList = new(FlushBlockList)
	return flushBlockList
}

func (flb *FlushBlockList) AddBlock(block *BufferBlock) {
	flb.mu.Lock()
	defer flb.mu.Unlock()
	flb.list.PushFront(block)
}

func (flb *FlushBlockList) IsEmpty() bool {
	return flb.list.Len() == 0
}

func (flb *FlushBlockList) GetLastBlock() *BufferBlock {
	flb.mu.Lock()
	defer flb.mu.Unlock()
	if flb.IsEmpty() {
		return nil
	}
	lastElement := flb.list.Back()
	flb.list.Remove(lastElement)
	return lastElement.Value.(*BufferBlock)
}
