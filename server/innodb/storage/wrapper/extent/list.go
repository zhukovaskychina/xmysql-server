package extent

import (
	"errors"
	"sync"
	"xmysql-server/server/innodb/basic"
)

var (
	ErrExtentNotFound = errors.New("extent not found")
	ErrEmptyList      = errors.New("extent list is empty")
)

// ExtentNode 区链表节点
type ExtentNode struct {
	extent basic.Extent
	next   *ExtentNode
	prev   *ExtentNode
}

// BaseExtentList 基础区链表实现
type BaseExtentList struct {
	mu      sync.RWMutex
	head    *ExtentNode
	tail    *ExtentNode
	count   uint32
	extents map[uint32]basic.Extent
}

// NewBaseExtentList 创建基础区链表
func NewBaseExtentList() *BaseExtentList {
	return &BaseExtentList{
		extents: make(map[uint32]basic.Extent),
	}
}

// Add 添加区
func (el *BaseExtentList) Add(extent basic.Extent) error {
	el.mu.Lock()
	defer el.mu.Unlock()

	// 检查是否已存在
	if _, exists := el.extents[extent.GetID()]; exists {
		return nil
	}

	// 创建新节点
	node := &ExtentNode{
		extent: extent,
	}

	// 添加到链表
	if el.head == nil {
		el.head = node
		el.tail = node
	} else {
		node.prev = el.tail
		el.tail.next = node
		el.tail = node
	}

	// 添加到映射
	el.extents[extent.GetID()] = extent
	el.count++

	return nil
}

// Remove 移除区
func (el *BaseExtentList) Remove(extentID uint32) error {
	el.mu.Lock()
	defer el.mu.Unlock()

	// 查找区
	_, exists := el.extents[extentID]
	if !exists {
		return ErrExtentNotFound
	}

	// 从链表中移除
	node := el.head
	for node != nil {
		if node.extent.GetID() == extentID {
			if node.prev != nil {
				node.prev.next = node.next
			} else {
				el.head = node.next
			}
			if node.next != nil {
				node.next.prev = node.prev
			} else {
				el.tail = node.prev
			}
			break
		}
		node = node.next
	}

	// 从映射中移除
	delete(el.extents, extentID)
	el.count--

	return nil
}

// Get 获取区
func (el *BaseExtentList) Get(extentID uint32) (basic.Extent, error) {
	el.mu.RLock()
	defer el.mu.RUnlock()

	extent, exists := el.extents[extentID]
	if !exists {
		return nil, ErrExtentNotFound
	}

	return extent, nil
}

// GetAll 获取所有区
func (el *BaseExtentList) GetAll() []basic.Extent {
	el.mu.RLock()
	defer el.mu.RUnlock()

	extents := make([]basic.Extent, 0, len(el.extents))
	node := el.head
	for node != nil {
		extents = append(extents, node.extent)
		node = node.next
	}

	return extents
}

// IsEmpty 是否为空
func (el *BaseExtentList) IsEmpty() bool {
	el.mu.RLock()
	defer el.mu.RUnlock()
	return el.count == 0
}

// GetCount 获取区数量
func (el *BaseExtentList) GetCount() uint32 {
	el.mu.RLock()
	defer el.mu.RUnlock()
	return el.count
}

// GetTotalPages 获取总页面数
func (el *BaseExtentList) GetTotalPages() uint32 {
	el.mu.RLock()
	defer el.mu.RUnlock()

	var total uint32
	for _, extent := range el.extents {
		total += extent.GetPageCount()
	}
	return total
}

// GetFreePages 获取空闲页面数
func (el *BaseExtentList) GetFreePages() uint32 {
	el.mu.RLock()
	defer el.mu.RUnlock()

	var free uint32
	for _, extent := range el.extents {
		free += uint32(len(extent.GetFreePages()))
	}
	return free
}

// Iterator 获取迭代器
func (el *BaseExtentList) Iterator() basic.ExtentIterator {
	return &BaseExtentIterator{
		list: el,
		curr: el.head,
	}
}

// BaseExtentIterator 基础区迭代器实现
type BaseExtentIterator struct {
	list *BaseExtentList
	curr *ExtentNode
}

// Next 移动到下一个
func (ei *BaseExtentIterator) Next() bool {
	if ei.curr == nil {
		return false
	}
	ei.curr = ei.curr.next
	return ei.curr != nil
}

// Current 获取当前区
func (ei *BaseExtentIterator) Current() basic.Extent {
	if ei.curr == nil {
		return nil
	}
	return ei.curr.extent
}

// Reset 重置迭代器
func (ei *BaseExtentIterator) Reset() {
	ei.curr = ei.list.head
}

// Close 关闭迭代器
func (ei *BaseExtentIterator) Close() {
	ei.curr = nil
	ei.list = nil
}
