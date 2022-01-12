package store

import (
	"container/list"
)

//用于构造ibd的extent链表，用于管理各种链表
type ExtentList struct {
	list           *list.List
	extentListType string
}

func NewExtentList(extentListType string) *ExtentList {
	var extentList = new(ExtentList)
	extentList.extentListType = extentListType
	extentList.list = list.New()
	return extentList
}

func (el *ExtentList) AddExtent(extent Extent) {
	el.list.PushBack(extent)
}

func (el *ExtentList) DequeFirstElement() Extent {
	element := el.list.Front()
	extent := element.Value
	el.list.Remove(element)
	return extent.(Extent)
}
func (el *ExtentList) GetFirstElement() Extent {
	element := el.list.Front()
	extent := element.Value
	return extent.(Extent)
}
func (el *ExtentList) GetLastElement() Extent {
	element := el.list.Back()
	extent := element.Value
	return extent.(Extent)
}

func (el *ExtentList) IsEmpty() bool {
	return el.list.Len() == 0
}

func (el *ExtentList) Size() int {
	return el.list.Len()
}

//
func (el *ExtentList) RangeCost(startExtentNumber, endExtentNumber uint32) int {
	var result = 0
	for e := el.list.Front(); e != nil; e = e.Next() {
		if e.Value.(Extent).GetExtentId() >= startExtentNumber && e.Value.(Extent).GetExtentId() <= endExtentNumber {
			result = result + 1
		}
	}
	return result
}
