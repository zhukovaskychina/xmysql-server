package store

import "container/list"

type INodeList struct {
	list     *list.List
	listType string
}

func NewINodeList(listType string) *INodeList {
	var list = list.New()
	var inodeList = new(INodeList)
	inodeList.list = list
	inodeList.listType = listType
	return inodeList
}

func (i *INodeList) AddINode(node *INode) {
	i.list.PushBack(node)
}
