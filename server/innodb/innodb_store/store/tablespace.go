package store

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

type TableSpace interface {
	basic.FileTableSpace
	//	LoadPageByPageNumber(pageNo uint32) ([]byte, error)

	GetSegINodeFullList() *INodeList

	GetSegINodeFreeList() *INodeList

	//初始化的时候第一个区64个页面都不再里面
	GetFspFreeExtentList() *ExtentList

	//有剩余空闲页面的碎片区，系统初始化的第一个区
	GetFspFreeFragExtentList() *ExtentList

	//没有剩余空闲页面的碎片区
	GetFspFullFragExtentList() *ExtentList

	//获取第一个Fsp
	GetFirstFsp() *Fsp

	//获取第一个INode
	GetFirstINode() *INode

	//	GetSpaceId() uint32

	LoadExtentFromDisk(extentNumber int) Extent

	//	FlushToDisk(pageNo uint32, content []byte)
}

/**
*
*
******/

type RowIter struct {
	index int
	rows  []basic.Row
}

func (r RowIter) Next() (basic.Row, error) {
	panic("implement me")
}

func (r RowIter) Close() error {
	panic("implement me")
}
