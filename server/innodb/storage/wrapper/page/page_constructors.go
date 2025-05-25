package page

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/system"
)

// NewInodePageWrapper 创建INode页面包装器 - 映射到page_inode_wrapper.go中的INode
func NewInodePageWrapper(id, spaceID uint32) *system.INode {
	return system.NewINode(spaceID, id)
}

// NewIBufPageWrapper 创建IBuf页面包装器
func NewIBufPageWrapper(id, spaceID uint32) *IBufPageWrapper {
	return &IBufPageWrapper{
		BasePageWrapper: NewBasePageWrapper(id, spaceID, common.FIL_PAGE_IBUF_FREE_LIST),
	}
}

// NewAllocatePageWrapper 创建已分配页面包装器
func NewAllocatePageWrapper(id, spaceID uint32) *AllocatedPageWrapper {
	return &AllocatedPageWrapper{
		BasePageWrapper: NewBasePageWrapper(id, spaceID, common.FIL_PAGE_TYPE_ALLOCATED),
	}
}

// IBufPageWrapper IBuf页面包装器
type IBufPageWrapper struct {
	*BasePageWrapper
}

func (p *IBufPageWrapper) ParseFromBytes(content []byte) error {
	return p.BasePageWrapper.ParseFromBytes(content)
}

func (p *IBufPageWrapper) ToBytes() ([]byte, error) {
	return p.BasePageWrapper.ToBytes()
}

// AllocatedPageWrapper 已分配页面包装器
type AllocatedPageWrapper struct {
	*BasePageWrapper
}

func (p *AllocatedPageWrapper) ParseFromBytes(content []byte) error {
	return p.BasePageWrapper.ParseFromBytes(content)
}

func (p *AllocatedPageWrapper) ToBytes() ([]byte, error) {
	return p.BasePageWrapper.ToBytes()
}

// CreateDataDictionaryPageWrapper 创建数据字典页面包装器
func CreateDataDictionaryPageWrapper(id, spaceID uint32, bp *buffer_pool.BufferPool) *DataDictionaryPageWrapper {
	return NewDataDictionaryPageWrapper(id, spaceID, bp)
}

// CreateFSPPageWrapper 创建表空间页面包装器
func CreateFSPPageWrapper(id, spaceID uint32, bp *buffer_pool.BufferPool) *FSPPageWrapper {
	return NewFSPPageWrapper(id, spaceID, bp)
}

// CreateIBufBitmapPageWrapper 创建Insert Buffer位图页面包装器
func CreateIBufBitmapPageWrapper(id, spaceID uint32, bp *buffer_pool.BufferPool) *IBufBitmapPageWrapper {
	return NewIBufBitmapPageWrapper(id, spaceID, bp)
}

// CreateXDESPageWrapper 创建扩展描述符页面包装器
func CreateXDESPageWrapper(id, spaceID uint32, bp *buffer_pool.BufferPool) *XDESPageWrapper {
	return NewXDESPageWrapper(id, spaceID, bp)
}

// CreateTrxSysPageWrapper 创建事务系统页面包装器
func CreateTrxSysPageWrapper(id, spaceID uint32, bp *buffer_pool.BufferPool) *TrxSysPageWrapper {
	return NewTrxSysPageWrapper(id, spaceID, bp)
}
