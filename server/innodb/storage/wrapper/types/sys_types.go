package types

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

// SysTableSpace 系统表空间接口
type SysTableSpace interface {
	TableSpace
	GetBlockFile() basic.BlockFile
	GetBufferPool() *buffer_pool.BufferPool

	// 系统表接口
	GetSysTables() basic.IIndexWrapper
	GetSysColumns() basic.IIndexWrapper
	GetSysIndexes() basic.IIndexWrapper
	GetSysFields() basic.IIndexWrapper

	// 数据字典接口
	GetDataDict() *DataDictWrapper
}

// DataDictWrapper 数据字典包装器
type DataDictWrapper struct {
	MaxTableID uint64
	MaxIndexID uint64
	MaxSpaceID uint32
	MaxRowID   uint64
}
