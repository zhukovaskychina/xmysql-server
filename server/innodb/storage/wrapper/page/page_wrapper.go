package page

import (
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/buffer_pool"
)

// PageWrapper 页面包装器接口
type PageWrapper interface {
	// 基本信息
	GetID() uint32
	GetSpaceID() uint32
	GetPageNo() uint32
	GetPageType() common.PageType

	// 内容访问
	GetContent() []byte
	SetContent([]byte) error

	// Buffer Pool支持
	GetBufferPage() *buffer_pool.BufferPage
	SetBufferPage(*buffer_pool.BufferPage)

	// 持久化
	Read() error  // 从磁盘或buffer pool读取
	Write() error // 写入buffer pool和磁盘

	// 生命周期
	Init() error
	Release() error
}
