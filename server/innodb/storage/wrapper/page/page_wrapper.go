package page

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/types"
)

// PageWrapper 页面包装器接口
//
// Deprecated: 使用 types.IPageWrapper 代替
// 此接口保留用于向后兼容，新代码应使用 types.IPageWrapper
type PageWrapper interface {
	types.IPageWrapper

	// Buffer Pool支持（扩展方法）
	GetBufferPage() *buffer_pool.BufferPage
	SetBufferPage(*buffer_pool.BufferPage)

	// 生命周期（扩展方法）
	Init() error
	Release() error

	// 内容访问（扩展方法）
	GetContent() []byte
	SetContent([]byte) error
}
