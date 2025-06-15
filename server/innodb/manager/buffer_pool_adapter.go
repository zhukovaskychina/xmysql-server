package manager

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

// BufferPoolManagerAdapter 适配器，让OptimizedBufferPoolManager兼容原有的BufferPoolManager接口
type BufferPoolManagerAdapter struct {
	optimized *OptimizedBufferPoolManager
}

// GetPage 获取页面
func (adapter *BufferPoolManagerAdapter) GetPage(spaceID, pageNo uint32) (*buffer_pool.BufferPage, error) {
	return adapter.optimized.GetPage(spaceID, pageNo)
}

// GetDirtyPage 获取页面并标记为脏页
func (adapter *BufferPoolManagerAdapter) GetDirtyPage(spaceID, pageNo uint32) (*buffer_pool.BufferPage, error) {
	return adapter.optimized.GetDirtyPage(spaceID, pageNo)
}

// FlushPage 刷新特定页面到磁盘
func (adapter *BufferPoolManagerAdapter) FlushPage(spaceID, pageNo uint32) error {
	return adapter.optimized.FlushPage(spaceID, pageNo)
}

// UnpinPage 减少页面的引用计数
func (adapter *BufferPoolManagerAdapter) UnpinPage(spaceID, pageNo uint32) error {
	return adapter.optimized.UnpinPage(spaceID, pageNo)
}

// MarkDirty 标记页面为脏页
func (adapter *BufferPoolManagerAdapter) MarkDirty(spaceID, pageNo uint32) error {
	return adapter.optimized.MarkDirty(spaceID, pageNo)
}

// FlushAllPages 刷新所有脏页
func (adapter *BufferPoolManagerAdapter) FlushAllPages() error {
	return adapter.optimized.FlushAllPages()
}

// GetStats 获取统计信息
func (adapter *BufferPoolManagerAdapter) GetStats() map[string]interface{} {
	return adapter.optimized.GetStats()
}

// Close 关闭缓冲池管理器
func (adapter *BufferPoolManagerAdapter) Close() error {
	return adapter.optimized.Close()
}
