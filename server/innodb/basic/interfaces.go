package basic

// IBufferPool represents a buffer pool interface
type IBufferPool interface {
	GetPage(spaceID, pageNo uint32) (IPage, error)
	NewPage(spaceID, pageNo uint32, pageType PageType) (IPage, error)
	FreePage(spaceID, pageNo uint32) error
	Flush() error
	Close() error
}

// IPage represents a unified page interface
// 统一的页面接口，合并了原来的Page和IPage接口的所有功能
type IPage interface {
	// ========================================
	// 基本信息获取
	// ========================================
	GetPageID() uint32     // 获取页面ID (原Page.ID())
	GetPageNo() uint32     // 获取页面号 (与GetPageID相同，保持兼容性)
	GetSpaceID() uint32    // 获取表空间ID
	GetPageType() PageType // 获取页面类型
	GetSize() uint32       // 获取页面大小

	// ========================================
	// 数据访问
	// ========================================
	GetData() []byte           // 获取页面数据
	GetContent() []byte        // 获取页面内容 (与GetData相同，保持兼容性)
	SetData(data []byte) error // 设置页面数据
	SetContent(content []byte) // 设置页面内容 (兼容性方法)

	// ========================================
	// 状态管理
	// ========================================
	IsDirty() bool            // 检查是否为脏页
	SetDirty(dirty bool)      // 设置脏页状态
	MarkDirty()               // 标记为脏页
	ClearDirty()              // 清除脏页标记
	GetState() PageState      // 获取页面状态
	SetState(state PageState) // 设置页面状态

	// ========================================
	// LSN (Log Sequence Number) 管理
	// ========================================
	GetLSN() uint64    // 获取日志序列号
	SetLSN(lsn uint64) // 设置日志序列号

	// ========================================
	// 缓冲池管理
	// ========================================
	Pin()   // 固定页面在缓冲池中
	Unpin() // 取消固定页面

	// ========================================
	// IO 操作
	// ========================================
	Read() error  // 从磁盘读取页面
	Write() error // 将页面写入磁盘

	// ========================================
	// 页面类型检查
	// ========================================
	IsLeafPage() bool // 检查是否为叶子页面

	// ========================================
	// 生命周期管理
	// ========================================
	Init() error // 初始化页面
	Release()    // 释放页面资源
}
