package basic

// IPageWrapper 空接口，用于类型标记
// 实际的页面包装器接口定义在 server/innodb/storage/wrapper/types.IPageWrapper
// 保持此接口为空以避免循环依赖
type IPageWrapper interface{}

// IIndexPage defines the interface for index pages
type IIndexPage interface {
	IPageWrapper

	// Index specific methods
	IsLeaf() bool
	IsRoot() bool
	GetLevel() uint16

	// Record operations
	AddRow(row Row)
	DeleteRow(key Row, row Row)
	FindRow(row Row) (Row, bool)
	GetRows() []Row
}

// IPageManager defines the interface for page management
type IPageManager interface {
	// Get a page
	GetPage(spaceID, pageNo uint32) (IPageWrapper, error)

	// Allocate a new page
	AllocPage(spaceID uint32, pageType uint16) (IPageWrapper, error)

	// Free a page
	FreePage(spaceID, pageNo uint32) error

	// Sync changes to disk
	Sync() error
}
