package types

// TableManager 表管理器接口
type TableManager interface {
	CreateTable(databaseName string, tuple *TableTupleMeta) error
	DropTable(databaseName string, tableName string) error
	GetTable(databaseName string, tableName string) (*TableTupleMeta, error)
}

// TableSpace 表空间接口
type TableSpace interface {
	GetSpaceID() uint32
	GetPageSize() uint32
	GetPageCount() uint64
	AllocatePage() (uint32, error)
	FreePage(pageNo uint32) error
}
