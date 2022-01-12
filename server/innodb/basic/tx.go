package basic

//TODO 定义事务
type XMySQLTransaction interface {

	//获取当前事务版本，该版本号由全局系统分配
	GetCurrentVersion() Version

	IsReadOnly() bool

	Commit() error
	// Rollback undoes the transaction operations to KV store.
	Rollback() error
	// String implements fmt.Stringer interface.
	String() string
}
