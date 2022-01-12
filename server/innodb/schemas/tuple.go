package schemas

type TupleLRUCache interface {

	//lru 中设置spaceId,pageNo
	Set(databaseName string, tableName string, table Table) error

	Get(databaseName string, tableName string) (Table, error)

	Remove(databaseName string, tableName string) bool

	// Has returns true if the key exists in the cache.
	Has(databaseName string, tableName string) bool

	Len() uint32
}
