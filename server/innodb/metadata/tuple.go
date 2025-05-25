package metadata

// RecordColumnInfo represents column information for record wrapper compatibility
type RecordColumnInfo struct {
	FieldType   string
	FieldLength int
}

// RecordTableRowTuple interface for record wrapper compatibility
type RecordTableRowTuple interface {
	GetColumnLength() int
	GetColumnInfos(index byte) RecordColumnInfo
	GetVarColumns() []RecordColumnInfo
}

type TupleLRUCache interface {

	//lru 中设置spaceId,pageNo
	Set(databaseName string, tableName string, table Table) error

	Get(databaseName string, tableName string) (Table, error)

	Remove(databaseName string, tableName string) bool

	// Has returns true if the key exists in the cache.
	Has(databaseName string, tableName string) bool

	Len() uint32
}
