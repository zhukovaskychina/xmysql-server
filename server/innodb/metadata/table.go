package metadata

// TODO: This file conflicts with Table struct in schema.go
// Commenting out to avoid compilation errors

/*
import (
	"xmysql-server/server/innodb/basic"
)

type TableInterface interface {
	TableName() string

	TableId() uint64

	SpaceId() uint32

	ColNums() int

	RowIter() (basic.RowIterator, error)

	GetBtree(indexName string) basic.Tree

	CheckFieldName(fieldName string) bool
}
*/
