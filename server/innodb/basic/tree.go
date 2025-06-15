package basic

//go:generate mockgen -source=btree.go -destination ./test/btree_test.go -package store_test
type Iterator func() (uint32, Value, Row, error, Iterator)

type RowItemsIterator func() (Row, error, RowItemsIterator)

type XMySQLSegment interface {
	//获取扫描的数量
	GetStatsCost(startPageNo, endPageNo uint32) map[string]int64
}
