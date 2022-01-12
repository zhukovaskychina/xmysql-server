package basic

//go:generate mockgen -source=btree.go -destination ./test/btree_test.go -package store_test
type Iterator func() (uint32, Value, Row, error, Iterator)

type RowItemsIterator func() (Row, error, RowItemsIterator)

type XMySQLSegment interface {
	//获取扫描的数量
	GetStatsCost(startPageNo, endPageNo uint32) map[string]int64
}

type Tree interface {
	Keys() (RowItemsIterator, error)

	Values() (RowItemsIterator, error)

	Iterate() (Iterator, error)

	Backward() (Iterator, error)

	Find(key Value) (Iterator, error)

	DoFind(key Value, do func(key Value, value Row) error) error

	Range(from, to Value) (Iterator, error)

	DoRange(from, to Value, do func(Value, Row) error) error

	Has(key Value) (bool, error)

	Count(key Value) (int, error)

	Add(key Value, value Row) error

	Remove(key []byte, where func([]byte) bool) error

	TREESize() int

	GetDataSegment() XMySQLSegment

	GetInternalSegment() XMySQLSegment
}
