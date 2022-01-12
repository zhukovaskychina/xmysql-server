package basic

//定义迭代
type RowIterator interface {
	Open() error

	GetCurrentRow() (Row, error)

	Next() (Row, error)

	Close() error
}
