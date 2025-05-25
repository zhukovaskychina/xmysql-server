package basic

// IIterator defines the interface for iterating over key-value pairs
type IIterator interface {
	// Next advances the iterator to the next key-value pair
	// Returns false if there are no more items
	Next() bool

	// Key returns the current key
	Key() []byte

	// Value returns the current value
	Value() interface{}

	// Error returns any error that occurred during iteration
	Error() error

	// Close releases any resources associated with the iterator
	Close() error
}

// IRangeIterator defines the interface for range iteration
type IRangeIterator interface {
	IIterator

	// Seek moves the iterator to the first key greater than or equal to the given key
	Seek(key []byte) bool

	// Prev moves the iterator to the previous key-value pair
	// Returns false if there are no more items
	Prev() bool
}

// RowIterator defines the interface for iterating over table rows
type RowIterator interface {
	// Next advances the iterator to the next row
	// Returns false if there are no more rows
	Next() bool

	// Row returns the current row
	Row() Row

	// Error returns any error that occurred during iteration
	Error() error

	// Close releases any resources associated with the iterator
	Close() error
}
