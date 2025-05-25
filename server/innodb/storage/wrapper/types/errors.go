package types

import "errors"

var (
	// ErrInvalidHeaderSize is returned when the header size is invalid
	ErrInvalidHeaderSize = errors.New("invalid header size")

	// ErrInvalidEntrySize is returned when the entry size is invalid
	ErrInvalidEntrySize = errors.New("invalid entry size")
)
