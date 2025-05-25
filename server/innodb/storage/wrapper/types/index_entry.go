package types

import (
	"encoding/binary"
)

// IndexEntry represents an entry in the index page
type IndexEntry struct {
	Key    Key
	PageNo uint32
	RowID  uint64
}

// ParseBytes parses the entry from bytes
func (e *IndexEntry) ParseBytes(data []byte) error {
	if len(data) < 12 {
		return ErrInvalidEntrySize
	}

	e.PageNo = binary.LittleEndian.Uint32(data[0:4])
	e.RowID = binary.LittleEndian.Uint64(data[4:12])

	// Parse key
	keyLen := binary.LittleEndian.Uint16(data[12:14])
	if len(data) < 14+int(keyLen) {
		return ErrInvalidEntrySize
	}

	key := make([]byte, keyLen)
	copy(key, data[14:14+keyLen])
	e.Key = NewKey(key)

	return nil
}

// GetBytes returns the entry as bytes
func (e *IndexEntry) GetBytes() []byte {
	keyBytes := e.Key.GetBytes()
	keyLen := len(keyBytes)

	data := make([]byte, 14+keyLen)

	binary.LittleEndian.PutUint32(data[0:4], e.PageNo)
	binary.LittleEndian.PutUint64(data[4:12], e.RowID)
	binary.LittleEndian.PutUint16(data[12:14], uint16(keyLen))
	copy(data[14:], keyBytes)

	return data
}

// Size returns the size of the entry in bytes
func (e *IndexEntry) Size() int {
	return 14 + len(e.Key.GetBytes())
}
