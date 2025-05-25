package types

import (
	"encoding/binary"
)

const (
	// IndexPageHeaderSize is the size of the index page header in bytes
	IndexPageHeaderSize = 64
)

// IndexPageHeader represents the header of an index page
type IndexPageHeader struct {
	KeyCount   uint16
	Level      uint16
	IndexID    uint64
	LeftPage   uint32
	RightPage  uint32
	ParentPage uint32
	IsLeaf     bool
	IsRoot     bool
}

// ParseBytes parses the header from bytes
func (h *IndexPageHeader) ParseBytes(data []byte) error {
	if len(data) < IndexPageHeaderSize {
		return ErrInvalidHeaderSize
	}

	h.KeyCount = binary.LittleEndian.Uint16(data[38:40])
	h.Level = binary.LittleEndian.Uint16(data[40:42])
	h.IndexID = binary.LittleEndian.Uint64(data[42:50])
	h.LeftPage = binary.LittleEndian.Uint32(data[50:54])
	h.RightPage = binary.LittleEndian.Uint32(data[54:58])
	h.ParentPage = binary.LittleEndian.Uint32(data[58:62])
	h.IsLeaf = data[62] == 1
	h.IsRoot = data[63] == 1

	return nil
}

// GetBytes returns the header as bytes
func (h *IndexPageHeader) GetBytes() []byte {
	data := make([]byte, IndexPageHeaderSize)

	binary.LittleEndian.PutUint16(data[38:40], h.KeyCount)
	binary.LittleEndian.PutUint16(data[40:42], h.Level)
	binary.LittleEndian.PutUint64(data[42:50], h.IndexID)
	binary.LittleEndian.PutUint32(data[50:54], h.LeftPage)
	binary.LittleEndian.PutUint32(data[54:58], h.RightPage)
	binary.LittleEndian.PutUint32(data[58:62], h.ParentPage)
	if h.IsLeaf {
		data[62] = 1
	}
	if h.IsRoot {
		data[63] = 1
	}

	return data
}
