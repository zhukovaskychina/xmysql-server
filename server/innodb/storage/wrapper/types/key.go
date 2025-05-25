package types

import "bytes"

// Key represents a key in the index
type Key interface {
	GetBytes() []byte
	Compare(other Key) int
	GetLength() uint16
}

// BytesKey is a simple implementation of Key using []byte
type BytesKey struct {
	data []byte
}

// NewKey creates a new BytesKey
func NewKey(data []byte) Key {
	return &BytesKey{data: data}
}

// GetBytes returns the key as bytes
func (k *BytesKey) GetBytes() []byte {
	return k.data
}

// Compare compares this key with another key
func (k *BytesKey) Compare(other Key) int {
	otherBytes := other.GetBytes()
	return bytes.Compare(k.data, otherBytes)
}

// GetLength returns the length of the key in bytes
func (k *BytesKey) GetLength() uint16 {
	return uint16(len(k.data))
}
