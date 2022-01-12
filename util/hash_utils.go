package util

import (
	"github.com/OneOfOne/xxhash"
)

// 将一个键进行Hash
func HashCode(key []byte) uint64 {
	h := xxhash.New64()
	h.Write(key)
	return h.Sum64()
}
