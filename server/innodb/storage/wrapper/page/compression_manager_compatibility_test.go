package page

import (
	"bytes"
	"testing"
)

func TestCompressionManagerZSTDUsesZSTDFrameAndRoundTrips(t *testing.T) {
	manager := NewCompressionManager(&CompressionConfig{
		Algorithm:           CompressionAlgoZSTD,
		EnableCompression:   true,
		MinCompressionRatio: 0,
		CacheSize:           8,
		CompressionLevel:    3,
	})
	pattern := []byte("zstd compatibility payload: " + repeatedCompressionPayload)
	input := bytes.Repeat(pattern, PageSize/len(pattern))
	input = append(input, make([]byte, PageSize-len(input))...)

	compressed, err := manager.CompressPage(1, input)
	if err != nil {
		t.Fatalf("CompressPage() error = %v", err)
	}
	if len(compressed) < 4 || compressed[0] != 0x28 || compressed[1] != 0xb5 || compressed[2] != 0x2f || compressed[3] != 0xfd {
		t.Fatalf("compressed payload has non-ZSTD frame header: %x", compressed[:minInt(4, len(compressed))])
	}
	decoded, err := manager.DecompressPage(1, compressed)
	if err != nil {
		t.Fatalf("DecompressPage() error = %v", err)
	}
	if string(decoded) != string(input) {
		t.Fatalf("decoded payload = %q, want %q", decoded, input)
	}
}

const repeatedCompressionPayload = "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789"

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
