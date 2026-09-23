package manager

import (
	"bytes"
	"testing"
)

func TestCompressionManagerReturnsStableCompressedPageBytes(t *testing.T) {
	compression := NewCompressionManager()
	compression.SetCompressionSettings(7, &CompressionSettings{
		SpaceID:    7,
		Method:     COMPRESSION_ZLIB,
		Level:      COMPRESSION_LEVEL_DEFAULT,
		MinSavings: 0,
	})

	first, err := compression.CompressPage(7, 1, []byte("first page payload that should be compressed"))
	if err != nil {
		t.Fatal(err)
	}
	pooled := compression.bufferPool.Get().(*bytes.Buffer)
	pooled.Reset()
	_, _ = pooled.Write([]byte("overwritten by a later pooled operation"))
	compression.bufferPool.Put(pooled)
	second, err := compression.CompressPage(7, 2, []byte("second page payload that should also be compressed"))
	if err != nil {
		t.Fatal(err)
	}
	wantFirst, err := compression.DecompressPage(7, 1, first)
	if err != nil {
		t.Fatal(err)
	}
	if string(wantFirst) != "first page payload that should be compressed" {
		t.Fatalf("first compressed page changed after a later compression: %q", wantFirst)
	}
	if len(second) == 0 {
		t.Fatal("second compressed page is empty")
	}
}

func TestCompressionManagerRejectsTruncatedCompressedHeader(t *testing.T) {
	compression := NewCompressionManager()
	compression.SetCompressionSettings(7, &CompressionSettings{SpaceID: 7, Method: COMPRESSION_ZLIB})

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("DecompressPage panicked on truncated header: %v", recovered)
		}
	}()
	if _, err := compression.DecompressPage(7, 1, []byte{0xC0, 0x4D, 0x50, 0x52}); err == nil {
		t.Fatal("DecompressPage should reject a truncated compressed header")
	}
}
