package manager

import (
	"bytes"
	"testing"
)

func TestCompressedStorageProviderUsesFixedPageEnvelope(t *testing.T) {
	inner := newEncryptedProviderTestStorage()
	compression := NewCompressionManager()
	compression.SetCompressionSettings(7, &CompressionSettings{
		SpaceID:    7,
		Method:     COMPRESSION_ZLIB,
		Level:      COMPRESSION_LEVEL_DEFAULT,
		MinSavings: 0,
	})
	provider, err := NewCompressedStorageProvider(inner, compression, 32, 7)
	if err != nil {
		t.Fatal(err)
	}

	plain := bytes.Repeat([]byte("x"), 32)
	if err := provider.WritePage(7, 3, plain); err != nil {
		t.Fatal(err)
	}
	raw, err := inner.ReadPage(7, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 32 || bytes.Equal(raw, plain) {
		t.Fatalf("inner page = %d bytes, equal plaintext=%v; want fixed compressed envelope", len(raw), bytes.Equal(raw, plain))
	}
	got, err := provider.ReadPage(7, 3)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("decompressed page = %q, %v; want %q", got, err, plain)
	}
	info, err := provider.GetSpaceInfo(7)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsCompressed {
		t.Fatal("compressed space metadata must report IsCompressed")
	}
}
