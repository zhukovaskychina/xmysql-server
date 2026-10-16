package manager

import (
	"bytes"
	"testing"
)

func TestCompressedStorageProviderUsesFixedPageEnvelope(t *testing.T) {
	// A tiny zlib stream can exceed its input size. Use enough repeated data
	// to exercise the compressed-envelope branch across Go toolchains.
	const pageSize = 512
	inner := newEncryptedProviderTestStorage()
	compression := NewCompressionManager()
	compression.SetCompressionSettings(7, &CompressionSettings{
		SpaceID:    7,
		Method:     COMPRESSION_ZLIB,
		Level:      COMPRESSION_LEVEL_DEFAULT,
		MinSavings: 0,
	})
	provider, err := NewCompressedStorageProvider(inner, compression, pageSize, 7)
	if err != nil {
		t.Fatal(err)
	}

	plain := bytes.Repeat([]byte("x"), pageSize)
	if err := provider.WritePage(7, 3, plain); err != nil {
		t.Fatal(err)
	}
	raw, err := inner.ReadPage(7, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != pageSize || bytes.Equal(raw, plain) {
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

func TestCompressedStorageProviderRetainsPlainPageWhenCompressionWouldGrow(t *testing.T) {
	const pageSize = 32
	inner := newEncryptedProviderTestStorage()
	compression := NewCompressionManager()
	compression.SetCompressionSettings(7, &CompressionSettings{
		SpaceID: 7, Method: COMPRESSION_ZLIB,
		Level: COMPRESSION_LEVEL_DEFAULT, MinSavings: 0,
	})
	provider, err := NewCompressedStorageProvider(inner, compression, pageSize, 7)
	if err != nil {
		t.Fatal(err)
	}
	// Distinct bytes offer no repeated runs to offset the stream/header cost.
	plain := make([]byte, pageSize)
	for i := range plain {
		plain[i] = byte(i)
	}
	if err := provider.WritePage(7, 3, plain); err != nil {
		t.Fatal(err)
	}
	raw, err := inner.ReadPage(7, 3)
	if err != nil || !bytes.Equal(raw, plain) {
		t.Fatalf("stored short page = %x, %v; want original fixed-size page", raw, err)
	}
	got, err := provider.ReadPage(7, 3)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("short page read = %x, %v; want original bytes", got, err)
	}
}
