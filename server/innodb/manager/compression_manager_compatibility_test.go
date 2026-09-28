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

func TestCompressionManagerTracksSuccessfulDecompression(t *testing.T) {
	compression := NewCompressionManager()
	compression.SetCompressionSettings(7, &CompressionSettings{
		SpaceID: 7, Method: COMPRESSION_ZLIB, Level: COMPRESSION_LEVEL_DEFAULT, MinSavings: 0,
	})
	compression.SetPerIndexEnabled(true)
	compressed, err := compression.CompressPage(7, 1, bytes.Repeat([]byte("d"), 4096))
	if err != nil {
		t.Fatal(err)
	}
	decompressed, err := compression.DecompressPage(7, 1, compressed)
	if err != nil {
		t.Fatal(err)
	}
	if len(decompressed) != 4096 {
		t.Fatalf("unexpected decompressed length: %d", len(decompressed))
	}
	if got := compression.GetStatsForSpace(7).UncompressedPages; got != 1 {
		t.Fatalf("expected one successful decompression, got %d", got)
	}
	stats := compression.GetStatsForSpace(7)
	if stats.CompressDuration <= 0 || stats.UncompressDuration <= 0 {
		t.Fatalf("expected non-zero compression timing sources: %+v", stats)
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

func TestCompressionManagerKeepsPerSpaceStatistics(t *testing.T) {
	compression := NewCompressionManager()
	compression.SetCompressionSettings(7, &CompressionSettings{
		SpaceID: 7, Method: COMPRESSION_ZLIB, Level: COMPRESSION_LEVEL_DEFAULT, MinSavings: 0,
	})
	compression.SetCompressionSettings(8, &CompressionSettings{
		SpaceID: 8, Method: COMPRESSION_ZLIB, Level: COMPRESSION_LEVEL_DEFAULT, MinSavings: 0,
	})
	compression.SetPerIndexEnabled(true)

	_, err := compression.CompressPage(7, 1, bytes.Repeat([]byte("a"), 4096))
	if err != nil {
		t.Fatal(err)
	}
	_, err = compression.CompressPage(8, 1, bytes.Repeat([]byte("b"), 4096))
	if err != nil {
		t.Fatal(err)
	}

	spaceSeven := compression.GetStatsForSpace(7)
	if spaceSeven.TotalPages != 1 || spaceSeven.CompressedPages != 1 {
		t.Fatalf("unexpected space 7 stats: %+v", spaceSeven)
	}
	spaceEight := compression.GetStatsForSpace(8)
	if spaceEight.TotalPages != 1 || spaceEight.CompressedPages != 1 {
		t.Fatalf("unexpected space 8 stats: %+v", spaceEight)
	}
	if compression.GetStats().TotalPages != 2 {
		t.Fatalf("unexpected aggregate stats: %+v", compression.GetStats())
	}
}

func TestCompressionManagerResetSnapshotsAndClearsCounters(t *testing.T) {
	compression := NewCompressionManager()
	compression.SetCompressionSettings(7, &CompressionSettings{
		SpaceID: 7, Method: COMPRESSION_ZLIB, Level: COMPRESSION_LEVEL_DEFAULT, MinSavings: 0,
	})
	compression.SetCompressionSettings(8, &CompressionSettings{
		SpaceID: 8, Method: COMPRESSION_ZLIB, Level: COMPRESSION_LEVEL_DEFAULT, MinSavings: 0,
	})
	compression.SetPerIndexEnabled(true)

	_, err := compression.CompressPage(7, 1, bytes.Repeat([]byte("a"), 4096))
	if err != nil {
		t.Fatal(err)
	}
	_, err = compression.CompressPage(8, 1, bytes.Repeat([]byte("b"), 4096))
	if err != nil {
		t.Fatal(err)
	}

	aggregate := compression.GetStatsAndReset()
	if aggregate.TotalPages != 2 || aggregate.CompressedPages != 2 {
		t.Fatalf("unexpected aggregate reset snapshot: %+v", aggregate)
	}
	if got := compression.GetStats(); got != (CompressionStats{}) {
		t.Fatalf("aggregate counters were not cleared: %+v", got)
	}

	spaceSeven := compression.GetStatsForSpaceAndReset(7)
	if spaceSeven.TotalPages != 1 || spaceSeven.CompressedPages != 1 {
		t.Fatalf("unexpected space reset snapshot: %+v", spaceSeven)
	}
	if got := compression.GetStatsForSpace(7); got != (CompressionStats{}) {
		t.Fatalf("space counters were not cleared: %+v", got)
	}
	if got := compression.GetStatsForSpace(8); got.TotalPages != 1 {
		t.Fatalf("reset of space 7 affected space 8: %+v", got)
	}
}

func TestCompressionManagerGatesPerIndexCollection(t *testing.T) {
	compression := NewCompressionManager()
	compression.SetCompressionSettings(7, &CompressionSettings{
		SpaceID: 7, Method: COMPRESSION_ZLIB, Level: COMPRESSION_LEVEL_DEFAULT, MinSavings: 0,
	})

	_, err := compression.CompressPage(7, 1, bytes.Repeat([]byte("g"), 4096))
	if err != nil {
		t.Fatal(err)
	}
	if got := compression.GetStats().TotalPages; got != 1 {
		t.Fatalf("aggregate compression should remain enabled: %d", got)
	}
	if got := compression.GetStatsForSpace(7).TotalPages; got != 0 {
		t.Fatalf("per-index counters should remain disabled: %d", got)
	}

	compression.SetPerIndexEnabled(true)
	_, err = compression.CompressPage(7, 2, bytes.Repeat([]byte("h"), 4096))
	if err != nil {
		t.Fatal(err)
	}
	if got := compression.GetStatsForSpace(7).TotalPages; got != 1 {
		t.Fatalf("per-index counters should start after enabling: %d", got)
	}
}
