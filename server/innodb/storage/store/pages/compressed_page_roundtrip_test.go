package pages

import (
	"bytes"
	"testing"
)

func TestCompressedPage_SerializeAndValidate(t *testing.T) {
	cp := NewCompressedPage(1, 3, CompressionZLIB)
	orig := bytes.Repeat([]byte("A"), 256)
	if err := cp.CompressData(orig); err != nil {
		t.Fatalf("CompressData error: %v", err)
	}

	s := NewDefaultPageSerializer()
	data, err := s.Serialize(cp)
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}

	checker := NewPageIntegrityChecker(ChecksumCRC32)
	if err := checker.ValidatePage(data); err != nil {
		t.Fatalf("ValidatePage error: %v", err)
	}

	// STG-001: 压缩/解压往返
	dec, err := cp.DecompressData()
	if err != nil {
		t.Fatalf("DecompressData error: %v", err)
	}
	if !bytes.Equal(dec, orig) {
		t.Errorf("decompressed data != original (len %d vs %d)", len(dec), len(orig))
	}
	ratio := cp.GetCompressionRatio()
	if ratio <= 0 || ratio > 1 {
		t.Errorf("compression ratio should be in (0,1], got %f", ratio)
	}
	t.Logf("compress/decompress roundtrip ok, ratio=%.2f", ratio)
}
