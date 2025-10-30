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
}
