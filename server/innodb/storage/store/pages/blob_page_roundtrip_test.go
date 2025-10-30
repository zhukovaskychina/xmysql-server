package pages

import (
	"testing"
)

func TestBlobPage_SerializeAndValidate(t *testing.T) {
	bp := NewBlobPage(1, 42, 1234)
	if err := bp.SetBlobData([]byte("hello world"), 11, 0, 0); err != nil {
		t.Fatalf("SetBlobData error: %v", err)
	}

	s := NewDefaultPageSerializer()
	data, err := s.Serialize(bp)
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}

	checker := NewPageIntegrityChecker(ChecksumCRC32)
	if err := checker.ValidatePage(data); err != nil {
		t.Fatalf("ValidatePage error: %v", err)
	}
}
