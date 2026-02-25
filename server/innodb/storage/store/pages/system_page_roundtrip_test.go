package pages

import "testing"

func TestSystemPage_SerializeAndValidate(t *testing.T) {
	sp := NewSystemPage(1, 7, 99)
	if err := sp.SetSystemData([]byte("sysdata")); err != nil {
		t.Fatalf("SetSystemData error: %v", err)
	}

	s := NewDefaultPageSerializer()
	data, err := s.Serialize(sp)
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}

	checker := NewPageIntegrityChecker(ChecksumCRC32)
	if err := checker.ValidatePage(data); err != nil {
		t.Fatalf("ValidatePage error: %v", err)
	}
}
