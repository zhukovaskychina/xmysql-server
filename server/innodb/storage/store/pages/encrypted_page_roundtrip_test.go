package pages

import (
	"bytes"
	"testing"
)

func TestEncryptedPage_SerializeAndValidate(t *testing.T) {
	ep := NewEncryptedPage(1, 5, EncryptionAES128CTR, 1, 1)
	key := bytes.Repeat([]byte{0x11}, 16)
	data := bytes.Repeat([]byte("z"), 128)
	if err := ep.EncryptData(data, key); err != nil {
		t.Fatalf("EncryptData error: %v", err)
	}

	s := NewDefaultPageSerializer()
	serialized, err := s.Serialize(ep)
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}

	checker := NewPageIntegrityChecker(ChecksumCRC32)
	if err := checker.ValidatePage(serialized); err != nil {
		t.Fatalf("ValidatePage error: %v", err)
	}
}
