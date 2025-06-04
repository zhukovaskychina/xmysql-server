package util

import "testing"

func TestHashConsistency(t *testing.T) {
	data := []byte("788788")
	if HashCode(data) != HashCode(data) {
		t.Errorf("hash should be deterministic")
	}
}

func TestConvertInt4BytesRoundTrip(t *testing.T) {
	val := int32(2)
	buf := ConvertInt4Bytes(val)
	got := ReadUB4Byte2UInt32(buf)
	if uint32(val) != got {
		t.Fatalf("expected %d, got %d", val, got)
	}
}
