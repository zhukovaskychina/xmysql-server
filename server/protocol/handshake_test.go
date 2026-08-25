package protocol

import "testing"

func TestNewHandshakePacketAuthDataIsPrintableASCII(t *testing.T) {
	packet := NewHandshakePacket(1)
	authData := packet.GetAuthData()

	if len(authData) != 20 {
		t.Fatalf("expected 20-byte auth data, got %d", len(authData))
	}
	for i, b := range authData {
		if b < 0x21 || b > 0x7e {
			t.Fatalf("auth data byte %d is not printable ASCII: 0x%02x", i, b)
		}
	}
}
