package protocol

import (
	"bytes"
	"testing"
)

// Public capability constants historically accepted either signed or unsigned
// integer destinations without a conversion. Keep that source compatibility.
var (
	_ uint32 = CLIENT_FOUND_ROWS
	_ int64  = CLIENT_FOUND_ROWS
	_ uint32 = CLIENT_OPTIONAL_RESULTSET_METADATA
	_ int64  = CLIENT_OPTIONAL_RESULTSET_METADATA
)

func TestHandshakeCapabilitiesAndStatusRemainWireCompatible(t *testing.T) {
	h := NewHandshakePacket(1)
	caps := uint32(h.CapabilityFlags1) | uint32(h.CapabilityFlags2)<<16
	if caps != 0x032fa22d || h.StatusFlags != 0x0002 {
		t.Fatalf("handshake capability/status bytes changed: %08x/%04x", caps, h.StatusFlags)
	}
}

func TestOKAndCursorStatusRemainWireCompatible(t *testing.T) {
	want := []byte{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}
	if got := EncodeOKPacket(nil, 0, 0, nil); !bytes.Equal(got, want) {
		t.Fatalf("OK packet = %x, want %x", got, want)
	}
	wantEOF := []byte{0xfe, 0, 0, 0xc2, 0}
	got := NewMySQLResultSetEncoder().WriteEOFPacket(0, SERVER_STATUS_AUTOCOMMIT|SERVER_STATUS_CURSOR_EXISTS|SERVER_STATUS_LAST_ROW_SENT)
	if !bytes.Equal(got, wantEOF) {
		t.Fatalf("cursor EOF = %x, want %x", got, wantEOF)
	}
}
