package engine

import "testing"

func TestWALChecksumCoversEntryPayload(t *testing.T) {
	entry := &WALEntry{
		LSN:     7,
		SpaceID: 2,
		PageNo:  3,
		Data:    []byte("same-length"),
		TxnID:   11,
	}
	checksum := calculateWALEntryChecksum(entry)
	entry.Checksum = checksum
	entry.Data = []byte("other-value")
	if calculateWALEntryChecksum(entry) == checksum {
		t.Fatal("WAL checksum must change when payload changes")
	}
}
