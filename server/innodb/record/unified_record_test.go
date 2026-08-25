package record

import (
	"encoding/binary"
	"testing"
)

func TestUnifiedRecordSetTransactionIdStoresInStorageHeader(t *testing.T) {
	record := NewUnifiedRecord()
	record.SetStorageData([]byte{7, 0x80, 0x00, 9, 0x00})

	record.SetTransactionId(12345)

	data := record.GetStorageData()
	if len(data) < 13 {
		t.Fatalf("storage data length = %d, want at least 13", len(data))
	}
	if got, want := data[:5], []byte{7, 0x80, 0x00, 9, 0x00}; string(got) != string(want) {
		t.Fatalf("storage header prefix = %v, want %v", got, want)
	}
	if got := binary.LittleEndian.Uint64(data[5:13]); got != 12345 {
		t.Fatalf("stored transaction id = %d, want 12345", got)
	}
}
