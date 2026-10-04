package manager

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestTransactionCommitPersistsStableIdentityInRedoRecord(t *testing.T) {
	dataDir := t.TempDir()
	tm, err := NewTransactionManager(filepath.Join(dataDir, "redo"), filepath.Join(dataDir, "undo"))
	if err != nil {
		t.Fatal(err)
	}
	trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		_ = tm.Close()
		t.Fatal(err)
	}
	trx.CommitMetadata = []byte("replication-id=server-a:1-17;journal=replication-abc")
	if err := tm.Commit(trx); err != nil {
		_ = tm.Close()
		t.Fatal(err)
	}
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(filepath.Join(dataDir, "redo", "redo.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for {
		var lsn uint64
		if err := binary.Read(file, binary.BigEndian, &lsn); err != nil {
			if err == io.EOF {
				t.Fatalf("commit record was not written")
			}
			t.Fatal(err)
		}
		var trxID int64
		var pageID uint64
		var entryType uint8
		var dataLen uint16
		if err := binary.Read(file, binary.BigEndian, &trxID); err != nil {
			t.Fatal(err)
		}
		if err := binary.Read(file, binary.BigEndian, &pageID); err != nil {
			t.Fatal(err)
		}
		if err := binary.Read(file, binary.BigEndian, &entryType); err != nil {
			t.Fatal(err)
		}
		if err := binary.Read(file, binary.BigEndian, &dataLen); err != nil {
			t.Fatal(err)
		}
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(file, data); err != nil {
			t.Fatal(err)
		}
		if trxID == trx.ID && entryType == LOG_TYPE_TXN_COMMIT {
			if !bytes.Equal(data, trx.CommitMetadata) {
				t.Fatalf("commit metadata = %q, want %q", data, trx.CommitMetadata)
			}
			return
		}
	}
}
