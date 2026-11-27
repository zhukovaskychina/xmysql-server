package manager

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRedoLogManagerMirrorsFlushedEntriesToArchiver(t *testing.T) {
	logDir := t.TempDir()
	archiveDir := t.TempDir()
	manager, err := NewRedoLogManager(logDir, 10)
	if err != nil {
		t.Fatalf("NewRedoLogManager() error = %v", err)
	}
	if err := manager.EnableLogArchival(archiveDir); err != nil {
		t.Fatalf("EnableLogArchival() error = %v", err)
	}

	if _, err := manager.Append(&RedoLogEntry{
		TrxID:  7,
		PageID: 9,
		Type:   LOG_TYPE_INSERT,
		Data:   []byte("archived-redo"),
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := manager.Flush(0); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	primary, err := os.Stat(filepath.Join(logDir, "redo.log"))
	if err != nil {
		t.Fatalf("stat primary redo log: %v", err)
	}
	archived, err := os.Stat(filepath.Join(logDir, "redo.log.1"))
	if err != nil {
		t.Fatalf("stat archived redo log: %v", err)
	}
	if primary.Size() == 0 || archived.Size() == 0 {
		t.Fatalf("primary/archive sizes = %d/%d, want both non-zero", primary.Size(), archived.Size())
	}
}
