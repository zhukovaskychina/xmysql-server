package manager

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLogArchiverStopBeforeStartClosesAndRejectsWrites(t *testing.T) {
	archiver, err := NewLogArchiver(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewLogArchiver() error = %v", err)
	}

	archiver.Stop()
	archiver.Stop()
	if _, err := archiver.Write([]byte("closed")); err == nil {
		t.Fatal("Write() after Stop() unexpectedly succeeded")
	}
}

func TestLogArchiverRotatesAndArchivesOldLog(t *testing.T) {
	logDir := t.TempDir()
	archiveDir := t.TempDir()
	archiver, err := NewLogArchiver(logDir, archiveDir)
	if err != nil {
		t.Fatalf("NewLogArchiver() error = %v", err)
	}
	archiver.SetMaxLogSize(4)
	archiver.archiveAge = -time.Minute

	if _, err := archiver.Write([]byte("1234")); err != nil {
		t.Fatalf("first Write() error = %v", err)
	}
	if _, err := archiver.Write([]byte("5")); err != nil {
		t.Fatalf("rotating Write() error = %v", err)
	}
	archiver.archiveOldLogs()
	archiver.Stop()

	archivePath := filepath.Join(archiveDir, "redo.log.1.gz")
	file, err := os.Open(archivePath)
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("gzip archive: %v", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if string(data) != "1234" {
		t.Fatalf("archived data = %q, want %q", data, "1234")
	}
}

func TestLogArchiverRetentionPolicyDeletesExpiredArchives(t *testing.T) {
	logDir := t.TempDir()
	archiveDir := t.TempDir()
	archiver, err := NewLogArchiver(logDir, archiveDir)
	if err != nil {
		t.Fatalf("NewLogArchiver() error = %v", err)
	}
	defer archiver.Stop()

	oldArchive := filepath.Join(archiveDir, "redo.log.1.gz")
	newArchive := filepath.Join(archiveDir, "redo.log.2.gz")
	if err := os.WriteFile(oldArchive, []byte("old"), 0644); err != nil {
		t.Fatalf("write old archive: %v", err)
	}
	if err := os.WriteFile(newArchive, []byte("new"), 0644); err != nil {
		t.Fatalf("write new archive: %v", err)
	}
	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(oldArchive, oldTime, oldTime); err != nil {
		t.Fatalf("age old archive: %v", err)
	}

	archiver.SetRetentionPolicy(10, time.Hour)
	archiver.cleanupOldArchives()

	if _, err := os.Stat(oldArchive); !os.IsNotExist(err) {
		t.Fatalf("expired archive still exists, stat error = %v", err)
	}
	if _, err := os.Stat(newArchive); err != nil {
		t.Fatalf("fresh archive was deleted: %v", err)
	}
}
