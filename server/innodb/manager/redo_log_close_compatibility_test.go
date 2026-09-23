package manager

import (
	"testing"
	"time"
)

func TestRedoLogManagerCloseDrainsAsyncCommitRequests(t *testing.T) {
	redo, err := NewRedoLogManager(t.TempDir(), 100)
	if err != nil {
		t.Fatalf("create redo manager: %v", err)
	}

	completed := make(chan error, 1)
	redo.FlushAsync(0, func(err error) { completed <- err })
	if err := redo.Close(); err != nil {
		t.Fatalf("close redo manager: %v", err)
	}
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("async commit callback returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not drain the pending async commit callback")
	}
}

func TestRedoLogManagerRecordsRealGroupCommitBatch(t *testing.T) {
	redo, err := NewRedoLogManager(t.TempDir(), 100)
	if err != nil {
		t.Fatalf("create redo manager: %v", err)
	}
	defer redo.Close()

	completed := make(chan error, 3)
	for index := 0; index < 3; index++ {
		redo.FlushAsync(0, func(err error) { completed <- err })
	}
	for index := 0; index < 3; index++ {
		select {
		case err := <-completed:
			if err != nil {
				t.Fatalf("async commit callback returned error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for async commit callback")
		}
	}

	stats := redo.groupCommit.GetStats()
	if stats.TotalCommits != 3 {
		t.Fatalf("expected three recorded commits, got %+v", stats)
	}
	if stats.TotalBatches == 0 || stats.TotalFsyncs == 0 {
		t.Fatalf("expected a real group commit batch and fsync, got %+v", stats)
	}
}

func TestRedoLogManagerFlushHonorsTargetLSN(t *testing.T) {
	redo, err := NewRedoLogManager(t.TempDir(), 100)
	if err != nil {
		t.Fatalf("create redo manager: %v", err)
	}
	defer redo.Close()

	first, err := redo.Append(&RedoLogEntry{TrxID: 1, PageID: 11, Type: LOG_TYPE_INSERT, Data: []byte("first")})
	if err != nil {
		t.Fatalf("append first redo entry: %v", err)
	}
	second, err := redo.Append(&RedoLogEntry{TrxID: 1, PageID: 12, Type: LOG_TYPE_INSERT, Data: []byte("second")})
	if err != nil {
		t.Fatalf("append second redo entry: %v", err)
	}
	if err := redo.Flush(first); err != nil {
		t.Fatalf("flush to first LSN: %v", err)
	}

	redo.mu.RLock()
	defer redo.mu.RUnlock()
	if len(redo.logBuffer) != 1 || redo.logBuffer[0].LSN != second {
		t.Fatalf("flush crossed target LSN: buffered=%+v first=%d second=%d", redo.logBuffer, first, second)
	}
}

func TestRedoLogManagerExposesGroupCommitConfigurationAndStats(t *testing.T) {
	redo, err := NewRedoLogManager(t.TempDir(), 100)
	if err != nil {
		t.Fatalf("create redo manager: %v", err)
	}
	defer redo.Close()

	if err := redo.ConfigureGroupCommit(2*time.Millisecond, 2); err != nil {
		t.Fatalf("configure group commit: %v", err)
	}
	stats := redo.GetGroupCommitStats()
	if stats == nil || stats.WindowDuration != 2*time.Millisecond || stats.MaxBatchSize != 2 {
		t.Fatalf("unexpected group commit configuration: %+v", stats)
	}
	if err := redo.ConfigureGroupCommit(0, 2); err == nil {
		t.Fatal("expected invalid group commit window to be rejected")
	}
}
