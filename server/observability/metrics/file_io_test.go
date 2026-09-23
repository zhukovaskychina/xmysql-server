package metrics

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestRuntimeRecorderTracksPhysicalFileLifecycle(t *testing.T) {
	recorder := NewRuntimeRecorder(NewRegistry())
	fileName := filepath.Join(t.TempDir(), "app", "users.ibd")
	recorder.RecordFileOpen(fileName)
	recorder.RecordFileRead(fileName, 3*time.Millisecond)
	recorder.RecordFileWrite(fileName, 5*time.Millisecond)
	recorder.RecordFileClose(fileName)

	rows := recorder.FileSummary()
	if len(rows) != 1 {
		t.Fatalf("FileSummary rows = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.FileName != filepath.Clean(fileName) {
		t.Fatalf("FileName = %q, want %q", row.FileName, filepath.Clean(fileName))
	}
	if row.EventName != "wait/io/file/innodb/innodb_data_file" {
		t.Fatalf("EventName = %q", row.EventName)
	}
	if row.OpenCount != 0 || row.CountRead != 1 || row.CountWrite != 1 {
		t.Fatalf("lifecycle counters = open=%d read=%d write=%d", row.OpenCount, row.CountRead, row.CountWrite)
	}
	if row.SumTimerRead != int64(3*time.Millisecond) || row.SumTimerWrite != int64(5*time.Millisecond) {
		t.Fatalf("timers = read=%d write=%d", row.SumTimerRead, row.SumTimerWrite)
	}
	if row.MinTimerRead != int64(3*time.Millisecond) || row.MaxTimerRead != int64(3*time.Millisecond) || row.MinTimerWrite != int64(5*time.Millisecond) || row.MaxTimerWrite != int64(5*time.Millisecond) {
		t.Fatalf("timer bounds = read=%d/%d write=%d/%d", row.MinTimerRead, row.MaxTimerRead, row.MinTimerWrite, row.MaxTimerWrite)
	}
}

func TestZeroValueRuntimeRecorderInitializesFileLifecycleConcurrently(t *testing.T) {
	var recorder RuntimeRecorder
	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			recorder.RecordFileOpen("users.ibd")
			recorder.RecordFileRead("users.ibd", time.Nanosecond)
			recorder.RecordFileClose("users.ibd")
		}()
	}
	wg.Wait()

	rows := recorder.FileSummary()
	if len(rows) != 1 {
		t.Fatalf("FileSummary rows = %d, want 1", len(rows))
	}
	if rows[0].OpenCount != 0 || rows[0].CountRead != workers {
		t.Fatalf("concurrent lifecycle counters = open=%d read=%d", rows[0].OpenCount, rows[0].CountRead)
	}
}
