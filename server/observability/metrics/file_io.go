package metrics

import (
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// FileSummaryRow is the bounded file-I/O lifecycle snapshot used by
// Performance Schema file_instances and file_summary_* compatibility views.
// Timers use nanoseconds, matching the runtime recorder's other duration
// snapshots. The row is deliberately limited to physical file operations
// observed by the storage layer; it is not a Go allocator or page-cache view.
type FileSummaryRow struct {
	FileName            string
	EventName           string
	ObjectInstanceBegin int64
	OpenCount           int64
	CountRead           int64
	SumTimerRead        int64
	MinTimerRead        int64
	MaxTimerRead        int64
	CountWrite          int64
	SumTimerWrite       int64
	MinTimerWrite       int64
	MaxTimerWrite       int64
	CountMisc           int64
	SumTimerMisc        int64
	MinTimerMisc        int64
	MaxTimerMisc        int64
}

type fileSummaryKey struct {
	fileName  string
	eventName string
}

type fileSummaryState struct {
	FileSummaryRow
	nextInstance int64
}

// fileSummaryState is kept separately from statement/memory history so file
// metrics can be updated by the low-level storage package without holding any
// SQL execution locks.
type fileSummaryRecorder struct {
	mu     sync.RWMutex
	rows   map[fileSummaryKey]*fileSummaryState
	nextID int64
}

func newFileSummaryRecorder() *fileSummaryRecorder {
	return &fileSummaryRecorder{rows: make(map[fileSummaryKey]*fileSummaryState), nextID: 1}
}

func normalizeFileName(fileName string) string {
	return filepath.Clean(strings.TrimSpace(fileName))
}

func fileEventName(fileName string) string {
	switch strings.ToLower(filepath.Ext(fileName)) {
	case ".ibd":
		return "wait/io/file/innodb/innodb_data_file"
	case ".log":
		return "wait/io/file/innodb/innodb_log_file"
	case ".frm":
		return "wait/io/file/sql/FRM"
	default:
		return "wait/io/file/sql/file"
	}
}

func (r *fileSummaryRecorder) rowLocked(fileName, eventName string) *fileSummaryState {
	if r.rows == nil {
		r.rows = make(map[fileSummaryKey]*fileSummaryState)
	}
	fileName = normalizeFileName(fileName)
	if eventName == "" {
		eventName = fileEventName(fileName)
	}
	key := fileSummaryKey{fileName: fileName, eventName: eventName}
	if row := r.rows[key]; row != nil {
		return row
	}
	row := &fileSummaryState{FileSummaryRow: FileSummaryRow{
		FileName: fileName, EventName: eventName,
		ObjectInstanceBegin: r.nextID,
	}}
	r.nextID++
	r.rows[key] = row
	return row
}

func (r *fileSummaryRecorder) open(fileName, eventName string) {
	if r == nil || strings.TrimSpace(fileName) == "" {
		return
	}
	r.mu.Lock()
	r.rowLocked(fileName, eventName).OpenCount++
	r.mu.Unlock()
}

func (r *fileSummaryRecorder) close(fileName, eventName string) {
	if r == nil || strings.TrimSpace(fileName) == "" {
		return
	}
	r.mu.Lock()
	row := r.rowLocked(fileName, eventName)
	if row.OpenCount > 0 {
		row.OpenCount--
	}
	r.mu.Unlock()
}

func updateTimer(count *int64, sum *int64, min *int64, max *int64, latency time.Duration) {
	nanos := latency.Nanoseconds()
	if nanos <= 0 {
		// Some supported platforms expose a coarse monotonic clock. Keep a
		// completed operation distinguishable from an unobserved one while
		// preserving the measured value whenever the clock has resolution.
		nanos = 1
	}
	*count++
	*sum += nanos
	if *min == 0 || nanos < *min {
		*min = nanos
	}
	if nanos > *max {
		*max = nanos
	}
}

func (r *fileSummaryRecorder) read(fileName, eventName string, latency time.Duration) {
	if r == nil || strings.TrimSpace(fileName) == "" {
		return
	}
	r.mu.Lock()
	row := r.rowLocked(fileName, eventName)
	updateTimer(&row.CountRead, &row.SumTimerRead, &row.MinTimerRead, &row.MaxTimerRead, latency)
	r.mu.Unlock()
}

func (r *fileSummaryRecorder) write(fileName, eventName string, latency time.Duration) {
	if r == nil || strings.TrimSpace(fileName) == "" {
		return
	}
	r.mu.Lock()
	row := r.rowLocked(fileName, eventName)
	updateTimer(&row.CountWrite, &row.SumTimerWrite, &row.MinTimerWrite, &row.MaxTimerWrite, latency)
	r.mu.Unlock()
}

func (r *fileSummaryRecorder) snapshot() []FileSummaryRow {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	rows := make([]FileSummaryRow, 0, len(r.rows))
	for _, row := range r.rows {
		if row != nil {
			rows = append(rows, row.FileSummaryRow)
		}
	}
	r.mu.RUnlock()
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].FileName != rows[j].FileName {
			return rows[i].FileName < rows[j].FileName
		}
		return rows[i].EventName < rows[j].EventName
	})
	return rows
}
