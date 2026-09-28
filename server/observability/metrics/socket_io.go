package metrics

import (
	"sort"
	"time"
)

const clientSocketEventName = "wait/io/socket/sql/client_connection"

// SocketSummaryRow is the instance-lifetime network I/O snapshot exposed by
// Performance Schema socket_summary_* compatibility views. Bytes are counted
// at the MySQL connection read/write boundary, while timers cover the
// corresponding transport operation.
type SocketSummaryRow struct {
	ThreadID      int64
	EventName     string
	CountRead     int64
	SumTimerRead  int64
	MinTimerRead  int64
	MaxTimerRead  int64
	BytesRead     int64
	CountWrite    int64
	SumTimerWrite int64
	MinTimerWrite int64
	MaxTimerWrite int64
	BytesWrite    int64
	CountMisc     int64
	SumTimerMisc  int64
	MinTimerMisc  int64
	MaxTimerMisc  int64
}

// RecordSocketRead records one completed transport read for a client thread.
func (r *RuntimeRecorder) RecordSocketRead(threadID, bytes int64, latency time.Duration) {
	if r == nil || threadID == 0 || bytes <= 0 {
		return
	}
	enabled, timed := r.instrumentSetting(clientSocketEventName)
	if !enabled {
		return
	}
	r.socketMu.Lock()
	if r.socketRows == nil {
		r.socketRows = make(map[int64]*SocketSummaryRow)
	}
	row := r.socketRows[threadID]
	if row == nil {
		row = &SocketSummaryRow{ThreadID: threadID, EventName: clientSocketEventName}
		r.socketRows[threadID] = row
	}
	updateTimerWithSetting(&row.CountRead, &row.SumTimerRead, &row.MinTimerRead, &row.MaxTimerRead, latency, timed)
	row.BytesRead += bytes
	r.socketMu.Unlock()
}

// RecordSocketWrite records one completed transport write for a client thread.
func (r *RuntimeRecorder) RecordSocketWrite(threadID, bytes int64, latency time.Duration) {
	if r == nil || threadID == 0 || bytes <= 0 {
		return
	}
	enabled, timed := r.instrumentSetting(clientSocketEventName)
	if !enabled {
		return
	}
	r.socketMu.Lock()
	if r.socketRows == nil {
		r.socketRows = make(map[int64]*SocketSummaryRow)
	}
	row := r.socketRows[threadID]
	if row == nil {
		row = &SocketSummaryRow{ThreadID: threadID, EventName: clientSocketEventName}
		r.socketRows[threadID] = row
	}
	updateTimerWithSetting(&row.CountWrite, &row.SumTimerWrite, &row.MinTimerWrite, &row.MaxTimerWrite, latency, timed)
	row.BytesWrite += bytes
	r.socketMu.Unlock()
}

// SocketSummary returns a stable, deterministic copy of transport I/O rows.
func (r *RuntimeRecorder) SocketSummary() []SocketSummaryRow {
	if r == nil {
		return nil
	}
	r.socketMu.RLock()
	rows := make([]SocketSummaryRow, 0, len(r.socketRows))
	for _, row := range r.socketRows {
		if row != nil {
			rows = append(rows, *row)
		}
	}
	r.socketMu.RUnlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].ThreadID < rows[j].ThreadID })
	return rows
}

// SocketSummaryWasReset reports whether socket summary counters have been
// explicitly reset since recorder creation. It lets the SQL projection avoid
// synthesizing connection-count fallback values after TRUNCATE.
func (r *RuntimeRecorder) SocketSummaryWasReset() bool {
	if r == nil {
		return false
	}
	r.socketMu.RLock()
	defer r.socketMu.RUnlock()
	return r.socketSummaryReset
}

// ResetSocketSummary implements Performance Schema socket-summary truncation
// while retaining socket identities for the instance projection.
func (r *RuntimeRecorder) ResetSocketSummary() {
	if r == nil {
		return
	}
	r.socketMu.Lock()
	defer r.socketMu.Unlock()
	for _, row := range r.socketRows {
		if row == nil {
			continue
		}
		row.CountRead = 0
		row.SumTimerRead = 0
		row.MinTimerRead = 0
		row.MaxTimerRead = 0
		row.BytesRead = 0
		row.CountWrite = 0
		row.SumTimerWrite = 0
		row.MinTimerWrite = 0
		row.MaxTimerWrite = 0
		row.BytesWrite = 0
		row.CountMisc = 0
		row.SumTimerMisc = 0
		row.MinTimerMisc = 0
		row.MaxTimerMisc = 0
	}
	r.socketSummaryReset = true
}
