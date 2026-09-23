package metrics

import (
	"testing"
	"time"
)

func TestRuntimeRecorderKeepsBoundedStatementHistory(t *testing.T) {
	registry := NewRegistry()
	RegisterP0Metrics(registry)
	recorder := NewRuntimeRecorder(registry)

	recorder.RecordStatement("app", "select 1", "SELECT", "ok", 3*time.Millisecond)
	recorder.RecordStatement("app", "insert into t values (1)", "INSERT", "error", 5*time.Millisecond)

	events := recorder.StatementHistory()
	if len(events) != 2 {
		t.Fatalf("expected two statement events, got %d", len(events))
	}
	if events[0].SQL != "select 1" || events[1].Status != "error" {
		t.Fatalf("unexpected statement history: %#v", events)
	}
	recorder.RecordStatementWithThreadID(17, "app", "select thread", "SELECT", "ok", time.Millisecond)
	events = recorder.StatementHistory()
	if events[2].ThreadID != 17 {
		t.Fatalf("expected thread id 17, got %#v", events[2])
	}
	for i := 0; i < 300; i++ {
		recorder.RecordStatement("app", "select bounded", "SELECT", "ok", time.Millisecond)
	}
	if got := len(recorder.StatementHistory()); got != statementHistoryLimit {
		t.Fatalf("expected history limit %d, got %d", statementHistoryLimit, got)
	}
}

func TestRuntimeRecorderTracksMemoryLifecycleByThread(t *testing.T) {
	recorder := NewRuntimeRecorder(NewRegistry())
	recorder.RecordMemoryAllocation(17, "memory/sql/THD::main_mem_root", 12)
	recorder.RecordMemoryAllocation(17, "memory/sql/THD::main_mem_root", 8)
	recorder.RecordMemoryFree(17, "memory/sql/THD::main_mem_root", 5)

	rows := recorder.MemorySummary()
	if len(rows) != 1 {
		t.Fatalf("expected one memory summary row, got %d", len(rows))
	}
	row := rows[0]
	if row.ThreadID != 17 || row.EventName != "memory/sql/THD::main_mem_root" {
		t.Fatalf("unexpected memory identity: %#v", row)
	}
	if row.CountAlloc != 2 || row.CountFree != 1 || row.BytesAlloc != 20 || row.BytesFree != 5 {
		t.Fatalf("unexpected memory counters: %#v", row)
	}
	if row.CurrentCountUsed != 1 || row.CurrentBytesUsed != 15 || row.HighCountUsed != 2 || row.HighBytesUsed != 20 {
		t.Fatalf("unexpected memory lifecycle: %#v", row)
	}
}
