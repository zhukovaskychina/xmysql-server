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

func TestRuntimeRecorderKeepsPerThreadStatementHistory(t *testing.T) {
	recorder := NewRuntimeRecorder(NewRegistry())
	recorder.RecordStatementWithThreadID(17, "app", "select thread 17", "SELECT", "ok", time.Millisecond)
	recorder.RecordStatementWithThreadID(18, "app", "select thread 18", "SELECT", "ok", time.Millisecond)

	events := recorder.StatementHistoryForThread(17)
	if len(events) != 1 || events[0].ThreadID != 17 || events[0].SQL != "select thread 17" {
		t.Fatalf("unexpected per-thread statement history: %#v", events)
	}
}

func TestRuntimeRecorderUsesPositiveTimerForZeroDurationStatements(t *testing.T) {
	recorder := NewRuntimeRecorder(NewRegistry())
	recorder.RecordStatement("app", "select zero", "SELECT", "ok", 0)

	rows := recorder.StatementSummary()
	if len(rows) != 1 {
		t.Fatalf("expected one statement summary row, got %d", len(rows))
	}
	if rows[0].SumTimerWait <= 0 || rows[0].MinTimerWait <= 0 || rows[0].MaxTimerWait <= 0 {
		t.Fatalf("expected positive timer values for a completed statement, got %#v", rows[0])
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

func TestRuntimeRecorderProjectsStatementMemoryHighWatermarks(t *testing.T) {
	recorder := NewRuntimeRecorder(NewRegistry())
	recorder.RecordMemoryAllocation(17, "memory/sql/THD::main_mem_root", 64)
	recorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		17, "app", "localhost", "app", "select 1", "SELECT", "ok", time.Millisecond, 0, 1, 0,
	)
	recorder.RecordMemoryFree(17, "memory/sql/THD::main_mem_root", 64)

	events := recorder.StatementHistory()
	if len(events) != 1 {
		t.Fatalf("expected one statement event, got %d", len(events))
	}
	if events[0].ControlledMemory != 64 || events[0].MaxControlledMemory != 64 ||
		events[0].TotalMemory != 64 || events[0].MaxTotalMemory != 64 {
		t.Fatalf("unexpected statement memory projection: %#v", events[0])
	}
	rows := recorder.StatementSummary()
	if len(rows) != 1 || rows[0].MaxControlledMemory != 64 || rows[0].MaxTotalMemory != 64 {
		t.Fatalf("unexpected statement summary memory projection: %#v", rows)
	}
}

func TestRuntimeRecorderDoesNotReusePreviousStatementMemoryPeak(t *testing.T) {
	recorder := NewRuntimeRecorder(NewRegistry())
	const eventName = "memory/sql/THD::main_mem_root"
	recorder.RecordMemoryAllocation(18, eventName, 128)
	recorder.RecordMemoryFree(18, eventName, 128)

	recorder.BeginStatementMemory(18)
	recorder.RecordMemoryAllocation(18, eventName, 32)
	recorder.RecordStatementWithThreadIDAndIdentityAndAccounting(
		18, "app", "localhost", "app", "select 2", "SELECT", "ok", time.Millisecond, 0, 1, 0,
	)
	recorder.RecordMemoryFree(18, eventName, 32)
	recorder.EndStatementMemory(18)

	events := recorder.StatementHistory()
	if len(events) != 1 || events[0].MaxControlledMemory != 32 || events[0].MaxTotalMemory != 32 {
		t.Fatalf("previous statement peak leaked into current event: %#v", events)
	}
}
