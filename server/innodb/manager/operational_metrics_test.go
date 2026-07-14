package manager

import (
	"testing"
	"time"
)

func TestOperationalMetricsSnapshotIncludesP0ProductionFields(t *testing.T) {
	metrics := NewOperationalMetrics(time.Unix(100, 0))
	metrics.SetActiveConnections(7)
	metrics.SetActiveTransactions(3)
	metrics.RecordQuery(10*time.Millisecond, false)
	metrics.RecordQuery(20*time.Millisecond, true)
	metrics.RecordQuery(30*time.Millisecond, false)
	metrics.RecordQuery(40*time.Millisecond, false)
	metrics.RecordRedoBytes(1024)
	metrics.RecordUndoBytes(256)
	metrics.RecordLockWait(15 * time.Millisecond)

	snapshot := metrics.SnapshotAt(time.Unix(104, 0))
	if snapshot.QPS != 1 {
		t.Fatalf("expected qps=1, got %v", snapshot.QPS)
	}
	if snapshot.ErrorRate != 0.25 {
		t.Fatalf("expected error rate=0.25, got %v", snapshot.ErrorRate)
	}
	if snapshot.ActiveConnections != 7 {
		t.Fatalf("expected active connections=7, got %d", snapshot.ActiveConnections)
	}
	if snapshot.ActiveTransactions != 3 {
		t.Fatalf("expected active transactions=3, got %d", snapshot.ActiveTransactions)
	}
	if snapshot.P50LatencyMs != 20 || snapshot.P95LatencyMs != 40 || snapshot.P99LatencyMs != 40 {
		t.Fatalf("unexpected latency percentiles: p50=%d p95=%d p99=%d", snapshot.P50LatencyMs, snapshot.P95LatencyMs, snapshot.P99LatencyMs)
	}
	if snapshot.RedoBytes != 1024 || snapshot.UndoBytes != 256 {
		t.Fatalf("unexpected redo/undo bytes: redo=%d undo=%d", snapshot.RedoBytes, snapshot.UndoBytes)
	}
	if snapshot.LockWaitCount != 1 || snapshot.LockWaitMs != 15 {
		t.Fatalf("unexpected lock wait metrics: count=%d ms=%d", snapshot.LockWaitCount, snapshot.LockWaitMs)
	}
}

func TestOperationalMetricsEvaluateAlertsTriggersAndRecovers(t *testing.T) {
	metrics := NewOperationalMetrics(time.Unix(100, 0))
	metrics.SetThresholds(OperationalMetricThresholds{
		MaxErrorRate:          0.10,
		MaxP99LatencyMs:       25,
		MaxActiveTransactions: 2,
	})

	metrics.SetActiveTransactions(3)
	metrics.RecordQuery(40*time.Millisecond, true)
	metrics.RecordQuery(50*time.Millisecond, false)

	alerts := metrics.EvaluateAlertsAt(time.Unix(101, 0))
	if len(alerts) != 3 {
		t.Fatalf("expected 3 alerts, got %d: %#v", len(alerts), alerts)
	}

	metrics.ResetWindow(time.Unix(102, 0))
	metrics.SetActiveTransactions(1)
	metrics.RecordQuery(5*time.Millisecond, false)

	alerts = metrics.EvaluateAlertsAt(time.Unix(103, 0))
	if len(alerts) != 0 {
		t.Fatalf("expected recovered metrics to produce no alerts, got %#v", alerts)
	}
}
