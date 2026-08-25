package manager

import (
	"math"
	"sort"
	"sync"
	"time"
)

// OperationalMetrics 提供 P0 生产验收所需的最小运行指标聚合。
type OperationalMetrics struct {
	mu sync.RWMutex

	windowStart time.Time

	totalQueries       uint64
	totalErrors        uint64
	activeConnections  int
	activeTransactions int

	latencies []time.Duration

	redoBytes     uint64
	undoBytes     uint64
	lockWaitCount uint64
	lockWaitTime  time.Duration

	thresholds OperationalMetricThresholds
}

type OperationalMetricThresholds struct {
	MaxErrorRate          float64 `json:"max_error_rate"`
	MaxP99LatencyMs       int64   `json:"max_p99_latency_ms"`
	MaxActiveTransactions int     `json:"max_active_transactions"`
}

type OperationalMetricsSnapshot struct {
	WindowStart        time.Time `json:"window_start"`
	Timestamp          time.Time `json:"timestamp"`
	QPS                float64   `json:"qps"`
	ErrorRate          float64   `json:"error_rate"`
	TotalQueries       uint64    `json:"total_queries"`
	TotalErrors        uint64    `json:"total_errors"`
	ActiveConnections  int       `json:"active_connections"`
	ActiveTransactions int       `json:"active_transactions"`
	P50LatencyMs       int64     `json:"p50_latency_ms"`
	P95LatencyMs       int64     `json:"p95_latency_ms"`
	P99LatencyMs       int64     `json:"p99_latency_ms"`
	RedoBytes          uint64    `json:"redo_bytes"`
	UndoBytes          uint64    `json:"undo_bytes"`
	LockWaitCount      uint64    `json:"lock_wait_count"`
	LockWaitMs         int64     `json:"lock_wait_ms"`
}

type OperationalMetricAlert struct {
	Type      string      `json:"type"`
	Level     string      `json:"level"`
	Message   string      `json:"message"`
	Value     interface{} `json:"value"`
	Threshold interface{} `json:"threshold"`
	Timestamp time.Time   `json:"timestamp"`
}

func NewOperationalMetrics(start time.Time) *OperationalMetrics {
	if start.IsZero() {
		start = time.Now()
	}
	return &OperationalMetrics{
		windowStart: start,
		thresholds: OperationalMetricThresholds{
			MaxErrorRate:          0.05,
			MaxP99LatencyMs:       1000,
			MaxActiveTransactions: 1000,
		},
	}
}

func (m *OperationalMetrics) SetThresholds(thresholds OperationalMetricThresholds) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if thresholds.MaxErrorRate > 0 {
		m.thresholds.MaxErrorRate = thresholds.MaxErrorRate
	}
	if thresholds.MaxP99LatencyMs > 0 {
		m.thresholds.MaxP99LatencyMs = thresholds.MaxP99LatencyMs
	}
	if thresholds.MaxActiveTransactions > 0 {
		m.thresholds.MaxActiveTransactions = thresholds.MaxActiveTransactions
	}
}

func (m *OperationalMetrics) SetActiveConnections(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeConnections = count
}

func (m *OperationalMetrics) SetActiveTransactions(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeTransactions = count
}

func (m *OperationalMetrics) RecordQuery(latency time.Duration, failed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalQueries++
	if failed {
		m.totalErrors++
	}
	if latency < 0 {
		latency = 0
	}
	m.latencies = append(m.latencies, latency)
}

func (m *OperationalMetrics) RecordRedoBytes(bytes uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.redoBytes += bytes
}

func (m *OperationalMetrics) RecordUndoBytes(bytes uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.undoBytes += bytes
}

func (m *OperationalMetrics) RecordLockWait(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if duration < 0 {
		duration = 0
	}
	m.lockWaitCount++
	m.lockWaitTime += duration
}

func (m *OperationalMetrics) ResetWindow(start time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if start.IsZero() {
		start = time.Now()
	}
	m.windowStart = start
	m.totalQueries = 0
	m.totalErrors = 0
	m.latencies = nil
	m.redoBytes = 0
	m.undoBytes = 0
	m.lockWaitCount = 0
	m.lockWaitTime = 0
}

func (m *OperationalMetrics) SnapshotAt(now time.Time) OperationalMetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snapshotLocked(now)
}

func (m *OperationalMetrics) Snapshot() OperationalMetricsSnapshot {
	return m.SnapshotAt(time.Now())
}

func (m *OperationalMetrics) EvaluateAlertsAt(now time.Time) []OperationalMetricAlert {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := m.snapshotLocked(now)
	alerts := make([]OperationalMetricAlert, 0, 3)
	if snapshot.ErrorRate > m.thresholds.MaxErrorRate {
		alerts = append(alerts, OperationalMetricAlert{
			Type:      "ERROR_RATE_HIGH",
			Level:     "ERROR",
			Message:   "error rate exceeds threshold",
			Value:     snapshot.ErrorRate,
			Threshold: m.thresholds.MaxErrorRate,
			Timestamp: now,
		})
	}
	if snapshot.P99LatencyMs > m.thresholds.MaxP99LatencyMs {
		alerts = append(alerts, OperationalMetricAlert{
			Type:      "P99_LATENCY_HIGH",
			Level:     "WARNING",
			Message:   "p99 latency exceeds threshold",
			Value:     snapshot.P99LatencyMs,
			Threshold: m.thresholds.MaxP99LatencyMs,
			Timestamp: now,
		})
	}
	if snapshot.ActiveTransactions > m.thresholds.MaxActiveTransactions {
		alerts = append(alerts, OperationalMetricAlert{
			Type:      "ACTIVE_TXNS_HIGH",
			Level:     "WARNING",
			Message:   "active transactions exceed threshold",
			Value:     snapshot.ActiveTransactions,
			Threshold: m.thresholds.MaxActiveTransactions,
			Timestamp: now,
		})
	}
	return alerts
}

func (m *OperationalMetrics) EvaluateAlerts() []OperationalMetricAlert {
	return m.EvaluateAlertsAt(time.Now())
}

func (m *OperationalMetrics) snapshotLocked(now time.Time) OperationalMetricsSnapshot {
	if now.IsZero() {
		now = time.Now()
	}
	elapsed := now.Sub(m.windowStart).Seconds()
	qps := 0.0
	if elapsed > 0 {
		qps = float64(m.totalQueries) / elapsed
	}
	errorRate := 0.0
	if m.totalQueries > 0 {
		errorRate = float64(m.totalErrors) / float64(m.totalQueries)
	}

	return OperationalMetricsSnapshot{
		WindowStart:        m.windowStart,
		Timestamp:          now,
		QPS:                qps,
		ErrorRate:          errorRate,
		TotalQueries:       m.totalQueries,
		TotalErrors:        m.totalErrors,
		ActiveConnections:  m.activeConnections,
		ActiveTransactions: m.activeTransactions,
		P50LatencyMs:       percentileMs(m.latencies, 0.50),
		P95LatencyMs:       percentileMs(m.latencies, 0.95),
		P99LatencyMs:       percentileMs(m.latencies, 0.99),
		RedoBytes:          m.redoBytes,
		UndoBytes:          m.undoBytes,
		LockWaitCount:      m.lockWaitCount,
		LockWaitMs:         m.lockWaitTime.Milliseconds(),
	}
}

func percentileMs(values []time.Duration, percentile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	idx := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx].Milliseconds()
}
