package engine

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/observability/metrics"
)

func TestTableDDLCoordinatorRecordsRuntimeLockWaitMetric(t *testing.T) {
	coordinator := newTableDDLCoordinator()
	lock := coordinator.lockFor("app.users")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/owner"))

	before := runtimeMetricSample(t, metrics.DefaultRuntimeRecorder().PrometheusText(), `xmysql_lock_waits_total{lock_type="write",resource_type="metadata"}`)
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/waiter")
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		lock.stateMu.Lock()
		_, waiting := lock.waiters["thread/waiter"]
		lock.stateMu.Unlock()
		if waiting {
			break
		}
		time.Sleep(time.Millisecond)
	}
	lock.unlockOwned(tableLockWrite, "thread/owner")
	require.NoError(t, <-waitDone)
	lock.unlockOwned(tableLockWrite, "thread/waiter")

	after := runtimeMetricSample(t, metrics.DefaultRuntimeRecorder().PrometheusText(), `xmysql_lock_waits_total{lock_type="write",resource_type="metadata"}`)
	require.Greater(t, after, before)
}

func runtimeMetricSample(t *testing.T, exposition, prefix string) float64 {
	t.Helper()
	for _, line := range strings.Split(exposition, "\n") {
		if !strings.HasPrefix(line, prefix+" ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("invalid metric line %q", line)
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		require.NoError(t, err)
		return value
	}
	return 0
}
