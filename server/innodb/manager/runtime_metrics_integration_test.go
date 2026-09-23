package manager

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/observability/metrics"
)

func TestCrashRecoveryAndCheckpointPathsRecordRuntimeMetrics(t *testing.T) {
	recoveryBefore := runtimeMetricSample(t, metrics.DefaultRuntimeRecorder().PrometheusText(), `xmysql_recovery_runs_total{result="success"}`)
	checkpointBefore := runtimeMetricSample(t, metrics.DefaultRuntimeRecorder().PrometheusText(), `xmysql_checkpoint_runs_total{result="success"}`)

	logDir := t.TempDir()
	redoLogManager, err := NewRedoLogManager(logDir, 100)
	require.NoError(t, err)
	defer redoLogManager.Close()
	undoLogManager, err := NewUndoLogManager(logDir)
	require.NoError(t, err)
	defer undoLogManager.Close()
	require.NoError(t, redoLogManager.Flush(1000))
	recovery := NewCrashRecovery(redoLogManager, undoLogManager, 0)
	require.NoError(t, recovery.Recover())

	monitor := NewCheckpointMonitor(nil, nil)
	monitor.RecordCheckpoint(&CheckpointRecord{Timestamp: time.Now(), Duration: time.Millisecond, Success: true})

	recoveryAfter := runtimeMetricSample(t, metrics.DefaultRuntimeRecorder().PrometheusText(), `xmysql_recovery_runs_total{result="success"}`)
	checkpointAfter := runtimeMetricSample(t, metrics.DefaultRuntimeRecorder().PrometheusText(), `xmysql_checkpoint_runs_total{result="success"}`)
	require.Greater(t, recoveryAfter, recoveryBefore)
	require.Greater(t, checkpointAfter, checkpointBefore)
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
