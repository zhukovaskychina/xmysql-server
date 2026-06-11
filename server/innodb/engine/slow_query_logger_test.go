package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlowQueryLogger_ThresholdReachedWritesRecord(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "slow_query.log")
	logger := NewSlowQueryLoggerFromConfig(SlowQueryLoggerConfig{
		Enabled:     true,
		FilePath:    logFile,
		ThresholdMs: 5,
	})

	logger.Record(
		"select * from t1",
		10*time.Millisecond,
		3,
		12,
		88,
		string(ExecutionErrorCodeUnknown),
		"success",
		"select",
	)

	content, err := os.ReadFile(logFile)
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	require.Len(t, lines, 1)

	var record slowQueryLogRecord
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &record))
	assert.Equal(t, "select * from t1", record.SQL)
	assert.Equal(t, int64(10), record.DurationMs)
	assert.Equal(t, 3, record.RowsAffected)
	assert.Equal(t, uint32(12), record.ConnID)
	assert.Equal(t, uint64(88), record.TxnID)
	assert.Equal(t, string(ExecutionErrorCodeUnknown), record.ErrorCode)
	assert.Equal(t, "success", record.Status)
	assert.Equal(t, "select", record.Stage)
	assert.NotEmpty(t, record.Timestamp)
}

func TestSlowQueryLogger_ThresholdNotReachedDoesNotWriteRecord(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "slow_query.log")
	logger := NewSlowQueryLoggerFromConfig(SlowQueryLoggerConfig{
		Enabled:     true,
		FilePath:    logFile,
		ThresholdMs: 50,
	})

	logger.Record(
		"select * from t1",
		10*time.Millisecond,
		0,
		12,
		88,
		"",
		"success",
		"select",
	)

	_, err := os.Stat(logFile)
	require.ErrorIs(t, err, os.ErrNotExist)
}
