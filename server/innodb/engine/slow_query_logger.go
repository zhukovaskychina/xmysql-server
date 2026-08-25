package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

type slowQueryLogRecord struct {
	Timestamp    string `json:"timestamp"`
	SQL          string `json:"sql"`
	DurationMs   int64  `json:"duration_ms"`
	RowsAffected int    `json:"rows_affected"`
	ConnID       uint32 `json:"conn_id"`
	TxnID        uint64 `json:"txn_id"`
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	ErrorCode    string `json:"error_code"`
	ErrorMsg     string `json:"error_msg"`
	Status       string `json:"status"`
	Stage        string `json:"stage"`
}

type SlowQueryLogger struct {
	enabled   bool
	filePath  string
	threshold time.Duration
	mu        sync.Mutex
}

type SlowQueryLoggerConfig struct {
	Enabled     bool
	FilePath    string
	ThresholdMs int
}

func NewSlowQueryLogger(cfg *conf.Cfg) *SlowQueryLogger {
	if cfg == nil {
		return &SlowQueryLogger{}
	}

	return NewSlowQueryLoggerFromConfig(SlowQueryLoggerConfig{
		Enabled:     cfg.SlowQueryLog,
		FilePath:    cfg.SlowQueryLogFile,
		ThresholdMs: cfg.LongQueryTimeMs,
	})
}

func NewSlowQueryLoggerFromConfig(cfg SlowQueryLoggerConfig) *SlowQueryLogger {
	threshold := time.Duration(cfg.ThresholdMs) * time.Millisecond
	return &SlowQueryLogger{
		enabled:   cfg.Enabled,
		filePath:  cfg.FilePath,
		threshold: threshold,
	}
}

func (s *SlowQueryLogger) IsEnabled() bool {
	return s != nil && s.enabled
}

func (s *SlowQueryLogger) log(record slowQueryLogRecord) {
	if s == nil || !s.enabled {
		return
	}

	if s.threshold > 0 && time.Duration(record.DurationMs)*time.Millisecond < s.threshold {
		return
	}

	if s.filePath == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(s.filePath), 0o755); err != nil {
		logger.Debugf("create slow query log dir failed: %v", err)
		return
	}

	f, err := os.OpenFile(s.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		logger.Debugf("open slow query log file failed: %v", err)
		return
	}
	defer f.Close()

	record.Timestamp = time.Now().Format(time.RFC3339Nano)

	line, err := json.Marshal(record)
	if err != nil {
		logger.Debugf("marshal slow query record failed: %v", err)
		return
	}

	if _, err := f.Write(append(line, '\n')); err != nil {
		logger.Debugf("write slow query log failed: %v", err)
	}
}

func (s *SlowQueryLogger) Record(query string, duration time.Duration, rowsAffected int, connID uint32, txnID uint64, errCode, errMsg, status, stage, schema, table string) {
	if !s.IsEnabled() {
		return
	}

	s.log(slowQueryLogRecord{
		SQL:          query,
		DurationMs:   duration.Milliseconds(),
		RowsAffected: rowsAffected,
		ConnID:       connID,
		TxnID:        txnID,
		Schema:       schema,
		Table:        table,
		ErrorCode:    errCode,
		ErrorMsg:     errMsg,
		Status:       status,
		Stage:        stage,
	})
}
