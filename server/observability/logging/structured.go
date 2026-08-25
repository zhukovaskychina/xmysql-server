package logging

import (
	"encoding/json"
	"io"
	"time"
)

// SlowQueryEvent is the P0-D structured slow-query log contract.
type SlowQueryEvent struct {
	Timestamp     string `json:"timestamp"`
	TraceID       string `json:"trace_id"`
	ConnectionID  uint64 `json:"connection_id"`
	TransactionID uint64 `json:"transaction_id"`
	Database      string `json:"database"`
	SQLDigest     string `json:"sql_digest"`
	DurationMS    int64  `json:"duration_ms"`
	RowsExamined  uint64 `json:"rows_examined"`
	RowsReturned  uint64 `json:"rows_returned"`
	ErrorCode     string `json:"error_code"`
	Status        string `json:"status"`
}

// ErrorEvent is the P0-D structured error log contract.
type ErrorEvent struct {
	Timestamp     string `json:"timestamp"`
	Level         string `json:"level"`
	TraceID       string `json:"trace_id"`
	Component     string `json:"component"`
	Operation     string `json:"operation"`
	ErrorCode     string `json:"error_code"`
	ErrorClass    string `json:"error_class"`
	Message       string `json:"message"`
	DurationMS    int64  `json:"duration_ms"`
	ConnectionID  uint64 `json:"connection_id"`
	TransactionID uint64 `json:"transaction_id"`
}

// Encoder writes structured observability events as JSON lines.
type Encoder struct {
	writer io.Writer
}

// NewEncoder creates a JSON-lines encoder.
func NewEncoder(writer io.Writer) *Encoder {
	return &Encoder{writer: writer}
}

// NewSlowQueryEvent creates a slow-query event using the current UTC time.
func NewSlowQueryEvent(traceID string, connectionID, transactionID uint64, database, sqlDigest string, duration time.Duration, rowsExamined, rowsReturned uint64, errorCode, status string) SlowQueryEvent {
	return SlowQueryEvent{
		Timestamp:     time.Now().UTC().Format(time.RFC3339Nano),
		TraceID:       traceID,
		ConnectionID:  connectionID,
		TransactionID: transactionID,
		Database:      database,
		SQLDigest:     sqlDigest,
		DurationMS:    duration.Milliseconds(),
		RowsExamined:  rowsExamined,
		RowsReturned:  rowsReturned,
		ErrorCode:     errorCode,
		Status:        status,
	}
}

// NewErrorEvent creates an error event using the current UTC time.
func NewErrorEvent(level, traceID, component, operation, errorCode, errorClass, message string, duration time.Duration, connectionID, transactionID uint64) ErrorEvent {
	return ErrorEvent{
		Timestamp:     time.Now().UTC().Format(time.RFC3339Nano),
		Level:         level,
		TraceID:       traceID,
		Component:     component,
		Operation:     operation,
		ErrorCode:     errorCode,
		ErrorClass:    errorClass,
		Message:       message,
		DurationMS:    duration.Milliseconds(),
		ConnectionID:  connectionID,
		TransactionID: transactionID,
	}
}

// WriteSlowQuery writes one slow-query event as a JSON line.
func (e *Encoder) WriteSlowQuery(event SlowQueryEvent) error {
	return writeJSONLine(e.writer, event)
}

// WriteError writes one error event as a JSON line.
func (e *Encoder) WriteError(event ErrorEvent) error {
	return writeJSONLine(e.writer, event)
}

func writeJSONLine(writer io.Writer, value interface{}) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	_, err = writer.Write(encoded)
	return err
}
