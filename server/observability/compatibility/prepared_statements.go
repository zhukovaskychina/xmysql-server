package compatibility

import "time"

// PreparedStatementSnapshot is the dependency-light observation contract
// shared by the wire protocol and Performance Schema compatibility layers.
// Keeping it outside either package avoids coupling the protocol to the SQL
// engine while still allowing a race-safe runtime inventory.
type PreparedStatementSnapshot struct {
	ID               uint32
	Name             string
	SQL              string
	ParamCount       uint16
	ColumnCount      uint16
	CreatedAt        time.Time
	LastUsedAt       time.Time
	ExecuteCount     uint64
	ExecuteTimeTotal time.Duration
	ExecuteTimeMin   time.Duration
	ExecuteTimeMax   time.Duration
	ErrorCount       uint64
	WarningCount     uint64
	RowsAffected     uint64
	RowsSent         uint64
	RowsExamined     uint64
}

// PreparedStatementExecutionStats is the runtime accounting recorded for one
// COM_STMT_EXECUTE attempt.  It is kept in the compatibility package so the
// protocol manager can publish authoritative observations without depending on
// the SQL engine's result types.
type PreparedStatementExecutionStats struct {
	Duration     time.Duration
	Failed       bool
	Warnings     uint64
	RowsAffected uint64
	RowsSent     uint64
	RowsExamined uint64
}
