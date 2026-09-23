package compatibility

import "time"

// PreparedStatementSnapshot is the dependency-light observation contract
// shared by the wire protocol and Performance Schema compatibility layers.
// Keeping it outside either package avoids coupling the protocol to the SQL
// engine while still allowing a race-safe runtime inventory.
type PreparedStatementSnapshot struct {
	ID           uint32
	SQL          string
	ParamCount   uint16
	ColumnCount  uint16
	CreatedAt    time.Time
	LastUsedAt   time.Time
	ExecuteCount uint64
}
