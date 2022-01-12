package mvcc

import "strconv"

// IsolationLevel is the transaction isolation level used in TxOptions.
type IsolationLevel int

// Various isolation levels that drivers may support in BeginTx.
// If a driver does not support a given isolation level an error may be returned.
//
// See https://en.wikipedia.org/wiki/Isolation_(database_systems)#Isolation_levels.
const (
	LevelDefault IsolationLevel = iota
	LevelReadUncommitted
	LevelReadCommitted
	LevelWriteCommitted
	LevelRepeatableRead
	LevelSnapshot
	LevelSerializable
	LevelLinearizable
)

// String returns the name of the transaction isolation level.
func (i IsolationLevel) String() string {
	switch i {
	case LevelDefault:
		return "Default"
	case LevelReadUncommitted:
		return "Read Uncommitted"
	case LevelReadCommitted:
		return "Read Committed"
	case LevelWriteCommitted:
		return "Write Committed"
	case LevelRepeatableRead:
		return "Repeatable Read"
	case LevelSnapshot:
		return "Snapshot"
	case LevelSerializable:
		return "Serializable"
	case LevelLinearizable:
		return "Linearizable"
	default:
		return "IsolationLevel(" + strconv.Itoa(int(i)) + ")"
	}
}

/** Transaction states (trxT::state) */
type trx_state_t int

const (
	TRX_STATE_NOT_STARTED trx_state_t = iota
	TRX_STATE_ACTIVE
	TRX_STATE_PREPARED /* Support for 2PC/XA */
	TRX_STATE_COMMITTED_IN_MEMORY
)

func (t trx_state_t) String() string {
	switch t {
	case TRX_STATE_ACTIVE:
		return "trx_state_active"
	case TRX_STATE_NOT_STARTED:
		return "trx_state_not_started"
	case TRX_STATE_PREPARED:
		return "TRX_STATE_PREPARED"
	case TRX_STATE_COMMITTED_IN_MEMORY:
		return "TRX_STATE_COMMITTED_IN_MEMORY"
	}
	return ""
}
