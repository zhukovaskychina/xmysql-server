package common

// 事务状态，保持现有事务管理器的编码和类型。
const (
	TRX_STATE_NOT_STARTED uint8 = 0
	TRX_STATE_ACTIVE      uint8 = 1
	TRX_STATE_PREPARED    uint8 = 2
	TRX_STATE_COMMITTED   uint8 = 3
	TRX_STATE_ROLLED_BACK uint8 = 4
)

// 事务隔离级别。
const (
	TRX_ISO_READ_UNCOMMITTED uint8 = 0
	TRX_ISO_READ_COMMITTED   uint8 = 1
	TRX_ISO_REPEATABLE_READ  uint8 = 2
	TRX_ISO_SERIALIZABLE     uint8 = 3
)

// 长事务告警级别。
const (
	LONG_TXN_LEVEL_WARNING  = "WARNING"
	LONG_TXN_LEVEL_CRITICAL = "CRITICAL"
)
