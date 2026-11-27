package common_test

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// Assignment to a distinct string type requires the warning constants to stay untyped.
type transactionAlertLevel string

const (
	_ transactionAlertLevel = common.LONG_TXN_LEVEL_WARNING
	_ transactionAlertLevel = common.LONG_TXN_LEVEL_CRITICAL
	_ transactionAlertLevel = manager.LONG_TXN_LEVEL_WARNING
	_ transactionAlertLevel = manager.LONG_TXN_LEVEL_CRITICAL
)

func TestTransactionConstantsCompatibility(t *testing.T) {
	tests := []struct {
		name    string
		common  any
		manager any
		want    any
	}{
		{"TRX_STATE_NOT_STARTED", common.TRX_STATE_NOT_STARTED, manager.TRX_STATE_NOT_STARTED, uint8(0)},
		{"TRX_STATE_ACTIVE", common.TRX_STATE_ACTIVE, manager.TRX_STATE_ACTIVE, uint8(1)},
		{"TRX_STATE_PREPARED", common.TRX_STATE_PREPARED, manager.TRX_STATE_PREPARED, uint8(2)},
		{"TRX_STATE_COMMITTED", common.TRX_STATE_COMMITTED, manager.TRX_STATE_COMMITTED, uint8(3)},
		{"TRX_STATE_ROLLED_BACK", common.TRX_STATE_ROLLED_BACK, manager.TRX_STATE_ROLLED_BACK, uint8(4)},
		{"TRX_ISO_READ_UNCOMMITTED", common.TRX_ISO_READ_UNCOMMITTED, manager.TRX_ISO_READ_UNCOMMITTED, uint8(0)},
		{"TRX_ISO_READ_COMMITTED", common.TRX_ISO_READ_COMMITTED, manager.TRX_ISO_READ_COMMITTED, uint8(1)},
		{"TRX_ISO_REPEATABLE_READ", common.TRX_ISO_REPEATABLE_READ, manager.TRX_ISO_REPEATABLE_READ, uint8(2)},
		{"TRX_ISO_SERIALIZABLE", common.TRX_ISO_SERIALIZABLE, manager.TRX_ISO_SERIALIZABLE, uint8(3)},
		{"LONG_TXN_LEVEL_WARNING", common.LONG_TXN_LEVEL_WARNING, manager.LONG_TXN_LEVEL_WARNING, "WARNING"},
		{"LONG_TXN_LEVEL_CRITICAL", common.LONG_TXN_LEVEL_CRITICAL, manager.LONG_TXN_LEVEL_CRITICAL, "CRITICAL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Comparing boxed values also detects a change in the static numeric type.
			if tt.common != tt.want {
				t.Errorf("common = %v (%T), want %v (%T)", tt.common, tt.common, tt.want, tt.want)
			}
			if tt.manager != tt.want {
				t.Errorf("manager = %v (%T), want %v (%T)", tt.manager, tt.manager, tt.want, tt.want)
			}
		})
	}
}
