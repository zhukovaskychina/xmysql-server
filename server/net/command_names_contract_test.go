package net

import (
	"fmt"
	"testing"
)

func TestCommandNamesPreserveLegacyFallback(t *testing.T) {
	names := []string{
		"COM_SLEEP", "COM_QUIT", "COM_INIT_DB", "COM_QUERY", "COM_FIELD_LIST", "COM_CREATE_DB", "COM_DROP_DB", "COM_REFRESH",
		"COM_SHUTDOWN", "COM_STATISTICS", "COM_PROCESS_INFO", "COM_CONNECT", "COM_PROCESS_KILL", "COM_DEBUG", "COM_PING", "COM_TIME",
		"COM_DELAYED_INSERT", "COM_CHANGE_USER", "COM_BINLOG_DUMP", "COM_TABLE_DUMP", "COM_CONNECT_OUT", "COM_REGISTER_SLAVE", "COM_STMT_PREPARE", "COM_STMT_EXECUTE",
		"COM_STMT_SEND_LONG_DATA", "COM_STMT_CLOSE", "COM_STMT_RESET", "COM_SET_OPTION", "COM_STMT_FETCH", "COM_DAEMON", "COM_BINLOG_DUMP_GTID", "COM_RESET_CONNECTION",
	}
	h := &DecoupledMySQLMessageHandler{}
	for code := 0; code < 256; code++ {
		want := fmt.Sprintf("UNKNOWN(0x%02X)", code)
		if code < len(names) {
			want = names[code]
		}
		if got := h.getCommandName(byte(code)); got != want {
			t.Fatalf("command %d: got %q, want %q", code, got, want)
		}
	}
}
