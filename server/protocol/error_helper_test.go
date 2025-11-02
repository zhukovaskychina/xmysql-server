package protocol

import (
	"errors"
	"fmt"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func TestErrorHelper_CreateSQLError(t *testing.T) {
	helper := NewErrorHelper()

	t.Run("CreateSyntaxError", func(t *testing.T) {
		err := helper.CreateSQLError(common.ErrParse, "You have an error in your SQL syntax")
		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if err.Code != common.ErrParse {
			t.Errorf("Expected error code %d, got %d", common.ErrParse, err.Code)
		}

		if err.State == "" {
			t.Error("Expected non-empty SQL state")
		}

		t.Logf("✅ Syntax error: %v", err)
	})

	t.Run("CreateAccessDeniedError", func(t *testing.T) {
		err := helper.CreateSQLError(common.ErrAccessDenied, "root", "localhost", "YES")
		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if err.Code != common.ErrAccessDenied {
			t.Errorf("Expected error code %d, got %d", common.ErrAccessDenied, err.Code)
		}

		if err.State != "28000" {
			t.Errorf("Expected SQL state 28000, got %s", err.State)
		}

		t.Logf("✅ Access denied error: %v", err)
	})

	t.Run("CreateTableNotFoundError", func(t *testing.T) {
		err := helper.CreateSQLError(common.ErrNoSuchTable, "test", "users")
		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if err.Code != common.ErrNoSuchTable {
			t.Errorf("Expected error code %d, got %d", common.ErrNoSuchTable, err.Code)
		}

		t.Logf("✅ Table not found error: %v", err)
	})
}

func TestErrorHelper_EncodeError(t *testing.T) {
	helper := NewErrorHelper()

	t.Run("EncodeSyntaxError", func(t *testing.T) {
		packet := helper.EncodeSyntaxError("syntax error near 'SELECT'")
		if packet == nil {
			t.Fatal("Expected packet, got nil")
		}

		// 检查包头
		if len(packet) < 4 {
			t.Fatal("Packet too short")
		}

		// 检查错误标识符 (0xFF)
		if packet[4] != 0xFF {
			t.Errorf("Expected error marker 0xFF, got 0x%02X", packet[4])
		}

		t.Logf("✅ Encoded syntax error packet: %d bytes", len(packet))
	})

	t.Run("EncodeAccessDeniedError", func(t *testing.T) {
		packet := helper.EncodeAccessDeniedError("root", "localhost")
		if packet == nil {
			t.Fatal("Expected packet, got nil")
		}

		if packet[4] != 0xFF {
			t.Errorf("Expected error marker 0xFF, got 0x%02X", packet[4])
		}

		t.Logf("✅ Encoded access denied error packet: %d bytes", len(packet))
	})

	t.Run("EncodeTableNotFoundError", func(t *testing.T) {
		packet := helper.EncodeTableNotFoundError("test", "users")
		if packet == nil {
			t.Fatal("Expected packet, got nil")
		}

		if packet[4] != 0xFF {
			t.Errorf("Expected error marker 0xFF, got 0x%02X", packet[4])
		}

		t.Logf("✅ Encoded table not found error packet: %d bytes", len(packet))
	})
}

func TestErrorHelper_CreateErrorMessage(t *testing.T) {
	helper := NewErrorHelper()

	t.Run("CreateErrorMessageFromCode", func(t *testing.T) {
		msg := helper.CreateErrorMessage("session123", common.ErrParse, "syntax error")
		if msg == nil {
			t.Fatal("Expected message, got nil")
		}

		if msg.Code != common.ErrParse {
			t.Errorf("Expected error code %d, got %d", common.ErrParse, msg.Code)
		}

		if msg.SessionID() != "session123" {
			t.Errorf("Expected session ID 'session123', got '%s'", msg.SessionID())
		}

		t.Logf("✅ Created error message: code=%d, state=%s, message=%s",
			msg.Code, msg.State, msg.Message)
	})

	t.Run("CreateErrorMessageFromGoError", func(t *testing.T) {
		goErr := errors.New("something went wrong")
		msg := helper.CreateErrorMessageFromGoError("session456", goErr)
		if msg == nil {
			t.Fatal("Expected message, got nil")
		}

		if msg.Code != common.ER_UNKNOWN_ERROR {
			t.Errorf("Expected error code %d, got %d", common.ER_UNKNOWN_ERROR, msg.Code)
		}

		if msg.Message != "something went wrong" {
			t.Errorf("Expected message 'something went wrong', got '%s'", msg.Message)
		}

		t.Logf("✅ Created error message from Go error: %v", msg)
	})

	t.Run("CreateErrorMessageFromSQLError", func(t *testing.T) {
		sqlErr := common.NewErr(common.ErrDupEntry, "1", "users.PRIMARY")
		msg := helper.CreateErrorMessageFromGoError("session789", sqlErr)
		if msg == nil {
			t.Fatal("Expected message, got nil")
		}

		if msg.Code != common.ErrDupEntry {
			t.Errorf("Expected error code %d, got %d", common.ErrDupEntry, msg.Code)
		}

		t.Logf("✅ Created error message from SQL error: %v", msg)
	})
}

func TestErrorHelper_CommonErrors(t *testing.T) {
	helper := NewErrorHelper()

	tests := []struct {
		name     string
		encode   func() []byte
		expected uint16
	}{
		{
			name:     "DuplicateKeyError",
			encode:   func() []byte { return helper.EncodeDuplicateKeyError("1", "users.PRIMARY") },
			expected: common.ErrDupEntry,
		},
		{
			name:     "TableExistsError",
			encode:   func() []byte { return helper.EncodeTableExistsError("users") },
			expected: common.ErrTableExists,
		},
		{
			name:     "DatabaseExistsError",
			encode:   func() []byte { return helper.EncodeDatabaseExistsError("test") },
			expected: common.ErrDBCreateExists,
		},
		{
			name:     "DatabaseNotFoundError",
			encode:   func() []byte { return helper.EncodeDatabaseNotFoundError("nonexistent") },
			expected: common.ErrBadDB,
		},
		{
			name:     "ColumnNotFoundError",
			encode:   func() []byte { return helper.EncodeColumnNotFoundError("id", "users") },
			expected: common.ErrBadField,
		},
		{
			name:     "LockWaitTimeoutError",
			encode:   func() []byte { return helper.EncodeLockWaitTimeoutError() },
			expected: common.ErrLockWaitTimeout,
		},
		{
			name:     "DeadlockError",
			encode:   func() []byte { return helper.EncodeDeadlockError() },
			expected: common.ErrLockDeadlock,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packet := tt.encode()
			if packet == nil {
				t.Fatal("Expected packet, got nil")
			}

			if packet[4] != 0xFF {
				t.Errorf("Expected error marker 0xFF, got 0x%02X", packet[4])
			}

			// 提取错误码（小端序）
			errorCode := uint16(packet[5]) | (uint16(packet[6]) << 8)
			if errorCode != tt.expected {
				t.Errorf("Expected error code %d, got %d", tt.expected, errorCode)
			}

			t.Logf("✅ %s: error code=%d, packet size=%d bytes", tt.name, errorCode, len(packet))
		})
	}
}

func TestGlobalErrorHelper(t *testing.T) {
	t.Run("NewSQLError", func(t *testing.T) {
		err := NewSQLError(common.ErrParse, "syntax error")
		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if err.Code != common.ErrParse {
			t.Errorf("Expected error code %d, got %d", common.ErrParse, err.Code)
		}

		t.Logf("✅ Global NewSQLError: %v", err)
	})

	t.Run("EncodeErrorFromCode", func(t *testing.T) {
		packet := EncodeErrorFromCode(common.ErrAccessDenied, "root", "localhost", "YES")
		if packet == nil {
			t.Fatal("Expected packet, got nil")
		}

		if packet[4] != 0xFF {
			t.Errorf("Expected error marker 0xFF, got 0x%02X", packet[4])
		}

		t.Logf("✅ Global EncodeErrorFromCode: %d bytes", len(packet))
	})

	t.Run("NewErrorMessage", func(t *testing.T) {
		msg := NewErrorMessage("session123", common.ErrDupEntry, "1", "users.PRIMARY")
		if msg == nil {
			t.Fatal("Expected message, got nil")
		}

		if msg.Code != common.ErrDupEntry {
			t.Errorf("Expected error code %d, got %d", common.ErrDupEntry, msg.Code)
		}

		t.Logf("✅ Global NewErrorMessage: %v", msg)
	})
}

func TestGetErrorCodeName(t *testing.T) {
	tests := []struct {
		code     uint16
		expected string
	}{
		{common.ErrParse, "ER_1064"},
		{common.ErrAccessDenied, "ER_1045"},
		{common.ErrNoSuchTable, "ER_1146"},
		{9999, "UNKNOWN_ERROR_9999"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Code_%d", tt.code), func(t *testing.T) {
			name := GetErrorCodeName(tt.code)
			if name == "" {
				t.Error("Expected non-empty name")
			}

			t.Logf("✅ Error code %d: %s", tt.code, name)
		})
	}
}

func TestGetErrorState(t *testing.T) {
	tests := []struct {
		code     uint16
		expected string
	}{
		{common.ErrParse, "42000"},
		{common.ErrAccessDenied, "28000"},
		{common.ErrNoSuchTable, "42S02"},
		{9999, common.DefaultMySQLState},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Code_%d", tt.code), func(t *testing.T) {
			state := GetErrorState(tt.code)
			if state == "" {
				t.Error("Expected non-empty state")
			}

			if state != tt.expected {
				t.Errorf("Expected state %s, got %s", tt.expected, state)
			}

			t.Logf("✅ Error code %d: SQL state=%s", tt.code, state)
		})
	}
}
