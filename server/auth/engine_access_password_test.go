package auth

import (
	"testing"
)

// TestGetStringWithByteArray 测试 getString 方法处理字节数组
func TestGetStringWithByteArray(t *testing.T) {
	ea := &InnoDBEngineAccess{}

	tests := []struct {
		name     string
		row      []interface{}
		index    int
		expected string
	}{
		{
			name:     "String value",
			row:      []interface{}{"test", "value"},
			index:    0,
			expected: "test",
		},
		{
			name:     "Byte array - password hash",
			row:      []interface{}{"user", []byte("*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19")},
			index:    1,
			expected: "*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19",
		},
		{
			name:     "Byte array - empty",
			row:      []interface{}{"user", []byte("")},
			index:    1,
			expected: "",
		},
		{
			name:     "Nil value",
			row:      []interface{}{"user", nil},
			index:    1,
			expected: "",
		},
		{
			name:     "Index out of range",
			row:      []interface{}{"user"},
			index:    5,
			expected: "",
		},
		{
			name:     "Integer value",
			row:      []interface{}{123},
			index:    0,
			expected: "123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ea.getString(tt.row, tt.index)
			if result != tt.expected {
				t.Errorf("getString() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// TestPasswordHashParsing 测试密码哈希解析
func TestPasswordHashParsing(t *testing.T) {
	ea := &InnoDBEngineAccess{}

	// 模拟从数据库查询返回的行数据
	// authentication_string 字段可能是字节数组
	row := []interface{}{
		"root", // User
		"%",    // Host
		[]byte("*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19"), // authentication_string (byte array)
		"N", // account_locked
		"N", // password_expired
		0,   // max_connections
		0,   // max_user_connections
	}

	// 测试获取密码哈希
	passwordHash := ea.getString(row, 2)

	// 验证密码哈希格式
	if len(passwordHash) == 0 {
		t.Fatal("Password hash is empty")
	}

	if passwordHash[0] != '*' {
		t.Errorf("Password hash should start with '*', got: %s", passwordHash)
	}

	if len(passwordHash) != 41 {
		t.Errorf("Password hash should be 41 characters (1 + 40 hex), got: %d", len(passwordHash))
	}

	// 验证是否可以解析为十六进制
	hashStr := passwordHash[1:]
	for i, c := range hashStr {
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')) {
			t.Errorf("Invalid hex character at position %d: %c", i, c)
		}
	}

	t.Logf("Password hash parsed successfully: %s", passwordHash)
}

// TestPasswordHashWithStringValue 测试字符串类型的密码哈希
func TestPasswordHashWithStringValue(t *testing.T) {
	ea := &InnoDBEngineAccess{}

	// 模拟从数据库查询返回的行数据（字符串类型）
	row := []interface{}{
		"root", // User
		"%",    // Host
		"*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19", // authentication_string (string)
		"N", // account_locked
		"N", // password_expired
		0,   // max_connections
		0,   // max_user_connections
	}

	// 测试获取密码哈希
	passwordHash := ea.getString(row, 2)

	// 验证密码哈希格式
	if passwordHash != "*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19" {
		t.Errorf("Password hash mismatch, got: %s", passwordHash)
	}

	t.Logf("String password hash parsed successfully: %s", passwordHash)
}
