package protocol

import (
	"testing"
)

func TestPreparedStatementManager_Prepare(t *testing.T) {
	mgr := NewPreparedStatementManager()

	tests := []struct {
		name           string
		sql            string
		expectedParams uint16
	}{
		{
			name:           "Simple SELECT with one parameter",
			sql:            "SELECT * FROM users WHERE id = ?",
			expectedParams: 1,
		},
		{
			name:           "SELECT with multiple parameters",
			sql:            "SELECT * FROM users WHERE age > ? AND city = ?",
			expectedParams: 2,
		},
		{
			name:           "INSERT with parameters",
			sql:            "INSERT INTO users (name, age, email) VALUES (?, ?, ?)",
			expectedParams: 3,
		},
		{
			name:           "Query without parameters",
			sql:            "SELECT * FROM users",
			expectedParams: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := mgr.Prepare(tt.sql)
			if err != nil {
				t.Fatalf("Prepare() error = %v", err)
			}

			if stmt.ID == 0 {
				t.Error("Expected non-zero statement ID")
			}

			if stmt.SQL != tt.sql {
				t.Errorf("Expected SQL = %q, got %q", tt.sql, stmt.SQL)
			}

			if stmt.ParamCount != tt.expectedParams {
				t.Errorf("Expected ParamCount = %d, got %d", tt.expectedParams, stmt.ParamCount)
			}

			if len(stmt.Params) != int(tt.expectedParams) {
				t.Errorf("Expected %d params, got %d", tt.expectedParams, len(stmt.Params))
			}
		})
	}
}

func TestPreparedStatementManager_Get(t *testing.T) {
	mgr := NewPreparedStatementManager()

	// 准备一个语句
	stmt1, err := mgr.Prepare("SELECT * FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	// 获取语句
	stmt2, err := mgr.Get(stmt1.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if stmt2.ID != stmt1.ID {
		t.Errorf("Expected ID = %d, got %d", stmt1.ID, stmt2.ID)
	}

	if stmt2.SQL != stmt1.SQL {
		t.Errorf("Expected SQL = %q, got %q", stmt1.SQL, stmt2.SQL)
	}

	// 尝试获取不存在的语句
	_, err = mgr.Get(99999)
	if err == nil {
		t.Error("Expected error when getting non-existent statement")
	}
}

func TestPreparedStatementManager_Close(t *testing.T) {
	mgr := NewPreparedStatementManager()

	// 准备一个语句
	stmt, err := mgr.Prepare("SELECT * FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	// 关闭语句
	err = mgr.Close(stmt.ID)
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// 验证语句已被删除
	_, err = mgr.Get(stmt.ID)
	if err == nil {
		t.Error("Expected error when getting closed statement")
	}

	// 尝试关闭不存在的语句
	err = mgr.Close(99999)
	if err == nil {
		t.Error("Expected error when closing non-existent statement")
	}
}

func TestPreparedStatementManager_Count(t *testing.T) {
	mgr := NewPreparedStatementManager()

	if mgr.Count() != 0 {
		t.Errorf("Expected count = 0, got %d", mgr.Count())
	}

	// 准备3个语句
	mgr.Prepare("SELECT * FROM users WHERE id = ?")
	mgr.Prepare("SELECT * FROM orders WHERE user_id = ?")
	mgr.Prepare("INSERT INTO logs (message) VALUES (?)")

	if mgr.Count() != 3 {
		t.Errorf("Expected count = 3, got %d", mgr.Count())
	}
}

func TestPreparedStatementManager_Concurrent(t *testing.T) {
	mgr := NewPreparedStatementManager()

	// 并发准备语句
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(index int) {
			_, err := mgr.Prepare("SELECT * FROM users WHERE id = ?")
			if err != nil {
				t.Errorf("Concurrent Prepare() error = %v", err)
			}
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}

	if mgr.Count() != 10 {
		t.Errorf("Expected count = 10, got %d", mgr.Count())
	}
}

func TestEncodePrepareResponse(t *testing.T) {
	stmt := &PreparedStatement{
		ID:          123,
		SQL:         "SELECT * FROM users WHERE id = ?",
		ParamCount:  1,
		ColumnCount: 0,
		Params: []*ParamMetadata{
			{
				Index:    0,
				Type:     0xfd, // VAR_STRING
				Unsigned: false,
				Name:     "?0",
			},
		},
		Columns: nil,
	}

	packets := EncodePrepareResponse(stmt, 1)

	// 应该有3个包：OK包 + 参数定义包 + EOF包
	expectedPackets := 3
	if len(packets) != expectedPackets {
		t.Errorf("Expected %d packets, got %d", expectedPackets, len(packets))
	}

	// 验证第一个包（OK包）
	if len(packets[0]) < 16 {
		t.Error("OK packet too short")
	}

	// 验证包头
	if packets[0][3] != 1 { // 序列号
		t.Errorf("Expected sequence ID = 1, got %d", packets[0][3])
	}

	// 验证OK标识符
	if packets[0][4] != 0x00 {
		t.Errorf("Expected OK marker = 0x00, got 0x%02x", packets[0][4])
	}

	// 验证语句ID
	stmtID := uint32(packets[0][5]) | uint32(packets[0][6])<<8 |
		uint32(packets[0][7])<<16 | uint32(packets[0][8])<<24
	if stmtID != 123 {
		t.Errorf("Expected statement ID = 123, got %d", stmtID)
	}
}

func TestBindParameters(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		params   []interface{}
		expected string
	}{
		{
			name:     "Single integer parameter",
			sql:      "SELECT * FROM users WHERE id = ?",
			params:   []interface{}{int32(123)},
			expected: "SELECT * FROM users WHERE id = 123",
		},
		{
			name:     "Single string parameter",
			sql:      "SELECT * FROM users WHERE name = ?",
			params:   []interface{}{"Alice"},
			expected: "SELECT * FROM users WHERE name = 'Alice'",
		},
		{
			name:     "Multiple parameters",
			sql:      "SELECT * FROM users WHERE age > ? AND city = ?",
			params:   []interface{}{int32(18), "Beijing"},
			expected: "SELECT * FROM users WHERE age > 18 AND city = 'Beijing'",
		},
		{
			name:     "NULL parameter",
			sql:      "INSERT INTO users (name, email) VALUES (?, ?)",
			params:   []interface{}{"Bob", nil},
			expected: "INSERT INTO users (name, email) VALUES ('Bob', NULL)",
		},
		{
			name:     "String with single quote",
			sql:      "SELECT * FROM users WHERE name = ?",
			params:   []interface{}{"O'Brien"},
			expected: "SELECT * FROM users WHERE name = 'O''Brien'",
		},
		{
			name:     "No parameters",
			sql:      "SELECT * FROM users",
			params:   []interface{}{},
			expected: "SELECT * FROM users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BindPreparedSQL(tt.sql, tt.params)
			if result != tt.expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", tt.expected, result)
			}
		})
	}
}

func TestReadLengthEncodedInteger(t *testing.T) {
	tests := []struct {
		name          string
		data          []byte
		expectedValue int64
		expectedBytes int
	}{
		{
			name:          "Single byte (< 251)",
			data:          []byte{0x05},
			expectedValue: 5,
			expectedBytes: 1,
		},
		{
			name:          "Two bytes (0xfc)",
			data:          []byte{0xfc, 0x00, 0x01},
			expectedValue: 256,
			expectedBytes: 3,
		},
		{
			name:          "Three bytes (0xfd)",
			data:          []byte{0xfd, 0x00, 0x00, 0x01},
			expectedValue: 65536,
			expectedBytes: 4,
		},
		{
			name:          "Eight bytes (0xfe)",
			data:          []byte{0xfe, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
			expectedValue: 72057594037927936,
			expectedBytes: 9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, bytes := ReadLengthEncodedInteger(tt.data)
			if value != tt.expectedValue {
				t.Errorf("Expected value = %d, got %d", tt.expectedValue, value)
			}
			if bytes != tt.expectedBytes {
				t.Errorf("Expected bytes = %d, got %d", tt.expectedBytes, bytes)
			}
		})
	}
}
