package protocol

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
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
		{
			name:           "Question marks inside literals and comments are not parameters",
			sql:            "SELECT '?' AS literal, \"?\" AS quoted, `?` AS identifier /* ? */ -- ?\nFROM users WHERE id = ? # ?\n",
			expectedParams: 1,
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

func TestPreparedStatementManager_ConcurrentGetTracksUsage(t *testing.T) {
	mgr := NewPreparedStatementManager()
	stmt, err := mgr.Prepare("select ?")
	if err != nil {
		t.Fatal(err)
	}
	const executions = 64
	var wg sync.WaitGroup
	for i := 0; i < executions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := mgr.Get(stmt.ID); err != nil {
				t.Errorf("concurrent Get() error = %v", err)
			}
		}()
	}
	wg.Wait()
	if got := stmt.ExecuteCount; got != executions {
		t.Fatalf("ExecuteCount = %d, want %d", got, executions)
	}
	if stmt.LastUsedAt.IsZero() {
		t.Fatal("LastUsedAt was not updated")
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

func TestPreparedStatementManager_SnapshotIsStableAndSorted(t *testing.T) {
	mgr := NewPreparedStatementManager()
	first, err := mgr.Prepare("select first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := mgr.Prepare("select second")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Get(second.ID); err != nil {
		t.Fatal(err)
	}

	snapshot := mgr.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("expected two prepared statements, got %#v", snapshot)
	}
	if snapshot[0].ID != first.ID || snapshot[1].ID != second.ID {
		t.Fatalf("snapshot order = %#v, want IDs %d then %d", snapshot, first.ID, second.ID)
	}
	if snapshot[1].ExecuteCount != 1 || snapshot[1].SQL != "select second" {
		t.Fatalf("snapshot did not capture execution metadata: %#v", snapshot[1])
	}

	if err := mgr.Close(first.ID); err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 2 {
		t.Fatal("snapshot was not independent of manager state")
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

func TestPreparedStatementManager_CursorLifecycle(t *testing.T) {
	mgr := NewPreparedStatementManager()
	stmt, err := mgr.Prepare("select id from users")
	if err != nil {
		t.Fatal(err)
	}
	result := &MessageQueryResult{Columns: []string{"id"}, ColumnTypes: []string{"int"}, Rows: [][]interface{}{{1}, {2}, {3}}, Type: "select"}
	if err := mgr.SetCursorResult(stmt.ID, result); err != nil {
		t.Fatal(err)
	}
	first, done, err := mgr.FetchCursor(stmt.ID, 2)
	if err != nil || done || len(first.Rows) != 2 {
		t.Fatalf("first cursor fetch = %#v, done=%v, err=%v", first, done, err)
	}
	second, done, err := mgr.FetchCursor(stmt.ID, 2)
	if err != nil || !done || len(second.Rows) != 1 {
		t.Fatalf("second cursor fetch = %#v, done=%v, err=%v", second, done, err)
	}
	if _, _, err := mgr.FetchCursor(stmt.ID, 1); err == nil {
		t.Fatal("expected exhausted cursor to be closed")
	}
}

func TestPreparedStatementManager_ZeroRowFetchDoesNotAdvanceCursor(t *testing.T) {
	mgr := NewPreparedStatementManager()
	stmt, err := mgr.Prepare("select id from users")
	if err != nil {
		t.Fatal(err)
	}
	result := &MessageQueryResult{Columns: []string{"id"}, ColumnTypes: []string{"int"}, Rows: [][]interface{}{{1}, {2}}, Type: "select"}
	if err := mgr.SetCursorResult(stmt.ID, result); err != nil {
		t.Fatal(err)
	}

	empty, done, err := mgr.FetchCursor(stmt.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if done || len(empty.Rows) != 0 {
		t.Fatalf("zero-row fetch = %#v, done=%v; want an empty non-terminal batch", empty, done)
	}

	first, done, err := mgr.FetchCursor(stmt.ID, 1)
	if err != nil || done || len(first.Rows) != 1 || first.Rows[0][0] != 1 {
		t.Fatalf("cursor advanced after zero-row fetch: %#v, done=%v, err=%v", first, done, err)
	}
}

func TestPreparedStatementManager_RejectsReplacingOpenCursor(t *testing.T) {
	mgr := NewPreparedStatementManager()
	stmt, err := mgr.Prepare("select id from users")
	if err != nil {
		t.Fatal(err)
	}
	result := &MessageQueryResult{Columns: []string{"id"}, Rows: [][]interface{}{{1}, {2}}, Type: "select"}
	if err := mgr.SetCursorResult(stmt.ID, result); err != nil {
		t.Fatal(err)
	}
	if err := mgr.SetCursorResult(stmt.ID, result); err == nil {
		t.Fatal("expected replacing an open cursor to fail")
	}
}

func TestEncodeBinaryRowPacketUsesNullBitmapAndColumnTypes(t *testing.T) {
	encoder := NewMySQLResultSetEncoder()
	columns := []*ColumnDefinition{
		encoder.CreateColumnDefinition("id", MYSQL_TYPE_LONG, 0),
		encoder.CreateColumnDefinition("name", MYSQL_TYPE_VAR_STRING, 0),
		encoder.CreateColumnDefinition("missing", MYSQL_TYPE_LONGLONG, 0),
	}
	packet := encoder.EncodeBinaryRowPacket([]interface{}{int64(42), "hello", nil}, columns, 7)
	if len(packet) < 4 {
		t.Fatalf("binary row packet too short: %d", len(packet))
	}
	payload := packet[4:]
	if payload[0] != 0x00 {
		t.Fatalf("binary row marker = %#x, want 0x00", payload[0])
	}
	if payload[1] != 0x10 {
		t.Fatalf("null bitmap = %#x, want bit 4 set for column 3", payload[1])
	}
	if got := binary.LittleEndian.Uint32(payload[2:6]); got != 42 {
		t.Fatalf("binary int = %d, want 42", got)
	}
	if payload[6] != 5 || string(payload[7:12]) != "hello" {
		t.Fatalf("binary string encoding = %v, want lenenc hello", payload[6:12])
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
			name:     "String with backslash and control characters",
			sql:      "SELECT ?",
			params:   []interface{}{"C:\\tmp\nline\r\x00\x1a"},
			expected: "SELECT 'C:\\\\tmp\\nline\\r\\0\\Z'",
		},
		{
			name:     "No parameters",
			sql:      "SELECT * FROM users",
			params:   []interface{}{},
			expected: "SELECT * FROM users",
		},
		{
			name:     "Only bind executable placeholders",
			sql:      "SELECT '?' AS literal, \"?\" AS quoted, `?` AS identifier /* ? */ WHERE id = ? -- ?\n",
			params:   []interface{}{int32(7)},
			expected: "SELECT '?' AS literal, \"?\" AS quoted, `?` AS identifier /* ? */ WHERE id = 7 -- ?\n",
		},
		{
			name:     "Backslash escaped quote keeps placeholder scanning in the string",
			sql:      "SELECT 'it\\'s ?' AS literal, ?",
			params:   []interface{}{int32(9)},
			expected: "SELECT 'it\\'s ?' AS literal, 9",
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

func TestParseBinaryStmtExecuteParamsHonorsUnsignedIntegerTypes(t *testing.T) {
	data := []byte{
		0x00, 0x01,
		common.COLUMN_TYPE_LONG, 0x80,
		common.COLUMN_TYPE_LONGLONG, 0x80,
		0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	}
	params, _, err := ParseBinaryStmtExecuteParams(data, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := params[0], uint32(0xffffffff); got != want {
		t.Fatalf("unsigned LONG = %#v (%T), want %#v", got, got, want)
	}
	if got, want := params[1], uint64(0xffffffffffffffff); got != want {
		t.Fatalf("unsigned LONGLONG = %#v (%T), want %#v", got, got, want)
	}
}

func TestParseBinaryStmtExecuteParamsDecodesTemporalTypes(t *testing.T) {
	data := []byte{
		0x00, 0x01,
		common.COLUMN_TYPE_DATE, 0x00,
		common.COLUMN_TYPE_DATETIME, 0x00,
		4, 0xe8, 0x07, 3, 5,
		7, 0xe8, 0x07, 3, 5, 14, 6, 7,
	}
	params, _, err := ParseBinaryStmtExecuteParams(data, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := params[0], "2024-03-05"; got != want {
		t.Fatalf("DATE = %#v, want %#v", got, want)
	}
	if got, want := params[1], "2024-03-05 14:06:07"; got != want {
		t.Fatalf("DATETIME = %#v, want %#v", got, want)
	}
}
