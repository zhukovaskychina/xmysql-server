package net

import (
	"bytes"
	"testing"
)

// TestWriteLenEncInt 测试 Length-Encoded Integer 编码
func TestWriteLenEncInt(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	tests := []struct {
		name     string
		value    uint64
		expected []byte
	}{
		{"小于251", 250, []byte{0xFA}},
		{"等于251", 251, []byte{0xFC, 0xFB, 0x00}},
		{"2字节最大值", 65535, []byte{0xFC, 0xFF, 0xFF}},
		{"3字节", 65536, []byte{0xFD, 0x00, 0x00, 0x01}},
		{"8字节", 16777216, []byte{0xFE, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encoder.WriteLenEncInt(tt.value)
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("WriteLenEncInt(%d) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

// TestWriteLenEncString 测试 Length-Encoded String 编码
func TestWriteLenEncString(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	tests := []struct {
		name     string
		value    string
		expected []byte
	}{
		{"空字符串", "", []byte{0x00}},
		{"单字符", "a", []byte{0x01, 0x61}},
		{"普通字符串", "hello", []byte{0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F}},
		{"中文", "你好", []byte{0x06, 0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encoder.WriteLenEncString(tt.value)
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("WriteLenEncString(%q) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

// TestWriteColumnDefinitionPacket 测试列定义包编码
func TestWriteColumnDefinitionPacket(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	// 测试一个简单的 BIGINT 列
	colDef := &ColumnDefinition{
		Catalog:      "def",
		Schema:       "",
		Table:        "",
		OrgTable:     "",
		Name:         "tx_read_only",
		OrgName:      "tx_read_only",
		CharacterSet: 0x21, // UTF-8
		ColumnLength: 20,
		ColumnType:   MYSQL_TYPE_LONGLONG,
		Flags:        FLAG_NOT_NULL,
		Decimals:     0,
	}

	result := encoder.WriteColumnDefinitionPacket(colDef)

	// 验证关键字段
	if result[0] != 0x03 { // "def" 长度
		t.Errorf("catalog length = %d, want 3", result[0])
	}

	// 验证包含 "def"
	if !bytes.Contains(result, []byte("def")) {
		t.Error("column definition should contain 'def'")
	}

	// 验证包含列名
	if !bytes.Contains(result, []byte("tx_read_only")) {
		t.Error("column definition should contain column name")
	}

	// 验证固定字段长度标记 0x0C
	foundFixedLength := false
	for i := 0; i < len(result)-12; i++ {
		if result[i] == 0x0C {
			// 检查后面是否是字符集
			if result[i+1] == 0x21 && result[i+2] == 0x00 {
				foundFixedLength = true
				// 验证类型字段
				typeOffset := i + 7 // 跳过 0x0C + charset(2) + length(4)
				if result[typeOffset] != MYSQL_TYPE_LONGLONG {
					t.Errorf("column type = 0x%02X, want 0x%02X", result[typeOffset], MYSQL_TYPE_LONGLONG)
				}
				break
			}
		}
	}

	if !foundFixedLength {
		t.Error("column definition should contain fixed length marker 0x0C")
	}
}

// TestWriteEOFPacket 测试 EOF 包编码
func TestWriteEOFPacket(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	result := encoder.WriteEOFPacket(0, SERVER_STATUS_AUTOCOMMIT)

	expected := []byte{0xFE, 0x00, 0x00, 0x02, 0x00}

	if !bytes.Equal(result, expected) {
		t.Errorf("WriteEOFPacket() = %v, want %v", result, expected)
	}
}

// TestWriteOKPacket 测试 OK 包编码
func TestWriteOKPacket(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	result := encoder.WriteOKPacket(1, 0, SERVER_STATUS_AUTOCOMMIT, 0)

	// 验证 OK marker
	if result[0] != 0x00 {
		t.Errorf("OK packet marker = 0x%02X, want 0x00", result[0])
	}

	// 验证 affected_rows = 1
	if result[1] != 0x01 {
		t.Errorf("affected_rows = %d, want 1", result[1])
	}
}

// TestWriteRowDataPacket 测试行数据包编码
func TestWriteRowDataPacket(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	tests := []struct {
		name   string
		values []interface{}
		check  func([]byte) bool
	}{
		{
			name:   "单个整数",
			values: []interface{}{int64(0)},
			check: func(data []byte) bool {
				// 应该是 lenenc-str("0")
				return bytes.Equal(data, []byte{0x01, 0x30}) // "0"
			},
		},
		{
			name:   "NULL值",
			values: []interface{}{nil},
			check: func(data []byte) bool {
				return data[0] == 0xFB // NULL marker
			},
		},
		{
			name:   "字符串",
			values: []interface{}{"hello"},
			check: func(data []byte) bool {
				return bytes.Equal(data, []byte{0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F})
			},
		},
		{
			name:   "多列",
			values: []interface{}{int64(1), "test", nil},
			check: func(data []byte) bool {
				// 应该包含 "1", "test", NULL
				return bytes.Contains(data, []byte{0x01, 0x31}) && // "1"
					bytes.Contains(data, []byte("test")) &&
					bytes.Contains(data, []byte{0xFB}) // NULL
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encoder.WriteRowDataPacket(tt.values)
			if !tt.check(result) {
				t.Errorf("WriteRowDataPacket(%v) = %v, check failed", tt.values, result)
			}
		})
	}
}

// TestInferMySQLType 测试类型推断
func TestInferMySQLType(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	tests := []struct {
		name     string
		value    interface{}
		expected byte
	}{
		{"int", int(1), MYSQL_TYPE_LONG},
		{"int32", int32(1), MYSQL_TYPE_LONG},
		{"int64", int64(1), MYSQL_TYPE_LONGLONG},
		{"float32", float32(1.0), MYSQL_TYPE_FLOAT},
		{"float64", float64(1.0), MYSQL_TYPE_DOUBLE},
		{"string", "hello", MYSQL_TYPE_VAR_STRING},
		{"bool", true, MYSQL_TYPE_TINY},
		{"[]byte", []byte{1, 2, 3}, MYSQL_TYPE_BLOB},
		{"nil", nil, MYSQL_TYPE_NULL},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encoder.InferMySQLType(tt.value)
			if result != tt.expected {
				t.Errorf("InferMySQLType(%T) = 0x%02X, want 0x%02X", tt.value, result, tt.expected)
			}
		})
	}
}

// TestValueToString 测试值转字符串
func TestValueToString(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"int", int(123), "123"},
		{"int64", int64(-456), "-456"},
		{"float64", float64(3.14), "3.14"},
		{"bool true", true, "1"},
		{"bool false", false, "0"},
		{"string", "hello", "hello"},
		{"nil", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encoder.valueToString(tt.value)
			if result != tt.expected {
				t.Errorf("valueToString(%v) = %q, want %q", tt.value, result, tt.expected)
			}
		})
	}
}

// TestSendResultSetPackets_TxReadOnly 测试 SELECT @@session.tx_read_only
func TestSendResultSetPackets_TxReadOnly(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	// 模拟 SELECT @@session.tx_read_only 的结果
	data := &ResultSetData{
		Columns: []string{"tx_read_only"},
		Rows: [][]interface{}{
			{int64(0)},
		},
	}

	packets := encoder.SendResultSetPackets(data)

	// 验证包的数量：1(column count) + 1(column def) + 1(EOF) + 1(row) + 1(EOF) = 5
	if len(packets) != 5 {
		t.Errorf("packet count = %d, want 5", len(packets))
	}

	// 验证第一个包是列数 = 1
	if !bytes.Equal(packets[0], []byte{0x01}) {
		t.Errorf("column count packet = %v, want [0x01]", packets[0])
	}

	// 验证列定义包包含列名
	if !bytes.Contains(packets[1], []byte("tx_read_only")) {
		t.Error("column definition should contain 'tx_read_only'")
	}

	// 验证第三个包是 EOF
	if packets[2][0] != 0xFE {
		t.Errorf("EOF packet marker = 0x%02X, want 0xFE", packets[2][0])
	}

	// 验证行数据包包含 "0"
	if !bytes.Contains(packets[3], []byte{0x01, 0x30}) { // lenenc-str("0")
		t.Errorf("row data should contain '0', got %v", packets[3])
	}

	// 验证最后一个包是 EOF
	if packets[4][0] != 0xFE {
		t.Errorf("final EOF packet marker = 0x%02X, want 0xFE", packets[4][0])
	}
}

// TestSendResultSetPackets_Select1 测试 SELECT 1
func TestSendResultSetPackets_Select1(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	data := &ResultSetData{
		Columns: []string{"1"},
		Rows: [][]interface{}{
			{int64(1)},
		},
	}

	packets := encoder.SendResultSetPackets(data)

	if len(packets) != 5 {
		t.Errorf("packet count = %d, want 5", len(packets))
	}

	// 验证行数据包含 "1"
	if !bytes.Contains(packets[3], []byte{0x01, 0x31}) { // lenenc-str("1")
		t.Errorf("row data should contain '1', got %v", packets[3])
	}
}

// TestSendResultSetPackets_MultiColumn 测试多列结果
func TestSendResultSetPackets_MultiColumn(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	data := &ResultSetData{
		Columns: []string{"id", "name", "age"},
		Rows: [][]interface{}{
			{int64(1), "Alice", int64(25)},
			{int64(2), "Bob", int64(30)},
		},
	}

	packets := encoder.SendResultSetPackets(data)

	// 1(column count) + 3(column defs) + 1(EOF) + 2(rows) + 1(EOF) = 8
	if len(packets) != 8 {
		t.Errorf("packet count = %d, want 8", len(packets))
	}

	// 验证列数 = 3
	if !bytes.Equal(packets[0], []byte{0x03}) {
		t.Errorf("column count = %v, want [0x03]", packets[0])
	}

	// 验证第一行数据包含 "1", "Alice", "25"
	row1 := packets[5]
	if !bytes.Contains(row1, []byte("Alice")) {
		t.Error("first row should contain 'Alice'")
	}
}

// TestSendResultSetPackets_EmptyResult 测试空结果集
func TestSendResultSetPackets_EmptyResult(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	data := &ResultSetData{
		Columns: []string{"id", "name"},
		Rows:    [][]interface{}{}, // 空行
	}

	packets := encoder.SendResultSetPackets(data)

	// 1(column count) + 2(column defs) + 1(EOF) + 0(rows) + 1(EOF) = 5
	if len(packets) != 5 {
		t.Errorf("packet count = %d, want 5", len(packets))
	}
}

// TestSendResultSetPackets_NullValues 测试 NULL 值
func TestSendResultSetPackets_NullValues(t *testing.T) {
	encoder := NewMySQLProtocolEncoder()

	data := &ResultSetData{
		Columns: []string{"id", "nullable_field"},
		Rows: [][]interface{}{
			{int64(1), nil},
			{int64(2), "value"},
		},
	}

	packets := encoder.SendResultSetPackets(data)

	// 验证第一行包含 NULL marker (0xFB)
	row1 := packets[5]
	if !bytes.Contains(row1, []byte{0xFB}) {
		t.Error("first row should contain NULL marker (0xFB)")
	}

	// 验证第二行包含 "value"
	row2 := packets[6]
	if !bytes.Contains(row2, []byte("value")) {
		t.Error("second row should contain 'value'")
	}
}

// BenchmarkWriteLenEncInt 性能测试
func BenchmarkWriteLenEncInt(b *testing.B) {
	encoder := NewMySQLProtocolEncoder()
	for i := 0; i < b.N; i++ {
		encoder.WriteLenEncInt(12345)
	}
}

// BenchmarkWriteLenEncString 性能测试
func BenchmarkWriteLenEncString(b *testing.B) {
	encoder := NewMySQLProtocolEncoder()
	for i := 0; i < b.N; i++ {
		encoder.WriteLenEncString("hello world")
	}
}

// BenchmarkWriteColumnDefinitionPacket 性能测试
func BenchmarkWriteColumnDefinitionPacket(b *testing.B) {
	encoder := NewMySQLProtocolEncoder()
	colDef := encoder.CreateColumnDefinition("test_column", MYSQL_TYPE_LONGLONG, FLAG_NOT_NULL)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.WriteColumnDefinitionPacket(colDef)
	}
}

// BenchmarkWriteRowDataPacket 性能测试
func BenchmarkWriteRowDataPacket(b *testing.B) {
	encoder := NewMySQLProtocolEncoder()
	row := []interface{}{int64(1), "test", int64(100)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.WriteRowDataPacket(row)
	}
}
