package protocol

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

// TestColumnTypeMapping 测试列类型映射
func TestColumnTypeMapping(t *testing.T) {
	encoder := &QueryResponseEncoder{}

	tests := []struct {
		name           string
		columnType     string
		expectedType   byte
		expectedLength uint32
	}{
		// 整数类型
		{"TINYINT", "tinyint", common.COLUMN_TYPE_TINY, 4},
		{"SMALLINT", "smallint", common.COLUMN_TYPE_SHORT, 6},
		{"MEDIUMINT", "mediumint", common.COLUMN_TYPE_INT24, 9},
		{"INT", "int", common.COLUMN_TYPE_LONG, 11},
		{"BIGINT", "bigint", common.COLUMN_TYPE_LONGLONG, 20},

		// 浮点类型
		{"FLOAT", "float", common.COLUMN_TYPE_FLOAT, 12},
		{"DOUBLE", "double", common.COLUMN_TYPE_DOUBLE, 22},
		{"DECIMAL", "decimal", common.COLUMN_TYPE_NEWDECIMAL, 10},

		// 日期时间类型
		{"DATE", "date", common.COLUMN_TYPE_DATE, 10},
		{"TIME", "time", common.COLUMN_TYPE_TIME, 10},
		{"DATETIME", "datetime", common.COLUMN_TYPE_DATETIME, 19},
		{"TIMESTAMP", "timestamp", common.COLUMN_TYPE_TIMESTAMP, 19},
		{"YEAR", "year", common.COLUMN_TYPE_YEAR, 4},

		// 字符串类型
		{"CHAR", "char", common.COLUMN_TYPE_STRING, 255},
		{"VARCHAR", "varchar", common.COLUMN_TYPE_VAR_STRING, 255},
		{"BINARY", "binary", common.COLUMN_TYPE_STRING, 255},
		{"VARBINARY", "varbinary", common.COLUMN_TYPE_VAR_STRING, 255},

		// BLOB类型
		{"TINYBLOB", "tinyblob", common.COLUMN_TYPE_TINY_BLOB, 255},
		{"BLOB", "blob", common.COLUMN_TYPE_BLOB, 65535},
		{"MEDIUMBLOB", "mediumblob", common.COLUMN_TYPE_MEDIUM_BLOB, 16777215},
		{"LONGBLOB", "longblob", common.COLUMN_TYPE_LONG_BLOB, 4294967295},

		// TEXT类型
		{"TINYTEXT", "tinytext", common.COLUMN_TYPE_TINY_BLOB, 255},
		{"TEXT", "text", common.COLUMN_TYPE_BLOB, 65535},
		{"MEDIUMTEXT", "mediumtext", common.COLUMN_TYPE_MEDIUM_BLOB, 16777215},
		{"LONGTEXT", "longtext", common.COLUMN_TYPE_LONG_BLOB, 4294967295},

		// 其他类型
		{"ENUM", "enum", common.COLUMN_TYPE_ENUM, 1},
		{"SET", "set", common.COLUMN_TYPE_SET, 1},
		{"JSON", "json", common.COLUMN_TYPE_JSON, 4294967295},
		{"BIT", "bit", common.COLUMN_TYPE_BIT, 1},
		{"GEOMETRY", "geometry", common.COLUMN_TYPE_GEOMETRY, 4294967295},

		// 默认类型（未知类型）
		{"UNKNOWN", "unknown_type", common.COLUMN_TYPE_VAR_STRING, 255},
		{"EMPTY", "", common.COLUMN_TYPE_VAR_STRING, 255},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mysqlType, columnLength, _, _ := encoder.getColumnTypeInfo(tt.columnType)

			if mysqlType != tt.expectedType {
				t.Errorf("Type mismatch for %s: got %d, want %d", tt.columnType, mysqlType, tt.expectedType)
			}

			if columnLength != tt.expectedLength {
				t.Errorf("Length mismatch for %s: got %d, want %d", tt.columnType, columnLength, tt.expectedLength)
			}
		})
	}
}

// TestColumnDefinitionEncoding 测试列定义编码
func TestColumnDefinitionEncoding(t *testing.T) {
	encoder := &QueryResponseEncoder{}

	tests := []struct {
		name       string
		columnName string
		columnType string
	}{
		{"INT Column", "id", "int"},
		{"VARCHAR Column", "name", "varchar"},
		{"DATETIME Column", "created_at", "datetime"},
		{"DECIMAL Column", "price", "decimal"},
		{"TEXT Column", "description", "text"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packet := encoder.encodeColumnDefinitionWithType(tt.columnName, tt.columnType, 1)

			// 验证包不为空
			if len(packet) == 0 {
				t.Errorf("Encoded packet is empty for column %s", tt.columnName)
			}

			// 验证包头（前4字节）
			if len(packet) < 4 {
				t.Errorf("Packet too short: %d bytes", len(packet))
			}

			// 包长度（前3字节）
			packetLength := uint32(packet[0]) | uint32(packet[1])<<8 | uint32(packet[2])<<16
			if packetLength == 0 {
				t.Errorf("Packet length is 0")
			}

			// 包序号（第4字节）
			sequenceID := packet[3]
			if sequenceID != 1 {
				t.Errorf("Sequence ID mismatch: got %d, want 1", sequenceID)
			}

			t.Logf("Column %s (%s): packet length=%d, total bytes=%d",
				tt.columnName, tt.columnType, packetLength, len(packet))
		})
	}
}

// TestQueryResultWithColumnTypes 测试带列类型的查询结果编码
func TestQueryResultWithColumnTypes(t *testing.T) {
	encoder := &QueryResponseEncoder{}

	result := &MessageQueryResult{
		Columns:     []string{"id", "name", "age", "created_at"},
		ColumnTypes: []string{"int", "varchar", "tinyint", "datetime"},
		Rows: [][]interface{}{
			{1, "Alice", 25, "2024-01-01 10:00:00"},
			{2, "Bob", 30, "2024-01-02 11:00:00"},
		},
		Type: "select",
	}

	response, err := encoder.encodeSelectResult(result)
	if err != nil {
		t.Fatalf("Failed to encode result: %v", err)
	}

	if len(response) == 0 {
		t.Fatal("Encoded response is empty")
	}

	t.Logf("Encoded response length: %d bytes", len(response))

	// 验证响应包含列数量包
	if len(response) < 5 {
		t.Fatal("Response too short")
	}

	// 第一个包应该是列数量包
	columnCount := response[4] // 跳过包头的4字节
	if columnCount != 4 {
		t.Errorf("Column count mismatch: got %d, want 4", columnCount)
	}
}

// TestQueryResultWithoutColumnTypes 测试不带列类型的查询结果编码（向后兼容）
func TestQueryResultWithoutColumnTypes(t *testing.T) {
	encoder := &QueryResponseEncoder{}

	result := &MessageQueryResult{
		Columns: []string{"id", "name"},
		Rows: [][]interface{}{
			{1, "Alice"},
			{2, "Bob"},
		},
		Type: "select",
	}

	response, err := encoder.encodeSelectResult(result)
	if err != nil {
		t.Fatalf("Failed to encode result: %v", err)
	}

	if len(response) == 0 {
		t.Fatal("Encoded response is empty")
	}

	t.Logf("Encoded response length: %d bytes", len(response))
}

// TestBinaryFlagForBinaryTypes 测试二进制类型的标志位
func TestBinaryFlagForBinaryTypes(t *testing.T) {
	encoder := &QueryResponseEncoder{}

	tests := []struct {
		name         string
		columnType   string
		expectBinary bool
	}{
		{"BINARY", "binary", true},
		{"VARBINARY", "varbinary", true},
		{"VARCHAR", "varchar", false},
		{"CHAR", "char", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, flags, _ := encoder.getColumnTypeInfo(tt.columnType)

			hasBinaryFlag := (flags & uint16(common.BinaryFlag)) != 0

			if hasBinaryFlag != tt.expectBinary {
				t.Errorf("Binary flag mismatch for %s: got %v, want %v",
					tt.columnType, hasBinaryFlag, tt.expectBinary)
			}
		})
	}
}

// TestDecimalsForFloatTypes 测试浮点类型的小数位
func TestDecimalsForFloatTypes(t *testing.T) {
	encoder := &QueryResponseEncoder{}

	tests := []struct {
		name             string
		columnType       string
		expectedDecimals byte
	}{
		{"FLOAT", "float", 31},
		{"DOUBLE", "double", 31},
		{"DECIMAL", "decimal", 0},
		{"INT", "int", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, decimals := encoder.getColumnTypeInfo(tt.columnType)

			if decimals != tt.expectedDecimals {
				t.Errorf("Decimals mismatch for %s: got %d, want %d",
					tt.columnType, decimals, tt.expectedDecimals)
			}
		})
	}
}

// BenchmarkColumnTypeMapping 性能测试
func BenchmarkColumnTypeMapping(b *testing.B) {
	encoder := &QueryResponseEncoder{}

	b.Run("INT", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encoder.getColumnTypeInfo("int")
		}
	})

	b.Run("VARCHAR", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encoder.getColumnTypeInfo("varchar")
		}
	})

	b.Run("DATETIME", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encoder.getColumnTypeInfo("datetime")
		}
	})
}
