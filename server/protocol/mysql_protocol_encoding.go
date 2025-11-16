package protocol

import (
	"fmt"
	"math"
	"time"
)

// ============================================================================
// MySQL Protocol Encoding - 完全符合 MySQL 协议规范
// ============================================================================
// 参考: https://dev.mysql.com/doc/internals/en/mysql-packet.html
// 参考: https://dev.mysql.com/doc/internals/en/com-query-response.html

// ============================================================================
// Length Encoded Integer (lenenc-int)
// ============================================================================
// 根据值的大小使用不同的编码格式：
// - 如果值 < 251，使用 1 字节
// - 如果值 >= 251 且 < 2^16，使用 0xFC + 2 字节（小端）
// - 如果值 >= 2^16 且 < 2^24，使用 0xFD + 3 字节（小端）
// - 如果值 >= 2^24，使用 0xFE + 8 字节（小端）

func appendLenEncInt(data []byte, value uint64) []byte {
	if value < 251 {
		return append(data, byte(value))
	} else if value < (1 << 16) {
		return append(data, 0xFC,
			byte(value),
			byte(value>>8))
	} else if value < (1 << 24) {
		return append(data, 0xFD,
			byte(value),
			byte(value>>8),
			byte(value>>16))
	} else {
		return append(data, 0xFE,
			byte(value),
			byte(value>>8),
			byte(value>>16),
			byte(value>>24),
			byte(value>>32),
			byte(value>>40),
			byte(value>>48),
			byte(value>>56))
	}
}

// ============================================================================
// Length Encoded String (lenenc-str)
// ============================================================================
// 格式: lenenc-int(length) + string_data

func appendLenEncString(data []byte, str string) []byte {
	strBytes := []byte(str)
	data = appendLenEncInt(data, uint64(len(strBytes)))
	return append(data, strBytes...)
}

// ============================================================================
// MySQL Packet Header
// ============================================================================
// 格式: length(3 bytes, little-endian) + sequence_id(1 byte)

func addPacketHeader(payload []byte, sequenceId byte) []byte {
	length := len(payload)
	header := make([]byte, 4)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = sequenceId
	return append(header, payload...)
}

// ============================================================================
// Column Definition Packet (Protocol::ColumnDefinition41)
// ============================================================================
// 完全符合 MySQL 协议规范的列定义包编码
// 参考: https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41
//
// 格式:
// - catalog (lenenc-str)      -- 总是 "def"
// - schema (lenenc-str)       -- 数据库名
// - table (lenenc-str)        -- 表别名
// - org_table (lenenc-str)    -- 原始表名
// - name (lenenc-str)         -- 列别名
// - org_name (lenenc-str)     -- 原始列名
// - 0x0C                      -- 固定长度字段的长度标记（12 字节）
// - character_set (uint16)    -- 字符集编号
// - column_length (uint32)    -- 列最大长度
// - type (uint8)              -- 列类型
// - flags (uint16)            -- 列标志
// - decimals (uint8)          -- 小数位数
// - 0x00 0x00                 -- 填充字节

// ColumnDefinition 列定义结构
type ColumnDefinition struct {
	Catalog      string // 总是 "def"
	Schema       string // 数据库名（可为空）
	Table        string // 表别名（可为空）
	OrgTable     string // 原始表名（可为空）
	Name         string // 列别名（必填）
	OrgName      string // 原始列名（通常与 Name 相同）
	CharacterSet uint16 // 字符集，0x21=utf8mb4, 0x3F=binary
	ColumnLength uint32 // 列最大长度
	ColumnType   byte   // MySQL 字段类型
	Flags        uint16 // 列标志
	Decimals     byte   // 小数位数
}

// EncodeColumnDefinitionPacket 编码列定义包（包含 MySQL 包头）
// 这是完全符合 MySQL 协议的实现
func EncodeColumnDefinitionPacket(def *ColumnDefinition, seq byte) []byte {
	var payload []byte

	// 1. catalog (lenenc-str) - 总是 "def"
	catalog := def.Catalog
	if catalog == "" {
		catalog = "def"
	}
	payload = appendLenEncString(payload, catalog)

	// 2. schema (lenenc-str) - 数据库名
	payload = appendLenEncString(payload, def.Schema)

	// 3. table (lenenc-str) - 表别名
	payload = appendLenEncString(payload, def.Table)

	// 4. org_table (lenenc-str) - 原始表名
	payload = appendLenEncString(payload, def.OrgTable)

	// 5. name (lenenc-str) - 列别名（显示名称）
	payload = appendLenEncString(payload, def.Name)

	// 6. org_name (lenenc-str) - 原始列名
	orgName := def.OrgName
	if orgName == "" {
		orgName = def.Name
	}
	payload = appendLenEncString(payload, orgName)

	// 7. 固定长度字段标记 (0x0C = 12 字节)
	payload = append(payload, 0x0C)

	// 8. character_set (2 bytes, little-endian)
	charset := def.CharacterSet
	if charset == 0 {
		charset = 0x21 // utf8mb4_general_ci
	}
	payload = append(payload,
		byte(charset),
		byte(charset>>8))

	// 9. column_length (4 bytes, little-endian)
	colLen := def.ColumnLength
	if colLen == 0 {
		colLen = 255 // 默认长度
	}
	payload = append(payload,
		byte(colLen),
		byte(colLen>>8),
		byte(colLen>>16),
		byte(colLen>>24))

	// 10. type (1 byte)
	colType := def.ColumnType
	if colType == 0 {
		colType = MYSQL_TYPE_VAR_STRING // 默认 VARCHAR
	}
	payload = append(payload, colType)

	// 11. flags (2 bytes, little-endian)
	payload = append(payload,
		byte(def.Flags),
		byte(def.Flags>>8))

	// 12. decimals (1 byte)
	payload = append(payload, def.Decimals)

	// 13. filler (2 bytes, 总是 0x00 0x00)
	payload = append(payload, 0x00, 0x00)

	// 添加 MySQL 包头
	return addPacketHeader(payload, seq)
}

// ============================================================================
// Row Data Packet (Text Protocol)
// ============================================================================
// 文本协议的行数据包编码
// 每列使用 lenenc-str 编码，NULL 值使用 0xFB
//
// 格式:
// - 对于每一列：
//   - 如果值为 NULL: 0xFB
//   - 否则: lenenc-str(value)

// EncodeRowDataPacket 编码行数据包（包含 MySQL 包头）
// 完全符合 MySQL 文本协议规范
func EncodeRowDataPacket(row []any, seq byte) []byte {
	var payload []byte

	for _, value := range row {
		if value == nil {
			// NULL 值编码为 0xFB
			payload = append(payload, 0xFB)
		} else {
			// 将值转换为字符串并编码
			strValue := valueToString(value)
			payload = appendLenEncString(payload, strValue)
		}
	}

	// 添加 MySQL 包头
	return addPacketHeader(payload, seq)
}

// valueToString 将任意类型转换为字符串（用于文本协议）
func valueToString(value any) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)

	// 整数类型
	case int:
		return fmt.Sprintf("%d", v)
	case int8:
		return fmt.Sprintf("%d", v)
	case int16:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case uint:
		return fmt.Sprintf("%d", v)
	case uint8:
		return fmt.Sprintf("%d", v)
	case uint16:
		return fmt.Sprintf("%d", v)
	case uint32:
		return fmt.Sprintf("%d", v)
	case uint64:
		return fmt.Sprintf("%d", v)

	// 浮点类型
	case float32:
		if math.IsNaN(float64(v)) {
			return "NULL"
		}
		if math.IsInf(float64(v), 0) {
			return "NULL"
		}
		return fmt.Sprintf("%g", v)
	case float64:
		if math.IsNaN(v) {
			return "NULL"
		}
		if math.IsInf(v, 0) {
			return "NULL"
		}
		return fmt.Sprintf("%g", v)

	// 布尔类型
	case bool:
		if v {
			return "1"
		}
		return "0"

	// 时间类型
	case time.Time:
		return v.Format("2006-01-02 15:04:05")

	// 默认使用 fmt.Sprintf
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ============================================================================
// EOF Packet (MySQL 5.x/8.x)
// ============================================================================
// EOF 包格式:
// - 0xFE                  -- EOF marker
// - warnings (uint16)     -- 警告数量
// - status_flags (uint16) -- 服务器状态标志

// EncodeEOFPacketWithSeq 编码 EOF 包（包含 MySQL 包头）
func EncodeEOFPacketWithSeq(warnings uint16, statusFlags uint16, seq byte) []byte {
	payload := make([]byte, 5)
	payload[0] = 0xFE // EOF marker

	// warnings (2 bytes, little-endian)
	payload[1] = byte(warnings)
	payload[2] = byte(warnings >> 8)

	// status_flags (2 bytes, little-endian)
	payload[3] = byte(statusFlags)
	payload[4] = byte(statusFlags >> 8)

	return addPacketHeader(payload, seq)
}

// ============================================================================
// OK Packet
// ============================================================================
// OK 包格式:
// - 0x00                      -- OK marker
// - affected_rows (lenenc)    -- 受影响行数
// - last_insert_id (lenenc)   -- 最后插入 ID
// - status_flags (uint16)     -- 服务器状态标志
// - warnings (uint16)         -- 警告数量

// EncodeOKPacketWithSeq 编码 OK 包（包含 MySQL 包头）
func EncodeOKPacketWithSeq(affectedRows, lastInsertID uint64, statusFlags, warnings uint16, seq byte) []byte {
	payload := []byte{0x00} // OK marker

	// affected_rows (lenenc-int)
	payload = appendLenEncInt(payload, affectedRows)

	// last_insert_id (lenenc-int)
	payload = appendLenEncInt(payload, lastInsertID)

	// status_flags (2 bytes, little-endian)
	payload = append(payload,
		byte(statusFlags),
		byte(statusFlags>>8))

	// warnings (2 bytes, little-endian)
	payload = append(payload,
		byte(warnings),
		byte(warnings>>8))

	return addPacketHeader(payload, seq)
}

// ============================================================================
// Error Packet
// ============================================================================
// Error 包格式:
// - 0xFF                  -- Error marker
// - error_code (uint16)   -- 错误代码
// - '#'                   -- SQL state marker
// - sql_state (5 bytes)   -- SQL 状态码
// - error_message (str)   -- 错误消息

// EncodeErrorPacketWithSeq 编码 Error 包（包含 MySQL 包头）
func EncodeErrorPacketWithSeq(errorCode uint16, sqlState string, message string, seq byte) []byte {
	payload := []byte{0xFF} // Error marker

	// error_code (2 bytes, little-endian)
	payload = append(payload,
		byte(errorCode),
		byte(errorCode>>8))

	// SQL state marker
	payload = append(payload, '#')

	// sql_state (5 bytes)
	if len(sqlState) != 5 {
		sqlState = "HY000" // 默认状态码
	}
	payload = append(payload, []byte(sqlState)...)

	// error_message (不使用 lenenc，直接追加)
	payload = append(payload, []byte(message)...)

	return addPacketHeader(payload, seq)
}
