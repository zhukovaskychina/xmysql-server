package net

import (
	"encoding/binary"
	"fmt"
	"math"
)

// MySQLProtocolEncoder MySQL 协议编码器
// 严格按照 MySQL 5.7/8.0 协议规范实现
type MySQLProtocolEncoder struct{}

// NewMySQLProtocolEncoder 创建协议编码器
func NewMySQLProtocolEncoder() *MySQLProtocolEncoder {
	return &MySQLProtocolEncoder{}
}

// ============================================================================
// Length-Encoded Integer (lenenc-int)
// ============================================================================
// 根据 MySQL 协议规范：
// - 如果值 < 251，使用 1 字节
// - 如果值 >= 251 且 < 2^16，使用 0xFC + 2 字节
// - 如果值 >= 2^16 且 < 2^24，使用 0xFD + 3 字节
// - 如果值 >= 2^24，使用 0xFE + 8 字节

// WriteLenEncInt 编码 Length-Encoded Integer
func (e *MySQLProtocolEncoder) WriteLenEncInt(value uint64) []byte {
	if value < 251 {
		return []byte{byte(value)}
	} else if value < (1 << 16) {
		return []byte{0xFC, byte(value), byte(value >> 8)}
	} else if value < (1 << 24) {
		return []byte{0xFD, byte(value), byte(value >> 8), byte(value >> 16)}
	} else {
		return []byte{
			0xFE,
			byte(value), byte(value >> 8), byte(value >> 16), byte(value >> 24),
			byte(value >> 32), byte(value >> 40), byte(value >> 48), byte(value >> 56),
		}
	}
}

// ============================================================================
// Length-Encoded String (lenenc-str)
// ============================================================================
// 格式：lenenc-int(length) + string bytes

// WriteLenEncString 编码 Length-Encoded String
func (e *MySQLProtocolEncoder) WriteLenEncString(str string) []byte {
	if str == "" {
		return []byte{0x00} // 空字符串编码为 0x00
	}

	strBytes := []byte(str)
	length := uint64(len(strBytes))

	// 先写入长度
	result := e.WriteLenEncInt(length)
	// 再写入字符串内容
	result = append(result, strBytes...)

	return result
}

// WriteLenEncNullString 编码可能为 NULL 的字符串
// NULL 值编码为 0xFB
func (e *MySQLProtocolEncoder) WriteLenEncNullString(str *string) []byte {
	if str == nil {
		return []byte{0xFB} // NULL marker
	}
	return e.WriteLenEncString(*str)
}

// ============================================================================
// Column Definition Packet (41 bytes protocol)
// ============================================================================
// 严格按照 MySQL 协议规范：
// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41

// ColumnDefinition 列定义结构
type ColumnDefinition struct {
	Catalog      string // 总是 "def"
	Schema       string // 数据库名（可为空）
	Table        string // 表名（可为空）
	OrgTable     string // 原始表名（可为空）
	Name         string // 列名（必填）
	OrgName      string // 原始列名（通常与 Name 相同）
	CharacterSet uint16 // 字符集，通常 0x21(utf8) 或 0x3F(binary)
	ColumnLength uint32 // 列长度
	ColumnType   byte   // MySQL 字段类型（见 MySQLFieldType）
	Flags        uint16 // 列标志（见 ColumnFlags）
	Decimals     byte   // 小数位数（数值类型使用）
}

// MySQLFieldType MySQL 字段类型常量
const (
	MYSQL_TYPE_DECIMAL     byte = 0x00
	MYSQL_TYPE_TINY        byte = 0x01 // TINYINT
	MYSQL_TYPE_SHORT       byte = 0x02 // SMALLINT
	MYSQL_TYPE_LONG        byte = 0x03 // INT
	MYSQL_TYPE_FLOAT       byte = 0x04 // FLOAT
	MYSQL_TYPE_DOUBLE      byte = 0x05 // DOUBLE
	MYSQL_TYPE_NULL        byte = 0x06
	MYSQL_TYPE_TIMESTAMP   byte = 0x07
	MYSQL_TYPE_LONGLONG    byte = 0x08 // BIGINT
	MYSQL_TYPE_INT24       byte = 0x09 // MEDIUMINT
	MYSQL_TYPE_DATE        byte = 0x0A
	MYSQL_TYPE_TIME        byte = 0x0B
	MYSQL_TYPE_DATETIME    byte = 0x0C
	MYSQL_TYPE_YEAR        byte = 0x0D
	MYSQL_TYPE_NEWDATE     byte = 0x0E
	MYSQL_TYPE_VARCHAR     byte = 0x0F
	MYSQL_TYPE_BIT         byte = 0x10
	MYSQL_TYPE_NEWDECIMAL  byte = 0xF6
	MYSQL_TYPE_ENUM        byte = 0xF7
	MYSQL_TYPE_SET         byte = 0xF8
	MYSQL_TYPE_TINY_BLOB   byte = 0xF9
	MYSQL_TYPE_MEDIUM_BLOB byte = 0xFA
	MYSQL_TYPE_LONG_BLOB   byte = 0xFB
	MYSQL_TYPE_BLOB        byte = 0xFC
	MYSQL_TYPE_VAR_STRING  byte = 0xFD // VARCHAR/TEXT
	MYSQL_TYPE_STRING      byte = 0xFE // CHAR
	MYSQL_TYPE_GEOMETRY    byte = 0xFF
)

// ColumnFlags 列标志常量
const (
	FLAG_NOT_NULL       uint16 = 0x0001
	FLAG_PRI_KEY        uint16 = 0x0002
	FLAG_UNIQUE_KEY     uint16 = 0x0004
	FLAG_MULTIPLE_KEY   uint16 = 0x0008
	FLAG_BLOB           uint16 = 0x0010
	FLAG_UNSIGNED       uint16 = 0x0020
	FLAG_ZEROFILL       uint16 = 0x0040
	FLAG_BINARY         uint16 = 0x0080
	FLAG_ENUM           uint16 = 0x0100
	FLAG_AUTO_INCREMENT uint16 = 0x0200
	FLAG_TIMESTAMP      uint16 = 0x0400
	FLAG_SET            uint16 = 0x0800
)

// WriteColumnDefinitionPacket 编码列定义包
// 严格按照 MySQL Protocol::ColumnDefinition41 格式
func (e *MySQLProtocolEncoder) WriteColumnDefinitionPacket(col *ColumnDefinition) []byte {
	var data []byte

	// 1. catalog (lenenc-str) - 总是 "def"
	data = append(data, e.WriteLenEncString(col.Catalog)...)

	// 2. schema (lenenc-str)
	data = append(data, e.WriteLenEncString(col.Schema)...)

	// 3. table (lenenc-str)
	data = append(data, e.WriteLenEncString(col.Table)...)

	// 4. org_table (lenenc-str)
	data = append(data, e.WriteLenEncString(col.OrgTable)...)

	// 5. name (lenenc-str) - 列名
	data = append(data, e.WriteLenEncString(col.Name)...)

	// 6. org_name (lenenc-str)
	data = append(data, e.WriteLenEncString(col.OrgName)...)

	// 7. length of fixed-length fields (always 0x0C = 12 bytes)
	data = append(data, 0x0C)

	// 8. character_set (2 bytes, little-endian)
	data = append(data, byte(col.CharacterSet), byte(col.CharacterSet>>8))

	// 9. column_length (4 bytes, little-endian)
	data = append(data,
		byte(col.ColumnLength),
		byte(col.ColumnLength>>8),
		byte(col.ColumnLength>>16),
		byte(col.ColumnLength>>24))

	// 10. type (1 byte)
	data = append(data, col.ColumnType)

	// 11. flags (2 bytes, little-endian)
	data = append(data, byte(col.Flags), byte(col.Flags>>8))

	// 12. decimals (1 byte)
	data = append(data, col.Decimals)

	// 13. filler (2 bytes, always 0x00 0x00)
	data = append(data, 0x00, 0x00)

	return data
}

// ============================================================================
// EOF Packet
// ============================================================================
// MySQL 5.x EOF Packet 格式：
// 0xFE + warnings(2) + status_flags(2)

// WriteEOFPacket 编码 EOF 包（MySQL 5.x 风格）
func (e *MySQLProtocolEncoder) WriteEOFPacket(warnings uint16, statusFlags uint16) []byte {
	data := make([]byte, 5)
	data[0] = 0xFE // EOF marker

	// warnings (2 bytes, little-endian)
	binary.LittleEndian.PutUint16(data[1:3], warnings)

	// status_flags (2 bytes, little-endian)
	binary.LittleEndian.PutUint16(data[3:5], statusFlags)

	return data
}

// ServerStatus 服务器状态标志
const (
	SERVER_STATUS_IN_TRANS             uint16 = 0x0001
	SERVER_STATUS_AUTOCOMMIT           uint16 = 0x0002
	SERVER_MORE_RESULTS_EXISTS         uint16 = 0x0008
	SERVER_STATUS_NO_GOOD_INDEX_USED   uint16 = 0x0010
	SERVER_STATUS_NO_INDEX_USED        uint16 = 0x0020
	SERVER_STATUS_CURSOR_EXISTS        uint16 = 0x0040
	SERVER_STATUS_LAST_ROW_SENT        uint16 = 0x0080
	SERVER_STATUS_DB_DROPPED           uint16 = 0x0100
	SERVER_STATUS_NO_BACKSLASH_ESCAPES uint16 = 0x0200
	SERVER_STATUS_METADATA_CHANGED     uint16 = 0x0400
	SERVER_QUERY_WAS_SLOW              uint16 = 0x0800
	SERVER_PS_OUT_PARAMS               uint16 = 0x1000
)

// ============================================================================
// OK Packet (MySQL 8.x style, can also be used in 5.x)
// ============================================================================
// OK Packet 格式：
// 0x00 + affected_rows(lenenc) + last_insert_id(lenenc) + status_flags(2) + warnings(2)

// WriteOKPacket 编码 OK 包
func (e *MySQLProtocolEncoder) WriteOKPacket(affectedRows, lastInsertID uint64, statusFlags, warnings uint16) []byte {
	data := []byte{0x00} // OK marker

	// affected_rows (lenenc-int)
	data = append(data, e.WriteLenEncInt(affectedRows)...)

	// last_insert_id (lenenc-int)
	data = append(data, e.WriteLenEncInt(lastInsertID)...)

	// status_flags (2 bytes, little-endian)
	data = append(data, byte(statusFlags), byte(statusFlags>>8))

	// warnings (2 bytes, little-endian)
	data = append(data, byte(warnings), byte(warnings>>8))

	return data
}

// ============================================================================
// Row Data Packet (Text Protocol)
// ============================================================================
// 文本协议的行数据：每列一个 lenenc-str
// NULL 值编码为 0xFB

// WriteRowDataPacket 编码行数据包（文本协议）
// values: 每列的值，interface{} 会被转换为字符串
func (e *MySQLProtocolEncoder) WriteRowDataPacket(values []interface{}) []byte {
	var data []byte

	for _, value := range values {
		if value == nil {
			// NULL 值
			data = append(data, 0xFB)
		} else {
			// 将值转换为字符串
			strValue := e.valueToString(value)
			data = append(data, e.WriteLenEncString(strValue)...)
		}
	}

	return data
}

// valueToString 将 interface{} 值转换为字符串（用于文本协议）
func (e *MySQLProtocolEncoder) valueToString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
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
	case float32:
		return fmt.Sprintf("%g", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ============================================================================
// 辅助函数：根据 Go 类型推断 MySQL 字段类型
// ============================================================================

// InferMySQLType 根据 Go 值类型推断 MySQL 字段类型
func (e *MySQLProtocolEncoder) InferMySQLType(value interface{}) byte {
	if value == nil {
		return MYSQL_TYPE_NULL
	}

	switch value.(type) {
	case bool:
		return MYSQL_TYPE_TINY
	case int8:
		return MYSQL_TYPE_TINY
	case uint8:
		return MYSQL_TYPE_TINY
	case int16:
		return MYSQL_TYPE_SHORT
	case uint16:
		return MYSQL_TYPE_SHORT
	case int, int32:
		return MYSQL_TYPE_LONG
	case uint, uint32:
		return MYSQL_TYPE_LONG
	case int64:
		return MYSQL_TYPE_LONGLONG
	case uint64:
		return MYSQL_TYPE_LONGLONG
	case float32:
		return MYSQL_TYPE_FLOAT
	case float64:
		return MYSQL_TYPE_DOUBLE
	case []byte:
		return MYSQL_TYPE_BLOB
	case string:
		return MYSQL_TYPE_VAR_STRING
	default:
		return MYSQL_TYPE_VAR_STRING
	}
}

// InferColumnLength 根据类型推断列长度
func (e *MySQLProtocolEncoder) InferColumnLength(fieldType byte) uint32 {
	switch fieldType {
	case MYSQL_TYPE_TINY:
		return 4 // TINYINT 最大 4 字符 (-128)
	case MYSQL_TYPE_SHORT:
		return 6 // SMALLINT 最大 6 字符 (-32768)
	case MYSQL_TYPE_LONG:
		return 11 // INT 最大 11 字符 (-2147483648)
	case MYSQL_TYPE_LONGLONG:
		return 20 // BIGINT 最大 20 字符
	case MYSQL_TYPE_FLOAT:
		return 12 // FLOAT
	case MYSQL_TYPE_DOUBLE:
		return 22 // DOUBLE
	case MYSQL_TYPE_DATE:
		return 10 // YYYY-MM-DD
	case MYSQL_TYPE_TIME:
		return 10 // HH:MM:SS
	case MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP:
		return 19 // YYYY-MM-DD HH:MM:SS
	case MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING:
		return 255 // VARCHAR 默认长度
	case MYSQL_TYPE_BLOB:
		return 65535 // BLOB
	default:
		return 255
	}
}

// CreateColumnDefinition 创建列定义（便捷方法）
func (e *MySQLProtocolEncoder) CreateColumnDefinition(
	name string,
	fieldType byte,
	flags uint16,
) *ColumnDefinition {
	return &ColumnDefinition{
		Catalog:      "def",
		Schema:       "",
		Table:        "",
		OrgTable:     "",
		Name:         name,
		OrgName:      name,
		CharacterSet: 0x21, // UTF-8
		ColumnLength: e.InferColumnLength(fieldType),
		ColumnType:   fieldType,
		Flags:        flags,
		Decimals:     0,
	}
}

// CreateColumnDefinitionFromValue 根据值自动创建列定义
func (e *MySQLProtocolEncoder) CreateColumnDefinitionFromValue(name string, value interface{}) *ColumnDefinition {
	fieldType := e.InferMySQLType(value)
	flags := uint16(0)

	// 对于数值类型，可以设置 NOT_NULL 标志（如果值不为 nil）
	if value != nil {
		switch fieldType {
		case MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG,
			MYSQL_TYPE_LONGLONG, MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:
			flags = FLAG_NOT_NULL
		}
	}

	return e.CreateColumnDefinition(name, fieldType, flags)
}

// ============================================================================
// 完整的 ResultSet 发送流程
// ============================================================================

// ResultSetData 结果集数据
type ResultSetData struct {
	Columns []string        // 列名
	Rows    [][]interface{} // 行数据
}

// SendResultSetPackets 生成完整的 ResultSet 包序列
// 返回所有需要发送的包（不包括 MySQL packet header）
func (e *MySQLProtocolEncoder) SendResultSetPackets(data *ResultSetData) [][]byte {
	var packets [][]byte

	// 1. Column Count Packet
	columnCount := uint64(len(data.Columns))
	packets = append(packets, e.WriteLenEncInt(columnCount))

	// 2. Column Definition Packets
	// 从第一行数据推断列类型
	for i, colName := range data.Columns {
		var colDef *ColumnDefinition

		if len(data.Rows) > 0 && i < len(data.Rows[0]) {
			// 根据第一行数据推断类型
			colDef = e.CreateColumnDefinitionFromValue(colName, data.Rows[0][i])
		} else {
			// 没有数据，默认为 VARCHAR
			colDef = e.CreateColumnDefinition(colName, MYSQL_TYPE_VAR_STRING, 0)
		}

		packets = append(packets, e.WriteColumnDefinitionPacket(colDef))
	}

	// 3. EOF Packet (after column definitions)
	// status_flags: SERVER_STATUS_AUTOCOMMIT
	packets = append(packets, e.WriteEOFPacket(0, SERVER_STATUS_AUTOCOMMIT))

	// 4. Row Data Packets
	for _, row := range data.Rows {
		packets = append(packets, e.WriteRowDataPacket(row))
	}

	// 5. EOF Packet (after rows)
	packets = append(packets, e.WriteEOFPacket(0, SERVER_STATUS_AUTOCOMMIT))

	return packets
}

// ============================================================================
// 工具函数：计算 lenenc-int 编码后的长度
// ============================================================================

// LenEncIntSize 返回 lenenc-int 编码后的字节数
func (e *MySQLProtocolEncoder) LenEncIntSize(value uint64) int {
	if value < 251 {
		return 1
	} else if value < (1 << 16) {
		return 3
	} else if value < (1 << 24) {
		return 4
	} else {
		return 9
	}
}

// LenEncStringSize 返回 lenenc-str 编码后的字节数
func (e *MySQLProtocolEncoder) LenEncStringSize(str string) int {
	if str == "" {
		return 1
	}
	length := uint64(len(str))
	return e.LenEncIntSize(length) + len(str)
}

// ============================================================================
// 特殊值处理
// ============================================================================

// IsNaN 检查浮点数是否为 NaN
func (e *MySQLProtocolEncoder) IsNaN(value interface{}) bool {
	switch v := value.(type) {
	case float32:
		return math.IsNaN(float64(v))
	case float64:
		return math.IsNaN(v)
	default:
		return false
	}
}

// IsInf 检查浮点数是否为无穷大
func (e *MySQLProtocolEncoder) IsInf(value interface{}) bool {
	switch v := value.(type) {
	case float32:
		return math.IsInf(float64(v), 0)
	case float64:
		return math.IsInf(v, 0)
	default:
		return false
	}
}
