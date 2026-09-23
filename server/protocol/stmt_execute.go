package protocol

import (
	"bytes"
	"fmt"
	"math"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

// ParseBinaryStmtExecuteParams 解析 COM_STMT_EXECUTE 中 null-bitmap 之后到包尾的二进制参数区。
// prevParamTypeBlock 为上一帧绑定的参数类型块（每条参数 2 字节：type + unsigned flag），
// 当 new_params_bound_flag==0 时复用；为 nil 时按 MYSQL_TYPE_VAR_STRING 推断。
func ParseBinaryStmtExecuteParams(data []byte, paramCount uint16, prevParamTypeBlock []byte) (params []interface{}, newParamTypeBlock []byte, err error) {
	if paramCount == 0 {
		return nil, prevParamTypeBlock, nil
	}
	params = make([]interface{}, paramCount)

	nullBitmapLen := (int(paramCount) + 7) / 8
	if len(data) < nullBitmapLen+1 {
		return nil, nil, fmt.Errorf("invalid execute packet: insufficient data for null bitmap")
	}

	nullBitmap := data[:nullBitmapLen]
	pos := nullBitmapLen

	newParamsBoundFlag := data[pos]
	pos++

	var paramTypes []byte
	switch newParamsBoundFlag {
	case 1:
		if len(data) < pos+int(paramCount)*2 {
			return nil, nil, fmt.Errorf("invalid execute packet: insufficient data for param types")
		}
		paramTypes = data[pos : pos+int(paramCount)*2]
		pos += int(paramCount) * 2
	case 0:
		paramTypes = prevParamTypeBlock
		if len(paramTypes) < int(paramCount)*2 {
			// 首跑且无历史类型：按字符串处理
			paramTypes = make([]byte, int(paramCount)*2)
			for i := 0; i < int(paramCount); i++ {
				paramTypes[i*2] = common.COLUMN_TYPE_VAR_STRING
				paramTypes[i*2+1] = 0
			}
		}
	default:
		return nil, nil, fmt.Errorf("invalid execute packet: unknown new_params_bound_flag %d", newParamsBoundFlag)
	}

	for i := uint16(0); i < paramCount; i++ {
		bytePos := int(i) / 8
		bitPos := int(i) % 8
		if nullBitmap[bytePos]&(1<<bitPos) != 0 {
			params[i] = nil
			continue
		}

		paramType := paramTypes[i*2]
		unsigned := paramTypes[i*2+1] != 0
		value, bytesRead, perr := parseBinaryParamValue(data[pos:], paramType, unsigned)
		if perr != nil {
			return nil, nil, fmt.Errorf("failed to parse param %d: %w", i, perr)
		}
		params[i] = value
		pos += bytesRead
	}

	usedTypes := make([]byte, paramCount*2)
	copy(usedTypes, paramTypes[:paramCount*2])
	return params, usedTypes, nil
}

func parseBinaryParamValue(data []byte, paramType byte, unsigned bool) (interface{}, int, error) {
	switch paramType {
	case common.COLUMN_TYPE_TINY:
		if len(data) < 1 {
			return nil, 0, fmt.Errorf("insufficient data for TINY")
		}
		if unsigned {
			return uint8(data[0]), 1, nil
		}
		return int8(data[0]), 1, nil

	case common.COLUMN_TYPE_SHORT:
		if len(data) < 2 {
			return nil, 0, fmt.Errorf("insufficient data for SHORT")
		}
		if unsigned {
			return uint16(data[0]) | uint16(data[1])<<8, 2, nil
		}
		value := int16(data[0]) | int16(data[1])<<8
		return value, 2, nil

	case common.COLUMN_TYPE_LONG, common.COLUMN_TYPE_INT24:
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for LONG")
		}
		if unsigned {
			return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24, 4, nil
		}
		value := int32(data[0]) | int32(data[1])<<8 | int32(data[2])<<16 | int32(data[3])<<24
		return value, 4, nil

	case common.COLUMN_TYPE_LONGLONG:
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for LONGLONG")
		}
		if unsigned {
			value := uint64(data[0]) | uint64(data[1])<<8 | uint64(data[2])<<16 | uint64(data[3])<<24 |
				uint64(data[4])<<32 | uint64(data[5])<<40 | uint64(data[6])<<48 | uint64(data[7])<<56
			return value, 8, nil
		}
		value := int64(data[0]) | int64(data[1])<<8 | int64(data[2])<<16 | int64(data[3])<<24 |
			int64(data[4])<<32 | int64(data[5])<<40 | int64(data[6])<<48 | int64(data[7])<<56
		return value, 8, nil

	case common.COLUMN_TYPE_DATE, common.COLUMN_TYPE_DATETIME, common.COLUMN_TYPE_TIMESTAMP,
		common.COLUMN_TYPE_DATETIME_V2, common.COLUMN_TYPE_TIMESTAMP_V2:
		return parseBinaryDateTime(data, paramType)

	case common.COLUMN_TYPE_TIME, common.COLUMN_TYPE_TIME_V2:
		return parseBinaryTime(data)

	case common.COLUMN_TYPE_FLOAT:
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for FLOAT")
		}
		bits := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
		return math.Float32frombits(bits), 4, nil

	case common.COLUMN_TYPE_DOUBLE:
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for DOUBLE")
		}
		bits := uint64(data[0]) | uint64(data[1])<<8 | uint64(data[2])<<16 | uint64(data[3])<<24 |
			uint64(data[4])<<32 | uint64(data[5])<<40 | uint64(data[6])<<48 | uint64(data[7])<<56
		return math.Float64frombits(bits), 8, nil

	case common.COLUMN_TYPE_VAR_STRING, common.COLUMN_TYPE_STRING, common.COLUMN_TYPE_VARCHAR,
		common.COLUMN_TYPE_BLOB, common.COLUMN_TYPE_TINY_BLOB, common.COLUMN_TYPE_MEDIUM_BLOB, common.COLUMN_TYPE_LONG_BLOB:
		strLen, lenBytes := readLenEncIntForStmt(data)
		if strLen < 0 {
			return nil, 0, fmt.Errorf("invalid length-encoded string")
		}
		totalBytes := lenBytes + int(strLen)
		if len(data) < totalBytes {
			return nil, 0, fmt.Errorf("insufficient data for string")
		}
		value := string(data[lenBytes:totalBytes])
		return value, totalBytes, nil

	default:
		strLen, lenBytes := readLenEncIntForStmt(data)
		if strLen < 0 {
			return nil, 0, fmt.Errorf("invalid length-encoded value")
		}
		totalBytes := lenBytes + int(strLen)
		if len(data) < totalBytes {
			return nil, 0, fmt.Errorf("insufficient data for value")
		}
		value := string(data[lenBytes:totalBytes])
		return value, totalBytes, nil
	}
}

func parseBinaryDateTime(data []byte, paramType byte) (interface{}, int, error) {
	if len(data) < 1 {
		return nil, 0, fmt.Errorf("insufficient data for temporal value")
	}
	length := int(data[0])
	if len(data) < length+1 {
		return nil, 0, fmt.Errorf("insufficient data for temporal value")
	}
	if length == 0 {
		return "0000-00-00", 1, nil
	}
	if length < 4 {
		return nil, 0, fmt.Errorf("invalid temporal value length %d", length)
	}
	payload := data[1 : length+1]
	year := uint16(payload[0]) | uint16(payload[1])<<8
	month, day := payload[2], payload[3]
	if paramType == common.COLUMN_TYPE_DATE {
		return fmt.Sprintf("%04d-%02d-%02d", year, month, day), length + 1, nil
	}
	if length < 7 {
		return fmt.Sprintf("%04d-%02d-%02d 00:00:00", year, month, day), length + 1, nil
	}
	value := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, payload[4], payload[5], payload[6])
	if length >= 11 {
		microseconds := uint32(payload[7]) | uint32(payload[8])<<8 | uint32(payload[9])<<16 | uint32(payload[10])<<24
		if microseconds > 0 {
			value += fmt.Sprintf(".%06d", microseconds)
		}
	}
	return value, length + 1, nil
}

func parseBinaryTime(data []byte) (interface{}, int, error) {
	if len(data) < 1 {
		return nil, 0, fmt.Errorf("insufficient data for TIME")
	}
	length := int(data[0])
	if len(data) < length+1 {
		return nil, 0, fmt.Errorf("insufficient data for TIME")
	}
	if length == 0 {
		return "00:00:00", 1, nil
	}
	if length < 8 {
		return nil, 0, fmt.Errorf("invalid TIME value length %d", length)
	}
	payload := data[1 : length+1]
	days := uint32(payload[1]) | uint32(payload[2])<<8 | uint32(payload[3])<<16 | uint32(payload[4])<<24
	hours := days*24 + uint32(payload[5])
	value := fmt.Sprintf("%02d:%02d:%02d", hours, payload[6], payload[7])
	if payload[0] != 0 {
		value = "-" + value
	}
	if length >= 12 {
		microseconds := uint32(payload[8]) | uint32(payload[9])<<8 | uint32(payload[10])<<16 | uint32(payload[11])<<24
		if microseconds > 0 {
			value += fmt.Sprintf(".%06d", microseconds)
		}
	}
	return value, length + 1, nil
}

// ReadLengthEncodedInteger 解析 MySQL length-encoded integer（供测试与协议工具使用）。
func ReadLengthEncodedInteger(data []byte) (int64, int) {
	return readLenEncIntForStmt(data)
}

func readLenEncIntForStmt(data []byte) (int64, int) {
	if len(data) == 0 {
		return -1, 0
	}

	first := data[0]
	if first < 0xfb {
		return int64(first), 1
	}

	switch first {
	case 0xfc:
		if len(data) < 3 {
			return -1, 0
		}
		return int64(data[1]) | int64(data[2])<<8, 3
	case 0xfd:
		if len(data) < 4 {
			return -1, 0
		}
		return int64(data[1]) | int64(data[2])<<8 | int64(data[3])<<16, 4
	case 0xfe:
		if len(data) < 9 {
			return -1, 0
		}
		return int64(data[1]) | int64(data[2])<<8 | int64(data[3])<<16 | int64(data[4])<<24 |
			int64(data[5])<<32 | int64(data[6])<<40 | int64(data[7])<<48 | int64(data[8])<<56, 9
	default:
		return -1, 0
	}
}

// BindPreparedSQL 将 ? 占位符替换为字面量（用于转回文本 SQL 走现有执行器）。
func BindPreparedSQL(sql string, params []interface{}) string {
	if len(params) == 0 {
		return sql
	}

	positions := sqlPlaceholderPositions(sql)
	if len(positions) == 0 {
		return sql
	}
	var result bytes.Buffer
	last := 0
	for paramIndex, position := range positions {
		result.WriteString(sql[last:position])
		if paramIndex < len(params) {
			result.WriteString(formatPreparedParameter(params[paramIndex]))
		} else {
			result.WriteByte('?')
		}
		last = position + 1
	}
	result.WriteString(sql[last:])
	return result.String()
}

func formatPreparedParameter(param interface{}) string {
	if param == nil {
		return "NULL"
	}
	switch v := param.(type) {
	case []byte:
		return "'" + escapePreparedString(v) + "'"
	case string:
		return "'" + escapePreparedString([]byte(v)) + "'"
	case int8, int16, int32, int64, int:
		return fmt.Sprintf("%d", v)
	case uint8, uint16, uint32, uint64, uint:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case float64:
		return fmt.Sprintf("%g", v)
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

func escapePreparedString(value []byte) string {
	var escaped bytes.Buffer
	for _, ch := range value {
		switch ch {
		case 0:
			escaped.WriteString(`\0`)
		case '\n':
			escaped.WriteString(`\n`)
		case '\r':
			escaped.WriteString(`\r`)
		case '\\':
			escaped.WriteString(`\\`)
		case 0x1a:
			escaped.WriteString(`\Z`)
		case '\'':
			escaped.WriteString("''")
		default:
			escaped.WriteByte(ch)
		}
	}
	return escaped.String()
}

// sqlPlaceholderPositions returns only executable '?' markers. MySQL permits
// question marks in string literals, quoted identifiers and comments; those
// bytes must not contribute to COM_STMT_PREPARE's parameter count or binding.
func sqlPlaceholderPositions(sql string) []int {
	positions := make([]int, 0)
	for i := 0; i < len(sql); {
		switch sql[i] {
		case '\'', '"', '`':
			quote := sql[i]
			i++
			for i < len(sql) {
				if sql[i] == '\\' {
					i += 2
					continue
				}
				if sql[i] == quote {
					if i+1 < len(sql) && sql[i+1] == quote {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
		case '#':
			i = skipSQLLineComment(sql, i+1)
		case '-':
			if i+2 < len(sql) && sql[i+1] == '-' && isSQLCommentWhitespace(sql[i+2]) {
				i = skipSQLLineComment(sql, i+2)
			} else {
				i++
			}
		case '/':
			if i+1 < len(sql) && sql[i+1] == '*' {
				i += 2
				for i+1 < len(sql) && !(sql[i] == '*' && sql[i+1] == '/') {
					i++
				}
				if i+1 < len(sql) {
					i += 2
				}
			} else {
				i++
			}
		case '?':
			positions = append(positions, i)
			i++
		default:
			i++
		}
	}
	return positions
}

func skipSQLLineComment(sql string, start int) int {
	for start < len(sql) && sql[start] != '\r' && sql[start] != '\n' {
		start++
	}
	return start
}

func isSQLCommentWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == '\f' || ch == '\v'
}
