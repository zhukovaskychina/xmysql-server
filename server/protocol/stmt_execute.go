package protocol

import (
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
		value, bytesRead, perr := parseBinaryParamValue(data[pos:], paramType)
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

func parseBinaryParamValue(data []byte, paramType byte) (interface{}, int, error) {
	switch paramType {
	case common.COLUMN_TYPE_TINY:
		if len(data) < 1 {
			return nil, 0, fmt.Errorf("insufficient data for TINY")
		}
		return int8(data[0]), 1, nil

	case common.COLUMN_TYPE_SHORT:
		if len(data) < 2 {
			return nil, 0, fmt.Errorf("insufficient data for SHORT")
		}
		value := int16(data[0]) | int16(data[1])<<8
		return value, 2, nil

	case common.COLUMN_TYPE_LONG, common.COLUMN_TYPE_INT24:
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for LONG")
		}
		value := int32(data[0]) | int32(data[1])<<8 | int32(data[2])<<16 | int32(data[3])<<24
		return value, 4, nil

	case common.COLUMN_TYPE_LONGLONG:
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for LONGLONG")
		}
		value := int64(data[0]) | int64(data[1])<<8 | int64(data[2])<<16 | int64(data[3])<<24 |
			int64(data[4])<<32 | int64(data[5])<<40 | int64(data[6])<<48 | int64(data[7])<<56
		return value, 8, nil

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

	result := ""
	paramIndex := 0

	for i := 0; i < len(sql); i++ {
		if sql[i] == '?' && paramIndex < len(params) {
			param := params[paramIndex]
			paramIndex++

			if param == nil {
				result += "NULL"
			} else {
				switch v := param.(type) {
				case string:
					escaped := ""
					for _, ch := range v {
						if ch == '\'' {
							escaped += "''"
						} else {
							escaped += string(ch)
						}
					}
					result += "'" + escaped + "'"
				case int8, int16, int32, int64, int:
					result += fmt.Sprintf("%d", v)
				case uint8, uint16, uint32, uint64, uint:
					result += fmt.Sprintf("%d", v)
				case float32:
					result += fmt.Sprintf("%g", v)
				case float64:
					result += fmt.Sprintf("%g", v)
				default:
					result += fmt.Sprintf("'%v'", v)
				}
			}
		} else {
			result += string(sql[i])
		}
	}

	return result
}
