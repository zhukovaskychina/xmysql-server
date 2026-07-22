package engine

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

const (
	clusteredRecordMagic   = "XIR1"
	clusteredRecordVersion = byte(1)

	clusteredRecordNull   = byte(0)
	clusteredRecordInt    = byte(1)
	clusteredRecordUint   = byte(2)
	clusteredRecordFloat  = byte(3)
	clusteredRecordBool   = byte(4)
	clusteredRecordString = byte(5)
	clusteredRecordBytes  = byte(6)
)

// EncodeClusteredRecord encodes a clustered row using table column order.
func EncodeClusteredRecord(row *InsertRowData, tableMeta *metadata.TableMeta) ([]byte, error) {
	if row == nil {
		return nil, fmt.Errorf("row is nil")
	}
	if tableMeta == nil {
		return nil, fmt.Errorf("table metadata is nil")
	}
	if len(tableMeta.Columns) > int(^uint16(0)) {
		return nil, fmt.Errorf("too many columns: %d", len(tableMeta.Columns))
	}

	buffer := make([]byte, 0)
	buffer = append(buffer, []byte(clusteredRecordMagic)...)
	buffer = append(buffer, clusteredRecordVersion)
	buffer = binary.BigEndian.AppendUint16(buffer, uint16(len(tableMeta.Columns)))

	for idx, col := range tableMeta.Columns {
		if col == nil {
			return nil, fmt.Errorf("column %d metadata is nil", idx)
		}

		value, ok := row.ColumnValues[col.Name]
		if !ok {
			value = col.DefaultValue
		}

		valueType, valueBytes, err := encodeClusteredRecordValue(value, col.Type)
		if err != nil {
			return nil, fmt.Errorf("encode column %s: %v", col.Name, err)
		}
		if len(valueBytes) > int(^uint32(0)) {
			return nil, fmt.Errorf("column %s too large: %d bytes", col.Name, len(valueBytes))
		}

		buffer = append(buffer, valueType)
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(valueBytes)))
		buffer = append(buffer, valueBytes...)
	}

	return buffer, nil
}

// DecodeClusteredRecord decodes a clustered row using table column order.
func DecodeClusteredRecord(data []byte, tableMeta *metadata.TableMeta) (*InsertRowData, error) {
	if tableMeta == nil {
		return nil, fmt.Errorf("table metadata is nil")
	}
	if len(data) < len(clusteredRecordMagic)+1+2 {
		return nil, fmt.Errorf("clustered record payload too short")
	}
	if string(data[:len(clusteredRecordMagic)]) != clusteredRecordMagic {
		return nil, fmt.Errorf("invalid clustered record magic")
	}

	offset := len(clusteredRecordMagic)
	if data[offset] != clusteredRecordVersion {
		return nil, fmt.Errorf("unsupported clustered record version: %d", data[offset])
	}
	offset++

	columnCount := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2
	if int(columnCount) > len(tableMeta.Columns) {
		return nil, fmt.Errorf("clustered record column count %d exceeds metadata column count %d", columnCount, len(tableMeta.Columns))
	}

	row := &InsertRowData{
		ColumnValues: make(map[string]interface{}, len(tableMeta.Columns)),
		ColumnTypes:  make(map[string]metadata.DataType, len(tableMeta.Columns)),
	}

	for idx, col := range tableMeta.Columns {
		if col == nil {
			return nil, fmt.Errorf("column %d metadata is nil", idx)
		}
		if idx >= int(columnCount) {
			if col.DefaultValue != nil {
				row.ColumnValues[col.Name] = normalizeDefaultValue(col.DefaultValue, col.Type)
			} else {
				row.ColumnValues[col.Name] = nil
			}
			row.ColumnTypes[col.Name] = col.Type
			continue
		}
		if offset+1+4 > len(data) {
			return nil, fmt.Errorf("column %s header truncated", col.Name)
		}

		valueType := data[offset]
		offset++
		valueLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if valueLen > uint32(len(data)-offset) {
			return nil, fmt.Errorf("column %s value truncated", col.Name)
		}

		value, err := decodeClusteredRecordValue(valueType, data[offset:offset+int(valueLen)])
		if err != nil {
			return nil, fmt.Errorf("decode column %s: %v", col.Name, err)
		}
		offset += int(valueLen)

		row.ColumnValues[col.Name] = value
		row.ColumnTypes[col.Name] = col.Type
	}

	if offset != len(data) {
		return nil, fmt.Errorf("clustered record has %d trailing bytes", len(data)-offset)
	}

	return row, nil
}

func encodeClusteredRecordValue(value interface{}, dataType metadata.DataType) (byte, []byte, error) {
	if value == nil {
		return clusteredRecordNull, nil, nil
	}

	switch dataType {
	case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt,
		metadata.TypeBigInt, metadata.TypeYear:
		if uintValue, ok := codecAsUint64(value); ok {
			return clusteredRecordUint, binary.BigEndian.AppendUint64(nil, uintValue), nil
		}
		intValue, err := codecToInt64(value)
		if err != nil {
			return 0, nil, err
		}
		return clusteredRecordInt, binary.BigEndian.AppendUint64(nil, uint64(intValue)), nil
	case metadata.TypeFloat, metadata.TypeDouble:
		floatValue, err := codecToFloat64(value)
		if err != nil {
			return 0, nil, err
		}
		return clusteredRecordFloat, binary.BigEndian.AppendUint64(nil, math.Float64bits(floatValue)), nil
	case metadata.TypeBool, metadata.TypeBoolean:
		boolValue, err := codecToBool(value)
		if err != nil {
			return 0, nil, err
		}
		if boolValue {
			return clusteredRecordBool, []byte{1}, nil
		}
		return clusteredRecordBool, []byte{0}, nil
	case metadata.TypeBinary, metadata.TypeVarBinary, metadata.TypeTinyBlob, metadata.TypeBlob,
		metadata.TypeMediumBlob, metadata.TypeLongBlob:
		bytesValue, err := codecToBytes(value)
		if err != nil {
			return 0, nil, err
		}
		return clusteredRecordBytes, bytesValue, nil
	default:
		stringValue, err := codecToString(value)
		if err != nil {
			return 0, nil, err
		}
		return clusteredRecordString, []byte(stringValue), nil
	}
}

func decodeClusteredRecordValue(valueType byte, data []byte) (interface{}, error) {
	switch valueType {
	case clusteredRecordNull:
		if len(data) != 0 {
			return nil, fmt.Errorf("null value length must be 0, got %d", len(data))
		}
		return nil, nil
	case clusteredRecordInt:
		if len(data) != 8 {
			return nil, fmt.Errorf("int value length must be 8, got %d", len(data))
		}
		return int64(binary.BigEndian.Uint64(data)), nil
	case clusteredRecordUint:
		if len(data) != 8 {
			return nil, fmt.Errorf("uint value length must be 8, got %d", len(data))
		}
		return binary.BigEndian.Uint64(data), nil
	case clusteredRecordFloat:
		if len(data) != 8 {
			return nil, fmt.Errorf("float value length must be 8, got %d", len(data))
		}
		return math.Float64frombits(binary.BigEndian.Uint64(data)), nil
	case clusteredRecordBool:
		if len(data) != 1 {
			return nil, fmt.Errorf("bool value length must be 1, got %d", len(data))
		}
		switch data[0] {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return nil, fmt.Errorf("invalid bool value: %d", data[0])
		}
	case clusteredRecordString:
		return string(data), nil
	case clusteredRecordBytes:
		return append([]byte(nil), data...), nil
	default:
		return nil, fmt.Errorf("unknown value type: %d", valueType)
	}
}

func codecAsUint64(value interface{}) (uint64, bool) {
	switch v := value.(type) {
	case uint:
		return uint64(v), true
	case uint8:
		return uint64(v), true
	case uint16:
		return uint64(v), true
	case uint32:
		return uint64(v), true
	case uint64:
		return v, true
	default:
		return 0, false
	}
}

func codecToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported int value %T", value)
	}
}

func codecToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("unsupported float value %T", value)
	}
}

func codecToBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int:
		return v != 0, nil
	case int8:
		return v != 0, nil
	case int16:
		return v != 0, nil
	case int32:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case uint:
		return v != 0, nil
	case uint8:
		return v != 0, nil
	case uint16:
		return v != 0, nil
	case uint32:
		return v != 0, nil
	case uint64:
		return v != 0, nil
	case string:
		return strconv.ParseBool(v)
	default:
		return false, fmt.Errorf("unsupported bool value %T", value)
	}
}

func codecToBytes(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return append([]byte(nil), v...), nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("unsupported bytes value %T", value)
	}
}

func codecToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}
