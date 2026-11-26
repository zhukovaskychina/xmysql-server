package engine

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// mysqlSetOperationColumnType resolves the result metadata for one column of a
// UNION, INTERSECT, or EXCEPT expression. MySQL derives these columns from all
// query blocks, rather than copying the first branch's type. XMySQL-only typed
// values remain strict: mixing CONTENT or VECTOR with another type has no
// defined implicit conversion and is rejected.
func mysqlSetOperationColumnType(left, right metadata.DataType) (metadata.DataType, error) {
	left = normalizeSetOperationType(left)
	right = normalizeSetOperationType(right)
	if left == "" || right == "" {
		return "", fmt.Errorf("set-operation result type is unknown")
	}
	if left == right {
		return left, nil
	}
	if isNativeSetOperationType(left) || isNativeSetOperationType(right) {
		return "", fmt.Errorf("cannot combine set-operation result types %s and %s", left, right)
	}
	if leftRank, leftInteger := setOperationIntegerRank(left); leftInteger {
		if rightRank, rightInteger := setOperationIntegerRank(right); rightInteger {
			if rightRank > leftRank {
				left = right
			}
			return left, nil
		}
	}
	if isSetOperationNumeric(left) && isSetOperationNumeric(right) {
		if left == metadata.TypeDouble || right == metadata.TypeDouble || left == metadata.TypeFloat || right == metadata.TypeFloat {
			return metadata.TypeDouble, nil
		}
		return metadata.TypeDecimal, nil
	}
	if isSetOperationString(left) && isSetOperationString(right) {
		if left == metadata.TypeEnum || left == metadata.TypeSet || left == metadata.TypeJSON || right == metadata.TypeEnum || right == metadata.TypeSet || right == metadata.TypeJSON {
			return metadata.TypeVarchar, nil
		}
		return widestSetOperationType(left, right, map[metadata.DataType]int{
			metadata.TypeChar: 1, metadata.TypeVarchar: 2,
			metadata.TypeTinyText: 3, metadata.TypeText: 4,
			metadata.TypeMediumText: 5, metadata.TypeLongText: 6,
		})
	}
	if isSetOperationBinary(left) && isSetOperationBinary(right) {
		return widestSetOperationType(left, right, map[metadata.DataType]int{
			metadata.TypeBinary: 1, metadata.TypeVarBinary: 2,
			metadata.TypeTinyBlob: 3, metadata.TypeBlob: 4,
			metadata.TypeMediumBlob: 5, metadata.TypeLongBlob: 6,
		})
	}
	if isSetOperationString(left) && isSetOperationBinary(right) || isSetOperationBinary(left) && isSetOperationString(right) {
		return metadata.TypeVarBinary, nil
	}
	if isSetOperationNumeric(left) && isSetOperationString(right) || isSetOperationString(left) && isSetOperationNumeric(right) {
		return metadata.TypeVarchar, nil
	}
	if left == metadata.TypeDate && (right == metadata.TypeDateTime || right == metadata.TypeTimestamp) || right == metadata.TypeDate && (left == metadata.TypeDateTime || left == metadata.TypeTimestamp) {
		return metadata.TypeDateTime, nil
	}
	if left == metadata.TypeDateTime && right == metadata.TypeTimestamp || right == metadata.TypeDateTime && left == metadata.TypeTimestamp {
		return metadata.TypeDateTime, nil
	}
	// For built-in scalar types without a narrower common representation, MySQL
	// exposes a string result. This does not change the stored types of inputs.
	return metadata.TypeVarchar, nil
}

func mergeSetOperationColumnTypes(left, right []string) ([]string, error) {
	if len(left) == 0 {
		return append([]string(nil), right...), nil
	}
	if len(right) == 0 {
		return append([]string(nil), left...), nil
	}
	if len(left) != len(right) {
		return nil, fmt.Errorf("set-operation column type counts differ")
	}
	merged := append([]string(nil), left...)
	for index := range merged {
		leftType := metadata.DataType(strings.TrimSpace(left[index]))
		rightType := metadata.DataType(strings.TrimSpace(right[index]))
		if leftType == "" {
			merged[index] = right[index]
			continue
		}
		if rightType == "" {
			continue
		}
		commonType, err := mysqlSetOperationColumnType(leftType, rightType)
		if err != nil {
			return nil, fmt.Errorf("set-operation column %d: %w", index+1, err)
		}
		merged[index] = strings.ToLower(string(commonType))
	}
	return merged, nil
}

// setOperationRecordKey compares values using the common result type. This is
// required for DISTINCT/INTERSECT/EXCEPT because the same SQL value may be
// represented by different Go types in separate branches (for example, 1 and
// "1" after MySQL resolves their common result column as VARCHAR).
func setOperationRecordKey(record Record, columnTypes []string) string {
	if record == nil {
		return "<nil-record>"
	}
	values := record.GetValues()
	var key strings.Builder
	for index, value := range values {
		part := "NULL"
		if value != nil && !value.IsNull() {
			typ := metadata.DataType("")
			if index < len(columnTypes) {
				typ = normalizeSetOperationType(metadata.DataType(columnTypes[index]))
			}
			part = setOperationCanonicalValue(typ, value)
		}
		fmt.Fprintf(&key, "%d:%s", len(part), part)
	}
	return key.String()
}

func coerceSetOperationRecords(records []Record, columnTypes []string) error {
	for recordIndex, record := range records {
		if record == nil {
			continue
		}
		values := record.GetValues()
		for columnIndex, value := range values {
			if value == nil || value.IsNull() || columnIndex >= len(columnTypes) {
				continue
			}
			typ := normalizeSetOperationType(metadata.DataType(columnTypes[columnIndex]))
			var converted basic.Value
			switch {
			case isSetOperationString(typ):
				converted = basic.NewStringValue(setOperationTextValue(value))
			case isSetOperationBinary(typ):
				converted = basic.NewBytes([]byte(setOperationTextValue(value)))
			case typ == metadata.TypeDouble || typ == metadata.TypeFloat:
				parsed, err := strconv.ParseFloat(setOperationTextValue(value), 64)
				if err != nil {
					return fmt.Errorf("set-operation row %d column %d cannot convert %q to %s", recordIndex+1, columnIndex+1, setOperationTextValue(value), typ)
				}
				converted = basic.NewFloat64(parsed)
			default:
				continue
			}
			if err := record.SetValueByIndex(columnIndex, converted); err != nil {
				return fmt.Errorf("set-operation row %d column %d: %w", recordIndex+1, columnIndex+1, err)
			}
		}
	}
	return nil
}

func setOperationTextValue(value basic.Value) string {
	switch value.Type() {
	case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeInt, basic.ValueTypeBigInt, basic.ValueTypeYear:
		return strconv.FormatInt(value.Int(), 10)
	case basic.ValueTypeFloat, basic.ValueTypeDouble:
		return strconv.FormatFloat(value.Float64(), 'g', -1, 64)
	case basic.ValueTypeDecimal:
		return value.ToString()
	case basic.ValueTypeBool, basic.ValueTypeBoolean:
		if value.Bool() {
			return "1"
		}
		return "0"
	case basic.ValueTypeBinary, basic.ValueTypeVarBinary, basic.ValueTypeTinyBlob, basic.ValueTypeBlob, basic.ValueTypeMediumBlob, basic.ValueTypeLongBlob:
		return string(value.Bytes())
	default:
		return value.String()
	}
}

func setOperationCanonicalValue(typ metadata.DataType, value basic.Value) string {
	var raw interface{} = value.Raw()
	switch value.Type() {
	case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeInt, basic.ValueTypeBigInt, basic.ValueTypeYear, basic.ValueTypeBool, basic.ValueTypeBoolean:
		raw = value.Int()
	case basic.ValueTypeFloat, basic.ValueTypeDouble:
		raw = value.Float64()
	case basic.ValueTypeDecimal:
		raw = value.ToString()
	case basic.ValueTypeChar, basic.ValueTypeVarchar, basic.ValueTypeTinyText, basic.ValueTypeText, basic.ValueTypeMediumText, basic.ValueTypeLongText, basic.ValueTypeEnum, basic.ValueTypeSet, basic.ValueTypeJSON:
		raw = value.String()
	case basic.ValueTypeBinary, basic.ValueTypeVarBinary, basic.ValueTypeTinyBlob, basic.ValueTypeBlob, basic.ValueTypeMediumBlob, basic.ValueTypeLongBlob:
		raw = value.Bytes()
	}
	if isSetOperationNumeric(typ) {
		if canonical, ok := canonicalSetOperationNumber(raw, typ); ok {
			return "N:" + canonical
		}
	}
	if isSetOperationBinary(typ) {
		switch typed := raw.(type) {
		case []byte:
			return "B:" + hex.EncodeToString(typed)
		case string:
			return "B:" + hex.EncodeToString([]byte(typed))
		}
	}
	if isSetOperationString(typ) {
		return "S:" + fmt.Sprint(raw)
	}
	return fmt.Sprintf("%T:%v", raw, raw)
}

func canonicalSetOperationNumber(value interface{}, typ metadata.DataType) (string, bool) {
	var text string
	switch typed := value.(type) {
	case float32:
		text = strconv.FormatFloat(float64(typed), 'g', -1, 32)
	case float64:
		if typ == metadata.TypeFloat || typ == metadata.TypeDouble {
			return "F:" + strconv.FormatFloat(typed, 'g', -1, 64), true
		}
		text = strconv.FormatFloat(typed, 'g', -1, 64)
	default:
		text = strings.TrimSpace(fmt.Sprint(value))
	}
	if typ == metadata.TypeFloat || typ == metadata.TypeDouble {
		parsed, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return "", false
		}
		return "F:" + strconv.FormatFloat(parsed, 'g', -1, 64), true
	}
	rat, ok := new(big.Rat).SetString(text)
	if !ok {
		return "", false
	}
	return "R:" + rat.RatString(), true
}

func normalizeSetOperationType(typ metadata.DataType) metadata.DataType {
	return metadata.DataType(strings.ToUpper(strings.TrimSpace(string(typ))))
}

func isNativeSetOperationType(typ metadata.DataType) bool {
	return typ == metadata.TypeContent || typ == metadata.TypeVector
}

func setOperationIntegerRank(typ metadata.DataType) (int, bool) {
	switch typ {
	case metadata.TypeTinyInt, metadata.TypeBool, metadata.TypeBoolean:
		return 1, true
	case metadata.TypeSmallInt:
		return 2, true
	case metadata.TypeMediumInt:
		return 3, true
	case metadata.TypeInt:
		return 4, true
	case metadata.TypeBigInt, metadata.TypeBit, metadata.TypeYear:
		return 5, true
	default:
		return 0, false
	}
}

func isSetOperationNumeric(typ metadata.DataType) bool {
	if _, ok := setOperationIntegerRank(typ); ok {
		return true
	}
	return typ == metadata.TypeFloat || typ == metadata.TypeDouble || typ == metadata.TypeDecimal
}

func isSetOperationString(typ metadata.DataType) bool {
	switch typ {
	case metadata.TypeChar, metadata.TypeVarchar, metadata.TypeTinyText, metadata.TypeText, metadata.TypeMediumText, metadata.TypeLongText, metadata.TypeEnum, metadata.TypeSet, metadata.TypeJSON:
		return true
	default:
		return false
	}
}

func isSetOperationBinary(typ metadata.DataType) bool {
	switch typ {
	case metadata.TypeBinary, metadata.TypeVarBinary, metadata.TypeTinyBlob, metadata.TypeBlob, metadata.TypeMediumBlob, metadata.TypeLongBlob:
		return true
	default:
		return false
	}
}

func widestSetOperationType(left, right metadata.DataType, rank map[metadata.DataType]int) (metadata.DataType, error) {
	leftRank, leftKnown := rank[left]
	rightRank, rightKnown := rank[right]
	if !leftKnown || !rightKnown {
		return "", fmt.Errorf("set-operation result type is unknown for %s and %s", left, right)
	}
	if rightRank > leftRank {
		return right, nil
	}
	return left, nil
}
