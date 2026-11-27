package engine

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
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
	// MySQL ignores NULL-typed expressions while aggregating set-operation
	// column types. Keep NULL only when every branch for the column is NULL.
	if left == metadata.TypeNull {
		return right, nil
	}
	if right == metadata.TypeNull {
		return left, nil
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

func mergeSetOperationColumnMetadata(left, right []*metadata.ColumnMeta, columnTypes []string) ([]*metadata.ColumnMeta, error) {
	if len(left) == 0 {
		return cloneSetOperationColumnMetadata(right, columnTypes), nil
	}
	if len(right) == 0 {
		return cloneSetOperationColumnMetadata(left, columnTypes), nil
	}
	if len(left) != len(right) || len(columnTypes) != len(left) {
		return nil, fmt.Errorf("set-operation column metadata counts differ")
	}
	merged := make([]*metadata.ColumnMeta, len(columnTypes))
	for index := range columnTypes {
		var output metadata.ColumnMeta
		unknownBranch := left[index] == nil || right[index] == nil
		if left[index] != nil {
			output = *left[index]
			output.EnumValues = append([]string(nil), left[index].EnumValues...)
		}
		if right[index] != nil {
			if left[index] == nil {
				output = *right[index]
				output.EnumValues = append([]string(nil), right[index].EnumValues...)
			} else {
				output.Length = max(output.Length, right[index].Length)
				output.Scale = max(output.Scale, right[index].Scale)
				output.IsUnsigned = output.IsUnsigned && right[index].IsUnsigned
				output.IsNullable = output.IsNullable || right[index].IsNullable
			}
		}
		output.Type = normalizeSetOperationType(metadata.DataType(columnTypes[index]))
		if isSetOperationString(output.Type) {
			rightCollation := ""
			if right[index] != nil {
				rightCollation = right[index].Collation
			}
			collation, charset, err := mergeSetOperationCollation(output.Collation, rightCollation, "UNION")
			if err != nil {
				return nil, fmt.Errorf("set-operation column %d: %w", index+1, err)
			}
			output.Collation, output.Charset = collation, charset
		} else {
			output.Collation, output.Charset = "", ""
		}
		if unknownBranch {
			output.IsNullable = true
		}
		if output.Type != metadata.TypeEnum && output.Type != metadata.TypeSet {
			output.EnumValues = nil
		}
		if output.Type == metadata.TypeDecimal {
			leftPrecision, leftScale := setOperationExactNumericShape(left[index])
			rightPrecision, rightScale := setOperationExactNumericShape(right[index])
			output.Scale = min(max(leftScale, rightScale), 30)
			output.Length = min(max(leftPrecision-leftScale, rightPrecision-rightScale)+output.Scale, 65)
			if output.Length < output.Scale {
				output.Length = output.Scale
			}
		}
		merged[index] = &output
	}
	return merged, nil
}

func cloneSetOperationColumnMetadata(columns []*metadata.ColumnMeta, columnTypes []string) []*metadata.ColumnMeta {
	if len(columns) == 0 {
		return nil
	}
	cloned := make([]*metadata.ColumnMeta, len(columns))
	for index, column := range columns {
		if column == nil {
			cloned[index] = &metadata.ColumnMeta{IsNullable: true}
		} else {
			copyColumn := *column
			copyColumn.EnumValues = append([]string(nil), column.EnumValues...)
			cloned[index] = &copyColumn
		}
		if index < len(columnTypes) {
			cloned[index].Type = normalizeSetOperationType(metadata.DataType(columnTypes[index]))
		}
	}
	return cloned
}

func applySetOperationColumnMetadata(records []Record, columns []*metadata.ColumnMeta) {
	if len(columns) == 0 {
		return
	}
	for _, record := range records {
		typed, ok := record.(*EngineExecutorRecord)
		if !ok || typed.GetTableMeta() == nil {
			continue
		}
		tableMeta := typed.GetTableMeta()
		for index, column := range columns {
			if column == nil || index >= len(tableMeta.Columns) || tableMeta.Columns[index] == nil {
				continue
			}
			copyColumn := *column
			copyColumn.Name = tableMeta.Columns[index].Name
			copyColumn.EnumValues = append([]string(nil), column.EnumValues...)
			tableMeta.Columns[index] = &copyColumn
		}
	}
}

func setOperationExactNumericShape(column *metadata.ColumnMeta) (int, int) {
	if column == nil {
		return 10, 0
	}
	typ := normalizeSetOperationType(column.Type)
	if typ == metadata.TypeNull {
		return 0, 0
	}
	if typ == metadata.TypeDecimal {
		precision := column.Length
		if precision < 1 {
			precision = 10
		}
		return min(precision, 65), min(max(column.Scale, 0), 30)
	}
	integerDigits := map[metadata.DataType]int{
		metadata.TypeTinyInt: 3, metadata.TypeSmallInt: 5, metadata.TypeMediumInt: 8,
		metadata.TypeInt: 10, metadata.TypeBigInt: 19, metadata.TypeYear: 4,
		metadata.TypeBool: 1, metadata.TypeBoolean: 1, metadata.TypeBit: 64,
	}[typ]
	if integerDigits == 0 {
		integerDigits = column.Length
	}
	if integerDigits < 1 {
		integerDigits = 10
	}
	if column.IsUnsigned && typ == metadata.TypeBigInt {
		integerDigits = 20
	}
	return min(integerDigits, 65), 0
}

func mergeSetOperationCollation(left, right, operation string) (string, string, error) {
	left, right = strings.TrimSpace(left), strings.TrimSpace(right)
	if left == "" {
		if info, found := common.CollationMetadataByName(right); found {
			return info.Name, info.Charset, nil
		}
		return right, "", nil
	}
	if right == "" || strings.EqualFold(left, right) {
		if info, found := common.CollationMetadataByName(left); found {
			return info.Name, info.Charset, nil
		}
		return left, "", nil
	}
	leftInfo, leftKnown := common.CollationMetadataByName(left)
	rightInfo, rightKnown := common.CollationMetadataByName(right)
	if leftKnown && rightKnown {
		if strings.EqualFold(leftInfo.Charset, rightInfo.Charset) {
			leftBinary := strings.HasSuffix(strings.ToLower(leftInfo.Name), "_bin")
			rightBinary := strings.HasSuffix(strings.ToLower(rightInfo.Name), "_bin")
			if leftBinary != rightBinary {
				if leftBinary {
					return leftInfo.Name, leftInfo.Charset, nil
				}
				return rightInfo.Name, rightInfo.Charset, nil
			}
		} else if isUnicodeCharset(leftInfo.Charset) != isUnicodeCharset(rightInfo.Charset) {
			if isUnicodeCharset(leftInfo.Charset) {
				return leftInfo.Name, leftInfo.Charset, nil
			}
			return rightInfo.Name, rightInfo.Charset, nil
		}
	}
	return "", "", common.NewErr(common.ErrCantAggregate2collations,
		left, "IMPLICIT", right, "IMPLICIT", operation)
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

func isNullLiteralExpression(expression sqlparser.Expr) bool {
	switch node := expression.(type) {
	case *sqlparser.NullVal:
		return true
	case *sqlparser.ParenExpr:
		return isNullLiteralExpression(node.Expr)
	default:
		return false
	}
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
