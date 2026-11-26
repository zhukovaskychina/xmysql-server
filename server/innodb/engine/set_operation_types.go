package engine

import (
	"fmt"
	"strings"

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
