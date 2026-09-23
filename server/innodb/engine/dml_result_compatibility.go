package engine

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func rowMapsEqual(left, right map[string]interface{}) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftValue := range left {
		rightValue, ok := right[key]
		if !ok || !dmlValuesEqual(leftValue, rightValue) {
			return false
		}
	}
	return true
}

func dmlValuesEqual(left, right interface{}) bool {
	if reflect.DeepEqual(left, right) {
		return true
	}
	// The storage layer can materialize a numeric literal as a string while
	// the row reader returns an integer. SQL equality treats those values as
	// equal for the purpose of changed-row accounting.
	return left != nil && right != nil && fmt.Sprint(left) == fmt.Sprint(right)
}

func (dml *StorageIntegratedDMLExecutor) applyStringLengthRules(rows []*InsertRowData, tableMeta *metadata.TableMeta) error {
	if tableMeta == nil {
		return nil
	}
	columns := make(map[string]*metadata.ColumnMeta, len(tableMeta.Columns))
	for _, column := range tableMeta.Columns {
		if column != nil {
			columns[column.Name] = column
		}
	}
	for _, row := range rows {
		for name, raw := range row.ColumnValues {
			column := columns[name]
			if column == nil || column.Length <= 0 || raw == nil || !isStringDataType(column.Type) {
				continue
			}
			text, ok := raw.(string)
			if !ok || len([]rune(text)) <= column.Length {
				continue
			}
			if !dml.ignoreMode && dml.strictMode {
				return fmt.Errorf("Data too long for column '%s'", name)
			}
			row.ColumnValues[name] = string([]rune(text)[:column.Length])
			dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1265, Message: fmt.Sprintf("Data truncated for column '%s'", name)})
		}
	}
	return nil
}

func (dml *StorageIntegratedDMLExecutor) applyColumnTypeRules(rows []*InsertRowData, tableMeta *metadata.TableMeta) error {
	if tableMeta == nil {
		return nil
	}
	columns := make(map[string]*metadata.ColumnMeta, len(tableMeta.Columns))
	for _, column := range tableMeta.Columns {
		if column != nil {
			columns[column.Name] = column
		}
	}
	for rowIndex, row := range rows {
		if row == nil {
			continue
		}
		for name, raw := range row.ColumnValues {
			column := columns[name]
			if column == nil || raw == nil {
				continue
			}
			switch metadata.DataType(strings.ToUpper(string(column.Type))) {
			case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt, metadata.TypeBigInt, metadata.TypeBit:
				parsed, ok, overflow := boundedIntegerColumnValue(raw, metadata.DataType(strings.ToUpper(string(column.Type))), column.IsUnsigned)
				if ok {
					row.ColumnValues[name] = parsed
					continue
				}
				text := fmt.Sprint(raw)
				if overflow {
					if !dml.ignoreMode && dml.strictMode {
						return fmt.Errorf("Out of range value: '%s' for column '%s'", text, name)
					}
					row.ColumnValues[name] = integerColumnRangeLimit(metadata.DataType(strings.ToUpper(string(column.Type))), column.IsUnsigned, raw)
					dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1264, Message: fmt.Sprintf("Out of range value for column '%s' at row %d", name, rowIndex+1)})
					continue
				}
				if !dml.ignoreMode && dml.strictMode {
					return fmt.Errorf("Incorrect integer value: '%s' for column '%s'", text, name)
				}
				row.ColumnValues[name] = int64(0)
				dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1366, Message: fmt.Sprintf("Incorrect integer value: '%s' for column '%s' at row %d", text, name, rowIndex+1)})
			case metadata.TypeFloat, metadata.TypeDouble, metadata.TypeDecimal:
				parsed, ok, overflow, rounded := boundedDecimalColumnValue(raw, column)
				if ok {
					row.ColumnValues[name] = parsed
					if rounded {
						dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1265, Message: fmt.Sprintf("Data truncated for column '%s' at row %d", name, rowIndex+1)})
					}
					continue
				}
				text := fmt.Sprint(raw)
				if overflow {
					if !dml.ignoreMode && dml.strictMode {
						return fmt.Errorf("Out of range value: '%s' for column '%s'", text, name)
					}
					row.ColumnValues[name] = decimalColumnRangeLimit(column)
					dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1264, Message: fmt.Sprintf("Out of range value for column '%s' at row %d", name, rowIndex+1)})
					continue
				}
				if !dml.ignoreMode && dml.strictMode {
					return fmt.Errorf("Incorrect decimal value: '%s' for column '%s'", text, name)
				}
				row.ColumnValues[name] = float64(0)
				dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1366, Message: fmt.Sprintf("Incorrect decimal value: '%s' for column '%s' at row %d", text, name, rowIndex+1)})
			case metadata.TypeDate, metadata.TypeTime, metadata.TypeDateTime, metadata.TypeTimestamp, metadata.TypeYear:
				dataType := metadata.DataType(strings.ToUpper(string(column.Type)))
				if parsed, ok := dml.zeroTemporalColumnValue(raw, dataType); ok {
					row.ColumnValues[name] = parsed
					continue
				}
				if parsed, ok := temporalColumnValue(raw, dataType); ok {
					row.ColumnValues[name] = parsed
					continue
				}
				text := fmt.Sprint(raw)
				if !dml.ignoreMode && dml.strictMode {
					return fmt.Errorf("Incorrect date value: '%s' for column '%s'", text, name)
				}
				row.ColumnValues[name] = zeroTemporalValue(metadata.DataType(strings.ToUpper(string(column.Type))))
				dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1292, Message: fmt.Sprintf("Incorrect date value: '%s' for column '%s' at row %d", text, name, rowIndex+1)})
			case metadata.TypeEnum, metadata.TypeSet:
				if parsed, ok := enumSetColumnValue(raw, metadata.DataType(strings.ToUpper(string(column.Type))), column.EnumValues); ok {
					row.ColumnValues[name] = parsed
					continue
				}
				text := fmt.Sprint(raw)
				if !dml.ignoreMode && dml.strictMode {
					return fmt.Errorf("Invalid %s value: '%s' for column '%s'", strings.ToUpper(string(column.Type)), text, name)
				}
				if metadata.DataType(strings.ToUpper(string(column.Type))) == metadata.TypeSet {
					row.ColumnValues[name] = enumSetIgnoreValue(raw, column.EnumValues)
				} else {
					row.ColumnValues[name] = ""
				}
				dml.warnings = append(dml.warnings, Warning{Level: "Warning", Code: 1265, Message: fmt.Sprintf("Data truncated for column '%s' at row %d", name, rowIndex+1)})
			}
		}
	}
	return nil
}

func (dml *StorageIntegratedDMLExecutor) zeroTemporalColumnValue(value interface{}, dataType metadata.DataType) (string, bool) {
	if dataType == metadata.TypeYear || dataType == metadata.TypeTime {
		return "", false
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" {
		return "", false
	}
	zeroDate := regexp.MustCompile(`^0{4}-0{2}-0{2}(?:[ T]0{2}:0{2}:0{2})?$`).MatchString(text)
	partialDate := regexp.MustCompile(`^(?:\d{4}-00-\d{2}|\d{4}-\d{2}-00)(?:[ T].*)?$`).MatchString(text)
	if zeroDate && !dml.noZeroDate {
		if dataType == metadata.TypeDate {
			return "0000-00-00", true
		}
		return "0000-00-00 00:00:00", true
	}
	if partialDate && !zeroDate && !dml.noZeroInDate {
		if dataType == metadata.TypeDate {
			return "0000-00-00", true
		}
		return "0000-00-00 00:00:00", true
	}
	return "", false
}

func enumSetIgnoreValue(value interface{}, allowed []string) string {
	selected := make(map[string]struct{})
	for _, part := range strings.Split(strings.TrimSpace(fmt.Sprint(value)), ",") {
		part = strings.TrimSpace(part)
		for _, candidate := range allowed {
			if part == candidate {
				selected[candidate] = struct{}{}
				break
			}
		}
	}
	ordered := make([]string, 0, len(selected))
	for _, candidate := range allowed {
		if _, ok := selected[candidate]; ok {
			ordered = append(ordered, candidate)
		}
	}
	return strings.Join(ordered, ",")
}

func enumSetColumnValue(value interface{}, dataType metadata.DataType, allowed []string) (string, bool) {
	if len(allowed) == 0 {
		return fmt.Sprint(value), true
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if dataType == metadata.TypeEnum {
		if index, ok := integerColumnValue(value); ok && index >= 1 && index <= int64(len(allowed)) {
			return allowed[index-1], true
		}
		for _, candidate := range allowed {
			if text == candidate {
				return candidate, true
			}
		}
		return "", false
	}
	if text == "" {
		return "", true
	}
	selected := make(map[string]struct{})
	for _, part := range strings.Split(text, ",") {
		part = strings.TrimSpace(part)
		found := false
		for _, candidate := range allowed {
			if part == candidate {
				selected[candidate] = struct{}{}
				found = true
				break
			}
		}
		if !found {
			return "", false
		}
	}
	ordered := make([]string, 0, len(selected))
	for _, candidate := range allowed {
		if _, ok := selected[candidate]; ok {
			ordered = append(ordered, candidate)
		}
	}
	return strings.Join(ordered, ","), true
}

func integerColumnValue(value interface{}) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int8:
		return int64(typed), true
	case int16:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	case uint:
		return int64(typed), true
	case uint8:
		return int64(typed), true
	case uint16:
		return int64(typed), true
	case uint32:
		return int64(typed), true
	case uint64:
		return int64(typed), typed <= uint64(^uint64(0)>>1)
	case float32:
		return int64(typed), float32(int64(typed)) == typed
	case float64:
		return int64(typed), float64(int64(typed)) == typed
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func integerColumnValueOrZero(value interface{}) int64 {
	parsed, ok := integerColumnValue(value)
	if !ok {
		return 0
	}
	return parsed
}

func boundedIntegerColumnValue(value interface{}, dataType metadata.DataType, unsigned bool) (interface{}, bool, bool) {
	maxUnsigned := integerColumnMax(dataType, true)
	if unsigned {
		parsed, ok := uint64ColumnValue(value)
		if !ok {
			return nil, false, false
		}
		if parsed > maxUnsigned {
			return nil, false, true
		}
		return parsed, true, false
	}
	parsed, ok := integerColumnValue(value)
	if !ok {
		if unsignedValue, unsignedOK := uint64ColumnValue(value); unsignedOK && unsignedValue > uint64(math.MaxInt64) {
			return nil, false, true
		}
		return nil, false, false
	}
	min, max := integerColumnSignedBounds(dataType)
	if parsed < min || parsed > max {
		return nil, false, true
	}
	return parsed, true, false
}

func integerColumnSignedBounds(dataType metadata.DataType) (int64, int64) {
	switch dataType {
	case metadata.TypeTinyInt:
		return -128, 127
	case metadata.TypeSmallInt:
		return -32768, 32767
	case metadata.TypeMediumInt:
		return -8388608, 8388607
	case metadata.TypeInt:
		return -2147483648, 2147483647
	default:
		return math.MinInt64, math.MaxInt64
	}
}

func integerColumnMax(dataType metadata.DataType, unsigned bool) uint64 {
	if !unsigned {
		_, max := integerColumnSignedBounds(dataType)
		return uint64(max)
	}
	switch dataType {
	case metadata.TypeTinyInt:
		return 1<<8 - 1
	case metadata.TypeSmallInt:
		return 1<<16 - 1
	case metadata.TypeMediumInt:
		return 1<<24 - 1
	case metadata.TypeInt:
		return 1<<32 - 1
	default:
		return math.MaxUint64
	}
}

func uint64ColumnValue(value interface{}) (uint64, bool) {
	switch typed := value.(type) {
	case uint:
		return uint64(typed), true
	case uint8:
		return uint64(typed), true
	case uint16:
		return uint64(typed), true
	case uint32:
		return uint64(typed), true
	case uint64:
		return typed, true
	case int:
		if typed < 0 {
			return 0, false
		}
		return uint64(typed), true
	case int8:
		if typed < 0 {
			return 0, false
		}
		return uint64(typed), true
	case int16:
		if typed < 0 {
			return 0, false
		}
		return uint64(typed), true
	case int32:
		if typed < 0 {
			return 0, false
		}
		return uint64(typed), true
	case int64:
		if typed < 0 {
			return 0, false
		}
		return uint64(typed), true
	case float32:
		if typed < 0 || float32(uint64(typed)) != typed {
			return 0, false
		}
		return uint64(typed), true
	case float64:
		if typed < 0 || float64(uint64(typed)) != typed {
			return 0, false
		}
		return uint64(typed), true
	case string:
		parsed, err := strconv.ParseUint(strings.TrimSpace(typed), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func integerColumnRangeLimit(dataType metadata.DataType, unsigned bool, raw interface{}) interface{} {
	if unsigned {
		return integerColumnMax(dataType, true)
	}
	min, max := integerColumnSignedBounds(dataType)
	if value, ok := integerColumnValue(raw); ok && value < 0 {
		return min
	}
	if text := strings.TrimSpace(fmt.Sprint(raw)); strings.HasPrefix(text, "-") {
		return min
	}
	return max
}

func decimalColumnValue(value interface{}) (float64, bool) {
	switch typed := value.(type) {
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func boundedDecimalColumnValue(value interface{}, column *metadata.ColumnMeta) (float64, bool, bool, bool) {
	parsed, ok := decimalColumnValue(value)
	if !ok {
		return 0, false, false, false
	}
	if column == nil || !strings.EqualFold(string(column.Type), string(metadata.TypeDecimal)) || column.Length <= 0 {
		return parsed, true, false, false
	}
	scale := column.Scale
	if scale < 0 {
		scale = 0
	}
	factor := math.Pow10(scale)
	roundedValue := math.Round(parsed*factor) / factor
	rounded := roundedValue != parsed
	integerDigits := column.Length - scale
	if integerDigits < 1 {
		integerDigits = 1
	}
	max := math.Pow10(integerDigits) - math.Pow10(-scale)
	if math.Abs(roundedValue) > max {
		return 0, false, true, rounded
	}
	return roundedValue, true, false, rounded
}

func decimalColumnRangeLimit(column *metadata.ColumnMeta) float64 {
	if column == nil || column.Length <= 0 {
		return 0
	}
	scale := column.Scale
	if scale < 0 {
		scale = 0
	}
	integerDigits := column.Length - scale
	if integerDigits < 1 {
		integerDigits = 1
	}
	return math.Pow10(integerDigits) - math.Pow10(-scale)
}

func temporalColumnValue(value interface{}, dataType metadata.DataType) (string, bool) {
	switch typed := value.(type) {
	case time.Time:
		return formatTemporalValue(typed, dataType), true
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		if dataType != metadata.TypeYear {
			break
		}
		year, ok := integerColumnValue(typed)
		if ok && year >= 0 && year <= 9999 {
			return fmt.Sprintf("%04d", year), true
		}
	case string:
		text := strings.TrimSpace(typed)
		layouts := []string{}
		switch dataType {
		case metadata.TypeDate:
			layouts = []string{"2006-01-02"}
		case metadata.TypeTime:
			layouts = []string{"15:04:05"}
		case metadata.TypeYear:
			layouts = []string{"2006"}
		default:
			layouts = []string{"2006-01-02 15:04:05", time.RFC3339}
		}
		for _, layout := range layouts {
			if parsed, err := time.Parse(layout, text); err == nil {
				return formatTemporalValue(parsed, dataType), true
			}
		}
	}
	return "", false
}

func formatTemporalValue(value time.Time, dataType metadata.DataType) string {
	switch dataType {
	case metadata.TypeDate:
		return value.Format("2006-01-02")
	case metadata.TypeTime:
		return value.Format("15:04:05")
	case metadata.TypeYear:
		return value.Format("2006")
	default:
		return value.Format("2006-01-02 15:04:05")
	}
}

func zeroTemporalValue(dataType metadata.DataType) string {
	switch dataType {
	case metadata.TypeDate:
		return "0000-00-00"
	case metadata.TypeTime:
		return "00:00:00"
	case metadata.TypeYear:
		return "0000"
	default:
		return "0000-00-00 00:00:00"
	}
}

func isStringDataType(dataType metadata.DataType) bool {
	upper := strings.ToUpper(string(dataType))
	return strings.Contains(upper, "CHAR") || strings.Contains(upper, "TEXT") || strings.Contains(upper, "BLOB")
}
