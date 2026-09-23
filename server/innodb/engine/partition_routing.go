package engine

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// validatePartitionRows enforces the logical partition contract before rows
// enter a physical partition clustered index. Every row is checked against
// exactly one durable partition descriptor, so out-of-range and ambiguous
// rows fail atomically before the fan-out storage path is used.
func validatePartitionRows(dataDir, schemaName, tableName string, rows []*InsertRowData) error {
	if dataDir == "" || schemaName == "" || tableName == "" || len(rows) == 0 {
		return nil
	}
	tableInfo, err := readTableMetadataMap(filepath.Join(dataDir, schemaName, tableName+".frm"))
	if err != nil {
		return nil
	}
	descriptor, ok := tableInfo["partitioning"].(map[string]interface{})
	if !ok {
		return nil
	}
	method, _ := descriptor["method"].(string)
	expression, _ := descriptor["expression"].(string)
	expression = strings.Trim(strings.TrimSpace(expression), "`")
	if expression == "" {
		return fmt.Errorf("partition expression is empty")
	}
	definitions, _ := descriptor["partitions"].([]interface{})
	if len(definitions) == 0 {
		return fmt.Errorf("partition definition is empty")
	}
	for _, row := range rows {
		values, err := partitionExpressionValues(row.ColumnValues, expression)
		if err != nil {
			return err
		}
		if _, err := partitionForValues(strings.ToUpper(method), values, definitions); err != nil {
			return fmt.Errorf("row does not map to a partition for %s.%s: %w", schemaName, tableName, err)
		}
	}
	return nil
}

func partitionExpressionValues(row map[string]interface{}, expression string) ([]interface{}, error) {
	columns := partitionExpressionColumns(expression)
	values := make([]interface{}, 0, len(columns))
	for _, column := range columns {
		value, exists := partitionExpressionValue(row, column)
		if !exists || value == nil {
			return nil, fmt.Errorf("row does not map to a partition: expression %s is NULL", expression)
		}
		values = append(values, value)
	}
	return values, nil
}

func partitionExpressionValuesFromRecord(columnNames []string, values []basic.Value, expression string) ([]interface{}, error) {
	row := make(map[string]interface{}, len(columnNames))
	for index, name := range columnNames {
		if index >= len(values) || values[index].IsNull() {
			row[name] = nil
			continue
		}
		row[name] = values[index].Raw()
	}
	return partitionExpressionValues(row, expression)
}

func partitionExpressionValuesFromRow(columnNames []string, values []interface{}, expression string) ([]interface{}, error) {
	row := make(map[string]interface{}, len(columnNames))
	for index, name := range columnNames {
		if index >= len(values) {
			row[name] = nil
			continue
		}
		row[name] = values[index]
	}
	return partitionExpressionValues(row, expression)
}

func partitionExpressionValue(row map[string]interface{}, expression string) (interface{}, bool) {
	identifier := strings.Trim(strings.TrimSpace(expression), "` ")
	if function, argument, ok := partitionDateFunction(identifier); ok {
		value, exists := partitionRowValue(row, argument)
		if !exists || value == nil {
			return nil, false
		}
		return partitionDateFunctionValue(function, value)
	}
	if value, ok := partitionCastValue(row, identifier); ok {
		return value, true
	}
	if value, ok := partitionArithmeticValue(row, identifier); ok {
		return value, true
	}
	return partitionRowValue(row, identifier)
}

func partitionCastValue(row map[string]interface{}, expression string) (interface{}, bool) {
	match := regexp.MustCompile("(?is)^\\s*cast\\s*\\(\\s*([^()]+?)\\s+as\\s+(signed|unsigned)\\s*\\)\\s*$").FindStringSubmatch(expression)
	if len(match) != 3 {
		return nil, false
	}
	value, exists := partitionRowValue(row, strings.Trim(strings.TrimSpace(match[1]), "` "))
	if !exists || value == nil {
		return nil, false
	}
	numeric, err := partitionNumericValue(value)
	if err != nil {
		return nil, false
	}
	if strings.EqualFold(match[2], "unsigned") {
		if numeric < 0 {
			return nil, false
		}
		return uint64(numeric), true
	}
	return numeric, true
}

func partitionArithmeticValue(row map[string]interface{}, expression string) (interface{}, bool) {
	match := regexp.MustCompile("(?is)^\\s*([a-zA-Z0-9_$`.]+)\\s*(div|[+*/%-])\\s*(-?[0-9]+)\\s*$").FindStringSubmatch(expression)
	if len(match) != 4 {
		return nil, false
	}
	base, exists := partitionRowValue(row, strings.Trim(strings.TrimSpace(match[1]), "` "))
	if !exists || base == nil {
		return nil, false
	}
	left, err := partitionNumericValue(base)
	if err != nil {
		return nil, false
	}
	right, err := strconv.ParseInt(match[3], 10, 64)
	if err != nil {
		return nil, false
	}
	switch strings.ToLower(match[2]) {
	case "+":
		return left + right, true
	case "-":
		return left - right, true
	case "*":
		return left * right, true
	case "/", "div":
		if right == 0 {
			return nil, false
		}
		return left / right, true
	case "%":
		if right == 0 {
			return nil, false
		}
		return left % right, true
	default:
		return nil, false
	}
}

func partitionDateFunction(expression string) (string, string, bool) {
	match := regexp.MustCompile(`(?is)^\s*(year|month|day|dayofmonth|to_days)\s*\(\s*([^()]+?)\s*\)\s*$`).FindStringSubmatch(expression)
	if len(match) != 3 {
		return "", "", false
	}
	return strings.ToUpper(match[1]), strings.Trim(strings.TrimSpace(match[2]), "` "), true
}

func partitionDateFunctionValue(function string, value interface{}) (interface{}, bool) {
	parsed, ok := partitionTemporalTime(value)
	if !ok {
		return nil, false
	}
	switch function {
	case "YEAR":
		return int64(parsed.Year()), true
	case "MONTH":
		return int64(parsed.Month()), true
	case "DAY", "DAYOFMONTH":
		return int64(parsed.Day()), true
	case "TO_DAYS":
		return partitionMySQLDayNumber(parsed), true
	default:
		return nil, false
	}
}

func partitionMySQLDayNumber(value time.Time) int64 {
	year := int64(value.Year())
	month := int64(value.Month())
	day := int64(value.Day())
	monthDays := [...]int64{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334}
	result := 365*year + (year+3)/4 - (year+99)/100 + (year+399)/400 + monthDays[month-1] + day
	if month > 2 && (year%4 == 0 && (year%100 != 0 || year%400 == 0)) {
		result++
	}
	return result
}

func partitionTemporalTime(value interface{}) (time.Time, bool) {
	switch typed := value.(type) {
	case time.Time:
		return typed, true
	case []byte:
		return partitionTemporalTime(string(typed))
	case string:
		text := strings.TrimSpace(typed)
		for _, layout := range []string{"2006-01-02", "2006-01-02 15:04:05", time.RFC3339} {
			if parsed, err := time.Parse(layout, text); err == nil {
				return parsed, true
			}
		}
	}
	return time.Time{}, false
}

func partitionExpressionColumns(expression string) []string {
	parts := splitTopLevelComma(expression)
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		column := strings.Trim(strings.TrimSpace(part), "` ")
		if column != "" {
			columns = append(columns, column)
		}
	}
	return columns
}

func partitionForValues(method string, values []interface{}, definitions []interface{}) (int, error) {
	if len(values) == 1 {
		return partitionForValue(method, values[0], definitions)
	}
	if len(values) == 0 || (method != "RANGE" && method != "LIST") {
		return -1, fmt.Errorf("multi-column partitioning requires RANGE or LIST COLUMNS")
	}
	for index, raw := range definitions {
		definition, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		definitionValues, _ := definition["values"].(string)
		if strings.Contains(strings.ToLower(definitionValues), "maxvalue") {
			return index, nil
		}
		if method == "LIST" {
			for _, candidate := range partitionListTupleBounds(definitionValues) {
				if len(candidate) != len(values) {
					continue
				}
				comparison, err := comparePartitionTuples(values, candidate)
				if err != nil {
					return -1, err
				}
				if comparison == 0 {
					return index, nil
				}
			}
			continue
		}
		bound, ok := partitionTupleBound(definitionValues)
		if !ok || len(bound) != len(values) {
			return -1, fmt.Errorf("partition definition %d has an incompatible tuple bound", index+1)
		}
		comparison, err := comparePartitionTuples(values, bound)
		if err != nil {
			return -1, err
		}
		if comparison < 0 {
			return index, nil
		}
	}
	return -1, fmt.Errorf("value %v is outside all RANGE COLUMNS partitions", values)
}

func partitionListTupleBounds(values string) [][]interface{} {
	lower := strings.ToLower(values)
	at := strings.Index(lower, "in")
	if at < 0 {
		return nil
	}
	open := strings.Index(values[at+2:], "(")
	if open < 0 {
		return nil
	}
	open += at + 2
	close := strings.LastIndex(values, ")")
	if close <= open {
		return nil
	}
	result := make([][]interface{}, 0)
	for _, raw := range splitTopLevelComma(values[open+1 : close]) {
		raw = strings.TrimSpace(raw)
		if !strings.HasPrefix(raw, "(") || !strings.HasSuffix(raw, ")") {
			continue
		}
		if tuple, ok := partitionTupleBound(raw); ok {
			result = append(result, tuple)
		}
	}
	return result
}

func partitionTupleBound(values string) ([]interface{}, bool) {
	open := strings.Index(values, "(")
	close := strings.LastIndex(values, ")")
	if open < 0 || close <= open {
		return nil, false
	}
	parts := splitTopLevelComma(values[open+1 : close])
	result := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		value, ok := partitionPredicateLiteral(strings.TrimSpace(part))
		if !ok {
			return nil, false
		}
		result = append(result, value)
	}
	return result, len(result) > 0
}

func comparePartitionTuples(left, right []interface{}) (int, error) {
	for index := range left {
		leftNumber, leftErr := partitionNumericValue(left[index])
		rightNumber, rightErr := partitionNumericValue(right[index])
		if leftErr == nil && rightErr == nil {
			if leftNumber < rightNumber {
				return -1, nil
			}
			if leftNumber > rightNumber {
				return 1, nil
			}
			continue
		}
		leftText, leftTextOK := partitionTextValue(left[index])
		rightText, rightTextOK := partitionTextValue(right[index])
		if !leftTextOK || !rightTextOK {
			return 0, fmt.Errorf("partition tuple values have incompatible types")
		}
		if leftText < rightText {
			return -1, nil
		}
		if leftText > rightText {
			return 1, nil
		}
	}
	return 0, nil
}

func partitionForValue(method string, value interface{}, definitions []interface{}) (int, error) {
	switch method {
	case "RANGE":
		numeric, numericErr := partitionNumericValue(value)
		textValue, textOK := partitionTextValue(value)
		if numericErr != nil && !textOK {
			return -1, numericErr
		}
		for index, raw := range definitions {
			definition, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			values, _ := definition["values"].(string)
			if strings.Contains(strings.ToLower(values), "maxvalue") {
				return index, nil
			}
			if bound, ok := firstPartitionNumber(values); ok {
				if numericErr == nil && numeric < bound {
					return index, nil
				}
				continue
			}
			if bound, ok := firstPartitionText(values); ok && textOK && textValue < bound {
				return index, nil
			}
		}
		return -1, fmt.Errorf("value %v is outside all RANGE partitions", value)
	case "LIST":
		candidate := strings.TrimSpace(fmt.Sprint(value))
		for index, raw := range definitions {
			definition, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			values, _ := definition["values"].(string)
			listText := strings.Trim(strings.TrimSpace(values[strings.Index(strings.ToLower(values), "in")+2:]), "()")
			for _, item := range splitTopLevelComma(listText) {
				item = strings.TrimSpace(item)
				if normalized, ok := partitionListTextLiteral(item); ok {
					item = normalized
				}
				if item == candidate {
					return index, nil
				}
			}
		}
		return -1, fmt.Errorf("value %v is outside all LIST partitions", value)
	case "HASH", "KEY":
		numeric, err := partitionNumericValue(value)
		if err != nil {
			return -1, err
		}
		return int(((numeric % int64(len(definitions))) + int64(len(definitions))) % int64(len(definitions))), nil
	default:
		return -1, fmt.Errorf("unsupported partition method %q", method)
	}
}

func partitionTextValue(value interface{}) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}

func partitionNumericValue(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int8:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	case uint:
		return int64(typed), nil
	case uint8:
		return int64(typed), nil
	case uint16:
		return int64(typed), nil
	case uint32:
		return int64(typed), nil
	case uint64:
		if typed > uint64(^uint64(0)>>1) {
			return 0, fmt.Errorf("partition value overflows signed range")
		}
		return int64(typed), nil
	case float32:
		return int64(typed), nil
	case float64:
		return int64(typed), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
	case []byte:
		return strconv.ParseInt(strings.TrimSpace(string(typed)), 10, 64)
	default:
		return 0, fmt.Errorf("partition value %v is not numeric", value)
	}
}

func firstPartitionNumber(values string) (int64, bool) {
	values = strings.TrimSpace(values)
	open := strings.Index(values, "(")
	if open >= 0 {
		values = values[open+1:]
	}
	close := strings.Index(values, ")")
	if close >= 0 {
		values = values[:close]
	}
	value, err := strconv.ParseInt(strings.TrimSpace(values), 10, 64)
	return value, err == nil
}

func firstPartitionText(values string) (string, bool) {
	values = strings.TrimSpace(values)
	open := strings.Index(values, "(")
	if open >= 0 {
		values = values[open+1:]
	}
	close := strings.LastIndex(values, ")")
	if close >= 0 {
		values = values[:close]
	}
	return partitionListTextLiteral(strings.TrimSpace(values))
}

func validatePartitionValueForMetadata(dataDir, schemaName, tableName string, row map[string]interface{}, _ *metadata.TableMeta) error {
	return validatePartitionRows(dataDir, schemaName, tableName, []*InsertRowData{{ColumnValues: row}})
}
