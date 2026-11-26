package engine

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

const (
	maxVectorSearchFilterJSONBytes = 16 << 10
	maxVectorSearchFilterColumns   = 32
)

type vectorSearchFilter struct {
	column   *metadata.ColumnMeta
	expected interface{}
	isNull   bool
}

func parseVectorSearchFilters(encoded string, table *metadata.TableMeta) ([]vectorSearchFilter, error) {
	if len(encoded) == 0 || len(encoded) > maxVectorSearchFilterJSONBytes {
		return nil, fmt.Errorf("VECTOR_SEARCH filter JSON must contain 1..%d bytes", maxVectorSearchFilterJSONBytes)
	}
	decoder := json.NewDecoder(strings.NewReader(encoded))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("decode VECTOR_SEARCH filter JSON: %w", err)
	}
	opening, ok := token.(json.Delim)
	if !ok || opening != '{' {
		return nil, fmt.Errorf("VECTOR_SEARCH filters must be a JSON object")
	}
	columns := make(map[string]*metadata.ColumnMeta)
	if table != nil {
		for _, column := range table.Columns {
			if column != nil {
				columns[strings.ToLower(strings.TrimSpace(column.Name))] = column
			}
		}
	}
	seen := make(map[string]struct{})
	filters := make([]vectorSearchFilter, 0)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("read VECTOR_SEARCH filter name: %w", err)
		}
		name, ok := keyToken.(string)
		if !ok || strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("VECTOR_SEARCH filter column names must be non-empty strings")
		}
		key := strings.ToLower(strings.TrimSpace(name))
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("VECTOR_SEARCH filter contains duplicate column %q", name)
		}
		seen[key] = struct{}{}
		if len(seen) > maxVectorSearchFilterColumns {
			return nil, fmt.Errorf("VECTOR_SEARCH accepts at most %d filter columns", maxVectorSearchFilterColumns)
		}
		column := columns[key]
		if column == nil {
			return nil, fmt.Errorf("VECTOR_SEARCH filter column %q does not exist", name)
		}
		if !vectorSearchFilterableColumn(column) {
			return nil, fmt.Errorf("VECTOR_SEARCH filter column %q is not a supported scalar type", column.Name)
		}
		var expected interface{}
		if err := decoder.Decode(&expected); err != nil {
			return nil, fmt.Errorf("decode VECTOR_SEARCH filter value for %q: %w", name, err)
		}
		filter := vectorSearchFilter{column: column, expected: expected, isNull: expected == nil}
		switch value := expected.(type) {
		case nil:
		case string:
			if !vectorSearchTextColumn(column) {
				return nil, fmt.Errorf("VECTOR_SEARCH filter value for %q must match its SQL column type", name)
			}
		case bool:
			if !isNumericSQLType(column.Type) {
				return nil, fmt.Errorf("VECTOR_SEARCH boolean filter for %q requires a numeric or boolean SQL column", name)
			}
		case json.Number:
			if !isNumericSQLType(column.Type) {
				return nil, fmt.Errorf("VECTOR_SEARCH numeric filter for %q requires a numeric or boolean SQL column", name)
			}
			numberText := value.String()
			if len(numberText) > 128 {
				return nil, fmt.Errorf("VECTOR_SEARCH filter number for %q exceeds 128 characters", name)
			}
			number, err := strconv.ParseFloat(numberText, 64)
			if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
				return nil, fmt.Errorf("VECTOR_SEARCH filter number for %q must be finite", name)
			}
			if _, ok := new(big.Rat).SetString(numberText); !ok {
				return nil, fmt.Errorf("VECTOR_SEARCH filter number for %q is invalid", name)
			}
		default:
			return nil, fmt.Errorf("VECTOR_SEARCH filter value for %q must be a JSON scalar", name)
		}
		filters = append(filters, filter)
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("close VECTOR_SEARCH filter JSON object: %w", err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return nil, fmt.Errorf("VECTOR_SEARCH filter JSON object is not closed")
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("VECTOR_SEARCH filter JSON contains trailing values")
		}
		return nil, fmt.Errorf("check VECTOR_SEARCH filter JSON boundary: %w", err)
	}
	return filters, nil
}

func vectorSearchFilterableColumn(column *metadata.ColumnMeta) bool {
	if column == nil {
		return false
	}
	switch metadata.DataType(strings.ToUpper(strings.TrimSpace(string(column.Type)))) {
	case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt, metadata.TypeBigInt,
		metadata.TypeYear, metadata.TypeBit, metadata.TypeBool, metadata.TypeBoolean, metadata.TypeDecimal, metadata.TypeFloat, metadata.TypeDouble,
		metadata.TypeChar, metadata.TypeVarchar, metadata.TypeTinyText, metadata.TypeText, metadata.TypeMediumText,
		metadata.TypeLongText, metadata.TypeEnum, metadata.TypeSet, metadata.TypeDate, metadata.TypeTime,
		metadata.TypeDateTime, metadata.TypeTimestamp:
		return true
	default:
		return false
	}
}

func vectorSearchFiltersMatch(row *InsertRowData, filters []vectorSearchFilter) (bool, error) {
	if row == nil {
		return false, fmt.Errorf("VECTOR_SEARCH filter received a missing source row")
	}
	for _, filter := range filters {
		actual, exists := clusteredRowColumnValue(row, filter.column.Name)
		if !exists {
			return false, fmt.Errorf("VECTOR_SEARCH source row is missing filter column %q", filter.column.Name)
		}
		if filter.isNull {
			if actual != nil {
				return false, nil
			}
			continue
		}
		if actual == nil {
			return false, nil
		}
		expected := filter.expected
		if expectedBool, ok := expected.(bool); ok {
			if expectedBool {
				expected = json.Number("1")
			} else {
				expected = json.Number("0")
			}
		}
		if isNumericSQLType(filter.column.Type) {
			comparison, err := compareVectorSearchNumeric(actual, expected)
			if err != nil {
				return false, fmt.Errorf("compare VECTOR_SEARCH filter column %q: %w", filter.column.Name, err)
			}
			if comparison != 0 {
				return false, nil
			}
			continue
		}
		if actualBytes, ok := actual.([]byte); ok && !isNumericSQLType(filter.column.Type) {
			actual = string(actualBytes)
		}
		if compareScalarValuesWithCollation(actual, expected, filter.column.Collation) != 0 {
			return false, nil
		}
	}
	return true, nil
}

func vectorSearchTextColumn(column *metadata.ColumnMeta) bool {
	if column == nil {
		return false
	}
	switch metadata.DataType(strings.ToUpper(strings.TrimSpace(string(column.Type)))) {
	case metadata.TypeChar, metadata.TypeVarchar, metadata.TypeTinyText, metadata.TypeText, metadata.TypeMediumText,
		metadata.TypeLongText, metadata.TypeEnum, metadata.TypeSet, metadata.TypeDate, metadata.TypeTime,
		metadata.TypeDateTime, metadata.TypeTimestamp:
		return true
	default:
		return false
	}
}

func compareVectorSearchNumeric(actual, expected interface{}) (int, error) {
	left, err := vectorSearchRat(actual)
	if err != nil {
		return 0, err
	}
	right, err := vectorSearchRat(expected)
	if err != nil {
		return 0, err
	}
	return left.Cmp(right), nil
}

func vectorSearchRat(value interface{}) (*big.Rat, error) {
	var text string
	switch typed := value.(type) {
	case json.Number:
		text = typed.String()
	case string:
		text = strings.TrimSpace(typed)
	case []byte:
		text = strings.TrimSpace(string(typed))
	case bool:
		if typed {
			text = "1"
		} else {
			text = "0"
		}
	case float32:
		text = strconv.FormatFloat(float64(typed), 'g', -1, 32)
	case float64:
		text = strconv.FormatFloat(typed, 'g', -1, 64)
	default:
		text = fmt.Sprint(typed)
	}
	if len(text) > 128 {
		return nil, fmt.Errorf("numeric value exceeds 128 characters")
	}
	rational, ok := new(big.Rat).SetString(text)
	if !ok {
		return nil, fmt.Errorf("%q is not a supported finite number", text)
	}
	return rational, nil
}

func isNumericSQLType(dataType metadata.DataType) bool {
	switch dataType {
	case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt, metadata.TypeBigInt,
		metadata.TypeYear, metadata.TypeBit, metadata.TypeBool, metadata.TypeBoolean, metadata.TypeDecimal, metadata.TypeFloat, metadata.TypeDouble:
		return true
	default:
		return false
	}
}

func vectorSearchFilterColumnNames(filters []vectorSearchFilter) []string {
	values := make([]string, 0, len(filters))
	for _, filter := range filters {
		values = append(values, strings.ToLower(filter.column.Name))
	}
	sort.Strings(values)
	return values
}
