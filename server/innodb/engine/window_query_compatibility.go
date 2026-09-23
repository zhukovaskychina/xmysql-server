package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type windowRowState struct {
	index       int
	key         string
	value       basic.Value
	orderValues []basic.Value
}

func (e *XMySQLExecutor) executeGeneralWindowQuery(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	if handled, err := e.executeMultipleWindowExpressions(ctx, trimmed, databaseName); handled {
		return true, err
	}
	if handled, err := e.executeWindowOnlyProjection(ctx, trimmed, databaseName); handled {
		return true, err
	}
	// Resolve the common named-window form before parsing the regular
	// OVER(...) form.  Keeping the resolved specification in one execution
	// path ensures named windows get the same frame and aggregate semantics as
	// inline windows.
	namedWindowPattern := regexp.MustCompile(`(?is)^select\s+(.+?),\s*(row_number|rank|dense_rank|ntile|lag|lead|first_value|last_value|nth_value|cume_dist|percent_rank|sum|avg|min|max|count|std|stddev|stddev_pop|stddev_samp|var_pop|var_samp|variance|bit_and|bit_or|bit_xor|json_arrayagg|json_objectagg)\s*\((.*?)\)\s+over\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s+as\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s+from\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s+window\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s+as\s*\((.*)\)$`)
	if named := namedWindowPattern.FindStringSubmatch(trimmed); len(named) > 0 {
		if !strings.EqualFold(named[4], named[7]) {
			return true, fmt.Errorf("unknown window %s", named[4])
		}
		rewritten := fmt.Sprintf("select %s, %s(%s) over (%s) as %s from %s", named[1], named[2], named[3], named[8], named[5], named[6])
		return e.executeGeneralWindowQuery(ctx, rewritten, databaseName)
	}
	match := regexp.MustCompile(`(?is)^select\s+(.+?),\s*(row_number|rank|dense_rank|ntile|lag|lead|first_value|last_value|nth_value|cume_dist|percent_rank|sum|avg|min|max|count|std|stddev|stddev_pop|stddev_samp|var_pop|var_samp|variance|bit_and|bit_or|bit_xor|json_arrayagg|json_objectagg)\s*\((.*?)\)\s+over\s*\(`).FindStringSubmatchIndex(trimmed)
	if len(match) == 0 {
		return false, nil
	}
	baseProjection := strings.TrimSpace(trimmed[match[2]:match[3]])
	functionName := strings.ToLower(trimmed[match[4]:match[5]])
	functionArgs := strings.TrimSpace(trimmed[match[6]:match[7]])
	overOpen := match[1] - 1
	overClose := matchingParenIndex(trimmed, overOpen)
	if overClose < 0 {
		return true, fmt.Errorf("window specification has unbalanced parentheses")
	}
	afterOver := strings.TrimSpace(trimmed[overClose+1:])
	afterMatch := regexp.MustCompile(`(?is)^as\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s+from\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `(?:\s+order\s+by\s+.+)?$`).FindStringSubmatch(afterOver)
	if len(afterMatch) == 0 {
		return true, fmt.Errorf("window query must provide an alias and source table")
	}
	sourceTable := afterMatch[2]
	sourceStmt, err := sqlparser.Parse("select * from " + sourceTable)
	if err != nil {
		return true, err
	}
	sourceSelect, ok := sourceStmt.(*sqlparser.Select)
	if !ok {
		return true, fmt.Errorf("window source is not SELECT")
	}
	sourceResult, err := e.executeSelectStatement(ctx, sourceSelect, databaseName)
	if err != nil {
		return true, err
	}
	windowSpec := trimmed[overOpen+1 : overClose]
	partitionColumns, orderColumns, orderDirections, err := parseWindowSpecification(windowSpec)
	if err != nil {
		return true, err
	}
	descending := len(orderDirections) > 0 && orderDirections[0]
	frame, err := parseWindowFrame(windowSpec, functionName)
	if err != nil {
		return true, err
	}
	baseColumns := splitTopLevelComma(baseProjection)
	baseIndexes := make([]int, 0, len(baseColumns))
	for _, column := range baseColumns {
		index := findResultColumn(sourceResult.Columns, column)
		if index < 0 {
			return true, fmt.Errorf("window column %s does not exist", column)
		}
		baseIndexes = append(baseIndexes, index)
	}
	partitionIndexes := make([]int, 0, len(partitionColumns))
	for _, column := range partitionColumns {
		index := findResultColumn(sourceResult.Columns, column)
		if index < 0 {
			return true, fmt.Errorf("window partition column %s does not exist", column)
		}
		partitionIndexes = append(partitionIndexes, index)
	}
	orderIndexes := make([]int, 0, len(orderColumns))
	for _, column := range orderColumns {
		index := findResultColumn(sourceResult.Columns, column)
		if index < 0 {
			return true, fmt.Errorf("window order column %s does not exist", column)
		}
		orderIndexes = append(orderIndexes, index)
	}
	argumentIndex := -1
	functionValueIndex := -1
	argumentOffset := int64(1)
	if functionName == "lag" || functionName == "lead" || functionName == "first_value" || functionName == "last_value" || functionName == "nth_value" || functionName == "sum" || functionName == "avg" || functionName == "min" || functionName == "max" || functionName == "count" || functionName == "std" || functionName == "stddev" || functionName == "stddev_pop" || functionName == "stddev_samp" || functionName == "var_pop" || functionName == "var_samp" || functionName == "variance" || functionName == "bit_and" || functionName == "bit_or" || functionName == "bit_xor" || functionName == "json_arrayagg" || functionName == "json_objectagg" {
		args := splitTopLevelComma(functionArgs)
		if len(args) == 0 && functionName != "count" {
			return true, fmt.Errorf("%s requires an expression", functionName)
		}
		if functionName == "json_objectagg" && len(args) != 2 {
			return true, fmt.Errorf("json_objectagg requires a key and value expression")
		}
		if functionName == "json_objectagg" {
			argumentIndex = findResultColumn(sourceResult.Columns, args[0])
			functionValueIndex = findResultColumn(sourceResult.Columns, args[1])
		} else if functionName == "json_arrayagg" {
			if len(args) != 1 {
				return true, fmt.Errorf("json_arrayagg requires an expression")
			}
			argumentIndex = findResultColumn(sourceResult.Columns, args[0])
		}
		if functionName == "count" && (len(args) == 0 || strings.TrimSpace(args[0]) == "*") {
			argumentIndex = -2
		} else if len(args) > 0 {
			argumentIndex = findResultColumn(sourceResult.Columns, args[0])
		}
		if argumentIndex < 0 && argumentIndex != -2 {
			return true, fmt.Errorf("window argument %s does not exist", args[0])
		}
		if (functionName == "lag" || functionName == "lead") && len(args) > 1 {
			argumentOffset, err = strconv.ParseInt(strings.TrimSpace(args[1]), 10, 64)
			if err != nil || argumentOffset < 0 {
				return true, fmt.Errorf("window offset must be a non-negative integer")
			}
		}
		if functionName == "nth_value" {
			if len(args) != 2 {
				return true, fmt.Errorf("nth_value requires an expression and a positive integer")
			}
			argumentOffset, err = strconv.ParseInt(strings.TrimSpace(args[1]), 10, 64)
			if err != nil || argumentOffset <= 0 {
				return true, fmt.Errorf("nth_value position must be a positive integer")
			}
		}
	}
	ntileCount := int64(0)
	if functionName == "ntile" {
		ntileCount, err = strconv.ParseInt(strings.TrimSpace(functionArgs), 10, 64)
		if err != nil || ntileCount <= 0 {
			return true, fmt.Errorf("NTILE requires a positive bucket count")
		}
	}

	groups := make(map[string][]windowRowState)
	groupOrder := make([]string, 0)
	for index, record := range sourceResult.Records {
		key := windowPartitionKey(record, partitionIndexes)
		if _, exists := groups[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		orderValues := make([]basic.Value, 0, len(orderIndexes))
		for _, orderIndex := range orderIndexes {
			values := record.GetValues()
			if orderIndex < len(values) {
				orderValues = append(orderValues, values[orderIndex])
			} else {
				orderValues = append(orderValues, nil)
			}
		}
		var orderValue basic.Value
		if len(orderValues) > 0 {
			orderValue = orderValues[0]
		}
		groups[key] = append(groups[key], windowRowState{index: index, key: windowOrderPeerKey(orderValues), value: orderValue, orderValues: orderValues})
	}
	windowValues := make([]interface{}, len(sourceResult.Records))
	for _, groupKey := range groupOrder {
		rows := groups[groupKey]
		sort.SliceStable(rows, func(i, j int) bool {
			return compareWindowOrder(rows[i].orderValues, rows[j].orderValues, orderDirections) < 0
		})
		var lastValue basic.Value
		rank, dense := 0, 0
		for position, row := range rows {
			if position == 0 || !sameWindowOrder(row.value, lastValue) || (position > 0 && row.key != rows[position-1].key) {
				rank = position + 1
				dense++
				lastValue = row.value
			}
			switch functionName {
			case "row_number":
				windowValues[row.index] = int64(position + 1)
			case "rank":
				windowValues[row.index] = int64(rank)
			case "dense_rank":
				windowValues[row.index] = int64(dense)
			case "ntile":
				windowValues[row.index] = int64((position*int(ntileCount))/len(rows) + 1)
			case "lag", "lead":
				target := position - int(argumentOffset)
				if functionName == "lead" {
					target = position + int(argumentOffset)
				}
				if target < 0 || target >= len(rows) {
					windowValues[row.index] = nil
				} else {
					windowValues[row.index] = basicValueInterface(sourceResult.Records[rows[target].index].GetValues()[argumentIndex])
				}
			case "first_value":
				start, _ := windowFrameBounds(frame, rows, position, descending)
				windowValues[row.index] = basicValueInterface(sourceResult.Records[rows[start].index].GetValues()[argumentIndex])
			case "last_value":
				start, end := windowFrameBounds(frame, rows, position, descending)
				_ = start
				windowValues[row.index] = basicValueInterface(sourceResult.Records[rows[end].index].GetValues()[argumentIndex])
			case "nth_value":
				start, end := windowFrameBounds(frame, rows, position, descending)
				target := start + int(argumentOffset) - 1
				if target < start || target > end {
					windowValues[row.index] = nil
				} else {
					windowValues[row.index] = basicValueInterface(sourceResult.Records[rows[target].index].GetValues()[argumentIndex])
				}
			case "cume_dist":
				lastPeer := position
				for lastPeer+1 < len(rows) && rows[lastPeer+1].key == row.key {
					lastPeer++
				}
				windowValues[row.index] = float64(lastPeer+1) / float64(len(rows))
			case "percent_rank":
				if len(rows) <= 1 {
					windowValues[row.index] = float64(0)
				} else {
					windowValues[row.index] = float64(rank-1) / float64(len(rows)-1)
				}
			case "sum", "avg", "min", "max", "count", "std", "stddev", "stddev_pop", "stddev_samp", "var_pop", "var_samp", "variance", "bit_and", "bit_or", "bit_xor", "json_arrayagg", "json_objectagg":
				start, end := windowFrameBounds(frame, rows, position, descending)
				// Without ORDER BY, MySQL's aggregate window frame is the whole
				// partition, not an input-order running frame. parseWindowFrame
				// supplies the ordered default frame, so this must be explicit.
				if len(orderIndexes) == 0 {
					start, end = 0, len(rows)-1
				}
				windowValues[row.index] = aggregateWindowFrame(functionName, sourceResult, rows, start, end, argumentIndex, functionValueIndex)
			}
		}
	}
	output := make([][]interface{}, 0, len(sourceResult.Records))
	for index, record := range sourceResult.Records {
		row := make([]interface{}, 0, len(baseIndexes)+1)
		for _, baseIndex := range baseIndexes {
			row = append(row, basicValueInterface(record.GetValues()[baseIndex]))
		}
		windowValue := windowValues[index]
		if functionName == "sum" || functionName == "avg" || functionName == "count" || strings.HasPrefix(functionName, "std") || strings.HasPrefix(functionName, "var_") || functionName == "variance" || strings.HasPrefix(functionName, "bit_") {
			windowValue = formatWindowAggregateValue(windowValue)
		}
		row = append(row, windowValue)
		output = append(output, row)
	}
	columns := make([]string, 0, len(baseColumns)+1)
	for _, column := range baseColumns {
		columns = append(columns, strings.Trim(strings.TrimSpace(column), "`"))
	}
	columns = append(columns, strings.Trim(afterMatch[1], "`"))
	windowResult := newInformationSchemaSelectResult("window", columns, output)
	windowResult.ColumnTypes = make([]string, 0, len(baseIndexes)+1)
	for _, baseIndex := range baseIndexes {
		if baseIndex >= 0 && baseIndex < len(sourceResult.ColumnTypes) {
			windowResult.ColumnTypes = append(windowResult.ColumnTypes, sourceResult.ColumnTypes[baseIndex])
		} else {
			windowResult.ColumnTypes = append(windowResult.ColumnTypes, "varchar")
		}
	}
	windowResult.ColumnTypes = append(windowResult.ColumnTypes, windowResultType(functionName))
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: windowResult, Message: fmt.Sprintf("window query returned %d rows", len(output))}
	return true, nil
}

func (e *XMySQLExecutor) executeWindowOnlyProjection(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	match := regexp.MustCompile(`(?is)^select\s+(row_number|rank|dense_rank|ntile|lag|lead|first_value|last_value|nth_value|cume_dist|percent_rank|sum|avg|min|max|count|std|stddev|stddev_pop|stddev_samp|var_pop|var_samp|variance|bit_and|bit_or|bit_xor|json_arrayagg|json_objectagg)\s*\((.*?)\)\s+over\s*\(`).FindStringSubmatchIndex(query)
	if len(match) == 0 {
		return false, nil
	}
	overOpen := match[1] - 1
	overClose := matchingParenIndex(query, overOpen)
	if overClose < 0 {
		return true, fmt.Errorf("window specification has unbalanced parentheses")
	}
	afterOver := strings.TrimSpace(query[overClose+1:])
	afterMatch := regexp.MustCompile("(?is)^as\\s+`?([a-zA-Z0-9_$]+)`?\\s+from\\s+`?([a-zA-Z0-9_$]+)`?(?:\\s+order\\s+by\\s+.+)?$").FindStringSubmatch(afterOver)
	if len(afterMatch) == 0 {
		return true, fmt.Errorf("window-only query must provide an alias and source table")
	}
	sourceTable := afterMatch[2]
	sourceStmt, err := sqlparser.Parse("select * from " + sourceTable)
	if err != nil {
		return true, err
	}
	sourceSelect, ok := sourceStmt.(*sqlparser.Select)
	if !ok {
		return true, fmt.Errorf("window source is not SELECT")
	}
	sourceResult, err := e.executeSelectStatement(ctx, sourceSelect, databaseName)
	if err != nil {
		return true, err
	}
	if sourceResult == nil || len(sourceResult.Columns) == 0 {
		return true, fmt.Errorf("window source %s has no columns", sourceTable)
	}
	windowExpression := strings.TrimSpace(query[len("select ") : overClose+1])
	singleQuery := "select " + sourceResult.Columns[0] + ", " + windowExpression + " " + afterOver
	results := make(chan *Result, 1)
	temporary := *ctx
	temporary.Results = results
	if _, err := e.executeGeneralWindowQuery(&temporary, singleQuery, databaseName); err != nil {
		return true, err
	}
	result := <-results
	if result == nil {
		return true, fmt.Errorf("window-only query returned no result")
	}
	if result.Err != nil {
		return true, result.Err
	}
	selectResult, ok := result.Data.(*SelectResult)
	if !ok || selectResult == nil {
		return true, fmt.Errorf("window-only query returned unexpected result %T", result.Data)
	}
	windowColumn := afterMatch[1]
	windowType := "varchar"
	if len(selectResult.ColumnTypes) > 1 {
		windowType = selectResult.ColumnTypes[len(selectResult.ColumnTypes)-1]
	}
	columns := []*metadata.ColumnMeta{{Name: windowColumn, Type: windowResultColumnType(windowType)}}
	records := make([]Record, 0, len(selectResult.Records))
	for _, record := range selectResult.Records {
		values := record.GetValues()
		if len(values) == 0 {
			records = append(records, NewExecutorRecordFromInterface([]interface{}{nil}, &metadata.TableMeta{Name: "window", Columns: columns}))
			continue
		}
		records = append(records, NewExecutorRecordFromInterface([]interface{}{windowResultInterface(values[len(values)-1])}, &metadata.TableMeta{Name: "window", Columns: columns}))
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: &SelectResult{Records: records, RowCount: len(records), Columns: []string{windowColumn}, ColumnTypes: []string{windowType}, ResultType: common.RESULT_TYPE_QUERY}, Message: fmt.Sprintf("window query returned %d rows", len(records))}
	return true, nil
}

func windowResultColumnType(typeName string) metadata.DataType {
	switch strings.ToLower(typeName) {
	case "bigint":
		return metadata.TypeBigInt
	case "double":
		return metadata.TypeDouble
	case "tinyint":
		return metadata.TypeTinyInt
	default:
		return metadata.TypeVarchar
	}
}

// executeMultipleWindowExpressions decomposes the common SELECT shape that
// projects several independent window expressions over the same row source.
// The single-window executor below already owns frame, peer, partition and
// type semantics; running it once per expression and merging the aligned
// source rows keeps those semantics in one place while removing the old
// one-window-per-query limitation.
func (e *XMySQLExecutor) executeMultipleWindowExpressions(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	lower := strings.ToLower(strings.TrimSpace(query))
	if !strings.HasPrefix(lower, "select ") {
		return false, nil
	}
	fromParts := splitTopLevelKeyword(strings.TrimSpace(query[len("select "):]), "from")
	if len(fromParts) != 2 {
		return false, nil
	}
	items := splitTopLevelComma(fromParts[0])
	if len(items) < 3 {
		return false, nil
	}
	baseItems := make([]string, 0, len(items))
	windowItems := make([]string, 0, len(items))
	for _, item := range items {
		trimmedItem := strings.TrimSpace(item)
		if !strings.Contains(strings.ToLower(trimmedItem), " over ") {
			baseItems = append(baseItems, trimmedItem)
			continue
		}
		if !regexp.MustCompile("(?is)\\s+as\\s+`?[a-zA-Z0-9_$]+`?\\s*$").MatchString(trimmedItem) {
			return false, nil
		}
		windowItems = append(windowItems, trimmedItem)
	}
	if len(windowItems) < 2 || len(baseItems) == 0 {
		return false, nil
	}

	baseProjection := strings.Join(baseItems, ", ")
	return e.executeMultipleWindowExpressionsViaTemporaryContext(ctx, databaseName, baseProjection, fromParts[1], windowItems)
}

func (e *XMySQLExecutor) executeMultipleWindowExpressionsViaTemporaryContext(ctx *ExecutionContext, databaseName, baseProjection, fromTail string, windowItems []string) (bool, error) {
	var merged *SelectResult
	for _, windowItem := range windowItems {
		results := make(chan *Result, 1)
		temporary := *ctx
		temporary.Results = results
		if _, err := e.executeGeneralWindowQuery(&temporary, "select "+baseProjection+", "+windowItem+" from "+strings.TrimSpace(fromTail), databaseName); err != nil {
			return true, err
		}
		result := <-results
		if result == nil {
			return true, fmt.Errorf("window expression %q returned no result", windowItem)
		}
		if result.Err != nil {
			return true, result.Err
		}
		current, ok := result.Data.(*SelectResult)
		if !ok {
			return true, fmt.Errorf("window expression %q returned unexpected result %T", windowItem, result.Data)
		}
		if current == nil {
			return true, fmt.Errorf("window expression %q returned no result", windowItem)
		}
		if merged == nil {
			merged = current
			continue
		}
		if len(current.Records) != len(merged.Records) || len(current.Columns) == 0 {
			return true, fmt.Errorf("window expressions returned different row sets")
		}
		merged.Columns = append(merged.Columns, current.Columns[len(current.Columns)-1])
		if len(current.ColumnTypes) > 0 {
			merged.ColumnTypes = append(merged.ColumnTypes, current.ColumnTypes[len(current.ColumnTypes)-1])
		}
		for index := range merged.Records {
			mergedValues := merged.Records[index].GetValues()
			currentValues := current.Records[index].GetValues()
			if len(currentValues) == 0 {
				return true, fmt.Errorf("window expression %q returned an empty row", windowItem)
			}
			merged.Records[index] = NewExecutorRecordFromInterface(append(basicValuesToInterfaces(mergedValues), windowResultInterface(currentValues[len(currentValues)-1])), &metadata.TableMeta{Name: "window", Columns: windowResultColumns(merged.Columns, merged.ColumnTypes)})
		}
	}
	merged.RowCount = len(merged.Records)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: merged, Message: fmt.Sprintf("window query returned %d rows", merged.RowCount)}
	return true, nil
}

func basicValuesToInterfaces(values []basic.Value) []interface{} {
	result := make([]interface{}, 0, len(values))
	for _, value := range values {
		result = append(result, windowResultInterface(value))
	}
	return result
}

func windowResultInterface(value basic.Value) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}
	switch value.Type() {
	case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeInt, basic.ValueTypeBigInt:
		return strconv.FormatInt(value.Int(), 10)
	case basic.ValueTypeFloat, basic.ValueTypeDouble, basic.ValueTypeDecimal:
		return strconv.FormatFloat(value.Float64(), 'f', -1, 64)
	default:
		return value.String()
	}
}

func windowResultColumns(columns, columnTypes []string) []*metadata.ColumnMeta {
	result := make([]*metadata.ColumnMeta, 0, len(columns))
	for index, column := range columns {
		typeName := metadata.TypeVarchar
		if index < len(columnTypes) {
			switch strings.ToLower(columnTypes[index]) {
			case "bigint":
				typeName = metadata.TypeBigInt
			case "double":
				typeName = metadata.TypeDouble
			case "tinyint":
				typeName = metadata.TypeTinyInt
			}
		}
		result = append(result, &metadata.ColumnMeta{Name: column, Type: typeName})
	}
	return result
}

func formatWindowAggregateValue(value interface{}) interface{} {
	switch number := value.(type) {
	case int:
		return strconv.Itoa(number)
	case int64:
		return strconv.FormatInt(number, 10)
	case float64:
		return strconv.FormatFloat(number, 'f', -1, 64)
	default:
		return value
	}
}

func aggregateWindowFrame(functionName string, sourceResult *SelectResult, rows []windowRowState, start, end, argumentIndex, valueArgumentIndex int) interface{} {
	if sourceResult == nil || start < 0 || end < start || end >= len(rows) {
		return nil
	}
	count := int64(0)
	var sum float64
	var integerSum int64
	allIntegers := true
	var minValue, maxValue basic.Value
	var mean, m2 float64
	bitValue := uint64(0)
	bitInitialized := false
	jsonValues := make([]interface{}, 0)
	jsonObject := make(map[string]interface{})
	if functionName == "bit_and" {
		bitValue = ^uint64(0)
	}
	for position := start; position <= end; position++ {
		if argumentIndex == -2 {
			count++
			continue
		}
		value := sourceResult.Records[rows[position].index].GetValues()[argumentIndex]
		if functionName == "json_objectagg" {
			key := jsonWindowValue(value, windowColumnType(sourceResult, argumentIndex))
			if key == nil {
				continue
			}
			valueRecord := sourceResult.Records[rows[position].index].GetValues()
			var objectValue basic.Value
			if valueArgumentIndex >= 0 && valueArgumentIndex < len(valueRecord) {
				objectValue = valueRecord[valueArgumentIndex]
			}
			jsonObject[fmt.Sprint(key)] = jsonWindowValue(objectValue, windowColumnType(sourceResult, valueArgumentIndex))
			continue
		}
		if functionName == "json_arrayagg" {
			columnType := windowColumnType(sourceResult, argumentIndex)
			jsonValues = append(jsonValues, jsonWindowValue(value, columnType))
			continue
		}
		if value == nil || value.IsNull() {
			continue
		}
		count++
		if minValue == nil || compareWindowValues(value, minValue) < 0 {
			minValue = value
		}
		if maxValue == nil || compareWindowValues(value, maxValue) > 0 {
			maxValue = value
		}
		if number, ok := numericWindowValue(value); ok {
			sum += number
			delta := number - mean
			mean += delta / float64(count)
			m2 += delta * (number - mean)
			if strings.HasPrefix(functionName, "bit_") {
				unsigned := uint64(number)
				switch functionName {
				case "bit_and":
					bitValue &= unsigned
				case "bit_or":
					bitValue |= unsigned
				case "bit_xor":
					bitValue ^= unsigned
				}
				bitInitialized = true
			}
			if value.Type() == basic.ValueTypeTinyInt || value.Type() == basic.ValueTypeSmallInt || value.Type() == basic.ValueTypeMediumInt || value.Type() == basic.ValueTypeInt || value.Type() == basic.ValueTypeBigInt || isIntegerWindowString(value.String()) {
				if parsed, err := strconv.ParseInt(strings.TrimSpace(value.String()), 10, 64); err == nil {
					integerSum += parsed
				} else {
					integerSum += value.Int()
				}
			} else {
				allIntegers = false
			}
		} else {
			allIntegers = false
		}
	}
	switch functionName {
	case "count":
		return count
	case "sum":
		if count == 0 {
			return nil
		}
		if allIntegers {
			return integerSum
		}
		return sum
	case "avg":
		if count == 0 {
			return nil
		}
		return sum / float64(count)
	case "std", "stddev", "stddev_pop":
		if count == 0 {
			return nil
		}
		return math.Sqrt(m2 / float64(count))
	case "stddev_samp":
		if count < 2 {
			return nil
		}
		return math.Sqrt(m2 / float64(count-1))
	case "var_pop", "variance":
		if count == 0 {
			return nil
		}
		return m2 / float64(count)
	case "var_samp":
		if count < 2 {
			return nil
		}
		return m2 / float64(count-1)
	case "bit_and", "bit_or", "bit_xor":
		if !bitInitialized {
			if functionName == "bit_and" {
				allBits := ^uint64(0)
				return int64(allBits)
			}
			return int64(0)
		}
		return int64(bitValue)
	case "json_arrayagg":
		encoded, err := json.Marshal(jsonValues)
		if err != nil {
			return nil
		}
		return string(encoded)
	case "json_objectagg":
		encoded, err := json.Marshal(jsonObject)
		if err != nil {
			return nil
		}
		return string(encoded)
	case "min":
		return basicValueInterface(minValue)
	case "max":
		return basicValueInterface(maxValue)
	default:
		return nil
	}
}

func windowColumnType(sourceResult *SelectResult, index int) string {
	if sourceResult == nil || index < 0 || index >= len(sourceResult.ColumnTypes) {
		return ""
	}
	return sourceResult.ColumnTypes[index]
}

func jsonWindowValue(value basic.Value, columnType string) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}
	columnType = strings.ToLower(columnType)
	switch columnType {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year":
		if parsed, err := strconv.ParseInt(strings.TrimSpace(value.String()), 10, 64); err == nil {
			return parsed
		}
		return value.Int()
	case "float", "double", "decimal":
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(value.String()), 64); err == nil {
			return parsed
		}
		return value.Float64()
	default:
		return basicValueInterface(value)
	}
}

func isIntegerWindowString(value string) bool {
	_, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	return err == nil
}

func windowResultType(functionName string) string {
	switch functionName {
	case "avg", "std", "stddev", "stddev_pop", "stddev_samp", "var_pop", "var_samp", "variance":
		return "double"
	case "cume_dist", "percent_rank":
		return "double"
	case "sum", "count", "bit_and", "bit_or", "bit_xor":
		return "bigint"
	case "json_arrayagg", "json_objectagg":
		return "varchar"
	case "min", "max":
		return "varchar"
	default:
		return "bigint"
	}
}

func sameWindowOrder(left, right basic.Value) bool {
	return compareWindowValues(left, right) == 0
}

func compareWindowOrder(left, right []basic.Value, descending []bool) int {
	for index := 0; index < len(left) && index < len(right); index++ {
		cmp := compareWindowValues(left[index], right[index])
		if cmp == 0 {
			continue
		}
		if index < len(descending) && descending[index] {
			return -cmp
		}
		return cmp
	}
	if len(left) < len(right) {
		return -1
	}
	if len(left) > len(right) {
		return 1
	}
	return 0
}

func windowOrderPeerKey(values []basic.Value) string {
	if len(values) == 0 {
		return "__no_order__"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, windowValueKey(value))
	}
	return strings.Join(parts, "|")
}

func compareWindowValues(left, right basic.Value) int {
	if left == nil || left.IsNull() {
		if right == nil || right.IsNull() {
			return 0
		}
		return -1
	}
	if right == nil || right.IsNull() {
		return 1
	}
	if leftNumeric, leftOK := numericWindowValue(left); leftOK {
		if rightNumeric, rightOK := numericWindowValue(right); rightOK {
			switch {
			case leftNumeric < rightNumeric:
				return -1
			case leftNumeric > rightNumeric:
				return 1
			default:
				return 0
			}
		}
	}
	return strings.Compare(left.String(), right.String())
}

func numericWindowValue(value basic.Value) (float64, bool) {
	if value == nil || value.IsNull() {
		return 0, false
	}
	switch value.Type() {
	case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeInt, basic.ValueTypeBigInt:
		return float64(value.Int()), true
	case basic.ValueTypeFloat, basic.ValueTypeDouble, basic.ValueTypeDecimal:
		return value.Float64(), true
	case basic.ValueTypeVarchar, basic.ValueTypeChar, basic.ValueTypeText:
		number, err := strconv.ParseFloat(strings.TrimSpace(value.String()), 64)
		return number, err == nil
	default:
		return 0, false
	}
}

func parseWindowSpecification(spec string) ([]string, []string, []bool, error) {
	lower := strings.ToLower(spec)
	partition := []string{}
	if at := strings.Index(lower, "partition by"); at >= 0 {
		start := at + len("partition by")
		end := len(spec)
		if orderAt := strings.Index(lower[start:], " order by"); orderAt >= 0 {
			end = start + orderAt
		}
		partition = splitTopLevelComma(strings.TrimSpace(spec[start:end]))
	}
	orderColumns := []string{}
	orderDirections := []bool{}
	if at := strings.Index(lower, "order by"); at >= 0 {
		order := strings.TrimSpace(spec[at+len("order by"):])
		order = regexp.MustCompile(`(?is)\s+(rows|range)\s+.+$`).ReplaceAllString(order, "")
		for _, term := range splitTopLevelComma(order) {
			fields := strings.Fields(strings.TrimSpace(term))
			if len(fields) == 0 {
				continue
			}
			orderColumns = append(orderColumns, strings.Trim(fields[0], "`"))
			orderDirections = append(orderDirections, len(fields) > 1 && strings.EqualFold(fields[1], "desc"))
		}
	}
	return partition, orderColumns, orderDirections, nil
}

// parseWindowFrame parses the frame portion of a window specification. The
// compatibility executor intentionally supports the row/range forms that are
// stable across MySQL clients and rejects malformed or negative bounds rather
// than silently treating them as the whole partition.
func parseWindowFrame(spec, functionName string) (*WindowFrame, error) {
	lower := strings.ToLower(strings.TrimSpace(spec))
	frameType := FrameTypeRange
	marker := "range"
	if strings.Contains(lower, " rows ") || strings.HasSuffix(lower, " rows") {
		frameType = FrameTypeRows
		marker = "rows"
	}
	frameAt := strings.Index(lower, marker)
	if frameAt < 0 {
		if functionName == "first_value" || functionName == "last_value" || functionName == "nth_value" || functionName == "sum" || functionName == "avg" || functionName == "min" || functionName == "max" || functionName == "count" || functionName == "std" || functionName == "stddev" || functionName == "stddev_pop" || functionName == "stddev_samp" || functionName == "var_pop" || functionName == "var_samp" || functionName == "variance" || functionName == "bit_and" || functionName == "bit_or" || functionName == "bit_xor" {
			// MySQL's default for ordered value functions is RANGE UNBOUNDED
			// PRECEDING ... CURRENT ROW.
			return &WindowFrame{Type: FrameTypeRange, Start: WindowFrameBound{Type: "UNBOUNDED_PRECEDING"}, End: WindowFrameBound{Type: "CURRENT_ROW"}}, nil
		}
		return nil, nil
	}
	frameText := strings.TrimSpace(spec[frameAt+len(marker):])
	if frameText == "" {
		return nil, fmt.Errorf("window frame requires a bound")
	}
	frameText = strings.TrimSpace(strings.ToLower(frameText))
	between := strings.HasPrefix(frameText, "between ")
	if between {
		frameText = strings.TrimSpace(strings.TrimPrefix(frameText, "between"))
	}
	parts := strings.SplitN(frameText, "between", 2)
	parseBound := func(value string) (WindowFrameBound, error) {
		value = strings.TrimSpace(value)
		if value == "unbounded preceding" {
			return WindowFrameBound{Type: "UNBOUNDED_PRECEDING"}, nil
		}
		if value == "unbounded following" {
			return WindowFrameBound{Type: "UNBOUNDED_FOLLOWING"}, nil
		}
		if value == "current row" {
			return WindowFrameBound{Type: "CURRENT_ROW"}, nil
		}
		fields := strings.Fields(value)
		if len(fields) != 2 {
			return WindowFrameBound{}, fmt.Errorf("invalid window frame bound %q", value)
		}
		offset, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil || offset < 0 || (fields[1] != "preceding" && fields[1] != "following") {
			return WindowFrameBound{}, fmt.Errorf("invalid window frame bound %q", value)
		}
		kind := "N_PRECEDING"
		if fields[1] == "following" {
			kind = "N_FOLLOWING"
		}
		return WindowFrameBound{Type: kind, Offset: offset}, nil
	}
	if !between {
		start, err := parseBound(parts[0])
		if err != nil {
			return nil, err
		}
		if start.Type == "UNBOUNDED_PRECEDING" || start.Type == "UNBOUNDED_FOLLOWING" || start.Type == "CURRENT_ROW" || start.Type == "N_PRECEDING" || start.Type == "N_FOLLOWING" {
			return &WindowFrame{Type: frameType, Start: start, End: WindowFrameBound{Type: "CURRENT_ROW"}}, nil
		}
	}
	if len(parts) != 1 {
		return nil, fmt.Errorf("window frame requires BETWEEN ... AND ...")
	}
	bounds := strings.SplitN(parts[0], "and", 2)
	if len(bounds) != 2 {
		return nil, fmt.Errorf("window frame requires BETWEEN ... AND ...")
	}
	start, err := parseBound(bounds[0])
	if err != nil {
		return nil, err
	}
	end, err := parseBound(bounds[1])
	if err != nil {
		return nil, err
	}
	return &WindowFrame{Type: frameType, Start: start, End: end}, nil
}

func windowFrameBounds(frame *WindowFrame, rows []windowRowState, position int, descending bool) (int, int) {
	if len(rows) == 0 {
		return 0, 0
	}
	start, end := 0, position
	if frame == nil {
		return start, end
	}
	if frame.Type == FrameTypeRange {
		// RANGE CURRENT ROW includes all peers with the same ORDER BY key.
		current, numeric := numericWindowValue(rows[position].value)
		rangeIndex := func(bound WindowFrameBound, isStart bool) int {
			switch bound.Type {
			case "UNBOUNDED_PRECEDING":
				return 0
			case "UNBOUNDED_FOLLOWING":
				return len(rows) - 1
			case "CURRENT_ROW":
				index := position
				if isStart {
					for index > 0 && rows[index-1].key == rows[position].key {
						index--
					}
				} else {
					for index+1 < len(rows) && rows[index+1].key == rows[position].key {
						index++
					}
				}
				return index
			}
			if !numeric {
				return position
			}
			var threshold float64
			switch bound.Type {
			case "N_PRECEDING":
				threshold = current - float64(bound.Offset)
				if descending {
					threshold = current + float64(bound.Offset)
				}
				if isStart {
					for index, row := range rows {
						value, ok := numericWindowValue(row.value)
						if ok && ((!descending && value >= threshold) || (descending && value <= threshold)) {
							return index
						}
					}
					return len(rows) - 1
				}
				for index := len(rows) - 1; index >= 0; index-- {
					value, ok := numericWindowValue(rows[index].value)
					if ok && ((!descending && value <= threshold) || (descending && value >= threshold)) {
						return index
					}
				}
			case "N_FOLLOWING":
				threshold = current + float64(bound.Offset)
				if descending {
					threshold = current - float64(bound.Offset)
				}
				if isStart {
					for index, row := range rows {
						value, ok := numericWindowValue(row.value)
						if ok && ((!descending && value >= threshold) || (descending && value <= threshold)) {
							return index
						}
					}
				} else {
					for index := len(rows) - 1; index >= 0; index-- {
						value, ok := numericWindowValue(rows[index].value)
						if ok && ((!descending && value <= threshold) || (descending && value >= threshold)) {
							return index
						}
					}
				}
			}
			return position
		}
		start = rangeIndex(frame.Start, true)
		end = rangeIndex(frame.End, false)
		if start > end {
			return 0, -1
		}
		return start, end
	}
	boundIndex := func(bound WindowFrameBound, current int, isStart bool) int {
		switch bound.Type {
		case "UNBOUNDED_PRECEDING":
			return 0
		case "UNBOUNDED_FOLLOWING":
			return len(rows) - 1
		case "CURRENT_ROW":
			return current
		case "N_PRECEDING":
			return current - int(bound.Offset)
		case "N_FOLLOWING":
			return current + int(bound.Offset)
		default:
			if isStart {
				return 0
			}
			return current
		}
	}
	start = boundIndex(frame.Start, position, true)
	end = boundIndex(frame.End, position, false)
	if start < 0 {
		start = 0
	}
	if end >= len(rows) {
		end = len(rows) - 1
	}
	if start >= len(rows) {
		start = len(rows) - 1
	}
	if end < 0 {
		end = 0
	}
	if start > end {
		return 0, -1
	}
	return start, end
}

func findResultColumn(columns []string, expression string) int {
	clean := strings.Trim(strings.TrimSpace(expression), "`")
	if dot := strings.LastIndex(clean, "."); dot >= 0 {
		clean = clean[dot+1:]
	}
	for index, column := range columns {
		if strings.EqualFold(strings.Trim(column, "`"), clean) {
			return index
		}
	}
	return -1
}

func windowPartitionKey(record Record, indexes []int) string {
	if len(indexes) == 0 {
		return "__all__"
	}
	parts := make([]string, 0, len(indexes))
	for _, index := range indexes {
		values := record.GetValues()
		if index < len(values) {
			parts = append(parts, windowValueKey(values[index]))
		}
	}
	return strings.Join(parts, "|")
}

func windowValueKey(value basic.Value) string {
	if value == nil || value.IsNull() {
		return "<null>"
	}
	return fmt.Sprintf("%d:%s", value.Type(), value.String())
}

func basicValueInterface(value basic.Value) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}
	switch value.Type() {
	case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeInt, basic.ValueTypeBigInt:
		return value.Int()
	case basic.ValueTypeFloat, basic.ValueTypeDouble, basic.ValueTypeDecimal:
		return value.Float64()
	default:
		return value.String()
	}
}
