package engine

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// CONTENT_TEXT_CHUNK(content_column, ordinal, max_bytes[, overlap_bytes[, min_boundary_bytes]])
// returns one source-bound text block. CONTENT_TEXT_CHUNK_COUNT accepts the
// same options except ordinal and returns the number of available blocks.
func evaluateContentTextChunkSQL(function *sqlparser.FuncExpr, values map[string]interface{}, countOnly bool) (interface{}, error) {
	minimumArguments, maximumArguments := 3, 5
	if countOnly {
		minimumArguments, maximumArguments = 2, 4
	}
	if function == nil || len(function.Exprs) < minimumArguments || len(function.Exprs) > maximumArguments {
		if countOnly {
			return nil, fmt.Errorf("CONTENT_TEXT_CHUNK_COUNT requires a CONTENT column, max_bytes, and optional overlap/minimum boundary sizes")
		}
		return nil, fmt.Errorf("CONTENT_TEXT_CHUNK requires a CONTENT column, ordinal, max_bytes, and optional overlap/minimum boundary sizes")
	}
	sourceColumnExpr, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("%s first argument must be a CONTENT column", function.Name.String())
	}
	sourceColumn, ok := sourceColumnExpr.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("%s first argument must be a CONTENT column", function.Name.String())
	}
	arguments := make([]interface{}, len(function.Exprs))
	for index, rawArgument := range function.Exprs {
		aliased, ok := rawArgument.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("%s arguments must be scalar expressions", function.Name.String())
		}
		value, err := evaluateExpressionWithRow(aliased.Expr, values)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		arguments[index] = value
	}
	var sourceValue content.Value
	switch typed := arguments[0].(type) {
	case content.Value:
		sourceValue = typed
	case *content.Value:
		if typed == nil {
			return nil, nil
		}
		sourceValue = *typed
	default:
		return nil, fmt.Errorf("%s requires a typed CONTENT column; got %T", function.Name.String(), arguments[0])
	}
	optionsOffset := 1
	var ordinal uint64
	if !countOnly {
		ordinalValue, err := contentRangeArgument(arguments[1], "ordinal")
		if err != nil {
			return nil, err
		}
		ordinal = ordinalValue
		optionsOffset = 2
	}
	maxBytes, err := contentRangeArgument(arguments[optionsOffset], "max_bytes")
	if err != nil {
		return nil, err
	}
	if maxBytes == 0 {
		return nil, fmt.Errorf("%s max_bytes must be greater than zero", function.Name.String())
	}
	options := content.TextChunkOptions{}
	if maxBytes > uint64(content.MaxReadRangeBytes) {
		return nil, fmt.Errorf("%s max_bytes exceeds %d", function.Name.String(), content.MaxReadRangeBytes)
	}
	options.MaxBytes = int(maxBytes)
	if len(arguments) > optionsOffset+1 {
		overlap, err := contentRangeArgument(arguments[optionsOffset+1], "overlap_bytes")
		if err != nil {
			return nil, err
		}
		if overlap > uint64(content.MaxReadRangeBytes) {
			return nil, fmt.Errorf("%s overlap_bytes exceeds %d", function.Name.String(), content.MaxReadRangeBytes)
		}
		options.OverlapBytes = int(overlap)
	}
	if len(arguments) > optionsOffset+2 {
		minimum, err := contentRangeArgument(arguments[optionsOffset+2], "min_boundary_bytes")
		if err != nil {
			return nil, err
		}
		if minimum > uint64(content.MaxReadRangeBytes) {
			return nil, fmt.Errorf("%s min_boundary_bytes exceeds %d", function.Name.String(), content.MaxReadRangeBytes)
		}
		options.MinBoundaryBytes = int(minimum)
	}
	store, err := contentSegmentStoreForValue(values, sourceValue)
	if err != nil {
		return nil, err
	}
	if countOnly {
		count, err := content.TextChunkCountFromStore(storedFunctionContext(values), store, sourceValue, options)
		if err != nil {
			return nil, fmt.Errorf("CONTENT_TEXT_CHUNK_COUNT failed: %w", err)
		}
		return int64(count), nil
	}
	if ordinal > uint64(^uint32(0)) {
		return nil, nil
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return nil, fmt.Errorf("CONTENT_TEXT_CHUNK requires stored-row context for the source column")
	}
	source, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, sourceValue)
	if err != nil {
		return nil, fmt.Errorf("CONTENT_TEXT_CHUNK source verification failed: %w", err)
	}
	block, exists, err := content.TextChunkAtFromStore(storedFunctionContext(values), store, sourceValue, source, uint32(ordinal), options)
	if err != nil {
		return nil, fmt.Errorf("CONTENT_TEXT_CHUNK failed: %w", err)
	}
	if !exists {
		return nil, nil
	}
	return content.BlockValue(block)
}
