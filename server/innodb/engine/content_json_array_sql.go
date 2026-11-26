package engine

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// CONTENT_PARSE_JSON_ARRAY(content_column) returns one source-bound typed
// table CONTENT block for an array of JSON objects in a stored row.
func evaluateContentParseJSONArray(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) != 1 {
		return nil, fmt.Errorf("CONTENT_PARSE_JSON_ARRAY requires exactly one stored CONTENT column")
	}
	argument, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("CONTENT_PARSE_JSON_ARRAY argument must be a CONTENT column")
	}
	sourceColumn, ok := argument.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("CONTENT_PARSE_JSON_ARRAY argument must be a CONTENT column")
	}
	sourceValue, err := evaluateExpressionWithRow(sourceColumn, values)
	if err != nil {
		return nil, err
	}
	if sourceValue == nil {
		return nil, nil
	}
	var typedSource content.Value
	switch typed := sourceValue.(type) {
	case content.Value:
		typedSource = typed
	case *content.Value:
		if typed == nil {
			return nil, nil
		}
		typedSource = *typed
	default:
		return nil, fmt.Errorf("CONTENT_PARSE_JSON_ARRAY requires a typed CONTENT source; got %T", sourceValue)
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return nil, fmt.Errorf("CONTENT_PARSE_JSON_ARRAY requires stored-row context for the source column")
	}
	source, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, typedSource)
	if err != nil {
		return nil, fmt.Errorf("CONTENT_PARSE_JSON_ARRAY source verification failed: %w", err)
	}
	store, err := contentSegmentStoreForValue(values, typedSource)
	if err != nil {
		return nil, err
	}
	block, err := content.ParseJSONArrayTableFromStore(storedFunctionContext(values), store, typedSource, source)
	if err != nil {
		return nil, fmt.Errorf("CONTENT_PARSE_JSON_ARRAY failed: %w", err)
	}
	return content.BlockValue(block)
}
