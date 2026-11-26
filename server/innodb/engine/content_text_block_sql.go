package engine

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

const (
	contentTextRangeProcessorID      = "xmysql.text-range"
	contentTextRangeProcessorVersion = "1"
)

// evaluateContentTextBlock creates a source-bound text block from one bounded
// UTF-8 byte range. The offset remains relative to the original CONTENT value,
// so the block is directly traceable to the exact bytes supplied to the SQL
// expression. Text extraction and encoding checks happen before a READY block
// is returned.
func evaluateContentTextBlock(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) != 4 {
		return nil, fmt.Errorf("CONTENT_TEXT_BLOCK requires a CONTENT column, block ordinal, byte offset and byte length")
	}
	arguments := make([]interface{}, 0, 4)
	for _, rawArgument := range function.Exprs {
		argument, ok := rawArgument.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("CONTENT_TEXT_BLOCK arguments must be scalar expressions")
		}
		value, err := evaluateExpressionWithRow(argument.Expr, values)
		if err != nil {
			return nil, err
		}
		arguments = append(arguments, value)
	}
	if arguments[0] == nil || arguments[1] == nil || arguments[2] == nil || arguments[3] == nil {
		return nil, nil
	}

	sourceColumnExpr, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("CONTENT_TEXT_BLOCK source must be a CONTENT column")
	}
	sourceColumn, ok := sourceColumnExpr.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("CONTENT_TEXT_BLOCK source must be a CONTENT column")
	}
	var sourceValue content.Value
	switch value := arguments[0].(type) {
	case content.Value:
		sourceValue = value
	case *content.Value:
		if value == nil {
			return nil, nil
		}
		sourceValue = *value
	default:
		return nil, fmt.Errorf("CONTENT_TEXT_BLOCK requires a typed CONTENT column; got %T", arguments[0])
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return nil, fmt.Errorf("CONTENT_TEXT_BLOCK requires stored-row context for the source column")
	}
	source, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, sourceValue)
	if err != nil {
		return nil, fmt.Errorf("CONTENT_TEXT_BLOCK source verification failed: %w", err)
	}

	ordinal, err := contentRangeArgument(arguments[1], "ordinal")
	if err != nil {
		return nil, err
	}
	if ordinal > uint64(^uint32(0)) {
		return nil, fmt.Errorf("CONTENT_TEXT_BLOCK ordinal exceeds the supported range")
	}
	offset, err := contentRangeArgument(arguments[2], "byte offset")
	if err != nil {
		return nil, err
	}
	length, err := contentRangeArgument(arguments[3], "byte length")
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, fmt.Errorf("CONTENT_TEXT_BLOCK byte length must be greater than zero")
	}
	if length > content.MaxReadRangeBytes {
		return nil, fmt.Errorf("CONTENT_TEXT_BLOCK byte length %d exceeds the %d byte range limit", length, content.MaxReadRangeBytes)
	}
	if offset >= sourceValue.Size() {
		return nil, nil
	}
	if remaining := sourceValue.Size() - offset; length > remaining {
		length = remaining
	}
	store, err := contentSegmentStoreForValue(values, sourceValue)
	if err != nil {
		return nil, err
	}
	textBytes, err := sourceValue.ReadRangeFromStore(storedFunctionContext(values), store, offset, length)
	if err != nil {
		return nil, fmt.Errorf("CONTENT_TEXT_BLOCK cannot read source range: %w", err)
	}
	inline, err := content.NewValue(sourceValue.MIMEType, sourceValue.Revision, textBytes)
	if err != nil {
		return nil, err
	}
	text, err := content.ExtractText(inline)
	if err != nil {
		return nil, fmt.Errorf("CONTENT_TEXT_BLOCK requires valid explicitly textual content: %w", err)
	}
	end := offset + uint64(len(textBytes))
	block, err := content.NewBlock(source, uint32(ordinal), content.BlockText, text, content.Locator{
		ByteStart: &offset,
		ByteEnd:   &end,
	}, contentTextRangeProcessorID, contentTextRangeProcessorVersion, "", "")
	if err != nil {
		return nil, fmt.Errorf("CONTENT_TEXT_BLOCK source range is invalid: %w", err)
	}
	return content.BlockValue(block)
}
