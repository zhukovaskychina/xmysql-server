package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// TABLE(CONTENT_VIDEO_SEGMENTS(content_column, segment_ms[, overlap_ms]))
// expands one source-bound ISO-BMFF video value into bounded time windows.
func visitContentVideoSegmentsTable(ctx context.Context, function *sqlparser.FuncExpr, values map[string]interface{}, visit func(derivedRow) error) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if visit == nil {
		return 0, fmt.Errorf("CONTENT_VIDEO_SEGMENTS row visitor is required")
	}
	if function == nil || len(function.Exprs) < 2 || len(function.Exprs) > 3 {
		return 0, fmt.Errorf("CONTENT_VIDEO_SEGMENTS requires a CONTENT column, segment_ms, and optional overlap_ms")
	}
	first, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return 0, fmt.Errorf("CONTENT_VIDEO_SEGMENTS first argument must be a CONTENT column")
	}
	sourceColumn, ok := first.Expr.(*sqlparser.ColName)
	if !ok {
		return 0, fmt.Errorf("CONTENT_VIDEO_SEGMENTS first argument must be a CONTENT column")
	}
	arguments := make([]interface{}, len(function.Exprs))
	for index, raw := range function.Exprs {
		argument, ok := raw.(*sqlparser.AliasedExpr)
		if !ok {
			return 0, fmt.Errorf("CONTENT_VIDEO_SEGMENTS arguments must be scalar expressions")
		}
		value, err := evaluateExpressionWithRow(argument.Expr, values)
		if err != nil {
			return 0, err
		}
		if value == nil {
			return 0, nil
		}
		arguments[index] = value
	}
	var sourceValue content.Value
	switch value := arguments[0].(type) {
	case content.Value:
		sourceValue = value
	case *content.Value:
		if value == nil {
			return 0, nil
		}
		sourceValue = *value
	default:
		return 0, fmt.Errorf("CONTENT_VIDEO_SEGMENTS requires a typed CONTENT column; got %T", arguments[0])
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return 0, fmt.Errorf("CONTENT_VIDEO_SEGMENTS requires stored-row context for the source column")
	}
	source, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, sourceValue)
	if err != nil {
		return 0, fmt.Errorf("CONTENT_VIDEO_SEGMENTS source verification failed: %w", err)
	}
	segmentMS, err := contentRangeArgument(arguments[1], "segment_ms")
	if err != nil {
		return 0, err
	}
	overlapMS := uint64(0)
	if len(arguments) == 3 {
		overlapMS, err = contentRangeArgument(arguments[2], "overlap_ms")
		if err != nil {
			return 0, err
		}
	}
	store, err := contentSegmentStoreForValue(values, sourceValue)
	if err != nil {
		return 0, err
	}
	blocks, err := content.ParseVideoSegmentsFromStore(ctx, store, sourceValue, source, segmentMS, overlapMS)
	if err != nil {
		return 0, fmt.Errorf("CONTENT_VIDEO_SEGMENTS failed: %w", err)
	}
	for _, block := range blocks {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		encoded, err := content.BlockValue(block)
		if err != nil {
			return 0, fmt.Errorf("encode video segment %d: %w", block.Ordinal, err)
		}
		if block.Locator.StartMS == nil || block.Locator.EndMS == nil {
			return 0, fmt.Errorf("video segment %d is missing its time interval", block.Ordinal)
		}
		if err := visit(derivedRow{values: map[string]interface{}{
			"ordinal": int64(block.Ordinal), "start_ms": int64(*block.Locator.StartMS),
			"end_ms": int64(*block.Locator.EndMS), "block": encoded,
		}}); err != nil {
			return 0, err
		}
	}
	return len(blocks), nil
}

func contentVideoSegmentsFunction(table *sqlparser.AliasedTableExpr) (*sqlparser.FuncExpr, string, bool) {
	if table == nil {
		return nil, "", false
	}
	tableFunction, ok := table.Expr.(*sqlparser.TableFunction)
	if !ok || tableFunction.Function == nil || !strings.EqualFold(tableFunction.Function.Name.String(), "CONTENT_VIDEO_SEGMENTS") {
		return nil, "", false
	}
	alias := strings.TrimSpace(table.As.String())
	if alias == "" {
		alias = "content_video_segments"
	}
	return tableFunction.Function, alias, true
}

func contentVideoSegmentsFunctionInExpr(expression sqlparser.TableExpr) (*sqlparser.AliasedTableExpr, bool) {
	table, ok := expression.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, false
	}
	_, _, ok = contentVideoSegmentsFunction(table)
	return table, ok
}

func lateralContentVideoSegments(ctx context.Context, left *derivedJoinSource, table *sqlparser.AliasedTableExpr, dataDir string) (*derivedJoinSource, error) {
	function, alias, ok := contentVideoSegmentsFunction(table)
	if !ok {
		return nil, fmt.Errorf("unsupported table-valued function")
	}
	if left == nil {
		return nil, fmt.Errorf("CONTENT_VIDEO_SEGMENTS requires a preceding row source")
	}
	if err := materializeDerivedSourceRows(left); err != nil {
		return nil, err
	}
	result := &derivedJoinSource{rows: make([]derivedRow, 0), columns: append([]string(nil), left.columns...)}
	segmentSource := emptyDeclaredTableFunctionSource("content_video_segments", contentVideoSegmentsResultColumns())
	for _, column := range segmentSource.columns {
		result.columns = append(result.columns, column)
	}
	result.qualifiedCols = cloneDerivedQualifiedColumns(left.qualifiedCols)
	result.qualifiedCols[strings.ToLower(alias)] = append([]string(nil), segmentSource.columns...)
	result.visitRows = func(visit func(derivedRow) error) error {
		for _, row := range left.rows {
			row.values[storedFunctionDataDirKey] = dataDir
			_, err := visitContentVideoSegmentsTable(ctx, function, row.values, func(segment derivedRow) error {
				values := cloneDerivedValues(segment.values)
				for key, value := range values {
					values[alias+"."+key] = value
					values[strings.ToLower(alias+"."+key)] = value
				}
				return visit(mergeDerivedRows(row, derivedRow{values: values}))
			})
			if err != nil {
				return err
			}
		}
		return nil
	}
	return result, nil
}
