package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func executionContextBase(ctx *ExecutionContext) context.Context {
	if ctx == nil || ctx.Context == nil {
		return context.Background()
	}
	return ctx.Context
}

// CONTENT_TEXT_CHUNKS(content_column, max_bytes[, overlap_bytes[, min_boundary_bytes]])
// is a lateral table function. The first column argument binds each generated
// block to the exact source row and CONTENT revision.
func evaluateContentTextChunksTable(function *sqlparser.FuncExpr, values map[string]interface{}) (*derivedJoinSource, error) {
	result := emptyContentTextChunksSource()
	_, err := visitContentTextChunksTable(context.Background(), function, values, func(row derivedRow) error {
		result.rows = append(result.rows, row)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func visitContentTextChunksTable(ctx context.Context, function *sqlparser.FuncExpr, values map[string]interface{}, visit func(derivedRow) error) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if visit == nil {
		return 0, fmt.Errorf("CONTENT_TEXT_CHUNKS row visitor is required")
	}
	if function == nil || len(function.Exprs) < 2 || len(function.Exprs) > 4 {
		return 0, fmt.Errorf("CONTENT_TEXT_CHUNKS requires a CONTENT column, max_bytes, and optional overlap/minimum boundary sizes")
	}
	sourceArgument, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return 0, fmt.Errorf("CONTENT_TEXT_CHUNKS first argument must be a CONTENT column")
	}
	sourceColumn, ok := sourceArgument.Expr.(*sqlparser.ColName)
	if !ok {
		return 0, fmt.Errorf("CONTENT_TEXT_CHUNKS first argument must be a CONTENT column")
	}
	arguments := make([]interface{}, len(function.Exprs))
	for index, rawArgument := range function.Exprs {
		argument, ok := rawArgument.(*sqlparser.AliasedExpr)
		if !ok {
			return 0, fmt.Errorf("CONTENT_TEXT_CHUNKS arguments must be scalar expressions")
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
		return 0, fmt.Errorf("CONTENT_TEXT_CHUNKS requires a typed CONTENT column; got %T", arguments[0])
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return 0, fmt.Errorf("CONTENT_TEXT_CHUNKS requires stored-row context for the source column")
	}
	source, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, sourceValue)
	if err != nil {
		return 0, fmt.Errorf("CONTENT_TEXT_CHUNKS source verification failed: %w", err)
	}
	maxBytes, err := contentRangeArgument(arguments[1], "max_bytes")
	if err != nil {
		return 0, err
	}
	if maxBytes == 0 || maxBytes > uint64(content.MaxReadRangeBytes) {
		return 0, fmt.Errorf("CONTENT_TEXT_CHUNKS max_bytes must be between 1 and %d", content.MaxReadRangeBytes)
	}
	options := content.TextChunkOptions{MaxBytes: int(maxBytes)}
	if len(arguments) > 2 {
		overlap, err := contentRangeArgument(arguments[2], "overlap_bytes")
		if err != nil {
			return 0, err
		}
		if overlap > uint64(content.MaxReadRangeBytes) {
			return 0, fmt.Errorf("CONTENT_TEXT_CHUNKS overlap_bytes exceeds %d", content.MaxReadRangeBytes)
		}
		options.OverlapBytes = int(overlap)
	}
	if len(arguments) > 3 {
		minimum, err := contentRangeArgument(arguments[3], "min_boundary_bytes")
		if err != nil {
			return 0, err
		}
		if minimum > uint64(content.MaxReadRangeBytes) {
			return 0, fmt.Errorf("CONTENT_TEXT_CHUNKS min_boundary_bytes exceeds %d", content.MaxReadRangeBytes)
		}
		options.MinBoundaryBytes = int(minimum)
	}
	store, err := contentSegmentStoreForValue(values, sourceValue)
	if err != nil {
		return 0, err
	}
	maintenanceStore := store
	if maintenanceStore == nil {
		if dataDir, _ := values[storedFunctionDataDirKey].(string); strings.TrimSpace(dataDir) != "" {
			maintenanceStore, err = contentSegmentStoreForDataDir(dataDir)
			if err != nil {
				return 0, err
			}
		}
	}
	visitBlock := func(block content.Block) error {
		blockValue, err := content.BlockValue(block)
		if err != nil {
			return err
		}
		start, end := int64(0), int64(0)
		if block.Locator.ByteStart != nil {
			start = int64(*block.Locator.ByteStart)
		}
		if block.Locator.ByteEnd != nil {
			end = int64(*block.Locator.ByteEnd)
		}
		return visit(derivedRow{values: map[string]interface{}{
			"ordinal": int64(block.Ordinal), "text": block.Text, "block": blockValue,
			"byte_start": start, "byte_end": end,
		}})
	}
	if count, reused, err := visitPublishedTextChunks(ctx, maintenanceStore, source, options, visitBlock); err != nil {
		return count, fmt.Errorf("CONTENT_TEXT_CHUNKS published result failed: %w", err)
	} else if reused {
		return count, nil
	}
	count, err := content.VisitTextChunksFromStore(ctx, store, sourceValue, source, options, visitBlock)
	if err != nil {
		return count, fmt.Errorf("CONTENT_TEXT_CHUNKS failed: %w", err)
	}
	return count, nil
}

func emptyContentTextChunksSource() *derivedJoinSource {
	return emptyDeclaredTableFunctionSource("content_text_chunks", contentTextChunksResultColumns())
}

func contentTextChunksFunction(table *sqlparser.AliasedTableExpr) (*sqlparser.FuncExpr, string, bool) {
	if table == nil {
		return nil, "", false
	}
	tableFunction, ok := table.Expr.(*sqlparser.TableFunction)
	if !ok || tableFunction.Function == nil || !strings.EqualFold(tableFunction.Function.Name.String(), "CONTENT_TEXT_CHUNKS") {
		return nil, "", false
	}
	alias := strings.TrimSpace(table.As.String())
	if alias == "" {
		alias = "content_text_chunks"
	}
	return tableFunction.Function, alias, true
}

func contentTextChunksFunctionInExpr(expression sqlparser.TableExpr) (*sqlparser.AliasedTableExpr, bool) {
	table, ok := expression.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, false
	}
	_, _, ok = contentTextChunksFunction(table)
	return table, ok
}

func lateralContentTextChunks(ctx context.Context, left *derivedJoinSource, table *sqlparser.AliasedTableExpr, dataDir string) (*derivedJoinSource, error) {
	function, alias, ok := contentTextChunksFunction(table)
	if !ok {
		return nil, fmt.Errorf("unsupported table-valued function")
	}
	if left == nil {
		return nil, fmt.Errorf("CONTENT_TEXT_CHUNKS requires a preceding row source")
	}
	if err := materializeDerivedSourceRows(left); err != nil {
		return nil, err
	}
	result := &derivedJoinSource{rows: make([]derivedRow, 0), columns: append([]string(nil), left.columns...)}
	chunkSource := emptyContentTextChunksSource()
	for _, column := range chunkSource.columns {
		result.columns = append(result.columns, column)
	}
	result.qualifiedCols = cloneDerivedQualifiedColumns(left.qualifiedCols)
	result.qualifiedCols[strings.ToLower(alias)] = append([]string(nil), chunkSource.columns...)
	result.visitRows = func(visit func(derivedRow) error) error {
		for _, row := range left.rows {
			row.values[storedFunctionDataDirKey] = dataDir
			_, err := visitContentTextChunksTable(ctx, function, row.values, func(chunk derivedRow) error {
				values := cloneDerivedValues(chunk.values)
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
