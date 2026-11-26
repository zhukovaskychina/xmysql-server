package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func visitContentXLSXRowsTable(ctx context.Context, parser content.XLSXRowParser, function *sqlparser.FuncExpr, values map[string]interface{}, visit func(derivedRow) error) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if function == nil || len(function.Exprs) != 1 {
		return 0, fmt.Errorf("CONTENT_PARSE_XLSX requires exactly one stored CONTENT column")
	}
	argument, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return 0, fmt.Errorf("CONTENT_PARSE_XLSX argument must be a CONTENT column")
	}
	sourceColumn, ok := argument.Expr.(*sqlparser.ColName)
	if !ok {
		return 0, fmt.Errorf("CONTENT_PARSE_XLSX argument must be a CONTENT column")
	}
	raw, err := evaluateExpressionWithRow(sourceColumn, values)
	if err != nil {
		return 0, err
	}
	if raw == nil {
		return 0, nil
	}
	var sourceValue content.Value
	switch value := raw.(type) {
	case content.Value:
		sourceValue = value
	case *content.Value:
		if value == nil {
			return 0, nil
		}
		sourceValue = *value
	default:
		return 0, fmt.Errorf("CONTENT_PARSE_XLSX requires a typed CONTENT value; got %T", raw)
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return 0, fmt.Errorf("CONTENT_PARSE_XLSX requires stored-row context for the source column")
	}
	source, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, sourceValue)
	if err != nil {
		return 0, fmt.Errorf("CONTENT_PARSE_XLSX source verification failed: %w", err)
	}
	store, err := contentSegmentStoreForValue(values, sourceValue)
	if err != nil {
		return 0, err
	}
	if parser == nil {
		return 0, fmt.Errorf("CONTENT_PARSE_XLSX worker is unavailable")
	}
	blocks, err := parser.ParseXLSXRows(ctx, store, sourceValue, source, content.XLSXOptions{})
	if err != nil {
		return 0, fmt.Errorf("CONTENT_PARSE_XLSX failed: %w", err)
	}
	for _, block := range blocks {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		encoded, err := content.BlockValue(block)
		if err != nil {
			return 0, fmt.Errorf("encode XLSX row %d block: %w", block.Ordinal, err)
		}
		rowNumber, err := strconv.ParseInt(block.Attributes["row_number"], 10, 64)
		if err != nil || rowNumber < 1 {
			return 0, fmt.Errorf("XLSX row block %d has invalid row number", block.Ordinal)
		}
		if err := visit(derivedRow{values: map[string]interface{}{
			"ordinal": int64(block.Ordinal), "sheet_name": block.Attributes["sheet_name"], "row_number": rowNumber,
			"text": block.Text, "block": encoded, "structure_path": block.Attributes["structure_path"],
		}}); err != nil {
			return 0, err
		}
	}
	return len(blocks), nil
}

func contentXLSXRowsFunction(table *sqlparser.AliasedTableExpr) (*sqlparser.FuncExpr, string, bool) {
	if table == nil {
		return nil, "", false
	}
	tableFunction, ok := table.Expr.(*sqlparser.TableFunction)
	if !ok || tableFunction.Function == nil || !strings.EqualFold(tableFunction.Function.Name.String(), "CONTENT_PARSE_XLSX") {
		return nil, "", false
	}
	alias := strings.TrimSpace(table.As.String())
	if alias == "" {
		alias = "content_parse_xlsx"
	}
	return tableFunction.Function, alias, true
}

func contentXLSXRowsFunctionInExpr(expression sqlparser.TableExpr) (*sqlparser.AliasedTableExpr, bool) {
	table, ok := expression.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, false
	}
	_, _, ok = contentXLSXRowsFunction(table)
	return table, ok
}

func lateralContentXLSXRows(ctx context.Context, left *derivedJoinSource, table *sqlparser.AliasedTableExpr, executor *XMySQLExecutor) (*derivedJoinSource, error) {
	function, alias, ok := contentXLSXRowsFunction(table)
	if !ok {
		return nil, fmt.Errorf("unsupported table-valued function")
	}
	if left == nil {
		return nil, fmt.Errorf("CONTENT_PARSE_XLSX requires a preceding row source")
	}
	if err := materializeDerivedSourceRows(left); err != nil {
		return nil, err
	}
	result := &derivedJoinSource{rows: make([]derivedRow, 0), columns: append([]string(nil), left.columns...)}
	rowSource := emptyDeclaredTableFunctionSource("content_parse_xlsx", contentXLSXRowsResultColumns())
	for _, column := range rowSource.columns {
		result.columns = append(result.columns, column)
	}
	result.qualifiedCols = cloneDerivedQualifiedColumns(left.qualifiedCols)
	result.qualifiedCols[strings.ToLower(alias)] = append([]string(nil), rowSource.columns...)
	result.visitRows = func(visit func(derivedRow) error) error {
		for _, row := range left.rows {
			if executor != nil {
				row.values[storedFunctionDataDirKey] = executor.getDataDir()
			}
			_, err := visitContentXLSXRowsTable(ctx, executor.contentXLSXRowParserSnapshot(), function, row.values, func(item derivedRow) error {
				values := cloneDerivedValues(item.values)
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
