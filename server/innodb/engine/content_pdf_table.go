package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// TABLE(CONTENT_PARSE_PDF(content_column)) exposes one row per original page.
func visitContentPDFPagesTable(ctx context.Context, function *sqlparser.FuncExpr, values map[string]interface{}, visit func(derivedRow) error) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if function == nil || len(function.Exprs) != 1 {
		return 0, fmt.Errorf("CONTENT_PARSE_PDF requires exactly one stored CONTENT column")
	}
	argument, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return 0, fmt.Errorf("CONTENT_PARSE_PDF argument must be a CONTENT column")
	}
	sourceColumn, ok := argument.Expr.(*sqlparser.ColName)
	if !ok {
		return 0, fmt.Errorf("CONTENT_PARSE_PDF argument must be a CONTENT column")
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
		return 0, fmt.Errorf("CONTENT_PARSE_PDF requires a typed CONTENT value; got %T", raw)
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return 0, fmt.Errorf("CONTENT_PARSE_PDF requires stored-row context for the source column")
	}
	source, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, sourceValue)
	if err != nil {
		return 0, fmt.Errorf("CONTENT_PARSE_PDF source verification failed: %w", err)
	}
	store, err := contentSegmentStoreForValue(values, sourceValue)
	if err != nil {
		return 0, err
	}
	blocks, err := content.ParsePDFPages(ctx, store, sourceValue, source, content.PDFOptions{})
	if err != nil {
		return 0, fmt.Errorf("CONTENT_PARSE_PDF failed: %w", err)
	}
	for _, block := range blocks {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		encoded, err := content.BlockValue(block)
		if err != nil {
			return 0, fmt.Errorf("encode PDF page %d block: %w", block.Ordinal, err)
		}
		status := block.Attributes["extraction_status"]
		if err := visit(derivedRow{values: map[string]interface{}{
			"ordinal": int64(block.Ordinal), "page": int64(*block.Locator.Page), "text": block.Text,
			"block": encoded, "extraction_status": status,
		}}); err != nil {
			return 0, err
		}
	}
	return len(blocks), nil
}

func contentPDFPagesFunction(table *sqlparser.AliasedTableExpr) (*sqlparser.FuncExpr, string, bool) {
	if table == nil {
		return nil, "", false
	}
	tableFunction, ok := table.Expr.(*sqlparser.TableFunction)
	if !ok || tableFunction.Function == nil || !strings.EqualFold(tableFunction.Function.Name.String(), "CONTENT_PARSE_PDF") {
		return nil, "", false
	}
	alias := strings.TrimSpace(table.As.String())
	if alias == "" {
		alias = "content_parse_pdf"
	}
	return tableFunction.Function, alias, true
}

func contentPDFPagesFunctionInExpr(expression sqlparser.TableExpr) (*sqlparser.AliasedTableExpr, bool) {
	table, ok := expression.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, false
	}
	_, _, ok = contentPDFPagesFunction(table)
	return table, ok
}

func lateralContentPDFPages(ctx context.Context, left *derivedJoinSource, table *sqlparser.AliasedTableExpr, dataDir string) (*derivedJoinSource, error) {
	function, alias, ok := contentPDFPagesFunction(table)
	if !ok {
		return nil, fmt.Errorf("unsupported table-valued function")
	}
	if left == nil {
		return nil, fmt.Errorf("CONTENT_PARSE_PDF requires a preceding row source")
	}
	if err := materializeDerivedSourceRows(left); err != nil {
		return nil, err
	}
	result := &derivedJoinSource{rows: make([]derivedRow, 0), columns: append([]string(nil), left.columns...)}
	pageSource := emptyDeclaredTableFunctionSource("content_parse_pdf", contentPDFPagesResultColumns())
	for _, column := range pageSource.columns {
		result.columns = append(result.columns, column)
	}
	result.qualifiedCols = cloneDerivedQualifiedColumns(left.qualifiedCols)
	result.qualifiedCols[strings.ToLower(alias)] = append([]string(nil), pageSource.columns...)
	result.visitRows = func(visit func(derivedRow) error) error {
		for _, row := range left.rows {
			row.values[storedFunctionDataDirKey] = dataDir
			_, err := visitContentPDFPagesTable(ctx, function, row.values, func(page derivedRow) error {
				values := cloneDerivedValues(page.values)
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
