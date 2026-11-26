package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func visitContentDOCXBlocksTable(ctx context.Context, parser content.DOCXBlockParser, function *sqlparser.FuncExpr, values map[string]interface{}, visit func(derivedRow) error) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if function == nil || len(function.Exprs) != 1 {
		return 0, fmt.Errorf("CONTENT_PARSE_DOCX requires exactly one stored CONTENT column")
	}
	argument, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return 0, fmt.Errorf("CONTENT_PARSE_DOCX argument must be a CONTENT column")
	}
	sourceColumn, ok := argument.Expr.(*sqlparser.ColName)
	if !ok {
		return 0, fmt.Errorf("CONTENT_PARSE_DOCX argument must be a CONTENT column")
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
		return 0, fmt.Errorf("CONTENT_PARSE_DOCX requires a typed CONTENT value; got %T", raw)
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return 0, fmt.Errorf("CONTENT_PARSE_DOCX requires stored-row context for the source column")
	}
	source, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, sourceValue)
	if err != nil {
		return 0, fmt.Errorf("CONTENT_PARSE_DOCX source verification failed: %w", err)
	}
	store, err := contentSegmentStoreForValue(values, sourceValue)
	if err != nil {
		return 0, err
	}
	if parser == nil {
		return 0, fmt.Errorf("CONTENT_PARSE_DOCX worker is unavailable")
	}
	blocks, err := parser.ParseDOCXBlocks(ctx, store, sourceValue, source, content.DOCXOptions{})
	if err != nil {
		return 0, fmt.Errorf("CONTENT_PARSE_DOCX failed: %w", err)
	}
	for _, block := range blocks {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		encoded, err := content.BlockValue(block)
		if err != nil {
			return 0, fmt.Errorf("encode DOCX block %d: %w", block.Ordinal, err)
		}
		if err := visit(derivedRow{values: map[string]interface{}{
			"ordinal": int64(block.Ordinal), "text": block.Text, "block": encoded,
			"structure_path": block.Attributes["structure_path"],
		}}); err != nil {
			return 0, err
		}
	}
	return len(blocks), nil
}

func contentDOCXBlocksFunction(table *sqlparser.AliasedTableExpr) (*sqlparser.FuncExpr, string, bool) {
	if table == nil {
		return nil, "", false
	}
	tableFunction, ok := table.Expr.(*sqlparser.TableFunction)
	if !ok || tableFunction.Function == nil || !strings.EqualFold(tableFunction.Function.Name.String(), "CONTENT_PARSE_DOCX") {
		return nil, "", false
	}
	alias := strings.TrimSpace(table.As.String())
	if alias == "" {
		alias = "content_parse_docx"
	}
	return tableFunction.Function, alias, true
}

func contentDOCXBlocksFunctionInExpr(expression sqlparser.TableExpr) (*sqlparser.AliasedTableExpr, bool) {
	table, ok := expression.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, false
	}
	_, _, ok = contentDOCXBlocksFunction(table)
	return table, ok
}

func lateralContentDOCXBlocks(ctx context.Context, left *derivedJoinSource, table *sqlparser.AliasedTableExpr, executor *XMySQLExecutor) (*derivedJoinSource, error) {
	function, alias, ok := contentDOCXBlocksFunction(table)
	if !ok {
		return nil, fmt.Errorf("unsupported table-valued function")
	}
	if left == nil {
		return nil, fmt.Errorf("CONTENT_PARSE_DOCX requires a preceding row source")
	}
	if err := materializeDerivedSourceRows(left); err != nil {
		return nil, err
	}
	result := &derivedJoinSource{rows: make([]derivedRow, 0), columns: append([]string(nil), left.columns...)}
	blockSource := emptyDeclaredTableFunctionSource("content_parse_docx", contentDOCXBlocksResultColumns())
	for _, column := range blockSource.columns {
		result.columns = append(result.columns, column)
	}
	result.qualifiedCols = cloneDerivedQualifiedColumns(left.qualifiedCols)
	result.qualifiedCols[strings.ToLower(alias)] = append([]string(nil), blockSource.columns...)
	result.visitRows = func(visit func(derivedRow) error) error {
		for _, row := range left.rows {
			if executor != nil {
				row.values[storedFunctionDataDirKey] = executor.getDataDir()
			}
			_, err := visitContentDOCXBlocksTable(ctx, executor.contentDOCXBlockParserSnapshot(), function, row.values, func(block derivedRow) error {
				values := cloneDerivedValues(block.values)
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
