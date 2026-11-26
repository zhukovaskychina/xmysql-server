package engine

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

const maxFullTextTokenRows = 262144

// FULLTEXT_TOKENS(text) exposes the database's exact lexical tokens as rows.
// With a CONTENT column it is a lateral source and retains the source row's
// context; with a scalar literal it can be the first FROM source.
func evaluateFullTextTokensTable(function *sqlparser.FuncExpr, values map[string]interface{}) (*derivedJoinSource, error) {
	if function == nil || len(function.Exprs) != 1 {
		return nil, fmt.Errorf("FULLTEXT_TOKENS requires exactly one text or textual CONTENT argument")
	}
	argument, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("FULLTEXT_TOKENS argument must be a scalar expression")
	}
	value, err := evaluateExpressionWithRow(argument.Expr, values)
	if err != nil {
		return nil, err
	}
	result := emptyFullTextTokensSource()
	if value == nil {
		return result, nil
	}
	text, err := fullTextStringArgument(value, "input", values)
	if err != nil {
		return nil, err
	}
	if !utf8.ValidString(text) {
		return nil, fmt.Errorf("FULLTEXT_TOKENS input must be valid UTF-8")
	}
	if uint64(len(text)) > content.MaxInlineTextBytes {
		return nil, fmt.Errorf("FULLTEXT_TOKENS input exceeds the %d byte limit", content.MaxInlineTextBytes)
	}
	tokens := tokenizeFullText(text, defaultFullTextStopwords)
	if len(tokens) > maxFullTextTokenRows {
		return nil, fmt.Errorf("FULLTEXT_TOKENS produced %d tokens; maximum is %d", len(tokens), maxFullTextTokenRows)
	}
	for index, token := range tokens {
		result.rows = append(result.rows, derivedRow{values: map[string]interface{}{
			"token": token, "position": int64(index),
		}})
	}
	return result, nil
}

func emptyFullTextTokensSource() *derivedJoinSource {
	return emptyDeclaredTableFunctionSource("fulltext_tokens", fullTextTokensResultColumns())
}

func fullTextTokensFunction(table *sqlparser.AliasedTableExpr) (*sqlparser.FuncExpr, string, bool) {
	if table == nil {
		return nil, "", false
	}
	tableFunction, ok := table.Expr.(*sqlparser.TableFunction)
	if !ok || tableFunction.Function == nil || !strings.EqualFold(tableFunction.Function.Name.String(), "FULLTEXT_TOKENS") {
		return nil, "", false
	}
	alias := strings.TrimSpace(table.As.String())
	if alias == "" {
		alias = "fulltext_tokens"
	}
	return tableFunction.Function, alias, true
}

func fullTextTokensFunctionInExpr(expression sqlparser.TableExpr) (*sqlparser.AliasedTableExpr, bool) {
	table, ok := expression.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, false
	}
	_, _, ok = fullTextTokensFunction(table)
	return table, ok
}

func lateralFullTextTokens(left *derivedJoinSource, table *sqlparser.AliasedTableExpr, dataDir string) (*derivedJoinSource, error) {
	function, alias, ok := fullTextTokensFunction(table)
	if !ok {
		return nil, fmt.Errorf("unsupported table-valued function")
	}
	if left == nil {
		left = &derivedJoinSource{rows: []derivedRow{{values: map[string]interface{}{}}}, qualifiedCols: map[string][]string{}}
	}
	result := &derivedJoinSource{rows: make([]derivedRow, 0), columns: append([]string(nil), left.columns...)}
	tokenSource := emptyFullTextTokensSource()
	result.columns = append(result.columns, tokenSource.columns...)
	result.qualifiedCols = cloneDerivedQualifiedColumns(left.qualifiedCols)
	result.qualifiedCols[strings.ToLower(alias)] = append([]string(nil), tokenSource.columns...)
	for _, row := range left.rows {
		row.values[storedFunctionDataDirKey] = dataDir
		tokens, err := evaluateFullTextTokensTable(function, row.values)
		if err != nil {
			return nil, err
		}
		for _, token := range tokens.rows {
			values := cloneDerivedValues(token.values)
			for key, value := range token.values {
				values[alias+"."+key] = value
				values[strings.ToLower(alias+"."+key)] = value
			}
			result.rows = append(result.rows, mergeDerivedRows(row, derivedRow{values: values}))
		}
	}
	return result, nil
}
