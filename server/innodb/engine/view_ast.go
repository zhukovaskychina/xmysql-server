package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// expandViewStatement returns a detached definition tree. Persistence and
// definer authorization stay in the existing executor path; this helper is
// the AST boundary used after those checks and never mutates shared metadata.
func (e *XMySQLExecutor) expandViewStatement(ctx *ExecutionContext, stmt sqlparser.Statement, databaseName string) (sqlparser.Statement, error) {
	view, ok := stmt.(*sqlparser.ViewStatement)
	if !ok || view == nil || view.Definition == nil {
		return nil, fmt.Errorf("view statement has no SELECT definition")
	}
	if ctx != nil && ctx.Context != nil {
		if err := ctx.Context.Err(); err != nil {
			return nil, err
		}
	}
	definition, err := rewriteStatementAST(view.Definition)
	if err != nil {
		return nil, err
	}
	if e != nil {
		schema := view.Name.Qualifier.String()
		if schema == "" {
			schema = databaseName
		}
		if _, depErr := e.collectViewSourceTables(schema, view.Name.Name.String(), sqlparser.String(definition)); depErr != nil {
			return nil, depErr
		}
	}
	return definition, nil
}

// rewriteViewQueryFromAST expands a top-level SELECT/UNION view reference by
// replacing the source table node with a detached subquery. The caller's
// aliases and tail clauses remain in the parsed tree, so literals and nested
// SQL text are never inspected as syntax.
func (e *XMySQLExecutor) rewriteViewQueryFromAST(query, databaseName string) (string, bool, error) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		statement, parseErr := sqlparser.Parse(query)
		if parseErr != nil {
			return "", false, nil
		}
		changed, rewriteErr := e.rewriteViewSelectStatement(statement, databaseName)
		if rewriteErr != nil || !changed {
			return "", changed, rewriteErr
		}
		return sqlparser.String(statement), true, nil
	}
	changed, err := e.rewriteViewSelectStatement(parsed.AST, databaseName)
	if err != nil || !changed {
		return "", changed, err
	}
	return sqlparser.String(parsed.AST), true, nil
}

func (e *XMySQLExecutor) rewriteViewQueryForExecution(query, databaseName string) (string, error) {
	if rewritten, changed, err := e.rewriteViewQueryFromAST(query, databaseName); err != nil {
		return "", err
	} else if changed {
		return rewritten, nil
	}
	return "", nil
}

func (e *XMySQLExecutor) rewriteViewQueryPreservingCTE(query, databaseName string) (string, bool, error) {
	schema, viewName, ok := viewQueryTargetFromAST(query, databaseName)
	if !ok {
		return "", false, nil
	}
	raw, err := os.ReadFile(filepath.Join(e.getDataDir(), schema, viewName+".view.json"))
	if os.IsNotExist(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	var info struct {
		Definition string `json:"definition"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return "", false, err
	}
	definition, err := sqlparser.Parse(strings.TrimSpace(strings.TrimSuffix(info.Definition, ";")))
	if err != nil {
		return "", false, err
	}
	if _, isWith := definition.(*sqlparser.With); !isWith {
		return "", false, nil
	}
	outer, err := sqlparser.Parse(query)
	if err != nil {
		return "", false, nil
	}
	selectStmt, ok := outer.(*sqlparser.Select)
	if !ok || len(selectStmt.From) != 1 {
		return "", false, nil
	}
	aliased, ok := selectStmt.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return "", false, nil
	}
	aliased.Expr = &sqlparser.Subquery{Select: definition.(sqlparser.SelectStatement)}
	if aliased.As.IsEmpty() {
		aliased.As = sqlparser.NewTableIdent(viewName)
	}
	return sqlparser.String(selectStmt), true, nil
}

func (e *XMySQLExecutor) rewriteViewSelectStatement(stmt sqlparser.Statement, databaseName string) (bool, error) {
	switch node := stmt.(type) {
	case *sqlparser.Select:
		return e.rewriteViewTableExprs(node.From, databaseName)
	case *sqlparser.Union:
		leftChanged, err := e.rewriteViewSelectStatement(node.Left, databaseName)
		if err != nil {
			return false, err
		}
		rightChanged, err := e.rewriteViewSelectStatement(node.Right, databaseName)
		return leftChanged || rightChanged, err
	case *sqlparser.With:
		return e.rewriteViewSelectStatement(node.Body, databaseName)
	default:
		return false, nil
	}
}

func (e *XMySQLExecutor) rewriteViewTableExprs(exprs sqlparser.TableExprs, databaseName string) (bool, error) {
	changed := false
	for _, expr := range exprs {
		oneChanged, err := e.rewriteViewTableExpr(expr, databaseName)
		if err != nil {
			return false, err
		}
		changed = changed || oneChanged
	}
	return changed, nil
}

func (e *XMySQLExecutor) rewriteViewTableExpr(expr sqlparser.TableExpr, databaseName string) (bool, error) {
	switch node := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		table, ok := node.Expr.(sqlparser.TableName)
		if !ok {
			return false, nil
		}
		schema := strings.TrimSpace(table.Qualifier.String())
		if schema == "" {
			schema = strings.TrimSpace(databaseName)
		}
		viewName := strings.TrimSpace(table.Name.String())
		if schema == "" || viewName == "" {
			return false, nil
		}
		definitionPath := filepath.Join(e.getDataDir(), schema, viewName+".view.json")
		raw, err := os.ReadFile(definitionPath)
		if os.IsNotExist(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		var info struct {
			Definition string   `json:"definition"`
			Columns    []string `json:"columns"`
		}
		if err := json.Unmarshal(raw, &info); err != nil {
			return false, err
		}
		definition := strings.TrimSpace(strings.TrimSuffix(info.Definition, ";"))
		if definition == "" {
			return false, fmt.Errorf("view %s.%s has an empty definition", schema, viewName)
		}
		definitionStmt, err := sqlparser.Parse(definition)
		if err != nil {
			return false, fmt.Errorf("parse view definition failed: %w", err)
		}
		selectStmt, ok := definitionStmt.(sqlparser.SelectStatement)
		if _, isWith := definitionStmt.(*sqlparser.With); isWith {
			ok = false
		}
		if !ok {
			// A derived table owns a SelectStatement, so expand a top-level CTE
			// definition through the existing AST-aware compatibility rewriter
			// before attaching it to the outer query.
			rewritten, rewriteErr := e.rewriteSimpleCTEQuery(definition)
			if rewriteErr != nil || strings.TrimSpace(rewritten) == "" {
				return false, rewriteErr
			}
			definition = rewritten
			definitionStmt, err = sqlparser.Parse(definition)
			if err != nil {
				return false, fmt.Errorf("reparse expanded view CTE failed: %w", err)
			}
			selectStmt, ok = definitionStmt.(sqlparser.SelectStatement)
			if !ok {
				return false, nil
			}
		}
		definition = bindViewDefinitionTables(definitionStmt, definition, schema)
		definitionStmt, err = sqlparser.Parse(definition)
		if err != nil {
			return false, fmt.Errorf("reparse bound view definition failed: %w", err)
		}
		selectStmt, ok = definitionStmt.(sqlparser.SelectStatement)
		if !ok {
			return false, nil
		}
		applyViewOutputColumnAliases(definitionStmt, info.Columns)
		node.Expr = &sqlparser.Subquery{Select: selectStmt}
		if node.As.IsEmpty() {
			node.As = sqlparser.NewTableIdent(viewName)
		}
		return true, nil
	case *sqlparser.ParenTableExpr:
		return e.rewriteViewTableExprs(node.Exprs, databaseName)
	case *sqlparser.JoinTableExpr:
		leftChanged, err := e.rewriteViewTableExpr(node.LeftExpr, databaseName)
		if err != nil {
			return false, err
		}
		rightChanged, err := e.rewriteViewTableExpr(node.RightExpr, databaseName)
		return leftChanged || rightChanged, err
	default:
		return false, nil
	}
}

func applyViewOutputColumnAliases(statement sqlparser.Statement, columns []string) {
	selectStmt, ok := statement.(*sqlparser.Select)
	if !ok || len(columns) == 0 {
		return
	}
	for index, name := range columns {
		if index >= len(selectStmt.SelectExprs) || strings.TrimSpace(name) == "" {
			break
		}
		if expression, ok := selectStmt.SelectExprs[index].(*sqlparser.AliasedExpr); ok {
			expression.As = sqlparser.NewColIdent(strings.Trim(name, "` "))
		}
	}
}

func viewQueryTargetFromAST(query, databaseName string) (schema, view string, ok bool) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		statement, parseErr := sqlparser.Parse(query)
		if parseErr != nil {
			return "", "", false
		}
		table, found := firstViewQueryTable(statement)
		if !found {
			return "", "", false
		}
		schema = strings.TrimSpace(table.Qualifier.String())
		if schema == "" {
			schema = strings.TrimSpace(databaseName)
		}
		view = strings.TrimSpace(table.Name.String())
		return schema, view, schema != "" && view != ""
	}
	table, found := firstViewQueryTable(parsed.AST)
	if !found {
		return "", "", false
	}
	schema = strings.TrimSpace(table.Qualifier.String())
	if schema == "" {
		schema = strings.TrimSpace(databaseName)
	}
	view = strings.TrimSpace(table.Name.String())
	return schema, view, schema != "" && view != ""
}

func firstViewQueryTable(stmt sqlparser.Statement) (sqlparser.TableName, bool) {
	switch node := stmt.(type) {
	case *sqlparser.Select:
		return firstViewQueryTableExpr(node.From)
	case *sqlparser.Union:
		return firstViewQueryTableSelect(node.Left)
	case *sqlparser.With:
		return firstViewQueryTable(node.Body)
	default:
		return sqlparser.TableName{}, false
	}
}

func firstViewQueryTableSelect(stmt sqlparser.SelectStatement) (sqlparser.TableName, bool) {
	switch node := stmt.(type) {
	case *sqlparser.Select:
		return firstViewQueryTableExpr(node.From)
	case *sqlparser.Union:
		return firstViewQueryTableSelect(node.Left)
	case *sqlparser.ParenSelect:
		return firstViewQueryTableSelect(node.Select)
	default:
		return sqlparser.TableName{}, false
	}
}

func firstViewQueryTableExpr(exprs sqlparser.TableExprs) (sqlparser.TableName, bool) {
	for _, expr := range exprs {
		switch node := expr.(type) {
		case *sqlparser.AliasedTableExpr:
			if table, ok := node.Expr.(sqlparser.TableName); ok {
				return table, true
			}
		case *sqlparser.ParenTableExpr:
			if table, ok := firstViewQueryTableExpr(node.Exprs); ok {
				return table, true
			}
		case *sqlparser.JoinTableExpr:
			if table, ok := firstViewQueryTableExpr(sqlparser.TableExprs{node.LeftExpr}); ok {
				return table, true
			}
			if table, ok := firstViewQueryTableExpr(sqlparser.TableExprs{node.RightExpr}); ok {
				return table, true
			}
		}
	}
	return sqlparser.TableName{}, false
}
