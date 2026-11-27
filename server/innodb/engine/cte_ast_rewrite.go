package engine

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// rewriteSimpleCTEFromAST expands a non-recursive WITH statement by substituting
// CTE table references with the parsed definition. Recursive WITH is left
// untouched so the recursive executor keeps the original statement.
func rewriteSimpleCTEFromAST(query string) (string, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	if !withStatementPrefix(trimmed) {
		return "", nil
	}
	stmt, err := sqlparser.Parse(trimmed)
	if err != nil {
		return "", err
	}
	with, ok := stmt.(*sqlparser.With)
	if !ok || with == nil {
		return "", fmt.Errorf("unsupported CTE syntax")
	}
	if with.Recursive {
		return "", nil
	}
	if len(with.CTEs) == 0 || with.Body == nil {
		return "", fmt.Errorf("CTE query has no main SELECT")
	}
	definitions := make(map[string]sqlparser.Statement, len(with.CTEs))
	for _, cte := range with.CTEs {
		if cte == nil || cte.Query == nil || strings.TrimSpace(cte.Name.String()) == "" {
			return "", fmt.Errorf("unsupported CTE syntax")
		}
		name := strings.ToLower(cte.Name.String())
		if _, exists := definitions[name]; exists {
			return "", fmt.Errorf("duplicate CTE name '%s'", cte.Name.String())
		}
		definition := cte.Query
		if len(cte.Columns) > 0 {
			columns := make([]string, 0, len(cte.Columns))
			for _, column := range cte.Columns {
				columns = append(columns, column.String())
			}
			aliased := applyCTEColumnAliases(sqlparser.String(definition), columns)
			parsed, parseErr := sqlparser.Parse(aliased)
			if parseErr != nil {
				return "", parseErr
			}
			definition = parsed
		}
		definitions[name] = definition
	}
	changed, err := substituteCTETables(with.Body, definitions, "", map[string]bool{})
	if err != nil {
		return "", err
	}
	if !changed {
		return "", nil
	}
	return sqlparser.String(with.Body), nil
}

func withStatementPrefix(query string) bool {
	lower := strings.ToLower(query)
	if !strings.HasPrefix(lower, "with") {
		return false
	}
	if len(lower) == len("with") {
		return true
	}
	switch lower[len("with")] {
	case ' ', '\t', '\r', '\n':
		return true
	default:
		return false
	}
}

func substituteCTETables(stmt sqlparser.SQLNode, definitions map[string]sqlparser.Statement, current string, visiting map[string]bool) (bool, error) {
	changed := false
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		aliased, ok := node.(*sqlparser.AliasedTableExpr)
		if !ok || aliased == nil {
			return true, nil
		}
		table, ok := aliased.Expr.(sqlparser.TableName)
		if !ok || table.Name.String() == "" || table.Qualifier.String() != "" {
			return true, nil
		}
		name := strings.ToLower(table.Name.String())
		if strings.EqualFold(name, current) || visiting[name] {
			return true, nil
		}
		definition, ok := definitions[name]
		if !ok {
			return true, nil
		}
		visiting[name] = true
		if _, err := substituteCTETables(definition, definitions, name, visiting); err != nil {
			delete(visiting, name)
			return false, err
		}
		delete(visiting, name)
		cloned, err := sqlparser.Parse(sqlparser.String(definition))
		if err != nil {
			return false, err
		}
		selectStmt, ok := cloned.(sqlparser.SelectStatement)
		if !ok {
			return true, nil
		}
		alias := aliased.As
		if alias.IsEmpty() {
			alias = sqlparser.NewTableIdent(table.Name.String())
		}
		aliased.Expr = &sqlparser.Subquery{Select: selectStmt}
		aliased.As = alias
		changed = true
		return false, nil
	}, stmt)
	return changed, err
}
