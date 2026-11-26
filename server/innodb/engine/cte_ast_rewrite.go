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
			if err := setCTEColumnAliases(definition, cte.Columns); err != nil {
				return "", err
			}
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

func setCTEColumnAliases(statement sqlparser.Statement, columns sqlparser.Columns) error {
	switch node := statement.(type) {
	case *sqlparser.ParenSelect:
		return setCTEColumnAliases(node.Select, columns)
	case *sqlparser.Union:
		// A set expression takes its output names from its first operand.
		return setCTEColumnAliases(node.Left, columns)
	case *sqlparser.Select:
		if len(node.SelectExprs) != len(columns) {
			return fmt.Errorf("CTE column list does not match query column count")
		}
		for index, expression := range node.SelectExprs {
			aliased, ok := expression.(*sqlparser.AliasedExpr)
			if !ok {
				return fmt.Errorf("CTE column aliases require an explicit projection")
			}
			aliased.As = sqlparser.NewColIdent(columns[index].String())
		}
		return nil
	default:
		return fmt.Errorf("CTE column aliases require a SELECT query")
	}
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
