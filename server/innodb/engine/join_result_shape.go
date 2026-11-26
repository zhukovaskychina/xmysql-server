package engine

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type joinOutputColumn struct {
	name string
	expr sqlparser.Expr
	typ  metadata.DataType
	meta *metadata.ColumnMeta
}

type joinResultShape struct {
	columns   []joinOutputColumn
	qualified map[string][]joinOutputColumn
}

// Resolve the output shape from metadata, including for empty inputs. USING
// and NATURAL collapse only the unqualified projection; t.* retains the
// physical columns of that table, including its own nullable join key.
func (se *SelectExecutor) expandJoinSelectExpressions(stmt *sqlparser.Select) (sqlparser.SelectExprs, error) {
	shape := joinResultShape{qualified: make(map[string][]joinOutputColumn)}
	for _, from := range stmt.From {
		part, err := se.resolveJoinResultShape(from)
		if err != nil {
			return nil, err
		}
		shape.columns = append(shape.columns, part.columns...)
		for alias, columns := range part.qualified {
			shape.qualified[alias] = columns
		}
	}
	if se.joinOrderTypes == nil {
		se.joinOrderTypes = make(map[string]metadata.DataType)
	}
	for _, column := range shape.columns {
		se.joinOrderTypes[strings.ToLower(column.name)] = column.typ
	}
	expanded := make(sqlparser.SelectExprs, 0, len(stmt.SelectExprs))
	for _, expression := range stmt.SelectExprs {
		star, ok := expression.(*sqlparser.StarExpr)
		if !ok {
			expanded = append(expanded, expression)
			continue
		}
		columns := shape.columns
		if !star.TableName.IsEmpty() {
			var found bool
			columns, found = shape.qualified[strings.ToLower(star.TableName.Name.String())]
			if !found {
				return nil, fmt.Errorf("unknown table %s in star projection", sqlparser.String(star.TableName))
			}
		}
		for _, column := range columns {
			expanded = append(expanded, &sqlparser.AliasedExpr{Expr: column.expr, As: sqlparser.NewColIdent(column.name)})
		}
	}
	return expanded, nil
}

func (se *SelectExecutor) resolveJoinResultShape(expression sqlparser.TableExpr) (joinResultShape, error) {
	switch node := expression.(type) {
	case *sqlparser.AliasedTableExpr:
		table, ok := node.Expr.(sqlparser.TableName)
		if !ok {
			return joinResultShape{}, fmt.Errorf("unsupported JOIN source %T", node.Expr)
		}
		schema := se.schemaName
		if !table.Qualifier.IsEmpty() {
			schema = table.Qualifier.String()
		}
		meta, err := se.loadTableMetaFromFrm(se.dataDir, schema, table.Name.String())
		if err != nil {
			return joinResultShape{}, err
		}
		alias := node.As
		if alias.IsEmpty() {
			alias = table.Name
		}
		shape := joinResultShape{qualified: make(map[string][]joinOutputColumn)}
		for _, column := range meta.Columns {
			if column == nil {
				continue
			}
			copyColumn := *column
			copyColumn.EnumValues = append([]string(nil), column.EnumValues...)
			shape.columns = append(shape.columns, joinOutputColumn{
				name: column.Name, expr: &sqlparser.ColName{Name: sqlparser.NewColIdent(column.Name), Qualifier: sqlparser.TableName{Name: alias}},
				typ: column.Type, meta: &copyColumn,
			})
		}
		shape.qualified[strings.ToLower(alias.String())] = shape.columns
		return shape, nil
	case *sqlparser.JoinTableExpr:
		left, err := se.resolveJoinResultShape(node.LeftExpr)
		if err != nil {
			return joinResultShape{}, err
		}
		right, err := se.resolveJoinResultShape(node.RightExpr)
		if err != nil {
			return joinResultShape{}, err
		}
		return mergeJoinResultShapes(node, left, right), nil
	default:
		return joinResultShape{}, fmt.Errorf("unsupported JOIN source %T", expression)
	}
}

// Shared by execution and schema-only PREPARE description, so NATURAL/USING
// column collapse and RIGHT JOIN ordering cannot drift between the two paths.
func mergeJoinResultShapes(node *sqlparser.JoinTableExpr, left, right joinResultShape) joinResultShape {
	common := make(map[string]bool)
	for _, column := range node.Condition.Using {
		common[strings.ToLower(column.String())] = true
	}
	if strings.HasPrefix(strings.ToLower(node.Join), "natural") {
		for _, l := range left.columns {
			for _, r := range right.columns {
				if strings.EqualFold(l.name, r.name) {
					common[strings.ToLower(l.name)] = true
				}
			}
		}
		node.Condition.Using = nil
		for _, column := range left.columns {
			if common[strings.ToLower(column.name)] {
				node.Condition.Using = append(node.Condition.Using, sqlparser.NewColIdent(column.name))
			}
		}
	}
	result := joinResultShape{qualified: make(map[string][]joinOutputColumn)}
	for alias, columns := range left.qualified {
		result.qualified[alias] = columns
	}
	for alias, columns := range right.qualified {
		result.qualified[alias] = columns
	}
	first, second := left.columns, right.columns
	if strings.Contains(strings.ToLower(node.Join), "right") && (len(common) > 0 || strings.HasPrefix(strings.ToLower(node.Join), "natural")) {
		first, second = second, first
	}
	for _, column := range first {
		if common[strings.ToLower(column.name)] {
			column.expr = &sqlparser.ColName{Name: sqlparser.NewColIdent(column.name)}
			result.columns = append(result.columns, column)
		}
	}
	for _, side := range [][]joinOutputColumn{first, second} {
		for _, column := range side {
			if !common[strings.ToLower(column.name)] {
				result.columns = append(result.columns, column)
			}
		}
	}
	return result
}
