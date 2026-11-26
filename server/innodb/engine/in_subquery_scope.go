package engine

import (
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// independentINColumns conservatively binds a materialized subquery to its
// own input columns. Unsupported correlation must remain an explicit boundary;
// an unresolved qualifier must never fall back to an inner column of the same
// name and turn an outer reference into a tautology.
func (e *XMySQLExecutor) independentINColumns(statement sqlparser.SelectStatement, database string) (map[string]bool, bool) {
	switch node := statement.(type) {
	case *sqlparser.ParenSelect:
		return e.independentINColumns(node.Select, database)
	case *sqlparser.Union:
		columns, leftOK := e.independentINColumns(node.Left, database)
		_, rightOK := e.independentINColumns(node.Right, database)
		return columns, leftOK && rightOK
	case *sqlparser.Select:
		local := make(map[string]bool)
		qualified := make(map[string]map[string]bool)
		joinConditions := make([]sqlparser.Expr, 0)
		var bindSource func(sqlparser.TableExpr) bool
		bindSource = func(source sqlparser.TableExpr) bool {
			switch table := source.(type) {
			case *sqlparser.AliasedTableExpr:
				columns := make(map[string]bool)
				alias := table.As.String()
				switch input := table.Expr.(type) {
				case sqlparser.TableName:
					if alias == "" {
						alias = input.Name.String()
					}
					schema := database
					if !input.Qualifier.IsEmpty() {
						schema = input.Qualifier.String()
					}
					if table.As.IsEmpty() {
						qualified[strings.ToLower(schema+"."+input.Name.String())] = columns
					}
					if !strings.EqualFold(input.Name.String(), "dual") {
						if strings.EqualFold(schema, "information_schema") {
							known, ok := informationSchemaTableRegistry[strings.ToLower(input.Name.String())]
							if !ok {
								return false
							}
							for _, name := range known {
								columns[strings.ToLower(name)] = true
							}
						} else {
							meta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), schema, input.Name.String())
							if err != nil {
								return false
							}
							for _, column := range meta.Columns {
								if column != nil {
									columns[strings.ToLower(column.Name)] = true
								}
							}
						}
					}
				case *sqlparser.Subquery:
					var ok bool
					columns, ok = e.independentINColumns(input.Select, database)
					if !ok {
						return false
					}
				default:
					return false
				}
				qualified[strings.ToLower(alias)] = columns
				for name := range columns {
					local[name] = true
				}
				return true
			case *sqlparser.JoinTableExpr:
				if !bindSource(table.LeftExpr) || !bindSource(table.RightExpr) {
					return false
				}
				if table.Condition.On != nil {
					joinConditions = append(joinConditions, table.Condition.On)
				}
				return true
			case *sqlparser.ParenTableExpr:
				for _, part := range table.Exprs {
					if !bindSource(part) {
						return false
					}
				}
				return true
			default:
				return false
			}
		}
		for _, source := range node.From {
			if !bindSource(source) {
				return nil, false
			}
		}
		output := make(map[string]bool)
		for _, expression := range node.SelectExprs {
			switch projection := expression.(type) {
			case *sqlparser.AliasedExpr:
				name := projection.As.String()
				if name == "" {
					if column, ok := projection.Expr.(*sqlparser.ColName); ok {
						name = column.Name.String()
					} else {
						name = sqlparser.String(projection.Expr)
					}
				}
				output[strings.ToLower(name)] = true
			case *sqlparser.StarExpr:
				columns := local
				if !projection.TableName.IsEmpty() {
					columns = qualified[strings.ToLower(projection.TableName.Name.String())]
				}
				for name := range columns {
					output[name] = true
				}
			}
		}
		check := func(root sqlparser.SQLNode, aliases bool) bool {
			safe := true
			_ = sqlparser.Walk(func(part sqlparser.SQLNode) (bool, error) {
				switch value := part.(type) {
				case *sqlparser.Subquery:
					_, ok := e.independentINColumns(value.Select, database)
					safe = safe && ok
					return false, nil
				case *sqlparser.ColName:
					name := strings.ToLower(value.Name.String())
					if strings.HasPrefix(name, "@") {
						return false, nil
					}
					if !value.Qualifier.IsEmpty() {
						qualifier := value.Qualifier.Name.String()
						if !value.Qualifier.Qualifier.IsEmpty() {
							qualifier = value.Qualifier.Qualifier.String() + "." + qualifier
						}
						safe = safe && qualified[strings.ToLower(qualifier)][name]
					} else {
						safe = safe && (local[name] || (aliases && output[name]))
					}
				}
				return true, nil
			}, root)
			return safe
		}
		if !check(node.SelectExprs, false) {
			return nil, false
		}
		if node.Where != nil && !check(node.Where, false) {
			return nil, false
		}
		for _, condition := range joinConditions {
			if !check(condition, false) {
				return nil, false
			}
		}
		if !check(node.GroupBy, true) || !check(node.Having, true) || !check(node.OrderBy, true) {
			return nil, false
		}
		return output, true
	default:
		return nil, false
	}
}
