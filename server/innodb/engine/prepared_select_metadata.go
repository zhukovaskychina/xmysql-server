package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// DescribePreparedSelect reads SQL and catalog definitions only. In particular,
// it must not evaluate expressions, read table rows, invoke models, or execute
// stored functions to discover their types. An unsupported shape/type is an
// error, not permission to fall back to running the query during PREPARE.
// Execution still resolves its own metadata after parameters are bound.
func (e *XMySQLExecutor) DescribePreparedSelect(session server.MySQLServerSession, query, database string) (*SelectResult, error) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return nil, err
	}
	d := preparedSelectDescriber{executor: e, ctx: &ExecutionContext{Context: context.Background(), Session: session}, database: database}
	columns, err := d.describe(parsed.AST, nil, 0)
	if err != nil {
		return nil, err
	}
	return emptySelectResultFromShape(columns), nil
}

func emptySelectResultFromShape(columns []joinOutputColumn) *SelectResult {
	result := &SelectResult{ResultType: common.RESULT_TYPE_QUERY, Records: make([]Record, 0)}
	for _, column := range columns {
		result.Columns = append(result.Columns, column.name)
		result.ColumnTypes = append(result.ColumnTypes, strings.ToLower(string(column.typ)))
	}
	return result
}

type preparedSelectDescriber struct {
	executor *XMySQLExecutor
	ctx      *ExecutionContext
	database string
}

func (d *preparedSelectDescriber) describe(statement sqlparser.Statement, ctes map[string][]joinOutputColumn, depth int) ([]joinOutputColumn, error) {
	if depth > 64 {
		return nil, fmt.Errorf("prepared SELECT metadata nesting exceeds 64")
	}
	switch node := statement.(type) {
	case *sqlparser.With:
		local := make(map[string][]joinOutputColumn, len(ctes)+len(node.CTEs))
		for name, columns := range ctes {
			local[name] = columns
		}
		for _, cte := range node.CTEs {
			// A recursive CTE gets its schema from the anchor, as execution does.
			anchor := cte.Query
			if union, ok := anchor.(*sqlparser.Union); ok && node.Recursive {
				anchor = union.Left
			}
			columns, err := d.describe(anchor, local, depth+1)
			if err != nil {
				return nil, err
			}
			if len(cte.Columns) > 0 {
				if len(cte.Columns) != len(columns) {
					return nil, fmt.Errorf("CTE column count does not match its projection")
				}
				for index, name := range cte.Columns {
					columns[index].name = name.String()
				}
			}
			local[strings.ToLower(cte.Name.String())] = columns
		}
		return d.describe(node.Body, local, depth+1)
	case *sqlparser.ParenSelect:
		return d.describe(node.Select, ctes, depth+1)
	case *sqlparser.Union:
		left, err := d.describe(node.Left, ctes, depth+1)
		if err != nil {
			return nil, err
		}
		right, err := d.describe(node.Right, ctes, depth+1)
		if err != nil {
			return nil, err
		}
		if len(left) != len(right) {
			return nil, fmt.Errorf("set-operation column counts differ")
		}
		for index := range left {
			if !strings.EqualFold(string(left[index].typ), string(right[index].typ)) {
				return nil, fmt.Errorf("set-operation metadata requires execution-time type reconciliation")
			}
		}
		return left, nil
	case *sqlparser.Select:
		if err := d.executor.checkSelectColumnPrivileges(d.ctx, node, d.database); err != nil {
			return nil, err
		}
		shape := joinResultShape{qualified: make(map[string][]joinOutputColumn)}
		for _, from := range node.From {
			part, err := d.source(from, ctes, depth+1)
			if err != nil {
				return nil, err
			}
			shape.columns = append(shape.columns, part.columns...)
			for alias, columns := range part.qualified {
				shape.qualified[alias] = columns
			}
		}
		return d.projection(node, shape, ctes, depth)
	default:
		return nil, fmt.Errorf("not a SELECT metadata statement: %T", statement)
	}
}

func (d *preparedSelectDescriber) projection(node *sqlparser.Select, shape joinResultShape, ctes map[string][]joinOutputColumn, depth int) ([]joinOutputColumn, error) {
	var output []joinOutputColumn
	for _, expression := range node.SelectExprs {
		switch expr := expression.(type) {
		case *sqlparser.StarExpr:
			columns := shape.columns
			if !expr.TableName.IsEmpty() {
				var exists bool
				columns, exists = shape.qualified[strings.ToLower(expr.TableName.Name.String())]
				if !exists {
					return nil, fmt.Errorf("unknown star qualifier %s", sqlparser.String(expr.TableName))
				}
			}
			output = append(output, columns...)
		case *sqlparser.AliasedExpr:
			typ, err := d.expressionType(expr.Expr, shape, ctes, depth+1)
			if err != nil {
				return nil, err
			}
			typ = mysqlSortedJoinMaterializationType(node, expr.Expr, typ)
			name := sqlparser.String(expr.Expr)
			if column, ok := expr.Expr.(*sqlparser.ColName); ok {
				name = column.Name.String()
			}
			if !expr.As.IsEmpty() {
				name = expr.As.String()
			}
			output = append(output, joinOutputColumn{name: name, typ: typ})
		default:
			return nil, fmt.Errorf("unsupported prepared projection %T", expression)
		}
	}
	return output, nil
}

// MySQL 8.4 reports boolean projections from JOIN queries with ORDER BY as
// INT. Unsorted JOINs and single-source predicates retain BIGINT in the
// current compatibility contract. Keep the rule AST-based so PREPARE and
// execution describe the same query shape.
func mysqlSortedJoinMaterializationType(stmt *sqlparser.Select, expression sqlparser.Expr, declared metadata.DataType) metadata.DataType {
	if stmt == nil || len(stmt.OrderBy) == 0 || !selectHasJoin(stmt) || !booleanSQLExpression(expression) {
		return declared
	}
	return metadata.TypeInt
}

// Materialized inputs already carry authorized SQL types. Resolve projections
// against those types rather than inspecting their first Go row, which loses
// integer widths and cannot describe an empty or NULL-first result.
func (e *XMySQLExecutor) describeMaterializedProjection(ctx *ExecutionContext, stmt *sqlparser.Select, alias string, input *SelectResult, database string) ([]joinOutputColumn, error) {
	shape := joinResultShape{qualified: make(map[string][]joinOutputColumn)}
	for index, name := range input.Columns {
		typ := metadata.DataType(strings.ToUpper(derivedColumnType(input, index)))
		if typ == "" {
			return nil, fmt.Errorf("materialized column %s has no SQL type", name)
		}
		shape.columns = append(shape.columns, joinOutputColumn{name: strings.Trim(name, "`"), typ: typ})
	}
	shape.qualified[strings.ToLower(alias)] = shape.columns
	if ctx == nil {
		ctx = &ExecutionContext{Context: context.Background()}
	}
	describer := preparedSelectDescriber{executor: e, ctx: ctx, database: database}
	return describer.projection(stmt, shape, nil, 0)
}

func (d *preparedSelectDescriber) source(expression sqlparser.TableExpr, ctes map[string][]joinOutputColumn, depth int) (joinResultShape, error) {
	if depth > 64 {
		return joinResultShape{}, fmt.Errorf("prepared SELECT metadata nesting exceeds 64")
	}
	switch node := expression.(type) {
	case *sqlparser.JoinTableExpr:
		left, err := d.source(node.LeftExpr, ctes, depth+1)
		if err != nil {
			return joinResultShape{}, err
		}
		right, err := d.source(node.RightExpr, ctes, depth+1)
		if err != nil {
			return joinResultShape{}, err
		}
		return mergeJoinResultShapes(node, left, right), nil
	case *sqlparser.ParenTableExpr:
		result := joinResultShape{qualified: make(map[string][]joinOutputColumn)}
		for _, from := range node.Exprs {
			part, err := d.source(from, ctes, depth+1)
			if err != nil {
				return joinResultShape{}, err
			}
			result.columns = append(result.columns, part.columns...)
			for alias, columns := range part.qualified {
				result.qualified[alias] = columns
			}
		}
		return result, nil
	case *sqlparser.AliasedTableExpr:
		alias := node.As.String()
		var columns []joinOutputColumn
		switch table := node.Expr.(type) {
		case sqlparser.TableName:
			if alias == "" {
				alias = table.Name.String()
			}
			if table.Qualifier.IsEmpty() {
				if cte, exists := ctes[strings.ToLower(table.Name.String())]; exists {
					columns = append([]joinOutputColumn(nil), cte...)
					break
				}
			}
			if strings.EqualFold(table.Name.String(), "dual") && table.Qualifier.IsEmpty() {
				break
			}
			schema := d.database
			if !table.Qualifier.IsEmpty() {
				schema = table.Qualifier.String()
			}
			name := table.Name.String()
			if physical, temporary := temporaryPhysicalTableName(d.ctx.Session, schema, name); temporary {
				name = physical
			}
			meta, err := (&SelectExecutor{}).loadTableMetaFromFrm(d.executor.getDataDir(), schema, name)
			if err != nil {
				return joinResultShape{}, err
			}
			for _, column := range meta.Columns {
				if column != nil {
					columns = append(columns, joinOutputColumn{name: column.Name, typ: column.Type})
				}
			}
		case *sqlparser.Subquery:
			var err error
			columns, err = d.describe(table.Select, ctes, depth+1)
			if err != nil {
				return joinResultShape{}, err
			}
		case *sqlparser.TableFunction:
			if table.Function == nil {
				return joinResultShape{}, fmt.Errorf("table function has no definition")
			}
			name := strings.ToLower(table.Function.Name.String())
			if alias == "" {
				alias = name
			}
			switch name {
			case "vector_search":
				columns = vectorSearchResultColumns()
			case "content_text_chunks":
				columns = contentTextChunksResultColumns()
			case "fulltext_tokens":
				columns = fullTextTokensResultColumns()
			default:
				return joinResultShape{}, fmt.Errorf("unsupported table-function metadata: %s", name)
			}
		default:
			return joinResultShape{}, fmt.Errorf("unsupported prepared SELECT source %T", node.Expr)
		}
		for index := range columns {
			columns[index].expr = &sqlparser.ColName{Name: sqlparser.NewColIdent(columns[index].name), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent(alias)}}
		}
		return joinResultShape{columns: columns, qualified: map[string][]joinOutputColumn{strings.ToLower(alias): columns}}, nil
	default:
		return joinResultShape{}, fmt.Errorf("unsupported prepared SELECT source %T", expression)
	}
}

func (d *preparedSelectDescriber) expressionType(expression sqlparser.Expr, shape joinResultShape, ctes map[string][]joinOutputColumn, depth int) (metadata.DataType, error) {
	if depth > 64 {
		return "", fmt.Errorf("prepared SELECT metadata nesting exceeds 64")
	}
	if typ, known := modelProjectionResultType(expression); known {
		return typ, nil
	}
	if booleanSQLExpression(expression) {
		return metadata.TypeBigInt, nil
	}
	switch expr := expression.(type) {
	case *sqlparser.ColName:
		columns := shape.columns
		if !expr.Qualifier.IsEmpty() {
			columns = shape.qualified[strings.ToLower(expr.Qualifier.Name.String())]
		}
		var typ metadata.DataType
		for _, column := range columns {
			if strings.EqualFold(column.name, expr.Name.String()) {
				if typ != "" {
					return "", fmt.Errorf("ambiguous column %s", sqlparser.String(expr))
				}
				typ = column.typ
			}
		}
		if typ != "" {
			return typ, nil
		}
	case *sqlparser.SQLVal:
		switch expr.Type {
		case sqlparser.IntVal:
			return metadata.TypeBigInt, nil
		case sqlparser.FloatVal:
			return metadata.TypeDouble, nil
		case sqlparser.StrVal:
			return metadata.TypeVarchar, nil
		case sqlparser.HexVal, sqlparser.HexNum, sqlparser.BitVal:
			return metadata.TypeVarBinary, nil
		}
	case *sqlparser.NullVal:
		return metadata.TypeVarchar, nil
	case *sqlparser.ParenExpr:
		return d.expressionType(expr.Expr, shape, ctes, depth+1)
	case *sqlparser.Subquery:
		columns, err := d.describe(expr.Select, ctes, depth+1)
		if err != nil {
			return "", err
		}
		if len(columns) == 1 {
			return columns[0].typ, nil
		}
	case *sqlparser.FuncExpr:
		switch strings.ToUpper(strings.Trim(expr.Name.String(), "` ")) {
		case "COUNT", "ROW_NUMBER", "RANK", "DENSE_RANK", "LENGTH", "OCTET_LENGTH", "CHAR_LENGTH", "CONTENT_REVISION", "CONTENT_LENGTH", "FULLTEXT_TOKEN_COUNT", "VECTOR_DIMENSION":
			return metadata.TypeBigInt, nil
		case "VECTOR_FROM_ARRAY", "VECTOR_FROM_SOURCE", "VECTOR_FROM_BLOCK":
			return metadata.TypeVector, nil
		case "CONTENT_FROM_BYTES", "CONTENT_BLOCK_FROM_JSON", "CONTENT_BLOCK_FROM_SOURCE", "CONTENT_TEXT_BLOCK", "CONTENT_TEXT_CHUNK", "CONTENT_PARSE_CSV", "CONTENT_PARSE_JSON_ARRAY", "CONTENT_PARSE_IMAGE", "CONTENT_PARSE_AUDIO", "CONTENT_PARSE_VIDEO":
			return metadata.TypeContent, nil
		case "CONTENT_SLICE", "CONTENT_BLOCK_SOURCE_SLICE":
			return metadata.TypeMediumBlob, nil
		case "CONTENT_BLOCK_RETRIEVAL_TEXT":
			return metadata.TypeMediumText, nil
		case "VECTOR_DISTANCE", "FULLTEXT_SCORE", "BM25_TERM_SCORE", "RRF_SCORE", "AVG":
			return metadata.TypeDouble, nil
		case "CONCAT", "LOWER", "UPPER", "CONTENT_TEXT", "CONTENT_MIME", "CONTENT_SHA256", "CONTENT_FAMILY", "CONTENT_BLOCK_TEXT", "CONTENT_BLOCK_CONTEXT", "CONTENT_BLOCK_STATE", "CONTENT_MAINTENANCE_STATUS":
			return metadata.TypeVarchar, nil
		}
	}
	return "", fmt.Errorf("prepared expression requires execution-time metadata: %s", sqlparser.String(expression))
}
