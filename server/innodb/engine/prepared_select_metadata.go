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
		if column.meta == nil {
			result.ColumnMetadata = append(result.ColumnMetadata, &metadata.ColumnMeta{Name: column.name, Type: column.typ, IsNullable: true})
		} else {
			copyColumn := *column.meta
			copyColumn.Name, copyColumn.Type = column.name, column.typ
			copyColumn.EnumValues = append([]string(nil), column.meta.EnumValues...)
			result.ColumnMetadata = append(result.ColumnMetadata, &copyColumn)
		}
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
			commonType, err := mysqlSetOperationColumnType(left[index].typ, right[index].typ)
			if err != nil {
				return nil, fmt.Errorf("set-operation column %d: %w", index+1, err)
			}
			left[index].typ = commonType
		}
		leftMetadata := make([]*metadata.ColumnMeta, len(left))
		rightMetadata := make([]*metadata.ColumnMeta, len(right))
		commonTypes := make([]string, len(left))
		for index := range left {
			leftMetadata[index], rightMetadata[index] = left[index].meta, right[index].meta
			commonTypes[index] = string(left[index].typ)
		}
		merged, err := mergeSetOperationColumnMetadata(leftMetadata, rightMetadata, commonTypes)
		if err != nil {
			return nil, err
		}
		for index := range left {
			left[index].meta = merged[index]
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
			output = append(output, joinOutputColumn{name: name, typ: typ, meta: joinOutputMetadata(expr.Expr, shape)})
		default:
			return nil, fmt.Errorf("unsupported prepared projection %T", expression)
		}
	}
	return output, nil
}

func joinOutputMetadata(expression sqlparser.Expr, shape joinResultShape) *metadata.ColumnMeta {
	column, ok := expression.(*sqlparser.ColName)
	if !ok || column == nil {
		return nil
	}
	candidates := shape.columns
	if !column.Qualifier.IsEmpty() {
		if qualified, found := shape.qualified[strings.ToLower(column.Qualifier.Name.String())]; found {
			candidates = qualified
		}
	}
	for _, candidate := range candidates {
		if !strings.EqualFold(candidate.name, column.Name.String()) || candidate.meta == nil {
			continue
		}
		copyColumn := *candidate.meta
		copyColumn.EnumValues = append([]string(nil), candidate.meta.EnumValues...)
		return &copyColumn
	}
	return nil
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
		var columnMetadata *metadata.ColumnMeta
		if index < len(input.ColumnMetadata) && input.ColumnMetadata[index] != nil {
			copyColumn := *input.ColumnMetadata[index]
			columnMetadata = &copyColumn
		}
		shape.columns = append(shape.columns, joinOutputColumn{name: strings.Trim(name, "`"), typ: typ, meta: columnMetadata})
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
					copyColumn := *column
					copyColumn.EnumValues = append([]string(nil), column.EnumValues...)
					columns = append(columns, joinOutputColumn{name: column.Name, typ: column.Type, meta: &copyColumn})
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
			case "content_audio_segments":
				columns = contentAudioSegmentsResultColumns()
			case "content_video_segments":
				columns = contentVideoSegmentsResultColumns()
			case "content_text_chunks":
				columns = contentTextChunksResultColumns()
			case "content_parse_text":
				columns = contentTextParseResultColumns()
			case "content_parse_pdf":
				columns = contentPDFPagesResultColumns()
			case "content_parse_docx":
				columns = contentDOCXBlocksResultColumns()
			case "content_parse_html":
				columns = contentHTMLBlocksResultColumns()
			case "content_parse_pptx":
				columns = contentPPTXBlocksResultColumns()
			case "content_parse_xlsx":
				columns = contentXLSXRowsResultColumns()
			case "fulltext_tokens":
				columns = fullTextTokensResultColumns()
			case "fulltext_search":
				columns = fullTextSearchResultColumns()
			case "hybrid_search":
				columns = hybridSearchResultColumns()
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
		if typ, known, err := d.builtinFunctionResultType(expr, shape, ctes, depth+1); known || err != nil {
			return typ, err
		}
		switch strings.ToUpper(strings.Trim(expr.Name.String(), "` ")) {
		case "COUNT", "ROW_NUMBER", "RANK", "DENSE_RANK", "LENGTH", "OCTET_LENGTH", "CHAR_LENGTH", "CONTENT_REVISION", "CONTENT_LENGTH", "FULLTEXT_TOKEN_COUNT", "VECTOR_DIMENSION":
			return metadata.TypeBigInt, nil
		case "VECTOR_FROM_ARRAY", "VECTOR_FROM_SOURCE", "VECTOR_FROM_BLOCK":
			return metadata.TypeVector, nil
		case "AI_EXTRACT", "AI_EXTRACT_RESULT", "AI_RAG_RESULT", "AI_NL_SQL", "AI_NL_SQL_EXECUTE":
			return metadata.TypeJSON, nil
		case "CONTENT_FROM_BYTES", "CONTENT_BLOCK_FROM_JSON", "CONTENT_BLOCK_FROM_SOURCE", "CONTENT_TEXT_BLOCK", "CONTENT_TEXT_CHUNK", "CONTENT_PARSE_CSV", "CONTENT_PARSE_JSON_ARRAY", "CONTENT_PARSE_IMAGE", "CONTENT_PARSE_AUDIO", "CONTENT_PARSE_VIDEO":
			return metadata.TypeContent, nil
		case "CONTENT_SLICE", "CONTENT_BLOCK_SOURCE_SLICE":
			return metadata.TypeMediumBlob, nil
		case "CONTENT_BLOCK_RETRIEVAL_TEXT":
			return metadata.TypeMediumText, nil
		case "VECTOR_DISTANCE", "FULLTEXT_SCORE", "BM25_TERM_SCORE", "BM25_SCORE", "RRF_SCORE", "AVG":
			return metadata.TypeDouble, nil
		case "CONCAT", "LOWER", "UPPER", "CONTENT_TEXT", "CONTENT_MIME", "CONTENT_SHA256", "CONTENT_FAMILY", "CONTENT_BLOCK_TEXT", "CONTENT_BLOCK_CONTEXT", "CONTENT_BLOCK_STATE", "CONTENT_MAINTENANCE_STATUS":
			return metadata.TypeVarchar, nil
		}
	}
	return "", fmt.Errorf("prepared expression requires execution-time metadata: %s", sqlparser.String(expression))
}

func (d *preparedSelectDescriber) builtinFunctionResultType(function *sqlparser.FuncExpr, shape joinResultShape, ctes map[string][]joinOutputColumn, depth int) (metadata.DataType, bool, error) {
	name := strings.ToUpper(strings.Trim(function.Name.String(), "` "))
	arguments := make([]sqlparser.Expr, 0, len(function.Exprs))
	for _, expression := range function.Exprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if !ok || aliased == nil || aliased.Expr == nil {
			return "", true, fmt.Errorf("prepared function %s has an unsupported argument shape", name)
		}
		arguments = append(arguments, aliased.Expr)
	}
	argumentType := func(index int) (metadata.DataType, error) {
		if index < 0 || index >= len(arguments) {
			return "", fmt.Errorf("prepared function %s has too few arguments", name)
		}
		return d.expressionType(arguments[index], shape, ctes, depth+1)
	}
	mergeArguments := func(start int) (metadata.DataType, error) {
		var result metadata.DataType
		for index := start; index < len(arguments); index++ {
			if _, isNull := arguments[index].(*sqlparser.NullVal); isNull {
				continue
			}
			typ, err := d.expressionType(arguments[index], shape, ctes, depth+1)
			if err != nil {
				return "", err
			}
			if result == "" {
				result = typ
				continue
			}
			result, err = mysqlSetOperationColumnType(result, typ)
			if err != nil {
				return "", err
			}
		}
		if result == "" {
			return metadata.TypeVarchar, nil
		}
		return result, nil
	}
	switch name {
	case "COALESCE", "GREATEST", "LEAST":
		typ, err := mergeArguments(0)
		return typ, true, err
	case "IF":
		if len(arguments) != 3 {
			return "", true, fmt.Errorf("prepared function IF requires 3 arguments")
		}
		if _, thenNull := arguments[1].(*sqlparser.NullVal); thenNull {
			typ, err := argumentType(2)
			return typ, true, err
		}
		if _, elseNull := arguments[2].(*sqlparser.NullVal); elseNull {
			typ, err := argumentType(1)
			return typ, true, err
		}
		typ, err := mergeArguments(1)
		return typ, true, err
	case "IFNULL":
		if len(arguments) != 2 {
			return "", true, fmt.Errorf("prepared function IFNULL requires 2 arguments")
		}
		if _, firstNull := arguments[0].(*sqlparser.NullVal); firstNull {
			typ, err := argumentType(1)
			return typ, true, err
		}
		if _, secondNull := arguments[1].(*sqlparser.NullVal); secondNull {
			typ, err := argumentType(0)
			return typ, true, err
		}
		typ, mergeErr := mergeArguments(0)
		return typ, true, mergeErr
	case "NULLIF":
		typ, err := argumentType(0)
		return typ, true, err
	case "ABS":
		typ, err := argumentType(0)
		if err == nil && !isSetOperationNumeric(typ) {
			typ = metadata.TypeDouble
		}
		return typ, true, err
	case "ROUND", "TRUNCATE":
		typ, err := argumentType(0)
		if err != nil {
			return "", true, err
		}
		switch typ {
		case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt, metadata.TypeBigInt, metadata.TypeBit, metadata.TypeYear:
			return metadata.TypeBigInt, true, nil
		case metadata.TypeDecimal:
			return metadata.TypeDecimal, true, nil
		default:
			return metadata.TypeDouble, true, nil
		}
	case "CEIL", "CEILING", "FLOOR":
		typ, err := argumentType(0)
		if err != nil {
			return "", true, err
		}
		if !isSetOperationNumeric(typ) {
			return metadata.TypeDouble, true, nil
		}
		if _, integer := setOperationIntegerRank(typ); integer {
			return metadata.TypeBigInt, true, nil
		}
		return typ, true, nil
	case "SQRT", "POWER", "POW", "EXP", "LN", "LOG", "LOG2", "LOG10", "SIN", "COS", "TAN", "COT", "ASIN", "ACOS", "ATAN", "ATAN2", "RADIANS", "DEGREES", "PI", "RAND":
		return metadata.TypeDouble, true, nil
	case "SUM", "MIN", "MAX", "ANY_VALUE":
		typ, err := argumentType(0)
		if name == "SUM" && err == nil {
			if typ == metadata.TypeFloat || typ == metadata.TypeDouble {
				return metadata.TypeDouble, true, nil
			}
			if isSetOperationNumeric(typ) {
				return metadata.TypeDecimal, true, nil
			}
		}
		return typ, true, err
	case "AVG":
		typ, err := argumentType(0)
		if err != nil {
			return "", true, err
		}
		if typ == metadata.TypeFloat || typ == metadata.TypeDouble {
			return metadata.TypeDouble, true, nil
		}
		if isSetOperationNumeric(typ) {
			return metadata.TypeDecimal, true, nil
		}
		return metadata.TypeDouble, true, nil
	case "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "VARIANCE":
		return metadata.TypeDouble, true, nil
	case "BIT_AND", "BIT_OR", "BIT_XOR", "SIGN", "ASCII", "ORD", "FIND_IN_SET", "INSTR", "LOCATE", "BIT_LENGTH", "BIT_COUNT", "STRCMP", "CRC32", "INET_ATON", "UUID_SHORT", "IS_IPV4", "IS_IPV6", "IS_IPV4_COMPAT", "IS_IPV4_MAPPED", "IS_UUID", "YEAR", "MONTH", "DAY", "DAYOFMONTH", "HOUR", "MINUTE", "SECOND", "MICROSECOND", "QUARTER", "WEEK", "YEARWEEK", "WEEKOFYEAR", "WEEKDAY", "DAYOFWEEK", "DAYOFYEAR", "LAST_INSERT_ID", "CONNECTION_ID":
		return metadata.TypeBigInt, true, nil
	case "GROUP_CONCAT":
		return metadata.TypeLongText, true, nil
	case "CONCAT", "CONCAT_WS", "SUBSTRING", "SUBSTR", "SUBSTRING_INDEX", "LEFT", "RIGHT", "REPLACE", "REVERSE", "LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM", "LPAD", "RPAD", "REPEAT", "SPACE", "FORMAT", "QUOTE", "HEX", "BIN", "OCT", "CONV", "SOUNDEX", "MD5", "SHA", "SHA1", "SHA2", "UUID", "BIN_TO_UUID", "INET_NTOA", "INET6_NTOA", "DATE_FORMAT", "TIME_FORMAT", "GET_FORMAT", "DAYNAME", "MONTHNAME":
		return metadata.TypeVarchar, true, nil
	case "INET6_ATON", "UUID_TO_BIN", "RANDOM_BYTES":
		return metadata.TypeVarBinary, true, nil
	case "CURRENT_TIMESTAMP", "NOW", "LOCALTIMESTAMP", "SYSDATE", "UTC_TIMESTAMP", "TIMESTAMP", "STR_TO_DATE", "CONVERT_TZ", "FROM_UNIXTIME":
		return metadata.TypeDateTime, true, nil
	case "CURRENT_DATE", "CURDATE", "UTC_DATE", "DATE", "LAST_DAY":
		return metadata.TypeDate, true, nil
	case "CURRENT_TIME", "CURTIME", "UTC_TIME", "TIME", "TIMEDIFF", "ADDTIME", "SUBTIME", "SEC_TO_TIME", "MAKETIME":
		return metadata.TypeTime, true, nil
	case "JSON_ARRAY", "JSON_OBJECT", "JSON_EXTRACT", "JSON_ARRAYAGG", "JSON_OBJECTAGG", "JSON_KEYS", "JSON_PRETTY", "JSON_MERGE", "JSON_MERGE_PATCH", "JSON_MERGE_PRESERVE", "JSON_ARRAY_APPEND", "JSON_ARRAY_INSERT", "JSON_INSERT", "JSON_SET", "JSON_REPLACE", "JSON_REMOVE", "JSON_SEARCH":
		return metadata.TypeJSON, true, nil
	case "JSON_CONTAINS", "JSON_CONTAINS_PATH", "JSON_OVERLAPS", "JSON_VALID", "JSON_SCHEMA_VALID":
		return metadata.TypeBigInt, true, nil
	case "JSON_LENGTH", "JSON_DEPTH", "JSON_STORAGE_SIZE", "JSON_STORAGE_FREE":
		return metadata.TypeBigInt, true, nil
	case "JSON_TYPE", "JSON_UNQUOTE", "JSON_QUOTE", "JSON_SCHEMA_VALIDATION_REPORT":
		return metadata.TypeVarchar, true, nil
	case "JSON_VALUE":
		return metadata.TypeVarchar, true, nil
	}
	return "", false, nil
}
