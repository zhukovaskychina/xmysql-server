package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

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
			output = append(output, joinOutputColumn{name: name, typ: typ, meta: d.preparedExpressionMetadata(expr.Expr, shape, typ)})
		default:
			return nil, fmt.Errorf("unsupported prepared projection %T", expression)
		}
	}
	return output, nil
}

func (d *preparedSelectDescriber) preparedExpressionMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType) *metadata.ColumnMeta {
	if booleanSQLExpression(expression) {
		return &metadata.ColumnMeta{
			Name: sqlparser.String(expression), Type: resultType,
			IsNullable: d.preparedBooleanExpressionNullable(expression, shape),
		}
	}
	switch node := expression.(type) {
	case *sqlparser.ColName:
		candidates := shape.columns
		if !node.Qualifier.IsEmpty() {
			if qualified, found := shape.qualified[strings.ToLower(node.Qualifier.Name.String())]; found {
				candidates = qualified
			}
		}
		for _, candidate := range candidates {
			if !strings.EqualFold(candidate.name, node.Name.String()) || candidate.meta == nil {
				continue
			}
			copyColumn := *candidate.meta
			copyColumn.Name, copyColumn.Type = candidate.name, resultType
			copyColumn.EnumValues = append([]string(nil), candidate.meta.EnumValues...)
			return &copyColumn
		}
	case *sqlparser.ParenExpr:
		return d.preparedExpressionMetadata(node.Expr, shape, resultType)
	case *sqlparser.UnaryExpr:
		if resultType != metadata.TypeDecimal && resultType != metadata.TypeBigInt {
			return nil
		}
		column := d.preparedExpressionMetadata(node.Expr, shape, resultType)
		if column == nil {
			return nil
		}
		column.Name, column.Type = sqlparser.String(expression), resultType
		if strings.TrimSpace(node.Operator) == "-" {
			column.IsUnsigned = false
		}
		return column
	case *sqlparser.CollateExpr:
		column := d.preparedExpressionMetadata(node.Expr, shape, resultType)
		if column == nil {
			return nil
		}
		if collation, found := common.CollationMetadataByName(node.Charset); found {
			column.Collation = collation.Name
			column.Charset = collation.Charset
		}
		return column
	case *sqlparser.ConvertExpr:
		if node.Type == nil {
			return nil
		}
		column := &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, IsNullable: true}
		if node.Type.Length != nil {
			if length, err := strconv.Atoi(string(node.Type.Length.Val)); err == nil && length > 0 {
				column.Length = length
			}
		}
		if node.Type.Scale != nil {
			if scale, err := strconv.Atoi(string(node.Type.Scale.Val)); err == nil && scale >= 0 {
				column.Scale = scale
			}
		}
		column.IsUnsigned = strings.Contains(strings.ToUpper(node.Type.Type), "UNSIGNED")
		if resultType == metadata.TypeChar || resultType == metadata.TypeVarchar {
			if explicitCharset := strings.TrimSpace(node.Type.Charset); explicitCharset != "" {
				column.Charset = explicitCharset
			} else if d != nil && d.ctx != nil && d.ctx.Session != nil {
				if collation, ok := d.ctx.Session.GetParamByName("collation_connection").(string); ok {
					if info, found := common.CollationMetadataByName(collation); found {
						column.Charset, column.Collation = info.Charset, info.Name
					}
				}
			}
		}
		return column
	case *sqlparser.ConvertUsingExpr:
		column := d.preparedExpressionMetadata(node.Expr, shape, resultType)
		if column == nil {
			column = &metadata.ColumnMeta{Name: sqlparser.String(expression), IsNullable: true}
		}
		column.Type = resultType
		column.Charset = strings.TrimSpace(node.Type)
		return column
	case *sqlparser.SubstrExpr:
		sourceType, err := d.expressionType(node.Name, shape, nil, 0)
		if err != nil {
			return nil
		}
		column := d.preparedExpressionMetadata(node.Name, shape, sourceType)
		if column == nil || resultType == metadata.TypeVarBinary && !isSetOperationBinary(sourceType) {
			return nil
		}
		column.Type = resultType
		column.Name = sqlparser.String(expression)
		if node.To != nil {
			if count, known := preparedStaticInteger(node.To); known {
				if count <= 0 {
					return nil
				}
				if count < int64(column.Length) {
					column.Length = int(count)
				}
			}
		}
		return column
	case *sqlparser.GroupConcatExpr:
		return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: metadata.TypeLongText, IsNullable: true}
	case *sqlparser.CaseExpr:
		values := make([]sqlparser.Expr, 0, len(node.Whens)+1)
		for _, when := range node.Whens {
			if when != nil && when.Val != nil {
				values = append(values, when.Val)
			}
		}
		if node.Else != nil {
			values = append(values, node.Else)
		}
		nullable := node.Else == nil
		for _, value := range values {
			nullable = nullable || !d.preparedExpressionDefinitelyNonNull(value, shape)
		}
		return d.preparedConditionalExpressionMetadata(expression, shape, resultType, values, nullable)
	case *sqlparser.SQLVal:
		switch node.Type {
		case sqlparser.IntVal:
			if precision, unsigned, ok := preparedIntegerLiteralShape(node.Val); ok {
				if resultType == metadata.TypeDecimal {
					return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: precision, IsNullable: false}
				}
				return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: preparedIntegerMetadataLength(unsigned), IsUnsigned: unsigned, IsNullable: false}
			}
		case sqlparser.StrVal:
			column := &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: utf8.RuneCount(node.Val), IsNullable: false}
			if d != nil && d.ctx != nil && d.ctx.Session != nil {
				if collation, ok := d.ctx.Session.GetParamByName("collation_connection").(string); ok {
					if info, found := common.CollationMetadataByName(collation); found {
						column.Charset, column.Collation = info.Charset, info.Name
					}
				}
			}
			return column
		case sqlparser.FloatVal:
			if resultType == metadata.TypeDecimal {
				if precision, scale, ok := preparedExactDecimalLiteralShape(node.Val); ok {
					return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: precision, Scale: scale, IsNullable: false}
				}
			}
			if resultType == metadata.TypeDouble {
				return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, IsNullable: false}
			}
		}
	case *sqlparser.NullVal:
		return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: metadata.TypeNull, IsNullable: true}
	case *sqlparser.BinaryExpr:
		numericShape, known := d.preparedNumericExpressionShape(expression, shape, 0)
		if !known {
			return nil
		}
		if resultType == metadata.TypeDecimal {
			return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: numericShape.precision, Scale: numericShape.scale, IsUnsigned: numericShape.unsigned, IsNullable: true}
		}
		if resultType == metadata.TypeBigInt {
			return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: preparedIntegerMetadataLength(numericShape.unsigned), IsUnsigned: numericShape.unsigned, IsNullable: true}
		}
		return nil
	case *sqlparser.FuncExpr:
		name := strings.ToUpper(strings.Trim(node.Name.String(), "` "))
		if name == "COUNT" && isPreparedCountStar(node) {
			return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, IsNullable: false}
		}
		arguments := make([]sqlparser.Expr, 0, len(node.Exprs))
		for _, selectExpression := range node.Exprs {
			aliased, ok := selectExpression.(*sqlparser.AliasedExpr)
			if !ok || aliased == nil || aliased.Expr == nil {
				return nil
			}
			arguments = append(arguments, aliased.Expr)
		}
		if name == "CONCAT" || name == "CONCAT_WS" {
			return d.preparedConcatMetadata(expression, shape, resultType, name, arguments)
		}
		switch name {
		case "DATE_ADD", "DATE_SUB", "ADDDATE", "SUBDATE":
			return d.preparedDateArithmeticMetadata(expression, shape, resultType, arguments)
		case "TIMESTAMP", "ADDTIME", "SUBTIME":
			return d.preparedAddTimeMetadata(expression, shape, resultType, name, arguments)
		case "CONVERT_TZ":
			return d.preparedConvertTZMetadata(expression, shape, resultType, arguments)
		case "FROM_UNIXTIME":
			return d.preparedFromUnixTimeMetadata(expression, shape, resultType, arguments)
		case "STR_TO_DATE":
			return preparedStrToDateMetadata(expression, resultType, arguments)
		case "TIME", "TIMEDIFF", "SEC_TO_TIME", "MAKETIME":
			return d.preparedTimeFunctionMetadata(expression, shape, resultType, name, arguments)
		case "UNIX_TIMESTAMP":
			return d.preparedUnixTimestampMetadata(expression, shape, resultType, arguments)
		case "TIMESTAMPADD":
			return d.preparedTimestampAddMetadata(expression, shape, resultType, arguments)
		case "TIMESTAMPDIFF":
			if len(arguments) == 3 && resultType == metadata.TypeBigInt {
				return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: preparedIntegerMetadataLength(false), IsNullable: true}
			}
		case "NOW", "CURRENT_TIMESTAMP", "LOCALTIMESTAMP", "SYSDATE", "UTC_TIMESTAMP", "CURTIME", "CURRENT_TIME", "LOCALTIME", "UTC_TIME", "CURDATE", "CURRENT_DATE", "UTC_DATE":
			return preparedCurrentTemporalMetadata(expression, resultType, arguments)
		case "COUNT":
			if len(arguments) == 0 {
				return nil
			}
			return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, IsNullable: false}
		case "ROUND", "TRUNCATE", "CEIL", "CEILING", "FLOOR":
			return d.preparedNumericRoundingMetadata(expression, shape, resultType, name, arguments)
		case "IF":
			if len(arguments) == 3 {
				branches := arguments[1:]
				nullable := !d.preparedExpressionDefinitelyNonNull(branches[0], shape) || !d.preparedExpressionDefinitelyNonNull(branches[1], shape)
				return d.preparedConditionalExpressionMetadata(expression, shape, resultType, branches, nullable)
			}
		case "IFNULL", "COALESCE":
			if len(arguments) >= 2 && (name != "IFNULL" || len(arguments) == 2) {
				nullable := true
				for _, argument := range arguments {
					if d.preparedExpressionDefinitelyNonNull(argument, shape) {
						nullable = false
						break
					}
				}
				return d.preparedConditionalExpressionMetadata(expression, shape, resultType, arguments, nullable)
			}
		case "GREATEST", "LEAST":
			if len(arguments) >= 2 {
				nullable := false
				for _, argument := range arguments {
					nullable = nullable || !d.preparedExpressionDefinitelyNonNull(argument, shape)
				}
				return d.preparedConditionalExpressionMetadata(expression, shape, resultType, arguments, nullable)
			}
		case "SUM", "AVG":
			if len(arguments) != 1 {
				return nil
			}
			argumentType, err := d.expressionType(arguments[0], shape, nil, 0)
			if err != nil || !isSetOperationNumeric(argumentType) {
				return nil
			}
			if resultType == metadata.TypeDouble {
				return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, IsNullable: true}
			}
			if resultType != metadata.TypeDecimal {
				return nil
			}
			argumentShape, known := d.preparedNumericExpressionShape(arguments[0], shape, 0)
			if !known {
				return nil
			}
			precision, scale := argumentShape.precision, argumentShape.scale
			if name == "SUM" {
				precision = preparedMinInt(precision+21, 65)
			} else {
				increment := d.divisionPrecisionIncrement()
				precision = preparedMinInt(precision+increment, 65)
				scale = preparedCapDecimalScale(scale + increment)
			}
			return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: precision, Scale: scale, IsNullable: true}
		case "JSON_ARRAYAGG", "JSON_OBJECTAGG":
			return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: metadata.TypeJSON, IsNullable: true}
		case "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "VARIANCE":
			if len(arguments) == 1 && resultType == metadata.TypeDouble {
				return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, IsNullable: true}
			}
		case "NULLIF", "MIN", "MAX", "ANY_VALUE", "ABS":
			if len(arguments) == 0 {
				return nil
			}
			argumentType, err := d.expressionType(arguments[0], shape, nil, 0)
			if err != nil || name == "ABS" && !isSetOperationNumeric(argumentType) {
				return nil
			}
			column := d.preparedExpressionMetadata(arguments[0], shape, resultType)
			if column == nil || name != "NULLIF" && name != "MIN" && name != "MAX" {
				return column
			}
			copyColumn := *column
			copyColumn.Name = sqlparser.String(expression)
			copyColumn.EnumValues = append([]string(nil), column.EnumValues...)
			// NULLIF can return NULL when its arguments compare equal. MIN/MAX
			// can return NULL when the aggregate input is empty, even when their
			// argument is statically non-null.
			copyColumn.IsNullable = true
			return &copyColumn
		}
		return d.preparedStringFunctionMetadata(expression, shape, resultType, name, arguments)
	}
	return nil
}

func (d *preparedSelectDescriber) preparedBooleanExpressionNullable(expression sqlparser.Expr, shape joinResultShape) bool {
	switch node := expression.(type) {
	case *sqlparser.IsExpr, *sqlparser.ExistsExpr, sqlparser.BoolVal:
		return false
	case *sqlparser.ComparisonExpr:
		if strings.EqualFold(strings.TrimSpace(node.Operator), sqlparser.NullSafeEqualStr) {
			return false
		}
		return !d.preparedExpressionDefinitelyNonNull(node.Left, shape) || !d.preparedExpressionDefinitelyNonNull(node.Right, shape)
	case *sqlparser.RangeCond:
		return !d.preparedExpressionDefinitelyNonNull(node.Left, shape) || !d.preparedExpressionDefinitelyNonNull(node.From, shape) || !d.preparedExpressionDefinitelyNonNull(node.To, shape)
	case *sqlparser.ParenExpr:
		return d.preparedBooleanExpressionNullable(node.Expr, shape)
	case *sqlparser.NotExpr:
		return d.preparedBooleanExpressionNullable(node.Expr, shape)
	case *sqlparser.AndExpr:
		return d.preparedBooleanExpressionNullable(node.Left, shape) || d.preparedBooleanExpressionNullable(node.Right, shape)
	case *sqlparser.OrExpr:
		return d.preparedBooleanExpressionNullable(node.Left, shape) || d.preparedBooleanExpressionNullable(node.Right, shape)
	default:
		return true
	}
}

func (d *preparedSelectDescriber) preparedConditionalExpressionMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, values []sqlparser.Expr, nullable bool) *metadata.ColumnMeta {
	column := d.preparedMergedExpressionMetadata(expression, shape, resultType, values)
	if column == nil {
		column = &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType}
	}
	column.Name, column.Type, column.IsNullable = sqlparser.String(expression), resultType, nullable
	return column
}

func (d *preparedSelectDescriber) preparedNumericRoundingMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, name string, arguments []sqlparser.Expr) *metadata.ColumnMeta {
	if len(arguments) == 0 {
		return nil
	}
	nullable := false
	for _, argument := range arguments {
		nullable = nullable || !d.preparedExpressionDefinitelyNonNull(argument, shape)
	}
	column := d.preparedExpressionMetadata(arguments[0], shape, resultType)
	if column == nil {
		column = &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType}
	}
	column.Name, column.Type, column.IsNullable = sqlparser.String(expression), resultType, nullable
	if resultType == metadata.TypeDouble {
		column.Length, column.Scale, column.IsUnsigned = 0, 0, false
		column.Charset, column.Collation, column.EnumValues = "", "", nil
		return column
	}

	argumentType, err := d.expressionType(arguments[0], shape, nil, 0)
	if err != nil {
		return column
	}
	if resultType == metadata.TypeDecimal && argumentType == metadata.TypeDecimal {
		numericShape, known := d.preparedNumericExpressionShape(arguments[0], shape, 0)
		if !known {
			return column
		}
		decimals := int64(0)
		if len(arguments) > 1 {
			if staticDecimals, known := preparedStaticInteger(arguments[1]); known {
				decimals = staticDecimals
			} else {
				decimals = int64(numericShape.scale)
			}
		}
		if decimals > 30 {
			decimals = 30
		} else if decimals < -30 {
			decimals = -30
		}
		precision, scale := numericShape.precision, numericShape.scale
		if decimals <= 0 {
			precision -= scale
			if name == "ROUND" || name != "TRUNCATE" && scale != 0 {
				precision++
			}
			scale = 0
		} else if decimals < int64(scale) {
			precision -= scale - int(decimals)
			if name != "TRUNCATE" {
				precision++
			}
			scale = int(decimals)
		}
		if precision < 1 {
			precision = 1
		}
		column.Length, column.Scale, column.IsUnsigned = preparedMinInt(precision, 65), scale, false
		return column
	}

	if (name == "CEIL" || name == "CEILING" || name == "FLOOR") && argumentType == metadata.TypeDecimal {
		numericShape, known := d.preparedNumericExpressionShape(arguments[0], shape, 0)
		if !known {
			return column
		}
		precision := numericShape.precision - numericShape.scale
		if numericShape.scale != 0 {
			precision++
		}
		if precision <= 18 {
			column.Type = metadata.TypeBigInt
			column.Length, column.Scale = preparedIntegerMetadataLength(false), 0
		} else {
			column.Type = metadata.TypeDecimal
			column.Length, column.Scale = preparedMinInt(precision, 65), 0
		}
		column.IsUnsigned = false
		return column
	}

	if resultType == metadata.TypeBigInt {
		column.Length = preparedIntegerMetadataLength(column.IsUnsigned)
		column.Scale = 0
	}
	return column
}

func preparedCurrentTemporalMetadata(expression sqlparser.Expr, resultType metadata.DataType, arguments []sqlparser.Expr) *metadata.ColumnMeta {
	fsp := int64(0)
	allowPrecision := resultType == metadata.TypeDateTime || resultType == metadata.TypeTime
	if !allowPrecision && len(arguments) != 0 {
		return nil
	}
	if allowPrecision {
		if len(arguments) > 1 {
			return nil
		}
		if len(arguments) == 1 {
			var known bool
			fsp, known = preparedStaticInteger(arguments[0])
			if !known || fsp < 0 || fsp > 6 {
				return nil
			}
		}
	}
	length := 10
	scale := 0
	switch resultType {
	case metadata.TypeDateTime:
		length = 19
		if fsp > 0 {
			length += int(fsp) + 1
			scale = int(fsp)
		}
	case metadata.TypeTime:
		length = 8
		if fsp > 0 {
			length += int(fsp) + 1
			scale = int(fsp)
		}
	case metadata.TypeDate:
		length = 10
	default:
		return nil
	}
	return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: length, Scale: scale, IsNullable: false}
}

func (d *preparedSelectDescriber) preparedDateArithmeticMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, arguments []sqlparser.Expr) *metadata.ColumnMeta {
	if len(arguments) != 2 {
		return nil
	}
	interval, ok := arguments[1].(*sqlparser.IntervalExpr)
	if !ok || interval == nil {
		return nil
	}
	inputType, err := d.expressionType(arguments[0], shape, nil, 0)
	if err != nil {
		if parameter, isParameter := arguments[0].(*sqlparser.SQLVal); !isParameter || parameter.Type != sqlparser.ValArg {
			return nil
		}
	}
	inputPrecision := 0
	if err == nil {
		if input := d.preparedExpressionMetadata(arguments[0], shape, inputType); input != nil {
			inputPrecision = input.Scale
		}
	}
	intervalPrecision := d.preparedDateArithmeticIntervalPrecision(interval, shape)
	precision := max(inputPrecision, intervalPrecision)
	length := 26
	scale := 0
	switch resultType {
	case metadata.TypeDate:
		length = 10
	case metadata.TypeDateTime:
		length = 19
		if precision > 0 {
			length += precision + 1
			scale = precision
		}
	case metadata.TypeTime:
		length = 8
		if precision > 0 {
			length += precision + 1
			scale = precision
		}
	case metadata.TypeVarchar:
		precision = 0
	default:
		return nil
	}
	return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: length, Scale: scale, IsNullable: true}
}

func (d *preparedSelectDescriber) preparedAddTimeMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, name string, arguments []sqlparser.Expr) *metadata.ColumnMeta {
	if name == "TIMESTAMP" && len(arguments) == 1 {
		firstType, err := d.expressionType(arguments[0], shape, nil, 0)
		if err != nil {
			firstType = metadata.TypeDateTime
		}
		precision := 0
		if column := d.preparedExpressionMetadata(arguments[0], shape, firstType); column != nil {
			precision = min(max(column.Scale, 0), 6)
		}
		length := 19
		if precision > 0 {
			length += precision + 1
		}
		return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: metadata.TypeDateTime, Length: length, Scale: precision, IsNullable: true}
	}
	if len(arguments) != 2 {
		return nil
	}
	firstType, err := d.expressionType(arguments[0], shape, nil, 0)
	if err != nil {
		if parameter, isParameter := arguments[0].(*sqlparser.SQLVal); !isParameter || parameter.Type != sqlparser.ValArg {
			return nil
		}
		// MySQL resolves an untyped first parameter as DATETIME for TIMESTAMP,
		// and TIME for ADDTIME/SUBTIME.
		if name == "TIMESTAMP" {
			firstType = metadata.TypeDateTime
		} else {
			firstType = metadata.TypeTime
		}
	}
	if name == "TIMESTAMP" {
		resultType = metadata.TypeDateTime
	} else {
		switch firstType {
		case metadata.TypeDate, metadata.TypeDateTime, metadata.TypeTimestamp:
			resultType = metadata.TypeDateTime
		case metadata.TypeTime:
			resultType = metadata.TypeTime
		default:
			resultType = metadata.TypeVarchar
		}
	}
	precisionOf := func(expression sqlparser.Expr, typ metadata.DataType) int {
		if typ == metadata.TypeDate {
			return 0
		}
		if column := d.preparedExpressionMetadata(expression, shape, typ); column != nil {
			return min(max(column.Scale, 0), 6)
		}
		return 0
	}
	firstPrecision := precisionOf(arguments[0], firstType)
	secondType, secondErr := d.expressionType(arguments[1], shape, nil, 0)
	if secondErr != nil {
		if parameter, isParameter := arguments[1].(*sqlparser.SQLVal); !isParameter || parameter.Type != sqlparser.ValArg {
			return nil
		}
		secondType = metadata.TypeTime
	}
	precision := max(firstPrecision, precisionOf(arguments[1], secondType))
	length := 26
	scale := 0
	switch resultType {
	case metadata.TypeDateTime:
		length = 19
		if precision > 0 {
			length += precision + 1
			scale = precision
		}
	case metadata.TypeTime:
		length = 8
		if precision > 0 {
			length += precision + 1
			scale = precision
		}
	case metadata.TypeVarchar:
		precision = 0
	default:
		return nil
	}
	return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: length, Scale: scale, IsNullable: true}
}

func (d *preparedSelectDescriber) preparedConvertTZMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, arguments []sqlparser.Expr) *metadata.ColumnMeta {
	if len(arguments) != 3 || resultType != metadata.TypeDateTime {
		return nil
	}
	inputType, err := d.expressionType(arguments[0], shape, nil, 0)
	if err != nil {
		if parameter, isParameter := arguments[0].(*sqlparser.SQLVal); !isParameter || parameter.Type != sqlparser.ValArg {
			return nil
		}
		inputType = metadata.TypeDateTime
	}
	precision := 0
	if input := d.preparedExpressionMetadata(arguments[0], shape, inputType); input != nil {
		precision = min(max(input.Scale, 0), 6)
	}
	length := 19
	if precision > 0 {
		length += precision + 1
	}
	return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: length, Scale: precision, IsNullable: true}
}

func (d *preparedSelectDescriber) preparedFromUnixTimeMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, arguments []sqlparser.Expr) *metadata.ColumnMeta {
	if len(arguments) == 2 && resultType == metadata.TypeVarchar {
		return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, IsNullable: true}
	}
	if len(arguments) != 1 || resultType != metadata.TypeDateTime {
		return nil
	}
	precision := 0
	if numericShape, known := d.preparedNumericExpressionShape(arguments[0], shape, 0); known {
		precision = min(max(numericShape.scale, 0), 6)
	}
	length := 19
	if precision > 0 {
		length += precision + 1
	}
	return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: length, Scale: precision, IsNullable: true}
}

func preparedStrToDateMetadata(expression sqlparser.Expr, resultType metadata.DataType, arguments []sqlparser.Expr) *metadata.ColumnMeta {
	if len(arguments) != 2 {
		return nil
	}
	_, precision := preparedStrToDateShape(arguments)
	length := 19
	switch resultType {
	case metadata.TypeDate:
		length, precision = 10, 0
	case metadata.TypeTime:
		length = 8
	case metadata.TypeDateTime:
	default:
		return nil
	}
	if precision > 0 {
		length += precision + 1
	}
	return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: length, Scale: precision, IsNullable: true}
}

func preparedStrToDateShape(arguments []sqlparser.Expr) (metadata.DataType, int) {
	// MySQL defaults to DATETIME(6) when the format is not a constant item.
	if len(arguments) != 2 {
		return metadata.TypeDateTime, 6
	}
	format, ok := arguments[1].(*sqlparser.SQLVal)
	if !ok || format.Type != sqlparser.StrVal {
		return metadata.TypeDateTime, 6
	}
	const timeDirectives = "HISThiklrs"
	const dateDirectives = "MVUXYWabcjmvuxyw"
	usesTime, usesDate, usesFraction := false, false, false
	for index := 0; index < len(format.Val); index++ {
		if format.Val[index] != '%' || index+1 >= len(format.Val) {
			continue
		}
		index++
		directive := format.Val[index]
		if directive == 'f' {
			usesTime, usesFraction = true, true
		} else if !usesTime && strings.IndexByte(timeDirectives, directive) >= 0 {
			usesTime = true
		} else if !usesDate && strings.IndexByte(dateDirectives, directive) >= 0 {
			usesDate = true
		}
		if usesDate && usesFraction {
			return metadata.TypeDateTime, 6
		}
	}
	if usesFraction {
		return metadata.TypeTime, 6
	}
	if usesTime {
		if usesDate {
			return metadata.TypeDateTime, 0
		}
		return metadata.TypeTime, 0
	}
	return metadata.TypeDate, 0
}

func (d *preparedSelectDescriber) preparedTimeFunctionMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, name string, arguments []sqlparser.Expr) *metadata.ColumnMeta {
	if resultType != metadata.TypeTime {
		return nil
	}
	wantArguments := 1
	switch name {
	case "TIMEDIFF":
		wantArguments = 2
	case "MAKETIME":
		wantArguments = 3
	}
	if len(arguments) != wantArguments {
		return nil
	}
	precisionOf := func(argument sqlparser.Expr) int {
		if numericShape, known := d.preparedNumericExpressionShape(argument, shape, 0); known {
			return min(max(numericShape.scale, 0), 6)
		}
		argumentType, err := d.expressionType(argument, shape, nil, 0)
		if err != nil {
			return 0
		}
		if column := d.preparedExpressionMetadata(argument, shape, argumentType); column != nil {
			return min(max(column.Scale, 0), 6)
		}
		return 0
	}
	precision := 0
	switch name {
	case "TIMEDIFF":
		precision = max(precisionOf(arguments[0]), precisionOf(arguments[1]))
	case "MAKETIME":
		precision = precisionOf(arguments[2])
	default:
		precision = precisionOf(arguments[0])
	}
	length := 8
	if precision > 0 {
		length += precision + 1
	}
	return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: length, Scale: precision, IsNullable: true}
}

func (d *preparedSelectDescriber) preparedUnixTimestampMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, arguments []sqlparser.Expr) *metadata.ColumnMeta {
	if len(arguments) == 0 {
		if resultType != metadata.TypeBigInt {
			return nil
		}
		return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: preparedIntegerMetadataLength(false), IsNullable: false}
	}
	if len(arguments) != 1 {
		return nil
	}
	precision := 0
	if argumentType, err := d.expressionType(arguments[0], shape, nil, 0); err == nil {
		if argument := d.preparedExpressionMetadata(arguments[0], shape, argumentType); argument != nil {
			precision = min(max(argument.Scale, 0), 6)
		}
	}
	if precision > 0 {
		if resultType != metadata.TypeDecimal {
			return nil
		}
		return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: 11 + precision, Scale: precision, IsNullable: true}
	}
	if resultType != metadata.TypeBigInt {
		return nil
	}
	return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: preparedIntegerMetadataLength(false), IsNullable: true}
}

func (d *preparedSelectDescriber) preparedTimestampAddMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, arguments []sqlparser.Expr) *metadata.ColumnMeta {
	if len(arguments) != 3 {
		return nil
	}
	unit, known := preparedTimestampIntervalUnit(arguments[0])
	if !known {
		return nil
	}
	inputType, err := d.expressionType(arguments[2], shape, nil, 0)
	if err != nil {
		if parameter, isParameter := arguments[2].(*sqlparser.SQLVal); !isParameter || parameter.Type != sqlparser.ValArg {
			return nil
		}
		inputType = mysqlDateArithmeticParameterType(unit)
	}
	precision := 0
	if input := d.preparedExpressionMetadata(arguments[2], shape, inputType); input != nil {
		precision = min(max(input.Scale, 0), 6)
	}
	if unit == "MICROSECOND" {
		precision = max(precision, 6)
	}
	length := 26
	scale := 0
	switch resultType {
	case metadata.TypeDate:
		length, precision = 10, 0
	case metadata.TypeDateTime:
		length = 19
		if precision > 0 {
			length += precision + 1
			scale = precision
		}
	case metadata.TypeTime:
		length = 8
		if precision > 0 {
			length += precision + 1
			scale = precision
		}
	case metadata.TypeVarchar:
		precision = 0
	default:
		return nil
	}
	return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: length, Scale: scale, IsNullable: true}
}

func preparedTimestampIntervalUnit(expression sqlparser.Expr) (string, bool) {
	unit := strings.ToUpper(strings.Trim(strings.TrimSpace(sqlparser.String(expression)), "`'\" "))
	unit = strings.TrimPrefix(unit, "SQL_TSI_")
	switch unit {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		return unit, true
	default:
		return "", false
	}
}

func (d *preparedSelectDescriber) preparedDateArithmeticIntervalPrecision(interval *sqlparser.IntervalExpr, shape joinResultShape) int {
	unit := strings.ToUpper(strings.TrimSpace(interval.Unit))
	if unit == "MICROSECOND" || strings.HasSuffix(unit, "_MICROSECOND") {
		return 6
	}
	if unit != "SECOND" {
		return 0
	}
	shapeResult, known := d.preparedNumericExpressionShape(interval.Expr, shape, 0)
	if !known {
		return 0
	}
	return min(shapeResult.scale, 6)
}

// preparedExpressionDefinitelyNonNull is deliberately proof-based: unknown
// expressions remain nullable instead of making PREPARE metadata optimistic.
func (d *preparedSelectDescriber) preparedExpressionDefinitelyNonNull(expression sqlparser.Expr, shape joinResultShape) bool {
	if expression == nil {
		return false
	}
	if booleanSQLExpression(expression) {
		return !d.preparedBooleanExpressionNullable(expression, shape)
	}
	switch node := expression.(type) {
	case *sqlparser.NullVal:
		return false
	case *sqlparser.SQLVal:
		return node.Type != sqlparser.ValArg
	case sqlparser.BoolVal:
		return true
	case *sqlparser.ParenExpr:
		return d.preparedExpressionDefinitelyNonNull(node.Expr, shape)
	case *sqlparser.CaseExpr:
		if node.Else == nil || !d.preparedExpressionDefinitelyNonNull(node.Else, shape) {
			return false
		}
		for _, when := range node.Whens {
			if when == nil || when.Val == nil || !d.preparedExpressionDefinitelyNonNull(when.Val, shape) {
				return false
			}
		}
		return true
	case *sqlparser.FuncExpr:
		name := strings.ToUpper(strings.Trim(node.Name.String(), "` "))
		arguments := make([]sqlparser.Expr, 0, len(node.Exprs))
		for _, selectExpression := range node.Exprs {
			aliased, ok := selectExpression.(*sqlparser.AliasedExpr)
			if !ok || aliased == nil || aliased.Expr == nil {
				return false
			}
			arguments = append(arguments, aliased.Expr)
		}
		switch name {
		case "IF":
			return len(arguments) == 3 && d.preparedExpressionDefinitelyNonNull(arguments[1], shape) && d.preparedExpressionDefinitelyNonNull(arguments[2], shape)
		case "IFNULL", "COALESCE":
			for _, argument := range arguments {
				if d.preparedExpressionDefinitelyNonNull(argument, shape) {
					return true
				}
			}
			return false
		case "GREATEST", "LEAST":
			if len(arguments) < 2 {
				return false
			}
			for _, argument := range arguments {
				if !d.preparedExpressionDefinitelyNonNull(argument, shape) {
					return false
				}
			}
			return true
		case "NULLIF":
			return false
		case "ROUND", "TRUNCATE", "CEIL", "CEILING", "FLOOR":
			if len(arguments) == 0 {
				return false
			}
			for _, argument := range arguments {
				if !d.preparedExpressionDefinitelyNonNull(argument, shape) {
					return false
				}
			}
			return true
		}
	}
	resultType, err := d.expressionType(expression, shape, nil, 0)
	if err != nil {
		return false
	}
	column := d.preparedExpressionMetadata(expression, shape, resultType)
	return column != nil && !column.IsNullable
}

func (d *preparedSelectDescriber) preparedMergedExpressionMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, values []sqlparser.Expr) *metadata.ColumnMeta {
	if resultType != metadata.TypeDecimal {
		return d.preparedMergedStringMetadata(expression, shape, resultType, values)
	}
	maximumIntegerDigits, maximumScale := 0, 0
	for _, value := range values {
		if value == nil {
			continue
		}
		if _, isNull := value.(*sqlparser.NullVal); isNull {
			continue
		}
		valueType, err := d.expressionType(value, shape, nil, 0)
		if err != nil || !isSetOperationNumeric(valueType) {
			return nil
		}
		numericShape, known := d.preparedNumericExpressionShape(value, shape, 0)
		if !known {
			return nil
		}
		integerDigits := numericShape.precision - numericShape.scale
		if integerDigits > maximumIntegerDigits {
			maximumIntegerDigits = integerDigits
		}
		if numericShape.scale > maximumScale {
			maximumScale = numericShape.scale
		}
	}
	precision := maximumIntegerDigits + maximumScale
	if precision <= 0 {
		return nil
	}
	return &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: preparedMinInt(precision, 65), Scale: maximumScale, IsNullable: true}
}

func isPreparedCountStar(function *sqlparser.FuncExpr) bool {
	if function == nil || !strings.EqualFold(strings.Trim(function.Name.String(), "` "), "COUNT") || len(function.Exprs) != 1 {
		return false
	}
	star, ok := function.Exprs[0].(*sqlparser.StarExpr)
	return ok && star != nil && star.TableName.IsEmpty()
}

func (d *preparedSelectDescriber) preparedMergedStringMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, values []sqlparser.Expr) *metadata.ColumnMeta {
	maximumLength := 0
	charset, collation := "", ""
	for _, value := range values {
		if value == nil {
			continue
		}
		if _, isNull := value.(*sqlparser.NullVal); isNull {
			continue
		}
		valueType, err := d.expressionType(value, shape, nil, 0)
		if err != nil || resultType == metadata.TypeVarBinary && !isSetOperationBinary(valueType) || resultType != metadata.TypeVarBinary && !isSetOperationString(valueType) {
			return nil
		}
		column := d.preparedExpressionMetadata(value, shape, valueType)
		if column == nil || column.Length <= 0 {
			return nil
		}
		if resultType != metadata.TypeVarBinary {
			if column.Charset == "" || column.Collation == "" {
				return nil
			}
			if charset == "" {
				charset, collation = column.Charset, column.Collation
			} else if !strings.EqualFold(charset, column.Charset) || !strings.EqualFold(collation, column.Collation) {
				return nil
			}
		}
		if column.Length > maximumLength {
			maximumLength = column.Length
		}
	}
	if maximumLength <= 0 {
		return nil
	}
	column := &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: maximumLength, IsNullable: true}
	if resultType == metadata.TypeVarBinary {
		column.Charset, column.Collation = "binary", "binary"
	} else {
		column.Charset, column.Collation = charset, collation
	}
	return column
}

func (d *preparedSelectDescriber) preparedConcatMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, name string, arguments []sqlparser.Expr) *metadata.ColumnMeta {
	lengths := make([]int, 0, len(arguments))
	charset, collation := "", ""
	for _, value := range arguments {
		argumentType, err := d.expressionType(value, shape, nil, 0)
		if err != nil || resultType == metadata.TypeVarBinary && !isSetOperationBinary(argumentType) || resultType != metadata.TypeVarBinary && !isSetOperationString(argumentType) {
			return nil
		}
		argument := d.preparedExpressionMetadata(value, shape, argumentType)
		if argument == nil || argument.Length <= 0 {
			return nil
		}
		if resultType != metadata.TypeVarBinary {
			if strings.TrimSpace(argument.Charset) == "" || strings.TrimSpace(argument.Collation) == "" {
				return nil
			}
			if charset == "" {
				charset, collation = argument.Charset, argument.Collation
			} else if !strings.EqualFold(charset, argument.Charset) || !strings.EqualFold(collation, argument.Collation) {
				return nil
			}
		}
		lengths = append(lengths, argument.Length)
	}
	if len(lengths) == 0 {
		return nil
	}
	length := 0
	for _, argumentLength := range lengths {
		if argumentLength > int(^uint(0)>>1)-length {
			return nil
		}
		length += argumentLength
	}
	if name == "CONCAT_WS" && len(lengths) > 2 {
		separatorLength := lengths[0]
		separatorCount := len(lengths) - 2
		if separatorLength > (int(^uint(0)>>1)-length)/separatorCount {
			return nil
		}
		length += separatorLength * separatorCount
	}
	column := &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: length, IsNullable: true}
	if resultType == metadata.TypeVarBinary {
		column.Charset, column.Collation = "binary", "binary"
	} else {
		column.Charset, column.Collation = charset, collation
	}
	return column
}

func (d *preparedSelectDescriber) preparedStringFunctionMetadata(expression sqlparser.Expr, shape joinResultShape, resultType metadata.DataType, name string, arguments []sqlparser.Expr) *metadata.ColumnMeta {
	if name == "SPACE" {
		if len(arguments) != 1 {
			return nil
		}
		count, known := preparedStaticInteger(arguments[0])
		if !known || count <= 0 || count > int64(int(^uint(0)>>1)) {
			return nil
		}
		column := &metadata.ColumnMeta{Name: sqlparser.String(expression), Type: resultType, Length: int(count), IsNullable: true}
		if d != nil && d.ctx != nil && d.ctx.Session != nil {
			if collation, ok := d.ctx.Session.GetParamByName("collation_connection").(string); ok {
				if info, found := common.CollationMetadataByName(collation); found {
					column.Charset, column.Collation = info.Charset, info.Name
				}
			}
		}
		return column
	}
	if len(arguments) == 0 {
		return nil
	}
	firstType, err := d.expressionType(arguments[0], shape, nil, 0)
	if err != nil || !isSetOperationString(firstType) && !isSetOperationBinary(firstType) {
		return nil
	}
	first := d.preparedExpressionMetadata(arguments[0], shape, firstType)
	if first == nil || first.Length <= 0 && name != "LPAD" && name != "RPAD" {
		return nil
	}
	if resultType == metadata.TypeVarBinary && !isSetOperationBinary(firstType) {
		return nil
	}
	if resultType != metadata.TypeVarBinary && (strings.TrimSpace(first.Charset) == "" || strings.TrimSpace(first.Collation) == "") {
		return nil
	}
	column := *first
	column.Name, column.Type, column.IsNullable = sqlparser.String(expression), resultType, true
	switch name {
	case "LOWER", "UPPER", "LCASE", "UCASE", "TRIM", "LTRIM", "RTRIM", "REVERSE", "SUBSTRING", "SUBSTR", "MID", "SUBSTRING_INDEX":
	case "LEFT", "RIGHT":
		if len(arguments) != 2 {
			return nil
		}
		if count, known := preparedStaticInteger(arguments[1]); known {
			if count <= 0 {
				return nil
			}
			if count < int64(column.Length) {
				column.Length = int(count)
			}
		}
	case "REPEAT":
		if len(arguments) != 2 {
			return nil
		}
		if count, known := preparedStaticInteger(arguments[1]); known {
			if count <= 0 || count > int64(int(^uint(0)>>1))/int64(column.Length) {
				return nil
			}
			column.Length *= int(count)
		}
	case "LPAD", "RPAD":
		if len(arguments) != 3 {
			return nil
		}
		count, known := preparedStaticInteger(arguments[1])
		if !known || count <= 0 || count > int64(int(^uint(0)>>1)) {
			return nil
		}
		padType, typeErr := d.expressionType(arguments[2], shape, nil, 0)
		pad := d.preparedExpressionMetadata(arguments[2], shape, padType)
		if typeErr != nil || pad == nil {
			return nil
		}
		if resultType == metadata.TypeVarBinary {
			if !isSetOperationBinary(firstType) || !isSetOperationBinary(padType) {
				return nil
			}
		} else if !strings.EqualFold(column.Charset, pad.Charset) || !strings.EqualFold(column.Collation, pad.Collation) || column.Charset == "" {
			return nil
		}
		column.Length = int(count)
	default:
		return nil
	}
	return &column
}

func preparedStaticInteger(expression sqlparser.Expr) (int64, bool) {
	switch node := expression.(type) {
	case *sqlparser.SQLVal:
		if node.Type != sqlparser.IntVal {
			return 0, false
		}
		value, err := strconv.ParseInt(string(node.Val), 10, 64)
		return value, err == nil
	case *sqlparser.UnaryExpr:
		value, ok := preparedStaticInteger(node.Expr)
		if !ok {
			return 0, false
		}
		switch strings.TrimSpace(node.Operator) {
		case "+":
			return value, true
		case "-":
			if value == -1<<63 {
				return 0, false
			}
			return -value, true
		}
	case *sqlparser.ParenExpr:
		return preparedStaticInteger(node.Expr)
	}
	return 0, false
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

func preparedExactDecimalLiteralShape(value []byte) (precision, scale int, ok bool) {
	token := strings.TrimSpace(string(value))
	if token == "" || strings.ContainsAny(token, "eE") {
		return 0, 0, false
	}
	if token[0] == '+' || token[0] == '-' {
		token = token[1:]
	}
	if token == "" {
		return 0, 0, false
	}
	dotSeen := false
	for index := 0; index < len(token); index++ {
		character := token[index]
		switch {
		case character >= '0' && character <= '9':
			precision++
			if dotSeen {
				scale++
			}
		case character == '.' && !dotSeen:
			dotSeen = true
		default:
			return 0, 0, false
		}
	}
	if precision == 0 || precision > 65 || scale > 30 {
		return 0, 0, false
	}
	return precision, scale, true
}

type preparedNumericShape struct {
	precision int
	scale     int
	unsigned  bool
}

func (d *preparedSelectDescriber) preparedNumericExpressionShape(expression sqlparser.Expr, shape joinResultShape, depth int) (preparedNumericShape, bool) {
	if expression == nil || depth > 64 {
		return preparedNumericShape{}, false
	}
	typ, err := d.expressionType(expression, shape, nil, depth+1)
	if err != nil {
		return preparedNumericShape{}, false
	}
	if typ != metadata.TypeDecimal {
		if _, integer := preparedIntegerPrecision(typ, false); !integer {
			return preparedNumericShape{}, false
		}
		switch node := expression.(type) {
		case *sqlparser.SQLVal:
			if node.Type != sqlparser.IntVal {
				return preparedNumericShape{}, false
			}
			digits := strings.TrimLeft(string(node.Val), "+-")
			if digits == "" {
				return preparedNumericShape{}, false
			}
			unsigned, known := preparedIntegerLiteralUnsigned(digits)
			if !known {
				return preparedNumericShape{}, false
			}
			return preparedNumericShape{precision: preparedMinInt(len(digits), 65), unsigned: unsigned}, true
		case *sqlparser.ColName:
			columns := shape.columns
			if !node.Qualifier.IsEmpty() {
				columns = shape.qualified[strings.ToLower(node.Qualifier.Name.String())]
			}
			for _, column := range columns {
				if !strings.EqualFold(column.name, node.Name.String()) {
					continue
				}
				unsigned := column.meta != nil && column.meta.IsUnsigned
				precision, ok := preparedIntegerPrecision(typ, unsigned)
				return preparedNumericShape{precision: precision, unsigned: unsigned}, ok
			}
		case *sqlparser.ParenExpr:
			return d.preparedNumericExpressionShape(node.Expr, shape, depth+1)
		case *sqlparser.UnaryExpr:
			switch strings.ToLower(strings.TrimSpace(node.Operator)) {
			case "+", "-":
				numericShape, known := d.preparedNumericExpressionShape(node.Expr, shape, depth+1)
				if strings.TrimSpace(node.Operator) == "-" {
					numericShape.unsigned = false
				}
				return numericShape, known
			}
		case *sqlparser.BinaryExpr:
			left, leftKnown := d.preparedNumericExpressionShape(node.Left, shape, depth+1)
			right, rightKnown := d.preparedNumericExpressionShape(node.Right, shape, depth+1)
			if !leftKnown || !rightKnown {
				return preparedNumericShape{}, false
			}
			unsigned := false
			switch strings.ToLower(strings.TrimSpace(node.Operator)) {
			case "+", "*", "div":
				unsigned = left.unsigned || right.unsigned
			case "-":
				unsigned = (left.unsigned || right.unsigned) && !d.noUnsignedSubtractionMode()
			case "%":
				unsigned = left.unsigned
			default:
				return preparedNumericShape{}, false
			}
			precision, _ := preparedIntegerPrecision(typ, unsigned)
			return preparedNumericShape{precision: precision, unsigned: unsigned}, true
		case *sqlparser.ConvertExpr:
			if node.Type == nil {
				return preparedNumericShape{}, false
			}
			unsigned := strings.Contains(strings.ToUpper(node.Type.Type), "UNSIGNED")
			switch strings.ToUpper(strings.TrimSpace(node.Type.Type)) {
			case "SIGNED", "SIGNED INTEGER":
				return preparedNumericShape{precision: 19}, true
			case "UNSIGNED", "UNSIGNED INTEGER":
				return preparedNumericShape{precision: 20, unsigned: true}, true
			case "INT", "INTEGER":
				return preparedNumericShape{precision: 10, unsigned: unsigned}, true
			case "BIGINT":
				precision := 19
				if unsigned || strings.EqualFold(strings.TrimSpace(node.Type.Type), "UNSIGNED") {
					precision = 20
				}
				return preparedNumericShape{precision: precision, unsigned: unsigned}, true
			}
		}
		return preparedNumericShape{}, false
	}
	switch node := expression.(type) {
	case *sqlparser.SQLVal:
		if node.Type == sqlparser.IntVal {
			if precision, unsigned, ok := preparedIntegerLiteralShape(node.Val); ok && preparedIntegerLiteralNeedsDecimal(node.Val) {
				return preparedNumericShape{precision: precision, unsigned: unsigned}, true
			}
		}
		if node.Type == sqlparser.FloatVal {
			precision, scale, ok := preparedExactDecimalLiteralShape(node.Val)
			return preparedNumericShape{precision: precision, scale: scale}, ok
		}
	case *sqlparser.ColName:
		columns := shape.columns
		if !node.Qualifier.IsEmpty() {
			columns = shape.qualified[strings.ToLower(node.Qualifier.Name.String())]
		}
		for _, column := range columns {
			if strings.EqualFold(column.name, node.Name.String()) && column.typ == metadata.TypeDecimal && column.meta != nil {
				if column.meta.Length > 0 && column.meta.Scale >= 0 && column.meta.Scale <= 30 {
					return preparedNumericShape{precision: column.meta.Length, scale: column.meta.Scale, unsigned: column.meta.IsUnsigned}, true
				}
				return preparedNumericShape{}, false
			}
		}
	case *sqlparser.ParenExpr:
		return d.preparedNumericExpressionShape(node.Expr, shape, depth+1)
	case *sqlparser.UnaryExpr:
		switch strings.ToLower(strings.TrimSpace(node.Operator)) {
		case "+", "-":
			numericShape, known := d.preparedNumericExpressionShape(node.Expr, shape, depth+1)
			if strings.TrimSpace(node.Operator) == "-" {
				numericShape.unsigned = false
			}
			return numericShape, known
		}
	case *sqlparser.ConvertExpr:
		if node.Type == nil || !strings.EqualFold(strings.TrimSpace(node.Type.Type), "DECIMAL") && !strings.EqualFold(strings.TrimSpace(node.Type.Type), "NUMERIC") {
			return preparedNumericShape{}, false
		}
		precision, scale := 10, 0
		if node.Type.Length != nil {
			parsed, parseErr := strconv.Atoi(string(node.Type.Length.Val))
			if parseErr != nil || parsed < 1 || parsed > 65 {
				return preparedNumericShape{}, false
			}
			precision = parsed
		}
		if node.Type.Scale != nil {
			parsed, parseErr := strconv.Atoi(string(node.Type.Scale.Val))
			if parseErr != nil || parsed < 0 || parsed > 30 || parsed > precision {
				return preparedNumericShape{}, false
			}
			scale = parsed
		}
		return preparedNumericShape{precision: precision, scale: scale, unsigned: strings.Contains(strings.ToUpper(node.Type.Type), "UNSIGNED")}, true
	case *sqlparser.BinaryExpr:
		left, leftKnown := d.preparedNumericExpressionShape(node.Left, shape, depth+1)
		right, rightKnown := d.preparedNumericExpressionShape(node.Right, shape, depth+1)
		if !leftKnown || !rightKnown {
			return preparedNumericShape{}, false
		}
		operator := strings.ToLower(strings.TrimSpace(node.Operator))
		switch operator {
		case "+", "-", "%":
			scale := maxInt(left.scale, right.scale)
			if operator == "%" {
				return preparedNumericShape{precision: preparedMinInt(maxInt(left.precision, right.precision), 65), scale: scale, unsigned: left.unsigned}, true
			}
			precision := maxInt(left.precision-left.scale, right.precision-right.scale) + 1 + scale
			unsigned := left.unsigned && right.unsigned
			if operator == "-" && d.noUnsignedSubtractionMode() {
				unsigned = false
			}
			return preparedNumericShape{precision: preparedMinInt(precision, 65), scale: scale, unsigned: unsigned}, true
		case "*":
			return preparedNumericShape{precision: preparedMinInt(left.precision+right.precision, 65), scale: preparedCapDecimalScale(left.scale + right.scale), unsigned: left.unsigned && right.unsigned}, true
		case "/":
			increment := d.divisionPrecisionIncrement()
			return preparedNumericShape{precision: preparedMinInt(left.precision+right.scale+increment, 65), scale: preparedCapDecimalScale(left.scale + increment), unsigned: left.unsigned && right.unsigned}, true
		}
	}
	return preparedNumericShape{}, false
}

func preparedIntegerPrecision(typ metadata.DataType, unsigned bool) (int, bool) {
	switch typ {
	case metadata.TypeTinyInt:
		return 3, true
	case metadata.TypeSmallInt:
		return 5, true
	case metadata.TypeMediumInt:
		if unsigned {
			return 8, true
		}
		return 7, true
	case metadata.TypeInt:
		return 10, true
	case metadata.TypeBigInt:
		if unsigned {
			return 20, true
		}
		return 19, true
	case metadata.TypeYear:
		return 4, true
	default:
		return 0, false
	}
}

func preparedMinInt(value, maximum int) int {
	if value > maximum {
		return maximum
	}
	return value
}

func preparedIntegerLiteralUnsigned(digits string) (bool, bool) {
	value, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return false, false
	}
	return value > uint64(1<<63-1), true
}

func preparedIntegerLiteralShape(value []byte) (precision int, unsigned bool, known bool) {
	digits := strings.TrimLeft(string(value), "0")
	if digits == "" {
		return 1, false, true
	}
	precision = preparedMinInt(len(digits), 65)
	if len(digits) > 20 {
		return precision, false, true
	}
	parsed, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return precision, false, true
	}
	return precision, parsed > uint64(1<<63-1), true
}

func preparedIntegerLiteralNeedsDecimal(value []byte) bool {
	digits := strings.TrimLeft(string(value), "0")
	if digits == "" {
		return false
	}
	if len(digits) > 20 {
		return true
	}
	_, err := strconv.ParseUint(digits, 10, 64)
	return err != nil
}

func preparedIntegerUnaryMinusNeedsDecimal(expression sqlparser.Expr) bool {
	for {
		parenthesized, ok := expression.(*sqlparser.ParenExpr)
		if !ok || parenthesized == nil {
			break
		}
		expression = parenthesized.Expr
	}
	literal, ok := expression.(*sqlparser.SQLVal)
	if !ok || literal == nil || literal.Type != sqlparser.IntVal {
		return false
	}
	value, err := strconv.ParseUint(strings.TrimLeft(string(literal.Val), "0"), 10, 64)
	return err == nil && value > uint64(1)<<63
}

func preparedIntegerMetadataLength(unsigned bool) int {
	if unsigned {
		return 20
	}
	return 19
}

func (d *preparedSelectDescriber) noUnsignedSubtractionMode() bool {
	if d == nil || d.ctx == nil || d.ctx.Session == nil {
		return false
	}
	mode, _ := d.ctx.Session.GetParamByName("sql_mode").(string)
	for _, name := range strings.Split(mode, ",") {
		if strings.EqualFold(strings.TrimSpace(name), "NO_UNSIGNED_SUBTRACTION") {
			return true
		}
	}
	return false
}

func (d *preparedSelectDescriber) divisionPrecisionIncrement() int {
	if d != nil && d.ctx != nil && d.ctx.Session != nil {
		switch value := d.ctx.Session.GetParamByName("div_precision_increment").(type) {
		case string:
			if parsed, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && parsed >= 0 && parsed <= 30 {
				return parsed
			}
		case int:
			if value >= 0 && value <= 30 {
				return value
			}
		case int64:
			if value >= 0 && value <= 30 {
				return int(value)
			}
		case uint64:
			if value <= 30 {
				return int(value)
			}
		}
	}
	return 4
}

func preparedCapDecimalScale(scale int) int {
	if scale > 30 {
		return 30
	}
	return scale
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
			if preparedIntegerLiteralNeedsDecimal(expr.Val) {
				return metadata.TypeDecimal, nil
			}
			return metadata.TypeBigInt, nil
		case sqlparser.FloatVal:
			if strings.ContainsAny(string(expr.Val), "eE") {
				return metadata.TypeDouble, nil
			}
			return metadata.TypeDecimal, nil
		case sqlparser.StrVal:
			return metadata.TypeVarchar, nil
		case sqlparser.HexVal, sqlparser.HexNum, sqlparser.BitVal:
			return metadata.TypeVarBinary, nil
		}
	case *sqlparser.NullVal:
		return metadata.TypeNull, nil
	case *sqlparser.ExtractExpr:
		return metadata.TypeBigInt, nil
	case *sqlparser.MatchExpr:
		return metadata.TypeDouble, nil
	case *sqlparser.GroupConcatExpr:
		return metadata.TypeLongText, nil
	case *sqlparser.SubstrExpr:
		source, err := d.expressionType(expr.Name, shape, ctes, depth+1)
		if err != nil {
			return "", err
		}
		if isSetOperationBinary(source) {
			return metadata.TypeVarBinary, nil
		}
		return metadata.TypeVarchar, nil
	case *sqlparser.BinaryExpr:
		left, err := d.expressionType(expr.Left, shape, ctes, depth+1)
		if err != nil {
			return "", err
		}
		right, err := d.expressionType(expr.Right, shape, ctes, depth+1)
		if err != nil {
			return "", err
		}
		operator := strings.ToLower(strings.TrimSpace(expr.Operator))
		switch operator {
		case "&", "|", "^", "<<", ">>", "div":
			return metadata.TypeBigInt, nil
		case "/":
			if left == metadata.TypeFloat || left == metadata.TypeDouble || right == metadata.TypeFloat || right == metadata.TypeDouble {
				return metadata.TypeDouble, nil
			}
			if isSetOperationNumeric(left) && isSetOperationNumeric(right) {
				return metadata.TypeDecimal, nil
			}
			return metadata.TypeDouble, nil
		case "+", "-", "*", "%":
			if !isSetOperationNumeric(left) || !isSetOperationNumeric(right) {
				return metadata.TypeDouble, nil
			}
			if left == metadata.TypeDouble || right == metadata.TypeDouble || left == metadata.TypeFloat || right == metadata.TypeFloat {
				return metadata.TypeDouble, nil
			}
			if left == metadata.TypeDecimal || right == metadata.TypeDecimal {
				return metadata.TypeDecimal, nil
			}
			return metadata.TypeBigInt, nil
		default:
			return "", fmt.Errorf("prepared arithmetic operator %q is unsupported", expr.Operator)
		}
	case *sqlparser.UnaryExpr:
		operand, err := d.expressionType(expr.Expr, shape, ctes, depth+1)
		if err != nil {
			return "", err
		}
		switch strings.ToLower(strings.TrimSpace(expr.Operator)) {
		case "+", "-":
			if isSetOperationNumeric(operand) {
				if strings.TrimSpace(expr.Operator) == "-" && operand == metadata.TypeBigInt && preparedIntegerUnaryMinusNeedsDecimal(expr.Expr) {
					return metadata.TypeDecimal, nil
				}
				return operand, nil
			}
			return metadata.TypeDouble, nil
		case "~", "!":
			return metadata.TypeBigInt, nil
		case "binary", "binary ", "_binary", "_binary ":
			return metadata.TypeVarBinary, nil
		default:
			return "", fmt.Errorf("prepared unary operator %q is unsupported", expr.Operator)
		}
	case *sqlparser.ParenExpr:
		return d.expressionType(expr.Expr, shape, ctes, depth+1)
	case *sqlparser.CollateExpr:
		return d.expressionType(expr.Expr, shape, ctes, depth+1)
	case *sqlparser.ConvertExpr:
		if expr.Type == nil {
			return "", fmt.Errorf("prepared CAST/CONVERT expression has no target type")
		}
		switch strings.ToUpper(strings.TrimSpace(expr.Type.Type)) {
		case "SIGNED", "SIGNED INTEGER", "INT", "INTEGER", "BIGINT", "UNSIGNED", "UNSIGNED INTEGER":
			return metadata.TypeBigInt, nil
		case "DECIMAL", "NUMERIC":
			return metadata.TypeDecimal, nil
		case "FLOAT":
			return metadata.TypeFloat, nil
		case "DOUBLE", "REAL":
			return metadata.TypeDouble, nil
		case "CHAR", "NCHAR", "VARCHAR", "STRING":
			return metadata.TypeVarchar, nil
		case "BINARY":
			return metadata.TypeVarBinary, nil
		case "DATE":
			return metadata.TypeDate, nil
		case "DATETIME":
			return metadata.TypeDateTime, nil
		case "TIMESTAMP":
			return metadata.TypeTimestamp, nil
		case "TIME":
			return metadata.TypeTime, nil
		case "JSON":
			return metadata.TypeJSON, nil
		default:
			return "", fmt.Errorf("prepared CAST/CONVERT target type %q is unsupported", expr.Type.Type)
		}
	case *sqlparser.ConvertUsingExpr:
		if strings.EqualFold(strings.TrimSpace(expr.Type), "binary") {
			return metadata.TypeVarBinary, nil
		}
		return metadata.TypeVarchar, nil
	case *sqlparser.CaseExpr:
		var result metadata.DataType
		mergeResult := func(value sqlparser.Expr) error {
			if value == nil {
				return nil
			}
			if _, isNull := value.(*sqlparser.NullVal); isNull {
				return nil
			}
			typ, err := d.expressionType(value, shape, ctes, depth+1)
			if err != nil {
				return err
			}
			if result == "" {
				result = typ
				return nil
			}
			result, err = mysqlSetOperationColumnType(result, typ)
			return err
		}
		for _, when := range expr.Whens {
			if when == nil {
				continue
			}
			if err := mergeResult(when.Val); err != nil {
				return "", err
			}
		}
		if err := mergeResult(expr.Else); err != nil {
			return "", err
		}
		if result == "" {
			return metadata.TypeVarchar, nil
		}
		return result, nil
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
	if name == "COUNT" && isPreparedCountStar(function) {
		return metadata.TypeBigInt, true, nil
	}
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
	if name == "CONCAT" || name == "CONCAT_WS" {
		minimumArguments := 1
		if name == "CONCAT_WS" {
			minimumArguments = 2
		}
		if len(arguments) < minimumArguments {
			return "", true, fmt.Errorf("prepared function %s requires at least %d arguments", name, minimumArguments)
		}
		for index := range arguments {
			if _, isNull := arguments[index].(*sqlparser.NullVal); isNull {
				continue
			}
			typ, err := argumentType(index)
			if err != nil {
				return "", true, err
			}
			if isSetOperationBinary(typ) {
				return metadata.TypeVarBinary, true, nil
			}
		}
		return metadata.TypeVarchar, true, nil
	}
	if name == "SUBSTRING_INDEX" {
		if len(arguments) != 3 {
			return "", true, fmt.Errorf("prepared function SUBSTRING_INDEX requires 3 arguments")
		}
		inputType, err := argumentType(0)
		if err != nil {
			return "", true, err
		}
		if isSetOperationBinary(inputType) {
			return metadata.TypeVarBinary, true, nil
		}
		return metadata.TypeVarchar, true, nil
	}
	switch name {
	case "LEFT", "RIGHT", "SUBSTRING", "SUBSTR", "MID", "REPEAT", "LOWER", "UPPER", "LCASE", "UCASE", "TRIM", "LTRIM", "RTRIM", "REVERSE", "LPAD", "RPAD":
		if len(arguments) == 0 {
			return "", true, fmt.Errorf("prepared function %s requires arguments", name)
		}
		binaryArguments := []int{0}
		if name == "LPAD" || name == "RPAD" || name == "TRIM" {
			for index := 1; index < len(arguments); index++ {
				binaryArguments = append(binaryArguments, index)
			}
		}
		for _, index := range binaryArguments {
			if index >= len(arguments) {
				continue
			}
			typ, err := argumentType(index)
			if err != nil {
				return "", true, err
			}
			if isSetOperationBinary(typ) {
				return metadata.TypeVarBinary, true, nil
			}
		}
		return metadata.TypeVarchar, true, nil
	}
	if name == "DATE_ADD" || name == "DATE_SUB" || name == "ADDDATE" || name == "SUBDATE" {
		if len(arguments) != 2 {
			return "", true, fmt.Errorf("prepared function %s requires 2 arguments", name)
		}
		unit := ""
		if interval, ok := arguments[1].(*sqlparser.IntervalExpr); ok && interval != nil {
			unit = interval.Unit
		}
		firstType, err := argumentType(0)
		if err != nil {
			if parameter, ok := arguments[0].(*sqlparser.SQLVal); ok && parameter.Type == sqlparser.ValArg {
				return mysqlDateArithmeticParameterType(unit), true, nil
			}
			return "", true, err
		}
		return mysqlDateArithmeticResultType(firstType, unit), true, nil
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
		if name == "ROUND" && (len(arguments) < 1 || len(arguments) > 2) || name == "TRUNCATE" && len(arguments) != 2 {
			return "", true, fmt.Errorf("prepared function %s has an invalid argument count", name)
		}
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
		if len(arguments) != 1 {
			return "", true, fmt.Errorf("prepared function %s requires 1 argument", name)
		}
		typ, err := argumentType(0)
		if err != nil {
			return "", true, err
		}
		if typ == metadata.TypeDecimal {
			numericShape, known := d.preparedNumericExpressionShape(arguments[0], shape, depth+1)
			if known {
				precision := numericShape.precision - numericShape.scale
				if numericShape.scale != 0 {
					precision++
				}
				if precision <= 18 {
					return metadata.TypeBigInt, true, nil
				}
			}
			return metadata.TypeDecimal, true, nil
		}
		if _, integer := setOperationIntegerRank(typ); integer {
			return metadata.TypeBigInt, true, nil
		}
		return metadata.TypeDouble, true, nil
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
	case "SUBSTRING", "SUBSTR", "MID", "SUBSTRING_INDEX", "LEFT", "RIGHT", "REPLACE", "REVERSE", "LOWER", "LCASE", "UPPER", "UCASE", "TRIM", "LTRIM", "RTRIM", "LPAD", "RPAD", "REPEAT", "SPACE", "FORMAT", "QUOTE", "HEX", "BIN", "OCT", "CONV", "SOUNDEX", "MD5", "SHA", "SHA1", "SHA2", "UUID", "BIN_TO_UUID", "INET_NTOA", "INET6_NTOA", "DATE_FORMAT", "TIME_FORMAT", "GET_FORMAT", "DAYNAME", "MONTHNAME":
		return metadata.TypeVarchar, true, nil
	case "INET6_ATON", "UUID_TO_BIN", "RANDOM_BYTES":
		return metadata.TypeVarBinary, true, nil
	case "CURRENT_TIMESTAMP", "NOW", "LOCALTIME", "LOCALTIMESTAMP", "SYSDATE", "UTC_TIMESTAMP":
		if len(arguments) > 1 {
			return "", true, fmt.Errorf("prepared function %s accepts at most one fractional-seconds precision", name)
		}
		if len(arguments) == 1 {
			fsp, known := preparedStaticInteger(arguments[0])
			if !known || fsp < 0 || fsp > 6 {
				return "", true, fmt.Errorf("prepared function %s requires fractional-seconds precision from 0 through 6", name)
			}
		}
		return metadata.TypeDateTime, true, nil
	case "TIMESTAMP":
		if len(arguments) < 1 || len(arguments) > 2 {
			return "", true, fmt.Errorf("prepared function TIMESTAMP requires 1 or 2 arguments")
		}
		return metadata.TypeDateTime, true, nil
	case "ADDTIME", "SUBTIME":
		if len(arguments) != 2 {
			return "", true, fmt.Errorf("prepared function %s requires 2 arguments", name)
		}
		firstType, err := argumentType(0)
		if err != nil {
			if parameter, ok := arguments[0].(*sqlparser.SQLVal); !ok || parameter.Type != sqlparser.ValArg {
				return "", true, err
			}
			firstType = metadata.TypeTime
		}
		switch firstType {
		case metadata.TypeDate, metadata.TypeDateTime, metadata.TypeTimestamp:
			return metadata.TypeDateTime, true, nil
		case metadata.TypeTime:
			return metadata.TypeTime, true, nil
		default:
			return metadata.TypeVarchar, true, nil
		}
	case "CONVERT_TZ":
		if len(arguments) != 3 {
			return "", true, fmt.Errorf("prepared function CONVERT_TZ requires 3 arguments")
		}
		return metadata.TypeDateTime, true, nil
	case "FROM_UNIXTIME":
		if len(arguments) == 1 {
			return metadata.TypeDateTime, true, nil
		}
		if len(arguments) == 2 {
			return metadata.TypeVarchar, true, nil
		}
		return "", true, fmt.Errorf("prepared function FROM_UNIXTIME requires 1 or 2 arguments")
	case "UNIX_TIMESTAMP":
		if len(arguments) > 1 {
			return "", true, fmt.Errorf("prepared function UNIX_TIMESTAMP accepts at most 1 argument")
		}
		if len(arguments) == 1 {
			argumentType, err := argumentType(0)
			if err != nil {
				if parameter, ok := arguments[0].(*sqlparser.SQLVal); !ok || parameter.Type != sqlparser.ValArg {
					return "", true, err
				}
			}
			precision := 0
			if err == nil {
				if argument := d.preparedExpressionMetadata(arguments[0], shape, argumentType); argument != nil {
					precision = min(max(argument.Scale, 0), 6)
				}
			}
			if precision > 0 {
				return metadata.TypeDecimal, true, nil
			}
		}
		return metadata.TypeBigInt, true, nil
	case "TIMESTAMPDIFF":
		if len(arguments) != 3 {
			return "", true, fmt.Errorf("prepared function TIMESTAMPDIFF requires 3 arguments")
		}
		if _, known := preparedTimestampIntervalUnit(arguments[0]); !known {
			return "", true, fmt.Errorf("prepared function TIMESTAMPDIFF has an unsupported interval unit")
		}
		return metadata.TypeBigInt, true, nil
	case "TIMESTAMPADD":
		if len(arguments) != 3 {
			return "", true, fmt.Errorf("prepared function TIMESTAMPADD requires 3 arguments")
		}
		unit, known := preparedTimestampIntervalUnit(arguments[0])
		if !known {
			return "", true, fmt.Errorf("prepared function TIMESTAMPADD has an unsupported interval unit")
		}
		dateType, err := argumentType(2)
		if err != nil {
			if parameter, ok := arguments[2].(*sqlparser.SQLVal); !ok || parameter.Type != sqlparser.ValArg {
				return "", true, err
			}
			dateType = mysqlDateArithmeticParameterType(unit)
		}
		return mysqlDateArithmeticResultType(dateType, unit), true, nil
	case "STR_TO_DATE":
		if len(arguments) != 2 {
			return "", true, fmt.Errorf("prepared function STR_TO_DATE requires 2 arguments")
		}
		resultType, _ := preparedStrToDateShape(arguments)
		return resultType, true, nil
	case "CURRENT_DATE", "CURDATE", "UTC_DATE":
		if len(arguments) != 0 {
			return "", true, fmt.Errorf("prepared function %s does not accept arguments", name)
		}
		return metadata.TypeDate, true, nil
	case "DATE", "LAST_DAY":
		return metadata.TypeDate, true, nil
	case "CURRENT_TIME", "CURTIME", "UTC_TIME":
		if len(arguments) > 1 {
			return "", true, fmt.Errorf("prepared function %s accepts at most one fractional-seconds precision", name)
		}
		if len(arguments) == 1 {
			fsp, known := preparedStaticInteger(arguments[0])
			if !known || fsp < 0 || fsp > 6 {
				return "", true, fmt.Errorf("prepared function %s requires fractional-seconds precision from 0 through 6", name)
			}
		}
		return metadata.TypeTime, true, nil
	case "TIME", "SEC_TO_TIME":
		if len(arguments) != 1 {
			return "", true, fmt.Errorf("prepared function %s requires 1 argument", name)
		}
		return metadata.TypeTime, true, nil
	case "TIMEDIFF":
		if len(arguments) != 2 {
			return "", true, fmt.Errorf("prepared function TIMEDIFF requires 2 arguments")
		}
		return metadata.TypeTime, true, nil
	case "MAKETIME":
		if len(arguments) != 3 {
			return "", true, fmt.Errorf("prepared function MAKETIME requires 3 arguments")
		}
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

func mysqlDateArithmeticResultType(dateType metadata.DataType, unit string) metadata.DataType {
	unit = strings.ToUpper(strings.TrimSpace(unit))
	usesTimeParts := strings.Contains(unit, "HOUR") || strings.Contains(unit, "MINUTE") || strings.Contains(unit, "SECOND") || strings.Contains(unit, "MICROSECOND")
	usesDateParts := strings.Contains(unit, "YEAR") || strings.Contains(unit, "QUARTER") || strings.Contains(unit, "MONTH") || strings.Contains(unit, "WEEK") || strings.Contains(unit, "DAY")
	switch dateType {
	case metadata.TypeDate:
		if usesTimeParts {
			return metadata.TypeDateTime
		}
		return metadata.TypeDate
	case metadata.TypeTime:
		if usesDateParts {
			return metadata.TypeDateTime
		}
		return metadata.TypeTime
	case metadata.TypeDateTime, metadata.TypeTimestamp:
		return metadata.TypeDateTime
	default:
		return metadata.TypeVarchar
	}
}

func mysqlDateArithmeticParameterType(unit string) metadata.DataType {
	unit = strings.ToUpper(strings.TrimSpace(unit))
	usesOnlyDateParts := unit != "" && !strings.Contains(unit, "HOUR") && !strings.Contains(unit, "MINUTE") && !strings.Contains(unit, "SECOND") && !strings.Contains(unit, "MICROSECOND")
	if usesOnlyDateParts {
		return metadata.TypeDate
	}
	return metadata.TypeDateTime
}
