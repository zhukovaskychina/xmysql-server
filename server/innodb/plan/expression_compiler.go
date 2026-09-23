package plan

import (
	"fmt"
	"strings"
)

// CompiledExpression is the reusable evaluator used by hot projection and
// filter paths. It deliberately keeps EvalContext as the input contract so
// compiled and interpreted expressions observe the same row bindings.
type CompiledExpression func(*EvalContext) (interface{}, error)

// CompileExpression compiles the expression subset used most often by scans.
// Expressions outside this subset return ok=false and callers must use
// Expression.Eval, preserving the complete interpreter semantics.
func CompileExpression(expr Expression) (compiled CompiledExpression, ok bool) {
	switch expression := expr.(type) {
	case *Constant:
		value := expression.Value
		return func(*EvalContext) (interface{}, error) { return value, nil }, true
	case *TupleExpression:
		compiledItems := make([]CompiledExpression, len(expression.Exprs))
		for index, item := range expression.Exprs {
			compiled, itemOK := CompileExpression(item)
			if !itemOK {
				return nil, false
			}
			compiledItems[index] = compiled
		}
		return func(ctx *EvalContext) (interface{}, error) {
			values := make([]interface{}, len(compiledItems))
			for index, item := range compiledItems {
				value, err := item(ctx)
				if err != nil {
					return nil, err
				}
				values[index] = value
			}
			return values, nil
		}, true
	case *Column:
		name := expression.Name
		return func(ctx *EvalContext) (interface{}, error) {
			if ctx == nil || ctx.Row == nil {
				return nil, fmt.Errorf("column %s not found", name)
			}
			if value, exists := ctx.Row[name]; exists {
				return value, nil
			}
			for key, value := range ctx.Row {
				if strings.EqualFold(key, name) || normalizeColumnReference(key) == normalizeColumnReference(name) {
					return value, nil
				}
			}
			return nil, fmt.Errorf("column %s not found", name)
		}, true
	case *Function:
		if !isCompiledFunction(expression.FuncName) {
			return nil, false
		}
		compiledArgs := make([]CompiledExpression, len(expression.FuncArgs))
		functionArgs := make([]Expression, len(expression.FuncArgs))
		for index, arg := range expression.FuncArgs {
			compiled, argOK := CompileExpression(arg)
			if !argOK {
				return nil, false
			}
			compiledArgs[index] = compiled
			functionArgs[index] = &compiledExpressionAdapter{source: arg, compiled: compiled}
		}
		function := &Function{
			FuncName:      expression.FuncName,
			FuncArgs:      functionArgs,
			UsingCharset:  expression.UsingCharset,
			CastType:      expression.CastType,
			CastLength:    expression.CastLength,
			HasCastLength: expression.HasCastLength,
			CastScale:     expression.CastScale,
			HasCastScale:  expression.HasCastScale,
		}
		return func(ctx *EvalContext) (interface{}, error) {
			return function.Eval(ctx)
		}, true
	case *BinaryOperation:
		left, leftOK := CompileExpression(expression.Left)
		right, rightOK := CompileExpression(expression.Right)
		if !leftOK || !rightOK {
			return nil, false
		}
		op := expression.Op
		var escape CompiledExpression
		if expression.Escape != nil {
			var escapeOK bool
			escape, escapeOK = CompileExpression(expression.Escape)
			if !escapeOK {
				return nil, false
			}
		}
		return func(ctx *EvalContext) (interface{}, error) {
			leftValue, err := left(ctx)
			if err != nil {
				return nil, err
			}
			rightValue, err := right(ctx)
			if err != nil {
				return nil, err
			}
			if escape != nil && (op == OpLike || op == OpNotLike) {
				escapeValue, escapeErr := escape(ctx)
				if escapeErr != nil {
					return nil, escapeErr
				}
				matched, matchErr := evalLikeWithEscape(leftValue, rightValue, escapeValue)
				if op == OpNotLike && matchErr == nil && matched != nil {
					return !matched.(bool), nil
				}
				return matched, matchErr
			}
			return evalCompiledBinary(op, leftValue, rightValue)
		}, true
	case *UnaryOperation:
		operand, operandOK := CompileExpression(expression.Operand)
		if !operandOK {
			return nil, false
		}
		operator := expression.Operator
		return func(ctx *EvalContext) (interface{}, error) {
			value, err := operand(ctx)
			if err != nil {
				return nil, err
			}
			return evalCompiledUnary(operator, value)
		}, true
	case *NotExpression:
		operand, operandOK := CompileExpression(expression.Operand)
		if !operandOK {
			return nil, false
		}
		return func(ctx *EvalContext) (interface{}, error) {
			value, err := operand(ctx)
			if err != nil || value == nil {
				return value, err
			}
			truth, err := expressionTruthValue(value)
			if err != nil {
				return nil, err
			}
			return !truth, nil
		}, true
	case *CaseExpression:
		var operand CompiledExpression
		if expression.Operand != nil {
			operand, ok = CompileExpression(expression.Operand)
			if !ok {
				return nil, false
			}
		}
		compiledWhens := make([]struct {
			condition CompiledExpression
			value     CompiledExpression
		}, len(expression.Whens))
		for index, when := range expression.Whens {
			condition, conditionOK := CompileExpression(when.Condition)
			value, valueOK := CompileExpression(when.Value)
			if !conditionOK || !valueOK {
				return nil, false
			}
			compiledWhens[index] = struct {
				condition CompiledExpression
				value     CompiledExpression
			}{condition: condition, value: value}
		}
		var elseExpr CompiledExpression
		if expression.Else != nil {
			elseExpr, ok = CompileExpression(expression.Else)
			if !ok {
				return nil, false
			}
		}
		return func(ctx *EvalContext) (interface{}, error) {
			var operandValue interface{}
			var err error
			if operand != nil {
				operandValue, err = operand(ctx)
				if err != nil {
					return nil, err
				}
			}
			for _, when := range compiledWhens {
				condition, conditionErr := when.condition(ctx)
				if conditionErr != nil {
					return nil, conditionErr
				}
				matched := false
				if operand != nil {
					matchedValue, matchErr := evalEQ(operandValue, condition)
					if matchErr != nil {
						return nil, matchErr
					}
					matched = matchedValue == true
				} else {
					matched, err = expressionTruthValue(condition)
					if err != nil {
						return nil, err
					}
				}
				if matched {
					return when.value(ctx)
				}
			}
			if elseExpr == nil {
				return nil, nil
			}
			return elseExpr(ctx)
		}, true
	case *InExpression:
		column, columnOK := CompileExpression(expression.Column)
		if !columnOK {
			return nil, false
		}
		values := append([]interface{}(nil), expression.Values...)
		return func(ctx *EvalContext) (interface{}, error) {
			value, err := column(ctx)
			if err != nil {
				return nil, err
			}
			return evalIn(value, values)
		}, true
	case *LikeExpression:
		column, columnOK := CompileExpression(expression.Column)
		if !columnOK {
			return nil, false
		}
		pattern := expression.Pattern
		return func(ctx *EvalContext) (interface{}, error) {
			value, err := column(ctx)
			if err != nil {
				return nil, err
			}
			return evalLike(value, pattern)
		}, true
	case *IsNullExpression:
		column, columnOK := CompileExpression(expression.Column)
		if !columnOK {
			return nil, false
		}
		isNull := expression.IsNull
		return func(ctx *EvalContext) (interface{}, error) {
			value, err := column(ctx)
			if err != nil {
				return nil, err
			}
			if isNull {
				return value == nil, nil
			}
			return value != nil, nil
		}, true
	case *IsTruthExpression:
		expr, exprOK := CompileExpression(expression.Expr)
		if !exprOK {
			return nil, false
		}
		operator := expression.Operator
		return func(ctx *EvalContext) (interface{}, error) {
			value, err := expr(ctx)
			if err != nil {
				return nil, err
			}
			truth := false
			if value != nil {
				if boolean, booleanOK := value.(bool); booleanOK {
					truth = boolean
				} else if numeric, numericOK := numericComparableValue(value); numericOK {
					truth = numeric != 0
				} else {
					return nil, fmt.Errorf("%s requires a boolean-compatible operand, got %T", operator, value)
				}
			}
			switch strings.ToLower(strings.TrimSpace(operator)) {
			case "is true":
				return truth, nil
			case "is false":
				return value != nil && !truth, nil
			case "is not true":
				return !truth, nil
			case "is not false":
				return value == nil || truth, nil
			default:
				return nil, fmt.Errorf("unsupported boolean predicate: %s", operator)
			}
		}, true
	case *BetweenExpression:
		column, columnOK := CompileExpression(expression.Column)
		if !columnOK {
			return nil, false
		}
		lower, lowerOK := compileValueExpression(expression.Lower, expression.LowerExpr)
		upper, upperOK := compileValueExpression(expression.Upper, expression.UpperExpr)
		if !lowerOK || !upperOK {
			return nil, false
		}
		not := expression.Not
		return func(ctx *EvalContext) (interface{}, error) {
			value, err := column(ctx)
			if err != nil {
				return nil, err
			}
			if value == nil {
				return nil, nil
			}
			lowerValue, err := lower(ctx)
			if err != nil {
				return nil, err
			}
			upperValue, err := upper(ctx)
			if err != nil {
				return nil, err
			}
			ge, err := evalGE(value, lowerValue)
			if err != nil {
				return nil, err
			}
			le, err := evalLE(value, upperValue)
			if err != nil {
				return nil, err
			}
			if ge == nil || le == nil {
				return nil, nil
			}
			matched := ge.(bool) && le.(bool)
			if not {
				return !matched, nil
			}
			return matched, nil
		}, true
	default:
		return nil, false
	}
}

type compiledExpressionAdapter struct {
	source   Expression
	compiled CompiledExpression
}

func (a *compiledExpressionAdapter) Eval(ctx *EvalContext) (interface{}, error) {
	return a.compiled(ctx)
}

func (a *compiledExpressionAdapter) GetType() DataType { return a.source.GetType() }

func (a *compiledExpressionAdapter) String() string { return a.source.String() }

func (a *compiledExpressionAdapter) Children() []Expression { return nil }

func isCompiledFunction(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "CONCAT", "CONCAT_WS", "SUBSTRING", "LOWER", "LCASE", "UPPER", "UCASE",
		"DATABASE", "SCHEMA", "USER", "CURRENT_USER", "SESSION_USER", "SYSTEM_USER", "CURRENT_ROLE", "VERSION", "CONNECTION_ID",
		"LAST_INSERT_ID", "ROW_COUNT",
		"LENGTH", "OCTET_LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "TRIM", "LTRIM", "RTRIM", "REPLACE",
		"LEFT", "RIGHT", "LPAD", "RPAD", "REPEAT", "REVERSE", "ABS", "CEIL", "CEILING",
		"FLOOR", "ROUND", "TRUNCATE", "MOD", "POW", "POWER", "LOG2", "COT", "RADIANS", "DEGREES", "GREATEST", "LEAST",
		"COALESCE", "IFNULL", "ISNULL", "NULLIF", "IF", "DATE_FORMAT", "TIME_FORMAT", "DATEDIFF", "DATE_ADD",
		"DATE_SUB", "TIMESTAMPDIFF", "TIMESTAMPADD", "CAST", "CONVERT", "CONVERT_USING", "JSON_VALID",
		"JSON_EXTRACT", "JSON_VALUE", "JSON_UNQUOTE", "JSON_ARRAY", "JSON_OBJECT", "JSON_CONTAINS",
		"JSON_CONTAINS_PATH", "JSON_OVERLAPS", "JSON_MERGE", "JSON_MERGE_PATCH", "JSON_MERGE_PRESERVE", "JSON_ARRAY_APPEND",
		"JSON_ARRAY_INSERT", "JSON_INSERT", "JSON_SET", "JSON_REPLACE", "JSON_REMOVE",
		"JSON_SEARCH", "JSON_LENGTH", "JSON_TYPE", "JSON_DEPTH", "JSON_KEYS", "JSON_PRETTY", "GET_FORMAT", "CONVERT_TZ",
		"JSON_QUOTE", "JSON_STORAGE_SIZE", "JSON_STORAGE_FREE", "JSON_SCHEMA_VALID", "JSON_SCHEMA_VALIDATION_REPORT", "LOCATE", "INSTR", "SUBSTRING_INDEX", "SPACE", "ASCII", "ORD", "FIND_IN_SET", "CONV", "MAKE_SET", "EXPORT_SET", "SOUNDEX",
		"GTID_SUBSET", "GTID_SUBTRACT", "INTERVAL",
		"BIT_LENGTH", "BIT_COUNT", "ELT", "FIELD", "CHAR", "QUOTE", "HEX", "UNHEX", "BIN", "OCT", "STRCMP",
		"FORMAT", "SQRT", "SIGN", "EXP", "LN", "LOG", "LOG10", "SIN", "COS", "TAN",
		"ASIN", "ACOS", "ATAN", "ATAN2", "PI", "YEAR", "MONTH", "QUARTER", "DAY", "DAYOFMONTH", "HOUR",
		"MINUTE", "SECOND", "MICROSECOND", "DATE", "TIME", "WEEK", "YEARWEEK", "WEEKOFYEAR", "WEEKDAY", "DAYOFWEEK", "DAYOFYEAR", "DAYNAME", "MONTHNAME", "LAST_DAY",
		"CURDATE", "CURRENT_DATE", "UTC_DATE", "CURTIME", "CURRENT_TIME", "LOCALTIME", "UTC_TIME", "NOW", "CURRENT_TIMESTAMP",
		"LOCALTIMESTAMP", "UTC_TIMESTAMP", "SYSDATE", "TO_DAYS", "FROM_DAYS", "TO_SECONDS", "PERIOD_ADD", "PERIOD_DIFF", "RAND", "UUID",
		"EXTRACT_YEAR_MONTH", "EXTRACT_DAY_HOUR", "EXTRACT_DAY_MINUTE", "EXTRACT_DAY_SECOND", "EXTRACT_DAY_MICROSECOND",
		"EXTRACT_HOUR_MINUTE", "EXTRACT_HOUR_SECOND", "EXTRACT_HOUR_MICROSECOND", "EXTRACT_MINUTE_SECOND",
		"EXTRACT_MINUTE_MICROSECOND", "EXTRACT_SECOND_MICROSECOND",
		"STR_TO_DATE", "UNIX_TIMESTAMP", "FROM_UNIXTIME", "TIME_TO_SEC", "SEC_TO_TIME", "MAKEDATE", "MAKETIME",
		"ADDDATE", "SUBDATE", "MD5", "SHA", "SHA1", "SHA2", "CRC32", "INET_ATON", "INET_NTOA", "INET6_ATON", "INET6_NTOA",
		"ADDTIME", "SUBTIME", "TIMESTAMP", "TIMEDIFF",
		"IS_IPV4", "IS_IPV6", "IS_IPV4_COMPAT", "IS_IPV4_MAPPED", "IS_UUID", "UUID_TO_BIN", "BIN_TO_UUID", "TO_BASE64", "FROM_BASE64", "RANDOM_BYTES",
		"ST_GEOMFROMTEXT", "ST_GEOMETRYFROMTEXT", "GEOMFROMTEXT", "ST_POINTFROMTEXT", "POINTFROMTEXT",
		"ST_LINEFROMTEXT", "ST_LINESTRINGFROMTEXT", "ST_POLYFROMTEXT", "ST_POLYGONFROMTEXT", "ST_MPOINTFROMTEXT", "ST_MULTIPOINTFROMTEXT", "ST_MLINEFROMTEXT", "ST_MULTILINESTRINGFROMTEXT", "ST_MPOLYFROMTEXT", "ST_MULTIPOLYGONFROMTEXT", "ST_GEOMCOLLFROMTEXT", "ST_GEOMETRYCOLLECTIONFROMTEXT", "ST_GEOMCOLLFROMTXT",
		"ST_GEOMFROMWKB", "ST_GEOMETRYFROMWKB", "GEOMFROMWKB", "ST_POINTFROMWKB", "POINTFROMWKB",
		"ST_MAKEPOINT", "POINT", "ST_LINESTRING", "LINESTRING", "ST_MAKEENVELOPE", "ST_ASTEXT", "ASTEXT", "ST_ASWKB", "ASWKB", "ST_SRID", "SRID",
		"ST_GEOMFROMGEOJSON", "ST_GEOMETRYFROMGEOJSON", "ST_ASGEOJSON",
		"ST_GEOHASH", "ST_LATFROMGEOHASH", "ST_LONGFROMGEOHASH", "ST_POINTFROMGEOHASH",
		"ST_TRANSFORM",
		"ST_GEOMETRYTYPE", "GEOMETRYTYPE", "ST_AREA", "AREA", "ST_LENGTH", "LENGTH_GEOMETRY", "ST_CENTROID", "CENTROID", "ST_ENVELOPE", "ENVELOPE", "ST_CONVEXHULL", "CONVEXHULL", "ST_BUFFER", "ST_SIMPLIFY", "ST_POINTATDISTANCE", "POINTATDISTANCE", "ST_LINEINTERPOLATEPOINT", "ST_LINEINTERPOLATEPOINTS",
		"ST_LATITUDE", "LATITUDE", "ST_LONGITUDE", "LONGITUDE", "ST_SWAPXY", "SWAPXY", "ST_X", "X", "ST_Y", "Y", "ST_ISEMPTY", "ISEMPTY",
		"ST_DIMENSION", "DIMENSION", "ST_NUMGEOMETRIES", "NUMGEOMETRIES", "ST_NUMPOINTS", "NUMPOINTS", "ST_GEOMETRYN", "GEOMETRYN",
		"ST_POINTN", "POINTN", "ST_STARTPOINT", "STARTPOINT", "ST_ENDPOINT", "ENDPOINT", "ST_NUMINTERIORRING", "NUMINTERIORRING", "ST_EXTERIORRING", "EXTERIORRING", "ST_INTERIORRINGN", "INTERIORRINGN", "ST_ISCLOSED", "ISCLOSED", "ST_ISRING", "ISRING",
		"ST_ISVALID", "ISVALID", "ST_VALIDATE", "VALIDATE", "ST_ISSIMPLE", "ISSIMPLE", "ST_IS_SIMPLE", "IS_SIMPLE", "ST_POINTONSURFACE", "POINTONSURFACE", "ST_EQUALS", "ST_CONTAINS", "ST_WITHIN", "ST_INTERSECTS", "ST_INTERSECTION", "ST_UNION", "ST_DIFFERENCE", "ST_SYMDIFFERENCE", "ST_DISJOINT", "ST_TOUCHES", "ST_OVERLAPS", "ST_CROSSES", "MBRCONTAINS", "MBRWITHIN", "MBRINTERSECTS", "MBREQUALS", "MBRDISJOINT", "ST_DISTANCE", "ST_DISTANCE_SPHERE", "ST_HAUSDORFFDISTANCE", "ST_FRECHETDISTANCE",
		"REGEXP_LIKE", "REGEXP_INSTR", "REGEXP_REPLACE", "REGEXP_SUBSTR":
		return true
	default:
		return false
	}
}

func evalCompiledUnary(operator string, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch strings.TrimSpace(strings.ToLower(operator)) {
	case "+":
		if _, ok := numericComparableValue(value); !ok {
			return nil, fmt.Errorf("unary + requires a numeric operand, got %T", value)
		}
		return value, nil
	case "-":
		switch numeric := value.(type) {
		case int:
			return -numeric, nil
		case int8:
			return -numeric, nil
		case int16:
			return -numeric, nil
		case int32:
			return -numeric, nil
		case int64:
			return -numeric, nil
		case float32:
			return -numeric, nil
		case float64:
			return -numeric, nil
		}
		numeric, ok := numericComparableValue(value)
		if !ok {
			return nil, fmt.Errorf("unary - requires a numeric operand, got %T", value)
		}
		return -numeric, nil
	case "!":
		truth, err := expressionTruthValue(value)
		if err != nil {
			return nil, err
		}
		return !truth, nil
	case "~":
		numeric, ok := numericComparableValue(value)
		if !ok {
			return nil, fmt.Errorf("unary ~ requires a numeric operand, got %T", value)
		}
		return ^int64(numeric), nil
	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", operator)
	}
}

func compileValueExpression(value interface{}, expression Expression) (CompiledExpression, bool) {
	if expression != nil {
		return CompileExpression(expression)
	}
	return func(*EvalContext) (interface{}, error) { return value, nil }, true
}

func evalCompiledBinary(op BinaryOp, left, right interface{}) (interface{}, error) {
	switch op {
	case OpAdd:
		return evalAdd(left, right)
	case OpSub:
		return evalSub(left, right)
	case OpMul:
		return evalMul(left, right)
	case OpDiv:
		return evalDiv(left, right)
	case OpEQ:
		return evalEQ(left, right)
	case OpNE:
		return evalNE(left, right)
	case OpLT:
		return evalLT(left, right)
	case OpLE:
		return evalLE(left, right)
	case OpGT:
		return evalGT(left, right)
	case OpGE:
		return evalGE(left, right)
	case OpAnd:
		return evalAnd(left, right)
	case OpOr:
		return evalOr(left, right)
	case OpLike:
		return evalLike(left, right)
	case OpNotLike:
		matched, err := evalLike(left, right)
		if err != nil || matched == nil {
			return matched, err
		}
		return !matched.(bool), nil
	case OpIn:
		return evalIn(left, right)
	case OpNotIn:
		return evalNotIn(left, right)
	case OpNullSafeEQ:
		return evalNullSafeEQ(left, right)
	case OpIntDiv:
		return evalIntDiv(left, right)
	case OpMod:
		return evalMod(left, right)
	case OpBitAnd:
		return evalBitwise(left, right, "&")
	case OpBitOr:
		return evalBitwise(left, right, "|")
	case OpBitXor:
		return evalBitwise(left, right, "^")
	case OpShiftLeft:
		return evalBitwise(left, right, "<<")
	case OpShiftRight:
		return evalBitwise(left, right, ">>")
	case OpRegexp:
		return evalRegexp(left, right, false)
	case OpNotRegexp:
		return evalRegexp(left, right, true)
	default:
		return nil, fmt.Errorf("unknown binary operator: %d", op)
	}
}
