package plan

import (
	"crypto/md5"
	cryptorand "crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"math/big"
	"math/bits"
	mathrand "math/rand"
	"net"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/ianaindex"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	unicodeencoding "golang.org/x/text/encoding/unicode"
)

// DataType 数据类型
type DataType int

const (
	TypeUnknown DataType = iota
	TypeInt
	TypeFloat
	TypeString
	TypeDateTime
	TypeBoolean
	TypeNull
)

// Expression 表达式接口
type Expression interface {
	// Eval 计算表达式的值
	Eval(ctx *EvalContext) (interface{}, error)
	// GetType 返回表达式的类型
	GetType() DataType
	// String 返回表达式的字符串表示
	String() string
	// Children 返回子表达式
	Children() []Expression
}

// EvalContext 表达式计算上下文
type EvalContext struct {
	Row                map[string]interface{}
	UUIDShortGenerator *UUIDShortGenerator
	// SessionValues carries statement/session state needed by scalar
	// compatibility functions. It is intentionally a plain map so the plan
	// package does not depend on the wire/session implementation.
	SessionValues map[string]interface{}
}

// BaseExpression 基础表达式实现
type BaseExpression struct {
	resultType DataType
	children   []Expression
}

func (e *BaseExpression) GetType() DataType {
	return e.resultType
}

func (e *BaseExpression) Children() []Expression {
	return e.children
}

// Column 列引用表达式
type Column struct {
	BaseExpression
	Name string
}

func (c *Column) Eval(ctx *EvalContext) (interface{}, error) {
	name := "<nil>"
	if c != nil {
		name = c.Name
	}
	if c == nil || ctx == nil || ctx.Row == nil {
		return nil, fmt.Errorf("column %s not found", name)
	}
	if val, ok := ctx.Row[c.Name]; ok {
		return val, nil
	}
	for name, val := range ctx.Row {
		if strings.EqualFold(name, c.Name) || normalizeColumnReference(name) == normalizeColumnReference(c.Name) {
			return val, nil
		}
	}
	return nil, fmt.Errorf("column %s not found", name)
}

func normalizeColumnReference(name string) string {
	parts := strings.Split(strings.TrimSpace(name), ".")
	for index, part := range parts {
		parts[index] = strings.Trim(strings.TrimSpace(part), "`")
	}
	return strings.ToLower(strings.Join(parts, "."))
}

func (c *Column) String() string {
	return c.Name
}

// Constant 常量表达式
type Constant struct {
	BaseExpression
	Value interface{}
}

func (c *Constant) Eval(ctx *EvalContext) (interface{}, error) {
	return c.Value, nil
}

func (c *Constant) String() string {
	return fmt.Sprintf("%v", c.Value)
}

// TupleExpression evaluates a value tuple whose members may be row-dependent
// expressions. Constant-only tuples are still represented by Constant for the
// optimizer's index-lookup path.
type TupleExpression struct {
	BaseExpression
	Exprs []Expression
}

func (t *TupleExpression) Eval(ctx *EvalContext) (interface{}, error) {
	values := make([]interface{}, len(t.Exprs))
	for index, expr := range t.Exprs {
		value, err := expr.Eval(ctx)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return values, nil
}

func (t *TupleExpression) String() string {
	parts := make([]string, len(t.Exprs))
	for index, expr := range t.Exprs {
		parts[index] = expr.String()
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// BinaryOp 二元运算符类型
type BinaryOp int

const (
	OpAdd BinaryOp = iota
	OpSub
	OpMul
	OpDiv
	OpEQ
	OpNE
	OpLT
	OpLE
	OpGT
	OpGE
	OpAnd
	OpOr
	OpLike
	OpNotLike
	OpIn
	OpNotIn
	OpNullSafeEQ
	OpIntDiv
	OpMod
	OpBitAnd
	OpBitOr
	OpBitXor
	OpShiftLeft
	OpShiftRight
	OpRegexp
	OpNotRegexp
)

// BinaryOperation 二元运算表达式
type BinaryOperation struct {
	BaseExpression
	Op       BinaryOp
	Operator string // 新增: 用于支持字符串操作符
	Left     Expression
	Right    Expression
	Escape   Expression // optional LIKE/NOT LIKE escape expression
}

func (b *BinaryOperation) Eval(ctx *EvalContext) (interface{}, error) {
	left, err := b.Left.Eval(ctx)
	if err != nil {
		return nil, err
	}
	right, err := b.Right.Eval(ctx)
	if err != nil {
		return nil, err
	}
	var escape interface{}
	if (b.Op == OpLike || b.Op == OpNotLike) && b.Escape != nil {
		escape, err = b.Escape.Eval(ctx)
		if err != nil {
			return nil, err
		}
	}

	switch b.Op {
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
		if b.Escape != nil {
			return evalLikeWithEscape(left, right, escape)
		}
		return evalLike(left, right)
	case OpNotLike:
		var matched interface{}
		if b.Escape != nil {
			matched, err = evalLikeWithEscape(left, right, escape)
		} else {
			matched, err = evalLike(left, right)
		}
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
		return nil, fmt.Errorf("unknown binary operator: %v", b.Op)
	}
}

func (b *BinaryOperation) String() string {
	result := fmt.Sprintf("(%s %s %s)", b.Left.String(), b.convertOperatorToString(), b.Right.String())
	if b.Escape != nil && (b.Op == OpLike || b.Op == OpNotLike) {
		result = fmt.Sprintf("(%s %s %s ESCAPE %s)", b.Left.String(), b.convertOperatorToString(), b.Right.String(), b.Escape.String())
	}
	return result
}

func (b *BinaryOperation) convertOperatorToString() string {
	switch b.Op {
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpEQ:
		return "="
	case OpNE:
		return "!="
	case OpLT:
		return "<"
	case OpLE:
		return "<="
	case OpGT:
		return ">"
	case OpGE:
		return ">="
	case OpAnd:
		return "AND"
	case OpOr:
		return "OR"
	case OpLike:
		return "LIKE"
	case OpNotLike:
		return "NOT LIKE"
	case OpIn:
		return "IN"
	case OpNotIn:
		return "NOT IN"
	case OpNullSafeEQ:
		return "<=>"
	case OpIntDiv:
		return "DIV"
	case OpMod:
		return "%"
	case OpBitAnd:
		return "&"
	case OpBitOr:
		return "|"
	case OpBitXor:
		return "^"
	case OpShiftLeft:
		return "<<"
	case OpShiftRight:
		return ">>"
	case OpRegexp:
		return "REGEXP"
	case OpNotRegexp:
		return "NOT REGEXP"
	default:
		return "UNKNOWN"
	}
}

// UnaryOperation represents a unary arithmetic or boolean operation.
type UnaryOperation struct {
	BaseExpression
	Operator string
	Operand  Expression
}

func (u *UnaryOperation) Eval(ctx *EvalContext) (interface{}, error) {
	value, err := u.Operand.Eval(ctx)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	switch strings.TrimSpace(strings.ToLower(u.Operator)) {
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
		return nil, fmt.Errorf("unsupported unary operator: %s", u.Operator)
	}
}

func (u *UnaryOperation) String() string {
	return fmt.Sprintf("%s%s", u.Operator, u.Operand.String())
}

// CaseWhen is one WHEN ... THEN ... arm of a CASE expression.
type CaseWhen struct {
	Condition Expression
	Value     Expression
}

// CaseExpression evaluates both searched and simple CASE expressions.
type CaseExpression struct {
	BaseExpression
	Operand Expression
	Whens   []CaseWhen
	Else    Expression
}

func (c *CaseExpression) Eval(ctx *EvalContext) (interface{}, error) {
	var operand interface{}
	var err error
	if c.Operand != nil {
		operand, err = c.Operand.Eval(ctx)
		if err != nil {
			return nil, err
		}
	}

	for _, when := range c.Whens {
		condition, err := when.Condition.Eval(ctx)
		if err != nil {
			return nil, err
		}
		matched := false
		if c.Operand != nil {
			matchedValue, err := evalEQ(operand, condition)
			if err != nil {
				return nil, err
			}
			matched = matchedValue == true
		} else {
			matched, err = expressionTruthValue(condition)
			if err != nil {
				return nil, err
			}
		}
		if matched {
			return when.Value.Eval(ctx)
		}
	}
	if c.Else == nil {
		return nil, nil
	}
	return c.Else.Eval(ctx)
}

func (c *CaseExpression) String() string {
	parts := make([]string, 0, len(c.Whens)+2)
	if c.Operand != nil {
		parts = append(parts, c.Operand.String())
	}
	for _, when := range c.Whens {
		parts = append(parts, fmt.Sprintf("WHEN %s THEN %s", when.Condition.String(), when.Value.String()))
	}
	if c.Else != nil {
		parts = append(parts, "ELSE "+c.Else.String())
	}
	return "CASE " + strings.Join(parts, " ") + " END"
}

func expressionTruthValue(value interface{}) (bool, error) {
	if value == nil {
		return false, nil
	}
	if boolean, ok := value.(bool); ok {
		return boolean, nil
	}
	if numeric, ok := numericComparableValue(value); ok {
		return numeric != 0, nil
	}
	return false, fmt.Errorf("boolean context requires a numeric or boolean value, got %T", value)
}

// Function 函数表达式
type Function struct {
	BaseExpression
	FuncName      string
	FuncArgs      []Expression
	Distinct      bool
	Separator     string
	UsingCharset  string
	CastType      string
	CastLength    int64
	HasCastLength bool
	CastScale     int64
	HasCastScale  bool
}

func isAggregateFunctionName(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "JSON_ARRAYAGG", "JSON_OBJECTAGG", "ANY_VALUE", "BIT_AND", "BIT_OR", "BIT_XOR", "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "VARIANCE":
		return true
	default:
		return false
	}
}

func (f *Function) Name() string {
	return f.FuncName
}

func (f *Function) Args() []Expression {
	return f.FuncArgs
}

func (f *Function) Eval(ctx *EvalContext) (interface{}, error) {
	// A physical aggregate emits its computed value under the aggregate
	// expression name. Reuse that value when a later HAVING/selection node
	// evaluates the same aggregate; otherwise COUNT(col) would be recomputed
	// against one already-aggregated scalar and incorrectly become 1.
	if ctx != nil && isAggregateFunctionName(f.FuncName) {
		if value, ok := ctx.Row[f.String()]; ok {
			return value, nil
		}
	}
	args := make([]interface{}, len(f.FuncArgs))
	for i, arg := range f.FuncArgs {
		val, err := arg.Eval(ctx)
		if err != nil {
			return nil, err
		}
		args[i] = val
	}

	switch strings.ToUpper(f.FuncName) {
	case "DATABASE", "SCHEMA":
		if len(args) != 0 {
			return nil, fmt.Errorf("%s requires no arguments", f.FuncName)
		}
		if ctx == nil || ctx.SessionValues == nil {
			return nil, nil
		}
		return ctx.SessionValues["database"], nil
	case "USER", "SESSION_USER", "SYSTEM_USER":
		if len(args) != 0 {
			return nil, fmt.Errorf("%s requires no arguments", f.FuncName)
		}
		if ctx == nil || ctx.SessionValues == nil {
			return nil, nil
		}
		return ctx.SessionValues["user"], nil
	case "CURRENT_ROLE":
		if len(args) != 0 {
			return nil, fmt.Errorf("%s requires no arguments", f.FuncName)
		}
		if ctx == nil || ctx.SessionValues == nil {
			return "NONE", nil
		}
		return ctx.SessionValues["current_role"], nil
	case "CURRENT_USER":
		if len(args) != 0 {
			return nil, fmt.Errorf("%s requires no arguments", f.FuncName)
		}
		if ctx == nil || ctx.SessionValues == nil {
			return nil, nil
		}
		return ctx.SessionValues["current_user"], nil
	case "VERSION":
		if len(args) != 0 {
			return nil, fmt.Errorf("%s requires no arguments", f.FuncName)
		}
		if ctx == nil || ctx.SessionValues == nil {
			return "8.0.32", nil
		}
		return ctx.SessionValues["version"], nil
	case "CONNECTION_ID":
		if len(args) != 0 {
			return nil, fmt.Errorf("%s requires no arguments", f.FuncName)
		}
		if ctx == nil || ctx.SessionValues == nil {
			return int64(0), nil
		}
		return ctx.SessionValues["connection_id"], nil
	case "LAST_INSERT_ID":
		return evalLastInsertID(args, ctx)
	case "ROW_COUNT":
		return evalRowCount(args, ctx)
	case "COUNT":
		return evalCount(args)
	case "SUM":
		return evalSum(args)
	case "AVG":
		return evalAvg(args)
	case "MAX":
		return evalMax(args)
	case "MIN":
		return evalMin(args)
	case "CONCAT":
		return evalConcat(args)
	case "GROUP_CONCAT":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		return fmt.Sprint(args[0]), nil
	case "JSON_ARRAYAGG":
		return evalJSONArrayAggregate(args)
	case "JSON_OBJECTAGG":
		return evalJSONObjectAggregate(args)
	case "ANY_VALUE":
		if len(args) != 1 {
			return nil, fmt.Errorf("ANY_VALUE requires exactly 1 argument")
		}
		if values, ok := args[0].([]interface{}); ok {
			if len(values) == 0 {
				return nil, nil
			}
			return values[0], nil
		}
		return args[0], nil
	case "BIT_AND", "BIT_OR", "BIT_XOR":
		return evalBitAggregate(strings.ToUpper(f.FuncName), args)
	case "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "VARIANCE":
		return evalStatAggregate(strings.ToUpper(f.FuncName), args)
	case "SUBSTRING":
		return evalSubstring(args)
	case "CAST", "CONVERT":
		var (
			value interface{}
			err   error
		)
		if f.UsingCharset != "" {
			convertArgs := args
			if f.HasCastLength && len(args) == 1 && args[0] != nil {
				convertArgs = []interface{}{truncateCastValue(args[0], f.CastType, f.CastLength)}
			}
			value, err = evalConvertUsing(convertArgs, f.UsingCharset)
		} else {
			value, err = evalCast(args)
		}
		if err != nil || value == nil {
			return value, err
		}
		if f.HasCastLength && f.UsingCharset == "" {
			value = truncateCastValue(value, f.CastType, f.CastLength)
		}
		return applyCastScale(value, f.CastType, f.CastScale, f.HasCastScale), nil
	case "CURDATE", "CURRENT_DATE":
		if len(args) != 0 {
			return nil, fmt.Errorf("%s requires no arguments", f.FuncName)
		}
		return time.Now().Format("2006-01-02"), nil
	case "UTC_DATE":
		if len(args) != 0 {
			return nil, fmt.Errorf("%s requires no arguments", f.FuncName)
		}
		return time.Now().UTC().Format("2006-01-02"), nil
	case "CURTIME", "CURRENT_TIME", "LOCALTIME":
		if len(args) != 0 {
			return nil, fmt.Errorf("%s requires no arguments", f.FuncName)
		}
		return time.Now().Format("15:04:05"), nil
	case "UTC_TIME":
		if len(args) != 0 {
			return nil, fmt.Errorf("%s requires no arguments", f.FuncName)
		}
		return time.Now().UTC().Format("15:04:05"), nil
	case "SYSDATE":
		if len(args) > 1 {
			return nil, fmt.Errorf("%s accepts zero or one argument", f.FuncName)
		}
		now := time.Now()
		if len(args) == 1 && args[0] != nil {
			fsp, err := integerArg(args[0])
			if err != nil || fsp < 0 || fsp > 6 {
				if err == nil {
					err = fmt.Errorf("fractional seconds precision must be between 0 and 6")
				}
				return nil, err
			}
			now = truncateTimeFraction(now, fsp)
		}
		return now, nil
	case "TO_DAYS", "FROM_DAYS", "TO_SECONDS", "PERIOD_ADD", "PERIOD_DIFF":
		return evalCalendarFunction(strings.ToUpper(f.FuncName), args)
	case "NOW", "CURRENT_TIMESTAMP", "LOCALTIMESTAMP":
		return time.Now(), nil
	case "UTC_TIMESTAMP":
		return time.Now().UTC(), nil
	case "RAND":
		if len(args) == 0 {
			return mathrand.Float64(), nil
		}
		if len(args) != 1 || args[0] == nil {
			return nil, fmt.Errorf("RAND accepts zero or one numeric argument")
		}
		seed, err := numericArg(args[0])
		if err != nil {
			return nil, err
		}
		return mathrand.New(mathrand.NewSource(int64(seed))).Float64(), nil
	case "UUID":
		if len(args) != 0 {
			return nil, fmt.Errorf("UUID requires no arguments")
		}
		bytes := make([]byte, 16)
		if _, err := cryptorand.Read(bytes); err != nil {
			return nil, fmt.Errorf("generate UUID: %w", err)
		}
		bytes[6] = (bytes[6] & 0x0f) | 0x40
		bytes[8] = (bytes[8] & 0x3f) | 0x80
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:16]), nil
	case "UUID_SHORT":
		if len(args) != 0 {
			return nil, fmt.Errorf("UUID_SHORT requires no arguments")
		}
		generator := defaultUUIDShortGenerator()
		if ctx != nil && ctx.UUIDShortGenerator != nil {
			generator = ctx.UUIDShortGenerator
		}
		return generator.Next()
	case "COALESCE":
		for _, arg := range args {
			if arg != nil {
				return arg, nil
			}
		}
		return nil, nil
	case "IFNULL":
		if len(args) != 2 {
			return nil, fmt.Errorf("IFNULL requires exactly 2 arguments")
		}
		if args[0] == nil {
			return args[1], nil
		}
		return args[0], nil
	case "ISNULL":
		if len(args) != 1 {
			return nil, fmt.Errorf("ISNULL requires exactly 1 argument")
		}
		if args[0] == nil {
			return int64(1), nil
		}
		return int64(0), nil
	case "NULLIF":
		if len(args) != 2 {
			return nil, fmt.Errorf("NULLIF requires exactly 2 arguments")
		}
		equal, err := evalEQ(args[0], args[1])
		if err != nil {
			return nil, err
		}
		if equal == true {
			return nil, nil
		}
		return args[0], nil
	case "IF":
		if len(args) != 3 {
			return nil, fmt.Errorf("IF requires exactly 3 arguments")
		}
		truth, ok := sqlBool(args[0])
		if !ok {
			return nil, fmt.Errorf("IF condition has unsupported type %T", args[0])
		}
		if truth {
			return args[1], nil
		}
		return args[2], nil
	case "LOWER", "LCASE":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		return strings.ToLower(fmt.Sprint(args[0])), nil
	case "UPPER", "UCASE":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		return strings.ToUpper(fmt.Sprint(args[0])), nil
	case "LENGTH", "OCTET_LENGTH":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		return int64(len([]byte(fmt.Sprint(args[0])))), nil
	case "CHAR_LENGTH", "CHARACTER_LENGTH":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		return int64(len([]rune(fmt.Sprint(args[0])))), nil
	case "TRIM":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		return strings.TrimSpace(fmt.Sprint(args[0])), nil
	case "LTRIM":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		return strings.TrimLeft(fmt.Sprint(args[0]), " \t\r\n"), nil
	case "RTRIM":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		return strings.TrimRight(fmt.Sprint(args[0]), " \t\r\n"), nil
	case "REPLACE":
		if len(args) != 3 || args[0] == nil || args[1] == nil || args[2] == nil {
			return nil, nil
		}
		return strings.ReplaceAll(fmt.Sprint(args[0]), fmt.Sprint(args[1]), fmt.Sprint(args[2])), nil
	case "LEFT":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		n, err := integerArg(args[1])
		if err != nil {
			return nil, err
		}
		runes := []rune(fmt.Sprint(args[0]))
		if n < 0 {
			n = 0
		}
		if n > int64(len(runes)) {
			n = int64(len(runes))
		}
		return string(runes[:n]), nil
	case "RIGHT":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		n, err := integerArg(args[1])
		if err != nil {
			return nil, err
		}
		runes := []rune(fmt.Sprint(args[0]))
		if n < 0 {
			n = 0
		}
		if n > int64(len(runes)) {
			n = int64(len(runes))
		}
		return string(runes[len(runes)-int(n):]), nil
	case "LOCATE", "INSTR":
		if len(args) < 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		needle, haystack := fmt.Sprint(args[0]), fmt.Sprint(args[1])
		if strings.EqualFold(strings.ToUpper(f.FuncName), "INSTR") {
			needle, haystack = fmt.Sprint(args[1]), fmt.Sprint(args[0])
		}
		return int64(strings.Index(haystack, needle) + 1), nil
	case "REVERSE":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		runes := []rune(fmt.Sprint(args[0]))
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil
	case "ABS", "ROUND", "TRUNCATE", "FLOOR", "CEIL", "CEILING", "SQRT", "POWER", "POW", "MOD", "SIGN", "EXP", "LN", "LOG", "LOG2", "LOG10", "SIN", "COS", "TAN", "COT", "ASIN", "ACOS", "ATAN", "ATAN2", "RADIANS", "DEGREES":
		return evalNumericFunction(strings.ToUpper(f.FuncName), args)
	case "PI":
		if len(args) != 0 {
			return nil, fmt.Errorf("PI requires no arguments")
		}
		return math.Pi, nil
	case "FORMAT":
		return evalFormatNumber(args)
	case "CONCAT_WS", "SUBSTRING_INDEX":
		return evalExtendedStringFunction(strings.ToUpper(f.FuncName), args)
	case "LPAD", "RPAD", "REPEAT", "SPACE", "ASCII", "ORD", "FIND_IN_SET", "BIT_LENGTH", "BIT_COUNT", "ELT", "FIELD", "CHAR", "QUOTE", "HEX", "UNHEX", "BIN", "OCT", "STRCMP", "MAKE_SET", "EXPORT_SET", "SOUNDEX":
		return evalStringCompatibilityFunction(strings.ToUpper(f.FuncName), args)
	case "CONV":
		return evalConv(args)
	case "INTERVAL":
		return evalInterval(args)
	case "MD5", "SHA", "SHA1", "SHA2", "CRC32", "INET_ATON", "INET_NTOA", "INET6_ATON", "INET6_NTOA", "IS_IPV4", "IS_IPV6", "IS_IPV4_COMPAT", "IS_IPV4_MAPPED", "IS_UUID", "UUID_TO_BIN", "BIN_TO_UUID", "TO_BASE64", "FROM_BASE64":
		return evalNetworkAndDigestFunction(strings.ToUpper(f.FuncName), args)
	case "RANDOM_BYTES":
		return evalRandomBytes(args)
	case "GREATEST", "LEAST":
		return evalMinMaxFunction(strings.ToUpper(f.FuncName), args)
	case "YEAR", "MONTH", "DAY", "DAYOFMONTH", "HOUR", "MINUTE", "SECOND", "MICROSECOND", "DATE", "TIME", "QUARTER", "WEEK", "YEARWEEK", "WEEKOFYEAR", "WEEKDAY", "DAYOFWEEK", "DAYOFYEAR", "DAYNAME", "MONTHNAME", "LAST_DAY":
		return evalDatePart(strings.ToUpper(f.FuncName), args)
	case "EXTRACT_YEAR_MONTH", "EXTRACT_DAY_HOUR", "EXTRACT_DAY_MINUTE", "EXTRACT_DAY_SECOND", "EXTRACT_DAY_MICROSECOND",
		"EXTRACT_HOUR_MINUTE", "EXTRACT_HOUR_SECOND", "EXTRACT_HOUR_MICROSECOND", "EXTRACT_MINUTE_SECOND",
		"EXTRACT_MINUTE_MICROSECOND", "EXTRACT_SECOND_MICROSECOND":
		return evalCompositeDatePart(strings.TrimPrefix(strings.ToUpper(f.FuncName), "EXTRACT_"), args)
	case "DATE_FORMAT", "TIME_FORMAT":
		return evalDateFormat(args)
	case "GET_FORMAT":
		return evalGetFormat(args)
	case "CONVERT_TZ":
		return evalConvertTZ(args)
	case "STR_TO_DATE":
		return evalStrToDate(args)
	case "DATE_ADD", "ADDDATE", "DATE_SUB", "SUBDATE":
		return evalDateArithmetic(strings.ToUpper(f.FuncName), args)
	case "ADDTIME", "SUBTIME":
		return evalTimeArithmetic(strings.ToUpper(f.FuncName), args)
	case "TIMESTAMP":
		return evalTimestampConstructor(args)
	case "DATEDIFF":
		return evalDateDiff(args)
	case "TIMEDIFF":
		return evalTimeDiff(args)
	case "TIMESTAMPDIFF":
		return evalTimestampDiff(args)
	case "TIMESTAMPADD":
		if len(args) != 3 || args[0] == nil || args[1] == nil || args[2] == nil {
			return nil, nil
		}
		unit := strings.ToUpper(strings.TrimSpace(compatString(args[0])))
		amount, err := integerArg(args[1])
		if err != nil {
			return nil, err
		}
		return evalDateArithmetic("DATE_ADD", []interface{}{args[2], fmt.Sprintf("%d %s", amount, unit)})
	case "UNIX_TIMESTAMP":
		return evalUnixTimestamp(args)
	case "FROM_UNIXTIME":
		return evalFromUnixTime(args)
	case "TIME_TO_SEC", "SEC_TO_TIME":
		return evalTimeConversionFunction(strings.ToUpper(f.FuncName), args)
	case "MAKEDATE", "MAKETIME":
		return evalDateTimeConstructor(strings.ToUpper(f.FuncName), args)
	case "CONVERT_USING":
		return evalConvertUsing(args, f.UsingCharset)
	case "JSON_VALUE_COMPAT":
		return evalJSONValueCompatibility(args)
	case "JSON_EXTRACT", "JSON_VALUE", "JSON_UNQUOTE", "JSON_ARRAY", "JSON_OBJECT", "JSON_CONTAINS", "JSON_CONTAINS_PATH", "JSON_OVERLAPS", "JSON_MERGE", "JSON_MERGE_PATCH", "JSON_MERGE_PRESERVE", "JSON_ARRAY_APPEND", "JSON_ARRAY_INSERT", "JSON_INSERT", "JSON_SET", "JSON_REPLACE", "JSON_REMOVE", "JSON_SEARCH", "JSON_LENGTH", "JSON_TYPE", "JSON_DEPTH", "JSON_KEYS", "JSON_PRETTY", "JSON_VALID", "JSON_QUOTE", "JSON_STORAGE_SIZE", "JSON_STORAGE_FREE", "JSON_SCHEMA_VALID", "JSON_SCHEMA_VALIDATION_REPORT":
		return evalJSONFunction(strings.ToUpper(f.FuncName), args)
	case "GTID_SUBSET", "GTID_SUBTRACT":
		return evalGTIDFunction(strings.ToUpper(f.FuncName), args)
	case "ST_GEOMFROMTEXT", "ST_GEOMETRYFROMTEXT", "GEOMFROMTEXT", "ST_POINTFROMTEXT", "POINTFROMTEXT",
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
		"ST_ISVALID", "ISVALID", "ST_VALIDATE", "VALIDATE", "ST_ISSIMPLE", "ISSIMPLE", "ST_IS_SIMPLE", "IS_SIMPLE", "ST_POINTONSURFACE", "POINTONSURFACE", "ST_EQUALS", "ST_CONTAINS", "ST_WITHIN", "ST_INTERSECTS", "ST_INTERSECTION", "ST_UNION", "ST_DIFFERENCE", "ST_SYMDIFFERENCE", "ST_DISJOINT", "ST_TOUCHES", "ST_OVERLAPS", "ST_CROSSES", "MBRCONTAINS", "MBRWITHIN", "MBRINTERSECTS", "MBREQUALS", "MBRDISJOINT", "ST_DISTANCE", "ST_DISTANCE_SPHERE", "ST_HAUSDORFFDISTANCE", "ST_FRECHETDISTANCE":
		return evalSpatialFunction(strings.ToUpper(f.FuncName), args)
	case "REGEXP_LIKE":
		if len(args) < 2 || len(args) > 3 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		matchType := ""
		if len(args) == 3 && args[2] != nil {
			matchType = compatString(args[2])
		}
		pattern, err := compileMySQLRegexp(fmt.Sprint(args[1]), matchType)
		if err != nil {
			return nil, err
		}
		return pattern.MatchString(fmt.Sprint(args[0])), nil
	case "REGEXP_INSTR":
		return evalRegexpInstr(args)
	case "REGEXP_REPLACE", "REGEXP_SUBSTR":
		return evalRegexpCompatibilityFunction(strings.ToUpper(f.FuncName), args)
	default:
		return nil, fmt.Errorf("unknown function: %s", f.FuncName)
	}
}

func evalLastInsertID(args []interface{}, ctx *EvalContext) (interface{}, error) {
	if len(args) > 1 {
		return nil, fmt.Errorf("LAST_INSERT_ID accepts zero or one argument")
	}
	if len(args) == 0 {
		if ctx == nil || ctx.SessionValues == nil {
			return int64(0), nil
		}
		return lastInsertIDResult(ctx.SessionValues["last_insert_id"]), nil
	}
	if args[0] == nil {
		return nil, nil
	}
	value, err := integerArg(args[0])
	if err != nil {
		return nil, err
	}
	if value < 0 {
		return nil, fmt.Errorf("LAST_INSERT_ID argument must not be negative")
	}
	if ctx != nil {
		if ctx.SessionValues == nil {
			ctx.SessionValues = make(map[string]interface{})
		}
		ctx.SessionValues["last_insert_id"] = uint64(value)
	}
	return value, nil
}

func evalRowCount(args []interface{}, ctx *EvalContext) (interface{}, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("ROW_COUNT accepts no arguments")
	}
	if ctx == nil || ctx.SessionValues == nil {
		return int64(0), nil
	}
	switch value := ctx.SessionValues["row_count"].(type) {
	case int64:
		return value, nil
	case int:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case uint64:
		if value > uint64(math.MaxInt64) {
			return int64(0), nil
		}
		return int64(value), nil
	case uint:
		if uint64(value) > uint64(math.MaxInt64) {
			return int64(0), nil
		}
		return int64(value), nil
	case uint32:
		return int64(value), nil
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err == nil {
			return parsed, nil
		}
	}
	return int64(0), nil
}

func lastInsertIDResult(value interface{}) interface{} {
	switch number := value.(type) {
	case uint64:
		if number > uint64(math.MaxInt64) {
			return number
		}
		return int64(number)
	case int64:
		return number
	case int:
		return int64(number)
	case uint:
		if uint64(number) > uint64(math.MaxInt64) {
			return uint64(number)
		}
		return int64(number)
	case string:
		parsed, err := strconv.ParseUint(strings.TrimSpace(number), 10, 64)
		if err == nil {
			return lastInsertIDResult(parsed)
		}
	}
	return int64(0)
}

func truncateTimeFraction(value time.Time, fsp int64) time.Time {
	if fsp <= 0 {
		return value.Truncate(time.Second)
	}
	if fsp >= 6 {
		return value.Truncate(time.Microsecond)
	}
	quantum := time.Second
	for index := int64(0); index < fsp; index++ {
		quantum /= 10
	}
	return value.Truncate(quantum)
}

func sqlBool(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case int64:
		return v != 0, true
	case int:
		return v != 0, true
	case float64:
		return v != 0, true
	case nil:
		return false, true
	default:
		return false, false
	}
}

func integerArg(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0, fmt.Errorf("numeric argument overflows int64")
		}
		return int64(v), nil
	default:
		return 0, fmt.Errorf("expected numeric argument, got %T", value)
	}
}

func numericArg(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("expected numeric argument, got %T", value)
	}
}

func evalMinMaxFunction(name string, args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("%s requires at least one argument", name)
	}
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
	}
	allNumeric := true
	for _, arg := range args {
		if _, err := numericArg(arg); err != nil {
			allNumeric = false
			break
		}
	}
	best := args[0]
	for _, arg := range args[1:] {
		var take bool
		if allNumeric {
			left, _ := numericArg(best)
			right, _ := numericArg(arg)
			if name == "GREATEST" {
				take = right > left
			} else {
				take = right < left
			}
		} else {
			left, right := fmt.Sprint(best), fmt.Sprint(arg)
			if name == "GREATEST" {
				take = right > left
			} else {
				take = right < left
			}
		}
		if take {
			best = arg
		}
	}
	return best, nil
}

func parseCompatTime(value interface{}) (time.Time, error) {
	if value == nil {
		return time.Time{}, fmt.Errorf("date value is NULL")
	}
	if parsed, ok := value.(time.Time); ok {
		return parsed, nil
	}
	text := strings.TrimSpace(compatString(value))
	formats := []string{
		"2006-01-02 15:04:05.999999", "2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999", "2006-01-02T15:04:05",
		"2006-01-02", "15:04:05", time.RFC3339,
	}
	for _, format := range formats {
		if parsed, err := time.ParseInLocation(format, text, time.UTC); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid date value %q", text)
}

func compatString(value interface{}) string {
	if bytes, ok := value.([]byte); ok {
		return string(bytes)
	}
	return fmt.Sprint(value)
}

func evalDateFormat(args []interface{}) (interface{}, error) {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil, nil
	}
	parsed, err := parseCompatTime(args[0])
	if err != nil {
		return nil, err
	}
	format := compatString(args[1])
	var out strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] != '%' || i+1 >= len(format) {
			out.WriteByte(format[i])
			continue
		}
		i++
		switch format[i] {
		case 'Y':
			out.WriteString(parsed.Format("2006"))
		case 'y':
			out.WriteString(parsed.Format("06"))
		case 'm':
			out.WriteString(parsed.Format("01"))
		case 'c':
			out.WriteString(strconv.Itoa(int(parsed.Month())))
		case 'd':
			out.WriteString(parsed.Format("02"))
		case 'e':
			out.WriteString(strconv.Itoa(parsed.Day()))
		case 'D':
			out.WriteString(fmt.Sprintf("%d%s", parsed.Day(), dateOrdinalSuffix(parsed.Day())))
		case 'j':
			out.WriteString(fmt.Sprintf("%03d", parsed.YearDay()))
		case 'H':
			out.WriteString(parsed.Format("15"))
		case 'k':
			out.WriteString(strconv.Itoa(parsed.Hour()))
		case 'h', 'I':
			out.WriteString(parsed.Format("03"))
		case 'l':
			out.WriteString(strconv.Itoa(parsed.Hour() % 12))
		case 'i':
			out.WriteString(parsed.Format("04"))
		case 's', 'S':
			out.WriteString(parsed.Format("05"))
		case 'r':
			out.WriteString(parsed.Format("03:04:05 PM"))
		case 'T':
			out.WriteString(parsed.Format("15:04:05"))
		case 'f':
			out.WriteString(parsed.Format("000000"))
		case 'p':
			out.WriteString(parsed.Format("PM"))
		case 'M':
			out.WriteString(parsed.Format("January"))
		case 'b':
			out.WriteString(parsed.Format("Jan"))
		case 'W':
			out.WriteString(parsed.Format("Monday"))
		case 'a':
			out.WriteString(parsed.Format("Mon"))
		case 'w':
			out.WriteString(strconv.Itoa(int(parsed.Weekday())))
		case 'U', 'u', 'V', 'v':
			mode := 0
			switch format[i] {
			case 'u':
				mode = 1
			case 'V':
				mode = 2
			case 'v':
				mode = 3
			}
			_, week := compatWeekOfYear(parsed, mode)
			out.WriteString(fmt.Sprintf("%02d", week))
		case 'X', 'x':
			mode := 2
			if format[i] == 'x' {
				mode = 3
			}
			weekYear, _ := compatWeekOfYear(parsed, mode)
			out.WriteString(strconv.Itoa(weekYear))
		case '%':
			out.WriteByte('%')
		default:
			out.WriteByte(format[i])
		}
	}
	return out.String(), nil
}

func dateOrdinalSuffix(day int) string {
	if day%100 >= 11 && day%100 <= 13 {
		return "th"
	}
	switch day % 10 {
	case 1:
		return "st"
	case 2:
		return "nd"
	case 3:
		return "rd"
	default:
		return "th"
	}
}

func mysqlDateLayout(format string) (string, error) {
	var out strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] != '%' || i+1 >= len(format) {
			// time.Parse layouts use literal punctuation directly. Regex
			// escaping here would turn a decimal point into `\\.`, which is
			// not a literal dot to the time parser.
			out.WriteByte(format[i])
			continue
		}
		i++
		switch format[i] {
		case 'Y':
			out.WriteString("2006")
		case 'y':
			out.WriteString("06")
		case 'm':
			out.WriteString("01")
		case 'c':
			out.WriteString("1")
		case 'd':
			out.WriteString("02")
		case 'e':
			out.WriteString("2")
		case 'H':
			out.WriteString("15")
		case 'k':
			out.WriteString("15")
		case 'h', 'I':
			out.WriteString("03")
		case 'l':
			out.WriteString("3")
		case 'i':
			out.WriteString("04")
		case 's', 'S':
			out.WriteString("05")
		case 'r':
			out.WriteString("03:04:05 PM")
		case 'T':
			out.WriteString("15:04:05")
		case 'f':
			out.WriteString("000000")
		case 'p':
			out.WriteString("PM")
		case 'M':
			out.WriteString("January")
		case 'b':
			out.WriteString("Jan")
		case 'W':
			out.WriteString("Monday")
		case 'a':
			out.WriteString("Mon")
		case 'j':
			out.WriteString("002")
		case '%':
			out.WriteByte('%')
		default:
			return "", fmt.Errorf("unsupported STR_TO_DATE format %%%c", format[i])
		}
	}
	return out.String(), nil
}

func evalStrToDate(args []interface{}) (interface{}, error) {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil, nil
	}
	layout, err := mysqlDateLayout(compatString(args[1]))
	if err != nil {
		return nil, err
	}
	parsed, err := time.ParseInLocation(layout, compatString(args[0]), time.UTC)
	if err != nil {
		return nil, err
	}
	if parsed.Nanosecond() != 0 {
		return parsed.Format("2006-01-02 15:04:05.000000"), nil
	}
	return parsed.Format("2006-01-02 15:04:05"), nil
}

func evalUnixTimestamp(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return time.Now().Unix(), nil
	}
	if len(args) != 1 || args[0] == nil {
		return nil, nil
	}
	parsed, err := parseCompatTime(args[0])
	if err != nil {
		return nil, err
	}
	return parsed.Unix(), nil
}

func evalFromUnixTime(args []interface{}) (interface{}, error) {
	if len(args) < 1 || len(args) > 2 || args[0] == nil {
		return nil, nil
	}
	seconds, err := integerArg(args[0])
	if err != nil {
		return nil, err
	}
	parsed := time.Unix(seconds, 0).UTC()
	if len(args) == 2 && args[1] != nil {
		return evalDateFormat([]interface{}{parsed, args[1]})
	}
	return parsed.Format("2006-01-02 15:04:05"), nil
}

func evalGetFormat(args []interface{}) (interface{}, error) {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil, nil
	}
	typeName := strings.ToUpper(strings.TrimSpace(compatString(args[0])))
	formatName := strings.ToUpper(strings.TrimSpace(compatString(args[1])))
	formats := map[string]map[string]string{
		"DATE": {
			"USA": "%m.%d.%Y", "JIS": "%Y-%m-%d", "ISO": "%Y-%m-%d", "EUR": "%d.%m.%Y", "INTERNAL": "%Y%m%d",
		},
		"DATETIME": {
			"USA": "%Y-%m-%d %H.%i.%s", "JIS": "%Y-%m-%d %H:%i:%s", "ISO": "%Y-%m-%d %H:%i:%s", "EUR": "%Y-%m-%d %H.%i.%s", "INTERNAL": "%Y%m%d%H%i%s",
		},
		"TIME": {
			"USA": "%h:%i:%s %p", "JIS": "%H:%i:%s", "ISO": "%H:%i:%s", "EUR": "%H.%i.%s", "INTERNAL": "%H%i%s",
		},
	}
	if values := formats[typeName]; values != nil {
		if format, ok := values[formatName]; ok {
			return format, nil
		}
	}
	return nil, nil
}

func evalConvertTZ(args []interface{}) (interface{}, error) {
	if len(args) != 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil, nil
	}
	parsed, err := parseCompatTime(args[0])
	if err != nil {
		return nil, err
	}
	fromLocation, ok := loadCompatTimezone(compatString(args[1]))
	if !ok {
		return nil, nil
	}
	toLocation, ok := loadCompatTimezone(compatString(args[2]))
	if !ok {
		return nil, nil
	}
	wallClock := time.Date(parsed.Year(), parsed.Month(), parsed.Day(), parsed.Hour(), parsed.Minute(), parsed.Second(), parsed.Nanosecond(), fromLocation)
	converted := wallClock.In(toLocation)
	if converted.Nanosecond() != 0 {
		return converted.Format("2006-01-02 15:04:05.999999"), nil
	}
	return converted.Format("2006-01-02 15:04:05"), nil
}

func loadCompatTimezone(value string) (*time.Location, bool) {
	if offset, ok := parseFixedTimezoneOffset(value); ok {
		return time.FixedZone("compat", offset), true
	}
	location, err := time.LoadLocation(strings.TrimSpace(value))
	if err != nil {
		return nil, false
	}
	return location, true
}

func parseFixedTimezoneOffset(value string) (int, bool) {
	text := strings.ToUpper(strings.TrimSpace(value))
	if text == "UTC" || text == "GMT" || text == "Z" {
		return 0, true
	}
	match := regexp.MustCompile(`^([+-])(\d{2})(?::?(\d{2}))?$`).FindStringSubmatch(text)
	if len(match) != 4 {
		return 0, false
	}
	hours, err := strconv.Atoi(match[2])
	if err != nil || hours > 14 {
		return 0, false
	}
	minutes := 0
	if match[3] != "" {
		minutes, err = strconv.Atoi(match[3])
		if err != nil || minutes > 59 {
			return 0, false
		}
	}
	seconds := (hours*60 + minutes) * 60
	if match[1] == "-" {
		seconds = -seconds
	}
	return seconds, true
}

func evalTimeConversionFunction(name string, args []interface{}) (interface{}, error) {
	if len(args) != 1 || args[0] == nil {
		return nil, nil
	}
	if name == "TIME_TO_SEC" {
		match := regexp.MustCompile(`^([+-]?)([0-9]+):([0-5][0-9]):([0-5][0-9])(?:\.([0-9]{1,6}))?$`).FindStringSubmatch(strings.TrimSpace(compatString(args[0])))
		if len(match) == 0 {
			return nil, nil
		}
		hours, err := strconv.ParseInt(match[2], 10, 64)
		if err != nil {
			return nil, err
		}
		minutes := int64(match[3][0]-'0')*10 + int64(match[3][1]-'0')
		remaining := int64(match[4][0]-'0')*10 + int64(match[4][1]-'0')
		seconds := hours*3600 + minutes*60 + remaining
		if match[5] == "" {
			if match[1] == "-" {
				return -seconds, nil
			}
			return seconds, nil
		}
		fraction, err := strconv.ParseInt(match[5]+strings.Repeat("0", 6-len(match[5])), 10, 64)
		if err != nil {
			return nil, err
		}
		result := float64(seconds) + float64(fraction)/1e6
		if match[1] == "-" {
			result = -result
		}
		return result, nil
	}

	seconds, err := numericArg(args[0])
	if err != nil {
		return nil, err
	}
	negative := seconds < 0
	seconds = math.Abs(seconds)
	whole := math.Floor(seconds)
	microseconds := int64(math.Round((seconds - whole) * 1e6))
	if microseconds == 1_000_000 {
		whole++
		microseconds = 0
	}
	hours := int64(whole) / 3600
	minutes := (int64(whole) % 3600) / 60
	remaining := int64(whole) % 60
	result := fmt.Sprintf("%d:%02d:%02d", hours, minutes, remaining)
	if microseconds > 0 {
		result += fmt.Sprintf(".%06d", microseconds)
	}
	if negative && (whole > 0 || microseconds > 0) {
		result = "-" + result
	}
	return result, nil
}

func evalDateTimeConstructor(name string, args []interface{}) (interface{}, error) {
	if name == "MAKEDATE" {
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		year, err := integerArg(args[0])
		if err != nil {
			return nil, err
		}
		dayOfYear, err := integerArg(args[1])
		if err != nil {
			return nil, err
		}
		if year < 1 || year > 9999 || dayOfYear < 1 || dayOfYear > 366 {
			return nil, nil
		}
		result := time.Date(int(year), time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(dayOfYear-1))
		if int64(result.Year()) != year {
			return nil, nil
		}
		return result.Format("2006-01-02"), nil
	}

	if len(args) != 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil, nil
	}
	hour, err := integerArg(args[0])
	if err != nil {
		return nil, err
	}
	minute, err := integerArg(args[1])
	if err != nil {
		return nil, err
	}
	second, err := numericArg(args[2])
	if err != nil {
		return nil, err
	}
	if hour < -838 || hour > 838 || minute < 0 || minute > 59 || second < 0 || second >= 60 {
		return nil, nil
	}
	wholeSecond := int64(math.Floor(second))
	microseconds := int64(math.Round((second - float64(wholeSecond)) * 1e6))
	if microseconds == 1_000_000 {
		wholeSecond++
		microseconds = 0
	}
	if wholeSecond >= 60 {
		return nil, nil
	}
	negative := hour < 0
	if hour == 0 && second < 0 {
		negative = true
	}
	hour = int64(math.Abs(float64(hour)))
	result := fmt.Sprintf("%d:%02d:%02d", hour, minute, wholeSecond)
	if microseconds > 0 {
		result += fmt.Sprintf(".%06d", microseconds)
	}
	if negative {
		result = "-" + result
	}
	return result, nil
}

func parseCompatInterval(value interface{}) (int64, string, error) {
	if value == nil {
		return 0, "", fmt.Errorf("interval value is NULL")
	}
	parts := strings.Fields(strings.TrimSpace(compatString(value)))
	if len(parts) == 3 && strings.EqualFold(parts[0], "INTERVAL") {
		parts = parts[1:]
	}
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid interval %q", compatString(value))
	}
	amount, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("invalid interval amount %q", parts[0])
	}
	return amount, strings.ToUpper(parts[1]), nil
}

func formatCompatDateResult(value interface{}, parsed time.Time) string {
	text := strings.TrimSpace(compatString(value))
	if len(text) == len("2006-01-02") {
		return parsed.Format("2006-01-02")
	}
	return parsed.Format("2006-01-02 15:04:05")
}

func evalDateArithmetic(name string, args []interface{}) (interface{}, error) {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil, nil
	}
	parsed, err := parseCompatTime(args[0])
	if err != nil {
		return nil, err
	}
	amount, unit, err := parseCompatInterval(args[1])
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(name, "SUB") {
		amount = -amount
	}
	switch unit {
	case "YEAR", "YEARS":
		parsed = parsed.AddDate(int(amount), 0, 0)
	case "QUARTER", "QUARTERS":
		parsed = parsed.AddDate(0, int(amount)*3, 0)
	case "MONTH", "MONTHS":
		parsed = parsed.AddDate(0, int(amount), 0)
	case "WEEK", "WEEKS":
		parsed = parsed.AddDate(0, 0, int(amount)*7)
	case "DAY", "DAYS":
		parsed = parsed.AddDate(0, 0, int(amount))
	case "HOUR", "HOURS":
		parsed = parsed.Add(time.Duration(amount) * time.Hour)
	case "MINUTE", "MINUTES":
		parsed = parsed.Add(time.Duration(amount) * time.Minute)
	case "SECOND", "SECONDS":
		parsed = parsed.Add(time.Duration(amount) * time.Second)
	case "MICROSECOND", "MICROSECONDS":
		parsed = parsed.Add(time.Duration(amount) * time.Microsecond)
	default:
		return nil, fmt.Errorf("unsupported interval unit %q", unit)
	}
	return formatCompatDateResult(args[0], parsed), nil
}

func evalTimeArithmetic(name string, args []interface{}) (interface{}, error) {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil, nil
	}
	base, err := parseCompatTime(args[0])
	if err != nil {
		return nil, err
	}
	delta, err := parseCompatDuration(args[1])
	if err != nil {
		return nil, err
	}
	if name == "SUBTIME" {
		delta = -delta
	}
	return formatCompatTimeArithmetic(args[0], base.Add(delta)), nil
}

func evalTimestampConstructor(args []interface{}) (interface{}, error) {
	if len(args) != 1 && len(args) != 2 || args[0] == nil {
		return nil, nil
	}
	base, err := parseCompatTime(args[0])
	if err != nil {
		return nil, err
	}
	if len(args) == 2 {
		if args[1] == nil {
			return nil, nil
		}
		delta, deltaErr := parseCompatDuration(args[1])
		if deltaErr != nil {
			return nil, deltaErr
		}
		base = time.Date(base.Year(), base.Month(), base.Day(), 0, 0, 0, 0, time.UTC).Add(delta)
	}
	return formatCompatDateTime(base), nil
}

func parseCompatDuration(value interface{}) (time.Duration, error) {
	text := strings.TrimSpace(compatString(value))
	if text == "" {
		return 0, fmt.Errorf("invalid time interval %q", text)
	}
	sign := time.Duration(1)
	if text[0] == '+' || text[0] == '-' {
		if text[0] == '-' {
			sign = -1
		}
		text = strings.TrimSpace(text[1:])
	}
	days := int64(0)
	if fields := strings.SplitN(text, " ", 2); len(fields) == 2 {
		parsedDays, err := strconv.ParseInt(strings.TrimSpace(fields[0]), 10, 64)
		if err != nil || parsedDays < 0 {
			return 0, fmt.Errorf("invalid time interval %q", compatString(value))
		}
		days, text = parsedDays, strings.TrimSpace(fields[1])
	}
	parts := strings.Split(text, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid time interval %q", compatString(value))
	}
	hours, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || hours < 0 {
		return 0, fmt.Errorf("invalid time interval %q", compatString(value))
	}
	minutes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || minutes < 0 || minutes > 59 {
		return 0, fmt.Errorf("invalid time interval %q", compatString(value))
	}
	seconds, err := strconv.ParseFloat(parts[2], 64)
	if err != nil || seconds < 0 || seconds >= 60 {
		return 0, fmt.Errorf("invalid time interval %q", compatString(value))
	}
	total := (float64(days)*24+float64(hours))*float64(time.Hour) + float64(minutes)*float64(time.Minute) + seconds*float64(time.Second)
	return sign * time.Duration(math.Round(total)), nil
}

func formatCompatDateTime(value time.Time) string {
	result := value.Format("2006-01-02 15:04:05")
	if value.Nanosecond() != 0 {
		result += fmt.Sprintf(".%06d", value.Nanosecond()/1000)
	}
	return result
}

func formatCompatTimeArithmetic(original interface{}, value time.Time) string {
	text := strings.TrimSpace(compatString(original))
	if strings.Contains(text, "-") || strings.Contains(text, "T") || strings.Contains(text, " ") {
		return formatCompatDateTime(value)
	}
	result := value.Format("15:04:05")
	if value.Nanosecond() != 0 {
		result += fmt.Sprintf(".%06d", value.Nanosecond()/1000)
	}
	return result
}

func evalDateDiff(args []interface{}) (interface{}, error) {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil, nil
	}
	left, err := parseCompatTime(args[0])
	if err != nil {
		return nil, err
	}
	right, err := parseCompatTime(args[1])
	if err != nil {
		return nil, err
	}
	leftDate := time.Date(left.Year(), left.Month(), left.Day(), 0, 0, 0, 0, time.UTC)
	rightDate := time.Date(right.Year(), right.Month(), right.Day(), 0, 0, 0, 0, time.UTC)
	return int64(leftDate.Sub(rightDate) / (24 * time.Hour)), nil
}

func evalTimeDiff(args []interface{}) (interface{}, error) {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil, nil
	}
	left, err := parseCompatTimeOrDuration(args[0])
	if err != nil {
		return nil, err
	}
	right, err := parseCompatTimeOrDuration(args[1])
	if err != nil {
		return nil, err
	}
	return formatCompatDuration(left - right), nil
}

func parseCompatTimeOrDuration(value interface{}) (time.Duration, error) {
	if parsed, err := parseCompatTime(value); err == nil {
		return time.Duration(parsed.Hour())*time.Hour + time.Duration(parsed.Minute())*time.Minute +
			time.Duration(parsed.Second())*time.Second + time.Duration(parsed.Nanosecond()), nil
	}
	return parseCompatDuration(value)
}

func formatCompatDuration(value time.Duration) string {
	negative := value < 0
	if negative {
		value = -value
	}
	hours := value / time.Hour
	value %= time.Hour
	minutes := value / time.Minute
	value %= time.Minute
	seconds := value / time.Second
	microseconds := (value % time.Second) / time.Microsecond
	result := fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
	if microseconds != 0 {
		result += fmt.Sprintf(".%06d", microseconds)
	}
	if negative && result != "00:00:00" {
		return "-" + result
	}
	return result
}

func evalTimestampDiff(args []interface{}) (interface{}, error) {
	if len(args) != 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil, nil
	}
	unit := strings.ToUpper(strings.TrimSpace(compatString(args[0])))
	start, err := parseCompatTime(args[1])
	if err != nil {
		return nil, err
	}
	end, err := parseCompatTime(args[2])
	if err != nil {
		return nil, err
	}
	if unit == "YEAR" || unit == "QUARTER" || unit == "MONTH" {
		months := int64((end.Year()-start.Year())*12 + int(end.Month()) - int(start.Month()))
		if end.Day() < start.Day() || (end.Day() == start.Day() && end.Sub(start) < 0) {
			months--
		}
		switch unit {
		case "YEAR":
			return months / 12, nil
		case "QUARTER":
			return months / 3, nil
		default:
			return months, nil
		}
	}
	delta := end.Sub(start)
	divisor := time.Second
	switch unit {
	case "WEEK":
		divisor = 7 * 24 * time.Hour
	case "DAY":
		divisor = 24 * time.Hour
	case "HOUR":
		divisor = time.Hour
	case "MINUTE":
		divisor = time.Minute
	case "SECOND":
		divisor = time.Second
	case "MICROSECOND":
		return delta.Microseconds(), nil
	default:
		return nil, fmt.Errorf("unsupported TIMESTAMPDIFF unit %q", unit)
	}
	return int64(delta / divisor), nil
}

func evalCast(args []interface{}) (interface{}, error) {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil, nil
	}
	value := args[0]
	typeName := strings.ToUpper(strings.TrimSpace(compatString(args[1])))
	switch typeName {
	case "SIGNED", "SIGNED INTEGER", "INT", "INTEGER", "BIGINT":
		text := strings.TrimSpace(compatString(value))
		if number, err := strconv.ParseInt(text, 10, 64); err == nil {
			return number, nil
		}
		if number, err := strconv.ParseFloat(text, 64); err == nil && !math.IsNaN(number) && !math.IsInf(number, 0) {
			return int64(math.Trunc(number)), nil
		}
		return int64(0), nil
	case "UNSIGNED", "UNSIGNED INTEGER":
		text := strings.TrimSpace(compatString(value))
		if number, err := strconv.ParseUint(text, 10, 64); err == nil {
			return number, nil
		}
		if number, err := strconv.ParseFloat(text, 64); err == nil && number >= 0 && !math.IsNaN(number) && !math.IsInf(number, 0) {
			return uint64(math.Trunc(number)), nil
		}
		return uint64(0), nil
	case "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL":
		number, err := strconv.ParseFloat(strings.TrimSpace(compatString(value)), 64)
		if err != nil {
			return float64(0), nil
		}
		return number, nil
	case "CHAR", "NCHAR", "VARCHAR", "STRING":
		return compatString(value), nil
	case "BINARY":
		return string([]byte(compatString(value))), nil
	case "DATE":
		parsed, err := parseCompatTime(value)
		if err != nil {
			return nil, err
		}
		return parsed.Format("2006-01-02"), nil
	case "DATETIME", "TIMESTAMP":
		parsed, err := parseCompatTime(value)
		if err != nil {
			return nil, err
		}
		return parsed.Format("2006-01-02 15:04:05"), nil
	case "TIME":
		parsed, err := parseCompatTime(value)
		if err != nil {
			return nil, err
		}
		return parsed.Format("15:04:05"), nil
	case "JSON":
		var decoded interface{}
		if err := json.Unmarshal([]byte(compatString(value)), &decoded); err != nil {
			return nil, err
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported CAST target %s", typeName)
	}
}

func truncateCastValue(value interface{}, castType string, length int64) interface{} {
	if length < 0 {
		return value
	}
	typeName := strings.ToUpper(strings.TrimSpace(castType))
	if typeName != "CHAR" && typeName != "NCHAR" && typeName != "VARCHAR" && typeName != "STRING" && typeName != "BINARY" {
		return value
	}
	text := compatString(value)
	if typeName == "BINARY" {
		bytes := []byte(text)
		if int64(len(bytes)) > length {
			return string(bytes[:length])
		}
		return text
	}
	runes := []rune(text)
	if int64(len(runes)) > length {
		return string(runes[:length])
	}
	return text
}

func applyCastScale(value interface{}, castType string, scale int64, enabled bool) interface{} {
	if !enabled || scale < 0 {
		return value
	}
	typeName := strings.ToUpper(strings.TrimSpace(castType))
	if typeName != "DECIMAL" && typeName != "NUMERIC" {
		return value
	}
	number, err := strconv.ParseFloat(strings.TrimSpace(compatString(value)), 64)
	if err != nil {
		return value
	}
	factor := math.Pow10(int(scale))
	return math.Round(number*factor) / factor
}

// evalConvertUsing applies the character-set conversion requested by
// CONVERT(expr USING charset). Expression values are UTF-8 at the storage and
// planner boundary; the returned string preserves the target charset bytes so
// the protocol layer can carry the same value without silently treating this
// syntax as a plain CAST(... AS CHAR).
func evalConvertUsing(args []interface{}, charsetName string) (interface{}, error) {
	if len(args) != 1 || args[0] == nil {
		return nil, nil
	}
	charsetName = strings.ToLower(strings.TrimSpace(charsetName))
	if charsetName == "utf-8" || charsetName == "utf8mb3" {
		charsetName = "utf8"
	}
	if charsetName == "utf-8mb4" {
		charsetName = "utf8mb4"
	}
	if charsetName == "binary" {
		return string([]byte(compatString(args[0]))), nil
	}
	targetEncoding, err := expressionCharsetEncoding(charsetName)
	if err != nil {
		return nil, fmt.Errorf("unsupported CONVERT USING charset %q: %w", charsetName, err)
	}
	decoded, err := unicodeencoding.UTF8.NewDecoder().Bytes([]byte(compatString(args[0])))
	if err != nil {
		return nil, fmt.Errorf("decode UTF-8 for CONVERT USING %q: %w", charsetName, err)
	}
	converted, err := targetEncoding.NewEncoder().Bytes(decoded)
	if err != nil {
		return nil, fmt.Errorf("encode %s for CONVERT USING: %w", charsetName, err)
	}
	return string(converted), nil
}

func expressionCharsetEncoding(name string) (encoding.Encoding, error) {
	switch name {
	case "utf8", "utf8mb4":
		return unicodeencoding.UTF8, nil
	case "ascii":
		return ianaindex.IANA.Encoding("US-ASCII")
	case "latin1":
		return charmap.ISO8859_1, nil
	case "latin2":
		return charmap.ISO8859_2, nil
	case "cp1250":
		return charmap.Windows1250, nil
	case "cp1251":
		return charmap.Windows1251, nil
	case "cp1256":
		return charmap.Windows1256, nil
	case "cp1257":
		return charmap.Windows1257, nil
	case "cp850":
		return charmap.CodePage850, nil
	case "gbk":
		return simplifiedchinese.GBK, nil
	case "gb2312":
		return simplifiedchinese.HZGB2312, nil
	case "big5":
		return traditionalchinese.Big5, nil
	case "euckr":
		return korean.EUCKR, nil
	case "sjis", "cp932":
		return japanese.ShiftJIS, nil
	default:
		return nil, fmt.Errorf("no Go encoding mapping")
	}
}

func evalNumericFunction(name string, args []interface{}) (interface{}, error) {
	if len(args) == 0 || args[0] == nil {
		return nil, nil
	}
	value, err := numericArg(args[0])
	if err != nil {
		return nil, err
	}
	switch name {
	case "ABS":
		return math.Abs(value), nil
	case "SIGN":
		switch {
		case value < 0:
			return int64(-1), nil
		case value > 0:
			return int64(1), nil
		default:
			return int64(0), nil
		}
	case "FLOOR":
		return int64(math.Floor(value)), nil
	case "CEIL", "CEILING":
		return int64(math.Ceil(value)), nil
	case "SQRT":
		if value < 0 {
			return nil, fmt.Errorf("invalid argument for SQRT")
		}
		return math.Sqrt(value), nil
	case "ROUND":
		precision := int64(0)
		if len(args) > 1 && args[1] != nil {
			precision, err = integerArg(args[1])
			if err != nil {
				return nil, err
			}
		}
		factor := math.Pow10(int(precision))
		return math.Round(value*factor) / factor, nil
	case "TRUNCATE":
		precision := int64(0)
		if len(args) > 1 && args[1] != nil {
			precision, err = integerArg(args[1])
			if err != nil {
				return nil, err
			}
		}
		factor := math.Pow10(int(precision))
		if math.IsInf(factor, 0) || factor == 0 {
			return value, nil
		}
		return math.Trunc(value*factor) / factor, nil
	case "POWER", "POW":
		if len(args) != 2 {
			return nil, fmt.Errorf("%s requires 2 arguments", name)
		}
		exponent, err := numericArg(args[1])
		if err != nil {
			return nil, err
		}
		return math.Pow(value, exponent), nil
	case "MOD":
		if len(args) != 2 {
			return nil, fmt.Errorf("MOD requires 2 arguments")
		}
		divisor, err := numericArg(args[1])
		if err != nil || divisor == 0 {
			if err == nil {
				err = fmt.Errorf("division by zero")
			}
			return nil, err
		}
		return math.Mod(value, divisor), nil
	case "EXP":
		return math.Exp(value), nil
	case "LN":
		if value <= 0 {
			return nil, fmt.Errorf("invalid argument for LN")
		}
		return math.Log(value), nil
	case "LOG10":
		if value <= 0 {
			return nil, fmt.Errorf("invalid argument for LOG10")
		}
		return math.Log10(value), nil
	case "LOG2":
		if value <= 0 {
			return nil, fmt.Errorf("invalid argument for LOG2")
		}
		return math.Log2(value), nil
	case "LOG":
		if len(args) == 1 {
			if value <= 0 {
				return nil, fmt.Errorf("invalid argument for LOG")
			}
			return math.Log(value), nil
		}
		base, err := numericArg(args[1])
		if err != nil || value <= 0 || base <= 0 || base == 1 {
			return nil, fmt.Errorf("invalid arguments for LOG")
		}
		return math.Log(value) / math.Log(base), nil
	case "SIN":
		return math.Sin(value), nil
	case "COS":
		return math.Cos(value), nil
	case "TAN":
		return math.Tan(value), nil
	case "COT":
		tangent := math.Tan(value)
		if tangent == 0 {
			return nil, fmt.Errorf("division by zero in COT")
		}
		return 1 / tangent, nil
	case "ASIN":
		if value < -1 || value > 1 {
			return nil, fmt.Errorf("invalid argument for ASIN")
		}
		return math.Asin(value), nil
	case "ACOS":
		if value < -1 || value > 1 {
			return nil, fmt.Errorf("invalid argument for ACOS")
		}
		return math.Acos(value), nil
	case "ATAN":
		if len(args) == 1 {
			return math.Atan(value), nil
		}
		other, err := numericArg(args[1])
		if err != nil {
			return nil, err
		}
		return math.Atan2(value, other), nil
	case "ATAN2":
		if len(args) != 2 {
			return nil, fmt.Errorf("ATAN2 requires 2 arguments")
		}
		other, err := numericArg(args[1])
		if err != nil {
			return nil, err
		}
		return math.Atan2(value, other), nil
	case "RADIANS":
		return value * math.Pi / 180, nil
	case "DEGREES":
		return value * 180 / math.Pi, nil
	}
	return nil, fmt.Errorf("unknown numeric function %s", name)
}

func evalFormatNumber(args []interface{}) (interface{}, error) {
	if len(args) < 1 || len(args) > 2 || args[0] == nil {
		return nil, nil
	}
	number, err := numericArg(args[0])
	if err != nil {
		return nil, err
	}
	precision := int64(0)
	if len(args) == 2 && args[1] != nil {
		precision, err = integerArg(args[1])
		if err != nil {
			return nil, err
		}
	}
	if precision < 0 {
		precision = 0
	}
	text := strconv.FormatFloat(number, 'f', int(precision), 64)
	parts := strings.SplitN(text, ".", 2)
	integer := parts[0]
	start := 0
	if strings.HasPrefix(integer, "-") {
		start = 1
	}
	for index := len(integer) - 3; index > start; index -= 3 {
		integer = integer[:index] + "," + integer[index:]
	}
	if len(parts) == 1 {
		return integer, nil
	}
	return integer + "." + parts[1], nil
}

func evalNetworkAndDigestFunction(name string, args []interface{}) (interface{}, error) {
	if len(args) == 0 || args[0] == nil {
		return nil, nil
	}
	switch name {
	case "MD5":
		if len(args) != 1 {
			return nil, nil
		}
		digest := md5.Sum([]byte(compatString(args[0])))
		return hex.EncodeToString(digest[:]), nil
	case "TO_BASE64":
		if len(args) != 1 {
			return nil, nil
		}
		return base64.StdEncoding.EncodeToString([]byte(compatString(args[0]))), nil
	case "FROM_BASE64":
		if len(args) != 1 {
			return nil, nil
		}
		decoded, err := base64.StdEncoding.DecodeString(strings.Map(func(r rune) rune {
			switch r {
			case ' ', '\t', '\r', '\n':
				return -1
			default:
				return r
			}
		}, compatString(args[0])))
		if err != nil {
			return nil, nil
		}
		return decoded, nil
	case "SHA", "SHA1":
		if len(args) != 1 {
			return nil, nil
		}
		digest := sha1.Sum([]byte(compatString(args[0])))
		return hex.EncodeToString(digest[:]), nil
	case "SHA2":
		if len(args) != 2 || args[1] == nil {
			return nil, nil
		}
		bits, err := integerArg(args[1])
		if err != nil {
			return nil, err
		}
		data := []byte(compatString(args[0]))
		var encoded []byte
		switch bits {
		case 0, 256:
			digest := sha256.Sum256(data)
			encoded = digest[:]
		case 224:
			digest := sha256.Sum224(data)
			encoded = digest[:]
		case 384:
			digest := sha512.Sum384(data)
			encoded = digest[:]
		case 512:
			digest := sha512.Sum512(data)
			encoded = digest[:]
		default:
			return nil, nil
		}
		return hex.EncodeToString(encoded), nil
	case "CRC32":
		if len(args) != 1 {
			return nil, nil
		}
		return uint64(crc32.ChecksumIEEE([]byte(compatString(args[0])))), nil
	case "INET_ATON":
		if len(args) != 1 {
			return nil, nil
		}
		ip := net.ParseIP(compatString(args[0])).To4()
		if ip == nil {
			return nil, nil
		}
		return uint64(ip[0])<<24 | uint64(ip[1])<<16 | uint64(ip[2])<<8 | uint64(ip[3]), nil
	case "INET_NTOA":
		if len(args) != 1 {
			return nil, nil
		}
		value, err := integerArg(args[0])
		if err != nil || value < 0 || value > 4294967295 {
			return nil, nil
		}
		return fmt.Sprintf("%d.%d.%d.%d", (value>>24)&255, (value>>16)&255, (value>>8)&255, value&255), nil
	case "INET6_ATON":
		if len(args) != 1 {
			return nil, nil
		}
		ip := net.ParseIP(compatString(args[0]))
		if ip == nil {
			return nil, nil
		}
		if ipv4 := ip.To4(); ipv4 != nil {
			return append([]byte(nil), ipv4...), nil
		}
		ipv6 := ip.To16()
		if ipv6 == nil {
			return nil, nil
		}
		return append([]byte(nil), ipv6...), nil
	case "INET6_NTOA":
		if len(args) != 1 {
			return nil, nil
		}
		var data []byte
		switch value := args[0].(type) {
		case []byte:
			data = value
		case string:
			data = []byte(value)
		default:
			return nil, nil
		}
		if len(data) != net.IPv4len && len(data) != net.IPv6len {
			return nil, nil
		}
		return net.IP(data).String(), nil
	case "IS_IPV4", "IS_IPV6":
		if len(args) != 1 {
			return nil, nil
		}
		ip := net.ParseIP(compatString(args[0]))
		if name == "IS_IPV4" && ip != nil && ip.To4() != nil {
			return int64(1), nil
		}
		if name == "IS_IPV6" && ip != nil && ip.To4() == nil && ip.To16() != nil {
			return int64(1), nil
		}
		return int64(0), nil
	case "IS_IPV4_COMPAT", "IS_IPV4_MAPPED":
		if len(args) != 1 {
			return nil, nil
		}
		ip := net.ParseIP(compatString(args[0])).To16()
		if ip == nil {
			return int64(0), nil
		}
		if name == "IS_IPV4_COMPAT" {
			allZeroPrefix := true
			for _, value := range ip[:12] {
				if value != 0 {
					allZeroPrefix = false
					break
				}
			}
			if allZeroPrefix && (ip[12] != 0 || ip[13] != 0 || ip[14] != 0 || ip[15] != 0) {
				return int64(1), nil
			}
			return int64(0), nil
		}
		if ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0 && ip[4] == 0 && ip[5] == 0 && ip[6] == 0 && ip[7] == 0 && ip[8] == 0 && ip[9] == 0 && ip[10] == 0xff && ip[11] == 0xff {
			return int64(1), nil
		}
		return int64(0), nil
	case "IS_UUID":
		if len(args) != 1 {
			return nil, nil
		}
		valid := regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`).MatchString(compatString(args[0]))
		if valid {
			return int64(1), nil
		}
		return int64(0), nil
	case "UUID_TO_BIN":
		if len(args) < 1 || len(args) > 2 {
			return nil, nil
		}
		value := strings.ReplaceAll(compatString(args[0]), "-", "")
		if len(value) != 32 {
			return nil, nil
		}
		decoded, err := hex.DecodeString(value)
		if err != nil {
			return nil, nil
		}
		if len(args) == 2 && args[1] != nil {
			swap, swapErr := integerArg(args[1])
			if swapErr != nil {
				return nil, swapErr
			}
			if swap != 0 {
				original := append([]byte(nil), decoded...)
				decoded = append(append(append(append([]byte{}, original[6:8]...), original[4:6]...), original[0:4]...), original[8:]...)
			}
		}
		return decoded, nil
	case "BIN_TO_UUID":
		if len(args) < 1 || len(args) > 2 {
			return nil, nil
		}
		var decoded []byte
		switch value := args[0].(type) {
		case []byte:
			decoded = append([]byte(nil), value...)
		case string:
			decoded = []byte(value)
		default:
			return nil, nil
		}
		if len(decoded) != 16 {
			return nil, nil
		}
		if len(args) == 2 && args[1] != nil {
			swap, swapErr := integerArg(args[1])
			if swapErr != nil {
				return nil, swapErr
			}
			if swap != 0 {
				original := append([]byte(nil), decoded...)
				decoded = append(append(append(append([]byte{}, original[4:8]...), original[2:4]...), original[0:2]...), original[8:]...)
			}
		}
		hexValue := hex.EncodeToString(decoded)
		return hexValue[0:8] + "-" + hexValue[8:12] + "-" + hexValue[12:16] + "-" + hexValue[16:20] + "-" + hexValue[20:32], nil
	}
	return nil, fmt.Errorf("unsupported network or digest function %s", name)
}

func evalRandomBytes(args []interface{}) (interface{}, error) {
	if len(args) != 1 || args[0] == nil {
		return nil, nil
	}
	length, err := integerArg(args[0])
	if err != nil {
		return nil, err
	}
	if length < 1 || length > 1024 {
		return nil, fmt.Errorf("RANDOM_BYTES length must be between 1 and 1024")
	}
	result := make([]byte, length)
	if _, err := cryptorand.Read(result); err != nil {
		return nil, fmt.Errorf("generate random bytes: %w", err)
	}
	return result, nil
}

func evalStringCompatibilityFunction(name string, args []interface{}) (interface{}, error) {
	if name == "BIT_COUNT" {
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		value, err := integerArg(args[0])
		if err != nil {
			return nil, err
		}
		return int64(bits.OnesCount64(uint64(value))), nil
	}
	if name == "SOUNDEX" {
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		text := strings.ToUpper(compatString(args[0]))
		first := -1
		for index := 0; index < len(text); index++ {
			if text[index] >= 'A' && text[index] <= 'Z' {
				first = index
				break
			}
		}
		if first < 0 {
			return "", nil
		}
		result := []byte{text[first]}
		lastCode := soundexCode(text[first])
		for index := first + 1; index < len(text) && len(result) < 4; index++ {
			character := text[index]
			if character == 'A' || character == 'E' || character == 'I' || character == 'O' || character == 'U' || character == 'Y' {
				lastCode = ""
				continue
			}
			code := soundexCode(character)
			if code == "" {
				continue
			}
			if code != lastCode {
				result = append(result, code[0])
			}
			lastCode = code
		}
		for len(result) < 4 {
			result = append(result, '0')
		}
		return string(result), nil
	}
	if name == "ORD" {
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		text := []byte(compatString(args[0]))
		if len(text) == 0 {
			return int64(0), nil
		}
		length := len(text)
		if length > 4 {
			length = 4
		}
		var value int64
		for _, character := range text[:length] {
			value = (value << 8) | int64(character)
		}
		return value, nil
	}
	if name == "MAKE_SET" {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		bits, err := integerArg(args[0])
		if err != nil {
			return nil, err
		}
		parts := make([]string, 0, len(args)-1)
		for index, arg := range args[1:] {
			if arg != nil && bits >= 0 && (uint64(bits)&(uint64(1)<<uint(index))) != 0 {
				parts = append(parts, compatString(arg))
			}
		}
		return strings.Join(parts, ","), nil
	}
	if name == "EXPORT_SET" {
		if len(args) < 3 || len(args) > 5 || args[0] == nil || args[1] == nil || args[2] == nil {
			return nil, nil
		}
		bits, err := integerArg(args[0])
		if err != nil {
			return nil, err
		}
		separator := ","
		if len(args) >= 4 && args[3] != nil {
			separator = compatString(args[3])
		}
		numberOfBits := int64(64)
		if len(args) == 5 && args[4] != nil {
			numberOfBits, err = integerArg(args[4])
			if err != nil {
				return nil, err
			}
		}
		if numberOfBits < 0 {
			return nil, fmt.Errorf("EXPORT_SET number_of_bits must be non-negative")
		}
		if numberOfBits > 64 {
			numberOfBits = 64
		}
		on, off := compatString(args[1]), compatString(args[2])
		parts := make([]string, int(numberOfBits))
		for index := range parts {
			if bits >= 0 && (uint64(bits)&(uint64(1)<<uint(index))) != 0 {
				parts[index] = on
			} else {
				parts[index] = off
			}
		}
		return strings.Join(parts, separator), nil
	}
	if name == "STRCMP" {
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		left, right := compatString(args[0]), compatString(args[1])
		if left < right {
			return int64(-1), nil
		}
		if left > right {
			return int64(1), nil
		}
		return int64(0), nil
	}
	if name == "HEX" {
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		switch value := args[0].(type) {
		case int64:
			return strings.ToUpper(strconv.FormatUint(uint64(value), 16)), nil
		case int:
			return strings.ToUpper(strconv.FormatInt(int64(value), 16)), nil
		default:
			return strings.ToUpper(hex.EncodeToString([]byte(compatString(value)))), nil
		}
	}
	if name == "UNHEX" {
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		text := compatString(args[0])
		if len(text)%2 != 0 {
			text = "0" + text
		}
		decoded, err := hex.DecodeString(text)
		if err != nil {
			return nil, nil
		}
		return decoded, nil
	}
	if name == "BIN" || name == "OCT" {
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		value, err := integerArg(args[0])
		if err != nil {
			return nil, err
		}
		if name == "BIN" {
			return strconv.FormatUint(uint64(value), 2), nil
		}
		return strconv.FormatUint(uint64(value), 8), nil
	}
	if name == "TRIM" || name == "LTRIM" || name == "RTRIM" {
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		text := compatString(args[0])
		switch name {
		case "TRIM":
			return strings.TrimSpace(text), nil
		case "LTRIM":
			return strings.TrimLeft(text, " \t\r\n"), nil
		default:
			return strings.TrimRight(text, " \t\r\n"), nil
		}
	}
	if name == "SPACE" {
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		n, err := integerArg(args[0])
		if err != nil {
			return nil, err
		}
		if n < 0 {
			n = 0
		}
		return strings.Repeat(" ", int(n)), nil
	}
	if name == "ASCII" {
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		text := compatString(args[0])
		if text == "" {
			return int64(0), nil
		}
		return int64(text[0]), nil
	}
	if name == "BIT_LENGTH" {
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		return int64(len([]byte(compatString(args[0]))) * 8), nil
	}
	if name == "FIND_IN_SET" {
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		needle, list := compatString(args[0]), strings.Split(compatString(args[1]), ",")
		for index, item := range list {
			if item == needle {
				return int64(index + 1), nil
			}
		}
		return int64(0), nil
	}
	if name == "CHAR" {
		var out strings.Builder
		for _, arg := range args {
			n, err := integerArg(arg)
			if err != nil {
				return nil, err
			}
			out.WriteByte(byte(n))
		}
		return out.String(), nil
	}
	if name == "QUOTE" {
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		text := compatString(args[0])
		text = strings.NewReplacer("\\", "\\\\", "'", "\\'", "\x00", "\\0", "\n", "\\n", "\r", "\\r").Replace(text)
		return "'" + text + "'", nil
	}
	if name == "ELT" {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		index, err := integerArg(args[0])
		if err != nil {
			return nil, err
		}
		if index < 1 || index >= int64(len(args)) {
			return nil, nil
		}
		return args[index], nil
	}
	if name == "FIELD" {
		if len(args) < 2 || args[0] == nil {
			return int64(0), nil
		}
		for index, candidate := range args[1:] {
			if compatString(candidate) == compatString(args[0]) {
				return int64(index + 1), nil
			}
		}
		return int64(0), nil
	}
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil, nil
	}
	text, lengthValue := compatString(args[0]), args[1]
	length, err := integerArg(lengthValue)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		length = 0
	}
	if name == "REPEAT" {
		return strings.Repeat(text, int(length)), nil
	}
	if len(args) < 3 || args[2] == nil {
		return nil, nil
	}
	pad := compatString(args[2])
	if pad == "" {
		return nil, fmt.Errorf("padding string must not be empty")
	}
	runes := []rune(text)
	target := int(length)
	if len(runes) >= target {
		return string(runes[:target]), nil
	}
	needed := target - len(runes)
	padding := []rune(strings.Repeat(pad, (needed+len([]rune(pad))-1)/len([]rune(pad))))[:needed]
	if name == "LPAD" {
		return string(padding) + text, nil
	}
	return text + string(padding), nil
}

func soundexCode(character byte) string {
	switch character {
	case 'B', 'F', 'P', 'V':
		return "1"
	case 'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z':
		return "2"
	case 'D', 'T':
		return "3"
	case 'L':
		return "4"
	case 'M', 'N':
		return "5"
	case 'R':
		return "6"
	default:
		return ""
	}
}

func evalExtendedStringFunction(name string, args []interface{}) (interface{}, error) {
	switch name {
	case "CONCAT_WS":
		if len(args) < 2 || args[0] == nil {
			return nil, nil
		}
		separator := compatString(args[0])
		parts := make([]string, 0, len(args)-1)
		for _, arg := range args[1:] {
			if arg != nil {
				parts = append(parts, compatString(arg))
			}
		}
		return strings.Join(parts, separator), nil
	case "SUBSTRING_INDEX":
		if len(args) != 3 || args[0] == nil || args[1] == nil || args[2] == nil {
			return nil, nil
		}
		text, delimiter := compatString(args[0]), compatString(args[1])
		count, err := integerArg(args[2])
		if err != nil {
			return nil, err
		}
		if count == 0 || delimiter == "" {
			if count == 0 {
				return "", nil
			}
			return text, nil
		}
		parts := strings.Split(text, delimiter)
		if count > 0 {
			if count >= int64(len(parts)) {
				return text, nil
			}
			return strings.Join(parts[:int(count)], delimiter), nil
		}
		count = -count
		if count >= int64(len(parts)) {
			return text, nil
		}
		return strings.Join(parts[len(parts)-int(count):], delimiter), nil
	default:
		return nil, fmt.Errorf("unknown extended string function: %s", name)
	}
}

func evalConv(args []interface{}) (interface{}, error) {
	if len(args) != 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil, nil
	}
	fromBase, err := integerArg(args[1])
	if err != nil {
		return nil, err
	}
	toBase, err := integerArg(args[2])
	if err != nil {
		return nil, err
	}
	if fromBase < 2 || fromBase > 36 || toBase < 2 || toBase > 36 {
		return nil, fmt.Errorf("CONV bases must be between 2 and 36")
	}

	value, ok := new(big.Int).SetString(strings.TrimSpace(compatString(args[0])), int(fromBase))
	if !ok {
		return nil, fmt.Errorf("invalid CONV number %q for base %d", compatString(args[0]), fromBase)
	}
	return strings.ToUpper(value.Text(int(toBase))), nil
}

// evalInterval implements MySQL's INTERVAL(N, N1, N2, ... ) function. The
// result is the one-based index of the last boundary that is less than or
// equal to N, or zero when N is below the first boundary. NULL input follows
// SQL's NULL propagation rules.
func evalInterval(args []interface{}) (interface{}, error) {
	if len(args) < 2 || args[0] == nil {
		return nil, nil
	}
	result := int64(0)
	for index, boundary := range args[1:] {
		if boundary == nil {
			continue
		}
		matches, err := evalLE(boundary, args[0])
		if err != nil {
			return nil, err
		}
		if matches == true {
			result = int64(index + 1)
		}
	}
	return result, nil
}

func compileMySQLRegexp(patternText, matchType string) (*regexp.Regexp, error) {
	caseInsensitive := false
	multiline := false
	dotMatchesNewline := false
	for _, flag := range strings.ToLower(strings.TrimSpace(matchType)) {
		switch flag {
		case 'c':
			caseInsensitive = false
		case 'i':
			caseInsensitive = true
		case 'm':
			multiline = true
		case 'n':
			dotMatchesNewline = true
		case 'u':
			// Go's regexp engine already treats LF as the line terminator;
			// there is no additional flag needed for MySQL's Unix-lines mode.
		default:
			return nil, fmt.Errorf("invalid regular expression match type %q", matchType)
		}
	}
	flags := ""
	if caseInsensitive {
		flags += "i"
	}
	if multiline {
		flags += "m"
	}
	if dotMatchesNewline {
		flags += "s"
	}
	if flags != "" {
		patternText = "(?" + flags + ")" + patternText
	}
	return regexp.Compile(patternText)
}

func evalRegexpCompatibilityFunction(name string, args []interface{}) (interface{}, error) {
	minimum := 2
	if name == "REGEXP_REPLACE" {
		minimum = 3
	}
	if len(args) < minimum || len(args) > 6 || args[0] == nil || args[1] == nil {
		return nil, nil
	}
	input := compatString(args[0])
	patternText := compatString(args[1])
	positionIndex := 2
	if name == "REGEXP_REPLACE" {
		positionIndex = 3
	}
	position := int64(1)
	occurrence := int64(0)
	matchType := ""
	var err error
	if len(args) > positionIndex {
		position, err = integerArg(args[positionIndex])
		if err != nil || position < 1 {
			return nil, fmt.Errorf("REGEXP position must be a positive integer")
		}
	}
	if len(args) > positionIndex+1 {
		occurrence, err = integerArg(args[positionIndex+1])
		if err != nil || occurrence < 0 {
			return nil, fmt.Errorf("REGEXP occurrence must be a non-negative integer")
		}
	}
	if len(args) > positionIndex+2 && args[positionIndex+2] != nil {
		matchType = compatString(args[positionIndex+2])
	}
	pattern, err := compileMySQLRegexp(patternText, matchType)
	if err != nil {
		return nil, err
	}
	start := int(position - 1)
	if start > len(input) {
		if name == "REGEXP_SUBSTR" {
			return nil, nil
		}
		return input, nil
	}
	suffix := input[start:]
	matches := pattern.FindAllStringIndex(suffix, -1)
	if name == "REGEXP_SUBSTR" {
		if occurrence == 0 {
			occurrence = 1
		}
		matchIndex := int(occurrence - 1)
		if matchIndex < 0 || matchIndex >= len(matches) {
			return nil, nil
		}
		match := matches[matchIndex]
		return suffix[match[0]:match[1]], nil
	}
	if args[2] == nil {
		return nil, nil
	}
	replacement := compatString(args[2])
	if occurrence == 0 {
		return input[:start] + pattern.ReplaceAllString(suffix, replacement), nil
	}
	matchIndex := int(occurrence - 1)
	if matchIndex < 0 || matchIndex >= len(matches) {
		return input, nil
	}
	match := matches[matchIndex]
	replaced := pattern.ReplaceAllString(suffix[match[0]:match[1]], replacement)
	return input[:start] + suffix[:match[0]] + replaced + suffix[match[1]:], nil
}

func evalRegexpInstr(args []interface{}) (interface{}, error) {
	if len(args) < 2 || len(args) > 6 || args[0] == nil || args[1] == nil {
		return nil, nil
	}
	position := int64(1)
	occurrence := int64(1)
	returnOption := int64(0)
	matchType := ""
	var err error
	if len(args) > 2 && args[2] != nil {
		position, err = integerArg(args[2])
		if err != nil || position < 1 {
			return nil, fmt.Errorf("REGEXP_INSTR position must be a positive integer")
		}
	}
	if len(args) > 3 && args[3] != nil {
		occurrence, err = integerArg(args[3])
		if err != nil || occurrence < 1 {
			return nil, fmt.Errorf("REGEXP_INSTR occurrence must be a positive integer")
		}
	}
	if len(args) > 4 && args[4] != nil {
		returnOption, err = integerArg(args[4])
		if err != nil || (returnOption != 0 && returnOption != 1) {
			return nil, fmt.Errorf("REGEXP_INSTR return option must be 0 or 1")
		}
	}
	if len(args) > 5 && args[5] != nil {
		matchType = compatString(args[5])
	}
	patternText := compatString(args[1])
	pattern, err := compileMySQLRegexp(patternText, matchType)
	if err != nil {
		return nil, err
	}
	input := compatString(args[0])
	start := int(position - 1)
	if start >= len(input) {
		return int64(0), nil
	}
	matches := pattern.FindAllStringIndex(input[start:], -1)
	matchIndex := int(occurrence - 1)
	if matchIndex < 0 || matchIndex >= len(matches) {
		return int64(0), nil
	}
	match := matches[matchIndex]
	if returnOption == 1 {
		return int64(start + match[1] + 1), nil
	}
	return int64(start + match[0] + 1), nil
}

func evalDatePart(name string, args []interface{}) (interface{}, error) {
	if name == "WEEK" || name == "YEARWEEK" || name == "WEEKOFYEAR" {
		if len(args) < 1 || len(args) > 2 || args[0] == nil {
			return nil, nil
		}
		if name == "WEEKOFYEAR" && len(args) != 1 {
			return nil, nil
		}
		parsed, err := parseCompatTime(args[0])
		if err != nil {
			return nil, err
		}
		mode := int64(0)
		if name == "WEEKOFYEAR" {
			mode = 3
		}
		if len(args) == 2 && args[1] != nil {
			mode, err = integerArg(args[1])
			if err != nil || mode < 0 || mode > 7 {
				if err == nil {
					err = fmt.Errorf("week mode must be between 0 and 7")
				}
				return nil, err
			}
		}
		weekYear, week := compatWeekOfYear(parsed, int(mode))
		if name == "YEARWEEK" {
			if week == 0 {
				weekYear, week = compatWeekOfYear(time.Date(parsed.Year()-1, time.December, 31, 0, 0, 0, 0, parsed.Location()), int(mode))
			}
			return int64(weekYear*100 + week), nil
		}
		return int64(week), nil
	}
	if len(args) != 1 || args[0] == nil {
		return nil, nil
	}
	parsed, err := parseCompatTime(args[0])
	if err != nil {
		return nil, err
	}
	switch name {
	case "YEAR":
		return int64(parsed.Year()), nil
	case "MONTH":
		return int64(parsed.Month()), nil
	case "QUARTER":
		return int64((int(parsed.Month())-1)/3 + 1), nil
	case "DAY", "DAYOFMONTH":
		return int64(parsed.Day()), nil
	case "MONTHNAME":
		return parsed.Month().String(), nil
	case "DAYNAME":
		return parsed.Weekday().String(), nil
	case "WEEKDAY":
		return int64((int(parsed.Weekday()) + 6) % 7), nil
	case "DAYOFWEEK":
		return int64(parsed.Weekday()) + 1, nil
	case "DAYOFYEAR":
		return int64(parsed.YearDay()), nil
	case "LAST_DAY":
		last := time.Date(parsed.Year(), parsed.Month()+1, 0, 0, 0, 0, 0, parsed.Location())
		return last.Format("2006-01-02"), nil
	case "HOUR":
		return int64(parsed.Hour()), nil
	case "MINUTE":
		return int64(parsed.Minute()), nil
	case "SECOND":
		return int64(parsed.Second()), nil
	case "MICROSECOND":
		return int64(parsed.Nanosecond() / 1000), nil
	case "DATE":
		return parsed.Format("2006-01-02"), nil
	case "TIME":
		return parsed.Format("15:04:05"), nil
	}
	return nil, fmt.Errorf("unknown date function %s", name)
}

func evalCalendarFunction(name string, args []interface{}) (interface{}, error) {
	switch name {
	case "TO_DAYS":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		parsed, err := parseCompatTime(args[0])
		if err != nil {
			return nil, err
		}
		return compatCalendarDayNumber(parsed), nil
	case "FROM_DAYS":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		days, err := integerArg(args[0])
		if err != nil {
			return nil, err
		}
		if days < 1 {
			return "0000-00-00", nil
		}
		value := time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(days-1))
		// Go's proleptic calendar treats year zero differently from MySQL's
		// day-number convention. Reconcile the small boundary offset through
		// the same day-number function used by TO_DAYS.
		for attempt := 0; attempt < 3; attempt++ {
			current := compatCalendarDayNumber(value)
			if current == days {
				break
			}
			if current < days {
				value = value.AddDate(0, 0, 1)
			} else {
				value = value.AddDate(0, 0, -1)
			}
		}
		if compatCalendarDayNumber(value) != days {
			return nil, fmt.Errorf("day number %d is outside supported calendar range", days)
		}
		return value.Format("2006-01-02"), nil
	case "TO_SECONDS":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		parsed, err := parseCompatTime(args[0])
		if err != nil {
			return nil, err
		}
		return compatCalendarDayNumber(parsed)*86400 + int64(parsed.Hour())*3600 + int64(parsed.Minute())*60 + int64(parsed.Second()), nil
	case "PERIOD_ADD":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		period, err := integerArg(args[0])
		if err != nil {
			return nil, err
		}
		months, err := integerArg(args[1])
		if err != nil {
			return nil, err
		}
		year, month, short, err := compatParsePeriod(period)
		if err != nil {
			return nil, err
		}
		total := year*12 + (month - 1) + months
		newYear, newMonth := total/12, total%12+1
		if short {
			return (newYear%100)*100 + newMonth, nil
		}
		return newYear*100 + newMonth, nil
	case "PERIOD_DIFF":
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		left, err := integerArg(args[0])
		if err != nil {
			return nil, err
		}
		right, err := integerArg(args[1])
		if err != nil {
			return nil, err
		}
		leftYear, leftMonth, _, err := compatParsePeriod(left)
		if err != nil {
			return nil, err
		}
		rightYear, rightMonth, _, err := compatParsePeriod(right)
		if err != nil {
			return nil, err
		}
		return (leftYear-rightYear)*12 + leftMonth - rightMonth, nil
	}
	return nil, fmt.Errorf("unknown calendar function %s", name)
}

func compatCalendarDayNumber(value time.Time) int64 {
	year, month, day := int64(value.Year()), int64(value.Month()), int64(value.Day())
	monthDays := [...]int64{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334}
	leapYearsBefore := int64(0)
	if year > 0 {
		completedYears := year - 1
		leapYearsBefore = completedYears/4 - completedYears/100 + completedYears/400
	}
	currentYearLeapDay := int64(0)
	if month > 2 && (year%400 == 0 || year%4 == 0 && year%100 != 0) {
		currentYearLeapDay = 1
	}
	return 365*year + leapYearsBefore + currentYearLeapDay + monthDays[month-1] + day
}

func compatParsePeriod(period int64) (year, month int64, short bool, err error) {
	if period < 0 {
		period = -period
	}
	month = period % 100
	if month < 1 || month > 12 {
		return 0, 0, false, fmt.Errorf("invalid period %d", period)
	}
	year = period / 100
	short = period < 10000
	if short {
		if year < 70 {
			year += 2000
		} else {
			year += 1900
		}
	}
	return year, month, short, nil
}

// evalCompositeDatePart implements MySQL's packed EXTRACT interval units.
// The non-microsecond variants are integer fields concatenated by decimal
// place value (for example DAY_SECOND => DDHHMMSS). Microsecond variants keep
// the fractional six digits as a decimal value, matching MySQL's DECIMAL-like
// result shape while preserving leading component positions.
func evalCompositeDatePart(name string, args []interface{}) (interface{}, error) {
	if len(args) != 1 || args[0] == nil {
		return nil, nil
	}
	parsed, err := parseCompatTime(args[0])
	if err != nil {
		return nil, err
	}
	year, month := int64(parsed.Year()), int64(parsed.Month())
	day, hour := int64(parsed.Day()), int64(parsed.Hour())
	minute, second := int64(parsed.Minute()), int64(parsed.Second())
	microsecond := int64(parsed.Nanosecond() / 1000)

	switch name {
	case "YEAR_MONTH":
		return year*100 + month, nil
	case "DAY_HOUR":
		return day*100 + hour, nil
	case "DAY_MINUTE":
		return day*10000 + hour*100 + minute, nil
	case "DAY_SECOND":
		return day*1000000 + hour*10000 + minute*100 + second, nil
	case "DAY_MICROSECOND":
		return packedMicrosecondValue(day*1000000+hour*10000+minute*100+second, microsecond), nil
	case "HOUR_MINUTE":
		return hour*100 + minute, nil
	case "HOUR_SECOND":
		return hour*10000 + minute*100 + second, nil
	case "HOUR_MICROSECOND":
		return packedMicrosecondValue(hour*10000+minute*100+second, microsecond), nil
	case "MINUTE_SECOND":
		return minute*100 + second, nil
	case "MINUTE_MICROSECOND":
		return packedMicrosecondValue(minute*100+second, microsecond), nil
	case "SECOND_MICROSECOND":
		return packedMicrosecondValue(second, microsecond), nil
	default:
		return nil, fmt.Errorf("unknown composite EXTRACT unit %s", name)
	}
}

func packedMicrosecondValue(integerPart, microsecond int64) float64 {
	return float64(integerPart) + float64(microsecond)/1_000_000
}

func compatWeekOfYear(value time.Time, mode int) (int, int) {
	if mode < 0 || mode > 7 {
		mode = 0
	}
	mondayFirst := mode == 1 || mode == 3 || mode == 5 || mode == 7
	fourDayFirst := mode == 1 || mode == 3 || mode == 4 || mode == 6
	oneBased := mode >= 2
	weekStart := func(t time.Time) time.Time {
		weekday := int(t.Weekday())
		start := 0
		if mondayFirst {
			start = 1
		}
		offset := (weekday - start + 7) % 7
		return t.AddDate(0, 0, -offset)
	}
	firstWeekStart := func(year int) time.Time {
		januaryFirst := time.Date(year, time.January, 1, 0, 0, 0, 0, value.Location())
		if fourDayFirst {
			return weekStart(time.Date(year, time.January, 4, 0, 0, 0, 0, value.Location()))
		}
		start := 0
		if mondayFirst {
			start = 1
		}
		offset := (start - int(januaryFirst.Weekday()) + 7) % 7
		return januaryFirst.AddDate(0, 0, offset)
	}

	year := value.Year()
	start := firstWeekStart(year)
	if value.Before(start) {
		if oneBased {
			previousYear, previousWeek := compatWeekOfYear(time.Date(year-1, time.December, 31, 0, 0, 0, 0, value.Location()), mode)
			return previousYear, previousWeek
		}
		return year, 0
	}
	nextStart := firstWeekStart(year + 1)
	if !value.Before(nextStart) {
		if oneBased {
			return year + 1, 1
		}
		return year + 1, 0
	}
	week := int(value.Sub(start).Hours()/24)/7 + 1
	return year, week
}

func evalJSONValueCompatibility(args []interface{}) (interface{}, error) {
	if len(args) != 7 {
		return nil, fmt.Errorf("JSON_VALUE_COMPAT requires seven arguments")
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	returningType := strings.TrimSpace(compatString(args[2]))
	baseType, castLength, hasCastLength, castScale, hasCastScale := parseJSONValueReturningType(returningType)
	emptyMode := strings.ToUpper(strings.TrimSpace(compatString(args[3])))
	emptyDefault := args[4]
	errorMode := strings.ToUpper(strings.TrimSpace(compatString(args[5])))
	errorDefault := args[6]
	applyPolicy := func(mode string, defaultValue interface{}, cause error) (interface{}, error) {
		switch mode {
		case "NULL", "":
			return nil, nil
		case "DEFAULT":
			return castJSONValue(defaultValue, baseType, castLength, hasCastLength, castScale, hasCastScale)
		case "ERROR":
			if cause == nil {
				cause = fmt.Errorf("JSON_VALUE conversion failed")
			}
			return nil, cause
		default:
			return nil, fmt.Errorf("unsupported JSON_VALUE policy %q", mode)
		}
	}
	var document interface{}
	if err := json.Unmarshal([]byte(compatString(args[0])), &document); err != nil {
		return applyPolicy(errorMode, errorDefault, err)
	}
	value, found := jsonExtractPathValue(document, compatString(args[1]))
	if !found || value == nil {
		return applyPolicy(emptyMode, emptyDefault, nil)
	}
	if _, object := value.(map[string]interface{}); object {
		return applyPolicy(errorMode, errorDefault, fmt.Errorf("JSON_VALUE extracted an object or array"))
	}
	if _, array := value.([]interface{}); array {
		return applyPolicy(errorMode, errorDefault, fmt.Errorf("JSON_VALUE extracted an object or array"))
	}
	converted, err := castJSONValue(value, baseType, castLength, hasCastLength, castScale, hasCastScale)
	if err != nil {
		return applyPolicy(errorMode, errorDefault, err)
	}
	return converted, nil
}

func parseJSONValueReturningType(typeName string) (string, int64, bool, int64, bool) {
	upper := strings.ToUpper(strings.TrimSpace(typeName))
	baseType := upper
	var length, scale int64
	var hasLength, hasScale bool
	if open := strings.IndexByte(upper, '('); open >= 0 && strings.HasSuffix(upper, ")") {
		baseType = strings.TrimSpace(upper[:open])
		parts := strings.Split(strings.TrimSpace(upper[open+1:len(upper)-1]), ",")
		if len(parts) > 0 {
			length, _ = strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
			hasLength = length >= 0
		}
		if len(parts) > 1 {
			scale, _ = strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			hasScale = scale >= 0
		}
	}
	return baseType, length, hasLength, scale, hasScale
}

func castJSONValue(value interface{}, baseType string, castLength int64, hasCastLength bool, castScale int64, hasCastScale bool) (interface{}, error) {
	converted, err := evalCast([]interface{}{value, baseType})
	if err != nil {
		return nil, err
	}
	if hasCastLength {
		converted = truncateCastValue(converted, baseType, castLength)
	}
	if hasCastScale {
		converted = applyCastScale(converted, baseType, castScale, true)
	}
	return converted, nil
}

func decodeJSONCompatibilityArgument(value interface{}) (interface{}, error) {
	switch value.(type) {
	case map[string]interface{}, []interface{}, bool, float64, string, nil:
		if _, isJSONText := value.(string); !isJSONText {
			return value, nil
		}
	}
	var decoded interface{}
	if err := json.Unmarshal([]byte(compatString(value)), &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func validateJSONSchemaCompatibility(schema, document interface{}, schemaPath, instancePath string) error {
	if boolean, ok := schema.(bool); ok {
		if boolean {
			return nil
		}
		return fmt.Errorf("schema %s rejects instance %s", schemaPath, instancePath)
	}
	schemaObject, ok := schema.(map[string]interface{})
	if !ok {
		return fmt.Errorf("schema %s must be an object or boolean", schemaPath)
	}
	if expected, exists := schemaObject["type"]; exists && !jsonSchemaTypeMatches(expected, document) {
		return fmt.Errorf("type at %s does not match schema %s", instancePath, schemaPath)
	}
	if required, ok := schemaObject["required"].([]interface{}); ok {
		object, objectOK := document.(map[string]interface{})
		if objectOK {
			for _, rawName := range required {
				name := compatString(rawName)
				if _, exists := object[name]; !exists {
					return fmt.Errorf("required property %q missing at %s", name, instancePath)
				}
			}
		}
	}
	if properties, ok := schemaObject["properties"].(map[string]interface{}); ok {
		if object, objectOK := document.(map[string]interface{}); objectOK {
			for name, propertySchema := range properties {
				if value, exists := object[name]; exists {
					if err := validateJSONSchemaCompatibility(propertySchema, value, schemaPath+"/properties/"+name, instancePath+"/"+name); err != nil {
						return err
					}
				}
			}
		}
	}
	if itemSchema, exists := schemaObject["items"]; exists {
		if items, itemsOK := document.([]interface{}); itemsOK {
			for index, item := range items {
				if err := validateJSONSchemaCompatibility(itemSchema, item, schemaPath+"/items", fmt.Sprintf("%s/%d", instancePath, index)); err != nil {
					return err
				}
			}
		}
	}
	if enum, ok := schemaObject["enum"].([]interface{}); ok {
		matched := false
		for _, candidate := range enum {
			if reflect.DeepEqual(candidate, document) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("enum at %s does not contain the instance", instancePath)
		}
	}
	if constant, exists := schemaObject["const"]; exists && !reflect.DeepEqual(constant, document) {
		return fmt.Errorf("const at %s does not match the instance", instancePath)
	}
	if numeric, ok := jsonSchemaNumber(document); ok {
		for keyword, rule := range map[string]struct {
			compare func(float64) bool
			invalid string
		}{
			"minimum": {func(bound float64) bool { return numeric < bound }, "below minimum"},
			"maximum": {func(bound float64) bool { return numeric > bound }, "above maximum"},
		} {
			if raw, exists := schemaObject[keyword]; exists {
				if bound, boundOK := jsonSchemaNumber(raw); boundOK && rule.compare(bound) {
					return fmt.Errorf("%s at %s", rule.invalid, instancePath)
				}
			}
		}
	}
	if text, ok := document.(string); ok {
		if raw, exists := schemaObject["minLength"]; exists {
			if bound, boundOK := jsonSchemaNumber(raw); boundOK && float64(len([]rune(text))) < bound {
				return fmt.Errorf("string at %s is shorter than minLength", instancePath)
			}
		}
		if raw, exists := schemaObject["maxLength"]; exists {
			if bound, boundOK := jsonSchemaNumber(raw); boundOK && float64(len([]rune(text))) > bound {
				return fmt.Errorf("string at %s exceeds maxLength", instancePath)
			}
		}
		if pattern, patternOK := schemaObject["pattern"].(string); patternOK {
			matched, err := regexp.MatchString(pattern, text)
			if err != nil {
				return fmt.Errorf("invalid pattern in schema %s: %w", schemaPath, err)
			}
			if !matched {
				return fmt.Errorf("string at %s does not match pattern", instancePath)
			}
		}
	}
	if items, ok := document.([]interface{}); ok {
		for keyword, invalid := range map[string]string{"minItems": "fewer than minItems", "maxItems": "more than maxItems"} {
			if raw, exists := schemaObject[keyword]; exists {
				if bound, boundOK := jsonSchemaNumber(raw); boundOK {
					if keyword == "minItems" && float64(len(items)) < bound || keyword == "maxItems" && float64(len(items)) > bound {
						return fmt.Errorf("array at %s has %s", instancePath, invalid)
					}
				}
			}
		}
	}
	if allOf, ok := schemaObject["allOf"].([]interface{}); ok {
		for index, child := range allOf {
			if err := validateJSONSchemaCompatibility(child, document, fmt.Sprintf("%s/allOf/%d", schemaPath, index), instancePath); err != nil {
				return err
			}
		}
	}
	if anyOf, ok := schemaObject["anyOf"].([]interface{}); ok {
		matched := false
		for _, child := range anyOf {
			if validateJSONSchemaCompatibility(child, document, schemaPath+"/anyOf", instancePath) == nil {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("instance at %s matches no anyOf schema", instancePath)
		}
	}
	if oneOf, ok := schemaObject["oneOf"].([]interface{}); ok {
		matches := 0
		for _, child := range oneOf {
			if validateJSONSchemaCompatibility(child, document, schemaPath+"/oneOf", instancePath) == nil {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf("instance at %s matches %d oneOf schemas", instancePath, matches)
		}
	}
	if notSchema, exists := schemaObject["not"]; exists && validateJSONSchemaCompatibility(notSchema, document, schemaPath+"/not", instancePath) == nil {
		return fmt.Errorf("instance at %s matches forbidden not schema", instancePath)
	}
	return nil
}

func jsonSchemaTypeMatches(expected, document interface{}) bool {
	if types, ok := expected.([]interface{}); ok {
		for _, candidate := range types {
			if jsonSchemaTypeMatches(candidate, document) {
				return true
			}
		}
		return false
	}
	typeName := strings.ToLower(compatString(expected))
	switch typeName {
	case "object":
		_, ok := document.(map[string]interface{})
		return ok
	case "array":
		_, ok := document.([]interface{})
		return ok
	case "string":
		_, ok := document.(string)
		return ok
	case "integer":
		number, ok := document.(float64)
		return ok && math.Trunc(number) == number
	case "number":
		_, ok := document.(float64)
		return ok
	case "boolean":
		_, ok := document.(bool)
		return ok
	case "null":
		return document == nil
	default:
		return false
	}
}

func jsonSchemaNumber(value interface{}) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	default:
		return 0, false
	}
}

func evalJSONFunction(name string, args []interface{}) (interface{}, error) {
	if name == "JSON_SCHEMA_VALID" || name == "JSON_SCHEMA_VALIDATION_REPORT" {
		if len(args) != 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		schema, err := decodeJSONCompatibilityArgument(args[0])
		if err != nil {
			return nil, err
		}
		document, err := decodeJSONCompatibilityArgument(args[1])
		if err != nil {
			return nil, err
		}
		validationErr := validateJSONSchemaCompatibility(schema, document, "$", "#")
		if name == "JSON_SCHEMA_VALID" {
			if validationErr == nil {
				return int64(1), nil
			}
			return int64(0), nil
		}
		report := map[string]interface{}{"valid": validationErr == nil}
		if validationErr != nil {
			report["error"] = validationErr.Error()
		}
		encoded, marshalErr := json.Marshal(report)
		if marshalErr != nil {
			return nil, marshalErr
		}
		return string(encoded), nil
	}
	if name == "JSON_STORAGE_FREE" {
		if len(args) != 1 {
			return nil, fmt.Errorf("JSON_STORAGE_FREE requires 1 argument")
		}
		if args[0] == nil {
			return nil, nil
		}
		var document interface{}
		if err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &document); err != nil {
			return nil, err
		}
		// A standalone document has no partial-update slack to reclaim. The
		// storage engine does not yet track binary-JSON free space per column.
		return int64(0), nil
	}
	if name == "JSON_STORAGE_SIZE" {
		if len(args) != 1 {
			return nil, fmt.Errorf("JSON_STORAGE_SIZE requires 1 argument")
		}
		if args[0] == nil {
			return nil, nil
		}
		var document interface{}
		if err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &document); err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(document)
		if err != nil {
			return nil, err
		}
		// This compatibility path reports the canonical JSON byte size. It is
		// intentionally independent of Go's map representation, while the
		// engine's full binary-JSON storage accounting remains a later boundary.
		return int64(len(encoded)), nil
	}
	if name == "JSON_VALID" {
		if len(args) != 1 {
			return nil, fmt.Errorf("JSON_VALID requires 1 argument")
		}
		if args[0] == nil {
			return nil, nil
		}
		var document interface{}
		if json.Unmarshal([]byte(fmt.Sprint(args[0])), &document) == nil {
			return int64(1), nil
		}
		return int64(0), nil
	}
	if name == "JSON_QUOTE" {
		if len(args) != 1 {
			return nil, fmt.Errorf("JSON_QUOTE requires 1 argument")
		}
		if args[0] == nil {
			return nil, nil
		}
		encoded, err := json.Marshal(fmt.Sprint(args[0]))
		if err != nil {
			return nil, err
		}
		return string(encoded), nil
	}
	if name == "JSON_OVERLAPS" {
		if len(args) != 2 {
			return nil, fmt.Errorf("JSON_OVERLAPS requires 2 arguments")
		}
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}
		var left, right interface{}
		if err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &left); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(fmt.Sprint(args[1])), &right); err != nil {
			return nil, err
		}
		if jsonValuesOverlap(left, right) {
			return int64(1), nil
		}
		return int64(0), nil
	}
	if name == "JSON_MERGE_PATCH" {
		if len(args) < 2 {
			return nil, fmt.Errorf("JSON_MERGE_PATCH requires at least 2 arguments")
		}
		var merged interface{}
		if err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &merged); err != nil {
			return nil, err
		}
		for _, raw := range args[1:] {
			var patch interface{}
			if err := json.Unmarshal([]byte(fmt.Sprint(raw)), &patch); err != nil {
				return nil, err
			}
			merged = mergeJSONPatch(merged, patch)
		}
		encoded, err := json.Marshal(merged)
		if err != nil {
			return nil, err
		}
		return string(encoded), nil
	}
	if name == "JSON_MERGE" || name == "JSON_MERGE_PRESERVE" {
		if len(args) < 2 {
			return nil, fmt.Errorf("JSON_MERGE_PRESERVE requires at least 2 arguments")
		}
		var merged interface{}
		if args[0] == nil {
			return nil, nil
		}
		if err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &merged); err != nil {
			return nil, err
		}
		for _, raw := range args[1:] {
			if raw == nil {
				return nil, nil
			}
			var document interface{}
			if err := json.Unmarshal([]byte(fmt.Sprint(raw)), &document); err != nil {
				return nil, err
			}
			merged = mergeJSONPreserve(merged, document)
		}
		encoded, err := json.Marshal(merged)
		if err != nil {
			return nil, err
		}
		return string(encoded), nil
	}
	if name == "JSON_ARRAY" {
		encoded, err := json.Marshal(args)
		if err != nil {
			return nil, err
		}
		return string(encoded), nil
	}
	if len(args) == 0 || args[0] == nil {
		return nil, nil
	}
	if name == "JSON_OBJECT" {
		if len(args)%2 != 0 {
			return nil, fmt.Errorf("JSON_OBJECT requires an even number of arguments")
		}
		object := make(map[string]interface{}, len(args)/2)
		for index := 0; index < len(args); index += 2 {
			if args[index] == nil {
				return nil, fmt.Errorf("JSON_OBJECT keys cannot be NULL")
			}
			object[compatString(args[index])] = args[index+1]
		}
		encoded, err := json.Marshal(object)
		if err != nil {
			return nil, err
		}
		return string(encoded), nil
	}
	if name == "JSON_UNQUOTE" {
		if len(args) != 1 {
			return nil, fmt.Errorf("JSON_UNQUOTE requires 1 argument")
		}
		var value interface{}
		if err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &value); err != nil {
			return strings.Trim(fmt.Sprint(args[0]), `"`), nil
		}
		return fmt.Sprint(value), nil
	}
	if name == "JSON_DEPTH" {
		if len(args) != 1 {
			return nil, fmt.Errorf("JSON_DEPTH requires 1 argument")
		}
		var document interface{}
		if err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &document); err != nil {
			return nil, err
		}
		return int64(jsonValueDepth(document)), nil
	}
	if name == "JSON_PRETTY" {
		if len(args) != 1 {
			return nil, fmt.Errorf("JSON_PRETTY requires 1 argument")
		}
		var document interface{}
		if err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &document); err != nil {
			return nil, err
		}
		encoded, err := json.MarshalIndent(document, "", "  ")
		if err != nil {
			return nil, err
		}
		return string(encoded), nil
	}
	if name == "JSON_KEYS" {
		if len(args) < 1 || len(args) > 2 {
			return nil, fmt.Errorf("JSON_KEYS requires 1 or 2 arguments")
		}
		var document interface{}
		if err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &document); err != nil {
			return nil, err
		}
		if len(args) == 2 {
			var found bool
			document, found = jsonLookupPath(document, compatString(args[1]))
			if !found {
				return nil, nil
			}
		}
		object, ok := document.(map[string]interface{})
		if !ok {
			return nil, nil
		}
		keys := make([]string, 0, len(object))
		for key := range object {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		encoded, err := json.Marshal(keys)
		if err != nil {
			return nil, err
		}
		return string(encoded), nil
	}
	if name != "JSON_TYPE" && name != "JSON_LENGTH" && len(args) < 2 {
		return nil, fmt.Errorf("JSON_EXTRACT requires a document and path")
	}
	var document interface{}
	if err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &document); err != nil {
		return nil, err
	}
	if name == "JSON_CONTAINS" {
		var candidate interface{}
		if err := json.Unmarshal([]byte(fmt.Sprint(args[1])), &candidate); err != nil {
			return nil, err
		}
		if jsonValueContains(document, candidate) {
			return int64(1), nil
		}
		return int64(0), nil
	}
	if name == "JSON_VALUE" {
		if len(args) != 2 {
			return nil, fmt.Errorf("JSON_VALUE requires a document and path")
		}
		value, found := jsonExtractPathValue(document, compatString(args[1]))
		if !found {
			return nil, nil
		}
		switch value.(type) {
		case map[string]interface{}, []interface{}:
			// JSON_VALUE returns SQL scalars; object/array extraction is
			// outside this basic compatibility path and becomes NULL.
			return nil, nil
		default:
			return value, nil
		}
	}
	if name == "JSON_CONTAINS_PATH" {
		if len(args) < 3 {
			return nil, fmt.Errorf("JSON_CONTAINS_PATH requires a document, mode, and path")
		}
		mode := strings.ToLower(compatString(args[1]))
		if mode != "one" && mode != "all" {
			return nil, fmt.Errorf("JSON_CONTAINS_PATH mode must be 'one' or 'all'")
		}
		matched := 0
		for _, pathArg := range args[2:] {
			if _, found := jsonLookupPath(document, compatString(pathArg)); found {
				matched++
			}
		}
		if (mode == "one" && matched > 0) || (mode == "all" && matched == len(args)-2) {
			return int64(1), nil
		}
		return int64(0), nil
	}
	if name == "JSON_TYPE" {
		return jsonTypeName(document), nil
	}
	if name == "JSON_LENGTH" {
		value := document
		if len(args) > 1 {
			var found bool
			value, found = jsonLookupPath(document, compatString(args[1]))
			if !found {
				return nil, nil
			}
		}
		switch typed := value.(type) {
		case []interface{}:
			return int64(len(typed)), nil
		case map[string]interface{}:
			return int64(len(typed)), nil
		default:
			return int64(1), nil
		}
	}
	if name == "JSON_ARRAY_APPEND" {
		return evalJSONArrayAppend(document, args[1:])
	}
	if name == "JSON_ARRAY_INSERT" {
		return evalJSONArrayInsert(document, args[1:])
	}
	if name == "JSON_SEARCH" {
		return evalJSONSearch(args)
	}
	if name == "JSON_INSERT" || name == "JSON_SET" || name == "JSON_REPLACE" || name == "JSON_REMOVE" {
		return evalJSONMutation(name, document, args[1:])
	}
	if name == "JSON_EXTRACT" {
		if len(args) > 2 {
			values := make([]interface{}, 0, len(args)-1)
			for _, pathArg := range args[1:] {
				value, found := jsonExtractPathValue(document, compatString(pathArg))
				if found {
					values = append(values, value)
				}
			}
			encoded, err := json.Marshal(values)
			if err != nil {
				return nil, err
			}
			return string(encoded), nil
		}
		value, found := jsonExtractPathValue(document, compatString(args[1]))
		if !found {
			return nil, nil
		}
		return value, nil
	}
	path := strings.TrimPrefix(fmt.Sprint(args[1]), "$.")
	current := document
	for _, part := range strings.Split(path, ".") {
		object, ok := current.(map[string]interface{})
		if !ok {
			return nil, nil
		}
		current, ok = object[part]
		if !ok {
			return nil, nil
		}
	}
	return current, nil
}

func jsonExtractPathValue(document interface{}, path string) (interface{}, bool) {
	trimmed := strings.TrimSpace(path)
	resolvedPath, resolved, hadLast := jsonResolveLastArrayIndexes(document, trimmed)
	if hadLast {
		if !resolved {
			return nil, false
		}
		trimmed = resolvedPath
	}
	if strings.Contains(trimmed, "**") {
		values, found := jsonCollectRecursivePath(document, trimmed)
		if !found {
			return nil, false
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return nil, false
		}
		return string(encoded), true
	}
	_, _, _, _, hasRange := jsonArrayRangeToken(trimmed)
	if !strings.Contains(trimmed, "[*]") && !strings.Contains(trimmed, ".*") && !hasRange {
		return jsonLookupPath(document, trimmed)
	}
	values, found := jsonCollectWildcardPath(document, trimmed)
	if !found {
		return nil, false
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return nil, false
	}
	return string(encoded), true
}

func jsonResolveLastArrayIndexes(document interface{}, path string) (string, bool, bool) {
	resolvedPath := path
	hadLast := false
	for depth := 0; depth < 16; depth++ {
		lowerPath := strings.ToLower(resolvedPath)
		lastOffset := strings.Index(lowerPath, "[last")
		if lastOffset < 0 {
			return resolvedPath, true, hadLast
		}
		hadLast = true
		closeOffset := strings.IndexByte(resolvedPath[lastOffset:], ']')
		if closeOffset < 0 {
			return "", false, hadLast
		}
		closeOffset += lastOffset
		token := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(resolvedPath[lastOffset+1:closeOffset]), " ", ""))
		offset := 0
		switch {
		case token == "last":
		case strings.HasPrefix(token, "last-"):
			parsed, err := strconv.Atoi(strings.TrimPrefix(token, "last-"))
			if err != nil || parsed < 0 {
				return "", false, hadLast
			}
			offset = parsed
		default:
			return "", false, hadLast
		}
		prefix := resolvedPath[:lastOffset]
		value, found := jsonLookupPath(document, prefix)
		array, ok := value.([]interface{})
		if !found || !ok || len(array) == 0 || offset >= len(array) {
			return "", false, hadLast
		}
		index := len(array) - 1 - offset
		resolvedPath = prefix + fmt.Sprintf("[%d]", index) + resolvedPath[closeOffset+1:]
	}
	return "", false, hadLast
}

func jsonCollectRecursivePath(document interface{}, path string) ([]interface{}, bool) {
	operator := strings.Index(path, "**")
	if operator < 0 {
		return nil, false
	}
	prefix := strings.TrimSuffix(path[:operator], ".")
	if prefix == "" {
		prefix = "$"
	}
	root, found := jsonLookupPath(document, prefix)
	if !found {
		return nil, false
	}
	suffix := path[operator+2:]
	if suffix == "" || (suffix[0] != '.' && suffix[0] != '[') {
		return nil, false
	}
	childPath := "$" + suffix
	values := make([]interface{}, 0)
	var walk func(interface{})
	walk = func(value interface{}) {
		if match, ok := jsonLookupPath(value, childPath); ok {
			values = append(values, match)
		}
		switch typed := value.(type) {
		case map[string]interface{}:
			keys := make([]string, 0, len(typed))
			for key := range typed {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				walk(typed[key])
			}
		case []interface{}:
			for _, child := range typed {
				walk(child)
			}
		}
	}
	walk(root)
	return values, len(values) > 0
}

// jsonCollectWildcardPath evaluates the bounded wildcard subset used by
// JSON_EXTRACT.  Results are collected in array order and object-key order so
// the JSON array representation is deterministic.  Every wildcard is applied
// recursively, which covers paths such as $.items[*].id and $.groups.*.name.
func jsonCollectWildcardPath(document interface{}, path string) ([]interface{}, bool) {
	arrayWildcard := strings.Index(path, "[*]")
	objectWildcard := strings.Index(path, ".*")
	rangeIndex, rangeLength, rangeStart, rangeEnd, hasRange := jsonArrayRangeToken(path)
	wildcardIndex := -1
	wildcardLength := 0
	wildcardKind := ""
	if arrayWildcard >= 0 {
		wildcardIndex = arrayWildcard
		wildcardLength = len("[*]")
		wildcardKind = "array"
	}
	if objectWildcard >= 0 && (wildcardIndex < 0 || objectWildcard < wildcardIndex) {
		wildcardIndex = objectWildcard
		wildcardLength = len(".*")
		wildcardKind = "object"
	}
	if hasRange && (wildcardIndex < 0 || rangeIndex < wildcardIndex) {
		wildcardIndex = rangeIndex
		wildcardLength = rangeLength
		wildcardKind = "range"
	}
	if wildcardIndex < 0 {
		value, found := jsonLookupPath(document, path)
		if !found {
			return nil, false
		}
		return []interface{}{value}, true
	}

	prefix := path[:wildcardIndex]
	value, found := jsonLookupPath(document, prefix)
	if !found {
		return nil, false
	}
	suffix := path[wildcardIndex+wildcardLength:]
	children := make([]interface{}, 0)
	switch wildcardKind {
	case "object":
		container, ok := value.(map[string]interface{})
		if !ok {
			return nil, false
		}
		keys := make([]string, 0, len(container))
		for key := range container {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			children = append(children, container[key])
		}
	case "array":
		container, ok := value.([]interface{})
		if !ok {
			return nil, false
		}
		children = append(children, container...)
	case "range":
		container, ok := value.([]interface{})
		if !ok || rangeStart >= len(container) {
			return nil, false
		}
		if rangeEnd >= len(container) {
			rangeEnd = len(container) - 1
		}
		if rangeStart > rangeEnd {
			return nil, false
		}
		children = append(children, container[rangeStart:rangeEnd+1]...)
	default:
		return nil, false
	}
	if suffix == "" {
		return children, true
	}

	result := make([]interface{}, 0, len(children))
	childPath := "$" + suffix
	for _, child := range children {
		matches, childFound := jsonCollectWildcardPath(child, childPath)
		if childFound {
			result = append(result, matches...)
		}
	}
	return result, len(result) > 0
}

func jsonArrayRangeToken(path string) (index, length, start, end int, found bool) {
	for offset := 0; offset < len(path); offset++ {
		if path[offset] != '[' {
			continue
		}
		closeOffset := strings.IndexByte(path[offset+1:], ']')
		if closeOffset < 0 {
			return 0, 0, 0, 0, false
		}
		closeOffset += offset + 1
		tokens := strings.Fields(strings.TrimSpace(path[offset+1 : closeOffset]))
		if len(tokens) != 3 || strings.ToLower(tokens[1]) != "to" {
			offset = closeOffset
			continue
		}
		lower, lowerErr := strconv.Atoi(tokens[0])
		upper, upperErr := strconv.Atoi(tokens[2])
		if lowerErr != nil || upperErr != nil || lower < 0 || upper < lower {
			offset = closeOffset
			continue
		}
		return offset, closeOffset - offset + 1, lower, upper, true
	}
	return 0, 0, 0, 0, false
}

func jsonValueDepth(value interface{}) int {
	switch typed := value.(type) {
	case map[string]interface{}:
		depth := 1
		for _, child := range typed {
			if childDepth := jsonValueDepth(child) + 1; childDepth > depth {
				depth = childDepth
			}
		}
		return depth
	case []interface{}:
		depth := 1
		for _, child := range typed {
			if childDepth := jsonValueDepth(child) + 1; childDepth > depth {
				depth = childDepth
			}
		}
		return depth
	default:
		return 1
	}
}

func evalJSONMutation(name string, document interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 1 || (name != "JSON_REMOVE" && len(args)%2 != 0) {
		return nil, fmt.Errorf("%s received an invalid argument list", name)
	}
	for index := 0; index < len(args); {
		path := compatString(args[index])
		parts, err := jsonPathParts(path)
		if err != nil {
			return nil, err
		}
		value := interface{}(nil)
		if name != "JSON_REMOVE" {
			value = args[index+1]
		}
		updated, _, err := mutateJSONPath(document, parts, value, name, name == "JSON_SET")
		if err != nil {
			return nil, err
		}
		document = updated
		if name == "JSON_REMOVE" {
			index++
		} else {
			index += 2
		}
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}

func evalJSONArrayAppend(document interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, fmt.Errorf("JSON_ARRAY_APPEND received an invalid argument list")
	}
	for index := 0; index < len(args); index += 2 {
		parts, err := jsonPathParts(compatString(args[index]))
		if err != nil {
			return nil, err
		}
		updated, changed := appendJSONPath(document, parts, args[index+1])
		if changed {
			document = updated
		}
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}

func evalJSONArrayInsert(document interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, fmt.Errorf("JSON_ARRAY_INSERT received an invalid argument list")
	}
	for index := 0; index < len(args); index += 2 {
		parts, err := jsonPathParts(compatString(args[index]))
		if err != nil {
			return nil, err
		}
		updated, changed := insertJSONPath(document, parts, args[index+1])
		if changed {
			document = updated
		}
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}

func evalJSONSearch(args []interface{}) (interface{}, error) {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil, nil
	}
	mode := strings.ToLower(compatString(args[1]))
	if mode != "one" && mode != "all" {
		return nil, fmt.Errorf("JSON_SEARCH mode must be 'one' or 'all'")
	}
	var document interface{}
	if err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &document); err != nil {
		return nil, err
	}
	escape := "\\"
	pathStart := 3
	if len(args) > 3 {
		if args[3] != nil {
			escape = compatString(args[3])
			if len([]rune(escape)) > 1 {
				return nil, fmt.Errorf("JSON_SEARCH escape character must be empty or one character")
			}
		}
		pathStart = 4
	}
	paths := []string{"$"}
	if len(args) > pathStart {
		paths = make([]string, 0, len(args)-pathStart)
		for _, rawPath := range args[pathStart:] {
			paths = append(paths, compatString(rawPath))
		}
	}
	pattern, err := jsonSearchPattern(compatString(args[2]), escape)
	if err != nil {
		return nil, err
	}
	matchedPaths := make([]string, 0, 4)
	for _, path := range paths {
		root, found := jsonLookupPath(document, path)
		if !found && path != "$" {
			continue
		}
		jsonSearchWalk(root, path, pattern, &matchedPaths)
		if mode == "one" && len(matchedPaths) > 0 {
			return matchedPaths[0], nil
		}
	}
	if len(matchedPaths) == 0 {
		return nil, nil
	}
	if mode == "one" {
		return matchedPaths[0], nil
	}
	encoded, err := json.Marshal(matchedPaths)
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}

func jsonSearchPattern(pattern, escape string) (*regexp.Regexp, error) {
	var builder strings.Builder
	builder.WriteString("^")
	escaped := false
	for _, char := range pattern {
		if escape != "" && string(char) == escape && !escaped {
			escaped = true
			continue
		}
		if !escaped {
			switch char {
			case '%':
				builder.WriteString(".*")
				continue
			case '_':
				builder.WriteByte('.')
				continue
			}
		}
		builder.WriteString(regexp.QuoteMeta(string(char)))
		escaped = false
	}
	if escaped {
		builder.WriteString(regexp.QuoteMeta(escape))
	}
	builder.WriteString("$")
	return regexp.Compile(builder.String())
}

func jsonSearchWalk(value interface{}, path string, pattern *regexp.Regexp, matches *[]string) {
	switch typed := value.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			childPath := path + "." + key
			jsonSearchWalk(typed[key], childPath, pattern, matches)
		}
	case []interface{}:
		for index, child := range typed {
			jsonSearchWalk(child, fmt.Sprintf("%s[%d]", path, index), pattern, matches)
		}
	case string:
		if pattern.MatchString(typed) {
			*matches = append(*matches, path)
		}
	}
}

type jsonPathPart struct {
	key     string
	index   int
	isIndex bool
}

func jsonPathParts(path string) ([]jsonPathPart, error) {
	trimmed := strings.TrimSpace(path)
	if !strings.HasPrefix(trimmed, "$") {
		return nil, fmt.Errorf("unsupported JSON path %q", path)
	}
	rest := strings.TrimPrefix(trimmed, "$")
	parts := make([]jsonPathPart, 0, 2)
	for len(rest) > 0 {
		switch rest[0] {
		case '.':
			rest = rest[1:]
			end := len(rest)
			if bracket := strings.IndexByte(rest, '['); bracket >= 0 && bracket < end {
				end = bracket
			}
			if next := strings.IndexByte(rest, '.'); next >= 0 && next < end {
				end = next
			}
			key := rest[:end]
			if key == "" {
				return nil, fmt.Errorf("unsupported JSON path %q", path)
			}
			parts = append(parts, jsonPathPart{key: key})
			rest = rest[end:]
		case '[':
			close := strings.IndexByte(rest, ']')
			if close <= 1 {
				return nil, fmt.Errorf("unsupported JSON path %q", path)
			}
			index, err := strconv.Atoi(rest[1:close])
			if err != nil || index < 0 {
				return nil, fmt.Errorf("unsupported JSON path %q", path)
			}
			parts = append(parts, jsonPathPart{index: index, isIndex: true})
			rest = rest[close+1:]
		default:
			return nil, fmt.Errorf("unsupported JSON path %q", path)
		}
	}
	return parts, nil
}

func jsonLookupPath(document interface{}, path string) (interface{}, bool) {
	parts, err := jsonPathParts(path)
	if err != nil {
		return nil, false
	}
	current := document
	for _, part := range parts {
		switch container := current.(type) {
		case map[string]interface{}:
			if part.isIndex {
				return nil, false
			}
			var exists bool
			current, exists = container[part.key]
			if !exists {
				return nil, false
			}
		case []interface{}:
			if !part.isIndex || part.index >= len(container) {
				return nil, false
			}
			current = container[part.index]
		default:
			return nil, false
		}
	}
	return current, true
}

func mutateJSONPath(container interface{}, parts []jsonPathPart, value interface{}, name string, create bool) (interface{}, bool, error) {
	if len(parts) == 0 {
		return container, false, nil
	}
	part := parts[0]
	if len(parts) == 1 {
		switch current := container.(type) {
		case map[string]interface{}:
			if part.isIndex {
				return container, false, nil
			}
			_, exists := current[part.key]
			switch name {
			case "JSON_SET":
				current[part.key] = value
			case "JSON_INSERT":
				if exists {
					return container, false, nil
				}
				current[part.key] = value
			case "JSON_REPLACE":
				if !exists {
					return container, false, nil
				}
				current[part.key] = value
			case "JSON_REMOVE":
				if !exists {
					return container, false, nil
				}
				delete(current, part.key)
			}
			return current, true, nil
		case []interface{}:
			if !part.isIndex {
				return container, false, nil
			}
			if part.index < 0 || part.index > len(current) || (name != "JSON_SET" && name != "JSON_INSERT" && part.index >= len(current)) {
				return container, false, nil
			}
			switch name {
			case "JSON_SET":
				if part.index == len(current) {
					current = append(current, value)
				} else {
					current[part.index] = value
				}
			case "JSON_INSERT":
				if part.index == len(current) {
					current = append(current, value)
				} else {
					return container, false, nil
				}
			case "JSON_REPLACE":
				current[part.index] = value
			case "JSON_REMOVE":
				current = append(current[:part.index], current[part.index+1:]...)
			}
			return current, true, nil
		default:
			return container, false, nil
		}
	}

	switch current := container.(type) {
	case map[string]interface{}:
		if part.isIndex {
			return container, false, nil
		}
		child, exists := current[part.key]
		if !exists {
			if !create {
				return container, false, nil
			}
			child = jsonMutationContainer(parts[1])
		}
		updated, changed, err := mutateJSONPath(child, parts[1:], value, name, create)
		if err != nil || !changed {
			return container, changed, err
		}
		current[part.key] = updated
		return current, true, nil
	case []interface{}:
		if !part.isIndex || part.index < 0 || part.index > len(current) || (part.index == len(current) && !create) {
			return container, false, nil
		}
		if part.index == len(current) {
			current = append(current, jsonMutationContainer(parts[1]))
		}
		updated, changed, err := mutateJSONPath(current[part.index], parts[1:], value, name, create)
		if err != nil || !changed {
			return container, changed, err
		}
		current[part.index] = updated
		return current, true, nil
	default:
		return container, false, nil
	}
}

func appendJSONPath(container interface{}, parts []jsonPathPart, value interface{}) (interface{}, bool) {
	if len(parts) == 0 {
		switch current := container.(type) {
		case []interface{}:
			return append(current, value), true
		default:
			return []interface{}{current, value}, true
		}
	}
	part := parts[0]
	if len(parts) == 1 {
		switch current := container.(type) {
		case map[string]interface{}:
			child, exists := current[part.key]
			if part.isIndex || !exists {
				return container, false
			}
			updated, changed := appendJSONPath(child, nil, value)
			if !changed {
				return container, false
			}
			current[part.key] = updated
			return current, true
		case []interface{}:
			if !part.isIndex || part.index < 0 || part.index >= len(current) {
				return container, false
			}
			updated, changed := appendJSONPath(current[part.index], nil, value)
			if !changed {
				return container, false
			}
			current[part.index] = updated
			return current, true
		default:
			return container, false
		}
	}

	switch current := container.(type) {
	case map[string]interface{}:
		if part.isIndex {
			return container, false
		}
		child, exists := current[part.key]
		if !exists {
			return container, false
		}
		updated, changed := appendJSONPath(child, parts[1:], value)
		if !changed {
			return container, false
		}
		current[part.key] = updated
		return current, true
	case []interface{}:
		if !part.isIndex || part.index < 0 || part.index >= len(current) {
			return container, false
		}
		updated, changed := appendJSONPath(current[part.index], parts[1:], value)
		if !changed {
			return container, false
		}
		current[part.index] = updated
		return current, true
	default:
		return container, false
	}
}

func insertJSONPath(container interface{}, parts []jsonPathPart, value interface{}) (interface{}, bool) {
	if len(parts) == 0 {
		return container, false
	}
	part := parts[0]
	if len(parts) == 1 {
		if !part.isIndex {
			return container, false
		}
		current, ok := container.([]interface{})
		if !ok || part.index < 0 || part.index > len(current) {
			return container, false
		}
		current = append(current, nil)
		copy(current[part.index+1:], current[part.index:])
		current[part.index] = value
		return current, true
	}

	switch current := container.(type) {
	case map[string]interface{}:
		if part.isIndex {
			return container, false
		}
		child, exists := current[part.key]
		if !exists {
			return container, false
		}
		updated, changed := insertJSONPath(child, parts[1:], value)
		if !changed {
			return container, false
		}
		current[part.key] = updated
		return current, true
	case []interface{}:
		if !part.isIndex || part.index < 0 || part.index >= len(current) {
			return container, false
		}
		updated, changed := insertJSONPath(current[part.index], parts[1:], value)
		if !changed {
			return container, false
		}
		current[part.index] = updated
		return current, true
	default:
		return container, false
	}
}

func jsonMutationContainer(next jsonPathPart) interface{} {
	if next.isIndex {
		return []interface{}{}
	}
	return map[string]interface{}{}
}

func jsonTypeName(value interface{}) string {
	switch value.(type) {
	case nil:
		return "NULL"
	case map[string]interface{}:
		return "OBJECT"
	case []interface{}:
		return "ARRAY"
	case bool:
		return "BOOLEAN"
	case string:
		return "STRING"
	case float64:
		if value.(float64) == math.Trunc(value.(float64)) {
			return "INTEGER"
		}
		return "DOUBLE"
	default:
		return "STRING"
	}
}

func jsonValueContains(document, candidate interface{}) bool {
	switch wanted := candidate.(type) {
	case map[string]interface{}:
		actual, ok := document.(map[string]interface{})
		if !ok {
			return false
		}
		for key, value := range wanted {
			actualValue, exists := actual[key]
			if !exists || !jsonValueContains(actualValue, value) {
				return false
			}
		}
		return true
	case []interface{}:
		actual, ok := document.([]interface{})
		if !ok {
			return false
		}
		for _, wantedValue := range wanted {
			found := false
			for _, actualValue := range actual {
				if jsonValueContains(actualValue, wantedValue) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	default:
		return fmt.Sprint(document) == fmt.Sprint(candidate)
	}
}

func jsonValuesOverlap(left, right interface{}) bool {
	if leftArray, ok := left.([]interface{}); ok {
		if rightArray, ok := right.([]interface{}); ok {
			for _, leftValue := range leftArray {
				for _, rightValue := range rightArray {
					if jsonValuesOverlap(leftValue, rightValue) {
						return true
					}
				}
			}
			return false
		}
	}
	if leftObject, ok := left.(map[string]interface{}); ok {
		if rightObject, ok := right.(map[string]interface{}); ok {
			for key, leftValue := range leftObject {
				if rightValue, exists := rightObject[key]; exists && jsonValuesOverlap(leftValue, rightValue) {
					return true
				}
			}
			return false
		}
	}
	return jsonValueContains(left, right) && jsonValueContains(right, left)
}

func mergeJSONPatch(target, patch interface{}) interface{} {
	patchObject, ok := patch.(map[string]interface{})
	if !ok {
		return patch
	}
	targetObject, ok := target.(map[string]interface{})
	if !ok {
		targetObject = make(map[string]interface{}, len(patchObject))
	}
	for key, value := range patchObject {
		if value == nil {
			delete(targetObject, key)
			continue
		}
		if nested, ok := value.(map[string]interface{}); ok {
			targetObject[key] = mergeJSONPatch(targetObject[key], nested)
			continue
		}
		targetObject[key] = value
	}
	return targetObject
}

// mergeJSONPreserve implements the legacy JSON merge operation. Unlike
// JSON_MERGE_PATCH, duplicate object members are retained by combining their
// values, and arrays are concatenated. When the two values have different
// shapes, MySQL treats both values as array elements.
func mergeJSONPreserve(left, right interface{}) interface{} {
	leftArray, leftIsArray := left.([]interface{})
	rightArray, rightIsArray := right.([]interface{})
	if leftIsArray || rightIsArray {
		merged := make([]interface{}, 0, len(leftArray)+len(rightArray)+2)
		if leftIsArray {
			merged = append(merged, leftArray...)
		} else {
			merged = append(merged, left)
		}
		if rightIsArray {
			merged = append(merged, rightArray...)
		} else {
			merged = append(merged, right)
		}
		return merged
	}

	leftObject, leftIsObject := left.(map[string]interface{})
	rightObject, rightIsObject := right.(map[string]interface{})
	if leftIsObject && rightIsObject {
		merged := make(map[string]interface{}, len(leftObject)+len(rightObject))
		for key, value := range leftObject {
			merged[key] = value
		}
		for key, value := range rightObject {
			if existing, ok := merged[key]; ok {
				merged[key] = mergeJSONPreserve(existing, value)
			} else {
				merged[key] = value
			}
		}
		return merged
	}

	return []interface{}{left, right}
}

func (f *Function) String() string {
	args := make([]string, len(f.FuncArgs))
	for i, arg := range f.FuncArgs {
		args[i] = arg.String()
	}
	prefix := ""
	if f.Distinct {
		prefix = "DISTINCT "
	}
	result := fmt.Sprintf("%s(%s%s)", f.FuncName, prefix, strings.Join(args, ", "))
	if f.UsingCharset != "" && (strings.EqualFold(f.FuncName, "CONVERT_USING") || strings.EqualFold(f.FuncName, "CONVERT")) {
		result = fmt.Sprintf("%s(%s USING %s)", f.FuncName, strings.Join(args, ", "), f.UsingCharset)
	}
	if f.Separator != "" {
		result = strings.TrimSuffix(result, ")") + fmt.Sprintf(" SEPARATOR '%s')", f.Separator)
	}
	return result
}

// 运算符求值函数
func evalAdd(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	switch l := left.(type) {
	case int64:
		if r, ok := right.(int64); ok {
			return l + r, nil
		}
	case float64:
		if r, ok := right.(float64); ok {
			return l + r, nil
		}
	}
	if result, ok, err := evalNumericArithmetic(left, right, '+'); ok {
		return result, err
	}
	return nil, fmt.Errorf("unsupported operand types for +: %T and %T", left, right)
}

func evalSub(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	switch l := left.(type) {
	case int64:
		if r, ok := right.(int64); ok {
			return l - r, nil
		}
	case float64:
		if r, ok := right.(float64); ok {
			return l - r, nil
		}
	}
	if result, ok, err := evalNumericArithmetic(left, right, '-'); ok {
		return result, err
	}
	return nil, fmt.Errorf("unsupported operand types for -: %T and %T", left, right)
}

func evalMul(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	switch l := left.(type) {
	case int64:
		if r, ok := right.(int64); ok {
			return l * r, nil
		}
	case float64:
		if r, ok := right.(float64); ok {
			return l * r, nil
		}
	}
	if result, ok, err := evalNumericArithmetic(left, right, '*'); ok {
		return result, err
	}
	return nil, fmt.Errorf("unsupported operand types for *: %T and %T", left, right)
}

func evalDiv(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	switch l := left.(type) {
	case int64:
		if r, ok := right.(int64); ok {
			if r == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return l / r, nil
		}
	case float64:
		if r, ok := right.(float64); ok {
			if r == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return l / r, nil
		}
	}
	if result, ok, err := evalNumericArithmetic(left, right, '/'); ok {
		return result, err
	}
	return nil, fmt.Errorf("unsupported operand types for /: %T and %T", left, right)
}

func evalIntDiv(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	leftNumber, leftOK := numericComparableValue(left)
	rightNumber, rightOK := numericComparableValue(right)
	if !leftOK || !rightOK {
		return nil, fmt.Errorf("unsupported operand types for DIV: %T and %T", left, right)
	}
	if rightNumber == 0 {
		return nil, fmt.Errorf("division by zero")
	}
	return int64(math.Trunc(leftNumber / rightNumber)), nil
}

func evalMod(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	leftNumber, leftOK := numericComparableValue(left)
	rightNumber, rightOK := numericComparableValue(right)
	if !leftOK || !rightOK {
		return nil, fmt.Errorf("unsupported operand types for %%: %T and %T", left, right)
	}
	if rightNumber == 0 {
		return nil, fmt.Errorf("division by zero")
	}
	result := math.Mod(leftNumber, rightNumber)
	if integerLikeArithmeticValue(left) && integerLikeArithmeticValue(right) {
		return int64(result), nil
	}
	return result, nil
}

func evalBitwise(left, right interface{}, operator string) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	leftNumber, leftOK := numericComparableValue(left)
	rightNumber, rightOK := numericComparableValue(right)
	if !leftOK || !rightOK {
		return nil, fmt.Errorf("unsupported operand types for %s: %T and %T", operator, left, right)
	}
	leftValue := uint64(int64(leftNumber))
	rightValue := uint64(int64(rightNumber))
	switch operator {
	case "&":
		return int64(leftValue & rightValue), nil
	case "|":
		return int64(leftValue | rightValue), nil
	case "^":
		return int64(leftValue ^ rightValue), nil
	case "<<":
		return int64(leftValue << rightValue), nil
	case ">>":
		return int64(leftValue >> rightValue), nil
	default:
		return nil, fmt.Errorf("unsupported bitwise operator: %s", operator)
	}
}

func evalRegexp(left, right interface{}, negate bool) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	matched, err := regexp.MatchString(fmt.Sprint(right), fmt.Sprint(left))
	if err != nil {
		return nil, err
	}
	if negate {
		return !matched, nil
	}
	return matched, nil
}

func evalNumericArithmetic(left, right interface{}, operator byte) (interface{}, bool, error) {
	leftNumber, leftOK := numericComparableValue(left)
	rightNumber, rightOK := numericComparableValue(right)
	if !leftOK || !rightOK {
		return nil, false, nil
	}
	if operator == '/' && rightNumber == 0 {
		return nil, true, fmt.Errorf("division by zero")
	}
	var result float64
	switch operator {
	case '+':
		result = leftNumber + rightNumber
	case '-':
		result = leftNumber - rightNumber
	case '*':
		result = leftNumber * rightNumber
	case '/':
		result = leftNumber / rightNumber
	default:
		return nil, false, nil
	}
	if integerLikeArithmeticValue(left) && integerLikeArithmeticValue(right) && operator != '/' {
		return int64(result), true, nil
	}
	return result, true, nil
}

func integerLikeArithmeticValue(value interface{}) bool {
	switch typed := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case string:
		_, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		return err == nil
	case []byte:
		_, err := strconv.ParseInt(strings.TrimSpace(string(typed)), 10, 64)
		return err == nil
	default:
		return false
	}
}

func evalEQ(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		// SQL equality is UNKNOWN when either operand is NULL; it is not
		// Go's nil equality. WHERE/EXISTS callers then discard UNKNOWN.
		return nil, nil
	}
	comparison, comparable := compareScalarValues(left, right)
	if comparable {
		return comparison == 0, nil
	}
	return false, nil
}

func evalNE(left, right interface{}) (interface{}, error) {
	eq, err := evalEQ(left, right)
	if err != nil {
		return nil, err
	}
	if eq == nil {
		return nil, nil
	}
	return !eq.(bool), nil
}

func evalLT(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	comparison, comparable := compareScalarValues(left, right)
	if comparable {
		return comparison < 0, nil
	}
	return nil, fmt.Errorf("cannot compare %T with %T", left, right)
}

func compareScalarValues(left, right interface{}) (int, bool) {
	// MySQL compares two strings lexically. Numeric coercion applies when at
	// least one operand is already numeric, which also covers the common
	// VARCHAR-to-number comparison used by JDBC predicates.
	_, leftString := left.(string)
	_, rightString := right.(string)
	if !leftString || !rightString {
		if leftNumber, ok := numericComparableValue(left); ok {
			if rightNumber, ok := numericComparableValue(right); ok {
				switch {
				case leftNumber < rightNumber:
					return -1, true
				case leftNumber > rightNumber:
					return 1, true
				default:
					return 0, true
				}
			}
		}
	}
	if leftValue, ok := left.(string); ok {
		if rightValue, ok := right.(string); ok {
			switch {
			case leftValue < rightValue:
				return -1, true
			case leftValue > rightValue:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	if leftValue, ok := left.(bool); ok {
		if rightValue, ok := right.(bool); ok {
			if leftValue == rightValue {
				return 0, true
			}
			if !leftValue {
				return -1, true
			}
			return 1, true
		}
	}
	return 0, false
}

func numericComparableValue(value interface{}) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	case []byte:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(string(typed)), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func evalLE(left, right interface{}) (interface{}, error) {
	lt, err := evalLT(left, right)
	if err != nil {
		return nil, err
	}
	eq, err := evalEQ(left, right)
	if err != nil {
		return nil, err
	}
	if lt == nil || eq == nil {
		return nil, nil
	}
	return lt.(bool) || eq.(bool), nil
}

func evalGT(left, right interface{}) (interface{}, error) {
	le, err := evalLE(left, right)
	if err != nil {
		return nil, err
	}
	if le == nil {
		return nil, nil
	}
	return !le.(bool), nil
}

func evalGE(left, right interface{}) (interface{}, error) {
	lt, err := evalLT(left, right)
	if err != nil {
		return nil, err
	}
	if lt == nil {
		return nil, nil
	}
	return !lt.(bool), nil
}

func evalAnd(left, right interface{}) (interface{}, error) {
	// SQL three-valued logic: FALSE dominates UNKNOWN; TRUE/UNKNOWN is
	// UNKNOWN. This matters when a nullable predicate is used in an index
	// merge branch or a correlated subquery.
	if left == nil {
		if right == nil {
			return nil, nil
		}
		rightTruth, err := expressionTruthValue(right)
		if err != nil {
			return nil, err
		}
		if !rightTruth {
			return false, nil
		}
		return nil, nil
	}
	if right == nil {
		leftTruth, err := expressionTruthValue(left)
		if err != nil {
			return nil, err
		}
		if !leftTruth {
			return false, nil
		}
		return nil, nil
	}
	l, err := expressionTruthValue(left)
	if err != nil {
		return nil, err
	}
	r, err := expressionTruthValue(right)
	if err != nil {
		return nil, err
	}
	return l && r, nil
}

func evalOr(left, right interface{}) (interface{}, error) {
	// TRUE dominates UNKNOWN; FALSE/UNKNOWN is UNKNOWN.
	if left == nil {
		if right == nil {
			return nil, nil
		}
		rightTruth, err := expressionTruthValue(right)
		if err != nil {
			return nil, err
		}
		if rightTruth {
			return true, nil
		}
		return nil, nil
	}
	if right == nil {
		leftTruth, err := expressionTruthValue(left)
		if err != nil {
			return nil, err
		}
		if leftTruth {
			return true, nil
		}
		return nil, nil
	}
	l, err := expressionTruthValue(left)
	if err != nil {
		return nil, err
	}
	r, err := expressionTruthValue(right)
	if err != nil {
		return nil, err
	}
	return l || r, nil
}

func evalLike(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	l := compatString(left)
	r := compatString(right)
	pattern := sqlLikePatternRegex(r)
	matched, err := regexp.MatchString(pattern, l)
	if err != nil {
		return nil, err
	}
	return matched, nil
}

func evalLikeWithEscape(left, right, escape interface{}) (interface{}, error) {
	if left == nil || right == nil || escape == nil {
		return nil, nil
	}
	escapeRunes := []rune(compatString(escape))
	if len(escapeRunes) != 1 {
		return nil, fmt.Errorf("LIKE ESCAPE requires a single character, got %q", compatString(escape))
	}
	pattern := sqlLikePatternRegexWithEscape(compatString(right), escapeRunes[0])
	matched, err := regexp.MatchString(pattern, compatString(left))
	if err != nil {
		return nil, err
	}
	return matched, nil
}

func sqlLikePatternRegex(pattern string) string {
	return sqlLikePatternRegexWithEscape(pattern, '\\')
}

func sqlLikePatternRegexWithEscape(pattern string, escapeCharacter rune) string {
	var regex strings.Builder
	regex.WriteString("(?i)^")
	escaped := false
	for _, character := range pattern {
		if escaped {
			regex.WriteString(regexp.QuoteMeta(string(character)))
			escaped = false
			continue
		}
		if character == escapeCharacter {
			escaped = true
			continue
		}
		switch character {
		case '%':
			regex.WriteString(".*")
		case '_':
			regex.WriteString(".")
		default:
			regex.WriteString(regexp.QuoteMeta(string(character)))
		}
	}
	if escaped {
		regex.WriteString(regexp.QuoteMeta(string(escapeCharacter)))
	}
	regex.WriteString("$")
	return regex.String()
}

func evalIn(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	list, ok := right.([]interface{})
	if !ok {
		return nil, fmt.Errorf("IN requires array as right operand, got %T", right)
	}
	unknown := false
	for _, item := range list {
		eq, err := evalEQ(left, item)
		if err != nil {
			return nil, err
		}
		if eq == nil {
			unknown = true
			continue
		}
		if eq.(bool) {
			return true, nil
		}
	}
	if unknown {
		return nil, nil
	}
	return false, nil
}

func evalNotIn(left, right interface{}) (interface{}, error) {
	matched, err := evalIn(left, right)
	if err != nil || matched == nil {
		return matched, err
	}
	return !matched.(bool), nil
}

func evalNullSafeEQ(left, right interface{}) (interface{}, error) {
	if left == nil && right == nil {
		return true, nil
	}
	if left == nil || right == nil {
		return false, nil
	}
	return evalEQ(left, right)
}

// 聚合函数求值
func evalCount(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("COUNT requires exactly 1 argument")
	}
	if args[0] == nil {
		return int64(0), nil
	}
	if list, ok := args[0].([]interface{}); ok {
		count := int64(0)
		for _, v := range list {
			if v != nil {
				count++
			}
		}
		return count, nil
	}
	return int64(1), nil
}

func evalSum(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SUM requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	if list, ok := args[0].([]interface{}); ok {
		var sum float64
		for _, v := range list {
			switch val := v.(type) {
			case int64:
				sum += float64(val)
			case float64:
				sum += val
			case nil:
				continue
			default:
				return nil, fmt.Errorf("SUM: unsupported type %T", v)
			}
		}
		return sum, nil
	}
	switch val := args[0].(type) {
	case int64:
		return float64(val), nil
	case float64:
		return val, nil
	default:
		return nil, fmt.Errorf("SUM: unsupported type %T", args[0])
	}
}

func evalAvg(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("AVG requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	if list, ok := args[0].([]interface{}); ok {
		var sum float64
		count := 0
		for _, v := range list {
			if v == nil {
				continue
			}
			switch val := v.(type) {
			case int64:
				sum += float64(val)
				count++
			case float64:
				sum += val
				count++
			default:
				return nil, fmt.Errorf("AVG: unsupported type %T", v)
			}
		}
		if count == 0 {
			return nil, nil
		}
		return sum / float64(count), nil
	}
	switch val := args[0].(type) {
	case int64:
		return float64(val), nil
	case float64:
		return val, nil
	default:
		return nil, fmt.Errorf("AVG: unsupported type %T", args[0])
	}
}

func evalMax(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("MAX requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	if list, ok := args[0].([]interface{}); ok {
		if len(list) == 0 {
			return nil, nil
		}
		var max interface{} = nil
		for _, v := range list {
			if v == nil {
				continue
			}
			if max == nil {
				max = v
				continue
			}
			gt, err := evalGT(v, max)
			if err != nil {
				return nil, err
			}
			if gt.(bool) {
				max = v
			}
		}
		return max, nil
	}
	return args[0], nil
}

func evalMin(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("MIN requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	if list, ok := args[0].([]interface{}); ok {
		if len(list) == 0 {
			return nil, nil
		}
		var min interface{} = nil
		for _, v := range list {
			if v == nil {
				continue
			}
			if min == nil {
				min = v
				continue
			}
			lt, err := evalLT(v, min)
			if err != nil {
				return nil, err
			}
			if lt.(bool) {
				min = v
			}
		}
		return min, nil
	}
	return args[0], nil
}

func aggregateInputValues(args []interface{}) ([]interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("aggregate requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	if values, ok := args[0].([]interface{}); ok {
		return values, nil
	}
	return []interface{}{args[0]}, nil
}

func evalJSONArrayAggregate(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("JSON_ARRAYAGG requires exactly 1 argument")
	}
	values, ok := args[0].([]interface{})
	if !ok {
		values = []interface{}{args[0]}
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}

func evalJSONObjectAggregate(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_OBJECTAGG requires exactly 2 arguments")
	}
	keys, keysAreList := args[0].([]interface{})
	values, valuesAreList := args[1].([]interface{})
	if !keysAreList {
		keys = []interface{}{args[0]}
	}
	if !valuesAreList {
		values = []interface{}{args[1]}
	}
	if len(keys) != len(values) {
		return nil, fmt.Errorf("JSON_OBJECTAGG key/value lengths differ")
	}
	object := make(map[string]interface{}, len(keys))
	for index, key := range keys {
		if key == nil {
			return nil, fmt.Errorf("JSON_OBJECTAGG key cannot be NULL")
		}
		object[fmt.Sprint(key)] = values[index]
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}

func evalBitAggregate(name string, args []interface{}) (interface{}, error) {
	values, err := aggregateInputValues(args)
	if err != nil {
		return nil, err
	}
	var result uint64
	initialized := false
	if name == "BIT_AND" {
		result = ^uint64(0)
	}
	for _, value := range values {
		if value == nil {
			continue
		}
		numeric, ok := numericComparableValue(value)
		if !ok {
			return nil, fmt.Errorf("%s: unsupported type %T", name, value)
		}
		unsigned := uint64(numeric)
		switch name {
		case "BIT_AND":
			result &= unsigned
		case "BIT_OR":
			result |= unsigned
		case "BIT_XOR":
			result ^= unsigned
		}
		initialized = true
	}
	if !initialized && name == "BIT_AND" {
		return uint64(^uint64(0)), nil
	}
	return result, nil
}

func evalStatAggregate(name string, args []interface{}) (interface{}, error) {
	values, err := aggregateInputValues(args)
	if err != nil {
		return nil, err
	}
	numbers := make([]float64, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		numeric, ok := numericComparableValue(value)
		if !ok {
			return nil, fmt.Errorf("%s: unsupported type %T", name, value)
		}
		numbers = append(numbers, numeric)
	}
	if len(numbers) == 0 {
		return nil, nil
	}
	mean := 0.0
	for _, number := range numbers {
		mean += number
	}
	mean /= float64(len(numbers))
	variance := 0.0
	for _, number := range numbers {
		delta := number - mean
		variance += delta * delta
	}
	if strings.HasSuffix(name, "_SAMP") {
		if len(numbers) < 2 {
			return nil, nil
		}
		variance /= float64(len(numbers) - 1)
	} else {
		variance /= float64(len(numbers))
	}
	if strings.HasPrefix(name, "STD") || name == "STD" {
		return math.Sqrt(variance), nil
	}
	return variance, nil
}

func evalConcat(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return "", nil
	}
	var result strings.Builder
	for _, arg := range args {
		if arg == nil {
			return nil, nil // MySQL CONCAT returns NULL if any argument is NULL
		}
		switch v := arg.(type) {
		case string:
			result.WriteString(v)
		case int64:
			result.WriteString(strconv.FormatInt(v, 10))
		case float64:
			result.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
		case bool:
			result.WriteString(strconv.FormatBool(v))
		default:
			return nil, fmt.Errorf("CONCAT: unsupported type %T", arg)
		}
	}
	return result.String(), nil
}

func evalSubstring(args []interface{}) (interface{}, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("SUBSTRING requires 2 or 3 arguments")
	}
	if args[0] == nil {
		return nil, nil
	}
	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("SUBSTRING: first argument must be string, got %T", args[0])
	}
	pos, ok := args[1].(int64)
	if !ok {
		return nil, fmt.Errorf("SUBSTRING: second argument must be integer, got %T", args[1])
	}
	// MySQL中位置从1开始
	if pos < 1 {
		pos = 1
	}
	pos-- // 转换为0-based索引
	if len(args) == 2 {
		if pos >= int64(len(str)) {
			return "", nil
		}
		return str[pos:], nil
	}
	length, ok := args[2].(int64)
	if !ok {
		return nil, fmt.Errorf("SUBSTRING: third argument must be integer, got %T", args[2])
	}
	if length < 0 {
		return "", nil
	}
	end := pos + length
	if end > int64(len(str)) {
		end = int64(len(str))
	}
	if pos >= int64(len(str)) {
		return "", nil
	}
	return str[pos:end], nil
}

// ============ 新增表达式类型 (OPT-017支持) ============
// 注意: NotExpression 已在 cnf_converter.go 中定义，此处不重复定义

// InExpression IN表达式
type InExpression struct {
	BaseExpression
	Column Expression
	Values []interface{}
}

func (i *InExpression) Eval(ctx *EvalContext) (interface{}, error) {
	colVal, err := i.Column.Eval(ctx)
	if err != nil {
		return nil, err
	}

	if colVal == nil {
		return nil, nil
	}
	hasNull := false
	for _, val := range i.Values {
		eq, err := evalEQ(colVal, val)
		if err != nil {
			continue
		}
		if eq == nil {
			hasNull = true
			continue
		}
		if eq.(bool) {
			return true, nil
		}
	}
	if hasNull {
		return nil, nil
	}
	return false, nil
}

func (i *InExpression) String() string {
	return fmt.Sprintf("%s IN (%v)", i.Column.String(), i.Values)
}

// LikeExpression LIKE表达式
type LikeExpression struct {
	BaseExpression
	Column  Expression
	Pattern string
	Escape  Expression
}

func (l *LikeExpression) Eval(ctx *EvalContext) (interface{}, error) {
	colVal, err := l.Column.Eval(ctx)
	if err != nil {
		return nil, err
	}

	if colVal == nil {
		return nil, nil
	}

	str := compatString(colVal)

	var matched interface{}
	if l.Escape != nil {
		escape, escapeErr := l.Escape.Eval(ctx)
		if escapeErr != nil {
			return nil, escapeErr
		}
		matched, err = evalLikeWithEscape(str, l.Pattern, escape)
	} else {
		pattern := sqlLikePatternRegex(l.Pattern)
		matched, err = regexp.MatchString(pattern, str)
	}
	if err != nil {
		return nil, err
	}

	return matched, nil
}

func (l *LikeExpression) String() string {
	result := fmt.Sprintf("%s LIKE '%s'", l.Column.String(), l.Pattern)
	if l.Escape != nil {
		result += fmt.Sprintf(" ESCAPE %s", l.Escape.String())
	}
	return result
}

// IsNullExpression IS NULL表达式
type IsNullExpression struct {
	BaseExpression
	Column Expression
	IsNull bool // true: IS NULL, false: IS NOT NULL
}

func (i *IsNullExpression) Eval(ctx *EvalContext) (interface{}, error) {
	colVal, err := i.Column.Eval(ctx)
	if err != nil {
		return nil, err
	}

	if i.IsNull {
		return colVal == nil, nil
	}
	return colVal != nil, nil
}

func (i *IsNullExpression) String() string {
	if i.IsNull {
		return fmt.Sprintf("%s IS NULL", i.Column.String())
	}
	return fmt.Sprintf("%s IS NOT NULL", i.Column.String())
}

// IsTruthExpression evaluates MySQL's boolean predicates: IS TRUE, IS FALSE,
// IS NOT TRUE and IS NOT FALSE. Unlike ordinary comparisons, these predicates
// never return UNKNOWN; NULL is false for IS TRUE/IS FALSE and true for the
// corresponding IS NOT predicates.
type IsTruthExpression struct {
	BaseExpression
	Expr     Expression
	Operator string
}

func (i *IsTruthExpression) Eval(ctx *EvalContext) (interface{}, error) {
	value, err := i.Expr.Eval(ctx)
	if err != nil {
		return nil, err
	}

	truth := false
	if value != nil {
		if boolean, ok := value.(bool); ok {
			truth = boolean
		} else if numeric, ok := numericComparableValue(value); ok {
			truth = numeric != 0
		} else {
			return nil, fmt.Errorf("%s requires a boolean-compatible operand, got %T", i.Operator, value)
		}
	}

	switch strings.ToLower(strings.TrimSpace(i.Operator)) {
	case "is true":
		return truth, nil
	case "is false":
		return value != nil && !truth, nil
	case "is not true":
		return !truth, nil
	case "is not false":
		return value == nil || truth, nil
	default:
		return nil, fmt.Errorf("unsupported boolean predicate: %s", i.Operator)
	}
}

func (i *IsTruthExpression) String() string {
	return fmt.Sprintf("%s %s", i.Expr.String(), strings.ToUpper(i.Operator))
}

// BetweenExpression BETWEEN表达式
type BetweenExpression struct {
	BaseExpression
	Column    Expression
	Lower     interface{}
	Upper     interface{}
	LowerExpr Expression
	UpperExpr Expression
	Not       bool
}

func (b *BetweenExpression) Eval(ctx *EvalContext) (interface{}, error) {
	colVal, err := b.Column.Eval(ctx)
	if err != nil {
		return nil, err
	}

	if colVal == nil {
		return nil, nil
	}

	lower := b.Lower
	if b.LowerExpr != nil {
		lower, err = b.LowerExpr.Eval(ctx)
		if err != nil {
			return nil, err
		}
	}
	upper := b.Upper
	if b.UpperExpr != nil {
		upper, err = b.UpperExpr.Eval(ctx)
		if err != nil {
			return nil, err
		}
	}

	ge, err := evalGE(colVal, lower)
	if err != nil {
		return nil, err
	}

	le, err := evalLE(colVal, upper)
	if err != nil {
		return nil, err
	}
	if ge == nil || le == nil {
		return nil, nil
	}

	matched := ge.(bool) && le.(bool)
	if b.Not {
		return !matched, nil
	}
	return matched, nil
}

func (b *BetweenExpression) String() string {
	operator := "BETWEEN"
	if b.Not {
		operator = "NOT BETWEEN"
	}
	lower := fmt.Sprint(b.Lower)
	if b.LowerExpr != nil {
		lower = b.LowerExpr.String()
	}
	upper := fmt.Sprint(b.Upper)
	if b.UpperExpr != nil {
		upper = b.UpperExpr.String()
	}
	return fmt.Sprintf("%s %s %s AND %s", b.Column.String(), operator, lower, upper)
}
