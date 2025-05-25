package plan

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
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
	Row map[string]interface{}
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
	if val, ok := ctx.Row[c.Name]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("column %s not found", c.Name)
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
	OpIn
)

// BinaryOperation 二元运算表达式
type BinaryOperation struct {
	BaseExpression
	Op    BinaryOp
	Left  Expression
	Right Expression
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
		return evalLike(left, right)
	case OpIn:
		return evalIn(left, right)
	default:
		return nil, fmt.Errorf("unknown binary operator: %v", b.Op)
	}
}

// Function 函数表达式
type Function struct {
	BaseExpression
	Name string
	Args []Expression
}

func (f *Function) Eval(ctx *EvalContext) (interface{}, error) {
	args := make([]interface{}, len(f.Args))
	for i, arg := range f.Args {
		val, err := arg.Eval(ctx)
		if err != nil {
			return nil, err
		}
		args[i] = val
	}

	switch f.Name {
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
	case "SUBSTRING":
		return evalSubstring(args)
	case "NOW":
		return time.Now(), nil
	default:
		return nil, fmt.Errorf("unknown function: %s", f.Name)
	}
}

// 运算符求值函数
func evalAdd(left, right interface{}) (interface{}, error) {
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
	return nil, fmt.Errorf("unsupported operand types for +: %T and %T", left, right)
}

func evalSub(left, right interface{}) (interface{}, error) {
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
	return nil, fmt.Errorf("unsupported operand types for -: %T and %T", left, right)
}

func evalMul(left, right interface{}) (interface{}, error) {
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
	return nil, fmt.Errorf("unsupported operand types for *: %T and %T", left, right)
}

func evalDiv(left, right interface{}) (interface{}, error) {
	if right == nil {
		return nil, fmt.Errorf("division by nil")
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
	return nil, fmt.Errorf("unsupported operand types for /: %T and %T", left, right)
}

func evalEQ(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return left == right, nil
	}
	switch l := left.(type) {
	case int64:
		if r, ok := right.(int64); ok {
			return l == r, nil
		}
	case float64:
		if r, ok := right.(float64); ok {
			return l == r, nil
		}
	case string:
		if r, ok := right.(string); ok {
			return l == r, nil
		}
	case bool:
		if r, ok := right.(bool); ok {
			return l == r, nil
		}
	}
	return false, nil
}

func evalNE(left, right interface{}) (interface{}, error) {
	eq, err := evalEQ(left, right)
	if err != nil {
		return nil, err
	}
	return !eq.(bool), nil
}

func evalLT(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	switch l := left.(type) {
	case int64:
		if r, ok := right.(int64); ok {
			return l < r, nil
		}
	case float64:
		if r, ok := right.(float64); ok {
			return l < r, nil
		}
	case string:
		if r, ok := right.(string); ok {
			return l < r, nil
		}
	}
	return nil, fmt.Errorf("cannot compare %T with %T", left, right)
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
	return lt.(bool) || eq.(bool), nil
}

func evalGT(left, right interface{}) (interface{}, error) {
	le, err := evalLE(left, right)
	if err != nil {
		return nil, err
	}
	return !le.(bool), nil
}

func evalGE(left, right interface{}) (interface{}, error) {
	lt, err := evalLT(left, right)
	if err != nil {
		return nil, err
	}
	return !lt.(bool), nil
}

func evalAnd(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	l, ok := left.(bool)
	if !ok {
		return nil, fmt.Errorf("AND requires boolean operands, got %T", left)
	}
	r, ok := right.(bool)
	if !ok {
		return nil, fmt.Errorf("AND requires boolean operands, got %T", right)
	}
	return l && r, nil
}

func evalOr(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	l, ok := left.(bool)
	if !ok {
		return nil, fmt.Errorf("OR requires boolean operands, got %T", left)
	}
	r, ok := right.(bool)
	if !ok {
		return nil, fmt.Errorf("OR requires boolean operands, got %T", right)
	}
	return l || r, nil
}

func evalLike(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	l, ok := left.(string)
	if !ok {
		return nil, fmt.Errorf("LIKE requires string operands, got %T", left)
	}
	r, ok := right.(string)
	if !ok {
		return nil, fmt.Errorf("LIKE requires string operands, got %T", right)
	}
	// 将SQL LIKE模式转换为正则表达式
	pattern := strings.ReplaceAll(r, "%", ".*")
	pattern = strings.ReplaceAll(pattern, "_", ".")
	pattern = "^" + pattern + "$"
	matched, err := regexp.MatchString(pattern, l)
	if err != nil {
		return nil, err
	}
	return matched, nil
}

func evalIn(left, right interface{}) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	list, ok := right.([]interface{})
	if !ok {
		return nil, fmt.Errorf("IN requires array as right operand, got %T", right)
	}
	for _, item := range list {
		eq, err := evalEQ(left, item)
		if err != nil {
			return nil, err
		}
		if eq.(bool) {
			return true, nil
		}
	}
	return false, nil
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
