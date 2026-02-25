package plan

import (
	"fmt"
	"strings"
)

// CNFConverter 合取范式转换器
// 负责将WHERE子句中的布尔表达式转换为CNF形式
// CNF形式: (a1 OR a2 OR ...) AND (b1 OR b2 OR ...) AND ...
type CNFConverter struct {
	// maxClauses 最大子句数量限制，防止表达式膨胀
	maxClauses int
	// maxDepth 最大嵌套深度限制
	maxDepth int
}

// NewCNFConverter 创建CNF转换器
func NewCNFConverter() *CNFConverter {
	return &CNFConverter{
		maxClauses: 100, // 默认最大100个子句
		maxDepth:   5,   // 默认最大深度5层
	}
}

// SetMaxClauses 设置最大子句数量
func (c *CNFConverter) SetMaxClauses(max int) {
	c.maxClauses = max
}

// SetMaxDepth 设置最大嵌套深度
func (c *CNFConverter) SetMaxDepth(max int) {
	c.maxDepth = max
}

// ConvertToCNF 将表达式转换为CNF形式
// 主入口函数，完成完整的CNF转换流程
func (c *CNFConverter) ConvertToCNF(expr Expression) Expression {
	if expr == nil {
		return nil
	}

	// 1. 消除双重否定
	expr = c.eliminateDoubleNegation(expr)

	// 2. 内移否定词（应用德摩根定律）
	expr = c.pushDownNegation(expr, false)

	// 3. 应用分配律展开
	expr = c.applyDistributiveLaw(expr)

	// 4. 扁平化同类运算符
	expr = c.flattenExpression(expr)

	// 5. 简化表达式
	expr = c.simplifyClause(expr)

	return expr
}

// isCNF 检查表达式是否已经是CNF形式
// CNF形式定义: 只有两层结构，第一层是AND，第二层是OR或原子谓词
func (c *CNFConverter) isCNF(expr Expression) bool {
	switch e := expr.(type) {
	case *BinaryOperation:
		if e.Op == OpAnd {
			// 检查所有AND的子表达式是否是OR子句或原子谓词
			return c.isDisjunctiveClause(e.Left) && c.isDisjunctiveClause(e.Right)
		} else if e.Op == OpOr {
			// 单个OR子句也是CNF
			return c.isDisjunctiveClause(e)
		}
		// 原子谓词（比较运算符）
		return c.isAtomicPredicate(e)
	case *Column, *Constant:
		return true
	case *NotExpression:
		// CNF中的NOT只能出现在原子谓词前
		return c.isAtomicPredicate(e.Operand)
	default:
		return false
	}
}

// isDisjunctiveClause 检查是否是析取子句（OR连接的原子谓词）
func (c *CNFConverter) isDisjunctiveClause(expr Expression) bool {
	switch e := expr.(type) {
	case *BinaryOperation:
		if e.Op == OpOr {
			return c.isDisjunctiveClause(e.Left) && c.isDisjunctiveClause(e.Right)
		}
		// 原子谓词
		return c.isAtomicPredicate(e)
	case *NotExpression:
		return c.isAtomicPredicate(e.Operand)
	case *Column, *Constant:
		return true
	default:
		return false
	}
}

// isAtomicPredicate 检查是否是原子谓词（不可再分的比较表达式）
func (c *CNFConverter) isAtomicPredicate(expr Expression) bool {
	switch e := expr.(type) {
	case *BinaryOperation:
		// 比较运算符是原子谓词
		return e.Op == OpEQ || e.Op == OpNE || e.Op == OpLT ||
			e.Op == OpLE || e.Op == OpGT || e.Op == OpGE ||
			e.Op == OpLike || e.Op == OpIn
	case *Column, *Constant:
		return true
	default:
		return false
	}
}

// eliminateDoubleNegation 消除双重否定
// NOT (NOT A) -> A
func (c *CNFConverter) eliminateDoubleNegation(expr Expression) Expression {
	switch e := expr.(type) {
	case *NotExpression:
		if inner, ok := e.Operand.(*NotExpression); ok {
			// 双重否定，直接返回内部表达式
			return c.eliminateDoubleNegation(inner.Operand)
		}
		// 递归处理子表达式
		e.Operand = c.eliminateDoubleNegation(e.Operand)
		return e
	case *BinaryOperation:
		e.Left = c.eliminateDoubleNegation(e.Left)
		e.Right = c.eliminateDoubleNegation(e.Right)
		return e
	default:
		return expr
	}
}

// pushDownNegation 内移否定词（应用德摩根定律）
// NOT (A AND B) -> (NOT A) OR (NOT B)
// NOT (A OR B) -> (NOT A) AND (NOT B)
func (c *CNFConverter) pushDownNegation(expr Expression, negate bool) Expression {
	if negate {
		// 需要对表达式取反
		switch e := expr.(type) {
		case *NotExpression:
			// NOT (NOT A) -> A (双重否定)
			return c.pushDownNegation(e.Operand, false)
		case *BinaryOperation:
			if e.Op == OpAnd {
				// NOT (A AND B) -> (NOT A) OR (NOT B)
				left := c.pushDownNegation(e.Left, true)
				right := c.pushDownNegation(e.Right, true)
				return &BinaryOperation{
					Op:    OpOr,
					Left:  left,
					Right: right,
				}
			} else if e.Op == OpOr {
				// NOT (A OR B) -> (NOT A) AND (NOT B)
				left := c.pushDownNegation(e.Left, true)
				right := c.pushDownNegation(e.Right, true)
				return &BinaryOperation{
					Op:    OpAnd,
					Left:  left,
					Right: right,
				}
			} else if c.isAtomicPredicate(e) {
				// 原子谓词（比较运算符），应用运算符否定
				return c.negateOperator(e)
			} else {
				// 算术运算符，无法直接取反
				return &NotExpression{Operand: expr}
			}
		default:
			// 其他类型，添加NOT节点
			return &NotExpression{Operand: expr}
		}
	} else {
		// 不需要取反，递归处理
		switch e := expr.(type) {
		case *NotExpression:
			// 遇到NOT，切换取反标志
			return c.pushDownNegation(e.Operand, true)
		case *BinaryOperation:
			if e.Op == OpAnd || e.Op == OpOr {
				e.Left = c.pushDownNegation(e.Left, false)
				e.Right = c.pushDownNegation(e.Right, false)
			}
			return e
		default:
			return expr
		}
	}
}

// negateOperator 对运算符取反
// NOT (a > b) -> a <= b
// NOT (a >= b) -> a < b
// NOT (a = b) -> a != b
// NOT (a != b) -> a = b
func (c *CNFConverter) negateOperator(expr *BinaryOperation) Expression {
	newOp := expr.Op
	switch expr.Op {
	case OpEQ:
		newOp = OpNE
	case OpNE:
		newOp = OpEQ
	case OpLT:
		newOp = OpGE
	case OpLE:
		newOp = OpGT
	case OpGT:
		newOp = OpLE
	case OpGE:
		newOp = OpLT
	default:
		// 不支持的运算符，保留NOT
		return &NotExpression{Operand: expr}
	}

	return &BinaryOperation{
		Op:    newOp,
		Left:  expr.Left,
		Right: expr.Right,
	}
}

// applyDistributiveLaw 应用分配律展开
// A OR (B AND C) -> (A OR B) AND (A OR C)
// (A AND B) OR C -> (A OR C) AND (B OR C)
// (A AND B) OR (C AND D) -> (A OR C) AND (A OR D) AND (B OR C) AND (B OR D)
func (c *CNFConverter) applyDistributiveLaw(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryOperation:
		// 先递归处理子表达式
		e.Left = c.applyDistributiveLaw(e.Left)
		e.Right = c.applyDistributiveLaw(e.Right)

		if e.Op == OpOr {
			// 检查是否需要应用分配律
			leftIsAnd := c.isAndOperation(e.Left)
			rightIsAnd := c.isAndOperation(e.Right)

			if leftIsAnd && rightIsAnd {
				// (A AND B) OR (C AND D)
				return c.distributeOrOverAnd(e.Left.(*BinaryOperation), e.Right.(*BinaryOperation))
			} else if leftIsAnd {
				// (A AND B) OR C
				return c.distributeOrOverLeftAnd(e.Left.(*BinaryOperation), e.Right)
			} else if rightIsAnd {
				// A OR (B AND C)
				return c.distributeOrOverRightAnd(e.Left, e.Right.(*BinaryOperation))
			}
		}
		return e
	default:
		return expr
	}
}

// isAndOperation 检查表达式是否是AND运算
func (c *CNFConverter) isAndOperation(expr Expression) bool {
	if bin, ok := expr.(*BinaryOperation); ok {
		return bin.Op == OpAnd
	}
	return false
}

// distributeOrOverRightAnd 分配OR到右侧AND
// A OR (B AND C) -> (A OR B) AND (A OR C)
func (c *CNFConverter) distributeOrOverRightAnd(left Expression, right *BinaryOperation) Expression {
	// 克隆left以避免共享引用
	leftClone := c.cloneExpression(left)

	leftOr := &BinaryOperation{
		Op:    OpOr,
		Left:  left,
		Right: right.Left,
	}
	rightOr := &BinaryOperation{
		Op:    OpOr,
		Left:  leftClone,
		Right: right.Right,
	}
	return &BinaryOperation{
		Op:    OpAnd,
		Left:  leftOr,
		Right: rightOr,
	}
}

// distributeOrOverLeftAnd 分配OR到左侧AND
// (A AND B) OR C -> (A OR C) AND (B OR C)
func (c *CNFConverter) distributeOrOverLeftAnd(left *BinaryOperation, right Expression) Expression {
	// 克隆right以避免共享引用
	rightClone := c.cloneExpression(right)

	leftOr := &BinaryOperation{
		Op:    OpOr,
		Left:  left.Left,
		Right: right,
	}
	rightOr := &BinaryOperation{
		Op:    OpOr,
		Left:  left.Right,
		Right: rightClone,
	}
	return &BinaryOperation{
		Op:    OpAnd,
		Left:  leftOr,
		Right: rightOr,
	}
}

// distributeOrOverAnd 分配OR到两侧AND
// (A AND B) OR (C AND D) -> (A OR C) AND (A OR D) AND (B OR C) AND (B OR D)
func (c *CNFConverter) distributeOrOverAnd(left *BinaryOperation, right *BinaryOperation) Expression {
	// 提取左侧AND的项
	leftItems := c.extractAndItems(left)
	// 提取右侧AND的项
	rightItems := c.extractAndItems(right)

	// 检查是否会导致子句膨胀
	totalClauses := len(leftItems) * len(rightItems)
	if totalClauses > c.maxClauses {
		// 超过限制，不展开
		return &BinaryOperation{
			Op:    OpOr,
			Left:  left,
			Right: right,
		}
	}

	// 构建所有组合的OR子句
	var clauses []Expression
	for _, l := range leftItems {
		for _, r := range rightItems {
			clauses = append(clauses, &BinaryOperation{
				Op:    OpOr,
				Left:  c.cloneExpression(l),
				Right: c.cloneExpression(r),
			})
		}
	}

	// 用AND连接所有子句
	result := clauses[0]
	for i := 1; i < len(clauses); i++ {
		result = &BinaryOperation{
			Op:    OpAnd,
			Left:  result,
			Right: clauses[i],
		}
	}
	return result
}

// extractAndItems 提取AND连接的项
func (c *CNFConverter) extractAndItems(expr Expression) []Expression {
	switch e := expr.(type) {
	case *BinaryOperation:
		if e.Op == OpAnd {
			left := c.extractAndItems(e.Left)
			right := c.extractAndItems(e.Right)
			return append(left, right...)
		}
	}
	return []Expression{expr}
}

// flattenExpression 扁平化表达式
// A AND (B AND (C AND D)) -> A AND B AND C AND D
func (c *CNFConverter) flattenExpression(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryOperation:
		if e.Op == OpAnd {
			items := c.extractAndItems(e)
			if len(items) <= 1 {
				return expr
			}
			result := items[0]
			for i := 1; i < len(items); i++ {
				result = &BinaryOperation{
					Op:    OpAnd,
					Left:  result,
					Right: c.flattenExpression(items[i]),
				}
			}
			return result
		} else if e.Op == OpOr {
			items := c.extractOrItems(e)
			if len(items) <= 1 {
				return expr
			}
			result := items[0]
			for i := 1; i < len(items); i++ {
				result = &BinaryOperation{
					Op:    OpOr,
					Left:  result,
					Right: c.flattenExpression(items[i]),
				}
			}
			return result
		}
	}
	return expr
}

// extractOrItems 提取OR连接的项
func (c *CNFConverter) extractOrItems(expr Expression) []Expression {
	switch e := expr.(type) {
	case *BinaryOperation:
		if e.Op == OpOr {
			left := c.extractOrItems(e.Left)
			right := c.extractOrItems(e.Right)
			return append(left, right...)
		}
	}
	return []Expression{expr}
}

// simplifyClause 简化子句
// 移除重复项、检测矛盾等
func (c *CNFConverter) simplifyClause(expr Expression) Expression {
	if expr == nil {
		return nil
	}

	// 1. 常量折叠
	expr = c.constantFolding(expr)

	// 2. 移除重复项
	expr = c.removeDuplicates(expr)

	// 3. 检测矛盾条件
	expr = c.detectContradictions(expr)

	// 4. 简化布尔常量
	expr = c.simplifyBooleanConstants(expr)

	return expr
}

// constantFolding 常量折叠 - 编译期计算常量表达式
func (c *CNFConverter) constantFolding(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryOperation:
		// 先递归处理子表达式
		e.Left = c.constantFolding(e.Left)
		e.Right = c.constantFolding(e.Right)

		// 检查是否都是常量
		leftConst, leftIsConst := e.Left.(*Constant)
		rightConst, rightIsConst := e.Right.(*Constant)

		if leftIsConst && rightIsConst {
			// 两个操作数都是常量，可以折叠
			ctx := &EvalContext{Row: make(map[string]interface{})}
			result, err := e.Eval(ctx)
			if err == nil {
				return &Constant{Value: result}
			}
		}

		// 应用代数简化规则
		if leftIsConst {
			// 左侧是常量
			switch e.Op {
			case OpAdd:
				if leftConst.Value == int64(0) || leftConst.Value == float64(0) {
					// 0 + x = x
					return e.Right
				}
			case OpMul:
				if leftConst.Value == int64(1) || leftConst.Value == float64(1) {
					// 1 * x = x
					return e.Right
				}
				if leftConst.Value == int64(0) || leftConst.Value == float64(0) {
					// 0 * x = 0
					return leftConst
				}
			case OpOr:
				if leftConst.Value == true {
					// TRUE OR x = TRUE
					return leftConst
				}
				if leftConst.Value == false {
					// FALSE OR x = x
					return e.Right
				}
			case OpAnd:
				if leftConst.Value == false {
					// FALSE AND x = FALSE
					return leftConst
				}
				if leftConst.Value == true {
					// TRUE AND x = x
					return e.Right
				}
			}
		}

		if rightIsConst {
			// 右侧是常量
			switch e.Op {
			case OpAdd:
				if rightConst.Value == int64(0) || rightConst.Value == float64(0) {
					// x + 0 = x
					return e.Left
				}
			case OpSub:
				if rightConst.Value == int64(0) || rightConst.Value == float64(0) {
					// x - 0 = x
					return e.Left
				}
			case OpMul:
				if rightConst.Value == int64(1) || rightConst.Value == float64(1) {
					// x * 1 = x
					return e.Left
				}
				if rightConst.Value == int64(0) || rightConst.Value == float64(0) {
					// x * 0 = 0
					return rightConst
				}
			case OpOr:
				if rightConst.Value == true {
					// x OR TRUE = TRUE
					return rightConst
				}
				if rightConst.Value == false {
					// x OR FALSE = x
					return e.Left
				}
			case OpAnd:
				if rightConst.Value == false {
					// x AND FALSE = FALSE
					return rightConst
				}
				if rightConst.Value == true {
					// x AND TRUE = x
					return e.Left
				}
			}
		}

		return e

	case *NotExpression:
		e.Operand = c.constantFolding(e.Operand)
		if constOp, ok := e.Operand.(*Constant); ok {
			if b, ok := constOp.Value.(bool); ok {
				return &Constant{Value: !b}
			}
		}
		return e

	case *Function:
		// 对纯函数进行常量折叠
		allConst := true
		for i, arg := range e.FuncArgs {
			e.FuncArgs[i] = c.constantFolding(arg)
			if _, ok := e.FuncArgs[i].(*Constant); !ok {
				allConst = false
			}
		}

		// 如果所有参数都是常量且是纯函数，可以折叠
		if allConst && c.isPureFunction(e.FuncName) {
			ctx := &EvalContext{Row: make(map[string]interface{})}
			result, err := e.Eval(ctx)
			if err == nil {
				return &Constant{Value: result}
			}
		}
		return e

	default:
		return expr
	}
}

// isPureFunction 判断函数是否为纯函数（无副作用，相同输入相同输出）
func (c *CNFConverter) isPureFunction(funcName string) bool {
	pureFunctions := map[string]bool{
		"UPPER":     true,
		"LOWER":     true,
		"CONCAT":    true,
		"SUBSTRING": true,
		"LENGTH":    true,
		"ABS":       true,
		"ROUND":     true,
	}
	return pureFunctions[strings.ToUpper(funcName)]
}

// removeDuplicates 移除重复的条件
func (c *CNFConverter) removeDuplicates(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryOperation:
		if e.Op == OpAnd {
			// 提取所有AND项
			items := c.extractAndItems(e)
			// 去重
			unique := c.uniqueExpressions(items)
			if len(unique) == 1 {
				return unique[0]
			}
			// 重建AND表达式
			result := unique[0]
			for i := 1; i < len(unique); i++ {
				result = &BinaryOperation{
					Op:    OpAnd,
					Left:  result,
					Right: unique[i],
				}
			}
			return result
		} else if e.Op == OpOr {
			// 提取所有OR项
			items := c.extractOrItems(e)
			// 去重
			unique := c.uniqueExpressions(items)
			if len(unique) == 1 {
				return unique[0]
			}
			// 重建OR表达式
			result := unique[0]
			for i := 1; i < len(unique); i++ {
				result = &BinaryOperation{
					Op:    OpOr,
					Left:  result,
					Right: unique[i],
				}
			}
			return result
		}
		return e
	default:
		return expr
	}
}

// uniqueExpressions 对表达式列表去重
func (c *CNFConverter) uniqueExpressions(exprs []Expression) []Expression {
	seen := make(map[string]bool)
	var result []Expression

	for _, expr := range exprs {
		key := expr.String()
		if !seen[key] {
			seen[key] = true
			result = append(result, expr)
		}
	}

	return result
}

// detectContradictions 检测矛盾条件
func (c *CNFConverter) detectContradictions(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryOperation:
		if e.Op == OpAnd {
			// 检查是否存在矛盾的条件
			items := c.extractAndItems(e)
			for i := 0; i < len(items); i++ {
				for j := i + 1; j < len(items); j++ {
					if c.areContradictory(items[i], items[j]) {
						// 发现矛盾，整个AND条件为FALSE
						return &Constant{Value: false}
					}
				}
			}
		} else if e.Op == OpOr {
			// 检查是否所有OR项都恒真
			items := c.extractOrItems(e)
			for _, item := range items {
				if c.isTautology(item) {
					// 存在恒真项，整个OR为TRUE
					return &Constant{Value: true}
				}
			}
		}
		return e
	default:
		return expr
	}
}

// areContradictory 检查两个表达式是否矛盾
func (c *CNFConverter) areContradictory(expr1, expr2 Expression) bool {
	bin1, ok1 := expr1.(*BinaryOperation)
	bin2, ok2 := expr2.(*BinaryOperation)

	if !ok1 || !ok2 {
		return false
	}

	// 检查是否是同一个列的不同条件
	col1, isCol1 := bin1.Left.(*Column)
	col2, isCol2 := bin2.Left.(*Column)

	if !isCol1 || !isCol2 || col1.Name != col2.Name {
		return false
	}

	// 检查常量值
	const1, isConst1 := bin1.Right.(*Constant)
	const2, isConst2 := bin2.Right.(*Constant)

	if !isConst1 || !isConst2 {
		return false
	}

	// 检查矛盾情况
	// 例如: age > 20 AND age < 10
	if bin1.Op == OpGT && bin2.Op == OpLT {
		if c.compareConstants(const1.Value, const2.Value) >= 0 {
			return true
		}
	}
	if bin1.Op == OpLT && bin2.Op == OpGT {
		if c.compareConstants(const1.Value, const2.Value) <= 0 {
			return true
		}
	}
	// 例如: age = 10 AND age = 20
	if bin1.Op == OpEQ && bin2.Op == OpEQ {
		if c.compareConstants(const1.Value, const2.Value) != 0 {
			return true
		}
	}

	return false
}

// isTautology 检查表达式是否恒真
func (c *CNFConverter) isTautology(expr Expression) bool {
	if constExpr, ok := expr.(*Constant); ok {
		if b, ok := constExpr.Value.(bool); ok && b {
			return true
		}
	}

	// 检查形如 (a > 0 OR a <= 0) 的恒真条件
	if binExpr, ok := expr.(*BinaryOperation); ok && binExpr.Op == OpOr {
		left := binExpr.Left
		right := binExpr.Right

		// 检查是否为互补条件
		if c.areComplementary(left, right) {
			return true
		}
	}

	return false
}

// areComplementary 检查两个条件是否互补（覆盖所有情况）
func (c *CNFConverter) areComplementary(expr1, expr2 Expression) bool {
	bin1, ok1 := expr1.(*BinaryOperation)
	bin2, ok2 := expr2.(*BinaryOperation)

	if !ok1 || !ok2 {
		return false
	}

	// 检查是否是同一个列
	col1, isCol1 := bin1.Left.(*Column)
	col2, isCol2 := bin2.Left.(*Column)

	if !isCol1 || !isCol2 || col1.Name != col2.Name {
		return false
	}

	// 检查常量值
	const1, isConst1 := bin1.Right.(*Constant)
	const2, isConst2 := bin2.Right.(*Constant)

	if !isConst1 || !isConst2 {
		return false
	}

	if c.compareConstants(const1.Value, const2.Value) != 0 {
		return false
	}

	// 检查互补关系
	// a > v OR a <= v
	if (bin1.Op == OpGT && bin2.Op == OpLE) || (bin1.Op == OpLE && bin2.Op == OpGT) {
		return true
	}
	// a >= v OR a < v
	if (bin1.Op == OpGE && bin2.Op == OpLT) || (bin1.Op == OpLT && bin2.Op == OpGE) {
		return true
	}

	return false
}

// compareConstants 比较两个常量
func (c *CNFConverter) compareConstants(v1, v2 interface{}) int {
	// 尝试转换为数值比较
	i1, ok1 := toInt64CNF(v1)
	i2, ok2 := toInt64CNF(v2)
	if ok1 && ok2 {
		if i1 < i2 {
			return -1
		} else if i1 > i2 {
			return 1
		}
		return 0
	}

	// 尝试字符串比较
	if s1, ok := v1.(string); ok {
		if s2, ok := v2.(string); ok {
			if s1 < s2 {
				return -1
			} else if s1 > s2 {
				return 1
			}
			return 0
		}
	}

	return 0
}

// toInt64CNF 尝试转换为int64
func toInt64CNF(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case uint:
		return int64(val), true
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		return int64(val), true
	default:
		return 0, false
	}
}

// simplifyBooleanConstants 简化布尔常量
func (c *CNFConverter) simplifyBooleanConstants(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryOperation:
		// 递归处理
		e.Left = c.simplifyBooleanConstants(e.Left)
		e.Right = c.simplifyBooleanConstants(e.Right)

		// 检查子表达式是否为常量
		leftConst, leftIsConst := e.Left.(*Constant)
		rightConst, rightIsConst := e.Right.(*Constant)

		if e.Op == OpAnd {
			if leftIsConst {
				if leftConst.Value == false {
					return leftConst // FALSE AND x = FALSE
				}
				if leftConst.Value == true {
					return e.Right // TRUE AND x = x
				}
			}
			if rightIsConst {
				if rightConst.Value == false {
					return rightConst // x AND FALSE = FALSE
				}
				if rightConst.Value == true {
					return e.Left // x AND TRUE = x
				}
			}
		} else if e.Op == OpOr {
			if leftIsConst {
				if leftConst.Value == true {
					return leftConst // TRUE OR x = TRUE
				}
				if leftConst.Value == false {
					return e.Right // FALSE OR x = x
				}
			}
			if rightIsConst {
				if rightConst.Value == true {
					return rightConst // x OR TRUE = TRUE
				}
				if rightConst.Value == false {
					return e.Left // x OR FALSE = x
				}
			}
		}

		return e

	default:
		return expr
	}
}

// cloneExpression 克隆表达式（深拷贝）
func (c *CNFConverter) cloneExpression(expr Expression) Expression {
	switch e := expr.(type) {
	case *Column:
		return &Column{
			BaseExpression: e.BaseExpression,
			Name:           e.Name,
		}
	case *Constant:
		return &Constant{
			BaseExpression: e.BaseExpression,
			Value:          e.Value,
		}
	case *BinaryOperation:
		return &BinaryOperation{
			BaseExpression: e.BaseExpression,
			Op:             e.Op,
			Left:           c.cloneExpression(e.Left),
			Right:          c.cloneExpression(e.Right),
		}
	case *NotExpression:
		return &NotExpression{
			BaseExpression: e.BaseExpression,
			Operand:        c.cloneExpression(e.Operand),
		}
	case *Function:
		args := make([]Expression, len(e.FuncArgs))
		for i, arg := range e.FuncArgs {
			args[i] = c.cloneExpression(arg)
		}
		return &Function{
			BaseExpression: e.BaseExpression,
			FuncName:       e.FuncName,
			FuncArgs:       args,
		}
	default:
		return expr
	}
}

// ExtractConjuncts 提取CNF中的合取子句（AND连接的项）
// 用于谓词下推时分解条件
func (c *CNFConverter) ExtractConjuncts(cnf Expression) []Expression {
	return c.extractAndItems(cnf)
}

// ExtractDisjuncts 提取析取项（OR连接的项）
func (c *CNFConverter) ExtractDisjuncts(expr Expression) []Expression {
	return c.extractOrItems(expr)
}

// NotExpression NOT表达式
type NotExpression struct {
	BaseExpression
	Operand Expression
}

func (n *NotExpression) Eval(ctx *EvalContext) (interface{}, error) {
	val, err := n.Operand.Eval(ctx)
	if err != nil {
		return nil, err
	}
	if b, ok := val.(bool); ok {
		return !b, nil
	}
	return nil, fmt.Errorf("NOT requires boolean operand, got %T", val)
}

func (n *NotExpression) String() string {
	return fmt.Sprintf("NOT (%s)", n.Operand.String())
}
