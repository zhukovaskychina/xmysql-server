package plan

import (
	"strings"
)

// ExpressionNormalizer 表达式规范化器
// 负责对表达式进行标准化处理，包括常量折叠、代数简化、谓词标准化、冗余消除等
type ExpressionNormalizer struct {
	cnfConverter *CNFConverter
}

// NewExpressionNormalizer 创建表达式规范化器
func NewExpressionNormalizer() *ExpressionNormalizer {
	return &ExpressionNormalizer{
		cnfConverter: NewCNFConverter(),
	}
}

// Normalize 规范化表达式
func (n *ExpressionNormalizer) Normalize(expr Expression) Expression {
	if expr == nil {
		return nil
	}

	// 1. 常量折叠
	expr = n.constantFolding(expr)

	// 2. 代数简化
	expr = n.algebraicSimplification(expr)

	// 3. 谓词标准化
	expr = n.predicateNormalization(expr)

	// 4. 冗余消除
	expr = n.eliminateRedundancy(expr)

	return expr
}

// constantFolding 常量折叠 - 编译期计算常量表达式
// 复用CNF转换器的常量折叠逻辑
func (n *ExpressionNormalizer) constantFolding(expr Expression) Expression {
	return n.cnfConverter.constantFolding(expr)
}

// algebraicSimplification 代数简化
func (n *ExpressionNormalizer) algebraicSimplification(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryOperation:
		// 先递归处理子表达式
		e.Left = n.algebraicSimplification(e.Left)
		e.Right = n.algebraicSimplification(e.Right)

		// 应用交换律：确保常量在右侧
		e = n.applyCommutativeLaw(e)

		// 应用结合律：扁平化嵌套运算
		e = n.applyAssociativeLaw(e)

		// 应用恒等元规则
		if simplified := n.applyIdentityLaw(e); simplified != nil {
			return simplified
		}

		// 应用零元规则
		if simplified := n.applyZeroLaw(e); simplified != nil {
			return simplified
		}

		// 应用吸收律
		if simplified := n.applyAbsorptionLaw(e); simplified != nil {
			return simplified
		}

		// 应用幂等律
		if simplified := n.applyIdempotentLaw(e); simplified != nil {
			return simplified
		}

		return e

	case *NotExpression:
		e.Operand = n.algebraicSimplification(e.Operand)
		return e

	case *Function:
		for i, arg := range e.FuncArgs {
			e.FuncArgs[i] = n.algebraicSimplification(arg)
		}
		return e

	default:
		return expr
	}
}

// applyCommutativeLaw 应用交换律：确保常量在右侧
func (n *ExpressionNormalizer) applyCommutativeLaw(e *BinaryOperation) *BinaryOperation {
	// 对于可交换的运算符
	if !n.isCommutativeOp(e.Op) {
		return e
	}

	// 如果左侧是常量，右侧不是，则交换
	_, leftIsConst := e.Left.(*Constant)
	_, rightIsConst := e.Right.(*Constant)

	if leftIsConst && !rightIsConst {
		e.Left, e.Right = e.Right, e.Left
	}

	return e
}

// isCommutativeOp 检查操作符是否满足交换律
func (n *ExpressionNormalizer) isCommutativeOp(op BinaryOp) bool {
	switch op {
	case OpAdd, OpMul, OpEQ, OpNE, OpAnd, OpOr:
		return true
	default:
		return false
	}
}

// applyAssociativeLaw 应用结合律：扁平化嵌套运算
func (n *ExpressionNormalizer) applyAssociativeLaw(e *BinaryOperation) *BinaryOperation {
	// 对于结合运算符，扁平化嵌套
	if !n.isAssociativeOp(e.Op) {
		return e
	}

	// 如果子节点也是相同的运算符，可以扁平化
	// 例如：(a + b) + c 可以理解为 a + b + c
	// 这里简化实现，暂不改变结构，仅返回

	return e
}

// isAssociativeOp 检查操作符是否满足结合律
func (n *ExpressionNormalizer) isAssociativeOp(op BinaryOp) bool {
	switch op {
	case OpAdd, OpMul, OpAnd, OpOr:
		return true
	default:
		return false
	}
}

// applyIdentityLaw 应用恒等元规则
func (n *ExpressionNormalizer) applyIdentityLaw(e *BinaryOperation) Expression {
	rightConst, rightIsConst := e.Right.(*Constant)
	if !rightIsConst {
		return nil
	}

	switch e.Op {
	case OpAdd, OpSub:
		// x + 0 = x, x - 0 = x
		if rightConst.Value == int64(0) || rightConst.Value == float64(0) {
			return e.Left
		}
	case OpMul, OpDiv:
		// x * 1 = x, x / 1 = x
		if rightConst.Value == int64(1) || rightConst.Value == float64(1) {
			return e.Left
		}
	case OpAnd:
		// x AND TRUE = x
		if rightConst.Value == true {
			return e.Left
		}
	case OpOr:
		// x OR FALSE = x
		if rightConst.Value == false {
			return e.Left
		}
	}

	return nil
}

// applyZeroLaw 应用零元规则
func (n *ExpressionNormalizer) applyZeroLaw(e *BinaryOperation) Expression {
	rightConst, rightIsConst := e.Right.(*Constant)
	if !rightIsConst {
		return nil
	}

	switch e.Op {
	case OpMul:
		// x * 0 = 0
		if rightConst.Value == int64(0) || rightConst.Value == float64(0) {
			return rightConst
		}
	case OpAnd:
		// x AND FALSE = FALSE
		if rightConst.Value == false {
			return rightConst
		}
	case OpOr:
		// x OR TRUE = TRUE
		if rightConst.Value == true {
			return rightConst
		}
	}

	return nil
}

// applyAbsorptionLaw 应用吸收律
// x AND (x OR y) = x
// x OR (x AND y) = x
func (n *ExpressionNormalizer) applyAbsorptionLaw(e *BinaryOperation) Expression {
	// 简化实现：暂不处理复杂的吸收律
	return nil
}

// applyIdempotentLaw 应用幂等律
// x AND x = x, x OR x = x
func (n *ExpressionNormalizer) applyIdempotentLaw(e *BinaryOperation) Expression {
	if e.Op != OpAnd && e.Op != OpOr {
		return nil
	}

	// 检查左右是否相同
	if e.Left.String() == e.Right.String() {
		return e.Left
	}

	return nil
}

// predicateNormalization 谓词标准化
// 统一谓词的形式：列在左侧，常量在右侧
func (n *ExpressionNormalizer) predicateNormalization(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryOperation:
		// 先递归处理子表达式
		e.Left = n.predicateNormalization(e.Left)
		e.Right = n.predicateNormalization(e.Right)

		// 对比较运算符进行标准化
		if n.isComparisonOp(e.Op) {
			e = n.normalizeComparison(e)
		}

		// 展开BETWEEN为标准比较
		// age BETWEEN 18 AND 60 -> age >= 18 AND age <= 60
		// 这里暂不实现，因为需要识别BETWEEN函数

		// 展开IN为标准比较
		if e.Op == OpIn {
			return n.expandInCondition(e)
		}

		return e

	case *NotExpression:
		e.Operand = n.predicateNormalization(e.Operand)
		return e

	case *Function:
		// 处理特殊函数
		funcName := strings.ToUpper(e.FuncName)
		if funcName == "NOT IN" {
			// NOT IN 转换为多个 != 条件
			return n.expandNotInCondition(e)
		}
		for i, arg := range e.FuncArgs {
			e.FuncArgs[i] = n.predicateNormalization(arg)
		}
		return e

	default:
		return expr
	}
}

// normalizeComparison 标准化比较表达式
// 确保列在左侧，常量在右侧
func (n *ExpressionNormalizer) normalizeComparison(e *BinaryOperation) *BinaryOperation {
	_, rightIsCol := e.Right.(*Column)
	_, leftIsConst := e.Left.(*Constant)

	// 如果左侧是常量，右侧是列，则交换并反转操作符
	if leftIsConst && rightIsCol {
		e.Left, e.Right = e.Right, e.Left
		e.Op = n.reverseComparisonOp(e.Op)
	}

	// 如果左右都是列或都是常量，保持原样

	return e
}

// isComparisonOp 检查是否为比较操作符
func (n *ExpressionNormalizer) isComparisonOp(op BinaryOp) bool {
	switch op {
	case OpEQ, OpNE, OpLT, OpLE, OpGT, OpGE:
		return true
	default:
		return false
	}
}

// reverseComparisonOp 反转比较操作符
// 5 > age -> age < 5
func (n *ExpressionNormalizer) reverseComparisonOp(op BinaryOp) BinaryOp {
	switch op {
	case OpLT:
		return OpGT
	case OpLE:
		return OpGE
	case OpGT:
		return OpLT
	case OpGE:
		return OpLE
	case OpEQ, OpNE:
		return op // 等于和不等于不需要反转
	default:
		return op
	}
}

// expandInCondition 展开IN条件
// age IN (1, 2, 3) -> age = 1 OR age = 2 OR age = 3
func (n *ExpressionNormalizer) expandInCondition(e *BinaryOperation) Expression {
	// 简化实现：保持IN形式，不展开
	// 因为IN在某些场景下性能更好
	return e
}

// expandNotInCondition 展开NOT IN条件
// age NOT IN (1, 2, 3) -> age != 1 AND age != 2 AND age != 3
func (n *ExpressionNormalizer) expandNotInCondition(e *Function) Expression {
	// 简化实现：暂不处理
	return e
}

// eliminateRedundancy 冗余消除
func (n *ExpressionNormalizer) eliminateRedundancy(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryOperation:
		// 先递归处理子表达式
		e.Left = n.eliminateRedundancy(e.Left)
		e.Right = n.eliminateRedundancy(e.Right)

		if e.Op == OpAnd {
			// 消除AND中的冗余
			return n.eliminateRedundantAnd(e)
		} else if e.Op == OpOr {
			// 消除OR中的冗余
			return n.eliminateRedundantOr(e)
		}

		return e

	case *NotExpression:
		e.Operand = n.eliminateRedundancy(e.Operand)
		return e

	case *Function:
		for i, arg := range e.FuncArgs {
			e.FuncArgs[i] = n.eliminateRedundancy(arg)
		}
		return e

	default:
		return expr
	}
}

// eliminateRedundantAnd 消除AND中的冗余条件
func (n *ExpressionNormalizer) eliminateRedundantAnd(e *BinaryOperation) Expression {
	// 提取所有AND项
	items := n.cnfConverter.extractAndItems(e)

	// 1. 去重
	items = n.cnfConverter.uniqueExpressions(items)

	// 2. 检测包含关系
	// 例如：age > 20 AND age > 18，保留 age > 20
	items = n.removeSubsumedConditions(items)

	// 3. 检测矛盾
	// 例如：age > 20 AND age < 10，返回FALSE
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			if n.cnfConverter.areContradictory(items[i], items[j]) {
				return &Constant{Value: false}
			}
		}
	}

	// 重建AND表达式
	if len(items) == 0 {
		return &Constant{Value: true}
	}
	if len(items) == 1 {
		return items[0]
	}

	result := items[0]
	for i := 1; i < len(items); i++ {
		result = &BinaryOperation{
			Op:    OpAnd,
			Left:  result,
			Right: items[i],
		}
	}

	return result
}

// eliminateRedundantOr 消除OR中的冗余条件
func (n *ExpressionNormalizer) eliminateRedundantOr(e *BinaryOperation) Expression {
	// 提取所有OR项
	items := n.cnfConverter.extractOrItems(e)

	// 1. 去重
	items = n.cnfConverter.uniqueExpressions(items)

	// 2. 检测恒真条件
	// 例如：age > 0 OR age <= 0，返回TRUE
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			if n.cnfConverter.areComplementary(items[i], items[j]) {
				return &Constant{Value: true}
			}
		}
	}

	// 重建OR表达式
	if len(items) == 0 {
		return &Constant{Value: false}
	}
	if len(items) == 1 {
		return items[0]
	}

	result := items[0]
	for i := 1; i < len(items); i++ {
		result = &BinaryOperation{
			Op:    OpOr,
			Left:  result,
			Right: items[i],
		}
	}

	return result
}

// removeSubsumedConditions 移除被包含的条件
// 例如：age > 20 AND age > 18，保留 age > 20
func (n *ExpressionNormalizer) removeSubsumedConditions(items []Expression) []Expression {
	var result []Expression

	for i, item := range items {
		subsumed := false
		for j, other := range items {
			if i == j {
				continue
			}
			if n.isSubsumed(item, other) {
				subsumed = true
				break
			}
		}
		if !subsumed {
			result = append(result, item)
		}
	}

	return result
}

// isSubsumed 检查cond1是否被cond2包含（即cond2更严格）
// 例如：age > 18 被 age > 20 包含
func (n *ExpressionNormalizer) isSubsumed(cond1, cond2 Expression) bool {
	bin1, ok1 := cond1.(*BinaryOperation)
	bin2, ok2 := cond2.(*BinaryOperation)

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

	cmp := n.cnfConverter.compareConstants(const1.Value, const2.Value)

	// 检查包含关系
	// age > 18 被 age > 20 包含 (20 > 18)
	if bin1.Op == OpGT && bin2.Op == OpGT {
		return cmp < 0 // const1 < const2
	}
	// age >= 18 被 age >= 20 包含
	if bin1.Op == OpGE && bin2.Op == OpGE {
		return cmp < 0
	}
	// age < 30 被 age < 20 包含 (30 > 20)
	if bin1.Op == OpLT && bin2.Op == OpLT {
		return cmp > 0 // const1 > const2
	}
	// age <= 30 被 age <= 20 包含
	if bin1.Op == OpLE && bin2.Op == OpLE {
		return cmp > 0
	}

	return false
}
