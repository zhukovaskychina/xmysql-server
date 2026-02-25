package plan

import (
	"testing"
)

// TestCNFIntegrationWithPredicatePushdown 测试CNF转换器与谓词下推的集成
func TestCNFIntegrationWithPredicatePushdown(t *testing.T) {
	// 测试1: 简单的NOT表达式应该被转换
	t.Run("Simple NOT conversion", func(t *testing.T) {
		converter := NewCNFConverter()

		// NOT (age > 18)
		age := &Column{Name: "age"}
		ageGt18 := &BinaryOperation{
			Op:    OpGT,
			Left:  age,
			Right: &Constant{Value: int64(18)},
		}
		notExpr := &NotExpression{Operand: ageGt18}

		result := converter.ConvertToCNF(notExpr)

		// 应该转换为 age <= 18
		if binOp, ok := result.(*BinaryOperation); !ok || binOp.Op != OpLE {
			t.Errorf("Expected LE operation, got %T with op %v", result, binOp.Op)
		}
	})

	// 测试2: 复杂表达式的CNF转换
	t.Run("Complex expression CNF conversion", func(t *testing.T) {
		converter := NewCNFConverter()

		// (a OR b) AND (c OR d) 已经是CNF形式
		a := &Column{Name: "a"}
		b := &Column{Name: "b"}
		c := &Column{Name: "c"}
		d := &Column{Name: "d"}

		orAB := &BinaryOperation{Op: OpOr, Left: a, Right: b}
		orCD := &BinaryOperation{Op: OpOr, Left: c, Right: d}
		andExpr := &BinaryOperation{Op: OpAnd, Left: orAB, Right: orCD}

		result := converter.ConvertToCNF(andExpr)

		// 提取合取项
		conjuncts := converter.ExtractConjuncts(result)
		if len(conjuncts) != 2 {
			t.Errorf("Expected 2 conjuncts, got %d", len(conjuncts))
		}
	})

	// 测试3: a OR (b AND c) 应该分配为 (a OR b) AND (a OR c)
	t.Run("Distributive law application", func(t *testing.T) {
		converter := NewCNFConverter()

		a := &Column{Name: "a"}
		b := &Column{Name: "b"}
		c := &Column{Name: "c"}

		andBC := &BinaryOperation{Op: OpAnd, Left: b, Right: c}
		orExpr := &BinaryOperation{Op: OpOr, Left: a, Right: andBC}

		result := converter.ConvertToCNF(orExpr)

		// 结果应该是AND运算
		if binOp, ok := result.(*BinaryOperation); !ok || binOp.Op != OpAnd {
			t.Errorf("Expected AND operation at root, got %T", result)
		}

		// 提取合取项
		conjuncts := converter.ExtractConjuncts(result)
		if len(conjuncts) != 2 {
			t.Errorf("Expected 2 conjuncts after distribution, got %d", len(conjuncts))
		}
	})

	// 测试4: 德摩根定律应用
	t.Run("DeMorgan's law", func(t *testing.T) {
		converter := NewCNFConverter()

		// NOT (a AND b) 应该转换为 (NOT a) OR (NOT b)
		a := &Column{Name: "a"}
		b := &Column{Name: "b"}

		andExpr := &BinaryOperation{Op: OpAnd, Left: a, Right: b}
		notExpr := &NotExpression{Operand: andExpr}

		result := converter.ConvertToCNF(notExpr)

		// 结果应该是OR运算
		if binOp, ok := result.(*BinaryOperation); !ok || binOp.Op != OpOr {
			t.Errorf("Expected OR operation, got %T", result)
		}
	})

	// 测试5: 提取合取项用于谓词下推
	t.Run("Extract conjuncts for predicate pushdown", func(t *testing.T) {
		converter := NewCNFConverter()

		// 创建一个复杂条件: (age > 18 AND city = 'Beijing') AND status = 'active'
		age := &Column{Name: "age"}
		city := &Column{Name: "city"}
		status := &Column{Name: "status"}

		ageGt18 := &BinaryOperation{
			Op:    OpGT,
			Left:  age,
			Right: &Constant{Value: int64(18)},
		}
		cityEqBeijing := &BinaryOperation{
			Op:    OpEQ,
			Left:  city,
			Right: &Constant{Value: "Beijing"},
		}
		statusActive := &BinaryOperation{
			Op:    OpEQ,
			Left:  status,
			Right: &Constant{Value: "active"},
		}

		and1 := &BinaryOperation{Op: OpAnd, Left: ageGt18, Right: cityEqBeijing}
		and2 := &BinaryOperation{Op: OpAnd, Left: and1, Right: statusActive}

		result := converter.ConvertToCNF(and2)
		conjuncts := converter.ExtractConjuncts(result)

		// 应该提取出3个独立的条件
		if len(conjuncts) != 3 {
			t.Errorf("Expected 3 conjuncts, got %d", len(conjuncts))
		}
	})

	// 测试6: 处理空条件
	t.Run("Handle nil expression", func(t *testing.T) {
		converter := NewCNFConverter()
		result := converter.ConvertToCNF(nil)
		if result != nil {
			t.Error("Expected nil for nil input")
		}
	})
}

// TestCNFConverterEdgeCases 测试边界情况
func TestCNFConverterEdgeCases(t *testing.T) {
	converter := NewCNFConverter()

	// 测试1: 单个列引用
	t.Run("Single column", func(t *testing.T) {
		col := &Column{Name: "a"}
		result := converter.ConvertToCNF(col)
		if _, ok := result.(*Column); !ok {
			t.Error("Single column should remain unchanged")
		}
	})

	// 测试2: 单个常量
	t.Run("Single constant", func(t *testing.T) {
		constant := &Constant{Value: int64(42)}
		result := converter.ConvertToCNF(constant)
		if _, ok := result.(*Constant); !ok {
			t.Error("Single constant should remain unchanged")
		}
	})

	// 测试3: 嵌套的双重否定
	t.Run("Nested double negation", func(t *testing.T) {
		col := &Column{Name: "a"}
		not1 := &NotExpression{Operand: col}
		not2 := &NotExpression{Operand: not1}
		not3 := &NotExpression{Operand: not2}

		result := converter.ConvertToCNF(not3)

		// 应该只剩下一个NOT
		if _, ok := result.(*NotExpression); !ok {
			t.Errorf("Expected NotExpression, got %T", result)
		}
	})

	// 测试4: 深层嵌套的AND
	t.Run("Deeply nested AND", func(t *testing.T) {
		a := &Column{Name: "a"}
		b := &Column{Name: "b"}
		c := &Column{Name: "c"}
		d := &Column{Name: "d"}

		// a AND (b AND (c AND d))
		andCD := &BinaryOperation{Op: OpAnd, Left: c, Right: d}
		andBCD := &BinaryOperation{Op: OpAnd, Left: b, Right: andCD}
		andABCD := &BinaryOperation{Op: OpAnd, Left: a, Right: andBCD}

		result := converter.ConvertToCNF(andABCD)
		conjuncts := converter.ExtractConjuncts(result)

		// 应该提取出4个项
		if len(conjuncts) != 4 {
			t.Errorf("Expected 4 conjuncts, got %d", len(conjuncts))
		}
	})

	// 测试5: 所有比较运算符的取反
	t.Run("All comparison operator negations", func(t *testing.T) {
		ops := []struct {
			original BinaryOp
			expected BinaryOp
		}{
			{OpEQ, OpNE},
			{OpNE, OpEQ},
			{OpLT, OpGE},
			{OpLE, OpGT},
			{OpGT, OpLE},
			{OpGE, OpLT},
		}

		for _, op := range ops {
			expr := &BinaryOperation{
				Op:    op.original,
				Left:  &Column{Name: "x"},
				Right: &Constant{Value: int64(10)},
			}
			notExpr := &NotExpression{Operand: expr}
			result := converter.ConvertToCNF(notExpr)

			if binOp, ok := result.(*BinaryOperation); ok {
				if binOp.Op != op.expected {
					t.Errorf("NOT %v should become %v, got %v", op.original, op.expected, binOp.Op)
				}
			} else {
				t.Errorf("Expected BinaryOperation, got %T", result)
			}
		}
	})
}

// TestCNFConverterMaxClausesLimit 测试子句数量限制
func TestCNFConverterMaxClausesLimit(t *testing.T) {
	converter := NewCNFConverter()
	converter.SetMaxClauses(10)

	// 构建一个会产生大量子句的表达式
	// (a1 AND a2 AND a3) OR (b1 AND b2 AND b3) 会产生 3*3=9 个子句
	a1 := &Column{Name: "a1"}
	a2 := &Column{Name: "a2"}
	a3 := &Column{Name: "a3"}
	b1 := &Column{Name: "b1"}
	b2 := &Column{Name: "b2"}
	b3 := &Column{Name: "b3"}

	andA := &BinaryOperation{
		Op:    OpAnd,
		Left:  &BinaryOperation{Op: OpAnd, Left: a1, Right: a2},
		Right: a3,
	}
	andB := &BinaryOperation{
		Op:    OpAnd,
		Left:  &BinaryOperation{Op: OpAnd, Left: b1, Right: b2},
		Right: b3,
	}

	orExpr := &BinaryOperation{Op: OpOr, Left: andA, Right: andB}

	result := converter.ConvertToCNF(orExpr)

	// 验证结果不为nil
	if result == nil {
		t.Error("Result should not be nil")
	}

	// 即使超过限制，也应该返回某种形式的结果
	t.Logf("Result type: %T", result)
}
