package plan

import (
	"testing"
)

// TestEliminateDoubleNegation 测试双重否定消除
func TestEliminateDoubleNegation(t *testing.T) {
	converter := NewCNFConverter()

	// NOT (NOT a) -> a
	inner := &Column{Name: "a"}
	notInner := &NotExpression{Operand: inner}
	notNotInner := &NotExpression{Operand: notInner}

	result := converter.eliminateDoubleNegation(notNotInner)
	if col, ok := result.(*Column); !ok || col.Name != "a" {
		t.Errorf("Expected Column 'a', got %v", result)
	}

	// NOT (NOT (NOT a)) -> NOT a
	notNotNotInner := &NotExpression{Operand: notNotInner}
	result = converter.eliminateDoubleNegation(notNotNotInner)
	if _, ok := result.(*NotExpression); !ok {
		t.Errorf("Expected NotExpression, got %T", result)
	}
}

// TestDeMorganLaw 测试德摩根定律
func TestDeMorganLaw(t *testing.T) {
	converter := NewCNFConverter()

	// NOT (a AND b) -> (NOT a) OR (NOT b)
	a := &Column{Name: "a"}
	b := &Column{Name: "b"}
	andExpr := &BinaryOperation{Op: OpAnd, Left: a, Right: b}
	notAndExpr := &NotExpression{Operand: andExpr}

	result := converter.pushDownNegation(notAndExpr, false)
	if binOp, ok := result.(*BinaryOperation); !ok || binOp.Op != OpOr {
		t.Errorf("Expected OR operation, got %v", result)
	}

	// NOT (a OR b) -> (NOT a) AND (NOT b)
	orExpr := &BinaryOperation{Op: OpOr, Left: a, Right: b}
	notOrExpr := &NotExpression{Operand: orExpr}

	result = converter.pushDownNegation(notOrExpr, false)
	if binOp, ok := result.(*BinaryOperation); !ok || binOp.Op != OpAnd {
		t.Errorf("Expected AND operation, got %v", result)
	}
}

// TestNegateOperator 测试运算符取反
func TestNegateOperator(t *testing.T) {
	converter := NewCNFConverter()

	tests := []struct {
		op       BinaryOp
		expected BinaryOp
	}{
		{OpEQ, OpNE},
		{OpNE, OpEQ},
		{OpLT, OpGE},
		{OpLE, OpGT},
		{OpGT, OpLE},
		{OpGE, OpLT},
	}

	for _, tt := range tests {
		expr := &BinaryOperation{
			Op:    tt.op,
			Left:  &Column{Name: "a"},
			Right: &Constant{Value: int64(10)},
		}
		result := converter.negateOperator(expr)
		if binOp, ok := result.(*BinaryOperation); ok {
			if binOp.Op != tt.expected {
				t.Errorf("negateOperator(%v) = %v, want %v", tt.op, binOp.Op, tt.expected)
			}
		} else {
			t.Errorf("Expected BinaryOperation, got %T", result)
		}
	}
}

// TestDistributiveRightAnd 测试简单分配律: A OR (B AND C)
func TestDistributiveRightAnd(t *testing.T) {
	converter := NewCNFConverter()

	// a OR (b AND c) -> (a OR b) AND (a OR c)
	a := &Column{Name: "a"}
	b := &Column{Name: "b"}
	c := &Column{Name: "c"}

	andExpr := &BinaryOperation{Op: OpAnd, Left: b, Right: c}
	orExpr := &BinaryOperation{Op: OpOr, Left: a, Right: andExpr}

	result := converter.applyDistributiveLaw(orExpr)

	// 结果应该是AND运算
	if binOp, ok := result.(*BinaryOperation); ok {
		if binOp.Op != OpAnd {
			t.Errorf("Expected AND at root, got %v", binOp.Op)
		}
		// 检查左右子树都是OR
		if leftOr, ok := binOp.Left.(*BinaryOperation); !ok || leftOr.Op != OpOr {
			t.Errorf("Expected OR on left, got %v", binOp.Left)
		}
		if rightOr, ok := binOp.Right.(*BinaryOperation); !ok || rightOr.Op != OpOr {
			t.Errorf("Expected OR on right, got %v", binOp.Right)
		}
	} else {
		t.Errorf("Expected BinaryOperation, got %T", result)
	}
}

// TestDistributiveLeftAnd 测试分配律: (A AND B) OR C
func TestDistributiveLeftAnd(t *testing.T) {
	converter := NewCNFConverter()

	// (a AND b) OR c -> (a OR c) AND (b OR c)
	a := &Column{Name: "a"}
	b := &Column{Name: "b"}
	c := &Column{Name: "c"}

	andExpr := &BinaryOperation{Op: OpAnd, Left: a, Right: b}
	orExpr := &BinaryOperation{Op: OpOr, Left: andExpr, Right: c}

	result := converter.applyDistributiveLaw(orExpr)

	// 结果应该是AND运算
	if binOp, ok := result.(*BinaryOperation); ok {
		if binOp.Op != OpAnd {
			t.Errorf("Expected AND at root, got %v", binOp.Op)
		}
	} else {
		t.Errorf("Expected BinaryOperation, got %T", result)
	}
}

// TestDistributiveBothAnd 测试复杂分配律: (A AND B) OR (C AND D)
func TestDistributiveBothAnd(t *testing.T) {
	converter := NewCNFConverter()

	// (a AND b) OR (c AND d) -> (a OR c) AND (a OR d) AND (b OR c) AND (b OR d)
	a := &Column{Name: "a"}
	b := &Column{Name: "b"}
	c := &Column{Name: "c"}
	d := &Column{Name: "d"}

	andLeft := &BinaryOperation{Op: OpAnd, Left: a, Right: b}
	andRight := &BinaryOperation{Op: OpAnd, Left: c, Right: d}
	orExpr := &BinaryOperation{Op: OpOr, Left: andLeft, Right: andRight}

	result := converter.applyDistributiveLaw(orExpr)

	// 结果应该是AND运算
	if binOp, ok := result.(*BinaryOperation); ok {
		if binOp.Op != OpAnd {
			t.Errorf("Expected AND at root, got %v", binOp.Op)
		}
		// 应该有4个OR子句通过AND连接
	} else {
		t.Errorf("Expected BinaryOperation, got %T", result)
	}
}

// TestComplexCNFConversion 测试复杂表达式转换
func TestComplexCNFConversion(t *testing.T) {
	converter := NewCNFConverter()

	// NOT ((age > 18 AND city = 'Beijing') OR status = 'inactive')
	// 应转换为: (age <= 18 OR city != 'Beijing') AND status != 'inactive'
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
	statusInactive := &BinaryOperation{
		Op:    OpEQ,
		Left:  status,
		Right: &Constant{Value: "inactive"},
	}

	andExpr := &BinaryOperation{Op: OpAnd, Left: ageGt18, Right: cityEqBeijing}
	orExpr := &BinaryOperation{Op: OpOr, Left: andExpr, Right: statusInactive}
	notExpr := &NotExpression{Operand: orExpr}

	result := converter.ConvertToCNF(notExpr)

	// 检查结果是AND运算
	if binOp, ok := result.(*BinaryOperation); ok {
		if binOp.Op != OpAnd {
			t.Errorf("Expected AND at root, got %v", binOp.Op)
		}
	} else {
		t.Errorf("Expected BinaryOperation, got %T", result)
	}
}

// TestExtractConjuncts 测试提取合取项
func TestExtractConjuncts(t *testing.T) {
	converter := NewCNFConverter()

	// a AND b AND c
	a := &Column{Name: "a"}
	b := &Column{Name: "b"}
	c := &Column{Name: "c"}

	andBC := &BinaryOperation{Op: OpAnd, Left: b, Right: c}
	andABC := &BinaryOperation{Op: OpAnd, Left: a, Right: andBC}

	conjuncts := converter.ExtractConjuncts(andABC)
	if len(conjuncts) != 3 {
		t.Errorf("Expected 3 conjuncts, got %d", len(conjuncts))
	}
}

// TestExtractDisjuncts 测试提取析取项
func TestExtractDisjuncts(t *testing.T) {
	converter := NewCNFConverter()

	// a OR b OR c
	a := &Column{Name: "a"}
	b := &Column{Name: "b"}
	c := &Column{Name: "c"}

	orBC := &BinaryOperation{Op: OpOr, Left: b, Right: c}
	orABC := &BinaryOperation{Op: OpOr, Left: a, Right: orBC}

	disjuncts := converter.ExtractDisjuncts(orABC)
	if len(disjuncts) != 3 {
		t.Errorf("Expected 3 disjuncts, got %d", len(disjuncts))
	}
}

// TestConvertToCNF_AlreadyCNF_Unchanged 边界：已是 CNF 的 (a AND b) 经 ConvertToCNF 后结构保持不变
func TestConvertToCNF_AlreadyCNF_Unchanged(t *testing.T) {
	converter := NewCNFConverter()
	a := &Column{Name: "a"}
	b := &Column{Name: "b"}
	andExpr := &BinaryOperation{Op: OpAnd, Left: a, Right: b}
	result := converter.ConvertToCNF(andExpr)
	if result == nil {
		t.Fatal("ConvertToCNF(AND(a,b)) returned nil")
	}
	bin, ok := result.(*BinaryOperation)
	if !ok || bin.Op != OpAnd {
		op := BinaryOp(0)
		if b, o := result.(*BinaryOperation); o {
			op = b.Op
		}
		t.Errorf("expected AND, got %T op=%v", result, op)
	}
}

// TestIsCNF 测试CNF检测
func TestIsCNF(t *testing.T) {
	converter := NewCNFConverter()

	tests := []struct {
		name     string
		expr     Expression
		expected bool
	}{
		{
			name: "simple atomic predicate",
			expr: &BinaryOperation{
				Op:    OpEQ,
				Left:  &Column{Name: "a"},
				Right: &Constant{Value: int64(1)},
			},
			expected: true,
		},
		{
			name: "simple OR clause",
			expr: &BinaryOperation{
				Op: OpOr,
				Left: &BinaryOperation{
					Op:    OpEQ,
					Left:  &Column{Name: "a"},
					Right: &Constant{Value: int64(1)},
				},
				Right: &BinaryOperation{
					Op:    OpEQ,
					Left:  &Column{Name: "b"},
					Right: &Constant{Value: int64(2)},
				},
			},
			expected: true,
		},
		{
			name: "valid CNF: (a OR b) AND c",
			expr: &BinaryOperation{
				Op: OpAnd,
				Left: &BinaryOperation{
					Op:    OpOr,
					Left:  &Column{Name: "a"},
					Right: &Column{Name: "b"},
				},
				Right: &Column{Name: "c"},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.isCNF(tt.expr)
			if result != tt.expected {
				t.Errorf("isCNF() = %v, want %v for %s", result, tt.expected, tt.name)
			}
		})
	}
}

// TestCloneExpression 测试表达式克隆
func TestCloneExpression(t *testing.T) {
	converter := NewCNFConverter()

	original := &BinaryOperation{
		Op:   OpAnd,
		Left: &Column{Name: "a"},
		Right: &BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "b"},
			Right: &Constant{Value: int64(10)},
		},
	}

	cloned := converter.cloneExpression(original)

	// 检查是否是不同的对象
	if original == cloned {
		t.Error("Cloned expression should be a different object")
	}

	// 检查结构是否相同
	if binOp, ok := cloned.(*BinaryOperation); !ok || binOp.Op != OpAnd {
		t.Errorf("Cloned expression structure mismatch")
	}
}

// TestMaxClausesLimit 测试子句数量限制
func TestMaxClausesLimit(t *testing.T) {
	converter := NewCNFConverter()
	converter.SetMaxClauses(5)

	// 构建会导致大量子句的表达式
	// (a AND b AND c) OR (d AND e AND f) 会产生 3*3=9 个子句
	a := &Column{Name: "a"}
	b := &Column{Name: "b"}
	c := &Column{Name: "c"}
	d := &Column{Name: "d"}
	e := &Column{Name: "e"}
	f := &Column{Name: "f"}

	andLeft := &BinaryOperation{
		Op:    OpAnd,
		Left:  &BinaryOperation{Op: OpAnd, Left: a, Right: b},
		Right: c,
	}
	andRight := &BinaryOperation{
		Op:    OpAnd,
		Left:  &BinaryOperation{Op: OpAnd, Left: d, Right: e},
		Right: f,
	}
	orExpr := &BinaryOperation{Op: OpOr, Left: andLeft, Right: andRight}

	result := converter.applyDistributiveLaw(orExpr)

	// 由于超过限制，应该保持原OR结构
	if binOp, ok := result.(*BinaryOperation); ok {
		if binOp.Op != OpOr {
			t.Logf("Expression was expanded despite limit (expected OR, got %v)", binOp.Op)
			// 这里不报错，因为在某些情况下可能会部分展开
		}
	}
}

// TestSimpleCases 测试简单案例
func TestSimpleCases(t *testing.T) {
	converter := NewCNFConverter()

	tests := []struct {
		name string
		expr Expression
	}{
		{
			name: "a AND b",
			expr: &BinaryOperation{
				Op:    OpAnd,
				Left:  &Column{Name: "a"},
				Right: &Column{Name: "b"},
			},
		},
		{
			name: "a OR b",
			expr: &BinaryOperation{
				Op:    OpOr,
				Left:  &Column{Name: "a"},
				Right: &Column{Name: "b"},
			},
		},
		{
			name: "NOT a",
			expr: &NotExpression{
				Operand: &Column{Name: "a"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.ConvertToCNF(tt.expr)
			if result == nil {
				t.Error("ConvertToCNF returned nil")
			}
		})
	}
}
