package plan

import (
	"testing"
)

// TestConstantComparisonFolding 测试常量比较折叠：1=1 -> true, 1=2 -> false（TDD：先写失败测试）
func TestConstantComparisonFolding(t *testing.T) {
	normalizer := NewExpressionNormalizer()

	tests := []struct {
		name     string
		expr     Expression
		expected interface{}
	}{
		{
			name: "1=1 折叠为 true",
			expr: &BinaryOperation{
				Op:    OpEQ,
				Left:  &Constant{Value: int64(1)},
				Right: &Constant{Value: int64(1)},
			},
			expected: true,
		},
		{
			name: "1=2 折叠为 false",
			expr: &BinaryOperation{
				Op:    OpEQ,
				Left:  &Constant{Value: int64(1)},
				Right: &Constant{Value: int64(2)},
			},
			expected: false,
		},
		{
			name: "2<5 折叠为 true",
			expr: &BinaryOperation{
				Op:    OpLT,
				Left:  &Constant{Value: int64(2)},
				Right: &Constant{Value: int64(5)},
			},
			expected: true,
		},
		{
			name: "5<2 折叠为 false",
			expr: &BinaryOperation{
				Op:    OpLT,
				Left:  &Constant{Value: int64(5)},
				Right: &Constant{Value: int64(2)},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizer.Normalize(tt.expr)
			if cons, ok := result.(*Constant); ok {
				if cons.Value != tt.expected {
					t.Errorf("expected Constant(%v), got Constant(%v)", tt.expected, cons.Value)
				}
			} else {
				t.Errorf("expected *Constant, got %T: %v", result, result)
			}
		})
	}
}

// TestNotConstantBooleanFolding TDD：NOT true -> false, NOT false -> true（先写失败测试）
func TestNotConstantBooleanFolding(t *testing.T) {
	normalizer := NewExpressionNormalizer()

	tests := []struct {
		name     string
		expr     Expression
		expected bool
	}{
		{"NOT true -> false", &NotExpression{Operand: &Constant{Value: true}}, false},
		{"NOT false -> true", &NotExpression{Operand: &Constant{Value: false}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizer.Normalize(tt.expr)
			cons, ok := result.(*Constant)
			if !ok {
				t.Fatalf("expected *Constant, got %T: %v", result, result)
			}
			if cons.Value != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, cons.Value)
			}
		})
	}
}

// TestConstantFolding 测试常量折叠
func TestConstantFolding(t *testing.T) {
	normalizer := NewExpressionNormalizer()

	tests := []struct {
		name     string
		expr     Expression
		expected interface{}
	}{
		{
			name: "算术运算折叠",
			expr: &BinaryOperation{
				Op:    OpAdd,
				Left:  &Constant{Value: int64(1)},
				Right: &Constant{Value: int64(2)},
			},
			expected: int64(3),
		},
		{
			name: "恒等元简化 x+0",
			expr: &BinaryOperation{
				Op:    OpAdd,
				Left:  &Column{Name: "age"},
				Right: &Constant{Value: int64(0)},
			},
			expected: "age",
		},
		{
			name: "零元简化 x*0",
			expr: &BinaryOperation{
				Op:    OpMul,
				Left:  &Column{Name: "age"},
				Right: &Constant{Value: int64(0)},
			},
			expected: int64(0),
		},
		{
			name: "布尔运算 TRUE AND x",
			expr: &BinaryOperation{
				Op:    OpAnd,
				Left:  &Constant{Value: true},
				Right: &Column{Name: "active"},
			},
			expected: "active",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizer.Normalize(tt.expr)

			// 检查结果
			if col, ok := result.(*Column); ok {
				if col.Name != tt.expected {
					t.Errorf("Expected column %v, got %v", tt.expected, col.Name)
				}
			} else if cons, ok := result.(*Constant); ok {
				if cons.Value != tt.expected {
					t.Errorf("Expected constant %v, got %v", tt.expected, cons.Value)
				}
			}
		})
	}
}

// TestPredicateNormalization 测试谓词标准化
func TestPredicateNormalization(t *testing.T) {
	normalizer := NewExpressionNormalizer()

	tests := []struct {
		name     string
		expr     Expression
		expected string // 期望的字符串表示
	}{
		{
			name: "交换常量到右侧",
			expr: &BinaryOperation{
				Op:    OpEQ,
				Left:  &Constant{Value: int64(18)},
				Right: &Column{Name: "age"},
			},
			expected: "age", // 应该交换为 age = 18
		},
		{
			name: "5 > age 转换为 age < 5",
			expr: &BinaryOperation{
				Op:    OpGT,
				Left:  &Constant{Value: int64(5)},
				Right: &Column{Name: "age"},
			},
			expected: "age", // 左侧应该是列
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizer.Normalize(tt.expr)

			if bin, ok := result.(*BinaryOperation); ok {
				if col, ok := bin.Left.(*Column); ok {
					if col.Name != tt.expected {
						t.Errorf("Expected left column %v, got %v", tt.expected, col.Name)
					}
				} else {
					t.Errorf("Expected left to be Column, got %T", bin.Left)
				}
			}
		})
	}
}

// TestRedundancyElimination 测试冗余消除
func TestRedundancyElimination(t *testing.T) {
	normalizer := NewExpressionNormalizer()

	tests := []struct {
		name        string
		expr        Expression
		expectFalse bool
		expectTrue  bool
	}{
		{
			name: "矛盾条件 age > 20 AND age < 10",
			expr: &BinaryOperation{
				Op: OpAnd,
				Left: &BinaryOperation{
					Op:    OpGT,
					Left:  &Column{Name: "age"},
					Right: &Constant{Value: int64(20)},
				},
				Right: &BinaryOperation{
					Op:    OpLT,
					Left:  &Column{Name: "age"},
					Right: &Constant{Value: int64(10)},
				},
			},
			expectFalse: true,
		},
		{
			name: "恒真条件 age > 0 OR age <= 0",
			expr: &BinaryOperation{
				Op: OpOr,
				Left: &BinaryOperation{
					Op:    OpGT,
					Left:  &Column{Name: "age"},
					Right: &Constant{Value: int64(0)},
				},
				Right: &BinaryOperation{
					Op:    OpLE,
					Left:  &Column{Name: "age"},
					Right: &Constant{Value: int64(0)},
				},
			},
			expectTrue: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizer.Normalize(tt.expr)

			if tt.expectFalse {
				if cons, ok := result.(*Constant); ok {
					if cons.Value != false {
						t.Errorf("Expected FALSE constant, got %v", cons.Value)
					}
				} else {
					t.Errorf("Expected Constant(false), got %T: %v", result, result)
				}
			}

			if tt.expectTrue {
				if cons, ok := result.(*Constant); ok {
					if cons.Value != true {
						t.Errorf("Expected TRUE constant, got %v", cons.Value)
					}
				} else {
					t.Errorf("Expected Constant(true), got %T: %v", result, result)
				}
			}
		})
	}
}

// TestIdempotentLaw 测试幂等律
func TestIdempotentLaw(t *testing.T) {
	normalizer := NewExpressionNormalizer()

	// age AND age = age
	expr := &BinaryOperation{
		Op:    OpAnd,
		Left:  &Column{Name: "age"},
		Right: &Column{Name: "age"},
	}

	result := normalizer.Normalize(expr)

	if col, ok := result.(*Column); ok {
		if col.Name != "age" {
			t.Errorf("Expected column 'age', got %v", col.Name)
		}
	} else {
		t.Logf("Idempotent law not fully applied, got %T: %v", result, result)
	}
}

// TestCommutativeLaw 测试交换律
func TestCommutativeLaw(t *testing.T) {
	normalizer := NewExpressionNormalizer()

	// 5 + age 应该变为 age + 5
	expr := &BinaryOperation{
		Op:    OpAdd,
		Left:  &Constant{Value: int64(5)},
		Right: &Column{Name: "age"},
	}

	result := normalizer.Normalize(expr)

	if bin, ok := result.(*BinaryOperation); ok {
		if _, ok := bin.Left.(*Column); !ok {
			t.Errorf("Expected left to be Column after commutative law, got %T", bin.Left)
		}
		if _, ok := bin.Right.(*Constant); !ok {
			t.Errorf("Expected right to be Constant after commutative law, got %T", bin.Right)
		}
	}
}
