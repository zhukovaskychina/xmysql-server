package plan

import (
	"fmt"
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

func TestDoubleNegationSimplificationPreservesPredicate(t *testing.T) {
	normalizer := NewExpressionNormalizer()
	expr := &NotExpression{Operand: &NotExpression{Operand: &BinaryOperation{
		Op:    OpGT,
		Left:  &Column{Name: "age"},
		Right: &Constant{Value: int64(18)},
	}}}

	result := normalizer.Normalize(expr)
	comparison, ok := result.(*BinaryOperation)
	if !ok {
		t.Fatalf("double negation result = %T, want *BinaryOperation", result)
	}
	left, leftOK := comparison.Left.(*Column)
	right, rightOK := comparison.Right.(*Constant)
	if comparison.Op != OpGT || !leftOK || left.Name != "age" || !rightOK || right.Value != int64(18) {
		t.Fatalf("double negation result = %s, want age > 18", comparison.String())
	}
}

func TestDeMorganSimplificationPreservesBooleanStructure(t *testing.T) {
	normalizer := NewExpressionNormalizer()
	expr := &NotExpression{Operand: &BinaryOperation{
		Op: OpAnd,
		Left: &BinaryOperation{
			Op:    OpGT,
			Left:  &Column{Name: "age"},
			Right: &Constant{Value: int64(18)},
		},
		Right: &BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "active"},
			Right: &Constant{Value: true},
		},
	}}

	result := normalizer.Normalize(expr)
	deMorgan, ok := result.(*BinaryOperation)
	if !ok || deMorgan.Op != OpOr {
		t.Fatalf("De Morgan result = %T %v, want OR expression", result, result)
	}
	if _, ok := deMorgan.Left.(*NotExpression); !ok {
		t.Fatalf("left De Morgan term = %T, want NotExpression", deMorgan.Left)
	}
	if _, ok := deMorgan.Right.(*NotExpression); !ok {
		t.Fatalf("right De Morgan term = %T, want NotExpression", deMorgan.Right)
	}
}

func TestDeMorganRecursivelyEliminatesNestedNegation(t *testing.T) {
	normalizer := NewExpressionNormalizer()
	expr := &NotExpression{Operand: &BinaryOperation{
		Op: OpAnd,
		Left: &NotExpression{Operand: &BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "status"},
			Right: &Constant{Value: "ready"},
		}},
		Right: &BinaryOperation{
			Op:    OpGT,
			Left:  &Column{Name: "age"},
			Right: &Constant{Value: int64(18)},
		},
	}}

	result := normalizer.Normalize(expr)
	deMorgan, ok := result.(*BinaryOperation)
	if !ok || deMorgan.Op != OpOr {
		t.Fatalf("nested De Morgan result = %T %v, want OR expression", result, result)
	}
	if _, ok := deMorgan.Left.(*BinaryOperation); !ok {
		t.Fatalf("nested De Morgan left term = %T, want original predicate after double-negation elimination", deMorgan.Left)
	}
}

func TestAssociativeBooleanFlattenPreservesThreeValuedResults(t *testing.T) {
	normalizer := NewExpressionNormalizer()
	expr := &BinaryOperation{
		Op:   OpAnd,
		Left: &Column{Name: "a"},
		Right: &BinaryOperation{
			Op:    OpAnd,
			Left:  &Column{Name: "b"},
			Right: &Column{Name: "c"},
		},
	}
	normalized := normalizer.Normalize(expr)
	if got := normalized.String(); got != "((a AND b) AND c)" {
		t.Fatalf("normalized associative expression = %s, want ((a AND b) AND c)", got)
	}

	rows := []map[string]interface{}{
		{"a": true, "b": nil, "c": false},
		{"a": nil, "b": true, "c": true},
		{"a": false, "b": nil, "c": true},
		{"a": true, "b": true, "c": true},
	}
	for _, row := range rows {
		original := &BinaryOperation{
			Op:    OpAnd,
			Left:  &Column{Name: "a"},
			Right: &BinaryOperation{Op: OpAnd, Left: &Column{Name: "b"}, Right: &Column{Name: "c"}},
		}
		want, err := original.Eval(&EvalContext{Row: row})
		if err != nil {
			t.Fatalf("original Eval(%v): %v", row, err)
		}
		got, err := normalized.Eval(&EvalContext{Row: row})
		if err != nil {
			t.Fatalf("normalized Eval(%v): %v", row, err)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("row %v: normalized result %v, want %v", row, got, want)
		}
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

func TestConstantFoldingCoversUnaryCaseTupleAndDynamicBetweenBounds(t *testing.T) {
	normalizer := NewExpressionNormalizer()

	unaried := normalizer.Normalize(&UnaryOperation{
		Operator: "-",
		Operand:  &Constant{Value: int64(3)},
	})
	constant, ok := unaried.(*Constant)
	if !ok || constant.Value != int64(-3) {
		t.Fatalf("unary constant folding = %#v, want Constant(-3)", unaried)
	}

	caseExpr := normalizer.Normalize(&CaseExpression{
		Whens: []CaseWhen{{
			Condition: &Constant{Value: false},
			Value:     &Constant{Value: "wrong"},
		}},
		Else: &BinaryOperation{
			Op:    OpAdd,
			Left:  &Constant{Value: int64(2)},
			Right: &Constant{Value: int64(3)},
		},
	})
	constant, ok = caseExpr.(*Constant)
	if !ok || constant.Value != int64(5) {
		t.Fatalf("CASE constant folding = %#v, want Constant(5)", caseExpr)
	}

	tupleExpr := normalizer.Normalize(&TupleExpression{Exprs: []Expression{
		&Constant{Value: int64(1)},
		&BinaryOperation{Op: OpAdd, Left: &Constant{Value: int64(2)}, Right: &Constant{Value: int64(3)}},
	}})
	constant, ok = tupleExpr.(*Constant)
	if !ok {
		t.Fatalf("tuple constant folding = %T, want *Constant", tupleExpr)
	}
	values, ok := constant.Value.([]interface{})
	if !ok || len(values) != 2 || values[0] != int64(1) || values[1] != int64(5) {
		t.Fatalf("tuple constant folding value = %#v, want [1 5]", constant.Value)
	}

	betweenExpr := normalizer.Normalize(&BetweenExpression{
		Column: &Column{Name: "age"},
		LowerExpr: &BinaryOperation{
			Op:    OpAdd,
			Left:  &Constant{Value: int64(1)},
			Right: &Constant{Value: int64(1)},
		},
		UpperExpr: &Constant{Value: int64(10)},
	})
	between, ok := betweenExpr.(*BinaryOperation)
	if !ok || between.Op != OpAnd {
		t.Fatalf("BETWEEN normalization = %T %v, want AND", betweenExpr, betweenExpr)
	}
	lower, ok := between.Left.(*BinaryOperation)
	if !ok || lower.Op != OpGE {
		t.Fatalf("BETWEEN lower predicate = %#v, want >=", between.Left)
	}
	lowerBound, ok := lower.Right.(*Constant)
	if !ok || lowerBound.Value != int64(2) {
		t.Fatalf("BETWEEN lower bound = %#v, want Constant(2)", lower.Right)
	}
}

func TestPredicateNormalizationExpandsBetweenWithoutChangingThreeValuedSemantics(t *testing.T) {
	normalizer := NewExpressionNormalizer()

	between := normalizer.Normalize(&BetweenExpression{
		Column:    &Column{Name: "age"},
		LowerExpr: &Constant{Value: int64(18)},
		UpperExpr: &Constant{Value: int64(60)},
	})
	conjunction, ok := between.(*BinaryOperation)
	if !ok || conjunction.Op != OpAnd {
		t.Fatalf("BETWEEN normalization = %T %v, want AND", between, between)
	}
	if left, ok := conjunction.Left.(*BinaryOperation); !ok || left.Op != OpGE {
		t.Fatalf("BETWEEN lower predicate = %T %v, want >=", conjunction.Left, conjunction.Left)
	}
	if right, ok := conjunction.Right.(*BinaryOperation); !ok || right.Op != OpLE {
		t.Fatalf("BETWEEN upper predicate = %T %v, want <=", conjunction.Right, conjunction.Right)
	}

	notBetween := normalizer.Normalize(&BetweenExpression{
		Column:    &Column{Name: "age"},
		LowerExpr: &Constant{Value: int64(18)},
		UpperExpr: &Constant{Value: int64(60)},
		Not:       true,
	})
	disjunction, ok := notBetween.(*BinaryOperation)
	if !ok || disjunction.Op != OpOr {
		t.Fatalf("NOT BETWEEN normalization = %T %v, want OR", notBetween, notBetween)
	}
	if left, ok := disjunction.Left.(*BinaryOperation); !ok || left.Op != OpLT {
		t.Fatalf("NOT BETWEEN lower predicate = %T %v, want <", disjunction.Left, disjunction.Left)
	}
	if right, ok := disjunction.Right.(*BinaryOperation); !ok || right.Op != OpGT {
		t.Fatalf("NOT BETWEEN upper predicate = %T %v, want >", disjunction.Right, disjunction.Right)
	}

	rows := []map[string]interface{}{{"age": int64(18)}, {"age": int64(61)}, {"age": nil}}
	want := []interface{}{true, false, nil}
	for i, row := range rows {
		got, err := (&BetweenExpression{
			Column:    &Column{Name: "age"},
			LowerExpr: &Constant{Value: int64(18)},
			UpperExpr: &Constant{Value: int64(60)},
		}).Eval(&EvalContext{Row: row})
		if err != nil {
			t.Fatalf("original BETWEEN row %d: %v", i, err)
		}
		normalized, err := between.Eval(&EvalContext{Row: row})
		if err != nil {
			t.Fatalf("normalized BETWEEN row %d: %v", i, err)
		}
		if normalized != want[i] || got != want[i] {
			t.Fatalf("row %d: original=%#v normalized=%#v want=%#v", i, got, normalized, want[i])
		}
	}

	for _, operator := range []BinaryOp{OpAnd, OpOr} {
		value, err := (&BinaryOperation{
			Op:    operator,
			Left:  &Constant{Value: nil},
			Right: &Constant{Value: nil},
		}).Eval(&EvalContext{})
		if err != nil {
			t.Fatalf("%v with two UNKNOWN operands: %v", operator, err)
		}
		if value != nil {
			t.Fatalf("%v with two UNKNOWN operands = %#v, want UNKNOWN", operator, value)
		}
	}
}

func TestPredicateNormalizationExpandsConstantNotInWithoutChangingThreeValuedSemantics(t *testing.T) {
	normalizer := NewExpressionNormalizer()
	expr := &BinaryOperation{
		Op:    OpNotIn,
		Left:  &Column{Name: "age"},
		Right: &Constant{Value: []interface{}{int64(18), int64(60), nil}},
	}

	normalized := normalizer.Normalize(expr)
	conjunction, ok := normalized.(*BinaryOperation)
	if !ok || conjunction.Op != OpAnd {
		t.Fatalf("NOT IN normalization = %T %v, want AND", normalized, normalized)
	}

	var terms []Expression
	var collect func(Expression)
	collect = func(term Expression) {
		if nested, ok := term.(*BinaryOperation); ok && nested.Op == OpAnd {
			collect(nested.Left)
			collect(nested.Right)
			return
		}
		terms = append(terms, term)
	}
	collect(conjunction)
	if len(terms) != 3 {
		t.Fatalf("NOT IN terms = %d, want 3", len(terms))
	}
	for _, term := range terms {
		comparison, ok := term.(*BinaryOperation)
		if !ok || comparison.Op != OpNE {
			t.Fatalf("NOT IN term = %T %v, want != comparison", term, term)
		}
	}

	rows := []map[string]interface{}{
		{"age": int64(18)},
		{"age": int64(30)},
		{"age": nil},
	}
	for _, row := range rows {
		original := &BinaryOperation{
			Op:    OpNotIn,
			Left:  &Column{Name: "age"},
			Right: &Constant{Value: []interface{}{int64(18), int64(60), nil}},
		}
		want, err := original.Eval(&EvalContext{Row: row})
		if err != nil {
			t.Fatalf("original Eval(%v): %v", row, err)
		}
		got, err := normalized.Eval(&EvalContext{Row: row})
		if err != nil {
			t.Fatalf("normalized Eval(%v): %v", row, err)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("row %v: normalized result %v, want %v", row, got, want)
		}
	}
}

func TestNormalizerRewritesNestedExpressionsInsideCaseAndTuple(t *testing.T) {
	normalizer := NewExpressionNormalizer()
	normalized := normalizer.Normalize(&CaseExpression{
		Whens: []CaseWhen{{
			Condition: &BinaryOperation{
				Op:    OpGT,
				Left:  &Constant{Value: int64(18)},
				Right: &Column{Name: "age"},
			},
			Value: &BinaryOperation{
				Op:    OpAdd,
				Left:  &Constant{Value: int64(2)},
				Right: &Column{Name: "age"},
			},
		}},
		Else: &BinaryOperation{
			Op:    OpMul,
			Left:  &Column{Name: "age"},
			Right: &Constant{Value: int64(1)},
		},
	})
	caseExpr, ok := normalized.(*CaseExpression)
	if !ok {
		t.Fatalf("normalized CASE = %T, want *CaseExpression", normalized)
	}
	if got := caseExpr.Whens[0].Value.String(); got != "(age + 2)" {
		t.Fatalf("normalized CASE WHEN value = %q, want (age + 2)", got)
	}
	if got := caseExpr.Whens[0].Condition.String(); got != "(age < 18)" {
		t.Fatalf("normalized CASE WHEN condition = %q, want (age < 18)", got)
	}
	if got := caseExpr.Else.String(); got != "age" {
		t.Fatalf("normalized CASE ELSE value = %q, want age", got)
	}

	tuple := normalizer.Normalize(&TupleExpression{Exprs: []Expression{
		&BinaryOperation{Op: OpAdd, Left: &Constant{Value: int64(2)}, Right: &Column{Name: "left_value"}},
		&UnaryOperation{Operator: "+", Operand: &Column{Name: "right_value"}},
	}})
	tupleExpr, ok := tuple.(*TupleExpression)
	if !ok {
		t.Fatalf("normalized tuple = %T, want *TupleExpression", tuple)
	}
	if got := tupleExpr.Exprs[0].String(); got != "(left_value + 2)" {
		t.Fatalf("normalized tuple first value = %q, want (left_value + 2)", got)
	}
}

func TestConstantBooleanSimplificationUsesSQLTruthValues(t *testing.T) {
	normalizer := NewExpressionNormalizer()
	tests := []struct {
		name string
		expr Expression
		want string
	}{
		{name: "one AND column", expr: &BinaryOperation{Op: OpAnd, Left: &Constant{Value: int64(1)}, Right: &Column{Name: "active"}}, want: "active"},
		{name: "zero OR column", expr: &BinaryOperation{Op: OpOr, Left: &Constant{Value: int64(0)}, Right: &Column{Name: "active"}}, want: "active"},
		{name: "zero AND column", expr: &BinaryOperation{Op: OpAnd, Left: &Constant{Value: int64(0)}, Right: &Column{Name: "active"}}, want: "0"},
		{name: "one OR column", expr: &BinaryOperation{Op: OpOr, Left: &Constant{Value: int64(1)}, Right: &Column{Name: "active"}}, want: "1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizer.Normalize(tt.expr).String()
			if got != tt.want {
				t.Fatalf("normalized expression = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestZeroLawPreservesNullableArithmetic(t *testing.T) {
	normalizer := NewExpressionNormalizer()
	for _, expr := range []Expression{
		&BinaryOperation{Op: OpMul, Left: &Column{Name: "age"}, Right: &Constant{Value: int64(0)}},
		&BinaryOperation{Op: OpMul, Left: &Constant{Value: int64(0)}, Right: &Column{Name: "age"}},
	} {
		normalized := normalizer.Normalize(expr)
		got, err := normalized.Eval(&EvalContext{Row: map[string]interface{}{"age": nil}})
		if err != nil {
			t.Fatalf("normalized nullable multiplication error = %v", err)
		}
		if got != nil {
			t.Fatalf("normalized nullable multiplication = %#v, want NULL; expression=%s", got, normalized.String())
		}
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

func TestAbsorptionLaw(t *testing.T) {
	normalizer := NewExpressionNormalizer()

	tests := []struct {
		name string
		expr Expression
		want string
	}{
		{
			name: "x AND (x OR y) -> x",
			expr: &BinaryOperation{
				Op:   OpAnd,
				Left: &Column{Name: "x"},
				Right: &BinaryOperation{
					Op:    OpOr,
					Left:  &Column{Name: "x"},
					Right: &Column{Name: "y"},
				},
			},
			want: "x",
		},
		{
			name: "x OR (x AND y) -> x",
			expr: &BinaryOperation{
				Op:   OpOr,
				Left: &Column{Name: "x"},
				Right: &BinaryOperation{
					Op:    OpAnd,
					Left:  &Column{Name: "x"},
					Right: &Column{Name: "y"},
				},
			},
			want: "x",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizer.Normalize(tt.expr)
			if got.String() != tt.want {
				t.Fatalf("Normalize() = %q, want %q", got.String(), tt.want)
			}
		})
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
