package plan

import (
	"testing"
	"time"
)

func TestConstantExpression(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		wantType DataType
	}{
		{"Int", int64(42), TypeInt},
		{"Float", 3.14, TypeFloat},
		{"String", "hello", TypeString},
		{"Bool", true, TypeBoolean},
		{"Null", nil, TypeNull},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &Constant{
				BaseExpression: BaseExpression{resultType: tt.wantType},
				Value:          tt.value,
			}

			// Test type
			if got := expr.GetType(); got != tt.wantType {
				t.Errorf("Constant.GetType() = %v, want %v", got, tt.wantType)
			}

			// Test evaluation
			ctx := &EvalContext{}
			got, err := expr.Eval(ctx)
			if err != nil {
				t.Errorf("Constant.Eval() error = %v", err)
				return
			}
			if got != tt.value {
				t.Errorf("Constant.Eval() = %v, want %v", got, tt.value)
			}
		})
	}
}

func TestColumnExpression(t *testing.T) {
	tests := []struct {
		name    string
		col     string
		row     map[string]interface{}
		want    interface{}
		wantErr bool
	}{
		{
			name: "ExistingColumn",
			col:  "id",
			row:  map[string]interface{}{"id": int64(1)},
			want: int64(1),
		},
		{
			name:    "NonExistingColumn",
			col:     "unknown",
			row:     map[string]interface{}{"id": int64(1)},
			wantErr: true,
		},
		{
			name: "NullValue",
			col:  "name",
			row:  map[string]interface{}{"name": nil},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &Column{
				BaseExpression: BaseExpression{},
				Name:           tt.col,
			}

			ctx := &EvalContext{Row: tt.row}
			got, err := expr.Eval(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Column.Eval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Column.Eval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryOperation(t *testing.T) {
	tests := []struct {
		name    string
		op      BinaryOp
		left    interface{}
		right   interface{}
		want    interface{}
		wantErr bool
	}{
		// 算术运算
		{"Add_Int", OpAdd, int64(1), int64(2), int64(3), false},
		{"Add_Float", OpAdd, float64(1.5), float64(2.5), float64(4.0), false},
		{"Sub_Int", OpSub, int64(5), int64(3), int64(2), false},
		{"Mul_Int", OpMul, int64(4), int64(3), int64(12), false},
		{"Div_Int", OpDiv, int64(10), int64(2), int64(5), false},
		{"Div_Zero", OpDiv, int64(1), int64(0), nil, true},

		// 比较运算
		{"EQ_Int", OpEQ, int64(1), int64(1), true, false},
		{"NE_Int", OpNE, int64(1), int64(2), true, false},
		{"LT_Int", OpLT, int64(1), int64(2), true, false},
		{"LE_Int", OpLE, int64(2), int64(2), true, false},
		{"GT_Int", OpGT, int64(3), int64(2), true, false},
		{"GE_Int", OpGE, int64(2), int64(2), true, false},

		// 逻辑运算
		{"And_True", OpAnd, true, true, true, false},
		{"And_False", OpAnd, true, false, false, false},
		{"Or_True", OpOr, true, false, true, false},
		{"Or_False", OpOr, false, false, false, false},

		// 字符串运算
		{"Like_Match", OpLike, "hello", "%ell%", true, false},
		{"Like_NoMatch", OpLike, "hello", "%xyz%", false, false},

		// NULL处理
		{"Add_Null", OpAdd, nil, int64(1), nil, false},
		{"EQ_Null", OpEQ, nil, nil, true, false},
		{"LT_Null", OpLT, nil, int64(1), nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left := &Constant{Value: tt.left}
			right := &Constant{Value: tt.right}
			expr := &BinaryOperation{
				BaseExpression: BaseExpression{},
				Op:             tt.op,
				Left:           left,
				Right:          right,
			}

			ctx := &EvalContext{}
			got, err := expr.Eval(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("BinaryOperation.Eval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("BinaryOperation.Eval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFunction(t *testing.T) {
	tests := []struct {
		name    string
		fn      string
		args    []interface{}
		want    interface{}
		wantErr bool
	}{
		// COUNT
		{"Count_List", "COUNT", []interface{}{[]interface{}{1, nil, 3}}, int64(2), false},
		{"Count_Null", "COUNT", []interface{}{nil}, int64(0), false},
		{"Count_Single", "COUNT", []interface{}{42}, int64(1), false},

		// SUM
		{"Sum_List", "SUM", []interface{}{[]interface{}{int64(1), int64(2), int64(3)}}, float64(6), false},
		{"Sum_Null", "SUM", []interface{}{nil}, nil, false},
		{"Sum_Mixed", "SUM", []interface{}{[]interface{}{int64(1), nil, int64(3)}}, float64(4), false},

		// AVG
		{"Avg_List", "AVG", []interface{}{[]interface{}{int64(1), int64(2), int64(3)}}, float64(2), false},
		{"Avg_Null", "AVG", []interface{}{nil}, nil, false},
		{"Avg_Mixed", "AVG", []interface{}{[]interface{}{int64(1), nil, int64(3)}}, float64(2), false},

		// MAX
		{"Max_List", "MAX", []interface{}{[]interface{}{int64(1), int64(3), int64(2)}}, int64(3), false},
		{"Max_Null", "MAX", []interface{}{nil}, nil, false},
		{"Max_Mixed", "MAX", []interface{}{[]interface{}{int64(1), nil, int64(3)}}, int64(3), false},

		// MIN
		{"Min_List", "MIN", []interface{}{[]interface{}{int64(2), int64(1), int64(3)}}, int64(1), false},
		{"Min_Null", "MIN", []interface{}{nil}, nil, false},
		{"Min_Mixed", "MIN", []interface{}{[]interface{}{int64(2), nil, int64(1)}}, int64(1), false},

		// CONCAT
		{"Concat_Strings", "CONCAT", []interface{}{"Hello", " ", "World"}, "Hello World", false},
		{"Concat_Mixed", "CONCAT", []interface{}{"Value: ", int64(42)}, "Value: 42", false},
		{"Concat_Null", "CONCAT", []interface{}{"Hello", nil, "World"}, nil, false},

		// SUBSTRING
		{"Substring_Basic", "SUBSTRING", []interface{}{"Hello", int64(1), int64(2)}, "He", false},
		{"Substring_From", "SUBSTRING", []interface{}{"Hello", int64(2)}, "ello", false},
		{"Substring_Invalid", "SUBSTRING", []interface{}{"Hello", int64(10)}, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := make([]Expression, len(tt.args))
			for i, arg := range tt.args {
				args[i] = &Constant{Value: arg}
			}

			expr := &Function{
				BaseExpression: BaseExpression{},
				Name:           tt.fn,
				Args:           args,
			}

			ctx := &EvalContext{}
			got, err := expr.Eval(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Function.Eval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Function.Eval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpressionString(t *testing.T) {
	tests := []struct {
		name string
		expr Expression
		want string
	}{
		{
			name: "Constant",
			expr: &Constant{Value: 42},
			want: "42",
		},
		{
			name: "Column",
			expr: &Column{Name: "id"},
			want: "id",
		},
		{
			name: "BinaryOperation",
			expr: &BinaryOperation{
				Op:    OpAdd,
				Left:  &Constant{Value: 1},
				Right: &Constant{Value: 2},
			},
			want: "(1 + 2)",
		},
		{
			name: "Function",
			expr: &Function{
				Name: "COUNT",
				Args: []Expression{&Column{Name: "id"}},
			},
			want: "COUNT(id)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.expr.String(); got != tt.want {
				t.Errorf("Expression.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
