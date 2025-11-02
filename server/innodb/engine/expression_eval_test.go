package engine

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

// TestExpressionEvaluation_Constant 测试常量表达式求值
func TestExpressionEvaluation_Constant(t *testing.T) {
	// 创建测试数据
	values := []basic.Value{
		basic.NewInt64(100),
		basic.NewString("test"),
	}

	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
	schema.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))

	record := NewExecutorRecordFromValues(values, schema)

	// 创建mock子算子
	mockChild := &ExpressionMockOperator{
		records: []Record{record},
		schema:  schema,
	}

	// 创建常量表达式
	exprs := []plan.Expression{
		&plan.Constant{Value: int64(42)},
		&plan.Constant{Value: "hello"},
	}

	// 创建投影算子
	projection := NewProjectionOperatorWithExprs(mockChild, exprs)

	// 执行
	ctx := context.Background()
	err := projection.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open projection: %v", err)
	}

	result, err := projection.Next(ctx)
	if err != nil {
		t.Fatalf("Failed to get next record: %v", err)
	}

	if result == nil {
		t.Fatal("Expected record, got nil")
	}

	// 验证结果
	resultValues := result.GetValues()
	if len(resultValues) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(resultValues))
	}

	if resultValues[0].Int() != 42 {
		t.Errorf("Expected 42, got %v", resultValues[0].Int())
	}

	if resultValues[1].String() != "hello" {
		t.Errorf("Expected 'hello', got %v", resultValues[1].String())
	}
}

// TestExpressionEvaluation_Column 测试列引用表达式求值
func TestExpressionEvaluation_Column(t *testing.T) {
	// 创建测试数据
	values := []basic.Value{
		basic.NewInt64(100),
		basic.NewString("test"),
	}

	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
	schema.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))

	record := NewExecutorRecordFromValues(values, schema)

	// 创建mock子算子
	mockChild := &ExpressionMockOperator{
		records: []Record{record},
		schema:  schema,
	}

	// 创建列引用表达式
	exprs := []plan.Expression{
		&plan.Column{Name: "id"},
		&plan.Column{Name: "name"},
	}

	// 创建投影算子
	projection := NewProjectionOperatorWithExprs(mockChild, exprs)

	// 执行
	ctx := context.Background()
	err := projection.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open projection: %v", err)
	}

	result, err := projection.Next(ctx)
	if err != nil {
		t.Fatalf("Failed to get next record: %v", err)
	}

	if result == nil {
		t.Fatal("Expected record, got nil")
	}

	// 验证结果
	resultValues := result.GetValues()
	if len(resultValues) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(resultValues))
	}

	if resultValues[0].Int() != 100 {
		t.Errorf("Expected 100, got %v", resultValues[0].Int())
	}

	if resultValues[1].String() != "test" {
		t.Errorf("Expected 'test', got %v", resultValues[1].String())
	}
}

// TestExpressionEvaluation_BinaryOp 测试二元运算表达式求值
func TestExpressionEvaluation_BinaryOp(t *testing.T) {
	// 创建测试数据
	values := []basic.Value{
		basic.NewInt64(10),
		basic.NewInt64(20),
	}

	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("a", metadata.TypeInt))
	schema.AddColumn(metadata.NewQueryColumn("b", metadata.TypeInt))

	record := NewExecutorRecordFromValues(values, schema)

	// 创建mock子算子
	mockChild := &ExpressionMockOperator{
		records: []Record{record},
		schema:  schema,
	}

	// 创建二元运算表达式: a + b
	exprs := []plan.Expression{
		&plan.BinaryOperation{
			Op:    plan.OpAdd,
			Left:  &plan.Column{Name: "a"},
			Right: &plan.Column{Name: "b"},
		},
	}

	// 创建投影算子
	projection := NewProjectionOperatorWithExprs(mockChild, exprs)

	// 执行
	ctx := context.Background()
	err := projection.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open projection: %v", err)
	}

	result, err := projection.Next(ctx)
	if err != nil {
		t.Fatalf("Failed to get next record: %v", err)
	}

	if result == nil {
		t.Fatal("Expected record, got nil")
	}

	// 验证结果
	resultValues := result.GetValues()
	if len(resultValues) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(resultValues))
	}

	if resultValues[0].Int() != 30 {
		t.Errorf("Expected 30 (10+20), got %v", resultValues[0].Int())
	}
}

// TestExpressionEvaluation_Function 测试函数表达式求值
func TestExpressionEvaluation_Function(t *testing.T) {
	// 创建测试数据
	values := []basic.Value{
		basic.NewString("Hello"),
		basic.NewString("World"),
	}

	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("str1", metadata.TypeVarchar))
	schema.AddColumn(metadata.NewQueryColumn("str2", metadata.TypeVarchar))

	record := NewExecutorRecordFromValues(values, schema)

	// 创建mock子算子
	mockChild := &ExpressionMockOperator{
		records: []Record{record},
		schema:  schema,
	}

	// 创建函数表达式: CONCAT(str1, str2)
	exprs := []plan.Expression{
		&plan.Function{
			FuncName: "CONCAT",
			FuncArgs: []plan.Expression{
				&plan.Column{Name: "str1"},
				&plan.Column{Name: "str2"},
			},
		},
	}

	// 创建投影算子
	projection := NewProjectionOperatorWithExprs(mockChild, exprs)

	// 执行
	ctx := context.Background()
	err := projection.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open projection: %v", err)
	}

	result, err := projection.Next(ctx)
	if err != nil {
		t.Fatalf("Failed to get next record: %v", err)
	}

	if result == nil {
		t.Fatal("Expected record, got nil")
	}

	// 验证结果
	resultValues := result.GetValues()
	if len(resultValues) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(resultValues))
	}

	if resultValues[0].String() != "HelloWorld" {
		t.Errorf("Expected 'HelloWorld', got %v", resultValues[0].String())
	}
}

// ExpressionMockOperator 用于表达式测试的mock算子
type ExpressionMockOperator struct {
	BaseOperator
	records []Record
	schema  *metadata.QuerySchema
	index   int
}

func (m *ExpressionMockOperator) Open(ctx context.Context) error {
	m.opened = true
	m.index = 0
	return nil
}

func (m *ExpressionMockOperator) Next(ctx context.Context) (Record, error) {
	if !m.opened {
		return nil, nil
	}
	if m.index >= len(m.records) {
		return nil, nil
	}
	record := m.records[m.index]
	m.index++
	return record, nil
}

func (m *ExpressionMockOperator) Close() error {
	m.opened = false
	return nil
}

func (m *ExpressionMockOperator) Schema() *metadata.QuerySchema {
	return m.schema
}
