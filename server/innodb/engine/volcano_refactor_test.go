package engine

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TestOperatorToExecutorAdapter 测试适配器功能
func TestOperatorToExecutorAdapter(t *testing.T) {
	// 创建一个模拟的Operator
	mockOp := &MockOperator{
		records: []Record{
			NewExecutorRecordFromValues(
				[]basic.Value{basic.NewInt64(1), basic.NewString("test1")},
				nil,
			),
			NewExecutorRecordFromValues(
				[]basic.Value{basic.NewInt64(2), basic.NewString("test2")},
				nil,
			),
		},
		index: 0,
	}

	// 创建执行上下文
	execCtx := &ExecutionContext{
		Context: context.Background(),
	}

	// 创建适配器
	adapter := NewOperatorToExecutorAdapter(mockOp, execCtx)

	// 测试Init
	if err := adapter.Init(); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// 测试Next和GetRow
	err := adapter.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	row := adapter.GetRow()
	if row == nil {
		t.Fatal("GetRow returned nil")
	}
	if len(row) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(row))
	}
	if row[0].(int64) != 1 {
		t.Fatalf("Expected first column to be 1, got %v", row[0])
	}

	// 测试第二行
	err = adapter.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	row = adapter.GetRow()
	if row[0].(int64) != 2 {
		t.Fatalf("Expected first column to be 2, got %v", row[0])
	}

	// 测试EOF
	err = adapter.Next()
	if err == nil {
		t.Fatal("Expected EOF error")
	}

	// 测试Close
	if err := adapter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// MockOperator 模拟算子用于测试
type MockOperator struct {
	BaseOperator
	records []Record
	index   int
	opened  bool
}

func (m *MockOperator) Open(ctx context.Context) error {
	m.opened = true
	m.index = 0
	return nil
}

func (m *MockOperator) Next(ctx context.Context) (Record, error) {
	if !m.opened {
		return nil, nil
	}
	if m.index >= len(m.records) {
		return nil, nil // EOF
	}
	record := m.records[m.index]
	m.index++
	return record, nil
}

func (m *MockOperator) Close() error {
	m.opened = false
	return nil
}

func (m *MockOperator) Schema() *metadata.Schema {
	return &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "id", DataType: "INT"},
			{Name: "name", DataType: "VARCHAR"},
		},
	}
}

// TestVolcanoExecutorBuild 测试VolcanoExecutor构建
func TestVolcanoExecutorBuild(t *testing.T) {
	// 由于需要实际的管理器实例,这个测试暂时跳过
	t.Skip("Skipping integration test - requires actual manager instances")
}

// TestRecordConversion 测试Record转换
func TestRecordConversion(t *testing.T) {
	values := []basic.Value{
		basic.NewInt64(42),
		basic.NewString("hello"),
		basic.NewFloat64(3.14),
		basic.NewBool(true),
		basic.NewNull(),
	}

	record := NewExecutorRecordFromValues(values, nil)

	// 验证GetValues
	retrievedValues := record.GetValues()
	if len(retrievedValues) != len(values) {
		t.Fatalf("Expected %d values, got %d", len(values), len(retrievedValues))
	}

	// 验证每个值
	if retrievedValues[0].ToInt64() != 42 {
		t.Errorf("Expected int64 42, got %v", retrievedValues[0].ToInt64())
	}
	if retrievedValues[1].ToString() != "hello" {
		t.Errorf("Expected string 'hello', got %v", retrievedValues[1].ToString())
	}
	if retrievedValues[2].ToFloat64() != 3.14 {
		t.Errorf("Expected float64 3.14, got %v", retrievedValues[2].ToFloat64())
	}
	if !retrievedValues[3].ToBool() {
		t.Errorf("Expected bool true")
	}
	if !retrievedValues[4].IsNull() {
		t.Errorf("Expected NULL value")
	}
}
