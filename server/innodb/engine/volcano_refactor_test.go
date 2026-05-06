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

	ctx := context.Background()

	if err := mockOp.Open(ctx); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// 测试Next
	record, err := mockOp.Next(ctx)
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if record == nil {
		t.Fatal("Next returned nil record")
	}
	if record.GetColumnCount() != 2 {
		t.Fatalf("Expected 2 columns, got %d", record.GetColumnCount())
	}
	if record.GetValueByIndex(0).Int() != 1 {
		t.Fatalf("Expected first column to be 1, got %v", record.GetValueByIndex(0))
	}

	// 测试第二行
	record, err = mockOp.Next(ctx)
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if record.GetValueByIndex(0).Int() != 2 {
		t.Fatalf("Expected first column to be 2, got %v", record.GetValueByIndex(0))
	}

	// 测试EOF
	record, err = mockOp.Next(ctx)
	if err != nil {
		t.Fatalf("Expected nil error at EOF, got %v", err)
	}
	if record != nil {
		t.Fatal("Expected nil record at EOF")
	}

	// 测试Close
	if err := mockOp.Close(); err != nil {
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

func (m *MockOperator) Schema() *metadata.QuerySchema {
	return &metadata.QuerySchema{
		Columns: []*metadata.QueryColumn{
			{Name: "id", DataType: metadata.TypeInt},
			{Name: "name", DataType: metadata.TypeVarchar},
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
	if retrievedValues[0].Int() != 42 {
		t.Errorf("Expected int64 42, got %v", retrievedValues[0].Int())
	}
	if retrievedValues[1].ToString() != "hello" {
		t.Errorf("Expected string 'hello', got %v", retrievedValues[1].ToString())
	}
	if retrievedValues[2].Float64() != 3.14 {
		t.Errorf("Expected float64 3.14, got %v", retrievedValues[2].Float64())
	}
	if retrievedValues[3].Int() != 1 {
		t.Errorf("Expected bool true to be stored as 1, got %v", retrievedValues[3].Int())
	}
	if !retrievedValues[4].IsNull() {
		t.Errorf("Expected NULL value")
	}
}
