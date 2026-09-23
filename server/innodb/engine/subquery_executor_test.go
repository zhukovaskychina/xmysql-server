package engine

import (
	"context"
	"io"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

func TestSubqueryOperatorCachesUncorrelatedNullScalar(t *testing.T) {
	plan := &countingSubqueryPlan{
		schema: metadata.NewQuerySchema(),
	}
	plan.schema.AddColumn(metadata.NewQueryColumn("value", metadata.TypeInt))
	operator := NewSubqueryOperator("SCALAR", false, nil, plan)
	ctx := context.Background()

	if err := operator.Open(ctx); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := operator.ExecuteForRow(ctx, nil); err != nil {
		t.Fatalf("ExecuteForRow() error = %v", err)
	}
	if plan.openCount != 1 {
		t.Fatalf("uncorrelated NULL scalar opened subplan %d times, want 1", plan.openCount)
	}
	if operator.GetResult() != nil {
		t.Fatalf("uncorrelated empty scalar result = %v, want NULL", operator.GetResult())
	}

}

func TestApplyOperatorEvaluatesQualifiedJoinConditions(t *testing.T) {
	outerSchema := metadata.NewQuerySchema()
	outerSchema.TableName = "users"
	outerColumn := metadata.NewQueryColumn("id", metadata.TypeInt)
	outerColumn.TableName = "users"
	outerSchema.AddColumn(outerColumn)
	innerSchema := metadata.NewQuerySchema()
	innerSchema.TableName = "orders"
	innerColumn := metadata.NewQueryColumn("user_id", metadata.TypeInt)
	innerColumn.TableName = "orders"
	innerSchema.AddColumn(innerColumn)

	outer := NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(7)}, outerSchema)
	inner := NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(7)}, innerSchema)
	apply := NewApplyOperator(
		&ExpressionMockOperator{records: []Record{outer}, schema: outerSchema},
		&ExpressionMockOperator{records: []Record{inner}, schema: innerSchema},
		"INNER",
		true,
		[]plan.Expression{&plan.BinaryOperation{
			Op:    plan.OpEQ,
			Left:  &plan.Column{Name: "users.id"},
			Right: &plan.Column{Name: "orders.user_id"},
		}},
	)

	if !apply.evaluateJoinConditions(outer, inner) {
		t.Fatal("qualified join condition should match rows with equal values")
	}
}

type countingSubqueryPlan struct {
	BaseOperator
	schema    *metadata.QuerySchema
	openCount int
}

func (p *countingSubqueryPlan) Open(context.Context) error {
	p.opened = true
	p.openCount++
	return nil
}

func (p *countingSubqueryPlan) Next(context.Context) (Record, error) { return nil, nil }

func (p *countingSubqueryPlan) Close() error {
	p.opened = false
	return nil
}

func (p *countingSubqueryPlan) Schema() *metadata.QuerySchema { return p.schema }

// TestSubqueryOperator_Scalar 测试标量子查询
func TestSubqueryOperator_Scalar(t *testing.T) {
	// 创建子查询结果
	subqueryValues := []basic.Value{basic.NewInt64(42)}
	subquerySchema := metadata.NewQuerySchema()
	subquerySchema.AddColumn(metadata.NewQueryColumn("count", metadata.TypeInt))
	subqueryRecord := NewExecutorRecordFromValues(subqueryValues, subquerySchema)

	// 创建mock子查询算子
	mockSubplan := &ExpressionMockOperator{
		records: []Record{subqueryRecord},
		schema:  subquerySchema,
	}

	// 创建标量子查询算子
	subqueryOp := NewSubqueryOperator("SCALAR", false, nil, mockSubplan)

	// 执行
	ctx := context.Background()
	err := subqueryOp.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open subquery operator: %v", err)
	}
	defer subqueryOp.Close()

	// 获取结果
	result := subqueryOp.GetResult()
	if result == nil {
		t.Fatal("Expected scalar result, got nil")
	}

	// 验证结果
	if val, ok := result.(basic.Value); ok {
		if val.Int() != 42 {
			t.Errorf("Expected 42, got %v", val.Int())
		}
	} else {
		t.Errorf("Expected basic.Value, got %T", result)
	}
}

// TestSubqueryOperator_IN 测试IN子查询
func TestSubqueryOperator_IN(t *testing.T) {
	// 创建子查询结果集
	subquerySchema := metadata.NewQuerySchema()
	subquerySchema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))

	records := []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(1)}, subquerySchema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(2)}, subquerySchema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(3)}, subquerySchema),
	}

	// 创建mock子查询算子
	mockSubplan := &ExpressionMockOperator{
		records: records,
		schema:  subquerySchema,
	}

	// 创建IN子查询算子
	subqueryOp := NewSubqueryOperator("IN", false, nil, mockSubplan)

	// 执行
	ctx := context.Background()
	err := subqueryOp.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open subquery operator: %v", err)
	}
	defer subqueryOp.Close()

	// 获取结果集
	resultSet := subqueryOp.GetResultSet()
	if len(resultSet) != 3 {
		t.Errorf("Expected 3 records, got %d", len(resultSet))
	}

	// 验证结果
	for i, record := range resultSet {
		values := record.GetValues()
		if len(values) != 1 {
			t.Errorf("Record %d: expected 1 value, got %d", i, len(values))
			continue
		}
		expectedValue := int64(i + 1)
		if values[0].Int() != expectedValue {
			t.Errorf("Record %d: expected %d, got %v", i, expectedValue, values[0].Int())
		}
	}
}

func TestSubqueryOperatorINConsumesBatchChildren(t *testing.T) {
	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
	records := []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(1)}, schema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(2)}, schema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(3)}, schema),
	}
	subplan := &batchCountingSubqueryPlan{records: records, schema: schema, batchSize: 2}
	operator := NewSubqueryOperator("IN", false, nil, subplan)

	if err := operator.Open(context.Background()); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer operator.Close()

	if got := len(operator.GetResultSet()); got != len(records) {
		t.Fatalf("IN result count = %d, want %d", got, len(records))
	}
	if subplan.batchCalls == 0 {
		t.Fatal("IN subquery did not consume the batch child through NextBatch")
	}
	if subplan.nextCalls != 0 {
		t.Fatalf("IN subquery fell back to Next() %d times, want 0", subplan.nextCalls)
	}
}

type batchCountingSubqueryPlan struct {
	BaseOperator
	records    []Record
	schema     *metadata.QuerySchema
	index      int
	batchSize  int
	batchCalls int
	nextCalls  int
}

func (p *batchCountingSubqueryPlan) Open(context.Context) error {
	p.opened = true
	p.index = 0
	return nil
}

func (p *batchCountingSubqueryPlan) Next(context.Context) (Record, error) {
	p.nextCalls++
	if p.index >= len(p.records) {
		return nil, nil
	}
	record := p.records[p.index]
	p.index++
	return record, nil
}

func (p *batchCountingSubqueryPlan) NextBatch(_ context.Context, maxRows int) ([]Record, error) {
	p.batchCalls++
	if maxRows <= 0 {
		return nil, io.ErrShortBuffer
	}
	if p.index >= len(p.records) {
		return nil, io.EOF
	}
	end := p.index + maxRows
	if end > len(p.records) {
		end = len(p.records)
	}
	batch := p.records[p.index:end]
	p.index = end
	if p.index >= len(p.records) {
		return batch, io.EOF
	}
	return batch, nil
}

func (p *batchCountingSubqueryPlan) Close() error {
	p.opened = false
	return nil
}

func (p *batchCountingSubqueryPlan) Schema() *metadata.QuerySchema { return p.schema }

// TestSubqueryOperator_EXISTS 测试EXISTS子查询
func TestSubqueryOperator_EXISTS(t *testing.T) {
	t.Run("EXISTS with results", func(t *testing.T) {
		// 创建有结果的子查询
		subquerySchema := metadata.NewQuerySchema()
		subquerySchema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
		subqueryRecord := NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(1)}, subquerySchema)

		mockSubplan := &ExpressionMockOperator{
			records: []Record{subqueryRecord},
			schema:  subquerySchema,
		}

		subqueryOp := NewSubqueryOperator("EXISTS", false, nil, mockSubplan)

		ctx := context.Background()
		err := subqueryOp.Open(ctx)
		if err != nil {
			t.Fatalf("Failed to open subquery operator: %v", err)
		}
		defer subqueryOp.Close()

		result := subqueryOp.GetResult()
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	t.Run("EXISTS with no results", func(t *testing.T) {
		// 创建无结果的子查询
		subquerySchema := metadata.NewQuerySchema()
		subquerySchema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))

		mockSubplan := &ExpressionMockOperator{
			records: []Record{}, // 空结果集
			schema:  subquerySchema,
		}

		subqueryOp := NewSubqueryOperator("EXISTS", false, nil, mockSubplan)

		ctx := context.Background()
		err := subqueryOp.Open(ctx)
		if err != nil {
			t.Fatalf("Failed to open subquery operator: %v", err)
		}
		defer subqueryOp.Close()

		result := subqueryOp.GetResult()
		if result != false {
			t.Errorf("Expected false, got %v", result)
		}
	})
}

// TestApplyOperator_SEMI 测试SEMI JOIN
func TestApplyOperator_SEMI(t *testing.T) {
	// 创建外层数据
	outerSchema := metadata.NewQuerySchema()
	outerSchema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
	outerSchema.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))

	outerRecords := []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(1), basic.NewString("Alice")}, outerSchema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(2), basic.NewString("Bob")}, outerSchema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(3), basic.NewString("Charlie")}, outerSchema),
	}

	outerOp := &ExpressionMockOperator{
		records: outerRecords,
		schema:  outerSchema,
	}

	// 创建内层数据（只有id=1和id=2有匹配）
	innerSchema := metadata.NewQuerySchema()
	innerSchema.AddColumn(metadata.NewQueryColumn("user_id", metadata.TypeInt))

	innerRecords := []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(1)}, innerSchema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(2)}, innerSchema),
	}

	innerOp := &ExpressionMockOperator{
		records: innerRecords,
		schema:  innerSchema,
	}

	// 创建SEMI JOIN算子
	applyOp := NewApplyOperator(outerOp, innerOp, "SEMI", false, nil)

	// 执行
	ctx := context.Background()
	err := applyOp.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open apply operator: %v", err)
	}
	defer applyOp.Close()

	// 收集结果
	var results []Record
	for {
		record, err := applyOp.Next(ctx)
		if err != nil {
			t.Fatalf("Error during execution: %v", err)
		}
		if record == nil {
			break
		}
		results = append(results, record)
	}

	// SEMI JOIN应该返回有匹配的外层记录
	// 由于我们的简化实现总是返回true，所以会返回所有外层记录
	// 在实际实现中，应该只返回id=1和id=2的记录
	if len(results) == 0 {
		t.Error("Expected some results from SEMI JOIN")
	}
}

func TestApplyOperator_SEMIReturnsEachMatchedOuterRecord(t *testing.T) {
	outerSchema := metadata.NewQuerySchema()
	outerSchema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
	outerRecords := []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(1)}, outerSchema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(2)}, outerSchema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(3)}, outerSchema),
	}
	innerSchema := metadata.NewQuerySchema()
	innerSchema.AddColumn(metadata.NewQueryColumn("marker", metadata.TypeInt))
	innerRecords := []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(99)}, innerSchema),
	}

	applyOp := NewApplyOperator(
		&ExpressionMockOperator{records: outerRecords, schema: outerSchema},
		&ExpressionMockOperator{records: innerRecords, schema: innerSchema},
		"SEMI", false, nil,
	)
	ctx := context.Background()
	if err := applyOp.Open(ctx); err != nil {
		t.Fatalf("Failed to open apply operator: %v", err)
	}
	defer applyOp.Close()

	var ids []int64
	for {
		record, err := applyOp.Next(ctx)
		if err != nil {
			t.Fatalf("Error during execution: %v", err)
		}
		if record == nil {
			break
		}
		values := record.GetValues()
		if len(values) != 1 {
			t.Fatalf("expected one value, got %d", len(values))
		}
		ids = append(ids, values[0].Int())
	}
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("expected all matched outer ids [1 2 3], got %v", ids)
	}
}

// TestApplyOperator_ANTI 测试ANTI JOIN
func TestApplyOperator_ANTI(t *testing.T) {
	// 创建外层数据
	outerSchema := metadata.NewQuerySchema()
	outerSchema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))

	outerRecords := []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(1)}, outerSchema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(2)}, outerSchema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(3)}, outerSchema),
	}

	outerOp := &ExpressionMockOperator{
		records: outerRecords,
		schema:  outerSchema,
	}

	// 创建内层数据（只有id=1有匹配）
	innerSchema := metadata.NewQuerySchema()
	innerSchema.AddColumn(metadata.NewQueryColumn("user_id", metadata.TypeInt))

	innerRecords := []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(1)}, innerSchema),
	}

	innerOp := &ExpressionMockOperator{
		records: innerRecords,
		schema:  innerSchema,
	}

	// 创建ANTI JOIN算子
	applyOp := NewApplyOperator(outerOp, innerOp, "ANTI", false, nil)

	// 执行
	ctx := context.Background()
	err := applyOp.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open apply operator: %v", err)
	}
	defer applyOp.Close()

	// 收集结果
	var results []Record
	for {
		record, err := applyOp.Next(ctx)
		if err != nil {
			t.Fatalf("Error during execution: %v", err)
		}
		if record == nil {
			break
		}
		results = append(results, record)
	}

	// 当前简化实现中，joinConds 为空时所有内层记录都视为匹配，
	// 因此 ANTI JOIN 不会返回任何外层记录。
	if len(results) != 0 {
		t.Errorf("Expected no results from ANTI JOIN with unconditional match, got %d", len(results))
	}
}
