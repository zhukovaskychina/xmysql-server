package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// ========================================
// HashJoinOperator 测试
// ========================================

func TestHashJoinOperator_InnerJoin(t *testing.T) {
	ctx := context.Background()

	// 创建左表数据: (id, name)
	leftSchema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "id", Type: metadata.TypeInt},
			{Name: "name", Type: metadata.TypeVarchar},
		},
	}
	leftData := [][]basic.Value{
		{basic.NewInt64(1), basic.NewString("Alice")},
		{basic.NewInt64(2), basic.NewString("Bob")},
		{basic.NewInt64(3), basic.NewString("Charlie")},
	}
	leftOp := NewMockDataOperator(leftData, leftSchema)

	// 创建右表数据: (user_id, age)
	rightSchema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "user_id", Type: metadata.TypeInt},
			{Name: "age", Type: metadata.TypeInt},
		},
	}
	rightData := [][]basic.Value{
		{basic.NewInt64(1), basic.NewInt64(25)},
		{basic.NewInt64(2), basic.NewInt64(30)},
		{basic.NewInt64(4), basic.NewInt64(35)}, // 不匹配
	}
	rightOp := NewMockDataOperator(rightData, rightSchema)

	// 创建HashJoin算子: JOIN ON left.id = right.user_id
	buildKey := func(r Record) string {
		values := r.GetValues()
		if len(values) > 0 {
			return values[0].ToString()
		}
		return ""
	}
	probeKey := func(r Record) string {
		values := r.GetValues()
		if len(values) > 0 {
			return values[0].ToString()
		}
		return ""
	}

	hashJoin := NewHashJoinOperator(leftOp, rightOp, "INNER", buildKey, probeKey)

	// 打开算子
	err := hashJoin.Open(ctx)
	assert.NoError(t, err)

	// 读取所有结果
	results := make([]Record, 0)
	for {
		record, err := hashJoin.Next(ctx)
		assert.NoError(t, err)
		if record == nil {
			break
		}
		results = append(results, record)
	}

	// 验证结果：应该有2条匹配记录 (id=1和id=2)
	assert.Equal(t, 2, len(results))

	// 验证第一条记录: (1, Alice, 1, 25)
	values0 := results[0].GetValues()
	assert.Equal(t, 4, len(values0))
	assert.Equal(t, int64(1), values0[0].ToInt64())
	assert.Equal(t, "Alice", values0[1].ToString())
	assert.Equal(t, int64(25), values0[3].ToInt64())

	// 关闭算子
	err = hashJoin.Close()
	assert.NoError(t, err)
}

func TestHashJoinOperator_EmptyBuildSide(t *testing.T) {
	ctx := context.Background()

	// 左表为空
	leftSchema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "id", Type: metadata.TypeInt},
		},
	}
	leftOp := NewMockDataOperator([][]basic.Value{}, leftSchema)

	// 右表有数据
	rightSchema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "id", Type: metadata.TypeInt},
		},
	}
	rightData := [][]basic.Value{
		{basic.NewInt64(1)},
		{basic.NewInt64(2)},
	}
	rightOp := NewMockDataOperator(rightData, rightSchema)

	// 创建HashJoin
	keyFunc := func(r Record) string {
		values := r.GetValues()
		if len(values) > 0 {
			return values[0].ToString()
		}
		return ""
	}

	hashJoin := NewHashJoinOperator(leftOp, rightOp, "INNER", keyFunc, keyFunc)
	err := hashJoin.Open(ctx)
	assert.NoError(t, err)

	// 结果应该为空
	record, err := hashJoin.Next(ctx)
	assert.NoError(t, err)
	assert.Nil(t, record)
}

// ========================================
// HashAggregateOperator 测试
// ========================================

func TestHashAggregateOperator_Count(t *testing.T) {
	ctx := context.Background()

	// 创建测试数据: (category, amount)
	schema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "category", Type: metadata.TypeVarchar},
			{Name: "amount", Type: metadata.TypeDouble},
		},
	}
	data := [][]basic.Value{
		{basic.NewString("A"), basic.NewFloat64(10.0)},
		{basic.NewString("B"), basic.NewFloat64(20.0)},
		{basic.NewString("A"), basic.NewFloat64(15.0)},
		{basic.NewString("B"), basic.NewFloat64(25.0)},
		{basic.NewString("A"), basic.NewFloat64(12.0)},
	}
	childOp := NewMockDataOperator(data, schema)

	// 创建HashAggregate: GROUP BY category, COUNT(*)
	groupByExprs := []int{0} // 按第0列分组
	aggFuncs := []AggregateFunc{&CountAgg{}}

	hashAgg := NewHashAggregateOperator(childOp, groupByExprs, aggFuncs)

	// 打开算子
	err := hashAgg.Open(ctx)
	assert.NoError(t, err)

	// 读取所有结果
	results := make([]Record, 0)
	for {
		record, err := hashAgg.Next(ctx)
		assert.NoError(t, err)
		if record == nil {
			break
		}
		results = append(results, record)
	}

	// 验证结果：应该有2个分组 (A和B)
	assert.Equal(t, 2, len(results))

	// 统计每个分组的COUNT值
	countMap := make(map[string]int64)
	for _, result := range results {
		values := result.GetValues()
		assert.Equal(t, 1, len(values)) // 只有COUNT列
		count := values[0].ToInt64()
		// A组应该有3条，B组应该有2条
		if count == 3 || count == 2 {
			if count == 3 {
				countMap["A"] = count
			} else {
				countMap["B"] = count
			}
		}
	}

	assert.Equal(t, int64(3), countMap["A"])
	assert.Equal(t, int64(2), countMap["B"])

	err = hashAgg.Close()
	assert.NoError(t, err)
}

func TestHashAggregateOperator_SumAvg(t *testing.T) {
	ctx := context.Background()

	// 创建测试数据
	schema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "category", Type: metadata.TypeVarchar},
			{Name: "amount", Type: metadata.TypeDouble},
		},
	}
	data := [][]basic.Value{
		{basic.NewString("A"), basic.NewFloat64(10.0)},
		{basic.NewString("A"), basic.NewFloat64(20.0)},
		{basic.NewString("A"), basic.NewFloat64(30.0)},
		{basic.NewString("B"), basic.NewFloat64(15.0)},
		{basic.NewString("B"), basic.NewFloat64(25.0)},
	}
	childOp := NewMockDataOperator(data, schema)

	// GROUP BY category, SUM(amount), AVG(amount)
	groupByExprs := []int{0}
	aggFuncs := []AggregateFunc{
		&SumAgg{},
		&AvgAgg{},
	}

	hashAgg := NewHashAggregateOperator(childOp, groupByExprs, aggFuncs)
	err := hashAgg.Open(ctx)
	assert.NoError(t, err)

	results := make([]Record, 0)
	for {
		record, err := hashAgg.Next(ctx)
		assert.NoError(t, err)
		if record == nil {
			break
		}
		results = append(results, record)
	}

	assert.Equal(t, 2, len(results))

	// 验证聚合结果
	for _, result := range results {
		values := result.GetValues()
		assert.Equal(t, 2, len(values)) // SUM和AVG

		sum := values[0].ToFloat64()
		avg := values[1].ToFloat64()

		// A组: SUM=60, AVG=20
		// B组: SUM=40, AVG=20
		if sum == 60.0 {
			assert.Equal(t, 20.0, avg)
		} else if sum == 40.0 {
			assert.Equal(t, 20.0, avg)
		} else {
			t.Errorf("Unexpected sum value: %f", sum)
		}
	}

	err = hashAgg.Close()
	assert.NoError(t, err)
}

func TestHashAggregateOperator_MinMax(t *testing.T) {
	ctx := context.Background()

	// 创建测试数据
	schema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "category", Type: metadata.TypeVarchar},
			{Name: "score", Type: metadata.TypeDouble},
		},
	}
	data := [][]basic.Value{
		{basic.NewString("A"), basic.NewFloat64(85.0)},
		{basic.NewString("A"), basic.NewFloat64(90.0)},
		{basic.NewString("A"), basic.NewFloat64(75.0)},
		{basic.NewString("B"), basic.NewFloat64(88.0)},
		{basic.NewString("B"), basic.NewFloat64(92.0)},
	}
	childOp := NewMockDataOperator(data, schema)

	// GROUP BY category, MIN(score), MAX(score)
	groupByExprs := []int{0}
	aggFuncs := []AggregateFunc{
		&MinAgg{},
		&MaxAgg{},
	}

	hashAgg := NewHashAggregateOperator(childOp, groupByExprs, aggFuncs)
	err := hashAgg.Open(ctx)
	assert.NoError(t, err)

	results := make([]Record, 0)
	for {
		record, err := hashAgg.Next(ctx)
		assert.NoError(t, err)
		if record == nil {
			break
		}
		results = append(results, record)
	}

	assert.Equal(t, 2, len(results))

	// 验证MIN和MAX
	for _, result := range results {
		values := result.GetValues()
		assert.Equal(t, 2, len(values))

		min := values[0].ToFloat64()
		max := values[1].ToFloat64()

		// A组: MIN=75, MAX=90
		// B组: MIN=88, MAX=92
		if min == 75.0 {
			assert.Equal(t, 90.0, max)
		} else if min == 88.0 {
			assert.Equal(t, 92.0, max)
		} else {
			t.Errorf("Unexpected min value: %f", min)
		}
	}

	err = hashAgg.Close()
	assert.NoError(t, err)
}

func TestHashAggregateOperator_NoGroupBy(t *testing.T) {
	ctx := context.Background()

	// 创建测试数据
	schema := &metadata.Schema{
		Columns: []*metadata.Column{
			{Name: "amount", Type: metadata.TypeDouble},
		},
	}
	data := [][]basic.Value{
		{basic.NewFloat64(10.0)},
		{basic.NewFloat64(20.0)},
		{basic.NewFloat64(30.0)},
	}
	childOp := NewMockDataOperator(data, schema)

	// 无分组，全表聚合: COUNT(*), SUM(amount)
	groupByExprs := []int{} // 空分组
	aggFuncs := []AggregateFunc{
		&CountAgg{},
		&SumAgg{},
	}

	hashAgg := NewHashAggregateOperator(childOp, groupByExprs, aggFuncs)
	err := hashAgg.Open(ctx)
	assert.NoError(t, err)

	// 应该只有一条结果
	record, err := hashAgg.Next(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, record)

	values := record.GetValues()
	assert.Equal(t, 2, len(values))
	assert.Equal(t, int64(3), values[0].ToInt64()) // COUNT = 3
	assert.Equal(t, 60.0, values[1].ToFloat64())   // SUM = 60

	// 第二次调用应该返回nil
	record, err = hashAgg.Next(ctx)
	assert.NoError(t, err)
	assert.Nil(t, record)

	err = hashAgg.Close()
	assert.NoError(t, err)
}

// ========================================
// 辅助：MockDataOperator
// ========================================

// MockDataOperator 模拟数据源算子，用于测试
type MockDataOperator struct {
	BaseOperator
	data   [][]basic.Value
	schema *metadata.Schema
	idx    int
}

func NewMockDataOperator(data [][]basic.Value, schema *metadata.Schema) *MockDataOperator {
	return &MockDataOperator{
		data:   data,
		schema: schema,
		idx:    0,
	}
}

func (m *MockDataOperator) Open(ctx context.Context) error {
	m.opened = true
	m.idx = 0
	return nil
}

func (m *MockDataOperator) Next(ctx context.Context) (Record, error) {
	if !m.opened {
		return nil, nil
	}
	if m.idx >= len(m.data) {
		return nil, nil // EOF
	}
	record := NewExecutorRecordFromValues(m.data[m.idx], m.schema)
	m.idx++
	return record, nil
}

func (m *MockDataOperator) Close() error {
	m.opened = false
	return nil
}

func (m *MockDataOperator) Schema() *metadata.Schema {
	return m.schema
}
