package engine

import (
	"context"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

func TestVolcanoExecutorExecuteBatchesBoundsCallbackSize(t *testing.T) {
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	executor.root = &MockOperator{records: []Record{
		NewExecutorRecordFromValues(nil, nil),
		NewExecutorRecordFromValues(nil, nil),
		NewExecutorRecordFromValues(nil, nil),
		NewExecutorRecordFromValues(nil, nil),
		NewExecutorRecordFromValues(nil, nil),
	}}

	var sizes []int
	var rows int
	err := executor.ExecuteBatches(context.Background(), 2, func(batch []Record) error {
		sizes = append(sizes, len(batch))
		rows += len(batch)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int{2, 2, 1}, sizes)
	require.Equal(t, 5, rows)
}

type batchAwareOperator struct {
	records    []Record
	position   int
	batchCalls int
	nextCalls  int
	opened     bool
	schema     *metadata.QuerySchema
}

func (o *batchAwareOperator) Open(context.Context) error {
	o.opened = true
	o.position = 0
	return nil
}

func (o *batchAwareOperator) Next(context.Context) (Record, error) {
	o.nextCalls++
	if o.position >= len(o.records) {
		return nil, nil
	}
	record := o.records[o.position]
	o.position++
	return record, nil
}

func (o *batchAwareOperator) NextBatch(_ context.Context, maxRows int) ([]Record, error) {
	o.batchCalls++
	if maxRows <= 0 {
		return nil, nil
	}
	if o.position >= len(o.records) {
		return nil, io.EOF
	}
	end := o.position + maxRows
	if end > len(o.records) {
		end = len(o.records)
	}
	batch := o.records[o.position:end]
	o.position = end
	return batch, nil
}

func (o *batchAwareOperator) Close() error { return nil }

func (o *batchAwareOperator) Schema() *metadata.QuerySchema { return o.schema }

func TestVolcanoExecutorExecuteBatchesUsesBatchOperator(t *testing.T) {
	root := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues(nil, nil),
		NewExecutorRecordFromValues(nil, nil),
		NewExecutorRecordFromValues(nil, nil),
		NewExecutorRecordFromValues(nil, nil),
		NewExecutorRecordFromValues(nil, nil),
	}}
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	executor.root = root

	var sizes []int
	err := executor.ExecuteBatches(context.Background(), 2, func(batch []Record) error {
		sizes = append(sizes, len(batch))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int{2, 2, 1}, sizes)
	require.Equal(t, 4, root.batchCalls) // the final call observes EOF
	require.Zero(t, root.nextCalls)
}

func TestVolcanoBatchOperatorsPropagateThroughFilterAndProjection(t *testing.T) {
	root := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(2)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(3)}, nil),
	}}
	projection := NewProjectionOperator(root, []int{0})
	filter := NewFilterOperator(projection, func(record Record) bool {
		return record.GetValueByIndex(0).Int() >= 2
	})

	require.NoError(t, filter.Open(context.Background()))
	batch, err := filter.NextBatch(context.Background(), 2)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.Equal(t, int64(2), batch[0].GetValueByIndex(0).Int())
	require.Greater(t, root.batchCalls, 0)
	require.Zero(t, root.nextCalls)
}

func TestVolcanoBatchOperatorsPropagateThroughHashAggregate(t *testing.T) {
	root := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(2)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(3)}, nil),
	}}
	aggregate := NewHashAggregateOperator(root, nil, []AggregateFunc{&CountAgg{}})
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	executor.root = aggregate

	var rows int
	err := executor.ExecuteBatches(context.Background(), 2, func(batch []Record) error {
		rows += len(batch)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	require.Greater(t, root.batchCalls, 0)
	require.Zero(t, root.nextCalls)
}

func TestVolcanoBatchOperatorsPropagateThroughSort(t *testing.T) {
	root := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(3)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(2)}, nil),
	}}
	sortOperator := NewSortOperator(root, []SortKey{{ColumnIdx: 0, Ascending: true}})
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	executor.root = sortOperator

	var values []int64
	err := executor.ExecuteBatches(context.Background(), 2, func(batch []Record) error {
		for _, record := range batch {
			values = append(values, record.GetValueByIndex(0).Int())
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3}, values)
	require.Greater(t, root.batchCalls, 0)
	require.Zero(t, root.nextCalls)
}

func TestVolcanoBatchOperatorsPropagateThroughLimit(t *testing.T) {
	root := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(2)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(3)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(4)}, nil),
	}}
	limit := NewLimitOperator(root, 1, 2)
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	executor.root = limit

	var values []int64
	err := executor.ExecuteBatches(context.Background(), 2, func(batch []Record) error {
		for _, record := range batch {
			values = append(values, record.GetValueByIndex(0).Int())
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3}, values)
	require.Greater(t, root.batchCalls, 0)
	require.Zero(t, root.nextCalls)
}

func TestVolcanoBatchOperatorsPropagateThroughCTEScanAndWindow(t *testing.T) {
	schema := createTestSchema([]testColumn{{Name: "id", Type: metadata.TypeInt}})
	records := []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(10)}, schema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(20)}, schema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(30)}, schema),
	}
	cteContext := NewCTEContext()
	cteContext.Materialize("numbers", records)
	cteScan := NewCTEScanOperator("numbers", cteContext, schema)
	window := NewWindowFunctionOperator(cteScan, WindowSpec{}, WindowFunction{Type: WindowFuncRowNumber})
	if err := window.Open(context.Background()); err != nil {
		t.Fatalf("window Open() error = %v", err)
	}
	defer window.Close()

	batch, err := window.NextBatch(context.Background(), 2)
	if err != nil {
		t.Fatalf("window NextBatch() error = %v", err)
	}
	require.Len(t, batch, 2)
	require.Equal(t, int64(1), batch[0].GetValueByIndex(1).Int())
	require.Equal(t, int64(2), batch[1].GetValueByIndex(1).Int())
	require.Equal(t, io.EOF, func() error {
		_, err := window.NextBatch(context.Background(), 2)
		return err
	}())
}

func TestWindowRowNumberStreamsUnorderedRowsThroughBatches(t *testing.T) {
	schema := createTestSchema([]testColumn{{Name: "id", Type: metadata.TypeInt}})
	child := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(10)}, schema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(20)}, schema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(30)}, schema),
	}, schema: schema}
	window := NewWindowFunctionOperator(child, WindowSpec{}, WindowFunction{Type: WindowFuncRowNumber})
	require.NoError(t, window.Open(context.Background()))
	defer window.Close()

	first, err := window.NextBatch(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, []int64{first[0].GetValueByIndex(1).Int(), first[1].GetValueByIndex(1).Int()})
	last, err := window.NextBatch(context.Background(), 2)
	require.ErrorIs(t, err, io.EOF)
	require.Len(t, last, 1)
	require.Equal(t, int64(3), last[0].GetValueByIndex(1).Int())
	require.Greater(t, child.batchCalls, 0)
	require.Zero(t, child.nextCalls)
}

func TestRankWindowsWithoutOrderingStreamConstantResults(t *testing.T) {
	schema := createTestSchema([]testColumn{{Name: "id", Type: metadata.TypeInt}})
	for _, windowType := range []WindowFunctionType{WindowFuncRank, WindowFuncDenseRank} {
		child := &batchAwareOperator{records: []Record{
			NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(10)}, schema),
			NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(20)}, schema),
			NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(30)}, schema),
		}, schema: schema}
		window := NewWindowFunctionOperator(child, WindowSpec{}, WindowFunction{Type: windowType})
		require.NoError(t, window.Open(context.Background()))
		batch, err := window.NextBatch(context.Background(), 2)
		require.NoError(t, err)
		require.Equal(t, []int64{1, 1}, []int64{batch[0].GetValueByIndex(1).Int(), batch[1].GetValueByIndex(1).Int()})
		require.Zero(t, child.nextCalls)
		require.Greater(t, child.batchCalls, 0)
		require.NoError(t, window.Close())
	}
}

func TestWindowFunctionOperatorSupportsDistributionAndNthValue(t *testing.T) {
	schema := createTestSchema([]testColumn{{Name: "score", Type: metadata.TypeInt}})
	rows := []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(10)}, schema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(20)}, schema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(20)}, schema),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(40)}, schema),
	}
	windowSpec := WindowSpec{OrderBy: []OrderBySpec{{ColumnIndex: 0, Ascending: true}}}

	tests := []struct {
		name string
		fn   WindowFunction
		want []float64
	}{
		{name: "cume_dist", fn: WindowFunction{Type: WindowFuncCumeDist}, want: []float64{0.25, 0.75, 0.75, 1}},
		{name: "percent_rank", fn: WindowFunction{Type: WindowFuncPercentRank}, want: []float64{0, 1.0 / 3, 1.0 / 3, 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			child := &ExpressionMockOperator{records: rows, schema: schema}
			operator := NewWindowFunctionOperator(child, windowSpec, test.fn)
			require.NoError(t, operator.Open(context.Background()))
			batch, err := operator.NextBatch(context.Background(), len(rows))
			require.ErrorIs(t, err, io.EOF)
			require.Len(t, batch, len(rows))
			for index, want := range test.want {
				require.InDelta(t, want, batch[index].GetValueByIndex(1).Float64(), 1e-9)
			}
			require.NoError(t, operator.Close())
		})
	}

	child := &ExpressionMockOperator{records: rows, schema: schema}
	nth := NewWindowFunctionOperator(child, windowSpec, WindowFunction{Type: WindowFuncNthValue, ColumnIndex: 0, N: 2})
	require.NoError(t, nth.Open(context.Background()))
	batch, err := nth.NextBatch(context.Background(), len(rows))
	require.ErrorIs(t, err, io.EOF)
	require.Len(t, batch, len(rows))
	require.True(t, batch[0].GetValueByIndex(1).IsNull())
	require.Equal(t, int64(20), batch[1].GetValueByIndex(1).Int())
	require.Equal(t, int64(20), batch[2].GetValueByIndex(1).Int())
	require.Equal(t, int64(20), batch[3].GetValueByIndex(1).Int())
	require.NoError(t, nth.Close())
}

func TestVolcanoHashJoinBuildsFromBatches(t *testing.T) {
	build := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(2)}, nil),
	}}
	probe := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(2)}, nil),
	}}
	join := NewHashJoinOperator(
		build,
		probe,
		"INNER",
		func(record Record) string { return record.GetValueByIndex(0).String() },
		func(record Record) string { return record.GetValueByIndex(0).String() },
	)
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	executor.root = join

	var rows int
	err := executor.ExecuteBatches(context.Background(), 2, func(batch []Record) error {
		rows += len(batch)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	require.Greater(t, build.batchCalls, 0)
	require.Greater(t, probe.batchCalls, 0)
	require.Zero(t, probe.nextCalls)
}

func TestVolcanoHashJoinBatchPreservesFullOuterRows(t *testing.T) {
	schema := createTestSchema([]testColumn{{Name: "id", Type: metadata.TypeInt}})
	build := &batchAwareOperator{
		records: []Record{
			NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, schema),
			NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(3)}, schema),
		},
		schema: schema,
	}
	probe := &batchAwareOperator{
		records: []Record{
			NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, schema),
			NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(2)}, schema),
		},
		schema: schema,
	}
	join := NewHashJoinOperator(
		build,
		probe,
		"FULL OUTER",
		func(record Record) string { return record.GetValueByIndex(0).String() },
		func(record Record) string { return record.GetValueByIndex(0).String() },
	)
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	executor.root = join

	var rows []Record
	err := executor.ExecuteBatches(context.Background(), 1, func(batch []Record) error {
		rows = append(rows, batch...)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.True(t, rows[1].GetValues()[1].IsNull())
	require.True(t, rows[2].GetValues()[0].IsNull())
	require.Greater(t, build.batchCalls, 0)
	require.Greater(t, probe.batchCalls, 0)
	require.Zero(t, build.nextCalls)
	require.Zero(t, probe.nextCalls)
}

func TestVolcanoHashAggregateSupportsGroupConcatDistinctAndSeparator(t *testing.T) {
	schema := createTestSchema([]testColumn{{Name: "name", Type: metadata.TypeVarchar}})
	child := &batchAwareOperator{
		records: []Record{
			NewExecutorRecordFromValues([]basic.Value{basic.NewString("a")}, schema),
			NewExecutorRecordFromValues([]basic.Value{basic.NewString("b")}, schema),
			NewExecutorRecordFromValues([]basic.Value{basic.NewString("a")}, schema),
			NewExecutorRecordFromValues([]basic.Value{basic.NewNull()}, schema),
		},
		schema: schema,
	}
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	functions := executor.buildAggFuncs([]plan.AggregateFunc{&plan.Function{
		FuncName:  "GROUP_CONCAT",
		Distinct:  true,
		Separator: ";",
		FuncArgs:  []plan.Expression{&plan.Column{Name: "name"}},
	}})
	require.Len(t, functions, 1)
	require.IsType(t, &GroupConcatAgg{}, functions[0])
	aggregate := NewHashAggregateOperator(child, nil, functions)
	records, err := collectOperatorRowsForTest(context.Background(), aggregate)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "a;b", records[0].GetValueByIndex(0).String())
}

func TestVolcanoHashAggregateSupportsBitAndVarianceAggregates(t *testing.T) {
	root := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(3)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(3)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewNull()}, nil),
	}}
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	aggregates := executor.buildAggFuncs([]plan.AggregateFunc{
		&plan.Function{FuncName: "BIT_AND", FuncArgs: []plan.Expression{&plan.Column{Name: "value"}}},
		&plan.Function{FuncName: "BIT_OR", FuncArgs: []plan.Expression{&plan.Column{Name: "value"}}},
		&plan.Function{FuncName: "BIT_XOR", FuncArgs: []plan.Expression{&plan.Column{Name: "value"}}},
		&plan.Function{FuncName: "VAR_POP", FuncArgs: []plan.Expression{&plan.Column{Name: "value"}}},
		&plan.Function{FuncName: "STDDEV_SAMP", FuncArgs: []plan.Expression{&plan.Column{Name: "value"}}},
	})
	aggregate := NewHashAggregateOperator(root, nil, aggregates)
	executor.root = aggregate

	var rows []Record
	err := executor.ExecuteBatches(context.Background(), 2, func(batch []Record) error {
		rows = append(rows, batch...)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	values := rows[0].GetValues()
	require.Equal(t, int64(1), values[0].Int())
	require.Equal(t, int64(3), values[1].Int())
	require.Equal(t, int64(1), values[2].Int())
	require.InDelta(t, float64(8)/9, values[3].Float64(), 1e-9)
	require.InDelta(t, math.Sqrt(float64(4)/3), values[4].Float64(), 1e-9)
	require.Greater(t, root.batchCalls, 0)
	require.Zero(t, root.nextCalls)
}

func TestVolcanoHashAggregateSupportsJSONArrayAgg(t *testing.T) {
	root := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewNull()}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(3)}, nil),
	}}
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	aggregates := executor.buildAggFuncs([]plan.AggregateFunc{&plan.Function{
		FuncName: "JSON_ARRAYAGG",
		FuncArgs: []plan.Expression{&plan.Column{Name: "value"}},
	}})
	aggregate := NewHashAggregateOperator(root, nil, aggregates)
	records, err := collectOperatorRowsForTest(context.Background(), aggregate)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, `[1,null,3]`, records[0].GetValueByIndex(0).String())
}

func TestVolcanoHashAggregateSupportsJSONObjectAgg(t *testing.T) {
	schema := createTestSchema([]testColumn{{Name: "key", Type: metadata.TypeVarchar}, {Name: "value", Type: metadata.TypeInt}})
	root := &batchAwareOperator{
		records: []Record{
			NewExecutorRecordFromValues([]basic.Value{basic.NewString("a"), basic.NewInt64Value(1)}, schema),
			NewExecutorRecordFromValues([]basic.Value{basic.NewString("b"), basic.NewInt64Value(2)}, schema),
			NewExecutorRecordFromValues([]basic.Value{basic.NewString("a"), basic.NewInt64Value(3)}, schema),
		},
		schema: schema,
	}
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	planFunctions := []plan.AggregateFunc{&plan.Function{
		FuncName: "JSON_OBJECTAGG",
		FuncArgs: []plan.Expression{&plan.Column{Name: "key"}, &plan.Column{Name: "value"}},
	}}
	aggregate := NewHashAggregateOperatorWithExpressions(root, nil, executor.buildAggFuncs(planFunctions), planFunctions)
	records, err := collectOperatorRowsForTest(context.Background(), aggregate)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, `{"a":3,"b":2}`, records[0].GetValueByIndex(0).String())
}

func TestVolcanoHashAggregateSupportsAnyValue(t *testing.T) {
	root := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewString("first")}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewString("second")}, nil),
	}}
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	planFunctions := []plan.AggregateFunc{&plan.Function{
		FuncName: "ANY_VALUE",
		FuncArgs: []plan.Expression{&plan.Column{Name: "value"}},
	}}
	aggregate := NewHashAggregateOperatorWithExpressions(root, nil, executor.buildAggFuncs(planFunctions), planFunctions)
	records, err := collectOperatorRowsForTest(context.Background(), aggregate)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "first", records[0].GetValueByIndex(0).String())
}

func TestVolcanoHashAggregateSupportsDistinctAggregates(t *testing.T) {
	root := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(2)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewNull()}, nil),
	}}
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	planFunctions := []plan.AggregateFunc{
		&plan.Function{FuncName: "COUNT", Distinct: true, FuncArgs: []plan.Expression{&plan.Column{Name: "value"}}},
		&plan.Function{FuncName: "SUM", Distinct: true, FuncArgs: []plan.Expression{&plan.Column{Name: "value"}}},
		&plan.Function{FuncName: "AVG", Distinct: true, FuncArgs: []plan.Expression{&plan.Column{Name: "value"}}},
	}
	aggregate := NewHashAggregateOperatorWithExpressions(root, nil, executor.buildAggFuncs(planFunctions), planFunctions)
	records, err := collectOperatorRowsForTest(context.Background(), aggregate)
	require.NoError(t, err)
	require.Len(t, records, 1)
	values := records[0].GetValues()
	require.Equal(t, int64(2), values[0].Int())
	require.InDelta(t, float64(3), values[1].Float64(), 1e-9)
	require.InDelta(t, float64(1.5), values[2].Float64(), 1e-9)
}

func TestVolcanoHashAggregateSupportsMultiColumnCountDistinct(t *testing.T) {
	schema := createTestSchema([]testColumn{{Name: "a", Type: metadata.TypeInt}, {Name: "b", Type: metadata.TypeInt}})
	root := &batchAwareOperator{
		records: []Record{
			NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1), basic.NewInt64Value(1)}, schema),
			NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1), basic.NewInt64Value(2)}, schema),
			NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1), basic.NewInt64Value(1)}, schema),
		},
		schema: schema,
	}
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	planFunctions := []plan.AggregateFunc{&plan.Function{
		FuncName: "COUNT",
		Distinct: true,
		FuncArgs: []plan.Expression{&plan.Column{Name: "a"}, &plan.Column{Name: "b"}},
	}}
	aggregate := NewHashAggregateOperatorWithExpressions(root, nil, executor.buildAggFuncs(planFunctions), planFunctions)
	records, err := collectOperatorRowsForTest(context.Background(), aggregate)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, int64(2), records[0].GetValueByIndex(0).Int())
}

func collectOperatorRowsForTest(ctx context.Context, operator Operator) ([]Record, error) {
	if err := operator.Open(ctx); err != nil {
		return nil, err
	}
	defer operator.Close()
	rows := make([]Record, 0)
	for {
		row, err := operator.Next(ctx)
		if err != nil {
			return nil, err
		}
		if row == nil {
			return rows, nil
		}
		rows = append(rows, row)
	}
}

func TestVolcanoNestedLoopJoinConsumesBatchChildren(t *testing.T) {
	left := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(2)}, nil),
	}}
	right := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(2)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(3)}, nil),
	}}
	join := NewNestedLoopJoinOperator(left, right, "INNER", func(leftRow, rightRow Record) bool {
		return leftRow.GetValueByIndex(0).Int() == rightRow.GetValueByIndex(0).Int()
	})
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	executor.root = join

	var values []int64
	err := executor.ExecuteBatches(context.Background(), 2, func(batch []Record) error {
		for _, record := range batch {
			values = append(values, record.GetValueByIndex(0).Int())
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int64{2}, values)
	require.Greater(t, left.batchCalls, 0)
	require.Greater(t, right.batchCalls, 0)
	require.Zero(t, left.nextCalls)
	require.Zero(t, right.nextCalls)
}

func TestVolcanoNestedLoopJoinBatchPreservesOuterRows(t *testing.T) {
	type joinCase struct {
		name          string
		expectedRows  int
		expectedNulls int
	}
	cases := []joinCase{
		{name: "LEFT", expectedRows: 2, expectedNulls: 1},
		{name: "RIGHT", expectedRows: 2, expectedNulls: 1},
		{name: "FULL", expectedRows: 3, expectedNulls: 2},
		{name: "RIGHT OUTER", expectedRows: 2, expectedNulls: 1},
		{name: "FULL OUTER", expectedRows: 3, expectedNulls: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := createTestSchema([]testColumn{{Name: "id", Type: metadata.TypeInt}})
			left := &batchAwareOperator{
				records: []Record{
					NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, schema),
					NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(2)}, schema),
				},
				schema: schema,
			}
			right := &batchAwareOperator{
				records: []Record{
					NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, schema),
					NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(3)}, schema),
				},
				schema: schema,
			}
			join := NewNestedLoopJoinOperator(left, right, tc.name, func(leftRow, rightRow Record) bool {
				return leftRow.GetValueByIndex(0).Int() == rightRow.GetValueByIndex(0).Int()
			})
			executor := NewVolcanoExecutor(nil, nil, nil, nil)
			executor.root = join

			var rows []Record
			err := executor.ExecuteBatches(context.Background(), 1, func(batch []Record) error {
				rows = append(rows, batch...)
				return nil
			})
			require.NoError(t, err)
			require.Len(t, rows, tc.expectedRows)
			nulls := 0
			for _, row := range rows {
				values := row.GetValues()
				if values[0].IsNull() {
					nulls++
				}
				if values[1].IsNull() {
					nulls++
				}
			}
			require.Equal(t, tc.expectedNulls, nulls)
			require.Greater(t, left.batchCalls, 0)
			require.Greater(t, right.batchCalls, 0)
			require.Zero(t, left.nextCalls)
			require.Zero(t, right.nextCalls)
		})
	}
}

func TestVolcanoApplyConsumesBatchChildren(t *testing.T) {
	outer := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(1)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(2)}, nil),
	}}
	inner := &batchAwareOperator{records: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(10)}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64Value(20)}, nil),
	}}
	apply := NewApplyOperator(outer, inner, "INNER", false, nil)
	executor := NewVolcanoExecutor(nil, nil, nil, nil)
	executor.root = apply

	var rows int
	err := executor.ExecuteBatches(context.Background(), 2, func(batch []Record) error {
		rows += len(batch)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 4, rows)
	require.Greater(t, outer.batchCalls, 0)
	require.Greater(t, inner.batchCalls, 0)
	require.Zero(t, outer.nextCalls)
	require.Zero(t, inner.nextCalls)
}
