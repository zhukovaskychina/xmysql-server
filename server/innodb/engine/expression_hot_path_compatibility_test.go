package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

func TestProjectionExpressionHotPathReusesRowMapAndClearsMissingValues(t *testing.T) {
	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
	schema.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))
	child := &expressionRowOperator{schema: schema, rows: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(1), basic.NewString("a")}, nil),
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(2)}, nil),
	}}
	projection := NewProjectionOperatorWithExprs(child, []plan.Expression{
		&plan.Function{FuncName: "COALESCE", FuncArgs: []plan.Expression{&plan.Column{Name: "name"}, &plan.Constant{Value: "missing"}}},
	})
	require.NoError(t, projection.Open(context.Background()))
	first, err := projection.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "a", first.GetValueByIndex(0).String())
	second, err := projection.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "missing", second.GetValueByIndex(0).String())
	require.NoError(t, projection.Close())
	require.NotNil(t, projection.evalRow)
}

func TestProjectionExpressionHotPathCompilesSupportedExpressions(t *testing.T) {
	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("amount", metadata.TypeInt))
	child := &expressionRowOperator{schema: schema, rows: []Record{
		NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(8)}, nil),
	}}
	projection := NewProjectionOperatorWithExprs(child, []plan.Expression{
		&plan.BinaryOperation{
			Op:    plan.OpGT,
			Left:  &plan.BinaryOperation{Op: plan.OpAdd, Left: &plan.Column{Name: "amount"}, Right: &plan.Constant{Value: int64(2)}},
			Right: &plan.Constant{Value: int64(9)},
		},
	})
	require.NoError(t, projection.Open(context.Background()))
	require.Len(t, projection.compiledExprs, 1)
	require.NotNil(t, projection.compiledExprs[0])
	result, err := projection.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.GetValueByIndex(0).Bool())
}

type expressionRowOperator struct {
	BaseOperator
	schema *metadata.QuerySchema
	rows   []Record
	index  int
}

func (o *expressionRowOperator) Open(context.Context) error { o.index = 0; return nil }
func (o *expressionRowOperator) Next(context.Context) (Record, error) {
	if o.index >= len(o.rows) {
		return nil, nil
	}
	row := o.rows[o.index]
	o.index++
	return row, nil
}
func (o *expressionRowOperator) Close() error                  { return nil }
func (o *expressionRowOperator) Schema() *metadata.QuerySchema { return o.schema }
