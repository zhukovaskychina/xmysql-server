package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestConvertToPhysicalPlanPreservesUnaryChildren(t *testing.T) {
	schema := metadata.NewSchema("app")
	table := metadata.NewTable("users")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	require.NoError(t, schema.AddTable(table))
	scan := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schema}, Table: table}

	projection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{schema: schema, children: []LogicalPlan{scan}},
		Exprs:           []Expression{&Column{Name: "id"}},
	}
	selection := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{schema: schema, children: []LogicalPlan{projection}},
		Conditions:      []Expression{&BinaryOperation{Op: OpGT, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(0)}}},
	}

	physical := ConvertToPhysicalPlan(selection)
	physicalSelection, ok := physical.(*PhysicalSelection)
	require.True(t, ok)
	require.Len(t, physicalSelection.Children(), 1)
	physicalProjection, ok := physicalSelection.Children()[0].(*PhysicalProjection)
	require.True(t, ok)
	require.Len(t, physicalProjection.Children(), 1)
	_, ok = physicalProjection.Children()[0].(*PhysicalTableScan)
	require.True(t, ok)
}

func TestConvertToPhysicalPlanPreservesAggregationChild(t *testing.T) {
	schema := metadata.NewSchema("app")
	table := metadata.NewTable("orders")
	table.AddColumn(&metadata.Column{Name: "amount", DataType: metadata.TypeInt})
	require.NoError(t, schema.AddTable(table))
	scan := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schema}, Table: table}
	aggregation := &LogicalAggregation{
		BaseLogicalPlan: BaseLogicalPlan{schema: schema, children: []LogicalPlan{scan}},
	}

	physical := ConvertToPhysicalPlan(aggregation)
	physicalAggregation := physical.Children()
	require.Len(t, physicalAggregation, 1)
	_, ok := physicalAggregation[0].(*PhysicalTableScan)
	require.True(t, ok)
}

func TestConvertToPhysicalPlanPreservesJoinAndApplyChildren(t *testing.T) {
	schema := metadata.NewSchema("app")
	leftTable := metadata.NewTable("users")
	leftTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	rightTable := metadata.NewTable("labels")
	rightTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	require.NoError(t, schema.AddTable(leftTable))
	require.NoError(t, schema.AddTable(rightTable))
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schema}, Table: leftTable}
	right := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: schema}, Table: rightTable}

	join := &LogicalJoin{BaseLogicalPlan: BaseLogicalPlan{schema: schema, children: []LogicalPlan{left, right}}}
	physicalJoin := ConvertToPhysicalPlan(join)
	require.Len(t, physicalJoin.Children(), 2)

	apply := &LogicalApply{BaseLogicalPlan: BaseLogicalPlan{schema: schema, children: []LogicalPlan{left, right}}}
	physicalApply, ok := ConvertToPhysicalPlan(apply).(*PhysicalApply)
	require.True(t, ok)
	require.Len(t, physicalApply.Children(), 2)
}
