package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

func TestUnifiedExecutor_BuildOperatorTree_Errors(t *testing.T) {
	ue := &UnifiedExecutor{}
	ctx := context.Background()

	_, err := ue.BuildOperatorTree(ctx, nil)
	assert.Error(t, err)

	var nilPlan plan.PhysicalPlan
	_, err = ue.BuildOperatorTree(ctx, nilPlan)
	assert.Error(t, err)
}

func TestUnifiedExecutor_BuildOperatorTree_TableScan(t *testing.T) {
	ue := &UnifiedExecutor{}
	ctx := context.Background()

	physical := plan.PhysicalPlan(&plan.PhysicalTableScan{
		Table: &metadata.Table{
			Name:   "users",
			Schema: metadata.NewSchema("testdb"),
		},
	})

	op, err := ue.BuildOperatorTree(ctx, physical)
	require.NoError(t, err)
	require.NotNil(t, op)

	tableScan, ok := op.(*TableScanOperator)
	assert.True(t, ok)
	assert.Equal(t, "testdb", tableScan.schemaName)
	assert.Equal(t, "users", tableScan.tableName)
}
