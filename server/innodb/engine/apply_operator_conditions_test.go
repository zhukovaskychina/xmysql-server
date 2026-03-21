package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

func TestApplyOperator_EvaluateJoinConditions(t *testing.T) {
	outerSchema := metadata.NewQuerySchema()
	outerSchema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
	outerSchema.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))

	innerSchema := metadata.NewQuerySchema()
	innerSchema.AddColumn(metadata.NewQueryColumn("user_id", metadata.TypeInt))

	outerRow := NewExecutorRecordFromValues([]basic.Value{
		basic.NewInt64(1),
		basic.NewString("alice"),
	}, outerSchema)
	innerMatch := NewExecutorRecordFromValues([]basic.Value{
		basic.NewInt64(1),
	}, innerSchema)
	innerUnmatch := NewExecutorRecordFromValues([]basic.Value{
		basic.NewInt64(2),
	}, innerSchema)

	joinCond := &plan.BinaryOperation{
		Op:    plan.OpEQ,
		Left:  &plan.Column{Name: "id"},
		Right: &plan.Column{Name: "user_id"},
	}

	applyOp := NewApplyOperator(nil, nil, "SEMI", false, []plan.Expression{joinCond})

	assert.True(t, applyOp.evaluateJoinConditions(outerRow, innerMatch))
	assert.False(t, applyOp.evaluateJoinConditions(outerRow, innerUnmatch))
}

func TestApplyOperator_EvaluateJoinConditions_NoCondsAndNonBool(t *testing.T) {
	outerSchema := metadata.NewQuerySchema()
	outerSchema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
	outerRow := NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(1)}, outerSchema)

	innerSchema := metadata.NewQuerySchema()
	innerSchema.AddColumn(metadata.NewQueryColumn("user_id", metadata.TypeInt))
	innerRow := NewExecutorRecordFromValues([]basic.Value{basic.NewInt64(1)}, innerSchema)

	noCondOp := NewApplyOperator(nil, nil, "SEMI", false, nil)
	assert.True(t, noCondOp.evaluateJoinConditions(outerRow, innerRow))

	nonBoolCondOp := NewApplyOperator(nil, nil, "SEMI", false, []plan.Expression{
		&plan.Column{Name: "id"},
	})
	assert.False(t, nonBoolCondOp.evaluateJoinConditions(outerRow, innerRow))
}
