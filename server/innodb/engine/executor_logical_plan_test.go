package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestXMySQLExecutor_GenerateLogicalPlan(t *testing.T) {
	parsed, err := sqlparser.Parse("select id from users")
	assert.NoError(t, err)

	stmt, ok := parsed.(*sqlparser.Select)
	assert.True(t, ok)

	executor := &XMySQLExecutor{}
	logicalPlan, err := executor.generateLogicalPlan(stmt, "testdb")

	assert.NoError(t, err)
	assert.NotNil(t, logicalPlan)
}

func TestXMySQLExecutor_GenerateLogicalPlan_NilStmt(t *testing.T) {
	executor := &XMySQLExecutor{}

	logicalPlan, err := executor.generateLogicalPlan(nil, "testdb")

	assert.Error(t, err)
	assert.Nil(t, logicalPlan)
}

func TestXMySQLExecutor_HasEquiJoinCondition(t *testing.T) {
	executor := &XMySQLExecutor{}

	eq := &plan.BinaryOperation{
		Op:       plan.OpEQ,
		Left:     &plan.Column{Name: "a"},
		Right:    &plan.Constant{Value: 1},
		Operator: "=",
	}
	neq := &plan.BinaryOperation{
		Op:       plan.OpNE,
		Left:     &plan.Column{Name: "a"},
		Right:    &plan.Constant{Value: 1},
		Operator: "!=",
	}

	assert.True(t, executor.hasEquiJoinCondition([]plan.Expression{eq}))
	assert.False(t, executor.hasEquiJoinCondition([]plan.Expression{neq}))

	nested := &plan.BinaryOperation{
		Op:       plan.OpAnd,
		Left:     neq,
		Right:    eq,
		Operator: "AND",
	}
	assert.True(t, executor.hasEquiJoinCondition([]plan.Expression{nested}))
}
