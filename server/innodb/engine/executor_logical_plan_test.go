package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
