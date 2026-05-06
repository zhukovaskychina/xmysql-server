package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestBuildShowPlan(t *testing.T) {
	stmtRaw, err := sqlparser.Parse("show databases")
	assert.NoError(t, err)

	stmt, ok := stmtRaw.(*sqlparser.Show)
	assert.True(t, ok)

	logicalPlan, err := BuildShowPlan(stmt)
	require.NoError(t, err)
	require.NotNil(t, logicalPlan)
	assert.Contains(t, logicalPlan.String(), "SHOW")
}

func TestBuildShowPlan_NilStmt(t *testing.T) {
	logicalPlan, err := BuildShowPlan(nil)
	assert.Error(t, err)
	assert.Nil(t, logicalPlan)
}
