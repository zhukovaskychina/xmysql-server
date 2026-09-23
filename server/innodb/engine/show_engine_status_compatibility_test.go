package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestShowEngineInnoDBStatusReturnsNativeShape(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: "show engine innodb status"}

	stmt, err := sqlparser.Parse("show engine innodb status")
	require.NoError(t, err)
	show, ok := stmt.(*sqlparser.Show)
	require.True(t, ok)
	executor.executeShowStatementWithQuery(ctx, show, nil, "show engine innodb status")

	result := <-results
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"Type", "Name", "Status"}, selectResult.Columns)
	require.Len(t, selectResult.Records, 1)
	require.Equal(t, "InnoDB", selectResult.Records[0].GetValues()[0].String())
	require.Equal(t, "InnoDB", selectResult.Records[0].GetValues()[1].String())
	require.True(t, strings.Contains(selectResult.Records[0].GetValues()[2].String(), "TRANSACTIONS"))
}
