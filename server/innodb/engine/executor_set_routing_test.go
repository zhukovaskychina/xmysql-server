package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	innodbcommon "github.com/zhukovaskychina/xmysql-server/server/innodb/common"
)

func TestXMySQLExecutor_ExecuteQuery_SetRoutesToExecuteSetStatement(t *testing.T) {
	executor := &XMySQLExecutor{}
	results := make(chan *Result, 4)
	ctx := &ExecutionContext{
		Context: context.Background(),
		Results: results,
	}

	executor.executeQuery(ctx, nil, "set autocommit=1", "testdb", results)

	result, ok := <-results
	assert.True(t, ok)
	assert.NotNil(t, result)
	assert.Equal(t, innodbcommon.RESULT_TYPE_ERROR, result.ResultType)
	assert.Error(t, result.Err)
	assert.Contains(t, result.Message, "session unavailable")
}
