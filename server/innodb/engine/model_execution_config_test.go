package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestModelExecutionLimitsLoadedFromINI(t *testing.T) {
	previousConfigPath := conf.ConfigPath
	t.Cleanup(func() { conf.ConfigPath = previousConfigPath })
	path := filepath.Join(t.TempDir(), "server.ini")
	require.NoError(t, os.WriteFile(path, []byte("[ai]\nmax_calls_per_query=1\nmax_input_bytes_per_query=100\ncall_timeout=20ms\n"), 0600))
	cfg := conf.NewCfg().Load(&conf.CommandLineArgs{ConfigPath: path})
	cfg.DataDir = t.TempDir()
	executor := NewXMySQLExecutor(nil, cfg)
	calls := 0
	executor.SetGenerationRuntime(cancellationGenerationRuntime(func(context.Context, GenerationRequest) (GenerationResult, error) {
		calls++
		return GenerationResult{ModelID: "m", ModelVersion: "1", Text: "answer"}, nil
	}))
	statement, err := sqlparser.Parse("SELECT AI_GENERATE('abc', 'model'), AI_SUMMARIZE('abc', 'model')")
	require.NoError(t, err)
	result, err := executor.executeSelectStatement(&ExecutionContext{Context: context.Background()}, statement.(*sqlparser.Select), "app")
	require.ErrorIs(t, err, ErrModelBudgetExceeded)
	require.Nil(t, result)
	require.Equal(t, 1, calls)
	statement, err = sqlparser.Parse("SELECT AI_GENERATE('" + strings.Repeat("x", 101) + "', 'model')")
	require.NoError(t, err)
	result, err = executor.executeSelectStatement(&ExecutionContext{Context: context.Background()}, statement.(*sqlparser.Select), "app")
	require.ErrorIs(t, err, ErrModelBudgetExceeded)
	require.Nil(t, result)
	require.Equal(t, 1, calls, "configured input-byte limit must reject before provider execution")

	executor.SetGenerationRuntime(cancellationGenerationRuntime(func(ctx context.Context, _ GenerationRequest) (GenerationResult, error) {
		deadline, exists := ctx.Deadline()
		require.True(t, exists)
		require.False(t, deadline.IsZero())
		require.LessOrEqual(t, time.Until(deadline), 20*time.Millisecond, "provider deadline must use the configured timeout")
		<-ctx.Done()
		return GenerationResult{ModelID: "m", ModelVersion: "1", Text: "late answer"}, nil
	}))
	statement, err = sqlparser.Parse("SELECT AI_GENERATE('abc', 'model')")
	require.NoError(t, err)
	result, err = executor.executeSelectStatement(&ExecutionContext{Context: context.Background()}, statement.(*sqlparser.Select), "app")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, result)
}
