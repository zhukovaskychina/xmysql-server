package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestModelSQLSharesBudgetAcrossProjections(t *testing.T) {
	for _, policy := range []ModelExecutionPolicy{
		{MaxCalls: 1, MaxInputBytes: 1024, CallTimeout: time.Second},
		{MaxCalls: 10, MaxInputBytes: 5, CallTimeout: time.Second},
	} {
		executor := &XMySQLExecutor{}
		require.NoError(t, executor.SetModelExecutionPolicy(policy))
		calls := 0
		executor.SetGenerationRuntime(cancellationGenerationRuntime(func(context.Context, GenerationRequest) (GenerationResult, error) {
			calls++
			return GenerationResult{ModelID: "m", ModelVersion: "1", Text: "answer"}, nil
		}))
		statement, err := sqlparser.Parse("SELECT AI_GENERATE('abc', 'model'), AI_SUMMARIZE('abc', 'model')")
		require.NoError(t, err)
		ctx := &ExecutionContext{Context: executor.withModelQueryBudget(context.Background())}
		result, err := executor.executeSelectStatement(ctx, statement.(*sqlparser.Select), "app")
		require.ErrorIs(t, err, ErrModelBudgetExceeded)
		require.Nil(t, result)
		require.Equal(t, 1, calls, "the denied call must not reach the provider")
		// A fork must retain the reservation even after the parent failed.
		_, err = executor.executeSelectStatement(ctx.Fork(), statement.(*sqlparser.Select), "app")
		require.ErrorIs(t, err, ErrModelBudgetExceeded)
		require.Equal(t, 1, calls)
	}
}

func TestModelBudgetConcurrentReservationAndFailures(t *testing.T) {
	executor := &XMySQLExecutor{}
	require.NoError(t, executor.SetModelExecutionPolicy(ModelExecutionPolicy{MaxCalls: 2, MaxInputBytes: 8, CallTimeout: time.Second}))
	ctx := executor.withModelQueryBudget(context.Background())
	var calls atomic.Int64
	providerErr := errors.New("unknown remote outcome")
	runtime := cancellationGenerationRuntime(func(context.Context, GenerationRequest) (GenerationResult, error) {
		calls.Add(1)
		return GenerationResult{}, providerErr
	})
	var group sync.WaitGroup
	results := make(chan error, 20)
	for i := 0; i < 20; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := invokeGeneration(ctx, runtime, GenerationRequest{Prompt: "abcd"})
			results <- err
		}()
	}
	group.Wait()
	close(results)
	var attempted, denied int
	for err := range results {
		if errors.Is(err, providerErr) {
			attempted++
		} else {
			require.ErrorIs(t, err, ErrModelBudgetExceeded)
			denied++
		}
	}
	require.Equal(t, int64(2), calls.Load())
	require.Equal(t, 2, attempted)
	require.Equal(t, 18, denied)
}

func TestModelPolicyDeadlineRejectsLateSuccess(t *testing.T) {
	executor := &XMySQLExecutor{}
	require.NoError(t, executor.SetModelExecutionPolicy(ModelExecutionPolicy{MaxCalls: 2, MaxInputBytes: 1024, CallTimeout: 10 * time.Millisecond}))
	ctx := executor.withModelQueryBudget(context.Background())
	result, err := invokeGeneration(ctx, cancellationGenerationRuntime(func(ctx context.Context, _ GenerationRequest) (GenerationResult, error) {
		_, hasDeadline := ctx.Deadline()
		require.True(t, hasDeadline)
		<-ctx.Done()
		return GenerationResult{Text: "late answer"}, nil
	}), GenerationRequest{Prompt: "prompt"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, result.Text)
	require.NoError(t, ctx.Err(), "a per-call timeout must not cancel unrelated statement work")
}

func TestModelBudgetSharedAcrossEmbeddingAndGeneration(t *testing.T) {
	executor := &XMySQLExecutor{}
	policy := ModelExecutionPolicy{MaxCalls: 10, MaxInputBytes: 5, CallTimeout: time.Second}
	require.NoError(t, executor.SetModelExecutionPolicy(policy))
	ctx := executor.withModelQueryBudget(context.Background())
	// Updating the server policy must not increase a running query's allowance.
	require.NoError(t, executor.SetModelExecutionPolicy(DefaultModelExecutionPolicy()))
	_, err := invokeEmbedding(ctx, cancellationEmbeddingRuntime(func(context.Context, EmbeddingRequest) (EmbeddingResult, error) {
		return EmbeddingResult{Elements: []float32{1}}, nil
	}), EmbeddingRequest{Text: "ab", Media: []byte{1, 2, 3}})
	require.NoError(t, err, "the exact byte limit is allowed")
	calls := 0
	_, err = invokeGeneration(ctx, cancellationGenerationRuntime(func(context.Context, GenerationRequest) (GenerationResult, error) {
		calls++
		return GenerationResult{}, nil
	}), GenerationRequest{Prompt: "x"})
	require.ErrorIs(t, err, ErrModelBudgetExceeded)
	require.Zero(t, calls)
}
