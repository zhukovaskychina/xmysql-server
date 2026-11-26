package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
)

type cancellationEmbeddingRuntime func(context.Context, EmbeddingRequest) (EmbeddingResult, error)

func (f cancellationEmbeddingRuntime) Embed(ctx context.Context, request EmbeddingRequest) (EmbeddingResult, error) {
	return f(ctx, request)
}

func TestConstantModelSQLUsesExplicitRuntimeAndRejectsIgnoredClauses(t *testing.T) {
	for _, suffix := range []string{"", " WHERE 0", " LIMIT 0", " GROUP BY 1", " HAVING 0", " ORDER BY 1"} {
		t.Run(suffix, func(t *testing.T) {
			calls := 0
			executor := &XMySQLExecutor{}
			executor.SetGenerationRuntime(cancellationGenerationRuntime(func(ctx context.Context, request GenerationRequest) (GenerationResult, error) {
				calls++
				require.Equal(t, "prompt", request.Prompt)
				return GenerationResult{ModelID: "model", ModelVersion: "1", Text: "answer"}, nil
			}))
			statement, err := sqlparser.Parse("SELECT AI_GENERATE('prompt', 'model') AS answer" + suffix)
			require.NoError(t, err)
			result, err := executor.executeSelectStatement(&ExecutionContext{Context: context.Background()}, statement.(*sqlparser.Select), "app")
			if suffix == "" {
				require.NoError(t, err)
				require.Equal(t, []string{"answer"}, result.Columns)
				require.Equal(t, 1, result.RowCount)
				require.Equal(t, 1, calls)
			} else {
				require.ErrorContains(t, err, "not supported")
				require.Nil(t, result)
				require.Zero(t, calls, "unsupported clauses must not issue model calls")
			}
		})
	}
}

type cancellationGenerationRuntime func(context.Context, GenerationRequest) (GenerationResult, error)

func (f cancellationGenerationRuntime) Generate(ctx context.Context, request GenerationRequest) (GenerationResult, error) {
	return f(ctx, request)
}

func TestModelSQLCancellationRejectsLateProviderResults(t *testing.T) {
	source, err := content.NewValue("text/plain", 1, []byte("source evidence"))
	require.NoError(t, err)
	ref, err := content.NewSourceRef("app", "docs", "body", []byte("1"), source)
	require.NoError(t, err)
	block, err := content.NewBlock(ref, 0, content.BlockText, "source evidence", content.Locator{}, "test-parser", "1", "", "")
	require.NoError(t, err)
	blockValue, err := content.BlockValue(block)
	require.NoError(t, err)
	values := map[string]interface{}{
		"body": source, "block": blockValue,
		contentSourceRowContextKey: contentSourceRowContext{Schema: "app", Table: "docs", PrimaryKey: []byte("1")},
	}
	for _, expression := range []string{
		"AI_EMBED('model', 'query')",
		"AI_EMBED('model', body)",
		"AI_EMBED_BLOCK(block, body, 'model')",
		"AI_GENERATE('prompt', 'model')",
		"AI_GENERATE_RESULT('prompt', 'model')",
		"AI_SUMMARIZE('prompt', 'model')",
		"AI_DESCRIBE_BLOCK(block, body, 'model')",
		"AI_DESCRIBE_BLOCK_RESULT(block, body, 'model')",
	} {
		t.Run(expression, func(t *testing.T) {
			for _, phase := range []string{"active", "cancel_before", "cancel_during", "deadline_during"} {
				t.Run(phase, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					if phase == "deadline_during" {
						cancel()
						ctx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
					}
					defer cancel()
					if phase == "cancel_before" {
						cancel()
					}
					calls := 0
					provider := func(providerContext context.Context) {
						calls++
						require.Equal(t, ctx.Err(), providerContext.Err())
						_, hasDeadline := providerContext.Deadline()
						require.True(t, hasDeadline, "model calls must carry a bounded deadline")
						if phase == "cancel_during" {
							cancel()
						} else if phase == "deadline_during" {
							<-providerContext.Done()
						}
						// Deliberately return success even after cancellation.
					}
					se := &SelectExecutor{
						modelContext: ctx,
						embeddingRuntime: cancellationEmbeddingRuntime(func(ctx context.Context, _ EmbeddingRequest) (EmbeddingResult, error) {
							provider(ctx)
							return EmbeddingResult{ModelID: "model", ModelVersion: "1", SpaceID: "text-space", Elements: []float32{1, 0}}, nil
						}),
						generationRuntime: cancellationGenerationRuntime(func(ctx context.Context, _ GenerationRequest) (GenerationResult, error) {
							provider(ctx)
							return GenerationResult{ModelID: "model", ModelVersion: "1", Text: "answer"}, nil
						}),
					}
					value, err := se.evaluateProjectionExpression(expression, values)
					if phase == "active" {
						require.NoError(t, err)
						require.NotNil(t, value)
					} else {
						require.ErrorIs(t, err, ctx.Err())
						if typed, ok := value.(vector.Value); ok {
							require.NotEqual(t, vector.StateReady, typed.State)
						} else {
							require.Nil(t, value)
						}
					}
					if phase == "cancel_before" {
						require.Zero(t, calls, "already cancelled SQL must not call the provider")
					} else {
						require.Equal(t, 1, calls)
					}
				})
			}
		})
	}
}
