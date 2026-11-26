package engine

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSQLJoinModelProjectionDoesNotSpendBudgetOnMetadata(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, filepath.Join(t.TempDir(), "db"))
	mustExecSQL(t, engine, "", "CREATE DATABASE app")
	mustExecSQL(t, engine, "app", "CREATE TABLE inputs (id INT PRIMARY KEY, prompt VARCHAR(32), body CONTENT)")
	mustExecSQL(t, engine, "app", "CREATE TABLE labels (id INT PRIMARY KEY, block_value CONTENT)")
	mustExecSQL(t, engine, "app", "INSERT INTO inputs VALUES (1,'alpha',CONTENT_FROM_BYTES('alpha','text/plain')),(2,'beta',CONTENT_FROM_BYTES('beta','text/plain'))")
	mustExecSQL(t, engine, "app", "INSERT INTO labels SELECT id, CONTENT_TEXT_BLOCK(body,0,0,5) FROM inputs")
	require.NoError(t, engine.QueryExecutor.SetModelExecutionPolicy(ModelExecutionPolicy{MaxCalls: 2, MaxInputBytes: 1024, CallTimeout: time.Second}))
	for _, test := range []struct{ expression, resultType string }{
		{"AI_GENERATE(n.prompt, 'model')", "varchar"},
		{"AI_SUMMARIZE(n.prompt, 'model')", "varchar"},
		{"AI_GENERATE_RESULT(n.prompt, 'model')", "varchar"},
		{"AI_EMBED('model', n.prompt)", "vector"},
		{"AI_EMBED('model', n.body)", "vector"},
		{"AI_EMBED_BLOCK(l.block_value, n.body, 'model')", "vector"},
		{"AI_DESCRIBE_BLOCK(l.block_value, n.body, 'model')", "varchar"},
		{"AI_DESCRIBE_BLOCK_RESULT(l.block_value, n.body, 'model')", "varchar"},
	} {
		t.Run(test.expression, func(t *testing.T) {
			calls := 0
			engine.QueryExecutor.SetGenerationRuntime(cancellationGenerationRuntime(func(context.Context, GenerationRequest) (GenerationResult, error) {
				calls++
				return GenerationResult{ModelID: "m", ModelVersion: "1", Text: "answer"}, nil
			}))
			engine.QueryExecutor.SetEmbeddingRuntime(cancellationEmbeddingRuntime(func(context.Context, EmbeddingRequest) (EmbeddingResult, error) {
				calls++
				return EmbeddingResult{ModelID: "m", ModelVersion: "1", SpaceID: "text-space", Elements: []float32{float32(calls), 1}}, nil
			}))
			result := <-engine.ExecuteQuery(nil, "SELECT n.id, "+test.expression+" AS answer FROM inputs n JOIN labels l ON n.id=l.id", "app")
			require.NoError(t, result.Err, "two output rows must fit a two-call model budget")
			require.Equal(t, 2, calls, "type inference must not issue an extra model call")
			metadata, ok := result.Data.(*SelectResult)
			require.True(t, ok)
			require.Len(t, metadata.Records, 2)
			require.Equal(t, test.resultType, metadata.ColumnTypes[1])

			calls = 0
			result = <-engine.ExecuteQuery(nil, "SELECT "+test.expression+" AS answer FROM inputs n JOIN labels l ON n.id=l.id WHERE n.id=-1", "app")
			require.NoError(t, result.Err)
			require.Zero(t, calls)
			metadata, ok = result.Data.(*SelectResult)
			require.True(t, ok)
			require.Empty(t, metadata.Records)
			require.Equal(t, test.resultType, metadata.ColumnTypes[0], "empty results must retain model result metadata")
		})
	}
}
