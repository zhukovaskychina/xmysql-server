package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreparedSelectMetadataUsesCatalogAndFunctionContracts(t *testing.T) {
	db := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, db, "", "CREATE DATABASE app")
	mustExecSQL(t, db, "app", "CREATE TABLE lhs (id INT PRIMARY KEY, body CONTENT, embedding VECTOR(2))")
	mustExecSQL(t, db, "app", "CREATE TABLE rhs (id BIGINT PRIMARY KEY, label VARCHAR(20))")
	executor := db.QueryExecutor
	executor.SetGenerationRuntime(cancellationGenerationRuntime(func(context.Context, GenerationRequest) (GenerationResult, error) {
		t.Error("metadata description invoked the generation provider")
		return GenerationResult{}, nil
	}))
	executor.SetEmbeddingRuntime(cancellationEmbeddingRuntime(func(context.Context, EmbeddingRequest) (EmbeddingResult, error) {
		t.Error("metadata description invoked the embedding provider")
		return EmbeddingResult{}, nil
	}))
	for _, test := range []struct {
		query   string
		columns []string
		types   []string
	}{
		{"SELECT AI_GENERATE('prompt','model') AS answer, AI_EMBED('model','prompt') AS embedding", []string{"answer", "embedding"}, []string{"varchar", "vector"}},
		{"SELECT AI_SUMMARIZE(body,'model') AS summary, AI_EMBED('model',body) AS embedding FROM lhs", []string{"summary", "embedding"}, []string{"varchar", "vector"}},
		{"SELECT l.id, AI_GENERATE('prompt','model') AS answer, l.body FROM lhs l JOIN rhs r ON l.id=r.id", []string{"id", "answer", "body"}, []string{"int", "varchar", "content"}},
		{"SELECT * FROM lhs RIGHT JOIN rhs USING(id)", []string{"id", "label", "body", "embedding"}, []string{"bigint", "varchar", "content", "vector"}},
		{"SELECT lhs.*, rhs.id AS other_id FROM lhs NATURAL JOIN rhs", []string{"id", "body", "embedding", "other_id"}, []string{"int", "content", "vector", "bigint"}},
		{"SELECT AI_EMBED('model','a') AS embedding UNION ALL SELECT AI_EMBED('model','b')", []string{"embedding"}, []string{"vector"}},
		{"SELECT a.* FROM (SELECT body, AI_GENERATE('prompt','model') AS answer FROM lhs) a", []string{"body", "answer"}, []string{"content", "varchar"}},
		{"WITH a(value) AS (SELECT AI_EMBED('model','a')) SELECT value AS embedding FROM a", []string{"embedding"}, []string{"vector"}},
		{"SELECT (SELECT AI_GENERATE('prompt','model')) AS answer, id <=> NULL AS missing FROM lhs", []string{"answer", "missing"}, []string{"varchar", "bigint"}},
		{"SELECT AI_EMBED_BLOCK(body,body,'model') AS embedding, AI_DESCRIBE_BLOCK(body,body,'model') AS answer FROM lhs", []string{"embedding", "answer"}, []string{"vector", "varchar"}},
		{"SELECT id FROM lhs WHERE AI_GENERATE('prompt','model')='answer'", []string{"id"}, []string{"int"}},
		{"SELECT v.entry_id, v.distance, v.primary_key, v.approximate FROM TABLE(VECTOR_SEARCH('app','lhs','idx',AI_EMBED('model','query'),1,'EXACT')) v", []string{"entry_id", "distance", "primary_key", "approximate"}, []string{"varchar", "double", "longblob", "tinyint"}},
		{"SELECT token, position FROM TABLE(FULLTEXT_TOKENS('alpha'))", []string{"token", "position"}, []string{"varchar", "bigint"}},
		{"SELECT c.* FROM lhs, TABLE(CONTENT_TEXT_CHUNKS(body,64)) c", []string{"ordinal", "text", "block", "byte_start", "byte_end"}, []string{"bigint", "varchar", "content", "bigint", "bigint"}},
	} {
		t.Run(test.query, func(t *testing.T) {
			result, err := executor.DescribePreparedSelect(nil, test.query, "app")
			require.NoError(t, err)
			require.Equal(t, test.columns, result.Columns)
			require.Equal(t, test.types, result.ColumnTypes)
			require.Empty(t, result.Records, "metadata description must not materialize results")
		})
	}

	// Unknown effectful functions are not executed as a type-inference fallback.
	for _, query := range []string{"SELECT unknown_function()", "SELECT SLEEP(60)", "SELECT id FROM lhs l JOIN rhs r ON l.id=r.id", "SELECT 1 UNION ALL SELECT 'text'"} {
		_, err := executor.DescribePreparedSelect(nil, query, "app")
		require.Error(t, err, query)
	}
}

func TestEmptyNativeTableFunctionsRetainDeclaredResultMetadata(t *testing.T) {
	db := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, db, "", "CREATE DATABASE app")
	mustExecSQL(t, db, "app", "CREATE TABLE docs (id INT PRIMARY KEY, body CONTENT)")
	for _, test := range []struct {
		query          string
		columns, types []string
	}{
		{"SELECT t.token, t.position FROM TABLE(FULLTEXT_TOKENS('')) t", []string{"token", "position"}, []string{"varchar", "bigint"}},
		{"SELECT t.* FROM TABLE(FULLTEXT_TOKENS('')) t", []string{"token", "position"}, []string{"varchar", "bigint"}},
		{"SELECT c.* FROM docs, TABLE(CONTENT_TEXT_CHUNKS(body,64)) c", []string{"ordinal", "text", "block", "byte_start", "byte_end"}, []string{"bigint", "varchar", "content", "bigint", "bigint"}},
		{"SELECT d.id,c.block FROM docs d, TABLE(CONTENT_TEXT_CHUNKS(d.body,64)) c", []string{"id", "block"}, []string{"int", "content"}},
	} {
		t.Run(test.query, func(t *testing.T) {
			result := <-db.ExecuteQuery(nil, test.query, "app")
			require.NoError(t, result.Err)
			selected, ok := result.Data.(*SelectResult)
			require.True(t, ok)
			require.Empty(t, selected.Records)
			require.Equal(t, test.columns, selected.Columns)
			require.Equal(t, test.types, selected.ColumnTypes)
		})
	}
	mustExecSQL(t, db, "app", "INSERT INTO docs VALUES (1,CONTENT_FROM_BYTES('alpha','text/plain'))")
	result := <-db.ExecuteQuery(nil, "SELECT d.id,c.block FROM docs d, TABLE(CONTENT_TEXT_CHUNKS(d.body,64)) c", "app")
	require.NoError(t, result.Err)
	selected, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, selected.Records, 1)
	require.Equal(t, []string{"id", "block"}, selected.Columns)
	require.Equal(t, []string{"int", "content"}, selected.ColumnTypes, "populated and empty results must use the same SQL types")
}
