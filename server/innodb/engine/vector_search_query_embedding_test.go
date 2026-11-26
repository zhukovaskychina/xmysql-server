package engine

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLVectorSearchEmbedsTextAndMultimodalQueryInputs(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, filepath.Join(t.TempDir(), "db"))
	mustExecSQL(t, engine, "", "CREATE DATABASE app")
	mustExecSQL(t, engine, "app", "CREATE TABLE docs (id INT PRIMARY KEY AUTO_INCREMENT, body CONTENT, embedding VECTOR(2))")
	mustExecSQL(t, engine, "app", "CREATE TRIGGER docs_normalize BEFORE INSERT ON docs FOR EACH ROW SET NEW.body = CONTENT_FROM_BYTES('alpha', 'text/plain')")
	mustExecSQL(t, engine, "app", "INSERT INTO docs (body, embedding) VALUES (CONTENT_FROM_BYTES('original', 'text/plain'), VECTOR_FROM_SOURCE('{\"model_id\":\"text-model\",\"model_version\":\"1\",\"space_id\":\"text-space\",\"source_modality\":\"text\",\"elements\":[1,0]}', body))")
	mustExecSQL(t, engine, "app", "CREATE VECTOR INDEX docs_embedding_idx ON docs (embedding) USING HNSW")

	runtime := &queryEmbeddingRuntime{result: EmbeddingResult{
		ModelID: "text-model", ModelVersion: "1", SpaceID: "text-space", Elements: []float32{0.98, 0.02},
	}}
	engine.QueryExecutor.SetEmbeddingRuntime(runtime)
	rows := mustQuerySQL(t, engine, "app", "SELECT v.entry_id, v.primary_key FROM TABLE(VECTOR_SEARCH('app', 'docs', 'docs_embedding_idx', AI_EMBED('text_encoder', 'find alpha'), 1, 'EXACT')) AS v")
	require.Len(t, rows, 1)
	require.Equal(t, 1, runtime.calls)
	require.Equal(t, "find alpha", runtime.request.Text)
	require.Equal(t, "text", runtime.request.Modality)
	require.Len(t, rows[0], 2)
	require.NotEmpty(t, rows[0][0])
	require.Equal(t, "1", rows[0][1])

	runtime.calls = 0
	rows = mustQuerySQL(t, engine, "app", "SELECT v.entry_id, v.primary_key FROM TABLE(VECTOR_SEARCH('app', 'docs', 'docs_embedding_idx', AI_EMBED('joint_encoder', CONTENT_FROM_BYTES(X'89504E47', 'image/png')), 1, 'EXACT')) AS v")
	require.Len(t, rows, 1)
	require.Equal(t, 1, runtime.calls)
	require.Equal(t, "image", runtime.request.Modality)
	require.Equal(t, "image/png", runtime.request.MIMEType)
	require.Equal(t, []byte{0x89, 0x50, 0x4e, 0x47}, runtime.request.Media)
	require.Equal(t, "1", rows[0][1])
}
