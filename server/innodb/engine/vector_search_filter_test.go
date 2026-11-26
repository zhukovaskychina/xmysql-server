package engine

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestParseVectorSearchFiltersValidatesScalarColumnsAndJSON(t *testing.T) {
	table := &metadata.TableMeta{Columns: []*metadata.ColumnMeta{
		{Name: "category", Type: metadata.TypeVarchar, Collation: "utf8mb4_0900_ai_ci"},
		{Name: "published", Type: metadata.TypeTinyInt},
		{Name: "deleted_at", Type: metadata.TypeTimestamp, IsNullable: true},
		{Name: "body", Type: metadata.TypeContent},
		{Name: "embedding", Type: metadata.TypeVector},
	}}
	filters, err := parseVectorSearchFilters(`{"category":"Public","published":true,"deleted_at":null}`, table)
	require.NoError(t, err)
	require.Len(t, filters, 3)
	require.Equal(t, []string{"category", "deleted_at", "published"}, vectorSearchFilterColumnNames(filters))

	for _, encoded := range []string{
		`[]`, `null`, `{"category":[]}`, `{"unknown":1}`, `{"body":"text"}`, `{"embedding":[1,0]}`,
		`{"category":"a","CATEGORY":"b"}`, `{"category":"x"} {}`, strings.Repeat("x", maxVectorSearchFilterJSONBytes+1),
	} {
		_, err := parseVectorSearchFilters(encoded, table)
		require.Error(t, err, "filter %q", encoded)
	}
}

func TestVectorSearchFiltersMatchTypedSourceValues(t *testing.T) {
	table := &metadata.TableMeta{Columns: []*metadata.ColumnMeta{
		{Name: "category", Type: metadata.TypeVarchar, Collation: "utf8mb4_0900_ai_ci"},
		{Name: "score", Type: metadata.TypeInt},
		{Name: "published", Type: metadata.TypeTinyInt},
		{Name: "deleted_at", Type: metadata.TypeTimestamp, IsNullable: true},
	}}
	filters, err := parseVectorSearchFilters(`{"category":"PUBLIC","score":7,"published":true,"deleted_at":null}`, table)
	require.NoError(t, err)
	row := &InsertRowData{ColumnValues: map[string]interface{}{
		"category": "Public", "score": int64(7), "published": int64(1), "deleted_at": nil,
	}}
	matches, err := vectorSearchFiltersMatch(row, filters)
	require.NoError(t, err)
	require.True(t, matches)
	row.ColumnValues["score"] = int64(8)
	matches, err = vectorSearchFiltersMatch(row, filters)
	require.NoError(t, err)
	require.False(t, matches)
}

func TestExactVectorSearchAppliesRowFiltersBeforeTopK(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, filepath.Join(t.TempDir(), "db"))
	mustExecSQL(t, engine, "", "CREATE DATABASE app")
	mustExecSQL(t, engine, "app", "CREATE TABLE docs (id INT PRIMARY KEY, category VARCHAR(32), body CONTENT, embedding VECTOR(2))")
	mustExecSQL(t, engine, "app", "CREATE TRIGGER docs_normalize BEFORE INSERT ON docs FOR EACH ROW SET NEW.body = CONTENT_FROM_BYTES('alpha', 'text/plain')")
	for _, row := range []struct {
		id       int
		category string
		vector   string
	}{
		{1, "other", `{"model_id":"filter-model","model_version":"1","space_id":"filter-space","source_modality":"text","elements":[1,0]}`},
		{2, "target", `{"model_id":"filter-model","model_version":"1","space_id":"filter-space","source_modality":"text","elements":[0.9,0.1]}`},
		{3, "target", `{"model_id":"filter-model","model_version":"1","space_id":"filter-space","source_modality":"text","elements":[0,1]}`},
	} {
		query := "INSERT INTO docs (id,category,body,embedding) VALUES (" + integerLiteral(int64(row.id)) + ", '" + row.category + "', CONTENT_FROM_BYTES('original','text/plain'), VECTOR_FROM_SOURCE('" + strings.ReplaceAll(row.vector, "'", "''") + "', body))"
		mustExecSQL(t, engine, "app", query)
	}
	mustExecSQL(t, engine, "app", "CREATE VECTOR INDEX docs_embedding_idx ON docs (embedding) USING HNSW")
	engine.QueryExecutor.SetEmbeddingRuntime(&queryEmbeddingRuntime{result: EmbeddingResult{
		ModelID: "filter-model", ModelVersion: "1", SpaceID: "filter-space", Elements: []float32{1, 0},
	}})
	queryVector := "AI_EMBED('text_encoder','query')"
	unfiltered := mustQuerySQL(t, engine, "app", "SELECT v.primary_key FROM TABLE(VECTOR_SEARCH('app','docs','docs_embedding_idx',"+queryVector+",1,'EXACT')) v")
	require.Len(t, unfiltered, 1)
	require.Equal(t, "1", unfiltered[0][0])
	filtered := mustQuerySQL(t, engine, "app", "SELECT v.primary_key FROM TABLE(VECTOR_SEARCH('app','docs','docs_embedding_idx',"+queryVector+",1,'EXACT','{\"category\":\"target\"}')) v")
	require.Len(t, filtered, 1)
	require.Equal(t, "2", filtered[0][0], "filter must be applied before exact top-k selection")
}
