package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestContentSourcePrimaryKeyAgreesAcrossInsertAndJoin(t *testing.T) {
	for _, key := range []interface{}{int64(1), "source-key", []byte{0, 255, 1}} {
		meta := &metadata.TableMeta{Name: "docs", PrimaryKey: []string{"id"}, Columns: []*metadata.ColumnMeta{{Name: "id", IsPrimary: true}}}
		row := &InsertRowData{ColumnValues: map[string]interface{}{"id": key}}
		require.NoError(t, bindPendingInsertSourceContext(row, meta, "app"))
		insertContext := row.ColumnValues[contentSourceRowContextKey].(contentSourceRowContext)
		joined := qualifiedJoinRow(&joinTableSource{schemaName: "app", tableName: "docs", alias: "d", meta: meta}, row.ColumnValues)
		joinContext := joined[contentSourceRowContextKeyForAlias("d")].(contentSourceRowContext)
		require.Equal(t, insertContext.PrimaryKey, joinContext.PrimaryKey)
		sourceKey, exists, err := contentSourcePrimaryKey(row.ColumnValues, meta)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, sourceKey, insertContext.PrimaryKey)
	}
	meta := &metadata.TableMeta{Name: "docs", PrimaryKey: []string{"a", "b"}}
	values := map[string]interface{}{"a": "x", "b": int64(2)}
	key, exists, err := contentSourcePrimaryKey(values, meta)
	require.NoError(t, err)
	require.True(t, exists)
	physical, err := buildCompositeKey(values, meta.PrimaryKey)
	require.NoError(t, err)
	require.Equal(t, physical, key, "multi-column source identities retain the established tuple encoding")
}

func TestSameRowBlockInvalidationUsesCanonicalSourceIdentity(t *testing.T) {
	before, err := content.NewValue("text/plain", 1, []byte("before"))
	require.NoError(t, err)
	after, err := content.NewValue("text/plain", 2, []byte("after"))
	require.NoError(t, err)
	ref, err := content.NewSourceRef("app", "docs", "body", []byte("1"), before)
	require.NoError(t, err)
	block, err := content.NewBlock(ref, 0, content.BlockText, "before", content.Locator{}, "parser", "1", "", "")
	require.NoError(t, err)
	value, err := content.BlockValue(block)
	require.NoError(t, err)
	existing := &InsertRowData{ColumnValues: map[string]interface{}{"id": int64(1), "body": before, "derived": value}}
	updated := &InsertRowData{ColumnValues: cloneTransactionRow(existing.ColumnValues)}
	updated.ColumnValues["body"] = after
	meta := &metadata.TableMeta{Name: "docs", PrimaryKey: []string{"id"}, Columns: []*metadata.ColumnMeta{{Name: "body", Type: metadata.TypeContent}, {Name: "derived", Type: metadata.TypeContent}}}
	dml := &StorageIntegratedDMLExecutor{schemaName: "app", tableName: "docs"}
	require.NoError(t, dml.invalidateChangedSourceBlocks(existing, updated, meta))
	changed, ok := dmlContentValue(updated.ColumnValues, "derived")
	require.True(t, ok)
	decoded, err := content.BlockFromValue(changed)
	require.NoError(t, err)
	require.Equal(t, content.BlockStateStale, decoded.State)
	unchanged, err := content.BlockFromValue(value)
	require.NoError(t, err)
	require.Equal(t, content.BlockStateReady, unchanged.State)
}
