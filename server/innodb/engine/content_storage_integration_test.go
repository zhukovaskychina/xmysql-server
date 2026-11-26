package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func contentStoredObjectIDs(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != "maintenance" {
			ids = append(ids, entry.Name())
		}
	}
	return ids
}

func TestContentUpdatePreservesSegmentedReference(t *testing.T) {
	store, err := content.NewSegmentStore(filepath.Join(t.TempDir(), "objects"), content.SegmentStoreOptions{})
	require.NoError(t, err)
	payload := strings.Repeat("segment-reference-", 2048)
	ref, err := store.Put(context.Background(), "application/octet-stream", 7, strings.NewReader(payload))
	require.NoError(t, err)
	external, err := content.NewSegmentedValue(ref)
	require.NoError(t, err)

	tableMeta := &metadata.TableMeta{
		Name:    "content_rows",
		Columns: []*metadata.ColumnMeta{{Name: "body", Type: metadata.TypeContent}},
	}
	updated, err := (&StorageIntegratedDMLExecutor{}).applyUpdateExpressions(
		&InsertRowData{ColumnValues: map[string]interface{}{"body": nil}},
		[]*UpdateExpression{{ColumnName: "body", NewValue: external, ColumnType: metadata.TypeContent}},
		tableMeta,
	)
	require.NoError(t, err)
	value, ok := dmlContentValue(updated.ColumnValues, "body")
	require.True(t, ok)
	require.NotNil(t, value.ExternalRef)
	require.Equal(t, ref.ID, value.ExternalRef.ID)
	require.Equal(t, uint64(len(payload)), value.Size())
	require.Equal(t, uint64(7), value.Revision)
}

func TestLargeContentExternalStorageSurvivesEngineRestart(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &conf.Cfg{
		DataDir: dataDir, InnodbDataDir: dataDir,
		InnodbRedoLogDir: filepath.Join(dataDir, "redo"), InnodbUndoLogDir: filepath.Join(dataDir, "undo"),
		InnodbBufferPoolSize: 16 * 1024 * 1024, InnodbPageSize: 16384,
	}
	first := NewXMySQLEngine(cfg)
	t.Cleanup(func() {
		if first != nil {
			_ = first.Close()
		}
	})
	mustExecSQL(t, first, "", "CREATE DATABASE app")
	mustExecSQL(t, first, "app", "CREATE TABLE content_rows (id INT PRIMARY KEY, body CONTENT)")

	payload := strings.Repeat("content-survives-restart-", (int(content.DefaultInlineThreshold)+2048)/25)
	digest := sha256.Sum256([]byte(payload))
	digestHex := hex.EncodeToString(digest[:])
	session := newTestMySQLSession()
	mustExecSessionSQL(t, first, session, "app", "BEGIN")
	mustExecSessionSQL(t, first, session, "app", "INSERT INTO content_rows VALUES (2, CONTENT_FROM_BYTES('"+payload+"', 'text/plain'))")
	objects := contentStoredObjectIDs(t, filepath.Join(dataDir, contentSegmentStoreDirectory))
	require.Len(t, objects, 1, "transaction should stage one immutable large object before commit")
	mustExecSessionSQL(t, first, session, "app", "ROLLBACK")
	objects = contentStoredObjectIDs(t, filepath.Join(dataDir, contentSegmentStoreDirectory))
	require.Empty(t, objects, "rolled-back CONTENT must not leave a published orphan")
	require.Empty(t, mustQuerySessionSQL(t, first, session, "app", "SELECT id FROM content_rows"))

	mustExecSQL(t, first, "app", "INSERT INTO content_rows VALUES (1, CONTENT_FROM_BYTES('"+payload+"', 'text/plain'))")

	rows := mustQuerySQL(t, first, "app", "SELECT CONTENT_LENGTH(body), CONTENT_MIME(body), CONTENT_SHA256(body), CONTENT_BYTES(body) FROM content_rows WHERE id=1")
	require.Len(t, rows, 1)
	require.Equal(t, []interface{}{strconv.Itoa(len(payload)), "text/plain", digestHex, payload}, rows[0])
	objects = contentStoredObjectIDs(t, filepath.Join(dataDir, contentSegmentStoreDirectory))
	require.Len(t, objects, 1, "large CONTENT should use the database-owned segmented store")
	objectID := objects[0]
	// A SQL assignment advances the row revision while reusing the immutable
	// object. The separate revisions must survive the clustered-row codec.
	mustExecSQL(t, first, "app", "UPDATE content_rows SET body=body WHERE id=1")

	require.NoError(t, first.Close())
	first = nil

	second := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	rows = mustQuerySQL(t, second, "app", "SELECT CONTENT_LENGTH(body), CONTENT_MIME(body), CONTENT_SHA256(body), CONTENT_BYTES(body) FROM content_rows WHERE id=1")
	require.Len(t, rows, 1)
	require.Equal(t, []interface{}{strconv.Itoa(len(payload)), "text/plain", digestHex, payload}, rows[0])

	refRows := mustQuerySQL(t, second, "app", "SELECT CONTENT_KIND(body), CONTENT_REVISION(body) FROM content_rows WHERE id=1")
	require.Equal(t, [][]interface{}{{"document", "2"}}, refRows)
	objectIDs := contentStoredObjectIDs(t, filepath.Join(dataDir, contentSegmentStoreDirectory))
	require.Equal(t, []string{objectID}, objectIDs, "restart must reuse the same committed source object; dependency catalog files are not objects")
}
