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
	"github.com/zhukovaskychina/xmysql-server/server/backup"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestPhysicalBackupRestoresContentInIndependentDirectory(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	configuration := func(dir string) *conf.Cfg {
		cfg := conf.NewCfg()
		cfg.DataDir, cfg.InnodbDataDir = dir, dir
		cfg.InnodbRedoLogDir, cfg.InnodbUndoLogDir = filepath.Join(dir, "redo"), filepath.Join(dir, "undo")
		cfg.InnodbBufferPoolSize, cfg.InnodbPageSize = 16<<20, 16384
		cfg.InnodbDoublewrite = false
		return cfg
	}
	source := NewXMySQLEngine(configuration(sourceDir))
	t.Cleanup(func() { _ = source.Close() })
	require.NoError(t, source.Start(context.Background()))
	mustExecSQL(t, source, "", "CREATE DATABASE app")
	mustExecSQL(t, source, "app", "CREATE TABLE docs (id INT PRIMARY KEY, body CONTENT)")
	payload := strings.Repeat("content backup retains source bytes\n", 4096)
	digest := sha256.Sum256([]byte(payload))
	mustExecSQL(t, source, "app", "INSERT INTO docs VALUES (1, CONTENT_FROM_BYTES('"+payload+"', 'text/plain'))")
	// Distinct logical row/object revisions exercise the v3 reference envelope.
	mustExecSQL(t, source, "app", "UPDATE docs SET body=body WHERE id=1")
	mustExecSQL(t, source, "app", "CREATE TABLE blocks (id INT PRIMARY KEY, block_value CONTENT)")
	mustExecSQL(t, source, "app", "INSERT INTO blocks SELECT id, CONTENT_TEXT_BLOCK(body, 0, 0, 32) FROM docs")
	require.Equal(t, [][]interface{}{{"1", payload[:32]}}, mustQuerySQL(t, source, "app", "SELECT CONTENT_BLOCK_MATCHES_SOURCE(b.block_value, d.body), CONTENT_BLOCK_VERIFIED_TEXT(b.block_value, d.body) FROM docs d JOIN blocks b ON d.id=b.id"))
	require.NoError(t, source.Close())
	archive := filepath.Join(root, "snapshot.xmb")
	manifest, err := source.CreatePhysicalBackup(context.Background(), archive)
	require.NoError(t, err)
	var segments, objectManifests, dependencyFiles int
	var firstSegment string
	for _, entry := range manifest.Entries {
		if strings.HasPrefix(entry.Path, contentSegmentStoreDirectory+"/") {
			if strings.HasSuffix(entry.Path, ".seg") {
				segments++
				if firstSegment == "" {
					firstSegment = entry.Path
				}
			}
			if strings.HasSuffix(entry.Path, "/manifest.json") {
				objectManifests++
			}
			if strings.Contains(entry.Path, "/dependencies.") {
				dependencyFiles++
			}
		}
	}
	require.Positive(t, segments)
	require.Equal(t, 1, objectManifests, "row revision change must not duplicate the source object")
	require.Positive(t, dependencyFiles)
	_, err = backup.VerifyPhysicalBackup(archive)
	require.NoError(t, err)
	// Both paths are explicitly inside this test's temporary root. Moving the
	// closed source prevents a restore from accidentally reading source files.
	require.Equal(t, root, filepath.Dir(sourceDir))
	require.NoError(t, os.Rename(sourceDir, filepath.Join(root, "offline-source")))
	restoredDir := filepath.Join(root, "restored")
	require.NoError(t, backup.RestorePhysicalBackup(context.Background(), archive, restoredDir))
	restored := NewXMySQLEngine(configuration(restoredDir))
	t.Cleanup(func() { require.NoError(t, restored.Close()) })
	require.NoError(t, restored.Start(context.Background()))
	rows := mustQuerySQL(t, restored, "app", "SELECT CONTENT_BYTES(body), CONTENT_LENGTH(body), CONTENT_SHA256(body), CONTENT_REVISION(body) FROM docs WHERE id=1")
	require.Equal(t, [][]interface{}{{payload, strconv.Itoa(len(payload)), hex.EncodeToString(digest[:]), "2"}}, rows)
	blocks := mustQuerySQL(t, restored, "app", "SELECT CONTENT_BLOCK_VERIFIED_TEXT(b.block_value, d.body) FROM docs d JOIN blocks b ON d.id=b.id")
	require.Equal(t, [][]interface{}{{payload[:32]}}, blocks, "restored derived evidence must still bind to the restored source")
	segmentPath := filepath.Join(restoredDir, filepath.FromSlash(firstSegment))
	require.True(t, strings.HasPrefix(segmentPath, restoredDir+string(filepath.Separator)))
	segmentBytes, err := os.ReadFile(segmentPath)
	require.NoError(t, err)
	corrupt := append([]byte(nil), segmentBytes...)
	corrupt[0] ^= 1
	require.NoError(t, os.WriteFile(segmentPath, corrupt, 0600))
	failed := <-restored.ExecuteQuery(nil, "SELECT CONTENT_SLICE(d.body, 0, 1) FROM docs d JOIN blocks b ON d.id=b.id", "app")
	require.NoError(t, os.WriteFile(segmentPath, segmentBytes, 0600))
	require.Error(t, failed.Err, "corrupted source reads in JOIN must fail rather than become NULL")
	mustExecSQL(t, restored, "app", "UPDATE docs SET body=CONTENT_FROM_BYTES('replacement', 'text/plain') WHERE id=1")
	require.Equal(t, [][]interface{}{{"STALE"}}, mustQuerySQL(t, restored, "app", "SELECT CONTENT_BLOCK_STATE(block_value) FROM blocks"), "restored dependencies must participate in later DML invalidation")
}
