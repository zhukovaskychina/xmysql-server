package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestMetadataPublishSurvivesOpenReader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "event_log.frm")
	require.NoError(t, os.WriteFile(path, []byte("old-identity"), 0644))
	reader, err := os.Open(path)
	require.NoError(t, err)
	defer reader.Close()

	published := []byte("new-identity")
	require.NoError(t, writeMetadataFileAtomic(path, published))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, published, got)
}

func TestDMLRootPagePublishKeepsConcurrentReadersValid(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "app"), 0755))
	path := filepath.Join(dir, "app", "rows.frm")
	raw, err := json.Marshal(map[string]interface{}{"payload": strings.Repeat("x", 256*1024), "storage_root_page": 0})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0644))
	dml := &StorageIntegratedDMLExecutor{dataDir: dir, schemaName: "app", tableName: "rows"}
	done := make(chan error, 1)
	go func() {
		for root := uint32(1); root <= 64; root++ {
			if err := dml.persistTableRootPage(&manager.TableStorageInfo{RootPageNo: root, SpaceID: 1}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	var readerErr error
	for {
		select {
		case err := <-done:
			require.NoError(t, err)
			require.NoError(t, readerErr)
			return
		default:
			raw, err := metadata.ReadFile(path)
			if readerErr == nil {
				if err != nil {
					readerErr = err
				} else if !json.Valid(raw) {
					readerErr = fmt.Errorf("reader observed incomplete metadata (%d bytes)", len(raw))
				}
			}
		}
	}
}
