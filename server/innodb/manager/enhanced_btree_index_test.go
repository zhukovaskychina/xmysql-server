package manager

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestEnhancedBTreeAdapterFullScanDoesNotReadSidecarWithoutIndex(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewStorageManager(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        filepath.Join(dataDir, "innodb"),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
	adapter := NewEnhancedBTreeAdapter(storage, DefaultBTreeConfig)
	adapter.spaceID = 7

	sidecarDir := filepath.Join(dataDir, "innodb", "_xmysql_btree_records")
	if err := os.MkdirAll(sidecarDir, 0755); err != nil {
		t.Fatalf("mkdir sidecar dir: %v", err)
	}
	sidecar := `[{"Key":"azE=","Value":"djE=","PageNo":1,"SlotNo":0,"TxnID":0,"DeleteMark":false}]`
	if err := os.WriteFile(filepath.Join(sidecarDir, "space_7_page_1.json"), []byte(sidecar), 0644); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}

	rows, err := adapter.FullScan(context.Background())
	if err == nil {
		t.Fatalf("expected missing index error instead of sidecar rows, got rows=%d", len(rows))
	}
	if !strings.Contains(err.Error(), "failed to get index") {
		t.Fatalf("expected missing index error, got %v", err)
	}
}

func TestSimpleRow_SetTransactionId(t *testing.T) {
	row := &SimpleRow{data: []byte{1, 2, 3, 4, 5}}

	row.SetTransactionId(12345)

	assert.Len(t, row.data, 13)
	assert.Equal(t, byte(1), row.data[0])
	assert.Equal(t, byte(2), row.data[1])
	assert.Equal(t, byte(3), row.data[2])
	assert.Equal(t, byte(4), row.data[3])
	assert.Equal(t, byte(5), row.data[4])
	assert.Equal(t, uint64(12345), binary.LittleEndian.Uint64(row.data[5:13]))
}
