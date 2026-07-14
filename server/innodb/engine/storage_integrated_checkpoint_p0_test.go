package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func TestCheckpointManager_WriteGateBlocksAndUnblocksWritePermits(t *testing.T) {
	cm := NewCheckpointManager(t.TempDir(), nil)

	cm.blockWritesForCheckpoint()
	if !cm.IsWriteBlocked() {
		t.Fatal("expected write gate to be blocked")
	}

	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- cm.WaitForWritePermit(context.Background())
	}()

	select {
	case err := <-waiterDone:
		t.Fatalf("write permit returned while blocked: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	cm.unblockWritesForCheckpoint()
	if cm.IsWriteBlocked() {
		t.Fatal("expected write gate to be unblocked")
	}

	select {
	case err := <-waiterDone:
		if err != nil {
			t.Fatalf("write permit returned error after unblock: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("write permit did not resume after unblock")
	}
}

func TestCheckpointManager_WriteSharpCheckpointUnblocksOnSuccess(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "xmysql_checkpoint_sharp_success_test")
	defer os.RemoveAll(tempDir)

	cm := NewCheckpointManager(tempDir, nil)
	if err := cm.Start(context.Background()); err != nil {
		t.Fatalf("start checkpoint manager: %v", err)
	}
	defer cm.Stop()

	if err := cm.WriteSharpCheckpoint(9001); err != nil {
		t.Fatalf("write sharp checkpoint: %v", err)
	}

	if cm.IsWriteBlocked() {
		t.Fatal("write gate remained blocked after successful sharp checkpoint")
	}

	latest, err := cm.ReadLatestCheckpoint()
	if err != nil {
		t.Fatalf("read latest checkpoint: %v", err)
	}
	if latest.CheckpointType != "Sharp" || latest.LSN != 9001 {
		t.Fatalf("unexpected checkpoint: type=%s lsn=%d", latest.CheckpointType, latest.LSN)
	}
}

func TestCheckpointManager_WriteSharpCheckpointUnblocksOnError(t *testing.T) {
	cm := NewCheckpointManager(t.TempDir(), nil)

	if err := cm.WriteSharpCheckpoint(9002); err == nil {
		t.Fatal("expected sharp checkpoint to fail when manager is not running")
	}

	if cm.IsWriteBlocked() {
		t.Fatal("write gate remained blocked after failed sharp checkpoint")
	}
}

func TestStorageIntegratedDMLWriteReturnsContextErrorWhenCheckpointGateTimesOut(t *testing.T) {
	cm := NewCheckpointManager(t.TempDir(), nil)
	cm.blockWritesForCheckpoint()
	defer cm.unblockWritesForCheckpoint()

	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	dml.checkpointManager = cm

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := dml.insertRowToStorage(
		ctx,
		nil,
		&InsertRowData{ColumnValues: map[string]interface{}{"id": int64(1)}},
		nil,
		&manager.TableStorageInfo{SpaceID: 1, RootPageNo: 1},
		nil,
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline from checkpoint gate, got %v", err)
	}
}
