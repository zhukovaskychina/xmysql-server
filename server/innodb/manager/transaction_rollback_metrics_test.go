package manager

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingRollbackExecutor struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *blockingRollbackExecutor) InsertRecord(tableID, recordID uint64, data []byte) error {
	return nil
}

func (e *blockingRollbackExecutor) UpdateRecord(tableID, recordID uint64, data, columnBitmap []byte) error {
	return nil
}

func (e *blockingRollbackExecutor) DeleteRecord(tableID, recordID uint64, primaryKeyData []byte) error {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	return nil
}

func TestTransactionManagerReportsActiveRollbacks(t *testing.T) {
	tm, err := NewTransactionManager(t.TempDir()+"/redo", t.TempDir()+"/undo")
	require.NoError(t, err)
	defer func() { require.NoError(t, tm.Close()) }()

	executor := &blockingRollbackExecutor{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	tm.GetUndoLogManager().SetRollbackExecutor(executor)

	trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	require.NoError(t, err)
	require.NoError(t, tm.GetUndoLogManager().Append(&UndoLogEntry{
		LSN:      1,
		TrxID:    trx.ID,
		TableID:  1,
		RecordID: 1,
		Type:     LOG_TYPE_INSERT,
		Data:     []byte("rollback-me"),
	}))

	rollbackDone := make(chan error, 1)
	go func() { rollbackDone <- tm.Rollback(trx) }()

	select {
	case <-executor.entered:
	case <-time.After(time.Second):
		t.Fatal("rollback executor was not entered")
	}
	require.Equal(t, int64(1), tm.GetActiveRollbacks())

	close(executor.release)
	require.NoError(t, <-rollbackDone)
	require.Equal(t, int64(0), tm.GetActiveRollbacks())
}
