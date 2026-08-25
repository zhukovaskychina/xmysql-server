package manager

import (
	"testing"

	"github.com/stretchr/testify/require"
	storepages "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
)

func TestCrashRecoveryRedoUndoAndPageChecksumClosure(t *testing.T) {
	logDir := t.TempDir()

	redoLogManager, err := NewRedoLogManager(logDir, 100)
	require.NoError(t, err)
	defer redoLogManager.Close()

	undoLogManager, err := NewUndoLogManager(logDir)
	require.NoError(t, err)
	defer undoLogManager.Close()

	rollbackExecutor := &MockRollbackExecutor{
		insertedRecords: make(map[uint64][]byte),
		updatedRecords:  make(map[uint64][]byte),
		deletedRecords:  make(map[uint64]bool),
	}
	undoLogManager.SetRollbackExecutor(rollbackExecutor)

	storage := NewMockStorage()
	initialPage := make([]byte, 16384)
	copy(initialPage[512:], []byte("existing tail must survive"))
	require.NoError(t, repairRecoveryPageChecksum(initialPage))
	require.NoError(t, storage.WritePage(42, initialPage))

	_, err = redoLogManager.Append(&RedoLogEntry{TrxID: 1, Type: LOG_TYPE_TXN_BEGIN})
	require.NoError(t, err)

	redoImage := make([]byte, 256)
	copy(redoImage[64:], []byte("committed redo payload"))
	committedLSN, err := redoLogManager.Append(&RedoLogEntry{
		TrxID:  1,
		PageID: 42,
		Type:   LOG_TYPE_PAGE_MODIFY,
		Data:   redoImage,
	})
	require.NoError(t, err)

	_, err = redoLogManager.Append(&RedoLogEntry{TrxID: 1, Type: LOG_TYPE_TXN_COMMIT})
	require.NoError(t, err)

	_, err = redoLogManager.Append(&RedoLogEntry{TrxID: 2, Type: LOG_TYPE_TXN_BEGIN})
	require.NoError(t, err)

	_, err = redoLogManager.Append(&RedoLogEntry{
		TrxID:  2,
		PageID: 43,
		Type:   LOG_TYPE_PAGE_MODIFY,
		Data:   []byte("uncommitted redo payload"),
	})
	require.NoError(t, err)

	require.NoError(t, undoLogManager.Append(&UndoLogEntry{
		LSN:      1,
		TrxID:    2,
		TableID:  7,
		RecordID: 99,
		Type:     LOG_TYPE_INSERT,
		Data:     []byte("pk-99"),
	}))

	require.NoError(t, redoLogManager.Flush(1000))

	recovery := NewCrashRecovery(redoLogManager, undoLogManager, 0)
	recovery.SetStorageManager(storage)

	require.NoError(t, recovery.Recover())

	recoveredPage, err := storage.ReadPage(42)
	require.NoError(t, err)
	require.Equal(t, committedLSN, recoveryPageLSN(recoveredPage))
	require.Contains(t, string(recoveredPage), "committed redo payload")
	require.Contains(t, string(recoveredPage), "existing tail must survive")
	require.NoError(t, storepages.NewPageIntegrityChecker(storepages.ChecksumCRC32).ValidateChecksum(recoveredPage))
	require.True(t, rollbackExecutor.deletedRecords[99], "active transaction insert must be undone")

	status := recovery.GetRecoveryStatus()
	require.True(t, status.AnalysisComplete)
	require.True(t, status.RedoComplete)
	require.True(t, status.UndoComplete)
}
