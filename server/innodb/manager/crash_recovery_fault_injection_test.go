package manager

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FaultInjector 故障注入器
type FaultInjector struct {
	crashAfterLSN    uint64  // 在指定LSN后崩溃
	crashProbability float64 // 崩溃概率 (0.0-1.0)
	crashed          bool
	mu               sync.RWMutex
}

// NewFaultInjector 创建故障注入器
func NewFaultInjector(crashAfterLSN uint64, crashProbability float64) *FaultInjector {
	return &FaultInjector{
		crashAfterLSN:    crashAfterLSN,
		crashProbability: crashProbability,
	}
}

// ShouldCrash 判断是否应该崩溃
func (f *FaultInjector) ShouldCrash(currentLSN uint64) bool {
	f.mu.RLock()
	if f.crashed {
		f.mu.RUnlock()
		return true
	}
	f.mu.RUnlock()

	if currentLSN >= f.crashAfterLSN {
		f.mu.Lock()
		f.crashed = true
		f.mu.Unlock()
		return true
	}

	return false
}

// IsCrashed 判断是否已崩溃
func (f *FaultInjector) IsCrashed() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.crashed
}

// TestFaultInjection_CrashDuringWrite 测试写入过程中崩溃
func TestFaultInjection_CrashDuringWrite(t *testing.T) {
	tmpDir := t.TempDir()

	// 场景：在写入第5个事务时崩溃
	crashPoint := uint64(4005)
	faultInjector := NewFaultInjector(crashPoint, 1.0)

	// 阶段1: 写入数据直到崩溃
	t.Run("Phase1_WriteUntilCrash", func(t *testing.T) {
		redoMgr, err := NewRedoLogManager(tmpDir, 10000)
		require.NoError(t, err)
		defer redoMgr.Close()

		undoMgr, err := NewUndoLogManager(tmpDir)
		require.NoError(t, err)
		defer undoMgr.Close()

		// 写入10个事务
		crashedAt := -1
		for i := 0; i < 10; i++ {
			txID := int64(4000 + i)
			lsn := uint64(4000 + i*10)

			// 检查是否应该崩溃
			if faultInjector.ShouldCrash(lsn) {
				crashedAt = i
				t.Logf("💥 模拟崩溃: 在事务 %d (LSN=%d) 时崩溃", txID, lsn)
				break
			}

			// 写入事务
			redoMgr.Append(&RedoLogEntry{
				TrxID:  txID,
				PageID: uint64(4000 + i),
				Type:   LOG_TYPE_TXN_BEGIN,
				LSN:    lsn,
				Data:   []byte(fmt.Sprintf("tx%d_begin", txID)),
			})

			redoMgr.Append(&RedoLogEntry{
				TrxID:  txID,
				PageID: uint64(4000 + i),
				Type:   LOG_TYPE_INSERT,
				LSN:    lsn + 1,
				Data:   []byte(fmt.Sprintf("tx%d_data", txID)),
			})

			redoMgr.Append(&RedoLogEntry{
				TrxID:  txID,
				PageID: uint64(4000 + i),
				Type:   LOG_TYPE_TXN_COMMIT,
				LSN:    lsn + 2,
				Data:   []byte(fmt.Sprintf("tx%d_commit", txID)),
			})

			// 刷新日志
			redoMgr.Flush(0)
		}

		assert.GreaterOrEqual(t, crashedAt, 0, "应该在某个点崩溃")
		t.Logf("✅ Phase1 完成: 在第 %d 个事务时崩溃", crashedAt)
	})

	// 阶段2: 恢复并验证数据一致性
	t.Run("Phase2_RecoveryAndVerify", func(t *testing.T) {
		redoMgr, err := NewRedoLogManager(tmpDir, 10000)
		require.NoError(t, err)
		defer redoMgr.Close()

		undoMgr, err := NewUndoLogManager(tmpDir)
		require.NoError(t, err)
		defer undoMgr.Close()

		bufferPool := NewMockBufferPool()
		crashRecovery := NewCrashRecovery(redoMgr, undoMgr, 0)
		crashRecovery.bufferPoolManager = bufferPool

		// 执行恢复
		err = crashRecovery.Recover()
		require.NoError(t, err)

		// 验证：崩溃前的事务应该都已提交
		assert.True(t, crashRecovery.analysisComplete)
		assert.True(t, crashRecovery.redoComplete)
		assert.True(t, crashRecovery.undoComplete)

		t.Logf("✅ Phase2 完成: 恢复成功")
		t.Logf("   - 活跃事务: %d", len(crashRecovery.activeTransactions))
		t.Logf("   - 回滚事务: %d", len(crashRecovery.undoTransactions))
	})
}

// TestFaultInjection_CrashDuringCommit 测试提交过程中崩溃
func TestFaultInjection_CrashDuringCommit(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("Phase1_CrashBeforeCommitLog", func(t *testing.T) {
		redoMgr, err := NewRedoLogManager(tmpDir, 10000)
		require.NoError(t, err)
		defer redoMgr.Close()

		undoMgr, err := NewUndoLogManager(tmpDir)
		require.NoError(t, err)
		defer undoMgr.Close()

		// 事务1: 完整提交
		redoMgr.Append(&RedoLogEntry{
			TrxID:  5000,
			PageID: 5000,
			Type:   LOG_TYPE_TXN_BEGIN,
			LSN:    5000,
			Data:   []byte("tx1_begin"),
		})

		redoMgr.Append(&RedoLogEntry{
			TrxID:  5000,
			PageID: 5000,
			Type:   LOG_TYPE_INSERT,
			LSN:    5001,
			Data:   []byte("tx1_data"),
		})

		redoMgr.Append(&RedoLogEntry{
			TrxID:  5000,
			PageID: 5000,
			Type:   LOG_TYPE_TXN_COMMIT,
			LSN:    5002,
			Data:   []byte("tx1_commit"),
		})

		// 事务2: 写入数据但未写入提交日志（模拟提交过程中崩溃）
		redoMgr.Append(&RedoLogEntry{
			TrxID:  5001,
			PageID: 5001,
			Type:   LOG_TYPE_TXN_BEGIN,
			LSN:    5003,
			Data:   []byte("tx2_begin"),
		})

		redoMgr.Append(&RedoLogEntry{
			TrxID:  5001,
			PageID: 5001,
			Type:   LOG_TYPE_INSERT,
			LSN:    5004,
			Data:   []byte("tx2_data"),
		})

		// 💥 崩溃：未写入 COMMIT 日志

		redoMgr.Flush(0)
		t.Logf("💥 模拟崩溃: 事务2在提交前崩溃")
	})

	t.Run("Phase2_RecoveryAndRollback", func(t *testing.T) {
		redoMgr, err := NewRedoLogManager(tmpDir, 10000)
		require.NoError(t, err)
		defer redoMgr.Close()

		undoMgr, err := NewUndoLogManager(tmpDir)
		require.NoError(t, err)
		defer undoMgr.Close()

		bufferPool := NewMockBufferPool()
		crashRecovery := NewCrashRecovery(redoMgr, undoMgr, 0)
		crashRecovery.bufferPoolManager = bufferPool

		// 执行恢复
		err = crashRecovery.Recover()
		require.NoError(t, err)

		// 验证：事务2应该被回滚
		assert.Len(t, crashRecovery.undoTransactions, 1, "应该有1个需要回滚的事务")

		// 验证事务2在回滚列表中
		found := false
			for _, txID := range crashRecovery.undoTransactions {
				if txID == 5001 {
					found = true
					break
				}
		}
		assert.True(t, found, "事务5001应该在回滚列表中")

		t.Logf("✅ Phase2 完成: 事务2已回滚")
	})
}

// TestFaultInjection_CrashDuringRedo 测试Redo阶段崩溃
func TestFaultInjection_CrashDuringRedo(t *testing.T) {
	tmpDir := t.TempDir()

	// 阶段1: 写入数据
	t.Run("Phase1_WriteData", func(t *testing.T) {
		redoMgr, err := NewRedoLogManager(tmpDir, 10000)
		require.NoError(t, err)
		defer redoMgr.Close()

		undoMgr, err := NewUndoLogManager(tmpDir)
		require.NoError(t, err)
		defer undoMgr.Close()

		// 写入多个事务
		for i := 0; i < 5; i++ {
			txID := int64(6000 + i)
			redoMgr.Append(&RedoLogEntry{
				TrxID:  txID,
				PageID: uint64(6000 + i),
				Type:   LOG_TYPE_TXN_BEGIN,
				LSN:    uint64(6000 + i*10),
				Data:   []byte(fmt.Sprintf("tx%d_begin", txID)),
			})

			redoMgr.Append(&RedoLogEntry{
				TrxID:  txID,
				PageID: uint64(6000 + i),
				Type:   LOG_TYPE_INSERT,
				LSN:    uint64(6000 + i*10 + 1),
				Data:   []byte(fmt.Sprintf("tx%d_data", txID)),
			})

			redoMgr.Append(&RedoLogEntry{
				TrxID:  txID,
				PageID: uint64(6000 + i),
				Type:   LOG_TYPE_TXN_COMMIT,
				LSN:    uint64(6000 + i*10 + 2),
				Data:   []byte(fmt.Sprintf("tx%d_commit", txID)),
			})
		}

		redoMgr.Flush(0)
		t.Logf("✅ Phase1 完成: 写入5个事务")
	})

	// 阶段2: 第一次恢复（模拟Redo阶段崩溃）
	t.Run("Phase2_FirstRecoveryCrash", func(t *testing.T) {
		// 注意：实际测试中很难模拟Redo阶段崩溃
		// 这里我们验证Redo的幂等性
		redoMgr, err := NewRedoLogManager(tmpDir, 10000)
		require.NoError(t, err)
		defer redoMgr.Close()

		undoMgr, err := NewUndoLogManager(tmpDir)
		require.NoError(t, err)
		defer undoMgr.Close()

		bufferPool := NewMockBufferPool()
		crashRecovery := NewCrashRecovery(redoMgr, undoMgr, 0)
		crashRecovery.bufferPoolManager = bufferPool

		// 第一次恢复
		err = crashRecovery.Recover()
		require.NoError(t, err)

		t.Logf("✅ Phase2 完成: 第一次恢复成功")
	})

	// 阶段3: 第二次恢复（验证幂等性）
	t.Run("Phase3_SecondRecoveryIdempotent", func(t *testing.T) {
		redoMgr, err := NewRedoLogManager(tmpDir, 10000)
		require.NoError(t, err)
		defer redoMgr.Close()

		undoMgr, err := NewUndoLogManager(tmpDir)
		require.NoError(t, err)
		defer undoMgr.Close()

		bufferPool := NewMockBufferPool()
		crashRecovery := NewCrashRecovery(redoMgr, undoMgr, 0)
		crashRecovery.bufferPoolManager = bufferPool

		// 第二次恢复（应该是幂等的）
		err = crashRecovery.Recover()
		require.NoError(t, err)

		// 验证：所有事务都已提交，没有需要回滚的
		assert.Len(t, crashRecovery.undoTransactions, 0, "不应该有需要回滚的事务")

		t.Logf("✅ Phase3 完成: 第二次恢复成功（幂等性验证）")
	})
}
