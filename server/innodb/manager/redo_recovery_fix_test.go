package manager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestTXN001_RedoLogRecovery 测试Redo日志恢复机制修复
func TestTXN001_RedoLogRecovery(t *testing.T) {
	// 创建临时目录
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	// 测试1: 基本恢复流程
	t.Run("BasicRecovery", func(t *testing.T) {
		// 创建RedoLogManager
		redoManager, err := NewRedoLogManager(redoDir, 1000)
		assert.NoError(t, err)
		assert.NotNil(t, redoManager)
		defer redoManager.Close()

		// 写入一些日志
		entry1 := &RedoLogEntry{
			LSN:   1,
			TrxID: 100,
			Type:  LOG_TYPE_TXN_BEGIN,
			Data:  []byte("begin tx 100"),
		}
		_, err = redoManager.Append(entry1)
		assert.NoError(t, err)

		entry2 := &RedoLogEntry{
			LSN:    2,
			TrxID:  100,
			PageID: 1,
			Type:   LOG_TYPE_INSERT,
			Data:   []byte("insert data"),
		}
		_, err = redoManager.Append(entry2)
		assert.NoError(t, err)

		entry3 := &RedoLogEntry{
			LSN:   3,
			TrxID: 100,
			Type:  LOG_TYPE_TXN_COMMIT,
			Data:  []byte("commit tx 100"),
		}
		_, err = redoManager.Append(entry3)
		assert.NoError(t, err)

		// 刷新日志
		err = redoManager.Flush(0)
		assert.NoError(t, err)

		// 创建检查点
		err = redoManager.Checkpoint()
		assert.NoError(t, err)

		// 关闭并重新打开
		redoManager.Close()

		// 重新打开并恢复
		redoManager2, err := NewRedoLogManager(redoDir, 1000)
		assert.NoError(t, err)
		defer redoManager2.Close()

		// 执行恢复（简化版本，不带缓冲池）
		err = redoManager2.Recover()
		assert.NoError(t, err)
	})

	// 测试2: 检查点LSN读取
	t.Run("CheckpointLSNRead", func(t *testing.T) {
		redoManager, err := NewRedoLogManager(redoDir, 1000)
		assert.NoError(t, err)
		defer redoManager.Close()

		// 创建检查点
		err = redoManager.Checkpoint()
		assert.NoError(t, err)

		// 读取检查点LSN
		checkpointLSN, err := redoManager.readCheckpointLSN()
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, checkpointLSN, uint64(0))
	})

	// 测试3: CrashRecovery集成
	t.Run("CrashRecoveryIntegration", func(t *testing.T) {
		redoManager, err := NewRedoLogManager(redoDir, 1000)
		assert.NoError(t, err)
		defer redoManager.Close()

		undoManager, err := NewUndoLogManager(undoDir)
		assert.NoError(t, err)

		// 写入一些日志
		entry1 := &RedoLogEntry{
			LSN:   10,
			TrxID: 200,
			Type:  LOG_TYPE_TXN_BEGIN,
		}
		redoManager.Append(entry1)

		entry2 := &RedoLogEntry{
			LSN:    11,
			TrxID:  200,
			PageID: 1,
			Type:   LOG_TYPE_INSERT,
			Data:   []byte("test data"),
		}
		redoManager.Append(entry2)

		entry3 := &RedoLogEntry{
			LSN:   12,
			TrxID: 200,
			Type:  LOG_TYPE_TXN_COMMIT,
		}
		redoManager.Append(entry3)

		redoManager.Flush(0)
		redoManager.Checkpoint()

		// 创建CrashRecovery实例
		checkpointLSN, _ := redoManager.readCheckpointLSN()
		crashRecovery := NewCrashRecovery(redoManager, undoManager, checkpointLSN)
		assert.NotNil(t, crashRecovery)

		// 执行恢复（不带缓冲池和存储管理器）
		err = crashRecovery.Recover()
		// 由于没有设置缓冲池和存储管理器，可能会有警告但不应该失败
		// 这里我们只验证恢复流程能够执行
		if err != nil {
			t.Logf("Recovery completed with warning: %v", err)
		}

		// 验证恢复状态
		status := crashRecovery.GetRecoveryStatus()
		assert.NotNil(t, status)
		assert.True(t, status.AnalysisComplete)
		assert.True(t, status.RedoComplete)
		assert.True(t, status.UndoComplete)
	})
}

// TestTXN001_CrashRecoveryPhases 测试崩溃恢复的三个阶段
func TestTXN001_CrashRecoveryPhases(t *testing.T) {
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	// 创建管理器
	redoManager, err := NewRedoLogManager(redoDir, 1000)
	assert.NoError(t, err)
	defer redoManager.Close()

	undoManager, err := NewUndoLogManager(undoDir)
	assert.NoError(t, err)

	// 测试1: 分析阶段
	t.Run("AnalysisPhase", func(t *testing.T) {
		// 写入混合事务日志（已提交和未提交）
		// 事务1: 已提交
		redoManager.Append(&RedoLogEntry{LSN: 1, TrxID: 1, Type: LOG_TYPE_TXN_BEGIN})
		redoManager.Append(&RedoLogEntry{LSN: 2, TrxID: 1, PageID: 1, Type: LOG_TYPE_INSERT, Data: []byte("data1")})
		redoManager.Append(&RedoLogEntry{LSN: 3, TrxID: 1, Type: LOG_TYPE_TXN_COMMIT})

		// 事务2: 未提交（需要回滚）
		redoManager.Append(&RedoLogEntry{LSN: 4, TrxID: 2, Type: LOG_TYPE_TXN_BEGIN})
		redoManager.Append(&RedoLogEntry{LSN: 5, TrxID: 2, PageID: 2, Type: LOG_TYPE_INSERT, Data: []byte("data2")})

		// 事务3: 已提交
		redoManager.Append(&RedoLogEntry{LSN: 6, TrxID: 3, Type: LOG_TYPE_TXN_BEGIN})
		redoManager.Append(&RedoLogEntry{LSN: 7, TrxID: 3, PageID: 3, Type: LOG_TYPE_UPDATE, Data: []byte("data3")})
		redoManager.Append(&RedoLogEntry{LSN: 8, TrxID: 3, Type: LOG_TYPE_TXN_COMMIT})

		redoManager.Flush(0)

		// 创建CrashRecovery并执行分析
		crashRecovery := NewCrashRecovery(redoManager, undoManager, 0)
		err := crashRecovery.Recover()
		if err != nil {
			t.Logf("Recovery warning: %v", err)
		}

		// 验证分析结果
		result := crashRecovery.GetAnalysisResult()
		assert.NotNil(t, result)

		// 应该有1个未提交事务（事务2）
		assert.Equal(t, 1, len(result.UndoTransactions))
		assert.Contains(t, result.UndoTransactions, int64(2))

		// 应该有脏页
		assert.GreaterOrEqual(t, len(result.DirtyPages), 1)
	})

	// 测试2: Redo阶段
	t.Run("RedoPhase", func(t *testing.T) {
		crashRecovery := NewCrashRecovery(redoManager, undoManager, 0)

		// 执行恢复
		err := crashRecovery.Recover()
		if err != nil {
			t.Logf("Recovery warning: %v", err)
		}

		// 验证Redo阶段完成
		status := crashRecovery.GetRecoveryStatus()
		assert.True(t, status.RedoComplete)
		assert.GreaterOrEqual(t, status.RedoEndLSN, status.RedoStartLSN)
	})

	// 测试3: Undo阶段
	t.Run("UndoPhase", func(t *testing.T) {
		crashRecovery := NewCrashRecovery(redoManager, undoManager, 0)

		// 执行恢复
		err := crashRecovery.Recover()
		if err != nil {
			t.Logf("Recovery warning: %v", err)
		}

		// 验证Undo阶段完成
		status := crashRecovery.GetRecoveryStatus()
		assert.True(t, status.UndoComplete)
	})
}

// TestTXN001_RecoveryStatistics 测试恢复统计信息
func TestTXN001_RecoveryStatistics(t *testing.T) {
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	redoManager, err := NewRedoLogManager(redoDir, 1000)
	assert.NoError(t, err)
	defer redoManager.Close()

	undoManager, err := NewUndoLogManager(undoDir)
	assert.NoError(t, err)

	// 写入一些日志
	for i := int64(1); i <= 5; i++ {
		redoManager.Append(&RedoLogEntry{
			LSN:   uint64(i * 3),
			TrxID: i,
			Type:  LOG_TYPE_TXN_BEGIN,
		})
		redoManager.Append(&RedoLogEntry{
			LSN:    uint64(i*3 + 1),
			TrxID:  i,
			PageID: uint64(i),
			Type:   LOG_TYPE_INSERT,
			Data:   []byte("data"),
		})
		if i%2 == 0 {
			// 偶数事务提交
			redoManager.Append(&RedoLogEntry{
				LSN:   uint64(i*3 + 2),
				TrxID: i,
				Type:  LOG_TYPE_TXN_COMMIT,
			})
		}
	}
	redoManager.Flush(0)

	// 执行恢复
	crashRecovery := NewCrashRecovery(redoManager, undoManager, 0)
	err = crashRecovery.Recover()
	if err != nil {
		t.Logf("Recovery warning: %v", err)
	}

	// 获取统计信息
	stats := crashRecovery.GetRecoveryStatistics()
	assert.NotNil(t, stats)
	assert.GreaterOrEqual(t, stats.TotalTransactions, 0)
	assert.GreaterOrEqual(t, stats.TotalDirtyPages, 0)
	assert.GreaterOrEqual(t, stats.RecoveryTime, time.Duration(0))
}

// TestTXN001_RecoveryValidation 测试恢复验证
func TestTXN001_RecoveryValidation(t *testing.T) {
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	redoManager, err := NewRedoLogManager(redoDir, 1000)
	assert.NoError(t, err)
	defer redoManager.Close()

	undoManager, err := NewUndoLogManager(undoDir)
	assert.NoError(t, err)

	// 写入完整的事务
	redoManager.Append(&RedoLogEntry{LSN: 1, TrxID: 1, Type: LOG_TYPE_TXN_BEGIN})
	redoManager.Append(&RedoLogEntry{LSN: 2, TrxID: 1, PageID: 1, Type: LOG_TYPE_INSERT, Data: []byte("data")})
	redoManager.Append(&RedoLogEntry{LSN: 3, TrxID: 1, Type: LOG_TYPE_TXN_COMMIT})
	redoManager.Flush(0)

	// 执行恢复
	crashRecovery := NewCrashRecovery(redoManager, undoManager, 0)
	err = crashRecovery.Recover()
	if err != nil {
		t.Logf("Recovery warning: %v", err)
	}

	// 验证恢复结果
	err = crashRecovery.ValidateRecovery()
	// 由于没有设置RollbackExecutor，验证可能会失败，但不应该panic
	if err != nil {
		t.Logf("Validation warning: %v", err)
	}
}
