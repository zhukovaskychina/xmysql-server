package manager

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCrashRecoveryFullCycle 测试完整的崩溃恢复周期
// 场景：多个事务并发执行，部分提交，部分未提交，然后崩溃恢复
func TestCrashRecoveryFullCycle(t *testing.T) {
	tmpDir := t.TempDir()

	// 阶段1: 正常运行，写入数据
	t.Run("Phase1_NormalOperation", func(t *testing.T) {
		redoMgr, err := NewRedoLogManager(tmpDir, 1000)
		require.NoError(t, err)
		defer redoMgr.Close()

		undoMgr, err := NewUndoLogManager(tmpDir)
		require.NoError(t, err)
		defer undoMgr.Close()

		// 事务1: 完整提交
		lsn1, err := redoMgr.Append(&RedoLogEntry{
			TrxID:  100,
			PageID: 1000,
			Type:   LOG_TYPE_TXN_BEGIN,
			Data:   []byte("tx1_begin"),
		})
		require.NoError(t, err)
		assert.Greater(t, lsn1, uint64(0))

		lsn2, err := redoMgr.Append(&RedoLogEntry{
			TrxID:  100,
			PageID: 1000,
			Type:   LOG_TYPE_INSERT,
			Data:   []byte("tx1_insert_data"),
		})
		require.NoError(t, err)
		assert.Greater(t, lsn2, lsn1)

		lsn3, err := redoMgr.Append(&RedoLogEntry{
			TrxID:  100,
			PageID: 1000,
			Type:   LOG_TYPE_TXN_COMMIT,
			Data:   []byte("tx1_commit"),
		})
		require.NoError(t, err)
		assert.Greater(t, lsn3, lsn2)

		// 事务2: 部分完成（未提交）
		redoMgr.Append(&RedoLogEntry{
			TrxID:  101,
			PageID: 1001,
			Type:   LOG_TYPE_TXN_BEGIN,
			LSN:    1003,
			Data:   []byte("tx2_begin"),
		})

		redoMgr.Append(&RedoLogEntry{
			TrxID:  101,
			PageID: 1001,
			Type:   LOG_TYPE_INSERT,
			LSN:    1004,
			Data:   []byte("tx2_insert_data"),
		})

		// 事务3: 完整提交
		redoMgr.Append(&RedoLogEntry{
			TrxID:  102,
			PageID: 1002,
			Type:   LOG_TYPE_TXN_BEGIN,
			LSN:    1005,
			Data:   []byte("tx3_begin"),
		})

		redoMgr.Append(&RedoLogEntry{
			TrxID:  102,
			PageID: 1002,
			Type:   LOG_TYPE_UPDATE,
			LSN:    1006,
			Data:   []byte("tx3_update_data"),
		})

		redoMgr.Append(&RedoLogEntry{
			TrxID:  102,
			PageID: 1002,
			Type:   LOG_TYPE_TXN_COMMIT,
			LSN:    1007,
			Data:   []byte("tx3_commit"),
		})

		// 事务4: 部分完成（未提交）
		redoMgr.Append(&RedoLogEntry{
			TrxID:  103,
			PageID: 1003,
			Type:   LOG_TYPE_TXN_BEGIN,
			LSN:    1008,
			Data:   []byte("tx4_begin"),
		})

		redoMgr.Append(&RedoLogEntry{
			TrxID:  103,
			PageID: 1003,
			Type:   LOG_TYPE_DELETE,
			LSN:    1009,
			Data:   []byte("tx4_delete_data"),
		})

		// 刷新所有日志到磁盘
		err = redoMgr.Flush(0)
		require.NoError(t, err)

		t.Logf("✅ Phase1 完成: 写入 4 个事务 (2个已提交, 2个未提交)")
	})

	// 阶段2: 模拟崩溃后恢复
	t.Run("Phase2_CrashRecovery", func(t *testing.T) {
		// 重新打开日志管理器（模拟崩溃重启）
		redoMgr, err := NewRedoLogManager(tmpDir, 1000)
		require.NoError(t, err)
		defer redoMgr.Close()

		undoMgr, err := NewUndoLogManager(tmpDir)
		require.NoError(t, err)
		defer undoMgr.Close()

		// 创建模拟缓冲池
		bufferPool := NewMockBufferPool()

		// 创建崩溃恢复管理器
		crashRecovery := NewCrashRecovery(redoMgr, undoMgr, 0)
		crashRecovery.bufferPoolManager = bufferPool

		// 执行完整恢复流程
		err = crashRecovery.Recover()
		require.NoError(t, err)

		// 验证恢复结果
		assert.True(t, crashRecovery.analysisComplete, "分析阶段应该完成")
		assert.True(t, crashRecovery.redoComplete, "Redo阶段应该完成")
		assert.True(t, crashRecovery.undoComplete, "Undo阶段应该完成")

		// 验证活跃事务（应该有2个未提交的事务）
		assert.Len(t, crashRecovery.activeTransactions, 2, "应该有2个活跃事务")

		// 验证需要回滚的事务
		assert.Len(t, crashRecovery.undoTransactions, 2, "应该有2个需要回滚的事务")

		// 验证脏页（4个事务操作了4个页面）
		assert.GreaterOrEqual(t, len(crashRecovery.dirtyPages), 2, "至少应该有2个脏页")

		t.Logf("✅ Phase2 完成: 恢复成功")
		t.Logf("   - 活跃事务: %d", len(crashRecovery.activeTransactions))
		t.Logf("   - 回滚事务: %d", len(crashRecovery.undoTransactions))
		t.Logf("   - 脏页数量: %d", len(crashRecovery.dirtyPages))
	})
}

// TestCrashRecoveryConcurrentTransactions 测试并发事务的崩溃恢复
func TestCrashRecoveryConcurrentTransactions(t *testing.T) {
	tmpDir := t.TempDir()

	// 阶段1: 并发写入多个事务
	t.Run("Phase1_ConcurrentWrites", func(t *testing.T) {
		redoMgr, err := NewRedoLogManager(tmpDir, 10000)
		require.NoError(t, err)
		defer redoMgr.Close()

		undoMgr, err := NewUndoLogManager(tmpDir)
		require.NoError(t, err)
		defer undoMgr.Close()

		// 并发写入10个事务
		var wg sync.WaitGroup
		numTxs := 10
		wg.Add(numTxs)

		for i := 0; i < numTxs; i++ {
			go func(txID int64) {
				defer wg.Done()

				// 开始事务
				redoMgr.Append(&RedoLogEntry{
					TrxID:  txID,
					PageID: uint64(2000 + txID),
					Type:   LOG_TYPE_TXN_BEGIN,
					LSN:    uint64(2000 + txID*10),
					Data:   []byte(fmt.Sprintf("tx%d_begin", txID)),
				})

				// 插入数据
				for j := 0; j < 5; j++ {
					redoMgr.Append(&RedoLogEntry{
						TrxID:  txID,
						PageID: uint64(2000 + txID),
						Type:   LOG_TYPE_INSERT,
						Data:   []byte(fmt.Sprintf("tx%d_insert_%d", txID, j)),
					})
					time.Sleep(1 * time.Millisecond)
				}

				// 只有偶数事务提交
				if txID%2 == 0 {
					redoMgr.Append(&RedoLogEntry{
						TrxID:  txID,
						PageID: uint64(2000 + txID),
						Type:   LOG_TYPE_TXN_COMMIT,
						LSN:    uint64(2000 + txID*10 + 6),
						Data:   []byte(fmt.Sprintf("tx%d_commit", txID)),
					})
				}
			}(int64(200 + i))
		}

		wg.Wait()

		// 刷新日志
		err = redoMgr.Flush(0)
		require.NoError(t, err)

		t.Logf("✅ Phase1 完成: 并发写入 %d 个事务 (5个已提交, 5个未提交)", numTxs)
	})

	// 阶段2: 恢复
	t.Run("Phase2_Recovery", func(t *testing.T) {
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

		// 验证：应该有5个未提交的事务需要回滚
		assert.Len(t, crashRecovery.undoTransactions, 5, "应该有5个需要回滚的事务")

		t.Logf("✅ Phase2 完成: 恢复成功，回滚 %d 个未提交事务", len(crashRecovery.undoTransactions))
	})
}

// TestCrashRecoveryLargeDataset 测试大数据量的崩溃恢复
func TestCrashRecoveryLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过大数据量测试")
	}

	tmpDir := t.TempDir()

	// 阶段1: 写入大量数据
	t.Run("Phase1_LargeDataWrite", func(t *testing.T) {
		redoMgr, err := NewRedoLogManager(tmpDir, 100000)
		require.NoError(t, err)
		defer redoMgr.Close()

		undoMgr, err := NewUndoLogManager(tmpDir)
		require.NoError(t, err)
		defer undoMgr.Close()

		// 写入1000个事务，每个事务10条记录
		numTxs := 1000
		recordsPerTx := 10

		startTime := time.Now()

		for i := 0; i < numTxs; i++ {
			txID := int64(3000 + i)

			// 开始事务
			redoMgr.Append(&RedoLogEntry{
				TrxID:  txID,
				PageID: uint64(3000 + i),
				Type:   LOG_TYPE_TXN_BEGIN,
				LSN:    uint64(3000 + i*100),
				Data:   []byte(fmt.Sprintf("tx%d_begin", txID)),
			})

			// 插入记录
			for j := 0; j < recordsPerTx; j++ {
				redoMgr.Append(&RedoLogEntry{
					TrxID:  txID,
					PageID: uint64(3000 + i),
					Type:   LOG_TYPE_INSERT,
					LSN:    uint64(3000 + i*100 + j + 1),
					Data:   make([]byte, 1024), // 1KB数据
				})
			}

			// 80% 的事务提交
			if i%5 != 0 {
				redoMgr.Append(&RedoLogEntry{
					TrxID:  txID,
					PageID: uint64(3000 + i),
					Type:   LOG_TYPE_TXN_COMMIT,
					LSN:    uint64(3000 + i*100 + recordsPerTx + 1),
					Data:   []byte(fmt.Sprintf("tx%d_commit", txID)),
				})
			}

			// 每100个事务刷新一次
			if i%100 == 0 {
				redoMgr.Flush(0)
			}
		}

		// 最终刷新
		err = redoMgr.Flush(0)
		require.NoError(t, err)

		elapsed := time.Since(startTime)
		t.Logf("✅ Phase1 完成: 写入 %d 个事务，耗时 %v", numTxs, elapsed)
	})

	// 阶段2: 恢复性能测试
	t.Run("Phase2_RecoveryPerformance", func(t *testing.T) {
		redoMgr, err := NewRedoLogManager(tmpDir, 100000)
		require.NoError(t, err)
		defer redoMgr.Close()

		undoMgr, err := NewUndoLogManager(tmpDir)
		require.NoError(t, err)
		defer undoMgr.Close()

		bufferPool := NewMockBufferPool()
		crashRecovery := NewCrashRecovery(redoMgr, undoMgr, 0)
		crashRecovery.bufferPoolManager = bufferPool

		// 测量恢复时间
		startTime := time.Now()
		err = crashRecovery.Recover()
		require.NoError(t, err)
		elapsed := time.Since(startTime)

		// 验证恢复结果
		expectedUncommitted := 200 // 1000 * 20% = 200
		actualUncommitted := len(crashRecovery.undoTransactions)

		assert.InDelta(t, expectedUncommitted, actualUncommitted, 10, "未提交事务数量应该接近200")

		t.Logf("✅ Phase2 完成: 恢复耗时 %v", elapsed)
		t.Logf("   - 未提交事务: %d", actualUncommitted)
		t.Logf("   - 恢复速度: %.2f 事务/秒", float64(1000)/elapsed.Seconds())

		// 性能断言：恢复1000个事务应该在10秒内完成
		assert.Less(t, elapsed.Seconds(), 10.0, "恢复时间应该小于10秒")
	})
}
