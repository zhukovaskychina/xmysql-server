package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUndoLogManager(t *testing.T) {
	// 准备测试目录
	testDir := t.TempDir()

	// 创建UndoLog管理器
	manager, err := NewUndoLogManager(testDir)
	require.NoError(t, err)
	defer manager.Close()

	t.Run("基本操作", func(t *testing.T) {
		// 创建测试日志条目
		entry := &UndoLogEntry{
			TxID:     1,
			PageID:   100,
			SpaceID:  1,
			Offset:   0,
			OldValue: []byte("old data"),
		}

		// 追加日志
		err := manager.Append(entry)
		require.NoError(t, err)

		// 验证活跃事务
		txns := manager.GetActiveTxns()
		assert.Contains(t, txns, int64(1))

		// 回滚事务
		err = manager.Rollback(1)
		require.NoError(t, err)

		// 验证事务已清理
		txns = manager.GetActiveTxns()
		assert.NotContains(t, txns, int64(1))
	})

	t.Run("多事务操作", func(t *testing.T) {
		// 创建多个事务的日志
		for txID := int64(1); txID <= 3; txID++ {
			for i := 0; i < 5; i++ {
				entry := &UndoLogEntry{
					TxID:     txID,
					PageID:   uint32(100 + i),
					SpaceID:  1,
					Offset:   uint16(i * 100),
					OldValue: []byte("old data"),
				}
				err := manager.Append(entry)
				require.NoError(t, err)
			}
		}

		// 验证活跃事务数量
		txns := manager.GetActiveTxns()
		assert.Len(t, txns, 3)

		// 获取最老事务时间
		oldestTime := manager.GetOldestTxnTime()
		assert.False(t, oldestTime.IsZero())

		// 回滚部分事务
		err := manager.Rollback(1)
		require.NoError(t, err)
		err = manager.Rollback(2)
		require.NoError(t, err)

		// 验证剩余活跃事务
		txns = manager.GetActiveTxns()
		assert.Len(t, txns, 1)
		assert.Contains(t, txns, int64(3))
	})

	t.Run("事务清理", func(t *testing.T) {
		// 创建测试事务
		entry := &UndoLogEntry{
			TxID:     100,
			PageID:   100,
			SpaceID:  1,
			Offset:   0,
			OldValue: []byte("old data"),
		}
		err := manager.Append(entry)
		require.NoError(t, err)

		// 直接清理事务
		manager.Cleanup(100)

		// 验证事务已清理
		txns := manager.GetActiveTxns()
		assert.NotContains(t, txns, int64(100))
	})
}

func TestUndoLogManager_Concurrent(t *testing.T) {
	testDir := t.TempDir()
	manager, err := NewUndoLogManager(testDir)
	require.NoError(t, err)
	defer manager.Close()

	// 并发写入和回滚
	const numGoroutines = 10
	const numEntriesPerGoroutine = 100

	// 并发写入
	done := make(chan bool)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			txID := int64(id + 1)
			for j := 0; j < numEntriesPerGoroutine; j++ {
				entry := &UndoLogEntry{
					TxID:     txID,
					PageID:   uint32(id*1000 + j),
					SpaceID:  1,
					Offset:   uint16(j * 100),
					OldValue: []byte("old data"),
				}
				if err := manager.Append(entry); err != nil {
					t.Error(err)
				}
			}
			done <- true
		}(i)
	}

	// 等待写入完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// 验证活跃事务数量
	txns := manager.GetActiveTxns()
	assert.Len(t, txns, numGoroutines)

	// 并发回滚
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			txID := int64(id + 1)
			if err := manager.Rollback(txID); err != nil {
				t.Error(err)
			}
			done <- true
		}(i)
	}

	// 等待回滚完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// 验证所有事务已清理
	txns = manager.GetActiveTxns()
	assert.Empty(t, txns)
}
