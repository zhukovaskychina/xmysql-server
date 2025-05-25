package manager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionManager(t *testing.T) {
	// 准备测试目录
	testDir := t.TempDir()

	// 创建事务管理器
	tm, err := NewTransactionManager(testDir, testDir)
	require.NoError(t, err)
	defer tm.Close()

	t.Run("基本事务操作", func(t *testing.T) {
		// 开启事务
		trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
		require.NoError(t, err)
		assert.Equal(t, TRX_STATE_ACTIVE, trx.State)
		assert.NotNil(t, trx.ReadView)

		// 验证事务记录
		assert.NotNil(t, tm.GetTransaction(trx.ID))

		// 提交事务
		err = tm.Commit(trx)
		require.NoError(t, err)
		assert.Equal(t, TRX_STATE_COMMITTED, trx.State)

		// 验证事务已清理
		assert.Nil(t, tm.GetTransaction(trx.ID))
	})

	t.Run("事务回滚", func(t *testing.T) {
		// 开启事务
		trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
		require.NoError(t, err)

		// 添加Undo日志
		undoEntry := UndoLogEntry{
			TxID:     trx.ID,
			PageID:   100,
			SpaceID:  1,
			Offset:   0,
			OldValue: []byte("old data"),
		}
		err = tm.undoManager.Append(&undoEntry)
		require.NoError(t, err)

		// 回滚事务
		err = tm.Rollback(trx)
		require.NoError(t, err)
		assert.Equal(t, TRX_STATE_ROLLED_BACK, trx.State)

		// 验证事务已清理
		assert.Nil(t, tm.GetTransaction(trx.ID))
	})

	t.Run("隔离级别测试", func(t *testing.T) {
		// 读未提交
		trx1, err := tm.Begin(false, TRX_ISO_READ_UNCOMMITTED)
		require.NoError(t, err)
		assert.Nil(t, trx1.ReadView)

		// 可重复读
		trx2, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
		require.NoError(t, err)
		assert.NotNil(t, trx2.ReadView)

		// 测试可见性
		assert.True(t, tm.IsVisible(trx1, 100))      // 读未提交总是可见
		assert.False(t, tm.IsVisible(trx2, trx1.ID)) // 较新的事务ID不可见

		// 清理
		tm.Commit(trx1)
		tm.Commit(trx2)
	})

	t.Run("并发事务", func(t *testing.T) {
		const numTrx = 10
		trxs := make([]*Transaction, numTrx)

		// 并发开启事务
		done := make(chan bool)
		for i := 0; i < numTrx; i++ {
			go func(id int) {
				var err error
				trxs[id], err = tm.Begin(false, TRX_ISO_REPEATABLE_READ)
				require.NoError(t, err)
				done <- true
			}(i)
		}

		// 等待所有事务开启
		for i := 0; i < numTrx; i++ {
			<-done
		}

		// 验证活跃事务数量
		count := 0
		for _, trx := range trxs {
			if tm.GetTransaction(trx.ID) != nil {
				count++
			}
		}
		assert.Equal(t, numTrx, count)

		// 并发提交事务
		for i := 0; i < numTrx; i++ {
			go func(id int) {
				err := tm.Commit(trxs[id])
				require.NoError(t, err)
				done <- true
			}(i)
		}

		// 等待所有事务提交
		for i := 0; i < numTrx; i++ {
			<-done
		}

		// 验证所有事务已清理
		for _, trx := range trxs {
			assert.Nil(t, tm.GetTransaction(trx.ID))
		}
	})

	t.Run("事务超时清理", func(t *testing.T) {
		// 修改默认超时时间为测试用
		tm.defaultTimeout = 100 * time.Millisecond

		// 开启事务
		trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
		require.NoError(t, err)

		// 等待超时
		time.Sleep(200 * time.Millisecond)

		// 执行清理
		tm.Cleanup()

		// 验证事务已被清理
		assert.Nil(t, tm.GetTransaction(trx.ID))
	})
}

func TestTransactionManager_ReadView(t *testing.T) {
	testDir := t.TempDir()
	tm, err := NewTransactionManager(testDir, testDir)
	require.NoError(t, err)
	defer tm.Close()

	// 创建多个事务形成特定场景
	trx1, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	require.NoError(t, err)

	trx2, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	require.NoError(t, err)

	trx3, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	require.NoError(t, err)

	// 验证ReadView的创建
	assert.NotNil(t, trx2.ReadView)
	assert.Contains(t, trx2.ReadView.GetActiveIDs(), trx1.ID)
	assert.Contains(t, trx2.ReadView.GetActiveIDs(), trx3.ID)

	// 测试可见性规则
	assert.False(t, tm.IsVisible(trx2, trx3.ID))  // 较新的事务不可见
	assert.True(t, tm.IsVisible(trx2, trx1.ID-1)) // 较老的事务可见

	// 提交trx1，对trx2仍不可见（RR隔离级别）
	tm.Commit(trx1)
	assert.Contains(t, trx2.ReadView.GetActiveIDs(), trx1.ID)

	// 清理
	tm.Commit(trx2)
	tm.Commit(trx3)
}
