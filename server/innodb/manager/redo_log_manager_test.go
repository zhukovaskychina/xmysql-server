package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedoLogManager(t *testing.T) {
	// 准备测试目录
	testDir := t.TempDir()

	// 创建RedoLog管理器
	manager, err := NewRedoLogManager(testDir, 10)
	require.NoError(t, err)
	defer manager.Close()

	t.Run("基本日志操作", func(t *testing.T) {
		// 创建测试日志条目
		entry := &RedoLogEntry{
			TxID:    1,
			PageID:  100,
			SpaceID: 1,
			Offset:  0,
			Value:   []byte("test data"),
		}

		// 追加日志
		lsn, err := manager.Append(entry)
		require.NoError(t, err)
		assert.Equal(t, int64(1), lsn)

		// 刷新日志
		err = manager.Flush(lsn)
		require.NoError(t, err)

		// 验证日志文件存在
		_, err = os.Stat(filepath.Join(testDir, "redo.log"))
		assert.NoError(t, err)
	})

	t.Run("批量日志操作", func(t *testing.T) {
		// 创建多个日志条目
		for i := 0; i < 20; i++ {
			entry := &RedoLogEntry{
				TxID:    int64(i),
				PageID:  uint32(100 + i),
				SpaceID: 1,
				Offset:  uint16(i * 100),
				Value:   []byte("test data"),
			}
			_, err := manager.Append(entry)
			require.NoError(t, err)
		}

		// 等待后台刷新
		time.Sleep(2 * time.Second)

		// 检查文件大小是否增长
		info, err := os.Stat(filepath.Join(testDir, "redo.log"))
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(0))
	})

	t.Run("恢复操作", func(t *testing.T) {
		// 创建新的管理器（模拟重启）
		newManager, err := NewRedoLogManager(testDir, 10)
		require.NoError(t, err)
		defer newManager.Close()

		// 执行恢复
		err = newManager.Recover()
		require.NoError(t, err)
	})

	t.Run("检查点操作", func(t *testing.T) {
		// 创建检查点
		err := manager.Checkpoint()
		require.NoError(t, err)

		// 验证检查点文件存在
		_, err = os.Stat(filepath.Join(testDir, "redo_checkpoint"))
		assert.NoError(t, err)
	})
}

func TestRedoLogManager_Concurrent(t *testing.T) {
	testDir := t.TempDir()
	manager, err := NewRedoLogManager(testDir, 10)
	require.NoError(t, err)
	defer manager.Close()

	// 并发写入日志
	const numGoroutines = 10
	const numEntriesPerGoroutine = 100

	done := make(chan bool)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < numEntriesPerGoroutine; j++ {
				entry := &RedoLogEntry{
					TxID:    int64(id*numEntriesPerGoroutine + j),
					PageID:  uint32(id*1000 + j),
					SpaceID: 1,
					Offset:  uint16(j * 100),
					Value:   []byte("test data"),
				}
				_, err := manager.Append(entry)
				if err != nil {
					t.Error(err)
				}
			}
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// 验证日志文件
	info, err := os.Stat(filepath.Join(testDir, "redo.log"))
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))
}
