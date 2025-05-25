package manager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferPoolManager(t *testing.T) {
	bpm := NewBufferPoolManager(100) // 创建100页的缓冲池
	defer bpm.Close()

	t.Run("基本页面操作", func(t *testing.T) {
		// 获取页面
		page, err := bpm.GetPage(1, 1)
		require.NoError(t, err)
		require.NotNil(t, page)

		// 标记为脏页
		bpm.MarkDirty(1, 1)

		// 解除固定
		bpm.UnpinPage(1, 1)

		// 刷新页面
		err = bpm.FlushPage(1, 1)
		require.NoError(t, err)
	})

	t.Run("缓存命中测试", func(t *testing.T) {
		// 首次访问（未命中）
		page1, err := bpm.GetPage(1, 2)
		require.NoError(t, err)
		require.NotNil(t, page1)

		// 再次访问（命中）
		page2, err := bpm.GetPage(1, 2)
		require.NoError(t, err)
		require.NotNil(t, page2)

		// 验证统计信息
		stats := bpm.GetStats()
		assert.Equal(t, uint64(1), stats["hits"])
		assert.Equal(t, uint64(1), stats["misses"])
	})

	t.Run("并发访问测试", func(t *testing.T) {
		const numGoroutines = 10
		done := make(chan bool)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				for j := 0; j < 10; j++ {
					page, err := bpm.GetPage(uint32(id), uint32(j))
					if err != nil {
						t.Error(err)
						return
					}
					bpm.MarkDirty(uint32(id), uint32(j))
					bpm.UnpinPage(uint32(id), uint32(j))
				}
				done <- true
			}(i)
		}

		// 等待所有goroutine完成
		for i := 0; i < numGoroutines; i++ {
			<-done
		}
	})

	t.Run("脏页刷新测试", func(t *testing.T) {
		// 创建多个脏页
		for i := 0; i < 10; i++ {
			page, err := bpm.GetPage(2, uint32(i))
			require.NoError(t, err)
			require.NotNil(t, page)
			bpm.MarkDirty(2, uint32(i))
		}

		// 等待后台刷新
		time.Sleep(2 * time.Second)

		// 检查刷新统计
		stats := bpm.GetStats()
		assert.Greater(t, stats["flushes"], uint64(0))
	})

	t.Run("缓冲池满载测试", func(t *testing.T) {
		// 尝试加载超过缓冲池大小的页面
		for i := 0; i < 150; i++ { // 超过100页的容量
			page, err := bpm.GetPage(3, uint32(i))
			if err != nil {
				// 期望在某个点会触发页面驱逐
				assert.Equal(t, ErrNoFreePages, err)
				break
			}
			require.NotNil(t, page)
		}

		// 验证是否发生驱逐
		stats := bpm.GetStats()
		assert.Greater(t, stats["evictions"], uint64(0))
	})

	t.Run("页面固定测试", func(t *testing.T) {
		// 获取并固定页面
		page, err := bpm.GetPage(4, 1)
		require.NoError(t, err)
		require.NotNil(t, page)

		// 验证页面是否被固定
		assert.True(t, page.IsPinned())

		// 解除固定
		bpm.UnpinPage(4, 1)
		assert.False(t, page.IsPinned())
	})
}

func TestBufferPoolManagerEdgeCases(t *testing.T) {
	t.Run("零大小缓冲池", func(t *testing.T) {
		bpm := NewBufferPoolManager(0) // 应该使用默认大小
		defer bpm.Close()

		page, err := bpm.GetPage(1, 1)
		require.NoError(t, err)
		require.NotNil(t, page)
	})

	t.Run("重复页面访问", func(t *testing.T) {
		bpm := NewBufferPoolManager(10)
		defer bpm.Close()

		// 多次访问同一页面
		for i := 0; i < 100; i++ {
			page, err := bpm.GetPage(1, 1)
			require.NoError(t, err)
			require.NotNil(t, page)
		}

		// 验证命中率
		stats := bpm.GetStats()
		assert.Equal(t, uint64(99), stats["hits"])
		assert.Equal(t, uint64(1), stats["misses"])
	})

	t.Run("关闭后访问", func(t *testing.T) {
		bpm := NewBufferPoolManager(10)
		bpm.Close()

		// 关闭后尝试访问
		_, err := bpm.GetPage(1, 1)
		assert.Error(t, err)
	})
}

func TestBufferPoolManagerStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	bpm := NewBufferPoolManager(1000)
	defer bpm.Close()

	t.Run("大量并发访问", func(t *testing.T) {
		const (
			numGoroutines = 100
			numOperations = 1000
		)

		done := make(chan bool)
		start := time.Now()

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				for j := 0; j < numOperations; j++ {
					spaceID := uint32(j % 10)
					pageNo := uint32(j)

					// 随机操作
					switch j % 3 {
					case 0: // 读取
						page, err := bpm.GetPage(spaceID, pageNo)
						if err == nil {
							bpm.UnpinPage(spaceID, pageNo)
						}
					case 1: // 写入
						page, err := bpm.GetPage(spaceID, pageNo)
						if err == nil {
							bpm.MarkDirty(spaceID, pageNo)
							bpm.UnpinPage(spaceID, pageNo)
						}
					case 2: // 刷新
						bpm.FlushPage(spaceID, pageNo)
					}
				}
				done <- true
			}(i)
		}

		// 等待所有goroutine完成
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		duration := time.Since(start)
		t.Logf("Stress test completed in %v", duration)

		// 输出统计信息
		stats := bpm.GetStats()
		t.Logf("Statistics: %+v", stats)
	})
}
