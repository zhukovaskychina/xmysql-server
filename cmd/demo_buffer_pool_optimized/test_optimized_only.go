package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=== 优化LRU缓存测试 ===")

	// 测试优化LRU缓存
	fmt.Println("\n1. 优化LRU缓存基本功能测试")
	testOptimizedLRU()

	// 并发性能测试
	fmt.Println("\n2. 优化LRU缓存并发测试")
	testConcurrentOptimized()

	// 测试优化的BufferPoolManager
	fmt.Println("\n3. 优化BufferPoolManager测试")
	testOptimizedBufferPoolManager()
}

func testOptimizedLRU() {
	// 创建优化LRU缓存
	cache := buffer_pool.NewOptimizedLRUCache(1000, 0.75, 0.25, 1000)

	start := time.Now()

	// 插入数据
	for i := uint32(0); i < 1000; i++ {
		page := buffer_pool.NewBufferPage(1, i)
		page.SetContent(make([]byte, 1024))
		block := buffer_pool.NewBufferBlock(page)
		cache.Set(1, i, block)
	}

	// 随机访问
	for i := uint32(0); i < 500; i++ {
		cache.Get(1, i%100)
	}

	elapsed := time.Since(start)
	util.Debugf("优化LRU缓存操作耗时: %v\n", elapsed)

	// 显示统计信息
	util.Debugf("命中次数: %d, 未命中次数: %d, 命中率: %.2f%%\n",
		cache.HitCount(), cache.MissCount(), cache.HitRate()*100)
	util.Debugf("缓存大小: %d\n", cache.Len())
}

func testConcurrentOptimized() {
	const numGoroutines = 10
	const operationsPerGoroutine = 100

	cache := buffer_pool.NewOptimizedLRUCache(1000, 0.75, 0.25, 1000)

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				spaceID := uint32(1)
				pageNo := uint32(goroutineID*operationsPerGoroutine + j)

				// 写操作
				if j%2 == 0 {
					page := buffer_pool.NewBufferPage(spaceID, pageNo)
					page.SetContent(make([]byte, 1024))
					block := buffer_pool.NewBufferBlock(page)
					cache.Set(spaceID, pageNo%50, block) // 限制在50个页面内
				} else {
					// 读操作
					cache.Get(spaceID, pageNo%50)
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	util.Debugf("  %d个goroutine，每个%d次操作\n", numGoroutines, operationsPerGoroutine)
	util.Debugf("  总耗时: %v\n", elapsed)
	util.Debugf("  平均每次操作: %v\n", elapsed/time.Duration(numGoroutines*operationsPerGoroutine))

	// 显示优化缓存的统计信息
	util.Debugf("  命中率: %.2f%%\n", cache.HitRate()*100)
	util.Debugf("  缓存大小: %d\n", cache.Len())
}

func testOptimizedBufferPoolManager() {
	// 创建模拟的存储提供者
	storageProvider := &MockStorageProvider{}

	// 创建配置
	config := &manager.BufferPoolConfig{
		PoolSize:        100,
		PageSize:        16384,
		FlushInterval:   time.Second,
		YoungListRatio:  0.75,
		OldListRatio:    0.25,
		OldBlockTime:    1000,
		PrefetchWorkers: 2,
		MaxQueueSize:    100,
		StorageProvider: storageProvider,
	}

	// 创建优化的BufferPoolManager
	bpm, err := manager.NewOptimizedBufferPoolManager(config)
	if err != nil {
		util.Debugf("创建OptimizedBufferPoolManager失败: %v\n", err)
		return
	}
	defer bpm.Close()

	fmt.Println("成功创建OptimizedBufferPoolManager")

	// 测试基本操作
	start := time.Now()

	// 获取一些页面
	for i := uint32(0); i < 20; i++ {
		page, err := bpm.GetPage(1, i)
		if err != nil {
			util.Debugf("获取页面失败: %v\n", err)
			continue
		}

		// 标记一些页面为脏页
		if i%3 == 0 {
			bpm.MarkDirty(1, i)
		}

		// 模拟使用页面
		_ = page
	}

	// 刷新所有脏页
	if err := bpm.FlushAllPages(); err != nil {
		util.Debugf("刷新脏页失败: %v\n", err)
	}

	elapsed := time.Since(start)
	util.Debugf("BufferPoolManager操作耗时: %v\n", elapsed)

	// 显示统计信息
	stats := bpm.GetStats()
	util.Debugf("统计信息:\n")
	for key, value := range stats {
		util.Debugf("  %s: %v\n", key, value)
	}

	// 内存使用情况
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	util.Debugf("内存使用: %.2f MB\n", float64(m.Alloc)/1024/1024)
}

// MockStorageProvider 模拟存储提供者
type MockStorageProvider struct{}

func (msp *MockStorageProvider) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	// 返回模拟页面数据
	data := make([]byte, 16384)
	// 填充一些测试数据
	for i := 0; i < len(data); i += 4 {
		data[i] = byte(spaceID)
		data[i+1] = byte(spaceID >> 8)
		data[i+2] = byte(pageNo)
		data[i+3] = byte(pageNo >> 8)
	}
	return data, nil
}

func (msp *MockStorageProvider) WritePage(spaceID, pageNo uint32, data []byte) error {
	// 模拟写入操作
	return nil
}

// 实现其他必需的接口方法（简化实现）
func (msp *MockStorageProvider) AllocatePage(spaceID uint32) (uint32, error) {
	return 1, nil
}

func (msp *MockStorageProvider) FreePage(spaceID, pageNo uint32) error {
	return nil
}

func (msp *MockStorageProvider) CreateSpace(name string, pageSize uint32) (uint32, error) {
	return 1, nil
}

func (msp *MockStorageProvider) OpenSpace(spaceID uint32) error {
	return nil
}

func (msp *MockStorageProvider) CloseSpace(spaceID uint32) error {
	return nil
}

func (msp *MockStorageProvider) DeleteSpace(spaceID uint32) error {
	return nil
}

func (msp *MockStorageProvider) GetSpaceInfo(spaceID uint32) (*basic.SpaceInfo, error) {
	return nil, nil
}

func (msp *MockStorageProvider) ListSpaces() ([]basic.SpaceInfo, error) {
	return nil, nil
}

func (msp *MockStorageProvider) BeginTransaction() (uint64, error) {
	return 1, nil
}

func (msp *MockStorageProvider) CommitTransaction(txID uint64) error {
	return nil
}

func (msp *MockStorageProvider) RollbackTransaction(txID uint64) error {
	return nil
}

func (msp *MockStorageProvider) Sync(spaceID uint32) error {
	return nil
}

func (msp *MockStorageProvider) Close() error {
	return nil
}
