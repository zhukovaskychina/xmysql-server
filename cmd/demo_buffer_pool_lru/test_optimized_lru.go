//go:build demo
// +build demo

package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("=== 浼樺寲LRU缂撳瓨鎬ц兘娴嬭瘯 ===")

	// 娴嬭瘯鍘熷LRU缂撳瓨
	fmt.Println("\n1. 鍘熷LRU缂撳瓨娴嬭瘯")
	testOriginalLRU()

	// 娴嬭瘯浼樺寲LRU缂撳瓨
	fmt.Println("\n2. 浼樺寲LRU缂撳瓨娴嬭瘯")
	testOptimizedLRU()

	// 骞跺彂鎬ц兘娴嬭瘯
	fmt.Println("\n3. 骞跺彂鎬ц兘瀵规瘮")
	compareConcurrentPerformance()

	// 娴嬭瘯浼樺寲鐨凚ufferPoolManager
	fmt.Println("\n4. 浼樺寲BufferPoolManager娴嬭瘯")
	testOptimizedBufferPoolManager()
}

func testOriginalLRU() {
	// 鍒涘缓鍘熷LRU缂撳瓨
	cache := buffer_pool.NewLRUCacheImpl(1000, 0.75, 0.25, 1000)

	start := time.Now()

	// 鎻掑叆鏁版嵁
	for i := uint32(0); i < 10000; i++ {
		page := buffer_pool.NewBufferPage(1, i)
		page.SetContent(make([]byte, 1024))
		block := buffer_pool.NewBufferBlock(page)
		cache.Set(1, i, block)
	}

	// 闅忔満璁块棶
	for i := uint32(0); i < 5000; i++ {
		cache.Get(1, i%1000)
	}

	elapsed := time.Since(start)
	logger.Debugf("鍘熷LRU缂撳瓨鎿嶄綔鑰楁椂: %v\n", elapsed)

	// 鏄剧ず缁熻淇℃伅
	if stats, ok := cache.(interface {
		HitCount() uint64
		MissCount() uint64
		HitRate() float64
	}); ok {
		logger.Debugf("鍛戒腑娆℃暟: %d, 鏈懡涓鏁? %d, 鍛戒腑鐜? %.2f%%\n",
			stats.HitCount(), stats.MissCount(), stats.HitRate()*100)
	}
}

func testOptimizedLRU() {
	// 鍒涘缓浼樺寲LRU缂撳瓨
	cache := buffer_pool.NewOptimizedLRUCache(1000, 0.75, 0.25, 1000)

	start := time.Now()

	// 鎻掑叆鏁版嵁
	for i := uint32(0); i < 10000; i++ {
		page := buffer_pool.NewBufferPage(1, i)
		page.SetContent(make([]byte, 1024))
		block := buffer_pool.NewBufferBlock(page)
		cache.Set(1, i, block)
	}

	// 闅忔満璁块棶
	for i := uint32(0); i < 5000; i++ {
		cache.Get(1, i%1000)
	}

	elapsed := time.Since(start)
	logger.Debugf("浼樺寲LRU缂撳瓨鎿嶄綔鑰楁椂: %v\n", elapsed)

	// 鏄剧ず缁熻淇℃伅
	logger.Debugf("鍛戒腑娆℃暟: %d, 鏈懡涓鏁? %d, 鍛戒腑鐜? %.2f%%\n",
		cache.HitCount(), cache.MissCount(), cache.HitRate()*100)
}

func compareConcurrentPerformance() {
	const numGoroutines = 10
	const operationsPerGoroutine = 1000

	// 娴嬭瘯鍘熷缂撳瓨骞跺彂鎬ц兘
	fmt.Println("\n鍘熷LRU缂撳瓨骞跺彂娴嬭瘯:")
	testConcurrentOriginal(numGoroutines, operationsPerGoroutine)

	// 娴嬭瘯浼樺寲缂撳瓨骞跺彂鎬ц兘
	fmt.Println("\n浼樺寲LRU缂撳瓨骞跺彂娴嬭瘯:")
	testConcurrentOptimized(numGoroutines, operationsPerGoroutine)
}

func testConcurrentOriginal(numGoroutines, operationsPerGoroutine int) {
	cache := buffer_pool.NewLRUCacheImpl(1000, 0.75, 0.25, 1000)

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				spaceID := uint32(1)
				pageNo := uint32(goroutineID*operationsPerGoroutine + j)

				// 鍐欐搷浣?
				if j%2 == 0 {
					page := buffer_pool.NewBufferPage(spaceID, pageNo)
					page.SetContent(make([]byte, 1024))
					block := buffer_pool.NewBufferBlock(page)
					cache.Set(spaceID, pageNo%500, block) // 闄愬埗鍦?00涓〉闈㈠唴
				} else {
					// 璇绘搷浣?
					cache.Get(spaceID, pageNo%500)
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	logger.Debugf("  %d涓猤oroutine锛屾瘡涓?d娆℃搷浣淺n", numGoroutines, operationsPerGoroutine)
	logger.Debugf("  鎬昏€楁椂: %v\n", elapsed)
	logger.Debugf("  骞冲潎姣忔鎿嶄綔: %v\n", elapsed/time.Duration(numGoroutines*operationsPerGoroutine))
}

func testConcurrentOptimized(numGoroutines, operationsPerGoroutine int) {
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

				// 鍐欐搷浣?
				if j%2 == 0 {
					page := buffer_pool.NewBufferPage(spaceID, pageNo)
					page.SetContent(make([]byte, 1024))
					block := buffer_pool.NewBufferBlock(page)
					cache.Set(spaceID, pageNo%500, block) // 闄愬埗鍦?00涓〉闈㈠唴
				} else {
					// 璇绘搷浣?
					cache.Get(spaceID, pageNo%500)
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	logger.Debugf("  %d涓猤oroutine锛屾瘡涓?d娆℃搷浣淺n", numGoroutines, operationsPerGoroutine)
	logger.Debugf("  鎬昏€楁椂: %v\n", elapsed)
	logger.Debugf("  骞冲潎姣忔鎿嶄綔: %v\n", elapsed/time.Duration(numGoroutines*operationsPerGoroutine))

	// 鏄剧ず浼樺寲缂撳瓨鐨勭粺璁′俊鎭?
	logger.Debugf("  鍛戒腑鐜? %.2f%%\n", cache.HitRate()*100)
	logger.Debugf("  缂撳瓨澶у皬: %d\n", cache.Len())
}

func testOptimizedBufferPoolManager() {
	// 鍒涘缓妯℃嫙鐨勫瓨鍌ㄦ彁渚涜€?
	storageProvider := &MockStorageProvider{}

	// 鍒涘缓閰嶇疆
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

	// 鍒涘缓浼樺寲鐨凚ufferPoolManager
	bpm, err := manager.NewOptimizedBufferPoolManager(config)
	if err != nil {
		logger.Debugf("鍒涘缓OptimizedBufferPoolManager澶辫触: %v\n", err)
		return
	}
	defer bpm.Close()

	fmt.Println("鎴愬姛鍒涘缓OptimizedBufferPoolManager")

	// 娴嬭瘯鍩烘湰鎿嶄綔
	start := time.Now()

	// 鑾峰彇涓€浜涢〉闈?
	for i := uint32(0); i < 50; i++ {
		page, err := bpm.GetPage(1, i)
		if err != nil {
			logger.Debugf("鑾峰彇椤甸潰澶辫触: %v\n", err)
			continue
		}

		// 鏍囪涓€浜涢〉闈负鑴忛〉
		if i%3 == 0 {
			bpm.MarkDirty(1, i)
		}

		// 妯℃嫙浣跨敤椤甸潰
		_ = page
	}

	// 鍒锋柊鎵€鏈夎剰椤?
	if err := bpm.FlushAllPages(); err != nil {
		logger.Debugf("鍒锋柊鑴忛〉澶辫触: %v\n", err)
	}

	elapsed := time.Since(start)
	logger.Debugf("BufferPoolManager鎿嶄綔鑰楁椂: %v\n", elapsed)

	// 鏄剧ず缁熻淇℃伅
	stats := bpm.GetStats()
	logger.Debugf("缁熻淇℃伅:\n")
	for key, value := range stats {
		logger.Debugf("  %s: %v\n", key, value)
	}

	// 鍐呭瓨浣跨敤鎯呭喌
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	logger.Debugf("鍐呭瓨浣跨敤: %.2f MB\n", float64(m.Alloc)/1024/1024)
}

// MockStorageProvider 妯℃嫙瀛樺偍鎻愪緵鑰?
type MockStorageProvider struct{}

func (msp *MockStorageProvider) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	// 杩斿洖妯℃嫙椤甸潰鏁版嵁
	data := make([]byte, 16384)
	// 濉厖涓€浜涙祴璇曟暟鎹?
	for i := 0; i < len(data); i += 4 {
		data[i] = byte(spaceID)
		data[i+1] = byte(spaceID >> 8)
		data[i+2] = byte(pageNo)
		data[i+3] = byte(pageNo >> 8)
	}
	return data, nil
}

func (msp *MockStorageProvider) WritePage(spaceID, pageNo uint32, data []byte) error {
	// 妯℃嫙鍐欏叆鎿嶄綔
	return nil
}

// 瀹炵幇鍏朵粬蹇呴渶鐨勬帴鍙ｆ柟娉曪紙绠€鍖栧疄鐜帮級
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
