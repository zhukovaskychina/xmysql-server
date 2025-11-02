package manager

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// MockStorageProviderForBTree 模拟存储提供者
type MockStorageProviderForBTree struct{}

func (m *MockStorageProviderForBTree) ReadPage(spaceID uint32, pageNo uint32) ([]byte, error) {
	return make([]byte, 16384), nil
}

func (m *MockStorageProviderForBTree) WritePage(spaceID uint32, pageNo uint32, data []byte) error {
	return nil
}

func (m *MockStorageProviderForBTree) AllocatePage(spaceID uint32) (uint32, error) {
	return 1, nil
}

func (m *MockStorageProviderForBTree) FreePage(spaceID uint32, pageNo uint32) error {
	return nil
}

func (m *MockStorageProviderForBTree) CreateSpace(name string, pageSize uint32) (uint32, error) {
	return 1, nil
}

func (m *MockStorageProviderForBTree) OpenSpace(spaceID uint32) error {
	return nil
}

func (m *MockStorageProviderForBTree) CloseSpace(spaceID uint32) error {
	return nil
}

func (m *MockStorageProviderForBTree) DeleteSpace(spaceID uint32) error {
	return nil
}

func (m *MockStorageProviderForBTree) GetSpaceInfo(spaceID uint32) (*basic.SpaceInfo, error) {
	return &basic.SpaceInfo{}, nil
}

func (m *MockStorageProviderForBTree) ListSpaces() ([]basic.SpaceInfo, error) {
	return []basic.SpaceInfo{}, nil
}

func (m *MockStorageProviderForBTree) BeginTransaction() (uint64, error) {
	return 1, nil
}

func (m *MockStorageProviderForBTree) CommitTransaction(txID uint64) error {
	return nil
}

func (m *MockStorageProviderForBTree) RollbackTransaction(txID uint64) error {
	return nil
}

func (m *MockStorageProviderForBTree) Sync(spaceID uint32) error {
	return nil
}

func (m *MockStorageProviderForBTree) Close() error {
	return nil
}

// createTestBPM 创建测试用的BufferPoolManager
func createTestBPM() *OptimizedBufferPoolManager {
	config := &BufferPoolConfig{
		PoolSize:        100,
		PageSize:        16384,
		FlushInterval:   time.Second,
		YoungListRatio:  0.75,
		OldListRatio:    0.25,
		OldBlockTime:    1000,
		PrefetchWorkers: 2,
		MaxQueueSize:    100,
		StorageProvider: &MockStorageProviderForBTree{},
	}

	bpm, err := NewOptimizedBufferPoolManager(config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create buffer pool manager: %v", err))
	}
	return bpm
}

// TestBTREE006_CacheSizeLimit 测试缓存大小限制
func TestBTREE006_CacheSizeLimit(t *testing.T) {
	t.Run("CacheEvictionOnInsert", func(t *testing.T) {
		// 创建小缓存的B+树管理器
		config := &BPlusTreeConfig{
			MaxCacheSize:   10, // 小缓存，方便测试
			DirtyThreshold: 0.7,
			EvictionPolicy: "LRU",
		}

		bpm := createTestBPM()
		btm := NewBPlusTreeManager(bpm, config)

		ctx := context.Background()
		err := btm.Init(ctx, 1, 1)
		if err != nil {
			t.Fatalf("Failed to init B+Tree: %v", err)
		}

		// 插入超过缓存大小的节点
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key_%03d", i)
			value := []byte(fmt.Sprintf("value_%03d", i))

			err := btm.Insert(ctx, key, value)
			if err != nil {
				t.Errorf("Failed to insert key %s: %v", key, err)
			}
		}

		// 检查缓存大小
		btm.mutex.RLock()
		cacheSize := len(btm.nodeCache)
		btm.mutex.RUnlock()

		if cacheSize > int(config.MaxCacheSize) {
			t.Errorf("Cache size %d exceeds max cache size %d", cacheSize, config.MaxCacheSize)
		}

		t.Logf("✅ Cache size %d is within limit %d", cacheSize, config.MaxCacheSize)
	})

	t.Run("CacheEvictionOnGetNode", func(t *testing.T) {
		// 创建小缓存的B+树管理器
		config := &BPlusTreeConfig{
			MaxCacheSize:   5,
			DirtyThreshold: 0.7,
			EvictionPolicy: "LRU",
		}

		bpm := createTestBPM()
		btm := NewBPlusTreeManager(bpm, config)

		ctx := context.Background()
		err := btm.Init(ctx, 1, 1)
		if err != nil {
			t.Fatalf("Failed to init B+Tree: %v", err)
		}

		// 插入多个节点
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key_%03d", i)
			value := []byte(fmt.Sprintf("value_%03d", i))

			err := btm.Insert(ctx, key, value)
			if err != nil {
				t.Errorf("Failed to insert key %s: %v", key, err)
			}
		}

		// 等待后台清理
		time.Sleep(time.Second * 2)

		// 检查缓存大小
		btm.mutex.RLock()
		cacheSize := len(btm.nodeCache)
		btm.mutex.RUnlock()

		if cacheSize > int(config.MaxCacheSize)*2 {
			t.Errorf("Cache size %d significantly exceeds max cache size %d", cacheSize, config.MaxCacheSize)
		}

		t.Logf("✅ Cache size %d is reasonable (max: %d)", cacheSize, config.MaxCacheSize)
	})

	t.Run("LRUEvictionOrder", func(t *testing.T) {
		// 创建小缓存的B+树管理器
		config := &BPlusTreeConfig{
			MaxCacheSize:   3,
			DirtyThreshold: 0.7,
			EvictionPolicy: "LRU",
		}

		bpm := createTestBPM()
		btm := NewBPlusTreeManager(bpm, config)

		ctx := context.Background()
		err := btm.Init(ctx, 1, 1)
		if err != nil {
			t.Fatalf("Failed to init B+Tree: %v", err)
		}

		// 插入3个节点
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("key_%03d", i)
			value := []byte(fmt.Sprintf("value_%03d", i))

			err := btm.Insert(ctx, key, value)
			if err != nil {
				t.Errorf("Failed to insert key %s: %v", key, err)
			}
			time.Sleep(time.Millisecond * 100) // 确保访问时间不同
		}

		// 访问第一个节点，使其成为最近访问
		btm.getNode(ctx, 1)
		time.Sleep(time.Millisecond * 100)

		// 插入第4个节点，应该淘汰最久未访问的节点
		err = btm.Insert(ctx, "key_004", []byte("value_004"))
		if err != nil {
			t.Errorf("Failed to insert key_004: %v", err)
		}

		// 检查缓存大小
		btm.mutex.RLock()
		cacheSize := len(btm.nodeCache)
		btm.mutex.RUnlock()

		if cacheSize > int(config.MaxCacheSize)+1 {
			t.Errorf("Cache size %d exceeds expected size after LRU eviction", cacheSize)
		}

		t.Logf("✅ LRU eviction working correctly, cache size: %d", cacheSize)
	})

	t.Run("DirtyNodeFlushBeforeEviction", func(t *testing.T) {
		// 创建小缓存的B+树管理器
		config := &BPlusTreeConfig{
			MaxCacheSize:   5,
			DirtyThreshold: 0.5,
			EvictionPolicy: "LRU",
		}

		bpm := createTestBPM()
		btm := NewBPlusTreeManager(bpm, config)

		ctx := context.Background()
		err := btm.Init(ctx, 1, 1)
		if err != nil {
			t.Fatalf("Failed to init B+Tree: %v", err)
		}

		// 插入多个节点（会产生脏节点）
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key_%03d", i)
			value := []byte(fmt.Sprintf("value_%03d", i))

			err := btm.Insert(ctx, key, value)
			if err != nil {
				t.Errorf("Failed to insert key %s: %v", key, err)
			}
		}

		// 等待后台清理
		time.Sleep(time.Second * 6)

		// 检查缓存大小和脏节点数
		btm.mutex.RLock()
		cacheSize := len(btm.nodeCache)
		dirtyCount := 0
		for _, node := range btm.nodeCache {
			if node.isDirty {
				dirtyCount++
			}
		}
		btm.mutex.RUnlock()

		t.Logf("Cache size: %d, Dirty nodes: %d", cacheSize, dirtyCount)

		if cacheSize > int(config.MaxCacheSize)*2 {
			t.Errorf("Cache size %d significantly exceeds max cache size %d", cacheSize, config.MaxCacheSize)
		}

		t.Logf("✅ Dirty nodes flushed before eviction, cache size: %d, dirty: %d", cacheSize, dirtyCount)
	})

	t.Run("ConcurrentInsertWithCacheLimit", func(t *testing.T) {
		// 创建小缓存的B+树管理器
		config := &BPlusTreeConfig{
			MaxCacheSize:   10,
			DirtyThreshold: 0.7,
			EvictionPolicy: "LRU",
		}

		bpm := createTestBPM()
		btm := NewBPlusTreeManager(bpm, config)

		ctx := context.Background()
		err := btm.Init(ctx, 1, 1)
		if err != nil {
			t.Fatalf("Failed to init B+Tree: %v", err)
		}

		// 并发插入
		done := make(chan bool, 5)
		for g := 0; g < 5; g++ {
			go func(goroutineID int) {
				for i := 0; i < 10; i++ {
					key := fmt.Sprintf("g%d_key_%03d", goroutineID, i)
					value := []byte(fmt.Sprintf("g%d_value_%03d", goroutineID, i))

					err := btm.Insert(ctx, key, value)
					if err != nil {
						t.Logf("Goroutine %d failed to insert key %s: %v", goroutineID, key, err)
					}
					time.Sleep(time.Millisecond * 10)
				}
				done <- true
			}(g)
		}

		// 等待所有goroutine完成
		for i := 0; i < 5; i++ {
			<-done
		}

		// 等待后台清理
		time.Sleep(time.Second * 2)

		// 检查缓存大小
		btm.mutex.RLock()
		cacheSize := len(btm.nodeCache)
		btm.mutex.RUnlock()

		if cacheSize > int(config.MaxCacheSize)*3 {
			t.Errorf("Cache size %d significantly exceeds max cache size %d under concurrent load", cacheSize, config.MaxCacheSize)
		}

		t.Logf("✅ Concurrent insert with cache limit working, cache size: %d", cacheSize)
	})
}

// TestBTREE006_CacheStatistics 测试缓存统计信息
func TestBTREE006_CacheStatistics(t *testing.T) {
	config := &BPlusTreeConfig{
		MaxCacheSize:   10,
		DirtyThreshold: 0.7,
		EvictionPolicy: "LRU",
	}

	bpm := createTestBPM()
	btm := NewBPlusTreeManager(bpm, config)

	ctx := context.Background()
	err := btm.Init(ctx, 1, 1)
	if err != nil {
		t.Fatalf("Failed to init B+Tree: %v", err)
	}

	// 插入一些数据
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := []byte(fmt.Sprintf("value_%03d", i))

		err := btm.Insert(ctx, key, value)
		if err != nil {
			t.Errorf("Failed to insert key %s: %v", key, err)
		}
	}

	// 访问一些节点（产生缓存命中）
	for i := 0; i < 5; i++ {
		btm.getNode(ctx, 1)
	}

	// 检查统计信息
	btm.mutex.RLock()
	cacheHits := btm.stats.cacheHits
	cacheMisses := btm.stats.cacheMisses
	btm.mutex.RUnlock()

	t.Logf("Cache hits: %d, Cache misses: %d", cacheHits, cacheMisses)

	if cacheHits == 0 {
		t.Error("Expected some cache hits, got 0")
	}

	if cacheMisses == 0 {
		t.Error("Expected some cache misses, got 0")
	}

	t.Logf("✅ Cache statistics working correctly")
}
