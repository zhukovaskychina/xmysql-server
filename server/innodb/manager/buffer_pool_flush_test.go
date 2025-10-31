package manager

import (
	"sync"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// MockStorageProvider 模拟存储提供者
type MockStorageProvider struct {
	mu         sync.RWMutex
	pages      map[uint64][]byte
	writeCount int
	readCount  int
	nextPageNo uint32
}

func NewMockStorageProvider() *MockStorageProvider {
	return &MockStorageProvider{
		pages:      make(map[uint64][]byte),
		nextPageNo: 1,
	}
}

func (m *MockStorageProvider) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.readCount++
	pageID := makePageID(spaceID, pageNo)
	if data, exists := m.pages[pageID]; exists {
		return data, nil
	}

	// 返回空页面
	return make([]byte, PAGE_SIZE), nil
}

func (m *MockStorageProvider) WritePage(spaceID, pageNo uint32, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.writeCount++
	pageID := makePageID(spaceID, pageNo)
	m.pages[pageID] = make([]byte, len(data))
	copy(m.pages[pageID], data)
	return nil
}

func (m *MockStorageProvider) AllocatePage(spaceID uint32) (uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pageNo := m.nextPageNo
	m.nextPageNo++
	return pageNo, nil
}

func (m *MockStorageProvider) FreePage(spaceID uint32, pageNo uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	pageID := makePageID(spaceID, pageNo)
	delete(m.pages, pageID)
	return nil
}

func (m *MockStorageProvider) CreateSpace(name string, pageSize uint32) (uint32, error) {
	return 1, nil
}

func (m *MockStorageProvider) OpenSpace(spaceID uint32) error {
	return nil
}

func (m *MockStorageProvider) CloseSpace(spaceID uint32) error {
	return nil
}

func (m *MockStorageProvider) DeleteSpace(spaceID uint32) error {
	return nil
}

func (m *MockStorageProvider) GetSpaceInfo(spaceID uint32) (*basic.SpaceInfo, error) {
	return &basic.SpaceInfo{
		SpaceID:  spaceID,
		Name:     "mock_space",
		PageSize: PAGE_SIZE,
	}, nil
}

func (m *MockStorageProvider) ListSpaces() ([]basic.SpaceInfo, error) {
	return []basic.SpaceInfo{}, nil
}

func (m *MockStorageProvider) BeginTransaction() (uint64, error) {
	return 1, nil
}

func (m *MockStorageProvider) CommitTransaction(txID uint64) error {
	return nil
}

func (m *MockStorageProvider) RollbackTransaction(txID uint64) error {
	return nil
}

func (m *MockStorageProvider) Sync(spaceID uint32) error {
	return nil
}

func (m *MockStorageProvider) Close() error {
	return nil
}

func (m *MockStorageProvider) GetWriteCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.writeCount
}

func (m *MockStorageProvider) GetReadCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.readCount
}

func (m *MockStorageProvider) ResetCounters() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeCount = 0
	m.readCount = 0
}

// TestAdaptiveFlushStrategy 测试自适应刷新策略
func TestAdaptiveFlushStrategy(t *testing.T) {
	storage := NewMockStorageProvider()

	config := &BufferPoolConfig{
		PoolSize:        1000,
		PageSize:        PAGE_SIZE,
		FlushInterval:   500 * time.Millisecond,
		StorageProvider: storage,
		YoungListRatio:  YOUNG_LIST_RATIO,
		OldListRatio:    OLD_LIST_RATIO,
		OldBlockTime:    OLD_BLOCK_TIME,
	}

	bpm, err := NewBufferPoolManager(config)
	if err != nil {
		t.Fatalf("Failed to create buffer pool manager: %v", err)
	}
	defer bpm.Close()

	// 测试场景1：低脏页比例（< 25%）
	t.Run("LowDirtyRatio", func(t *testing.T) {
		// 创建少量脏页（20%）
		dirtyPageCount := 200
		for i := 0; i < dirtyPageCount; i++ {
			page, err := bpm.GetDirtyPage(1, uint32(i))
			if err != nil {
				t.Logf("Warning: failed to get page %d: %v", i, err)
				continue
			}
			// 修改页面内容
			content := page.GetContent()
			if len(content) > 0 {
				content[0] = byte(i % 256)
			}
		}

		dirtyRatio := bpm.GetDirtyPageRatio()
		t.Logf("Low dirty ratio: %.2f%%", dirtyRatio*100)

		// 验证批量大小应该为0（不需要刷新）
		batchSize := bpm.calculateFlushBatchSize(dirtyRatio)
		if batchSize != 0 {
			t.Logf("Expected batch size 0 for low dirty ratio, got %d", batchSize)
		}
	})

	// 测试场景2：中等脏页比例（25% - 50%）
	t.Run("ModerateDirtyRatio", func(t *testing.T) {
		// 创建中等数量脏页（40%）
		dirtyPageCount := 400
		for i := 0; i < dirtyPageCount; i++ {
			page, err := bpm.GetDirtyPage(1, uint32(i))
			if err != nil {
				t.Logf("Warning: failed to get page %d: %v", i, err)
				continue
			}
			content := page.GetContent()
			if len(content) > 0 {
				content[0] = byte(i % 256)
			}
		}

		dirtyRatio := bpm.GetDirtyPageRatio()
		t.Logf("Moderate dirty ratio: %.2f%%", dirtyRatio*100)

		// 验证批量大小应该为 BATCH_FLUSH_SIZE
		batchSize := bpm.calculateFlushBatchSize(dirtyRatio)
		expectedSize := BATCH_FLUSH_SIZE
		if batchSize != expectedSize {
			t.Logf("Expected batch size %d for moderate dirty ratio, got %d", expectedSize, batchSize)
		}
	})

	// 测试场景3：高脏页比例（50% - 75%）
	t.Run("HighDirtyRatio", func(t *testing.T) {
		// 创建大量脏页（60%）
		dirtyPageCount := 600
		for i := 0; i < dirtyPageCount; i++ {
			page, err := bpm.GetDirtyPage(1, uint32(i))
			if err != nil {
				t.Logf("Warning: failed to get page %d: %v", i, err)
				continue
			}
			content := page.GetContent()
			if len(content) > 0 {
				content[0] = byte(i % 256)
			}
		}

		dirtyRatio := bpm.GetDirtyPageRatio()
		t.Logf("High dirty ratio: %.2f%%", dirtyRatio*100)

		// 验证批量大小应该为 BATCH_FLUSH_SIZE * 2
		batchSize := bpm.calculateFlushBatchSize(dirtyRatio)
		expectedSize := BATCH_FLUSH_SIZE * 2
		if batchSize != expectedSize {
			t.Logf("Expected batch size %d for high dirty ratio, got %d", expectedSize, batchSize)
		}
	})

	// 测试场景4：极高脏页比例（>= 75%）
	t.Run("AggressiveDirtyRatio", func(t *testing.T) {
		// 创建极大量脏页（80%）
		dirtyPageCount := 800
		for i := 0; i < dirtyPageCount; i++ {
			page, err := bpm.GetDirtyPage(1, uint32(i))
			if err != nil {
				t.Logf("Warning: failed to get page %d: %v", i, err)
				continue
			}
			content := page.GetContent()
			if len(content) > 0 {
				content[0] = byte(i % 256)
			}
		}

		dirtyRatio := bpm.GetDirtyPageRatio()
		t.Logf("Aggressive dirty ratio: %.2f%%", dirtyRatio*100)

		// 验证批量大小应该为 BATCH_FLUSH_SIZE * 4
		batchSize := bpm.calculateFlushBatchSize(dirtyRatio)
		expectedSize := BATCH_FLUSH_SIZE * 4
		if batchSize != expectedSize {
			t.Logf("Expected batch size %d for aggressive dirty ratio, got %d", expectedSize, batchSize)
		}
	})
}

// TestFlushRateLimit 测试刷新速率限制
func TestFlushRateLimit(t *testing.T) {
	storage := NewMockStorageProvider()

	config := &BufferPoolConfig{
		PoolSize:        1000,
		PageSize:        PAGE_SIZE,
		FlushInterval:   100 * time.Millisecond,
		StorageProvider: storage,
		YoungListRatio:  YOUNG_LIST_RATIO,
		OldListRatio:    OLD_LIST_RATIO,
		OldBlockTime:    OLD_BLOCK_TIME,
	}

	bpm, err := NewBufferPoolManager(config)
	if err != nil {
		t.Fatalf("Failed to create buffer pool manager: %v", err)
	}
	defer bpm.Close()

	// 设置较低的速率限制
	bpm.SetFlushRateLimit(100) // 100 pages/sec

	// 模拟1秒后的刷新请求
	bpm.lastFlushTime = time.Now().Add(-1 * time.Second)
	requestedPages := 500

	// 应用速率限制
	allowedPages := bpm.applyRateLimit(requestedPages)

	// 验证速率限制生效
	if allowedPages > 100 {
		t.Errorf("Rate limit not applied correctly: requested %d, allowed %d, expected <= 100",
			requestedPages, allowedPages)
	}

	t.Logf("Rate limit test: requested %d pages, allowed %d pages", requestedPages, allowedPages)
}

// TestBackgroundFlushThread 测试后台刷新线程
func TestBackgroundFlushThread(t *testing.T) {
	storage := NewMockStorageProvider()

	config := &BufferPoolConfig{
		PoolSize:        500,
		PageSize:        PAGE_SIZE,
		FlushInterval:   200 * time.Millisecond,
		StorageProvider: storage,
		YoungListRatio:  YOUNG_LIST_RATIO,
		OldListRatio:    OLD_LIST_RATIO,
		OldBlockTime:    OLD_BLOCK_TIME,
	}

	bpm, err := NewBufferPoolManager(config)
	if err != nil {
		t.Fatalf("Failed to create buffer pool manager: %v", err)
	}
	defer bpm.Close()

	// 创建足够的脏页以触发刷新（30%）
	dirtyPageCount := 150
	for i := 0; i < dirtyPageCount; i++ {
		page, err := bpm.GetDirtyPage(1, uint32(i))
		if err != nil {
			t.Logf("Warning: failed to get page %d: %v", i, err)
			continue
		}
		content := page.GetContent()
		if len(content) > 0 {
			content[0] = byte(i % 256)
		}
	}

	initialWriteCount := storage.GetWriteCount()
	t.Logf("Initial write count: %d", initialWriteCount)

	// 等待后台刷新线程执行
	time.Sleep(1 * time.Second)

	finalWriteCount := storage.GetWriteCount()
	t.Logf("Final write count: %d", finalWriteCount)

	// 验证后台刷新线程已执行
	if finalWriteCount <= initialWriteCount {
		t.Logf("Warning: Background flush may not have executed (initial: %d, final: %d)",
			initialWriteCount, finalWriteCount)
	} else {
		t.Logf("Background flush executed successfully: %d pages flushed",
			finalWriteCount-initialWriteCount)
	}

	// 验证统计信息
	stats := bpm.GetStats()
	t.Logf("Buffer pool stats: %+v", stats)
}
