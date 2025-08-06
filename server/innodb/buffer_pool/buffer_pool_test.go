package buffer_pool

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"testing"
)

// MockStorageManager 模拟存储管理器
type MockStorageManager struct{}

func (m *MockStorageManager) GetSpace(spaceID uint32) (basic.Space, error) {
	return &MockSpace{spaceID: spaceID}, nil
}

// MockSpace 模拟表空间
type MockSpace struct {
	spaceID uint32
}

func (m *MockSpace) ID() uint32 {
	return m.spaceID
}

func (m *MockSpace) LoadPageByPageNumber(pageNo uint32) ([]byte, error) {
	// 返回模拟的页面数据
	data := make([]byte, 16384)
	// 设置页面头部信息
	data[4] = byte(pageNo)
	data[5] = byte(pageNo >> 8)
	data[6] = byte(pageNo >> 16)
	data[7] = byte(pageNo >> 24)
	return data, nil
}

func (m *MockSpace) FlushToDisk(pageNo uint32, data []byte) error {
	return nil
}

// TestPageIntegrityChecker 测试页面完整性检查器
func TestPageIntegrityChecker(t *testing.T) {
	checker := pages.NewPageIntegrityChecker(pages.ChecksumCRC32)

	// 创建测试页面数据
	pageData := make([]byte, 16384)

	// 计算校验和
	checksum := checker.CalculateChecksum(pageData)

	// 设置校验和到页面头部
	pageData[0] = byte(checksum)
	pageData[1] = byte(checksum >> 8)
	pageData[2] = byte(checksum >> 16)
	pageData[3] = byte(checksum >> 24)

	// 验证校验和
	if err := checker.ValidateChecksum(pageData); err != nil {
		t.Errorf("ValidateChecksum failed: %v", err)
	}

	// 测试损坏的页面
	pageData[100] = 0xFF // 修改数据
	if err := checker.ValidateChecksum(pageData); err == nil {
		t.Error("Expected checksum validation to fail for corrupted page")
	}
}

// TestPrefetchManager 测试预读管理器
func TestPrefetchManager(t *testing.T) {
	config := &BufferPoolConfig{
		TotalPages:       100,
		PageSize:         16384,
		BufferPoolSize:   1638400,
		YoungListPercent: 0.6,
		OldListPercent:   0.4,
		OldBlocksTime:    1000,
		PrefetchSize:     4,
		MaxQueueSize:     10,
		PrefetchWorkers:  2,
		StorageManager:   &MockStorageManager{},
	}

	bp := NewBufferPool(config)

	// 测试访问历史更新
	bp.prefetchManager.UpdateAccessHistory(1, 100)
	bp.prefetchManager.UpdateAccessHistory(1, 101)
	bp.prefetchManager.UpdateAccessHistory(1, 102)

	// 测试模式分析
	pattern := bp.prefetchManager.AnalyzeAccessPattern()
	if pattern == PatternUnknown {
		t.Log("Pattern is unknown, which is expected with limited history")
	}

	// 测试智能预读触发
	bp.prefetchManager.TriggerSmartPrefetch(1, 100)

	// 检查队列长度
	queueLen := bp.prefetchManager.GetQueueLength()
	if queueLen < 0 {
		t.Error("Queue length should not be negative")
	}
}

// TestFlushStrategy 测试刷新策略
func TestFlushStrategy(t *testing.T) {
	// 创建测试页面
	pages := []*BufferPage{
		NewBufferPage(1, 1),
		NewBufferPage(1, 2),
		NewBufferPage(1, 3),
	}

	// 设置不同的LSN
	pages[0].newestModification = 100
	pages[1].newestModification = 200
	pages[2].newestModification = 50

	// 测试LSN策略
	lsnStrategy := NewLSNBasedFlushStrategy()
	selected := lsnStrategy.SelectPagesToFlush(pages, 2)

	if len(selected) != 2 {
		t.Errorf("Expected 2 pages, got %d", len(selected))
	}

	// 应该选择LSN最小的页面
	if selected[0].GetLSN() != 50 {
		t.Errorf("Expected LSN 50, got %d", selected[0].GetLSN())
	}
}

// TestBufferPool 测试缓冲池基本功能
func TestBufferPool(t *testing.T) {
	config := &BufferPoolConfig{
		TotalPages:       100,
		PageSize:         16384,
		BufferPoolSize:   1638400,
		YoungListPercent: 0.6,
		OldListPercent:   0.4,
		OldBlocksTime:    1000,
		PrefetchSize:     4,
		MaxQueueSize:     10,
		PrefetchWorkers:  2,
		StorageManager:   &MockStorageManager{},
	}

	bp := NewBufferPool(config)

	// 测试页面获取
	page, err := bp.GetPage(1, 1)
	if err != nil {
		t.Errorf("GetPage failed: %v", err)
	}

	if page == nil {
		t.Error("Retrieved page is nil")
	}

	// 测试命中率
	hitRatio := bp.GetHitRatio()
	if hitRatio < 0 || hitRatio > 1 {
		t.Errorf("Hit ratio should be between 0 and 1, got %f", hitRatio)
	}
}
