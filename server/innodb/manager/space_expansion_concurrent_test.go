package manager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// MockSpaceManager 模拟空间管理器
type MockSpaceManager struct {
	sync.RWMutex
	spaces        map[uint32]*MockSpace
	allocateCount uint64
}

// MockSpace 模拟表空间
type MockSpace struct {
	id      uint32
	size    uint64
	extents uint32
	mu      sync.RWMutex
}

// MockExtent 模拟Extent
type MockExtent struct {
	id uint32
}

func (e *MockExtent) GetID() uint32 {
	return e.id
}

func (e *MockExtent) GetState() basic.ExtentState {
	return basic.ExtentStateFree
}

func (e *MockExtent) GetType() basic.ExtentType {
	return basic.ExtentTypeData
}

func (e *MockExtent) GetSpaceID() uint32 {
	return 0
}

func (e *MockExtent) GetSegmentID() uint64 {
	return 0
}

func (e *MockExtent) AllocatePage() (uint32, error) {
	return 0, nil
}

func (e *MockExtent) FreePage(pageNo uint32) error {
	return nil
}

func (e *MockExtent) GetPageCount() uint32 {
	return 64
}

func (e *MockExtent) GetFreePages() []uint32 {
	return nil
}

func (e *MockExtent) GetFreeSpace() uint64 {
	return 0
}

func (e *MockExtent) Defragment() error {
	return nil
}

func (e *MockExtent) GetBitmap() []byte {
	return nil
}

func (e *MockExtent) GetStartPage() uint32 {
	return 0
}

func (e *MockExtent) GetStats() *basic.ExtentStats {
	return &basic.ExtentStats{
		TotalPages: 64,
		FreePages:  64,
	}
}

func (e *MockExtent) IsEmpty() bool {
	return false
}

func (e *MockExtent) IsFull() bool {
	return false
}

func (e *MockExtent) Lock() {
}

func (e *MockExtent) Unlock() {
}

func (e *MockExtent) RLock() {
}

func (e *MockExtent) RUnlock() {
}

func (e *MockExtent) Reset() error {
	return nil
}

func (e *MockExtent) StartPage() int {
	return 0
}

func NewMockSpaceManager() *MockSpaceManager {
	return &MockSpaceManager{
		spaces: make(map[uint32]*MockSpace),
	}
}

func (m *MockSpaceManager) GetSpace(spaceID uint32) (basic.Space, error) {
	m.Lock()
	defer m.Unlock()

	space, ok := m.spaces[spaceID]
	if !ok {
		// 创建新空间
		space = &MockSpace{
			id:      spaceID,
			size:    1024 * 1024, // 1MB
			extents: 1,
		}
		m.spaces[spaceID] = space
	}
	return space, nil
}

func (m *MockSpaceManager) AllocateExtent(spaceID uint32, purpose basic.ExtentPurpose) (basic.Extent, error) {
	atomic.AddUint64(&m.allocateCount, 1)

	m.Lock()
	defer m.Unlock()

	space, ok := m.spaces[spaceID]
	if !ok {
		return nil, fmt.Errorf("space %d not found", spaceID)
	}

	space.mu.Lock()
	defer space.mu.Unlock()

	extentID := space.extents
	space.extents++
	space.size += 64 * 16384 // 64页 * 16KB

	return &MockExtent{id: extentID}, nil
}

func (m *MockSpaceManager) CreateSpace(spaceID uint32, name string, isSystem bool) (basic.Space, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockSpaceManager) DropSpace(spaceID uint32) error {
	return fmt.Errorf("not implemented")
}

func (m *MockSpaceManager) FreeExtent(spaceID, extentID uint32) error {
	return fmt.Errorf("not implemented")
}

func (m *MockSpaceManager) Begin() (basic.Tx, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockSpaceManager) CreateNewTablespace(name string) uint32 {
	return 0
}

func (m *MockSpaceManager) CreateTableSpace(name string) (uint32, error) {
	return 0, fmt.Errorf("not implemented")
}

func (m *MockSpaceManager) GetTableSpace(spaceID uint32) (basic.FileTableSpace, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockSpaceManager) GetTableSpaceByName(name string) (basic.FileTableSpace, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockSpaceManager) GetTableSpaceInfo(spaceID uint32) (*basic.TableSpaceInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockSpaceManager) DropTableSpace(spaceID uint32) error {
	return fmt.Errorf("not implemented")
}

func (m *MockSpaceManager) Close() error {
	return nil
}

// MockSpace 实现 basic.Space 接口
func (m *MockSpace) ID() uint32 {
	return m.id
}

func (m *MockSpace) Name() string {
	return fmt.Sprintf("space_%d", m.id)
}

func (m *MockSpace) IsSystem() bool {
	return false
}

func (m *MockSpace) AllocateExtent(purpose basic.ExtentPurpose) (basic.Extent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	extentID := m.extents
	m.extents++
	m.size += 64 * 16384

	return &MockExtent{id: extentID}, nil
}

func (m *MockSpace) FreeExtent(extentID uint32) error {
	return fmt.Errorf("not implemented")
}

func (m *MockSpace) GetPageCount() uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.extents * 64
}

func (m *MockSpace) GetExtentCount() uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.extents
}

func (m *MockSpace) GetUsedSpace() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *MockSpace) Close() error {
	return nil
}

func (m *MockSpace) Sync() error {
	return nil
}

func (m *MockSpace) IsActive() bool {
	return true
}

func (m *MockSpace) SetActive(active bool) {
}

func (m *MockSpace) LoadPageByPageNumber(no uint32) ([]byte, error) {
	return nil, nil
}

func (m *MockSpace) FlushToDisk(no uint32, content []byte) error {
	return nil
}

// TestConcurrentCheckAndExpand 测试并发检查和扩展
func TestConcurrentCheckAndExpand(t *testing.T) {
	spaceManager := NewMockSpaceManager()
	config := &ExpansionConfig{
		Strategy:         ExpansionStrategyFixed,
		AutoExpand:       true,
		AsyncExpand:      false, // 使用同步扩展便于测试
		LowWaterMark:     10.0,
		HighWaterMark:    30.0,
		FixedExtents:     4,
		MinExtents:       MinExtentsPerExpand,
		MaxExtents:       MaxExtentsPerExpand,
		MaxSpaceSize:     DefaultMaxSpaceSize,
		EnablePrediction: false,
	}

	sem := NewSpaceExpansionManager(spaceManager, config)
	defer sem.Stop()

	// 并发执行多个 CheckAndExpand
	const numGoroutines = 10
	const numIterations = 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numIterations)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				spaceID := uint32(id%3 + 1) // 使用3个不同的空间
				if err := sem.CheckAndExpand(spaceID); err != nil {
					errors <- fmt.Errorf("goroutine %d iteration %d: %v", id, j, err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查错误
	errorCount := 0
	for err := range errors {
		t.Logf("Error: %v", err)
		errorCount++
	}

	if errorCount > 0 {
		t.Errorf("Got %d errors during concurrent CheckAndExpand", errorCount)
	}

	// 验证统计信息
	stats := sem.GetStats()
	t.Logf("Total expansions: %d", stats.TotalExpansions)
	t.Logf("Failed expansions: %d", stats.FailedExpansions)
	t.Logf("Total extents added: %d", stats.TotalExtentsAdded)
}

// TestConcurrentExpandSpace 测试并发手动扩展
func TestConcurrentExpandSpace(t *testing.T) {
	spaceManager := NewMockSpaceManager()
	config := &ExpansionConfig{
		Strategy:     ExpansionStrategyFixed,
		AutoExpand:   true,
		AsyncExpand:  false,
		FixedExtents: 4,
		MinExtents:   MinExtentsPerExpand,
		MaxExtents:   MaxExtentsPerExpand,
		MaxSpaceSize: DefaultMaxSpaceSize,
	}

	sem := NewSpaceExpansionManager(spaceManager, config)
	defer sem.Stop()

	const numGoroutines = 20
	const spaceID = uint32(1)

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := sem.ExpandSpace(spaceID, 2); err != nil {
				errors <- fmt.Errorf("goroutine %d: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查错误
	errorCount := 0
	for err := range errors {
		t.Logf("Error: %v", err)
		errorCount++
	}

	if errorCount > 0 {
		t.Errorf("Got %d errors during concurrent ExpandSpace", errorCount)
	}

	// 验证统计信息
	stats := sem.GetStats()
	t.Logf("Total expansions: %d", stats.TotalExpansions)
	t.Logf("Manual expansions: %d", stats.ManualExpansions)
	t.Logf("Total extents added: %d", stats.TotalExtentsAdded)

	if stats.TotalExpansions != numGoroutines {
		t.Errorf("Expected %d expansions, got %d", numGoroutines, stats.TotalExpansions)
	}
}

// TestConcurrentAsyncExpand 测试并发异步扩展
func TestConcurrentAsyncExpand(t *testing.T) {
	spaceManager := NewMockSpaceManager()
	config := &ExpansionConfig{
		Strategy:     ExpansionStrategyFixed,
		AutoExpand:   true,
		AsyncExpand:  true, // 使用异步扩展
		FixedExtents: 2,
		MinExtents:   MinExtentsPerExpand,
		MaxExtents:   MaxExtentsPerExpand,
		MaxSpaceSize: DefaultMaxSpaceSize,
	}

	sem := NewSpaceExpansionManager(spaceManager, config)
	defer sem.Stop()

	const numGoroutines = 50
	const spaceID = uint32(1)

	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := sem.ExpandSpace(spaceID, 1); err != nil {
				t.Logf("Goroutine %d error: %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	// 等待异步扩展完成
	time.Sleep(2 * time.Second)

	// 验证统计信息
	stats := sem.GetStats()
	t.Logf("Total expansions: %d", stats.TotalExpansions)
	t.Logf("Manual expansions: %d", stats.ManualExpansions)
	t.Logf("Total extents added: %d", stats.TotalExtentsAdded)
	t.Logf("Allocate count: %d", atomic.LoadUint64(&spaceManager.allocateCount))
}

// TestConcurrentGetStats 测试并发获取统计信息
func TestConcurrentGetStats(t *testing.T) {
	spaceManager := NewMockSpaceManager()
	config := &ExpansionConfig{
		Strategy:     ExpansionStrategyFixed,
		AutoExpand:   true,
		AsyncExpand:  false,
		FixedExtents: 2,
		MinExtents:   MinExtentsPerExpand,
		MaxExtents:   MaxExtentsPerExpand,
		MaxSpaceSize: DefaultMaxSpaceSize,
	}

	sem := NewSpaceExpansionManager(spaceManager, config)
	defer sem.Stop()

	const numReaders = 10
	const numWriters = 5

	var wg sync.WaitGroup

	// 启动读取者
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				stats := sem.GetStats()
				_ = stats // 使用stats避免编译警告
			}
		}(i)
	}

	// 启动写入者
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				spaceID := uint32(id + 1)
				_ = sem.ExpandSpace(spaceID, 1)
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	// 验证最终统计
	stats := sem.GetStats()
	t.Logf("Final stats - Total: %d, Manual: %d, Failed: %d",
		stats.TotalExpansions, stats.ManualExpansions, stats.FailedExpansions)
}

// TestRaceConditionDetection 使用 Go race detector 检测竞态条件
// 运行: go test -race -run TestRaceConditionDetection
func TestRaceConditionDetection(t *testing.T) {
	spaceManager := NewMockSpaceManager()
	config := &ExpansionConfig{
		Strategy:         ExpansionStrategyAdaptive,
		AutoExpand:       true,
		AsyncExpand:      true,
		LowWaterMark:     10.0,
		FixedExtents:     2,
		MinExtents:       MinExtentsPerExpand,
		MaxExtents:       MaxExtentsPerExpand,
		MaxSpaceSize:     DefaultMaxSpaceSize,
		EnablePrediction: true,
		PredictionWindow: 60,
	}

	sem := NewSpaceExpansionManager(spaceManager, config)
	defer sem.Stop()

	var wg sync.WaitGroup

	// 混合操作
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			spaceID := uint32(id%3 + 1)

			// CheckAndExpand
			_ = sem.CheckAndExpand(spaceID)

			// ExpandSpace
			_ = sem.ExpandSpace(spaceID, 1)

			// PredictiveExpand
			_ = sem.PredictiveExpand(spaceID)

			// GetStats
			_ = sem.GetStats()

			// GetHistory
			_ = sem.GetHistory()
		}(i)
	}

	wg.Wait()
	time.Sleep(1 * time.Second) // 等待异步操作完成
}
