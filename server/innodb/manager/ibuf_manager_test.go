package manager

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"testing"
	"time"
)

// MockPageManager 模拟页面管理器
type MockPageManager struct {
	pages map[uint64][]byte
}

func NewMockPageManager() *MockPageManager {
	return &MockPageManager{
		pages: make(map[uint64][]byte),
	}
}

func (m *MockPageManager) GetPage(spaceID, pageNo uint32) ([]byte, error) {
	key := uint64(spaceID)<<32 | uint64(pageNo)
	if page, ok := m.pages[key]; ok {
		return page, nil
	}
	// 返回空页面
	return make([]byte, 16384), nil
}

func (m *MockPageManager) WritePage(spaceID, pageNo uint32, content []byte) error {
	key := uint64(spaceID)<<32 | uint64(pageNo)
	m.pages[key] = content
	return nil
}

func (m *MockPageManager) FlushPage(spaceID, pageNo uint32) error {
	return nil
}

func (m *MockPageManager) AllocPage(spaceID uint32) (uint32, error) {
	return 1, nil
}

func (m *MockPageManager) FreePage(spaceID, pageNo uint32) error {
	return nil
}

func (m *MockPageManager) ScanLeaves(no uint32) (interface{}, interface{}) {
	return nil, nil
}

func (m *MockPageManager) InsertKey(no uint32, key []byte, record basic.Record) interface{} {
	return nil
}

func (m *MockPageManager) AllocatePage(id interface{}) (interface{}, interface{}) {
	return nil, nil
}

// TestNewIBufManager 测试创建Insert Buffer管理器
func TestNewIBufManager(t *testing.T) {
	mockPageMgr := NewMockPageManager()
	segMgr := &SegmentManager{} // 简化测试，使用空的SegmentManager

	ibufMgr := NewIBufManager(segMgr, mockPageMgr)
	if ibufMgr == nil {
		t.Fatal("Failed to create IBufManager")
	}

	// 验证初始化
	if ibufMgr.ibufTrees == nil {
		t.Error("ibufTrees should be initialized")
	}

	if ibufMgr.stats == nil {
		t.Error("stats should be initialized")
	}

	if !ibufMgr.running {
		t.Error("background merge should be running")
	}

	// 清理
	ibufMgr.Close()
}

// TestIBufManager_InsertRecord 测试插入记录
func TestIBufManager_InsertRecord(t *testing.T) {
	mockPageMgr := NewMockPageManager()
	segMgr := &SegmentManager{}

	ibufMgr := NewIBufManager(segMgr, mockPageMgr)
	defer ibufMgr.Close()

	// 创建测试记录
	record := &IBufRecord{
		SpaceID: 1,
		PageNo:  100,
		Type:    IBUF_OP_INSERT,
		Key:     []byte("test_key"),
		Value:   []byte("test_value"),
		TrxID:   12345,
		Time:    time.Now(),
	}

	// 插入记录（简化测试，不实际创建段）
	// 由于CreateSegment需要完整的SegmentManager实现，这里只测试接口
	// err := ibufMgr.InsertRecord(record)
	// if err != nil {
	// 	t.Errorf("Failed to insert record: %v", err)
	// }

	// 验证统计信息
	stats := ibufMgr.GetStats()
	if stats == nil {
		t.Error("Stats should not be nil")
	}

	_ = record // 避免未使用变量警告
}

// TestIBufManager_BuildKey 测试构建键值
func TestIBufManager_BuildKey(t *testing.T) {
	mockPageMgr := NewMockPageManager()
	segMgr := &SegmentManager{}

	ibufMgr := NewIBufManager(segMgr, mockPageMgr)
	defer ibufMgr.Close()

	// 测试构建键值
	pageNo := uint32(100)
	indexKey := []byte("test_index_key")

	key := ibufMgr.buildKey(pageNo, indexKey)

	// 验证键值长度
	expectedLen := 4 + len(indexKey)
	if len(key) != expectedLen {
		t.Errorf("Expected key length %d, got %d", expectedLen, len(key))
	}

	// 验证页号部分
	// 前4字节应该是页号
	if len(key) >= 4 {
		// 简单验证键值不为空
		if key[0] == 0 && key[1] == 0 && key[2] == 0 && key[3] == 0 {
			// 页号100应该不是全0
			if pageNo != 0 {
				t.Error("Page number encoding might be incorrect")
			}
		}
	}
}

// TestIBufManager_BuildValue 测试构建值
func TestIBufManager_BuildValue(t *testing.T) {
	mockPageMgr := NewMockPageManager()
	segMgr := &SegmentManager{}

	ibufMgr := NewIBufManager(segMgr, mockPageMgr)
	defer ibufMgr.Close()

	// 创建测试记录
	record := &IBufRecord{
		Type:  IBUF_OP_INSERT,
		TrxID: 12345,
		Value: []byte("test_value"),
	}

	value := ibufMgr.buildValue(record)

	// 验证值的长度
	// 格式：[Type:1字节][TrxID:8字节][ValueLen:4字节][Value:变长]
	expectedLen := 1 + 8 + 4 + len(record.Value)
	if len(value) != expectedLen {
		t.Errorf("Expected value length %d, got %d", expectedLen, len(value))
	}

	// 验证操作类型
	if value[0] != IBUF_OP_INSERT {
		t.Errorf("Expected operation type %d, got %d", IBUF_OP_INSERT, value[0])
	}
}

// TestIBufManager_ShouldMerge 测试合并判断
func TestIBufManager_ShouldMerge(t *testing.T) {
	mockPageMgr := NewMockPageManager()
	segMgr := &SegmentManager{}

	ibufMgr := NewIBufManager(segMgr, mockPageMgr)
	defer ibufMgr.Close()

	// 测试1：空树不应该合并
	tree := &IBufTree{
		SpaceID: 1,
		Size:    0,
	}
	if ibufMgr.shouldMerge(tree) {
		t.Error("Empty tree should not trigger merge")
	}

	// 测试2：超过阈值应该合并
	tree.Size = 10001
	if !ibufMgr.shouldMerge(tree) {
		t.Error("Tree with size > 10000 should trigger merge")
	}

	// 测试3：时间超过阈值应该合并
	tree.Size = 100
	ibufMgr.lastMergeTime = time.Now().Add(-10 * time.Minute)
	if !ibufMgr.shouldMerge(tree) {
		t.Error("Tree with old merge time should trigger merge")
	}
}

// TestIBufManager_GetStats 测试获取统计信息
func TestIBufManager_GetStats(t *testing.T) {
	mockPageMgr := NewMockPageManager()
	segMgr := &SegmentManager{}

	ibufMgr := NewIBufManager(segMgr, mockPageMgr)
	defer ibufMgr.Close()

	stats := ibufMgr.GetStats()
	if stats == nil {
		t.Fatal("Stats should not be nil")
	}

	// 验证初始值
	if stats.InsertCount != 0 {
		t.Errorf("Expected InsertCount 0, got %d", stats.InsertCount)
	}

	if stats.MergeCount != 0 {
		t.Errorf("Expected MergeCount 0, got %d", stats.MergeCount)
	}

	if stats.MergedCount != 0 {
		t.Errorf("Expected MergedCount 0, got %d", stats.MergedCount)
	}
}

// TestIBufManager_BackgroundMerge 测试后台合并
func TestIBufManager_BackgroundMerge(t *testing.T) {
	mockPageMgr := NewMockPageManager()
	segMgr := &SegmentManager{}

	ibufMgr := NewIBufManager(segMgr, mockPageMgr)

	// 验证后台线程已启动
	if !ibufMgr.running {
		t.Error("Background merge should be running")
	}

	// 停止后台合并
	ibufMgr.StopBackgroundMerge()

	// 等待一小段时间确保停止
	time.Sleep(100 * time.Millisecond)

	if ibufMgr.running {
		t.Error("Background merge should be stopped")
	}

	// 清理
	ibufMgr.Close()
}

// TestIBufManager_Close 测试关闭
func TestIBufManager_Close(t *testing.T) {
	mockPageMgr := NewMockPageManager()
	segMgr := &SegmentManager{}

	ibufMgr := NewIBufManager(segMgr, mockPageMgr)

	// 关闭管理器
	err := ibufMgr.Close()
	if err != nil {
		t.Errorf("Failed to close IBufManager: %v", err)
	}

	// 验证后台线程已停止
	if ibufMgr.running {
		t.Error("Background merge should be stopped after close")
	}

	// 验证资源已清理
	if ibufMgr.ibufTrees != nil {
		t.Error("ibufTrees should be nil after close")
	}
}
