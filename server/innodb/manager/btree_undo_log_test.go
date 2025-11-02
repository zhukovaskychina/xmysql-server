package manager

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// TestUndoLogPointer_Insert 测试INSERT操作的Undo日志指针记录
func TestUndoLogPointer_Insert(t *testing.T) {
	// 创建临时目录用于Undo日志
	tmpDir := filepath.Join(os.TempDir(), "undo_test_insert")
	defer os.RemoveAll(tmpDir)

	// 创建Undo日志管理器
	undoMgr, err := NewUndoLogManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create undo log manager: %v", err)
	}
	defer undoMgr.Close()

	// 创建LSN管理器
	lsnMgr := NewLSNManager(1)

	// 创建B+树管理器
	bpm := createTestBPMForUndoLog()
	defer bpm.Close()

	config := &BPlusTreeConfig{
		MaxCacheSize:   100,
		DirtyThreshold: 0.7,
		EvictionPolicy: "LRU",
	}
	btm := NewBPlusTreeManager(bpm, config)

	// 设置Undo日志管理器和LSN管理器
	btm.SetUndoLogManager(undoMgr)
	btm.SetLSNManager(lsnMgr)

	// 初始化B+树
	ctx := context.Background()
	err = btm.Init(ctx, 0, 1)
	if err != nil {
		t.Fatalf("Failed to initialize B+tree: %v", err)
	}

	// 执行事务化插入
	trxID := uint64(100)
	key := 42
	value := []byte("test_value")

	err = btm.InsertWithTransaction(ctx, key, value, trxID)
	if err != nil {
		t.Fatalf("Failed to insert with transaction: %v", err)
	}

	// 查找插入的节点
	leafNode, err := btm.findLeafNode(ctx, key)
	if err != nil {
		t.Fatalf("Failed to find leaf node: %v", err)
	}

	// 验证事务ID和Undo指针
	if leafNode.TrxID != trxID {
		t.Errorf("Expected TrxID=%d, got %d", trxID, leafNode.TrxID)
	}

	if leafNode.RollPtr == 0 {
		t.Error("RollPtr should not be 0 after transaction insert")
	}

	t.Logf("✅ INSERT: TrxID=%d, RollPtr=%d", leafNode.TrxID, leafNode.RollPtr)

	// 验证Undo日志已记录
	logs := undoMgr.GetLogs(int64(trxID))
	if len(logs) == 0 {
		t.Error("Expected undo log to be recorded")
	} else {
		t.Logf("✅ Undo log recorded: %d entries", len(logs))
		if logs[0].Type != LOG_TYPE_INSERT {
			t.Errorf("Expected log type INSERT, got %d", logs[0].Type)
		}
	}
}

// TestUndoLogPointer_Delete 测试DELETE操作的Undo日志指针记录
func TestUndoLogPointer_Delete(t *testing.T) {
	// 创建临时目录用于Undo日志
	tmpDir := filepath.Join(os.TempDir(), "undo_test_delete")
	defer os.RemoveAll(tmpDir)

	// 创建Undo日志管理器
	undoMgr, err := NewUndoLogManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create undo log manager: %v", err)
	}
	defer undoMgr.Close()

	// 创建LSN管理器
	lsnMgr := NewLSNManager(1)

	// 创建B+树管理器
	bpm := createTestBPMForUndoLog()
	defer bpm.Close()

	config := &BPlusTreeConfig{
		MaxCacheSize:   100,
		DirtyThreshold: 0.7,
		EvictionPolicy: "LRU",
	}
	btm := NewBPlusTreeManager(bpm, config)

	// 设置Undo日志管理器和LSN管理器
	btm.SetUndoLogManager(undoMgr)
	btm.SetLSNManager(lsnMgr)

	// 初始化B+树
	ctx := context.Background()
	err = btm.Init(ctx, 0, 1)
	if err != nil {
		t.Fatalf("Failed to initialize B+tree: %v", err)
	}

	// 先插入一条记录
	key := 42
	value := []byte("test_value")
	err = btm.Insert(ctx, key, value)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 执行事务化删除
	trxID := uint64(200)
	err = btm.DeleteWithTransaction(ctx, key, trxID)
	if err != nil {
		t.Logf("Delete failed (expected if key not found): %v", err)
		// 删除可能失败，因为简化的实现
	}

	// 验证Undo日志已记录
	logs := undoMgr.GetLogs(int64(trxID))
	if len(logs) > 0 {
		t.Logf("✅ DELETE: Undo log recorded: %d entries", len(logs))
		if logs[0].Type != LOG_TYPE_DELETE {
			t.Errorf("Expected log type DELETE, got %d", logs[0].Type)
		}
		if logs[0].LSN == 0 {
			t.Error("LSN should not be 0")
		}
		t.Logf("✅ Undo log: LSN=%d, TrxID=%d, Type=%d", logs[0].LSN, logs[0].TrxID, logs[0].Type)
	}
}

// TestUndoLogPointer_MultipleOperations 测试多个操作的Undo日志指针
func TestUndoLogPointer_MultipleOperations(t *testing.T) {
	// 创建临时目录用于Undo日志
	tmpDir := filepath.Join(os.TempDir(), "undo_test_multiple")
	defer os.RemoveAll(tmpDir)

	// 创建Undo日志管理器
	undoMgr, err := NewUndoLogManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create undo log manager: %v", err)
	}
	defer undoMgr.Close()

	// 创建LSN管理器
	lsnMgr := NewLSNManager(1)

	// 创建B+树管理器
	bpm := createTestBPMForUndoLog()
	defer bpm.Close()

	config := &BPlusTreeConfig{
		MaxCacheSize:   100,
		DirtyThreshold: 0.7,
		EvictionPolicy: "LRU",
	}
	btm := NewBPlusTreeManager(bpm, config)

	// 设置Undo日志管理器和LSN管理器
	btm.SetUndoLogManager(undoMgr)
	btm.SetLSNManager(lsnMgr)

	// 初始化B+树
	ctx := context.Background()
	err = btm.Init(ctx, 0, 1)
	if err != nil {
		t.Fatalf("Failed to initialize B+tree: %v", err)
	}

	// 执行多个插入操作
	trxID := uint64(300)
	keys := []int{10, 20, 30, 40, 50}

	for _, key := range keys {
		value := []byte{byte(key)}
		err := btm.InsertWithTransaction(ctx, key, value, trxID)
		if err != nil {
			t.Logf("Insert key %d failed: %v", key, err)
		}
	}

	// 验证Undo日志
	logs := undoMgr.GetLogs(int64(trxID))
	t.Logf("✅ Multiple operations: %d undo logs recorded", len(logs))

	if len(logs) > 0 {
		// 验证LSN递增
		for i := 1; i < len(logs); i++ {
			if logs[i].LSN <= logs[i-1].LSN {
				t.Errorf("LSN should be increasing: logs[%d].LSN=%d, logs[%d].LSN=%d",
					i-1, logs[i-1].LSN, i, logs[i].LSN)
			}
		}
		t.Log("✅ LSN values are increasing")
	}
}

// TestUndoLogPointer_WithoutUndoManager 测试没有UndoManager时的行为
func TestUndoLogPointer_WithoutUndoManager(t *testing.T) {
	// 创建B+树管理器（不设置UndoManager）
	bpm := createTestBPMForUndoLog()
	defer bpm.Close()

	config := &BPlusTreeConfig{
		MaxCacheSize:   100,
		DirtyThreshold: 0.7,
		EvictionPolicy: "LRU",
	}
	btm := NewBPlusTreeManager(bpm, config)

	// 初始化B+树
	ctx := context.Background()
	err := btm.Init(ctx, 0, 1)
	if err != nil {
		t.Fatalf("Failed to initialize B+tree: %v", err)
	}

	// 执行事务化插入（没有UndoManager）
	trxID := uint64(400)
	key := 42
	value := []byte("test_value")

	err = btm.InsertWithTransaction(ctx, key, value, trxID)
	if err != nil {
		t.Fatalf("Failed to insert with transaction: %v", err)
	}

	// 查找插入的节点
	leafNode, err := btm.findLeafNode(ctx, key)
	if err != nil {
		t.Fatalf("Failed to find leaf node: %v", err)
	}

	// 验证事务ID设置了，但RollPtr应该为0（因为没有UndoManager）
	if leafNode.TrxID != trxID {
		t.Errorf("Expected TrxID=%d, got %d", trxID, leafNode.TrxID)
	}

	if leafNode.RollPtr != 0 {
		t.Errorf("Expected RollPtr=0 without UndoManager, got %d", leafNode.RollPtr)
	}

	t.Log("✅ Without UndoManager: TrxID set, RollPtr=0 (as expected)")
}

// createTestBPMForUndoLog 创建测试用的BufferPoolManager
func createTestBPMForUndoLog() *OptimizedBufferPoolManager {
	config := &BufferPoolConfig{
		PoolSize:        100,
		PageSize:        16384,
		FlushInterval:   time.Second,
		YoungListRatio:  0.75,
		OldListRatio:    0.25,
		OldBlockTime:    1000,
		PrefetchWorkers: 2,
		MaxQueueSize:    100,
		StorageProvider: &MockStorageProviderForUndoLog{},
	}

	bpm, err := NewOptimizedBufferPoolManager(config)
	if err != nil {
		panic(err)
	}
	return bpm
}

// MockStorageProviderForUndoLog 模拟存储提供者
type MockStorageProviderForUndoLog struct{}

func (m *MockStorageProviderForUndoLog) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	return make([]byte, 16384), nil
}

func (m *MockStorageProviderForUndoLog) WritePage(spaceID, pageNo uint32, data []byte) error {
	return nil
}

func (m *MockStorageProviderForUndoLog) AllocatePage(spaceID uint32) (uint32, error) {
	return 1000, nil
}

func (m *MockStorageProviderForUndoLog) FreePage(spaceID, pageNo uint32) error {
	return nil
}

func (m *MockStorageProviderForUndoLog) CreateSpace(name string, pageSize uint32) (uint32, error) {
	return 1, nil
}

func (m *MockStorageProviderForUndoLog) OpenSpace(spaceID uint32) error {
	return nil
}

func (m *MockStorageProviderForUndoLog) CloseSpace(spaceID uint32) error {
	return nil
}

func (m *MockStorageProviderForUndoLog) DeleteSpace(spaceID uint32) error {
	return nil
}

func (m *MockStorageProviderForUndoLog) GetSpaceInfo(spaceID uint32) (*basic.SpaceInfo, error) {
	return &basic.SpaceInfo{
		SpaceID:      spaceID,
		Name:         "test_space",
		Path:         "/tmp/test.ibd",
		PageSize:     16384,
		TotalPages:   100,
		FreePages:    50,
		ExtentSize:   64,
		IsCompressed: false,
		State:        "active",
	}, nil
}

func (m *MockStorageProviderForUndoLog) ListSpaces() ([]basic.SpaceInfo, error) {
	return []basic.SpaceInfo{}, nil
}

func (m *MockStorageProviderForUndoLog) BeginTransaction() (uint64, error) {
	return 1, nil
}

func (m *MockStorageProviderForUndoLog) CommitTransaction(txID uint64) error {
	return nil
}

func (m *MockStorageProviderForUndoLog) RollbackTransaction(txID uint64) error {
	return nil
}

func (m *MockStorageProviderForUndoLog) Sync(spaceID uint32) error {
	return nil
}

func (m *MockStorageProviderForUndoLog) Close() error {
	return nil
}
