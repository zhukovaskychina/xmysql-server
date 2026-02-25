package manager

import (
	"testing"
	"time"
)

// TestRedoPageSplit 测试页面分裂的 Redo 操作
func TestRedoPageSplit(t *testing.T) {
	// 创建测试环境
	cr := createTestCrashRecovery(t)
	defer cleanupTestCrashRecovery(cr)

	// 创建页面分裂日志
	entry := &RedoLogEntry{
		LSN:       1000,
		TrxID:     1,
		PageID:    100,
		Type:      LOG_TYPE_PAGE_SPLIT,
		Data:      []byte("split_data"),
		Timestamp: time.Now(),
	}

	// 执行 Redo
	err := cr.redoPageSplit(entry)
	if err != nil {
		t.Fatalf("Redo 页面分裂失败: %v", err)
	}

	t.Log("✅ 页面分裂 Redo 测试通过")
}

// TestRedoPageMerge 测试页面合并的 Redo 操作
func TestRedoPageMerge(t *testing.T) {
	cr := createTestCrashRecovery(t)
	defer cleanupTestCrashRecovery(cr)

	entry := &RedoLogEntry{
		LSN:       2000,
		TrxID:     2,
		PageID:    200,
		Type:      LOG_TYPE_PAGE_MERGE,
		Data:      []byte("merge_data"),
		Timestamp: time.Now(),
	}

	err := cr.redoPageMerge(entry)
	if err != nil {
		t.Fatalf("Redo 页面合并失败: %v", err)
	}

	t.Log("✅ 页面合并 Redo 测试通过")
}

// TestRedoIndexInsert 测试索引插入的 Redo 操作
func TestRedoIndexInsert(t *testing.T) {
	cr := createTestCrashRecovery(t)
	defer cleanupTestCrashRecovery(cr)

	entry := &RedoLogEntry{
		LSN:       3000,
		TrxID:     3,
		PageID:    300,
		Type:      LOG_TYPE_INDEX_INSERT,
		Data:      []byte("index_insert_data"),
		Timestamp: time.Now(),
	}

	err := cr.redoIndexInsert(entry)
	if err != nil {
		t.Fatalf("Redo 索引插入失败: %v", err)
	}

	t.Log("✅ 索引插入 Redo 测试通过")
}

// TestRedoIndexDelete 测试索引删除的 Redo 操作
func TestRedoIndexDelete(t *testing.T) {
	cr := createTestCrashRecovery(t)
	defer cleanupTestCrashRecovery(cr)

	entry := &RedoLogEntry{
		LSN:       4000,
		TrxID:     4,
		PageID:    400,
		Type:      LOG_TYPE_INDEX_DELETE,
		Data:      []byte("index_delete_data"),
		Timestamp: time.Now(),
	}

	err := cr.redoIndexDelete(entry)
	if err != nil {
		t.Fatalf("Redo 索引删除失败: %v", err)
	}

	t.Log("✅ 索引删除 Redo 测试通过")
}

// TestRedoFileExtend 测试文件扩展的 Redo 操作
func TestRedoFileExtend(t *testing.T) {
	cr := createTestCrashRecovery(t)
	defer cleanupTestCrashRecovery(cr)

	entry := &RedoLogEntry{
		LSN:       5000,
		TrxID:     5,
		PageID:    500,
		Type:      LOG_TYPE_FILE_EXTEND,
		Data:      []byte("extend_data"),
		Timestamp: time.Now(),
	}

	err := cr.redoFileExtend(entry)
	if err != nil {
		t.Fatalf("Redo 文件扩展失败: %v", err)
	}

	t.Log("✅ 文件扩展 Redo 测试通过")
}

// TestRedoIdempotency 测试 Redo 操作的幂等性
func TestRedoIdempotency(t *testing.T) {
	cr := createTestCrashRecovery(t)
	defer cleanupTestCrashRecovery(cr)

	entry := &RedoLogEntry{
		LSN:       6000,
		TrxID:     6,
		PageID:    600,
		Type:      LOG_TYPE_INSERT,
		Data:      []byte("test_data"),
		Timestamp: time.Now(),
	}

	// 第一次执行
	err := cr.redoInsert(entry)
	if err != nil {
		t.Fatalf("第一次 Redo 失败: %v", err)
	}

	// 第二次执行（应该被跳过）
	err = cr.redoInsert(entry)
	if err != nil {
		t.Fatalf("第二次 Redo 失败: %v", err)
	}

	t.Log("✅ Redo 幂等性测试通过")
}

// TestAllLogTypes 测试所有日志类型的处理
func TestAllLogTypes(t *testing.T) {
	cr := createTestCrashRecovery(t)
	defer cleanupTestCrashRecovery(cr)

	logTypes := []struct {
		name    string
		logType uint8
	}{
		{"INSERT", LOG_TYPE_INSERT},
		{"UPDATE", LOG_TYPE_UPDATE},
		{"DELETE", LOG_TYPE_DELETE},
		{"PAGE_CREATE", LOG_TYPE_PAGE_CREATE},
		{"PAGE_DELETE", LOG_TYPE_PAGE_DELETE},
		{"PAGE_MODIFY", LOG_TYPE_PAGE_MODIFY},
		{"PAGE_SPLIT", LOG_TYPE_PAGE_SPLIT},
		{"PAGE_MERGE", LOG_TYPE_PAGE_MERGE},
		{"INDEX_INSERT", LOG_TYPE_INDEX_INSERT},
		{"INDEX_DELETE", LOG_TYPE_INDEX_DELETE},
		{"INDEX_UPDATE", LOG_TYPE_INDEX_UPDATE},
		{"FILE_EXTEND", LOG_TYPE_FILE_EXTEND},
	}

	for i, lt := range logTypes {
		entry := &RedoLogEntry{
			LSN:       uint64(7000 + i),
			TrxID:     int64(7 + i),
			PageID:    uint64(700 + i),
			Type:      lt.logType,
			Data:      []byte("test_data"),
			Timestamp: time.Now(),
		}

		err := cr.redoLogEntry(entry)
		if err != nil {
			t.Errorf("处理日志类型 %s 失败: %v", lt.name, err)
		}
	}

	t.Log("✅ 所有日志类型处理测试通过")
}

// createTestCrashRecovery 创建测试用的崩溃恢复管理器
func createTestCrashRecovery(t *testing.T) *CrashRecovery {
	tmpDir := t.TempDir()

	redoMgr, err := NewRedoLogManager(tmpDir, 1000)
	if err != nil {
		t.Fatalf("创建 Redo 日志管理器失败: %v", err)
	}

	undoMgr, err := NewUndoLogManager(tmpDir)
	if err != nil {
		t.Fatalf("创建 Undo 日志管理器失败: %v", err)
	}

	bufferPool := NewMockBufferPool()
	cr := NewCrashRecovery(redoMgr, undoMgr, 0)
	cr.bufferPoolManager = bufferPool

	return cr
}

// cleanupTestCrashRecovery 清理测试环境
func cleanupTestCrashRecovery(cr *CrashRecovery) {
	if cr.redoLogManager != nil {
		cr.redoLogManager.Close()
	}
	if cr.undoLogManager != nil {
		cr.undoLogManager.Close()
	}
}
