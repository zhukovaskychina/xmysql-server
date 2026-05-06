package manager

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestRedoReplayIdempotency 测试Redo重放的幂等性
func TestRedoReplayIdempotency(t *testing.T) {
	logDir := t.TempDir()

	redoLogManager, err := NewRedoLogManager(logDir, 100)
	assert.NoError(t, err)
	defer redoLogManager.Close()

	recovery := NewCrashRecovery(redoLogManager, nil, 0)

	// 创建模拟缓冲池
	mockBP := NewMockBufferPool()
	recovery.SetBufferPoolManager(mockBP)

	// 测试数据
	testData := []byte("test data for idempotency")
	entry := &RedoLogEntry{
		LSN:    100,
		TrxID:  1,
		PageID: 1,
		Type:   LOG_TYPE_INSERT,
		Data:   testData,
	}

	// 第一次重放
	err = recovery.redoInsert(entry)
	assert.NoError(t, err)

	// 验证页面LSN
	page, err := mockBP.FetchPage(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), page.GetLSN())

	// 第二次重放相同的日志（应该被跳过）
	err = recovery.redoInsert(entry)
	assert.NoError(t, err)

	// LSN应该保持不变
	page, err = mockBP.FetchPage(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), page.GetLSN())

	t.Log("✓ Redo重放幂等性测试通过")
}

// TestRedoReplayWithStorage 测试使用存储管理器的Redo重放
func TestRedoReplayWithStorage(t *testing.T) {
	logDir := t.TempDir()

	redoLogManager, err := NewRedoLogManager(logDir, 100)
	assert.NoError(t, err)
	defer redoLogManager.Close()

	recovery := NewCrashRecovery(redoLogManager, nil, 0)

	// 创建模拟存储
	mockStorage := NewMockStorage()
	recovery.SetStorageManager(mockStorage)

	// 准备初始页面数据
	initialData := make([]byte, 16384)
	binary.BigEndian.PutUint64(initialData[0:8], 0) // 初始LSN为0
	copy(initialData[8:], []byte("initial data"))
	mockStorage.WritePage(10, initialData)

	// 创建更新日志
	updatedData := make([]byte, 16384)
	binary.BigEndian.PutUint64(updatedData[0:8], 200) // 新LSN
	copy(updatedData[8:], []byte("updated data"))

	entry := &RedoLogEntry{
		LSN:    200,
		TrxID:  2,
		PageID: 10,
		Type:   LOG_TYPE_UPDATE,
		Data:   updatedData,
	}

	// 执行重放
	err = recovery.redoWithStorage(entry)
	assert.NoError(t, err)

	// 验证页面已更新
	pageData, err := mockStorage.ReadPage(10)
	assert.NoError(t, err)
	pageLSN := binary.BigEndian.Uint64(pageData[0:8])
	assert.Equal(t, uint64(200), pageLSN)

	t.Log("✓ 使用存储管理器的Redo重放测试通过")
}

// TestRedoReplayPageOperations 测试页面操作的Redo重放
func TestRedoReplayPageOperations(t *testing.T) {
	logDir := t.TempDir()

	redoLogManager, err := NewRedoLogManager(logDir, 100)
	assert.NoError(t, err)
	defer redoLogManager.Close()

	recovery := NewCrashRecovery(redoLogManager, nil, 0)

	mockStorage := NewMockStorage()
	recovery.SetStorageManager(mockStorage)

	t.Run("页面创建", func(t *testing.T) {
		entry := &RedoLogEntry{
			LSN:    300,
			TrxID:  3,
			PageID: 1,
			Type:   LOG_TYPE_PAGE_CREATE,
			Data:   []byte("new page data"),
		}

		err := recovery.redoPageCreate(entry)
		assert.NoError(t, err)

		// 验证页面已创建
		data, err := mockStorage.ReadPage(1)
		assert.NoError(t, err)
		assert.NotNil(t, data)

		t.Log("✓ 页面创建重放测试通过")
	})

	t.Run("页面删除", func(t *testing.T) {
		// 先创建一个页面
		err := mockStorage.CreatePage(2)
		assert.NoError(t, err)

		entry := &RedoLogEntry{
			LSN:    400,
			TrxID:  4,
			PageID: 2,
			Type:   LOG_TYPE_PAGE_DELETE,
		}

		err = recovery.redoPageDelete(entry)
		assert.NoError(t, err)

		t.Log("✓ 页面删除重放测试通过")
	})
}

// TestRedoReplayLSNCheck 测试LSN检查逻辑
func TestRedoReplayLSNCheck(t *testing.T) {
	logDir := t.TempDir()

	redoLogManager, err := NewRedoLogManager(logDir, 100)
	assert.NoError(t, err)
	defer redoLogManager.Close()

	recovery := NewCrashRecovery(redoLogManager, nil, 0)

	mockBP := NewMockBufferPool()
	recovery.SetBufferPoolManager(mockBP)

	// 创建一个已经有较高LSN的页面
	page, _ := mockBP.FetchPage(1)
	page.SetLSN(500)
	mockBP.UnpinPage(1, true)

	// 尝试重放一个较低LSN的日志（应该被跳过）
	entry := &RedoLogEntry{
		LSN:    300,
		TrxID:  1,
		PageID: 1,
		Type:   LOG_TYPE_INSERT,
		Data:   []byte("old data"),
	}

	err = recovery.redoInsert(entry)
	assert.NoError(t, err)

	// 验证LSN没有被降低
	page, _ = mockBP.FetchPage(1)
	assert.Equal(t, uint64(500), page.GetLSN())

	t.Log("✓ LSN检查逻辑测试通过")
}

// TestRedoReplayMultipleOperations 测试多个操作的重放
func TestRedoReplayMultipleOperations(t *testing.T) {
	logDir := t.TempDir()

	redoLogManager, err := NewRedoLogManager(logDir, 100)
	assert.NoError(t, err)
	defer redoLogManager.Close()

	recovery := NewCrashRecovery(redoLogManager, nil, 0)

	mockBP := NewMockBufferPool()
	recovery.SetBufferPoolManager(mockBP)

	// 模拟一系列操作
	operations := []struct {
		lsn    uint64
		pageID uint64
		opType uint8
		data   string
	}{
		{100, 1, LOG_TYPE_INSERT, "insert data 1"},
		{200, 1, LOG_TYPE_UPDATE, "update data 1"},
		{300, 2, LOG_TYPE_INSERT, "insert data 2"},
		{400, 1, LOG_TYPE_UPDATE, "update data 1 again"},
		{500, 2, LOG_TYPE_DELETE, "delete data 2"},
	}

	for _, op := range operations {
		entry := &RedoLogEntry{
			LSN:    op.lsn,
			TrxID:  1,
			PageID: op.pageID,
			Type:   op.opType,
			Data:   []byte(op.data),
		}

		var err error
		switch op.opType {
		case LOG_TYPE_INSERT:
			err = recovery.redoInsert(entry)
		case LOG_TYPE_UPDATE:
			err = recovery.redoUpdate(entry)
		case LOG_TYPE_DELETE:
			err = recovery.redoDelete(entry)
		}

		assert.NoError(t, err)
	}

	// 验证最终状态
	page1, _ := mockBP.FetchPage(1)
	assert.Equal(t, uint64(400), page1.GetLSN())

	page2, _ := mockBP.FetchPage(2)
	assert.Equal(t, uint64(500), page2.GetLSN())

	t.Log("✓ 多个操作重放测试通过")
}
