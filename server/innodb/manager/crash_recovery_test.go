package manager

import (
	"os"
	"testing"
)

// TestCrashRecoveryAnalysisPhase 测试分析阶段
func TestCrashRecoveryAnalysisPhase(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "crash_recovery_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建Redo日志管理器
	redoLogManager, err := NewRedoLogManager(tmpDir, 100)
	if err != nil {
		t.Fatalf("创建Redo日志管理器失败: %v", err)
	}
	defer redoLogManager.Close()

	// 创建Undo日志管理器
	undoLogManager, err := NewUndoLogManager(tmpDir)
	if err != nil {
		t.Fatalf("创建Undo日志管理器失败: %v", err)
	}
	defer undoLogManager.Close()

	// 写入一些测试日志
	// 事务1：开始 -> 插入 -> 提交
	redoLogManager.Append(&RedoLogEntry{
		TrxID:  1,
		PageID: 100,
		Type:   LOG_TYPE_TXN_BEGIN,
		Data:   []byte("begin"),
	})
	redoLogManager.Append(&RedoLogEntry{
		TrxID:  1,
		PageID: 100,
		Type:   LOG_TYPE_INSERT,
		Data:   []byte("insert data"),
	})
	redoLogManager.Append(&RedoLogEntry{
		TrxID:  1,
		PageID: 100,
		Type:   LOG_TYPE_TXN_COMMIT,
		Data:   []byte("commit"),
	})

	// 事务2：开始 -> 插入（未提交）
	redoLogManager.Append(&RedoLogEntry{
		TrxID:  2,
		PageID: 101,
		Type:   LOG_TYPE_TXN_BEGIN,
		Data:   []byte("begin"),
	})
	redoLogManager.Append(&RedoLogEntry{
		TrxID:  2,
		PageID: 101,
		Type:   LOG_TYPE_INSERT,
		Data:   []byte("insert data 2"),
	})

	// 刷新日志
	redoLogManager.Flush(0)

	// 创建崩溃恢复管理器
	crashRecovery := NewCrashRecovery(redoLogManager, undoLogManager, 0)

	// 执行分析阶段
	err = crashRecovery.analysisPhase()
	if err != nil {
		t.Fatalf("分析阶段失败: %v", err)
	}

	// 验证结果
	if !crashRecovery.analysisComplete {
		t.Error("分析阶段未标记为完成")
	}

	// 应该有1个活跃事务（事务2）
	if len(crashRecovery.activeTransactions) != 1 {
		t.Errorf("活跃事务数量错误: 期望1, 实际%d", len(crashRecovery.activeTransactions))
	}

	// 应该有1个需要回滚的事务
	if len(crashRecovery.undoTransactions) != 1 {
		t.Errorf("需要回滚的事务数量错误: 期望1, 实际%d", len(crashRecovery.undoTransactions))
	}

	// 应该有2个脏页
	if len(crashRecovery.dirtyPages) != 2 {
		t.Errorf("脏页数量错误: 期望2, 实际%d", len(crashRecovery.dirtyPages))
	}
}

// TestCrashRecoveryRedoPhase 测试Redo阶段
func TestCrashRecoveryRedoPhase(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "crash_recovery_redo_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建Redo日志管理器
	redoLogManager, err := NewRedoLogManager(tmpDir, 100)
	if err != nil {
		t.Fatalf("创建Redo日志管理器失败: %v", err)
	}
	defer redoLogManager.Close()

	// 创建Undo日志管理器
	undoLogManager, err := NewUndoLogManager(tmpDir)
	if err != nil {
		t.Fatalf("创建Undo日志管理器失败: %v", err)
	}
	defer undoLogManager.Close()

	// 写入测试日志
	redoLogManager.Append(&RedoLogEntry{
		TrxID:  1,
		PageID: 100,
		Type:   LOG_TYPE_INSERT,
		Data:   []byte("test data"),
	})
	redoLogManager.Flush(0)

	// 创建崩溃恢复管理器
	crashRecovery := NewCrashRecovery(redoLogManager, undoLogManager, 0)

	// 先执行分析阶段
	err = crashRecovery.analysisPhase()
	if err != nil {
		t.Fatalf("分析阶段失败: %v", err)
	}

	// 执行Redo阶段
	err = crashRecovery.redoPhase()
	if err != nil {
		t.Fatalf("Redo阶段失败: %v", err)
	}

	// 验证结果
	if !crashRecovery.redoComplete {
		t.Error("Redo阶段未标记为完成")
	}
}

// TestCrashRecoveryUndoPhase 测试Undo阶段
func TestCrashRecoveryUndoPhase(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "crash_recovery_undo_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建Redo日志管理器
	redoLogManager, err := NewRedoLogManager(tmpDir, 100)
	if err != nil {
		t.Fatalf("创建Redo日志管理器失败: %v", err)
	}
	defer redoLogManager.Close()

	// 创建Undo日志管理器
	undoLogManager, err := NewUndoLogManager(tmpDir)
	if err != nil {
		t.Fatalf("创建Undo日志管理器失败: %v", err)
	}
	defer undoLogManager.Close()

	// 创建模拟的回滚执行器
	mockExecutor := &MockRollbackExecutor{
		insertedRecords: make(map[uint64][]byte),
		updatedRecords:  make(map[uint64][]byte),
		deletedRecords:  make(map[uint64]bool),
	}
	undoLogManager.SetRollbackExecutor(mockExecutor)

	// 写入Undo日志（模拟未提交的INSERT）
	undoLogManager.Append(&UndoLogEntry{
		LSN:     1,
		TrxID:   1,
		TableID: 100,
		Type:    LOG_TYPE_INSERT,
		Data:    []byte("primary_key_1"),
	})

	// 创建崩溃恢复管理器
	crashRecovery := NewCrashRecovery(redoLogManager, undoLogManager, 0)

	// 设置需要回滚的事务
	crashRecovery.undoTransactions = []int64{1}
	crashRecovery.redoComplete = true

	// 执行Undo阶段
	err = crashRecovery.undoPhase()
	if err != nil {
		t.Fatalf("Undo阶段失败: %v", err)
	}

	// 验证结果
	if !crashRecovery.undoComplete {
		t.Error("Undo阶段未标记为完成")
	}

	// 验证回滚操作被执行
	if len(mockExecutor.deletedRecords) != 1 {
		t.Errorf("回滚操作未执行: 期望删除1条记录, 实际删除%d条", len(mockExecutor.deletedRecords))
	}
}

// TestFullCrashRecovery 测试完整的崩溃恢复流程
func TestFullCrashRecovery(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "full_crash_recovery_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建Redo日志管理器
	redoLogManager, err := NewRedoLogManager(tmpDir, 100)
	if err != nil {
		t.Fatalf("创建Redo日志管理器失败: %v", err)
	}
	defer redoLogManager.Close()

	// 创建Undo日志管理器
	undoLogManager, err := NewUndoLogManager(tmpDir)
	if err != nil {
		t.Fatalf("创建Undo日志管理器失败: %v", err)
	}
	defer undoLogManager.Close()

	// 设置回滚执行器
	mockExecutor := &MockRollbackExecutor{
		insertedRecords: make(map[uint64][]byte),
		updatedRecords:  make(map[uint64][]byte),
		deletedRecords:  make(map[uint64]bool),
	}
	undoLogManager.SetRollbackExecutor(mockExecutor)

	// 创建崩溃恢复管理器
	crashRecovery := NewCrashRecovery(redoLogManager, undoLogManager, 0)

	// 执行完整恢复流程
	err = crashRecovery.Recover()
	if err != nil {
		t.Fatalf("崩溃恢复失败: %v", err)
	}

	// 验证所有阶段都完成
	if !crashRecovery.analysisComplete {
		t.Error("分析阶段未完成")
	}
	if !crashRecovery.redoComplete {
		t.Error("Redo阶段未完成")
	}
	if !crashRecovery.undoComplete {
		t.Error("Undo阶段未完成")
	}
}

// MockRollbackExecutor 模拟回滚执行器
type MockRollbackExecutor struct {
	insertedRecords map[uint64][]byte
	updatedRecords  map[uint64][]byte
	deletedRecords  map[uint64]bool
}

func (m *MockRollbackExecutor) InsertRecord(tableID, recordID uint64, data []byte) error {
	m.insertedRecords[recordID] = data
	return nil
}

func (m *MockRollbackExecutor) UpdateRecord(tableID, recordID uint64, data, columnBitmap []byte) error {
	m.updatedRecords[recordID] = data
	return nil
}

func (m *MockRollbackExecutor) DeleteRecord(tableID, recordID uint64, primaryKeyData []byte) error {
	m.deletedRecords[recordID] = true
	return nil
}
