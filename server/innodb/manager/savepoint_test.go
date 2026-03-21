package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func setSavepointCreatedAt(trx *Transaction, name string, createdAt time.Time) {
	if sp, ok := trx.Savepoints[name]; ok {
		sp.CreatedAt = createdAt
	}
}

// mockRollbackExecutor 模拟回滚执行器
type mockRollbackExecutor struct{}

func (m *mockRollbackExecutor) InsertRecord(tableID uint64, recordID uint64, data []byte) error {
	return nil
}

func (m *mockRollbackExecutor) UpdateRecord(tableID uint64, recordID uint64, data []byte, bitmap []byte) error {
	return nil
}

func (m *mockRollbackExecutor) DeleteRecord(tableID uint64, recordID uint64, data []byte) error {
	return nil
}

// TestSavepoint_BasicOperations 测试基本的Savepoint操作
func TestSavepoint_BasicOperations(t *testing.T) {
	// 创建临时目录
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("savepoint_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	redoDir := filepath.Join(tmpDir, "redo")
	undoDir := filepath.Join(tmpDir, "undo")

	// 创建事务管理器
	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	t.Run("CreateSavepoint", func(t *testing.T) {
		// 开始事务
		trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// 创建保存点
		err = tm.Savepoint(trx, "sp1")
		if err != nil {
			t.Errorf("Failed to create savepoint: %v", err)
		}

		// 验证保存点已创建
		if _, exists := trx.Savepoints["sp1"]; !exists {
			t.Error("Savepoint 'sp1' not found in transaction")
		}

		t.Logf("✅ Savepoint 'sp1' created successfully")

		// 清理
		tm.Rollback(trx)
	})

	t.Run("MultipleSavepoints", func(t *testing.T) {
		// 开始事务
		trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		baseTime := time.Now()

		// 创建多个保存点
		err = tm.Savepoint(trx, "sp1")
		if err != nil {
			t.Errorf("Failed to create savepoint sp1: %v", err)
		}
		setSavepointCreatedAt(trx, "sp1", baseTime)

		err = tm.Savepoint(trx, "sp2")
		if err != nil {
			t.Errorf("Failed to create savepoint sp2: %v", err)
		}
		setSavepointCreatedAt(trx, "sp2", baseTime.Add(time.Millisecond))

		err = tm.Savepoint(trx, "sp3")
		if err != nil {
			t.Errorf("Failed to create savepoint sp3: %v", err)
		}
		setSavepointCreatedAt(trx, "sp3", baseTime.Add(2*time.Millisecond))

		// 验证所有保存点都存在
		if len(trx.Savepoints) != 3 {
			t.Errorf("Expected 3 savepoints, got %d", len(trx.Savepoints))
		}

		t.Logf("✅ Created 3 savepoints successfully")

		// 清理
		tm.Rollback(trx)
	})

	t.Run("ReleaseSavepoint", func(t *testing.T) {
		// 开始事务
		trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// 创建保存点
		err = tm.Savepoint(trx, "sp1")
		if err != nil {
			t.Errorf("Failed to create savepoint: %v", err)
		}

		// 释放保存点
		err = tm.ReleaseSavepoint(trx, "sp1")
		if err != nil {
			t.Errorf("Failed to release savepoint: %v", err)
		}

		// 验证保存点已删除
		if _, exists := trx.Savepoints["sp1"]; exists {
			t.Error("Savepoint 'sp1' should have been released")
		}

		t.Logf("✅ Savepoint released successfully")

		// 清理
		tm.Rollback(trx)
	})

	t.Run("ReleaseNonExistentSavepoint", func(t *testing.T) {
		// 开始事务
		trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// 尝试释放不存在的保存点
		err = tm.ReleaseSavepoint(trx, "nonexistent")
		if err == nil {
			t.Error("Expected error when releasing non-existent savepoint")
		}

		t.Logf("✅ Correctly rejected release of non-existent savepoint")

		// 清理
		tm.Rollback(trx)
	})
}

// TestSavepoint_RollbackToSavepoint 测试回滚到保存点
func TestSavepoint_RollbackToSavepoint(t *testing.T) {
	// 创建临时目录
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("savepoint_rollback_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	redoDir := filepath.Join(tmpDir, "redo")
	undoDir := filepath.Join(tmpDir, "undo")

	// 创建事务管理器
	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	t.Run("RollbackToSavepoint", func(t *testing.T) {
		// 设置mock rollback executor
		tm.undoManager.SetRollbackExecutor(&mockRollbackExecutor{})

		// 开始事务
		trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// 模拟一些操作（添加Undo日志到UndoLogManager）
		undoLog1 := &UndoLogEntry{
			TrxID:   trx.ID,
			LSN:     1,
			Type:    LOG_TYPE_INSERT,
			TableID: 1,
			Data:    []byte("data1"),
		}
		err = tm.undoManager.Append(undoLog1)
		if err != nil {
			t.Fatalf("Failed to append undo log: %v", err)
		}
		trx.UndoLogs = append(trx.UndoLogs, *undoLog1)

		// 创建保存点sp1
		err = tm.Savepoint(trx, "sp1")
		if err != nil {
			t.Fatalf("Failed to create savepoint sp1: %v", err)
		}

		sp1UndoCount := len(trx.UndoLogs)
		sp1 := trx.Savepoints["sp1"]
		t.Logf("Savepoint sp1 created with %d undo logs, LSN=%d", sp1UndoCount, sp1.LSN)

		// 继续操作（添加更多Undo日志）
		undoLog2 := &UndoLogEntry{
			TrxID:   trx.ID,
			LSN:     2,
			Type:    LOG_TYPE_INSERT,
			TableID: 1,
			Data:    []byte("data2"),
		}
		err = tm.undoManager.Append(undoLog2)
		if err != nil {
			t.Fatalf("Failed to append undo log: %v", err)
		}
		trx.UndoLogs = append(trx.UndoLogs, *undoLog2)

		undoLog3 := &UndoLogEntry{
			TrxID:   trx.ID,
			LSN:     3,
			Type:    LOG_TYPE_INSERT,
			TableID: 1,
			Data:    []byte("data3"),
		}
		err = tm.undoManager.Append(undoLog3)
		if err != nil {
			t.Fatalf("Failed to append undo log: %v", err)
		}
		trx.UndoLogs = append(trx.UndoLogs, *undoLog3)

		t.Logf("Added 2 more operations, total undo logs: %d", len(trx.UndoLogs))

		// 回滚到sp1
		err = tm.RollbackToSavepoint(trx, "sp1")
		if err != nil {
			t.Errorf("Failed to rollback to savepoint: %v", err)
		}

		// 验证Undo日志被截断
		if len(trx.UndoLogs) != sp1UndoCount {
			t.Errorf("Expected %d undo logs after rollback, got %d", sp1UndoCount, len(trx.UndoLogs))
		}

		t.Logf("✅ Successfully rolled back to savepoint sp1, undo logs: %d", len(trx.UndoLogs))

		// 清理
		tm.Rollback(trx)
	})

	t.Run("RollbackToNonExistentSavepoint", func(t *testing.T) {
		// 开始事务
		trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// 尝试回滚到不存在的保存点
		err = tm.RollbackToSavepoint(trx, "nonexistent")
		if err == nil {
			t.Error("Expected error when rolling back to non-existent savepoint")
		}

		t.Logf("✅ Correctly rejected rollback to non-existent savepoint")

		// 清理
		tm.Rollback(trx)
	})
}

// TestSavepoint_NestedSavepoints 测试嵌套保存点
func TestSavepoint_NestedSavepoints(t *testing.T) {
	// 创建临时目录
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("savepoint_nested_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	redoDir := filepath.Join(tmpDir, "redo")
	undoDir := filepath.Join(tmpDir, "undo")

	// 创建事务管理器
	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	// 设置mock rollback executor
	tm.undoManager.SetRollbackExecutor(&mockRollbackExecutor{})

	// 开始事务
	trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// 操作1
	undoLog1 := &UndoLogEntry{TrxID: trx.ID, LSN: 1, Type: LOG_TYPE_INSERT, TableID: 1, Data: []byte("data1")}
	tm.undoManager.Append(undoLog1)
	trx.UndoLogs = append(trx.UndoLogs, *undoLog1)

	// 创建sp1
	err = tm.Savepoint(trx, "sp1")
	if err != nil {
		t.Fatalf("Failed to create savepoint sp1: %v", err)
	}
	baseTime := time.Now()
	setSavepointCreatedAt(trx, "sp1", baseTime)
	t.Logf("Created sp1 with %d undo logs", len(trx.UndoLogs))

	// 操作2
	undoLog2 := &UndoLogEntry{TrxID: trx.ID, LSN: 2, Type: LOG_TYPE_INSERT, TableID: 1, Data: []byte("data2")}
	tm.undoManager.Append(undoLog2)
	trx.UndoLogs = append(trx.UndoLogs, *undoLog2)

	// 创建sp2
	err = tm.Savepoint(trx, "sp2")
	if err != nil {
		t.Fatalf("Failed to create savepoint sp2: %v", err)
	}
	setSavepointCreatedAt(trx, "sp2", baseTime.Add(time.Millisecond))
	t.Logf("Created sp2 with %d undo logs", len(trx.UndoLogs))

	// 操作3
	undoLog3 := &UndoLogEntry{TrxID: trx.ID, LSN: 3, Type: LOG_TYPE_INSERT, TableID: 1, Data: []byte("data3")}
	tm.undoManager.Append(undoLog3)
	trx.UndoLogs = append(trx.UndoLogs, *undoLog3)

	// 创建sp3
	err = tm.Savepoint(trx, "sp3")
	if err != nil {
		t.Fatalf("Failed to create savepoint sp3: %v", err)
	}
	setSavepointCreatedAt(trx, "sp3", baseTime.Add(2*time.Millisecond))
	t.Logf("Created sp3 with %d undo logs", len(trx.UndoLogs))

	// 操作4
	undoLog4 := &UndoLogEntry{TrxID: trx.ID, LSN: 4, Type: LOG_TYPE_INSERT, TableID: 1, Data: []byte("data4")}
	tm.undoManager.Append(undoLog4)
	trx.UndoLogs = append(trx.UndoLogs, *undoLog4)
	t.Logf("After operation 4: %d undo logs", len(trx.UndoLogs))

	// 回滚到sp2
	err = tm.RollbackToSavepoint(trx, "sp2")
	if err != nil {
		t.Errorf("Failed to rollback to sp2: %v", err)
	}

	// 验证：sp3应该被删除，sp2和sp1应该保留
	if _, exists := trx.Savepoints["sp3"]; exists {
		t.Error("Savepoint sp3 should have been removed")
	}
	if _, exists := trx.Savepoints["sp2"]; !exists {
		t.Error("Savepoint sp2 should still exist")
	}
	if _, exists := trx.Savepoints["sp1"]; !exists {
		t.Error("Savepoint sp1 should still exist")
	}

	// 验证Undo日志数量
	if len(trx.UndoLogs) != 2 {
		t.Errorf("Expected 2 undo logs after rollback to sp2, got %d", len(trx.UndoLogs))
	}

	t.Logf("✅ Nested savepoints working correctly")
	t.Logf("   - sp3 removed: %v", trx.Savepoints["sp3"] == nil)
	t.Logf("   - sp2 exists: %v", trx.Savepoints["sp2"] != nil)
	t.Logf("   - sp1 exists: %v", trx.Savepoints["sp1"] != nil)
	t.Logf("   - Undo logs: %d", len(trx.UndoLogs))

	// 清理
	tm.Rollback(trx)
}
