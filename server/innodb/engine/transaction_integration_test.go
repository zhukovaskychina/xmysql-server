package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// TestTransactionBeginCommit 测试事务开始和提交
func TestTransactionBeginCommit(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "txn_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建事务管理器
	redoDir := filepath.Join(tmpDir, "redo")
	undoDir := filepath.Join(tmpDir, "undo")
	os.MkdirAll(redoDir, 0755)
	os.MkdirAll(undoDir, 0755)

	txManager, err := manager.NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer txManager.Close()

	// 创建DML执行器（简化版本，只测试事务部分）
	executor := &StorageIntegratedDMLExecutor{
		txManager: txManager,
		stats:     &DMLExecutorStats{},
	}

	ctx := context.Background()

	// 测试开始事务
	txn, err := executor.beginStorageTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	txnCtx, ok := txn.(*StorageTransactionContext)
	if !ok {
		t.Fatal("Invalid transaction context type")
	}

	// 验证事务上下文
	if txnCtx.Status != "ACTIVE" {
		t.Errorf("Expected status ACTIVE, got %s", txnCtx.Status)
	}

	if txnCtx.RealTransaction == nil {
		t.Error("RealTransaction should not be nil when txManager is present")
	}

	t.Logf("✅ Transaction started: TxnID=%d, Status=%s", txnCtx.TransactionID, txnCtx.Status)

	// 模拟一些修改
	txnCtx.ModifiedPages["1:100"] = 100
	txnCtx.ModifiedPages["1:101"] = 101

	// 测试提交事务
	err = executor.commitStorageTransaction(ctx, txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// 验证事务状态
	if txnCtx.Status != "COMMITTED" {
		t.Errorf("Expected status COMMITTED, got %s", txnCtx.Status)
	}

	if txnCtx.EndTime.IsZero() {
		t.Error("EndTime should be set after commit")
	}

	t.Logf("✅ Transaction committed: TxnID=%d, Duration=%v", txnCtx.TransactionID, txnCtx.EndTime.Sub(txnCtx.StartTime))
}

// TestTransactionRollback 测试事务回滚
func TestTransactionRollback(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "txn_rollback_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建事务管理器
	redoDir := filepath.Join(tmpDir, "redo")
	undoDir := filepath.Join(tmpDir, "undo")
	os.MkdirAll(redoDir, 0755)
	os.MkdirAll(undoDir, 0755)

	txManager, err := manager.NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer txManager.Close()

	// 创建DML执行器
	executor := &StorageIntegratedDMLExecutor{
		txManager: txManager,
		stats:     &DMLExecutorStats{},
	}

	ctx := context.Background()

	// 开始事务
	txn, err := executor.beginStorageTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	txnCtx, ok := txn.(*StorageTransactionContext)
	if !ok {
		t.Fatal("Invalid transaction context type")
	}

	t.Logf("✅ Transaction started: TxnID=%d", txnCtx.TransactionID)

	// 模拟一些修改
	txnCtx.ModifiedPages["1:200"] = 200
	txnCtx.ModifiedPages["1:201"] = 201
	txnCtx.ModifiedPages["1:202"] = 202

	// 测试回滚事务
	err = executor.rollbackStorageTransaction(ctx, txn)
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// 验证事务状态
	if txnCtx.Status != "ROLLED_BACK" {
		t.Errorf("Expected status ROLLED_BACK, got %s", txnCtx.Status)
	}

	if txnCtx.EndTime.IsZero() {
		t.Error("EndTime should be set after rollback")
	}

	t.Logf("✅ Transaction rolled back: TxnID=%d, Duration=%v", txnCtx.TransactionID, txnCtx.EndTime.Sub(txnCtx.StartTime))
}

// TestTransactionIsolationLevel 测试事务隔离级别
func TestTransactionIsolationLevel(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "txn_isolation_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建事务管理器
	redoDir := filepath.Join(tmpDir, "redo")
	undoDir := filepath.Join(tmpDir, "undo")
	os.MkdirAll(redoDir, 0755)
	os.MkdirAll(undoDir, 0755)

	txManager, err := manager.NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer txManager.Close()

	// 创建DML执行器
	executor := &StorageIntegratedDMLExecutor{
		txManager: txManager,
		stats:     &DMLExecutorStats{},
	}

	// 测试不同的隔离级别
	isolationLevels := []struct {
		name  string
		level uint8
	}{
		{"READ_UNCOMMITTED", manager.TRX_ISO_READ_UNCOMMITTED},
		{"READ_COMMITTED", manager.TRX_ISO_READ_COMMITTED},
		{"REPEATABLE_READ", manager.TRX_ISO_REPEATABLE_READ},
		{"SERIALIZABLE", manager.TRX_ISO_SERIALIZABLE},
	}

	for _, il := range isolationLevels {
		t.Run(il.name, func(t *testing.T) {
			// 创建带隔离级别的上下文
			ctx := context.WithValue(context.Background(), "isolation_level", il.level)

			// 开始事务
			txn, err := executor.beginStorageTransaction(ctx)
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}

			txnCtx, ok := txn.(*StorageTransactionContext)
			if !ok {
				t.Fatal("Invalid transaction context type")
			}

			// 验证隔离级别
			if txnCtx.RealTransaction != nil {
				if txnCtx.RealTransaction.IsolationLevel != il.level {
					t.Errorf("Expected isolation level %d, got %d", il.level, txnCtx.RealTransaction.IsolationLevel)
				}
				t.Logf("✅ Transaction with %s: TxnID=%d", il.name, txnCtx.TransactionID)
			}

			// 提交事务
			err = executor.commitStorageTransaction(ctx, txn)
			if err != nil {
				t.Fatalf("Failed to commit transaction: %v", err)
			}
		})
	}
}

// TestTransactionReadOnly 测试只读事务
func TestTransactionReadOnly(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "txn_readonly_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建事务管理器
	redoDir := filepath.Join(tmpDir, "redo")
	undoDir := filepath.Join(tmpDir, "undo")
	os.MkdirAll(redoDir, 0755)
	os.MkdirAll(undoDir, 0755)

	txManager, err := manager.NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer txManager.Close()

	// 创建DML执行器
	executor := &StorageIntegratedDMLExecutor{
		txManager: txManager,
		stats:     &DMLExecutorStats{},
	}

	// 创建只读上下文
	ctx := context.WithValue(context.Background(), "read_only", true)

	// 开始只读事务
	txn, err := executor.beginStorageTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	txnCtx, ok := txn.(*StorageTransactionContext)
	if !ok {
		t.Fatal("Invalid transaction context type")
	}

	// 验证只读标志
	if txnCtx.RealTransaction != nil {
		if !txnCtx.RealTransaction.IsReadOnly {
			t.Error("Expected read-only transaction")
		}
		t.Logf("✅ Read-only transaction: TxnID=%d", txnCtx.TransactionID)
	}

	// 提交事务
	err = executor.commitStorageTransaction(ctx, txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

// TestTransactionWithoutManager 测试没有事务管理器的情况
func TestTransactionWithoutManager(t *testing.T) {
	// 创建没有事务管理器的DML执行器
	executor := &StorageIntegratedDMLExecutor{
		txManager: nil, // 没有事务管理器
		stats:     &DMLExecutorStats{},
	}

	ctx := context.Background()

	// 开始事务
	txn, err := executor.beginStorageTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	txnCtx, ok := txn.(*StorageTransactionContext)
	if !ok {
		t.Fatal("Invalid transaction context type")
	}

	// 验证使用简化事务上下文
	if txnCtx.RealTransaction != nil {
		t.Error("RealTransaction should be nil when txManager is not present")
	}

	if txnCtx.Status != "ACTIVE" {
		t.Errorf("Expected status ACTIVE, got %s", txnCtx.Status)
	}

	t.Logf("✅ Simplified transaction: TxnID=%d", txnCtx.TransactionID)

	// 提交事务
	err = executor.commitStorageTransaction(ctx, txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if txnCtx.Status != "COMMITTED" {
		t.Errorf("Expected status COMMITTED, got %s", txnCtx.Status)
	}
}
