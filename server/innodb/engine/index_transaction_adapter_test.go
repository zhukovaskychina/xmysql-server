package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// TestIndexAdapterInsertDelete 测试索引插入和删除
func TestIndexAdapterInsertDelete(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "index_adapter_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建简化的索引适配器（不需要完整的存储引擎）
	indexAdapter := &IndexAdapter{
		indexManager:   nil, // 简化测试，不使用真实的索引管理器
		btreeManager:   nil,
		storageAdapter: nil,
	}

	ctx := context.Background()

	// 测试插入（简化模式，应该返回错误因为没有btreeManager）
	err = indexAdapter.InsertEntry(ctx, 1, []byte("key1"), []byte("value1"))
	if err == nil {
		t.Error("Expected error when btreeManager is nil, got nil")
	}
	t.Logf("✅ InsertEntry correctly returns error when btreeManager is nil: %v", err)

	// 测试删除（简化模式，应该返回错误因为没有btreeManager）
	err = indexAdapter.DeleteEntry(ctx, 1, []byte("key1"))
	if err == nil {
		t.Error("Expected error when btreeManager is nil, got nil")
	}
	t.Logf("✅ DeleteEntry correctly returns error when btreeManager is nil: %v", err)

	// 测试空key
	err = indexAdapter.InsertEntry(ctx, 1, []byte{}, []byte("value"))
	if err == nil {
		t.Error("Expected error for empty key, got nil")
	}
	t.Logf("✅ InsertEntry correctly rejects empty key: %v", err)
}

// TestIndexMetadataIsCoveringIndex 测试覆盖索引判定
func TestIndexMetadataIsCoveringIndex(t *testing.T) {
	// 测试主键索引
	primaryIndex := &IndexMetadata{
		IndexID:     1,
		IndexName:   "PRIMARY",
		IsPrimary:   true,
		IsUnique:    true,
		Columns:     []string{"id"},
		IsClustered: true,
	}

	// 主键索引应该覆盖所有列
	if !primaryIndex.IsCoveringIndex([]string{"id", "name", "age"}) {
		t.Error("Primary index should cover all columns")
	}
	t.Logf("✅ Primary index covers all columns")

	// 测试普通索引
	secondaryIndex := &IndexMetadata{
		IndexID:     2,
		IndexName:   "idx_name_age",
		IsPrimary:   false,
		IsUnique:    false,
		Columns:     []string{"name", "age"},
		IsClustered: false,
	}

	// 测试覆盖的情况（索引列 + 主键列）
	if !secondaryIndex.IsCoveringIndex([]string{"name", "age"}) {
		t.Error("Index should cover its own columns")
	}
	t.Logf("✅ Index covers its own columns")

	// 测试覆盖的情况（包含主键）
	if !secondaryIndex.IsCoveringIndex([]string{"name", "id"}) {
		t.Error("Index should cover its columns + primary key")
	}
	t.Logf("✅ Index covers its columns + primary key")

	// 测试不覆盖的情况
	if secondaryIndex.IsCoveringIndex([]string{"name", "email"}) {
		t.Error("Index should not cover columns not in index")
	}
	t.Logf("✅ Index correctly identifies non-covering columns")

	// 测试空列表
	if !secondaryIndex.IsCoveringIndex([]string{}) {
		t.Error("Index should cover empty column list")
	}
	t.Logf("✅ Index covers empty column list")
}

// TestTransactionAdapterCommitRollback 测试事务提交和回滚
func TestTransactionAdapterCommitRollback(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "txn_adapter_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建存储管理器（简化版本）
	dataDir := filepath.Join(tmpDir, "data")
	os.MkdirAll(dataDir, 0755)

	// 创建锁管理器
	lockManager := manager.NewLockManager()
	defer lockManager.Close()

	// 创建事务适配器（不使用真实的存储管理器）
	txnAdapter := &TransactionAdapter{
		storageManager: nil, // 简化测试
		lockManager:    lockManager,
	}

	ctx := context.Background()

	// 创建测试事务
	txn := &Transaction{
		TxnID:          1,
		ReadOnly:       false,
		IsolationLevel: "REPEATABLE_READ",
		StartTime:      0,
	}

	// 测试提交（简化模式，应该成功）
	err = txnAdapter.CommitTransaction(ctx, txn)
	if err != nil {
		t.Errorf("Failed to commit transaction: %v", err)
	}
	t.Logf("✅ Transaction committed successfully")

	// 创建另一个事务测试回滚
	txn2 := &Transaction{
		TxnID:          2,
		ReadOnly:       false,
		IsolationLevel: "REPEATABLE_READ",
		StartTime:      0,
	}

	// 测试回滚（简化模式，应该成功）
	err = txnAdapter.RollbackTransaction(ctx, txn2)
	if err != nil {
		t.Errorf("Failed to rollback transaction: %v", err)
	}
	t.Logf("✅ Transaction rolled back successfully")

	// 测试nil事务
	err = txnAdapter.CommitTransaction(ctx, nil)
	if err == nil {
		t.Error("Expected error for nil transaction, got nil")
	}
	t.Logf("✅ CommitTransaction correctly rejects nil transaction: %v", err)
}

// TestTransactionAdapterLocking 测试锁获取和释放
func TestTransactionAdapterLocking(t *testing.T) {
	// 创建锁管理器
	lockManager := manager.NewLockManager()
	defer lockManager.Close()

	// 创建事务适配器
	txnAdapter := &TransactionAdapter{
		storageManager: nil,
		lockManager:    lockManager,
	}

	ctx := context.Background()

	// 创建测试事务
	txn := &Transaction{
		TxnID:          1,
		ReadOnly:       false,
		IsolationLevel: "REPEATABLE_READ",
		StartTime:      0,
	}

	// 测试获取共享锁
	err := txnAdapter.AcquireLock(ctx, txn, "S", "1:100:5")
	if err != nil {
		t.Errorf("Failed to acquire shared lock: %v", err)
	}
	t.Logf("✅ Acquired shared lock successfully")

	// 测试获取排他锁（不同资源）
	err = txnAdapter.AcquireLock(ctx, txn, "X", "1:100:6")
	if err != nil {
		t.Errorf("Failed to acquire exclusive lock: %v", err)
	}
	t.Logf("✅ Acquired exclusive lock successfully")

	// 测试释放锁
	err = txnAdapter.ReleaseLock(ctx, txn, "1:100:5")
	if err != nil {
		t.Errorf("Failed to release lock: %v", err)
	}
	t.Logf("✅ Released lock successfully")

	// 测试无效的锁类型
	err = txnAdapter.AcquireLock(ctx, txn, "INVALID", "1:100:7")
	if err == nil {
		t.Error("Expected error for invalid lock type, got nil")
	}
	t.Logf("✅ AcquireLock correctly rejects invalid lock type: %v", err)

	// 测试无效的资源格式
	err = txnAdapter.AcquireLock(ctx, txn, "S", "invalid_format")
	if err == nil {
		t.Error("Expected error for invalid resource format, got nil")
	}
	t.Logf("✅ AcquireLock correctly rejects invalid resource format: %v", err)

	// 测试简化格式 "tableID:rowID"
	err = txnAdapter.AcquireLock(ctx, txn, "S", "1:100")
	if err != nil {
		t.Errorf("Failed to acquire lock with simplified format: %v", err)
	}
	t.Logf("✅ Acquired lock with simplified format successfully")
}

// TestTransactionAdapterWithoutLockManager 测试没有锁管理器的情况
func TestTransactionAdapterWithoutLockManager(t *testing.T) {
	// 创建没有锁管理器的事务适配器
	txnAdapter := &TransactionAdapter{
		storageManager: nil,
		lockManager:    nil, // 没有锁管理器
	}

	ctx := context.Background()

	txn := &Transaction{
		TxnID:          1,
		ReadOnly:       false,
		IsolationLevel: "REPEATABLE_READ",
		StartTime:      0,
	}

	// 测试获取锁（应该成功，但不实际获取锁）
	err := txnAdapter.AcquireLock(ctx, txn, "S", "1:100:5")
	if err != nil {
		t.Errorf("AcquireLock should succeed without lock manager: %v", err)
	}
	t.Logf("✅ AcquireLock succeeds without lock manager (simplified mode)")

	// 测试释放锁（应该成功，但不实际释放锁）
	err = txnAdapter.ReleaseLock(ctx, txn, "1:100:5")
	if err != nil {
		t.Errorf("ReleaseLock should succeed without lock manager: %v", err)
	}
	t.Logf("✅ ReleaseLock succeeds without lock manager (simplified mode)")

	// 测试提交（应该成功）
	err = txnAdapter.CommitTransaction(ctx, txn)
	if err != nil {
		t.Errorf("CommitTransaction should succeed without lock manager: %v", err)
	}
	t.Logf("✅ CommitTransaction succeeds without lock manager")
}

// TestGetIndexMetadata 测试获取索引元数据
func TestGetIndexMetadata(t *testing.T) {
	// 创建简化的索引适配器
	indexAdapter := &IndexAdapter{
		indexManager:        nil,
		btreeManager:        nil,
		storageAdapter:      nil,
		tableStorageManager: nil,
	}

	ctx := context.Background()

	// 测试获取主键索引元数据（简化模式）
	metadata, err := indexAdapter.GetIndexMetadata(ctx, "test_db", "test_table", "PRIMARY")
	if err != nil {
		t.Errorf("Failed to get PRIMARY index metadata: %v", err)
	}

	if metadata == nil {
		t.Fatal("Metadata should not be nil")
	}

	if !metadata.IsPrimary {
		t.Error("PRIMARY index should have IsPrimary=true")
	}

	if !metadata.IsClustered {
		t.Error("PRIMARY index should have IsClustered=true")
	}

	t.Logf("✅ Got PRIMARY index metadata: %+v", metadata)

	// 测试获取普通索引元数据（简化模式）
	metadata2, err := indexAdapter.GetIndexMetadata(ctx, "test_db", "test_table", "idx_name")
	if err != nil {
		t.Errorf("Failed to get secondary index metadata: %v", err)
	}

	if metadata2 == nil {
		t.Fatal("Metadata should not be nil")
	}

	if metadata2.IsPrimary {
		t.Error("Secondary index should have IsPrimary=false")
	}

	t.Logf("✅ Got secondary index metadata: %+v", metadata2)
}
