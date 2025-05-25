package mvcc

import (
	"testing"
	"time"
	"xmysql-server/server/innodb/basic"
)

func TestTransactionManager(t *testing.T) {
	// 创建事务管理器
	tm := NewTransactionManager(RepeatableRead)

	// 测试事务创建
	t.Run("TestBeginTransaction", func(t *testing.T) {
		txn, err := tm.BeginTransaction(RepeatableRead)
		if err != nil {
			t.Errorf("Failed to begin transaction: %v", err)
		}
		if txn.Status != Active {
			t.Errorf("Expected transaction status Active, got %v", txn.Status)
		}
	})

	// 测试事务提交
	t.Run("TestCommitTransaction", func(t *testing.T) {
		txn, _ := tm.BeginTransaction(RepeatableRead)
		err := tm.CommitTransaction(txn)
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}
		if txn.Status != Committed {
			t.Errorf("Expected transaction status Committed, got %v", txn.Status)
		}
	})

	// 测试事务回滚
	t.Run("TestRollbackTransaction", func(t *testing.T) {
		txn, _ := tm.BeginTransaction(RepeatableRead)
		err := tm.RollbackTransaction(txn)
		if err != nil {
			t.Errorf("Failed to rollback transaction: %v", err)
		}
		if txn.Status != Aborted {
			t.Errorf("Expected transaction status Aborted, got %v", txn.Status)
		}
	})

	// 测试锁获取和释放
	t.Run("TestLockOperations", func(t *testing.T) {
		txn1, _ := tm.BeginTransaction(RepeatableRead)
		txn2, _ := tm.BeginTransaction(RepeatableRead)

		// 获取共享锁
		err := tm.AcquireLock(txn1, "resource1", SharedLock)
		if err != nil {
			t.Errorf("Failed to acquire shared lock: %v", err)
		}

		// 另一个事务也可以获取共享锁
		err = tm.AcquireLock(txn2, "resource1", SharedLock)
		if err != nil {
			t.Errorf("Failed to acquire shared lock for second transaction: %v", err)
		}

		// 尝试获取排他锁(应该失败)
		err = tm.AcquireLock(txn2, "resource1", ExclusiveLock)
		if err == nil {
			t.Error("Expected lock conflict error, got nil")
		}

		// 释放锁
		tm.ReleaseLock(txn1, "resource1")
		tm.ReleaseLock(txn2, "resource1")
	})

	// 测试隔离级别
	t.Run("TestIsolationLevels", func(t *testing.T) {
		// 读未提交
		txn1, _ := tm.BeginTransaction(ReadUncommitted)
		if txn1.IsolationLevel != ReadUncommitted {
			t.Errorf("Expected ReadUncommitted isolation level, got %v", txn1.IsolationLevel)
		}

		// 可重复读
		txn2, _ := tm.BeginTransaction(RepeatableRead)
		if txn2.SnapshotVersion == 0 {
			t.Error("Expected non-zero snapshot version for RepeatableRead")
		}
	})
}

func TestDeadlockScenarios(t *testing.T) {
	tm := NewTransactionManager(RepeatableRead)

	// 测试简单死锁场景
	t.Run("TestSimpleDeadlock", func(t *testing.T) {
		txn1, _ := tm.BeginTransaction(RepeatableRead)
		txn2, _ := tm.BeginTransaction(RepeatableRead)

		// txn1获取resource1的锁
		err := tm.AcquireLock(txn1, "resource1", ExclusiveLock)
		if err != nil {
			t.Errorf("Failed to acquire lock for txn1: %v", err)
		}

		// txn2获取resource2的锁
		err = tm.AcquireLock(txn2, "resource2", ExclusiveLock)
		if err != nil {
			t.Errorf("Failed to acquire lock for txn2: %v", err)
		}

		// txn1尝试获取resource2的锁(会等待)
		err = tm.AcquireLock(txn1, "resource2", ExclusiveLock)
		if err == nil {
			t.Error("Expected lock conflict error for txn1")
		}

		// txn2尝试获取resource1的锁(应该检测到死锁)
		err = tm.AcquireLock(txn2, "resource1", ExclusiveLock)
		if err != basic.ErrDeadlockDetected {
			t.Errorf("Expected deadlock error, got %v", err)
		}
	})

	// 测试复杂死锁场景
	t.Run("TestComplexDeadlock", func(t *testing.T) {
		txn1, _ := tm.BeginTransaction(RepeatableRead)
		txn2, _ := tm.BeginTransaction(RepeatableRead)
		txn3, _ := tm.BeginTransaction(RepeatableRead)

		// 创建环形等待
		_ = tm.AcquireLock(txn1, "resource1", ExclusiveLock)
		_ = tm.AcquireLock(txn2, "resource2", ExclusiveLock)
		_ = tm.AcquireLock(txn3, "resource3", ExclusiveLock)

		_ = tm.AcquireLock(txn1, "resource2", ExclusiveLock) // 等待txn2
		_ = tm.AcquireLock(txn2, "resource3", ExclusiveLock) // 等待txn3

		// 这个请求应该检测到死锁
		err := tm.AcquireLock(txn3, "resource1", ExclusiveLock)
		if err != basic.ErrDeadlockDetected {
			t.Errorf("Expected deadlock error, got %v", err)
		}
	})

	// 测试死锁检测的性能
	t.Run("TestDeadlockDetectionPerformance", func(t *testing.T) {
		const numTransactions = 100
		const numResources = 50
		transactions := make([]*Transaction, numTransactions)

		start := time.Now()

		// 创建多个事务
		for i := 0; i < numTransactions; i++ {
			txn, _ := tm.BeginTransaction(RepeatableRead)
			transactions[i] = txn
		}

		// 随机获取锁
		for i := 0; i < numTransactions; i++ {
			for j := 0; j < 5; j++ { // 每个事务尝试获取5个锁
				resourceID := fmt.Sprintf("resource%d", j%numResources)
				_ = tm.AcquireLock(transactions[i], resourceID, SharedLock)
			}
		}

		duration := time.Since(start)
		if duration > time.Second*2 {
			t.Errorf("Deadlock detection took too long: %v", duration)
		}
	})
}
