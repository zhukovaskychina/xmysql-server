package manager

import (
	"errors"
	"sync"
	"testing"
)

func TestLockManager_BasicLocking(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	// Test shared lock acquisition
	err := lm.AcquireLock(1, 1, 1, 1, LOCK_S)
	if err != nil {
		t.Errorf("Failed to acquire shared lock: %v", err)
	}

	// Test another shared lock acquisition
	err = lm.AcquireLock(2, 1, 1, 1, LOCK_S)
	if err != nil {
		t.Errorf("Failed to acquire second shared lock: %v", err)
	}

	// Test exclusive lock conflict
	err = lm.AcquireLock(3, 1, 1, 1, LOCK_X)
	if !errors.Is(err, ErrLockConflict) {
		t.Errorf("Expected lock conflict, got %v", err)
	}

	// Release locks
	lm.ReleaseLocks(1)
	lm.ReleaseLocks(2)

	// Now exclusive lock should succeed
	err = lm.AcquireLock(3, 1, 1, 1, LOCK_X)
	if err != nil {
		t.Errorf("Failed to acquire exclusive lock after release: %v", err)
	}
}

func TestLockManager_DeadlockDetection(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	err := lm.AcquireLock(1, 1, 1, 1, LOCK_X) // Resource A
	if err != nil {
		t.Fatalf("T1: Failed to acquire first lock: %v", err)
	}

	err = lm.AcquireLock(2, 1, 1, 2, LOCK_X) // Resource B
	if err != nil {
		t.Fatalf("T2: Failed to acquire first lock: %v", err)
	}

	err = lm.AcquireLock(1, 1, 1, 2, LOCK_X) // T1 waits on B
	if !errors.Is(err, ErrLockConflict) {
		t.Fatalf("T1: Expected lock conflict while waiting on B, got %v", err)
	}

	err = lm.AcquireLock(2, 1, 1, 1, LOCK_X) // T2 forms cycle on A
	if !errors.Is(err, ErrDeadlockDetected) {
		t.Fatalf("T2: Expected deadlock detection, got %v", err)
	}
}

func TestLockManager_LockUpgrade(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	// Acquire shared lock
	err := lm.AcquireLock(1, 1, 1, 1, LOCK_S)
	if err != nil {
		t.Errorf("Failed to acquire shared lock: %v", err)
	}

	// Upgrade to exclusive lock
	err = lm.AcquireLock(1, 1, 1, 1, LOCK_X)
	if err != nil {
		t.Errorf("Failed to upgrade lock: %v", err)
	}

	// Another transaction should not be able to acquire any lock
	err = lm.AcquireLock(2, 1, 1, 1, LOCK_S)
	if !errors.Is(err, ErrLockConflict) {
		t.Errorf("Expected lock conflict after upgrade, got %v", err)
	}
}

func TestLockManager_ConcurrentAccess(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	const numTx = 10
	const numResources = 5
	var wg sync.WaitGroup
	wg.Add(numTx)

	for i := uint64(1); i <= numTx; i++ {
		go func(txID uint64) {
			defer wg.Done()
			for j := uint64(1); j <= numResources; j++ {
				// Try to acquire shared lock
				err := lm.AcquireLock(txID, 1, 1, j, LOCK_S)
				if err != nil {
					t.Errorf("TX%d failed to acquire S lock on resource %d: %v", txID, j, err)
					return
				}
			}
			// Release all locks
			lm.ReleaseLocks(txID)
		}(i)
	}

	wg.Wait()
}

func TestLockManager_ReleaseSingleLock(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	err := lm.AcquireLock(1, 1, 1, 1, LOCK_X)
	if err != nil {
		t.Fatalf("Failed to acquire first lock: %v", err)
	}

	err = lm.AcquireLock(2, 1, 1, 1, LOCK_S)
	if !errors.Is(err, ErrLockConflict) {
		t.Fatalf("Expected conflict while releasing lock not processed: %v", err)
	}

	if err := lm.ReleaseLock(1, makeResourceID(1, 1, 1)); err != nil {
		t.Fatalf("Failed to release single lock: %v", err)
	}

	err = lm.AcquireLock(2, 1, 1, 1, LOCK_S)
	if err != nil {
		t.Fatalf("Expected to acquire lock after single release, got %v", err)
	}

	if err := lm.ReleaseLock(2, makeResourceID(1, 1, 1)); err != nil {
		t.Fatalf("Failed to release single lock for tx2: %v", err)
	}
}

func TestLockManager_ReleaseSingleLockNotFound(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	if err := lm.ReleaseLock(1, makeResourceID(1, 1, 1)); !errors.Is(err, ErrLockNotFound) {
		t.Fatalf("Expected ErrLockNotFound for missing lock, got %v", err)
	}
}

func TestLockManager_AbortTransactionCallback(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	callbackCalled := false
	var callbackTxID uint64
	lm.SetAbortTransactionHandler(func(txID uint64) {
		callbackCalled = true
		callbackTxID = txID
	})

	err := lm.AcquireLock(1, 1, 1, 1, LOCK_X)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}
	err = lm.AcquireLock(2, 1, 1, 1, LOCK_S)
	if !errors.Is(err, ErrLockConflict) {
		t.Fatalf("Expected lock conflict, got %v", err)
	}

	// 直接触发回滚清理，避免依赖异步死锁检测时序
	lm.abortTransaction(1)

	if !callbackCalled {
		t.Fatalf("Expected abort callback to be called")
	}
	if callbackTxID != 1 {
		t.Fatalf("Expected callback with txID=1, got %d", callbackTxID)
	}

	// 回滚后锁应已释放，其他事务可再次获取
	if err = lm.AcquireLock(2, 1, 1, 1, LOCK_S); err != nil {
		t.Fatalf("Expected lock acquisition after abort, got %v", err)
	}
}

func TestLockManager_LockRelease(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	// Acquire exclusive lock
	err := lm.AcquireLock(1, 1, 1, 1, LOCK_X)
	if err != nil {
		t.Errorf("Failed to acquire exclusive lock: %v", err)
	}

	// Try to acquire shared lock (should fail)
	err = lm.AcquireLock(2, 1, 1, 1, LOCK_S)
	if !errors.Is(err, ErrLockConflict) {
		t.Errorf("Expected lock conflict, got %v", err)
	}

	// Release exclusive lock
	lm.ReleaseLocks(1)

	// Now shared lock should succeed
	err = lm.AcquireLock(2, 1, 1, 1, LOCK_S)
	if err != nil {
		t.Errorf("Failed to acquire shared lock after release: %v", err)
	}
}
