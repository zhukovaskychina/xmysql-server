package manager

import (
	"sync"
	"testing"
	"time"
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
	if err == nil {
		t.Error("Expected conflict with exclusive lock, but got none")
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

	// Create a deadlock scenario
	var wg sync.WaitGroup
	wg.Add(2)

	// Transaction 1: Get S lock on A, then try X lock on B
	go func() {
		defer wg.Done()
		err := lm.AcquireLock(1, 1, 1, 1, LOCK_S) // Resource A
		if err != nil {
			t.Errorf("T1: Failed to acquire first lock: %v", err)
			return
		}

		time.Sleep(100 * time.Millisecond) // Ensure T2 gets its first lock

		err = lm.AcquireLock(1, 1, 1, 2, LOCK_X) // Resource B
		if err != nil && err.Error() != "deadlock detected" {
			t.Errorf("T1: Expected deadlock detection, got: %v", err)
		}
	}()

	// Transaction 2: Get X lock on B, then try S lock on A
	go func() {
		defer wg.Done()
		err := lm.AcquireLock(2, 1, 1, 2, LOCK_X) // Resource B
		if err != nil {
			t.Errorf("T2: Failed to acquire first lock: %v", err)
			return
		}

		time.Sleep(100 * time.Millisecond) // Ensure T1 gets its first lock

		err = lm.AcquireLock(2, 1, 1, 1, LOCK_S) // Resource A
		if err != nil && err.Error() != "deadlock detected" {
			t.Errorf("T2: Expected deadlock detection, got: %v", err)
		}
	}()

	wg.Wait()
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
	if err == nil {
		t.Error("Expected lock conflict after upgrade, but got none")
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
				time.Sleep(10 * time.Millisecond)
			}
			// Release all locks
			lm.ReleaseLocks(txID)
		}(i)
	}

	wg.Wait()
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
	if err == nil {
		t.Error("Expected lock conflict, but got none")
	}

	// Release exclusive lock
	lm.ReleaseLocks(1)

	// Now shared lock should succeed
	err = lm.AcquireLock(2, 1, 1, 1, LOCK_S)
	if err != nil {
		t.Errorf("Failed to acquire shared lock after release: %v", err)
	}
}
