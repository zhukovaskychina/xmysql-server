package manager

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLockManagerCloseIsIdempotent(t *testing.T) {
	lm := NewLockManager()
	lm.Close()
	lm.Close()
}

func TestLockManagerVictimSelectionIsDeterministicOnEqualWaitTime(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()
	created := time.Unix(100, 0)
	lm.lockTable["1_0_1"] = &LockInfo{ResourceID: "1_0_1", Requests: []*LockRequest{
		{TxID: 9, Created: created, Granted: false},
		{TxID: 3, Created: created, Granted: false},
	}}

	if got := lm.findOldestWaitingTx(); got != 3 {
		t.Fatalf("expected lower transaction ID as deterministic victim, got %d", got)
	}
}

func TestWaitGraphSnapshotIncludesLockMetadataAndDuration(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()
	created := time.Now().Add(-75 * time.Millisecond)
	lm.lockTable["1_0_1"] = &LockInfo{ResourceID: "1_0_1", Requests: []*LockRequest{
		{TxID: 10, LockType: LOCK_X, Mode: LOCK_MODE_RECORD, Granted: true, Created: created},
		{TxID: 20, LockType: LOCK_X, Mode: LOCK_MODE_RECORD, Granted: false, Created: created},
	}}
	lm.waitGraph[20] = []uint64{10}

	edges := lm.WaitGraphSnapshot()
	if len(edges) != 1 {
		t.Fatalf("expected one wait edge, got %#v", edges)
	}
	if edges[0].ResourceID != "1_0_1" || edges[0].WaitingTxID != 20 || edges[0].BlockingTxID != 10 {
		t.Fatalf("unexpected wait edge: %#v", edges[0])
	}
	if edges[0].LockType != LOCK_X || edges[0].Mode != LOCK_MODE_RECORD || edges[0].WaitDuration < 50*time.Millisecond {
		t.Fatalf("wait edge metadata incomplete: %#v", edges[0])
	}
}

func TestWaitGraphRemovesEdgeAfterWaitingLockIsGranted(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()
	if err := lm.AcquireLock(1, 1, 1, 1, LOCK_X); err != nil {
		t.Fatalf("acquire owner lock: %v", err)
	}
	if err := lm.AcquireLock(2, 1, 1, 1, LOCK_S); !errors.Is(err, ErrLockConflict) {
		t.Fatalf("expected waiter conflict, got %v", err)
	}
	if edges := lm.WaitGraphSnapshot(); len(edges) != 1 {
		t.Fatalf("expected one wait edge before release, got %#v", edges)
	}
	lm.ReleaseLocks(1)
	if edges := lm.WaitGraphSnapshot(); len(edges) != 0 {
		t.Fatalf("granted waiter left stale wait edges: %#v", edges)
	}
	if err := lm.AcquireLock(2, 1, 1, 1, LOCK_S); err != nil {
		t.Fatalf("granted waiter should be reentrant, got %v", err)
	}
}

func TestWaitHistoryRetainsCompletedLockWait(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()
	if err := lm.AcquireLock(1, 1, 1, 1, LOCK_X); err != nil {
		t.Fatalf("acquire owner lock: %v", err)
	}
	if err := lm.AcquireLock(2, 1, 1, 1, LOCK_S); !errors.Is(err, ErrLockConflict) {
		t.Fatalf("expected waiter conflict, got %v", err)
	}
	lm.ReleaseLocks(1)
	history := lm.WaitHistorySnapshot()
	if len(history) != 1 {
		t.Fatalf("expected one completed wait, got %#v", history)
	}
	if history[0].WaitingTxID != 2 || history[0].BlockingTxID != 1 || history[0].ResourceID != "1_1_1" || history[0].WaitDuration < 0 {
		t.Fatalf("unexpected completed wait: %#v", history[0])
	}
}

func TestDeadlockSnapshotIncludesCycleVictimAndWaitDuration(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()
	now := time.Now()
	lm.lockTable["1_0_1"] = &LockInfo{ResourceID: "1_0_1", Requests: []*LockRequest{
		{TxID: 1, Granted: true, Created: now.Add(-300 * time.Millisecond)},
		{TxID: 2, Granted: false, Created: now.Add(-200 * time.Millisecond)},
	}}
	lm.lockTable["1_0_2"] = &LockInfo{ResourceID: "1_0_2", Requests: []*LockRequest{
		{TxID: 2, Granted: true, Created: now.Add(-300 * time.Millisecond)},
		{TxID: 1, Granted: false, Created: now.Add(-100 * time.Millisecond)},
	}}
	lm.waitGraph[1] = []uint64{2}
	lm.waitGraph[2] = []uint64{1}

	lm.mu.Lock()
	victim := lm.detectDeadlockLocked()
	lm.mu.Unlock()
	if victim != 2 {
		t.Fatalf("expected oldest waiting transaction 2 as victim, got %d", victim)
	}
	snapshot := lm.LastDeadlockSnapshot()
	if snapshot == nil || snapshot.VictimTxID != 2 || len(snapshot.Cycle) != 3 || snapshot.WaitDuration < 150*time.Millisecond {
		t.Fatalf("incomplete deadlock snapshot: %#v", snapshot)
	}
}

func TestDeadlockVictimSelectionStaysInsideDetectedCycle(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()
	now := time.Now()
	lm.lockTable["cycle-a"] = &LockInfo{ResourceID: "cycle-a", Requests: []*LockRequest{
		{TxID: 1, Granted: true, Created: now.Add(-300 * time.Millisecond)},
		{TxID: 2, Granted: false, Created: now.Add(-200 * time.Millisecond)},
	}}
	lm.lockTable["cycle-b"] = &LockInfo{ResourceID: "cycle-b", Requests: []*LockRequest{
		{TxID: 2, Granted: true, Created: now.Add(-300 * time.Millisecond)},
		{TxID: 1, Granted: false, Created: now.Add(-100 * time.Millisecond)},
	}}
	// An unrelated waiter is older than both members of the cycle and must
	// not be selected as the deadlock victim.
	lm.lockTable["unrelated"] = &LockInfo{ResourceID: "unrelated", Requests: []*LockRequest{
		{TxID: 99, Granted: false, Created: now.Add(-10 * time.Second)},
	}}
	lm.waitGraph[1] = []uint64{2}
	lm.waitGraph[2] = []uint64{1}

	lm.mu.Lock()
	victim := lm.detectDeadlockLocked()
	lm.mu.Unlock()
	if victim != 2 {
		t.Fatalf("expected oldest waiter inside cycle (tx 2), got %d", victim)
	}
}

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
