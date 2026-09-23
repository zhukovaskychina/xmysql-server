package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStatementTableWritesRecognizesLockingReads(t *testing.T) {
	for _, query := range []string{
		"select * from users for update",
		"select * from users lock in share mode",
		"select * from users for share",
		"select * from users where id = 1 for update skip locked",
	} {
		require.True(t, statementTableWrites(query), query)
	}
	require.False(t, statementTableWrites("select * from users"))
}

func TestTableDDLCoordinatorBlocksReadersDuringExclusiveDDL(t *testing.T) {
	coordinator := newTableDDLCoordinator()
	ddl := coordinator.lockFor("app.users")
	ddl.Lock()

	acquired := make(chan struct{})
	go func() {
		coordinator.lockFor("app.users").RLock()
		close(acquired)
		coordinator.lockFor("app.users").RUnlock()
	}()

	select {
	case <-acquired:
		t.Fatal("reader acquired table lock while exclusive DDL was active")
	case <-time.After(25 * time.Millisecond):
	}

	ddl.Unlock()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("reader did not acquire table lock after DDL completed")
	}
}

func TestTableDDLCoordinatorDoesNotCrossContaminateTables(t *testing.T) {
	coordinator := newTableDDLCoordinator()
	coordinator.lockFor("app.users").Lock()
	defer coordinator.lockFor("app.users").Unlock()

	otherAcquired := make(chan struct{})
	go func() {
		coordinator.lockFor("app.orders").RLock()
		close(otherAcquired)
		coordinator.lockFor("app.orders").RUnlock()
	}()

	select {
	case <-otherAcquired:
	case <-time.After(time.Second):
		require.Fail(t, "independent table lock was blocked by unrelated DDL")
	}
}

func TestTableDDLCoordinatorSharedDDLAllowsReadersButBlocksDML(t *testing.T) {
	coordinator := newTableDDLCoordinator()
	ddl := coordinator.lockFor("app.users")
	ddl.LockShared()

	readerAcquired := make(chan struct{})
	go func() {
		coordinator.lockFor("app.users").RLock()
		close(readerAcquired)
		coordinator.lockFor("app.users").RUnlock()
	}()
	select {
	case <-readerAcquired:
	case <-time.After(time.Second):
		t.Fatal("shared DDL blocked a reader")
	}

	dmlAcquired := make(chan struct{})
	go func() {
		coordinator.lockFor("app.users").DMLRLock()
		close(dmlAcquired)
		coordinator.lockFor("app.users").DMLRUnlock()
	}()
	select {
	case <-dmlAcquired:
		t.Fatal("shared DDL allowed a writer")
	case <-time.After(25 * time.Millisecond):
	}
	ddl.UnlockShared()
	select {
	case <-dmlAcquired:
	case <-time.After(time.Second):
		t.Fatal("writer did not acquire after shared DDL completed")
	}
}

func TestTableDDLCoordinatorContextCancellationInterruptsWait(t *testing.T) {
	coordinator := newTableDDLCoordinator()
	ddl := coordinator.lockFor("app.users")
	ddl.Lock()
	defer ddl.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- ddl.lockWithContext(ctx, tableLockRead)
	}()

	select {
	case err := <-result:
		t.Fatalf("reader returned before cancellation: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("reader did not stop after context cancellation")
	}
}

func TestTableDDLCoordinatorContextCancellationInterruptsDMLWait(t *testing.T) {
	coordinator := newTableDDLCoordinator()
	ddl := coordinator.lockFor("app.users")
	ddl.LockShared()
	defer ddl.UnlockShared()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- ddl.lockWithContext(ctx, tableLockDML)
	}()

	select {
	case err := <-result:
		t.Fatalf("DML returned before cancellation: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("DML did not stop after context cancellation")
	}
}

func TestTableDDLCoordinatorTracksMetadataOwnersAndWaiters(t *testing.T) {
	coordinator := newTableDDLCoordinator()
	lock := coordinator.lockFor("app.users")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/101"))

	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- lock.lockWithContextOwned(context.Background(), tableLockRead, "thread/202")
	}()

	require.Eventually(t, func() bool {
		for _, snapshot := range coordinator.MetadataLocks() {
			if snapshot.Owner == "thread/202" && snapshot.Status == "PENDING" && snapshot.Mode == tableLockRead {
				return true
			}
		}
		return false
	}, time.Second, 5*time.Millisecond)

	ownerSeen := false
	for _, snapshot := range coordinator.MetadataLocks() {
		if snapshot.Owner == "thread/101" && snapshot.Status == "GRANTED" && snapshot.Mode == tableLockWrite {
			ownerSeen = true
		}
	}
	require.True(t, ownerSeen)
	lock.unlockOwned(tableLockWrite, "thread/101")
	require.NoError(t, <-waiterDone)

	remaining := coordinator.MetadataLocks()
	require.Len(t, remaining, 1)
	require.Equal(t, "thread/202", remaining[0].Owner)
	require.Equal(t, "GRANTED", remaining[0].Status)
	lock.unlockOwned(tableLockRead, "thread/202")
	require.Empty(t, coordinator.MetadataLocks())
}

func TestExecutorDDLWriteLockContextCancellationInterruptsWait(t *testing.T) {
	executor := &XMySQLExecutor{ddlCoordinator: newTableDDLCoordinator()}
	holder := &ExecutionContext{Context: context.Background()}
	release, err := executor.acquireDDLWriteLock(holder, "app.users", false)
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiter := &ExecutionContext{Context: ctx}
	result := make(chan error, 1)
	go func() {
		_, acquireErr := executor.acquireDDLWriteLock(waiter, "app.users", false)
		result <- acquireErr
	}()

	select {
	case err := <-result:
		t.Fatalf("ALTER lock returned before cancellation: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("ALTER lock did not stop after context cancellation")
	}
}

func TestExecutorDDLWriteLockReentrancyReleasesOnlyAtOuterScope(t *testing.T) {
	executor := &XMySQLExecutor{ddlCoordinator: newTableDDLCoordinator()}
	ctx := &ExecutionContext{Context: context.Background()}
	outerRelease, err := executor.acquireDDLWriteLock(ctx, "app.users", false)
	require.NoError(t, err)
	innerRelease, err := executor.acquireDDLWriteLock(ctx, "app.users", false)
	require.NoError(t, err)

	innerRelease()
	select {
	case err := <-tryAcquireDDLLock(executor, "app.users"):
		t.Fatalf("lock became available while outer scope was active: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	outerRelease()
	select {
	case err := <-tryAcquireDDLLock(executor, "app.users"):
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("lock did not become available after outer scope released")
	}
}

func TestTableDDLCoordinatorOwnerReentrancyAndSharedUpgrade(t *testing.T) {
	lock := newTableDDLCoordinator().lockFor("app.users")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockShared, "thread/101"))
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/101"))

	snapshots := lockOwnerSnapshots(lock)
	require.Len(t, snapshots, 1)
	require.Equal(t, tableLockWrite, snapshots[0].Mode)

	lock.unlockOwned(tableLockShared, "thread/101")
	blocked := make(chan struct{})
	go func() {
		lock.RLock()
		close(blocked)
		lock.RUnlock()
	}()
	select {
	case <-blocked:
		t.Fatal("reader acquired before upgraded owner released its final lease")
	case <-time.After(25 * time.Millisecond):
	}
	lock.unlockOwned(tableLockWrite, "thread/101")
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("reader did not acquire after upgraded owner released")
	}
}

func TestMetadataLockWaitEdgesOnlyIncludeConflictingModes(t *testing.T) {
	coordinator := newTableDDLCoordinator()
	lock := coordinator.lockFor("app.users")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockShared, "thread/shared"))
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockRead, "thread/read"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitDone := make(chan error, 1)
	go func() { waitDone <- lock.lockWithContextOwned(ctx, tableLockDML, "thread/dml") }()
	require.Eventually(t, func() bool {
		for _, edge := range coordinator.MetadataLockWaitEdges() {
			if edge.WaitingOwner == "thread/dml" && edge.BlockingOwner == "thread/shared" {
				return true
			}
		}
		return false
	}, time.Second, 5*time.Millisecond)
	for _, edge := range coordinator.MetadataLockWaitEdges() {
		require.NotEqual(t, "thread/read", edge.BlockingOwner, "SELECT-compatible read owner must not block DML")
	}
	cancel()
	require.ErrorIs(t, <-waitDone, context.Canceled)
	lock.unlockOwned(tableLockRead, "thread/read")
	lock.unlockOwned(tableLockShared, "thread/shared")
}

func TestMetadataLockWaitHistoryRetainsCompletedWait(t *testing.T) {
	coordinator := newTableDDLCoordinator()
	lock := coordinator.lockFor("app.users")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/101"))

	waitDone := make(chan error, 1)
	go func() { waitDone <- lock.lockWithContextOwned(context.Background(), tableLockRead, "thread/202") }()
	require.Eventually(t, func() bool { return len(coordinator.MetadataLockWaitEdges()) == 1 }, time.Second, 5*time.Millisecond)

	lock.unlockOwned(tableLockWrite, "thread/101")
	require.NoError(t, <-waitDone)
	history := coordinator.MetadataLockWaitHistory()
	require.Len(t, history, 1)
	require.Equal(t, "app.users", history[0].Table)
	require.Equal(t, "thread/202", history[0].WaitingOwner)
	require.Equal(t, "thread/101", history[0].BlockingOwner)
	require.Equal(t, tableLockRead, history[0].WaitingMode)
	require.Equal(t, tableLockWrite, history[0].BlockingMode)
	require.Positive(t, history[0].WaitDuration)

	lock.unlockOwned(tableLockRead, "thread/202")
}

func lockOwnerSnapshots(lock *tableDDLTableLock) []MetadataLockSnapshot {
	coordinator := newTableDDLCoordinator()
	coordinator.locks["app.users"] = lock
	return coordinator.MetadataLocks()
}

func tryAcquireDDLLock(executor *XMySQLExecutor, table string) <-chan error {
	result := make(chan error, 1)
	go func() {
		release, err := executor.acquireDDLWriteLock(&ExecutionContext{Context: context.Background()}, table, false)
		if err == nil {
			release()
		}
		result <- err
	}()
	return result
}

func TestAlterLockModesMapToCoordinatorBehavior(t *testing.T) {
	for _, test := range []struct {
		query string
		write bool
	}{
		{query: "alter table users add column c int", write: true},
		{query: "alter table users add column c int, lock=default", write: true},
		{query: "alter table users add column c int, lock=exclusive", write: true},
		{query: "alter table users add column c int, lock=shared", write: true},
		{query: "alter table users add column c int, lock=none", write: false},
	} {
		require.Equal(t, test.write, alterRequiresWriteLock(test.query), test.query)
	}
	require.True(t, alterUsesSharedLock("alter table users add column c int, lock=shared"))
	require.False(t, alterUsesSharedLock("alter table users add column c int, lock=exclusive"))
	require.Equal(t, "app.users", mustTableAccess(t, "select * from users", "app"))
	require.Equal(t, "app.users", mustAlterAccess(t, "alter table users add column c int", "app"))
}

func TestStatementTableAccessesCollectsAndSortsJoinSources(t *testing.T) {
	require.Equal(t, []string{"app.orders", "app.users"}, statementTableAccesses(
		"select u.id from users u join orders o on o.user_id = u.id", "app"))
	require.Equal(t, []string{"app.orders", "app.users"}, statementTableAccesses(
		"update users u join orders o on o.user_id = u.id set u.id = u.id", "app"))
}

func mustTableAccess(t *testing.T, query, database string) string {
	t.Helper()
	table, ok := statementTableAccess(query, database)
	require.True(t, ok)
	return table
}

func mustAlterAccess(t *testing.T, query, database string) string {
	t.Helper()
	table, ok := alterTableAccess(query, database)
	require.True(t, ok)
	return table
}
