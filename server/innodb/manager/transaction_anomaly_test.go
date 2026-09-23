package manager

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionIsolationAnomalyMatrix(t *testing.T) {
	mvcc := NewMVCCManager(&MVCCConfig{MaxActiveTxs: 64})
	writer, err := mvcc.BeginTransactionWithIsolation(TRX_ISO_READ_COMMITTED)
	require.NoError(t, err)
	rrReader, err := mvcc.BeginTransactionWithIsolation(TRX_ISO_REPEATABLE_READ)
	require.NoError(t, err)
	ruReader, err := mvcc.BeginTransactionWithIsolation(TRX_ISO_READ_UNCOMMITTED)
	require.NoError(t, err)

	visible, err := mvcc.IsVisible(rrReader, writer)
	require.NoError(t, err)
	require.False(t, visible, "RR must not dirty-read an active writer")
	visible, err = mvcc.IsVisible(ruReader, writer)
	require.NoError(t, err)
	require.True(t, visible, "RU may observe an uncommitted version")

	require.NoError(t, mvcc.CommitTransaction(writer))
	visible, err = mvcc.IsVisible(rrReader, writer)
	require.NoError(t, err)
	require.False(t, visible, "RR keeps the snapshot taken before commit")
	rcReader, err := mvcc.BeginTransactionWithIsolation(TRX_ISO_READ_COMMITTED)
	require.NoError(t, err)
	visible, err = mvcc.IsVisible(rcReader, writer)
	require.NoError(t, err)
	require.True(t, visible, "RC sees a version committed before its statement")

	// A new version created after the RR snapshot is the phantom boundary.
	phantomWriter, err := mvcc.BeginTransaction()
	require.NoError(t, err)
	visible, err = mvcc.IsVisible(rrReader, phantomWriter)
	require.NoError(t, err)
	require.False(t, visible, "RR snapshot excludes a newly inserted row")
	require.NoError(t, mvcc.CommitTransaction(phantomWriter))

	// X locks serialize the two writers, preventing a lost update.
	locks := NewLockManager()
	defer locks.Close()
	require.NoError(t, locks.AcquireLock(10, 1, 1, 1, LOCK_X))
	require.ErrorIs(t, locks.AcquireLock(11, 1, 1, 1, LOCK_X), ErrLockConflict)
	locks.ReleaseLocks(10)
	require.NoError(t, locks.AcquireLock(11, 1, 1, 1, LOCK_X))
	locks.ReleaseLocks(11)

	// Serializable invariant checks use a shared table resource; the second
	// writer cannot commit a write-skew decision while the first holds it.
	require.NoError(t, locks.AcquireLock(20, 2, 0, 0, LOCK_X))
	require.ErrorIs(t, locks.AcquireLock(21, 2, 0, 0, LOCK_X), ErrLockConflict)
	locks.ReleaseLocks(20)
	require.NoError(t, locks.AcquireLock(21, 2, 0, 0, LOCK_X))
	locks.ReleaseLocks(21)
}
