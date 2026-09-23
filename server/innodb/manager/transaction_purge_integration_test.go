package manager

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionManagerRegistersAndUnregistersReadViewsWithUndoPurger(t *testing.T) {
	tm, err := NewTransactionManager(t.TempDir(), t.TempDir())
	require.NoError(t, err)
	defer tm.Close()
	purger := NewUndoPurger(nil)
	tm.SetUndoPurger(purger)

	trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	require.NoError(t, err)
	require.NotNil(t, trx.ReadView)
	require.Len(t, purger.activeReadViews, 1)
	require.NoError(t, tm.Rollback(trx))
	require.Len(t, purger.activeReadViews, 0)
}

func TestTransactionManagerMovesReadViewsWhenUndoPurgerChanges(t *testing.T) {
	tm, err := NewTransactionManager(t.TempDir(), t.TempDir())
	require.NoError(t, err)
	defer tm.Close()
	oldPurger := NewUndoPurger(nil)
	newPurger := NewUndoPurger(nil)
	tm.SetUndoPurger(oldPurger)

	trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	require.NoError(t, err)
	require.Len(t, oldPurger.activeReadViews, 1)

	tm.SetUndoPurger(newPurger)
	require.Len(t, oldPurger.activeReadViews, 0)
	require.Len(t, newPurger.activeReadViews, 1)

	require.NoError(t, tm.Rollback(trx))
	require.Len(t, newPurger.activeReadViews, 0)
}
