package replication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteReplicationFileAtomicPropagatesSyncFailureBeforeRename(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replication-state.json")
	previous := replicationFileSyncHook
	replicationFileSyncHook = func(*os.File) error {
		return errors.New("injected replication state sync failure")
	}
	t.Cleanup(func() { replicationFileSyncHook = previous })

	err := writeReplicationFileAtomic(path, []byte(`{"executed":{}}`))
	require.ErrorContains(t, err, "injected replication state sync failure")
	_, statErr := os.Stat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist)
	_, tempErr := os.Stat(path + ".tmp")
	require.ErrorIs(t, tempErr, os.ErrNotExist)
}
