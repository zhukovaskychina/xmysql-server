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

func TestWriteReplicationFileAtomicSyncsDirectoryAfterRename(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replication-state.json")
	var syncedPath string
	previous := replicationDirectorySyncHook
	replicationDirectorySyncHook = func(path string) error {
		syncedPath = path
		return nil
	}
	t.Cleanup(func() { replicationDirectorySyncHook = previous })

	require.NoError(t, writeReplicationFileAtomic(path, []byte(`{"executed":{}}`)))
	require.Equal(t, filepath.Dir(path), syncedPath)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.JSONEq(t, `{"executed":{}}`, string(raw))
}

func TestWriteReplicationFileAtomicPropagatesDirectorySyncFailureAfterRename(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replication-state.json")
	previous := replicationDirectorySyncHook
	replicationDirectorySyncHook = func(string) error {
		return errors.New("injected replication directory sync failure")
	}
	t.Cleanup(func() { replicationDirectorySyncHook = previous })

	err := writeReplicationFileAtomic(path, []byte(`{"executed":{}}`))
	require.ErrorContains(t, err, "injected replication directory sync failure")
	_, statErr := os.Stat(path)
	require.NoError(t, statErr)
	_, tempErr := os.Stat(path + ".tmp")
	require.ErrorIs(t, tempErr, os.ErrNotExist)
}
