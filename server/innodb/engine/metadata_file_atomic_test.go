package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataPublishSurvivesOpenReader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "event_log.frm")
	require.NoError(t, os.WriteFile(path, []byte("old-identity"), 0644))
	reader, err := os.Open(path)
	require.NoError(t, err)
	defer reader.Close()

	published := []byte("new-identity")
	require.NoError(t, writeMetadataFileAtomic(path, published))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, published, got)
}
