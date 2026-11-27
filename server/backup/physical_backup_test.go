package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPhysicalBackupCreateVerifyAndRestore(t *testing.T) {
	source := t.TempDir()
	archive := filepath.Join(t.TempDir(), "snapshot.xmb")
	restored := filepath.Join(t.TempDir(), "restored")
	require.NoError(t, os.MkdirAll(filepath.Join(source, "app"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(source, "app", "users.frm"), []byte("metadata"), 0640))
	require.NoError(t, os.WriteFile(filepath.Join(source, "ibdata1"), []byte("pages"), 0600))

	called := false
	manifest, err := CreatePhysicalBackup(context.Background(), PhysicalBackupOptions{
		SourceDir:   source,
		ArchivePath: archive,
		Sync: func() error {
			called = true
			return nil
		},
	})
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, PhysicalBackupFormatVersion, manifest.Version)
	require.Len(t, manifest.Entries, 2)
	require.FileExists(t, archive)

	verified, err := VerifyPhysicalBackup(archive)
	require.NoError(t, err)
	require.Equal(t, manifest, verified)

	require.NoError(t, RestorePhysicalBackup(context.Background(), archive, restored))
	require.Equal(t, []byte("metadata"), mustReadFile(t, filepath.Join(restored, "app", "users.frm")))
	require.Equal(t, []byte("pages"), mustReadFile(t, filepath.Join(restored, "ibdata1")))
}

func TestPhysicalBackupRejectsUnsafeSourceAndArchiveTargets(t *testing.T) {
	source := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(source, "data"), []byte("x"), 0600))

	_, err := CreatePhysicalBackup(context.Background(), PhysicalBackupOptions{
		SourceDir:   source,
		ArchivePath: filepath.Join(source, "nested", "snapshot.xmb"),
	})
	require.ErrorContains(t, err, "inside source directory")

	link := filepath.Join(source, "link")
	if err := os.Symlink(filepath.Join(source, "data"), link); err != nil {
		t.Skipf("symlink is unavailable: %v", err)
	}
	_, err = CreatePhysicalBackup(context.Background(), PhysicalBackupOptions{
		SourceDir:   source,
		ArchivePath: filepath.Join(t.TempDir(), "snapshot.xmb"),
	})
	require.ErrorContains(t, err, "symbolic link")
}

func TestPhysicalBackupRejectsFailedSyncAndExistingRestoreTarget(t *testing.T) {
	source := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(source, "data"), []byte("x"), 0600))
	archive := filepath.Join(t.TempDir(), "snapshot.xmb")

	_, err := CreatePhysicalBackup(context.Background(), PhysicalBackupOptions{
		SourceDir:   source,
		ArchivePath: archive,
		Sync:        func() error { return os.ErrPermission },
	})
	require.ErrorIs(t, err, os.ErrPermission)

	_, err = CreatePhysicalBackup(context.Background(), PhysicalBackupOptions{
		SourceDir:   source,
		ArchivePath: archive,
	})
	require.NoError(t, err)
	restored := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(restored, "existing"), []byte("keep"), 0600))
	require.ErrorContains(t, RestorePhysicalBackup(context.Background(), archive, restored), "not empty")
}

func TestPhysicalBackupRejectsCorruptedArchive(t *testing.T) {
	source := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(source, "data"), []byte("x"), 0600))
	archive := filepath.Join(t.TempDir(), "snapshot.xmb")
	_, err := CreatePhysicalBackup(context.Background(), PhysicalBackupOptions{SourceDir: source, ArchivePath: archive})
	require.NoError(t, err)
	raw, err := os.ReadFile(archive)
	require.NoError(t, err)
	raw[len(raw)-1] ^= 0x01
	require.NoError(t, os.WriteFile(archive, raw, 0600))
	_, err = VerifyPhysicalBackup(archive)
	require.Error(t, err)
}

func TestPhysicalBackupRejectsPathTraversalManifest(t *testing.T) {
	archive := filepath.Join(t.TempDir(), "unsafe.xmb")
	manifest := PhysicalBackupManifest{
		Version: PhysicalBackupFormatVersion, CreatedAt: time.Now().UTC(),
		Entries: []PhysicalBackupEntry{{Path: "../evil", Type: "file", SHA256: strings.Repeat("0", 64)}},
	}
	manifestBytes, err := json.Marshal(manifest)
	require.NoError(t, err)
	var encoded bytes.Buffer
	gzipWriter := gzip.NewWriter(&encoded)
	tarWriter := tar.NewWriter(gzipWriter)
	require.NoError(t, tarWriter.WriteHeader(&tar.Header{Name: physicalBackupManifestPath, Typeflag: tar.TypeReg, Size: int64(len(manifestBytes))}))
	_, err = tarWriter.Write(manifestBytes)
	require.NoError(t, err)
	require.NoError(t, tarWriter.Close())
	require.NoError(t, gzipWriter.Close())
	require.NoError(t, os.WriteFile(archive, encoded.Bytes(), 0600))
	_, err = VerifyPhysicalBackup(archive)
	require.ErrorContains(t, err, "unsafe physical backup path")
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
