package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// PhysicalBackupFormatVersion identifies the on-disk physical snapshot
// format. The format is intentionally XMySQL-specific; it is not an InnoDB
// file interchange format for the upstream MySQL server.
const PhysicalBackupFormatVersion = 1

const physicalBackupManifestPath = ".xmysql/manifest.json"

// PhysicalBackupOptions controls creation of a stopped/quiesced data-dir
// snapshot. Sync is supplied by the engine layer so this package does not
// depend on the storage implementation.
type PhysicalBackupOptions struct {
	SourceDir   string
	ArchivePath string
	Sync        func() error
}

// PhysicalBackupManifest is the integrity manifest stored inside every
// physical backup archive.
type PhysicalBackupManifest struct {
	Version   int                   `json:"version"`
	CreatedAt time.Time             `json:"created_at"`
	SourceDir string                `json:"source_dir"`
	Entries   []PhysicalBackupEntry `json:"entries"`
}

// PhysicalBackupEntry describes one regular file in the source data
// directory. Paths are slash-separated and always relative to SourceDir.
type PhysicalBackupEntry struct {
	Path   string `json:"path"`
	Type   string `json:"type"`
	Mode   uint32 `json:"mode"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// CreatePhysicalBackup creates a versioned gzip/tar physical snapshot. The
// archive is written to a sibling temporary file and renamed only after all
// files, hashes, and the archive stream have been verified.
func CreatePhysicalBackup(ctx context.Context, options PhysicalBackupOptions) (PhysicalBackupManifest, error) {
	if err := contextError(ctx); err != nil {
		return PhysicalBackupManifest{}, err
	}
	sourceDir, err := filepath.Abs(filepath.Clean(strings.TrimSpace(options.SourceDir)))
	if err != nil || sourceDir == "." || strings.TrimSpace(options.SourceDir) == "" {
		return PhysicalBackupManifest{}, fmt.Errorf("invalid physical backup source directory")
	}
	info, err := os.Stat(sourceDir)
	if err != nil {
		return PhysicalBackupManifest{}, fmt.Errorf("stat physical backup source: %w", err)
	}
	if !info.IsDir() {
		return PhysicalBackupManifest{}, fmt.Errorf("physical backup source is not a directory")
	}
	archivePath, err := filepath.Abs(filepath.Clean(strings.TrimSpace(options.ArchivePath)))
	if err != nil || strings.TrimSpace(options.ArchivePath) == "" {
		return PhysicalBackupManifest{}, fmt.Errorf("invalid physical backup archive path")
	}
	if pathWithin(sourceDir, archivePath) {
		return PhysicalBackupManifest{}, fmt.Errorf("physical backup archive is inside source directory")
	}
	if _, err := os.Stat(archivePath); err == nil {
		return PhysicalBackupManifest{}, fmt.Errorf("physical backup archive already exists")
	} else if !os.IsNotExist(err) {
		return PhysicalBackupManifest{}, fmt.Errorf("stat physical backup archive: %w", err)
	}
	if options.Sync != nil {
		if err := options.Sync(); err != nil {
			return PhysicalBackupManifest{}, err
		}
	}

	entries, err := collectPhysicalBackupEntries(ctx, sourceDir)
	if err != nil {
		return PhysicalBackupManifest{}, err
	}
	manifest := PhysicalBackupManifest{
		Version:   PhysicalBackupFormatVersion,
		CreatedAt: time.Now().UTC(),
		SourceDir: sourceDir,
		Entries:   entries,
	}

	parent := filepath.Dir(archivePath)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return PhysicalBackupManifest{}, fmt.Errorf("create physical backup parent: %w", err)
	}
	tmp, err := os.CreateTemp(parent, ".xmysql-physical-backup-*.tmp")
	if err != nil {
		return PhysicalBackupManifest{}, fmt.Errorf("create physical backup staging file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := writePhysicalBackupArchive(ctx, tmp, sourceDir, manifest); err != nil {
		_ = tmp.Close()
		return PhysicalBackupManifest{}, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return PhysicalBackupManifest{}, fmt.Errorf("sync physical backup archive: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return PhysicalBackupManifest{}, fmt.Errorf("close physical backup archive: %w", err)
	}
	if err := os.Rename(tmpPath, archivePath); err != nil {
		return PhysicalBackupManifest{}, fmt.Errorf("publish physical backup archive: %w", err)
	}
	return manifest, nil
}

func collectPhysicalBackupEntries(ctx context.Context, sourceDir string) ([]PhysicalBackupEntry, error) {
	entries := make([]PhysicalBackupEntry, 0)
	err := filepath.WalkDir(sourceDir, func(path string, dirEntry os.DirEntry, walkErr error) error {
		if err := contextError(ctx); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if path == sourceDir {
			return nil
		}
		info, err := dirEntry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("physical backup source contains symbolic link: %s", path)
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("physical backup source contains unsupported file: %s", path)
		}
		rel, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		checksum, size, err := hashPhysicalFile(path)
		if err != nil {
			return err
		}
		entries = append(entries, PhysicalBackupEntry{
			Path: filepath.ToSlash(rel), Type: "file", Mode: uint32(info.Mode().Perm()),
			Size: size, SHA256: checksum,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("collect physical backup files: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, nil
}

func writePhysicalBackupArchive(ctx context.Context, output io.Writer, sourceDir string, manifest PhysicalBackupManifest) error {
	gzipWriter := gzip.NewWriter(output)
	tarWriter := tar.NewWriter(gzipWriter)
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("encode physical backup manifest: %w", err)
	}
	if err := tarWriter.WriteHeader(&tar.Header{Name: physicalBackupManifestPath, Mode: 0600, Size: int64(len(manifestBytes)), Typeflag: tar.TypeReg, ModTime: manifest.CreatedAt}); err != nil {
		return fmt.Errorf("write physical backup manifest header: %w", err)
	}
	if _, err := tarWriter.Write(manifestBytes); err != nil {
		return fmt.Errorf("write physical backup manifest: %w", err)
	}
	for _, entry := range manifest.Entries {
		if err := contextError(ctx); err != nil {
			return err
		}
		path := filepath.Join(sourceDir, filepath.FromSlash(entry.Path))
		if !pathWithin(sourceDir, path) {
			return fmt.Errorf("physical backup entry escapes source directory: %s", entry.Path)
		}
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open physical backup file %s: %w", entry.Path, err)
		}
		if err := tarWriter.WriteHeader(&tar.Header{Name: entry.Path, Mode: int64(entry.Mode), Size: entry.Size, Typeflag: tar.TypeReg, ModTime: manifest.CreatedAt}); err != nil {
			_ = file.Close()
			return fmt.Errorf("write physical backup file header %s: %w", entry.Path, err)
		}
		hash := sha256.New()
		written, copyErr := io.Copy(io.MultiWriter(tarWriter, hash), file)
		closeErr := file.Close()
		if copyErr != nil {
			return fmt.Errorf("copy physical backup file %s: %w", entry.Path, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close physical backup file %s: %w", entry.Path, closeErr)
		}
		if written != entry.Size || hex.EncodeToString(hash.Sum(nil)) != entry.SHA256 {
			return fmt.Errorf("physical backup source changed while reading: %s", entry.Path)
		}
	}
	if err := tarWriter.Close(); err != nil {
		return fmt.Errorf("close physical backup tar: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return fmt.Errorf("close physical backup gzip: %w", err)
	}
	return nil
}

// VerifyPhysicalBackup validates the manifest, archive paths, file sizes and
// SHA-256 checksums without writing anything to the data directory.
func VerifyPhysicalBackup(archivePath string) (PhysicalBackupManifest, error) {
	return inspectPhysicalBackup(context.Background(), archivePath, func(entry PhysicalBackupEntry, reader io.Reader) error {
		hash := sha256.New()
		size, err := io.Copy(hash, reader)
		if err != nil {
			return err
		}
		if size != entry.Size || hex.EncodeToString(hash.Sum(nil)) != entry.SHA256 {
			return fmt.Errorf("physical backup checksum mismatch: %s", entry.Path)
		}
		return nil
	})
}

// RestorePhysicalBackup validates and atomically extracts a physical backup
// into a new target directory. Existing non-empty directories are rejected.
func RestorePhysicalBackup(ctx context.Context, archivePath, targetDir string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	targetDir, err := filepath.Abs(filepath.Clean(strings.TrimSpace(targetDir)))
	if err != nil || strings.TrimSpace(targetDir) == "" {
		return fmt.Errorf("invalid physical backup restore directory")
	}
	if info, statErr := os.Stat(targetDir); statErr == nil {
		if !info.IsDir() {
			return fmt.Errorf("physical backup restore target is not a directory")
		}
		contents, readErr := os.ReadDir(targetDir)
		if readErr != nil {
			return fmt.Errorf("inspect physical backup restore target: %w", readErr)
		}
		if len(contents) > 0 {
			return fmt.Errorf("physical backup restore target is not empty")
		}
		return fmt.Errorf("physical backup restore target already exists")
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("stat physical backup restore target: %w", statErr)
	}
	parent := filepath.Dir(targetDir)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return fmt.Errorf("create physical backup restore parent: %w", err)
	}
	stage, err := os.MkdirTemp(parent, ".xmysql-physical-restore-*")
	if err != nil {
		return fmt.Errorf("create physical backup restore staging directory: %w", err)
	}
	defer os.RemoveAll(stage)
	_, err = inspectPhysicalBackup(ctx, archivePath, func(entry PhysicalBackupEntry, reader io.Reader) error {
		path := filepath.Join(stage, filepath.FromSlash(entry.Path))
		if !pathWithin(stage, path) {
			return fmt.Errorf("physical backup entry escapes restore directory: %s", entry.Path)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(entry.Mode)&07777)
		if err != nil {
			return fmt.Errorf("create restored file %s: %w", entry.Path, err)
		}
		hash := sha256.New()
		written, copyErr := io.Copy(file, io.TeeReader(reader, hash))
		syncErr := file.Sync()
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if syncErr != nil {
			return syncErr
		}
		if closeErr != nil {
			return closeErr
		}
		if written != entry.Size || hex.EncodeToString(hash.Sum(nil)) != entry.SHA256 {
			return fmt.Errorf("physical backup checksum mismatch: %s", entry.Path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := os.Rename(stage, targetDir); err != nil {
		return fmt.Errorf("publish physical backup restore: %w", err)
	}
	return nil
}

func inspectPhysicalBackup(ctx context.Context, archivePath string, onFile func(PhysicalBackupEntry, io.Reader) error) (PhysicalBackupManifest, error) {
	archive, err := os.Open(archivePath)
	if err != nil {
		return PhysicalBackupManifest{}, fmt.Errorf("open physical backup archive: %w", err)
	}
	defer archive.Close()
	gzipReader, err := gzip.NewReader(archive)
	if err != nil {
		return PhysicalBackupManifest{}, fmt.Errorf("open physical backup gzip: %w", err)
	}
	defer gzipReader.Close()
	tarReader := tar.NewReader(gzipReader)
	header, err := tarReader.Next()
	if err != nil {
		return PhysicalBackupManifest{}, fmt.Errorf("read physical backup manifest: %w", err)
	}
	if header.Name != physicalBackupManifestPath || header.Typeflag != tar.TypeReg {
		return PhysicalBackupManifest{}, fmt.Errorf("physical backup manifest must be the first regular archive entry")
	}
	manifestBytes, err := io.ReadAll(tarReader)
	if err != nil {
		return PhysicalBackupManifest{}, fmt.Errorf("read physical backup manifest: %w", err)
	}
	var manifest PhysicalBackupManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return PhysicalBackupManifest{}, fmt.Errorf("decode physical backup manifest: %w", err)
	}
	if err := validatePhysicalBackupManifest(manifest); err != nil {
		return PhysicalBackupManifest{}, err
	}
	expected := make(map[string]PhysicalBackupEntry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		expected[entry.Path] = entry
	}
	seen := make(map[string]bool, len(expected))
	for {
		if err := contextError(ctx); err != nil {
			return PhysicalBackupManifest{}, err
		}
		header, err = tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return PhysicalBackupManifest{}, fmt.Errorf("read physical backup entry: %w", err)
		}
		if header.Name == physicalBackupManifestPath {
			return PhysicalBackupManifest{}, fmt.Errorf("physical backup contains duplicate manifest")
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != 0 {
			return PhysicalBackupManifest{}, fmt.Errorf("physical backup contains unsupported archive entry: %s", header.Name)
		}
		if err := validatePhysicalBackupPath(header.Name); err != nil {
			return PhysicalBackupManifest{}, err
		}
		entry, ok := expected[header.Name]
		if !ok {
			return PhysicalBackupManifest{}, fmt.Errorf("physical backup contains unexpected file: %s", header.Name)
		}
		if seen[header.Name] {
			return PhysicalBackupManifest{}, fmt.Errorf("physical backup contains duplicate file: %s", header.Name)
		}
		if header.Size != entry.Size {
			return PhysicalBackupManifest{}, fmt.Errorf("physical backup size mismatch: %s", header.Name)
		}
		if onFile != nil {
			if err := onFile(entry, tarReader); err != nil {
				return PhysicalBackupManifest{}, err
			}
		}
		seen[header.Name] = true
	}
	// tar reaches its logical end before gzip necessarily reads the member
	// footer. Drain the decompressor so its CRC and uncompressed size checks
	// are actually evaluated for VerifyPhysicalBackup and restore.
	if _, err := io.Copy(io.Discard, gzipReader); err != nil {
		return PhysicalBackupManifest{}, fmt.Errorf("verify physical backup gzip checksum: %w", err)
	}
	for path := range expected {
		if !seen[path] {
			return PhysicalBackupManifest{}, fmt.Errorf("physical backup is missing file: %s", path)
		}
	}
	return manifest, nil
}

func validatePhysicalBackupManifest(manifest PhysicalBackupManifest) error {
	if manifest.Version != PhysicalBackupFormatVersion {
		return fmt.Errorf("unsupported physical backup format version: %d", manifest.Version)
	}
	seen := make(map[string]struct{}, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		if entry.Type != "file" || entry.Size < 0 || len(entry.SHA256) != sha256.Size*2 {
			return fmt.Errorf("invalid physical backup manifest entry: %s", entry.Path)
		}
		if _, err := hex.DecodeString(entry.SHA256); err != nil {
			return fmt.Errorf("invalid physical backup checksum: %s", entry.Path)
		}
		if err := validatePhysicalBackupPath(entry.Path); err != nil {
			return err
		}
		if _, ok := seen[entry.Path]; ok {
			return fmt.Errorf("duplicate physical backup manifest entry: %s", entry.Path)
		}
		seen[entry.Path] = struct{}{}
	}
	return nil
}

func validatePhysicalBackupPath(path string) error {
	if path == "" || filepath.IsAbs(path) || strings.Contains(path, "\\") {
		return fmt.Errorf("invalid physical backup path: %s", path)
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(path)))
	if clean != path || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("unsafe physical backup path: %s", path)
	}
	return nil
}

func hashPhysicalFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hash.Sum(nil)), size, nil
}

func pathWithin(parent, candidate string) bool {
	rel, err := filepath.Rel(parent, candidate)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
