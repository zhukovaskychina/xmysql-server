package vector

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const defaultMaxVectorSegmentBytes = 1 << 30

var ErrSegmentNotFound = errors.New("vector segment not found")

// SegmentRef identifies an immutable content-addressed HNSW segment file.
type SegmentRef struct {
	ID     string
	Size   uint64
	Digest [sha256.Size]byte
}

// SegmentStore keeps immutable vector index segments under the database data
// directory. It does not publish an index manifest or make DML transactional;
// the engine manifest layer must decide which references are visible.
type SegmentStore struct {
	root       string
	segmentDir string
	maxBytes   int64
	mu         sync.RWMutex
}

// OpenSegmentStore creates or opens a database-local vector segment directory.
// A zero maxBytes selects a 1 GiB per-segment read/write bound.
func OpenSegmentStore(root string, maxBytes int64) (*SegmentStore, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("vector segment store root is empty")
	}
	if maxBytes == 0 {
		maxBytes = defaultMaxVectorSegmentBytes
	}
	if maxBytes < 1 || maxBytes > defaultMaxVectorSegmentBytes {
		return nil, fmt.Errorf("vector segment maxBytes must be between 1 and %d", defaultMaxVectorSegmentBytes)
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve vector segment store root: %w", err)
	}
	if err := os.MkdirAll(absoluteRoot, 0700); err != nil {
		return nil, fmt.Errorf("create vector segment store root: %w", err)
	}
	info, err := os.Lstat(absoluteRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("vector segment store root must be a non-symlink directory")
	}
	segmentDir := filepath.Join(absoluteRoot, "segments")
	if err := os.MkdirAll(segmentDir, 0700); err != nil {
		return nil, fmt.Errorf("create vector segment directory: %w", err)
	}
	segmentInfo, err := os.Lstat(segmentDir)
	if err != nil || !segmentInfo.IsDir() || segmentInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("vector segment directory must be a non-symlink directory")
	}
	return &SegmentStore{root: absoluteRoot, segmentDir: segmentDir, maxBytes: maxBytes}, nil
}

// Put serializes and durably publishes a segment under its SHA-256 identity.
// Repeating the same write is idempotent; an existing file is read and checked
// before its reference is returned.
func (s *SegmentStore) Put(segment *HNSWSegment) (SegmentRef, error) {
	if s == nil || segment == nil {
		return SegmentRef{}, fmt.Errorf("vector segment store and segment are required")
	}
	encoded, err := segment.MarshalBinary()
	if err != nil {
		return SegmentRef{}, err
	}
	if int64(len(encoded)) > s.maxBytes {
		return SegmentRef{}, fmt.Errorf("vector segment exceeds configured maximum of %d bytes", s.maxBytes)
	}
	digest := sha256.Sum256(encoded)
	id := hex.EncodeToString(digest[:])
	finalPath := filepath.Join(s.segmentDir, id+".xmh")
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, readErr := s.readFile(finalPath); readErr == nil {
		if !bytes.Equal(existing, encoded) {
			return SegmentRef{}, fmt.Errorf("content-addressed vector segment collision for %s", id)
		}
		return SegmentRef{ID: id, Size: uint64(len(encoded)), Digest: digest}, nil
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return SegmentRef{}, fmt.Errorf("read existing vector segment: %w", readErr)
	}
	temporary, err := os.CreateTemp(s.segmentDir, ".stage-")
	if err != nil {
		return SegmentRef{}, fmt.Errorf("create vector segment staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return SegmentRef{}, fmt.Errorf("secure vector segment staging file: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return SegmentRef{}, fmt.Errorf("write vector segment: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return SegmentRef{}, fmt.Errorf("sync vector segment: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return SegmentRef{}, fmt.Errorf("close vector segment: %w", err)
	}
	if err := os.Rename(temporaryPath, finalPath); err != nil {
		if existing, readErr := s.readFile(finalPath); readErr == nil && bytes.Equal(existing, encoded) {
			return SegmentRef{ID: id, Size: uint64(len(encoded)), Digest: digest}, nil
		}
		return SegmentRef{}, fmt.Errorf("publish vector segment: %w", err)
	}
	if err := syncDirectory(s.segmentDir); err != nil {
		return SegmentRef{}, fmt.Errorf("sync vector segment directory: %w", err)
	}
	return SegmentRef{ID: id, Size: uint64(len(encoded)), Digest: digest}, nil
}

// Read resolves only a canonical SHA-256 segment ID and verifies both the
// content-address digest and the HNSW segment's own internal checksum.
func (s *SegmentStore) Read(reference SegmentRef) (*HNSWSegment, error) {
	if s == nil {
		return nil, fmt.Errorf("vector segment store is nil")
	}
	decodedID, err := hex.DecodeString(reference.ID)
	if err != nil || len(decodedID) != sha256.Size || hex.EncodeToString(decodedID) != reference.ID {
		return nil, fmt.Errorf("invalid vector segment ID")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	encoded, err := s.readFile(filepath.Join(s.segmentDir, reference.ID+".xmh"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrSegmentNotFound
		}
		return nil, fmt.Errorf("read vector segment: %w", err)
	}
	digest := sha256.Sum256(encoded)
	if hex.EncodeToString(digest[:]) != reference.ID || (reference.Digest != ([sha256.Size]byte{}) && reference.Digest != digest) ||
		(reference.Size != 0 && reference.Size != uint64(len(encoded))) {
		return nil, fmt.Errorf("vector segment reference integrity mismatch")
	}
	var segment HNSWSegment
	if err := segment.UnmarshalBinary(encoded); err != nil {
		return nil, err
	}
	return &segment, nil
}

func (s *SegmentStore) readFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 1 || info.Size() > s.maxBytes {
		return nil, fmt.Errorf("invalid or oversized vector segment file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil || !os.SameFile(info, openedInfo) {
		return nil, fmt.Errorf("vector segment changed while opening")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, s.maxBytes+1))
	if err != nil || int64(len(encoded)) != openedInfo.Size() || int64(len(encoded)) > s.maxBytes {
		return nil, fmt.Errorf("invalid or oversized vector segment file")
	}
	return encoded, nil
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// SweepStaging removes only abandoned writer files from this store's staging
// namespace. Published segments are immutable and are retained until an
// engine-owned manifest and reader-retention policy can prove they are unused.
func (s *SegmentStore) SweepStaging(olderThan time.Duration) (int, error) {
	if s == nil {
		return 0, fmt.Errorf("vector segment store is nil")
	}
	cutoff := time.Now().Add(-olderThan)
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.segmentDir)
	if err != nil {
		return 0, fmt.Errorf("list vector segment directory: %w", err)
	}
	removed := 0
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".stage-") || entry.IsDir() {
			continue
		}
		path := filepath.Join(s.segmentDir, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return removed, err
		}
		if !info.Mode().IsRegular() || info.ModTime().After(cutoff) {
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return removed, err
		}
		removed++
	}
	if removed > 0 {
		if err := syncDirectory(s.segmentDir); err != nil {
			return removed, err
		}
	}
	return removed, nil
}
