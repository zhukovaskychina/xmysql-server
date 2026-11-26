package content

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

const (
	segmentManifestVersion = 1
	DefaultSegmentSize     = 1 << 20
	MaxSegmentSize         = 64 << 20
	DefaultMaxObjectBytes  = uint64(1) << 40
	DefaultInlineThreshold = 8 << 10
	maxTransferHeaderBytes = 64 << 10
)

var objectTransferMagic = [4]byte{'X', 'C', 'T', 1}

var (
	ErrInvalidObjectRef = errors.New("invalid segmented content reference")
	ErrObjectNotFound   = errors.New("segmented content object not found")
	ErrObjectMismatch   = errors.New("segmented content object does not match reference")
	ErrObjectTooLarge   = errors.New("segmented content object exceeds configured limit")
)

// ObjectRef identifies an immutable object in a database-owned segment store.
// It is a storage identity, not an authorization token; callers must authorize
// every read against the owning SQL row and current session privileges.
type ObjectRef struct {
	ID             string
	MIMEType       string
	Revision       uint64
	Size           uint64
	Digest         [sha256.Size]byte
	ManifestDigest [sha256.Size]byte
}

// SegmentStoreOptions configures the private, database-managed content area.
// SegmentSize is bounded so range reads never need to materialize an entire
// object. MaxObjectBytes is enforced while ingesting a stream.
type SegmentStoreOptions struct {
	SegmentSize    uint32
	MaxObjectBytes uint64
}

// SegmentStore stores immutable content beneath a database-owned directory.
// Segments and manifests are published by an atomic directory rename after
// every file has been flushed. It intentionally has no knowledge of SQL
// transactions; the caller must publish references transactionally and later
// reclaim objects that were written but never referenced by committed rows.
type SegmentStore struct {
	root           string
	segmentSize    uint32
	maxObjectBytes uint64
	mu             sync.RWMutex
}

// MaxObjectBytes returns the configured upper bound for one immutable object.
func (s *SegmentStore) MaxObjectBytes() uint64 {
	if s == nil {
		return 0
	}
	return s.maxObjectBytes
}

// CleanupStaging removes unpublished writer directories left by a process
// that terminated before Commit or Abort. Call only during exclusive database
// startup, before accepting client writes.
func (s *SegmentStore) CleanupStaging() (int, error) {
	if s == nil {
		return 0, ErrInvalidObjectRef
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return 0, fmt.Errorf("list CONTENT store for staging cleanup: %w", err)
	}
	removed := 0
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".staging-") || !entry.IsDir() {
			continue
		}
		if err := os.RemoveAll(filepath.Join(s.root, entry.Name())); err != nil {
			return removed, fmt.Errorf("remove abandoned CONTENT staging directory %s: %w", entry.Name(), err)
		}
		removed++
	}
	if removed > 0 {
		if err := syncDirectory(s.root); err != nil {
			return removed, fmt.Errorf("sync CONTENT store after staging cleanup: %w", err)
		}
	}
	return removed, nil
}

// ReclaimUnreferenced removes published objects absent from a complete live
// reference inventory. Use only during exclusive startup or under a database
// snapshot/maintenance gate that excludes writes and readers.
func (s *SegmentStore) ReclaimUnreferenced(ctx context.Context, live map[string]ObjectRef) (int, error) {
	if s == nil {
		return 0, ErrInvalidObjectRef
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return 0, fmt.Errorf("list CONTENT store for object reclamation: %w", err)
	}
	objects := make(map[string]ObjectRef)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		id := entry.Name()
		if strings.HasPrefix(id, ".staging-") || !entry.IsDir() {
			continue
		}
		decodedID, decodeErr := hex.DecodeString(id)
		if decodeErr != nil || len(decodedID) != 16 || hex.EncodeToString(decodedID) != id {
			continue
		}
		manifestPath := filepath.Join(s.root, id, "manifest.json")
		fileInfo, statErr := os.Lstat(manifestPath)
		if statErr != nil {
			return 0, fmt.Errorf("inspect CONTENT object %s manifest: %w", id, statErr)
		}
		if !fileInfo.Mode().IsRegular() {
			return 0, fmt.Errorf("CONTENT object %s manifest is not a regular file", id)
		}
		encoded, readErr := os.ReadFile(manifestPath)
		if readErr != nil {
			return 0, fmt.Errorf("read CONTENT object %s manifest: %w", id, readErr)
		}
		var manifest segmentManifest
		if err := json.Unmarshal(encoded, &manifest); err != nil {
			return 0, fmt.Errorf("decode CONTENT object %s manifest: %w", id, err)
		}
		digestBytes, decodeErr := hex.DecodeString(manifest.Digest)
		if decodeErr != nil || len(digestBytes) != sha256.Size {
			return 0, fmt.Errorf("CONTENT object %s manifest has an invalid digest", id)
		}
		ref := ObjectRef{ID: id, MIMEType: manifest.MIMEType, Revision: manifest.Revision, Size: manifest.Size, ManifestDigest: sha256.Sum256(encoded)}
		copy(ref.Digest[:], digestBytes)
		if !validObjectRef(ref) {
			return 0, fmt.Errorf("CONTENT object %s manifest has an invalid identity", id)
		}
		if _, err := s.readManifest(ref); err != nil {
			return 0, fmt.Errorf("validate CONTENT object %s before reclamation: %w", id, err)
		}
		objects[id] = ref
	}

	for id, expected := range live {
		found, exists := objects[id]
		if !exists {
			return 0, fmt.Errorf("live CONTENT object %s is missing", id)
		}
		if found != expected {
			return 0, fmt.Errorf("live CONTENT object %s does not match its row reference", id)
		}
	}

	staleIDs := make([]string, 0)
	for id := range objects {
		if _, exists := live[id]; !exists {
			staleIDs = append(staleIDs, id)
		}
	}
	sort.Strings(staleIDs)
	for _, id := range staleIDs {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := os.RemoveAll(filepath.Join(s.root, id)); err != nil {
			return 0, fmt.Errorf("remove orphaned CONTENT object %s: %w", id, err)
		}
	}
	if len(staleIDs) > 0 {
		if err := syncDirectory(s.root); err != nil {
			return len(staleIDs), fmt.Errorf("sync CONTENT store after object reclamation: %w", err)
		}
	}
	return len(staleIDs), nil
}

type segmentManifest struct {
	Version  uint16            `json:"version"`
	ID       string            `json:"id"`
	MIMEType string            `json:"mime_type"`
	Revision uint64            `json:"revision"`
	Size     uint64            `json:"size_bytes"`
	Digest   string            `json:"sha256"`
	Segments []segmentMetadata `json:"segments"`
}

type segmentMetadata struct {
	Ordinal uint32 `json:"ordinal"`
	Size    uint32 `json:"size_bytes"`
	Digest  string `json:"sha256"`
}

// SegmentWriter incrementally builds one immutable CONTENT object with at
// most one segment of payload buffered in memory. A writer is single-owner;
// callers must Commit or Abort it exactly once.
type SegmentWriter struct {
	store      *SegmentStore
	ctx        context.Context
	mimeType   string
	revision   uint64
	id         string
	stagingDir string
	payload    []byte
	wholeHash  hash.Hash
	segments   []segmentMetadata
	total      uint64
	failed     error
	closed     bool
}

// NewSegmentStore opens a database-owned immutable content store. The caller
// supplies a trusted directory inside the database data directory, never a
// path received from SQL or a client.
func NewSegmentStore(root string, options SegmentStoreOptions) (*SegmentStore, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("content segment store root is required")
	}
	if options.SegmentSize == 0 {
		options.SegmentSize = DefaultSegmentSize
	}
	if options.SegmentSize > MaxSegmentSize || options.SegmentSize < 4096 {
		return nil, fmt.Errorf("content segment size must be between 4096 and %d bytes", MaxSegmentSize)
	}
	if options.MaxObjectBytes == 0 {
		options.MaxObjectBytes = DefaultMaxObjectBytes
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve content segment root: %w", err)
	}
	if err := os.MkdirAll(absoluteRoot, 0700); err != nil {
		return nil, fmt.Errorf("create content segment root: %w", err)
	}
	rootInfo, err := os.Lstat(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("inspect content segment root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("content segment root must be a real directory, not a symbolic link or file")
	}
	return &SegmentStore{root: absoluteRoot, segmentSize: options.SegmentSize, maxObjectBytes: options.MaxObjectBytes}, nil
}

// Put consumes source with a bounded payload buffer and publishes an immutable
// object only after all segment bytes and the manifest have been flushed.
func (s *SegmentStore) Put(ctx context.Context, mimeType string, revision uint64, source io.Reader) (ObjectRef, error) {
	if source == nil {
		return ObjectRef{}, ErrInvalidObjectRef
	}
	writer, err := s.NewWriter(ctx, mimeType, revision)
	if err != nil {
		return ObjectRef{}, err
	}
	if _, err := io.Copy(writer, source); err != nil {
		writeErr := writer.failed
		_ = writer.Abort()
		if writeErr != nil {
			return ObjectRef{}, writeErr
		}
		return ObjectRef{}, fmt.Errorf("read content stream: %w", err)
	}
	return writer.Commit()
}

// PutVerified incrementally imports one object and publishes it only when its
// computed source digest matches the digest supplied by the transfer
// manifest. The imported object receives a local storage ID; callers must
// store the returned reference in the receiving row image.
func (s *SegmentStore) PutVerified(ctx context.Context, mimeType string, revision uint64, expectedDigest [sha256.Size]byte, source io.Reader) (ObjectRef, error) {
	return s.putVerified(ctx, mimeType, revision, 0, false, expectedDigest, source)
}

func (s *SegmentStore) putVerified(ctx context.Context, mimeType string, revision, expectedSize uint64, checkSize bool, expectedDigest [sha256.Size]byte, source io.Reader) (ObjectRef, error) {
	if source == nil {
		return ObjectRef{}, ErrInvalidObjectRef
	}
	if s == nil {
		return ObjectRef{}, ErrInvalidObjectRef
	}
	if checkSize && expectedSize > s.maxObjectBytes {
		return ObjectRef{}, ErrObjectTooLarge
	}
	writer, err := s.NewWriter(ctx, mimeType, revision)
	if err != nil {
		return ObjectRef{}, err
	}
	if _, err := io.Copy(writer, source); err != nil {
		writeErr := writer.failed
		_ = writer.Abort()
		if writeErr != nil {
			return ObjectRef{}, writeErr
		}
		return ObjectRef{}, fmt.Errorf("read verified CONTENT stream: %w", err)
	}
	if checkSize && writer.total != expectedSize {
		_ = writer.Abort()
		return ObjectRef{}, fmt.Errorf("%w: transferred content length %d does not match the declared size %d", ErrObjectMismatch, writer.total, expectedSize)
	}
	return writer.CommitVerified(expectedDigest)
}

type objectTransferHeader struct {
	Version  uint16 `json:"version"`
	MIMEType string `json:"mime_type"`
	Revision uint64 `json:"revision"`
	Size     uint64 `json:"size_bytes"`
	Digest   string `json:"sha256"`
}

// WriteTransfer writes a versioned CONTENT object frame followed by the exact
// source bytes. The frame is length-delimited and streams the payload without
// materializing the object. The destination must provide its own transport
// authentication and authorization.
func (s *SegmentStore) WriteTransfer(ctx context.Context, ref ObjectRef, destination io.Writer) error {
	if s == nil || destination == nil || !validObjectRef(ref) {
		return ErrInvalidObjectRef
	}
	header, err := json.Marshal(objectTransferHeader{
		Version: 1, MIMEType: ref.MIMEType, Revision: ref.Revision,
		Size: ref.Size, Digest: hex.EncodeToString(ref.Digest[:]),
	})
	if err != nil {
		return fmt.Errorf("encode CONTENT transfer header: %w", err)
	}
	if len(header) == 0 || len(header) > maxTransferHeaderBytes {
		return fmt.Errorf("CONTENT transfer header exceeds %d bytes", maxTransferHeaderBytes)
	}
	framePrefix := make([]byte, 8)
	copy(framePrefix[:4], objectTransferMagic[:])
	binary.BigEndian.PutUint32(framePrefix[4:], uint32(len(header)))
	if err := writeAll(destination, framePrefix); err != nil {
		return fmt.Errorf("write CONTENT transfer prefix: %w", err)
	}
	if err := writeAll(destination, header); err != nil {
		return fmt.Errorf("write CONTENT transfer header: %w", err)
	}
	written, err := s.ReadTo(ctx, ref, destination)
	if err != nil {
		return fmt.Errorf("stream CONTENT transfer payload: %w", err)
	}
	if written != ref.Size {
		return fmt.Errorf("%w: transferred %d of %d CONTENT bytes", ErrObjectMismatch, written, ref.Size)
	}
	return nil
}

// ReadTransfer consumes one length-delimited CONTENT frame, verifies the
// declared size and digest, and publishes a new locally identified object
// only after both checks succeed. It leaves any following frame unread.
func (s *SegmentStore) ReadTransfer(ctx context.Context, source io.Reader) (ObjectRef, error) {
	if s == nil || source == nil {
		return ObjectRef{}, ErrInvalidObjectRef
	}
	var prefix [8]byte
	if _, err := io.ReadFull(source, prefix[:]); err != nil {
		return ObjectRef{}, fmt.Errorf("read CONTENT transfer prefix: %w", err)
	}
	if !bytes.Equal(prefix[:4], objectTransferMagic[:]) {
		return ObjectRef{}, fmt.Errorf("invalid CONTENT transfer frame magic or version")
	}
	headerLength := binary.BigEndian.Uint32(prefix[4:])
	if headerLength == 0 || headerLength > maxTransferHeaderBytes {
		return ObjectRef{}, fmt.Errorf("invalid CONTENT transfer header length %d", headerLength)
	}
	headerBytes := make([]byte, int(headerLength))
	if _, err := io.ReadFull(source, headerBytes); err != nil {
		return ObjectRef{}, fmt.Errorf("read CONTENT transfer header: %w", err)
	}
	var header objectTransferHeader
	decoder := json.NewDecoder(bytes.NewReader(headerBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&header); err != nil {
		return ObjectRef{}, fmt.Errorf("decode CONTENT transfer header: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ObjectRef{}, fmt.Errorf("CONTENT transfer header contains trailing data")
	}
	if header.Version != 1 || header.Revision == 0 {
		return ObjectRef{}, fmt.Errorf("unsupported or invalid CONTENT transfer header")
	}
	digestBytes, err := hex.DecodeString(header.Digest)
	if err != nil || len(digestBytes) != sha256.Size || hex.EncodeToString(digestBytes) != header.Digest {
		return ObjectRef{}, fmt.Errorf("invalid CONTENT transfer SHA-256 digest")
	}
	var digest [sha256.Size]byte
	copy(digest[:], digestBytes)
	if header.Size > math.MaxInt64 {
		return ObjectRef{}, ErrObjectTooLarge
	}
	limited := &io.LimitedReader{R: source, N: int64(header.Size)}
	ref, err := s.putVerified(ctx, header.MIMEType, header.Revision, header.Size, true, digest, limited)
	if err != nil {
		return ObjectRef{}, err
	}
	return ref, nil
}

func writeAll(destination io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := destination.Write(data)
		if written < 0 || written > len(data) {
			return fmt.Errorf("invalid write count %d", written)
		}
		data = data[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// NewWriter opens an incremental object publication. Bytes passed to Write
// are copied into a bounded segment buffer and flushed as each segment fills.
// The returned ObjectRef becomes visible only after Commit succeeds.
func (s *SegmentStore) NewWriter(ctx context.Context, mimeType string, revision uint64) (*SegmentWriter, error) {
	if s == nil || revision == 0 {
		return nil, ErrInvalidObjectRef
	}
	if ctx == nil {
		ctx = context.Background()
	}
	value, err := NewValue(mimeType, revision, nil)
	if err != nil {
		return nil, err
	}
	id, err := newObjectID()
	if err != nil {
		return nil, fmt.Errorf("generate content object ID: %w", err)
	}
	stagingDir, err := os.MkdirTemp(s.root, ".staging-")
	if err != nil {
		return nil, fmt.Errorf("create content staging directory: %w", err)
	}
	return &SegmentWriter{
		store: s, ctx: ctx, mimeType: value.MIMEType, revision: revision,
		id: id, stagingDir: stagingDir, payload: make([]byte, 0, int(s.segmentSize)),
		wholeHash: sha256.New(), segments: make([]segmentMetadata, 0, 1),
	}, nil
}

func (w *SegmentWriter) Write(data []byte) (int, error) {
	if w == nil || w.closed {
		return 0, errors.New("CONTENT segment writer is closed")
	}
	if w.failed != nil {
		return 0, w.failed
	}
	if err := w.ctx.Err(); err != nil {
		w.failed = err
		return 0, err
	}
	if uint64(len(data)) > w.store.maxObjectBytes-w.total {
		w.failed = ErrObjectTooLarge
		return 0, w.failed
	}
	if _, err := w.wholeHash.Write(data); err != nil {
		w.failed = err
		return 0, err
	}
	w.total += uint64(len(data))
	written := 0
	for len(data) > 0 {
		available := int(w.store.segmentSize) - len(w.payload)
		if available > len(data) {
			available = len(data)
		}
		w.payload = append(w.payload, data[:available]...)
		written += available
		data = data[available:]
		if len(w.payload) == int(w.store.segmentSize) {
			if err := w.flushSegment(); err != nil {
				w.failed = err
				return written, err
			}
		}
	}
	return written, nil
}

func (w *SegmentWriter) flushSegment() error {
	if len(w.payload) == 0 {
		return nil
	}
	metadata, err := writeContentSegment(w.stagingDir, uint32(len(w.segments)), w.payload)
	if err != nil {
		return err
	}
	w.segments = append(w.segments, metadata)
	w.payload = w.payload[:0]
	return nil
}

// Commit flushes the final partial segment and atomically publishes the
// completed object. Failed or cancelled writes cannot produce a reference.
func (w *SegmentWriter) Commit() (ObjectRef, error) {
	return w.commit(nil)
}

// CommitVerified rejects a stream whose complete digest differs from the
// transfer manifest before publishing any object directory.
func (w *SegmentWriter) CommitVerified(expectedDigest [sha256.Size]byte) (ObjectRef, error) {
	return w.commit(&expectedDigest)
}

func (w *SegmentWriter) commit(expectedDigest *[sha256.Size]byte) (ObjectRef, error) {
	if w == nil || w.closed {
		return ObjectRef{}, errors.New("CONTENT segment writer is closed")
	}
	if w.failed != nil {
		err := w.failed
		_ = w.Abort()
		return ObjectRef{}, err
	}
	if err := w.ctx.Err(); err != nil {
		_ = w.Abort()
		return ObjectRef{}, err
	}
	var digest [sha256.Size]byte
	copy(digest[:], w.wholeHash.Sum(nil))
	if expectedDigest != nil && digest != *expectedDigest {
		_ = w.Abort()
		return ObjectRef{}, fmt.Errorf("%w: transferred content digest does not match the declared source digest", ErrObjectMismatch)
	}
	if err := w.flushSegment(); err != nil {
		_ = w.Abort()
		return ObjectRef{}, err
	}
	manifest := segmentManifest{
		Version: segmentManifestVersion, ID: w.id, MIMEType: w.mimeType,
		Revision: w.revision, Size: w.total, Digest: hex.EncodeToString(digest[:]), Segments: w.segments,
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		_ = w.Abort()
		return ObjectRef{}, fmt.Errorf("encode content manifest: %w", err)
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	if err := writeSyncedFile(filepath.Join(w.stagingDir, "manifest.json"), manifestBytes, 0600); err != nil {
		_ = w.Abort()
		return ObjectRef{}, fmt.Errorf("write content manifest: %w", err)
	}
	if err := syncDirectory(w.stagingDir); err != nil {
		_ = w.Abort()
		return ObjectRef{}, fmt.Errorf("sync content staging directory: %w", err)
	}
	finalDir := filepath.Join(w.store.root, w.id)
	w.store.mu.Lock()
	if err := os.Rename(w.stagingDir, finalDir); err != nil {
		w.store.mu.Unlock()
		_ = w.Abort()
		return ObjectRef{}, fmt.Errorf("publish content object: %w", err)
	}
	if err := syncDirectory(w.store.root); err != nil {
		_ = os.RemoveAll(finalDir)
		w.store.mu.Unlock()
		w.closed = true
		return ObjectRef{}, fmt.Errorf("sync content store root: %w", err)
	}
	w.store.mu.Unlock()
	w.closed = true
	return ObjectRef{ID: w.id, MIMEType: w.mimeType, Revision: w.revision, Size: w.total, Digest: digest, ManifestDigest: manifestDigest}, nil
}

// Abort discards any unpublished object data. It is idempotent.
func (w *SegmentWriter) Abort() error {
	if w == nil || w.closed {
		return nil
	}
	w.closed = true
	if err := os.RemoveAll(w.stagingDir); err != nil {
		return fmt.Errorf("remove CONTENT staging directory: %w", err)
	}
	return nil
}

// CopyRange streams a validated byte range to dst. Only touched segments are
// read; each is checksum-verified before any of its bytes are returned.
func (s *SegmentStore) CopyRange(ctx context.Context, ref ObjectRef, offset, length uint64, dst io.Writer) (uint64, error) {
	if s == nil || dst == nil || !validObjectRef(ref) {
		return 0, ErrInvalidObjectRef
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if length > math.MaxUint64-offset {
		return 0, fmt.Errorf("content range overflows uint64")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	manifest, err := s.readManifest(ref)
	if err != nil {
		return 0, err
	}
	if offset >= ref.Size || length == 0 {
		return 0, nil
	}
	if remaining := ref.Size - offset; length > remaining {
		length = remaining
	}
	var written uint64
	var whole hash.Hash
	verifyWhole := offset == 0 && length == ref.Size
	if verifyWhole {
		whole = sha256.New()
	}
	position := offset
	for written < length {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		ordinal := position / uint64(s.segmentSize)
		if ordinal >= uint64(len(manifest.Segments)) {
			return written, fmt.Errorf("%w: missing segment %d", ErrObjectMismatch, ordinal)
		}
		segment := manifest.Segments[ordinal]
		if segment.Ordinal != uint32(ordinal) {
			return written, fmt.Errorf("%w: segment sequence mismatch", ErrObjectMismatch)
		}
		segmentBytes, err := s.readSegment(ref.ID, segment)
		if err != nil {
			return written, err
		}
		inSegment := position % uint64(s.segmentSize)
		if inSegment >= uint64(len(segmentBytes)) {
			return written, fmt.Errorf("%w: range exceeds segment %d", ErrObjectMismatch, ordinal)
		}
		available := uint64(len(segmentBytes)) - inSegment
		if available > length-written {
			available = length - written
		}
		part := segmentBytes[int(inSegment):int(inSegment+available)]
		if whole != nil {
			_, _ = whole.Write(part)
		}
		n, err := dst.Write(part)
		written += uint64(n)
		position += uint64(n)
		if err != nil {
			return written, err
		}
		if n != len(part) {
			return written, io.ErrShortWrite
		}
	}
	if whole != nil && !bytes.Equal(whole.Sum(nil), ref.Digest[:]) {
		return written, fmt.Errorf("%w: full object digest mismatch", ErrObjectMismatch)
	}
	return written, nil
}

// ReadRange returns an owned bounded range. Use CopyRange for larger reads.
func (s *SegmentStore) ReadRange(ctx context.Context, ref ObjectRef, offset, length uint64) ([]byte, error) {
	if length > MaxReadRangeBytes {
		return nil, fmt.Errorf("content range length %d exceeds the %d byte limit", length, MaxReadRangeBytes)
	}
	var buffer bytes.Buffer
	buffer.Grow(int(length))
	if _, err := s.CopyRange(ctx, ref, offset, length, &buffer); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// ReadTo streams the complete immutable object to dst while checking every
// segment and the full object digest.
func (s *SegmentStore) ReadTo(ctx context.Context, ref ObjectRef, dst io.Writer) (uint64, error) {
	return s.CopyRange(ctx, ref, 0, ref.Size, dst)
}

// Delete removes an immutable object only when the supplied reference matches
// its authenticated manifest. Transactional callers should invoke deletion
// from a database-owned reclamation process after proving no committed row or
// retained snapshot references the object.
func (s *SegmentStore) Delete(ref ObjectRef) error {
	if s == nil || !validObjectRef(ref) {
		return ErrInvalidObjectRef
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.readManifest(ref); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(s.root, ref.ID)); err != nil {
		return fmt.Errorf("delete content object %s: %w", ref.ID, err)
	}
	return syncDirectory(s.root)
}

func (s *SegmentStore) readManifest(ref ObjectRef) (segmentManifest, error) {
	var manifest segmentManifest
	manifestPath := filepath.Join(s.root, ref.ID, "manifest.json")
	encoded, err := os.ReadFile(manifestPath)
	if errors.Is(err, os.ErrNotExist) {
		return manifest, fmt.Errorf("%w: %s", ErrObjectNotFound, ref.ID)
	}
	if err != nil {
		return manifest, fmt.Errorf("read content manifest: %w", err)
	}
	if sha256.Sum256(encoded) != ref.ManifestDigest {
		return manifest, fmt.Errorf("%w: manifest checksum mismatch", ErrObjectMismatch)
	}
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		return manifest, fmt.Errorf("%w: invalid manifest: %v", ErrObjectMismatch, err)
	}
	if manifest.Version != segmentManifestVersion || manifest.ID != ref.ID ||
		manifest.MIMEType != ref.MIMEType || manifest.Revision != ref.Revision || manifest.Size != ref.Size ||
		manifest.Digest != hex.EncodeToString(ref.Digest[:]) {
		return manifest, ErrObjectMismatch
	}
	if err := validateManifest(manifest, s.segmentSize, s.maxObjectBytes); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func (s *SegmentStore) readSegment(id string, metadata segmentMetadata) ([]byte, error) {
	path := filepath.Join(s.root, id, segmentFileName(metadata.Ordinal))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read content segment %d: %w", metadata.Ordinal, err)
	}
	if uint64(len(data)) != uint64(metadata.Size) {
		return nil, fmt.Errorf("%w: segment %d length mismatch", ErrObjectMismatch, metadata.Ordinal)
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != metadata.Digest {
		return nil, fmt.Errorf("%w: segment %d checksum mismatch", ErrObjectMismatch, metadata.Ordinal)
	}
	return data, nil
}

func writeContentSegment(directory string, ordinal uint32, data []byte) (segmentMetadata, error) {
	path := filepath.Join(directory, segmentFileName(ordinal))
	if err := writeSyncedFile(path, data, 0600); err != nil {
		return segmentMetadata{}, fmt.Errorf("write content segment %d: %w", ordinal, err)
	}
	digest := sha256.Sum256(data)
	return segmentMetadata{Ordinal: ordinal, Size: uint32(len(data)), Digest: hex.EncodeToString(digest[:])}, nil
}

func validateManifest(manifest segmentManifest, segmentSize uint32, maxObjectBytes uint64) error {
	if manifest.Size > maxObjectBytes || manifest.Size > math.MaxInt64 {
		return ErrObjectTooLarge
	}
	if _, err := hex.DecodeString(manifest.Digest); err != nil || len(manifest.Digest) != sha256.Size*2 {
		return fmt.Errorf("%w: invalid object digest", ErrObjectMismatch)
	}
	expectedSegments := uint64(0)
	if manifest.Size > 0 {
		expectedSegments = (manifest.Size + uint64(segmentSize) - 1) / uint64(segmentSize)
	}
	if uint64(len(manifest.Segments)) != expectedSegments {
		return fmt.Errorf("%w: segment count mismatch", ErrObjectMismatch)
	}
	var total uint64
	for index, segment := range manifest.Segments {
		if segment.Ordinal != uint32(index) || segment.Size == 0 || segment.Size > segmentSize ||
			(index < len(manifest.Segments)-1 && segment.Size != segmentSize) {
			return fmt.Errorf("%w: invalid segment %d", ErrObjectMismatch, index)
		}
		if _, err := hex.DecodeString(segment.Digest); err != nil || len(segment.Digest) != sha256.Size*2 {
			return fmt.Errorf("%w: invalid segment digest %d", ErrObjectMismatch, index)
		}
		total += uint64(segment.Size)
	}
	if total != manifest.Size {
		return fmt.Errorf("%w: manifest size mismatch", ErrObjectMismatch)
	}
	return nil
}

func validObjectRef(ref ObjectRef) bool {
	if len(ref.ID) != 32 || ref.MIMEType == "" || ref.Revision == 0 || strings.ContainsAny(ref.MIMEType, "\r\n") {
		return false
	}
	decoded, err := hex.DecodeString(ref.ID)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == ref.ID
}

func newObjectID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func segmentFileName(ordinal uint32) string { return fmt.Sprintf("%08x.seg", ordinal) }

func writeSyncedFile(path string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	n, writeErr := file.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil && runtime.GOOS != "windows" {
		return err
	}
	return nil
}
