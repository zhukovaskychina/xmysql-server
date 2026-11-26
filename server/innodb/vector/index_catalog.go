package vector

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
)

const (
	indexManifestVersion  = 1
	maxIndexManifestBytes = 16 << 20
)

var (
	ErrIndexManifestConflict = errors.New("vector index manifest generation conflict")
	ErrIndexNotReady         = errors.New("vector index is not ready for search")
)

type IndexFreshness string

const (
	IndexFreshnessReady   IndexFreshness = "READY"
	IndexFreshnessDirty   IndexFreshness = "DIRTY"
	IndexFreshnessDropped IndexFreshness = "DROPPED"
)

// IndexManifest is an immutable publication record for a set of compatible
// ANN base segments. The engine must populate SourceSnapshotID from its own
// committed snapshot and must reconcile row revisions, deltas and tombstones
// before reporting the index as current.
type IndexManifest struct {
	IndexID                string         `json:"index_id"`
	IndexName              string         `json:"index_name,omitempty"`
	Generation             uint64         `json:"generation"`
	Schema                 string         `json:"schema"`
	Table                  string         `json:"table"`
	Column                 string         `json:"column"`
	SpaceID                string         `json:"space_id"`
	Dimension              uint32         `json:"dimension"`
	Metric                 Metric         `json:"metric"`
	Freshness              IndexFreshness `json:"freshness,omitempty"`
	SourceSnapshotID       string         `json:"source_snapshot_id"`
	AppliedCommitSequence  uint64         `json:"applied_commit_sequence"`
	LastTransactionID      string         `json:"last_transaction_id,omitempty"`
	LastTransactionBatchID string         `json:"last_transaction_batch_id,omitempty"`
	Segments               []SegmentRef   `json:"segments"`
	Deltas                 []DeltaRef     `json:"deltas,omitempty"`
}

type indexManifestEnvelope struct {
	Version int             `json:"version"`
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

// IndexCatalog stores immutable manifest generations next to vector segment
// files under one database-local directory. Generations are never overwritten;
// publishing the final filename with a same-directory hard link makes the
// complete manifest visible atomically and also provides a cross-process
// create-if-absent CAS boundary on filesystems that support hard links.
type IndexCatalog struct {
	root         string
	manifestDir  string
	segments     *SegmentStore
	deltas       *DeltaStore
	mu           sync.RWMutex
	visibilityMu sync.RWMutex
}

func OpenIndexCatalog(root string, maxSegmentBytes int64) (*IndexCatalog, error) {
	segments, err := OpenSegmentStore(root, maxSegmentBytes)
	if err != nil {
		return nil, err
	}
	manifestDir := filepath.Join(segments.root, "indexes")
	if err := os.MkdirAll(manifestDir, 0700); err != nil {
		return nil, fmt.Errorf("create vector index manifest directory: %w", err)
	}
	info, err := os.Lstat(manifestDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("vector index manifest directory must be a non-symlink directory")
	}
	deltas, err := OpenDeltaStore(segments.root)
	if err != nil {
		return nil, err
	}
	return &IndexCatalog{root: segments.root, manifestDir: manifestDir, segments: segments, deltas: deltas}, nil
}

// AcquireSearchFence pins vector search visibility while a SQL table function
// reads the manifest, base segments, deltas, and source rows. A transaction
// commit fence prevents readers from seeing the interval between the physical
// row commit and its derived-index publication within this database process.
func (c *IndexCatalog) AcquireSearchFence() func() {
	if c == nil {
		return func() {}
	}
	c.visibilityMu.RLock()
	return c.visibilityMu.RUnlock
}

// AcquireCommitFence serializes physical commits that change VECTOR-bearing
// rows with vector searches until post-commit deltas or DIRTY fences publish.
func (c *IndexCatalog) AcquireCommitFence() func() {
	if c == nil {
		return func() {}
	}
	c.visibilityMu.Lock()
	return c.visibilityMu.Unlock
}

func (c *IndexCatalog) SegmentStore() *SegmentStore {
	if c == nil {
		return nil
	}
	return c.segments
}

func (c *IndexCatalog) DeltaStore() *DeltaStore {
	if c == nil {
		return nil
	}
	return c.deltas
}

// ReadLatest loads the highest published generation. If that generation is
// corrupt, the read fails closed instead of silently falling back to an older
// catalog view.
func (c *IndexCatalog) ReadLatest(indexID string) (IndexManifest, bool, error) {
	if c == nil || !validVectorIndexID(indexID) {
		return IndexManifest{}, false, fmt.Errorf("invalid vector index catalog or ID")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	path, generation, found, err := c.latestPath(indexID)
	if err != nil || !found {
		return IndexManifest{}, found, err
	}
	manifest, err := c.readManifest(path)
	if err != nil {
		return IndexManifest{}, false, err
	}
	if manifest.IndexID != indexID || manifest.Generation != generation {
		return IndexManifest{}, false, fmt.Errorf("vector index manifest filename does not match its payload")
	}
	if err := c.validateManifest(manifest); err != nil {
		return IndexManifest{}, false, err
	}
	if manifest.Freshness == "" {
		// Manifests created before the freshness field was introduced are
		// readable as their original generation; subsequent publications write
		// the explicit state into the immutable payload.
		manifest.Freshness = IndexFreshnessReady
	}
	return cloneIndexManifest(manifest), true, nil
}

// ListLatest returns the newest manifest for every index ID in deterministic
// order. It is used by database-owned maintenance consumers that must discover
// affected indexes from committed table changes without maintaining a second
// process-local index registry.
func (c *IndexCatalog) ListLatest() ([]IndexManifest, error) {
	if c == nil {
		return nil, fmt.Errorf("vector index catalog is nil")
	}
	c.mu.RLock()
	entries, err := os.ReadDir(c.manifestDir)
	c.mu.RUnlock()
	if err != nil {
		return nil, fmt.Errorf("list vector index manifests: %w", err)
	}
	ids := make(map[string]struct{})
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".xmi") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".xmi")
		separator := strings.LastIndexByte(name, '.')
		if separator <= 0 {
			continue
		}
		if _, err := strconv.ParseUint(name[separator+1:], 10, 64); err != nil {
			continue
		}
		indexID := name[:separator]
		if validVectorIndexID(indexID) {
			ids[indexID] = struct{}{}
		}
	}
	ordered := make([]string, 0, len(ids))
	for indexID := range ids {
		ordered = append(ordered, indexID)
	}
	sort.Strings(ordered)
	manifests := make([]IndexManifest, 0, len(ordered))
	for _, indexID := range ordered {
		manifest, found, err := c.ReadLatest(indexID)
		if err != nil {
			return nil, fmt.Errorf("read latest vector index %s: %w", indexID, err)
		}
		if found {
			manifests = append(manifests, manifest)
		}
	}
	return manifests, nil
}

func (c *IndexCatalog) readLatestManifestHeader(indexID string) (IndexManifest, bool, error) {
	if c == nil || !validVectorIndexID(indexID) {
		return IndexManifest{}, false, fmt.Errorf("invalid vector index catalog or ID")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	path, generation, found, err := c.latestPath(indexID)
	if err != nil || !found {
		return IndexManifest{}, found, err
	}
	manifest, err := c.readManifest(path)
	if err != nil {
		return IndexManifest{}, false, err
	}
	if manifest.IndexID != indexID || manifest.Generation != generation {
		return IndexManifest{}, false, fmt.Errorf("vector index manifest filename does not match its payload")
	}
	if err := validateManifestHeader(manifest); err != nil {
		return IndexManifest{}, false, err
	}
	if manifest.Freshness == "" {
		manifest.Freshness = IndexFreshnessReady
	}
	return cloneIndexManifest(manifest), true, nil
}

// Publish appends a complete immutable manifest generation if expectedGeneration
// is still current. Segment references are integrity-checked and all referenced
// graphs must match the manifest's metric, dimension and shared model space.
func (c *IndexCatalog) Publish(expectedGeneration uint64, manifest IndexManifest) (IndexManifest, error) {
	return c.publish(expectedGeneration, manifest, false)
}

// PublishRebuilt atomically installs a complete new base snapshot and resets
// its delta watermark. It is the only publication path allowed to move the
// sequence back to zero because the replacement base itself contains the
// authoritative rows through the rebuild snapshot.
func (c *IndexCatalog) PublishRebuilt(expectedGeneration uint64, manifest IndexManifest) (IndexManifest, error) {
	if manifest.Freshness != "" && manifest.Freshness != IndexFreshnessReady {
		return IndexManifest{}, fmt.Errorf("rebuilt vector index must publish a READY generation")
	}
	manifest.Freshness = IndexFreshnessReady
	if manifest.AppliedCommitSequence != 0 || manifest.LastTransactionID != "" || manifest.LastTransactionBatchID != "" || len(manifest.Deltas) != 0 {
		return IndexManifest{}, fmt.Errorf("rebuilt vector index must start with an empty delta watermark")
	}
	return c.publish(expectedGeneration, manifest, true)
}

func (c *IndexCatalog) publish(expectedGeneration uint64, manifest IndexManifest, rebuilt bool) (IndexManifest, error) {
	if c == nil || !validVectorIndexID(manifest.IndexID) {
		return IndexManifest{}, fmt.Errorf("invalid vector index catalog or ID")
	}
	if manifest.Freshness == "" {
		manifest.Freshness = IndexFreshnessReady
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	currentPath, currentGeneration, found, err := c.latestPath(manifest.IndexID)
	if err != nil {
		return IndexManifest{}, err
	}
	if !found {
		currentGeneration = 0
	}
	if currentGeneration != expectedGeneration {
		return IndexManifest{}, ErrIndexManifestConflict
	}
	if found {
		current, readErr := c.readManifest(currentPath)
		if readErr != nil {
			return IndexManifest{}, readErr
		}
		if current.IndexID != manifest.IndexID || current.Generation != currentGeneration {
			return IndexManifest{}, fmt.Errorf("current vector index manifest filename does not match its payload")
		}
		if readErr := c.validateManifest(current); readErr != nil {
			return IndexManifest{}, readErr
		}
		if !rebuilt && (manifest.AppliedCommitSequence < current.AppliedCommitSequence ||
			manifest.AppliedCommitSequence == current.AppliedCommitSequence &&
				(manifest.LastTransactionID != current.LastTransactionID || manifest.LastTransactionBatchID != current.LastTransactionBatchID)) {
			return IndexManifest{}, ErrStaleDeltaBatch
		}
		if !rebuilt && manifest.AppliedCommitSequence > current.AppliedCommitSequence {
			if manifest.LastTransactionID == "" || manifest.LastTransactionBatchID == "" {
				return IndexManifest{}, fmt.Errorf("advancing commit sequence requires a transaction and delta batch identity")
			}
			foundBatch := false
			for _, reference := range manifest.Deltas {
				if reference.ID != manifest.LastTransactionBatchID {
					continue
				}
				batch, readErr := c.deltas.Read(reference)
				if readErr != nil {
					return IndexManifest{}, readErr
				}
				foundBatch = batch.TransactionID == manifest.LastTransactionID && batch.CommitSequence == manifest.AppliedCommitSequence
				break
			}
			if !foundBatch {
				return IndexManifest{}, fmt.Errorf("advancing commit sequence requires its delta batch in the published manifest")
			}
		}
	} else if manifest.AppliedCommitSequence != 0 || manifest.LastTransactionID != "" || manifest.LastTransactionBatchID != "" {
		return IndexManifest{}, fmt.Errorf("initial vector index manifest cannot claim applied commits")
	}
	if expectedGeneration == ^uint64(0) || manifest.Generation != expectedGeneration+1 {
		return IndexManifest{}, fmt.Errorf("vector index manifest generation must increment by one")
	}
	if err := c.validateManifest(manifest); err != nil {
		return IndexManifest{}, err
	}
	if err := c.writeManifestGeneration(manifest); err != nil {
		return IndexManifest{}, err
	}
	return cloneIndexManifest(manifest), nil
}

func (c *IndexCatalog) writeManifestGeneration(manifest IndexManifest) error {
	encoded, err := marshalIndexManifest(manifest)
	if err != nil {
		return err
	}
	if len(encoded) > maxIndexManifestBytes {
		return fmt.Errorf("vector index manifest exceeds %d bytes", maxIndexManifestBytes)
	}
	finalPath := filepath.Join(c.manifestDir, vectorManifestName(manifest.IndexID, manifest.Generation))
	temporary, err := os.CreateTemp(c.manifestDir, ".stage-")
	if err != nil {
		return fmt.Errorf("create vector manifest staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Link(temporaryPath, finalPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrIndexManifestConflict
		}
		return fmt.Errorf("atomically publish vector index manifest: %w", err)
	}
	if err := syncDirectory(c.manifestDir); err != nil {
		return fmt.Errorf("sync vector index manifest directory: %w", err)
	}
	return nil
}

// AppendDelta persists a source-bound mutation batch and publishes it by
// advancing the manifest generation. A transaction ID already present with
// the same content-addressed batch is idempotent; a conflicting replay fails.
func (c *IndexCatalog) AppendDelta(expectedGeneration uint64, batch DeltaBatch) (IndexManifest, error) {
	if c == nil {
		return IndexManifest{}, fmt.Errorf("vector index catalog is nil")
	}
	reference, err := c.deltas.Put(batch)
	if err != nil {
		return IndexManifest{}, err
	}
	manifest, found, err := c.ReadLatest(batch.IndexID)
	if err != nil {
		return IndexManifest{}, err
	}
	if !found {
		return IndexManifest{}, fmt.Errorf("vector index %q is not published", batch.IndexID)
	}
	if manifest.Freshness != IndexFreshnessReady {
		return IndexManifest{}, ErrIndexNotReady
	}
	for _, existingRef := range manifest.Deltas {
		existing, readErr := c.deltas.Read(existingRef)
		if readErr != nil {
			return IndexManifest{}, readErr
		}
		if existing.TransactionID != batch.TransactionID {
			continue
		}
		if existingRef.ID == reference.ID {
			return manifest, nil
		}
		return IndexManifest{}, fmt.Errorf("transaction %q already published with different vector mutations", batch.TransactionID)
	}
	if batch.CommitSequence == manifest.AppliedCommitSequence && batch.TransactionID == manifest.LastTransactionID && reference.ID == manifest.LastTransactionBatchID {
		return manifest, nil
	}
	if batch.CommitSequence <= manifest.AppliedCommitSequence {
		return IndexManifest{}, ErrStaleDeltaBatch
	}
	if manifest.Generation != expectedGeneration {
		return IndexManifest{}, ErrIndexManifestConflict
	}
	if manifest.Generation == ^uint64(0) {
		return IndexManifest{}, fmt.Errorf("vector index manifest generation exhausted")
	}
	manifest.Generation++
	manifest.SourceSnapshotID = batch.SourceSnapshotID
	manifest.AppliedCommitSequence = batch.CommitSequence
	manifest.LastTransactionID = batch.TransactionID
	manifest.LastTransactionBatchID = reference.ID
	manifest.Deltas = append(manifest.Deltas, reference)
	return c.Publish(expectedGeneration, manifest)
}

// Compact builds one replacement base segment from the pinned manifest and all
// its exact deltas. Publication uses the observed manifest generation as CAS;
// any concurrent delta commit forces the compaction caller to retry instead of
// dropping that commit. It clears the compacted delta list while preserving the
// applied commit watermark for monotonic future appends. Old generations and
// segment files remain available to readers and are not garbage-collected here.
func (c *IndexCatalog) Compact(indexID string, expectedGeneration uint64, config HNSWConfig) (IndexManifest, error) {
	manifest, found, err := c.ReadLatest(indexID)
	if err != nil {
		return IndexManifest{}, err
	}
	if !found {
		return IndexManifest{}, fmt.Errorf("vector index %q is not published", indexID)
	}
	if manifest.Generation != expectedGeneration {
		return IndexManifest{}, ErrIndexManifestConflict
	}
	if manifest.Freshness != IndexFreshnessReady {
		return IndexManifest{}, ErrIndexNotReady
	}
	mutations, err := c.currentDeltaMutations(manifest)
	if err != nil {
		return IndexManifest{}, err
	}
	entries := make(map[string]HNSWEntry)
	for _, reference := range manifest.Segments {
		segment, err := c.segments.Read(reference)
		if err != nil {
			return IndexManifest{}, err
		}
		for _, node := range segment.nodes {
			if _, changed := mutations[node.entry.ID]; changed {
				continue
			}
			entries[node.entry.ID] = cloneHNSWEntry(node.entry)
		}
	}
	for _, mutation := range mutations {
		if mutation.Delete || mutation.Value == nil {
			continue
		}
		entries[mutation.EntryID] = HNSWEntry{ID: mutation.EntryID, Value: mutation.Value.Clone()}
	}
	ordered := make([]HNSWEntry, 0, len(entries))
	for _, entry := range entries {
		ordered = append(ordered, entry)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	manifest.Generation++
	manifest.Segments = nil
	manifest.Deltas = nil
	if len(ordered) > 0 {
		segment, err := BuildHNSWSegment(ordered, manifest.Metric, config)
		if err != nil {
			return IndexManifest{}, err
		}
		reference, err := c.segments.Put(segment)
		if err != nil {
			return IndexManifest{}, err
		}
		manifest.Segments = []SegmentRef{reference}
	}
	return c.Publish(expectedGeneration, manifest)
}

func (c *IndexCatalog) validateManifest(manifest IndexManifest) error {
	if err := validateManifestHeader(manifest); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(manifest.Segments))
	entryIDs := make(map[string]string)
	entrySources := make(map[string]content.SourceRef)
	for _, reference := range manifest.Segments {
		if _, exists := seen[reference.ID]; exists {
			return fmt.Errorf("%w: duplicate segment %q", ErrInvalidSegment, reference.ID)
		}
		seen[reference.ID] = struct{}{}
		segment, err := c.segments.Read(reference)
		if err != nil {
			return fmt.Errorf("validate vector index segment %q: %w", reference.ID, err)
		}
		if segment.space.ID != manifest.SpaceID || segment.dimension != int(manifest.Dimension) || segment.metric != manifest.Metric {
			return fmt.Errorf("%w: segment %q has incompatible model space, dimension or metric", ErrInvalidSegment, reference.ID)
		}
		for _, node := range segment.nodes {
			if previousSegment, exists := entryIDs[node.entry.ID]; exists {
				return fmt.Errorf("%w: entry %q appears in segments %q and %q", ErrInvalidSegment, node.entry.ID, previousSegment, reference.ID)
			}
			entryIDs[node.entry.ID] = reference.ID
			source := node.entry.Value.Source
			if source == nil || !strings.EqualFold(source.Schema, manifest.Schema) || !strings.EqualFold(source.Table, manifest.Table) {
				return fmt.Errorf("%w: entry %q is bound to a different source table", ErrInvalidSegment, node.entry.ID)
			}
			entrySources[node.entry.ID] = *source
		}
	}
	deltaRefs := make(map[string]struct{}, len(manifest.Deltas))
	transactionIDs := make(map[string]struct{}, len(manifest.Deltas))
	previousCommitSequence := uint64(0)
	lastDelta := DeltaBatch{}
	lastDeltaRef := DeltaRef{}
	for _, reference := range manifest.Deltas {
		if _, exists := deltaRefs[reference.ID]; exists {
			return fmt.Errorf("%w: duplicate delta batch %q", ErrInvalidSegment, reference.ID)
		}
		deltaRefs[reference.ID] = struct{}{}
		batch, err := c.deltas.Read(reference)
		if err != nil {
			return fmt.Errorf("validate vector delta %q: %w", reference.ID, err)
		}
		if batch.IndexID != manifest.IndexID {
			return fmt.Errorf("%w: delta batch belongs to another index", ErrInvalidSegment)
		}
		if batch.CommitSequence <= previousCommitSequence || batch.CommitSequence > manifest.AppliedCommitSequence {
			return fmt.Errorf("%w: delta commit sequence is not increasing", ErrInvalidSegment)
		}
		previousCommitSequence = batch.CommitSequence
		lastDelta, lastDeltaRef = batch, reference
		if _, exists := transactionIDs[batch.TransactionID]; exists {
			return fmt.Errorf("%w: transaction %q appears in multiple delta batches", ErrInvalidSegment, batch.TransactionID)
		}
		transactionIDs[batch.TransactionID] = struct{}{}
		for _, mutation := range batch.Mutations {
			source := mutation.Source
			if !strings.EqualFold(source.Schema, manifest.Schema) || !strings.EqualFold(source.Table, manifest.Table) {
				return fmt.Errorf("%w: delta entry %q is bound to a different source table", ErrInvalidSegment, mutation.EntryID)
			}
			if previous, exists := entrySources[mutation.EntryID]; exists && !previous.Matches(source) {
				return fmt.Errorf("%w: immutable entry ID %q was rebound to another source revision", ErrInvalidSegment, mutation.EntryID)
			}
			entrySources[mutation.EntryID] = source
			if !mutation.Delete && (mutation.Value == nil || mutation.Value.Space.ID != manifest.SpaceID || uint32(mutation.Value.Dimension()) != manifest.Dimension) {
				return fmt.Errorf("%w: delta entry %q has incompatible vector space or dimension", ErrInvalidSegment, mutation.EntryID)
			}
		}
	}
	if len(manifest.Deltas) > 0 && (lastDelta.CommitSequence != manifest.AppliedCommitSequence ||
		lastDelta.TransactionID != manifest.LastTransactionID || lastDeltaRef.ID != manifest.LastTransactionBatchID ||
		lastDelta.SourceSnapshotID != manifest.SourceSnapshotID) {
		return fmt.Errorf("%w: delta tail does not match the published source snapshot watermark", ErrInvalidSegment)
	}
	return nil
}

func validateManifestHeader(manifest IndexManifest) error {
	if !validVectorIndexID(manifest.IndexID) || manifest.Generation == 0 || strings.TrimSpace(manifest.Schema) == "" ||
		strings.TrimSpace(manifest.Table) == "" || strings.TrimSpace(manifest.Column) == "" || strings.TrimSpace(manifest.SpaceID) == "" ||
		manifest.Dimension == 0 || manifest.Dimension > maxDimension || strings.TrimSpace(manifest.SourceSnapshotID) == "" {
		return fmt.Errorf("%w: missing identity, vector space, dimension or source snapshot", ErrInvalidSegment)
	}
	if manifest.Metric != MetricL2 && manifest.Metric != MetricCosine && manifest.Metric != MetricInnerProduct {
		return ErrInvalidMetric
	}
	if manifest.Freshness != "" && manifest.Freshness != IndexFreshnessReady && manifest.Freshness != IndexFreshnessDirty && manifest.Freshness != IndexFreshnessDropped {
		return fmt.Errorf("%w: unsupported index freshness state %q", ErrInvalidSegment, manifest.Freshness)
	}
	if manifest.AppliedCommitSequence == 0 {
		if manifest.LastTransactionID != "" || manifest.LastTransactionBatchID != "" {
			return fmt.Errorf("%w: transaction watermark without an applied commit", ErrInvalidSegment)
		}
	} else {
		batchID, err := hex.DecodeString(manifest.LastTransactionBatchID)
		if err != nil || len(batchID) != sha256.Size || hex.EncodeToString(batchID) != manifest.LastTransactionBatchID || manifest.LastTransactionID == "" {
			return fmt.Errorf("%w: invalid transaction watermark", ErrInvalidSegment)
		}
	}
	return nil
}

// MarkDirty atomically advances the manifest generation and prevents searches
// from using an index after its source may have changed. A failed derived-index
// maintenance hook must leave rows committed while ensuring this state remains
// visible until a full source rebuild publishes a READY generation.
func (c *IndexCatalog) MarkDirty(indexID string) (IndexManifest, error) {
	if c == nil || !validVectorIndexID(indexID) {
		return IndexManifest{}, fmt.Errorf("invalid vector index catalog or ID")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	path, generation, found, err := c.latestPath(indexID)
	if err != nil {
		return IndexManifest{}, err
	}
	if !found {
		return IndexManifest{}, fmt.Errorf("vector index %q is not published", indexID)
	}
	manifest, err := c.readManifest(path)
	if err != nil {
		return IndexManifest{}, err
	}
	if manifest.IndexID != indexID || manifest.Generation != generation {
		return IndexManifest{}, fmt.Errorf("vector index manifest filename does not match its payload")
	}
	if err := validateManifestHeader(manifest); err != nil {
		return IndexManifest{}, err
	}
	if manifest.Freshness == IndexFreshnessDirty || manifest.Freshness == IndexFreshnessDropped {
		return cloneIndexManifest(manifest), nil
	}
	if generation == ^uint64(0) {
		return IndexManifest{}, fmt.Errorf("vector index manifest generation exhausted")
	}
	manifest.Generation = generation + 1
	manifest.Freshness = IndexFreshnessDirty
	if err := c.writeManifestGeneration(manifest); err != nil {
		return IndexManifest{}, err
	}
	return cloneIndexManifest(manifest), nil
}

// MarkDropped atomically publishes a tombstone generation while retaining old
// immutable segment references for readers pinned to earlier generations.
func (c *IndexCatalog) MarkDropped(indexID string) (IndexManifest, error) {
	if c == nil || !validVectorIndexID(indexID) {
		return IndexManifest{}, fmt.Errorf("invalid vector index catalog or ID")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	path, generation, found, err := c.latestPath(indexID)
	if err != nil {
		return IndexManifest{}, err
	}
	if !found {
		return IndexManifest{}, fmt.Errorf("vector index %q is not published", indexID)
	}
	manifest, err := c.readManifest(path)
	if err != nil {
		return IndexManifest{}, err
	}
	if manifest.IndexID != indexID || manifest.Generation != generation {
		return IndexManifest{}, fmt.Errorf("vector index manifest filename does not match its payload")
	}
	if err := validateManifestHeader(manifest); err != nil {
		return IndexManifest{}, err
	}
	if manifest.Freshness == IndexFreshnessDropped {
		return cloneIndexManifest(manifest), nil
	}
	if generation == ^uint64(0) {
		return IndexManifest{}, fmt.Errorf("vector index manifest generation exhausted")
	}
	manifest.Generation = generation + 1
	manifest.Freshness = IndexFreshnessDropped
	if err := c.writeManifestGeneration(manifest); err != nil {
		return IndexManifest{}, err
	}
	return cloneIndexManifest(manifest), nil
}

// MarkTableDirty invalidates every published vector index for a source table.
// It is idempotent and returns the affected index IDs in stable order.
func (c *IndexCatalog) MarkTableDirty(schema, table string) ([]string, error) {
	if c == nil || strings.TrimSpace(schema) == "" || strings.TrimSpace(table) == "" {
		return nil, fmt.Errorf("vector index catalog, schema and table are required")
	}
	entries, err := os.ReadDir(c.manifestDir)
	if err != nil {
		return nil, fmt.Errorf("read vector index manifests: %w", err)
	}
	indexIDs := make(map[string]struct{})
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".xmi") {
			continue
		}
		stem := strings.TrimSuffix(entry.Name(), ".xmi")
		separator := strings.LastIndexByte(stem, '.')
		if separator <= 0 {
			continue
		}
		indexID := stem[:separator]
		if validVectorIndexID(indexID) {
			indexIDs[indexID] = struct{}{}
		}
	}
	ordered := make([]string, 0, len(indexIDs))
	for indexID := range indexIDs {
		ordered = append(ordered, indexID)
	}
	sort.Strings(ordered)
	dirty := make([]string, 0)
	for _, indexID := range ordered {
		manifest, found, err := c.readLatestManifestHeader(indexID)
		if err != nil {
			return dirty, fmt.Errorf("read vector index %s: %w", indexID, err)
		}
		if !found || manifest.Freshness == IndexFreshnessDropped || !strings.EqualFold(manifest.Schema, schema) || !strings.EqualFold(manifest.Table, table) {
			continue
		}
		if _, err := c.MarkDirty(indexID); err != nil {
			return dirty, fmt.Errorf("mark vector index %s dirty: %w", indexID, err)
		}
		dirty = append(dirty, indexID)
	}
	return dirty, nil
}

func (c *IndexCatalog) latestPath(indexID string) (string, uint64, bool, error) {
	entries, err := os.ReadDir(c.manifestDir)
	if err != nil {
		return "", 0, false, fmt.Errorf("list vector index manifests: %w", err)
	}
	prefix := indexID + "."
	var latestPath string
	var latestGeneration uint64
	found := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) || !strings.HasSuffix(entry.Name(), ".xmi") {
			continue
		}
		generationText := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), prefix), ".xmi")
		generation, parseErr := strconv.ParseUint(generationText, 10, 64)
		if parseErr != nil {
			continue
		}
		if !found || generation > latestGeneration {
			latestPath = filepath.Join(c.manifestDir, entry.Name())
			latestGeneration = generation
			found = true
		}
	}
	return latestPath, latestGeneration, found, nil
}

func (c *IndexCatalog) readManifest(path string) (IndexManifest, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return IndexManifest{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 1 || info.Size() > maxIndexManifestBytes {
		return IndexManifest{}, fmt.Errorf("invalid or oversized vector index manifest")
	}
	file, err := os.Open(path)
	if err != nil {
		return IndexManifest{}, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return IndexManifest{}, fmt.Errorf("vector index manifest changed while opening")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maxIndexManifestBytes+1))
	if err != nil || int64(len(encoded)) != opened.Size() || len(encoded) > maxIndexManifestBytes {
		return IndexManifest{}, fmt.Errorf("read vector index manifest: invalid or oversized file")
	}
	var envelope indexManifestEnvelope
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return IndexManifest{}, fmt.Errorf("decode vector index manifest: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return IndexManifest{}, fmt.Errorf("decode vector index manifest: trailing payload")
	}
	if envelope.Version != indexManifestVersion {
		return IndexManifest{}, fmt.Errorf("unsupported vector index manifest version %d", envelope.Version)
	}
	digest := sha256.Sum256(envelope.Payload)
	if !strings.EqualFold(envelope.SHA256, hex.EncodeToString(digest[:])) {
		return IndexManifest{}, fmt.Errorf("vector index manifest checksum mismatch")
	}
	var manifest IndexManifest
	payloadDecoder := json.NewDecoder(bytes.NewReader(envelope.Payload))
	payloadDecoder.DisallowUnknownFields()
	if err := payloadDecoder.Decode(&manifest); err != nil {
		return IndexManifest{}, fmt.Errorf("decode vector index manifest payload: %w", err)
	}
	if err := payloadDecoder.Decode(new(any)); err != io.EOF {
		return IndexManifest{}, fmt.Errorf("decode vector index manifest payload: trailing data")
	}
	return manifest, nil
}

func marshalIndexManifest(manifest IndexManifest) ([]byte, error) {
	payload, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(payload)
	return json.Marshal(indexManifestEnvelope{Version: indexManifestVersion, Payload: payload, SHA256: hex.EncodeToString(digest[:])})
}

func vectorManifestName(indexID string, generation uint64) string {
	return fmt.Sprintf("%s.%020d.xmi", indexID, generation)
}

func validVectorIndexID(indexID string) bool {
	if indexID == "" || len(indexID) > 128 {
		return false
	}
	for _, char := range indexID {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func cloneIndexManifest(manifest IndexManifest) IndexManifest {
	manifest.Segments = append([]SegmentRef(nil), manifest.Segments...)
	return manifest
}
