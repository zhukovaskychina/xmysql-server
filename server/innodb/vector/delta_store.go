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
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
)

const (
	deltaBatchVersion  = 1
	maxDeltaBatchBytes = 128 << 20
	maxDeltaMutations  = 1 << 20
)

var (
	ErrDeltaNotFound   = errors.New("vector delta batch not found")
	ErrStaleDeltaBatch = errors.New("vector delta batch commit sequence is stale")
)

type DeltaRef struct {
	ID     string            `json:"id"`
	Size   uint64            `json:"size"`
	Digest [sha256.Size]byte `json:"digest"`
}

// DeltaMutation is an ordered upsert or tombstone for one immutable entry ID.
// Upsert vectors and delete source references carry exact row-version identity.
type DeltaMutation struct {
	EntryID string
	Delete  bool
	Value   *Value
	Source  content.SourceRef
}

type DeltaBatch struct {
	IndexID          string
	TransactionID    string
	CommitSequence   uint64
	SourceSnapshotID string
	Mutations        []DeltaMutation
}

type deltaBatchEnvelope struct {
	Version int             `json:"version"`
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

type deltaBatchPayload struct {
	IndexID          string              `json:"index_id"`
	TransactionID    string              `json:"transaction_id"`
	CommitSequence   uint64              `json:"commit_sequence"`
	SourceSnapshotID string              `json:"source_snapshot_id"`
	Mutations        []deltaMutationWire `json:"mutations"`
}

type deltaMutationWire struct {
	EntryID string            `json:"entry_id"`
	Delete  bool              `json:"delete"`
	Vector  []byte            `json:"vector,omitempty"`
	Source  content.SourceRef `json:"source"`
}

type DeltaStore struct {
	dir      string
	maxBytes int64
	mu       sync.RWMutex
}

func OpenDeltaStore(root string) (*DeltaStore, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("vector delta store root is empty")
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve vector delta root: %w", err)
	}
	dir := filepath.Join(absoluteRoot, "deltas")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create vector delta directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("vector delta directory must be a non-symlink directory")
	}
	return &DeltaStore{dir: dir, maxBytes: maxDeltaBatchBytes}, nil
}

func (s *DeltaStore) Put(batch DeltaBatch) (DeltaRef, error) {
	if s == nil {
		return DeltaRef{}, fmt.Errorf("vector delta store is nil")
	}
	encoded, err := marshalDeltaBatch(batch)
	if err != nil {
		return DeltaRef{}, err
	}
	if int64(len(encoded)) > s.maxBytes {
		return DeltaRef{}, fmt.Errorf("vector delta batch exceeds %d bytes", s.maxBytes)
	}
	digest := sha256.Sum256(encoded)
	id := hex.EncodeToString(digest[:])
	finalPath := filepath.Join(s.dir, id+".xvd")
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, err := s.readFile(finalPath); err == nil {
		if !bytes.Equal(existing, encoded) {
			return DeltaRef{}, fmt.Errorf("content-addressed vector delta collision for %s", id)
		}
		return DeltaRef{ID: id, Size: uint64(len(encoded)), Digest: digest}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return DeltaRef{}, err
	}
	temporary, err := os.CreateTemp(s.dir, ".stage-")
	if err != nil {
		return DeltaRef{}, err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return DeltaRef{}, err
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return DeltaRef{}, err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return DeltaRef{}, err
	}
	if err := temporary.Close(); err != nil {
		return DeltaRef{}, err
	}
	if err := os.Link(temporaryPath, finalPath); err != nil {
		if existing, readErr := s.readFile(finalPath); readErr == nil && bytes.Equal(existing, encoded) {
			return DeltaRef{ID: id, Size: uint64(len(encoded)), Digest: digest}, nil
		}
		return DeltaRef{}, fmt.Errorf("publish vector delta batch: %w", err)
	}
	if err := syncDirectory(s.dir); err != nil {
		return DeltaRef{}, err
	}
	return DeltaRef{ID: id, Size: uint64(len(encoded)), Digest: digest}, nil
}

func (s *DeltaStore) Read(reference DeltaRef) (DeltaBatch, error) {
	if s == nil {
		return DeltaBatch{}, fmt.Errorf("vector delta store is nil")
	}
	idBytes, err := hex.DecodeString(reference.ID)
	if err != nil || len(idBytes) != sha256.Size || hex.EncodeToString(idBytes) != reference.ID {
		return DeltaBatch{}, fmt.Errorf("invalid vector delta ID")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	encoded, err := s.readFile(filepath.Join(s.dir, reference.ID+".xvd"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return DeltaBatch{}, ErrDeltaNotFound
		}
		return DeltaBatch{}, err
	}
	digest := sha256.Sum256(encoded)
	if hex.EncodeToString(digest[:]) != reference.ID || reference.Digest != ([sha256.Size]byte{}) && reference.Digest != digest || reference.Size != 0 && reference.Size != uint64(len(encoded)) {
		return DeltaBatch{}, fmt.Errorf("vector delta reference integrity mismatch")
	}
	return unmarshalDeltaBatch(encoded)
}

func (s *DeltaStore) readFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 1 || info.Size() > s.maxBytes {
		return nil, fmt.Errorf("invalid or oversized vector delta file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil || !os.SameFile(info, openedInfo) {
		return nil, fmt.Errorf("vector delta changed while opening")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, s.maxBytes+1))
	if err != nil || int64(len(encoded)) != openedInfo.Size() || int64(len(encoded)) > s.maxBytes {
		return nil, fmt.Errorf("invalid or oversized vector delta file")
	}
	return encoded, nil
}

func marshalDeltaBatch(batch DeltaBatch) ([]byte, error) {
	if !validVectorIndexID(batch.IndexID) || strings.TrimSpace(batch.TransactionID) == "" || len(batch.TransactionID) > 256 || batch.CommitSequence == 0 ||
		strings.TrimSpace(batch.SourceSnapshotID) == "" || len(batch.SourceSnapshotID) > 1024 || len(batch.Mutations) == 0 || len(batch.Mutations) > maxDeltaMutations {
		return nil, fmt.Errorf("%w: invalid delta batch identity or mutation count", ErrInvalidSegment)
	}
	payload := deltaBatchPayload{
		IndexID: batch.IndexID, TransactionID: batch.TransactionID, CommitSequence: batch.CommitSequence, SourceSnapshotID: batch.SourceSnapshotID,
		Mutations: make([]deltaMutationWire, len(batch.Mutations)),
	}
	for index, mutation := range batch.Mutations {
		if mutation.EntryID == "" || len(mutation.EntryID) > 65535 {
			return nil, fmt.Errorf("%w: mutation %d has an invalid entry ID", ErrInvalidSegment, index)
		}
		if err := mutation.Source.Validate(); err != nil {
			return nil, fmt.Errorf("%w: mutation %d has an invalid source reference", ErrInvalidSegment, index)
		}
		wire := deltaMutationWire{EntryID: mutation.EntryID, Delete: mutation.Delete, Source: mutation.Source}
		if mutation.Delete {
			if mutation.Value != nil {
				return nil, fmt.Errorf("%w: delete mutation %d cannot include a vector", ErrInvalidSegment, index)
			}
		} else {
			if mutation.Value == nil {
				return nil, fmt.Errorf("%w: upsert mutation %d requires a vector", ErrInvalidSegment, index)
			}
			if err := mutation.Value.Validate(); err != nil || mutation.Value.State != StateReady || mutation.Value.Source == nil || !mutation.Value.Source.Matches(mutation.Source) {
				return nil, fmt.Errorf("%w: upsert mutation %d must contain a READY vector bound to the mutation source", ErrInvalidSegment, index)
			}
			encoded, err := mutation.Value.MarshalBinary()
			if err != nil {
				return nil, err
			}
			wire.Vector = encoded
		}
		payload.Mutations[index] = wire
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(payloadBytes)
	return json.Marshal(deltaBatchEnvelope{Version: deltaBatchVersion, Payload: payloadBytes, SHA256: hex.EncodeToString(digest[:])})
}

func unmarshalDeltaBatch(encoded []byte) (DeltaBatch, error) {
	if len(encoded) < 1 || len(encoded) > maxDeltaBatchBytes {
		return DeltaBatch{}, ErrInvalidSegment
	}
	var envelope deltaBatchEnvelope
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return DeltaBatch{}, fmt.Errorf("decode vector delta batch: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF || envelope.Version != deltaBatchVersion {
		return DeltaBatch{}, fmt.Errorf("invalid vector delta batch version or trailing payload")
	}
	digest := sha256.Sum256(envelope.Payload)
	if !strings.EqualFold(envelope.SHA256, hex.EncodeToString(digest[:])) {
		return DeltaBatch{}, fmt.Errorf("vector delta batch checksum mismatch")
	}
	var payload deltaBatchPayload
	payloadDecoder := json.NewDecoder(bytes.NewReader(envelope.Payload))
	payloadDecoder.DisallowUnknownFields()
	if err := payloadDecoder.Decode(&payload); err != nil {
		return DeltaBatch{}, fmt.Errorf("decode vector delta payload: %w", err)
	}
	if err := payloadDecoder.Decode(new(any)); err != io.EOF {
		return DeltaBatch{}, fmt.Errorf("vector delta payload has trailing data")
	}
	batch := DeltaBatch{
		IndexID: payload.IndexID, TransactionID: payload.TransactionID, CommitSequence: payload.CommitSequence, SourceSnapshotID: payload.SourceSnapshotID,
		Mutations: make([]DeltaMutation, len(payload.Mutations)),
	}
	for index, wire := range payload.Mutations {
		mutation := DeltaMutation{EntryID: wire.EntryID, Delete: wire.Delete, Source: wire.Source}
		if !wire.Delete {
			var value Value
			if err := value.UnmarshalBinary(wire.Vector); err != nil {
				return DeltaBatch{}, fmt.Errorf("decode vector delta mutation %d: %w", index, err)
			}
			mutation.Value = &value
		}
		batch.Mutations[index] = mutation
	}
	if _, err := marshalDeltaBatch(batch); err != nil {
		return DeltaBatch{}, err
	}
	return batch, nil
}
