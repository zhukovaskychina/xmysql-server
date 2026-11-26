package content

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func testSegmentStore(t *testing.T, maxBytes uint64) *SegmentStore {
	t.Helper()
	store, err := NewSegmentStore(t.TempDir(), SegmentStoreOptions{
		SegmentSize:    4096,
		MaxObjectBytes: maxBytes,
	})
	require.NoError(t, err)
	return store
}

func TestSegmentStoreStreamsAndReadsAcrossSegmentBoundaries(t *testing.T) {
	ctx := context.Background()
	store := testSegmentStore(t, 1<<20)
	data := make([]byte, 3*4096+731)
	for i := range data {
		data[i] = byte((i*37 + 11) % 251)
	}

	ref, err := store.Put(ctx, "application/octet-stream", 7, bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, uint64(len(data)), ref.Size)
	require.Equal(t, uint64(7), ref.Revision)
	require.Equal(t, sha256.Sum256(data), ref.Digest)

	for _, read := range []struct {
		offset uint64
		length uint64
	}{
		{offset: 0, length: 37},
		{offset: 4090, length: 32},
		{offset: 8190, length: 4099},
		{offset: uint64(len(data) - 19), length: 64},
		{offset: uint64(len(data)), length: 10},
	} {
		got, err := store.ReadRange(ctx, ref, read.offset, read.length)
		require.NoError(t, err)
		start := int(read.offset)
		if start > len(data) {
			start = len(data)
		}
		end := start + int(read.length)
		if end > len(data) {
			end = len(data)
		}
		require.Equal(t, data[start:end], got)
	}

	var copied bytes.Buffer
	n, err := store.ReadTo(ctx, ref, &copied)
	require.NoError(t, err)
	require.Equal(t, uint64(len(data)), n)
	require.Equal(t, data, copied.Bytes())
}

func TestSegmentStoreRejectsOversizedInputWithoutPublishingObject(t *testing.T) {
	ctx := context.Background()
	store := testSegmentStore(t, 8192)
	_, err := store.Put(ctx, "application/octet-stream", 1, bytes.NewReader(make([]byte, 8193)))
	require.ErrorIs(t, err, ErrObjectTooLarge)

	removed, err := store.CleanupStaging()
	require.NoError(t, err)
	require.Zero(t, removed)

	entries, err := os.ReadDir(store.root)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestSegmentStoreTransferVerifiesAndPreservesSourceIdentity(t *testing.T) {
	ctx := context.Background()
	source := testSegmentStore(t, 1<<20)
	target := testSegmentStore(t, 1<<20)
	data := bytes.Repeat([]byte("multimodal-source\x00"), 900)
	ref, err := source.Put(ctx, "image/png", 3, bytes.NewReader(data))
	require.NoError(t, err)

	var transfer bytes.Buffer
	require.NoError(t, source.WriteTransfer(ctx, ref, &transfer))
	imported, err := target.ReadTransfer(ctx, &transfer)
	require.NoError(t, err)
	require.Equal(t, ref.MIMEType, imported.MIMEType)
	require.Equal(t, ref.Revision, imported.Revision)
	require.Equal(t, ref.Size, imported.Size)
	require.Equal(t, ref.Digest, imported.Digest)

	got, err := target.ReadRange(ctx, imported, 0, imported.Size)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestSegmentStoreReclaimsOnlyObjectsOutsideCompleteLiveSet(t *testing.T) {
	ctx := context.Background()
	store := testSegmentStore(t, 1<<20)
	live, err := store.Put(ctx, "text/plain", 1, bytes.NewBufferString("still referenced"))
	require.NoError(t, err)
	orphan, err := store.Put(ctx, "text/plain", 1, bytes.NewBufferString("orphaned"))
	require.NoError(t, err)

	reclaimed, err := store.ReclaimUnreferenced(ctx, map[string]ObjectRef{live.ID: live})
	require.NoError(t, err)
	require.Equal(t, 1, reclaimed)
	got, err := store.ReadRange(ctx, live, 0, live.Size)
	require.NoError(t, err)
	require.Equal(t, []byte("still referenced"), got)
	_, err = store.ReadRange(ctx, orphan, 0, orphan.Size)
	require.ErrorIs(t, err, ErrObjectNotFound)
}

func TestSegmentStoreCancellationDoesNotPublishPartialObject(t *testing.T) {
	store := testSegmentStore(t, 1<<20)
	ctx, cancel := context.WithCancel(context.Background())
	writer, err := store.NewWriter(ctx, "application/octet-stream", 1)
	require.NoError(t, err)
	_, err = writer.Write(bytes.Repeat([]byte{0x7a}, 4096))
	require.NoError(t, err)
	cancel()
	_, err = writer.Commit()
	require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, ErrInvalidObjectRef), "unexpected commit result: %v", err)
	require.NoError(t, writer.Abort())

	entries, err := os.ReadDir(store.root)
	require.NoError(t, err)
	require.Empty(t, entries)
}
