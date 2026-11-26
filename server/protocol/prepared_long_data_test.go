package protocol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
)

func TestPreparedStatementLongDataAccumulatesAndResets(t *testing.T) {
	mgr := NewPreparedStatementManager()
	stmt, err := mgr.Prepare("insert into blobs (payload) values (?)")
	if err != nil {
		t.Fatal(err)
	}

	if err := mgr.AppendLongData(stmt.ID, 0, []byte("hello ")); err != nil {
		t.Fatal(err)
	}
	if err := mgr.AppendLongData(stmt.ID, 0, []byte("world")); err != nil {
		t.Fatal(err)
	}
	data, err := mgr.ConsumeLongData(stmt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data[0], []byte("hello world")) {
		t.Fatalf("long data = %q, want %q", data[0], "hello world")
	}
	data, err = mgr.ConsumeLongData(stmt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("long data after consume = %#v, want empty", data)
	}
}

func TestPreparedContentLongDataStreamsDirectlyToDatabaseStore(t *testing.T) {
	store, err := content.NewSegmentStore(t.TempDir(), content.SegmentStoreOptions{
		SegmentSize: 4096, MaxObjectBytes: 1 << 20,
	})
	require.NoError(t, err)
	mgr := NewPreparedStatementManager()
	mgr.SetContentSegmentStore(store)
	stmt, err := mgr.Prepare("insert into media (body) values (CONTENT_FROM_BYTES(?, 'image/png'))")
	require.NoError(t, err)
	require.Equal(t, uint16(1), stmt.ParamCount)
	require.Equal(t, "image/png", stmt.ContentParamMIMETypes[0])

	data := bytes.Repeat([]byte{0x89, 0x50, 0x4e, 0x47, 0x00, 0xff}, 3000)
	for start := 0; start < len(data); start += 997 {
		end := start + 997
		if end > len(data) {
			end = len(data)
		}
		require.NoError(t, mgr.AppendLongData(stmt.ID, 0, data[start:end]))
	}
	_, contentValues, refs, err := mgr.ConsumeLongDataForExecution(stmt.ID)
	require.NoError(t, err)
	require.Empty(t, stmt.LongData, "streamed bytes must not accumulate in the inline buffer")
	require.Len(t, refs, 1)
	require.Len(t, contentValues, 1)
	value, exists := contentValues[0]
	require.True(t, exists)
	require.True(t, value.TransientRef)
	require.NotNil(t, value.ExternalRef)
	require.Equal(t, uint64(len(data)), value.Size())
	require.Equal(t, sha256.Sum256(data), value.Digest)

	got, err := value.ReadAllFromStore(context.Background(), store, uint64(len(data)))
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.NoError(t, store.Delete(refs[0]))
}

func TestPreparedContentLongDataResetAbortsUnpublishedWriter(t *testing.T) {
	storeDir := t.TempDir()
	store, err := content.NewSegmentStore(storeDir, content.SegmentStoreOptions{
		SegmentSize: 4096, MaxObjectBytes: 1 << 20,
	})
	require.NoError(t, err)
	mgr := NewPreparedStatementManager()
	mgr.SetContentSegmentStore(store)
	stmt, err := mgr.Prepare("insert into media (body) values (CONTENT_FROM_BYTES(?, 'audio/wav'))")
	require.NoError(t, err)
	require.NoError(t, mgr.AppendLongData(stmt.ID, 0, bytes.Repeat([]byte{0x55}, 1000)))
	require.NoError(t, mgr.Reset(stmt.ID))

	entries, err := os.ReadDir(storeDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestPreparedContentLongDataFailureIsLatchedAndDoesNotPublish(t *testing.T) {
	storeDir := t.TempDir()
	store, err := content.NewSegmentStore(storeDir, content.SegmentStoreOptions{
		SegmentSize: 4096, MaxObjectBytes: 4096,
	})
	require.NoError(t, err)
	mgr := NewPreparedStatementManager()
	mgr.SetContentSegmentStore(store)
	stmt, err := mgr.Prepare("insert into media (body) values (CONTENT_FROM_BYTES(?, 'video/mp4'))")
	require.NoError(t, err)

	firstErr := mgr.AppendLongData(stmt.ID, 0, bytes.Repeat([]byte{0x6b}, 4097))
	require.ErrorIs(t, firstErr, content.ErrObjectTooLarge)
	_, _, _, consumeErr := mgr.ConsumeLongDataForExecution(stmt.ID)
	require.Error(t, consumeErr)
	require.True(t, errors.Is(consumeErr, content.ErrObjectTooLarge))
	entries, err := os.ReadDir(storeDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}
