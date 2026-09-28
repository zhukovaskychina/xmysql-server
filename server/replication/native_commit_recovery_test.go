package replication

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceRetryAfterNativeAppendFailureDoesNotDuplicateTransaction(t *testing.T) {
	dir := t.TempDir()
	source, err := NewSource(dir, "native-retry-source", 17)
	require.NoError(t, err)

	previous := nativeAppendHook
	nativeAppendHook = func([]BinlogEvent) error {
		nativeAppendHook = nil
		return errors.New("injected native append failure")
	}
	t.Cleanup(func() { nativeAppendHook = previous })

	changes := []RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(1)},
	}}
	_, err = source.AppendCommittedTransactionWithKey("retry-key", changes, nil)
	require.ErrorContains(t, err, "injected native append failure")

	_, err = source.AppendCommittedTransactionWithKey("retry-key", changes, nil)
	require.NoError(t, err)

	logical, err := source.Writer.ReadFrom(4)
	require.NoError(t, err)
	commits := 0
	for _, event := range logical {
		if event.Type == EventCommit {
			commits++
		}
	}
	require.Equal(t, 1, commits)

	native, err := source.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	decoded, err := NewNativeBinlogDecoder().DecodeTransactions(native)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
}

func TestSourceKeyedCommitReturnsNewlyAppendedEvents(t *testing.T) {
	dir := t.TempDir()
	source, err := NewSource(dir, "native-keyed-events-source", 19)
	require.NoError(t, err)

	changes := []RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(19)},
	}}
	events, err := source.AppendCommittedTransactionWithKey("events-key", changes, nil)
	require.NoError(t, err)
	require.Len(t, events, 3, "keyed commit must return BEGIN/ROW/COMMIT for the new transaction")
	require.Equal(t, EventBegin, events[0].Type)
	require.Equal(t, EventCommit, events[len(events)-1].Type)

	duplicate, err := source.AppendCommittedTransactionWithKey("events-key", changes, nil)
	require.NoError(t, err)
	require.Empty(t, duplicate, "idempotent retry must not return or append a second transaction")
}

func TestSourceXARetryAfterNativeAppendFailureDoesNotDuplicateTerminal(t *testing.T) {
	dir := t.TempDir()
	source, err := NewSource(dir, "native-xa-retry-source", 18)
	require.NoError(t, err)
	xid := XAIdentity{GTRID: "native-xa-retry", BQUAL: "branch", FormatID: 18}
	require.NoError(t, source.PrepareXATransaction("xa-retry-key", xid, nil, nil))

	previous := nativeAppendHook
	nativeAppendHook = func([]BinlogEvent) error {
		nativeAppendHook = nil
		return errors.New("injected native XA append failure")
	}
	t.Cleanup(func() { nativeAppendHook = previous })

	err = source.CommitXATransaction("xa-retry-key", xid)
	require.ErrorContains(t, err, "injected native XA append failure")

	require.NoError(t, source.CommitXATransaction("xa-retry-key", xid))

	logical, err := source.Writer.ReadFrom(4)
	require.NoError(t, err)
	terminals := 0
	for _, event := range logical {
		if event.Type == EventXACommit {
			terminals++
		}
	}
	require.Equal(t, 1, terminals)
}
