package content

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentedRowRevisionSurvivesCodecAndStoreReopen(t *testing.T) {
	root := t.TempDir()
	store, err := NewSegmentStore(root, SegmentStoreOptions{})
	require.NoError(t, err)
	ref, err := store.Put(context.Background(), "text/plain", 1, strings.NewReader("immutable source bytes"))
	require.NoError(t, err)
	value, err := NewSegmentedValue(ref)
	require.NoError(t, err)
	legacy, err := value.MarshalStorageBinary()
	require.NoError(t, err)
	require.Equal(t, byte(2), legacy[4])
	var legacyDecoded Value
	require.NoError(t, legacyDecoded.UnmarshalBinary(legacy))
	require.Equal(t, uint64(1), legacyDecoded.Revision)
	require.Equal(t, ref, *legacyDecoded.ExternalRef)

	value.Revision = 7 // Row assignment must not mutate the immutable object.
	encoded, err := value.MarshalStorageBinary()
	require.NoError(t, err)
	require.Equal(t, byte(3), encoded[4])
	var restored Value
	require.NoError(t, restored.UnmarshalBinary(encoded))
	require.Equal(t, uint64(7), restored.Revision)
	require.Equal(t, ref, *restored.ExternalRef)
	reopened, err := NewSegmentStore(root, SegmentStoreOptions{})
	require.NoError(t, err)
	body, err := restored.ReadRangeFromStore(context.Background(), reopened, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, "immutable source bytes", string(body))
	for length := 0; length < len(encoded); length++ {
		var truncated Value
		require.Error(t, truncated.UnmarshalBinary(encoded[:length]), "truncated v3 at %d", length)
	}
}
