package content

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTextChunksCanBePersistedWithTheirDerivationAttributes(t *testing.T) {
	value, err := NewValue("text/plain", 1, []byte("alpha"))
	require.NoError(t, err)
	source, err := NewSourceRef("app", "docs", "body", []byte("1"), value)
	require.NoError(t, err)
	var previousID string
	for _, maxBytes := range []int{64, 128} {
		options := TextChunkOptions{MaxBytes: maxBytes}
		blocks, err := ChunkText(value, source, options)
		require.NoError(t, err)
		require.Len(t, blocks, 1)
		block := blocks[0]
		encoded, err := BlockValue(block)
		require.NoError(t, err, "chunk attributes must participate in the finalized persisted identity")
		restored, err := BlockFromValue(encoded)
		require.NoError(t, err)
		require.Equal(t, block, restored)
		single, exists, err := TextChunkAt(value, source, 0, options)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, block, single)
		if previousID != "" {
			require.NotEqual(t, previousID, block.ID, "different derivation settings must have different IDs even for identical text")
		}
		previousID = block.ID
		block.Attributes = map[string]string{"chunk_max_bytes": "999"}
		_, err = BlockValue(block)
		require.ErrorIs(t, err, ErrInvalidBlockEncoding, "post-publication attribute tampering must still be rejected")
	}
}
