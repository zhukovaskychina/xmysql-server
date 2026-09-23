package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCharsetConverterRoundTripsUTF8AndLatin1(t *testing.T) {
	manager := NewCharsetManager()
	converter := NewCharsetConverter(manager)

	latin1, err := converter.Convert([]byte("café"), 45, 8)
	require.NoError(t, err)
	require.Equal(t, []byte{'c', 'a', 'f', 0xe9}, latin1)

	utf8, err := converter.Convert(latin1, 8, 45)
	require.NoError(t, err)
	require.Equal(t, []byte("café"), utf8)
}

func TestCharsetConverterRejectsInvalidUTF8(t *testing.T) {
	manager := NewCharsetManager()
	converter := NewCharsetConverter(manager)

	require.Error(t, converter.ValidateString([]byte{0xff, 0xfe}, 45))
}
