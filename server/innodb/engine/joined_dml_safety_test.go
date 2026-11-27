package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoinedDMLSelectParserReturnsErrorInsteadOfPanicking(t *testing.T) {
	_, err := parseJoinedDMLSelect("not a select")
	require.Error(t, err)
}
