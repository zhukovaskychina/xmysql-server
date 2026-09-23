package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConstantSelectPreservesProjectionAliasForClientMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select 1 as first_value")

	require.Equal(t, []string{"first_value"}, result.Columns)
}
