package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckConstraintAcceptsUnknownButRejectsFalse(t *testing.T) {
	dataDir := t.TempDir()
	tableDir := filepath.Join(dataDir, "app")
	require.NoError(t, os.MkdirAll(tableDir, 0o755))
	metadata := tableCheckInfo{Checks: []string{"value IS NOT NULL"}}
	raw, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(tableDir, "items.frm"), raw, 0o644))

	dml := &StorageIntegratedDMLExecutor{dataDir: dataDir}
	err = dml.validateCheckConstraints([]*InsertRowData{{ColumnValues: map[string]interface{}{"value": nil}}}, "app", "items")
	require.Error(t, err, "IS NOT NULL evaluates FALSE for NULL and must violate CHECK")

	metadata.Checks = []string{"value > 0"}
	raw, err = json.Marshal(metadata)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(tableDir, "items.frm"), raw, 0o644))
	err = dml.validateCheckConstraints([]*InsertRowData{{ColumnValues: map[string]interface{}{"value": nil}}}, "app", "items")
	require.NoError(t, err, "ordinary comparison with NULL evaluates UNKNOWN and passes CHECK")
}
