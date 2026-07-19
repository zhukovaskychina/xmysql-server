package engine

import (
	"path/filepath"
)

// tableRowsSidecarPath returns the legacy table-row sidecar path so DDL cleanup
// can remove files created by older versions. New DML/SELECT paths must use
// clustered B+Tree pages instead.
func tableRowsSidecarPath(dataDir, schemaName, tableName string) string {
	if dataDir == "" || schemaName == "" || tableName == "" {
		return ""
	}
	return filepath.Join(dataDir, schemaName, tableName+".xrows.json")
}
