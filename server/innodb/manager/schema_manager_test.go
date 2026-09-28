package manager

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestSimpleDatabaseLoadsFRMDefinitionsAfterRestart(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "users.frm"), mustMarshalSchemaTestTable(t, &metadata.TableMeta{
		Name:   "users",
		Engine: "InnoDB",
		Columns: []*metadata.ColumnMeta{{
			Name:       "id",
			Type:       metadata.TypeInt,
			IsNullable: false,
			IsPrimary:  true,
		}},
	}), 0644))

	db := &SimpleDatabase{name: "app", path: dir, tables: make(map[string]*metadata.Table)}
	tables := db.ListTables()
	require.Len(t, tables, 1)
	require.Equal(t, "users", tables[0].Name)
	require.Len(t, tables[0].Columns, 1)
	require.Equal(t, "id", tables[0].Columns[0].Name)
}

func mustMarshalSchemaTestTable(t *testing.T, table *metadata.TableMeta) []byte {
	t.Helper()
	raw, err := json.Marshal(table)
	require.NoError(t, err)
	return raw
}
