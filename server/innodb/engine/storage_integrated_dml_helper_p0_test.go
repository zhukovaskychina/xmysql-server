package engine

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestStorageIntegratedDMLExecutor_FindRowsToUpdateRejectsMissingWhere(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	rows, err := executor.findRowsToUpdateInStorage(context.Background(), nil, nil, nil, nil, nil)

	assert.Error(t, err)
	assert.Nil(t, rows)
}

func TestStorageIntegratedDMLExecutor_FindRowsToDeleteRejectsMissingWhere(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	rows, err := executor.findRowsToDeleteInStorage(context.Background(), nil, []string{}, nil, nil, nil)

	assert.Error(t, err)
	assert.Nil(t, rows)
}

func TestStorageIntegratedDMLExecutor_RowSerializationUsesClusteredRecordWithoutLegacyPagePayload(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	tableMeta := &metadata.TableMeta{
		Name: "users",
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: metadata.TypeInt, IsPrimary: true},
			{Name: "name", Type: metadata.TypeVarchar},
		},
	}
	row := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"id":   int64(1),
			"name": "alice",
		},
		ColumnTypes: map[string]metadata.DataType{
			"id":   metadata.TypeInt,
			"name": metadata.TypeVarchar,
		},
	}

	serialized, err := executor.serializeRowData(row, tableMeta)
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(string(serialized), clusteredRecordMagic))
	assert.NotContains(t, string(serialized), "XDML"+"ROWS1")
}

func TestStorageIntegratedDMLExecutor_SourceHasNoLegacyRootPageRowHelpers(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	engineDir := filepath.Dir(currentFile)
	files := []string{
		"storage_integrated_dml_helper.go",
		"storage_integrated_dml_executor.go",
		"select_executor.go",
	}
	forbidden := []string{
		"XDML" + "ROWS1",
		"dml" + "Page" + "RowsMagic",
		"append" + "DML" + "PageRow",
		"replace" + "DML" + "PageRow",
		"mark" + "DML" + "PageRowDeleted",
		"encode" + "DML" + "PageRows",
		"decode" + "DML" + "PageRows",
	}

	for _, file := range files {
		source, err := os.ReadFile(filepath.Join(engineDir, file))
		require.NoError(t, err)
		for _, term := range forbidden {
			assert.NotContains(t, string(source), term, "%s still contains legacy page-row symbol %q", file, term)
		}
	}
}
