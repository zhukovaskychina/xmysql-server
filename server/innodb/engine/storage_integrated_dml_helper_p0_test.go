package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestStorageIntegratedDMLExecutor_MarkRowDeletedInPageContentPreservesOtherRows(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	first, err := executor.serializeRowData(&InsertRowData{ColumnValues: map[string]interface{}{"id": int64(1)}}, nil)
	require.NoError(t, err)
	second, err := executor.serializeRowData(&InsertRowData{ColumnValues: map[string]interface{}{"id": int64(2)}}, nil)
	require.NoError(t, err)

	pageContent, err := encodeDMLPageRows([]dmlPageRow{
		{Data: first},
		{Data: second},
	})
	require.NoError(t, err)

	updatedContent, err := markDMLPageRowDeleted(pageContent, 0)
	require.NoError(t, err)

	rows, err := decodeDMLPageRows(updatedContent)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.True(t, rows[0].Deleted)
	assert.False(t, rows[1].Deleted)

	row, err := executor.deserializeRowData(rows[1].Data)
	require.NoError(t, err)
	assert.Equal(t, int64(2), row.ColumnValues["id"])
}

func TestStorageIntegratedDMLExecutor_PageRowsAppendAndReplace(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	first, err := executor.serializeRowData(&InsertRowData{ColumnValues: map[string]interface{}{"id": int64(1), "name": "alice"}}, nil)
	require.NoError(t, err)
	second, err := executor.serializeRowData(&InsertRowData{ColumnValues: map[string]interface{}{"id": int64(2), "name": "bob"}}, nil)
	require.NoError(t, err)

	content, slot, err := appendDMLPageRow(make([]byte, 128), first)
	require.NoError(t, err)
	assert.Equal(t, 0, slot)

	content, slot, err = appendDMLPageRow(content, second)
	require.NoError(t, err)
	assert.Equal(t, 1, slot)

	replacement, err := executor.serializeRowData(&InsertRowData{ColumnValues: map[string]interface{}{"id": int64(1), "name": "carol"}}, nil)
	require.NoError(t, err)

	content, err = replaceDMLPageRow(content, 0, replacement)
	require.NoError(t, err)

	rows, err := decodeDMLPageRows(content)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	replaced, err := executor.deserializeRowData(rows[0].Data)
	require.NoError(t, err)
	assert.Equal(t, "carol", replaced.ColumnValues["name"])

	unchanged, err := executor.deserializeRowData(rows[1].Data)
	require.NoError(t, err)
	assert.Equal(t, "bob", unchanged.ColumnValues["name"])
}

func TestStorageIntegratedDMLExecutor_PageRowsRejectReplaceDeletedSlot(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	first, err := executor.serializeRowData(&InsertRowData{ColumnValues: map[string]interface{}{"id": int64(1)}}, nil)
	require.NoError(t, err)
	content, _, err := appendDMLPageRow(nil, first)
	require.NoError(t, err)

	content, err = markDMLPageRowDeleted(content, 0)
	require.NoError(t, err)

	replacement, err := executor.serializeRowData(&InsertRowData{ColumnValues: map[string]interface{}{"id": int64(2)}}, nil)
	require.NoError(t, err)

	_, err = replaceDMLPageRow(content, 0, replacement)
	assert.Error(t, err)
}
