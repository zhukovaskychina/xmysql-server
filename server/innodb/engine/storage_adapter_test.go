package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestStorageAdapter_GetRecordByPrimaryKey_ReturnsErrorWhenTableStorageManagerMissing(t *testing.T) {
	adapter := &StorageAdapter{}
	schema := &metadata.Table{
		Name: "users",
		Columns: []*metadata.Column{
			{Name: "id", DataType: metadata.TypeInt},
		},
	}

	record, err := adapter.GetRecordByPrimaryKey(context.Background(), 1, []byte("1"), schema)

	require.Error(t, err)
	assert.Nil(t, record)
	assert.ErrorIs(t, err, ErrStorageAdapterTableStorageManagerNil)
}

func TestStorageAdapter_GetRecordByPrimaryKey_ReturnsErrorWhenSchemaMissing(t *testing.T) {
	adapter := &StorageAdapter{}

	record, err := adapter.GetRecordByPrimaryKey(context.Background(), 1, []byte("1"), nil)

	require.Error(t, err)
	assert.Nil(t, record)
	assert.ErrorIs(t, err, ErrStorageAdapterSchemaNil)
}

func TestStorageAdapter_GetRecordByPrimaryKey_ReturnsErrorWhenBufferPoolManagerMissing(t *testing.T) {
	adapter := &StorageAdapter{
		tableStorageManager: manager.NewTableStorageManager(nil),
	}
	schema := &metadata.Table{
		Name: "users",
		Columns: []*metadata.Column{
			{Name: "id", DataType: metadata.TypeInt},
		},
	}

	record, err := adapter.GetRecordByPrimaryKey(context.Background(), 1, []byte("1"), schema)

	require.Error(t, err)
	assert.Nil(t, record)
	assert.ErrorIs(t, err, ErrStorageAdapterBufferPoolManagerNil)
}

func TestStorageAdapter_GetRecordByPrimaryKey_ReturnsErrorWhenTableStorageInfoMissing(t *testing.T) {
	tableStorageManager := manager.NewTableStorageManager(nil)
	require.NotNil(t, tableStorageManager)

	adapter := NewStorageAdapter(
		nil,
		&manager.OptimizedBufferPoolManager{},
		nil,
		tableStorageManager,
	)
	schema := &metadata.Table{
		Name: "users",
		Columns: []*metadata.Column{
			{Name: "id", DataType: metadata.TypeInt},
		},
	}

	record, err := adapter.GetRecordByPrimaryKey(context.Background(), 999999, []byte("1"), schema)

	require.Error(t, err)
	assert.Nil(t, record)
	assert.ErrorContains(t, err, "failed to get table storage info by spaceID 999999")
}
