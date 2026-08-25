package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestStorageAdapter_GetTableMetadata_ReturnsErrorWhenDependenciesMissing(t *testing.T) {
	t.Run("missing table manager", func(t *testing.T) {
		adapter := &StorageAdapter{tableStorageManager: manager.NewTableStorageManager(nil)}

		meta, err := adapter.GetTableMetadata(context.Background(), "test", "users")
		require.Error(t, err)
		assert.Nil(t, meta)
		assert.ErrorIs(t, err, ErrStorageAdapterTableManagerNil)
	})

	t.Run("missing table storage manager", func(t *testing.T) {
		adapter := &StorageAdapter{
			tableManager: manager.NewTableManager(&mockInfoSchemaForSortTest{}),
		}

		meta, err := adapter.GetTableMetadata(context.Background(), "test", "users")
		require.Error(t, err)
		assert.Nil(t, meta)
		assert.ErrorIs(t, err, ErrStorageAdapterTableStorageManagerNil)
	})
}

func TestStorageAdapter_ReadPage_ReturnsErrorWhenBufferPoolManagerMissing(t *testing.T) {
	adapter := &StorageAdapter{}

	page, err := adapter.ReadPage(context.Background(), 1, 1)
	assert.Error(t, err)
	assert.Nil(t, page)
	assert.ErrorIs(t, err, ErrStorageAdapterBufferPoolManagerNil)
}

func TestStorageAdapter_ParseRecords_RequiresPageAndContent(t *testing.T) {
	adapter := &StorageAdapter{}

	_, err := adapter.ParseRecords(context.Background(), nil, &metadata.Table{Name: "users"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "page is nil")

	_, err = adapter.ParseRecords(context.Background(), &buffer_pool.BufferPage{}, &metadata.Table{Name: "users"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "page content is nil")
}

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
