package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.False(t, errors.Is(err, ErrStorageAdapterBufferPoolManagerNil))
}
