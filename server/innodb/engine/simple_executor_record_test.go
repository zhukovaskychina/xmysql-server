package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestSimpleExecutorRecord_SetAndGetValueByName(t *testing.T) {
	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
	schema.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))

	record := &SimpleExecutorRecord{
		values: []basic.Value{
			basic.NewInt64Value(1),
			basic.NewStringValue("alice"),
		},
		schema: schema,
	}

	err := record.SetValueByName("name", basic.NewStringValue("bob"))
	assert.NoError(t, err)

	value, err := record.GetValueByName("name")
	assert.NoError(t, err)
	assert.Equal(t, "bob", value.ToString())
}

func TestSimpleExecutorRecord_ByNameErrors(t *testing.T) {
	record := &SimpleExecutorRecord{
		values: []basic.Value{basic.NewInt64Value(1)},
		schema: nil,
	}

	_, err := record.GetValueByName("id")
	assert.Error(t, err)

	err = record.SetValueByName("id", basic.NewInt64Value(2))
	assert.Error(t, err)
}

func TestSimpleExecutorRecord_ColumnNotFoundAndOutOfRange(t *testing.T) {
	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
	schema.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))

	record := &SimpleExecutorRecord{
		values: []basic.Value{
			basic.NewInt64Value(1),
		},
		schema: schema,
	}

	_, err := record.GetValueByName("missing")
	assert.Error(t, err)

	err = record.SetValueByName("missing", basic.NewStringValue("x"))
	assert.Error(t, err)

	_, err = record.GetValueByName("name")
	assert.Error(t, err)

	err = record.SetValueByName("name", basic.NewStringValue("x"))
	assert.Error(t, err)
}
