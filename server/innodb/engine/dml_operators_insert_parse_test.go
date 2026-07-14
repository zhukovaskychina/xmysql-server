package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestInsertOperator_ParseInsertRows_MapsExplicitColumns(t *testing.T) {
	stmt, err := sqlparser.Parse("INSERT INTO users (id, name) VALUES (1, 'alice')")
	require.NoError(t, err)

	insertStmt, ok := stmt.(*sqlparser.Insert)
	assert.True(t, ok)

	table := metadata.NewTable("users")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar, CharMaxLength: 32})

	op := &InsertOperator{stmt: insertStmt}
	rows, err := op.parseInsertRows(table)

	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(1), rows[0]["id"])
	assert.Equal(t, "alice", rows[0]["name"])
}

func TestInsertOperator_ParseInsertRows_UsesSchemaOrderWhenColumnsOmitted(t *testing.T) {
	stmt, err := sqlparser.Parse("INSERT INTO users VALUES (2, 'bob')")
	require.NoError(t, err)

	insertStmt, ok := stmt.(*sqlparser.Insert)
	assert.True(t, ok)

	table := metadata.NewTable("users")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar, CharMaxLength: 32})

	op := &InsertOperator{stmt: insertStmt}
	rows, err := op.parseInsertRows(table)

	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(2), rows[0]["id"])
	assert.Equal(t, "bob", rows[0]["name"])
}

func TestInsertOperator_ParseInsertRows_RejectsValueCountMismatch(t *testing.T) {
	stmt, err := sqlparser.Parse("INSERT INTO users (id, name) VALUES (1)")
	require.NoError(t, err)

	insertStmt, ok := stmt.(*sqlparser.Insert)
	assert.True(t, ok)

	table := metadata.NewTable("users")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar, CharMaxLength: 32})

	op := &InsertOperator{stmt: insertStmt}
	rows, err := op.parseInsertRows(table)

	assert.Error(t, err)
	assert.Nil(t, rows)
}
