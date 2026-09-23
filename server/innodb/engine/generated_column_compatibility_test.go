package engine

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestGeneratedColumnMetadataIsLoadedFromFrm(t *testing.T) {
	dataDir := t.TempDir()
	dbDir := filepath.Join(dataDir, "app")
	require.NoError(t, os.MkdirAll(dbDir, 0755))
	frm := `{"table_name":"orders","columns":[{"name":"qty","type":"INT","nullable":false},{"name":"price","type":"INT","nullable":false},{"name":"total","type":"INT","nullable":false,"generated":true,"generated_expression":"qty * price"}]}`
	require.NoError(t, os.WriteFile(filepath.Join(dbDir, "orders.frm"), []byte(frm), 0644))

	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	dml.SetDataDir(dataDir)
	dml.schemaName = "app"
	dml.tableName = "orders"
	meta, err := dml.getTableMetadata()
	require.NoError(t, err)
	require.True(t, meta.Columns[2].IsGenerated)
	require.Equal(t, "qty * price", meta.Columns[2].GeneratedExpression)
}

func TestInnoDBVirtualBasePositionsFollowGeneratedExpression(t *testing.T) {
	columns := []frmMetadataColumn{
		{name: "qty"},
		{name: "price"},
		{name: "total", generatedExpression: "qty * price"},
	}
	require.Equal(t, []int{0, 1}, innodbVirtualBasePositions("qty * price", columns, 2))
	require.Equal(t, []int{1}, innodbVirtualBasePositions("concat('qty', price)", columns, 2))
}

func TestGeneratedColumnIsComputedBeforeInsertValidation(t *testing.T) {
	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	row := &InsertRowData{ColumnValues: map[string]interface{}{"qty": int64(3), "price": int64(7)}, ColumnTypes: map[string]metadata.DataType{}}
	meta := &metadata.TableMeta{Columns: []*metadata.ColumnMeta{
		{Name: "qty", Type: metadata.TypeInt, IsNullable: false},
		{Name: "price", Type: metadata.TypeInt, IsNullable: false},
		{Name: "total", Type: metadata.TypeInt, IsNullable: false, IsGenerated: true, GeneratedExpression: "qty * price"},
	}}
	require.NoError(t, dml.validateInsertData([]*InsertRowData{row}, meta))
	require.Equal(t, int64(21), row.ColumnValues["total"])
}

func TestGeneratedColumnDDLAndInsertUsePersistedExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table orders (qty int not null, price int not null, total int generated always as (qty * price) stored)")
	mustExecSQL(t, executor, "app", "insert into orders (qty, price) values (3, 7)")
	rows := mustQuerySQL(t, executor, "app", "select qty, price, total from orders")
	require.Len(t, rows, 1)
	require.Equal(t, string(binary.BigEndian.AppendUint64(nil, 21)), rows[0][2])
}

func TestRenameColumnUpdatesGeneratedExpression(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table rename_generated (amount int, total int generated always as (amount + 1) stored)")
	mustExecSQL(t, executor, "app", "alter table rename_generated rename column amount to score")
	mustExecSQL(t, executor, "app", "insert into rename_generated (score) values (2)")
	rows := mustQuerySQL(t, executor, "app", "select score, total from rename_generated")
	require.Equal(t, [][]interface{}{{string(binary.BigEndian.AppendUint64(nil, 2)), string(binary.BigEndian.AppendUint64(nil, 3))}}, rows)
}
