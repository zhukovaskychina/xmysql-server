package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type fakeShowInfoSchema struct {
	schemas []string
	tables  map[string][]*metadata.Table
}

func (f *fakeShowInfoSchema) GetSchemaByName(ctx context.Context, name string) (metadata.Schema, error) {
	return nil, fmt.Errorf("not implemented in fake")
}
func (f *fakeShowInfoSchema) HasSchema(ctx context.Context, name string) bool { return false }
func (f *fakeShowInfoSchema) GetAllSchemaNames(ctx context.Context) ([]string, error) {
	return append([]string(nil), f.schemas...), nil
}
func (f *fakeShowInfoSchema) GetAllSchemas(ctx context.Context) ([]metadata.Schema, error) {
	return nil, fmt.Errorf("not implemented in fake")
}
func (f *fakeShowInfoSchema) CreateSchema(ctx context.Context, schema metadata.Schema) error {
	return fmt.Errorf("not implemented in fake")
}
func (f *fakeShowInfoSchema) DropSchema(ctx context.Context, name string) error {
	return fmt.Errorf("not implemented in fake")
}
func (f *fakeShowInfoSchema) GetTableByName(ctx context.Context, schemaName, tableName string) (*metadata.Table, error) {
	for _, table := range f.tables[schemaName] {
		if table.Name == tableName {
			return table, nil
		}
	}
	return nil, fmt.Errorf("table not found")
}
func (f *fakeShowInfoSchema) HasTable(ctx context.Context, schemaName, tableName string) bool {
	_, err := f.GetTableByName(ctx, schemaName, tableName)
	return err == nil
}
func (f *fakeShowInfoSchema) GetAllTables(ctx context.Context, schemaName string) ([]*metadata.Table, error) {
	return append([]*metadata.Table(nil), f.tables[schemaName]...), nil
}
func (f *fakeShowInfoSchema) CreateTable(ctx context.Context, schemaName string, table *metadata.Table) error {
	return fmt.Errorf("not implemented in fake")
}
func (f *fakeShowInfoSchema) DropTable(ctx context.Context, schemaName, tableName string) error {
	return fmt.Errorf("not implemented in fake")
}
func (f *fakeShowInfoSchema) RefreshMetadata(ctx context.Context, schemaName string) error {
	return nil
}
func (f *fakeShowInfoSchema) GetTableMetadata(ctx context.Context, schemaName, tableName string) (*metadata.TableMeta, error) {
	table, err := f.GetTableByName(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	cols := make([]*metadata.ColumnMeta, 0, len(table.Columns))
	for _, col := range table.Columns {
		cols = append(cols, &metadata.ColumnMeta{Name: col.Name, Type: col.DataType, IsNullable: col.IsNullable})
	}
	return &metadata.TableMeta{Name: table.Name, Columns: cols}, nil
}
func (f *fakeShowInfoSchema) GetTableStats(ctx context.Context, schemaName, tableName string) (*metadata.InfoTableStats, error) {
	return nil, fmt.Errorf("not implemented in fake")
}
func (f *fakeShowInfoSchema) UpdateTableStats(ctx context.Context, schemaName, tableName string, stats *metadata.InfoTableStats) error {
	return fmt.Errorf("not implemented in fake")
}
func (f *fakeShowInfoSchema) DatabaseExists(name string) (bool, error) { return false, nil }
func (f *fakeShowInfoSchema) DropDatabase(name string) bool            { return false }

func TestShowExecutor_ShowDatabasesUsesInfoSchema(t *testing.T) {
	executor := &ShowExecutor{
		showType:          "DATABASES",
		infoSchemaManager: &fakeShowInfoSchema{schemas: []string{"information_schema", "app"}},
		current:           -1,
	}

	require.NoError(t, executor.Init())
	require.NoError(t, executor.Next())
	assert.Equal(t, []interface{}{"app"}, executor.GetRow())
	require.NoError(t, executor.Next())
	assert.Equal(t, []interface{}{"information_schema"}, executor.GetRow())
}

func TestShowExecutor_ShowTablesUsesInfoSchema(t *testing.T) {
	users := metadata.NewTable("users")
	executor := &ShowExecutor{
		showType:          "TABLES",
		schemaName:        "app",
		infoSchemaManager: &fakeShowInfoSchema{tables: map[string][]*metadata.Table{"app": {users}}},
		current:           -1,
	}

	require.NoError(t, executor.Init())
	require.NoError(t, executor.Next())
	assert.Equal(t, []interface{}{"users"}, executor.GetRow())
}

func TestShowExecutor_ShowColumnsUsesInfoSchema(t *testing.T) {
	users := metadata.NewTable("users")
	users.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt, IsNullable: false})
	users.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar, CharMaxLength: 32, IsNullable: true})

	executor := &ShowExecutor{
		showType:          "COLUMNS",
		schemaName:        "app",
		tableName:         "users",
		infoSchemaManager: &fakeShowInfoSchema{tables: map[string][]*metadata.Table{"app": {users}}},
		current:           -1,
	}

	require.NoError(t, executor.Init())
	require.NoError(t, executor.Next())
	assert.Equal(t, []interface{}{"id", "int", "NO", "", nil, ""}, executor.GetRow())
	require.NoError(t, executor.Next())
	assert.Equal(t, []interface{}{"name", "varchar", "YES", "", nil, ""}, executor.GetRow())

	schema := executor.Schema()
	require.NotNil(t, schema)
	assert.Len(t, schema.Columns, 6)
}

func TestXMySQLExecutor_ShowTablesUsesInfoSchemaInsteadOfDataDir(t *testing.T) {
	users := metadata.NewTable("users")
	executor := &XMySQLExecutor{
		infosSchemaManager: &fakeShowInfoSchema{tables: map[string][]*metadata.Table{"app": {users}}},
	}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results}
	session := newTestMySQLSession()
	session.SetParamByName("database", "app")

	executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "tables"}, session, "show tables")

	result := <-results
	require.NoError(t, result.Err)
	data, ok := result.Data.(map[string]interface{})
	require.True(t, ok)
	rows, ok := data["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "users", rows[0][0])
}
