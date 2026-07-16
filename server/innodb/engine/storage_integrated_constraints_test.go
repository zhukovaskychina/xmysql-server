package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInsertRejectsNullForNotNullColumn(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, username varchar(50) not null)")

	err := execSQLExpectError(t, executor, "app", "insert into users (id, username) values (1, null)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "not null")
}

func TestCompositePrimaryKeyUsesAllColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table memberships (user_id int, group_id int, primary key (user_id, group_id))")
	mustExecSQL(t, executor, "app", "insert into memberships (user_id, group_id) values (1, 10)")
	mustExecSQL(t, executor, "app", "insert into memberships (user_id, group_id) values (1, 20)")

	err := execSQLExpectError(t, executor, "app", "insert into memberships (user_id, group_id) values (1, 10)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "duplicate")
}

func TestCreateTablePreservesSecondaryIndexMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, username varchar(50), index idx_username (username))")

	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	dml.SetDataDir(executor.conf.InnodbDataDir)
	dml.schemaName = "app"
	dml.tableName = "users"
	tableMeta, err := dml.getTableMetadata()
	require.NoError(t, err)

	require.Len(t, tableMeta.Indices, 2)
	require.Equal(t, "PRIMARY", tableMeta.Indices[0].Name)
	require.Equal(t, []string{"id"}, tableMeta.Indices[0].Columns)
	require.Equal(t, "idx_username", tableMeta.Indices[1].Name)
	require.Equal(t, []string{"username"}, tableMeta.Indices[1].Columns)
	require.False(t, tableMeta.Indices[1].Unique)
}

func TestCreateTableRejectsDeferredAdvancedConstraints(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sql      string
		contains string
	}{
		{
			name:     "foreign key",
			sql:      "create table child (id int primary key, parent_id int, foreign key (parent_id) references parent(id))",
			contains: "foreign key",
		},
		{
			name:     "check",
			sql:      "create table checked_values (id int primary key, age int check (age >= 0))",
			contains: "check",
		},
		{
			name:     "fulltext",
			sql:      "create table docs (id int primary key, content text, fulltext index idx_content (content))",
			contains: "fulltext",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executor := newTestStorageIntegratedExecutor(t, t.TempDir())
			mustExecSQL(t, executor, "", "create database app")

			err := execSQLExpectError(t, executor, "app", tc.sql)
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), tc.contains)
		})
	}
}

func execSQLExpectError(t *testing.T, executor *XMySQLEngine, databaseName, sql string) error {
	t.Helper()
	got := <-executor.ExecuteQuery(nil, sql, databaseName)
	return got.Err
}
