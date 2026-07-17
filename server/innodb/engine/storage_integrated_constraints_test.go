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

func TestRowMatchesWhereConditionsWithValueColumnRange(t *testing.T) {
	matches, err := rowMatchesWhereConditions(map[string]interface{}{"value": int64(5500)}, []string{"value > 5000 and value < 6000"})
	require.NoError(t, err)
	require.True(t, matches)

	matches, err = rowMatchesWhereConditions(map[string]interface{}{"value": int64(6000)}, []string{"value > 5000 and value < 6000"})
	require.NoError(t, err)
	require.False(t, matches)
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

func TestCreateTableAllowsForeignKeyMetadataSyntax(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(100) not null)")
	mustExecSQL(t, executor, "app", "create table orders (id int primary key, user_id int, foreign key (user_id) references users(id))")
}

func TestCreateTableAcceptsAdvancedConstraintSyntax(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "inline references on delete cascade",
			sql:  "create table child (id int primary key, parent_id int references parent(id) on delete cascade)",
		},
		{
			name: "inline references on update cascade",
			sql:  "create table child (id int primary key, parent_id int references parent(id) on update cascade)",
		},
		{
			name: "check",
			sql:  "create table checked_values (id int primary key, age int check (age >= 0))",
		},
		{
			name: "fulltext",
			sql:  "create table docs (id int primary key, content text, fulltext index idx_content (content))",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executor := newTestStorageIntegratedExecutor(t, t.TempDir())
			mustExecSQL(t, executor, "", "create database app")
			mustExecSQL(t, executor, "app", tc.sql)
		})
	}
}

func TestUpdateRejectsNullForNotNullColumn(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, username varchar(50) not null)")
	mustExecSQL(t, executor, "app", "insert into users (id, username) values (1, 'alice')")

	err := execSQLExpectError(t, executor, "app", "update users set username = null where id = 1")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "not null")
}

func TestUpdateRejectsPrimaryAndUniqueKeyConflicts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sql      string
		contains string
	}{
		{
			name:     "primary key conflict",
			sql:      "update users set id = 2 where id = 1",
			contains: "primary",
		},
		{
			name:     "unique key conflict",
			sql:      "update users set email = 'bob@example.com' where id = 1",
			contains: "email",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executor := newTestStorageIntegratedExecutor(t, t.TempDir())
			mustExecSQL(t, executor, "", "create database app")
			mustExecSQL(t, executor, "app", "create table users (id int primary key, email varchar(100) unique)")
			mustExecSQL(t, executor, "app", "insert into users (id, email) values (1, 'alice@example.com')")
			mustExecSQL(t, executor, "app", "insert into users (id, email) values (2, 'bob@example.com')")

			err := execSQLExpectError(t, executor, "app", tc.sql)
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), tc.contains)
			require.Contains(t, strings.ToLower(err.Error()), "duplicate")
		})
	}
}

func TestUpdateWithoutPrimaryKeyUnsupportedInsteadOfRowIdWrite(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (email varchar(100), name varchar(50))")
	mustExecSQL(t, executor, "app", "insert into users (email, name) values ('alice@example.com', 'Alice')")

	err := execSQLExpectError(t, executor, "app", "update users set name = 'Alice2' where email = 'alice@example.com'")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "unsupported")
	require.Contains(t, strings.ToLower(err.Error()), "primary key")
}

func TestInsertPlainTableWithoutPrimaryKeyStillWorks(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (email varchar(100), name varchar(50))")

	mustExecSQL(t, executor, "app", "insert into users (email, name) values ('alice@example.com', 'Alice')")
}

func TestInsertIndexedTableWithoutPrimaryKeyUnsupportedInsteadOfOverwritingID(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int, email varchar(100) unique)")

	err := execSQLExpectError(t, executor, "app", "insert into users (id, email) values (1, 'alice@example.com')")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "unsupported")
	require.Contains(t, strings.ToLower(err.Error()), "primary key")
}

func TestInsertSecondaryIndexedTableWithoutPrimaryKeyUnsupported(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (email varchar(100), index idx_email (email))")

	err := execSQLExpectError(t, executor, "app", "insert into users (email) values ('alice@example.com')")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "unsupported")
	require.Contains(t, strings.ToLower(err.Error()), "primary key")
}

func TestCompositePrimaryKeyUpdateDeleteUnsupportedInsteadOfRowIdWrite(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "update",
			sql:  "update memberships set group_id = 30 where user_id = 1 and group_id = 10",
		},
		{
			name: "delete",
			sql:  "delete from memberships where user_id = 1 and group_id = 10",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executor := newTestStorageIntegratedExecutor(t, t.TempDir())
			mustExecSQL(t, executor, "", "create database app")
			mustExecSQL(t, executor, "app", "create table memberships (user_id int, group_id int, primary key (user_id, group_id))")
			mustExecSQL(t, executor, "app", "insert into memberships (user_id, group_id) values (1, 10)")
			mustExecSQL(t, executor, "app", "insert into memberships (user_id, group_id) values (1, 20)")

			err := execSQLExpectError(t, executor, "app", tc.sql)
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), "unsupported")
			require.Contains(t, strings.ToLower(err.Error()), "composite")
		})
	}
}

func execSQLExpectError(t *testing.T, executor *XMySQLEngine, databaseName, sql string) error {
	t.Helper()
	got := <-executor.ExecuteQuery(nil, sql, databaseName)
	return got.Err
}
