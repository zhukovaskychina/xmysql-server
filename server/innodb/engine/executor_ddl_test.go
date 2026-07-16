package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestDropDatabaseIfExistsMissingIsOk(t *testing.T) {
	tmp := t.TempDir()
	executor := &XMySQLExecutor{conf: &conf.Cfg{InnodbDataDir: tmp}}
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results}

	executor.executeDropDatabaseStatement(ctx, &sqlparser.DBDDL{
		Action:   "drop",
		DBName:   "missing_db",
		IfExists: true,
	})

	got := <-results
	require.NoError(t, got.Err)
	require.Equal(t, common.RESULT_TYPE_DDL, got.ResultType)
}

func TestAlterTableAddColumnUpdatesFrm(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "app")
	require.NoError(t, os.MkdirAll(dbPath, 0755))

	executor := NewXMySQLExecutor(nil, &conf.Cfg{InnodbDataDir: tmp})
	createSQL := "create table users (id int primary key)"
	createStmt, err := sqlparser.Parse(createSQL)
	require.NoError(t, err)
	require.NoError(t, executor.createTableImpl("app", "users", createStmt.(*sqlparser.DDL)))

	alterStmt, err := sqlparser.Parse("alter table users add column name varchar(100)")
	require.NoError(t, err)

	results := make(chan *Result, 1)
	executor.executeDDL(alterStmt.(*sqlparser.DDL), nil, "app", results)
	got := <-results
	require.NoError(t, got.Err)

	raw, err := os.ReadFile(filepath.Join(dbPath, "users.frm"))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"name": "name"`)
	require.Contains(t, string(raw), `"type": "varchar"`)
}
