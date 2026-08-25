package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestXMySQLEngine_resolveDmlDatabaseName(t *testing.T) {
	engine := &XMySQLEngine{}

	session := newTestMySQLSession()
	session.SetParamByName("database", "p0e_db")

	// 无显式库名，入口数据库被污染为 mysql 时，回退会话库
	got := engine.resolveDmlDatabaseName(session, "mysql", "")
	assert.Equal(t, "p0e_db", got)

	// 显式库名时应不受污染数据库影响
	got = engine.resolveDmlDatabaseName(session, "mysql", "p0e_db")
	assert.Equal(t, "p0e_db", got)

	// 没有会话库时保持原入参
	sessionWithoutDB := &testMySQLSession{params: map[string]interface{}{}}
	got = engine.resolveDmlDatabaseName(sessionWithoutDB, "mysql", "")
	assert.Equal(t, "mysql", got)

	// 明确参数为空时，优先使用会话库
	got = engine.resolveDmlDatabaseName(session, "", "")
	assert.Equal(t, "p0e_db", got)
}

func TestXMySQLEngine_extractTableExprSchema(t *testing.T) {
	engine := &XMySQLEngine{}

	stmt, err := sqlparser.Parse("update p0e_db.t1 set c1 = 1")
	assert.NoError(t, err)
	updateStmt, ok := stmt.(*sqlparser.Update)
	assert.True(t, ok)

	got := engine.extractTableExprSchema(updateStmt.TableExprs)
	assert.Equal(t, "p0e_db", got)
}

func TestXMySQLEngineExecuteQueryAlterTableAddColumn(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "app")
	require.NoError(t, os.MkdirAll(dbPath, 0755))

	cfg := &conf.Cfg{InnodbDataDir: tmp}
	executor := NewXMySQLExecutor(nil, cfg)
	createStmt, err := sqlparser.Parse("create table users (id int primary key)")
	require.NoError(t, err)
	require.NoError(t, executor.createTableImpl("app", "users", createStmt.(*sqlparser.DDL)))

	engine := &XMySQLEngine{conf: cfg, QueryExecutor: executor}
	got := <-engine.ExecuteQuery(nil, "alter table users add column name varchar(100)", "app")
	require.NoError(t, got.Err)

	raw, err := os.ReadFile(filepath.Join(dbPath, "users.frm"))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"name": "name"`)
}
