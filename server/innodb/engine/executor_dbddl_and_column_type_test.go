package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestNormalizeDBDDLOptionsFromAST(t *testing.T) {
	stmt, err := sqlparser.Parse("CREATE DATABASE IF NOT EXISTS xmysql_prod")
	assert.NoError(t, err)

	dbddl, ok := stmt.(*sqlparser.DBDDL)
	assert.True(t, ok)
	exec := &XMySQLExecutor{}
	exec.normalizeDBDDLOptions(dbddl)

	assert.True(t, dbddl.IfExists)
	assert.Equal(t, sqlparser.CreateStr, dbddl.Action)
}

func TestNormalizeDBDDLOptionsDropWithoutExistsStaysFalse(t *testing.T) {
	stmt, err := sqlparser.Parse("DROP DATABASE xmysql_prod")
	assert.NoError(t, err)

	dbddl, ok := stmt.(*sqlparser.DBDDL)
	assert.True(t, ok)
	exec := &XMySQLExecutor{}
	exec.normalizeDBDDLOptions(dbddl)

	assert.False(t, dbddl.IfExists)
	assert.Equal(t, sqlparser.DropStr, dbddl.Action)
}

func TestFormatShowColumnTypeUsesExplicitLengthOnlyForStringLikeTypes(t *testing.T) {
	exec := &XMySQLExecutor{}

	assert.Equal(t, "VARCHAR(64)", exec.formatShowColumnType(metadata.TypeVarchar, 64))
	assert.Equal(t, "INT", exec.formatShowColumnType(metadata.TypeInt, 11))
	assert.Equal(t, "JSON", exec.formatShowColumnType(metadata.TypeJSON, 16))
	assert.Equal(t, "CHAR(16)", exec.formatShowColumnType(metadata.TypeChar, 16))
}

func TestCreateTableStorageMappingIsIdempotent(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	}

	storageManager := manager.NewStorageManager(cfg)
	require.NotNil(t, storageManager)
	tableStorageManager := manager.NewTableStorageManager(storageManager)

	exec := &XMySQLExecutor{conf: cfg}
	exec.SetAdditionalManagers(nil, storageManager, tableStorageManager)

	// 预先创建若干空间以避开系统表占用的 space ID
	for i := 0; i < 30; i++ {
		_, err := storageManager.CreateTablespace(fmt.Sprintf("seed_space_%d", i))
		assert.NoError(t, err)
	}

	assert.NoError(t, exec.createTableStorageMapping("mysql", "t1"))
	assert.NoError(t, exec.createTableStorageMapping("mysql", "t1"))

	info, err := tableStorageManager.GetTableStorageInfo("mysql", "t1")
	assert.NoError(t, err)
	assert.Equal(t, "mysql", info.SchemaName)
	assert.Equal(t, "t1", info.TableName)
}

func TestCreateTableStorageMappingRefreshesStaleRegisteredMapping(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	}

	storageManager := manager.NewStorageManager(cfg)
	require.NotNil(t, storageManager)
	tableStorageManager := manager.NewTableStorageManager(storageManager)

	oldHandle, err := storageManager.CreateTablespace("old/users")
	require.NoError(t, err)
	require.NoError(t, tableStorageManager.RegisterTable(t.Context(), &manager.TableStorageInfo{
		SchemaName:    "app",
		TableName:     "users",
		SpaceID:       oldHandle.SpaceID,
		RootPageNo:    3,
		IndexPageNo:   3,
		DataSegmentID: oldHandle.DataSegmentID,
		Type:          manager.TableTypeUser,
	}))

	exec := &XMySQLExecutor{conf: cfg}
	exec.SetAdditionalManagers(nil, storageManager, tableStorageManager)

	require.NoError(t, exec.createTableStorageMapping("app", "users"))

	newHandle, err := storageManager.GetTablespace("app/users")
	require.NoError(t, err)
	require.NotEqual(t, oldHandle.SpaceID, newHandle.SpaceID)

	info, err := tableStorageManager.GetTableStorageInfo("app", "users")
	require.NoError(t, err)
	assert.Equal(t, newHandle.SpaceID, info.SpaceID)
	assert.Equal(t, newHandle.DataSegmentID, info.DataSegmentID)
}

func TestCreateTableStructureFileFallsBackToRawSQLWhenDDLSpecIsPartial(t *testing.T) {
	tmp := t.TempDir()
	stmt, err := sqlparser.Parse("create table users")
	require.NoError(t, err)

	rawQuery := "CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, username VARCHAR(50) NOT NULL, salary DECIMAL(10,2), is_active BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
	exec := &XMySQLExecutor{}
	require.NoError(t, exec.createTableStructureFile(tmp, "users", stmt.(*sqlparser.DDL), rawQuery))

	data, err := os.ReadFile(filepath.Join(tmp, "users.frm"))
	require.NoError(t, err)
	var got struct {
		Columns []map[string]interface{} `json:"columns"`
	}
	require.NoError(t, json.Unmarshal(data, &got))
	require.Len(t, got.Columns, 5)
	assert.Equal(t, "id", got.Columns[0]["name"])
	assert.Equal(t, "INT", got.Columns[0]["type"])
	assert.Equal(t, true, got.Columns[0]["primary"])
	assert.Equal(t, true, got.Columns[0]["auto_increment"])
	assert.Equal(t, "DECIMAL", got.Columns[2]["type"])
	assert.Equal(t, float64(10), got.Columns[2]["length"])
	assert.Equal(t, float64(2), got.Columns[2]["scale"])
	assert.Equal(t, "BOOLEAN", got.Columns[3]["type"])
	assert.Equal(t, "TIMESTAMP", got.Columns[4]["type"])
}
