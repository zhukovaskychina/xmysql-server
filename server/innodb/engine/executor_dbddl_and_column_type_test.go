package engine

import (
	"fmt"
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
