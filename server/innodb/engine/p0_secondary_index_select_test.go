package engine

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestP0SelectUsesDurableSecondaryIndexAfterRestart(t *testing.T) {
	tmp := t.TempDir()
	first := newP0SecondaryIndexTestEngine(tmp)
	mustExecSQL(t, first, "", "create database app")
	mustExecSQL(t, first, "app", "create table users (id int primary key, email varchar(100), name varchar(50), index idx_email (email))")
	mustExecSQL(t, first, "app", "insert into users (id, email, name) values (1, 'a@example.com', 'alice')")
	mustExecSQL(t, first, "app", "insert into users (id, email, name) values (2, 'b@example.com', 'bob')")
	require.NoError(t, first.Close())

	second := newP0SecondaryIndexTestEngine(tmp)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	selectExecutor := newP0SecondaryIndexTestSelectExecutor(t, second, tmp)
	rows := mustQueryRows(t, selectExecutor, "app", "select id, name from users where email = 'b@example.com'")
	require.Equal(t, [][]interface{}{{int64(2), "bob"}}, rows)
	require.True(t, selectExecutor.lastAccessPathWasSecondaryIndex("idx_email"))
}

func newP0SecondaryIndexTestEngine(dataDir string) *XMySQLEngine {
	return NewXMySQLEngine(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
}

func newP0SecondaryIndexTestSelectExecutor(t *testing.T, engine *XMySQLEngine, dataDir string) *SelectExecutor {
	t.Helper()
	queryExecutor := engine.QueryExecutor
	optimizerManager, _ := queryExecutor.optimizerManager.(*manager.OptimizerManager)
	bufferPoolManager, _ := queryExecutor.bufferPoolManager.(*manager.OptimizedBufferPoolManager)
	btreeManager, _ := queryExecutor.btreeManager.(basic.BPlusTreeManager)
	tableManager, _ := queryExecutor.tableManager.(*manager.TableManager)

	return NewSelectExecutor(
		optimizerManager,
		bufferPoolManager,
		btreeManager,
		queryExecutor.storageManager,
		tableManager,
		dataDir,
	)
}

func mustQueryRows(t *testing.T, executor *SelectExecutor, schemaName, query string) [][]interface{} {
	t.Helper()
	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)
	selectStmt, ok := stmt.(*sqlparser.Select)
	require.True(t, ok, "expected SELECT statement, got %T", stmt)

	result, err := executor.ExecuteSelect(context.Background(), selectStmt, schemaName)
	require.NoError(t, err)
	rows := make([][]interface{}, len(result.Records))
	for rowIndex, record := range result.Records {
		values := record.GetValues()
		rows[rowIndex] = make([]interface{}, len(values))
		for valueIndex, value := range values {
			if bytes, ok := value.Raw().([]byte); ok {
				if integer, err := strconv.ParseInt(string(bytes), 10, 64); err == nil {
					rows[rowIndex][valueIndex] = integer
					continue
				}
				rows[rowIndex][valueIndex] = string(bytes)
				continue
			}
			rows[rowIndex][valueIndex] = value.Raw()
		}
	}
	return rows
}

func (se *SelectExecutor) lastAccessPathWasSecondaryIndex(indexName string) bool {
	return se.lastAccessPath == "secondary_index:"+indexName
}
