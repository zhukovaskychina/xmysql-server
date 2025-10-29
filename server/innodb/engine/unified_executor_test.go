package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TestStorageAdapter 测试存储适配器基本功能
func TestStorageAdapter(t *testing.T) {
	// 创建测试配置
	cfg := &conf.Cfg{
		DataDir:              "testdata",
		InnodbDataDir:        "testdata/innodb",
		InnodbBufferPoolSize: 16 * 1024 * 1024, // 16MB
		InnodbPageSize:       16384,            // 16KB
	}

	// 创建管理器
	storageManager := manager.NewStorageManager(cfg)
	require.NotNil(t, storageManager, "StorageManager should not be nil")

	// 注意：这里需要实际的TableManager、BufferPoolManager等实例
	// 由于依赖复杂，这里只测试接口存在性
	t.Run("CreateStorageAdapter", func(t *testing.T) {
		adapter := NewStorageAdapter(nil, nil, storageManager, nil)
		assert.NotNil(t, adapter, "StorageAdapter should not be nil")
	})
}

// TestIndexAdapter 测试索引适配器
func TestIndexAdapter(t *testing.T) {
	t.Run("CreateIndexAdapter", func(t *testing.T) {
		adapter := NewIndexAdapter(nil, nil, nil)
		assert.NotNil(t, adapter, "IndexAdapter should not be nil")
	})
}

// TestTransactionAdapter 测试事务适配器
func TestTransactionAdapter(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              "testdata",
		InnodbDataDir:        "testdata/innodb",
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	}

	storageManager := manager.NewStorageManager(cfg)
	require.NotNil(t, storageManager)

	t.Run("BeginTransaction", func(t *testing.T) {
		adapter := NewTransactionAdapter(storageManager)
		assert.NotNil(t, adapter, "TransactionAdapter should not be nil")

		ctx := context.Background()
		txn, err := adapter.BeginTransaction(ctx, false, "READ COMMITTED")
		assert.NoError(t, err, "BeginTransaction should not error")
		assert.NotNil(t, txn, "Transaction should not be nil")
		assert.False(t, txn.ReadOnly, "Transaction should not be read-only")
		assert.Equal(t, "READ COMMITTED", txn.IsolationLevel)
	})
}

// TestTableScanOperator 测试表扫描算子
func TestTableScanOperator(t *testing.T) {
	t.Run("CreateTableScanOperator", func(t *testing.T) {
		storageAdapter := NewStorageAdapter(nil, nil, nil, nil)
		op := NewTableScanOperator("testdb", "testtable", storageAdapter)

		assert.NotNil(t, op, "TableScanOperator should not be nil")
		assert.Equal(t, "testdb", op.schemaName)
		assert.Equal(t, "testtable", op.tableName)
		assert.NotNil(t, op.storageAdapter)
	})
}

// TestIndexScanOperator 测试索引扫描算子
func TestIndexScanOperator(t *testing.T) {
	t.Run("CreateIndexScanOperator", func(t *testing.T) {
		storageAdapter := NewStorageAdapter(nil, nil, nil, nil)
		indexAdapter := NewIndexAdapter(nil, nil, storageAdapter)

		op := NewIndexScanOperator(
			"testdb",
			"testtable",
			"idx_test",
			storageAdapter,
			indexAdapter,
			nil,
			nil,
			[]string{"col1", "col2"},
		)

		assert.NotNil(t, op, "IndexScanOperator should not be nil")
		assert.Equal(t, "testdb", op.schemaName)
		assert.Equal(t, "testtable", op.tableName)
		assert.Equal(t, "idx_test", op.indexName)
		assert.Equal(t, []string{"col1", "col2"}, op.requiredColumns)
	})
}

// TestDMLOperators 测试DML算子
func TestDMLOperators(t *testing.T) {
	storageAdapter := NewStorageAdapter(nil, nil, nil, nil)
	indexAdapter := NewIndexAdapter(nil, nil, storageAdapter)
	transactionAdapter := NewTransactionAdapter(nil)

	t.Run("CreateInsertOperator", func(t *testing.T) {
		op := NewInsertOperator(
			"testdb",
			"testtable",
			nil, // stmt
			storageAdapter,
			indexAdapter,
			transactionAdapter,
		)

		assert.NotNil(t, op, "InsertOperator should not be nil")
		assert.Equal(t, "testdb", op.schemaName)
		assert.Equal(t, "testtable", op.tableName)
		assert.False(t, op.executed)
		assert.Equal(t, int64(0), op.affectedRows)
	})

	t.Run("CreateUpdateOperator", func(t *testing.T) {
		scanOp := NewTableScanOperator("testdb", "testtable", storageAdapter)

		op := NewUpdateOperator(
			"testdb",
			"testtable",
			nil, // stmt
			storageAdapter,
			indexAdapter,
			transactionAdapter,
			scanOp,
		)

		assert.NotNil(t, op, "UpdateOperator should not be nil")
		assert.Equal(t, "testdb", op.schemaName)
		assert.Equal(t, "testtable", op.tableName)
		assert.NotNil(t, op.scanOperator)
	})

	t.Run("CreateDeleteOperator", func(t *testing.T) {
		scanOp := NewTableScanOperator("testdb", "testtable", storageAdapter)

		op := NewDeleteOperator(
			"testdb",
			"testtable",
			nil, // stmt
			storageAdapter,
			indexAdapter,
			transactionAdapter,
			scanOp,
		)

		assert.NotNil(t, op, "DeleteOperator should not be nil")
		assert.Equal(t, "testdb", op.schemaName)
		assert.Equal(t, "testtable", op.tableName)
		assert.NotNil(t, op.scanOperator)
	})
}

// TestUnifiedExecutor 测试统一执行器
func TestUnifiedExecutor(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              "testdata",
		InnodbDataDir:        "testdata/innodb",
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	}

	// 创建基础管理器
	storageManager := manager.NewStorageManager(cfg)
	require.NotNil(t, storageManager)

	// 创建InfoSchemaManager（简化版本）
	infoSchemaManager := metadata.NewInfoSchemaManager()
	require.NotNil(t, infoSchemaManager)

	tableManager := manager.NewTableManagerWithStorage(infoSchemaManager, storageManager)
	require.NotNil(t, tableManager)

	t.Run("CreateUnifiedExecutor", func(t *testing.T) {
		executor := NewUnifiedExecutor(
			tableManager,
			nil, // indexManager
			storageManager.GetBufferPoolManager(),
			storageManager,
			nil, // tableStorageManager
		)

		assert.NotNil(t, executor, "UnifiedExecutor should not be nil")
		assert.NotNil(t, executor.storageAdapter)
		assert.NotNil(t, executor.indexAdapter)
		assert.NotNil(t, executor.transactionAdapter)
	})
}

// TestOperatorInterface 测试算子接口一致性
func TestOperatorInterface(t *testing.T) {
	storageAdapter := NewStorageAdapter(nil, nil, nil, nil)
	indexAdapter := NewIndexAdapter(nil, nil, storageAdapter)

	// 测试所有算子都实现了Operator接口
	var _ Operator = NewTableScanOperator("db", "table", storageAdapter)
	var _ Operator = NewIndexScanOperator("db", "table", "idx", storageAdapter, indexAdapter, nil, nil, nil)
	var _ Operator = NewFilterOperator(nil, nil)
	var _ Operator = NewProjectionOperator(nil, nil)

	t.Log("All operators correctly implement Operator interface")
}

// TestRecordInterface 测试Record接口
func TestRecordInterface(t *testing.T) {
	t.Run("SimpleExecutorRecord", func(t *testing.T) {
		schema := &metadata.Schema{
			Columns: []*metadata.Column{
				{Name: "id", Type: "INT"},
				{Name: "name", Type: "VARCHAR"},
			},
		}

		values := []interface{}{1, "test"}
		// 注意：这里需要实际的basic.Value类型
		// record := NewExecutorRecordFromValues(values, schema)

		// assert.NotNil(t, record)
		// assert.Equal(t, len(values), len(record.GetValues()))

		t.Log("Record interface test placeholder")
	})
}

// BenchmarkTableScanOperator 表扫描算子性能测试
func BenchmarkTableScanOperator(b *testing.B) {
	storageAdapter := NewStorageAdapter(nil, nil, nil, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := NewTableScanOperator("testdb", "testtable", storageAdapter)
		_ = op
	}
}

// BenchmarkUnifiedExecutorCreation 统一执行器创建性能测试
func BenchmarkUnifiedExecutorCreation(b *testing.B) {
	cfg := &conf.Cfg{
		DataDir:              "testdata",
		InnodbDataDir:        "testdata/innodb",
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	}

	storageManager := manager.NewStorageManager(cfg)
	infoSchemaManager := metadata.NewInfoSchemaManager()
	tableManager := manager.NewTableManagerWithStorage(infoSchemaManager, storageManager)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor := NewUnifiedExecutor(
			tableManager,
			nil,
			storageManager.GetBufferPoolManager(),
			storageManager,
			nil,
		)
		_ = executor
	}
}
