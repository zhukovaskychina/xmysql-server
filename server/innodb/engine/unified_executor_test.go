package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
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
		adapter := NewTransactionAdapter(storageManager, nil)
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
	transactionAdapter := NewTransactionAdapter(nil, nil)

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
	infoSchemaManager := manager.NewInfoSchemaManager(nil, nil, nil)
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

	t.Run("BuildOperatorTreeRejectsNilPlan", func(t *testing.T) {
		executor := NewUnifiedExecutor(
			tableManager,
			nil,
			storageManager.GetBufferPoolManager(),
			storageManager,
			nil,
		)

		op, err := executor.BuildOperatorTree(context.Background(), nil)
		require.Error(t, err)
		assert.Nil(t, op)
		assert.Contains(t, err.Error(), "physical plan is nil")
	})

	t.Run("BuildOperatorTreeBuildsPhysicalTableScan", func(t *testing.T) {
		executor := NewUnifiedExecutor(
			tableManager,
			nil,
			storageManager.GetBufferPoolManager(),
			storageManager,
			nil,
		)

		physicalPlan := &plan.PhysicalTableScan{
			Table: &metadata.Table{
				Name:   "users",
				Schema: metadata.NewSchema("testdb"),
			},
		}

		op, err := executor.BuildOperatorTree(context.Background(), physicalPlan)
		require.NoError(t, err)

		tableScan, ok := op.(*TableScanOperator)
		require.True(t, ok, "expected *TableScanOperator, got %T", op)
		assert.Equal(t, "testdb", tableScan.schemaName)
		assert.Equal(t, "users", tableScan.tableName)
		assert.NotNil(t, tableScan.storageAdapter)
	})

	t.Run("CollectSelectResultIncludesRecordsAndColumns", func(t *testing.T) {
		executor := NewUnifiedExecutor(
			tableManager,
			nil,
			storageManager.GetBufferPoolManager(),
			storageManager,
			nil,
		)

		schema := metadata.NewQuerySchema()
		schema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
		schema.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))

		record := NewExecutorRecordFromValues(
			[]basic.Value{basic.NewInt64(1), basic.NewString("alice")},
			schema,
		)
		rootOperator := &ExpressionMockOperator{
			records: []Record{record},
			schema:  schema,
		}

		result, err := executor.collectSelectResult(context.Background(), rootOperator)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Records, 1)
		assert.Equal(t, record, result.Records[0])
		assert.Equal(t, 1, result.RowCount)
		assert.Equal(t, []string{"id", "name"}, result.Columns)
		assert.Equal(t, "SELECT", result.ResultType)
		assert.Equal(t, "Success", result.Message)
	})

	t.Run("BuildOperatorTreeBuildsPhysicalProjection", func(t *testing.T) {
		executor := NewUnifiedExecutor(
			tableManager,
			nil,
			storageManager.GetBufferPoolManager(),
			storageManager,
			nil,
		)

		child := &plan.PhysicalTableScan{
			Table: &metadata.Table{
				Name:   "users",
				Schema: metadata.NewSchema("testdb"),
			},
		}
		projection := &plan.PhysicalProjection{}
		projection.SetChildren([]plan.PhysicalPlan{child})

		op, err := executor.BuildOperatorTree(context.Background(), projection)
		require.NoError(t, err)
		_, ok := op.(*ProjectionOperator)
		require.True(t, ok, "expected *ProjectionOperator, got %T", op)
	})

	t.Run("BuildOperatorTreeBuildsPhysicalSelection", func(t *testing.T) {
		executor := NewUnifiedExecutor(
			tableManager,
			nil,
			storageManager.GetBufferPoolManager(),
			storageManager,
			nil,
		)

		child := &plan.PhysicalTableScan{
			Table: &metadata.Table{
				Name:   "users",
				Schema: metadata.NewSchema("testdb"),
			},
		}
		selection := &plan.PhysicalSelection{}
		selection.SetChildren([]plan.PhysicalPlan{child})

		op, err := executor.BuildOperatorTree(context.Background(), selection)
		require.NoError(t, err)
		_, ok := op.(*FilterOperator)
		require.True(t, ok, "expected *FilterOperator, got %T", op)
	})

	t.Run("BuildSelectOperatorTreeAddsProjectionForColumnSelect", func(t *testing.T) {
		executor := &UnifiedExecutor{
			storageAdapter: NewStorageAdapter(nil, nil, nil, nil),
		}

		stmt, err := sqlparser.Parse("select id from users")
		require.NoError(t, err)

		selectStmt, ok := stmt.(*sqlparser.Select)
		require.True(t, ok, "expected *sqlparser.Select, got %T", stmt)

		op, err := executor.buildSelectOperatorTree(context.Background(), selectStmt, "testdb")
		require.NoError(t, err)
		_, ok = op.(*ProjectionOperator)
		require.True(t, ok, "expected *ProjectionOperator, got %T", op)
	})

	t.Run("BuildSelectOperatorTreeKeepsTableScanForSelectStar", func(t *testing.T) {
		executor := &UnifiedExecutor{
			storageAdapter: NewStorageAdapter(nil, nil, nil, nil),
		}

		stmt, err := sqlparser.Parse("select * from users")
		require.NoError(t, err)

		selectStmt, ok := stmt.(*sqlparser.Select)
		require.True(t, ok, "expected *sqlparser.Select, got %T", stmt)

		op, err := executor.buildSelectOperatorTree(context.Background(), selectStmt, "testdb")
		require.NoError(t, err)
		_, ok = op.(*TableScanOperator)
		require.True(t, ok, "expected *TableScanOperator, got %T", op)
	})

	t.Run("BuildSelectOperatorTreeAddsLimitOperator", func(t *testing.T) {
		executor := &UnifiedExecutor{
			storageAdapter: NewStorageAdapter(nil, nil, nil, nil),
		}

		stmt, err := sqlparser.Parse("select * from users limit 5")
		require.NoError(t, err)

		selectStmt, ok := stmt.(*sqlparser.Select)
		require.True(t, ok, "expected *sqlparser.Select, got %T", stmt)

		op, err := executor.buildSelectOperatorTree(context.Background(), selectStmt, "testdb")
		require.NoError(t, err)
		limitOp, ok := op.(*LimitOperator)
		require.True(t, ok, "expected *LimitOperator, got %T", op)
		assert.Equal(t, int64(0), limitOp.offset)
		assert.Equal(t, int64(5), limitOp.limit)
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
		schema := metadata.NewQuerySchema()
		schema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
		schema.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))

		values := []basic.Value{basic.NewInt64(1), basic.NewString("test")}
		record := NewExecutorRecordFromValues(values, schema)

		require.NotNil(t, record)
		assert.Equal(t, len(values), record.GetColumnCount())
		assert.Equal(t, int64(1), record.GetValueByIndex(0).Int())
		assert.Equal(t, "test", record.GetValueByIndex(1).ToString())

		valueByName, err := record.GetValueByName("name")
		require.NoError(t, err)
		assert.Equal(t, "test", valueByName.ToString())

		err = record.SetValueByName("name", basic.NewString("updated"))
		require.NoError(t, err)
		assert.Equal(t, "updated", record.GetValueByIndex(1).ToString())
	})
}

func TestProjectionOperatorWithExprsDerivesSchemaFromChildColumns(t *testing.T) {
	childSchema := metadata.NewQuerySchema()
	childSchema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
	childSchema.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))

	child := &ExpressionMockOperator{
		records: []Record{
			NewExecutorRecordFromValues(
				[]basic.Value{basic.NewInt64(1), basic.NewString("alice")},
				childSchema,
			),
		},
		schema: childSchema,
	}

	op := NewProjectionOperatorWithExprs(child, []plan.Expression{
		&plan.Column{Name: "name"},
	})

	err := op.Open(context.Background())
	require.NoError(t, err)
	defer op.Close()

	schema := op.Schema()
	require.NotNil(t, schema)
	require.Len(t, schema.Columns, 1)
	assert.Equal(t, "name", schema.Columns[0].Name)
	assert.Equal(t, metadata.TypeVarchar, schema.Columns[0].DataType)

	record, err := op.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, record)
	assert.Equal(t, "alice", record.GetValueByIndex(0).ToString())
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
	infoSchemaManager := manager.NewInfoSchemaManager(nil, nil, nil)
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
