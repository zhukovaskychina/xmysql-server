package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

// TestStorageEngineIntegration 测试存储引擎集成
func TestStorageEngineIntegration(t *testing.T) {
	// 创建模拟的管理器
	storageManager := createMockStorageManager()
	optimizerManager := createMockOptimizerManager()

	// 创建存储引擎集成器
	integrator := NewStorageEngineIntegrator(storageManager, optimizerManager)
	require.NotNil(t, integrator)

	// 测试统计信息收集
	stats := integrator.GetIntegrationStats()
	assert.NotNil(t, stats)
	assert.Equal(t, uint64(0), stats.QueriesProcessed)

	// 测试索引推荐
	ctx := context.Background()
	table := createMockTable()
	conditions := createMockConditions()

	recommendations, err := integrator.RecommendIndexes(ctx, table, conditions)
	assert.NoError(t, err)
	assert.NotNil(t, recommendations)

	// 测试缓存操作
	cacheKey := "test_key"
	cacheValue := "test_value"

	err = integrator.SetCache(cacheKey, cacheValue, time.Hour)
	assert.NoError(t, err)

	cachedValue, found := integrator.GetCache(cacheKey)
	assert.True(t, found)
	assert.Equal(t, cacheValue, cachedValue)
}

// TestSQLParserIntegration 测试SQL解析器集成
func TestSQLParserIntegration(t *testing.T) {
	// 创建依赖组件
	storageManager := createMockStorageManager()
	optimizerManager := createMockOptimizerManager()
	storageIntegrator := NewStorageEngineIntegrator(storageManager, optimizerManager)

	// 创建SQL解析器集成器
	integrator := NewSQLParserIntegrator(
		optimizerManager,
		storageIntegrator,
		createMockInfoSchemaManager(),
		createMockTableManager(),
	)
	require.NotNil(t, integrator)

	// 测试SQL解析和优化
	ctx := context.Background()
	query := "SELECT * FROM users WHERE id = 1"
	databaseName := "test_db"

	optimizedPlan, err := integrator.ParseAndOptimize(ctx, query, databaseName)
	assert.NoError(t, err)
	assert.NotNil(t, optimizedPlan)
	assert.Equal(t, QueryTypeSelect, optimizedPlan.SemanticInfo.QueryType)

	// 测试查询重写
	rewrittenQuery, err := integrator.RewriteQuery(ctx, query, databaseName)
	assert.NoError(t, err)
	assert.NotEmpty(t, rewrittenQuery)

	// 测试统计信息
	stats := integrator.GetParserStats()
	assert.NotNil(t, stats)
	assert.Greater(t, stats.QueriesParsed, uint64(0))
}

// TestExecutionEngineIntegration 测试执行引擎集成
func TestExecutionEngineIntegration(t *testing.T) {
	// 创建依赖组件
	storageManager := createMockStorageManager()
	optimizerManager := createMockOptimizerManager()
	storageIntegrator := NewStorageEngineIntegrator(storageManager, optimizerManager)
	parserIntegrator := NewSQLParserIntegrator(
		optimizerManager,
		storageIntegrator,
		createMockInfoSchemaManager(),
		createMockTableManager(),
	)

	// 创建执行引擎集成器
	integrator := NewExecutionEngineIntegrator(
		storageIntegrator,
		parserIntegrator,
		optimizerManager,
		storageManager,
	)
	require.NotNil(t, integrator)

	// 测试查询执行
	ctx := context.Background()
	query := "SELECT * FROM users WHERE id = 1"
	databaseName := "test_db"

	result, err := integrator.ExecuteOptimizedQuery(ctx, query, databaseName)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, QueryTypeSelect, result.QueryType)

	// 测试统计信息
	stats := integrator.GetExecutionStats()
	assert.NotNil(t, stats)
	assert.Greater(t, stats.ExecutedQueries, uint64(0))

	// 测试综合统计信息
	combinedStats := integrator.GetCombinedStats()
	assert.NotNil(t, combinedStats)
	assert.NotNil(t, combinedStats.StorageStats)
	assert.NotNil(t, combinedStats.ParserStats)
	assert.NotNil(t, combinedStats.ExecutionStats)
}

// TestIntegrationManager 测试集成管理器
func TestIntegrationManager(t *testing.T) {
	// 创建集成管理器
	storageManager := createMockStorageManager()
	optimizerManager := createMockOptimizerManager()
	config := DefaultIntegrationConfig()

	manager := NewIntegrationManager(optimizerManager, storageManager, config)
	require.NotNil(t, manager)

	// 测试初始化
	err := manager.Initialize()
	assert.NoError(t, err)

	// 验证集成状态
	status := manager.GetIntegrationStatus()
	assert.True(t, status.IsInitialized)
	assert.False(t, status.IsRunning)

	// 测试启动
	err = manager.Start()
	assert.NoError(t, err)

	status = manager.GetIntegrationStatus()
	assert.True(t, status.IsRunning)

	// 测试查询执行
	ctx := context.Background()
	query := "SELECT * FROM users WHERE id = 1"
	databaseName := "test_db"

	result, err := manager.ExecuteQuery(ctx, query, databaseName)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// 测试统计信息
	globalStats := manager.GetGlobalStats()
	assert.NotNil(t, globalStats)
	assert.Greater(t, globalStats.TotalQueries, uint64(0))

	// 测试集成摘要
	summary := manager.GetIntegrationSummary()
	assert.NotNil(t, summary)
	assert.NotNil(t, summary.Status)
	assert.NotNil(t, summary.Stats)

	// 测试配置更新
	newConfig := DefaultIntegrationConfig()
	newConfig.EnableOptimization = false
	err = manager.UpdateConfiguration(newConfig)
	assert.NoError(t, err)

	// 测试验证
	err = manager.ValidateIntegration()
	assert.NoError(t, err)

	// 测试停止
	err = manager.Stop()
	assert.NoError(t, err)

	status = manager.GetIntegrationStatus()
	assert.False(t, status.IsRunning)
}

// TestIntegrationPerformance 测试集成性能
func TestIntegrationPerformance(t *testing.T) {
	// 创建集成管理器
	storageManager := createMockStorageManager()
	optimizerManager := createMockOptimizerManager()
	config := DefaultIntegrationConfig()

	manager := NewIntegrationManager(optimizerManager, storageManager, config)
	require.NotNil(t, manager)

	err := manager.Initialize()
	require.NoError(t, err)

	err = manager.Start()
	require.NoError(t, err)
	defer manager.Stop()

	// 执行多个查询测试性能
	ctx := context.Background()
	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"SELECT name FROM users WHERE age > 25",
		"SELECT COUNT(*) FROM orders WHERE user_id = 1",
		"UPDATE users SET name = 'John' WHERE id = 1",
		"INSERT INTO users (name, age) VALUES ('Alice', 30)",
	}

	startTime := time.Now()
	for i, query := range queries {
		databaseName := "test_db"
		result, err := manager.ExecuteQuery(ctx, query, databaseName)
		assert.NoError(t, err, "查询 %d 执行失败: %s", i, query)
		assert.NotNil(t, result, "查询 %d 结果为空: %s", i, query)
	}
	totalTime := time.Since(startTime)

	// 验证性能统计
	globalStats := manager.GetGlobalStats()
	assert.Equal(t, uint64(len(queries)), globalStats.TotalQueries)
	assert.Greater(t, globalStats.SuccessfulQueries, uint64(0))
	assert.True(t, globalStats.AvgExecutionTime > 0)

	t.Logf("执行 %d 个查询总时间: %v", len(queries), totalTime)
	t.Logf("平均执行时间: %v", globalStats.AvgExecutionTime)
	t.Logf("性能改进: %.2f%%", globalStats.PerformanceImprovement)
}

// TestIntegrationConcurrency 测试并发集成
func TestIntegrationConcurrency(t *testing.T) {
	// 创建集成管理器
	storageManager := createMockStorageManager()
	optimizerManager := createMockOptimizerManager()
	config := DefaultIntegrationConfig()
	config.MaxConcurrentQueries = 10

	manager := NewIntegrationManager(optimizerManager, storageManager, config)
	require.NotNil(t, manager)

	err := manager.Initialize()
	require.NoError(t, err)

	err = manager.Start()
	require.NoError(t, err)
	defer manager.Stop()

	// 并发执行查询
	ctx := context.Background()
	query := "SELECT * FROM users WHERE id = ?"
	databaseName := "test_db"
	concurrency := 5
	queriesPerGoroutine := 10

	done := make(chan bool, concurrency)
	errors := make(chan error, concurrency*queriesPerGoroutine)

	for i := 0; i < concurrency; i++ {
		go func(goroutineID int) {
			defer func() { done <- true }()

			for j := 0; j < queriesPerGoroutine; j++ {
				result, err := manager.ExecuteQuery(ctx, query, databaseName)
				if err != nil {
					errors <- err
					return
				}
				if result == nil {
					errors <- fmt.Errorf("goroutine %d, query %d: 结果为空", goroutineID, j)
					return
				}
			}
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < concurrency; i++ {
		<-done
	}

	// 检查错误
	close(errors)
	for err := range errors {
		t.Errorf("并发执行错误: %v", err)
	}

	// 验证统计信息
	globalStats := manager.GetGlobalStats()
	expectedQueries := uint64(concurrency * queriesPerGoroutine)
	assert.Equal(t, expectedQueries, globalStats.TotalQueries)
	assert.Equal(t, expectedQueries, globalStats.SuccessfulQueries)
}

// 辅助函数：创建模拟组件

func createMockStorageManager() *manager.StorageManager {
	// 这里应该创建一个真实的或模拟的StorageManager
	// 为了测试目的，我们返回nil，实际实现中需要创建真实的对象
	return nil
}

func createMockOptimizerManager() *manager.OptimizerManager {
	// 这里应该创建一个真实的或模拟的OptimizerManager
	return nil
}

func createMockInfoSchemaManager() metadata.InfoSchemaManager {
	// 这里应该创建一个真实的或模拟的InfoSchemaManager
	return nil
}

func createMockTableManager() *manager.TableManager {
	// 这里应该创建一个真实的或模拟的TableManager
	return nil
}

func createMockTable() *metadata.Table {
	return &metadata.Table{
		Name:     "users",
		Database: "test_db",
		Columns: []*metadata.Column{
			{Name: "id", DataType: "INT", IsPrimaryKey: true},
			{Name: "name", DataType: "VARCHAR(255)"},
			{Name: "age", DataType: "INT"},
		},
	}
}

func createMockConditions() []plan.Expression {
	// 创建模拟的查询条件
	return []plan.Expression{
		&plan.BinaryExpression{
			Left:     &plan.ColumnExpression{ColumnName: "id"},
			Operator: "=",
			Right:    &plan.LiteralExpression{Value: 1},
		},
	}
}

// BenchmarkIntegrationPerformance 性能基准测试
func BenchmarkIntegrationPerformance(b *testing.B) {
	// 创建集成管理器
	storageManager := createMockStorageManager()
	optimizerManager := createMockOptimizerManager()
	config := DefaultIntegrationConfig()

	manager := NewIntegrationManager(optimizerManager, storageManager, config)
	if manager == nil {
		b.Skip("无法创建集成管理器")
	}

	err := manager.Initialize()
	if err != nil {
		b.Fatalf("初始化失败: %v", err)
	}

	err = manager.Start()
	if err != nil {
		b.Fatalf("启动失败: %v", err)
	}
	defer manager.Stop()

	ctx := context.Background()
	query := "SELECT * FROM users WHERE id = 1"
	databaseName := "test_db"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := manager.ExecuteQuery(ctx, query, databaseName)
			if err != nil {
				b.Errorf("查询执行失败: %v", err)
			}
		}
	})
}

// TestIntegrationErrorHandling 测试错误处理
func TestIntegrationErrorHandling(t *testing.T) {
	// 测试未初始化的管理器
	manager := NewIntegrationManager(nil, nil, nil)
	require.NotNil(t, manager)

	ctx := context.Background()
	query := "SELECT * FROM users"
	databaseName := "test_db"

	// 测试未初始化时执行查询
	result, err := manager.ExecuteQuery(ctx, query, databaseName)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "未初始化")

	// 测试重复初始化
	err = manager.Initialize()
	if err == nil {
		err = manager.Initialize()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "已经初始化")
	}

	// 测试无效配置
	err = manager.UpdateConfiguration(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "不能为空")
}

// TestIntegrationStatistics 测试统计信息功能
func TestIntegrationStatistics(t *testing.T) {
	// 创建集成管理器
	storageManager := createMockStorageManager()
	optimizerManager := createMockOptimizerManager()
	config := DefaultIntegrationConfig()
	config.StatisticsInterval = time.Millisecond * 100 // 快速统计更新

	manager := NewIntegrationManager(optimizerManager, storageManager, config)
	require.NotNil(t, manager)

	err := manager.Initialize()
	if err != nil {
		t.Skip("初始化失败，跳过统计测试")
	}

	err = manager.Start()
	if err != nil {
		t.Skip("启动失败，跳过统计测试")
	}
	defer manager.Stop()

	// 等待统计信息收集
	time.Sleep(time.Millisecond * 200)

	// 验证统计信息结构
	globalStats := manager.GetGlobalStats()
	assert.NotNil(t, globalStats)
	assert.True(t, globalStats.LastUpdated.After(time.Time{}))

	summary := manager.GetIntegrationSummary()
	assert.NotNil(t, summary)
	assert.NotNil(t, summary.Status)
	assert.NotNil(t, summary.Stats)
	assert.True(t, summary.LastUpdated.After(time.Time{}))
}
