package integration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// ExecutionEngineIntegrator 执行引擎集成器
// 负责查询优化器与查询执行引擎的协作
type ExecutionEngineIntegrator struct {
	sync.RWMutex

	// 执行引擎组件
	xmysqlExecutor       *engine.XMySQLExecutor
	selectExecutor       *engine.SelectExecutor
	dmlExecutor          *engine.DMLExecutor
	storageIntegratedDML *engine.StorageIntegratedDMLExecutor

	// 集成组件
	storageIntegrator *StorageEngineIntegrator
	parserIntegrator  *SQLParserIntegrator
	optimizerManager  *manager.OptimizerManager

	// 管理器组件
	storageManager     *manager.StorageManager
	bufferPoolManager  *manager.OptimizedBufferPoolManager
	btreeManager       basic.BPlusTreeManager
	tableManager       *manager.TableManager
	indexManager       *manager.IndexManager
	transactionManager *manager.TransactionManager

	// 集成状态
	isInitialized  bool
	executionStats *ExecutionIntegrationStats
}

// ExecutionIntegrationStats 执行引擎集成统计信息
type ExecutionIntegrationStats struct {
	ExecutedQueries     uint64
	OptimizedExecutions uint64
	CacheHits           uint64
	CacheMisses         uint64
	AvgExecutionTime    time.Duration
	AvgOptimizationTime time.Duration
	TotalExecutionTime  time.Duration
	IndexScansUsed      uint64
	TableScansUsed      uint64
	CoveringIndexUsed   uint64
}

// NewExecutionEngineIntegrator 创建执行引擎集成器
func NewExecutionEngineIntegrator(
	storageIntegrator *StorageEngineIntegrator,
	parserIntegrator *SQLParserIntegrator,
	optimizerManager *manager.OptimizerManager,
	storageManager *manager.StorageManager,
) *ExecutionEngineIntegrator {
	integrator := &ExecutionEngineIntegrator{
		storageIntegrator: storageIntegrator,
		parserIntegrator:  parserIntegrator,
		optimizerManager:  optimizerManager,
		storageManager:    storageManager,
		isInitialized:     false,
		executionStats:    &ExecutionIntegrationStats{},
	}

	// 初始化集成组件
	integrator.initializeIntegration()

	return integrator
}

// initializeIntegration 初始化集成组件
func (eei *ExecutionEngineIntegrator) initializeIntegration() {
	// 获取管理器组件
	eei.bufferPoolManager = eei.storageManager.GetBufferPoolManager()
	eei.btreeManager = eei.storageManager.GetBTreeManager()
	eei.tableManager = eei.storageManager.GetTableManager()
	eei.indexManager = eei.storageManager.GetIndexManager()
	eei.transactionManager = eei.storageManager.GetTransactionManager()

	// 初始化执行器
	eei.initializeExecutors()

	eei.isInitialized = true
	logger.Info("执行引擎集成器初始化完成")
}

// initializeExecutors 初始化执行器
func (eei *ExecutionEngineIntegrator) initializeExecutors() {
	// 创建SELECT执行器
	eei.selectExecutor = engine.NewSelectExecutor(
		eei.optimizerManager,
		eei.bufferPoolManager,
		eei.btreeManager,
		eei.tableManager,
	)

	// 创建DML执行器
	eei.dmlExecutor = engine.NewDMLExecutor(
		eei.optimizerManager,
		eei.bufferPoolManager,
		eei.btreeManager,
		eei.tableManager,
		eei.transactionManager,
	)

	// 创建存储引擎集成的DML执行器
	eei.storageIntegratedDML = engine.NewStorageIntegratedDMLExecutor(
		eei.optimizerManager,
		eei.bufferPoolManager,
		eei.btreeManager,
		eei.tableManager,
		eei.transactionManager,
		eei.indexManager,
		eei.storageManager,
		eei.storageManager.GetTableStorageManager(),
	)
}

// ExecuteOptimizedQuery 执行优化后的查询
func (eei *ExecutionEngineIntegrator) ExecuteOptimizedQuery(
	ctx context.Context,
	query string,
	databaseName string,
) (*ExecutionResult, error) {
	startTime := time.Now()
	defer func() {
		eei.updateExecutionStats(time.Since(startTime))
	}()

	if !eei.isInitialized {
		return nil, fmt.Errorf("执行引擎集成器未初始化")
	}

	// 1. 解析并优化SQL
	optimizedSQLPlan, err := eei.parserIntegrator.ParseAndOptimize(ctx, query, databaseName)
	if err != nil {
		return nil, fmt.Errorf("SQL解析和优化失败: %v", err)
	}

	// 2. 根据查询类型选择执行路径
	result, err := eei.executeByQueryType(ctx, optimizedSQLPlan)
	if err != nil {
		return nil, fmt.Errorf("查询执行失败: %v", err)
	}

	// 3. 更新统计信息
	eei.updateExecutionStatistics(optimizedSQLPlan)

	return result, nil
}

// executeByQueryType 根据查询类型执行
func (eei *ExecutionEngineIntegrator) executeByQueryType(
	ctx context.Context,
	optimizedPlan *OptimizedSQLPlan,
) (*ExecutionResult, error) {
	switch optimizedPlan.SemanticInfo.QueryType {
	case QueryTypeSelect:
		return eei.executeSelectQuery(ctx, optimizedPlan)
	case QueryTypeInsert:
		return eei.executeInsertQuery(ctx, optimizedPlan)
	case QueryTypeUpdate:
		return eei.executeUpdateQuery(ctx, optimizedPlan)
	case QueryTypeDelete:
		return eei.executeDeleteQuery(ctx, optimizedPlan)
	default:
		return nil, fmt.Errorf("不支持的查询类型: %v", optimizedPlan.SemanticInfo.QueryType)
	}
}

// executeSelectQuery 执行SELECT查询
func (eei *ExecutionEngineIntegrator) executeSelectQuery(
	ctx context.Context,
	optimizedPlan *OptimizedSQLPlan,
) (*ExecutionResult, error) {
	selectStmt, ok := optimizedPlan.ParsedStatement.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("无效的SELECT语句")
	}

	// 使用优化后的执行计划
	executionPlan := eei.buildOptimizedExecutionPlan(optimizedPlan)

	// 执行查询
	selectResult, err := eei.executeSelectWithPlan(ctx, selectStmt, executionPlan, optimizedPlan.SemanticInfo.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("SELECT执行失败: %v", err)
	}

	return &ExecutionResult{
		QueryType:        QueryTypeSelect,
		SelectResult:     selectResult,
		RowsAffected:     int64(selectResult.RowCount),
		ExecutionTime:    selectResult.ExecutionTime,
		OptimizationUsed: true,
		AccessMethod:     optimizedPlan.OptimizedPlan.AccessMethod.String(),
		IndexUsed:        optimizedPlan.OptimizedPlan.StorageHints.IndexName,
	}, nil
}

// executeSelectWithPlan 使用计划执行SELECT
func (eei *ExecutionEngineIntegrator) executeSelectWithPlan(
	ctx context.Context,
	stmt *sqlparser.Select,
	executionPlan *OptimizedExecutionPlan,
	databaseName string,
) (*engine.SelectResult, error) {
	// 根据访问方法选择执行策略
	switch executionPlan.AccessMethod {
	case AccessMethodIndexScan, AccessMethodCoveringIndex:
		return eei.executeIndexScan(ctx, stmt, executionPlan, databaseName)
	case AccessMethodTableScan:
		return eei.executeTableScan(ctx, stmt, executionPlan, databaseName)
	default:
		return eei.selectExecutor.ExecuteSelect(ctx, stmt, databaseName)
	}
}

// executeIndexScan 执行索引扫描
func (eei *ExecutionEngineIntegrator) executeIndexScan(
	ctx context.Context,
	stmt *sqlparser.Select,
	executionPlan *OptimizedExecutionPlan,
	databaseName string,
) (*engine.SelectResult, error) {
	// 使用索引扫描优化
	eei.executionStats.IndexScansUsed++

	// 应用存储提示
	if err := eei.applyStorageHints(executionPlan.StorageHints); err != nil {
		logger.Warnf("应用存储提示失败: %v", err)
	}

	// 执行索引扫描
	result, err := eei.selectExecutor.ExecuteSelect(ctx, stmt, databaseName)
	if err != nil {
		return nil, err
	}

	// 标记使用了索引优化
	result.OptimizationInfo = &engine.OptimizationInfo{
		IndexUsed:     true,
		IndexName:     executionPlan.StorageHints.IndexName,
		CoveringIndex: executionPlan.AccessMethod == AccessMethodCoveringIndex,
		EstimatedCost: executionPlan.CostEstimate.TotalCost,
		ActualRows:    int64(result.RowCount),
	}

	return result, nil
}

// executeTableScan 执行表扫描
func (eei *ExecutionEngineIntegrator) executeTableScan(
	ctx context.Context,
	stmt *sqlparser.Select,
	executionPlan *OptimizedExecutionPlan,
	databaseName string,
) (*engine.SelectResult, error) {
	// 使用表扫描
	eei.executionStats.TableScansUsed++

	// 应用存储提示
	if err := eei.applyStorageHints(executionPlan.StorageHints); err != nil {
		logger.Warnf("应用存储提示失败: %v", err)
	}

	// 执行表扫描
	result, err := eei.selectExecutor.ExecuteSelect(ctx, stmt, databaseName)
	if err != nil {
		return nil, err
	}

	// 标记使用了表扫描
	result.OptimizationInfo = &engine.OptimizationInfo{
		IndexUsed:     false,
		IndexName:     "",
		CoveringIndex: false,
		EstimatedCost: executionPlan.CostEstimate.TotalCost,
		ActualRows:    int64(result.RowCount),
	}

	return result, nil
}

// executeInsertQuery 执行INSERT查询
func (eei *ExecutionEngineIntegrator) executeInsertQuery(
	ctx context.Context,
	optimizedPlan *OptimizedSQLPlan,
) (*ExecutionResult, error) {
	insertStmt, ok := optimizedPlan.ParsedStatement.(*sqlparser.Insert)
	if !ok {
		return nil, fmt.Errorf("无效的INSERT语句")
	}

	// 使用存储引擎集成的DML执行器
	dmlResult, err := eei.storageIntegratedDML.ExecuteInsert(ctx, insertStmt, optimizedPlan.SemanticInfo.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("INSERT执行失败: %v", err)
	}

	return &ExecutionResult{
		QueryType:        QueryTypeInsert,
		DMLResult:        dmlResult,
		RowsAffected:     dmlResult.RowsAffected,
		ExecutionTime:    dmlResult.ExecutionTime,
		OptimizationUsed: true,
		AccessMethod:     "STORAGE_INTEGRATED",
	}, nil
}

// executeUpdateQuery 执行UPDATE查询
func (eei *ExecutionEngineIntegrator) executeUpdateQuery(
	ctx context.Context,
	optimizedPlan *OptimizedSQLPlan,
) (*ExecutionResult, error) {
	updateStmt, ok := optimizedPlan.ParsedStatement.(*sqlparser.Update)
	if !ok {
		return nil, fmt.Errorf("无效的UPDATE语句")
	}

	// 使用存储引擎集成的DML执行器
	dmlResult, err := eei.storageIntegratedDML.ExecuteUpdate(ctx, updateStmt, optimizedPlan.SemanticInfo.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("UPDATE执行失败: %v", err)
	}

	return &ExecutionResult{
		QueryType:        QueryTypeUpdate,
		DMLResult:        dmlResult,
		RowsAffected:     dmlResult.RowsAffected,
		ExecutionTime:    dmlResult.ExecutionTime,
		OptimizationUsed: true,
		AccessMethod:     "STORAGE_INTEGRATED",
	}, nil
}

// executeDeleteQuery 执行DELETE查询
func (eei *ExecutionEngineIntegrator) executeDeleteQuery(
	ctx context.Context,
	optimizedPlan *OptimizedSQLPlan,
) (*ExecutionResult, error) {
	deleteStmt, ok := optimizedPlan.ParsedStatement.(*sqlparser.Delete)
	if !ok {
		return nil, fmt.Errorf("无效的DELETE语句")
	}

	// 使用存储引擎集成的DML执行器
	dmlResult, err := eei.storageIntegratedDML.ExecuteDelete(ctx, deleteStmt, optimizedPlan.SemanticInfo.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("DELETE执行失败: %v", err)
	}

	return &ExecutionResult{
		QueryType:        QueryTypeDelete,
		DMLResult:        dmlResult,
		RowsAffected:     dmlResult.RowsAffected,
		ExecutionTime:    dmlResult.ExecutionTime,
		OptimizationUsed: true,
		AccessMethod:     "STORAGE_INTEGRATED",
	}, nil
}

// buildOptimizedExecutionPlan 构建优化的执行计划
func (eei *ExecutionEngineIntegrator) buildOptimizedExecutionPlan(
	optimizedPlan *OptimizedSQLPlan,
) *OptimizedExecutionPlan {
	return &OptimizedExecutionPlan{
		QueryType:      optimizedPlan.SemanticInfo.QueryType,
		AccessMethod:   optimizedPlan.OptimizedPlan.AccessMethod,
		Table:          optimizedPlan.OptimizedPlan.Table,
		IndexCandidate: optimizedPlan.OptimizedPlan.IndexCandidate,
		CostEstimate:   optimizedPlan.OptimizedPlan.CostEstimate,
		StorageHints:   optimizedPlan.OptimizedPlan.StorageHints,
		Conditions:     optimizedPlan.SemanticInfo.Conditions,
		SelectColumns:  eei.extractSelectColumns(optimizedPlan.SemanticInfo.Columns),
	}
}

// applyStorageHints 应用存储提示
func (eei *ExecutionEngineIntegrator) applyStorageHints(hints *StorageHints) error {
	if hints == nil {
		return nil
	}

	// 应用缓冲池提示
	if hints.BufferPoolHint != "" {
		if err := eei.bufferPoolManager.ApplyHint(hints.BufferPoolHint); err != nil {
			return fmt.Errorf("应用缓冲池提示失败: %v", err)
		}
	}

	// 应用预读提示
	if hints.PrefetchPages && hints.ReadAheadPages > 0 {
		if err := eei.bufferPoolManager.SetReadAheadPages(hints.ReadAheadPages); err != nil {
			return fmt.Errorf("设置预读页面数失败: %v", err)
		}
	}

	return nil
}

// extractSelectColumns 提取SELECT列名
func (eei *ExecutionEngineIntegrator) extractSelectColumns(columns []*metadata.Column) []string {
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.Name
	}
	return names
}

// updateExecutionStatistics 更新执行统计信息
func (eei *ExecutionEngineIntegrator) updateExecutionStatistics(optimizedPlan *OptimizedSQLPlan) {
	eei.Lock()
	defer eei.Unlock()

	eei.executionStats.ExecutedQueries++
	eei.executionStats.OptimizedExecutions++

	// 更新访问方法统计
	if optimizedPlan.OptimizedPlan != nil {
		switch optimizedPlan.OptimizedPlan.AccessMethod {
		case AccessMethodCoveringIndex:
			eei.executionStats.CoveringIndexUsed++
		}
	}
}

// updateExecutionStats 更新执行统计信息
func (eei *ExecutionEngineIntegrator) updateExecutionStats(duration time.Duration) {
	eei.Lock()
	defer eei.Unlock()

	eei.executionStats.TotalExecutionTime += duration
	if eei.executionStats.ExecutedQueries > 0 {
		eei.executionStats.AvgExecutionTime =
			eei.executionStats.TotalExecutionTime / time.Duration(eei.executionStats.ExecutedQueries)
	}
}

// GetExecutionStats 获取执行统计信息
func (eei *ExecutionEngineIntegrator) GetExecutionStats() *ExecutionIntegrationStats {
	eei.RLock()
	defer eei.RUnlock()

	stats := *eei.executionStats
	return &stats
}

// GetCombinedStats 获取综合统计信息
func (eei *ExecutionEngineIntegrator) GetCombinedStats() *CombinedIntegrationStats {
	eei.RLock()
	defer eei.RUnlock()

	// 获取各组件统计信息
	storageStats := eei.storageIntegrator.GetIntegrationStats()
	parserStats := eei.parserIntegrator.GetParserStats()
	executionStats := eei.executionStats

	return &CombinedIntegrationStats{
		StorageStats:       storageStats,
		ParserStats:        parserStats,
		ExecutionStats:     executionStats,
		TotalQueries:       executionStats.ExecutedQueries,
		SuccessRate:        eei.calculateSuccessRate(),
		OverallPerformance: eei.calculateOverallPerformance(),
	}
}

// calculateSuccessRate 计算成功率
func (eei *ExecutionEngineIntegrator) calculateSuccessRate() float64 {
	if eei.executionStats.ExecutedQueries == 0 {
		return 0.0
	}

	parserStats := eei.parserIntegrator.GetParserStats()
	totalErrors := parserStats.ParseErrors + parserStats.OptimizationErrors
	successfulQueries := eei.executionStats.ExecutedQueries - totalErrors

	return float64(successfulQueries) / float64(eei.executionStats.ExecutedQueries)
}

// calculateOverallPerformance 计算整体性能
func (eei *ExecutionEngineIntegrator) calculateOverallPerformance() float64 {
	// 基于索引使用率、缓存命中率等计算整体性能分数
	indexUsageRate := float64(eei.executionStats.IndexScansUsed) /
		float64(eei.executionStats.IndexScansUsed+eei.executionStats.TableScansUsed+1)

	storageStats := eei.storageIntegrator.GetIntegrationStats()
	cacheHitRate := storageStats.CacheHitRate

	// 综合性能分数 (0-100)
	return (indexUsageRate*0.6 + cacheHitRate*0.4) * 100
}

// Close 关闭集成器
func (eei *ExecutionEngineIntegrator) Close() error {
	eei.Lock()
	defer eei.Unlock()

	eei.isInitialized = false
	logger.Info("执行引擎集成器已关闭")
	return nil
}

// OptimizedExecutionPlan 优化的执行计划
type OptimizedExecutionPlan struct {
	QueryType      QueryType
	AccessMethod   AccessMethod
	Table          *metadata.Table
	IndexCandidate *plan.IndexCandidate
	CostEstimate   *plan.CostEstimate
	StorageHints   *StorageHints
	Conditions     []plan.Expression
	SelectColumns  []string
}

// ExecutionResult 执行结果
type ExecutionResult struct {
	QueryType        QueryType
	SelectResult     *engine.SelectResult
	DMLResult        *engine.DMLResult
	RowsAffected     int64
	ExecutionTime    time.Duration
	OptimizationUsed bool
	AccessMethod     string
	IndexUsed        string
}

// CombinedIntegrationStats 综合集成统计信息
type CombinedIntegrationStats struct {
	StorageStats       *IntegrationStats
	ParserStats        *ParserIntegrationStats
	ExecutionStats     *ExecutionIntegrationStats
	TotalQueries       uint64
	SuccessRate        float64
	OverallPerformance float64
}
