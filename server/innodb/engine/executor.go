package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	innodbcommon "github.com/zhukovaskychina/xmysql-server/server/innodb/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// XMySQLExecutor 是 SQL 执行器的核心结构，负责整个 SQL 的解析与执行
// 支持解析 SELECT、DDL、SHOW 等语句，并调用相应执行逻辑
// 注意：实际的算子执行使用volcano_executor.go中的Operator接口和火山模型
// 执行流程：解析 -> 生成逻辑计划 -> 转物理计划 -> 构造执行器 -> 流式迭代执行
// 当前实现简化处理，仅返回模拟执行结果

// XMySQLExecutor SQL执行器结构体
type XMySQLExecutor struct {
	infosSchemaManager metadata.InfoSchemaManager // 信息模式管理器
	conf               *conf.Cfg                  // 配置项
	ctx                *ExecutionContext          // 执行上下文
	results            chan *Result               // 结果通道

	// 管理器组件 - 添加这些字段来访问各个管理器
	optimizerManager  interface{} // 查询优化器管理器
	bufferPoolManager interface{} // 缓冲池管理器
	btreeManager      interface{} // B+树管理器
	tableManager      interface{} // 表管理器

	// 存储引擎相关管理器 - 新增字段
	indexManager        *manager.IndexManager        // 索引管理器
	storageManager      *manager.StorageManager      // 存储管理器
	tableStorageManager *manager.TableStorageManager // 表存储映射管理器
}

// NewXMySQLExecutor 构造 SQL 执行器实例
func NewXMySQLExecutor(infosSchemaManager metadata.InfoSchemaManager, conf *conf.Cfg) *XMySQLExecutor {
	return &XMySQLExecutor{
		infosSchemaManager: infosSchemaManager,
		conf:               conf,
	}
}

// SetManagers 设置管理器组件
func (e *XMySQLExecutor) SetManagers(
	optimizerManager interface{},
	bufferPoolManager interface{},
	btreeManager interface{},
	tableManager interface{},
) {
	e.optimizerManager = optimizerManager
	e.bufferPoolManager = bufferPoolManager
	e.btreeManager = btreeManager
	e.tableManager = tableManager
}

// ExecuteWithQuery 接收原始 SQL 查询，异步执行并返回结果通道
func (e *XMySQLExecutor) ExecuteWithQuery(mysqlSession server.MySQLServerSession, query string, databaseName string) <-chan *Result {
	results := make(chan *Result)
	e.ctx = &ExecutionContext{
		Context:     context.Background(),
		statementId: 0,
		QueryId:     0,
		Results:     results,
		Cfg:         nil,
	}
	go e.executeQuery(e.ctx, mysqlSession, query, databaseName, results)
	return results
}

// executeQuery 是实际的 SQL 执行过程，包括解析和语义分派
func (e *XMySQLExecutor) executeQuery(ctx *ExecutionContext, mysqlSession server.MySQLServerSession, query string, databaseName string, results chan *Result) {
	defer close(results)
	defer e.recover(query, results)

	// SQL语法解析
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		results <- &Result{Err: fmt.Errorf("SQL parse error: %v", err), ResultType: common.RESULT_TYPE_QUERY, Message: "Failed to parse SQL statement"}
		return
	}

	// 根据不同语句类型分派执行
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// 执行SELECT查询
		selectResult, err := e.executeSelectStatement(ctx, stmt, databaseName)
		if err != nil {
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: fmt.Sprintf("SELECT query failed: %v", err)}
		} else {
			// 将SelectResult转换为Result
			result := &Result{
				ResultType: common.RESULT_TYPE_QUERY,
				Data:       selectResult,
				Message:    fmt.Sprintf("SELECT query executed successfully, %d rows returned", selectResult.RowCount),
			}
			results <- result
		}
	case *sqlparser.Insert:
		// 执行INSERT语句
		dmlResult, err := e.executeInsertStatement(ctx, stmt, databaseName)
		if err != nil {
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: fmt.Sprintf("INSERT failed: %v", err)}
		} else {
			result := &Result{
				ResultType: common.RESULT_TYPE_QUERY,
				Data:       dmlResult,
				Message:    dmlResult.Message,
			}
			results <- result
		}
	case *sqlparser.Update:
		// 执行UPDATE语句
		dmlResult, err := e.executeUpdateStatement(ctx, stmt, databaseName)
		if err != nil {
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: fmt.Sprintf("UPDATE failed: %v", err)}
		} else {
			result := &Result{
				ResultType: common.RESULT_TYPE_QUERY,
				Data:       dmlResult,
				Message:    dmlResult.Message,
			}
			results <- result
		}
	case *sqlparser.Delete:
		// 执行DELETE语句
		dmlResult, err := e.executeDeleteStatement(ctx, stmt, databaseName)
		if err != nil {
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: fmt.Sprintf("DELETE failed: %v", err)}
		} else {
			result := &Result{
				ResultType: common.RESULT_TYPE_QUERY,
				Data:       dmlResult,
				Message:    dmlResult.Message,
			}
			results <- result
		}
	case *sqlparser.DDL:
		e.executeDDL(stmt, mysqlSession, databaseName, results)
	case *sqlparser.DBDDL:
		e.executeDBDDL(stmt, results)
	case *sqlparser.Show:
		results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "SHOW statement executed (simplified implementation)"}
	case *sqlparser.Set:
		results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "SET statement executed (simplified implementation)"}
	case *sqlparser.Use:
		// 处理USE语句
		dbName := stmt.DBName.String()
		logger.Debugf(" 处理USE语句: %s", dbName)

		// 设置会话的数据库上下文（如果有会话的话）
		// 注意：这里的mysqlSession可能为nil，需要检查
		if mysqlSession != nil {
			mysqlSession.SetParamByName("database", dbName)
			logger.Debugf(" 会话数据库上下文已设置为: %s", dbName)
		}

		results <- &Result{
			ResultType: common.RESULT_TYPE_QUERY,
			Message:    fmt.Sprintf("Database changed to '%s'", dbName),
		}
	default:
		results <- &Result{Err: fmt.Errorf("unsupported statement type: %T", stmt), ResultType: common.RESULT_TYPE_QUERY, Message: "Unsupported statement type"}
	}
}

// executeDDL 处理 DDL 类型语句，如 CREATE TABLE, DROP TABLE
func (e *XMySQLExecutor) executeDDL(stmt *sqlparser.DDL, mysqlSession server.MySQLServerSession, databaseName string, results chan *Result) {
	// 创建执行上下文
	ctx := &ExecutionContext{
		Context:     context.Background(),
		statementId: 0,
		QueryId:     0,
		Results:     results,
		Cfg:         e.conf,
	}

	// 从会话中获取当前数据库
	currentDB := databaseName
	if currentDB == "" && mysqlSession != nil {
		if dbParam := mysqlSession.GetParamByName("database"); dbParam != nil {
			if db, ok := dbParam.(string); ok {
				currentDB = db
			}
		}
	}

	switch stmt.Action {
	case "create":
		logger.Debugf(" CREATE TABLE使用数据库: %s", currentDB)
		e.executeCreateTableStatement(ctx, currentDB, stmt)
	case "drop":
		logger.Debugf("🗑️ DROP TABLE使用数据库: %s", currentDB)
		e.executeDropTableStatement(ctx, stmt)
	default:
		results <- &Result{Err: fmt.Errorf("unsupported DDL action: %s", stmt.Action), ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Unsupported DDL action: %s", stmt.Action)}
	}
}

// executeDBDDL 处理数据库级的DDL语句，如 CREATE DATABASE
func (e *XMySQLExecutor) executeDBDDL(stmt *sqlparser.DBDDL, results chan *Result) {
	// 创建执行上下文
	ctx := &ExecutionContext{
		Context:     context.Background(),
		statementId: 0,
		QueryId:     0,
		Results:     results,
		Cfg:         e.conf,
	}

	switch stmt.Action {
	case "create":
		e.executeCreateDatabaseStatement(ctx, stmt)
	case "drop":
		e.executeDropDatabaseStatement(ctx, stmt)
	default:
		results <- &Result{Err: fmt.Errorf("unsupported database DDL action: %s", stmt.Action), ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Unsupported database DDL action: %s", stmt.Action)}
	}
}

// buildExecutorTree 从物理计划构建VolcanoExecutor
func (e *XMySQLExecutor) buildExecutorTree(ctx context.Context, physicalPlan plan.PhysicalPlan) (*VolcanoExecutor, error) {
	// 验证管理器实例有效性
	var tableManager *manager.TableManager
	var bufferPoolManager *manager.OptimizedBufferPoolManager
	var storageManager *manager.StorageManager
	var indexManager *manager.IndexManager

	// 类型断言获取管理器
	if e.tableManager != nil {
		if tm, ok := e.tableManager.(*manager.TableManager); ok {
			tableManager = tm
		}
	}
	if e.bufferPoolManager != nil {
		if bpm, ok := e.bufferPoolManager.(*manager.OptimizedBufferPoolManager); ok {
			bufferPoolManager = bpm
		}
	}
	storageManager = e.storageManager
	indexManager = e.indexManager

	// 验证必需的管理器
	if tableManager == nil {
		return nil, fmt.Errorf("tableManager is nil, cannot build executor tree")
	}
	if bufferPoolManager == nil {
		return nil, fmt.Errorf("bufferPoolManager is nil, cannot build executor tree")
	}

	// 创建VolcanoExecutor实例
	volcanoExec := NewVolcanoExecutor(
		tableManager,
		bufferPoolManager,
		storageManager,
		indexManager,
	)

	// 构建算子树
	if err := volcanoExec.BuildFromPhysicalPlan(ctx, physicalPlan); err != nil {
		return nil, fmt.Errorf("failed to build operator tree: %w", err)
	}

	return volcanoExec, nil
}

// recover 用于捕获 panic，避免系统崩溃
func (e *XMySQLExecutor) recover(query string, results chan *Result) {
	if err := recover(); err != nil {
		results <- &Result{StatementID: -1, Err: fmt.Errorf("%s [panic:%v]", query, err)}
	}
}

// executeSelectStatement 执行 SELECT 查询
func (e *XMySQLExecutor) executeSelectStatement(ctx *ExecutionContext, stmt *sqlparser.Select, databaseName string) (*SelectResult, error) {
	// 类型断言获取具体的管理器类型
	var optimizerManager *manager.OptimizerManager
	var bufferPoolManager *manager.OptimizedBufferPoolManager
	var btreeManager basic.BPlusTreeManager // 使用接口类型
	var tableManager *manager.TableManager

	if e.optimizerManager != nil {
		if om, ok := e.optimizerManager.(*manager.OptimizerManager); ok {
			optimizerManager = om
		}
	}
	if e.bufferPoolManager != nil {
		if bpm, ok := e.bufferPoolManager.(*manager.OptimizedBufferPoolManager); ok {
			bufferPoolManager = bpm
		}
	}
	if e.btreeManager != nil {
		// 尝试断言为 basic.BPlusTreeManager 接口
		if btm, ok := e.btreeManager.(basic.BPlusTreeManager); ok {
			btreeManager = btm
		} else if btm, ok := e.btreeManager.(*manager.DefaultBPlusTreeManager); ok {
			// 向后兼容：如果是 DefaultBPlusTreeManager，也接受
			btreeManager = btm
		}
	}
	if e.tableManager != nil {
		if tm, ok := e.tableManager.(*manager.TableManager); ok {
			tableManager = tm
		}
	}

	// 创建SELECT执行器
	selectExecutor := NewSelectExecutor(
		optimizerManager,
		bufferPoolManager,
		btreeManager,
		tableManager,
	)

	// 执行SELECT查询
	result, err := selectExecutor.ExecuteSelect(ctx.Context, stmt, databaseName)
	if err != nil {
		return nil, fmt.Errorf("execute SELECT failed: %v", err)
	}

	return result, nil
}

// generateLogicalPlan 从SQL生成逻辑计划
func (e *XMySQLExecutor) generateLogicalPlan(stmt *sqlparser.Select, databaseName string) (plan.LogicalPlan, error) {
	// 获取优化器管理器
	var optimizerManager *manager.OptimizerManager
	if e.optimizerManager != nil {
		if om, ok := e.optimizerManager.(*manager.OptimizerManager); ok {
			optimizerManager = om
		}
	}

	if optimizerManager == nil {
		return nil, fmt.Errorf("optimizerManager is nil, cannot generate logical plan")
	}

	// TODO: 实现逻辑计划生成
	// 这里需要调用优化器管理器的方法来生成逻辑计划
	return nil, fmt.Errorf("logical plan generation not yet implemented")
}

// optimizeToPhysicalPlan 逻辑计划优化为物理计划
func (e *XMySQLExecutor) optimizeToPhysicalPlan(logicalPlan plan.LogicalPlan) (plan.PhysicalPlan, error) {
	// 获取优化器管理器
	var optimizerManager *manager.OptimizerManager
	if e.optimizerManager != nil {
		if om, ok := e.optimizerManager.(*manager.OptimizerManager); ok {
			optimizerManager = om
		}
	}

	if optimizerManager == nil {
		return nil, fmt.Errorf("optimizerManager is nil, cannot optimize to physical plan")
	}

	logger.Debugf("🔧 开始物理计划优化...")

	// 使用优化器管理器生成物理计划
	physicalPlan, err := e.generatePhysicalPlan(logicalPlan, optimizerManager)
	if err != nil {
		return nil, fmt.Errorf("failed to generate physical plan: %v", err)
	}

	logger.Debugf("✅ 物理计划优化完成")
	return physicalPlan, nil
}

// generatePhysicalPlan 生成物理计划
func (e *XMySQLExecutor) generatePhysicalPlan(logicalPlan plan.LogicalPlan, optimizerManager *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	// 根据逻辑计划类型生成对应的物理计划
	switch lp := logicalPlan.(type) {
	case *plan.LogicalTableScan:
		return e.generatePhysicalTableScan(lp, optimizerManager)

	case *plan.LogicalIndexScan:
		return e.generatePhysicalIndexScan(lp, optimizerManager)

	case *plan.LogicalJoin:
		return e.generatePhysicalJoin(lp, optimizerManager)

	case *plan.LogicalAggregation:
		return e.generatePhysicalAggregation(lp, optimizerManager)

	case *plan.LogicalProjection:
		return e.generatePhysicalProjection(lp, optimizerManager)

	case *plan.LogicalSelection:
		return e.generatePhysicalSelection(lp, optimizerManager)

	default:
		// 简化：不支持的计划类型，返回默认表扫描
		logger.Debugf("Unsupported logical plan type: %T, using default table scan", logicalPlan)
		return &plan.PhysicalTableScan{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
		}, nil
	}
}

// generatePhysicalTableScan 生成物理表扫描计划
func (e *XMySQLExecutor) generatePhysicalTableScan(lp *plan.LogicalTableScan, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理表扫描计划: table=%s", lp.Table.Name)

	return &plan.PhysicalTableScan{
		BasePhysicalPlan: plan.BasePhysicalPlan{},
		Table:            lp.Table,
	}, nil
}

// generatePhysicalIndexScan 生成物理索引扫描计划
func (e *XMySQLExecutor) generatePhysicalIndexScan(lp *plan.LogicalIndexScan, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理索引扫描计划: table=%s, index=%s", lp.Table.Name, lp.Index.Name)

	// 将plan.Index转换为metadata.Index
	metadataIndex := &metadata.Index{
		Name:     lp.Index.Name,
		Columns:  lp.Index.Columns,
		IsUnique: lp.Index.Unique,
	}

	return &plan.PhysicalIndexScan{
		BasePhysicalPlan: plan.BasePhysicalPlan{},
		Table:            lp.Table,
		Index:            metadataIndex,
	}, nil
}

// generatePhysicalJoin 生成物理连接计划
func (e *XMySQLExecutor) generatePhysicalJoin(lp *plan.LogicalJoin, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理连接计划: type=%s", lp.JoinType)

	// 递归生成左右子计划
	children := lp.Children()
	if len(children) < 2 {
		return nil, fmt.Errorf("join plan needs at least 2 children")
	}

	leftPlan, err := e.generatePhysicalPlan(children[0], om)
	if err != nil {
		return nil, fmt.Errorf("failed to generate left plan: %v", err)
	}

	rightPlan, err := e.generatePhysicalPlan(children[1], om)
	if err != nil {
		return nil, fmt.Errorf("failed to generate right plan: %v", err)
	}

	// 选择连接算法（Hash Join, Nested Loop Join, Sort-Merge Join）
	joinAlgorithm := e.chooseJoinAlgorithm(lp, leftPlan, rightPlan, om)

	switch joinAlgorithm {
	case "hash":
		return &plan.PhysicalHashJoin{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
			JoinType:         lp.JoinType,
			Conditions:       lp.Conditions,
			LeftSchema:       lp.LeftSchema,
			RightSchema:      lp.RightSchema,
		}, nil

	case "merge":
		return &plan.PhysicalMergeJoin{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
			JoinType:         lp.JoinType,
			Conditions:       lp.Conditions,
			LeftSchema:       lp.LeftSchema,
			RightSchema:      lp.RightSchema,
		}, nil

	default:
		// 默认使用Hash Join
		return &plan.PhysicalHashJoin{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
			JoinType:         lp.JoinType,
			Conditions:       lp.Conditions,
			LeftSchema:       lp.LeftSchema,
			RightSchema:      lp.RightSchema,
		}, nil
	}
}

// generatePhysicalAggregation 生成物理聚合计划
func (e *XMySQLExecutor) generatePhysicalAggregation(lp *plan.LogicalAggregation, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理聚合计划")

	// 递归生成子计划
	children := lp.Children()
	if len(children) == 0 {
		return nil, fmt.Errorf("aggregation plan has no child")
	}

	childPlan, err := e.generatePhysicalPlan(children[0], om)
	if err != nil {
		return nil, fmt.Errorf("failed to generate child plan: %v", err)
	}

	// 选择聚合算法（Hash Aggregate, Sort Aggregate）
	aggAlgorithm := e.chooseAggregateAlgorithm(lp, childPlan, om)

	switch aggAlgorithm {
	case "hash":
		return &plan.PhysicalHashAgg{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
			GroupByItems:     lp.GroupByItems,
			AggFuncs:         lp.AggFuncs,
		}, nil

	case "stream":
		return &plan.PhysicalStreamAgg{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
			GroupByItems:     lp.GroupByItems,
			AggFuncs:         lp.AggFuncs,
		}, nil

	default:
		// 默认使用Hash Aggregate
		return &plan.PhysicalHashAgg{
			BasePhysicalPlan: plan.BasePhysicalPlan{},
			GroupByItems:     lp.GroupByItems,
			AggFuncs:         lp.AggFuncs,
		}, nil
	}
}

// generatePhysicalProjection 生成物理投影计划
func (e *XMySQLExecutor) generatePhysicalProjection(lp *plan.LogicalProjection, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理投影计划")

	// 递归生成子计划
	children := lp.Children()
	if len(children) == 0 {
		return nil, fmt.Errorf("projection plan has no child")
	}

	_, err := e.generatePhysicalPlan(children[0], om)
	if err != nil {
		return nil, fmt.Errorf("failed to generate child plan: %v", err)
	}

	return &plan.PhysicalProjection{
		BasePhysicalPlan: plan.BasePhysicalPlan{},
		Exprs:            lp.Exprs,
	}, nil
}

// generatePhysicalSelection 生成物理选择计划
func (e *XMySQLExecutor) generatePhysicalSelection(lp *plan.LogicalSelection, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理选择计划")

	// 递归生成子计划
	children := lp.Children()
	if len(children) == 0 {
		return nil, fmt.Errorf("selection plan has no child")
	}

	_, err := e.generatePhysicalPlan(children[0], om)
	if err != nil {
		return nil, fmt.Errorf("failed to generate child plan: %v", err)
	}

	return &plan.PhysicalSelection{
		BasePhysicalPlan: plan.BasePhysicalPlan{},
		Conditions:       lp.Conditions,
	}, nil
}

// generatePhysicalSort 生成物理排序计划
func (e *XMySQLExecutor) generatePhysicalSort(lp *plan.BaseLogicalPlan, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理排序计划")

	// 递归生成子计划
	children := lp.Children()
	if len(children) == 0 {
		return nil, fmt.Errorf("sort plan has no child")
	}

	_, err := e.generatePhysicalPlan(children[0], om)
	if err != nil {
		return nil, fmt.Errorf("failed to generate child plan: %v", err)
	}

	return &plan.PhysicalSort{
		BasePhysicalPlan: plan.BasePhysicalPlan{},
		ByItems:          []plan.ByItem{}, // 简化：空排序项
	}, nil
}

// generatePhysicalLimit 生成物理限制计划
func (e *XMySQLExecutor) generatePhysicalLimit(lp *plan.BaseLogicalPlan, om *manager.OptimizerManager) (plan.PhysicalPlan, error) {
	logger.Debugf("生成物理限制计划")

	// 递归生成子计划
	children := lp.Children()
	if len(children) == 0 {
		return nil, fmt.Errorf("limit plan has no child")
	}

	childPlan, err := e.generatePhysicalPlan(children[0], om)
	if err != nil {
		return nil, fmt.Errorf("failed to generate child plan: %v", err)
	}

	// 简化：直接返回子计划，limit逻辑在执行时处理
	return childPlan, nil
}

// chooseJoinAlgorithm 选择连接算法
func (e *XMySQLExecutor) chooseJoinAlgorithm(lp *plan.LogicalJoin, leftPlan, rightPlan plan.PhysicalPlan, om *manager.OptimizerManager) string {
	// 简化实现：基于表大小选择算法
	// 实际应该使用代价估算

	// 如果有等值连接条件，优先使用Hash Join
	if e.hasEquiJoinCondition(lp.Conditions) {
		return "hash"
	}

	// 否则使用Nested Loop Join
	return "nested_loop"
}

// chooseAggregateAlgorithm 选择聚合算法
func (e *XMySQLExecutor) chooseAggregateAlgorithm(lp *plan.LogicalAggregation, childPlan plan.PhysicalPlan, om *manager.OptimizerManager) string {
	// 简化实现：默认使用Hash Aggregate
	// 实际应该使用代价估算

	// 如果有GROUP BY，使用Hash Aggregate
	if len(lp.GroupByItems) > 0 {
		return "hash"
	}

	// 否则使用Stream Aggregate
	return "stream"
}

// hasEquiJoinCondition 检查是否有等值连接条件
func (e *XMySQLExecutor) hasEquiJoinCondition(conditions []plan.Expression) bool {
	// 简化实现：检查是否有等号条件
	for _, cond := range conditions {
		// TODO: 实际应该解析表达式AST
		condStr := fmt.Sprintf("%v", cond)
		if contains(condStr, "=") && !contains(condStr, "!=") {
			return true
		}
	}
	return false
}

// contains 检查字符串是否包含子串
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

// containsMiddle 检查字符串中间是否包含子串
func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// convertToSelectResult 将Record数组转换为SelectResult
func (e *XMySQLExecutor) convertToSelectResult(records []Record, schema *metadata.Table) (*SelectResult, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}

	// 构建列名和类型
	columnNames := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		columnNames = append(columnNames, col.Name)
	}

	// 转换记录为行数据
	rows := make([][]interface{}, 0, len(records))
	for _, record := range records {
		values := record.GetValues()
		row := make([]interface{}, len(values))
		for i, v := range values {
			row[i] = e.convertValueToInterface(v)
		}
		rows = append(rows, row)
	}

	return &SelectResult{
		Records:  records,
		RowCount: len(rows),
		Columns:  columnNames,
	}, nil
}

// convertValueToInterface 将basic.Value转换为interface{}
func (e *XMySQLExecutor) convertValueToInterface(value basic.Value) interface{} {
	if value.IsNull() {
		return nil
	}

	// 使用Value interface的方法
	switch value.Type() {
	case basic.ValueTypeBigInt, basic.ValueTypeInt, basic.ValueTypeMediumInt, basic.ValueTypeSmallInt, basic.ValueTypeTinyInt:
		return value.Int()
	case basic.ValueTypeFloat, basic.ValueTypeDouble:
		return value.Float64()
	case basic.ValueTypeChar, basic.ValueTypeVarchar, basic.ValueTypeText, basic.ValueTypeMediumText, basic.ValueTypeLongText:
		return value.String()
	case basic.ValueTypeBinary, basic.ValueTypeVarBinary, basic.ValueTypeBlob, basic.ValueTypeMediumBlob, basic.ValueTypeLongBlob:
		return value.Bytes()
	case basic.ValueTypeDate, basic.ValueTypeTime, basic.ValueTypeDateTime, basic.ValueTypeTimestamp:
		return value.Time()
	default:
		return value.Raw()
	}
}

// executeInsertStatement 执行 INSERT 语句
func (e *XMySQLExecutor) executeInsertStatement(ctx *ExecutionContext, stmt *sqlparser.Insert, databaseName string) (*DMLResult, error) {
	// 类型断言获取具体的管理器类型
	var optimizerManager *manager.OptimizerManager
	var bufferPoolManager *manager.OptimizedBufferPoolManager
	var btreeManager basic.BPlusTreeManager
	var tableManager *manager.TableManager

	if e.optimizerManager != nil {
		if om, ok := e.optimizerManager.(*manager.OptimizerManager); ok {
			optimizerManager = om
		}
	}
	if e.bufferPoolManager != nil {
		if bpm, ok := e.bufferPoolManager.(*manager.OptimizedBufferPoolManager); ok {
			bufferPoolManager = bpm
		}
	}
	if e.btreeManager != nil {
		if btm, ok := e.btreeManager.(basic.BPlusTreeManager); ok {
			btreeManager = btm
		} else if btm, ok := e.btreeManager.(*manager.DefaultBPlusTreeManager); ok {
			btreeManager = btm
		}
	}
	if e.tableManager != nil {
		if tm, ok := e.tableManager.(*manager.TableManager); ok {
			tableManager = tm
		}
	}

	// 使用实际的存储管理器字段
	indexManager := e.indexManager
	storageManager := e.storageManager
	tableStorageManager := e.tableStorageManager

	// 根据配置选择DML执行器类型
	useStorageIntegrated := true // 可以从配置中读取

	if useStorageIntegrated && indexManager != nil && storageManager != nil && tableStorageManager != nil {
		logger.Debugf("🚀 Using storage-integrated DML executor for INSERT")

		// 使用存储引擎集成的DML执行器
		storageIntegratedExecutor := NewStorageIntegratedDMLExecutor(
			optimizerManager,
			bufferPoolManager,
			btreeManager,
			tableManager,
			nil, // TODO: 添加事务管理器
			indexManager,
			storageManager,
			tableStorageManager,
		)

		// 执行INSERT语句
		result, err := storageIntegratedExecutor.ExecuteInsert(ctx.Context, stmt, databaseName)
		if err != nil {
			return nil, fmt.Errorf("execute storage-integrated INSERT failed: %v", err)
		}

		return result, nil
	} else {
		logger.Debugf(" Falling back to basic DML executor for INSERT (missing managers: indexManager=%v, storageManager=%v, tableStorageManager=%v)",
			indexManager != nil, storageManager != nil, tableStorageManager != nil)

		// 回退到原有的DML执行器
		dmlExecutor := NewDMLExecutor(
			optimizerManager,
			bufferPoolManager,
			btreeManager,
			tableManager,
			nil,            // TODO: 添加事务管理器
			e.indexManager, // 索引管理器
		)

		// 执行INSERT语句
		result, err := dmlExecutor.ExecuteInsert(ctx.Context, stmt, databaseName)
		if err != nil {
			return nil, fmt.Errorf("execute INSERT failed: %v", err)
		}

		return result, nil
	}
}

// executeUpdateStatement 执行 UPDATE 语句
func (e *XMySQLExecutor) executeUpdateStatement(ctx *ExecutionContext, stmt *sqlparser.Update, databaseName string) (*DMLResult, error) {
	// 类型断言获取具体的管理器类型
	var optimizerManager *manager.OptimizerManager
	var bufferPoolManager *manager.OptimizedBufferPoolManager
	var btreeManager basic.BPlusTreeManager
	var tableManager *manager.TableManager
	var indexManager *manager.IndexManager
	var storageManager *manager.StorageManager
	var tableStorageManager *manager.TableStorageManager

	if e.optimizerManager != nil {
		if om, ok := e.optimizerManager.(*manager.OptimizerManager); ok {
			optimizerManager = om
		}
	}
	if e.bufferPoolManager != nil {
		if bpm, ok := e.bufferPoolManager.(*manager.OptimizedBufferPoolManager); ok {
			bufferPoolManager = bpm
		}
	}
	if e.btreeManager != nil {
		if btm, ok := e.btreeManager.(basic.BPlusTreeManager); ok {
			btreeManager = btm
		} else if btm, ok := e.btreeManager.(*manager.DefaultBPlusTreeManager); ok {
			btreeManager = btm
		}
	}
	if e.tableManager != nil {
		if tm, ok := e.tableManager.(*manager.TableManager); ok {
			tableManager = tm
		}
	}

	// 根据配置选择DML执行器类型
	useStorageIntegrated := true // 可以从配置中读取

	if useStorageIntegrated && indexManager != nil && storageManager != nil && tableStorageManager != nil {
		// 使用存储引擎集成的DML执行器
		storageIntegratedExecutor := NewStorageIntegratedDMLExecutor(
			optimizerManager,
			bufferPoolManager,
			btreeManager,
			tableManager,
			nil, // TODO: 添加事务管理器
			indexManager,
			storageManager,
			tableStorageManager,
		)

		// 执行UPDATE语句
		result, err := storageIntegratedExecutor.ExecuteUpdate(ctx.Context, stmt, databaseName)
		if err != nil {
			return nil, fmt.Errorf("execute storage-integrated UPDATE failed: %v", err)
		}

		return result, nil
	} else {
		// 回退到原有的DML执行器
		dmlExecutor := NewDMLExecutor(
			optimizerManager,
			bufferPoolManager,
			btreeManager,
			tableManager,
			nil,            // TODO: 添加事务管理器
			e.indexManager, // 索引管理器
		)

		// 执行UPDATE语句
		result, err := dmlExecutor.ExecuteUpdate(ctx.Context, stmt, databaseName)
		if err != nil {
			return nil, fmt.Errorf("execute UPDATE failed: %v", err)
		}

		return result, nil
	}
}

// executeDeleteStatement 执行 DELETE 语句
func (e *XMySQLExecutor) executeDeleteStatement(ctx *ExecutionContext, stmt *sqlparser.Delete, databaseName string) (*DMLResult, error) {
	// 类型断言获取具体的管理器类型
	var optimizerManager *manager.OptimizerManager
	var bufferPoolManager *manager.OptimizedBufferPoolManager
	var btreeManager basic.BPlusTreeManager
	var tableManager *manager.TableManager
	var indexManager *manager.IndexManager
	var storageManager *manager.StorageManager
	var tableStorageManager *manager.TableStorageManager

	if e.optimizerManager != nil {
		if om, ok := e.optimizerManager.(*manager.OptimizerManager); ok {
			optimizerManager = om
		}
	}
	if e.bufferPoolManager != nil {
		if bpm, ok := e.bufferPoolManager.(*manager.OptimizedBufferPoolManager); ok {
			bufferPoolManager = bpm
		}
	}
	if e.btreeManager != nil {
		if btm, ok := e.btreeManager.(basic.BPlusTreeManager); ok {
			btreeManager = btm
		} else if btm, ok := e.btreeManager.(*manager.DefaultBPlusTreeManager); ok {
			btreeManager = btm
		}
	}
	if e.tableManager != nil {
		if tm, ok := e.tableManager.(*manager.TableManager); ok {
			tableManager = tm
		}
	}

	// 根据配置选择DML执行器类型
	useStorageIntegrated := true // 可以从配置中读取

	if useStorageIntegrated && indexManager != nil && storageManager != nil && tableStorageManager != nil {
		// 使用存储引擎集成的DML执行器
		storageIntegratedExecutor := NewStorageIntegratedDMLExecutor(
			optimizerManager,
			bufferPoolManager,
			btreeManager,
			tableManager,
			nil, // TODO: 添加事务管理器
			indexManager,
			storageManager,
			tableStorageManager,
		)

		// 执行DELETE语句
		result, err := storageIntegratedExecutor.ExecuteDelete(ctx.Context, stmt, databaseName)
		if err != nil {
			return nil, fmt.Errorf("execute storage-integrated DELETE failed: %v", err)
		}

		return result, nil
	} else {
		// 回退到原有的DML执行器
		dmlExecutor := NewDMLExecutor(
			optimizerManager,
			bufferPoolManager,
			btreeManager,
			tableManager,
			nil,            // TODO: 添加事务管理器
			e.indexManager, // 索引管理器
		)

		// 执行DELETE语句
		result, err := dmlExecutor.ExecuteDelete(ctx.Context, stmt, databaseName)
		if err != nil {
			return nil, fmt.Errorf("execute DELETE failed: %v", err)
		}

		return result, nil
	}
}

// executeCreateDatabaseStatement 执行 CREATE DATABASE
func (e *XMySQLExecutor) executeCreateDatabaseStatement(ctx *ExecutionContext, stmt *sqlparser.DBDDL) {
	logger.Debugf(" Executing CREATE DATABASE: %s", stmt.DBName)

	// 获取SchemaManager (需要从引擎中获取)
	// 这里需要添加SchemaManager到XMySQLExecutor结构体中
	// 暂时使用简化的实现

	// 解析CREATE DATABASE语句的选项
	charset := stmt.Charset
	collation := stmt.Collate
	ifNotExists := stmt.IfExists

	// 设置默认值
	if charset == "" {
		charset = "utf8mb4"
	}
	if collation == "" {
		collation = "utf8mb4_general_ci"
	}

	// 创建数据库目录和元数据
	if err := e.createDatabaseImpl(stmt.DBName, charset, collation, ifNotExists); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("CREATE DATABASE failed: %v", err),
		}
		return
	}

	// 返回成功结果
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_DDL,
		Message:    fmt.Sprintf("Database '%s' created successfully", stmt.DBName),
	}

	logger.Infof(" CREATE DATABASE '%s' executed successfully", stmt.DBName)
}

// createDatabaseImpl 实际的数据库创建实现
func (e *XMySQLExecutor) createDatabaseImpl(dbName, charset, collation string, ifNotExists bool) error {
	// 1. 验证数据库名称
	if err := validateDatabaseName(dbName); err != nil {
		return fmt.Errorf("invalid database name '%s': %v", dbName, err)
	}

	// 2. 获取数据目录
	dataDir := e.conf.DataDir
	if dataDir == "" {
		dataDir = "data" // 默认数据目录
	}

	// 3. 构建数据库路径
	dbPath := filepath.Join(dataDir, dbName)

	// 4. 检查数据库是否已存在
	if _, err := os.Stat(dbPath); err == nil {
		if ifNotExists {
			logger.Debugf("Database '%s' already exists, skipping creation due to IF NOT EXISTS", dbName)
			return nil
		}
		return fmt.Errorf("database '%s' already exists", dbName)
	}

	// 5. 创建数据库目录
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create database directory '%s': %v", dbPath, err)
	}

	// 6. 创建数据库元数据文件 (db.opt)
	if err := createDatabaseMetadataFile(dbPath, charset, collation); err != nil {
		// 回滚：删除已创建的目录
		os.RemoveAll(dbPath)
		return fmt.Errorf("failed to create database metadata: %v", err)
	}

	logger.Infof("📂 Created database directory: %s", dbPath)
	return nil
}

// validateDatabaseName 验证数据库名称
func validateDatabaseName(name string) error {
	// 1. 检查长度
	if len(name) == 0 {
		return fmt.Errorf("database name cannot be empty")
	}
	if len(name) > 64 {
		return fmt.Errorf("database name too long (max 64 characters)")
	}

	// 2. 检查字符合法性 (MySQL标准)
	for i, char := range name {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_' || char == '$') {
			return fmt.Errorf("database name contains invalid character at position %d: '%c'", i, char)
		}
	}

	// 3. 检查是否以数字开头
	if name[0] >= '0' && name[0] <= '9' {
		return fmt.Errorf("database name cannot start with a number")
	}

	// 4. 检查保留字
	reservedWords := []string{
		"information_schema", "mysql", "performance_schema", "sys",
	}
	lowerName := strings.ToLower(name)
	for _, reserved := range reservedWords {
		if lowerName == reserved {
			return fmt.Errorf("'%s' is a reserved database name", name)
		}
	}

	return nil
}

// createDatabaseMetadataFile 创建数据库元数据文件
func createDatabaseMetadataFile(dbPath, charset, collation string) error {
	// 创建 db.opt 文件 (MySQL兼容格式)
	dbOptPath := filepath.Join(dbPath, "db.opt")
	dbOptContent := fmt.Sprintf("default-character-set=%s\ndefault-collation=%s\n", charset, collation)

	if err := ioutil.WriteFile(dbOptPath, []byte(dbOptContent), 0644); err != nil {
		return fmt.Errorf("failed to create db.opt file: %v", err)
	}

	logger.Debugf(" Created database metadata file: %s", dbOptPath)
	return nil
}

// buildWhereConditions 构建 WHERE 条件表达式（占位）
func (e *XMySQLExecutor) buildWhereConditions(where *sqlparser.Where) {}

// executeSetStatement 执行 SET 语句
func (e *XMySQLExecutor) executeSetStatement(ctx *ExecutionContext, stmt *sqlparser.Set, session server.MySQLServerSession) {
	if ctx == nil || ctx.Results == nil {
		logger.Errorf(" [executeSetStatement] execution context is not initialized")
		return
	}

	logger.Debugf(" [executeSetStatement] 执行SET语句，包含 %d 个表达式", len(stmt.Exprs))

	if session == nil {
		logger.Errorf(" [executeSetStatement] session is nil, 无法设置会话变量")
		ctx.Results <- &Result{
			Err:        fmt.Errorf("session is required for SET statements"),
			ResultType: innodbcommon.RESULT_TYPE_ERROR,
			Message:    "session unavailable for SET statement",
		}
		return
	}

	affectedVars := 0
	var errors []string

	for _, expr := range stmt.Exprs {
		varName := expr.Name.String()
		logger.Debugf(" [executeSetStatement] 处理变量: %s", varName)

		value, err := e.evaluateSetValue(expr.Expr)
		if err != nil {
			errMsg := fmt.Sprintf("failed to evaluate value for %s: %v", varName, err)
			logger.Errorf(" [executeSetStatement] %s", errMsg)
			errors = append(errors, errMsg)
			continue
		}

		if err := e.setSessionVariable(session, varName, value); err != nil {
			errMsg := fmt.Sprintf("failed to set %s: %v", varName, err)
			logger.Warnf(" [executeSetStatement] %s", errMsg)
			errors = append(errors, errMsg)
			continue
		}

		affectedVars++
		logger.Debugf(" [executeSetStatement] 成功设置变量: %s = %v", varName, value)
	}

	if affectedVars == 0 && len(errors) > 0 {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("SET statement failed: %s", strings.Join(errors, "; ")),
			ResultType: innodbcommon.RESULT_TYPE_ERROR,
			Message:    "SET statement failed",
		}
		return
	}

	message := fmt.Sprintf("SET statement executed, %d variables processed", affectedVars)
	if len(errors) > 0 {
		message = fmt.Sprintf("%s (%d warnings: %s)", message, len(errors), strings.Join(errors, "; "))
	}

	ctx.Results <- &Result{
		ResultType: innodbcommon.RESULT_TYPE_SET,
		Message:    message,
	}
	logger.Debugf(" [executeSetStatement] SET语句执行完成: %s", message)
}

// evaluateSetValue 计算 SET 表达式的值
func (e *XMySQLExecutor) evaluateSetValue(expr sqlparser.Expr) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.IntVal:
			val, err := strconv.ParseInt(string(v.Val), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid integer value '%s'", string(v.Val))
			}
			return val, nil
		case sqlparser.FloatVal:
			val, err := strconv.ParseFloat(string(v.Val), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float value '%s'", string(v.Val))
			}
			return val, nil
		case sqlparser.StrVal:
			return string(v.Val), nil
		default:
			return string(v.Val), nil
		}
	case sqlparser.BoolVal:
		if bool(v) {
			return int64(1), nil
		}
		return int64(0), nil
	case *sqlparser.NullVal:
		return nil, nil
	case *sqlparser.ColName:
		return v.Name.String(), nil
	case *sqlparser.ParenExpr:
		return e.evaluateSetValue(v.Expr)
	default:
		return sqlparser.String(expr), nil
	}
}

// setSessionVariable 设置会话变量
func (e *XMySQLExecutor) setSessionVariable(session server.MySQLServerSession, name string, value interface{}) error {
	if session == nil {
		return fmt.Errorf("session is nil")
	}

	cleanName := strings.TrimSpace(name)
	cleanName = strings.Trim(cleanName, "`")
	cleanName = strings.TrimPrefix(cleanName, "@@")
	cleanName = strings.TrimPrefix(cleanName, "session.")
	cleanName = strings.TrimPrefix(cleanName, "global.")
	cleanName = strings.ToLower(cleanName)

	if cleanName == "" {
		return fmt.Errorf("variable name cannot be empty")
	}

	logger.Debugf(" [setSessionVariable] 设置变量: %s = %v (type=%T)", cleanName, value, value)

	switch cleanName {
	case "autocommit":
		session.SetParamByName(cleanName, fmt.Sprintf("%d", boolishToInt(value)))
	case "names":
		charset := fmt.Sprintf("%v", value)
		if charset == "" {
			charset = "utf8mb4"
		}
		session.SetParamByName("character_set_client", charset)
		session.SetParamByName("character_set_connection", charset)
		session.SetParamByName("character_set_results", charset)
	case "character_set_client", "character_set_connection", "character_set_results",
		"character_set_database", "character_set_server":
		session.SetParamByName(cleanName, fmt.Sprintf("%v", value))
	case "sql_mode", "time_zone", "transaction_isolation", "tx_isolation",
		"net_write_timeout", "net_read_timeout", "max_allowed_packet":
		session.SetParamByName(cleanName, fmt.Sprintf("%v", value))
	default:
		session.SetParamByName(cleanName, fmt.Sprintf("%v", value))
	}

	return nil
}

func boolishToInt(value interface{}) int64 {
	switch v := value.(type) {
	case int64:
		if v != 0 {
			return 1
		}
		return 0
	case int:
		if v != 0 {
			return 1
		}
		return 0
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "on", "true", "enabled":
			return 1
		default:
			return 0
		}
	case nil:
		return 0
	default:
		return 0
	}
}

// executeCreateTableStatement 执行 CREATE TABLE
func (e *XMySQLExecutor) executeCreateTableStatement(ctx *ExecutionContext, databaseName string, stmt *sqlparser.DDL) {
	logger.Debugf(" Executing CREATE TABLE: %s", stmt.Table.Name.String())
	logger.Debugf(" [executeCreateTableStatement] DDL语句详细信息:")
	logger.Debugf("   - Action: %s", stmt.Action)
	logger.Debugf("   - Table.Name: '%s'", stmt.Table.Name.String())
	logger.Debugf("   - Table.Qualifier: '%s'", stmt.Table.Qualifier.String())
	logger.Debugf("   - NewName: '%s'", stmt.NewName.Name.String())
	logger.Debugf("   - IfExists: %v", stmt.IfExists)
	logger.Debugf("   - TableSpec: %v", stmt.TableSpec != nil)

	// 1. 获取当前数据库名称
	currentDB := databaseName
	if currentDB == "" {
		// 如果没有指定数据库，尝试从表名中解析
		if stmt.Table.Qualifier.String() != "" {
			currentDB = stmt.Table.Qualifier.String()
		} else {
			ctx.Results <- &Result{
				Err:        fmt.Errorf("no database selected"),
				ResultType: common.RESULT_TYPE_DDL,
				Message:    "CREATE TABLE failed: no database selected",
			}
			return
		}
	}

	// 2. 验证数据库是否存在
	if err := e.validateDatabaseExists(currentDB); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("CREATE TABLE failed: %v", err),
		}
		return
	}

	// 3. 解析表名
	tableName := stmt.Table.Name.String()
	if tableName == "" {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("table name cannot be empty"),
			ResultType: common.RESULT_TYPE_DDL,
			Message:    "CREATE TABLE failed: table name cannot be empty",
		}
		return
	}

	// 4. 检查表是否已存在
	if exists, err := e.checkTableExists(currentDB, tableName); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("CREATE TABLE failed: %v", err),
		}
		return
	} else if exists {
		if stmt.IfExists {
			logger.Debugf("Table '%s.%s' already exists, skipping creation due to IF NOT EXISTS", currentDB, tableName)
			ctx.Results <- &Result{
				ResultType: common.RESULT_TYPE_DDL,
				Message:    fmt.Sprintf("Table '%s' already exists", tableName),
			}
			return
		} else {
			ctx.Results <- &Result{
				Err:        fmt.Errorf("table '%s' already exists", tableName),
				ResultType: common.RESULT_TYPE_DDL,
				Message:    fmt.Sprintf("CREATE TABLE failed: table '%s' already exists", tableName),
			}
			return
		}
	}

	// 5. 创建表实现
	if err := e.createTableImpl(currentDB, tableName, stmt); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("CREATE TABLE failed: %v", err),
		}
		return
	}

	// 6. 创建表存储映射
	if err := e.createTableStorageMapping(currentDB, tableName); err != nil {
		// 回滚：删除已创建的表文件
		e.dropTableImpl(currentDB, tableName)
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("CREATE TABLE failed: %v", err),
		}
		return
	}

	// 7. 返回成功结果
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_DDL,
		Message:    fmt.Sprintf("Table '%s' created successfully", tableName),
	}

	logger.Infof(" CREATE TABLE '%s.%s' executed successfully", currentDB, tableName)
}

// createTableStorageMapping 创建表存储映射
func (e *XMySQLExecutor) createTableStorageMapping(dbName, tableName string) error {
	// 获取存储管理器
	storageManager := e.storageManager
	if storageManager == nil {
		return fmt.Errorf("storage manager not available")
	}

	// 创建表空间名称
	spaceName := fmt.Sprintf("%s/%s", dbName, tableName)

	// 创建表空间
	handle, err := storageManager.CreateTablespace(spaceName)
	if err != nil {
		return fmt.Errorf("failed to create tablespace: %v", err)
	}

	// 获取表存储映射管理器
	tableStorageManager := e.tableStorageManager
	if tableStorageManager == nil {
		return fmt.Errorf("table storage manager not available")
	}

	// 创建表存储信息
	info := &manager.TableStorageInfo{
		SchemaName:    dbName,
		TableName:     tableName,
		SpaceID:       handle.SpaceID,
		RootPageNo:    3, // 默认根页面号
		IndexPageNo:   3, // 默认索引页面号
		DataSegmentID: handle.DataSegmentID,
		Type:          manager.TableTypeUser,
	}

	// 注册表存储信息
	if err := tableStorageManager.RegisterTable(context.Background(), info); err != nil {
		return fmt.Errorf("failed to register table storage: %v", err)
	}

	return nil
}

// executeDropTableStatement 执行 DROP TABLE
func (e *XMySQLExecutor) executeDropTableStatement(ctx *ExecutionContext, stmt *sqlparser.DDL) {
	logger.Debugf("🗑️ Executing DROP TABLE: %s", stmt.Table.Name.String())

	// 1. 解析表名和数据库名
	tableName := stmt.Table.Name.String()
	databaseName := stmt.Table.Qualifier.String()

	if tableName == "" {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("table name cannot be empty"),
			ResultType: common.RESULT_TYPE_DDL,
			Message:    "DROP TABLE failed: table name cannot be empty",
		}
		return
	}

	// 2. 如果没有指定数据库，需要有当前数据库上下文
	if databaseName == "" {
		// 这里应该从会话中获取当前数据库，暂时使用默认逻辑
		ctx.Results <- &Result{
			Err:        fmt.Errorf("no database selected"),
			ResultType: common.RESULT_TYPE_DDL,
			Message:    "DROP TABLE failed: no database selected",
		}
		return
	}

	// 3. 验证数据库是否存在
	if err := e.validateDatabaseExists(databaseName); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("DROP TABLE failed: %v", err),
		}
		return
	}

	// 4. 检查表是否存在
	exists, err := e.checkTableExists(databaseName, tableName)
	if err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("DROP TABLE failed: %v", err),
		}
		return
	}

	if !exists {
		if stmt.IfExists {
			logger.Debugf("Table '%s.%s' does not exist, skipping drop due to IF EXISTS", databaseName, tableName)
			ctx.Results <- &Result{
				ResultType: common.RESULT_TYPE_DDL,
				Message:    fmt.Sprintf("Table '%s' does not exist", tableName),
			}
			return
		} else {
			ctx.Results <- &Result{
				Err:        fmt.Errorf("table '%s' does not exist", tableName),
				ResultType: common.RESULT_TYPE_DDL,
				Message:    fmt.Sprintf("DROP TABLE failed: table '%s' does not exist", tableName),
			}
			return
		}
	}

	// 5. 删除表实现
	if err := e.dropTableImpl(databaseName, tableName); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("DROP TABLE failed: %v", err),
		}
		return
	}

	// 6. 返回成功结果
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_DDL,
		Message:    fmt.Sprintf("Table '%s' dropped successfully", tableName),
	}

	logger.Infof(" DROP TABLE '%s.%s' executed successfully", databaseName, tableName)
}

// PhysicalPlan 是逻辑计划转换后的物理执行计划（别名）
type PhysicalPlan = plan.PhysicalPlan

// InfoSchemaAdapter在select_executor.go中已定义

// OptimizeLogicalPlan 对逻辑计划进行优化（简化实现）
func OptimizeLogicalPlan(logicalPlan plan.LogicalPlan) plan.LogicalPlan {
	return logicalPlan
}

// BuildShowPlan 构建 SHOW 语句的逻辑计划（简化实现）
func BuildShowPlan(stmt *sqlparser.Show) (plan.LogicalPlan, error) {
	return nil, fmt.Errorf("SHOW statements not implemented yet")
}

// SetAdditionalManagers 设置额外的管理器组件（用于存储引擎集成）
func (e *XMySQLExecutor) SetAdditionalManagers(
	indexManager *manager.IndexManager,
	storageManager *manager.StorageManager,
	tableStorageManager *manager.TableStorageManager,
) {
	// 存储额外的管理器，以便DML执行器可以访问
	e.indexManager = indexManager
	e.storageManager = storageManager
	e.tableStorageManager = tableStorageManager

	logger.Debugf(" Additional managers set: IndexManager=%v, StorageManager=%v, TableStorageManager=%v",
		indexManager != nil, storageManager != nil, tableStorageManager != nil)
}

// executeDropDatabaseStatement 执行 DROP DATABASE
func (e *XMySQLExecutor) executeDropDatabaseStatement(ctx *ExecutionContext, stmt *sqlparser.DBDDL) {
	logger.Debugf("🗑️ Executing DROP DATABASE: %s", stmt.DBName)

	// 解析DROP DATABASE语句的选项
	ifExists := stmt.IfExists

	// 删除数据库
	if err := e.dropDatabaseImpl(stmt.DBName, ifExists); err != nil {
		ctx.Results <- &Result{
			Err:        err,
			ResultType: common.RESULT_TYPE_DDL,
			Message:    fmt.Sprintf("DROP DATABASE failed: %v", err),
		}
		return
	}

	// 返回成功结果
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_DDL,
		Message:    fmt.Sprintf("Database '%s' dropped successfully", stmt.DBName),
	}

	logger.Infof(" DROP DATABASE '%s' executed successfully", stmt.DBName)
}

// dropDatabaseImpl 实际的数据库删除实现
func (e *XMySQLExecutor) dropDatabaseImpl(dbName string, ifExists bool) error {
	// 1. 检查是否为系统数据库
	if isSystemDatabase(dbName) {
		return fmt.Errorf("cannot drop system database '%s'", dbName)
	}

	// 2. 获取数据目录
	dataDir := e.conf.DataDir
	if dataDir == "" {
		dataDir = "data" // 默认数据目录
	}

	// 3. 构建数据库路径
	dbPath := filepath.Join(dataDir, dbName)

	// 4. 检查数据库是否存在
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if ifExists {
			logger.Debugf("Database '%s' does not exist, skipping drop due to IF EXISTS", dbName)
			return nil
		}
		return fmt.Errorf("database '%s' does not exist", dbName)
	}

	// 5. 删除数据库目录
	if err := os.RemoveAll(dbPath); err != nil {
		return fmt.Errorf("failed to remove database directory '%s': %v", dbPath, err)
	}

	logger.Infof("📂 Removed database directory: %s", dbPath)
	return nil
}

// isSystemDatabase 检查是否为系统数据库
func isSystemDatabase(name string) bool {
	systemDatabases := []string{
		"information_schema",
		"mysql",
		"performance_schema",
		"sys",
	}

	lowerName := strings.ToLower(name)
	for _, sysDB := range systemDatabases {
		if lowerName == sysDB {
			return true
		}
	}
	return false
}

// validateDatabaseExists 验证数据库是否存在
func (e *XMySQLExecutor) validateDatabaseExists(dbName string) error {
	// 获取数据目录
	dataDir := e.conf.DataDir
	if dataDir == "" {
		dataDir = "data"
	}

	// 构建数据库路径
	dbPath := filepath.Join(dataDir, dbName)

	// 检查数据库目录是否存在
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database '%s' does not exist", dbName)
	}

	return nil
}

// checkTableExists 检查表是否存在
func (e *XMySQLExecutor) checkTableExists(dbName, tableName string) (bool, error) {
	// 获取数据目录
	dataDir := e.conf.DataDir
	if dataDir == "" {
		dataDir = "data"
	}

	// 构建表文件路径 (.frm文件或.ibd文件)
	dbPath := filepath.Join(dataDir, dbName)
	frmPath := filepath.Join(dbPath, tableName+".frm")
	ibdPath := filepath.Join(dbPath, tableName+".ibd")

	// 检查是否存在.frm文件或.ibd文件
	if _, err := os.Stat(frmPath); err == nil {
		return true, nil
	}
	if _, err := os.Stat(ibdPath); err == nil {
		return true, nil
	}

	return false, nil
}

// createTableImpl 实际的表创建实现
func (e *XMySQLExecutor) createTableImpl(dbName, tableName string, stmt *sqlparser.DDL) error {
	logger.Debugf(" Creating table %s.%s", dbName, tableName)

	// 获取数据目录
	dataDir := e.conf.DataDir
	if dataDir == "" {
		dataDir = "data"
	}

	dbPath := filepath.Join(dataDir, dbName)

	// 1. 创建表结构文件 (.frm)
	if err := e.createTableStructureFile(dbPath, tableName, stmt); err != nil {
		return fmt.Errorf("failed to create table structure file: %v", err)
	}

	// 2. 创建表数据文件 (.ibd)
	if err := e.createTableDataFile(dbPath, tableName); err != nil {
		// 回滚：删除已创建的.frm文件
		os.Remove(filepath.Join(dbPath, tableName+".frm"))
		return fmt.Errorf("failed to create table data file: %v", err)
	}

	logger.Infof(" Created table files for %s.%s", dbName, tableName)
	return nil
}

// dropTableImpl 实际的表删除实现
func (e *XMySQLExecutor) dropTableImpl(dbName, tableName string) error {
	logger.Debugf("🗑️ Dropping table %s.%s", dbName, tableName)

	// 获取数据目录
	dataDir := e.conf.DataDir
	if dataDir == "" {
		dataDir = "data"
	}

	dbPath := filepath.Join(dataDir, dbName)

	// 删除表相关文件
	filesToDelete := []string{
		filepath.Join(dbPath, tableName+".frm"), // 表结构文件
		filepath.Join(dbPath, tableName+".ibd"), // 表数据文件
		filepath.Join(dbPath, tableName+".MYD"), // MyISAM数据文件
		filepath.Join(dbPath, tableName+".MYI"), // MyISAM索引文件
	}

	var errors []string
	for _, filePath := range filesToDelete {
		if _, err := os.Stat(filePath); err == nil {
			if err := os.Remove(filePath); err != nil {
				errors = append(errors, fmt.Sprintf("failed to remove %s: %v", filePath, err))
			} else {
				logger.Debugf("🗑️ Removed file: %s", filePath)
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("errors during table drop: %s", strings.Join(errors, "; "))
	}

	logger.Infof("🗑️ Dropped table %s.%s", dbName, tableName)
	return nil
}

// createTableStructureFile 创建表结构文件 (.frm)
func (e *XMySQLExecutor) createTableStructureFile(dbPath, tableName string, stmt *sqlparser.DDL) error {
	frmPath := filepath.Join(dbPath, tableName+".frm")

	// 构建表结构信息
	tableInfo := map[string]interface{}{
		"table_name": tableName,
		"columns":    e.parseTableColumns(stmt.TableSpec),
		"indexes":    e.parseTableIndexes(stmt.TableSpec),
		"options":    e.parseTableOptions(stmt.TableSpec),
		"created_at": time.Now().Format(time.RFC3339),
	}

	// 序列化为JSON
	data, err := json.MarshalIndent(tableInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize table structure: %v", err)
	}

	// 写入文件
	if err := ioutil.WriteFile(frmPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write .frm file: %v", err)
	}

	logger.Debugf(" Created table structure file: %s", frmPath)
	return nil
}

// createTableDataFile 创建表数据文件 (.ibd)
func (e *XMySQLExecutor) createTableDataFile(dbPath, tableName string) error {
	ibdPath := filepath.Join(dbPath, tableName+".ibd")

	// 创建空的数据文件
	file, err := os.Create(ibdPath)
	if err != nil {
		return fmt.Errorf("failed to create .ibd file: %v", err)
	}
	defer file.Close()

	// 写入基本的InnoDB页头信息（简化版本）
	header := make([]byte, 16384)     // 16KB页大小
	copy(header[0:4], []byte("IBDT")) // InnoDB数据文件标识

	if _, err := file.Write(header); err != nil {
		return fmt.Errorf("failed to write .ibd header: %v", err)
	}

	logger.Debugf(" Created table data file: %s", ibdPath)
	return nil
}

// parseTableColumns 解析表列定义
func (e *XMySQLExecutor) parseTableColumns(spec *sqlparser.TableSpec) []map[string]interface{} {
	if spec == nil {
		return nil
	}

	var columns []map[string]interface{}
	for _, col := range spec.Columns {
		column := map[string]interface{}{
			"name":     col.Name.String(),
			"type":     col.Type.Type,
			"length":   col.Type.Length,
			"scale":    col.Type.Scale,
			"unsigned": col.Type.Unsigned,
			"zerofill": col.Type.Zerofill,
			"nullable": !col.Type.NotNull, // NotNull是BoolVal类型
			"charset":  col.Type.Charset,
			"collate":  col.Type.Collate,
		}

		// 解析列选项
		if col.Type.Autoincrement {
			column["auto_increment"] = true
		}

		// 解析默认值
		if col.Type.Default != nil {
			column["default"] = sqlparser.String(col.Type.Default)
		}

		// 解析ON UPDATE
		if col.Type.OnUpdate != nil {
			column["on_update"] = sqlparser.String(col.Type.OnUpdate)
		}

		// 解析注释
		if col.Type.Comment != nil {
			column["comment"] = sqlparser.String(col.Type.Comment)
		}

		// 解析键选项
		switch col.Type.KeyOpt {
		case 1: // colKeyPrimary
			column["key"] = "PRIMARY"
		case 2: // colKeyUnique
			column["key"] = "UNIQUE"
		case 3: // colKeyUniqueKey
			column["key"] = "UNIQUE KEY"
		case 4: // colKeySpatialKey
			column["key"] = "SPATIAL KEY"
		case 5: // colKey
			column["key"] = "KEY"
		}

		// 解析枚举值
		if len(col.Type.EnumValues) > 0 {
			column["enum_values"] = col.Type.EnumValues
		}

		columns = append(columns, column)
	}

	return columns
}

// parseTableIndexes 解析表索引定义
func (e *XMySQLExecutor) parseTableIndexes(spec *sqlparser.TableSpec) []map[string]interface{} {
	if spec == nil {
		return nil
	}

	var indexes []map[string]interface{}
	for _, idx := range spec.Indexes {
		index := map[string]interface{}{
			"name":    idx.Info.Name.String(),
			"type":    idx.Info.Type,
			"unique":  idx.Info.Unique,
			"primary": idx.Info.Primary,
			"columns": make([]string, 0),
		}

		// 解析索引列
		for _, col := range idx.Columns {
			index["columns"] = append(index["columns"].([]string), col.Column.String())
		}

		indexes = append(indexes, index)
	}

	return indexes
}

// parseTableOptions 解析表选项
func (e *XMySQLExecutor) parseTableOptions(spec *sqlparser.TableSpec) map[string]interface{} {
	options := make(map[string]interface{})

	if spec == nil {
		return options
	}

	// 设置默认选项
	options["engine"] = "InnoDB"
	options["charset"] = "utf8mb4"
	options["collation"] = "utf8mb4_general_ci"

	// 解析表选项（如果有的话）
	if spec.Options != "" {
		// 这里可以解析ENGINE、CHARSET等选项
		// 简化实现，使用默认值
		options["raw_options"] = spec.Options
	}

	return options
}

// executeShowStatement 执行 SHOW 语句
func (e *XMySQLExecutor) executeShowStatement(ctx *ExecutionContext, stmt *sqlparser.Show, session server.MySQLServerSession) {
	logger.Debugf(" [executeShowStatement] 处理SHOW语句: %s", stmt.Type)

	showType := strings.ToLower(stmt.Type)

	switch showType {
	case "databases":
		e.executeShowDatabases(ctx)
	case "tables":
		e.executeShowTables(ctx, session)
	case "columns", "fields":
		e.executeShowColumns(ctx, stmt)
	case "variables":
		e.executeShowVariables(ctx, stmt)
	case "status":
		e.executeShowStatus(ctx, stmt)
	case "create table":
		e.executeShowCreateTable(ctx, stmt)
	case "engines":
		e.executeShowEngines(ctx)
	case "warnings":
		e.executeShowWarnings(ctx)
	case "errors":
		e.executeShowErrors(ctx)
	default:
		logger.Warnf(" [executeShowStatement] 不支持的SHOW类型: %s", showType)
		ctx.Results <- &Result{
			Err:        fmt.Errorf("unsupported SHOW type: %s", showType),
			ResultType: "ERROR",
		}
	}
}

// executeShowDatabases 执行 SHOW DATABASES
func (e *XMySQLExecutor) executeShowDatabases(ctx *ExecutionContext) {
	logger.Debugf(" [executeShowDatabases] 执行SHOW DATABASES")

	// 获取数据目录
	dataDir := e.conf.DataDir
	if dataDir == "" {
		dataDir = "data"
	}

	// 读取数据目录下的所有子目录（每个子目录代表一个数据库）
	var databases []string

	// 添加系统数据库
	databases = append(databases, "information_schema", "mysql", "performance_schema", "sys")

	// 读取用户数据库
	if entries, err := os.ReadDir(dataDir); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				dbName := entry.Name()
				// 过滤掉隐藏目录和系统目录
				if !strings.HasPrefix(dbName, ".") && !strings.HasPrefix(dbName, "_") {
					databases = append(databases, dbName)
				}
			}
		}
	}

	// 构造结果集
	rows := make([][]interface{}, len(databases))
	for i, db := range databases {
		rows[i] = []interface{}{db}
	}

	// 使用 Data 字段存储结果，格式为 map
	resultData := map[string]interface{}{
		"columns": []string{"Database"},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
		Message:    fmt.Sprintf("Found %d databases", len(databases)),
	}

	logger.Debugf(" [executeShowDatabases] 返回 %d 个数据库", len(databases))
}

// executeShowTables 执行 SHOW TABLES
func (e *XMySQLExecutor) executeShowTables(ctx *ExecutionContext, session server.MySQLServerSession) {
	logger.Debugf(" [executeShowTables] 执行SHOW TABLES")

	// 获取当前数据库
	currentDB := session.GetParamByName("database")
	if currentDB == "" {
		ctx.Results <- &Result{
			Err:        fmt.Errorf("no database selected"),
			ResultType: "ERROR",
		}
		return
	}

	// 从SchemaManager获取表列表
	tables := []string{}

	// 这里简化实现，返回空列表
	// 实际应该从 SchemaManager 获取
	logger.Debugf(" [executeShowTables] 当前数据库: %s", currentDB)

	// 构造结果集
	rows := make([][]interface{}, len(tables))
	for i, table := range tables {
		rows[i] = []interface{}{table}
	}

	columnName := fmt.Sprintf("Tables_in_%s", currentDB)
	resultData := map[string]interface{}{
		"columns": []string{columnName},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
		Message:    fmt.Sprintf("Found %d tables", len(tables)),
	}

	logger.Debugf(" [executeShowTables] 返回 %d 个表", len(tables))
}

// executeShowColumns 执行 SHOW COLUMNS
func (e *XMySQLExecutor) executeShowColumns(ctx *ExecutionContext, stmt *sqlparser.Show) {
	logger.Debugf(" [executeShowColumns] 执行SHOW COLUMNS")

	// 简化实现，返回空结果
	resultData := map[string]interface{}{
		"columns": []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
		"rows":    [][]interface{}{},
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
	}
}

// executeShowVariables 执行 SHOW VARIABLES
func (e *XMySQLExecutor) executeShowVariables(ctx *ExecutionContext, stmt *sqlparser.Show) {
	logger.Debugf(" [executeShowVariables] 执行SHOW VARIABLES")

	// 简化实现，返回一些常见变量
	rows := [][]interface{}{
		{"character_set_client", "utf8mb4"},
		{"character_set_connection", "utf8mb4"},
		{"character_set_database", "utf8mb4"},
		{"character_set_results", "utf8mb4"},
		{"character_set_server", "utf8mb4"},
		{"collation_connection", "utf8mb4_general_ci"},
		{"collation_database", "utf8mb4_general_ci"},
		{"collation_server", "utf8mb4_general_ci"},
		{"version", "8.0.0-xmysql"},
		{"version_comment", "XMySQL Server"},
	}

	resultData := map[string]interface{}{
		"columns": []string{"Variable_name", "Value"},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
	}
}

// executeShowStatus 执行 SHOW STATUS
func (e *XMySQLExecutor) executeShowStatus(ctx *ExecutionContext, stmt *sqlparser.Show) {
	logger.Debugf(" [executeShowStatus] 执行SHOW STATUS")

	// 简化实现，返回一些状态变量
	rows := [][]interface{}{
		{"Threads_connected", "1"},
		{"Uptime", "3600"},
		{"Questions", "100"},
	}

	resultData := map[string]interface{}{
		"columns": []string{"Variable_name", "Value"},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
	}
}

// executeShowCreateTable 执行 SHOW CREATE TABLE
func (e *XMySQLExecutor) executeShowCreateTable(ctx *ExecutionContext, stmt *sqlparser.Show) {
	logger.Debugf(" [executeShowCreateTable] 执行SHOW CREATE TABLE")

	// 简化实现
	ctx.Results <- &Result{
		Err:        fmt.Errorf("SHOW CREATE TABLE not yet fully implemented"),
		ResultType: "ERROR",
	}
}

// executeShowEngines 执行 SHOW ENGINES
func (e *XMySQLExecutor) executeShowEngines(ctx *ExecutionContext) {
	logger.Debugf(" [executeShowEngines] 执行SHOW ENGINES")

	rows := [][]interface{}{
		{"InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"},
	}

	resultData := map[string]interface{}{
		"columns": []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"},
		"rows":    rows,
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
	}
}

// executeShowWarnings 执行 SHOW WARNINGS
func (e *XMySQLExecutor) executeShowWarnings(ctx *ExecutionContext) {
	logger.Debugf(" [executeShowWarnings] 执行SHOW WARNINGS")

	resultData := map[string]interface{}{
		"columns": []string{"Level", "Code", "Message"},
		"rows":    [][]interface{}{},
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
	}
}

// executeShowErrors 执行 SHOW ERRORS
func (e *XMySQLExecutor) executeShowErrors(ctx *ExecutionContext) {
	logger.Debugf(" [executeShowErrors] 执行SHOW ERRORS")

	resultData := map[string]interface{}{
		"columns": []string{"Level", "Code", "Message"},
		"rows":    [][]interface{}{},
	}

	ctx.Results <- &Result{
		ResultType: "QUERY",
		Data:       resultData,
	}
}
