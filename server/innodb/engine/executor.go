package engine

import (
	"context"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// Iterator 火山模型中的迭代器接口，每个算子实现该接口用于迭代数据
type Iterator interface {
	Init() error           // 初始化迭代器
	Next() error           // 获取下一行数据，若无更多数据返回 io.EOF
	GetRow() []interface{} // 获取当前行数据
	Close() error          // 释放资源
}

// Executor 是算子接口，继承自 Iterator
// 每个执行算子如 TableScan、Join 等都要实现该接口
type Executor interface {
	Iterator
	Schema() *metadata.Schema        // 返回输出的字段结构
	Children() []Executor            // 返回子节点
	SetChildren(children []Executor) // 设置子节点
}

// BaseExecutor 所有执行器的基础结构
// 提供公共字段如 schema、子节点、执行上下文等
type BaseExecutor struct {
	schema   *metadata.Schema
	children []Executor
	ctx      *ExecutionContext
	closed   bool
}

// XMySQLExecutor 是 SQL 执行器的核心结构，负责整个 SQL 的解析与执行
// 支持解析 SELECT、DDL、SHOW 等语句，并调用相应执行逻辑
// 执行流程：解析 -> 生成逻辑计划 -> 转物理计划 -> 构造执行器 -> 流式迭代执行
// 当前实现简化处理，仅返回模拟执行结果

// XMySQLExecutor SQL执行器结构体
type XMySQLExecutor struct {
	infosSchemaManager metadata.InfoSchemaManager // 信息模式管理器
	conf               *conf.Cfg                  // 配置项
	ctx                *ExecutionContext          // 执行上下文
	results            chan *Result               // 结果通道
	rootExecutor       Executor                   // 根算子节点

	// 管理器组件 - 添加这些字段来访问各个管理器
	optimizerManager  interface{} // 查询优化器管理器
	bufferPoolManager interface{} // 缓冲池管理器
	btreeManager      interface{} // B+树管理器
	tableManager      interface{} // 表管理器
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
	case *sqlparser.DDL:
		e.executeDDL(stmt, results)
	case *sqlparser.DBDDL:
		e.executeDBDDL(stmt, results)
	case *sqlparser.Show:
		results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "SHOW statement executed (simplified implementation)"}
	case *sqlparser.Set:
		results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "SET statement executed (simplified implementation)"}
	default:
		results <- &Result{Err: fmt.Errorf("unsupported statement type: %T", stmt), ResultType: common.RESULT_TYPE_QUERY, Message: "Unsupported statement type"}
	}
}

// executeDDL 处理 DDL 类型语句，如 CREATE TABLE
func (e *XMySQLExecutor) executeDDL(stmt *sqlparser.DDL, results chan *Result) {
	switch stmt.Action {
	case "create":
		results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: "CREATE TABLE executed (simplified implementation)"}
	default:
		results <- &Result{Err: fmt.Errorf("unsupported DDL action: %s", stmt.Action), ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Unsupported DDL action: %s", stmt.Action)}
	}
}

// executeDBDDL 处理数据库级的DDL语句，如 CREATE DATABASE
func (e *XMySQLExecutor) executeDBDDL(stmt *sqlparser.DBDDL, results chan *Result) {
	switch stmt.Action {
	case "create":
		results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Database %s created successfully (simplified)", stmt.DBName)}
	case "drop":
		results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Database %s dropped successfully (simplified)", stmt.DBName)}
	default:
		results <- &Result{Err: fmt.Errorf("unsupported database DDL action: %s", stmt.Action), ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Unsupported database DDL action: %s", stmt.Action)}
	}
}

// buildExecutorTree 构造物理计划对应的算子执行树
func (e *XMySQLExecutor) buildExecutorTree(plan PhysicalPlan) Executor {
	return nil // TODO: 实现基于计划节点的执行器构建
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

// executeCreateDatabaseStatement 执行 CREATE DATABASE（占位）
func (e *XMySQLExecutor) executeCreateDatabaseStatement(ctx *ExecutionContext, stmt *sqlparser.DBDDL) {
}

// buildWhereConditions 构建 WHERE 条件表达式（占位）
func (e *XMySQLExecutor) buildWhereConditions(where *sqlparser.Where) {}

// executeInsertStatement 执行 INSERT 语句（占位）
func (e *XMySQLExecutor) executeInsertStatement(ctx *ExecutionContext, stmt *sqlparser.SelectStatement) {
}

// executeSetStatement 执行 SET 语句（占位）
func (e *XMySQLExecutor) executeSetStatement(ctx *ExecutionContext, stmt *sqlparser.Set) {}

// executeCreateTableStatement 执行 CREATE TABLE（占位）
func (e *XMySQLExecutor) executeCreateTableStatement(ctx *ExecutionContext, databaseName string, stmt *sqlparser.DDL) {
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

// Record 表示一条数据记录
// 包含字段值和值对应的表结构信息
type Record struct {
	Values []interface{}       // 字段值
	Schema *metadata.TableMeta // 表结构定义
}

// GetValue 按列名获取字段值
func (r *Record) GetValue(columnName string) (interface{}, error) {
	for i, col := range r.Schema.Columns {
		if col.Name == columnName {
			return r.Values[i], nil
		}
	}
	return nil, fmt.Errorf("column %s not found", columnName)
}

// SetValue 按列名设置字段值
func (r *Record) SetValue(columnName string, value interface{}) error {
	for i, col := range r.Schema.Columns {
		if col.Name == columnName {
			r.Values[i] = value
			return nil
		}
	}
	return fmt.Errorf("column %s not found", columnName)
}
