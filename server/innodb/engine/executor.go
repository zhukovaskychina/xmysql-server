package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

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

	// TODO: 实现物理计划优化
	// 这里需要调用优化器管理器的优化方法
	return nil, fmt.Errorf("physical plan optimization not yet implemented")
}

// convertToSelectResult 将Record数组转换为SelectResult
func (e *XMySQLExecutor) convertToSelectResult(records []Record, schema *metadata.Schema) (*SelectResult, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}

	// 构建列名和类型
	columnNames := make([]string, 0, len(schema.Columns))
	columnTypes := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		columnNames = append(columnNames, col.Name)
		columnTypes = append(columnTypes, col.DataType)
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
		ColumnNames: columnNames,
		ColumnTypes: columnTypes,
		Rows:        rows,
		RowCount:    len(rows),
	}, nil
}

// convertValueToInterface 将basic.Value转换为interface{}
func (e *XMySQLExecutor) convertValueToInterface(value basic.Value) interface{} {
	if value.IsNull() {
		return nil
	}

	switch value.GetType() {
	case basic.TypeInt64:
		return value.ToInt64()
	case basic.TypeFloat64:
		return value.ToFloat64()
	case basic.TypeString:
		return value.ToString()
	case basic.TypeBool:
		return value.ToBool()
	case basic.TypeBytes:
		return value.ToBytes()
	case basic.TypeDecimal:
		return value.ToDecimal()
	case basic.TypeDate:
		return value.ToDate()
	case basic.TypeTimestamp:
		return value.ToTimestamp()
	default:
		return value.ToString()
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
			nil, // TODO: 添加事务管理器
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
			nil, // TODO: 添加事务管理器
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
			nil, // TODO: 添加事务管理器
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

// executeSetStatement 执行 SET 语句（占位）
func (e *XMySQLExecutor) executeSetStatement(ctx *ExecutionContext, stmt *sqlparser.Set) {}

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
