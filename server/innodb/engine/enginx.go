// 优化建议：
// 1. 架构清晰化（模块分组）
// 2. 初始化解耦（分模块独立函数）
// 3. 命名规范统一
// 4. 错误处理更优雅

package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// XMySQLEngine is the unified SQL engine coordinating all submodules.
type XMySQLEngine struct {
	conf *conf.Cfg

	// Core modules
	QueryExecutor *XMySQLExecutor
	storageMgr    *manager.StorageManager
	btreeMgr      basic.BPlusTreeManager

	// Schema & Metadata
	infoSchemaManager *manager.InfoSchemaManager
	dictManager       *manager.DictionaryManager

	// Transaction
	txManager   *manager.TransactionManager
	mvccManager *manager.MVCCManager

	// Utilities
	ibufManager     *manager.IBufManager
	encryptManager  *manager.EncryptionManager
	compressManager *manager.CompressionManager

	indexManager    *manager.IndexManager
	slowQueryLogger *SlowQueryLogger

	// Reliability & Recovery
	checkpointManager *CheckpointManager
	crashRecovery     *manager.CrashRecovery
}

func NewXMySQLEngine(conf *conf.Cfg) *XMySQLEngine {
	engine := &XMySQLEngine{conf: conf}

	// 初始化各核心模块
	engine.initStorageLayer()
	engine.initIndexLayer()
	engine.initTxnLayer()
	engine.initMetaLayer()
	engine.initUtilityManagers()
	engine.initQueryExecutor()
	engine.initSlowQueryLogger()

	// 初始化恢复与检查点层
	engine.initRecoveryLayer()

	return engine
}

// Start 启动引擎，执行崩溃恢复并启动后台服务
func (e *XMySQLEngine) Start(ctx context.Context) error {
	logger.Info("🚀 Starting XMySQL Engine...")

	// 1. 执行崩溃恢复
	if e.crashRecovery != nil {
		logger.Info("🏥 Performing crash recovery...")
		if err := e.crashRecovery.Recover(); err != nil {
			return fmt.Errorf("crash recovery failed: %v", err)
		}
		logger.Info("✅ Crash recovery completed successfully")
	} else {
		logger.Warnf("crash recovery unavailable, skip recovery step for degraded mode")
	}

	// 2. 启动检查点管理器
	if e.checkpointManager != nil {
		logger.Info("💾 Starting Checkpoint Manager...")
		if err := e.checkpointManager.Start(ctx); err != nil {
			return fmt.Errorf("failed to start checkpoint manager: %v", err)
		}
	}

	logger.Info("✅ XMySQL Engine started successfully")
	return nil
}

func (e *XMySQLEngine) initSlowQueryLogger() {
	e.slowQueryLogger = NewSlowQueryLogger(e.conf)
}

// Close 关闭引擎
func (e *XMySQLEngine) Close() error {
	logger.Info("🛑 Stopping XMySQL Engine...")

	if e.checkpointManager != nil {
		e.checkpointManager.Stop()
	}

	if e.txManager != nil {
		e.txManager.Close()
	}

	if e.storageMgr != nil {
		// storageMgr 没有 Close 方法，但如果有资源需释放可在此处理
	}

	return nil
}

func (e *XMySQLEngine) initStorageLayer() {
	e.storageMgr = manager.NewStorageManager(e.conf)
}

func (e *XMySQLEngine) initIndexLayer() {
	btreeCfg := &manager.BTreeConfig{
		MaxCacheSize:   1000,
		CachePolicy:    "LRU",
		PrefetchSize:   4,
		PageSize:       16384,
		FillFactor:     0.8,
		MinFillFactor:  0.4,
		SplitThreshold: 0.9,
		MergeThreshold: 0.3,
		AsyncIO:        true,
		EnableStats:    true,
		StatsInterval:  time.Minute * 5,
		EnableLogging:  true,
		LogLevel:       "INFO",
	}
	// 使用增强版B+树管理器适配器
	e.btreeMgr = manager.NewEnhancedBTreeAdapter(e.storageMgr, btreeCfg)
}

func (e *XMySQLEngine) initTxnLayer() {
	e.mvccManager = manager.NewMVCCManager(&manager.MVCCConfig{
		TxTimeout:         time.Minute * 5,
		MaxActiveTxs:      1000,
		SnapshotRetention: time.Hour,
	})

	// 处理空目录：为测试与默认配置提供安全的临时目录
	redoDir := e.conf.GetString("innodb.redo_log_dir")
	undoDir := e.conf.GetString("innodb.undo_log_dir")
	if redoDir == "" {
		redoDir = filepath.Join(os.TempDir(), "xmysql-server", "redo")
	}
	if undoDir == "" {
		undoDir = filepath.Join(os.TempDir(), "xmysql-server", "undo")
	}
	_ = os.MkdirAll(redoDir, 0755)
	_ = os.MkdirAll(undoDir, 0755)

	txManager, err := manager.NewTransactionManager(redoDir, undoDir)
	if err != nil {
		logger.Warnf("failed to init TransactionManager, continue with reduced transaction features: %v", err)
		e.txManager = nil
		return
	}
	e.txManager = txManager
}

func (e *XMySQLEngine) initMetaLayer() {

	segManager := e.storageMgr.GetSegmentManager()
	spaceManager := e.storageMgr.GetSpaceManager()
	// 使用带存储管理器的索引管理器构造函数，以支持增强版B+树
	e.indexManager = manager.NewIndexManagerWithStorage(segManager, e.storageMgr.GetBufferPoolManager(), e.storageMgr, nil)
	e.dictManager = manager.NewDictionaryManager(segManager, e.storageMgr.GetBufferPoolManager())
	e.infoSchemaManager = manager.NewInfoSchemaManager(
		e.dictManager,
		spaceManager,
		e.indexManager,
	)
}

func (e *XMySQLEngine) initUtilityManagers() {
	e.ibufManager = manager.NewIBufManager(
		e.storageMgr.GetSegmentManager(),
		e.storageMgr.GetPageManager(),
	)

	encCfg := manager.EncryptionSettings{
		Method:          manager.ENCRYPTION_METHOD_AES,
		KeyRotationDays: uint32(e.conf.GetInt("innodb.encryption.key_rotation_days")),
		ThreadsNum:      uint8(e.conf.GetInt("innodb.encryption.threads")),
		BufferSize:      uint32(e.conf.GetInt("innodb.encryption.buffer_size")),
	}
	masterKey := []byte(e.conf.GetString("innodb.encryption.master_key"))
	e.encryptManager = manager.NewEncryptionManager(masterKey, encCfg)

	e.compressManager = manager.NewCompressionManager()
}

func (e *XMySQLEngine) initRecoveryLayer() {
	// 1. 初始化 CheckpointManager
	dataDir := e.conf.GetString("innodb.data_dir")
	if dataDir == "" {
		dataDir = e.conf.DataDir
	}

	bufferPoolMgr := e.storageMgr.GetBufferPoolManager()
	e.checkpointManager = NewCheckpointManager(dataDir, bufferPoolMgr)

	if e.txManager == nil {
		logger.Warnf("transaction manager is not initialized, skip crash recovery initialization")
		e.crashRecovery = nil
		return
	}

	redoLogManager := e.txManager.GetRedoLogManager()
	undoLogManager := e.txManager.GetUndoLogManager()
	if redoLogManager == nil || undoLogManager == nil {
		logger.Warnf("transaction managers missing (redo=%v undo=%v), skip crash recovery initialization", redoLogManager != nil, undoLogManager != nil)
		e.crashRecovery = nil
		return
	}

	// 2. 初始化 CrashRecovery
	// 需要从 CheckpointManager 获取最新的 Checkpoint LSN
	var checkpointLSN uint64 = 0

	// 尝试读取最新的 Checkpoint
	// 注意：这里不应该调用 Start，只读取元数据
	// 如果没有 Checkpoint 文件，LSN 为 0，表示从头开始
	if latestCP, err := e.checkpointManager.ReadLatestCheckpoint(); err == nil && latestCP != nil {
		checkpointLSN = latestCP.LSN
		logger.Infof("Found latest checkpoint at LSN: %d", checkpointLSN)
	} else {
		logger.Infof("No checkpoint found, starting recovery from LSN 0")
	}

	e.crashRecovery = manager.NewCrashRecovery(
		redoLogManager,
		undoLogManager,
		checkpointLSN,
	)

	// 设置 CrashRecovery 的依赖
	e.crashRecovery.SetBufferPoolManager(&RecoveryBufferPoolAdapter{bpm: bufferPoolMgr})

	// 设置 StorageManager
	e.crashRecovery.SetStorageManager(&RecoveryStorageAdapter{sm: e.storageMgr})
}

func (e *XMySQLEngine) initQueryExecutor() {
	e.QueryExecutor = NewXMySQLExecutor(e.infoSchemaManager, e.conf)

	// 设置管理器组件
	if e.QueryExecutor != nil {
		// 创建优化器管理器
		optimizerManager := manager.NewOptimizerManager(e.infoSchemaManager)

		// 创建带存储管理器的表管理器
		tableManager := manager.NewTableManagerWithStorage(e.infoSchemaManager, e.storageMgr)

		// 从存储管理器获取缓冲池管理器
		bufferPoolManager := e.storageMgr.GetBufferPoolManager()

		// 直接使用 btreeManager 接口，无需类型转换
		// 因为 e.btreeMgr 已经是 basic.BPlusTreeManager 接口类型
		btreeManager := e.btreeMgr

		// 设置基础管理器
		e.QueryExecutor.SetManagers(
			optimizerManager,
			bufferPoolManager,
			btreeManager, // 直接传递接口
			tableManager,
		)

		// 设置存储引擎相关的管理器 - 新增
		tableStorageManager := manager.NewTableStorageManager(e.storageMgr)
		if err := tableStorageManager.SyncFromInfoSchema(e.infoSchemaManager); err != nil {
			logger.Warnf("failed to sync table storage mapping from info schema: %v", err)
		}
		e.QueryExecutor.SetAdditionalManagers(
			e.indexManager,
			e.storageMgr,
			tableStorageManager, // 创建新的表存储映射管理器
		)
		e.QueryExecutor.SetTransactionManager(e.txManager)

		// 将管理器注入 StorageManager，供集成层等通过 GetTableManager/GetTableStorageManager 等统一获取
		e.storageMgr.SetTableManager(tableManager)
		e.storageMgr.SetTableStorageManager(tableStorageManager)
		e.storageMgr.SetIndexManager(e.indexManager)
		e.storageMgr.SetTransactionManager(e.txManager)
		e.storageMgr.SetBTreeManager(e.btreeMgr)

		logger.Debugf(" QueryExecutor initialized with all managers")
	}
}

// GetStorageManager 获取存储管理器
func (e *XMySQLEngine) GetStorageManager() *manager.StorageManager {
	return e.storageMgr
}

func (e *XMySQLEngine) ExecuteQuery(session server.MySQLServerSession, query string, databaseName string) <-chan *Result {
	results := make(chan *Result)
	go func() {
		defer close(results)

		start := time.Now()
		rowsAffected := 0
		txnID := uint64(0)
		stage := "parse"
		status := "success"
		var execErr error

		defer func() {
			e.logSlowQuery(session, query, time.Since(start), rowsAffected, txnID, stage, status, execErr)
		}()

		logger.Debugf(" [XMySQLEngine.ExecuteQuery] 开始执行查询: %s", query)
		logger.Debugf(" [XMySQLEngine.ExecuteQuery] 数据库名称: %s", databaseName)
		logger.Debugf(" [XMySQLEngine.ExecuteQuery] 会话对象: %v", session != nil)

		ctx := &ExecutionContext{
			Context:      context.Background(),
			statementId:  0,
			QueryId:      0,
			Results:      results,
			Cfg:          e.conf,
			DatabaseName: databaseName,
			RawQuery:     query,
		}

		if cmd, name, ok := normalizedTransactionCommand(query); ok {
			stage = "transaction"
			e.QueryExecutor.executeTransactionCommand(ctx, cmd, name, session)
			return
		}

		if err := rejectUnsupportedCreateTableConstraintsSQL(query); err != nil {
			execErr = err
			status = "failed"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: err.Error()}
			return
		}

		stmt, err := sqlparser.Parse(query)
		if err != nil {
			logger.Errorf(" [XMySQLEngine.ExecuteQuery] SQL解析错误: %v", err)
			execErr = fmt.Errorf("parse error: %v", err)
			status = "failed"
			results <- &Result{Err: execErr, ResultType: common.RESULT_TYPE_ERROR}
			return
		}

		logger.Debugf(" [XMySQLEngine.ExecuteQuery] SQL解析成功，语句类型: %T", stmt)
		stage = "dispatch"

		switch stmt := stmt.(type) {
		case *sqlparser.Select:
			stage = "select"
			result, err2 := e.QueryExecutor.executeSelectStatement(ctx, stmt, databaseName)
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("SELECT failed: %v", err2)}
			} else {
				rowsAffected = result.RowCount
				results <- &Result{Data: result, ResultType: common.RESULT_TYPE_SELECT}
			}

		case *sqlparser.DDL:
			switch stmt.Action {
			case "create":
				// 从会话中获取当前数据库
				stage = "ddl-create-table"
				currentDB := databaseName
				if currentDB == "" && session != nil {
					if dbParam := session.GetParamByName("database"); dbParam != nil {
						if db, ok := dbParam.(string); ok {
							currentDB = db
						}
					}
				}
				logger.Debugf(" CREATE TABLE使用数据库: %s", currentDB)
				e.QueryExecutor.executeCreateTableStatement(ctx, currentDB, stmt)
			case "drop":
				stage = "ddl-drop-table"
				// 从会话中获取当前数据库
				currentDB := databaseName
				if currentDB == "" && session != nil {
					if dbParam := session.GetParamByName("database"); dbParam != nil {
						if db, ok := dbParam.(string); ok {
							currentDB = db
						}
					}
				}
				logger.Debugf("🗑️ DROP TABLE使用数据库: %s", currentDB)
				e.QueryExecutor.executeDropTableStatement(ctx, stmt)
			case "truncate":
				stage = "ddl-truncate-table"
				currentDB := databaseName
				if currentDB == "" && session != nil {
					if dbParam := session.GetParamByName("database"); dbParam != nil {
						if db, ok := dbParam.(string); ok {
							currentDB = db
						}
					}
				}
				logger.Debugf("TRUNCATE TABLE使用数据库: %s", currentDB)
				e.QueryExecutor.executeTruncateTableStatement(ctx, currentDB, stmt)
			case "alter":
				stage = "ddl-alter-table"
				currentDB := databaseName
				if currentDB == "" && session != nil {
					if dbParam := session.GetParamByName("database"); dbParam != nil {
						if db, ok := dbParam.(string); ok {
							currentDB = db
						}
					}
				}
				logger.Debugf("ALTER TABLE使用数据库: %s", currentDB)
				e.QueryExecutor.executeAlterTableStatement(ctx, currentDB, stmt)
			default:
				execErr = fmt.Errorf("unsupported DDL action: %s", stmt.Action)
				status = "failed"
				results <- &Result{Err: fmt.Errorf("unsupported DDL action: %s", stmt.Action), ResultType: common.RESULT_TYPE_ERROR}
			}

		case *sqlparser.DBDDL:
			switch stmt.Action {
			case "create":
				stage = "ddl-create-database"
				e.QueryExecutor.executeCreateDatabaseStatement(ctx, stmt)
			case "drop":
				stage = "ddl-drop-database"
				e.QueryExecutor.executeDropDatabaseStatement(ctx, stmt)
			default:
				execErr = fmt.Errorf("unsupported DB action: %s", stmt.Action)
				status = "failed"
				results <- &Result{Err: fmt.Errorf("unsupported DB action: %s", stmt.Action), ResultType: common.RESULT_TYPE_ERROR}
			}

		case *sqlparser.Show:
			stage = "show"
			// 处理 SHOW 语句
			logger.Debugf(" [XMySQLEngine.ExecuteQuery] 处理SHOW语句: %s", stmt.Type)
			e.QueryExecutor.executeShowStatementWithQuery(ctx, stmt, session, query)

		case *sqlparser.Set:
			stage = "set"
			// SET 语句需要统一由执行器处理，避免在协议层重复发送OK包
			logger.Debugf(" [XMySQLEngine.ExecuteQuery] 处理SET语句，包含 %d 个表达式", len(stmt.Exprs))
			e.QueryExecutor.executeSetStatement(ctx, stmt, session)

		case *sqlparser.Use:
			stage = "use"
			// 处理USE语句
			dbName := stmt.DBName.String()
			logger.Debugf(" [XMySQLEngine.ExecuteQuery] 处理USE语句: %s", dbName)
			logger.Debugf(" [XMySQLEngine.ExecuteQuery] USE语句类型: %T", stmt)
			logger.Debugf(" [XMySQLEngine.ExecuteQuery] 会话对象: %v", session != nil)

			if session == nil {
				execErr = fmt.Errorf("session is required for USE statement")
				status = "failed"
				results <- &Result{Err: execErr, ResultType: common.RESULT_TYPE_ERROR, Message: execErr.Error()}
				return
			}

			// 设置会话的数据库上下文
			session.SetParamByName("database", dbName)
			logger.Debugf(" [XMySQLEngine.ExecuteQuery] 会话数据库上下文已设置为: %s", dbName)

			// USE语句处理 - 返回OK响应
			results <- &Result{
				StatementID: ctx.statementId,
				ResultType:  common.RESULT_TYPE_QUERY,
				Message:     fmt.Sprintf("Database changed to '%s'", dbName),
			}
			logger.Debugf(" [XMySQLEngine.ExecuteQuery] USE语句处理完成，返回OK结果")

		case *sqlparser.Insert:
			// 处理INSERT语句
			stage = "insert"
			logger.Debugf(" 处理INSERT语句")
			explicitSchema := strings.TrimSpace(stmt.Table.Qualifier.String())
			result, err := e.QueryExecutor.executeInsertStatement(ctx, stmt, e.resolveDmlDatabaseName(session, databaseName, explicitSchema))
			if err != nil {
				execErr = err
				status = "failed"
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("INSERT failed: %v", err)}
			} else {
				txnID = result.TxnID
				rowsAffected = result.AffectedRows
				results <- &Result{
					Data:       result,
					ResultType: common.RESULT_TYPE_QUERY,
					Message:    result.Message,
				}
			}

		case *sqlparser.Update:
			// 处理UPDATE语句
			stage = "update"
			logger.Debugf("✏️ 处理UPDATE语句")
			explicitSchema := e.extractTableExprSchema(stmt.TableExprs)
			result, err := e.QueryExecutor.executeUpdateStatement(ctx, stmt, e.resolveDmlDatabaseName(session, databaseName, explicitSchema))
			if err != nil {
				execErr = err
				status = "failed"
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("UPDATE failed: %v", err)}
			} else {
				txnID = result.TxnID
				rowsAffected = result.AffectedRows
				results <- &Result{
					Data:       result,
					ResultType: common.RESULT_TYPE_QUERY,
					Message:    result.Message,
				}
			}

		case *sqlparser.Delete:
			// 处理DELETE语句
			stage = "delete"
			logger.Debugf("🗑️ 处理DELETE语句")
			explicitSchema := e.extractTableExprSchema(stmt.TableExprs)
			result, err := e.QueryExecutor.executeDeleteStatement(ctx, stmt, e.resolveDmlDatabaseName(session, databaseName, explicitSchema))
			if err != nil {
				execErr = err
				status = "failed"
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("DELETE failed: %v", err)}
			} else {
				txnID = result.TxnID
				rowsAffected = result.AffectedRows
				results <- &Result{
					Data:       result,
					ResultType: common.RESULT_TYPE_QUERY,
					Message:    result.Message,
				}
			}

		default:
			execErr = fmt.Errorf("unsupported statement type")
			status = "failed"
			results <- &Result{Err: fmt.Errorf("unsupported statement type"), ResultType: common.RESULT_TYPE_ERROR}
		}
	}()

	return results
}

func (e *XMySQLEngine) logSlowQuery(session server.MySQLServerSession, query string, duration time.Duration, rowsAffected int, txnID uint64, stage, status string, execErr error) {
	if e.slowQueryLogger == nil || !e.slowQueryLogger.IsEnabled() {
		return
	}

	connID := e.getSessionConnectionID(session)
	errorCode := ""
	errorMsg := ""
	if execErr != nil {
		errorCode = e.getExecutionErrorCode(execErr)
		errorMsg = execErr.Error()
	}

	e.slowQueryLogger.Record(
		query,
		duration,
		rowsAffected,
		connID,
		txnID,
		errorCode,
		errorMsg,
		status,
		stage,
		e.getSlowQuerySchema(session),
		extractSlowQueryTable(query),
	)
}

func (e *XMySQLEngine) getSlowQuerySchema(session server.MySQLServerSession) string {
	if session == nil {
		return ""
	}
	if dbParam := session.GetParamByName("database"); dbParam != nil {
		if db, ok := dbParam.(string); ok {
			return strings.TrimSpace(db)
		}
	}
	return ""
}

func extractSlowQueryTable(query string) string {
	normalized := strings.NewReplacer("(", " ", ")", " ", ",", " ", ";", " ").Replace(query)
	fields := strings.Fields(normalized)
	if len(fields) == 0 {
		return ""
	}

	tableAfter := func(keyword string) string {
		for i := 0; i < len(fields)-1; i++ {
			if strings.EqualFold(fields[i], keyword) {
				return cleanSlowQueryTableName(fields[i+1])
			}
		}
		return ""
	}

	switch strings.ToLower(fields[0]) {
	case "insert":
		return tableAfter("into")
	case "update":
		if len(fields) > 1 {
			return cleanSlowQueryTableName(fields[1])
		}
	case "delete", "select":
		return tableAfter("from")
	case "replace":
		return tableAfter("into")
	}
	return ""
}

func cleanSlowQueryTableName(raw string) string {
	raw = strings.Trim(raw, "` ")
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, ".")
	return strings.Trim(parts[len(parts)-1], "` ")
}

func (e *XMySQLEngine) getSessionConnectionID(session server.MySQLServerSession) uint32 {
	if session == nil {
		return 0
	}

	ctx := session.SessionContext()
	if ctx == nil {
		return 0
	}

	return ctx.GetConnectionID()
}

func (e *XMySQLEngine) getExecutionErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var code ExecutionErrorCode
	if execErr, ok := err.(*ExecutionError); ok && execErr != nil {
		code = execErr.ErrorCode
	}
	if code == "" {
		code = ExecutionErrorCodeUnknown
	}
	return string(code)
}

// resolveDmlDatabaseName 根据 DML 语句返回最终数据库名：
// 1) SQL 显式库名优先
// 2) 否则优先使用会话库（如存在）
// 3) 否则回退到入参数据库名
// 4) 对入参为系统库 mysql 的特殊处理：避免把它当作默认数据库污染到未显式表名的 DML
func (e *XMySQLEngine) resolveDmlDatabaseName(session server.MySQLServerSession, fallbackSchema string, explicitSchema string) string {
	explicitSchema = strings.TrimSpace(explicitSchema)
	if explicitSchema != "" {
		return explicitSchema
	}

	sessionSchema := ""
	if session != nil {
		if dbParam := session.GetParamByName("database"); dbParam != nil {
			if db, ok := dbParam.(string); ok {
				sessionSchema = strings.TrimSpace(db)
			}
		}
	}

	fallbackSchema = strings.TrimSpace(fallbackSchema)
	if sessionSchema != "" && strings.EqualFold(fallbackSchema, "mysql") {
		logger.Debugf(" [XMySQLEngine.resolveDmlDatabaseName] 入口数据库为mysql，使用会话数据库: %s", sessionSchema)
		return sessionSchema
	}

	if fallbackSchema == "" {
		return sessionSchema
	}

	return fallbackSchema
}

func (e *XMySQLEngine) extractTableExprSchema(tableExprs []sqlparser.TableExpr) string {
	if len(tableExprs) == 0 {
		return ""
	}

	tableExpr, ok := tableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return ""
	}

	tableName, ok := tableExpr.Expr.(sqlparser.TableName)
	if !ok {
		return ""
	}

	return strings.TrimSpace(tableName.Qualifier.String())
}
