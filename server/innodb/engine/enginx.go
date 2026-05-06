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

	indexManager *manager.IndexManager

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

	// 初始化恢复与检查点层
	engine.initRecoveryLayer()

	return engine
}

// Start 启动引擎，执行崩溃恢复并启动后台服务
func (e *XMySQLEngine) Start(ctx context.Context) error {
	logger.Info("🚀 Starting XMySQL Engine...")

	// 1. 执行崩溃恢复
	logger.Info("🏥 Performing crash recovery...")
	if err := e.crashRecovery.Recover(); err != nil {
		return fmt.Errorf("crash recovery failed: %v", err)
	}
	logger.Info("✅ Crash recovery completed successfully")

	// 2. 启动检查点管理器
	logger.Info("💾 Starting Checkpoint Manager...")
	if err := e.checkpointManager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start checkpoint manager: %v", err)
	}

	logger.Info("✅ XMySQL Engine started successfully")
	return nil
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
		panic(fmt.Errorf("failed to init TransactionManager: %w", err))
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
		e.txManager.GetRedoLogManager(),
		e.txManager.GetUndoLogManager(),
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
		e.QueryExecutor.SetAdditionalManagers(
			e.indexManager,
			e.storageMgr,
			tableStorageManager, // 创建新的表存储映射管理器
		)

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

		logger.Debugf(" [XMySQLEngine.ExecuteQuery] 开始执行查询: %s", query)
		logger.Debugf(" [XMySQLEngine.ExecuteQuery] 数据库名称: %s", databaseName)
		logger.Debugf(" [XMySQLEngine.ExecuteQuery] 会话对象: %v", session != nil)

		stmt, err := sqlparser.Parse(query)
		if err != nil {
			logger.Errorf(" [XMySQLEngine.ExecuteQuery] SQL解析错误: %v", err)
			results <- &Result{Err: fmt.Errorf("parse error: %v", err), ResultType: common.RESULT_TYPE_ERROR}
			return
		}

		logger.Debugf(" [XMySQLEngine.ExecuteQuery] SQL解析成功，语句类型: %T", stmt)

		ctx := &ExecutionContext{
			Context:     context.Background(),
			statementId: 0,
			QueryId:     0,
			Results:     results,
			Cfg:         e.conf,
		}

		switch stmt := stmt.(type) {
		case *sqlparser.Select:
			result, err2 := e.QueryExecutor.executeSelectStatement(ctx, stmt, databaseName)
			if err2 != nil {
				results <- &Result{Err: nil, ResultType: common.RESULT_TYPE_ERROR}
			} else {
				results <- &Result{Data: result, ResultType: common.RESULT_TYPE_SELECT}
			}

		case *sqlparser.DDL:
			switch stmt.Action {
			case "create":
				// 从会话中获取当前数据库
				currentDB := databaseName
				if currentDB == "" {
					if dbParam := session.GetParamByName("database"); dbParam != nil {
						if db, ok := dbParam.(string); ok {
							currentDB = db
						}
					}
				}
				logger.Debugf(" CREATE TABLE使用数据库: %s", currentDB)
				e.QueryExecutor.executeCreateTableStatement(ctx, currentDB, stmt)
			case "drop":
				// 从会话中获取当前数据库
				currentDB := databaseName
				if currentDB == "" {
					if dbParam := session.GetParamByName("database"); dbParam != nil {
						if db, ok := dbParam.(string); ok {
							currentDB = db
						}
					}
				}
				logger.Debugf("🗑️ DROP TABLE使用数据库: %s", currentDB)
				e.QueryExecutor.executeDropTableStatement(ctx, stmt)
			default:
				results <- &Result{Err: fmt.Errorf("unsupported DDL action: %s", stmt.Action), ResultType: common.RESULT_TYPE_ERROR}
			}

		case *sqlparser.DBDDL:
			switch stmt.Action {
			case "create":
				e.QueryExecutor.executeCreateDatabaseStatement(ctx, stmt)
			case "drop":
				e.QueryExecutor.executeDropDatabaseStatement(ctx, stmt)
			default:
				results <- &Result{Err: fmt.Errorf("unsupported DB action: %s", stmt.Action), ResultType: common.RESULT_TYPE_ERROR}
			}

		case *sqlparser.Show:
			// 处理 SHOW 语句
			logger.Debugf(" [XMySQLEngine.ExecuteQuery] 处理SHOW语句: %s", stmt.Type)
			e.QueryExecutor.executeShowStatement(ctx, stmt, session)

		case *sqlparser.Set:
			// SET 语句需要统一由执行器处理，避免在协议层重复发送OK包
			logger.Debugf(" [XMySQLEngine.ExecuteQuery] 处理SET语句，包含 %d 个表达式", len(stmt.Exprs))
			e.QueryExecutor.executeSetStatement(ctx, stmt, session)

		case *sqlparser.Use:
			// 处理USE语句
			dbName := stmt.DBName.String()
			logger.Debugf(" [XMySQLEngine.ExecuteQuery] 处理USE语句: %s", dbName)
			logger.Debugf(" [XMySQLEngine.ExecuteQuery] USE语句类型: %T", stmt)
			logger.Debugf(" [XMySQLEngine.ExecuteQuery] 会话对象: %v", session != nil)

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
			logger.Debugf(" 处理INSERT语句")
			result, err := e.QueryExecutor.executeInsertStatement(ctx, stmt, databaseName)
			if err != nil {
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("INSERT failed: %v", err)}
			} else {
				results <- &Result{
					Data:       result,
					ResultType: common.RESULT_TYPE_QUERY,
					Message:    result.Message,
				}
			}

		case *sqlparser.Update:
			// 处理UPDATE语句
			logger.Debugf("✏️ 处理UPDATE语句")
			result, err := e.QueryExecutor.executeUpdateStatement(ctx, stmt, databaseName)
			if err != nil {
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("UPDATE failed: %v", err)}
			} else {
				results <- &Result{
					Data:       result,
					ResultType: common.RESULT_TYPE_QUERY,
					Message:    result.Message,
				}
			}

		case *sqlparser.Delete:
			// 处理DELETE语句
			logger.Debugf("🗑️ 处理DELETE语句")
			result, err := e.QueryExecutor.executeDeleteStatement(ctx, stmt, databaseName)
			if err != nil {
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("DELETE failed: %v", err)}
			} else {
				results <- &Result{
					Data:       result,
					ResultType: common.RESULT_TYPE_QUERY,
					Message:    result.Message,
				}
			}

		default:
			results <- &Result{Err: fmt.Errorf("unsupported statement type"), ResultType: common.RESULT_TYPE_ERROR}
		}
	}()

	return results
}
