// 优化建议：
// 1. 架构清晰化（模块分组）
// 2. 初始化解耦（分模块独立函数）
// 3. 命名规范统一
// 4. 错误处理更优雅

package engine

import (
	"context"
	"fmt"
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

	return engine
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

	txManager, err := manager.NewTransactionManager(
		e.conf.GetString("innodb.redo_log_dir"),
		e.conf.GetString("innodb.undo_log_dir"),
	)
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
			default:
				results <- &Result{Err: fmt.Errorf("unsupported DB action: %s", stmt.Action), ResultType: common.RESULT_TYPE_ERROR}
			}

		case *sqlparser.Show:
			results <- &Result{Err: fmt.Errorf("SHOW not yet implemented"), ResultType: common.RESULT_TYPE_ERROR}

		case *sqlparser.Set:
			for _, expr := range stmt.Exprs {
				logger.Error(expr)
				results <- &Result{
					StatementID: ctx.statementId,
					ResultType:  common.RESULT_TYPE_SET,
				}
				session.SendOK()
			}

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
