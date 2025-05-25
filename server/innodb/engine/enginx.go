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
	"xmysql-server/server/innodb/basic"

	"xmysql-server/server"
	"xmysql-server/server/conf"
	"xmysql-server/server/innodb/common"
	"xmysql-server/server/innodb/manager"
	"xmysql-server/server/innodb/sqlparser"
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
	btreeCfg := &manager.BPlusTreeConfig{}
	e.btreeMgr = manager.NewBPlusTreeManager(e.storageMgr.GetBufferPoolManager(), btreeCfg)
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
	e.indexManager = manager.NewIndexManager(segManager, e.storageMgr.GetBufferPoolManager(), nil)
	e.dictManager = manager.NewDictionaryManager(segManager)
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
	//e.QueryExecutor = NewXMySQLExecutor(e.infoSchemaManager, e.conf)
}

func (e *XMySQLEngine) ExecuteQuery(session server.MySQLServerSession, query string, databaseName string) <-chan *Result {
	results := make(chan *Result)
	go func() {
		defer close(results)

		stmt, err := sqlparser.Parse(query)
		if err != nil {
			results <- &Result{Err: fmt.Errorf("parse error: %v", err), ResultType: common.RESULT_TYPE_ERROR}
			return
		}

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
				e.QueryExecutor.executeCreateTableStatement(ctx, databaseName, stmt)
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
				fmt.Println(expr)
				results <- &Result{
					StatementID: ctx.statementId,
					ResultType:  common.RESULT_TYPE_SET,
				}
				session.SendOK()
			}

		default:
			results <- &Result{Err: fmt.Errorf("unsupported statement type"), ResultType: common.RESULT_TYPE_ERROR}
		}
	}()

	return results
}
