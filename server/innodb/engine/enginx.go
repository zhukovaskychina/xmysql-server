// 优化建议：
// 1. 架构清晰化（模块分组）
// 2. 初始化解耦（分模块独立函数）
// 3. 命名规范统一
// 4. 错误处理更优雅

package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/backup"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
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
	checkpointManager  *CheckpointManager
	crashRecovery      *manager.CrashRecovery
	replicationRuntime *replication.Runtime
	replicaRegistry    *replication.ReplicaRegistry
	ready              atomic.Bool
}

func NewXMySQLEngine(conf *conf.Cfg) *XMySQLEngine {
	engine := &XMySQLEngine{conf: conf, replicaRegistry: replication.NewReplicaRegistry()}

	// 初始化各核心模块
	engine.initStorageLayer()
	engine.initIndexLayer()
	engine.initTxnLayer()
	engine.initMetaLayer()
	engine.initUtilityManagers()
	engine.initQueryExecutor()
	engine.initReplicationLayer()
	engine.initSlowQueryLogger()

	// 初始化恢复与检查点层
	engine.initRecoveryLayer()

	return engine
}

// Start 启动引擎，执行崩溃恢复并启动后台服务
func (e *XMySQLEngine) Start(ctx context.Context) error {
	if e == nil {
		return fmt.Errorf("engine is not initialized")
	}
	e.ready.Store(false)
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
	if e.QueryExecutor != nil {
		if err := e.QueryExecutor.loadPreparedXATransactions(); err != nil {
			return fmt.Errorf("prepared XA recovery metadata load failed: %v", err)
		}
		if err := e.QueryExecutor.loadSuspendedXATransactions(); err != nil {
			return fmt.Errorf("suspended XA recovery metadata load failed: %v", err)
		}
		if err := e.QueryExecutor.RecoverOrphanedTransactions(); err != nil {
			return fmt.Errorf("orphan transaction recovery failed: %v", err)
		}
		if err := e.QueryExecutor.CleanupOrphanedTemporaryTables(); err != nil {
			return fmt.Errorf("orphan temporary table cleanup failed: %v", err)
		}
	}

	// 2. 启动检查点管理器
	if e.checkpointManager != nil {
		logger.Info("💾 Starting Checkpoint Manager...")
		if err := e.checkpointManager.Start(ctx); err != nil {
			return fmt.Errorf("failed to start checkpoint manager: %v", err)
		}
	}
	if e.replicationRuntime != nil {
		if err := e.replicationRuntime.Start(ctx); err != nil {
			return fmt.Errorf("failed to start replication runtime: %v", err)
		}
	}

	logger.Info("✅ XMySQL Engine started successfully")
	e.ready.Store(true)
	return nil
}

// SetEventScheduler exposes the explicit SQL EVENT scheduler through the
// engine facade used by integration tests and server wiring.
func (e *XMySQLEngine) SetEventScheduler(scheduler *EventScheduler) {
	if e != nil && e.QueryExecutor != nil {
		e.QueryExecutor.SetEventScheduler(scheduler)
	}
}

func (e *XMySQLEngine) StartEventScheduler(ctx context.Context) error {
	if e == nil || e.QueryExecutor == nil {
		return fmt.Errorf("query executor is not initialized")
	}
	return e.QueryExecutor.StartEventScheduler(ctx)
}

func (e *XMySQLEngine) StopEventScheduler() bool {
	if e == nil || e.QueryExecutor == nil {
		return false
	}
	return e.QueryExecutor.StopEventScheduler()
}

func (e *XMySQLEngine) initSlowQueryLogger() {
	e.slowQueryLogger = NewSlowQueryLogger(e.conf)
}

// Close 关闭引擎
func (e *XMySQLEngine) Close() error {
	if e == nil {
		return nil
	}
	e.ready.Store(false)
	logger.Info("🛑 Stopping XMySQL Engine...")
	if e.QueryExecutor != nil {
		e.QueryExecutor.StopEventScheduler()
	}

	if e.checkpointManager != nil {
		e.checkpointManager.Stop()
	}
	if e.replicationRuntime != nil {
		if err := e.replicationRuntime.Close(); err != nil {
			return fmt.Errorf("failed to close replication runtime: %w", err)
		}
	}

	if e.txManager != nil {
		e.txManager.Close()
	}
	if e.QueryExecutor != nil && e.QueryExecutor.tableStorageManager != nil {
		if err := e.QueryExecutor.tableStorageManager.CloseBTreeManagers(); err != nil {
			return fmt.Errorf("failed to close table btree managers: %w", err)
		}
	}
	if e.btreeMgr != nil {
		closeOwnedBTreeManager(e.btreeMgr)
	}

	if e.storageMgr != nil {
		if err := e.storageMgr.Close(); err != nil {
			return fmt.Errorf("failed to close storage manager: %w", err)
		}
	}

	return nil
}

// IsReady reports whether the engine completed its startup sequence and is
// still available for serving traffic. It is intentionally separate from
// listener liveness so orchestration can wait for recovery and replication
// initialization before routing MySQL clients to the node.
func (e *XMySQLEngine) IsReady() bool {
	return e != nil && e.ready.Load()
}

// GetDataDir returns the durable data directory used by the engine. It is
// exposed for authentication and administrative integrations that need to
// reload persisted compatibility metadata.
func (e *XMySQLEngine) GetDataDir() string {
	if e == nil || e.QueryExecutor == nil {
		return ""
	}
	return e.QueryExecutor.getDataDir()
}

// CreatePhysicalBackup flushes a sharp checkpoint when the engine has one,
// then creates an integrity-checked XMySQL physical snapshot. Restoration is
// intentionally exposed by server/backup and must be performed with the
// engine stopped.
func (e *XMySQLEngine) CreatePhysicalBackup(ctx context.Context, archivePath string) (backup.PhysicalBackupManifest, error) {
	if e == nil {
		return backup.PhysicalBackupManifest{}, fmt.Errorf("engine is not initialized")
	}
	dataDir := e.GetDataDir()
	if dataDir == "" {
		return backup.PhysicalBackupManifest{}, fmt.Errorf("engine data directory is not configured")
	}
	return backup.CreatePhysicalBackup(ctx, backup.PhysicalBackupOptions{
		SourceDir:   dataDir,
		ArchivePath: archivePath,
		Sync: func() error {
			if e.checkpointManager != nil {
				stats := e.checkpointManager.GetStats()
				if err := e.checkpointManager.WriteSharpCheckpoint(stats.LastCheckpointLSN); err != nil {
					return fmt.Errorf("write sharp checkpoint before physical backup: %w", err)
				}
				return nil
			}
			if e.storageMgr == nil {
				return fmt.Errorf("storage manager is not initialized")
			}
			return e.storageMgr.Flush()
		},
	})
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
	e.checkpointManager.SetActiveTransactionProvider(e.txManager)

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
	e.infoSchemaManager.SetStatsLoader(func(ctx context.Context, schemaName, tableName string) (*metadata.InfoTableStats, error) {
		info, err := e.QueryExecutor.readPersistedTableInfo(schemaName, tableName)
		if err != nil || info == nil {
			return nil, err
		}
		if stats, sidecarErr := e.QueryExecutor.readPersistedTableStatistics(schemaName, tableName, info); sidecarErr == nil && stats != nil {
			return stats, nil
		}
		return info.Stats, nil
	})
	e.infoSchemaManager.SetStatsPersister(func(ctx context.Context, schemaName, tableName string, stats *metadata.InfoTableStats) error {
		info, err := e.QueryExecutor.readPersistedTableInfo(schemaName, tableName)
		if err != nil {
			return err
		}
		return e.QueryExecutor.persistTableStatistics(schemaName, tableName, info, stats)
	})

	// 设置管理器组件
	if e.QueryExecutor != nil {
		e.QueryExecutor.SetReplicaRegistrationProvider(e.replicaRegistry.Snapshot)
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
		e.QueryExecutor.applyPersistedSystemVariables()

		// 将管理器注入 StorageManager，供集成层等通过 GetTableManager/GetTableStorageManager 等统一获取
		e.storageMgr.SetTableManager(tableManager)
		e.storageMgr.SetTableStorageManager(tableStorageManager)
		e.storageMgr.SetIndexManager(e.indexManager)
		e.storageMgr.SetTransactionManager(e.txManager)
		e.storageMgr.SetBTreeManager(e.btreeMgr)

		logger.Debugf(" QueryExecutor initialized with all managers")
	}
}

func (e *XMySQLEngine) initReplicationLayer() {
	if e == nil || e.conf == nil || e.QueryExecutor == nil {
		return
	}
	role := e.conf.ReplicationRole
	if role == "" {
		role = replication.RoleStandalone
	}
	runtime, err := replication.NewRuntime(replication.RuntimeConfig{
		Role:         role,
		DataDir:      e.GetDataDir(),
		UUID:         e.conf.ReplicationUUID,
		ServerID:     e.conf.ReplicationServerID,
		ListenAddr:   e.conf.ReplicationListenAddress,
		SourceURL:    e.conf.ReplicationSourceURL,
		PollInterval: e.conf.ReplicationPollIntervalDuration,
		ApplyRows:    e.applyReplicationRows,
		Apply:        e.applyReplicationStatements,
	})
	if err != nil {
		logger.Warnf("replication runtime disabled: %v", err)
		return
	}
	e.replicationRuntime = runtime
	e.QueryExecutor.SetReplicationStatusProvider(runtime.Status)
	e.QueryExecutor.SetReplicationSourceProvider(runtime.Source)
	e.QueryExecutor.SetReplicationReplicaProvider(runtime.Replica)
	e.QueryExecutor.SetReplicationControl(runtime.StartReplica, runtime.StopReplica)
	e.QueryExecutor.SetReplicationSourceControl(runtime.ChangeSource)
	e.QueryExecutor.SetReplicationResetControl(runtime.ResetReplica)
	e.QueryExecutor.SetReplicationResetAllControl(runtime.ResetReplicaAll)
	e.QueryExecutor.SetReplicationSourceAdminControl(runtime.FlushBinaryLogs, runtime.ResetMaster)
	if role == replication.RoleSource {
		e.QueryExecutor.SetReplicationCommitTransactionHook(e.appendReplicationTransaction)
		e.QueryExecutor.SetReplicationCommitTransactionHookWithID(e.appendReplicationTransactionWithID)
	}
}

func (e *XMySQLEngine) appendReplicationStatements(statements []replication.Statement) error {
	if e == nil || e.replicationRuntime == nil {
		return nil
	}
	return e.replicationRuntime.AppendCommitted(statements)
}

func (e *XMySQLEngine) appendReplicationTransaction(changes []replication.RowChange, statements []replication.Statement) error {
	if e == nil || e.replicationRuntime == nil {
		return nil
	}
	return e.replicationRuntime.AppendCommittedTransaction(changes, statements)
}

func (e *XMySQLEngine) appendReplicationTransactionWithID(transactionID string, changes []replication.RowChange, statements []replication.Statement) error {
	if e == nil || e.replicationRuntime == nil {
		return nil
	}
	return e.replicationRuntime.AppendCommittedTransactionWithKey(transactionID, changes, statements)
}

// ReplicationStatus returns the local runtime status for operational probes.
func (e *XMySQLEngine) ReplicationStatus() interface{} {
	if e == nil || e.replicationRuntime == nil {
		return nil
	}
	return e.replicationRuntime.Status()
}

// ReplicationSource exposes the local source stream to protocol adapters.
func (e *XMySQLEngine) ReplicationSource() *replication.Source {
	if e == nil || e.replicationRuntime == nil {
		return nil
	}
	return e.replicationRuntime.Source()
}

// ReplicaRegistry exposes the listener-local native replication registrations
// to the protocol handler without coupling the SQL engine to net.Session.
func (e *XMySQLEngine) ReplicaRegistry() *replication.ReplicaRegistry {
	if e == nil {
		return nil
	}
	return e.replicaRegistry
}

// PromoteReplication promotes the local replica to a source. The caller is
// responsible for routing writes to the promoted node after the response.
func (e *XMySQLEngine) PromoteReplication() error {
	if e == nil || e.replicationRuntime == nil {
		return fmt.Errorf("replication runtime is not configured")
	}
	return e.replicationRuntime.Promote()
}

// GetStorageManager 获取存储管理器
func (e *XMySQLEngine) GetStorageManager() *manager.StorageManager {
	return e.storageMgr
}

// GetMandatoryRoles returns the configured mandatory roles that currently
// resolve to real role accounts.  MySQL ignores configured names that do not
// exist yet for role activation purposes.
func (e *XMySQLEngine) GetMandatoryRoles() []string {
	if e == nil || e.QueryExecutor == nil {
		return nil
	}
	return e.QueryExecutor.allGrantedRolesForMandatoryRoles()
}

// ActivateAllRolesOnLogin reports whether login should activate every role
// granted to the account, including existing mandatory roles.
func (e *XMySQLEngine) ActivateAllRolesOnLogin() bool {
	if e == nil || e.storageMgr == nil {
		return false
	}
	value, err := e.storageMgr.GetSystemVariablesManager().GetVariable("", "activate_all_roles_on_login", manager.GlobalScope)
	if err != nil {
		return false
	}
	return strings.EqualFold(fmt.Sprint(value), "on") || fmt.Sprint(value) == "1" || strings.EqualFold(fmt.Sprint(value), "true")
}

func (e *XMySQLEngine) ExecuteQuery(session server.MySQLServerSession, query string, databaseName string) <-chan *Result {
	query = rewriteCharsetIntroducers(query)
	// ExecuteQuery normally produces one terminal result, but a few legacy
	// branches can emit more than one. Collect the worker's results internally
	// and expose them only after the worker's deferred metrics/slow-query
	// bookkeeping has completed.
	results := make(chan *Result)
	workerResults := make(chan *Result)
	statementSummary := make(chan statementExecutionSummary, 1)
	go func() {
		pending := make([]*Result, 0, 1)
		accounting := statementResultAccounting{}
		for result := range workerResults {
			persistDirectEngineSessionState(session, result)
			if result != nil && result.Err != nil {
				recordSessionError(session, result.Err)
			}
			resultAccounting := statementResultAccountingFor(result)
			accounting.rowsAffected += resultAccounting.rowsAffected
			accounting.rowsSent += resultAccounting.rowsSent
			accounting.warnings += resultAccounting.warnings
			pending = append(pending, result)
		}
		summary := <-statementSummary
		if e.QueryExecutor != nil && e.QueryExecutor.metricsRecorder != nil {
			actorSetting := e.QueryExecutor.performanceSchemaStatementSettingForSession(session)
			e.QueryExecutor.metricsRecorder.RecordStatementWithThreadIDAndIdentityAndAccountingWithRowsExaminedAndScan(
				summary.threadID, summary.user, summary.host, databaseName, strings.TrimSpace(query), metricStatementType(query), summary.status, summary.latency,
				accounting.rowsAffected, accounting.rowsSent, summary.rowsExamined, summary.selectScan, accounting.warnings, actorSetting.Enabled, actorSetting.History,
			)
		}
		for _, result := range pending {
			results <- result
		}
		close(results)
	}()
	go func() {
		results := workerResults
		defer close(workerResults)

		start := time.Now()
		memoryThreadID := int64(0)
		if session != nil {
			memoryThreadID = int64(sessionConnectionID(session))
		}
		if e.QueryExecutor != nil && e.QueryExecutor.metricsRecorder != nil {
			e.QueryExecutor.metricsRecorder.RecordMemoryAllocation(memoryThreadID, "memory/sql/THD::main_mem_root", int64(len(query)))
			defer e.QueryExecutor.metricsRecorder.RecordMemoryFree(memoryThreadID, "memory/sql/THD::main_mem_root", int64(len(query)))
		}
		rowsAffected := 0
		txnID := uint64(0)
		stage := "parse"
		status := "success"
		var execErr error
		var statementContext *ExecutionContext

		defer func() {
			if execErr == nil && status == "success" && session != nil && e.QueryExecutor != nil && isReplicationDDLQuery(query) {
				if statement, ok := session.GetParamByName("replication_current_statement").(replication.Statement); ok {
					e.QueryExecutor.recordReplicationStatement(session, statement)
				}
			}
			e.logSlowQuery(session, query, time.Since(start), rowsAffected, txnID, stage, status, execErr)
			if e.QueryExecutor != nil && e.QueryExecutor.metricsRecorder != nil {
				latency := time.Since(start)
				e.QueryExecutor.metricsRecorder.RecordQuery(databaseName, metricStatementType(query), status, latency)
				threadID := int64(0)
				user, host := "", ""
				if session != nil {
					threadID = int64(sessionConnectionID(session))
					user, _ = session.GetParamByName("user").(string)
					host, _ = session.GetParamByName("host").(string)
				}
				if execErr != nil {
					e.QueryExecutor.recordQueryErrorForSession(session, databaseName, execErr)
				} else if status != "success" {
					// Some legacy branches only set the status and put the error
					// directly on Result.Err. Keep the runtime error counter honest
					// even when that branch has not populated execErr yet.
					e.QueryExecutor.metricsRecorder.RecordQueryError(databaseName, "execution", string(ExecutionErrorCodeUnknown))
				}
				rowsExamined := int64(0)
				selectScan := int64(0)
				if statementContext != nil {
					rowsExamined = statementContext.statementRowsExamined.Load()
					selectScan = statementContext.statementSelectScan.Load()
				}
				statementSummary <- statementExecutionSummary{threadID: threadID, user: user, host: host, status: status, latency: latency, rowsExamined: rowsExamined, selectScan: selectScan}
			}
			if e.QueryExecutor == nil || e.QueryExecutor.metricsRecorder == nil {
				rowsExamined := int64(0)
				selectScan := int64(0)
				if statementContext != nil {
					rowsExamined = statementContext.statementRowsExamined.Load()
					selectScan = statementContext.statementSelectScan.Load()
				}
				statementSummary <- statementExecutionSummary{status: status, latency: time.Since(start), rowsExamined: rowsExamined, selectScan: selectScan}
			}
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
			Session:      session,
		}
		statementContext = ctx
		if session != nil {
			previousProcesslistQuery := session.GetParamByName("processlist_query")
			previousProcesslistStart := session.GetParamByName("processlist_start_time")
			session.SetParamByName("processlist_query", query)
			session.SetParamByName("processlist_start_time", time.Now())
			defer session.SetParamByName("processlist_query", previousProcesslistQuery)
			defer session.SetParamByName("processlist_start_time", previousProcesslistStart)
		}
		if e.QueryExecutor != nil {
			queryContext, cleanup := e.QueryExecutor.beginActiveQuery(session)
			defer cleanup()
			ctx.Context = queryContext
		}
		if effectiveSession, viewSecurityRequired, err := e.QueryExecutor.applyViewExecutionSecurity(session, query, databaseName); err != nil {
			execErr = err
			status = "failed"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
			return
		} else if effectiveSession != session {
			session = effectiveSession
			ctx.Session = effectiveSession
			ctx.ViewSecurityRequired = viewSecurityRequired
		} else {
			ctx.ViewSecurityRequired = viewSecurityRequired
		}
		if rewritten, err2 := rewriteJSONValueReturningQuery(query); err2 != nil {
			execErr = err2
			status = "failed"
			results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_QUERY, Message: err2.Error()}
			return
		} else if rewritten != query {
			query = rewritten
			ctx.RawQuery = rewritten
		}
		if handled, err2 := e.QueryExecutor.executeRawJSONValueCompatibility(ctx, query, databaseName); handled {
			stage = "json-value-returning-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_QUERY, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeRawTemporaryTableCompatibility(ctx, query, databaseName); handled {
			stage = "temporary-table-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_DDL, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeTablespaceCompatibility(ctx, query); handled {
			stage = "tablespace-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_DDL, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeHandlerCompatibility(ctx, session, query, databaseName, results); handled {
			stage = "handler-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_QUERY, Message: err2.Error()}
			}
			return
		}
		if rewritten := e.QueryExecutor.rewriteTemporaryTableReferences(session, query); rewritten != query {
			query = rewritten
			ctx.RawQuery = rewritten
		}
		if rewritten, err2 := rewriteExtractDateFunctions(query); err2 != nil {
			execErr = err2
			status = "failed"
			results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_QUERY, Message: err2.Error()}
			return
		} else if rewritten != query {
			query = rewritten
			ctx.RawQuery = rewritten
		}
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(query)), "select ") {
			if rewritten := e.QueryExecutor.rewriteSessionUserVariables(query, session); rewritten != query {
				query = rewritten
				ctx.RawQuery = rewritten
			}
		}
		if session != nil {
			replaying, _ := session.GetParamByName("replication_replay").(bool)
			if !replaying && (isReplicationDMLQuery(query) || isReplicationDDLQuery(query)) {
				session.SetParamByName("replication_current_statement", replication.Statement{Database: databaseName, SQL: strings.TrimSpace(query)})
			} else {
				session.SetParamByName("replication_current_statement", replication.Statement{})
			}
		}
		if reason := e.QueryExecutor.globalReadOnlyWriteBlockReason(session, query, databaseName); reason != "" {
			err := fmt.Errorf("%s", reason)
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: err.Error()}
			return
		}
		if reason := e.replicationWriteBlockReason(session, query); reason != "" {
			err := fmt.Errorf("%s", reason)
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: err.Error()}
			return
		}
		if rewritten, err2 := e.QueryExecutor.rewriteNestedCTESubqueries(query); err2 != nil {
			execErr = err2
			status = "failed"
			results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			return
		} else if rewritten != "" {
			query = rewritten
			ctx.RawQuery = rewritten
		}
		if err := validateCTEQuerySyntax(query); err != nil {
			execErr = err
			status = "failed"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: err.Error()}
			return
		}
		if unlock, err := e.QueryExecutor.acquireStatementTableLocks(ctx, session, query, databaseName); err != nil {
			execErr = err
			status = "failed"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
			return
		} else {
			defer unlock()
		}
		globalUnlock, globalErr := e.QueryExecutor.acquireGlobalReadLockForStatement(ctx.Context, session, query)
		if globalErr != nil {
			execErr = globalErr
			status = "failed"
			results <- &Result{Err: globalErr, ResultType: common.RESULT_TYPE_QUERY, Message: globalErr.Error()}
			return
		}
		defer globalUnlock()
		if shouldClearSessionWarnings(query) {
			clearSessionWarnings(session)
		}
		if e.QueryExecutor.executeUserVariableAssignment(ctx, query, session) {
			return
		}

		if cmd, name, ok := normalizedTransactionCommand(query); ok {
			stage = "transaction"
			e.QueryExecutor.executeTransactionCommand(ctx, cmd, name, session)
			return
		}
		if handled, err2 := e.QueryExecutor.executeAdminCompatibility(ctx, session, query); handled {
			stage = "admin-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_QUERY, Message: err2.Error()}
				return
			}
			if ctx.AdminResult != nil {
				results <- ctx.AdminResult
				return
			}
			results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "statement executed successfully"}
			return
		}

		if err := rejectUnsupportedCreateTableConstraintsSQL(query); err != nil {
			execErr = err
			status = "failed"
			results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: err.Error()}
			return
		}
		if handled, err2 := e.QueryExecutor.executeShowCreateDatabaseCompatibility(ctx, query); handled {
			stage = "show-create-database-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_QUERY, Message: err2.Error()}
			}
			return
		}

		if isShowFullTablesQuery(query) {
			stage = "show"
			e.QueryExecutor.executeShowFullTablesRaw(ctx, session, query, databaseName)
			return
		}
		if handled := e.QueryExecutor.executeAccountStatement(ctx, query); handled {
			stage = "account"
			return
		}
		if handled := e.QueryExecutor.executeStoredObjectCall(ctx, query, databaseName); handled {
			stage = "stored-object-call"
			return
		}
		if handled := e.QueryExecutor.executeStoredFunctionCall(ctx, query, databaseName); handled {
			stage = "stored-function-call"
			return
		}
		if handled := e.QueryExecutor.executeShowCreateStoredObject(ctx, query, databaseName); handled {
			stage = "show-stored-object"
			return
		}
		if handled := e.QueryExecutor.executeStoredObjectAlter(ctx, query, databaseName); handled {
			stage = "stored-object-alter"
			return
		}
		if handled := e.QueryExecutor.executeStoredObjectDDL(ctx, query, databaseName); handled {
			stage = "stored-object-ddl"
			return
		}
		if handled, err2 := e.QueryExecutor.executeRawCreateTableIndexVisibility(ctx, query, databaseName); handled {
			stage = "create-table-index-visibility-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeRawCreateTableLikeCompatibility(ctx, query, databaseName); handled {
			stage = "create-table-like-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeRawCreateTableAsSelectCompatibility(ctx, query, databaseName); handled {
			stage = "create-table-as-select-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeRawFullTextSpatialCreate(ctx, query, databaseName); handled {
			stage = "advanced-index-create-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeRawStandaloneIndexCompatibility(ctx, query, databaseName); handled {
			stage = "standalone-index-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_DDL, Message: err2.Error()}
			} else {
				results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: "index executed successfully"}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeRawCreatePartitionCompatibility(ctx, query, databaseName); handled {
			stage = "partition-create-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeRawPartitionMaintenance(ctx, query, databaseName); handled {
			stage = "partition-maintenance"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_DDL, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeFullTextQuery(ctx, query, databaseName); handled {
			stage = "fulltext-query-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeSpatialQuery(ctx, query, databaseName); handled {
			stage = "spatial-query-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeRawAlterCompatibility(ctx, query, databaseName); handled {
			stage = "alter-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
				return
			}
			results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: "ALTER TABLE executed successfully"}
			return
		}
		if handled, dropped, err2 := e.QueryExecutor.executeRawDropTableCompatibility(ctx, query, databaseName, session); handled {
			stage = "drop-table-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
				return
			}
			rowsAffected = dropped
			results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("%d table(s) dropped successfully", dropped)}
			return
		}
		if handled, err2 := e.QueryExecutor.executeGeneralWindowQuery(ctx, query, databaseName); handled {
			stage = "window-general-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_QUERY, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeAdvancedWindowQuery(ctx, query, databaseName); handled {
			stage = "window-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_QUERY, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeCTECompatibility(ctx, query, databaseName); handled {
			stage = "cte-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeCorrelatedScalarSubqueryCompatibility(ctx, query, databaseName); handled {
			stage = "correlated-scalar-subquery-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeCorrelatedPredicateSubqueryCompatibility(ctx, query, databaseName); handled {
			stage = "correlated-predicate-subquery-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeCorrelatedDerivedOuterCompatibility(ctx, query, databaseName); handled {
			stage = "correlated-derived-outer-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if handled, err2 := e.QueryExecutor.executeCorrelatedSubqueryCompatibility(ctx, query, databaseName); handled {
			stage = "correlated-subquery-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if rewritten, handled, err2 := e.QueryExecutor.rewriteCorrelatedDMLSubquery(ctx, query, databaseName); handled {
			stage = "correlated-dml-subquery-compatibility"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
				return
			}
			query = rewritten
			ctx.RawQuery = rewritten
		}
		if handled, err2 := e.QueryExecutor.executeSimpleWindowQuery(ctx, query, databaseName); handled {
			stage = "window"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			}
			return
		}
		if handled := e.QueryExecutor.executeViewDDL(ctx, query, databaseName); handled {
			stage = "view-ddl"
			return
		}
		if handled := e.QueryExecutor.executeShowCreateView(ctx, query, databaseName); handled {
			stage = "show-create-view"
			return
		}
		if rewritten, err2 := e.QueryExecutor.rewriteSimpleCTEQuery(query); err2 != nil {
			execErr = err2
			status = "failed"
			results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			return
		} else if rewritten != "" {
			query = rewritten
			ctx.RawQuery = rewritten
		}
		if rewritten, err2 := e.QueryExecutor.rewriteViewQuery(query, databaseName); err2 != nil {
			execErr = err2
			status = "failed"
			results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
			return
		} else if rewritten != "" {
			query = rewritten
			ctx.RawQuery = rewritten
		}

		if handled := e.QueryExecutor.executeAdminReadQuery(ctx, query, databaseName); handled {
			stage = "admin-read"
			return
		}
		if result, handled, err2 := e.QueryExecutor.executePerformanceSchemaSetupUpdate(query); handled {
			stage = "performance-schema-setup-update"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: err2.Error()}
				return
			}
			rowsAffected = result.AffectedRows
			results <- result
			return
		}

		if result, handled, err2 := e.QueryExecutor.executeInformationSchemaMetadataSelect(query, session); handled {
			stage = "metadata-select"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("SELECT failed: %v", err2)}
				return
			}
			result = normalizeInformationSchemaAggregate(query, result)
			rowsAffected = result.RowCount
			results <- &Result{Data: result, ResultType: common.RESULT_TYPE_SELECT}
			return
		}
		if handled, err2 := e.QueryExecutor.executeDerivedTableCompatibility(ctx, query, databaseName); handled {
			stage = "derived-table"
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("derived table failed: %v", err2)}
			}
			return
		}
		if !isInsertOrReplaceQuery(query) {
			if branches, operators, ok := splitSetOperationQuery(query); ok && hasNonUnionSetOperator(operators) {
				stage = "mixed-set-operation"
				result, err2 := e.QueryExecutor.executeMixedSetOperationQuery(ctx, branches, operators, databaseName)
				if err2 != nil {
					execErr = err2
					status = "failed"
					results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("set operation failed: %v", err2)}
					return
				}
				rowsAffected = result.RowCount
				results <- &Result{Data: result, ResultType: common.RESULT_TYPE_SELECT}
				return
			}

			if branches, unionAll, ok := splitUnionQuery(query); ok {
				stage = "union"
				result, err2 := e.QueryExecutor.executeUnionQuery(ctx, branches, unionAll, databaseName)
				if err2 != nil {
					execErr = err2
					status = "failed"
					results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("UNION failed: %v", err2)}
					return
				}
				rowsAffected = result.RowCount
				results <- &Result{Data: result, ResultType: common.RESULT_TYPE_SELECT}
				return
			}
		}
		if branches, operators, ok := splitIntersectExceptQuery(query); ok {
			stage = "set-operation"
			result, err2 := e.QueryExecutor.executeIntersectExceptQuery(ctx, branches, operators, databaseName)
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("set operation failed: %v", err2)}
				return
			}
			rowsAffected = result.RowCount
			results <- &Result{Data: result, ResultType: common.RESULT_TYPE_SELECT}
			return
		}

		if rewritten, err2 := e.QueryExecutor.rewriteSimpleInSubquery(ctx, query, databaseName); err2 != nil {
			execErr = err2
			status = "failed"
			results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("subquery failed: %v", err2)}
			return
		} else if rewritten != "" {
			query = rewritten
			ctx.RawQuery = rewritten
		}
		if rewritten, err2 := e.QueryExecutor.rewriteSimpleQuantifiedSubqueries(ctx, query, databaseName); err2 != nil {
			execErr = err2
			status = "failed"
			results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("quantified subquery failed: %v", err2)}
			return
		} else if rewritten != "" {
			query = rewritten
			ctx.RawQuery = rewritten
		}
		if rewritten, err2 := e.QueryExecutor.rewriteSimpleScalarSubqueries(ctx, query, databaseName); err2 != nil {
			execErr = err2
			status = "failed"
			results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("scalar subquery failed: %v", err2)}
			return
		} else if rewritten != "" {
			query = rewritten
			ctx.RawQuery = rewritten
		}
		if rewritten := e.QueryExecutor.rewriteSessionUserVariables(query, session); rewritten != query {
			query = rewritten
			ctx.RawQuery = rewritten
		}
		if rewritten := rewriteSessionMetadataFunctions(query); rewritten != query {
			query = rewritten
			ctx.RawQuery = rewritten
		}

		if handled, err := e.QueryExecutor.executeXACompatibility(ctx, session, query); handled {
			if err != nil {
				execErr = err
				status = "failed"
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY, Message: err.Error()}
			} else if !isXARecoverQuery(query) {
				results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "XA statement executed successfully"}
			}
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

		case *sqlparser.Union:
			stage = "metadata-union"
			result, handled, err2 := e.QueryExecutor.executeInformationSchemaMetadataSelect(query, session)
			if !handled {
				execErr = fmt.Errorf("unsupported statement type")
				status = "failed"
				results <- &Result{Err: execErr, ResultType: common.RESULT_TYPE_ERROR}
				return
			}
			if err2 != nil {
				execErr = err2
				status = "failed"
				results <- &Result{Err: err2, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("SELECT failed: %v", err2)}
				return
			}
			result = normalizeInformationSchemaAggregate(query, result)
			rowsAffected = result.RowCount
			results <- &Result{Data: result, ResultType: common.RESULT_TYPE_SELECT}

		case *sqlparser.DDL:
			releaseDDL, ddlLockErr := e.QueryExecutor.acquireSimpleDDLTableLock(ctx, session, stmt.Table, stmt.Action, databaseName)
			if ddlLockErr != nil {
				execErr = ddlLockErr
				status = "failed"
				results <- &Result{Err: ddlLockErr, ResultType: common.RESULT_TYPE_DDL, Message: ddlLockErr.Error()}
				return
			}
			defer releaseDDL()
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
				e.QueryExecutor.executeDropTableStatement(ctx, currentDB, stmt, session)
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
				e.QueryExecutor.executeAlterTableStatement(ctx, currentDB, stmt, ctx.RawQuery)
			default:
				execErr = fmt.Errorf("unsupported DDL action: %s", stmt.Action)
				status = "failed"
				results <- &Result{Err: fmt.Errorf("unsupported DDL action: %s", stmt.Action), ResultType: common.RESULT_TYPE_ERROR}
			}

		case *sqlparser.DBDDL:
			if ddlCommitErr := e.QueryExecutor.prepareDDLImplicitCommit(session); ddlCommitErr != nil {
				execErr = ddlCommitErr
				status = "failed"
				results <- &Result{Err: ddlCommitErr, ResultType: common.RESULT_TYPE_DDL, Message: ddlCommitErr.Error()}
				return
			}
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
			if err := e.QueryExecutor.prepareTransactionalDML(session); err != nil {
				execErr = err
				status = "failed"
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: err.Error()}
				return
			}
			explicitSchema := strings.TrimSpace(stmt.Table.Qualifier.String())
			result, err := e.QueryExecutor.executeInsertStatement(ctx, stmt, e.resolveDmlDatabaseName(session, databaseName, explicitSchema), session)
			if err != nil {
				execErr = err
				status = "failed"
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("INSERT failed: %v", err)}
			} else {
				txnID = result.TxnID
				rowsAffected = result.AffectedRows
				if session != nil {
					session.SetParamByName("row_count", int64(result.AffectedRows))
				}
				results <- &Result{
					Data:         result,
					AffectedRows: result.AffectedRows,
					LastInsertID: result.LastInsertId,
					Warnings:     result.Warnings,
					ResultType:   common.RESULT_TYPE_QUERY,
					Message:      result.Message,
				}
			}

		case *sqlparser.Update:
			// 处理UPDATE语句
			stage = "update"
			logger.Debugf("✏️ 处理UPDATE语句")
			if err := e.QueryExecutor.prepareTransactionalDML(session); err != nil {
				execErr = err
				status = "failed"
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: err.Error()}
				return
			}
			explicitSchema := e.extractTableExprSchema(stmt.TableExprs)
			result, err := e.QueryExecutor.executeUpdateStatement(ctx, stmt, e.resolveDmlDatabaseName(session, databaseName, explicitSchema), session)
			if err != nil {
				execErr = err
				status = "failed"
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("UPDATE failed: %v", err)}
			} else {
				txnID = result.TxnID
				rowsAffected = result.AffectedRows
				if session != nil {
					session.SetParamByName("row_count", int64(result.AffectedRows))
				}
				results <- &Result{
					Data:         result,
					AffectedRows: result.AffectedRows,
					LastInsertID: result.LastInsertId,
					Warnings:     result.Warnings,
					ResultType:   common.RESULT_TYPE_QUERY,
					Message:      result.Message,
				}
			}

		case *sqlparser.Delete:
			// 处理DELETE语句
			stage = "delete"
			logger.Debugf("🗑️ 处理DELETE语句")
			if err := e.QueryExecutor.prepareTransactionalDML(session); err != nil {
				execErr = err
				status = "failed"
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: err.Error()}
				return
			}
			explicitSchema := e.extractTableExprSchema(stmt.TableExprs)
			result, err := e.QueryExecutor.executeDeleteStatement(ctx, stmt, e.resolveDmlDatabaseName(session, databaseName, explicitSchema), session)
			if err != nil {
				execErr = err
				status = "failed"
				results <- &Result{Err: err, ResultType: common.RESULT_TYPE_ERROR, Message: fmt.Sprintf("DELETE failed: %v", err)}
			} else {
				txnID = result.TxnID
				rowsAffected = result.AffectedRows
				if session != nil {
					session.SetParamByName("row_count", int64(result.AffectedRows))
				}
				results <- &Result{
					Data:         result,
					AffectedRows: result.AffectedRows,
					LastInsertID: result.LastInsertId,
					Warnings:     result.Warnings,
					ResultType:   common.RESULT_TYPE_QUERY,
					Message:      result.Message,
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

// ResetSession applies the engine-side rollback and session-state reset used
// by the wire-level COM_RESET_CONNECTION handlers.
func (e *XMySQLEngine) ResetSession(session server.MySQLServerSession) error {
	if e == nil || e.QueryExecutor == nil {
		return nil
	}
	return e.QueryExecutor.ResetSession(session)
}

// CleanupTemporaryTables releases all connection-local temporary tables.
func (e *XMySQLEngine) CleanupTemporaryTables(session server.MySQLServerSession) error {
	if e == nil || e.QueryExecutor == nil {
		return nil
	}
	return e.QueryExecutor.CleanupTemporaryTables(session)
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
	var execErr *ExecutionError
	if errors.As(err, &execErr) && execErr != nil && execErr.ErrorCode != "" {
		return string(execErr.ErrorCode)
	}
	return string(ExecutionErrorCodeUnknown)
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
